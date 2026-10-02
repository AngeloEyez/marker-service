"""
================================================================================
模組名稱: server.converter
用途說明: Marker 文件轉換核心包裝器、階段進度回報與協同中斷管線
技術規格:
  - 核心類別: CommonParams (API 輸入參數模型), ProgressPdfConverter (動態繼承包裝器)
  - 核心函式: get_progress_converter_class(), execute_conversion()
  - 整合模型: Surya OCR (GPU 0), Marker PyTorch 啟發式模型 (GPU 1), 遠端 vLLM Qwen3.8-27B
  - 協同中斷: 在各階段 (Layout OCR、Structure、LLM Processors) 插入 job_manager.is_cancelled()
    檢測，若偵測到中斷訊號立即拋出 InterruptedError，即刻終止運算並釋放 GPU/顯存資源。
維護指南:
  - AI Agent 擴充處理器階段日誌或提示詞注入時，請維持各 processor 執行前後的中斷安全檢查。
================================================================================
"""

import base64
import io
import os
import time
import traceback
from typing import Annotated, Any, Dict, List, Optional
from pydantic import BaseModel, Field

from marker.config.parser import ConfigParser
from marker.logger import get_logger
from marker.models import create_model_dict
from marker.output import text_from_rendered
from marker.settings import settings as marker_settings
from marker.providers.registry import provider_from_filepath
from marker.builders.document import DocumentBuilder
from marker.builders.line import LineBuilder
from marker.builders.ocr import OcrBuilder
from marker.builders.structure import StructureBuilder

from server.config import settings
from server.jobs import TaskCancelledException, job_manager

logger = get_logger()


# ==============================================================================
# 參數模型定義 (Swagger / REST API 參數規格)
# ==============================================================================
class CommonParams(BaseModel):
    filepath: Annotated[str, Field(description="PDF 或文件檔案路徑")]
    use_llm: Annotated[bool, Field(description="是否啟用遠端 LLM (Qwen3.8-27B) 進行高精準度後校正 (預設: True)", default=True)]
    mode: Annotated[str, Field(description="轉換模式: 'balanced' (GPU 最佳，預設) 或 'fast' (CPU 最佳)", default="balanced")]
    output_format: Annotated[str, Field(description="輸出格式: 'markdown', 'json', 'html', 'chunks'", default="markdown")]
    page_range: Annotated[Optional[str], Field(description="轉換頁數範圍，預設為空 (處理整份文件)；如需指定頁數可填寫例如: 0,5-10,20", default=None)]
    force_ocr: Annotated[bool, Field(description="強制對所有頁面進行 OCR", default=False)]
    paginate_output: Annotated[bool, Field(description="輸出內容是否分頁標註 (預設: True)", default=True)]
    strip_existing_ocr: Annotated[bool, Field(description="清除現有劣質 OCR 並由本機 Surya 重新 OCR", default=False)]
    disable_image_extraction: Annotated[bool, Field(description="不抽取內嵌圖片", default=False)]
    remove_watermarks: Annotated[bool, Field(description="是否啟用 VLM/LLM 智慧浮水印與背景雜訊去除功能 (自動帶入專屬英文 Prompt 進行校正)", default=False)]
    block_correction_prompt: Annotated[Optional[str], Field(description="自訂 block_correction_prompt 提示詞 (若為空且 remove_watermarks=True，則自動採用預設英文浮水印去除 Prompt)", default=None)]
    reasoning_effort: Annotated[Optional[str], Field(description="LLM 思考推理強度等級 ('low', 'medium', 'high', 'none')，若未指定則使用環境變數 LLM_REASONING_EFFORT", default=None)]
    enable_thinking: Annotated[Optional[bool], Field(description="是否啟用 LLM 思維鏈/思考過程 (若未指定則使用環境變數 LLM_ENABLE_THINKING)", default=None)]


# ==============================================================================
# 具備階段進度回報與協同中斷之客製化 Marker 轉換器
# ==============================================================================
def get_progress_converter_class(base_converter_cls, job_id: Optional[str], effective_prompt: Optional[str] = None):
    """
    包裝 Marker 原生 Converter，提供即時細部進度推播與幽靈任務快速中斷檢測
    """
    if not job_id:
        return base_converter_cls

    class ProgressPdfConverter(base_converter_cls):
        def build_document(self, filepath: str):
            # 1. 執行前檢測是否已被中斷
            if job_id and job_manager.is_cancelled(job_id):
                raise TaskCancelledException(f"任務 [{job_id}] 已在開始前被使用者取消")

            job_manager.update_job(
                job_id,
                status="processing",
                stage="layout_ocr",
                progress=25,
                message="正在使用 GPU 0 (RTX 3050) 執行 Surya OCR 與版面偵測...",
            )

            provider_cls = provider_from_filepath(filepath)
            layout_builder = self.resolve_dependencies(self.layout_builder_class)
            line_builder = self.resolve_dependencies(LineBuilder)
            ocr_builder = self.resolve_dependencies(OcrBuilder)
            provider = provider_cls(filepath, self.config)
            document = DocumentBuilder(self.config)(
                provider, layout_builder, line_builder, ocr_builder
            )

            # 2. 版面偵測後檢測中斷
            if job_id and job_manager.is_cancelled(job_id):
                raise TaskCancelledException(f"任務 [{job_id}] 於版面分析階段被使用者中斷")

            job_manager.update_job(
                job_id,
                stage="structure",
                progress=50,
                message="正在解析閱讀順序、段落階層與目錄結構...",
            )

            structure_builder_cls = self.resolve_dependencies(StructureBuilder)
            structure_builder_cls(document)

            # 3. 處理器迴圈 (含 LLM 語意精修與浮水印去除)
            total_procs = len(self.processor_list)
            for idx, processor in enumerate(self.processor_list):
                # 處理器單步前檢測中斷訊號
                if job_id and job_manager.is_cancelled(job_id):
                    raise TaskCancelledException(f"任務 [{job_id}] 於處理器執行階段被使用者中斷")

                pname = getattr(processor, "__name__", processor.__class__.__name__)
                pct = 50 + int(((idx + 1) / max(total_procs, 1)) * 38)
                if "llm" in pname.lower():
                    if "pagecorrection" in pname.lower() and effective_prompt:
                        msg = "正在透過遠端 vLLM (Qwen3.8-27B) 執行全頁區塊重整與浮水印雜訊去除 (LLMPageCorrectionProcessor)..."
                    else:
                        msg = f"正在透過遠端 vLLM (Qwen3.8-27B) 進行高精準度語意與格式校正 ({pname})..."
                    stg = "llm_refinement"
                else:
                    msg = f"正在執行文件處理模組: {pname}..."
                    stg = "processing"

                job_manager.update_job(job_id, stage=stg, progress=pct, message=msg)
                processor(document)

            return document

    return ProgressPdfConverter


def execute_conversion(params: CommonParams, job_id: Optional[str] = None, models: Optional[dict] = None) -> dict:
    """
    執行單一文件轉換任務的完整管線
    
    Args:
        params: 轉換輸入參數
        job_id: 任務識別碼 (若為 None 則不記錄狀態)
        models: 共享預載模型快取字典 (若為 None 自動初始化)
        
    Returns:
        dict: 轉換成果字典 (包含 success, output, images, metadata 等)
    """
    if params.output_format not in ["markdown", "json", "html", "chunks"]:
        raise ValueError(f"不支援的輸出格式: {params.output_format}")

    # 檢查是否在進入轉換前就已被中斷
    if job_id and job_manager.is_cancelled(job_id):
        logger.info(f"[execute_conversion] 任務 [{job_id}] 已標記為中斷，略過執行。")
        return {"success": False, "status": "cancelled", "message": "任務已被使用者手動取消", "job_id": job_id}

    if job_id:
        job_manager.update_job(
            job_id,
            status="processing",
            stage="preparing",
            progress=15,
            message="正在載入配置參數與轉換核心...",
        )

    # 決定有效 block_correction_prompt
    effective_prompt = params.block_correction_prompt
    if not effective_prompt and params.remove_watermarks:
        effective_prompt = settings.DEFAULT_WATERMARK_REMOVAL_PROMPT
    if not effective_prompt and settings.DEFAULT_BLOCK_CORRECTION_PROMPT:
        effective_prompt = settings.DEFAULT_BLOCK_CORRECTION_PROMPT

    use_llm_effective = params.use_llm or bool(effective_prompt)

    options = {
        "output_format": params.output_format,
        "mode": params.mode or settings.DEFAULT_MODE,
        "force_ocr": params.force_ocr,
        "paginate_output": params.paginate_output,
        "strip_existing_ocr": params.strip_existing_ocr,
        "disable_image_extraction": params.disable_image_extraction,
        "page_range": params.page_range if params.page_range and params.page_range.strip() else None,
    }

    if effective_prompt:
        options["block_correction_prompt"] = effective_prompt

    if use_llm_effective:
        options["use_llm"] = True
        options["llm_service"] = "server.services.OptimizedOpenAIService"
        os.environ["OPENAI_BASE_URL"] = settings.REMOTE_LLM_URL
        os.environ["OPENAI_MODEL"] = settings.REMOTE_LLM_MODEL
        os.environ["OPENAI_API_KEY"] = settings.REMOTE_LLM_API_KEY
    else:
        options["use_llm"] = False

    try:
        config_parser = ConfigParser(options)
        config_dict = config_parser.generate_config_dict()
        config_dict["pdftext_workers"] = 1
        config_dict["timeout"] = settings.LLM_TIMEOUT
        config_dict["max_retries"] = settings.LLM_MAX_RETRIES

        if effective_prompt:
            config_dict["block_correction_prompt"] = effective_prompt

        if use_llm_effective:
            config_dict["openai_base_url"] = settings.REMOTE_LLM_URL
            config_dict["openai_model"] = settings.REMOTE_LLM_MODEL
            config_dict["openai_api_key"] = settings.REMOTE_LLM_API_KEY
            config_dict["reasoning_effort"] = (
                params.reasoning_effort if params.reasoning_effort is not None else settings.LLM_REASONING_EFFORT
            )
            config_dict["enable_thinking"] = (
                params.enable_thinking if params.enable_thinking is not None else settings.LLM_ENABLE_THINKING
            )

        converter_cls = config_parser.get_converter_cls()
        wrapped_cls = get_progress_converter_class(converter_cls, job_id, effective_prompt=effective_prompt)

        if models is None:
            models = create_model_dict()

        converter = wrapped_cls(
            config=config_dict,
            artifact_dict=models,
            processor_list=config_parser.get_processors(),
            renderer=config_parser.get_renderer(),
            llm_service=config_parser.get_llm_service(),
        )

        if hasattr(converter, "llm_service") and converter.llm_service:
            converter.llm_service.current_job_id = job_id

        # 呼叫轉換
        rendered = converter(params.filepath)

        if job_id and job_manager.is_cancelled(job_id):
            raise TaskCancelledException(f"任務 [{job_id}] 於渲染前被使用者手動取消")

        if job_id:
            job_manager.update_job(
                job_id,
                stage="rendering",
                progress=92,
                message="正在渲染輸出 Markdown 與抽取圖表影像...",
            )

        text, _, images = text_from_rendered(rendered)
        metadata = rendered.metadata

        encoded_images = {}
        for k, v in images.items():
            byte_stream = io.BytesIO()
            v.save(byte_stream, format=marker_settings.OUTPUT_IMAGE_FORMAT)
            encoded_images[k] = base64.b64encode(byte_stream.getvalue()).decode(
                marker_settings.OUTPUT_ENCODING
            )

        result = {
            "success": True,
            "format": params.output_format,
            "output": text,
            "images": encoded_images,
            "images_count": len(encoded_images),
            "metadata": metadata,
            "job_id": job_id,
        }

        if job_id:
            job_manager.update_job(
                job_id,
                status="completed",
                stage="done",
                progress=100,
                message="文件轉換成功完成！",
                result=result,
            )

        return result

    except (TaskCancelledException, InterruptedError) as e:
        # 手動中斷例外處理
        msg = str(e)
        logger.warning(f"[execute_conversion] 攔截到手動中斷訊號: {msg}")
        if job_id:
            job_manager.cancel_job(job_id, reason=msg)
        return {
            "success": False,
            "status": "cancelled",
            "message": msg,
            "job_id": job_id,
        }

    except Exception as e:
        err_msg = str(e)
        tb = traceback.format_exc()
        traceback.print_exc()
        if job_id:
            job_manager.update_job(
                job_id,
                status="failed",
                stage="error",
                message=f"轉換失敗: {err_msg}",
                error=tb,
            )
        return {
            "success": False,
            "error": err_msg,
            "traceback": tb,
            "job_id": job_id,
        }
