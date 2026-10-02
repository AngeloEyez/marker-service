import asyncio
import base64
from contextlib import asynccontextmanager
from datetime import datetime
import io
import json
import os
import shutil
import time
import traceback
from typing import Annotated, Any, Dict, List, Optional, Set, Tuple
import uuid
import zipfile

import requests
import torch
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field

from marker.config.parser import ConfigParser
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict, shutdown_models
from marker.output import text_from_rendered
from marker.settings import settings as marker_settings
from marker.providers.registry import provider_from_filepath
from marker.builders.document import DocumentBuilder
from marker.builders.line import LineBuilder
from marker.builders.ocr import OcrBuilder
from marker.builders.structure import StructureBuilder
from server.config import settings

app_data = {}


# ==============================================================================
# 即時任務與進度追蹤管理器 (Job & Progress Manager)
# ==============================================================================
class JobInfo(BaseModel):
    job_id: str
    filename: str
    status: str = "queued"  # queued, processing, completed, failed
    stage: str = "uploaded"  # uploaded, layout_ocr, structure, processors, llm_refinement, rendering, done, error
    progress: int = 0
    message: str = "任務已排隊..."
    queue_position: Optional[int] = None
    file_path: Optional[str] = None
    created_at: float
    updated_at: float
    elapsed_seconds: float = 0.0
    error: Optional[str] = None
    result: Optional[dict] = None
    logs: List[dict] = []


class JobManager:
    def __init__(self):
        self.jobs: Dict[str, JobInfo] = {}
        self.listeners: Dict[str, List[asyncio.Queue]] = {}

    def create_job(self, filename: str, file_path: Optional[str] = None) -> JobInfo:
        job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        now = time.time()
        job = JobInfo(
            job_id=job_id,
            filename=filename,
            status="queued",
            stage="uploaded",
            progress=5,
            message="檔案上傳成功，排隊處理中...",
            file_path=file_path,
            created_at=now,
            updated_at=now,
            logs=[{"time": datetime.now().strftime("%H:%M:%S"), "msg": f"檔案「{filename}」已上傳，建立任務 ID: {job_id}"}],
        )
        self.jobs[job_id] = job
        self.listeners[job_id] = []
        job.queue_position = self.get_queue_position(job_id)

        # 記憶體配額保護：只從 completed 或 failed 的歷史任務中淘汰超額紀錄，絕不淘汰 queued 或 processing 中的任務！
        completed_keys = [k for k, v in self.jobs.items() if v.status in ["completed", "failed"]]
        if len(completed_keys) > settings.MAX_COMPLETED_JOBS_HISTORY:
            oldest_completed = min(completed_keys, key=lambda k: self.jobs[k].created_at)
            self.jobs.pop(oldest_completed, None)
            self.listeners.pop(oldest_completed, None)
        return job

    def update_job(
        self,
        job_id: str,
        status: Optional[str] = None,
        stage: Optional[str] = None,
        progress: Optional[int] = None,
        message: Optional[str] = None,
        error: Optional[str] = None,
        result: Optional[dict] = None,
    ):
        if job_id not in self.jobs:
            return
        job = self.jobs[job_id]
        now = time.time()
        if status:
            job.status = status
        if stage:
            job.stage = stage
        if progress is not None:
            job.progress = min(max(progress, 0), 100)
        if message:
            job.message = message
            job.logs.append({"time": datetime.now().strftime("%H:%M:%S"), "msg": message})
            # 限制單一任務最新日誌最多保留 150 筆，避免長任務消耗記憶體
            if len(job.logs) > 150:
                job.logs = job.logs[-150:]
        if error:
            job.error = error
        if result:
            job.result = result
        job.updated_at = now
        job.elapsed_seconds = round(now - job.created_at, 2)
        job.queue_position = self.get_queue_position(job_id)

        # 實時推播給所有 SSE 串流連線
        data_dict = job.dict()
        for queue in list(self.listeners.get(job_id, [])):
            try:
                queue.put_nowait(data_dict)
            except Exception:
                pass

    def get_job(self, job_id: str) -> Optional[JobInfo]:
        if job_id in self.jobs:
            job = self.jobs[job_id]
            if job.status == "processing":
                job.elapsed_seconds = round(time.time() - job.created_at, 2)
            job.queue_position = self.get_queue_position(job_id)
            return job
        return None

    def get_queued_jobs(self) -> List[JobInfo]:
        queued = [j for j in self.jobs.values() if j.status == "queued"]
        queued.sort(key=lambda x: x.created_at)
        return queued

    def get_queued_count(self) -> int:
        return sum(1 for j in self.jobs.values() if j.status == "queued")

    def get_active_count(self) -> int:
        return sum(1 for j in self.jobs.values() if j.status == "processing")

    def get_active_file_identifiers(self) -> Tuple[Set[str], Set[str]]:
        """
        取得所有正在排隊 (queued) 或正在執行 (processing) 的任務所關聯的暫存檔案絕對路徑、檔名與 job_id 集合。
        這些檔案屬於白名單，任何清理程序均絕對嚴禁刪除！
        """
        active_paths = set()
        active_job_ids = set()
        for j in self.jobs.values():
            if j.status in ["queued", "processing"]:
                active_job_ids.add(j.job_id)
                if j.file_path:
                    active_paths.add(os.path.abspath(j.file_path))
                    active_paths.add(os.path.basename(j.file_path))
        return active_paths, active_job_ids

    def get_queue_position(self, job_id: str) -> Optional[int]:
        if job_id not in self.jobs or self.jobs[job_id].status != "queued":
            return None
        queued = self.get_queued_jobs()
        for idx, j in enumerate(queued):
            if j.job_id == job_id:
                return idx + 1
        return None

    def notify_queue_update(self):
        queued = self.get_queued_jobs()
        for idx, j in enumerate(queued):
            j.queue_position = idx + 1
            self.update_job(
                j.job_id,
                message=f"目前 GPU 運算資源忙碌中，任務正在排隊中 (排隊順位: 第 {idx + 1} 位)...",
            )

    def cleanup_expired_jobs(self, max_retention_seconds: int) -> int:
        now = time.time()
        to_remove = []
        for jid, job in self.jobs.items():
            if job.status in ["completed", "failed"] and (now - job.created_at) > max_retention_seconds:
                to_remove.append(jid)
        for jid in to_remove:
            self.jobs.pop(jid, None)
            self.listeners.pop(jid, None)
        return len(to_remove)

    def register_listener(self, job_id: str) -> asyncio.Queue:
        if job_id not in self.listeners:
            self.listeners[job_id] = []
        q = asyncio.Queue()
        self.listeners[job_id].append(q)
        return q

    def unregister_listener(self, job_id: str, q: asyncio.Queue):
        if job_id in self.listeners and q in self.listeners[job_id]:
            self.listeners[job_id].remove(q)


job_manager = JobManager()


# ==============================================================================
# 具備階段進度回報之客製化 Marker 轉換器
# ==============================================================================
def get_progress_converter_class(base_converter_cls, job_id: Optional[str], effective_prompt: Optional[str] = None):
    if not job_id:
        return base_converter_cls

    class ProgressPdfConverter(base_converter_cls):
        def build_document(self, filepath: str):
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

            job_manager.update_job(
                job_id,
                stage="structure",
                progress=50,
                message="正在解析閱讀順序、段落階層與目錄結構...",
            )

            structure_builder_cls = self.resolve_dependencies(StructureBuilder)
            structure_builder_cls(document)

            total_procs = len(self.processor_list)
            for idx, processor in enumerate(self.processor_list):
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


conversion_semaphore: Optional[asyncio.Semaphore] = None


async def periodic_cleanup_worker():
    """
    背景自動垃圾清理循環（預設每天執行一次，CLEANUP_INTERVAL_SECONDS=86400）：
    1. 定期清理 /tmp/marker_uploads 中超過 FILE_RETENTION_SECONDS (預設 24 小時) 的孤立暫存檔案
       【關鍵安全白名單機制】：
       自動識別並豁免所有處於 queued (排隊等待中) 或 processing (執行中) 的任務檔案。
       無論排隊等待多久，檔案均受白名單絕對保護，絕不誤刪！
    2. 定期從記憶體中淘汰超過 JOB_RETENTION_SECONDS (預設 24 小時) 的已結束 (completed/failed) 任務
    3. 定期調用 torch.cuda.empty_cache() 釋放 PyTorch 顯存碎片
    """
    print(
        f"[Marker Service] Periodic cleanup worker started "
        f"(Interval: {settings.CLEANUP_INTERVAL_SECONDS}s, "
        f"File retention: {settings.FILE_RETENTION_SECONDS}s, "
        f"Job retention: {settings.JOB_RETENTION_SECONDS}s, "
        f"Active task whitelist protection: ENABLED)"
    )
    while True:
        try:
            await asyncio.sleep(settings.CLEANUP_INTERVAL_SECONDS)
            now = time.time()
            upload_dir = settings.UPLOAD_DIRECTORY
            if os.path.exists(upload_dir):
                active_paths, active_job_ids = job_manager.get_active_file_identifiers()
                for fname in os.listdir(upload_dir):
                    fpath = os.path.abspath(os.path.join(upload_dir, fname))

                    # 關鍵安全防護 1: 完整路徑或檔名命中活躍任務白名單，絕對不刪
                    if fpath in active_paths or fname in active_paths:
                        continue

                    # 關鍵安全防護 2: 檔名前綴屬於任何處於 queued 或 processing 狀態的 job_id，絕對不刪
                    if any(fname.startswith(jid) for jid in active_job_ids):
                        continue

                    # 非活躍任務檔案（真正的孤立殘留檔案）：檢查修改時間是否超過閾值
                    try:
                        if os.path.isfile(fpath):
                            mtime = os.path.getmtime(fpath)
                            if (now - mtime) > settings.FILE_RETENTION_SECONDS:
                                os.remove(fpath)
                                print(f"[Cleanup] Deleted stale orphan upload file: {fname}")
                    except Exception as e:
                        print(f"[Cleanup] Error deleting stale orphan file {fpath}: {e}")

            evicted = job_manager.cleanup_expired_jobs(settings.JOB_RETENTION_SECONDS)
            if evicted > 0:
                print(f"[Cleanup] Evicted {evicted} expired jobs from memory.")

            if torch.cuda.is_available():
                torch.cuda.empty_cache()

        except asyncio.CancelledError:
            print("[Marker Service] Periodic cleanup worker stopped.")
            break
        except Exception as e:
            print(f"[Cleanup] Unexpected error in cleanup loop: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global conversion_semaphore
    conversion_semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_CONVERSIONS)
    print(f"[Marker Service] Concurrency limiter enabled (Max concurrent conversions: {settings.MAX_CONCURRENT_CONVERSIONS})")

    print("[Marker Service] Initializing models and inference manager...")
    try:
        if not os.getenv("SURYA_INFERENCE_BACKEND"):
            os.environ["SURYA_INFERENCE_BACKEND"] = settings.SURYA_INFERENCE_BACKEND
        if not os.getenv("LLAMA_CPP_NGL"):
            os.environ["LLAMA_CPP_NGL"] = str(settings.LLAMA_CPP_NGL)

        app_data["models"] = create_model_dict()
        print("[Marker Service] Models successfully initialized.")
    except Exception as e:
        print(f"[Marker Service] Note during model initialization: {e}")
        app_data["models"] = None

    cleanup_task = asyncio.create_task(periodic_cleanup_worker())

    yield

    cleanup_task.cancel()
    if "models" in app_data and app_data["models"] is not None:
        print("[Marker Service] Shutting down models...")
        try:
            shutdown_models(app_data["models"])
        except Exception as e:
            print(f"[Marker Service] Error during shutdown: {e}")
        del app_data["models"]


app = FastAPI(
    title="Marker Document Conversion Microservice",
    description="具備雙 GPU 加速與遠端 vLLM Qwen3.8-27B 語意校正之高品質文件轉換微服務，支援實時進度串流 (SSE) 與非同步任務查詢。",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs(settings.UPLOAD_DIRECTORY, exist_ok=True)


# ==============================================================================
# 參數模型定義 (Swagger 預設值配置)
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


def _execute_conversion(params: CommonParams, job_id: Optional[str] = None):
    if params.output_format not in ["markdown", "json", "html", "chunks"]:
        raise HTTPException(status_code=400, detail=f"不支援的輸出格式: {params.output_format}")

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

        models = app_data.get("models")
        if models is None:
            models = create_model_dict()
            app_data["models"] = models

        converter = wrapped_cls(
            config=config_dict,
            artifact_dict=models,
            processor_list=config_parser.get_processors(),
            renderer=config_parser.get_renderer(),
            llm_service=config_parser.get_llm_service(),
        )

        rendered = converter(params.filepath)

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


# ==============================================================================
# API 路由實作
# ==============================================================================
@app.get("/health")
async def health_check():
    cuda_available = torch.cuda.is_available()
    gpu_list = []
    if cuda_available:
        for i in range(torch.cuda.device_count()):
            gpu_list.append({
                "index": i,
                "name": torch.cuda.get_device_name(i),
                "total_memory_mb": round(torch.cuda.get_device_properties(i).total_memory / (1024 * 1024), 2),
            })

    remote_llm_status = {"configured_url": settings.REMOTE_LLM_URL, "connected": False, "models": []}
    try:
        r = requests.get(f"{settings.REMOTE_LLM_URL.rstrip('/')}/models", timeout=2)
        if r.status_code == 200:
            data = r.json().get("data", [])
            remote_llm_status["connected"] = True
            remote_llm_status["models"] = [m.get("id") for m in data]
    except Exception as e:
        remote_llm_status["error"] = str(e)

    status = "healthy" if cuda_available else "degraded (no cuda)"

    return {
        "status": status,
        "cuda_available": cuda_available,
        "gpu_count": len(gpu_list),
        "gpus": gpu_list,
        "remote_vllm": remote_llm_status,
        "models_loaded": app_data.get("models") is not None,
        "queue_status": {
            "max_concurrent_limit": settings.MAX_CONCURRENT_CONVERSIONS,
            "active_processing_jobs": job_manager.get_active_count(),
            "queued_waiting_jobs": job_manager.get_queued_count(),
            "total_jobs_in_memory": len(job_manager.jobs),
        },
        "retention_policy": {
            "job_retention_seconds": settings.JOB_RETENTION_SECONDS,
            "file_retention_seconds": settings.FILE_RETENTION_SECONDS,
            "cleanup_interval_seconds": settings.CLEANUP_INTERVAL_SECONDS,
            "protected_active_tasks_count": len(job_manager.get_active_file_identifiers()[1]),
        },
    }


@app.post("/marker")
async def convert_file_path(params: CommonParams):
    if not os.path.exists(params.filepath):
        raise HTTPException(status_code=404, detail=f"檔案不存在: {params.filepath}")
    job = job_manager.create_job(os.path.basename(params.filepath), file_path=params.filepath)
    async with conversion_semaphore:
        try:
            return await run_in_threadpool(_execute_conversion, params, job.job_id)
        finally:
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
            job_manager.notify_queue_update()


@app.post("/marker/upload")
async def convert_uploaded_file(
    file: UploadFile = File(..., description="待轉換的文件檔案 (PDF/圖片/DOCX/PPTX 等)"),
    use_llm: bool = Form(default=True, description="是否啟用遠端 LLM (Qwen3.8-27B) 校正 (預設: True)"),
    mode: str = Form(default="balanced", description="轉換模式: balanced (GPU 最佳，預設) 或 fast (CPU)"),
    output_format: str = Form(default="markdown", description="輸出格式: markdown (預設), json, html, chunks"),
    page_range: Optional[str] = Form(default=None, description="分頁範圍，預設為空 (處理整份文件)；如需指定可填寫例如: 0,2-5"),
    force_ocr: bool = Form(default=False, description="是否強制對所有頁面進行 OCR"),
    paginate_output: bool = Form(default=True, description="輸出內容是否分頁標註 (預設: True)"),
    strip_existing_ocr: bool = Form(default=False, description="是否移除劣質 OCR 並由本機重新辨識"),
    disable_image_extraction: bool = Form(default=False, description="是否停用圖片抽取"),
    remove_watermarks: bool = Form(default=False, description="是否啟用 VLM/LLM 智慧浮水印與背景雜訊去除功能"),
    block_correction_prompt: Optional[str] = Form(default=None, description="自訂 block_correction_prompt 提示詞 (若為空且 remove_watermarks=True，則自動採用預設英文浮水印去除 Prompt)"),
    reasoning_effort: Optional[str] = Form(default=None, description="LLM 思考等級 ('low', 'medium', 'high', 'none')，若未指定則使用環境變數設定"),
    enable_thinking: Optional[bool] = Form(default=None, description="是否啟用 LLM 思維鏈/思考過程 (預設: False，避免過長思考導致延遲)"),
):
    job = job_manager.create_job(file.filename)
    upload_path = os.path.join(settings.UPLOAD_DIRECTORY, f"{job.job_id}_{file.filename}")
    job.file_path = upload_path
    try:
        content = await file.read()
        with open(upload_path, "wb+") as f:
            f.write(content)

        params = CommonParams(
            filepath=upload_path,
            use_llm=use_llm,
            mode=mode,
            output_format=output_format,
            page_range=page_range,
            force_ocr=force_ocr,
            paginate_output=paginate_output,
            strip_existing_ocr=strip_existing_ocr,
            disable_image_extraction=disable_image_extraction,
            remove_watermarks=remove_watermarks,
            block_correction_prompt=block_correction_prompt,
            reasoning_effort=reasoning_effort,
            enable_thinking=enable_thinking,
        )

        # 排隊檢測
        active_count = job_manager.get_active_count()
        if active_count >= settings.MAX_CONCURRENT_CONVERSIONS:
            pos = job_manager.get_queue_position(job.job_id)
            job_manager.update_job(
                job.job_id,
                status="queued",
                stage="queued",
                progress=5,
                message=f"目前 GPU 運算資源忙碌中，任務正在排隊中 (排隊順位: 第 {pos} 位)...",
            )

        async with conversion_semaphore:
            job_manager.update_job(
                job.job_id,
                status="processing",
                stage="preparing",
                progress=10,
                message="已取得 GPU 運算資源，正在啟動轉換管線...",
            )
            result = await run_in_threadpool(_execute_conversion, params, job.job_id)
            return result
    finally:
        if os.path.exists(upload_path):
            try:
                os.remove(upload_path)
            except Exception:
                pass
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        job_manager.notify_queue_update()


@app.post("/marker/upload/async")
async def convert_uploaded_file_async(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="待轉換的文件檔案 (PDF/圖片/DOCX/PPTX 等)"),
    use_llm: bool = Form(default=True, description="是否啟用遠端 LLM (Qwen3.8-27B) 校正 (預設: True)"),
    mode: str = Form(default="balanced", description="轉換模式: balanced (GPU 最佳，預設) 或 fast (CPU)"),
    output_format: str = Form(default="markdown", description="輸出格式: markdown (預設), json, html, chunks"),
    page_range: Optional[str] = Form(default=None, description="分頁範圍，預設為空 (處理整份文件)；如需指定可填寫例如: 0,2-5"),
    force_ocr: bool = Form(default=False, description="是否強制對所有頁面進行 OCR"),
    paginate_output: bool = Form(default=True, description="輸出內容是否分頁標註 (預設: True)"),
    strip_existing_ocr: bool = Form(default=False, description="是否移除劣質 OCR 並由本機重新辨識"),
    disable_image_extraction: bool = Form(default=False, description="是否停用圖片抽取"),
    remove_watermarks: bool = Form(default=False, description="是否啟用 VLM/LLM 智慧浮水印與背景雜訊去除功能"),
    block_correction_prompt: Optional[str] = Form(default=None, description="自訂 block_correction_prompt 提示詞 (若為空且 remove_watermarks=True，則自動採用預設英文浮水印去除 Prompt)"),
    reasoning_effort: Optional[str] = Form(default=None, description="LLM 思考等級 ('low', 'medium', 'high', 'none')，若未指定則使用環境變數設定"),
    enable_thinking: Optional[bool] = Form(default=None, description="是否啟用 LLM 思維鏈/思考過程 (預設: False，避免過長思考導致延遲)"),
):
    """
    非同步上傳端點：立即回傳 job_id，具備並發限制與自動佇列排程保護。
    當多個請求同時到達時，自動依序排隊，防止 GPU 顯存 OOM。
    """
    job = job_manager.create_job(file.filename)
    upload_path = os.path.join(settings.UPLOAD_DIRECTORY, f"{job.job_id}_{file.filename}")
    job.file_path = upload_path
    content = await file.read()
    with open(upload_path, "wb+") as f:
        f.write(content)

    params = CommonParams(
        filepath=upload_path,
        use_llm=use_llm,
        mode=mode,
        output_format=output_format,
        page_range=page_range,
        force_ocr=force_ocr,
        paginate_output=paginate_output,
        strip_existing_ocr=strip_existing_ocr,
        disable_image_extraction=disable_image_extraction,
        remove_watermarks=remove_watermarks,
        block_correction_prompt=block_correction_prompt,
        reasoning_effort=reasoning_effort,
        enable_thinking=enable_thinking,
    )

    async def _scheduled_worker():
        active_count = job_manager.get_active_count()
        if active_count >= settings.MAX_CONCURRENT_CONVERSIONS:
            pos = job_manager.get_queue_position(job.job_id)
            job_manager.update_job(
                job.job_id,
                status="queued",
                stage="queued",
                progress=5,
                message=f"目前 GPU 運算資源忙碌中，任務正在排隊中 (排隊順位: 第 {pos} 位，同時最大併發數: {settings.MAX_CONCURRENT_CONVERSIONS})...",
            )

        async with conversion_semaphore:
            try:
                job_manager.update_job(
                    job.job_id,
                    status="processing",
                    stage="preparing",
                    progress=10,
                    message="已取得 GPU 運算資源，正在啟動轉換管線...",
                )
                await run_in_threadpool(_execute_conversion, params, job_id=job.job_id)
            finally:
                if os.path.exists(upload_path):
                    try:
                        os.remove(upload_path)
                    except Exception:
                        pass
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                job_manager.notify_queue_update()

    background_tasks.add_task(_scheduled_worker)

    return {
        "job_id": job.job_id,
        "status": job.status,
        "queue_position": job.queue_position,
        "filename": file.filename,
        "message": job.message,
        "poll_url": f"/marker/jobs/{job.job_id}",
        "stream_url": f"/marker/jobs/{job.job_id}/stream",
        "download_url": f"/marker/jobs/{job.job_id}/download",
    }


@app.get("/marker/jobs/{job_id}")
async def get_job_status(job_id: str):
    """查詢特定轉換任務的即時進度、階段狀態與輸出結果"""
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"找不到指定的任務 ID: {job_id}")
    return job.dict()


@app.get("/marker/jobs/{job_id}/stream")
async def stream_job_progress(job_id: str):
    """
    Server-Sent Events (SSE) 實時串流端點。
    瀏覽器或用戶端連線後，即時接收進度百分比、當前處理階段、即時日誌直到轉換結束。
    """
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"找不到指定的任務 ID: {job_id}")

    queue = job_manager.register_listener(job_id)

    async def event_generator():
        try:
            # 立即發送初始狀態
            cur_job = job_manager.get_job(job_id)
            if cur_job:
                yield f"data: {json.dumps(cur_job.dict(), ensure_ascii=False)}\n\n"
                if cur_job.status in ["completed", "failed"]:
                    return

            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
                    if data.get("status") in ["completed", "failed"]:
                        break
                except asyncio.TimeoutError:
                    # 心跳檢測，保持連線
                    yield ": keep-alive\n\n"
                    cur = job_manager.get_job(job_id)
                    if cur and cur.status in ["completed", "failed"]:
                        break
        finally:
            job_manager.unregister_listener(job_id, queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/marker/jobs")
async def list_recent_jobs():
    """列出最近處理的任務清單"""
    return [j.dict() for j in reversed(list(job_manager.jobs.values()))]


@app.get("/marker/jobs/{job_id}/download")
@app.get("/marker/jobs/{job_id}/zip")
async def download_job_archive(job_id: str):
    """
    打包下載轉換結果為 ZIP 壓縮檔案。
    內容包含：
    - {檔名}.md (Markdown 完整內文)
    - 所有抽取之圖表與影像檔案 (PNG/JPEG)
    - metadata.json (結構化元數據)
    """
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"找不到指定的任務 ID: {job_id}")
    if job.status != "completed" or not job.result:
        raise HTTPException(status_code=400, detail=f"任務尚未完成或轉換失敗 (目前狀態: {job.status})")

    base_name = os.path.splitext(job.filename)[0] or "document"
    output_text = job.result.get("output", "")
    images = job.result.get("images", {})
    metadata = job.result.get("metadata", {})

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        # 1. 寫入 Markdown 檔案
        md_filename = f"{base_name}.md"
        zip_file.writestr(md_filename, output_text.encode("utf-8"))

        # 2. 寫入所有抽取之圖表與影像
        for img_name, b64_str in images.items():
            try:
                img_data = base64.b64decode(b64_str)
                zip_file.writestr(img_name, img_data)
            except Exception as e:
                print(f"[Warning] Failed to decode image {img_name}: {e}")

        # 3. 寫入元數據 metadata.json
        if metadata:
            meta_json = json.dumps(metadata, ensure_ascii=False, indent=2)
            zip_file.writestr("metadata.json", meta_json.encode("utf-8"))

    zip_buffer.seek(0)
    zip_filename = f"{base_name}_converted.zip"
    
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{zip_filename}"',
            "X-Extracted-Images-Count": str(len(images)),
        },
    )


@app.get("/marker/jobs/{job_id}/images/{image_name}")
async def get_job_image(job_id: str, image_name: str):
    """取得特定任務中擷取的單張圖片 (原生二進位輸出)"""
    job = job_manager.get_job(job_id)
    if not job or not job.result:
        raise HTTPException(status_code=404, detail=f"找不到指定的任務 ID: {job_id}")
    images = job.result.get("images", {})
    if image_name not in images:
        raise HTTPException(status_code=404, detail=f"任務中無此圖片: {image_name}")

    img_b64 = images[image_name]
    img_bytes = base64.b64decode(img_b64)
    ext = os.path.splitext(image_name)[1].lower()
    media_type = "image/png" if ext == ".png" else "image/jpeg"
    return Response(content=img_bytes, media_type=media_type)


# ==============================================================================
# 即時進度儀表板 (Web UI Dashboard)
# ==============================================================================
@app.get("/", response_class=HTMLResponse)
@app.get("/ui", response_class=HTMLResponse)
async def web_ui():
    return """<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Marker 文件轉換微服務 — 即時進度儀表板</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/marked/12.0.2/marked.min.js"></script>
    <style>
        :root {
            --primary: #2563eb;
            --primary-hover: #1d4ed8;
            --bg: #f8fafc;
            --card-bg: #ffffff;
            --text-main: #0f172a;
            --text-sub: #475569;
            --border: #e2e8f0;
            --success: #10b981;
            --warning: #f59e0b;
            --error: #ef4444;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "PingFang TC", "Microsoft JhengHei", sans-serif; background-color: var(--bg); color: var(--text-main); line-height: 1.5; padding: 24px 16px; }
        .container { max-width: 980px; margin: 0 auto; }
        header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid var(--border); }
        .logo-group h1 { font-size: 22px; font-weight: 700; color: #1e293b; display: flex; align-items: center; gap: 8px; }
        .badge { background: #dbeafe; color: var(--primary); font-size: 12px; padding: 2px 8px; border-radius: 9999px; font-weight: 600; }
        .nav-links a { color: var(--primary); text-decoration: none; font-size: 14px; font-weight: 500; margin-left: 16px; }
        .nav-links a:hover { text-decoration: underline; }
        
        .card { background: var(--card-bg); border-radius: 12px; border: 1px solid var(--border); box-shadow: 0 2px 4px rgba(0,0,0,0.02); padding: 24px; margin-bottom: 24px; }
        .card-title { font-size: 16px; font-weight: 600; margin-bottom: 16px; display: flex; align-items: center; gap: 8px; }
        
        /* 雙 GPU 與遠端狀態膠囊 */
        .status-bar { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-bottom: 24px; }
        .status-pill { background: white; border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px; display: flex; align-items: center; gap: 12px; }
        .status-icon { width: 36px; height: 36px; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 16px; }
        .icon-green { background: #dcfce7; color: var(--success); }
        .icon-blue { background: #dbeafe; color: var(--primary); }
        .status-text h4 { font-size: 13px; color: var(--text-sub); }
        .status-text p { font-size: 14px; font-weight: 600; }

        /* 上傳區塊 */
        .dropzone { border: 2px dashed var(--border); border-radius: 10px; padding: 36px 20px; text-align: center; cursor: pointer; transition: all 0.2s; background: #fafafa; }
        .dropzone:hover, .dropzone.dragover { border-color: var(--primary); background: #eff6ff; }
        .dropzone i { font-size: 40px; color: #94a3b8; margin-bottom: 12px; }
        .file-selected { margin-top: 12px; font-weight: 600; color: var(--primary); font-size: 14px; }
        
        /* 參數設定列 */
        .settings-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin: 20px 0; }
        .form-group label { display: block; font-size: 13px; font-weight: 600; color: var(--text-sub); margin-bottom: 6px; }
        .form-control { width: 100%; padding: 8px 12px; border: 1px solid var(--border); border-radius: 6px; font-size: 14px; background: white; }
        .checkbox-label { display: flex; align-items: center; gap: 8px; cursor: pointer; font-size: 14px; font-weight: 500; margin-top: 26px; }
        .checkbox-label input { width: 16px; height: 16px; accent-color: var(--primary); }

        .btn-submit { width: 100%; background: var(--primary); color: white; border: none; padding: 12px 20px; border-radius: 8px; font-size: 15px; font-weight: 600; cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 8px; transition: background 0.2s; }
        .btn-submit:hover { background: var(--primary-hover); }
        .btn-submit:disabled { opacity: 0.6; cursor: not-allowed; }

        /* 即時進度狀態面板 */
        .progress-section { display: none; margin-top: 24px; padding-top: 20px; border-top: 1px solid var(--border); }
        .progress-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
        .stage-badge { background: #fef3c7; color: #b45309; padding: 4px 10px; border-radius: 20px; font-size: 13px; font-weight: 600; display: inline-flex; align-items: center; gap: 6px; }
        .stage-badge.queued { background: #e0e7ff; color: #4338ca; }
        .stage-badge.completed { background: #dcfce7; color: #15803d; }
        .stage-badge.failed { background: #fee2e2; color: #b91c1c; }
        .progress-bar-bg { width: 100%; height: 12px; background: #e2e8f0; border-radius: 6px; overflow: hidden; margin-bottom: 12px; }
        .progress-bar-fill { height: 100%; background: linear-gradient(90deg, #3b82f6, #10b981); width: 0%; transition: width 0.3s ease; }
        .elapsed-timer { font-size: 13px; color: var(--text-sub); }

        /* 即時終端機日誌 */
        .terminal-box { background: #0f172a; color: #f8fafc; border-radius: 8px; padding: 14px 16px; font-family: monospace; font-size: 13px; max-height: 180px; overflow-y: auto; margin-top: 14px; }
        .terminal-line { margin-bottom: 4px; display: flex; gap: 8px; }
        .term-time { color: #64748b; }
        .term-msg { color: #38bdf8; }

        /* 錯誤警告框 */
        .error-alert { display: none; background: #fef2f2; border: 1px solid #fecaca; border-radius: 8px; padding: 16px; margin-top: 16px; color: #991b1b; }
        .error-alert h4 { font-size: 14px; font-weight: 700; margin-bottom: 6px; display: flex; align-items: center; gap: 6px; }
        .error-alert pre { background: #fee2e2; padding: 10px; border-radius: 6px; font-size: 12px; overflow-x: auto; margin-top: 8px; }

        /* 成果展示區 */
        .result-section { display: none; margin-top: 24px; }
        .result-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; flex-wrap: wrap; gap: 10px; }
        .tab-buttons { display: flex; gap: 8px; margin-bottom: 14px; border-bottom: 1px solid var(--border); padding-bottom: 10px; }
        .tab-btn { background: #f1f5f9; border: 1px solid var(--border); padding: 8px 16px; border-radius: 6px; font-size: 13px; font-weight: 600; cursor: pointer; color: var(--text-sub); display: flex; align-items: center; gap: 6px; transition: all 0.2s; }
        .tab-btn:hover { background: #e2e8f0; color: var(--text-main); }
        .tab-btn.active { background: var(--primary); color: white; border-color: var(--primary); }
        .badge-count { background: #e0e7ff; color: #3730a3; font-size: 11px; padding: 2px 7px; border-radius: 10px; font-weight: 700; margin-left: 4px; }
        .tab-btn.active .badge-count { background: white; color: var(--primary); }
        
        .result-actions { display: flex; gap: 8px; flex-wrap: wrap; }
        .btn-action { background: white; border: 1px solid var(--border); padding: 8px 14px; border-radius: 6px; font-size: 13px; font-weight: 600; cursor: pointer; display: flex; align-items: center; gap: 6px; transition: all 0.15s; color: #1e293b; text-decoration: none; }
        .btn-action:hover { background: #f1f5f9; border-color: #cbd5e1; }
        .btn-zip { background: #16a34a; color: white; border: none; }
        .btn-zip:hover { background: #15803d; color: white; }

        .result-content { background: white; border: 1px solid var(--border); border-radius: 8px; padding: 18px; max-height: 520px; overflow-y: auto; font-size: 14px; }
        .result-content pre { white-space: pre-wrap; word-break: break-word; font-family: monospace; font-size: 13.5px; }

        /* 渲染 Markdown 預覽樣式 */
        .markdown-preview-body { padding: 10px 14px; line-height: 1.75; font-size: 14.5px; color: #1e293b; }
        .markdown-preview-body h1, .markdown-preview-body h2, .markdown-preview-body h3, .markdown-preview-body h4 { margin-top: 18px; margin-bottom: 10px; color: #0f172a; font-weight: 700; }
        .markdown-preview-body h1 { font-size: 22px; border-bottom: 1px solid var(--border); padding-bottom: 6px; }
        .markdown-preview-body h2 { font-size: 18px; border-bottom: 1px solid var(--border); padding-bottom: 4px; }
        .markdown-preview-body p { margin-bottom: 12px; }
        .markdown-preview-body ul, .markdown-preview-body ol { margin-left: 24px; margin-bottom: 12px; }
        .markdown-preview-body blockquote { border-left: 4px solid var(--primary); padding: 8px 14px; margin: 12px 0; color: #475569; background: #f8fafc; border-radius: 0 6px 6px 0; }
        .markdown-preview-body code { background: #f1f5f9; padding: 2px 6px; border-radius: 4px; font-size: 13px; font-family: monospace; color: #d97706; }
        .markdown-preview-body pre code { display: block; padding: 12px; background: #0f172a; color: #f8fafc; border-radius: 8px; overflow-x: auto; }
        .markdown-preview-body table { width: 100%; border-collapse: collapse; margin: 16px 0; font-size: 13.5px; }
        .markdown-preview-body th, .markdown-preview-body td { border: 1px solid #cbd5e1; padding: 8px 12px; text-align: left; }
        .markdown-preview-body th { background: #f1f5f9; font-weight: 600; }
        .markdown-preview-body tr:nth-child(even) { background: #f8fafc; }
        .markdown-preview-body img { max-width: 100%; height: auto; border-radius: 6px; margin: 12px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.08); border: 1px solid var(--border); display: block; }

        /* 圖片畫廊網格與卡片 */
        .images-gallery-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 16px; padding: 4px; }
        .image-card { background: white; border: 1px solid var(--border); border-radius: 8px; overflow: hidden; display: flex; flex-direction: column; transition: transform 0.15s, box-shadow 0.15s; }
        .image-card:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.08); }
        .image-thumb-wrapper { height: 140px; background: #f8fafc; display: flex; align-items: center; justify-content: center; overflow: hidden; padding: 8px; cursor: pointer; border-bottom: 1px solid var(--border); }
        .image-thumb-wrapper img { max-width: 100%; max-height: 100%; object-fit: contain; }
        .image-info { padding: 10px 12px; display: flex; flex-direction: column; gap: 8px; }
        .image-name { font-size: 12px; font-weight: 600; color: #1e293b; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .btn-download-img { background: #f1f5f9; border: 1px solid var(--border); border-radius: 6px; padding: 5px 8px; font-size: 12px; cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 6px; color: #334155; text-decoration: none; font-weight: 500; }
        .btn-download-img:hover { background: #e2e8f0; color: var(--primary); }
        .empty-gallery { text-align: center; padding: 48px 20px; color: #94a3b8; }
        .empty-gallery i { font-size: 48px; margin-bottom: 12px; color: #cbd5e1; }
        .empty-gallery p { font-size: 14px; }

        /* 圖片放大預覽燈箱 */
        .lightbox-modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(15,23,42,0.85); z-index: 9999; align-items: center; justify-content: center; padding: 20px; backdrop-filter: blur(4px); }
        .lightbox-content { max-width: 90vw; max-height: 90vh; background: white; border-radius: 10px; padding: 16px; position: relative; display: flex; flex-direction: column; box-shadow: 0 20px 25px -5px rgba(0,0,0,0.3); }
        .lightbox-content img { max-width: 100%; max-height: calc(90vh - 90px); object-fit: contain; border-radius: 6px; }
        .lightbox-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
        .lightbox-title { font-size: 14px; font-weight: 600; color: #1e293b; }
        .lightbox-close { font-size: 20px; cursor: pointer; color: #64748b; background: none; border: none; }
        .lightbox-close:hover { color: #ef4444; }
    </style>
</head>
<body>
<div class="container">
    <header>
        <div class="logo-group">
            <h1><i class="fa-solid fa-file-pdf" style="color:#ef4444;"></i> Marker 微服務 <span class="badge">v2.0.0</span></h1>
        </div>
        <div class="nav-links">
            <a href="/docs" target="_blank"><i class="fa-solid fa-code"></i> Swagger API (/docs)</a>
            <a href="/redoc" target="_blank"><i class="fa-solid fa-book"></i> ReDoc 規格</a>
            <a href="/health" target="_blank"><i class="fa-solid fa-heart-pulse"></i> 健康狀態 (/health)</a>
        </div>
    </header>

    <!-- 狀態指示列 -->
    <div class="status-bar">
        <div class="status-pill">
            <div class="status-icon icon-green"><i class="fa-solid fa-microchip"></i></div>
            <div class="status-text">
                <h4>GPU 0 (RTX 3050)</h4>
                <p id="gpu0-stat">Surya OCR (CUDA)</p>
            </div>
        </div>
        <div class="status-pill">
            <div class="status-icon icon-green"><i class="fa-solid fa-server"></i></div>
            <div class="status-text">
                <h4>GPU 1 (T1000)</h4>
                <p id="gpu1-stat">本機版面分析輔助</p>
            </div>
        </div>
        <div class="status-pill">
            <div class="status-icon icon-blue"><i class="fa-solid fa-brain"></i></div>
            <div class="status-text">
                <h4>遠端 vLLM (Qwen3.8-27B)</h4>
                <p id="vllm-stat">192.168.1.5:8000</p>
            </div>
        </div>
    </div>

    <!-- 轉換上傳卡片 -->
    <div class="card">
        <h3 class="card-title"><i class="fa-solid fa-cloud-arrow-up"></i> 文件轉換與即時進度追蹤</h3>
        
        <form id="convert-form">
            <div class="dropzone" id="dropzone">
                <i class="fa-solid fa-cloud-arrow-up"></i>
                <p style="font-weight: 600;">點擊此處選擇檔案，或直接拖曳檔案至此</p>
                <p style="font-size: 12px; color: var(--text-sub); margin-top: 4px;">支援 PDF、DOCX、PPTX、XLSX、PNG、JPG、EPUB 等檔案</p>
                <input type="file" id="file-input" style="display: none;" accept=".pdf,.docx,.pptx,.xlsx,.png,.jpg,.jpeg,.epub">
                <div class="file-selected" id="file-selected"></div>
            </div>

            <!-- 參數列 (完全符合預設值要求) -->
            <div class="settings-grid">
                <div class="form-group">
                    <label for="mode-select"><i class="fa-solid fa-gauge-high"></i> 轉換模式 (mode)</label>
                    <select id="mode-select" class="form-control">
                        <option value="balanced" selected>balanced (GPU 最佳，預設)</option>
                        <option value="fast">fast (CPU 速度優先)</option>
                    </select>
                </div>
                <div class="form-group">
                    <label for="format-select"><i class="fa-solid fa-file-export"></i> 輸出格式 (format)</label>
                    <select id="format-select" class="form-control">
                        <option value="markdown" selected>markdown (標準 Markdown)</option>
                        <option value="json">json (完整結構化物件)</option>
                        <option value="html">html (HTML 網頁)</option>
                        <option value="chunks">chunks (區塊切割分片)</option>
                    </select>
                </div>
                <div class="form-group">
                    <label for="pagerange-input"><i class="fa-solid fa-book-open"></i> 頁數範圍 (page_range)</label>
                    <input type="text" id="pagerange-input" class="form-control" placeholder="留空處理整份文件 (預設)，或輸入 0,2-5">
                </div>
                <div class="form-group">
                    <label class="checkbox-label">
                        <input type="checkbox" id="llm-check" checked>
                        <span><i class="fa-solid fa-wand-magic-sparkles" style="color:var(--primary);"></i> 啟用 Qwen3.8-27B 語意校正 (use_llm)</span>
                    </label>
                </div>
                <div class="form-group">
                    <label class="checkbox-label">
                        <input type="checkbox" id="paginate-check" checked>
                        <span><i class="fa-solid fa-bars-staggered"></i> 輸出分頁標註 (paginate_output)</span>
                    </label>
                </div>
                <div class="form-group">
                    <label class="checkbox-label">
                        <input type="checkbox" id="watermark-check">
                        <span><i class="fa-solid fa-eraser" style="color:#d97706;"></i> 浮水印與背景雜訊去除 (remove_watermarks)</span>
                    </label>
                </div>
                <div class="form-group">
                    <label for="reasoning-select"><i class="fa-solid fa-brain" style="color:#6366f1;"></i> LLM 思考等級 (reasoning_effort)</label>
                    <select id="reasoning-select" class="form-control">
                        <option value="low" selected>low (建議，輕度思考/極速處理)</option>
                        <option value="none">none (完全關閉思考，適用純文字格式清理)</option>
                        <option value="medium">medium (中度思考，適用複雜排版)</option>
                        <option value="high">high (深度思考，最慢)</option>
                    </select>
                </div>
                <div class="form-group" style="grid-column: 1 / -1; margin-top: 4px;">
                    <label for="prompt-input"><i class="fa-solid fa-terminal"></i> 區塊校正提示詞 (block_correction_prompt, 選填)</label>
                    <input type="text" id="prompt-input" class="form-control" placeholder="留空時若勾選去除浮水印將自動帶入最佳化英文 Prompt，亦可填寫自訂英文校正指令">
                </div>
            </div>

            <button type="submit" class="btn-submit" id="btn-submit">
                <i class="fa-solid fa-play"></i> 開始轉換並實時串流進度
            </button>
        </form>

        <!-- 即時進度顯示區 -->
        <div class="progress-section" id="progress-section">
            <div class="progress-header">
                <div>
                    <span class="stage-badge" id="stage-badge"><i class="fa-solid fa-spinner fa-spin"></i> 準備中...</span>
                    <span id="stage-msg" style="font-size: 14px; font-weight: 500; margin-left: 8px;">正在建立任務...</span>
                </div>
                <div class="elapsed-timer" id="elapsed-timer">已耗時: 0.0s</div>
            </div>
            
            <div class="progress-bar-bg">
                <div class="progress-bar-fill" id="progress-fill"></div>
            </div>

            <!-- 即時終端機日誌 -->
            <div class="terminal-box" id="terminal-box"></div>
        </div>

        <!-- 錯誤警告視窗 -->
        <div class="error-alert" id="error-alert">
            <h4><i class="fa-solid fa-triangle-exclamation"></i> 轉換發生錯誤</h4>
            <div id="error-desc"></div>
            <pre id="error-trace"></pre>
        </div>

        <!-- 轉換結果展示區 -->
        <div class="result-section" id="result-section">
            <div class="result-header">
                <h3 class="card-title" style="margin-bottom: 0;"><i class="fa-solid fa-square-poll-vertical"></i> 轉換成果</h3>
                <div class="result-actions">
                    <button class="btn-action btn-zip" id="btn-download-zip" title="打包下載 Markdown、所有抽取圖片與元數據">
                        <i class="fa-solid fa-file-zipper"></i> 下載完整 ZIP (Markdown + 圖片)
                    </button>
                    <button class="btn-action" id="btn-download-md" title="僅下載 Markdown 文件">
                        <i class="fa-solid fa-file-lines"></i> 下載 .md 檔案
                    </button>
                    <button class="btn-action" id="btn-copy" title="複製 Markdown 內容至剪貼簿">
                        <i class="fa-regular fa-copy"></i> 複製 Markdown
                    </button>
                </div>
            </div>

            <!-- 標籤頁切換 -->
            <div class="tab-buttons">
                <button class="tab-btn active" id="tab-btn-md"><i class="fa-regular fa-file-code"></i> 原始 Markdown</button>
                <button class="tab-btn" id="tab-btn-preview"><i class="fa-regular fa-eye"></i> 圖文渲染預覽</button>
                <button class="tab-btn" id="tab-btn-images"><i class="fa-regular fa-images"></i> 擷取圖片清單 <span class="badge-count" id="img-count-badge">0</span></button>
            </div>

            <!-- Tab 1: 原始 Markdown -->
            <div class="result-content" id="panel-md">
                <pre id="result-text"></pre>
            </div>

            <!-- Tab 2: 圖文渲染預覽 -->
            <div class="result-content" id="panel-preview" style="display: none;">
                <div class="markdown-preview-body" id="result-rendered"></div>
            </div>

            <!-- Tab 3: 擷取圖片清單 -->
            <div class="result-content" id="panel-images" style="display: none;">
                <div class="images-gallery-grid" id="images-gallery"></div>
            </div>
        </div>
    </div>
</div>

<!-- 圖片放大燈箱視窗 -->
<div class="lightbox-modal" id="lightbox-modal">
    <div class="lightbox-content">
        <div class="lightbox-header">
            <div class="lightbox-title" id="lightbox-title">圖片檢視</div>
            <button class="lightbox-close" id="lightbox-close">&times;</button>
        </div>
        <img id="lightbox-img" src="" alt="放大預覽">
    </div>
</div>

<script>
    const dropzone = document.getElementById('dropzone');
    const fileInput = document.getElementById('file-input');
    const fileSelected = document.getElementById('file-selected');
    const convertForm = document.getElementById('convert-form');
    const btnSubmit = document.getElementById('btn-submit');
    const progressSection = document.getElementById('progress-section');
    const progressFill = document.getElementById('progress-fill');
    const stageBadge = document.getElementById('stage-badge');
    const stageMsg = document.getElementById('stage-msg');
    const elapsedTimer = document.getElementById('elapsed-timer');
    const terminalBox = document.getElementById('terminal-box');
    const errorAlert = document.getElementById('error-alert');
    const errorDesc = document.getElementById('error-desc');
    const errorTrace = document.getElementById('error-trace');
    const resultSection = document.getElementById('result-section');
    const resultText = document.getElementById('result-text');
    const btnCopy = document.getElementById('btn-copy');
    const btnDownloadMd = document.getElementById('btn-download-md');
    const btnDownloadZip = document.getElementById('btn-download-zip');

    let selectedFile = null;
    let eventSource = null;
    let timerInterval = null;
    let startTime = 0;
    let currentJobId = null;
    let currentResult = null;
    let activeTab = 'md';

    // 拖曳選擇檔案事件
    dropzone.addEventListener('click', () => fileInput.click());
    dropzone.addEventListener('dragover', (e) => { e.preventDefault(); dropzone.classList.add('dragover'); });
    dropzone.addEventListener('dragleave', () => dropzone.classList.remove('dragover'));
    dropzone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropzone.classList.remove('dragover');
        if (e.dataTransfer.files.length) {
            handleFile(e.dataTransfer.files[0]);
        }
    });
    fileInput.addEventListener('change', () => {
        if (fileInput.files.length) {
            handleFile(fileInput.files[0]);
        }
    });

    function handleFile(file) {
        selectedFile = file;
        fileSelected.innerText = `已選擇: ${file.name} (${(file.size / 1024 / 1024).toFixed(2)} MB)`;
    }

    function addLog(timeStr, msg) {
        const line = document.createElement('div');
        line.className = 'terminal-line';
        line.innerHTML = `<span class="term-time">[${timeStr}]</span> <span class="term-msg">${msg}</span>`;
        terminalBox.appendChild(line);
        terminalBox.scrollTop = terminalBox.scrollHeight;
    }

    // 標籤頁面切換控制
    function switchTab(tab) {
        activeTab = tab;
        document.getElementById('tab-btn-md').classList.toggle('active', tab === 'md');
        document.getElementById('tab-btn-preview').classList.toggle('active', tab === 'preview');
        document.getElementById('tab-btn-images').classList.toggle('active', tab === 'images');

        document.getElementById('panel-md').style.display = tab === 'md' ? 'block' : 'none';
        document.getElementById('panel-preview').style.display = tab === 'preview' ? 'block' : 'none';
        document.getElementById('panel-images').style.display = tab === 'images' ? 'block' : 'none';

        if (tab === 'preview') renderPreview();
        if (tab === 'images') renderImagesGallery();
    }

    document.getElementById('tab-btn-md').addEventListener('click', () => switchTab('md'));
    document.getElementById('tab-btn-preview').addEventListener('click', () => switchTab('preview'));
    document.getElementById('tab-btn-images').addEventListener('click', () => switchTab('images'));

    function renderPreview() {
        if (!currentResult || !currentResult.output) return;
        let md = currentResult.output;
        let html = '';
        if (typeof marked !== 'undefined' && marked.parse) {
            html = marked.parse(md);
        } else {
            html = `<pre>${md}</pre>`;
        }
        // 將圖片標籤連結替換為實際圖片 (優先使用 base64 或 API 路由)
        if (currentResult.images) {
            Object.keys(currentResult.images).forEach(imgName => {
                const b64 = currentResult.images[imgName];
                const ext = imgName.endsWith('.png') ? 'image/png' : 'image/jpeg';
                const dataUri = `data:${ext};base64,${b64}`;
                html = html.replaceAll(`src="${imgName}"`, `src="${dataUri}"`);
            });
        }
        document.getElementById('result-rendered').innerHTML = html;
    }

    function renderImagesGallery() {
        const gallery = document.getElementById('images-gallery');
        gallery.innerHTML = '';
        if (!currentResult || !currentResult.images || Object.keys(currentResult.images).length === 0) {
            gallery.innerHTML = `
                <div class="empty-gallery" style="grid-column: 1/-1;">
                    <i class="fa-regular fa-image"></i>
                    <p>本文件無獨立擷取的圖表或插圖（純文字或純向量 PDF 屬正常現象）</p>
                </div>`;
            return;
        }

        const images = currentResult.images;
        Object.keys(images).forEach(imgName => {
            const b64 = images[imgName];
            const ext = imgName.endsWith('.png') ? 'image/png' : 'image/jpeg';
            const dataUri = `data:${ext};base64,${b64}`;

            const card = document.createElement('div');
            card.className = 'image-card';
            card.innerHTML = `
                <div class="image-thumb-wrapper" onclick="openLightbox('${imgName}', '${dataUri}')" title="點擊放大檢視">
                    <img src="${dataUri}" alt="${imgName}">
                </div>
                <div class="image-info">
                    <div class="image-name" title="${imgName}">${imgName}</div>
                    <a href="${dataUri}" download="${imgName}" class="btn-download-img">
                        <i class="fa-solid fa-download"></i> 下載圖片
                    </a>
                </div>
            `;
            gallery.appendChild(card);
        });
    }

    // 燈箱控制
    const lightboxModal = document.getElementById('lightbox-modal');
    const lightboxImg = document.getElementById('lightbox-img');
    const lightboxTitle = document.getElementById('lightbox-title');
    const lightboxClose = document.getElementById('lightbox-close');

    function openLightbox(title, src) {
        lightboxTitle.innerText = title;
        lightboxImg.src = src;
        lightboxModal.style.display = 'flex';
    }

    lightboxClose.addEventListener('click', () => lightboxModal.style.display = 'none');
    lightboxModal.addEventListener('click', (e) => {
        if (e.target === lightboxModal) lightboxModal.style.display = 'none';
    });

    // 表單提交與實時串流處理
    convertForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        if (!selectedFile) {
            alert('請先選擇或拖曳欲轉換的文件檔案！');
            return;
        }

        // 初始化介面狀態
        btnSubmit.disabled = true;
        progressSection.style.display = 'block';
        errorAlert.style.display = 'none';
        resultSection.style.display = 'none';
        terminalBox.innerHTML = '';
        progressFill.style.width = '5%';
        stageBadge.className = 'stage-badge';
        stageBadge.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> 準備中...';
        stageMsg.innerText = '正在上傳檔案並向伺服器建立非同步任務...';

        startTime = Date.now();
        clearInterval(timerInterval);
        timerInterval = setInterval(() => {
            const sec = ((Date.now() - startTime) / 1000).toFixed(1);
            elapsedTimer.innerText = `已耗時: ${sec}s`;
        }, 100);

        const formData = new FormData();
        formData.append('file', selectedFile);
        formData.append('use_llm', document.getElementById('llm-check').checked);
        formData.append('mode', document.getElementById('mode-select').value);
        formData.append('output_format', document.getElementById('format-select').value);
        formData.append('paginate_output', document.getElementById('paginate-check').checked);
        formData.append('remove_watermarks', document.getElementById('watermark-check').checked);
        const reasoningEffort = document.getElementById('reasoning-select').value;
        if (reasoningEffort) formData.append('reasoning_effort', reasoningEffort);
        const customPrompt = document.getElementById('prompt-input').value.trim();
        if (customPrompt) formData.append('block_correction_prompt', customPrompt);
        const pr = document.getElementById('pagerange-input').value.trim();
        if (pr) formData.append('page_range', pr);

        try {
            // 呼叫非同步端點建立任務
            const res = await fetch('/marker/upload/async', { method: 'POST', body: formData });
            if (!res.ok) throw new Error(`上傳請求失敗 (HTTP ${res.status})`);
            const jobData = await res.json();
            const jobId = jobData.job_id;
            currentJobId = jobId;
            addLog(new Date().toTimeString().split(' ')[0], `任務已建立: ${jobId}，正在建立實時串流...`);

            // 建立 Server-Sent Events (SSE) 即時監聽進度
            if (eventSource) eventSource.close();
            eventSource = new EventSource(`/marker/jobs/${jobId}/stream`);

            eventSource.onmessage = (event) => {
                const data = JSON.parse(event.data);
                
                // 更新進度條
                progressFill.style.width = `${data.progress}%`;
                stageMsg.innerText = data.message;

                // 更新終端日誌
                if (data.logs && data.logs.length) {
                    terminalBox.innerHTML = '';
                    data.logs.forEach(l => addLog(l.time, l.msg));
                }

                // 判斷狀態
                if (data.status === 'completed') {
                    eventSource.close();
                    clearInterval(timerInterval);
                    btnSubmit.disabled = false;
                    stageBadge.className = 'stage-badge completed';
                    stageBadge.innerHTML = '<i class="fa-solid fa-circle-check"></i> 轉換成功';
                    progressFill.style.width = '100%';

                    // 儲存結果並呈現
                    if (data.result) {
                        currentResult = data.result;
                        resultText.innerText = data.result.output || '';
                        
                        const imgCount = (data.result.images ? Object.keys(data.result.images).length : 0);
                        document.getElementById('img-count-badge').innerText = imgCount;

                        switchTab('md');
                        resultSection.style.display = 'block';
                    }
                } else if (data.status === 'failed') {
                    eventSource.close();
                    clearInterval(timerInterval);
                    btnSubmit.disabled = false;
                    stageBadge.className = 'stage-badge failed';
                    stageBadge.innerHTML = '<i class="fa-solid fa-triangle-exclamation"></i> 轉換失敗';
                    errorDesc.innerText = data.message || '文件轉換過程發生未預期錯誤';
                    errorTrace.innerText = data.error || '無詳細 Traceback';
                    errorAlert.style.display = 'block';
                } else if (data.status === 'queued' || data.stage === 'queued') {
                    stageBadge.className = 'stage-badge queued';
                    const posText = data.queue_position ? ` (第 ${data.queue_position} 位)` : '';
                    stageBadge.innerHTML = `<i class="fa-solid fa-clock"></i> 排隊等待中${posText}`;
                } else {
                    stageBadge.className = 'stage-badge';
                    stageBadge.innerHTML = `<i class="fa-solid fa-gear fa-spin"></i> ${data.progress}% 處理中`;
                }
            };

            eventSource.onerror = () => {
                // 若 SSE 中斷，平滑降級為輪詢
                console.warn('SSE 連線中斷，切換為輪詢模式...');
                eventSource.close();
                pollJob(jobId);
            };

        } catch (err) {
            clearInterval(timerInterval);
            btnSubmit.disabled = false;
            stageBadge.className = 'stage-badge failed';
            stageBadge.innerHTML = '<i class="fa-solid fa-triangle-exclamation"></i> 上傳失敗';
            errorDesc.innerText = err.message;
            errorAlert.style.display = 'block';
        }
    });

    async function pollJob(jobId) {
        const pollTimer = setInterval(async () => {
            try {
                const r = await fetch(`/marker/jobs/${jobId}`);
                if (!r.ok) return;
                const d = await r.json();
                progressFill.style.width = `${d.progress}%`;
                stageMsg.innerText = d.message;
                if (d.status === 'queued' || d.stage === 'queued') {
                    stageBadge.className = 'stage-badge queued';
                    const posText = d.queue_position ? ` (第 ${d.queue_position} 位)` : '';
                    stageBadge.innerHTML = `<i class="fa-solid fa-clock"></i> 排隊等待中${posText}`;
                }
                if (d.status === 'completed' || d.status === 'failed') {
                    clearInterval(pollTimer);
                    clearInterval(timerInterval);
                    btnSubmit.disabled = false;
                    if (d.status === 'completed') {
                        stageBadge.className = 'stage-badge completed';
                        stageBadge.innerHTML = '<i class="fa-solid fa-circle-check"></i> 轉換成功';
                        if (d.result) {
                            currentResult = d.result;
                            resultText.innerText = d.result.output || '';
                            const imgCount = (d.result.images ? Object.keys(d.result.images).length : 0);
                            document.getElementById('img-count-badge').innerText = imgCount;
                            switchTab('md');
                            resultSection.style.display = 'block';
                        }
                    } else {
                        stageBadge.className = 'stage-badge failed';
                        stageBadge.innerHTML = '<i class="fa-solid fa-triangle-exclamation"></i> 轉換失敗';
                        errorDesc.innerText = d.message;
                        errorTrace.innerText = d.error || '';
                        errorAlert.style.display = 'block';
                    }
                }
            } catch (e) {
                console.error('Polling error:', e);
            }
        }, 1500);
    }

    // 複製 Markdown 功能
    btnCopy.addEventListener('click', () => {
        navigator.clipboard.writeText(resultText.innerText).then(() => {
            alert('已成功複製 Markdown 內容至剪貼簿！');
        });
    });

    // 下載單一 .md 檔案功能
    btnDownloadMd.addEventListener('click', () => {
        const blob = new Blob([resultText.innerText], { type: 'text/markdown;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = (selectedFile ? selectedFile.name.replace(/\\.[^/.]+$/, "") : "document") + ".md";
        a.click();
        URL.revokeObjectURL(url);
    });

    // 下載完整 ZIP (Markdown + 圖片) 功能
    btnDownloadZip.addEventListener('click', () => {
        if (!currentJobId) {
            alert('尚未有已完成的任務！');
            return;
        }
        window.location.href = `/marker/jobs/${currentJobId}/download`;
    });

    // 頁面載入時檢查健康度
    fetch('/health').then(r => r.json()).then(d => {
        if (d.gpus && d.gpus.length >= 2) {
            document.getElementById('gpu0-stat').innerText = `${d.gpus[0].name} (OCR)`;
            document.getElementById('gpu1-stat').innerText = `${d.gpus[1].name} (Auxiliary)`;
        }
        if (d.remote_vllm && d.remote_vllm.connected) {
            document.getElementById('vllm-stat').innerText = `已連線: ${d.remote_vllm.models[0] || 'Qwen'}`;
        }
    }).catch(console.error);
</script>
</body>
</html>"""


def start_server():
    import uvicorn
    uvicorn.run(
        "server.app:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=False,
    )


if __name__ == "__main__":
    start_server()
