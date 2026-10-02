"""
================================================================================
模組名稱: server.app
用途說明: Marker 文件轉換微服務主入口、FastAPI 應用程式與 RESTful/SSE 路由層
技術規格:
  - 核心架構:
    - 任務狀態機: 由 server.jobs.job_manager 集中管理
    - 轉換管線: 由 server.converter.execute_conversion 封裝協同中斷與進度追蹤
    - 推理服務: 由 server.services.OptimizedOpenAIService 抑制過度思考 (reasoning_effort)
    - 系統配置: 由 server.config.settings 統一由環境變數注入
    - 使用者介面: 由 server/templates/index.html 提供現代化 Web 儀表板與任務監控
  - 核心端點:
    - GET  /health                      : 雙 GPU、vLLM 與佇列健康檢查
    - GET  / 及 /ui                     : 現代化單頁 Web 儀表板
    - POST /marker                      : 傳統檔案路徑同步轉換 (JSON body)
    - POST /marker/upload               : 檔案上傳同步轉換 (multipart)
    - POST /marker/upload/async         : 檔案上傳非同步排程 (回傳 job_id 與排隊順位)
    - GET  /marker/jobs                 : 查詢最近/活躍任務清單 (支援 Web UI 監控)
    - GET  /marker/jobs/{job_id}        : 查詢指定任務狀態與產出
    - POST /marker/jobs/{job_id}/cancel : 中斷排隊中任務或強制終止幽靈/卡死任務
    - GET  /marker/jobs/{job_id}/stream : Server-Sent Events (SSE) 實時進度串流
    - GET  /marker/jobs/{job_id}/download: 打包下載 Markdown、擷取圖片與 Metadata 之 ZIP 檔
    - GET  /marker/jobs/{job_id}/images/{name}: 下載單張擷取圖片
  - 資源保護:
    - 號誌鎖 (conversion_semaphore): 限制最大併發數，防止 GPU 顯存 OOM
    - 動態白名單清理 (periodic_cleanup_worker): 每日巡檢清理孤立暫存檔，嚴格保護排隊與執行中任務
維護指南:
  - 路由定義保持簡潔，具體業務邏輯與狀態計算委由 jobs 與 converter 模組處理。
================================================================================
"""

import asyncio
from contextlib import asynccontextmanager
import io
import json
import os
import shutil
import time
import traceback
from typing import Annotated, Optional
import zipfile

import requests
import torch
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

from marker.logger import get_logger
from marker.models import create_model_dict, shutdown_models

from server.config import settings
from server.jobs import job_manager
from server.converter import CommonParams, execute_conversion

logger = get_logger()
app_data = {}
conversion_semaphore: Optional[asyncio.Semaphore] = None

TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "templates", "index.html")


# ==============================================================================
# 背景自動清理工作者 (定期維護磁碟與記憶體)
# ==============================================================================
async def periodic_cleanup_worker():
    """
    背景自動垃圾清理循環 (預設每天執行一次，CLEANUP_INTERVAL_SECONDS=86400):
    1. 定期清理 /tmp/marker_uploads 中超過 FILE_RETENTION_SECONDS 的孤立暫存檔案
       【白名單防護機制】: 自動豁免 queued (排隊等待中) 與 processing (執行中) 的任務檔案。
    2. 定期從記憶體中淘汰超過 JOB_RETENTION_SECONDS 的已結束 (completed/failed/cancelled) 任務
    3. 定期調用 torch.cuda.empty_cache() 釋放 PyTorch 顯存碎片
    """
    while True:
        try:
            await asyncio.sleep(settings.CLEANUP_INTERVAL_SECONDS)
            now = time.time()
            logger.info("[Cleanup Worker] 開始執行排程垃圾檔案與記憶體任務巡檢...")

            # 收集白名單檔案
            protected_files = set()
            for job in job_manager.jobs.values():
                if job.status in ("queued", "processing") and job.file_path:
                    protected_files.add(os.path.abspath(job.file_path))

            # 清理孤立暫存檔案
            cleaned_files_count = 0
            if os.path.exists(settings.UPLOAD_DIRECTORY):
                for fname in os.listdir(settings.UPLOAD_DIRECTORY):
                    fpath = os.path.abspath(os.path.join(settings.UPLOAD_DIRECTORY, fname))
                    if fpath in protected_files:
                        continue
                    try:
                        if os.path.isfile(fpath) or os.path.islink(fpath):
                            file_mtime = os.path.getmtime(fpath)
                            if now - file_mtime > settings.FILE_RETENTION_SECONDS:
                                os.remove(fpath)
                                cleaned_files_count += 1
                        elif os.path.isdir(fpath):
                            dir_mtime = os.path.getmtime(fpath)
                            if now - dir_mtime > settings.FILE_RETENTION_SECONDS:
                                shutil.rmtree(fpath, ignore_errors=True)
                                cleaned_files_count += 1
                    except Exception as fe:
                        logger.error(f"[Cleanup Worker] 清理檔案 {fpath} 失敗: {fe}")

            # 清理記憶體中過期任務
            pruned_jobs_count = job_manager.prune_old_jobs(
                retention_seconds=settings.JOB_RETENTION_SECONDS,
                max_history=settings.MAX_COMPLETED_JOBS_HISTORY,
            )

            # 釋放 GPU 顯存
            if torch.cuda.is_available():
                torch.cuda.empty_cache()

            logger.info(
                f"[Cleanup Worker] 巡檢完成: 安全清理孤立檔案 {cleaned_files_count} 個，"
                f"淘汰過期任務 {pruned_jobs_count} 筆，受保護活躍檔案 {len(protected_files)} 個。"
            )
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[Cleanup Worker] 例行巡檢過程發生異常: {e}")


# ==============================================================================
# 生命週期事件 (Lifespan)
# ==============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global conversion_semaphore
    conversion_semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_CONVERSIONS)
    logger.info(
        f"[Marker Service] 服務初始化完成，GPU 轉換號誌鎖最大併發數: {settings.MAX_CONCURRENT_CONVERSIONS}"
    )

    cleanup_task = asyncio.create_task(periodic_cleanup_worker())

    yield

    cleanup_task.cancel()
    if "models" in app_data and app_data["models"] is not None:
        logger.info("[Marker Service] 正在關閉模型快取...")
        try:
            shutdown_models(app_data["models"])
        except Exception as e:
            logger.error(f"[Marker Service] 關閉模型時發生異常: {e}")
        del app_data["models"]


# ==============================================================================
# FastAPI 應用程式實例
# ==============================================================================
app = FastAPI(
    title="Marker Document Conversion Microservice",
    description="具備雙 GPU 加速、遠端 vLLM Qwen3.8-27B 思考等級控制之高品質文件轉換微服務，支援實時進度串流 (SSE) 與幽靈任務中斷。",
    version="2.1.0",
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
# 路由定義 (API Routes)
# ==============================================================================

@app.get("/health")
async def health_check():
    """微服務健康檢查與硬體狀態回報"""
    gpus = []
    cuda_available = torch.cuda.is_available()
    if cuda_available:
        for i in range(torch.cuda.device_count()):
            gpus.append({
                "index": i,
                "name": torch.cuda.get_device_name(i),
                "total_memory_mb": round(torch.cuda.get_device_properties(i).total_memory / (1024 * 1024), 2),
            })

    vllm_connected = False
    vllm_models = []
    try:
        r = requests.get(f"{settings.REMOTE_LLM_URL}/models", timeout=3)
        if r.status_code == 200:
            vllm_connected = True
            vllm_models = [m.get("id") for m in r.json().get("data", [])]
    except Exception:
        pass

    queued_count = sum(1 for j in job_manager.jobs.values() if j.status == "queued")
    active_count = job_manager.get_active_count()

    return {
        "status": "healthy",
        "cuda_available": cuda_available,
        "gpu_count": len(gpus),
        "gpus": gpus,
        "remote_vllm": {
            "configured_url": settings.REMOTE_LLM_URL,
            "connected": vllm_connected,
            "models": vllm_models,
        },
        "models_loaded": "models" in app_data and app_data["models"] is not None,
        "queue_status": {
            "max_concurrent_limit": settings.MAX_CONCURRENT_CONVERSIONS,
            "active_processing_jobs": active_count,
            "queued_waiting_jobs": queued_count,
            "total_jobs_in_memory": len(job_manager.jobs),
        },
        "retention_policy": {
            "job_retention_seconds": settings.JOB_RETENTION_SECONDS,
            "file_retention_seconds": settings.FILE_RETENTION_SECONDS,
            "cleanup_interval_seconds": settings.CLEANUP_INTERVAL_SECONDS,
            "protected_active_tasks_count": active_count + queued_count,
        },
    }


@app.get("/", response_class=HTMLResponse)
@app.get("/ui", response_class=HTMLResponse)
async def web_ui():
    """提供現代化單頁 Web 儀表板與即時任務監控面板"""
    if os.path.exists(TEMPLATE_PATH):
        with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
            return f.read()
    return HTMLResponse("<h1>UI 模板載入中，請確認 server/templates/index.html 檔案存在。</h1>")


@app.post("/marker")
async def convert_file_path(params: CommonParams):
    """同步路徑轉換端點 (直接傳遞本地檔案路徑)"""
    if not os.path.exists(params.filepath):
        raise HTTPException(status_code=400, detail=f"檔案不存在: {params.filepath}")
    
    async with conversion_semaphore:
        models = app_data.get("models")
        return await run_in_threadpool(execute_conversion, params, None, models)


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
    """同步上傳端點：上傳並等待轉換完畢後回傳結果"""
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

        current_task = asyncio.current_task()
        if current_task:
            job_manager.register_async_task(job.job_id, current_task)

        async with conversion_semaphore:
            job_manager.set_active_job(job.job_id)
            try:
                job_manager.update_job(
                    job.job_id,
                    status="processing",
                    stage="preparing",
                    progress=10,
                    message="已取得 GPU 運算資源，正在啟動轉換管線...",
                )
                models = app_data.get("models")
                result = await run_in_threadpool(execute_conversion, params, job.job_id, models)
                return result
            finally:
                job_manager.clear_active_job(job.job_id)
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
            # 檢查在排隊等待期間是否已被使用者手動取消
            if job.is_cancelled:
                logger.info(f"[Worker] 任務 [{job.job_id}] 在排隊期間已取消，略過轉換。")
                job_manager.notify_queue_update()
                return

            job_manager.set_active_job(job.job_id)
            try:
                job_manager.update_job(
                    job.job_id,
                    status="processing",
                    stage="preparing",
                    progress=10,
                    message="已取得 GPU 運算資源，正在啟動轉換管線...",
                )
                models = app_data.get("models")
                await run_in_threadpool(execute_conversion, params, job.job_id, models)
            except asyncio.CancelledError:
                logger.warning(f"[Worker] 任務 [{job.job_id}] 協程接收到 CancelledError")
            except Exception as e:
                logger.error(f"[Worker] 非同步任務例外: {e}")
                job_manager.update_job(
                    job.job_id,
                    status="failed",
                    stage="error",
                    message=f"非同步轉換執行失敗: {e}",
                    error=traceback.format_exc(),
                )
            finally:
                job_manager.clear_active_job(job.job_id)
                if os.path.exists(upload_path):
                    try:
                        os.remove(upload_path)
                    except Exception:
                        pass
                if torch.cuda.is_available():
                    torch.cuda.empty_cache()
                job_manager.notify_queue_update()

    worker_task = asyncio.create_task(_scheduled_worker())
    job_manager.register_async_task(job.job_id, worker_task)

    return {
        "job_id": job.job_id,
        "filename": file.filename,
        "status": job.status,
        "stage": job.stage,
        "progress": job.progress,
        "queue_position": job_manager.get_queue_position(job.job_id),
        "message": "檔案上傳成功，排隊處理中...",
        "poll_url": f"/marker/jobs/{job.job_id}",
        "stream_url": f"/marker/jobs/{job.job_id}/stream",
        "download_url": f"/marker/jobs/{job.job_id}/download",
    }


@app.get("/marker/jobs")
async def list_jobs(limit: int = 50, status: Optional[str] = None):
    """
    列出最近或特定狀態的任務清單 (供前端 Web UI 任務監控面板即時調用)
    支援以 ?status=active 篩選執行中或排隊中任務。
    """
    return job_manager.list_jobs(limit=limit, status_filter=status)


@app.get("/marker/jobs/{job_id}")
async def get_job_status(job_id: str):
    """輪詢查詢特定任務當前進度狀態與轉換產出"""
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"找不到任務 ID: {job_id}")
    return job.dict()


@app.post("/marker/jobs/{job_id}/cancel")
@app.delete("/marker/jobs/{job_id}")
async def cancel_job_endpoint(job_id: str, force: bool = False):
    """
    中斷指定任務 (包含排隊中任務與執行中的幽靈/卡死任務)。
    - 若為排隊中：立即取消並釋放佇列順位。
    - 若為執行中：向轉換管線發出 TaskCancelledException 訊號並終止後續處理器運算，即刻釋放 GPU 運算鎖。
    """
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"找不到任務 ID: {job_id}")

    success = job_manager.cancel_job(job_id, reason="任務已被使用者手動取消", force=force)

    # 檢查號誌鎖 watchdog：若已無非取消之 processing 任務但 semaphore 仍處於鎖定狀態，執行安全重置
    active_processing = [
        j for j in job_manager.jobs.values()
        if j.status == "processing" and not j.is_cancelled
    ]
    if not active_processing and conversion_semaphore is not None and conversion_semaphore.locked():
        logger.warning("[Watchdog] 檢測到已無執行中任務但 conversion_semaphore 仍處於鎖定狀態，執行安全重置")
        try:
            conversion_semaphore.release()
        except ValueError:
            pass

    job_manager.notify_queue_update()

    return {
        "success": success,
        "job_id": job_id,
        "status": "cancelled",
        "message": f"任務 [{job_id}] 已成功中斷並釋放資源。",
    }


@app.get("/marker/jobs/{job_id}/stream")
async def stream_job_progress(job_id: str):
    """Server-Sent Events (SSE) 即時推播任務進度與終端機日誌"""
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"找不到任務 ID: {job_id}")

    async def event_generator():
        q = job_manager.register_listener(job_id)
        try:
            # 立即發送首個當前狀態快照
            initial_event = {
                "job_id": job.job_id,
                "status": job.status,
                "stage": job.stage,
                "progress": job.progress,
                "message": job.message,
                "queue_position": job.queue_position,
                "elapsed_seconds": job.elapsed_seconds,
                "log": job.logs[-1] if job.logs else None,
                "result": job.result if job.status in ("completed", "cancelled", "failed") else None,
                "error": job.error if job.status == "failed" else None,
            }
            yield f"data: {json.dumps(initial_event, ensure_ascii=False)}\n\n"

            if job.status in ("completed", "failed", "cancelled"):
                return

            while True:
                data = await q.get()
                yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
                if data.get("status") in ("completed", "failed", "cancelled"):
                    break
        finally:
            job_manager.unregister_listener(job_id, q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


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
        raise HTTPException(status_code=404, detail=f"找不到任務 ID: {job_id}")
    if job.status != "completed" or not job.result:
        raise HTTPException(status_code=400, detail=f"任務尚未完成，目前狀態為: {job.status}")

    res = job.result
    base_name = os.path.splitext(job.filename)[0] if job.filename else "converted"

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        md_text = res.get("output", "")
        zip_file.writestr(f"{base_name}.md", md_text.encode("utf-8"))

        meta_dict = res.get("metadata", {})
        zip_file.writestr("metadata.json", json.dumps(meta_dict, ensure_ascii=False, indent=2).encode("utf-8"))

        images_dict = res.get("images", {})
        for img_name, b64_data in images_dict.items():
            try:
                import base64
                img_bytes = base64.b64decode(b64_data)
                zip_file.writestr(f"images/{img_name}", img_bytes)
            except Exception as e:
                logger.error(f"打包圖片 {img_name} 失敗: {e}")

    zip_buffer.seek(0)
    zip_filename = f"{base_name}_archive.zip"
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_filename}"'},
    )


@app.get("/marker/jobs/{job_id}/images/{image_name}")
async def download_single_image(job_id: str, image_name: str):
    """直接下載單張擷取之高解析度圖表原始二進位圖檔"""
    job = job_manager.get_job(job_id)
    if not job or not job.result:
        raise HTTPException(status_code=404, detail="找不到任務或任務尚未產出結果")

    images = job.result.get("images", {})
    if image_name not in images:
        raise HTTPException(status_code=404, detail=f"找不到圖片: {image_name}")

    import base64
    img_bytes = base64.b64decode(images[image_name])
    ext = os.path.splitext(image_name)[1].lower()
    media_type = "image/png"
    if ext in [".jpg", ".jpeg"]:
        media_type = "image/jpeg"
    elif ext == ".webp":
        media_type = "image/webp"

    return Response(
        content=img_bytes,
        media_type=media_type,
        headers={"Content-Disposition": f'inline; filename="{image_name}"'},
    )


def start_server():
    """微服務啟動進入點"""
    import uvicorn
    uvicorn.run(
        "server.app:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=False,
    )


if __name__ == "__main__":
    start_server()
