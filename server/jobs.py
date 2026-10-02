"""
================================================================================
模組名稱: server.jobs
用途說明: 任務狀態管理、生命週期維護、FIFO 佇列排程與幽靈任務中斷機制
技術規格:
  - 核心類別: JobInfo (Pydantic 資料結構), JobManager (執行緒安全記憶體狀態機)
  - 支援狀態: queued (排隊中), processing (執行中), completed (完成), failed (失敗), cancelled (已中斷)
  - 實時通訊: 支援註冊 asyncio.Queue 用於 Server-Sent Events (SSE) 實時串流
  - 幽靈任務防護: 具備 cancel_job() 協同中斷與強制取消機制，支援排隊取消與執行中主動中斷
  - 資源清理: 配合設定提供 prune_old_jobs()，保障記憶體不無限膨脹
維護指南:
  - AI Agent 修改時請注意與 server.converter 中 ProgressPdfConverter 的中斷檢測 (is_cancelled) 連動。
================================================================================
"""

import asyncio
import os
import time
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from marker.logger import get_logger

logger = get_logger()


class TaskCancelledException(BaseException):
    """
    任務被手動取消時拋出之特殊中斷例外。
    繼承自 BaseException 而非 Exception，以避免被 Marker 內建各處理器
    內部的 `except Exception:` 區塊靜默攔截吞沒，確保能夠穿透整個 AST/處理器調用棧，
    即刻終止迴圈並釋放資源。
    """
    pass


class JobInfo(BaseModel):
    """
    文件轉換任務實體模型
    記錄任務完整生命週期資訊、執行階段、進度百分比、歷程日誌與轉換產出。
    """
    job_id: str = Field(description="全域唯一任務識別碼 (格式: job_YYYYMMDD_HHMMSS_xxxxxx)")
    filename: str = Field(description="上傳之原始檔案名稱")
    status: str = Field(
        default="queued",
        description="任務當前狀態: queued (排隊中), processing (執行中), completed (完成), failed (失敗), cancelled (已中斷)"
    )
    stage: str = Field(
        default="queued",
        description="細部處理階段: queued, uploaded, preparing, layout_ocr, structure, processing, llm_refinement, rendering, done, cancelled, error"
    )
    progress: int = Field(default=0, ge=0, le=100, description="任務處理進度百分比 (0~100)")
    message: str = Field(default="任務排隊等待處理中...", description="當前階段詳細執行說明或錯誤訊息")
    queue_position: Optional[int] = Field(default=None, description="於 FIFO 佇列中的等待順位 (1 代表下一位)")
    file_path: Optional[str] = Field(default=None, description="伺服器暫存路徑")
    created_at: float = Field(default_factory=time.time, description="任務建立 UNIX 時間戳記")
    updated_at: float = Field(default_factory=time.time, description="最後更新 UNIX 時間戳記")
    elapsed_seconds: float = Field(default=0.0, description="累計執行耗時 (秒)")
    error: Optional[str] = Field(default=None, description="若執行失敗記錄 Python 錯誤摘要與堆疊")
    result: Optional[dict] = Field(default=None, description="轉換成功產出之 Markdown、Metadata 與圖片摘要")
    logs: List[dict] = Field(default_factory=list, description="時間序結構化日誌清單 [{'time': 'HH:MM:SS', 'msg': '...'}]")
    is_cancelled: bool = Field(default=False, description="標記任務是否已被手動中斷/取消")


class JobManager:
    """
    任務生命週期管理員 (Job Manager)
    負責維護記憶體中的所有任務狀態、SSE 串流分發、佇列序位計算與幽靈任務中斷。
    """

    def __init__(self):
        self.jobs: Dict[str, JobInfo] = {}
        self.listeners: Dict[str, List[asyncio.Queue]] = {}
        self.active_job_id: Optional[str] = None
        self.async_tasks: Dict[str, asyncio.Task] = {}
        self.active_clients: Dict[str, List[Any]] = {}

    def set_active_job(self, job_id: str):
        self.active_job_id = job_id

    def clear_active_job(self, job_id: str):
        if self.active_job_id == job_id:
            self.active_job_id = None

    def register_async_task(self, job_id: str, task: asyncio.Task):
        self.async_tasks[job_id] = task

    def register_client(self, job_id: str, client: Any):
        if job_id not in self.active_clients:
            self.active_clients[job_id] = []
        self.active_clients[job_id].append(client)

    def unregister_client(self, job_id: str, client: Any):
        if job_id in self.active_clients and client in self.active_clients[job_id]:
            self.active_clients[job_id].remove(client)

    def create_job(self, filename: str) -> JobInfo:
        """建立並登記新任務，預設進入 queued 狀態"""
        now = time.strftime("%Y%m%d_%H%M%S")
        sub_id = os.urandom(3).hex()
        job_id = f"job_{now}_{sub_id}"

        job = JobInfo(
            job_id=job_id,
            filename=filename,
            status="queued",
            stage="queued",
            progress=5,
            message="檔案已接收，已進入系統排隊佇列...",
        )
        self.jobs[job_id] = job
        self.listeners[job_id] = []
        self._append_log(job, f"檔案「{filename}」已上傳，建立任務 ID: {job_id}")
        return job

    def get_job(self, job_id: str) -> Optional[JobInfo]:
        """根據 job_id 取得任務狀態"""
        return self.jobs.get(job_id)

    def is_cancelled(self, job_id: str) -> bool:
        """快速檢測任務是否已被取消 (供轉換器管線輪詢)"""
        job = self.jobs.get(job_id)
        return bool(job and job.is_cancelled)

    def update_job(
        self,
        job_id: str,
        status: Optional[str] = None,
        stage: Optional[str] = None,
        progress: Optional[int] = None,
        message: Optional[str] = None,
        result: Optional[dict] = None,
        error: Optional[str] = None,
    ):
        """
        更新指定任務的狀態，自動計算耗時並推播給所有監聽此任務之 SSE 佇列
        """
        job = self.jobs.get(job_id)
        if not job:
            return

        # 若任務已標記為取消，不允許再被更新為 processing 或 completed
        if job.is_cancelled and status not in ("cancelled", None):
            logger.info(f"任務 [{job_id}] 已取消，略過後續狀態更新 ({status})")
            return

        now = time.time()
        job.updated_at = now
        job.elapsed_seconds = round(now - job.created_at, 2)

        if status:
            job.status = status
        if stage:
            job.stage = stage
        if progress is not None:
            job.progress = progress
        if message:
            job.message = message
            self._append_log(job, message)
        if result is not None:
            job.result = result
        if error:
            job.error = error
            self._append_log(job, f"錯誤: {error}")

        # 即時推播給 SSE 串流監聽者
        self._notify_listeners(job)

    def cancel_job(self, job_id: str, reason: str = "任務已被使用者手動取消", force: bool = False) -> bool:
        """
        中斷/取消任務 (包含排隊中任務與執行中的幽靈任務)
        
        Args:
            job_id: 任務識別碼
            reason: 中斷原因描述
            force: 是否強制終止
            
        Returns:
            bool: 是否成功執行中斷
        """
        job = self.jobs.get(job_id)
        if not job:
            return False

        if job.status in ("completed", "failed", "cancelled"):
            return True

        was_queued = (job.status == "queued")
        job.is_cancelled = True
        job.status = "cancelled"
        job.stage = "cancelled"
        job.message = reason
        job.updated_at = time.time()
        job.elapsed_seconds = round(job.updated_at - job.created_at, 2)
        job.queue_position = None
        self._append_log(job, f"🛑 {reason}")

        logger.warning(f"[JobManager] 任務 [{job_id}] 已被成功中斷 (原狀態: {'queued' if was_queued else 'processing'})")

        # 推播中斷通知至該任務之 SSE 串流
        self._notify_listeners(job)

        # 1. 強制關閉任務註冊中的所有 OpenAI / HTTP client，瞬間打斷任何等待中的遠端 VLM 請求
        clients = self.active_clients.pop(job_id, [])
        for c in clients:
            try:
                c.close()
                logger.warning(f"[JobManager] 已強制關閉任務 [{job_id}] 的連線 client 以打斷 HTTP 阻塞")
            except Exception as ce:
                logger.debug(f"關閉 client 異常: {ce}")

        # 2. 如果有綁定的 asyncio 協程，立即 cancel()
        task = self.async_tasks.pop(job_id, None)
        if task and not task.done():
            logger.warning(f"[JobManager] 立即取消任務 [{job_id}] 的 asyncio 協程任務")
            task.cancel()

        # 3. 清除 active_job_id
        if self.active_job_id == job_id:
            self.active_job_id = None

        # 若中斷的是排隊中的任務，立刻重新整理其他排隊任務的順位
        if was_queued:
            self.notify_queue_update()

        # 安全刪除暫存檔案
        if job.file_path and os.path.exists(job.file_path):
            try:
                os.remove(job.file_path)
            except Exception as e:
                logger.error(f"清理已取消任務暫存檔案失敗: {e}")

        return True

    def list_jobs(self, limit: int = 50, status_filter: Optional[str] = None) -> List[dict]:
        """
        列出任務清單 (依建立時間倒序排列)
        支援以 status_filter 篩選 'active' (queued + processing) 或特定狀態
        """
        job_list = list(self.jobs.values())
        if status_filter == "active":
            filtered = [j for j in job_list if j.status in ("queued", "processing")]
        elif status_filter:
            filtered = [j for j in job_list if j.status == status_filter]
        else:
            filtered = job_list

        # 最新任務排在最前面
        sorted_jobs = sorted(filtered, key=lambda x: x.created_at, reverse=True)
        return [j.dict() for j in sorted_jobs[:limit]]

    def get_active_count(self) -> int:
        """取得目前正在處理中 (processing) 的任務數量"""
        return sum(1 for j in self.jobs.values() if j.status == "processing" and not j.is_cancelled)

    def get_queue_position(self, job_id: str) -> Optional[int]:
        """計算指定任務在所有排隊任務中的順位 (1-indexed)"""
        job = self.jobs.get(job_id)
        if not job or job.status != "queued" or job.is_cancelled:
            return None

        queued_jobs = [j for j in self.jobs.values() if j.status == "queued" and not j.is_cancelled]
        queued_jobs.sort(key=lambda x: x.created_at)

        for idx, q_job in enumerate(queued_jobs):
            if q_job.job_id == job_id:
                pos = idx + 1
                job.queue_position = pos
                return pos
        return None

    def notify_queue_update(self):
        """重新計算所有處於 queued 狀態之任務排隊順位並推播"""
        queued_jobs = [j for j in self.jobs.values() if j.status == "queued" and not j.is_cancelled]
        queued_jobs.sort(key=lambda x: x.created_at)

        for idx, q_job in enumerate(queued_jobs):
            q_job.queue_position = idx + 1
            msg = f"目前運算資源忙碌中，任務正在排隊中 (排隊順位: 第 {idx + 1} 位)..."
            q_job.message = msg
            self._notify_listeners(q_job)

    def register_listener(self, job_id: str) -> asyncio.Queue:
        """註冊 SSE 事件佇列"""
        q = asyncio.Queue()
        if job_id not in self.listeners:
            self.listeners[job_id] = []
        self.listeners[job_id].append(q)
        return q

    def unregister_listener(self, job_id: str, q: asyncio.Queue):
        """移除 SSE 事件佇列"""
        if job_id in self.listeners and q in self.listeners[job_id]:
            self.listeners[job_id].remove(q)

    def prune_old_jobs(self, retention_seconds: int = 86400, max_history: int = 500) -> int:
        """
        清除過期的已結束任務，防止記憶體膨脹。
        嚴格豁免 queued 與 processing 任務！
        """
        now = time.time()
        to_delete = []

        completed_jobs = [
            j for j in self.jobs.values()
            if j.status in ("completed", "failed", "cancelled")
        ]
        completed_jobs.sort(key=lambda x: x.created_at)

        # 1. 超過保留時間的任務
        for j in completed_jobs:
            if now - j.updated_at > retention_seconds:
                to_delete.append(j.job_id)

        # 2. 超過最大歷史數量的任務
        remaining_count = len(completed_jobs) - len(to_delete)
        if remaining_count > max_history:
            excess = remaining_count - max_history
            for j in completed_jobs:
                if j.job_id not in to_delete:
                    to_delete.append(j.job_id)
                    excess -= 1
                    if excess <= 0:
                        break

        for jid in to_delete:
            if jid in self.jobs:
                del self.jobs[jid]
            if jid in self.listeners:
                del self.listeners[jid]

        return len(to_delete)

    def _append_log(self, job: JobInfo, msg: str):
        """加入帶有 HH:MM:SS 時間戳記的日誌，保留最多 100 筆"""
        t_str = time.strftime("%H:%M:%S")
        job.logs.append({"time": t_str, "msg": msg})
        if len(job.logs) > 100:
            job.logs = job.logs[-100:]

    def _notify_listeners(self, job: JobInfo):
        """推播事件至所有連線中的 SSE 佇列"""
        if job.job_id in self.listeners:
            event_payload = {
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
            for q in list(self.listeners[job.job_id]):
                try:
                    q.put_nowait(event_payload)
                except Exception:
                    pass


# 全域單例任務管理器
job_manager = JobManager()
