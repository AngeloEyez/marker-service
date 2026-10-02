"""
================================================================================
模組名稱: server.config
用途說明: 全域設定檔與環境變數動態綁定模組
技術規格:
  - 核心類別: Settings (基於 pydantic_settings.BaseSettings)
  - 支援設定:
    - 網路監聽: HOST (0.0.0.0), PORT (8090)
    - Surya 本地推論: SURYA_INFERENCE_BACKEND, SURYA_INFERENCE_URL, LLAMA_CPP_NGL
    - 遠端 vLLM / OpenAI: REMOTE_LLM_URL, REMOTE_LLM_MODEL, REMOTE_LLM_API_KEY, LLM_TIMEOUT, LLM_MAX_RETRIES
    - 推理思考抑制: LLM_REASONING_EFFORT ('low'), LLM_ENABLE_THINKING (False)
    - 轉換預設: DEFAULT_MODE ('balanced'), DEFAULT_OUTPUT_FORMAT ('markdown'), MAX_UPLOAD_SIZE_MB
    - 提示詞工程: DEFAULT_WATERMARK_REMOVAL_PROMPT, DEFAULT_BLOCK_CORRECTION_PROMPT
    - 排程與清理政策: MAX_CONCURRENT_CONVERSIONS (1), JOB_RETENTION_SECONDS (86400), FILE_RETENTION_SECONDS (86400)
維護指南:
  - AI Agent 或維護者新增環境變數時，請同時同步至 .env 與 .env.example。
================================================================================
"""

import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8090"))
    
    # Surya / Marker Local Perception Configuration
    SURYA_INFERENCE_BACKEND: str = os.getenv("SURYA_INFERENCE_BACKEND", "llamacpp")
    SURYA_INFERENCE_URL: str = os.getenv("SURYA_INFERENCE_URL", "")
    LLAMA_CPP_NGL: int = int(os.getenv("LLAMA_CPP_NGL", "99"))
    
    # Remote vLLM / OpenAI Service Configuration for --use_llm
    REMOTE_LLM_URL: str = os.getenv("OPENAI_BASE_URL", "http://192.168.1.5:8000/v1")
    REMOTE_LLM_MODEL: str = os.getenv("OPENAI_MODEL", "Qwen3.8-27B")
    REMOTE_LLM_API_KEY: str = os.getenv("OPENAI_API_KEY", "none")
    LLM_TIMEOUT: int = int(os.getenv("LLM_TIMEOUT", "180"))
    LLM_MAX_RETRIES: int = int(os.getenv("LLM_MAX_RETRIES", "3"))
    LLM_REASONING_EFFORT: str = os.getenv("LLM_REASONING_EFFORT", "low")
    LLM_ENABLE_THINKING: bool = os.getenv("LLM_ENABLE_THINKING", "false").lower() in ("true", "1", "yes")
    
    # Conversion Defaults
    DEFAULT_MODE: str = os.getenv("DEFAULT_MODE", "balanced")
    DEFAULT_OUTPUT_FORMAT: str = os.getenv("DEFAULT_OUTPUT_FORMAT", "markdown")
    MAX_UPLOAD_SIZE_MB: int = int(os.getenv("MAX_UPLOAD_SIZE_MB", "100"))
    UPLOAD_DIRECTORY: str = os.getenv("UPLOAD_DIRECTORY", "/tmp/marker_uploads")

    # VLM / LLM Block Correction & Watermark Removal
    DEFAULT_WATERMARK_REMOVAL_PROMPT: str = os.getenv(
        "DEFAULT_WATERMARK_REMOVAL_PROMPT",
        (
            "You are a professional document cleanup and sanitization specialist. "
            "Carefully analyze the page image and text blocks. Identify and eliminate all irrelevant background noise, "
            "diagonal or faded watermarks (such as 'CONFIDENTIAL', 'DRAFT', 'SAMPLE', or internal stamps), organization branding labels, "
            "confidentiality notices, tracking email addresses, and repetitive header/footer noise unrelated to the main content. "
            "For blocks consisting solely of watermarks, boilerplate disclaimers, or noise, clear their content by setting their 'html' field to an empty string (\"\"). "
            "For blocks where noise is interspersed with valid text, strip out the noise while preserving the legitimate content. "
            "Strictly preserve all genuine body paragraphs, section headers, code blocks, and table contents intact. "
            "Only return the blocks that have been modified."
        ),
    )
    DEFAULT_BLOCK_CORRECTION_PROMPT: str = os.getenv("DEFAULT_BLOCK_CORRECTION_PROMPT", "")

    # 排程與資源保護機制 (Queue & Cleanup Settings)
    MAX_CONCURRENT_CONVERSIONS: int = int(os.getenv("MAX_CONCURRENT_CONVERSIONS", "1"))
    MAX_COMPLETED_JOBS_HISTORY: int = int(os.getenv("MAX_COMPLETED_JOBS_HISTORY", "500"))
    JOB_RETENTION_SECONDS: int = int(os.getenv("JOB_RETENTION_SECONDS", "86400"))      # 已完成任務於記憶體保留 24 小時 (1天)
    FILE_RETENTION_SECONDS: int = int(os.getenv("FILE_RETENTION_SECONDS", "86400"))     # 孤立殘留檔案保留 24 小時 (1天)
    CLEANUP_INTERVAL_SECONDS: int = int(os.getenv("CLEANUP_INTERVAL_SECONDS", "86400"))  # 背景自動巡檢間隔 24 小時 (1天一次)

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
