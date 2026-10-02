# Marker Document Conversion Microservice (雙 GPU 加速文件轉換微服務)

高效、精準、微服務化的文件轉 Markdown 引擎。基於 [datalab-to/marker](https://github.com/datalab-to/marker)，針對異質雙 GPU 硬體架構（NVIDIA RTX 3050 OEM + T1000 8GB）深度優化，整合本機 VLM 與遠端大型語言模型 (LLM)，並提供現代化 Web 儀表板、RESTful API、實時 SSE 進度串流、完整 ZIP 下載、FIFO 佇列排程與自動垃圾回收機制。

---

## 🌟 核心特色 (Key Features)

- **雙 GPU 異質協同運算 (Dual GPU Partitioning)**：
  - **GPU 0 (RTX 3050 OEM 8GB)**：以 CUDA 加速之 `llama-server` 專屬運行 `surya-2.gguf` 與 `surya-2-mmproj.gguf`，負責全頁面高解析度視覺 OCR 與數學公式標記。
  - **GPU 1 (T1000 8GB)**：負責 Marker 本地 PyTorch 輔助分析（`rf-detr` 版面偵測器、閱讀順序排序模型）。
- **遠端 LLM 語意精修與浮水印去除 (Remote vLLM Integration & Watermark Sanitization)**：
  - 支援 `--use_llm` 對接區域網路 vLLM 伺服器 (`Qwen3.8-27B`)，自動重組跨頁複雜表格、校正代數公式與目錄結構。
  - **智慧去除浮水印與干擾雜訊**：支援 `remove_watermarks` 與客製化 `block_correction_prompt`，自動識別並剔除機密背景印章、對角線浮水印與非本文雜訊。
  - **推理思考等級調控 (Reasoning Effort & Thinking Control)**：自訂可插拔 `OptimizedOpenAIService`，支援 `LLM_REASONING_EFFORT` (`low`/`none`/`medium`) 與 `LLM_ENABLE_THINKING`，抑制冗長思維鏈，避免超時並使校正速度提升數倍。
- **現代化單頁 Web 儀表板 (`http://<HOST>:8090/`)**：
  - 拖曳上傳、即時動態進度條、階段徽章（排隊中、版面與 OCR、段落結構、LLM 校正、渲染完成）。
  - **三合一成果檢視**：原始 Markdown、`marked.js` 圖文內嵌即時預覽、擷取圖片相簿（含點擊放大燈箱與單圖下載）。
  - **一鍵下載**：支援下載純 `.md` 檔案，以及一鍵打包內含 Markdown、所有抽取圖片與結構化元數據的完整 ZIP 壓縮檔。
- **高併發保護與 FIFO 佇列排程**：
  - 徹底解決 Marker 底層 Google `libpdfium.so` 多執行緒競爭崩潰 (Segfault) 與 8GB 顯存 OOM 問題。
  - 實施非同步號誌排程 (`MAX_CONCURRENT_CONVERSIONS=1`)，超額請求零拒絕自動進入 FIFO 佇列，即時推播排隊順位。
- **每日自動垃圾回收巡檢工 (Active Whitelist Garbage Collection)**：
  - 上傳暫存檔案於任務完成時立即在 `finally` 區塊刪除。
  - 背景守護協程每日執行一次巡檢，具備「動態白名單防護」，排隊中與處理中任務之檔案享有絕對豁免權，安全清理超過 24 小時之無效孤立殘留檔案。

- **即時任務隊列監控與幽靈任務中斷 (Task Monitor & Ghost Job Cancellation)**：
  - Web UI 控制面板新增即時任務清單，展示所有排隊中與執行中任務之檔名、階段、即時進度條與已耗時。
  - 提供一鍵切換即時日誌串流與「中斷幽靈任務」功能，協同終止卡死任務並即刻釋放 GPU 號誌鎖與運算資源。
  - **多層穿透式即時中斷架構 (Multi-Layer Immediate Cancellation)**：採用繼承自 `BaseException` 之特殊 `TaskCancelledException`，直接穿透 Marker 內部各處理器（如表格、頁面校正）之 `except Exception:` 迴圈；同步強制關閉阻塞中之遠端 VLM HTTP 連線 socket 並調用協程 `task.cancel()`，確保號誌鎖即時歸還，後續排隊任務零延遲自動接續處理。
- **高內聚模組化架構 (Modular & Agent-Friendly Architecture)**：
  - 徹底拆解單一龐大 Python 檔案，依據職責分離為任務管理、轉換管線、推論適配與路由層，各模組均具備詳盡技術檔頭規格，極度利於 AI Agent 與開發者分析維護。

---

## 🚀 快速開始 (Quick Start)

### 1. 複製設定檔
```bash
cp .env.example .env
# 依實際環境編輯 .env (例如設定遠端 LLM 服務位址或模型名稱)
```

### 2. 下載本機 Surya GGUF 模型
```bash
./scripts/download_models.sh
```

### 3. 一鍵啟動微服務
```bash
docker compose up -d
```

### 4. 驗證服務健康狀態
```bash
curl -s http://127.0.0.1:8090/health | jq .
```

瀏覽器開啟 `http://<伺服器IP>:8090/` 即可直接進入 Web 儀表板，或造訪 `http://<伺服器IP>:8090/docs` 查看 Swagger 互動式 API 文件。

---

## 🧩 專案模組架構與檔案清單 (Codebase Modular Architecture)

微服務核心採用模組化分離設計，各模組職責單一明確：

| 檔案路徑 | 模組定位 | 核心職責與技術規格 |
| :--- | :--- | :--- |
| [`server/app.py`](server/app.py) | **主入口與路由層** | FastAPI 實例、生命週期 (`lifespan`)、號誌鎖併發控制、RESTful/SSE 路由轉發與每日白名單垃圾回收巡檢工。 |
| [`server/jobs.py`](server/jobs.py) | **任務狀態機管理** | `JobInfo` 資料結構、`JobManager` 記憶體狀態機、FIFO 佇列序位計算、SSE 佇列分發、過期任務淘汰與幽靈任務中斷 (`cancel_job`)。 |
| [`server/converter.py`](server/converter.py) | **轉換管線與協同中斷** | `execute_conversion()` 轉換核心、`CommonParams` 參數模型、`ProgressPdfConverter` 動態包裝器，於各處理器步驟內建 `is_cancelled` 中斷檢測。 |
| [`server/services.py`](server/services.py) | **可插拔推論適配層** | `OptimizedOpenAIService` 繼承自 Marker 原生類別，支援 `reasoning_effort` 思考抑制、思維鏈關閉 (`thinking: False`) 與 `BadRequestError` 自動平滑降級。 |
| [`server/config.py`](server/config.py) | **全域環境設定** | 基於 Pydantic Settings 動態載入 `.env` 環境變數（雙 GPU 配置、vLLM 端點、併發限制與浮水印清理預設 Prompt）。 |
| [`server/templates/index.html`](server/templates/index.html) | **單頁 Web 儀表板** | 獨立前端 HTML/CSS/JS 介面，支援檔案拖曳、即時任務隊列監控、圖文渲染預覽、圖片畫廊燈箱與幽靈任務一鍵中斷。 |

---

## 📡 核心 API 端點 (API Endpoints)

| 方法 | 端點路徑 | 說明 |
| :--- | :--- | :--- |
| `GET` | `/health` | 檢查雙 GPU、遠端 vLLM 連線狀態、佇列深度與清理政策 |
| `GET` | `/marker/jobs` | 查詢最近或特定狀態的任務清單（供 Web UI 任務監控面板即時調用） |
| `POST` | `/marker/jobs/{job_id}/cancel` | 中斷排隊中任務或強制終止執行中的幽靈/卡死任務並釋放資源 |
| `POST` | `/marker/upload/async` | 非同步上傳文件，立即取得 `job_id` 與排隊序位 |
| `GET` | `/marker/jobs/{job_id}/stream` | Server-Sent Events (SSE) 實時串流進度與終端機日誌 |
| `GET` | `/marker/jobs/{job_id}` | 輪詢查詢任務當前進度、階段狀態與轉換結果 |
| `GET` | `/marker/jobs/{job_id}/download` | 打包下載完整 ZIP（含 Markdown、所有圖表與 metadata） |
| `GET` | `/marker/jobs/{job_id}/images/{name}` | 下載單張擷取之高解析度圖表原始二進位圖檔 |
| `POST` | `/marker/upload` | 傳統同步上傳轉換端點（直接等待全部轉換完畢回傳） |

---

## 📚 專案技術文件 (Documentation)

更深入的技術細節、架構圖解與維護指南請參閱 `doc/` 資料夾：

- 📐 [系統架構規劃說明書 (architecture.md)](doc/architecture.md)：雙 GPU 硬體拓撲、推論層解耦、垃圾回收與 FIFO 佇列技術內幕。
- 📖 [使用者與 API 操作指南 (user_guide.md)](doc/user_guide.md)：詳細 API 規格、參數說明、SSE 串流範例程式碼（Python / JS）。
- 🛠️ [技術維護手冊 (maintenance.md)](doc/maintenance.md)：NVIDIA 驅動診斷、Docker 生命週期、版本一鍵升級與常見疑難排解。
- 📋 [專案建置與執行計畫書 (implementation_plan.md)](doc/implementation_plan.md)：專案開發與各階段執行驗證計畫。
