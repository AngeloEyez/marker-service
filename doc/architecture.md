# Marker 文件轉換微服務 — 系統架構規劃說明書

本文件詳細闡述 **Marker Document Conversion Microservice** 在雙 GPU (NVIDIA RTX 3050 OEM + T1000) 環境下的完整技術架構、硬體拓撲、容器編排與資料流程設計。

---

## 一、 整體架構總覽 (Architecture Overview)

系統採用**微服務化 (Microservices) 與運算層級分離**之架構設計。前端提供標準 RESTful API，推論層分為**本機視覺感知 (Local VLM Perception)** 與**遠端語意校正 (Remote LLM Reasoning)** 雙軌運作。

```mermaid
flowchart TB
    subgraph ClientLayer["客戶端應用層"]
        ClientWeb["Web 前端 / RAG 應用"]
        ClientScript["批次排程 Python / cURL"]
    end

    subgraph Host["Debian 13 (dockerVM) 主機環境"]
        subgraph DockerCompose["Docker Compose 服務群"]
            API["FastAPI 微服務 (marker-api)\nPort: 8090"]
            LlamaServer["llama-server 推論引擎 (marker-llama-server)\nPort: 8001 / CUDA 加速"]
        end

        subgraph Hardware["主機硬體與驅動層 (Kernel 6.12.111 + Driver 550.163)"]
            GPU0["GPU 0: NVIDIA RTX 3050 OEM\n(Ampere CC 8.6, 8GB VRAM)"]
            GPU1["GPU 1: NVIDIA T1000\n(Turing CC 7.5, 8GB VRAM)"]
        end

        subgraph Storage["磁碟掛載與快取 (獨立 /dev/sdb, 84GB 可用)"]
            ModelDir["/models (GGUF 模型)"]
            CacheDir["/data/cache (HF / Torch 快取)"]
        end
    end

    subgraph RemoteNetwork["區域網路推論伺服器 (192.168.1.5)"]
        RemoteVLLM["vLLM 伺服器 (Port 8000)"]
        QwenModel["Qwen3.8-27B 密集語言模型"]
    end

    ClientWeb -->|"HTTP POST (Port 8090)"| API
    ClientScript -->|"HTTP POST (Port 8090)"| API
    
    API -->|"1. 數位文字解析 (pdftext)"| Host
    API -->|"2. 視覺 OCR / 佈局辨識\n(SURYA_INFERENCE_URL)"| LlamaServer
    LlamaServer -->|"全層 GPU 卸載 (-ngl 99)"| GPU0
    LlamaServer -.->|"讀取"| ModelDir

    API -->|"3. 本地版面輔助模型 (rf-detr / PyTorch)"| GPU1
    API -.->|"快取"| CacheDir

    API -->|"4. --use_llm 語意校正\n(表格合併/數學公式/版面重整)"| RemoteVLLM
    RemoteVLLM --- QwenModel
```

---

## 二、 硬體與 GPU 運算拓撲設計

本虛擬機器配置有兩張不同架構世代的 NVIDIA GPU，各自具備 8GB 專屬視訊記憶體 (VRAM)：

| 屬性 | GPU 0 (主要視覺推論卡) | GPU 1 (輔助分析卡) |
| :--- | :--- | :--- |
| **晶片型號** | NVIDIA GeForce RTX 3050 OEM | NVIDIA T1000 8GB |
| **微架構代號** | GA106 (Ampere) | TU117GL (Turing) |
| **Compute Capability** | **8.6** (支援 BF16, FP16, INT8, INT4) | **7.5** (支援 FP16, INT8, INT4) |
| **視訊記憶體 (VRAM)** | 8,192 MB GDDR6 | 8,192 MB GDDR6 |
| **專屬任務分配** | **llama-server (Surya 2 GGUF VLM)**<br>專責全頁面 OCR、高解析度公式辨識 | **Marker 本地 PyTorch 輔助模型**<br>專責 `rf-detr` 版面偵測器、閱讀順序排序模型、OCR 錯誤過濾 |
| **顯存預估佔用** | 約 1.5 GB ~ 2.0 GB (保留 6GB+ 作為並行批次緩衝) | 約 1.0 GB ~ 1.5 GB |

> [!NOTE]
> **異質 GPU 架構決策**：
> 由於 Ampere (CC 8.6) 與 Turing (CC 7.5) 之指令集與運算單元特性不同，無法跨卡進行 Tensor Parallel (TP=2) 共同載入單一大型權重。本架構採**功能解耦 (Role-based Partitioning)**，充分發揮兩張顯卡的各自長處，避免跨卡匯流排同步之效能損耗。

---

## 三、 容器編排與服務組件

微服務由 `docker-compose.yml` 統籌管理兩個核心容器：

### 3.1 `marker-llama-server` 容器
* **映像檔**：`ghcr.io/ggml-org/llama.cpp:server-cuda`
* **功能**：以極致 C++ 效率提供 OpenAI 相容之 Vision-Language 推論端點。
* **模型**：載入 `datalab-to/surya-ocr-2-gguf` (`surya-2.gguf` 主模型 + `surya-2-mmproj.gguf` 多模態投影權重)。
* **加速參數**：
  * `-ngl 99`：全模型權重 100% 卸載至 RTX 3050 顯存中。
  * `--ctx-size 16384`：提供足夠的影像 Token 與文字 Context 空間。
  * `--parallel 8`：支援最高 8 路並行 OCR 辨識槽位。
* **監聽端口**：容器內部與內部網路 `8001`。

### 3.2 `marker-api` 容器
* **映像檔**：本機建置之 `Dockerfile`（基於 CUDA 12.4 Runtime 與 Python 3.11）。
* **功能**：
  * 提供對外 HTTP RESTful API（監聽於主機 Port `8090`）。
  * 整合 `marker-pdf[full]` 核心轉換管線，可自動處理 PDF、DOCX、PPTX、XLSX、EPUB 與一般影像。
  * 整合 `FastAPI`，支援 Swagger UI (`/docs`)、即時健康監控 (`/health`) 與檔案上傳處理。
  * 透過 `SURYA_INFERENCE_URL=http://llama-server:8001/v1` 將視覺請求交由 `marker-llama-server` 處理。
  * 當用戶傳入 `use_llm=true` 時，動態將跨頁表格、內嵌數學公式等語意增強任務路由至遠端 `http://192.168.1.5:8000/v1` 的 `Qwen3.8-27B` 進行處理。

---

## 四、 核心轉換時序圖 (Sequence Diagram)

```mermaid
sequenceDiagram
    autonumber
    actor User as 用戶 / 客戶端
    participant API as Marker FastAPI (8090)
    participant Engine as Marker 核心管線
    participant Llama as llama-server (RTX 3050)
    participant Remote as 遠端 vLLM (Qwen3.8-27B)

    User->>API: POST /marker/upload (文件 + 參數: use_llm=true, mode=balanced)
    API->>API: 暫存檔案至 /tmp/marker_uploads
    API->>Engine: 初始化 PdfConverter 管線
    
    rect rgb(240, 248, 255)
        note over Engine, Llama: 階段一：視覺版面辨識與 OCR (GPU 0: RTX 3050)
        Engine->>Engine: 解析數位文字層 (pdftext)
        Engine->>Llama: POST /v1/chat/completions (頁面影像 + 視覺提示詞)
        Llama-->>Engine: 回傳版面方框、文字識別與數學公式標記
    end

    rect rgb(255, 245, 238)
        note over Engine, Remote: 階段二：LLM 語意格式校正 (遠端 vLLM)
        opt 當 use_llm == true
            Engine->>Remote: POST /v1/chat/completions (跨頁表格合併、複雜公式轉換)
            Remote-->>Engine: 回傳校正後結構化 Markdown / JSON
        end
    end

    Engine->>API: 組裝轉換結果 (Markdown 內文, Base64 抽取圖片, 元數據)
    API->>API: 清理暫存上傳檔
    API-->>User: 200 OK (回傳 JSON 成果)
```

---

## 五、 儲存架構與磁碟保護策略

由於虛擬機器根目錄 `/` 空間僅剩約 18GB，為避免模型快取與暫存檔導致系統磁區耗盡，本架構採取嚴謹的儲存隔離規劃：

1. **大容量專屬分區**：
   * 所有 Docker 資料、容器層、卷冊均位於 `/var/lib/docker`，此路徑掛載於實體磁碟 `/dev/sdb`（容量 94GB，可用高達 **84GB**）。
2. **目錄映射規劃**：
   * `./models`：掛載 Surya GGUF 模型權重檔（總計約 1.4GB）。
   * `./cache/huggingface`：HuggingFace 暫存快取目錄。
   * `./cache/torch`：PyTorch 輔助模型 (`rf-detr` 等) 下載目錄。
   * `./uploads`：上傳檔案臨時目錄（處理完畢後立即自動卸載與刪除）。

---

## 六、 即時進度追蹤與非同步架構 (Job & Progress Tracking Engine)

針對複雜長篇 PDF 或啟用大型語言模型 (LLM) 帶來的處理延遲，本系統在 `marker-api` 內構建了一套非同步任務管理與事件推播引擎：

```mermaid
flowchart LR
    Client["客戶端 / 瀏覽器"] -->|"POST /marker/upload/async"| API["FastAPI 接收端點"]
    API -->|"建立 JobInfo (保留最近100筆)"| JM["JobManager (Thread-Safe)"]
    API -->|"BackgroundTasks 派發"| Worker["執行緒集區 (Threadpool)"]
    Worker -->|"動態回報 stage / progress / log"| JM
    JM -->|"廣播事件"| SSE["SSE 監聽器隊列 (Queue)"]
    SSE -->|"GET /marker/jobs/{id}/stream"| Client
    Client -.->|"或定時輪詢 GET /marker/jobs/{id}"| JM
```

1. **轉換器階層攔截 (`ProgressPdfConverter`)**：
   * 動態繼承 Marker 的 `PdfConverter` 類別，在 `build_document` 生命週期關鍵節點插入探針。
   * 精準捕獲：`uploaded (5%)` $\to$ `preparing (15%)` $\to$ `layout_ocr (25%)` $\to$ `structure (50%)` $\to$ `processors / llm_refinement (50%~90%)` $\to$ `rendering (92%)` $\to$ `done (100%)`。
2. **雙軌監聽機制 (SSE + Polling)**：
   * **SSE (`/marker/jobs/{id}/stream`)**：單一連線、低延遲推播，支援瀏覽器原生 `EventSource`，包含每 15 秒心跳保活。
   * **Polling (`/marker/jobs/{id}`)**：提供傳統無連線狀態的 RESTful 輪詢支援。
3. **即時日誌與例外捕獲**：
   * 每個任務均維護結構化時間戳日誌（`logs`）。
   * 任何在 OCR、PyTorch 或 LLM 階段產生的例外（如 PDF 損壞、格式不支援、模型超時），都會被全域例外處理器捕獲，將完整的錯誤訊息與 Python Traceback 寫入任務物件，並將狀態標記為 `failed`，避免客戶端永久掛起。

---

## 七、 資源回收與自動清理機制 (Resource Management & Garbage Collection)

為保障微服務在高負載、多請求環境下長期穩定運作，避免磁碟空間佔滿或 RAM/VRAM 顯存耗盡，同時確保在「大量排程任務同時湧入（數百個檔案排隊處理數小時以上）」情境下，**暫存檔案絕不遭到誤刪**，系統實施具備「動態白名單豁免機制」之三層式資源生命週期管理策略：

```mermaid
flowchart TD
    subgraph Layer1["第 1 層：任務即時回收 (Immediate Cleanup)"]
        TaskDone["任務完成 (Completed) 或失敗 (Failed)"] --> FinalBlock["finally 執行區塊"]
        FinalBlock --> DeleteFile["立即刪除 /tmp/marker_uploads 暫存原始檔"]
        FinalBlock --> EmptyCache["調用 torch.cuda.empty_cache() 釋放暫時顯存"]
    end

    subgraph Layer2["第 2 層：記憶體容量配額保護 (Active-Safe Quota)"]
        JobLog["任務單次 Log 紀錄"] --> CapLog{"Log 筆數 > 150 筆?"}
        CapLog -- 是 --> TrimLog["滑動視窗截斷，僅保留最新 150 筆"]
        CapLog -- 否 --> KeepLog["正常寫入"]
        JobCount["歷史已結束任務 > 500 筆?"] --> EvictOldest["僅從 completed/failed 中淘汰最舊紀錄\n(嚴禁淘汰 queued/processing 任務)"]
    end

    subgraph Layer3["第 3 層：每日定期垃圾回收巡檢工 (Daily Cleanup Worker: 86400s)"]
        WorkerTimer["每隔 24 小時 (CLEANUP_INTERVAL_SECONDS)"] --> GetWhitelist["動態抓取活躍任務白名單\n(所有 queued 與 processing 之檔案與 job_id)"]
        GetWhitelist --> ScanDisk["掃描 /tmp/marker_uploads 實體檔案"]
        ScanDisk --> CheckWhitelist{"命中活躍任務白名單\n或前綴包含 active job_id?"}
        CheckWhitelist -- 是 (排隊/處理中) --> KeepSafe["【絕對豁免】保留檔案，絕不誤刪"]
        CheckWhitelist -- 否 (孤立殘留檔案) --> ExpiredDisk{"檔案 mtime 超過 24 小時\n(FILE_RETENTION_SECONDS)?"}
        ExpiredDisk -- 是 --> DelDisk["強制清理過期孤立殘留檔案"]
        WorkerTimer --> ScanMemory["掃描 JobManager 記憶體字典"]
        ScanMemory --> ExpiredMem{"已結束任務超過 24 小時\n(JOB_RETENTION_SECONDS)?"}
        ExpiredMem -- 是 --> DelMem["淘汰過期任務，釋放系統 RAM"]
    end
```

### 7.1 第一層：任務執行結束立即回收
* **暫存原始檔立即刪除**：無論同步或非同步轉換，文件寫入磁碟後均在 `try ... finally` 區塊中嚴格調用 `os.remove(upload_path)`，確保文件轉換結束（或遇錯中斷）時立刻銷毀實體檔案，磁碟不留滯任何正常任務的 PDF。
* **PyTorch 顯存即時釋放**：在任務完成時主動調用 `torch.cuda.empty_cache()`，將暫時分配的張量記憶體返還給作業系統與驅動，防止跨任務累積顯存碎片。

### 7.2 第二層：記憶體容量配額安全管制
* **任務日誌滑動視窗 (150 筆上限)**：每個任務維護的 `job.logs` 採用截斷式滑動視窗，上限為最新 150 條。即使極大文件包含數十個處理器，也不會因無窮日誌堆疊導致 FastAPI 行程記憶體膨脹。
* **歷史任務安全淘汰 (500 筆歷史配額)**：當歷史任務超出配額時，系統**僅針對已結束 (`completed` 或 `failed`) 的任務**進行淘汰，**永遠保留處於 `queued` (排隊中) 或 `processing` (執行中) 的活躍任務**。此舉防止在數百個檔案批次湧入時，排隊中任務被意外踢出記憶體造成客戶端查詢 404。

### 7.3 第三層：每日定期垃圾回收巡檢工 (`periodic_cleanup_worker`)
由 FastAPI `lifespan` 啟動的非同步背景協程，固定每隔 `CLEANUP_INTERVAL_SECONDS`（預設 **86400 秒 / 24 小時，一天一次**）全自動巡檢：
1. **動態活躍白名單保護 (Active Task Whitelist Protection)**：
   * 巡檢工掃描磁碟目錄時，即時調用 `job_manager.get_active_file_identifiers()`。
   * **完整路徑與檔名比對**：所有當前狀態為 `queued` 或 `processing` 之任務關聯檔案，直接列入豁免清單。
   * **Job ID 前綴比對**：凡檔名前綴開頭為任何活躍中的 `job_id`（如 `job_20261001_...`），一律強制豁免。
   * **效力保證**：即使排隊佇列累積數百份檔案、排隊耗時長達 10~20 個小時，**只要任務仍在隊列中等待，其暫存檔案就絕不會被誤刪**。
2. **孤立殘留檔案清理 (TTL: 86400 秒 / 24 小時)**：
   * 只有**完全不屬於任何排隊或執行中任務**的非活躍檔案（如客戶端上傳中途斷網導致的未完成傳輸檔、或伺服器非正常重啟遺留的前次垃圾），且最後修改時間超過 24 小時者，才判定為孤立殘留垃圾予以安全刪除。
3. **已結束任務記憶體淘汰 (TTL: 86400 秒 / 24 小時)**：
   * 已完成或失敗超過 24 小時的任務元數據自動從記憶體中抹除。
4. **顯存碎片深層整理**：巡檢週期觸發 `torch.cuda.empty_cache()`。

---

## 八、 並行排程與併發流量保護機制 (Concurrency Control & FIFO Queue)

在文件轉換場景中，PDF 解析與 OCR 涉及深層 C++ 函式庫與 GPU 矩陣加速，若無併發流量管制，多請求同時湧入將導致系統致命性崩潰。

### 8.1 關鍵技術內幕：為什麼不能盲目多執行緒並行？
在架構設計初期驗證過程中，我們發現了兩項底層硬體與函式庫的嚴格限制：
1. **`libpdfium.so` 底層 C++ 執行緒不安全 (Critical Race Condition)**：
   Marker 底層的 `pypdfium2`（基於 Google Chrome PDFium 引擎）在多執行緒環境中**非執行緒安全 (Not Thread-Safe)**。當同一行程內有多個執行緒同時呼叫 PDFium 解析或渲染頁面時，會發生記憶體衝突，直接引發作業系統核心陷阱：
   `traps: uvicorn[...] general protection fault in libpdfium.so`
   這會導致整個 Python 行程瞬間 Segfault 終止、Docker 容器無預警重啟，所有進行中的任務全數中斷。
2. **GPU 顯存 (VRAM) 安全上限**：
   本機輔助卡 NVIDIA T1000 僅具備 8GB 顯存。Marker 的 `rf-detr`、閱讀順序排序模型與版面分析在處理高解析度、多頁面時會佔用 1.5GB ~ 2.5GB 顯存。若允許多個大文件並行推論，極易突破 8GB 觸發 `CUDA Out Of Memory (OOM)`。

### 8.2 FIFO 佇列排程器設計
為徹底根絕上述問題，系統採用 **非同步號誌佇列 (Asyncio Semaphore Queue)**：

```mermaid
sequenceDiagram
    autonumber
    participant Client1 as 用戶端 A (請求 1)
    participant Client2 as 用戶端 B (請求 2)
    participant Client3 as 用戶端 C (請求 3)
    participant Gateway as FastAPI API 閘道
    participant Sem as conversion_semaphore (Limit=1)
    participant Pipeline as Marker GPU 運算管線 (GPU 1 / PDFium)

    Client1->>Gateway: POST /upload/async
    Gateway->>Sem: 取得號誌 (立即成功 1/1)
    Gateway-->>Client1: 狀態: processing, queue_position: null
    Gateway->>Pipeline: 執行任務 1 (獨佔 GPU 1 與 PDFium)

    par 用戶端 B 請求
        Client2->>Gateway: POST /upload/async
        Gateway->>Sem: 請求號誌 (目前已滿，進入非同步等待)
        Gateway-->>Client2: 狀態: queued, queue_position: 1
    and 用戶端 C 請求
        Client3->>Gateway: POST /upload/async
        Gateway->>Sem: 請求號誌 (目前已滿，進入非同步等待)
        Gateway-->>Client3: 狀態: queued, queue_position: 2
    end

    Pipeline-->>Gateway: 任務 1 執行完成
    Gateway->>Gateway: 清理暫存檔、空出顯存碎片
    Gateway->>Sem: 釋放號誌 (0/1)
    Gateway-->>Client1: 廣播 completed (SSE)

    Sem->>Gateway: 喚醒任務 2
    Gateway-->>Client2: 廣播 processing, queue_position: null (SSE)
    Gateway-->>Client3: 廣播 queued, queue_position: 1 (SSE 排隊序位前進)
    Gateway->>Pipeline: 執行任務 2
```

1. **核心序列化保護 (`MAX_CONCURRENT_CONVERSIONS=1`)**：
   * 系統透過 `asyncio.Semaphore(1)` 確保任何時刻**只有一個轉換任務**存取 `pypdfium2` 與本地 GPU 1。
   * 此舉保證了 `libpdfium.so` 運算絕對安全、0% Segfault 崩潰風險、0% CUDA OOM 風險。
2. **零阻斷排隊機制 (Non-blocking Queued State)**：
   * 當系統正忙碌時，新進的非同步請求**不會被拒絕 (不會 429 或 503)**，而是立即建立任務，賦予 `status="queued"` 與 `queue_position`（第 1 位、第 2 位...）。
   * 外部客戶端可立即取得 `job_id`，並透過 SSE 串流連線接收即時序位通知。
3. **動態序位推播 (Dynamic Queue Notification)**：
   * 每當前一個任務完成並釋放號誌時，`JobManager.notify_queue_update()` 會主動重新計算後續所有排隊中任務的序位，並透過 SSE 自動推播「排隊序位前進」給所有等待中的用戶端。
4. **即時健康監控指標**：
   * `GET /health` 端點即時揭露 `queue_status`：
     ```json
     "queue_status": {
         "max_concurrent_limit": 1,
         "active_processing_jobs": 0,
         "queued_waiting_jobs": 0,
         "total_jobs_in_memory": 3
     }
     ```
   維運人員或外部負載平衡器可輕易掌握當前系統忙碌狀態與隊列深度。


