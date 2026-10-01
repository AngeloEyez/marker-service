# Marker 文件轉換微服務 — 使用手冊 (User Guide)

本手冊提供 **Marker Document Conversion Microservice** 的詳細 API 規格說明、即時進度更新機制、Web 儀表板操作與跨語言呼叫範例。

---

## 一、 快速入門 (Quick Start)

### 1.1 服務存取位址
* **即時進度儀表板 (Web UI)**：`http://<伺服器IP>:8090/` 或 `http://<伺服器IP>:8090/ui`
* **Swagger 互動文件**：`http://<伺服器IP>:8090/docs`
* **ReDoc 文件規格**：`http://<伺服器IP>:8090/redoc`
* **健康檢查端點**：`http://<伺服器IP>:8090/health`
* **非同步轉換與即時串流**：`POST /marker/upload/async`、`GET /marker/jobs/{job_id}/stream`

### 1.2 檢查微服務與 GPU 狀態
在發送轉換任務前，建議先測試 `/health` 確認 GPU 及遠端 vLLM 連線正常：

```bash
curl -s http://127.0.0.1:8090/health | jq .
```

**預期回傳範例**：
```json
{
  "status": "healthy",
  "cuda_available": true,
  "gpu_count": 2,
  "gpus": [
    {
      "index": 0,
      "name": "NVIDIA GeForce RTX 3050 OEM",
      "total_memory_mb": 8192.0
    },
    {
      "index": 1,
      "name": "NVIDIA T1000 8GB",
      "total_memory_mb": 8192.0
    }
  ],
  "remote_vllm": {
    "configured_url": "http://192.168.1.5:8000/v1",
    "connected": true,
    "models": ["Qwen3.8-27B", "qwen"]
  },
  "models_loaded": true,
  "active_jobs_count": 2
}
```

---

## 二、 預設參數調整與 Swagger 介面規範

Swagger 介面預設值已根據生產環境最佳實踐調整如下：

| 參數名稱 | 類型 | 預設值 | 調整說明 |
| :--- | :--- | :--- | :--- |
| `use_llm` | boolean | **`true`** | **預設啟用** 遠端 vLLM (`Qwen3.8-27B`) 進行表格修復、多欄段落閱讀順序與高階語意校正。 |
| `mode` | string | **`"balanced"`** | **預設 balanced**，充分調用 GPU 0 (RTX 3050) 的 Surya 視覺模型與 OCR 引擎。 |
| `page_range` | string | **`null` (留空)** | **預設為空**，代表完整處理上傳之整份文件。如需自訂可填入如 `0,2-5`。 |
| `paginate_output`| boolean | **`true`** | **預設啟用** 分頁標記，在 Markdown 中自動生成 `{頁碼}----...` 分隔標籤，便於 RAG 切塊與引用。 |
| `output_format` | string | **`"markdown"`** | 預設輸出優化後的 Markdown 文字。 |

---

## 三、 即時進度更新與非同步處理機制 (Real-time Progress)

由於大型 PDF 或啟用 LLM 後校正處理時間可能達數十秒至數分鐘，微服務提供 **非同步排程 (Async Job)**、**輪詢 (Polling)** 以及 **伺服器推播 (Server-Sent Events, SSE)** 三種機制，徹底解決「上傳後不知道卡住還是處理中」的問題。

### 3.1 處理階段生命週期 (Lifecycle Stages)

| 階段代碼 (`stage`) | 進度估計 (`progress`) | 階段說明 |
| :--- | :--- | :--- |
| `queued` | 0% ~ 5% | 系統 GPU 或運算資源忙碌中，任務自動進入 FIFO 佇列等待，具備排隊順位 (`queue_position`)。 |
| `uploaded` | 5% | 檔案成功上傳，建立 `job_id`，準備進入轉換管線。 |
| `preparing` | 15% | 取得 GPU 資源鎖，載入模型推論設定、初始化轉換處理管線。 |
| `layout_ocr` | 25% ~ 50% | 調用 GPU 0 (RTX 3050 OEM) 的 Surya 模型進行版面分析與 OCR 文字辨識。 |
| `structure` | 50% | 分析閱讀順序、段落層級、目錄結構與跨頁連結。 |
| `processing` / `llm_refinement` | 50% ~ 90% | 執行 25+ 個精修處理器；當涉及複雜表格或數學公式時，調用遠端 vLLM (`Qwen3.8-27B`) 進行語意修正。 |
| `rendering` | 92% | 將 AST 結構樹渲染為最終 Markdown，並擷取抽取圖表影像。 |
| `done` | 100% | 轉換順利完成，回傳結果。 |
| `error` | - | 發生異常，立即中斷並記錄完整 Python Traceback 與錯誤訊息。 |

---

### 3.2 相關 API 端點

#### 1. 非同步上傳檔案：`POST /marker/upload/async`
* **請求**：與 `/marker/upload` 相同之 `multipart/form-data`。
* **回傳**：立即回傳任務資訊（不需等待轉換完成），若系統忙碌中會標記排隊序位。
```json
{
  "job_id": "job_20261001_063027_cd95db",
  "status": "queued",
  "queue_position": 1,
  "filename": "annual_report.pdf",
  "message": "檔案上傳成功，排隊處理中...",
  "poll_url": "/marker/jobs/job_20261001_063027_cd95db",
  "stream_url": "/marker/jobs/job_20261001_063027_cd95db/stream",
  "download_url": "/marker/jobs/job_20261001_063027_cd95db/download"
}
```

#### 2. 查詢任務狀態與結果：`GET /marker/jobs/{job_id}`
* 輪詢查詢目前進度、當前階段、耗時秒數、排隊順位、詳細執行歷程日誌與轉換產出。
```json
{
  "job_id": "job_20261001_063027_cd95db",
  "filename": "annual_report.pdf",
  "status": "processing",
  "stage": "layout_ocr",
  "progress": 25,
  "queue_position": null,
  "message": "正在使用 GPU 0 (RTX 3050) 執行 Surya OCR 與版面偵測...",
  "elapsed_seconds": 18.5,
  "logs": [
    {"time": "14:30:27", "msg": "檔案「annual_report.pdf」已上傳，建立任務 ID: job_20261001_063027_cd95db"},
    {"time": "14:30:31", "msg": "正在使用 GPU 0 (RTX 3050) 執行 Surya OCR 與版面偵測..."}
  ],
  "result": null,
  "error": null
}
```

#### 3. 實時事件推播：`GET /marker/jobs/{job_id}/stream` (SSE)
* 基於 `text/event-stream` 規範。連線建立後，每當階段切換、進度更新或產生新日誌時自動發送事件，完成或報錯時自動終止。
```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache

data: {"job_id":"job_...","stage":"layout_ocr","progress":25,"message":"正在使用 GPU 0 (RTX 3050) 執行 Surya OCR 與版面偵測..."}

data: {"job_id":"job_...","stage":"llm_refinement","progress":70,"message":"正在透過遠端 vLLM (Qwen3.8-27B) 進行高精準度語意與格式校正 (LLMTableProcessor)..."}

data: {"job_id":"job_...","status":"completed","stage":"done","progress":100,"result":{...}}
```

#### 4. 打包下載完整資料包：`GET /marker/jobs/{job_id}/download` (或 `/zip`)
* **功能**：將轉換產出的 Markdown 檔案、所有抽取的獨立圖表/影像（PNG/JPEG），以及 `metadata.json` 打包成單一標準 ZIP 壓縮檔案下載。
* **標頭**：`Content-Type: application/zip`，`Content-Disposition: attachment; filename="{檔名}_converted.zip"`。
* **解壓結構**：
  ```text
  annual_report_converted.zip
  ├── annual_report.md          # 完整 Markdown 內文（圖片連結與檔名吻合）
  ├── _page_2_Figure_1.png      # 抽取的圖表 1
  ├── _page_5_Figure_3.png      # 抽取的圖表 2
  └── metadata.json             # 頁碼統計、目錄樹等結構化資料
  ```

#### 5. 擷取圖片原生下載：`GET /marker/jobs/{job_id}/images/{image_name}`
* 取得特定任務所擷取的單張圖片二進位原始資料 (`image/png` 或 `image/jpeg`)，便於前端動態 `<img src="...">` 引用或下載。

#### 6. 最近任務清單：`GET /marker/jobs`
* 取得記憶體中保留的最近 100 筆任務清單與概要。

---

## 四、 內建 Web 儀表板 (`http://<IP>:8090/`)

微服務內建現代化單頁 Web 儀表板，提供使用者零代碼操作體驗：
1. **拖曳上傳區**：支援直接將 PDF、Word、PPT 拖入瀏覽器。
2. **參數快捷面板**：預設已勾選「啟用 Qwen3.8-27B LLM」、「Balanced 模式」、「分頁標記」。
3. **即時動態進度條**：以 SSE 串流連線即時更新 0% ~ 100% 視覺化進度。
4. **當前階段狀態徽章**：動態標記「排隊等待中 (第 N 位)」、「版面與 OCR」、「段落與結構」、「遠端 LLM 校正」、「Markdown 渲染」。
5. **即時終端機日誌 (Live Terminal Logs)**：詳細印出各處理器的執行時間點與進展。
6. **多元成果展示與下載功能**：
   * **📦 下載完整 ZIP (Markdown + 圖片)**：一鍵打包下載內含 `.md` 檔案、所有抽取插圖與 `metadata.json` 之 ZIP 壓縮檔。
   * **📥 下載 .md 檔案**：僅下載純 Markdown 文字檔。
   * **📋 複製 Markdown**：一鍵將內容複製至系統剪貼簿。
7. **三合一成果切換分頁**：
   * **📝 原始 Markdown**：檢視原始 Markdown 原始碼。
   * **👁️ 圖文渲染預覽**：以 `marked.js` 進行排版渲染，自動將抽取之圖表以內嵌方式無縫展示於文中相對應位置。
   * **🖼️ 擷取圖片相簿**：顯示圖片數量徽章（如 `[3]`），以卡片網格展示所有抽取的圖表預覽、提供點擊放大檢視燈箱 (Lightbox)，並支援個別圖片單獨下載。
8. **異常警示視窗**：若 PDF 損壞或 LLM 超時，以紅色警示卡片顯示錯誤原因與 Python Traceback，方便排查。

---

## 五、 自動清除機制與高併發排程

### 5.1 暫存區與日誌自動清除機制 (Garbage Collection)
為確保長時間運作不會造成磁碟膨脹或記憶體外洩，同時確保**大量排程（數百份檔案排隊數小時）之暫存檔案絕不被誤刪**，系統實施具備「動態白名單防護」之三層清理保障：
1. **任務即刻清理 (Immediate Cleanup)**：
   * 用戶上傳的文件在轉換結束（成功或失敗）當下，系統於 `finally` 區塊立即執行 `os.remove` 刪除上傳檔案，磁碟**絕不滯留**任何正常處理完的暫存檔。
   * 任務完成後主動調用 `torch.cuda.empty_cache()`，即刻釋放顯存碎片。
2. **日誌與記憶體容量安全限制**：
   * 單一任務的日誌訊息上限為 150 筆，超過時自動滑動視窗保留最新紀錄，避免長文件大量 log 耗費記憶體。
   * 系統最多保留最近 500 筆歷史任務摘要；且淘汰機制**只會清除已結束 (`completed`/`failed`) 的舊紀錄，嚴禁淘汰排隊中 (`queued`) 或執行中 (`processing`) 的活躍任務**。
3. **每日定期垃圾回收巡檢工 (Daily Cleanup Worker)**：
   * 服務啟動時自動在背景運行巡檢常駐協程，固定**一天執行一次**（每隔 `CLEANUP_INTERVAL_SECONDS = 86400` 秒）：
     * **活躍任務動態白名單**：自動取得所有當前排隊中與執行中任務的檔案路徑與 `job_id` 前綴，享有無條件豁免權，即使排隊數天也絕對不刪。
     * **孤立殘留檔案清理**：僅針對非活躍任務且最後修改時間超過 `FILE_RETENTION_SECONDS` (預設 86400 秒 / 24 小時) 的孤立無效檔案進行強制刪除。
     * **歷史任務淘汰**：已完成或失敗超過 `JOB_RETENTION_SECONDS` (預設 86400 秒 / 24 小時) 的任務元數據，自動自記憶體字典抹除。

### 5.2 高併發請求與排程排隊機制 (Concurrency & Queue)
當多個用戶或多個系統同時發送轉換請求時：
1. **單一行程安全保護 (`MAX_CONCURRENT_CONVERSIONS=1`)**：
   * Marker 底層的 PDF 解析核心（`pypdfium2` / `libpdfium.so`）在同行程內多執行緒並行時存在 C++ 記憶體競爭缺陷（會引發 Segfault 崩潰）。同時，本地輔助 GPU (T1000) 具備 8GB 顯存，多工並行極易引發 CUDA OOM。
   * 因此系統預設配置嚴格的序列化排程保護 (`MAX_CONCURRENT_CONVERSIONS=1`)，確保各任務在硬體與函式庫層級 100% 穩定安全。
2. **FIFO 無阻斷排隊 (Zero-rejection Queuing)**：
   * 當 GPU 正在處理某個文件時，後續進來的所有請求**不會被拒絕**，而是立刻建立任務並取得 `job_id`，狀態標記為 `queued`。
   * 系統自動計算每位請求者的排隊順位 (`queue_position`：第 1 位、第 2 位...)，並透過 SSE 串流連線即時推播給網頁前端與 API 調用端。
   * 當前一項任務完成並清理暫存後，排隊序位第一的任務會立即自動開始執行，無需人工干預。
3. **健康檢查即時監控**：
   * 任何調用端皆可透過 `GET /health` 獲取當前隊列忙碌狀態：
     * `active_processing_jobs`：目前正在推論運算的任務數（0 或 1）。
     * `queued_waiting_jobs`：目前正在排隊等待的任務數。
     * `total_jobs_in_memory`：當前記憶體中保留的任務總數。

---

## 六、 呼叫範例代碼

### 6.1 Python 串流監聽範例 (SSE)
```python
import json
import requests
import sseclient  # pip install sseclient-py

BASE_URL = "http://127.0.0.1:8090"
PDF_FILE = "sample.pdf"

# 1. 提交非同步任務
with open(PDF_FILE, "rb") as f:
    resp = requests.post(
        f"{BASE_URL}/marker/upload/async",
        files={"file": (PDF_FILE, f, "application/pdf")},
        data={"use_llm": "true", "mode": "balanced", "paginate_output": "true"}
    )
job_info = resp.json()
job_id = job_info["job_id"]
print(f"[建立任務] ID: {job_id}")

# 2. 透過 SSE 接收即時進度更新
stream_url = f"{BASE_URL}/marker/jobs/{job_id}/stream"
response = requests.get(stream_url, stream=True)
client = sseclient.SSEClient(response)

for event in client.events():
    if not event.data or event.data == ": keep-alive":
        continue
    data = json.loads(event.data)
    stage = data.get("stage")
    progress = data.get("progress")
    msg = data.get("message")
    elapsed = data.get("elapsed_seconds")
    print(f"[{elapsed}s][{progress}%][{stage}] {msg}")

    if data.get("status") == "completed":
        print("[成功] 轉換完成！輸出前 100 字：")
        print(data["result"]["output"][:100])
        break
    elif data.get("status") == "failed":
        print(f"[失敗] 錯誤原因: {data.get('error')}")
        break
```

### 6.2 前端 JavaScript 整合範例 (Browser Fetch & EventSource)
```javascript
async function uploadAndTrack(file) {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("use_llm", "true");
  formData.append("mode", "balanced");
  formData.append("paginate_output", "true");

  // 1. 發送非同步上傳
  const res = await fetch("/marker/upload/async", {
    method: "POST",
    body: formData
  });
  const { job_id, stream_url } = await res.json();
  console.log("任務 ID:", job_id);

  // 2. 建立 SSE 連線
  const evtSource = new EventSource(stream_url);

  evtSource.onmessage = function(event) {
    const data = JSON.parse(event.data);
    console.log(`[${data.progress}%] ${data.stage}: ${data.message}`);

    // 更新網頁 UI 進度條
    document.getElementById("my-progress-bar").style.width = data.progress + "%";

    if (data.status === "completed") {
      evtSource.close();
      console.log("轉換結果：", data.result.output);
    } else if (data.status === "failed") {
      evtSource.close();
      alert("轉換發生錯誤：" + data.message);
    }
  };
}
```

### 6.3 傳統同步 cURL 呼叫
如需直接等待結果返回，仍可使用原始同步端點：
```bash
curl -X POST "http://127.0.0.1:8090/marker/upload" \
  -F "file=@/path/to/report.pdf" \
  -o output.json
```
