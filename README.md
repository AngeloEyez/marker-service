# Marker Document Conversion Microservice (雙 GPU 加速文件轉換微服務)

高效、精準、微服務化的文件轉 Markdown 引擎。基於 [datalab-to/marker](https://github.com/datalab-to/marker)，針對異質雙 GPU 硬體架構（NVIDIA RTX 3050 OEM + T1000 8GB）深度優化，整合本機 VLM 與遠端大型語言模型 (LLM)，並提供現代化 Web 儀表板、RESTful API、實時 SSE 進度串流、完整 ZIP 下載、FIFO 佇列排程與自動垃圾回收機制。

---

## 🌟 核心特色 (Key Features)

- **雙 GPU 異質協同運算 (Dual GPU Partitioning)**：
  - **GPU 0 (RTX 3050 OEM 8GB)**：以 CUDA 加速之 `llama-server` 專屬運行 `surya-2.gguf` 與 `surya-2-mmproj.gguf`，負責全頁面高解析度視覺 OCR 與數學公式標記。
  - **GPU 1 (T1000 8GB)**：負責 Marker 本地 PyTorch 輔助分析（`rf-detr` 版面偵測器、閱讀順序排序模型）。
- **遠端 LLM 語意精修 (Remote vLLM Integration)**：
  - 支援 `--use_llm` 對接區域網路 vLLM 伺服器 (`Qwen3.8-27B`)，自動重組跨頁複雜表格、校正代數公式與目錄結構。
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

## 📡 核心 API 端點 (API Endpoints)

| 方法 | 端點路徑 | 說明 |
| :--- | :--- | :--- |
| `GET` | `/health` | 檢查雙 GPU、遠端 vLLM 連線狀態、佇列深度與清理政策 |
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
