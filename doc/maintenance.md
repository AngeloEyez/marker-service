# Marker 文件轉換微服務 — 技術維護手冊 (Maintenance Guide)

本手冊供系統管理員與維運人員參考，涵蓋 NVIDIA 驅動維護、Docker 容器生命週期管理、Marker 版本更新與常見疑難排解。

---

## 一、 NVIDIA 顯卡驅動與主機核心管理

### 1.1 核心與驅動生效機制
* **目前狀態診斷**：
  * 主機目前已安裝官方專用驅動 `nvidia-driver 550.163.01` 及 `nvidia-kernel-dkms`。
  * DKMS 模組係針對 Linux 核心 `6.12.111+deb13-amd64` 進行編譯。
  * 若主機目前運行於舊核心（如 `6.12.74`），開機預設會載入開源之 `nouveau` 驅動，造成 `nvidia-smi` 報錯。
* **重開機生效指令**：
  ```bash
  sudo systemctl reboot
  ```
* **重啟後驗證程序**：
  1. 檢查當前核心版本：
     ```bash
     uname -r
     # 預期輸出包含：6.12.111+deb13-amd64
     ```
  2. 檢查 NVIDIA 驅動狀態：
     ```bash
     nvidia-smi
     ```
     預期應能正確列出兩張 GPU：
     * `GPU 0`：GeForce RTX 3050 OEM (8GB)
     * `GPU 1`：T1000 8GB
  3. 驗證 Docker GPU 穿透功能：
     ```bash
     docker run --rm --gpus all nvidia/cuda:12.4.1-base-ubuntu22.04 nvidia-smi
     ```

---

## 二、 容器生命週期管理 (Docker Compose)

所有操作均於專案目錄 `/home/gaven/marker-service` 下執行：

### 2.1 常用管理指令
* **啟動所有微服務 (背景執行)**：
  ```bash
  docker compose up -d
  ```
* **停止微服務**：
  ```bash
  docker compose down
  ```
* **重啟特定服務**：
  ```bash
  docker compose restart marker-service
  docker compose restart llama-server
  ```
* **查看即時運行日誌**：
  ```bash
  # 查看所有服務日誌
  docker compose logs -f --tail=100

  # 僅查看 Marker API 日誌
  docker compose logs -f marker-service

  # 僅查看 llama-server 推論日誌
  docker compose logs -f llama-server
  ```
* **檢查容器運行狀態**：
  ```bash
  docker compose ps
  ```

---

## 三、 Marker 版本平滑升級 (Updating to Latest Version)

### 3.1 一鍵自動升級腳本
專案內附自動化升級腳本 `scripts/update.sh`，會自動無快取重新建置映像檔以拉取 PyPI 上最新的 `marker-pdf[full]` 套件，並平滑重啟服務：

```bash
cd /home/gaven/marker-service
./scripts/update.sh
```

### 3.2 手動升級方式
若欲手動控制建置流程：
```bash
# 1. 無快取重新編譯映像檔 (拉取最新 marker-pdf)
docker compose build --no-cache marker-service

# 2. 強制以新映像檔重啟容器
docker compose up -d --force-recreate marker-service

# 3. 測試健康檢查確認正常
curl -s http://127.0.0.1:8090/health | jq .
```

---

## 四、 系統資源監控與效能微調

### 4.1 GPU 即時負載監控
可透過 `watch` 即時觀察兩張顯卡的視訊記憶體（VRAM）與計算核心佔用率：
```bash
watch -n 1 nvidia-smi
```

### 4.2 容器資源統計
觀察 Docker 容器的 CPU、RAM 與網路 I/O：
```bash
docker stats marker-api marker-llama-server
```

### 4.3 並行度與快取生命週期調優 (Concurrency & Retention Tuning)
可於 `.env` 中調整以下參數以配合生產環境之負載需求：
* `MAX_CONCURRENT_CONVERSIONS=1`：**強烈建議維持 1**。限制同時間執行的轉換任務數，保護 `pypdfium2` 不發生 C++ 執行緒記憶體競爭崩潰，並保障 T1000 8GB 顯存不發生 CUDA OOM。超出之請求會自動無損進入 FIFO 佇列等待。
* `MAX_COMPLETED_JOBS_HISTORY=500`：記憶體中保留的已結束歷史任務數量上限（排隊中與執行中任務不受此限，永不淘汰）。
* `JOB_RETENTION_SECONDS=86400`：已完成或失敗任務在記憶體中的保留時效（秒，預設 86400 = 24 小時 / 1 天），過期自動被垃圾回收工淘汰。
* `FILE_RETENTION_SECONDS=86400`：非活躍之孤立殘留檔案的安全保留時效（秒，預設 86400 = 24 小時 / 1 天）。（排隊中與處理中任務自動列入保護白名單，無論排隊多久絕不受影響）。
* `CLEANUP_INTERVAL_SECONDS=86400`：背景垃圾回收常駐工巡檢週期（秒，預設每 86400 秒 = 24 小時 / 一天執行一次）。
* `SURYA_INFERENCE_PARALLEL=8`：控制 llama-server 支援的並行推論通道（預設為 8，可依顯存佔用提升至 12 或 16）。
* `MAX_UPLOAD_SIZE_MB=100`：限制單次上傳檔案之大小上限。

---

## 五、 常見疑難排解 (Troubleshooting & FAQ)

### Q1: `nvidia-smi` 報錯：`NVIDIA-SMI has failed because it couldn't communicate with the NVIDIA driver`
* **原因**：系統目前開機在舊核心 `6.12.74`，導致載入了 `nouveau` 驅動，而官方驅動模組已編譯在 `6.12.111` 核心中。
* **解法**：執行 `sudo reboot`，系統開機選單已配置為預設進入 `6.12.111`，重啟後即可自動恢復正常。

### Q2: 遠端 vLLM (`http://192.168.1.5:8000/v1`) 連線失敗或逾時
* **檢查**：
  1. 主機端執行 `curl -m 5 http://192.168.1.5:8000/v1/models` 確認區域網路與通訊埠暢通。
  2. 確認該伺服器之防火牆已開放 Port 8000。
  3. 若遠端伺服器更換 IP 或模型，可於 `.env` 中修正 `OPENAI_BASE_URL` 與 `OPENAI_MODEL` 後執行 `docker compose up -d` 重新生效。

### Q3: 磁碟空間告警
* **原因**：虛擬機器根目錄 `/` 剩餘約 18GB，若未依規劃掛載，快取可能填滿根目錄。
* **預防與修復**：
  * 專案內已將模型快取導向至專屬掛載分區 `/var/lib/docker`（位於 `/dev/sdb`，有 84GB 空間）。
  * 上傳檔案在轉換完成當下皆會在 `finally` 中立即自動刪除；另有背景定期垃圾回收線程（每 5 分鐘）清除 30 分鐘以上的孤立暫存檔。
  * 若需清理 Docker 舊映像檔與臨時快取：
    ```bash
    docker image prune -a -f
    docker builder prune -f
    ```

### Q4: 能否將 `MAX_CONCURRENT_CONVERSIONS` 增加至 2 以上？
* **技術分析**：
  1. **`libpdfium.so` 執行緒安全缺陷**：Marker 依賴的 `pypdfium2` 在同一個 Python 行程的多執行緒中並行渲染時，會觸發 Google PDFium 底層 C++ 競爭，導致 `general protection fault in libpdfium.so` (Segfault) 使整個 Docker 容器重啟崩潰。
  2. **T1000 8GB 顯存容量上限**：Marker 本地 PyTorch 輔助模型在轉換大檔案時顯存峰值約 2.5GB，雙任務並行即逼近 5GB~6GB，極具 CUDA OOM 風險。
* **建議架構**：
  若業務未來需要處理更高吞吐量的並行需求，建議採「**水平擴展多個獨立容器行程**」（如啟動 `marker-api-1`、`marker-api-2` 各綁定獨立進程，前置 Nginx 進行負載平衡），而**不應在同一容器內部盲目調高多執行緒並發**。
