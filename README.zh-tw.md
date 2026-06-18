[English](README.md) · [繁體中文](README.zh-tw.md)

# oci-shipper

一個輕量級 log shipper，從 stdin 或具名管道（named pipe）讀取 log 行，透過 Logging Ingestion API 轉送至 [OCI Logging](https://docs.oracle.com/en-us/iaas/Content/Logging/Concepts/loggingoverview.htm)。

## 為什麼不用 Fluent Bit？

Fluent Bit 的 OCI output plugin 目標是 **OCI Logging Analytics**——這是另一個服務，無法透過標準 OCI Logging 整合從 Grafana 存取。本 shipper 直接對應 **OCI Logging**（`loggingingestion` API），log 無需額外 pipeline 即可出現在 Grafana。

## 輸入模式

### 1. Pipe / 重新導向（一次性）

```bash
tail -f /var/log/nginx/access.log | ./oci-shipper -log-id ocid1.log...
# 或
./oci-shipper -log-id ocid1.log... < /var/log/app.log
```

### 2. Daemon 模式（互動式終端機）

未接管道直接啟動時，shipper 會建立以 PID 命名的 FIFO，透過 `dup2` 掛到 `fd 0` 後刪除路徑，僅保留 `/proc/{pid}/fd/0`。

```bash
./oci-shipper -log-id ocid1.log...
# PID: 12345
# 推送 log 的方式：
#   echo 'log line' > /proc/12345/fd/0
```

### 3. Sidecar 模式（固定 FIFO 路徑）

傳入 `-pipe`（或 `OCI_PIPE_PATH`）可在固定路徑建立 FIFO。路徑在重啟後持續存在，讓寫入方永遠知道目標位置。

```bash
./oci-shipper -log-id ocid1.log... -pipe /var/run/oci-shipper/in.pipe
# FIFO: /var/run/oci-shipper/in.pipe
# 推送 log 的方式：
#   echo 'log line' > /var/run/oci-shipper/in.pipe
```

若重啟時 FIFO 已存在（例如 K8s container 重啟），會自動沿用。

## FIFO 運作原理

FIFO 以 `O_RDWR` 開啟，shipper 自己持有寫入端，防止外部寫入方關閉時送出 EOF——shipper 會持續執行並等待下一行。使用短暫寫入（`echo "..." > /pipe`）的寫入方，若 shipper 暫時停止，會在 `open()` 阻塞，等 shipper 重啟後自動恢復。

## 設定

所有 flag 均可改用環境變數（詳見下表）。flag 優先於環境變數。

| Flag | 環境變數 | 預設值 | 說明 |
|------|---------|--------|------|
| `-log-id` | `OCI_LOG_ID` | *必填* | OCI Log OCID |
| `-source` | `OCI_LOG_SOURCE` | `oci-shipper` | log batch 中的 `source` 欄位 |
| `-type` | `OCI_LOG_TYPE` | `com.oraclecloud.logging.custom` | Log 類型 |
| `-subject` | `OCI_LOG_SUBJECT` | `` | Log subject |
| `-max-retries` | — | `3` | OCI 傳送最大重試次數 |
| `-oci-config` | `OCI_CONFIG_FILE` | `~/.oci/config` | OCI config 檔案路徑 |
| `-oci-profile` | `OCI_CONFIG_PROFILE` | `DEFAULT` | OCI config profile |
| `-pipe` | `OCI_PIPE_PATH` | `` | 固定 FIFO 路徑（sidecar 模式） |
| `-health-port` | — | `8080` | HTTP 健康檢查 port |
| `-health-threshold` | — | `30s` | 距上次成功傳送超過此時間後，`/health` 回傳 503 |

## 批次處理

Log 行會緩衝後批次傳送至 OCI，滿足以下任一條件即立即 flush：

- 累積 **100 行**
- 距上次 flush 超過 **5 秒**

收到 SIGTERM / SIGINT 或 stdin EOF 時，會先 flush 剩餘緩衝再退出。

## 重試機制

傳送失敗會以指數退避（2 秒、4 秒……）重試，上限為 `-max-retries`。

若所有嘗試均失敗（例如 OCI 暫時無法連線），該 batch 會進入**記憶體重試佇列**而非直接丟棄。每次 flush 時，shipper 會先嘗試清空重試佇列，再傳送新資料。遇到第一個失敗即停止清空，避免對仍然不可用的 OCI 端點密集重試。

重試佇列最多保留 100 個 batch（約 10,000 行）。佇列滿時新 batch 失敗，會淘汰最舊的 batch 以騰出空間。

僅在 shipper 程序本身被強制終止且仍有 batch 在佇列中時，才會發生資料遺失。

## 健康檢查

`-health-port`（預設 `8080`）上運行一個最小化 HTTP server：

- `GET /health` → `200 ok`（健康時）
- `GET /health` → `503 stale: no successful send within threshold`（上次成功傳送距今超過 `-health-threshold`）

啟動時有一個等同於 `-health-threshold` 的寬限期，避免在第一個 batch 傳送前就觸發探針。

### threshold 實際檢查的是什麼

threshold 反映的是**端對端 pipeline 是否正常運作**——log 抵達 FIFO 並被 OCI 接受。實務上，若應用程式（或 sidecar）以固定頻率產生 log（例如來自自身的健康檢查流量），threshold 會自然更新而不會觸發。

threshold 在兩種情況下會過期：

| 情況 | 重啟有幫助？ |
|------|------------|
| Shipper 卡住 / 有 bug | 是 |
| OCI Logging 端點暫時停機 | 否——會造成 crash loop |

為避免短暫 OCI 中斷時發生不必要的 crash loop，建議將 `failureThreshold` 設得足夠高以涵蓋預期的恢復時間，而非維持 `1`：

```yaml
livenessProbe:
  periodSeconds: 5
  failureThreshold: 6   # 最多容忍 30 秒的 OCI 不可用再重啟
```

## Kubernetes Sidecar

```yaml
volumes:
  - name: log-pipe
    emptyDir: {}

containers:
  - name: app
    volumeMounts:
      - mountPath: /var/run/oci-shipper
        name: log-pipe
    # 寫入 log：echo "..." > /var/run/oci-shipper/in.pipe

  - name: oci-shipper
    image: your-registry/oci-shipper:latest
    env:
      - name: OCI_LOG_ID
        value: "ocid1.log.oc1.ap-tokyo-1.xxx"
      - name: OCI_PIPE_PATH
        value: "/var/run/oci-shipper/in.pipe"
    volumeMounts:
      - mountPath: /var/run/oci-shipper
        name: log-pipe
    livenessProbe:
      httpGet:
        path: /health
        port: 8080
      initialDelaySeconds: 5
      periodSeconds: 5
      failureThreshold: 6
```

應用程式 container 將 log 寫入共享的 `emptyDir` volume。shipper 重啟時，短暫寫入的 writer 會在 `open()` 阻塞並自動恢復；liveness probe 將重啟視窗限制在約 5–10 秒內。

## 開發

需要 Go 1.22+ 與支援 Buildx 的 Docker。

```bash
make build    # 編譯 binary → ./oci-shipper
make test     # go test
make lint     # go vet
make clean    # 刪除 binary
make docker   # 建立本地開發 image（host arch，載入 docker daemon）
```

### Dev container

專案包含 `.devcontainer/` 設定。用 VS Code **Dev Containers** 擴充套件開啟專案，即可啟動預先掛載工作區的 `golang:1.22` container，所有 `make` target 開箱即用。

## 發版

版本號遵循 [Semantic Versioning](https://semver.org/)，image tag 由當前 git tag 自動推導。

```bash
git tag v1.2.3
REGISTRY=ghcr.io/myorg make release   # 建立 amd64 + arm64 並推送
```

`make release` 在呼叫 Docker 前會執行兩項檢查：
- `REGISTRY` 必須明確設定（無預設值——防止意外推送）
- `TAG` 必須符合 `v[0-9]+.[0-9]+.[0-9]+`（必須在某個精確的 git tag 上）

`:<tag>` 與 `:latest` 會在同一次操作中一起推送。

## CI/CD（GitHub Actions）

workflow 在任何 `v*` tag push 時觸發，建立多架構 image 並推送至 GitHub Container Registry。不需要額外的 registry secret——使用內建的 `GITHUB_TOKEN`。

兩個 job 依序執行：

- **`build`**——在**原生** runner 上**並行**建立 `linux/amd64` 與 `linux/arm64`（amd64 用 `ubuntu-24.04`，arm64 用 `ubuntu-24.04-arm`），設定 `fail-fast: false`，單一平台失敗不會取消另一個。各 platform 使用獨立的 registry cache key（`oci-shipper:cache-amd64` / `oci-shipper:cache-arm64`）以避免並發寫入衝突。Image 僅以 digest 推送，尚未加 tag。

- **`merge`**——下載兩個 digest，合併為單一多架構 manifest 並標記 `:<version>` 與 `:latest`，接著套用兩層供應鏈安全機制：
  1. **GitHub provenance attestation**（[`actions/attest-build-provenance`](https://github.com/actions/attest-build-provenance)）——將 manifest 連結至確切的 source commit 與 workflow run；可在 GHCR package 頁面查看。
  2. **Cosign keyless 簽名**——透過 Sigstore OIDC 對 manifest 簽名，無需管理金鑰。

發版方式：

```bash
git tag v1.2.3
git push origin v1.2.3   # 觸發 workflow
```

### 驗證 image 簽名

```bash
cosign verify \
  --certificate-identity-regexp "https://github.com/<owner>/oci-shipper/.*" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  ghcr.io/<owner>/oci-shipper:latest
```

## OCI 設定

Shipper 使用標準 OCI SDK config 檔案格式。詳見 [OCI SDK 文件](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm)。

```ini
[DEFAULT]
user=ocid1.user.oc1..xxx
fingerprint=xx:xx:xx:...
tenancy=ocid1.tenancy.oc1..xxx
region=ap-tokyo-1
key_file=~/.oci/oci_api_key.pem
```
