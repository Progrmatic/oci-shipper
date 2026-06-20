[English](README.md) · [繁體中文](README.zh-tw.md)

# oci-shipper

A lightweight log shipper that reads log lines from stdin (or a named pipe) and forwards them to [OCI Logging](https://docs.oracle.com/en-us/iaas/Content/Logging/Concepts/loggingoverview.htm) via the Logging Ingestion API.

## Container image

```bash
# latest stable
docker pull ghcr.io/progrmatic/oci-shipper:latest

# specific version
docker pull ghcr.io/progrmatic/oci-shipper:v0.1.0
```

Multi-arch: supports `linux/amd64` and `linux/arm64` — Docker selects the right variant automatically.

> **Note:** The [GHCR package page](https://github.com/Progrmatic/oci-shipper/pkgs/container/oci-shipper) also lists supply-chain security artifacts (`.sig` for the Cosign signature, `.att` for the GitHub provenance attestation). Ignore those — use the commands above.

## Why not Fluent Bit?

Fluent Bit's OCI output plugin targets **OCI Logging Analytics** — a separate service that is not accessible from Grafana via the standard OCI Logging integration. This shipper targets **OCI Logging** (the `loggingingestion` API) directly, so logs appear in Grafana without any additional pipeline.

## Input modes

### 1. Pipe / redirect (one-shot)

```bash
tail -f /var/log/nginx/access.log | ./oci-shipper -log-id ocid1.log...
# or
./oci-shipper -log-id ocid1.log... < /var/log/app.log
```

### 2. Daemon mode (interactive terminal)

Launched without a pipe, the shipper creates a PID-based FIFO, opens it, then unlinks the filesystem path. The FIFO remains accessible only via `/proc/{pid}/fd/{n}`, where `{n}` is the actual file descriptor number printed on startup.

```bash
./oci-shipper -log-id ocid1.log...
# PID: 12345
# Push logs with:
#   echo 'log line' > /proc/12345/fd/3
```

### 3. Sidecar mode (fixed FIFO path)

Pass `-pipe` (or `OCI_PIPE_PATH`) to create a FIFO at a fixed path. The path persists across restarts so writers always know where to send data.

```bash
./oci-shipper -log-id ocid1.log... -pipe /var/run/oci-shipper/in.pipe
# FIFO: /var/run/oci-shipper/in.pipe
# Push logs with:
#   echo 'log line' > /var/run/oci-shipper/in.pipe
```

If the FIFO already exists on restart (e.g. K8s container restart), it is reused automatically.

## How the FIFO works

The FIFO is opened with `O_RDWR` so the shipper itself holds the write end. This prevents EOF from being sent when an external writer closes — the shipper keeps running and waiting for the next line. Writers that use short-lived writes (`echo "..." > /pipe`) will **block** if the shipper is temporarily down and resume automatically once it restarts.

The `*os.File` returned by `openFIFO` is left in Go's runtime poller. Reads park only the goroutine (not the OS thread), so the process remains responsive to flush ticks and signals while waiting for data.

## Configuration

All flags also accept an environment variable (listed in the table). Flags take precedence over environment variables.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `-log-id` | `OCI_LOG_ID` | *required* | OCI Log OCID |
| `-source` | `OCI_LOG_SOURCE` | `oci-shipper` | `source` field in the log batch |
| `-type` | `OCI_LOG_TYPE` | `com.oraclecloud.logging.custom` | Log type |
| `-subject` | `OCI_LOG_SUBJECT` | `` | Log subject |
| `-max-retries` | — | `3` | Max OCI send retries |
| `-oci-config` | `OCI_CONFIG_FILE` | `~/.oci/config` | OCI config file path |
| `-oci-profile` | `OCI_CONFIG_PROFILE` | `DEFAULT` | OCI config profile |
| `-pipe` | `OCI_PIPE_PATH` | `` | Fixed FIFO path (sidecar mode) |
| `-health-port` | — | `8080` | HTTP health check port |
| `-health-threshold` | — | `30s` | Time since last successful send before `/health` returns 503 |

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Clean exit — EOF on the reader or SIGTERM / SIGINT received |
| `1` | Unexpected read error (e.g. `bad file descriptor`) — the FIFO became invalid |

Kubernetes uses the exit code to decide restart behaviour. Exit `1` triggers **CrashLoopBackOff** (exponential back-off), preventing a tight restart loop when the FIFO is persistently broken. Exit `0` restarts immediately with no back-off, so it is reserved for intentional shutdowns only.

## Batching

Lines are buffered and flushed to OCI when either condition is met first:

- **100 lines** accumulated
- **5 seconds** elapsed since the last flush

On SIGTERM / SIGINT or stdin EOF, the remaining buffer is flushed before exit.

## Retry

Failed sends are retried with exponential backoff (2 s, 4 s, …) up to `-max-retries`.

If all attempts fail (e.g. OCI is temporarily unreachable), the batch is placed into an **in-memory retry queue** rather than dropped. On each subsequent flush the shipper first tries to drain the retry queue before sending new lines. Draining stops at the first failure so a still-unavailable OCI endpoint is not hammered on every cycle.

The retry queue holds up to 100 batches (~10 000 lines). When the queue is full and a new batch fails, the oldest batch is evicted to make room.

Data is only lost if the shipper process itself is killed while batches are still queued.

## Health check

A minimal HTTP server runs on `-health-port` (default `8080`):

- `GET /health` → `200 ok` while healthy
- `GET /health` → `503 stale: no successful send within threshold` when the last successful OCI send is older than `-health-threshold`

A startup grace period equal to `-health-threshold` is applied, so the probe does not trip during initial startup before any batch has been sent.

### What the threshold actually checks

The threshold reflects whether the **end-to-end pipeline is working** — logs arriving at the FIFO and being accepted by OCI. In practice, if the app (or a sidecar) generates logs at a regular cadence (e.g. from its own health check traffic), the threshold is refreshed naturally and never trips.

The threshold becomes stale in two distinct situations:

| Situation | Restart helps? |
|-----------|---------------|
| Shipper is stuck / has a bug | Yes |
| OCI Logging endpoint is temporarily down | No — causes a crash loop |

To avoid an unnecessary crash loop during a transient OCI outage, set `failureThreshold` high enough to cover the expected recovery time rather than leaving it at `1`:

```yaml
livenessProbe:
  periodSeconds: 5
  failureThreshold: 6   # tolerate up to 30 s of OCI unavailability before restarting
```

## Docker Compose sidecar

The following example ships nginx access logs to OCI Logging. The two
containers share a named volume so they can exchange data through the FIFO.
`oci-shipper` must start first so the pipe exists before `app` tries to write to it.

```yaml
services:
  app:
    image: nginx:latest
    volumes:
      - oci-pipe:/var/run/oci-shipper
    ports:
      - "80:80"
    # The official nginx image routes access logs to stdout.
    # tee duplicates that stream into the pipe for oci-shipper to pick up.
    command: ["/bin/sh", "-c", "nginx -g 'daemon off;' | tee /var/run/oci-shipper/in.pipe"]
    depends_on:
      - oci-shipper

  oci-shipper:
    image: ghcr.io/progrmatic/oci-shipper:latest
    environment:
      - OCI_LOG_ID=ocid1.log.oc1.<region>.<unique_id>
      - OCI_PIPE_PATH=/var/run/oci-shipper/in.pipe
      - OCI_CONFIG_FILE=/app/.oci/config
    volumes:
      - .oci:/app/.oci:ro          # OCI SDK config + key file
      - oci-pipe:/var/run/oci-shipper

volumes:
  oci-pipe:
```

The `.oci/config` `key_file` must reference the path **inside the container**:

```ini
[DEFAULT]
user=ocid1.user.oc1...<unique_id>
fingerprint=xx:xx:xx:...
tenancy=ocid1.tenancy.oc1...<unique_id>
region=ap-tokyo-1
key_file=/app/.oci/oci_api_key.pem
```

## Kubernetes sidecar

```yaml
volumes:
  - name: log-pipe
    emptyDir: {}

containers:
  - name: app
    volumeMounts:
      - mountPath: /var/run/oci-shipper
        name: log-pipe
    # write logs with: echo "..." > /var/run/oci-shipper/in.pipe

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

The app container writes logs to the shared `emptyDir` volume. If the shipper restarts, short-lived writers block at `open()` and resume automatically; the liveness probe bounds the restart window to roughly 5–10 seconds.

## Development

Requires Go 1.22+ and Docker with Buildx.

```bash
make build    # compile binary → ./oci-shipper
make test     # go test
make lint     # go vet
make clean    # remove binary
make docker   # build local dev image (host arch, loaded into docker daemon)
```

### Dev container

A `.devcontainer/` configuration is included. Open the project with the VS Code **Dev Containers** extension — it starts a `golang:1.22` container with the workspace pre-mounted. All `make` targets work out of the box.

## Release

Versioning follows [Semantic Versioning](https://semver.org/). The image tag is derived automatically from the current git tag.

```bash
git tag v1.2.3
REGISTRY=ghcr.io/myorg make release   # builds amd64 + arm64 and pushes
```

`make release` enforces two guards before calling Docker:
- `REGISTRY` must be explicitly set (no default — prevents accidental pushes)
- `TAG` must match `v[0-9]+.[0-9]+.[0-9]+` (must be on an exact git tag)

Both `:<tag>` and `:latest` are pushed in a single operation.

## CI/CD (GitHub Actions)

The workflow triggers on any `v*` tag push, builds a multi-arch image, and pushes it to GitHub Container Registry. No extra registry secrets are needed — it uses the built-in `GITHUB_TOKEN`.

Two jobs run in sequence:

- **`build`** — builds `linux/amd64` and `linux/arm64` in **parallel** on **native** runners (`ubuntu-24.04` for amd64, `ubuntu-24.04-arm` for arm64) with `fail-fast: false`, so a single platform failure does not cancel the other. Each platform uses its own registry cache key (`oci-shipper:cache-amd64` / `oci-shipper:cache-arm64`) to avoid concurrent write conflicts. The image is pushed by digest only — no tag is applied yet.

- **`merge`** — downloads both digests, combines them into a single multi-arch manifest tagged `:<version>` and `:latest`, then applies two layers of supply-chain security:
  1. **GitHub provenance attestation** ([`actions/attest-build-provenance`](https://github.com/actions/attest-build-provenance)) — links the manifest to the exact source commit and workflow run; visible on the GHCR package page.
  2. **Cosign keyless signing** — signs the manifest via Sigstore OIDC; no key management required.

To release:

```bash
git tag v1.2.3
git push origin v1.2.3   # triggers the workflow
```

### Verifying the image signature

```bash
cosign verify \
  --certificate-identity-regexp "https://github.com/<owner>/oci-shipper/.*" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  ghcr.io/<owner>/oci-shipper:latest
```

## OCI config

The shipper uses the standard OCI SDK config file format. See the [OCI SDK documentation](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm) for details.

```ini
[DEFAULT]
user=ocid1.user.oc1..xxx
fingerprint=xx:xx:xx:...
tenancy=ocid1.tenancy.oc1..xxx
region=ap-tokyo-1
key_file=~/.oci/oci_api_key.pem
```
