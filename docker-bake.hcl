variable "REGISTRY" {
  default = ""  # required — set via env: REGISTRY=ghcr.io/myorg
}

variable "IMAGE" {
  default = "oci-shipper"
}

variable "TAG" {
  default = "dev"
}

# ── targets ──────────────────────────────────────────────────────────────────

# Default: quick local build for the host platform only, loaded into docker daemon.
group "default" {
  targets = ["dev"]
}

target "dev" {
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64"]
  tags       = ["${IMAGE}:dev"]
  output     = ["type=docker"]
}

# Multi-arch build pushed to registry.  Set REGISTRY / TAG via env vars.
#   REGISTRY=ghcr.io/myorg TAG=v1.2.3 docker buildx bake release
target "release" {
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]
  tags       = ["${REGISTRY}/${IMAGE}:${TAG}", "${REGISTRY}/${IMAGE}:latest"]
  output     = ["type=registry"]
}

# Same as release but wires up registry cache — intended for CI pipelines.
# The cache layer is stored alongside the image so cold builds warm up quickly.
target "ci" {
  inherits   = ["release"]
  cache-from = ["type=registry,ref=${REGISTRY}/${IMAGE}:cache"]
  cache-to   = ["type=registry,ref=${REGISTRY}/${IMAGE}:cache,mode=max"]
}
