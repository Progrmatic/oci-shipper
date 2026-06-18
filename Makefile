IMAGE    ?= oci-shipper
REGISTRY ?=

# Derive TAG from the current exact git tag (e.g. v1.2.3).
# Falls back to "dev" when not on a tagged commit.
GIT_TAG  := $(shell git describe --tags --exact-match 2>/dev/null)
TAG      ?= $(if $(GIT_TAG),$(GIT_TAG),dev)

.PHONY: build test lint clean docker release ci

# ── Go ────────────────────────────────────────────────────────────────────────

build:
	go build -buildvcs=false -o oci-shipper .

test:
	go test -count=1 ./...

lint:
	go vet ./...

clean:
	rm -f oci-shipper

# ── Docker (delegates to docker-bake.hcl) ────────────────────────────────────

docker:
	docker buildx bake

release: _require-registry _require-semver
	REGISTRY=$(REGISTRY) IMAGE=$(IMAGE) TAG=$(TAG) docker buildx bake release

ci: _require-registry _require-semver
	REGISTRY=$(REGISTRY) IMAGE=$(IMAGE) TAG=$(TAG) docker buildx bake ci

# ── Guards ───────────────────────────────────────────────────────────────────

_require-registry:
	@[ -n "$(REGISTRY)" ] || (echo "error: REGISTRY is not set (e.g. REGISTRY=ghcr.io/myorg make release)"; exit 1)

_require-semver:
	@echo "$(TAG)" | grep -qE '^v[0-9]+\.[0-9]+\.[0-9]+' \
		|| (echo "error: TAG '$(TAG)' is not semver — create a git tag first (e.g. git tag v1.0.0)"; exit 1)
