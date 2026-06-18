FROM golang:1.22-alpine AS builder

# Injected by docker buildx --platform; defaults to host arch for plain docker build.
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build \
      -buildvcs=false \
      -ldflags="-s -w" \
      -o oci-shipper .

# distroless/static includes CA certs (needed for OCI HTTPS) and runs as non-root.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /app/oci-shipper /oci-shipper

ENTRYPOINT ["/oci-shipper"]
