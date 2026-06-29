# syntax=docker/dockerfile:1
#
# Minimal scratch image for the yp binary (M16). Multi-arch via buildx
# (linux/amd64, linux/arm64); the version is injected at link time.

FROM golang:1.26.4 AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src

RUN apt-get update && apt-get install ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/yp ./cmd/yp

# scratch: nothing but the static binary. CA certificates are copied from the
# builder for outbound TLS (mTLS to seeds, HTTPS health checks); scratch has no
# /etc/passwd, so run as a numeric UID (the distroless "nonroot" 65532) — no user
# name to resolve.
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/yp /usr/local/bin/yp
EXPOSE 9900 8500 8600 9901
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/yp"]
