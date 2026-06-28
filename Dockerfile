# syntax=docker/dockerfile:1
#
# Minimal distroless image for the yp binary (M16). Multi-arch via buildx
# (linux/amd64, linux/arm64); the version is injected at link time.

FROM --platform=$BUILDPLATFORM golang:1.26.4 AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/yp ./cmd/yp

# Distroless static, non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/yp /usr/local/bin/yp
EXPOSE 9900 8500 8600 9901
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/yp"]
