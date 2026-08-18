# Stratum Docker image — single stage, copies a pre-built static binary.
#
# The original Dockerfile did a multi-stage `golang:1.24-alpine` build, but
# this environment cannot reach Docker Hub / registry mirrors to pull that
# image. Instead the binary is built on the host with the local Go toolchain:
#
#   CGO_ENABLED=0 go build -o integration/docker/stratum ./cmd/stratum/
#
# and this Dockerfile just packages it on top of the locally-available
# alpine base image.
FROM alpine:latest
COPY integration/docker/stratum /usr/local/bin/stratum
EXPOSE 7000 8000 9000
ENTRYPOINT ["stratum", "-config", "/etc/stratum/config.yaml"]
