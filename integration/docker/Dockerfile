# Stratum Docker build — multi-stage Go build.
FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /stratum ./cmd/stratum/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /stratum /usr/local/bin/stratum
EXPOSE 7000 8000 9000
ENTRYPOINT ["stratum", "-config", "/etc/stratum/config.yaml"]
