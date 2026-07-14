FROM golang:1.26.5-alpine3.24@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY api ./api
COPY app ./app
COPY cmd ./cmd
COPY gateway ./gateway
COPY observer ./observer
COPY policy ./policy
COPY proxy ./proxy
COPY store ./store

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/llmgw ./cmd/llmgw

FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

# BusyBox provides the wget applet used by the Compose health check.
RUN apk add --no-cache ca-certificates

RUN addgroup -S llmgw && adduser -S -G llmgw -H llmgw

WORKDIR /app

RUN mkdir -p /etc/llmgw

COPY --from=build /out/llmgw /usr/local/bin/llmgw
COPY config/config.example.yaml /etc/llmgw/config.yaml

USER llmgw

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/llmgw"]
CMD ["-config", "/etc/llmgw/config.yaml"]
