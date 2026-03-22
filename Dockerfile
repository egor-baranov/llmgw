FROM golang:1.24.4-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/llmgw ./cmd/llmgw

FROM alpine:3.21

RUN apk add --no-cache ca-certificates wget

WORKDIR /app

RUN mkdir -p /etc/llmgw

COPY --from=build /out/llmgw /usr/local/bin/llmgw
COPY config/config.example.yaml /etc/llmgw/config.yaml

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/llmgw"]
CMD ["-config", "/etc/llmgw/config.yaml"]
