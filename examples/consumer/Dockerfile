FROM golang:1.24.5-alpine3.22 AS builder

ENV GO111MODULE=on
ENV CGO_ENABLED=1
ENV GOOS=linux

WORKDIR /src

COPY . .

WORKDIR /src/examples

RUN apk add --no-cache ca-certificates pkgconf git bash build-base sudo librdkafka-dev \
    && go mod download \
    && go build -tags musl --ldflags "-extldflags -static" -o consumer ./consumer

FROM scratch AS runner

COPY --from=builder /src/examples/consumer /

ENTRYPOINT ["./consumer"]
