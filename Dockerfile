FROM golang:1.25-alpine

RUN apk add git

WORKDIR /usr/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -v -o /usr/local/bin/metricq-source-capella ./cmd/metricq-source-capella

ENTRYPOINT [ "/usr/local/bin/metricq-source-capella" ]
