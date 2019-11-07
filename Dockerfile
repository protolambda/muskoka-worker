FROM golang:1.13.0-alpine3.10

WORKDIR /app

COPY . .

RUN go build -o muskoka_worker -v -i .

