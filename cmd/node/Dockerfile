FROM --platform=linux/arm64 golang:alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .
RUN GOARCH=amd64 go build -o cmd/node/main ./cmd/node/main.go

EXPOSE 8000
EXPOSE 9000

CMD ["/app/cmd/node/main"]