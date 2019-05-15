FROM golang:1.12-alpine

RUN mkdir /app
ADD main.go /app/main.go
WORKDIR /app
RUN go build main.go
RUN rm main.go
ENTRYPOINT ["main"]