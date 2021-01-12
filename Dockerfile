#FROM golang:alpine as builder
#
#WORKDIR /go/src/github.com/p4tin/goaws
#
#RUN apk add --update --repository https://dl-3.alpinelinux.org/alpine/edge/testing/ git
#RUN go get github.com/golang/dep/cmd/dep
#
#COPY Gopkg.lock Gopkg.toml app ./
#RUN dep ensure
#COPY . .
#
#RUN go build -o goaws_linux_amd64 app/cmd/goaws.goc
FROM alpine

EXPOSE 4100

COPY goaws_linux_amd64 /goaws
COPY app/conf/goaws.yaml /conf/
ENTRYPOINT ["/goaws"]
