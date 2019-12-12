FROM golang:1.13.5-alpine3.10

WORKDIR /go/src/esmini

COPY . /go/src/esmini

ENV GO111MODULE=on

RUN apk add --no-cache \
      alpine-sdk \
      git

CMD ["go", "mod", "download"]
