FROM golang:tip-alpine3.23 as builder
ARG APP_NAME
RUN apk update && apk add curl make git gcc cmake g++ ca-certificates
RUN mkdir -p /go/src/github.com/beaujr/${APP_NAME}

ENV GOPATH=/go

WORKDIR /go/src/github.com/beaujr/${APP_NAME}

COPY . .
RUN go mod vendor
RUN make build APP_NAME=${APP_NAME} GOOS=${TARGETOS} GOARCH=${TARGETARCH}
RUN ls -latr bin/

FROM scratch

ARG APP_NAME
WORKDIR /
COPY --from=builder /go/src/github.com/beaujr/${APP_NAME}/bin/${APP_NAME} app
COPY --from=builder /etc/ssl/certs/ /etc/ssl/certs/
ENTRYPOINT ["./app"]
ARG VCS_REF
LABEL org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-url="https://github.com/beaujr/${APP_NAME}" \
      org.label-schema.license="Apache-2.0"