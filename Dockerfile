FROM alpine as builder
FROM scratch
ARG TARGETARCH
ARG TARGETOS
ARG APP_NAME
WORKDIR /
COPY --from=builder /etc/ssl/certs/ /etc/ssl/certs/
COPY ./dist/${APP_NAME}_${TARGETOS}_${TARGETARCH}/app app
ENTRYPOINT ["./app"]
ARG VCS_REF
LABEL org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-url="https://github.com/beaujr/${APP_NAME}" \
      org.label-schema.license="Apache-2.0"