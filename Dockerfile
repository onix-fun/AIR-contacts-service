FROM alpine:3.21

WORKDIR /app

RUN apk --no-cache add ca-certificates

COPY build/docker/device-ws /app/device-ws

EXPOSE 8080

CMD ["/app/device-ws"]
