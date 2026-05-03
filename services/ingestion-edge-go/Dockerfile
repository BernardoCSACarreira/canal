# syntax=docker/dockerfile:1
FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY . ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ingestion-edge ./cmd/ingestion-edge

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ingestion-edge /ingestion-edge
EXPOSE 8080
ENV PORT=8080
USER nonroot:nonroot
ENTRYPOINT ["/ingestion-edge"]
