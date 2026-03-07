FROM --platform=$BUILDPLATFORM alpine AS tzdata
RUN apk add --no-cache tzdata

FROM alpine

LABEL org.opencontainers.image.authors="Philipp C. Heckel <phil@mail.ntfy.sh>"
LABEL org.opencontainers.image.url="https://ntfy.sh/"
LABEL org.opencontainers.image.documentation="https://docs.ntfy.sh/"
LABEL org.opencontainers.image.source="https://github.com/binwiederhier/ntfy"
LABEL org.opencontainers.image.vendor="Philipp C. Heckel"
LABEL org.opencontainers.image.licenses="Apache-2.0, GPL-2.0"
LABEL org.opencontainers.image.title="ntfy"
LABEL org.opencontainers.image.description="Send push notifications to your phone or desktop using PUT/POST"

ARG TARGETPLATFORM

# Install tzdata in a separate stage using the build platform to avoid
# needing QEMU to run "apk add" for non-native target architectures
COPY --from=tzdata /usr/share/zoneinfo /usr/share/zoneinfo

COPY $TARGETPLATFORM/ntfy /usr/bin/ntfy

EXPOSE 80/tcp
ENTRYPOINT ["ntfy"]
