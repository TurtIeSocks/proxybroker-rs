# A `FROM scratch` image: the entire crate — the geo database (include_bytes!) and providers.yaml
# (include_str!) — is embedded, and TLS is ring-only rustls, so the static musl binary has zero
# runtime file or libc dependencies. The final image is just that binary plus the licences.

# Stage 1 — build the fully static binary. rust:alpine targets musl natively, so a plain release
# build is already static; build-base supplies the C toolchain ring needs.
FROM rust:alpine AS build
RUN apk add --no-cache build-base
WORKDIR /src
COPY . .
RUN cargo build --release --locked

# Stage 2 — scratch. Carry the CC BY 4.0 attribution (LICENSE-DATA + NOTICE) that must travel with
# the embedded DB-IP geo data.
FROM scratch
COPY --from=build /src/target/release/proxybroker /proxybroker
COPY LICENSE LICENSE-DATA NOTICE /
# Run as a non-root UID. There is no /etc/passwd in a scratch image, so this has to be numeric —
# the binary needs no user lookup, no home, and no writable path. 65532 is the conventional
# "nonroot" UID (distroless). The listener defaults to :8888, above the privileged range, so
# dropping root costs nothing. A bind mount for --store sqlite:// must be writable by this UID.
USER 65532:65532
ENTRYPOINT ["/proxybroker"]
