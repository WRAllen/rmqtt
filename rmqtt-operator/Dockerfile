ARG RMQTT_AMD64_BUILD_BASE_IMAGE=rmqtt-build-base:rust-1.89-alpine-amd64-v1
FROM ${RMQTT_AMD64_BUILD_BASE_IMAGE} AS builder

ARG RMQTT_OPERATOR_BUILD_ID
ENV PATH="/usr/local/cargo/bin:${PATH}"

WORKDIR /src
COPY . .
RUN unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy \
    && test -n "${RMQTT_OPERATOR_BUILD_ID}" \
    && RMQTT_OPERATOR_BUILD_ID="${RMQTT_OPERATOR_BUILD_ID}" cargo build --release -p rmqtt-operator

FROM alpine:3.24.1

ARG RMQTT_OPERATOR_BUILD_ID
LABEL org.opencontainers.image.version="${RMQTT_OPERATOR_BUILD_ID}"

RUN adduser -D -u 10001 rmqtt-operator
COPY --from=builder /src/target/release/rmqtt-operator /usr/local/bin/rmqtt-operator
RUN test -n "${RMQTT_OPERATOR_BUILD_ID}" \
    && mkdir -p /usr/local/share/rmqtt-operator \
    && printf '%s\n' "${RMQTT_OPERATOR_BUILD_ID}" > /usr/local/share/rmqtt-operator/BUILD_ID

USER 10001
ENTRYPOINT ["/usr/local/bin/rmqtt-operator"]
