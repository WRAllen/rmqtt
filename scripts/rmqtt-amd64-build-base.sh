#!/usr/bin/env bash

RMQTT_AMD64_BUILD_BASE_IMAGE="${RMQTT_AMD64_BUILD_BASE_IMAGE:-rmqtt-build-base:rust-1.89-alpine-amd64-v1}"

rmqtt_amd64_build_base_exists() {
  docker image inspect "${RMQTT_AMD64_BUILD_BASE_IMAGE}" >/dev/null 2>&1
}

ensure_rmqtt_amd64_build_base() {
  if rmqtt_amd64_build_base_exists; then
    echo "==> 复用 RMQTT amd64 编译基础镜像 ${RMQTT_AMD64_BUILD_BASE_IMAGE}"
    return
  fi

  echo "==> 首次运行，构建 RMQTT amd64 编译基础镜像 ${RMQTT_AMD64_BUILD_BASE_IMAGE}"
  docker build --pull --platform linux/amd64 \
    -f "${ROOT}/scripts/Dockerfile.amd64-build-base" \
    -t "${RMQTT_AMD64_BUILD_BASE_IMAGE}" \
    "${ROOT}"
}

require_rmqtt_amd64_build_base() {
  if rmqtt_amd64_build_base_exists; then
    echo "==> 复用 RMQTT amd64 编译基础镜像 ${RMQTT_AMD64_BUILD_BASE_IMAGE}"
    return
  fi

  echo "错误：未找到 RMQTT amd64 编译基础镜像 ${RMQTT_AMD64_BUILD_BASE_IMAGE}" >&2
  echo "请先运行 RMQTT 打包脚本（首次运行会自动构建该基础镜像），例如：" >&2
  echo "  PUSH_IMAGE=false ./scripts/build-rmqtt-amd64-image.sh rmqtt:local" >&2
  exit 1
}
