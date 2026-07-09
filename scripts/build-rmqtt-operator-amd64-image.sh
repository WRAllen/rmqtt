#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "用法：$0 <完整镜像名称:tag>" >&2
  echo "默认在构建及校验通过后推送镜像；设置 PUSH_IMAGE=false 可跳过推送。" >&2
  exit 2
fi

IMAGE="$1"
PUSH_IMAGE="${PUSH_IMAGE:-true}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/scripts/rmqtt-amd64-build-base.sh"
GIT_SHA="$(git -C "${ROOT}" rev-parse --short=12 HEAD 2>/dev/null || printf 'nogit')"
GIT_STATE=""
if [[ -n "$(git -C "${ROOT}" status --porcelain --untracked-files=normal 2>/dev/null)" ]]; then
  GIT_STATE="-dirty"
fi
BUILD_ID="$(date '+%Y%m%d%H%M%S')-${GIT_SHA}${GIT_STATE}"

case "${PUSH_IMAGE}" in
  true | false) ;;
  *)
    echo "错误：PUSH_IMAGE 只能是 true 或 false，当前值：${PUSH_IMAGE}" >&2
    exit 2
    ;;
esac

require_rmqtt_amd64_build_base

echo "==> 构建 RMQTT Operator 镜像，build id=${BUILD_ID}"
docker build --platform linux/amd64 \
  --build-arg "RMQTT_AMD64_BUILD_BASE_IMAGE=${RMQTT_AMD64_BUILD_BASE_IMAGE}" \
  --build-arg "RMQTT_OPERATOR_BUILD_ID=${BUILD_ID}" \
  -f "${ROOT}/rmqtt-operator/Dockerfile" \
  -t "${IMAGE}" \
  "${ROOT}"

echo "==> 校验镜像中的 Operator binary"
IMAGE_VERSION="$(
  docker run --rm --platform linux/amd64 \
    "${IMAGE}" \
    --version
)"
echo "${IMAGE_VERSION}"
if [[ "${IMAGE_VERSION}" != *"-${BUILD_ID}" ]]; then
  echo "错误：镜像内 Operator build id 与本次构建不一致，期望 ${BUILD_ID}" >&2
  exit 1
fi

IMAGE_BUILD_ID="$(
  docker run --rm --platform linux/amd64 \
    --entrypoint cat \
    "${IMAGE}" \
    /usr/local/share/rmqtt-operator/BUILD_ID
)"
if [[ "${IMAGE_BUILD_ID}" != "${BUILD_ID}" ]]; then
  echo "错误：镜像内 BUILD_ID 文件不一致，期望 ${BUILD_ID}，实际 ${IMAGE_BUILD_ID}" >&2
  exit 1
fi

IMAGE_LABEL="$(
  docker image inspect "${IMAGE}" \
    --format '{{ index .Config.Labels "org.opencontainers.image.version" }}'
)"
if [[ "${IMAGE_LABEL}" != "${BUILD_ID}" ]]; then
  echo "错误：镜像 OCI version label 不一致，期望 ${BUILD_ID}，实际 ${IMAGE_LABEL}" >&2
  exit 1
fi

echo "==> 构建及校验完成"
echo "镜像：${IMAGE}"
echo "版本：${IMAGE_VERSION}"
if [[ "${PUSH_IMAGE}" == "true" ]]; then
  echo "==> 推送镜像 ${IMAGE}"
  docker push "${IMAGE}"
  echo "==> 镜像推送完成：${IMAGE}"
else
  echo "==> PUSH_IMAGE=false，跳过镜像推送"
  echo "手动推送命令：docker push ${IMAGE}"
fi
