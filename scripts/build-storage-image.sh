#!/usr/bin/env bash
# build-storage-image.sh — build the storage-layer Docker image
# (integration/docker/Dockerfile.storage).
#
# The image packages three things: the static Go stratum binary, the C++
# vecstore_server, and every shared library vecstore_server links against.
#
# The library set is collected from this host with ldd rather than written down
# by hand. A hand-written list looks tidier and is wrong the first time the
# vecstore picks up a transitive dependency — the failure mode is a container
# that starts on the developer's machine and dies on the next one with
# "cannot open shared object file".
#
# The C++ binary is not rebuilt here: this environment cannot pull a toolchain
# image, so it is expected to have been built already (see
# vecstore/CMakeLists.txt).
#
# Usage: scripts/build-storage-image.sh [image-tag]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIBS="$ROOT/run/docker/vecstore-libs"
IMAGE="${1:-stratum-storage:latest}"
VECSTORE="$ROOT/run/bin/vecstore_server"

if [[ ! -x "$VECSTORE" ]]; then
  echo "error: $VECSTORE is missing or not executable." >&2
  echo "       Build the C++ side first (cmake --build … --target vecstore_server)," >&2
  echo "       or run scripts/docker-cluster.sh build, which does it for the all-in-one image." >&2
  exit 1
fi

if [[ ! -x "$ROOT/integration/docker/stratum" ]]; then
  echo "error: $ROOT/integration/docker/stratum is missing." >&2
  echo "       Run: CGO_ENABLED=0 go build -o integration/docker/stratum ./cmd/stratum/" >&2
  exit 1
fi

echo "==> collecting vecstore_server's shared libraries into $LIBS"
rm -rf "$LIBS"
mkdir -p "$LIBS"
ldd "$VECSTORE" | awk '{print $3}' | grep '^/' | sort -u | while read -r lib; do
  cp -L "$lib" "$LIBS/"
done
# The loader travels with the libraries: the host's glibc is newer than the base
# image's, so the binary must run under the host's loader with the host's libc
# beside it (see Dockerfile.storage).
cp -L /lib64/ld-linux-x86-64.so.2 "$LIBS/"
echo "    $(ls -1 "$LIBS" | wc -l) libraries, $(du -sh "$LIBS" | cut -f1)"

echo "==> building $IMAGE"
docker build -t "$IMAGE" -f "$ROOT/integration/docker/Dockerfile.storage" "$ROOT"
echo "==> done: $IMAGE"
