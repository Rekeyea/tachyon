#!/usr/bin/env bash
# Binario + imagen del motor Tachyon para el bench.
#
#   bench/build-engine.sh
#
# - Compila el binario con el perfil `benchfast` (opt-level=3, LTO thin).
# - Lo deja en el volumen `tachyon-target` (/work/target en el contenedor).
# - Construye la imagen `tachyon-build`: filesystem del host (el binario
#   está linkado contra la glibc del host) + nsswitch.conf mínimo.
set -euo pipefail
cd "$(dirname "$0")/.."   # raíz del workspace

echo ">> cargo build --profile benchfast"
cargo build --profile benchfast

echo ">> volumen tachyon-target"
docker volume create tachyon-target
docker run --rm -v "$PWD/target/benchfast":/src:ro -v tachyon-target:/dst alpine \
  sh -c 'mkdir -p /dst/benchfast && cp /src/tachyon /dst/benchfast/'

echo ">> imagen tachyon-build (hostfs + nsswitch)"
TARBALL=$(mktemp /tmp/tachyon-hostfs.XXXXXX.tar)
trap 'rm -f "$TARBALL"' EXIT
tar -cf "$TARBALL" -C / lib64 lib usr/lib etc dev bin usr/bin 2>/dev/null || true
docker import "$TARBALL" tachyon-hostfs
docker build -q -t tachyon-build bench/engine

echo ">> listo: $("$PWD/target/benchfast/tachyon" --version 2>/dev/null || echo binario ok)"
