#!/usr/bin/env bash
# Fail if any ELF file in <dir> needs a newer glibc than the final-stage image
# of <dockerfile> ships. The runtime version is read by running `ldd --version`
# in that image, so the floor moves with the Dockerfile instead of a constant.
#
#   scripts/check_glibc_floor.sh backend/Dockerfile.prebuilt dist/
#
# Needs docker, binutils (readelf, objdump) and GNU sort.
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <dockerfile> <dir>" >&2
  exit 2
fi
dockerfile=$1
dir=$2

# Last FROM wins: it is the stage the image ships. Skips `--platform=...`.
image=$(awk 'toupper($1) == "FROM" { i = 2; while ($i ~ /^--/) i++; img = $i } END { print img }' "$dockerfile")
if [ -z "$image" ] || [[ "$image" == *'$'* ]]; then
  echo "::error::cannot read a literal runtime image from the last FROM in $dockerfile (got '$image')"
  exit 1
fi

ldd_line=$(docker run --rm --platform linux/amd64 --entrypoint ldd "$image" --version | sed -n 1p)
runtime=${ldd_line##* }
if ! [[ "$runtime" =~ ^[0-9]+\.[0-9]+$ ]]; then
  echo "::error::could not parse a glibc version from '$ldd_line' (ldd --version in $image)"
  exit 1
fi
echo "runtime image $image ships glibc $runtime ($ldd_line)"

checked=0
failed=0
for f in "$dir"/*; do
  [ -f "$f" ] || continue
  readelf -h "$f" >/dev/null 2>&1 || continue
  checked=$((checked + 1))
  need=$(objdump -T "$f" | grep -o 'GLIBC_[0-9][0-9.]*' | sed 's/^GLIBC_//' | sort -Vu | tail -n 1 || true)
  if [ -z "$need" ]; then
    echo "  $(basename "$f"): no versioned glibc symbols"
    continue
  fi
  echo "  $(basename "$f"): needs GLIBC_$need"
  if [ "$(printf '%s\n%s\n' "$need" "$runtime" | sort -V | tail -n 1)" != "$runtime" ]; then
    echo "::error::$(basename "$f") needs GLIBC_$need but $image ships glibc $runtime; it would fail to load in production"
    failed=$((failed + 1))
  fi
done

if [ "$checked" -eq 0 ]; then
  echo "::error::no ELF files found in $dir; nothing was checked"
  exit 1
fi
if [ "$failed" -gt 0 ]; then
  echo "$failed of $checked binaries need a newer glibc than $image"
  exit 1
fi
echo "all $checked binaries load on glibc $runtime"
