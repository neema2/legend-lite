#!/usr/bin/env bash
# The warehouse server as a GraalVM native image (docs/WAREHOUSE_W1_DESIGN_2026_09_26.md, W1e), with
# DuckDB's native library beside it, where the binary looks for it.
#
#   warehouse/tools/build-native.sh OUT_DIR    build OUT_DIR/warehouse and DuckDB's library beside it
#   warehouse/tools/build-native.sh --record   re-record the reachability metadata: the warehouse test
#                                              suite runs against the JVM server under GraalVM's agent
#
# Needs GraalVM's native-image: GRAALVM_HOME, else native-image on the PATH. Linux and macOS.
set -euo pipefail
cd "$(dirname "$0")/../.."
if [ -n "${GRAALVM_HOME:-}" ]; then bin="$GRAALVM_HOME/bin/"; else bin=""; fi

# BAZEL_FLAGS: the caller's Bazel flags (CI passes its caches)
read -r -a flags <<< "${BAZEL_FLAGS:-}"
bazel build ${flags[@]+"${flags[@]}"} //warehouse:server_lib //warehouse:sqlapi //core //warehouse:server_deploy.jar >/dev/null
jar() { bazel cquery ${flags[@]+"${flags[@]}"} --output=files "$1" 2>/dev/null | grep -v -- '-hjar\|-src\|native-header' | head -1; }
cp_="$(jar //warehouse:server_lib):$(jar //warehouse:sqlapi):$(jar //core)"
deploy="$(jar //warehouse:server_deploy.jar)"

case "$(uname -s)/$(uname -m)" in
  Darwin/*) lib=libduckdb_java.so_osx_universal ;;
  Linux/aarch64|Linux/arm64) lib=libduckdb_java.so_linux_arm64 ;;
  Linux/*) lib=libduckdb_java.so_linux_amd64 ;;
  *) echo "no native build for $(uname -s)/$(uname -m)" >&2; exit 1 ;;
esac

if [ "${1:-}" = "--record" ]; then
  work="$(mktemp -d)"
  unzip -p "$deploy" "$lib" > "$work/$lib"
  mkdir -p "$work/runs"
  cat > "$work/server.sh" <<SH
#!/bin/bash
exec ${bin}java -agentlib:native-image-agent=config-output-dir=$work/runs/run-{pid} --enable-native-access=ALL-UNNAMED -cp $PWD/$deploy com.legend.warehouse.server.WarehouseServer "\$@"
SH
  chmod +x "$work/server.sh"
  bazel test //warehouse:tests --test_output=errors --sandbox_writable_path="$work/runs" --test_env=PATH \
    --test_env=WAREHOUSE_BINARY="$work/server.sh" --test_env=WAREHOUSE_DUCKDB_LIBRARY="$work/$lib"
  inputs=()
  for d in "$work"/runs/run-*; do inputs+=("--input-dir=$d"); done
  "${bin}native-image-configure" generate "${inputs[@]}" --output-dir="$work/merged"
  cp "$work/merged/reachability-metadata.json" warehouse/src/main/resources/META-INF/native-image/com.legend/warehouse/
  echo "recorded: warehouse/src/main/resources/META-INF/native-image/com.legend/warehouse/reachability-metadata.json"
  exit 0
fi

out="${1:?usage: build-native.sh OUT_DIR | --record}"
mkdir -p "$out"
"${bin}native-image" --no-fallback --enable-native-access=ALL-UNNAMED -cp "$cp_" \
  -o "$out/warehouse" com.legend.warehouse.server.WarehouseServer
unzip -p "$deploy" "$lib" > "$out/$lib"
echo "built: $out/warehouse, with $out/$lib beside it"
