#!/usr/bin/env bash
# The spike's processes. State (the venv, Maven's repository, the build, logs, pid files) lives in
# $SPIKE_STATE, outside the source tree.
#
#   SPIKE_STATE=/some/dir ./run.sh setup      # venv with deephaven-server 42.7 + pydeephaven; Maven 3.9.11
#   SPIKE_STATE=/some/dir ./run.sh build      # the connector
#   SPIKE_STATE=/some/dir ./run.sh server     # Deephaven on :10000 with the simulated feed
#   SPIKE_STATE=/some/dir ./run.sh connector  # the connector and the page on :8090
#   SPIKE_STATE=/some/dir ./run.sh stop
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE="${SPIKE_STATE:?set SPIKE_STATE to a directory outside the source tree}"
mkdir -p "$STATE"
MVN="$STATE/apache-maven-3.9.11/bin/mvn"
PY="$STATE/venv/bin/python"
CP="$STATE/connector-build/deephaven-spike-connector-0.1.jar:$STATE/connector-build/lib/*"

wait_for() {  # wait_for <log> <text>
  for _ in $(seq 1 90); do
    grep -q "$2" "$1" 2>/dev/null && return 0
    sleep 1
  done
  echo "timed out waiting for '$2' in $1" >&2
  tail -20 "$1" >&2
  return 1
}

stop_one() {  # stop_one <name>
  local pidfile="$STATE/$1.pid"
  if [[ -f "$pidfile" ]]; then
    kill "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile"
  fi
}

case "${1:-}" in
  setup)
    python3 -m venv "$STATE/venv"
    "$STATE/venv/bin/pip" install -q "deephaven-server==42.7" "pydeephaven==42.7"
    if [[ ! -x "$MVN" ]]; then
      curl -sSfL -o "$STATE/mvn.tgz" \
        https://repo1.maven.org/maven2/org/apache/maven/apache-maven/3.9.11/apache-maven-3.9.11-bin.tar.gz
      tar xzf "$STATE/mvn.tgz" -C "$STATE"
    fi
    ;;
  build)
    "$MVN" -q -B -f "$HERE/connector/pom.xml" -Dmaven.repo.local="$STATE/m2" \
      -Dspike.build="$STATE/connector-build" package
    ;;
  server)
    stop_one server
    nohup "$PY" -u "$HERE/server/start_server.py" "${@:2}" > "$STATE/server.log" 2>&1 &
    echo $! > "$STATE/server.pid"
    wait_for "$STATE/server.log" "deephaven-spike: server"
    ;;
  connector)
    stop_one connector
    nohup java -cp "$CP" com.legend.experiments.deephaven.SpikeServer \
      --plans "$HERE/plans" --web "$HERE/web" "${@:2}" > "$STATE/connector.log" 2>&1 &
    echo $! > "$STATE/connector.pid"
    wait_for "$STATE/connector.log" "deephaven-spike connector"
    ;;
  stop)
    stop_one connector
    stop_one server
    ;;
  *)
    sed -n '2,10p' "$0"
    exit 2
    ;;
esac
