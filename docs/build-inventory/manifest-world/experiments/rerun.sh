#!/bin/bash
# The whole manifest-world pipeline, in order (homework scratch). Usage: rerun.sh <repo> <homework/world dir>
set -euo pipefail
B=$1; H=$2; ET=$(cat $H/et); PT=$(cat $H/pt); P=$H/probe
JDK=${JDK:?set JDK to a JDK 25 home (for example the remotejdk25 in Bazel's output base)}
# the module graph and the declaration index first (the scripts one level up): repos.json, files.tsv, decl_repo.json
python3 $H/../graph.py "$PT" "$ET" "$H"
python3 $H/../index.py "$H" "$B" > $H/index.txt 2>&1
python3 $H/enginepat.py "$H" "$B" > $H/enginepat.txt 2>&1
python3 $H/englist.py "$H" "$B" "$ET" > /dev/null
python3 $H/enggroups.py "$H" > $H/enggroups.txt
python3 $H/coverage.py "$H" "$B" > $H/coverage.txt 2>&1
python3 $H/e1.py "$H" "$B" > $H/e1.txt 2>&1
python3 $H/modworld.py "$H" "$B" > $H/modworld.txt 2>&1
python3 - "$H" "$B" <<'PY'
import sys
H, B = sys.argv[1:3]
src = open(f'{H}/modworld.py').read(); defs = src.split("core_fns = [r for r")[0]
g = {'__name__': 'x'}; sys.argv = [sys.argv[0], H, B]; exec(compile(defs, 'modworld', 'exec'), g)
repos = g['repos']
roots = [r for r in repos if r.startswith('platform') or r.startswith('core_functions')] + ['core', 'core_relational', 'core_service', 'core_external_language_java_compiler', 'core_external_compiler', 'core_external_store_relational_postgres_sql_parser', 'core_external_store_relational_sdt', 'pure_ide_debug']
g['world']('UNI', roots, write=True)
PY
$JDK/bin/java -Xmx12g -cp "$P/classes:$(cat $P/cp2)" ClosureProbe $H/synth/UNI.files $H/uni_edges.tsv 2>&1 | grep -v "^WARN\|^SLF4J" | tail -1
python3 $H/closure.py "$H" "$B" > $H/closure.txt 2>&1
python3 $H/bootdemand.py "$H" "$B" > $H/bootdemand.txt 2>&1
