#!/bin/bash
# THE BUMP — move upstream to ONE new release, mechanically
# (docs/UPSTREAM_BOUNDARY_PROGRAM.md §5 "a bump is one line"; the phases are
# docs/UPSTREAM_BOUNDARY_HOMEWORK_2026_09_10.md §5, phases 0–2, which batch 1
# ran by hand; this script is batch 8's deliverable).
#
#   tools/bump.sh <engine release>          e.g. tools/bump.sh 4.145.0
#   tools/bump.sh <engine release> --pins   phases 0–1 only (no regeneration)
#
# What it does, in order — every step is MECHANICAL (M in the homework):
#
#   phase 0  DECIDE   the release must be PUBLISHED on Maven Central (tags and
#                     Central disagree: 4.142.0 is tagged and unpublished); the
#                     pure version is DERIVED from that engine release's own
#                     pom (INV-1 — nobody types a pure version); the two tag
#                     COMMITS come from `git ls-remote` (peeled refs).
#   phase 1  MOVE     both local checkouts onto the tag commits (fetching the
#                     tag if the clone lacks it; a dirty checkout is refused);
#                     tools/oracle-pins.env (the ONE pin); the root pom's
#                     legend.engine.version / legend.pure.version and its four
#                     INV-6 managed third-party versions at the engine's own
#                     values; tools/engine-runner/pom.xml.
#   phase 2  REGEN    install core at the OLD generated facts (the generators
#                     run on the installed core and READ the checkouts), then
#                     every generator that writes into core — natives
#                     (Pure.java + membership), prelude, dynafn registry, core
#                     imports — then re-install core so the claims ledger reads
#                     the NEW registries, then the claims ledger; then the
#                     parser-equivalence resources: the engine fixture harvest
#                     — tier 1 from the published grammar/compiler tests-jars,
#                     tier 2 from the checkout's extension grammar test SOURCES
#                     (relationalStore / service / persistence: unpublished,
#                     compiled here) — deduped into one snapshot renamed to the
#                     new release (INV-4 is a filename) with an origin census
#                     against the committed one; the corpus manifest; the
#                     protocol roster ledger. Finally tools/version-report.sh
#                     --check must exit 0.
#
# What it does NOT do — the JUDGEMENT half (homework §5 phases 3–6):
#   * run the chain: `GATES_PARALLEL=1 tools/allgates.sh` ONCE, in the
#     background, after this script; CI runs the same nine gates on three
#     platforms from the pins this script wrote;
#   * re-pin the ratchets the chain reports moved (each with a reason) or
#     re-adjudicate the ledgers (shrink-only);
#   * review the generated-resource diffs: `git diff --stat` at the end IS the
#     upstream change made legible — read it before the chain.
#
# A generator that FAILS under its generate flag is the "new thing we cannot
# parse yet" case: fix the platform first, re-run this script (it is
# idempotent — every step rewrites from the pins).
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
cd "$ROOT"

NEW=${1:-}
[ -n "$NEW" ] || { sed -n '2,12p' "$0"; exit 2; }
PINS_ONLY=0
[ "${2:-}" = "--pins" ] && PINS_ONLY=1

CENTRAL=https://repo1.maven.org/maven2
OUT=${BUMP_OUT:-target/bump}
mkdir -p "$OUT"
LOG="$OUT/bump-$NEW.log"
: > "$LOG"

step() { echo; echo "== $*" | tee -a "$LOG"; }
die()  { echo "BUMP FAILED: $*" | tee -a "$LOG" >&2; exit 1; }

# run <log-name> <cmd...> — a maven step with its whole output in a file;
# on failure the tail is shown and the script stops (set -e).
run() {
  local name=$1; shift
  echo "   $*" | tee -a "$LOG"
  if ! "$@" > "$OUT/$name.out" 2>&1; then
    echo "---- $OUT/$name.out (tail) ----" >&2
    tail -40 "$OUT/$name.out" >&2
    die "step '$name' failed — full output in $OUT/$name.out"
  fi
  # a quiet, successful mvn prints nothing — an empty summary is not a failure
  { grep -E "Tests run:.*Fail|BUILD (SUCCESS|FAILURE)" "$OUT/$name.out" || true; } | tail -2 | sed 's/^/   /' | tee -a "$LOG"
}

# --------------------------------------------------------------- phase 0 ---
step "phase 0: decide — $NEW must be published, pure derived, tag commits resolved"
. "$HERE/oracle-pins.env"
OLD=$LEGEND_ENGINE_RELEASE
OLD_PURE=$LEGEND_PURE_RELEASE
echo "   from $OLD / $OLD_PURE" | tee -a "$LOG"

ENGINE_POM=$(curl -sf --max-time 40 "$CENTRAL/org/finos/legend/engine/legend-engine/$NEW/legend-engine-$NEW.pom") \
  || die "legend-engine $NEW is not on Maven Central (tags and Central disagree; pick a PUBLISHED release)"
PURE=$(printf '%s' "$ENGINE_POM" | sed -n 's|.*<legend.pure.version>\([^<]*\)</legend.pure.version>.*|\1|p' | head -1)
[ -n "$PURE" ] || die "engine $NEW's pom declares no legend.pure.version"
curl -sf --max-time 40 -o /dev/null "$CENTRAL/org/finos/legend/pure/legend-pure-m3-core/$PURE/legend-pure-m3-core-$PURE.pom" \
  || die "legend-pure $PURE (engine $NEW's own pairing) is not on Maven Central"
echo "   to   $NEW / $PURE (INV-1: derived from engine $NEW's pom)" | tee -a "$LOG"

ENGINE_TAG="legend-engine-$NEW"
PURE_TAG="legend-pure-$PURE"
# the COMMIT a tag names: the peeled ref of an annotated tag, else the ref
# itself (upstream's tags are lightweight since the 4.14x "CI-friendly
# versions" release workflow — 4.138.2 was annotated, 4.145.0 is not)
peeled() {
  local sha
  sha=$(git ls-remote --tags "https://github.com/$1" "refs/tags/$2^{}" | awk '{print $1}' | head -1)
  [ -n "$sha" ] || sha=$(git ls-remote --tags "https://github.com/$1" "refs/tags/$2" | awk '{print $1}' | head -1)
  echo "$sha"
}
ENGINE_SHA=$(peeled "$LEGEND_ENGINE_REPO" "$ENGINE_TAG")
PURE_SHA=$(peeled "$LEGEND_PURE_REPO" "$PURE_TAG")
[ -n "$ENGINE_SHA" ] || die "no tag $ENGINE_TAG on $LEGEND_ENGINE_REPO"
[ -n "$PURE_SHA" ]   || die "no tag $PURE_TAG on $LEGEND_PURE_REPO"
echo "   $ENGINE_TAG = $ENGINE_SHA" | tee -a "$LOG"
echo "   $PURE_TAG = $PURE_SHA" | tee -a "$LOG"

# the engine's OWN managed versions of the four shared third-party artifacts
# (INV-6: the root pom manages them at exactly these values)
prop() { printf '%s' "$ENGINE_POM" | sed -n "s|.*<$1>\([^<]*\)</$1>.*|\1|p" | head -1; }
HIKARI=$(prop hikaricp.version); LANG3=$(prop commons-lang3.version)
HTTPCORE=$(prop httpcore.version); JUNIT4=$(prop junit.version)
for v in HIKARI LANG3 HTTPCORE JUNIT4; do [ -n "${!v}" ] || die "engine $NEW's pom lacks the $v property INV-6 derives from"; done
echo "   INV-6 at $NEW: HikariCP $HIKARI, commons-lang3 $LANG3, httpcore $HTTPCORE, junit $JUNIT4" | tee -a "$LOG"

# --------------------------------------------------------------- phase 1 ---
step "phase 1: move — checkouts, the pin, the poms"
ROOT_ENGINE=${LEGEND_ENGINE_ROOT:-$HOME/legend/legend-engine}
ROOT_PURE=${LEGEND_PURE_ROOT:-$HOME/legend/legend-pure}
move_checkout() {  # <dir> <tag> <sha>
  local dir=$1 tag=$2 sha=$3
  [ -d "$dir/.git" ] || die "$dir is not a git checkout"
  [ -z "$(git -C "$dir" status --porcelain)" ] || die "$dir is dirty — the oracle checkout must be pristine"
  if ! git -C "$dir" cat-file -e "$sha^{commit}" 2>/dev/null; then
    echo "   fetching $tag into $dir (shallow)" | tee -a "$LOG"
    git -C "$dir" fetch -q --depth 1 origin "refs/tags/$tag:refs/tags/$tag"
  fi
  git -C "$dir" checkout -q --detach "$sha"
  [ "$(git -C "$dir" rev-parse HEAD)" = "$sha" ] || die "$dir did not land on $sha"
  echo "   $dir -> $tag ($sha)" | tee -a "$LOG"
}
move_checkout "$ROOT_ENGINE" "$ENGINE_TAG" "$ENGINE_SHA"
move_checkout "$ROOT_PURE"   "$PURE_TAG"   "$PURE_SHA"

# a literal KEY=VALUE rewrite of the six pin lines (perl: portable in-place)
perl -pi -e "
  s/^LEGEND_ENGINE_RELEASE=.*/LEGEND_ENGINE_RELEASE=$NEW/;
  s/^LEGEND_PURE_RELEASE=.*/LEGEND_PURE_RELEASE=$PURE/;
  s/^LEGEND_ENGINE_SHA=.*/LEGEND_ENGINE_SHA=$ENGINE_SHA/;
  s/^LEGEND_ENGINE_DESCRIBE=.*/LEGEND_ENGINE_DESCRIBE=$ENGINE_TAG/;
  s/^LEGEND_PURE_SHA=.*/LEGEND_PURE_SHA=$PURE_SHA/;
  s/^LEGEND_PURE_DESCRIBE=.*/LEGEND_PURE_DESCRIBE=$PURE_TAG/;
" "$HERE/oracle-pins.env"
echo "   tools/oracle-pins.env -> $NEW / $PURE" | tee -a "$LOG"

perl -pi -e "
  s|<legend.engine.version>[^<]*</legend.engine.version>|<legend.engine.version>$NEW</legend.engine.version>|;
  s|<legend.pure.version>[^<]*</legend.pure.version>|<legend.pure.version>$PURE</legend.pure.version>|;
" pom.xml
# the four INV-6 managed entries: the <version> that FOLLOWS each artifactId
python3 - "$HIKARI" "$LANG3" "$HTTPCORE" "$JUNIT4" <<'PY'
import re, sys
hikari, lang3, httpcore, junit4 = sys.argv[1:5]
p = "pom.xml"
t = open(p, encoding="utf-8").read()
for artifact, version in (("HikariCP", hikari), ("commons-lang3", lang3), ("httpcore", httpcore), ("junit", junit4)):
    pat = re.compile(r"(<artifactId>%s</artifactId>\s*<version>)[^<]*(</version>)" % re.escape(artifact))
    t, n = pat.subn(lambda m: m.group(1) + version + m.group(2), t, count=1)
    if n != 1:
        sys.exit("pom.xml: no managed <artifactId>%s</artifactId> followed by <version> (INV-6)" % artifact)
open(p, "w", encoding="utf-8").write(t)
PY
echo "   pom.xml -> $NEW / $PURE (+ INV-6 managed versions)" | tee -a "$LOG"

perl -pi -e "s|<legend.version>[^<]*</legend.version>|<legend.version>$NEW</legend.version>|" tools/engine-runner/pom.xml
echo "   tools/engine-runner/pom.xml -> $NEW" | tee -a "$LOG"

. "$HERE/oracle-roots.sh"
oracle_roots_check >> "$LOG" || die "the checkouts do not sit on the new pins"

if [ "$PINS_ONLY" = 1 ]; then
  step "pins only — stopping before regeneration (INV-4 will fail --check until the fixture is re-harvested)"
  git status --short | tee -a "$LOG"
  exit 0
fi

# --------------------------------------------------------------- phase 2 ---
step "phase 2a: install core at the OLD generated facts (the generators run on it)"
run core-install-1 mvn -q -pl .,core install -DskipTests

gen() {  # <module> <Test> <flag>  — one generator, flag on
  run "gen-$2" mvn -q -pl "$1" test -Dtest="$2" "-D$3=1" -Dsurefire.failIfNoSpecifiedTests=false "$R1" "$R2"
}
step "phase 2b: the generators that write into core (natives -> prelude -> dynafn -> imports)"
gen spec NativeSignatureGeneratorTest natives.generate
gen spec PreludeGeneratorTest         prelude.generate
gen spec DynaFnRegistryTest           dynafn.generate
gen spec CoreImportsParityTest        imports.generate

step "phase 2c: re-install core at the NEW facts, then the claims ledger (reads the registries)"
run core-install-2 mvn -q -pl .,core install -DskipTests
gen spec ClaimRegistryTest claims.generate

step "phase 2d: parser-equivalence resources — fixture harvest (renamed), corpus manifest, protocol roster"
FIX_DIR=parser-equivalence/src/test/resources
NEW_FIX="$FIX_DIR/engine-grammar-fixtures-$NEW.jsonl"
# the committed snapshot is whichever fixture file git tracks (NOT the pin —
# a re-run after a failed run has already moved the pin); INV-4 = one file
OLD_FIX=$(git ls-files "$FIX_DIR" | grep -E "/engine-grammar-fixtures-[0-9.]+\.jsonl$" | head -1)
[ -n "$OLD_FIX" ] || die "no committed engine-grammar-fixtures-*.jsonl under $FIX_DIR"
DUMP=parser-equivalence/target/engine-fixtures.jsonl

# TIER 1: the engine's PUBLISHED grammar + compiler tests-jars, run under the
# recording shims (ZEngineFixtureHarvest deletes the dump first)
run harvest-tier1 mvn -q -pl parser-equivalence test -Pengine-fixture-harvest -Dtest=ZEngineFixtureHarvest \
    -Dsurefire.failIfNoSpecifiedTests=false "$R1" "$R2"
[ -s "$DUMP" ] || die "the tier-1 harvest wrote no fixtures"
head -1 "$DUMP" | grep -q "^# engine=$NEW\$" \
  || die "harvest header is not '# engine=$NEW' (FixtureRecorder reads the pins; is the pin written?)"
TIER1=$(grep -vc '^#' "$DUMP")

# TIER 2: the checkout's EXTENSION grammar tests (relationalStore, service,
# persistence) — upstream publishes no tests-jars for these, so their test
# SOURCES are compiled here against the harvest profile's classpath (the
# shims first, so every test(...) records instead of asserting) and run
# through ZTier2FixtureHarvest, which APPENDS to the same dump. A file that
# does not compile is skipped per file and counted; the origin census below
# is where a vanished class shows.
TIER2_MODULES="legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-grammar
legend-engine-xts-service/legend-engine-language-pure-dsl-service
legend-engine-xts-persistence/legend-engine-xt-persistence-grammar"
T2="$OUT/tier2-classes"
rm -rf "$T2"; mkdir -p "$T2"
run tier2-classpath mvn -q -pl parser-equivalence dependency:build-classpath -Pengine-fixture-harvest \
    -Dmdep.includeScope=test "-Dmdep.outputFile=$OUT/tier2.cp"
T2CP="$ROOT/parser-equivalence/target/test-classes:$ROOT/parser-equivalence/target/classes:$(cat "$OUT/tier2.cp")"
echo "$TIER2_MODULES" | while read -r mod; do
  src="$ROOT_ENGINE/$mod/src/test/java"
  [ -d "$src" ] || die "tier-2 module has no test sources at $NEW: $mod"
  name=${mod##*/}
  if javac -nowarn -proc:none -encoding UTF-8 -d "$T2" -cp "$T2CP" -sourcepath "$src" \
       $(find "$src" -name '*.java') > "$OUT/tier2-javac-$name.out" 2>&1; then
    echo "   tier 2 $name: compiled whole" | tee -a "$LOG"
  else
    ok=0; bad=""
    for f in $(find "$src" -name 'Test*.java'); do
      if javac -nowarn -proc:none -encoding UTF-8 -d "$T2" -cp "$T2CP" -sourcepath "$src" "$f" \
           >> "$OUT/tier2-javac-$name-perfile.out" 2>&1; then ok=$((ok+1)); else bad="$bad ${f##*/}"; fi
    done
    echo "   tier 2 $name: compiled per file, ok=$ok, FAILED:${bad:- none} ($OUT/tier2-javac-$name-perfile.out)" | tee -a "$LOG"
  fi
done
run harvest-tier2 mvn -q -pl parser-equivalence test -Pengine-fixture-harvest -Dtest=ZTier2FixtureHarvest \
    "-Dtier2.classes=$T2" -Dsurefire.failIfNoSpecifiedTests=false "$R1" "$R2"
grep "@@ tier2" "$OUT/harvest-tier2.out" | sed 's/^/   /' | tee -a "$LOG"

# one snapshot: header + fixtures deduped by exact source (first wins, the
# reader's own rule), and the ORIGIN census against the committed file — a
# class that vanished is upstream's change or a compile miss, never silent
if [ "$OLD_FIX" != "$NEW_FIX" ]; then
  git mv "$OLD_FIX" "$NEW_FIX"
fi
python3 - "$DUMP" "$NEW_FIX" "$OLD_FIX" "$LOG" <<'PY'
import json, sys, collections
dump, new, old, log = sys.argv[1:5]
def origins(lines):
    c = collections.Counter()
    for l in lines:
        d = json.loads(l); c[(d.get("origin") or "?").split("#")[0].rsplit(".", 1)[-1]] += 1
    return c
header, out, seen = None, [], set()
for l in open(dump, encoding="utf-8"):
    if l.startswith("#"):
        header = header or l; continue
    if not l.strip(): continue
    s = json.loads(l)["source"]
    if s in seen: continue
    seen.add(s); out.append(l)
import subprocess
before = [l for l in subprocess.run(["git", "show", "HEAD:" + old], capture_output=True, text=True).stdout.splitlines(True)
          if l.strip() and not l.startswith("#")]
open(new, "w", encoding="utf-8").write(header + "".join(out))
o, n = origins(before), origins(out)
msg = ["   %s: %d fixtures (was %d)" % (new, len(out), len(before))]
for k in sorted(set(o) | set(n), key=lambda k: -(o[k] + n[k])):
    if o[k] != n[k]: msg.append("      %5d -> %5d  %s" % (o[k], n[k], k))
print("\n".join(msg)); open(log, "a").write("\n".join(msg) + "\n")
PY

run manifest mvn -q -pl parser-equivalence test -Dtest=CorpusManifestTest -Dcorpus.manifest.regen=1 \
    -Dsurefire.failIfNoSpecifiedTests=false "$R1" "$R2"
cp parser-equivalence/target/corpus-manifest.tsv "$FIX_DIR/corpus-manifest.tsv"
echo "   corpus-manifest.tsv: $(wc -l < "$FIX_DIR/corpus-manifest.tsv" | tr -d ' ') rows (was $(git show "HEAD:$FIX_DIR/corpus-manifest.tsv" | wc -l | tr -d ' '))" | tee -a "$LOG"

gen parser-equivalence ProtocolRosterCensusTest roster.generate

step "phase 2e: the six checks that say 'one release' — version-report.sh --check"
"$HERE/version-report.sh" --check | tee -a "$LOG" || die "version-report.sh --check is red"
echo "   all invariants hold at $NEW / $PURE" | tee -a "$LOG"

step "done — the upstream change, made legible:"
git status --short | tee -a "$LOG"
git diff --stat | tail -1 | tee -a "$LOG"
cat <<EOF | tee -a "$LOG"

NEXT (judgement — homework §5 phases 3–6):
  1. read the diffs above: prelude.pure / Pure.java / native-*.tsv / DynaFn.java
     / corpus-manifest.tsv / protocol-roster.tsv — that IS the upstream change;
  2. GATES_PARALLEL=1 tools/allgates.sh   (ONCE, in the background);
  3. re-pin every ratchet the chain moves, each with a reason; ledgers shrink-only;
  4. commit named files; push; CI runs the same gates from the pins.
Log: $LOG
EOF
