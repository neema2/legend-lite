#!/bin/bash
# The standing gate chain (docs/GATES.md). Sequential BY DESIGN — concurrent
# heavy JVMs get killed on small machines. The engine module is DELETED:
# its suite migrated into core, so GATE 3 folded into GATE 1 (gate numbers
# kept stable so logs and habits stay comparable).
#
# Usage:
#   LEGEND_ENGINE_ROOT=~/legend/legend-engine \
#   LEGEND_PURE_ROOT=~/legend/legend-pure \
#   caffeinate -dims tools/allgates.sh
#
# CI runs THIS SCRIPT too (.github/workflows/gate.yml): one job per gate,
# GATES=<n> each, against the legend-engine / legend-pure commits pinned in
# tools/oracle-pins.env. The gate logic lives here and nowhere else.
#
# Optional: MVN_SETTINGS=<settings.xml> adds -s to the OFFLINE-friendly gates
# (1-3, 8). Gates 4-5 always run plain mvn — the corpus h2-exec backend resolves
# artifacts at runtime. Run under caffeinate: a ~900s gate with near-zero CPU
# means the machine slept mid-run, not a real regression.
set -u
cd "$(dirname "$0")/.."
# the roots decide every corpus denominator — resolved and CHECKED first
# (presence + pin drift), so a run on a stale fallback checkout (an
# unexported LEGEND_*_ROOT, batch 174) stops here instead of re-pinning
# scoreboards against the wrong spec. tools/oracle-roots.sh sets
# ROOT_ENGINE / ROOT_PURE / R1 / R2.
. tools/oracle-roots.sh
oracle_roots_check || { echo "ALLGATES_DONE — FAILED: oracle roots (see above)" >&2; exit 1; }
SFLAG=()
[ -n "${MVN_SETTINGS:-}" ] && SFLAG=(-s "$MVN_SETTINGS")
# Offline by default (local hygiene: skips remote metadata checks). CI has a
# cold ~/.m2 and MUST resolve, so it sets MVN_OFFLINE=0.
OFF=()
[ "${MVN_OFFLINE:-1}" = "1" ] && OFF=(-o)
# Gate subset: GATES=1,2,3 runs only those. Default is all nine.
WANT=${GATES:-1,2,3,4,5,6,7,8,9}
want() { case ",$WANT," in *",$1,"*) return 0;; *) return 1;; esac; }
# Default the log to a PER-USER path. A fixed /tmp/gates.log is shared across
# accounts on this box (it was found owned by another user), so writes fail
# silently and you end up reading someone else's run. TMPDIR is per-user on
# macOS; the id -un suffix covers Linux, where it is not.
L=${GATES_LOG:-${TMPDIR:-/tmp}/gates-$(id -un).log}
# Per-run scratch dir. The old fixed /tmp/g<n>.out paths are shared across
# users and concurrent runs on this box: "$OUT/g1.out" was found owned by a
# DIFFERENT account, so the redirect failed and the log's grep silently read
# that other user's stale output and reported it as this run's result.
OUT=$(mktemp -d "${TMPDIR:-/tmp}/allgates.XXXXXX")
trap 'rm -rf "$OUT"' EXIT
: > "$L"
FAILED=()
G_T0=$(date +%s)
g() { echo "=== $1" >> "$L"; G_T0=$(date +%s); }

# rec <n> <exit> — record a gate's verdict so the script can fail at the end.
# A gate script that cannot fail is not a gate.
rec() {
  echo "G$1_EXIT=$2 (took $(( $(date +%s) - G_T0 ))s)" >> "$L"
  [ "$2" -ne 0 ] && FAILED+=("G$1")
  return 0
}

# roots_present — gates 4, 5 and 8 are meaningless without the upstream
# checkouts. The `skipped()` detector below CANNOT catch this for gate 8: the
# harness ships a committed in-repo fixture tier, so Corpus is never empty and
# the tests genuinely RUN on a starved corpus rather than Assumptions-skipping.
# Measured 2026-08-11: with both roots absent, PmcdEquivalenceTest runs on 774
# rows instead of 6,033, reports "0 diff", and the build SUCCEEDS. Three gated
# tests have no floor at all and pass green on a 99%-shrunk corpus. So check the
# roots up front rather than trying to detect the symptom afterwards.
roots_present() {
  local ok=0
  [ -d "$ROOT_ENGINE" ] || { echo "MISSING legend-engine checkout: $ROOT_ENGINE" >> "$L"; ok=1; }
  [ -d "$ROOT_PURE" ]   || { echo "MISSING legend-pure checkout: $ROOT_PURE"   >> "$L"; ok=1; }
  return $ok
}

# skipped <file> — surefire reports "Skipped: N" for Assumptions-skipped tests.
# Gates 4, 5 and 8 skip silently without the upstream checkouts; that is NOT a
# pass. Returns 0 (true) when the run was entirely skipped.
skipped() {
  # awk, not a grep backreference — ERE backrefs are not portable to BSD grep.
  # STRICT BY DESIGN (user ruling 2026-08-21): ANY fully-skipped class
  # marks the gate — a class that always skips inside a gate is roster
  # theater; make it self-sufficient or take it off the roster.
  awk '/Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+, Skipped: [0-9]+/ {
         run=0; skip=0
         for (i = 1; i <= NF; i++) {
           if ($i == "run:")     { run  = $(i+1) + 0 }
           if ($i == "Skipped:") { skip = $(i+1) + 0 }
         }
         if (run > 0 && run == skip) { found = 1 }
       }
       END { exit(found ? 0 : 1) }' "$1" 2>/dev/null
}

# PX.1 (WZ incident): snapshot the tree — an EXTERNAL writer mutated a
# source file mid-chain and the chain certified the poisoned state.
# Only docs/RELATIONAL_CORPUS.md may change during a chain (G4 writes it).
TREE0=$(git status --porcelain | grep -v "docs/RELATIONAL_CORPUS.md")

# THE BUILD — ONE compile of core per chain, ALWAYS, before any suite.
#
# It was three: gate 1 `clean test`, gate 2 `install`, and gate 8's `-am clean`.
# Nothing downstream needs core's TESTS to have passed, only its JAR, so the
# build is hoisted out and everything reads the single result: core's own
# suites use core/target directly (never cleaned after this), while pct and
# parser-equivalence resolve the installed jar from ~/.m2.
#
# CLEAN is load-bearing HERE now: NullAway binds to default-compile, so a warm
# target/ silently no-ops the null gate. Verified 2026-09-10 by injecting a
# `return null` into a @NonNull method — this step failed with the NullAway
# error, so the gate still fires from its new home.
#
# `-pl .,core` installs the PARENT POM too: pct builds standalone and resolves
# it from the repository (CI's cold ~/.m2 caught that, 2026-09-09).
#
# It runs even for a single-gate selection, and that is a FIX, not overhead:
# `GATES=6` used to run pct against whatever jar happened to be installed. The
# `-am` on gate 8 guarded exactly that hazard for one gate and left it open for
# the three pct gates. Building first closes it for all of them.
g "BUILD core once (clean compile = the null gate; install = what pct and"
g "      parser-equivalence resolve)"
mvn ${OFF[@]+"${OFF[@]}"} -pl .,core clean install -DskipTests > "$OUT/g2.out" 2>&1
BUILD_EXIT=$?
# INV-5 (tools/classpath-convergence.sh) rides gate 2, AFTER the install it
# resolves against: every shared artifact at one version, zero org.finos.legend
# artifacts on core's and spec's classpaths. It ran in the CI gate-env once
# (batch 5 audit) and passed only from a cache of the OLD groupId — before the
# install it has nothing to resolve (batch 7c, 2026-09-11).
if [ "$BUILD_EXIT" -eq 0 ]; then
  if ! tools/classpath-convergence.sh --quiet > "$OUT/g2-convergence.out" 2>&1; then
    echo "G2 CONVERGENCE RED — tools/classpath-convergence.sh (INV-5 / the boundary):" >> "$L"
    tail -8 "$OUT/g2-convergence.out" >> "$L"
    BUILD_EXIT=1
  fi
fi
rec 2 $BUILD_EXIT
if [ "$BUILD_EXIT" -ne 0 ]; then
  echo "ALLGATES_DONE — FAILED: BUILD (nothing can be trusted without it)" >> "$L"
  echo "ALLGATES_DONE — FAILED: BUILD  (detail: $L)" >&2
  cp "$OUT/g2.out" "${L%.log}.g2.out" 2>/dev/null
  exit 1
fi

# GATE3 spec parity (batch 7, 2026-09-11): core's GENERATED facts — the prelude,
# the signature text, the dynafunction registry, the implicit-import sequence,
# the claims ledger, the platform spellings — recomputed from the pinned
# checkouts by their generators (spec, com.legend.generators) and asserted
# byte-identical to the committed files; plus the spec census, the path
# manifest and the subsumed registry. Backend-free, so it runs once. (The old
# gate 3 was the deleted engine module's suite, folded into gate 1.)
gate3() {
  if ! want 3; then return 0; fi
  if ! roots_present; then
    echo "G3 NOT RUN — upstream checkouts absent. NOT a pass." >> "$L"
    rec 3 1
  else
  g "GATE3 spec parity (generated facts vs the pinned release; census; manifest)"
  mvn -pl spec test "$R1" "$R2" > "$OUT/g3.out" 2>&1
  G3=$?; if skipped "$OUT/g3.out"; then
    echo "G3 SKIPPED — no upstream checkouts ($ROOT_ENGINE / $ROOT_PURE). NOT a pass." >> "$L"; G3=1
  fi
  rec 3 $G3; grep -E "Tests run: [0-9]+, Fail" "$OUT/g3.out" | tail -1 >> "$L"
  fi
}

gate1() {
  if ! want 1; then return 0; fi
  g "GATE1 core suite (the SUITE only — THE BUILD compiled main and ran the"
  g "      null gate; a second clean here would just redo that work)"
  # $R1/$R2: the suite reads the spec checkouts (PreludeGeneratorTest
  # regenerates prelude.pure from them; SpecBodyCensusTest's typing census is
  # a shrink-only pin over legend-pure).
  mvn ${OFF[@]+"${OFF[@]}"} -pl core test "$R1" "$R2" > "$OUT/g1.out" 2>&1
  rec 1 $?; grep -E "Tests run: [0-9]+, Fail" "$OUT/g1.out" | tail -1 >> "$L"
}

gate4() {
  if ! want 4; then return 0; fi
  if ! roots_present; then
    echo "G4 NOT RUN — legend-engine checkout absent. NOT a pass." >> "$L"
    rec 4 1
  else
  g "GATE4 DuckDB corpus"
  mvn -pl spec test -Dtest=MinimalCorpusTest -Dsurefire.excludedGroups= "$R1" "$R2" > "$OUT/g4.out" 2>&1
  G4=$?; if skipped "$OUT/g4.out"; then
    echo "G4 SKIPPED — no legend-engine checkout at $ROOT_ENGINE. NOT a pass." >> "$L"; G4=1
  fi
  rec 4 $G4; grep -E "h2-exec|Tests run: [0-9]+, Fail" "$OUT/g4.out" | tail -3 >> "$L"
  # a failed lane keeps its log beside the chain log (like G1): the
  # moved pin's assertion is in there, and re-running the lane to read
  # it cost a full sweep per pin (2026-09-02)
  fi
}

gate5() {
  if ! want 5; then return 0; fi
  if ! roots_present; then
    echo "G5 NOT RUN — legend-engine checkout absent. NOT a pass." >> "$L"
    rec 5 1
  else
  g "GATE5 h2 corpus"
  mvn -pl spec test -Dtest=MinimalCorpusTest -Dsurefire.excludedGroups= -Drcorpus.backend=h2 "$R1" "$R2" > "$OUT/g5.out" 2>&1
  G5=$?; if skipped "$OUT/g5.out"; then
    echo "G5 SKIPPED — no legend-engine checkout at $ROOT_ENGINE. NOT a pass." >> "$L"; G5=1
  fi
  rec 5 $G5; grep -E "EXACT|h2|Tests run: [0-9]+, Fail" "$OUT/g5.out" | tail -3 >> "$L"
  fi
}

gate6() {
  if ! want 6; then return 0; fi
  g "GATE6 PCT full DuckDB (the five PCT suites; Channel B runs ONCE, in G9)"
  # $R1 AND $R2 are REQUIRED: the Standard/Relation/Unclassified scopes
  # read the REAL legend-engine trees at legend.engine.root; without the
  # properties they fall back to the ~/legend checkouts — DIFFERENT
  # (stale) trees. The discovery pins caught the skew (relation 280 !=
  # 287, 2026-08-19). The Channel B suites are EXCLUDED here (2026-09-02
  # homework: G6 and G9 executed the same five classes on the same
  # inputs — ~13s of duplicate work per chain); G9 is their one run.
  ( cd pct && mvn ${OFF[@]+"${OFF[@]}"} clean test -Dtest='!ChannelB*' "$R1" "$R2" ) > "$OUT/g6.out" 2>&1
  rec 6 $?; grep -E "Tests run: [0-9]+, Fail" "$OUT/g6.out" | tail -1 >> "$L"
}

# Ledger: 350 run, <=1 failure, <=24 errors. CEILINGS, not equality — the old
# `grep -q "Tests run: 348, Failures: 1, Errors: 22"` went RED the moment you
# fixed one of the 22. Lower these numbers when you earn it.
# 348/22 -> 350/24 on 2026-09-10 (upstream boundary batch 1): the PCT jars
# moved 4.133.0 -> 4.138.2 (one release with the source checkouts), and the
# relation jar universe gained exactly two tests —
# testVariantMapColumn_{keys,values}_LateralFlatten — both in the LATERAL
# family the H2 lane already errors on (`Function "LATERAL" not found`; the
# DuckDB lane runs all 350 with 0 errors). The other 22 are the same
# dialect-capability rows as before (LIST_*, UNNEST, fold, LATERAL x4).
# 4.145.0 (batch 8): the relation jar universe grew 350 -> 469 (the new
# quantified comparisons etc. are EXPECTED failures, pinned per test in
# Test_LegendLite_RelationFunctions_PCT); the H2 LATERAL family gained two
# more tests (24 -> 26)
G7_MIN_RUN=469; G7_MAX_FAIL=1; G7_MAX_ERR=26
gate7() {
  if ! want 7; then return 0; fi
  g "GATE7 PCT h2modern Relation (run>=$G7_MIN_RUN, fail<=$G7_MAX_FAIL, err<=$G7_MAX_ERR)"
  ( cd pct && LEGENDLITE_PCT_BACKEND=h2 mvn ${OFF[@]+"${OFF[@]}"} test -Dtest=Test_LegendLite_RelationFunctions_PCT -Dh2.version=2.4.240 "$R1" "$R2" ) > "$OUT/g7.out" 2>&1
  # Anchor on the SUITE line, not `tail -1`. Surefire prints a trailing
  # "Tests run: 1, Failures: 0, Errors: 1" summarising failing CLASSES, and
  # taking the last match picks that instead of the 348-test result — which
  # reported a false RED on a genuinely ledgered run.
  G7_LINE=$(grep -E "Tests run: .*Test_LegendLite_RelationFunctions_PCT" "$OUT/g7.out" | tail -1)
  [ -z "$G7_LINE" ] && G7_LINE=$(grep -E "Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+" "$OUT/g7.out" | tail -1)
  G7=1
  if [[ "$G7_LINE" =~ Tests\ run:\ ([0-9]+),\ Failures:\ ([0-9]+),\ Errors:\ ([0-9]+) ]]; then
    R=${BASH_REMATCH[1]}; F=${BASH_REMATCH[2]}; E=${BASH_REMATCH[3]}
    if [ "$R" -ge "$G7_MIN_RUN" ] && [ "$F" -le "$G7_MAX_FAIL" ] && [ "$E" -le "$G7_MAX_ERR" ]; then
      G7=0
      if [ "$F" -lt "$G7_MAX_FAIL" ] || [ "$E" -lt "$G7_MAX_ERR" ]; then
        echo "G7 IMPROVED — fail $F/$G7_MAX_FAIL, err $E/$G7_MAX_ERR. Ratchet tools/allgates.sh." >> "$L"
      fi
    fi
  else
    echo "G7 no surefire summary found — treating as failure" >> "$L"
  fi
  rec 7 $G7; echo "${G7_LINE:-<no summary>}" >> "$L"
}

gate9() {
  if ! want 9; then return 0; fi
  if ! roots_present; then
    echo "G9 NOT RUN — upstream checkout absent. NOT a pass." >> "$L"
    rec 9 1
  else
  g "GATE9 ChannelB dual-verdict suites (discovery + disagree-0 + decline ceilings)"
  # ChannelB reads -Dlegend.pure.root / -Dlegend.engine.root SYSTEM
  # PROPERTIES (like rcorpus) — an env-only invocation silently referees
  # the stale $HOME checkout and fakes a discovery regression (V11
  # trap, recorded 2026-08-22). Added as a gate because the X-slice
  # pushed with these pins unvalidated: the suites were in no gate.
  ( cd pct && mvn ${OFF[@]+"${OFF[@]}"} test -Dtest='ChannelB*' "$R1" "$R2" ) > "$OUT/g9.out" 2>&1
  G9_LINE=$(grep -E "Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+" "$OUT/g9.out" | tail -1)
  G9=1
  if [[ "$G9_LINE" =~ Tests\ run:\ ([0-9]+),\ Failures:\ ([0-9]+),\ Errors:\ ([0-9]+) ]]; then
    R=${BASH_REMATCH[1]}; F=${BASH_REMATCH[2]}; E=${BASH_REMATCH[3]}
    [ "$R" -ge 5 ] && [ "$F" -eq 0 ] && [ "$E" -eq 0 ] && G9=0
  else
    echo "G9 no surefire summary found — treating as failure" >> "$L"
  fi
  rec 9 $G9; echo "${G9_LINE:-<no summary>}" >> "$L"
  grep -hE "census=|canon: " "$OUT/g9.out" | head -10 >> "$L"
  fi
}

gate8() {
  if ! want 8; then return 0; fi
  if ! roots_present; then
    echo "G8 NOT RUN — upstream checkout absent. NOT a pass." >> "$L"
    rec 8 1
  else
  g "GATE8 parser-equivalence: byte parity (corpus + own corpus + seeds) + rejection parity + SPI seam + pull sentinel + protocol roster"
  # `-am` is GONE (2026-09-10). It existed so GATES=8 alone could not A/B a
  # previously installed jar — but THE BUILD now runs before every selection,
  # so the installed jar is always this tree's. Dropping it stops gate 8
  # rebuilding core (measured: same 40 tests, 71s -> 59s) and takes gate 8 out
  # of core/target, which is what lets it run beside the other streams.
  # CLEAN is load-bearing here too: a warm target/ runs test classes
  # compiled against the PREVIOUS core jar (stale-class NoSuchMethodError,
  # or worse, stale tests silently passing old behavior)
  # ROSTER = EVERY test class in the module (DEEP_AUDIT §11c: seven
  # classes sat outside the old 20-class list, so the module was RED at
  # HEAD while this gate was green — the surgical fix appends them; the
  # allowlist + rename-goes-red discipline stays).
  # THE DIAGNOSTICS BATTERY IS OUT (user ruling 2026-08-26, reviving
  # the 08-14 "triggered, not scheduled" cadence e87fffa8 that the
  # roster restoration clobbered): seven measurement classes — the
  # benchmark, six censuses/sizers (five assertless by their own docs;
  # GrammarCoverage ratchets against PINNED inputs, a constant between
  # pin changes) — run via tools/diagnostics.sh with its OWN
  # rename-goes-red roster, on three triggers: corpus manifest change,
  # oracle-pin bump, parser/protocol/census-code change. The gate
  # roster below = every ASSERTING parity class; nothing sits outside
  # some roster.
  mvn ${SFLAG[@]+"${SFLAG[@]}"} -pl parser-equivalence clean test \
      -Dtest='CorpusSweepTest,RejectionParityTest,SectionParseSentinelTest,FixtureAdjudicationTest,EngineSectionRosterTest,EngineElementRosterTest,ViewFilterParityTest,ComparatorSelfTest,QuotedImportParityTest,CorpusManifestTest,OffsetCompositionParityTest,AdversarialParityTest,MessageParityTest,OwnCorpusConformanceTest,OwnDialectCensusTest,SurfaceCensusTest,FixtureCorpusParityTest,MutationFuzzTest,GenerativeDualParseTest,PctParseCensusTest,OwnCorpusParityTest,ProtocolSeedParityTest,ProtocolRosterCensusTest' \
      -Dsurefire.failIfNoSpecifiedTests=false "$R1" "$R2" > "$OUT/g8.out" 2>&1
  G8=$?
  # RENAME-GOES-RED (deep-audit M1/§5): failIfNoSpecifiedTests=false is
  # required because -am builds core (which has none of these classes) —
  # so instead verify each named class actually RAN; a renamed or deleted
  # test can no longer silently shrink the gate.
  for tc in CorpusSweepTest RejectionParityTest SectionParseSentinelTest \
      FixtureAdjudicationTest EngineSectionRosterTest EngineElementRosterTest \
      ViewFilterParityTest ComparatorSelfTest QuotedImportParityTest \
      CorpusManifestTest OffsetCompositionParityTest AdversarialParityTest \
      SurfaceCensusTest \
      FixtureCorpusParityTest \
      MutationFuzzTest \
      MessageParityTest OwnCorpusConformanceTest OwnDialectCensusTest \
      GenerativeDualParseTest \
      PctParseCensusTest \
      OwnCorpusParityTest ProtocolSeedParityTest ProtocolRosterCensusTest; do
    if ! grep -q "in com.legend.equivalence.$tc" "$OUT/g8.out"; then
      echo "G8 MISSING TEST CLASS: $tc did not run — rename/delete goes RED." >> "$L"; G8=1
    fi
  done
  if skipped "$OUT/g8.out"; then
    echo "G8 SKIPPED — no upstream checkouts ($ROOT_ENGINE / $ROOT_PURE). NOT a pass." >> "$L"; G8=1
  fi
  rec 8 $G8
  sed -n '4,10p' parser-equivalence/target/equivalence-report.txt >> "$L" 2>/dev/null
  sed -n '3,6p' parser-equivalence/target/rejection-report.txt >> "$L" 2>/dev/null
  sed -n '3,9p' parser-equivalence/target/spi-seam-report.txt >> "$L" 2>/dev/null
  sed -n '3,5p' parser-equivalence/target/section-sentinel-report.txt >> "$L" 2>/dev/null
  fi
}

# ---- THE STREAMS -------------------------------------------------------
# Suites conflict exactly when they write the same directory, so the safe
# decomposition is three groups and no finer:
#
#   A  core/target + spec/target gate 1 (core), gate 4, gate 5 (spec)
#   B  pct/target                gate 6, gate 7, gate 9
#   C  parser-equivalence/target gate 8   (only since it dropped -am)
#
# Gates 4 and 5 both write spec/target/corpus2-{pass,fail,skipped}.txt at
# FIXED paths and share one surefire-reports dir, which is why they stay
# sequential; since batch 7b (2026-09-11) they run in the spec module, so
# gate 1 (core/target) could split off them — kept in stream A on purpose
# until the H2 lane's memory beside gate 1 is measured.
#
# SEQUENTIAL BY DEFAULT. GATES_PARALLEL=1 runs the three streams at once,
# which is only sound because THE BUILD already ran: every stream reads a
# finished artifact and none of them writes another's directory. Set it on a
# machine with cores and RAM to spare; the old warning stands otherwise, and
# concurrent heavy JVMs on a small box get killed.
stream() {
  local name=$1; shift
  for fn in "$@"; do "$fn"; done
  return 0
}

if [ "${GATES_PARALLEL:-0}" = "1" ]; then
  echo "streams: A(1,3,4,5) B(6,7,9) C(8) in PARALLEL" >> "$L"
  # Each stream is a subshell with its OWN log. Three writers appending to one
  # file can tear a line, and the verdict below is derived from those lines —
  # so they are kept apart and concatenated in a fixed order afterwards, which
  # also makes the log read the same every run. The subshell's own G_T0 keeps
  # each gate's timing honest.
  ( L="$OUT/stream-A.log"; : > "$L"; stream A gate1 gate3 gate4 gate5 ) &
  PA=$!
  ( L="$OUT/stream-B.log"; : > "$L"; stream B gate6 gate7 gate9 ) &
  PB=$!
  ( L="$OUT/stream-C.log"; : > "$L"; stream C gate8 ) &
  PC=$!
  wait $PA; wait $PB; wait $PC
  cat "$OUT/stream-A.log" "$OUT/stream-B.log" "$OUT/stream-C.log" >> "$L" 2>/dev/null
  # a subshell cannot mutate the parent's FAILED array, so rebuild it from the
  # verdict lines the streams wrote
  FAILED=()
  while read -r n; do FAILED+=("G$n"); done < <(
    grep -E "^G[0-9]+_EXIT=[1-9]" "$L" | sed -E 's/^G([0-9]+)_EXIT=.*/\1/')
else
  stream A gate1 gate3 gate4 gate5
  stream B gate6 gate7 gate9
  stream C gate8
fi

# NO VERDICT IS A FAILURE. Rebuilding the verdict from log lines means a gate
# that never REPORTED looks exactly like a gate that never ran — and in
# parallel mode a stream killed mid-flight (an OOM on a small box is the
# obvious way) takes its remaining gates' lines with it. Proven 2026-09-10 on a
# synthetic log: streams A/B/C dead after gate 1 produced an EMPTY failure list
# and the chain reported GREEN. So every SELECTED gate must have written a
# verdict; a missing one is a failure with its own name, never silence.
# ("A gate script that cannot fail is not a gate" — the rule this restores.)
for n in 1 4 5 6 7 8 9; do
  want "$n" || continue
  grep -qE "^G${n}_EXIT=" "$L" || {
    echo "G${n} NO VERDICT — its stream did not report (killed? crashed?). NOT a pass." >> "$L"
    FAILED+=("G${n}(no-verdict)")
  }
done

# EVERY gate's output is kept, GREEN INCLUDED (2026-09-09). Only G4/G5 were
# kept on green and the rest only on failure, so a PASSING gate left no record
# — and when gate 6 became CI's critical path there was no way to see where its
# time went without re-running it. A gate's output is evidence whatever the
# verdict; the scratch dir is deleted on exit, so it is keep-now or lose-it.
for f in "$OUT"/g*.out; do
  [ -f "$f" ] && cp "$f" "${L%.log}.$(basename "$f")" 2>/dev/null
done

# PX.1 tripwire runs UNCONDITIONALLY (DEEP_AUDIT §11c: it only ran on
# all-green chains, so a failed-gate chain never checked tree
# mutation) and REPORTS FAILURE to automated callers (it printed
# FAILED then `exit 0` — the one branch detecting a poisoned
# certification was the one reporting success).
TREE1=$(git status --porcelain | grep -v "docs/RELATIONAL_CORPUS.md")
if [ "$TREE0" != "$TREE1" ]; then
  echo "ALLGATES_DONE — FAILED: TREE MUTATED MID-CHAIN (PX.1 tripwire)" >> "$L"
  echo "ALLGATES_DONE — FAILED: TREE MUTATED MID-CHAIN (PX.1 tripwire)"
  diff <(echo "$TREE0") <(echo "$TREE1") | head -10
  exit 1
fi
if [ ${#FAILED[@]} -eq 0 ]; then
  echo "ALLGATES_DONE — GREEN (gates: $WANT)" >> "$L"
  echo "ALLGATES_DONE — GREEN (gates: $WANT)"
  exit 0
fi
echo "ALLGATES_DONE — FAILED: ${FAILED[*]}" >> "$L"
echo "ALLGATES_DONE — FAILED: ${FAILED[*]}  (detail: $L)" >&2
echo "every gate's output is beside $L" >&2
exit 1
