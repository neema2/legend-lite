#!/bin/bash
# THE VERSION REPORT — every upstream identity legend-lite carries, side by side
# with the ONE release tools/oracle-pins.env names, plus the invariants that say
# whether they agree. Upstream is one release (docs/UPSTREAM_BOUNDARY_PROGRAM.md
# §3 A); this script is what makes "one" a checked fact rather than a habit.
#
# The identities (docs/UPSTREAM_BOUNDARY_HOMEWORK_2026_09_10.md §1 measured six
# of them live at once before 2026-09-10):
#
#   RELEASE  tools/oracle-pins.env        THE pin: LEGEND_ENGINE_RELEASE and the
#                                         pure version derived from it
#   SOURCE   tools/oracle-pins.env        the checkout commits gates 1,4,5,8,9 and
#                                         the prelude generator read as the spec —
#                                         must be the release TAG's commit
#   POM      pom.xml                      <legend.engine.version>/<legend.pure.version>
#                                         — what pct (gates 6,7,9) and
#                                         parser-equivalence (gate 8) resolve
#   RUNNER   tools/engine-runner/pom.xml  the perf harness (standalone pom, not a gate)
#   FIXTURE  parser-equivalence/.../engine-grammar-fixtures-*.jsonl
#                                         harvested from the oracle jars (tier C6)
#
# Usage:
#   tools/version-report.sh            report (exit 0 unless an invariant breaks)
#   tools/version-report.sh --offline  pins only, no network
#   tools/version-report.sh --check    invariants only, quiet on success (CI:
#                                      .github/actions/gate-env, beside
#                                      oracle_roots_check)
#
# Exit 1 on an invariant violation, 2 on a broken pin file.

set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]:-tools/version-report.sh}")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)

OFFLINE=0
CHECK_ONLY=0
for a in "$@"; do
  case "$a" in
    --offline) OFFLINE=1 ;;
    --check) CHECK_ONLY=1 ;;
    -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown argument: $a" >&2; exit 2 ;;
  esac
done

CENTRAL=https://repo1.maven.org/maven2
ENGINE_GA=org/finos/legend/engine/legend-engine-language-pure-grammar
PURE_GA=org/finos/legend/pure/legend-pure-m3-core

say() { [ "$CHECK_ONLY" = 1 ] || echo "$@"; }

# ---------------------------------------------------------------- the pins ---
. "$HERE/oracle-pins.env" || { echo "cannot read tools/oracle-pins.env" >&2; exit 2; }
for k in LEGEND_ENGINE_RELEASE LEGEND_PURE_RELEASE LEGEND_ENGINE_SHA LEGEND_PURE_SHA \
         LEGEND_ENGINE_DESCRIBE LEGEND_PURE_DESCRIBE LEGEND_ENGINE_REPO LEGEND_PURE_REPO; do
  [ -n "${!k:-}" ] || { echo "tools/oracle-pins.env: $k is missing" >&2; exit 2; }
done

# <property> out of a pom's own <properties>
pom_prop() {  # pom_prop <file> <property>
  sed -n "s|.*<$2>\([^<]*\)</$2>.*|\1|p" "$1" | head -1
}

POM_ENGINE=$(pom_prop "$ROOT/pom.xml" legend.engine.version)
POM_PURE=$(pom_prop "$ROOT/pom.xml" legend.pure.version)
RUNNER_ENGINE=$(pom_prop "$ROOT/tools/engine-runner/pom.xml" legend.version)

# a module pom that declares its OWN legend version would silently shadow the
# root's; the rule is that none does
own_versions() {  # own_versions <module pom>
  # grep -c prints 0 AND exits 1 on no match — no `|| echo 0` fallback
  grep -cE '<legend\.(engine|pure)\.version>' "$1" 2>/dev/null || true
}

# the version carried by the committed FIXTURE snapshot. Recorded INSIDE the
# file as its first line (`# engine=<version>`, batch 2 of the program); the
# filename carries it too and the two must agree.
FIXTURE=$(ls "$ROOT"/parser-equivalence/src/test/resources/engine-grammar-fixtures-*.jsonl 2>/dev/null | head -1)
FIXTURE_NAME_VER=$(basename "${FIXTURE:-engine-grammar-fixtures-none.jsonl}" .jsonl)
FIXTURE_NAME_VER=${FIXTURE_NAME_VER#engine-grammar-fixtures-}
FIXTURE_HEAD_VER=""
[ -n "$FIXTURE" ] && FIXTURE_HEAD_VER=$(head -1 "$FIXTURE" | sed -n 's|^# engine=\([^ ]*\).*|\1|p')

# ------------------------------------------------------------- the upstream ---
# Maven Central and the git tags DISAGREE: 4.142.0 is tagged and was never
# published, 4.135.3/.4 likewise. Jars can only pin what Central has; source
# pins can name any commit. Both are reported.
LATEST_ENGINE=""; LATEST_PURE=""; TAG_ENGINE=""; TAG_PURE=""
ENGINE_VERSIONS=""; PURE_VERSIONS=""
if [ "$OFFLINE" = 0 ] && [ "$CHECK_ONLY" = 0 ]; then
  ENGINE_META=$(curl -sf --max-time 40 "$CENTRAL/$ENGINE_GA/maven-metadata.xml" 2>/dev/null)
  PURE_META=$(curl -sf --max-time 40 "$CENTRAL/$PURE_GA/maven-metadata.xml" 2>/dev/null)
  LATEST_ENGINE=$(printf '%s' "$ENGINE_META" | sed -n 's|.*<release>\([^<]*\)</release>.*|\1|p')
  LATEST_PURE=$(printf '%s' "$PURE_META" | sed -n 's|.*<release>\([^<]*\)</release>.*|\1|p')
  ENGINE_VERSIONS=$(printf '%s' "$ENGINE_META" | grep -oE '<version>[^<]+' | sed 's|<version>||')
  PURE_VERSIONS=$(printf '%s' "$PURE_META" | grep -oE '<version>[^<]+' | sed 's|<version>||')
  # ls-remote, NOT the GitHub tags API: that endpoint PAGES (legend-engine has
  # 1,232 tags) and does not return them in version order — its first page opens
  # with the legacy `legend-engine-release-4.114.0` names, so a first-page read
  # sorted with `sort -V` is sampling, and silently reports the wrong "latest"
  # as soon as the newest tag falls off page one.
  TAG_ENGINE=$(git ls-remote --tags "https://github.com/$LEGEND_ENGINE_REPO" 2>/dev/null \
    | sed -n 's|.*refs/tags/legend-engine-\([0-9][0-9.]*\)$|\1|p' | sort -V | tail -1)
  TAG_PURE=$(git ls-remote --tags "https://github.com/$LEGEND_PURE_REPO" 2>/dev/null \
    | sed -n 's|.*refs/tags/legend-pure-\([0-9][0-9.]*\)$|\1|p' | sort -V | tail -1)
fi

# releases published AFTER <version>, from a metadata version list. Legacy
# non-numeric versions (release-4.114.0, legend-pure-4.5.8) are dropped first:
# `sort -V` sorts them to the END of the list and they inflate every count.
behind() {  # behind <version> <newline-separated list>
  local v=$1 list=$2
  [ -n "$list" ] || { echo "?"; return; }
  printf '%s\n' "$list" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -V \
    | awk -v v="$v" 'seen{n++} $0==v{seen=1} END{print seen?n:"?"}'
}

# the legend-pure version an ENGINE release declares in its own pom — the
# upstream pairing, and the authority for invariant 1
pure_of_engine() {  # pure_of_engine <engine version>
  [ "$OFFLINE" = 0 ] || { echo ""; return; }
  curl -sf --max-time 40 \
    "$CENTRAL/org/finos/legend/engine/legend-engine/$1/legend-engine-$1.pom" 2>/dev/null \
    | sed -n 's|.*<legend.pure.version>\([^<]*\)</legend.pure.version>.*|\1|p' | head -1
}

# ----------------------------------------------------------------- report ---
say
say "==================== legend-lite upstream pins, $(date +%Y-%m-%d) ===================="
say
printf_row() { [ "$CHECK_ONLY" = 1 ] || printf '%-9s %-40s %-12s %-10s %s\n' "$1" "$2" "$3" "$4" "$5"; }
printf_row IDENTITY "DECLARED IN" ENGINE PURE "RELEASES BEHIND"
printf_row --------- ---------------------------------------- ------------ ---------- ---------------
printf_row RELEASE "tools/oracle-pins.env" "$LEGEND_ENGINE_RELEASE" "$LEGEND_PURE_RELEASE" \
  "$(behind "$LEGEND_ENGINE_RELEASE" "$ENGINE_VERSIONS")/$(behind "$LEGEND_PURE_RELEASE" "$PURE_VERSIONS")"
printf_row SOURCE "tools/oracle-pins.env (tag commits)" "${LEGEND_ENGINE_DESCRIBE#legend-engine-}" "${LEGEND_PURE_DESCRIBE#legend-pure-}" ""
printf_row POM "pom.xml (pct, parser-equivalence)" "$POM_ENGINE" "$POM_PURE" ""
printf_row RUNNER "tools/engine-runner/pom.xml" "$RUNNER_ENGINE" "-" ""
printf_row FIXTURE "parser-equiv .../fixtures-*.jsonl" "${FIXTURE_HEAD_VER:-?} (name $FIXTURE_NAME_VER)" "-" ""
say
printf_row LATEST "maven central (release)" "${LATEST_ENGINE:-?}" "${LATEST_PURE:-?}" ""
printf_row LATEST "git tags (newest)" "${TAG_ENGINE:-?}" "${TAG_PURE:-?}" ""
say

# local checkouts: present, and on the pins?
say "---- local checkouts ----"
for pair in "engine|${LEGEND_ENGINE_ROOT:-$HOME/legend/legend-engine}|$LEGEND_ENGINE_SHA|$LEGEND_ENGINE_DESCRIBE" \
            "pure|${LEGEND_PURE_ROOT:-$HOME/legend/legend-pure}|$LEGEND_PURE_SHA|$LEGEND_PURE_DESCRIBE"; do
  IFS='|' read -r name dir want tag <<< "$pair"
  if [ ! -d "$dir" ]; then
    say "  legend-$name: MISSING at $dir"
  else
    h=$(git -C "$dir" rev-parse HEAD 2>/dev/null)
    d=$(git -C "$dir" describe --tags 2>/dev/null)
    if [ -z "$h" ]; then say "  legend-$name: $dir (not a git checkout)"
    elif [ "$h" = "$want" ]; then say "  legend-$name: ON PIN    $d  ($dir)"
    else say "  legend-$name: PIN DRIFT $d  ($h != $want; $dir)"
    fi
  fi
done
say

# ------------------------------------------------------------- invariants ---
FAIL=0
inv() {  # inv <id> <ok:0|1> <text>
  if [ "$2" = 0 ]; then say "  OK    INV-$1  $3"
  else echo "  BROKEN INV-$1  $3"; FAIL=1
  fi
}
say "---- invariants (docs/UPSTREAM_BOUNDARY_PROGRAM.md §3 A, §5) ----"

# INV-0: ONE release. Every declared version identity equals the pin.
inv "0a" "$([ "$POM_ENGINE" = "$LEGEND_ENGINE_RELEASE" ] && echo 0 || echo 1)" \
  "pom.xml legend.engine.version $POM_ENGINE == LEGEND_ENGINE_RELEASE $LEGEND_ENGINE_RELEASE"
inv "0b" "$([ "$POM_PURE" = "$LEGEND_PURE_RELEASE" ] && echo 0 || echo 1)" \
  "pom.xml legend.pure.version $POM_PURE == LEGEND_PURE_RELEASE $LEGEND_PURE_RELEASE"
inv "0c" "$([ "$RUNNER_ENGINE" = "$LEGEND_ENGINE_RELEASE" ] && echo 0 || echo 1)" \
  "tools/engine-runner/pom.xml legend.version $RUNNER_ENGINE == LEGEND_ENGINE_RELEASE"
own=$(( $(own_versions "$ROOT/pct/pom.xml") + $(own_versions "$ROOT/parser-equivalence/pom.xml") ))
inv "0d" "$([ "$own" = 0 ] && echo 0 || echo 1)" \
  "no module pom declares its own legend.engine.version / legend.pure.version ($own found; the root pom is the one place)"

# INV-1: the pure version is the one THIS engine release declares in its own
# pom. Mixing an engine with a pure it was never built against means the
# oracle's own platform sources disagree with its compiler.
if [ "$OFFLINE" = 1 ]; then
  say "  SKIP  INV-1  (offline: upstream pairing not checked)"
else
  want=$(pure_of_engine "$LEGEND_ENGINE_RELEASE")
  inv "1" "$([ -n "$want" ] && [ "$want" = "$LEGEND_PURE_RELEASE" ] && echo 0 || echo 1)" \
    "LEGEND_PURE_RELEASE $LEGEND_PURE_RELEASE == engine $LEGEND_ENGINE_RELEASE's own legend.pure.version ${want:-<unresolved: Central unreachable, or the release is not published>}"
fi

# INV-2: the SOURCE pins are the release TAGS' commits — the checkouts gates
# 1/4/5/8/9 read and the jars gates 6/7/8 load are then the SAME release. Jars
# exist only at release tags, so a source pin on an arbitrary commit makes an
# identical oracle IMPOSSIBLE (2026-09-10: the 4.137.0+36 pin was 20 commits
# ahead of the 4.138.2 tag and 11 behind it; docs/version-skew-claims.tsv's 25
# rows were the rent). The DESCRIBE must be the bare tag; whether the SHA is
# that tag's commit is checked by oracle_roots_check on a checkout that has the
# tag (tools/oracle-roots.sh).
inv "2a" "$([ "$LEGEND_ENGINE_DESCRIBE" = "legend-engine-$LEGEND_ENGINE_RELEASE" ] && echo 0 || echo 1)" \
  "SOURCE engine pin $LEGEND_ENGINE_DESCRIBE is the release tag legend-engine-$LEGEND_ENGINE_RELEASE"
inv "2b" "$([ "$LEGEND_PURE_DESCRIBE" = "legend-pure-$LEGEND_PURE_RELEASE" ] && echo 0 || echo 1)" \
  "SOURCE pure pin $LEGEND_PURE_DESCRIBE is the release tag legend-pure-$LEGEND_PURE_RELEASE"

# INV-2c: the pinned SHAs ARE the tags' commits — asked of the REMOTE (the
# peeled `^{}` ref of `git ls-remote --tags` for an ANNOTATED tag; the ref
# itself for a LIGHTWEIGHT one — upstream's tags are lightweight since the
# 4.14x "CI-friendly versions" release workflow: 4.138.2 was annotated,
# 4.145.0 / 5.99.0 are not), so no clone needs tags and CI's shallow clones
# are never consulted (USER 2026-09-10: no git against the oracle clones in
# CI). Network; skipped offline.
tag_commit() {  # tag_commit <owner/repo> <tag>
  local sha
  sha=$(git ls-remote --tags "https://github.com/$1" "refs/tags/$2^{}" 2>/dev/null | awk '{print $1}' | head -1)
  [ -n "$sha" ] || sha=$(git ls-remote --tags "https://github.com/$1" "refs/tags/$2" 2>/dev/null | awk '{print $1}' | head -1)
  echo "$sha"
}
if [ "$OFFLINE" = 1 ]; then
  say "  SKIP  INV-2c (offline: tag commits not asked of the remote)"
else
  for pair in "engine|$LEGEND_ENGINE_REPO|$LEGEND_ENGINE_DESCRIBE|$LEGEND_ENGINE_SHA" \
              "pure|$LEGEND_PURE_REPO|$LEGEND_PURE_DESCRIBE|$LEGEND_PURE_SHA"; do
    IFS='|' read -r name repo tag sha <<< "$pair"
    remote=$(tag_commit "$repo" "$tag")
    inv "2c" "$([ -n "$remote" ] && [ "$remote" = "$sha" ] && echo 0 || echo 1)" \
      "SOURCE $name SHA ${sha:0:9} is the commit tag $tag points to on the remote (${remote:-<unresolved: remote unreachable or no such tag>})"
  done
fi

# INV-3 (PCT jars == source) and the old ORACLE == SOURCE row are now
# consequences of INV-0 + INV-2: one pom property, one tag. Reported for the
# reader, not re-checked.
say "  (INV-3 PCT jars == SOURCE, and ORACLE == SOURCE, follow from INV-0 + INV-2: one property, one tag)"

# INV-4: the committed fixture snapshot was harvested from the ORACLE jars it
# is adjudicated against (tier C6 of the parser corpus). The filename carries
# the version; when the file ALSO carries it in its header (batch 2 of the
# program: `# engine=<version>` first line, asserted by the reader) the two
# must agree.
inv "4a" "$([ "$FIXTURE_NAME_VER" = "$LEGEND_ENGINE_RELEASE" ] && echo 0 || echo 1)" \
  "FIXTURE filename version $FIXTURE_NAME_VER == LEGEND_ENGINE_RELEASE $LEGEND_ENGINE_RELEASE"
inv "4b" "$([ -n "$FIXTURE_HEAD_VER" ] && [ "$FIXTURE_HEAD_VER" = "$LEGEND_ENGINE_RELEASE" ] && echo 0 || echo 1)" \
  "FIXTURE snapshot header engine=${FIXTURE_HEAD_VER:-<none>} == LEGEND_ENGINE_RELEASE (the reader, Corpus.engineFixtures, asserts the same)"

# INV-6: the four third-party versions the root pom manages for classpath
# convergence (INV-5, tools/classpath-convergence.sh) are the ENGINE RELEASE'S
# OWN managed versions, read from the pinned engine checkout's pom — derived,
# not chosen. Skipped when the checkout is absent (CI has it: gate-env).
ENGINE_POM=${LEGEND_ENGINE_ROOT:-$HOME/legend/legend-engine}/pom.xml
if [ -f "$ENGINE_POM" ]; then
  managed() {  # managed <artifactId> — the version the ROOT pom manages
    grep -A1 "<artifactId>$1</artifactId>" "$ROOT/pom.xml" | sed -n 's|.*<version>\([^<$]*\)</version>.*|\1|p' | head -1
  }
  for pair in "HikariCP|hikaricp.version" "commons-lang3|commons-lang3.version" \
              "httpcore|httpcore.version" "junit|junit.version"; do
    IFS='|' read -r art prop <<< "$pair"
    ours=$(managed "$art"); theirs=$(pom_prop "$ENGINE_POM" "$prop")
    inv "6" "$([ -n "$theirs" ] && [ "$ours" = "$theirs" ] && echo 0 || echo 1)" \
      "root pom manages $art at ${ours:-<none>} == engine $LEGEND_ENGINE_RELEASE's own <$prop> ${theirs:-<unresolved>}"
  done
else
  say "  SKIP  INV-6  (no engine checkout at $ENGINE_POM: managed third-party versions not compared)"
fi

say
if [ "$FAIL" = 0 ]; then say "all invariants hold."; else echo; echo "INVARIANT VIOLATIONS above — see docs/UPSTREAM_BOUNDARY_PROGRAM.md §3 A / §5."; fi
exit $FAIL
