#!/usr/bin/env bash
# The D0 census's mechanical half (2026-11-10): every SQL raise in main and its literal text, the engine-text
# reachers, and the "engine/pure" comment hits. Run from the repository root; read-only.
set -u
M=core/src/main/java/com/legend
echo "## 1. SQL raises (SqlFn.ERROR, Scalars.guarded, PureSql.raise) and the string literal nearest each"
grep -rn "SqlFn.ERROR\|guarded(\|PureSql.raise(" --include=*.java $M/lowering $M/sql/dialect/AnsiSqlRenderer.java \
  | grep -v "static SqlExpr guarded\|static com.legend.sql.SqlExpr raise" | while IFS=: read -r f l _; do
    txt=$(sed -n "${l},$((l+8))p" "$f" | grep -o '"[^"]*"' | grep -v '^"[$:]*"$' | head -3 | tr '\n' ' ')
    echo "$f:$l  $txt"
  done
echo
echo "## 2. Who builds an engine-text dialect"
grep -rn "new EngineStyle\(H2\|DB2\|Composite\)" --include=*.java $M
grep -rn "EngineText\.\(engineText\|enginePlanText\)(" --include=*.java $M
echo
echo "## 3. Comments naming upstream's text"
grep -rn -i "real pure's message\|pure's message\|pure's own message\|engine's words\|legend-engine's words\|in legend-engine's\|engine-corpus-compat\|as DuckDB prints" --include=*.java $M | cut -c1-200
