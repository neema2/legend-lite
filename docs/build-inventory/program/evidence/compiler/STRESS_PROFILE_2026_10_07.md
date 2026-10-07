# The stress corpus end to end under Flight Recorder (2026-10-07)

**What ran.** `//core:stress_tool` over the stress corpus (`core/src/test/resources/stress`: 202 files, 344,824 lines,
4,745 service tests; D23's base) on DuckDB, one run on this desk, under
`-XX:StartFlightRecording=settings=profile,dumponexit=true` with `-XX:FlightRecorderOptions=repository=<a writable
folder>` (the recording's repository defaults to the JVM's temp folder, read-only under Bazel's output tree). Outcome:
4,705 pass, 15 fail, 16 skipped (the plan's §1b number is 4,700 pass; the 15 are the wrong-rows set D23 attributes).
Wall clock 5 minutes 26 seconds including the Bazel build and the JVM start; 3,669 execution samples. The aggregation is
`jfr_phases.py` (a sample belongs to the first phase found on its stack, top down), `jfr_other.py` (the unattributed
samples by their nearest legend frame; string, hash-map and line-index work by caller) and `jfr_callers.py` (callers and
leaf frames of named frames). DuckDB's own work is native and is not in these samples.

## By phase

| phase | samples | share | what it is |
|---|---|---|---|
| other | 1,210 | 33.0% | execution and loading (`exec` 146: the CSV seeds, the DuckDB appender), the model (119: the mapping include closure, below), SQL rendering (`sql.dialect` 96: `AnsiSqlRenderer.ident/render`, `RawSqlBoundary.quoteCreateColumns`), the test runner (89: `ServiceTestRunner`, provisions keyed by strings), Java streams and string work |
| parse | 598 | 16.3% | **14.4% of it is `MappingProtocolParser.readIsland`** (below); the rest is lexing |
| type | 485 | 13.2% | `Typer.synth` 149, `Typer.applyFunction` 123, `Overloads.checkGeneric` 87, `Overloads.synth` 70, `Typer.applyCore` 63 |
| store-resolve | 467 | 12.7% | `StoreResolver.resolveChain` 94, `resolveObject` 123 (inclusive), `anchoredNode` 73, `TemporalFrame`, `ClassSources.findBinding/bindsIn` (string-keyed maps): spread, no single hot spot |
| names | 465 | 12.7% | `BareNames.catalogTiered` 274, `catalog` 253, `tiered` 154, `ResolvedNames.referents` 143, `NameResolver.resolveVs` 92: the same frames as the whole-world profile |
| model | 186 | 5.1% | `ModelBuilder.findMapping` → `SymbolTable.resolveId` 87 (a hash lookup by the FQN string, `String.equals` on each) |
| lower | 134 | 3.7% | |
| inline | 58 | 1.6% | |
| probe/io | 39 | 1.1% | |
| normalize | 27 | 0.7% | |

## The hottest frames at the top of the stack

| frame | samples |
|---|---|
| `java.lang.AbstractStringBuilder.needsNewBuffer` | 299 |
| `java.util.HashMap.getNode` | 290 |
| `com.legend.lexer.Lexer.skipWhitespace` | 148 |
| `java.util.HashMap.putVal` | 145 |
| `com.legend.lexer.TokenStream.lineStarts` | 103 |
| `java.util.HashMap.resize` | 73 |
| `java.lang.StringLatin1.hashCode` | 63 |
| `java.lang.String.equals` | 53 |
| `java.lang.StringLatin1.lastIndexOf` | 47 |

## Finding 1: islands are copied, padded and lexed again (14.4% of the run)

`MappingProtocolParser.readIsland` (`core/src/main/java/com/legend/parser/MappingProtocolParser.java`, the
`IslandBlock` record): for every island (`#{ … }#`: test data, external-format blocks, embedded values, assertions)
the reader copies the island's text into a new string **padded with one newline per line before it and one space per
column**, so that the re-lexed text reports the island's true line and column, then runs the lexer on that string and
gives the result its own line index. The comment above it cites the 2026-08 deep audit's H2 ("the island path was
O(K·N) — twelve call sites each rescanned the whole prefix per island") and the fix (the stream's cached line index for
`lineOf`); the padding kept the cost.

Of `readIsland`'s 528 inclusive samples the leaves are `AbstractStringBuilder.needsNewBuffer` 299 (the padding loop,
one character per append), `Lexer.skipWhitespace` 134 (the lexer skipping the padding) and `TokenStream.lineStarts` 81
(the line index of the padded string). Callers: `parseExternalFormat` 187, `assertionValue` 105, `parseEmbeddedValue`
101. `Lexer.tokenize` is 163 inclusive, 143 of them from `readIsland`. The corpus file `94-fanout-services.pure` holds
14,948 of the corpus's 15,134 islands in 291,278 lines: about 2.2 billion characters of padding appended, skipped and
indexed over the run.

Done right, a nested grammar is parsed from the same token stream — a slice with the island's bounds, the line index
shared, as `TokenStream.slice` already does for sections — or by a lexer mode; no copy, no padding, no second lexing.
Spans stay what they are today (the padding exists only to make them right). Judges: the parser-parity lane (spans
unchanged), the stress corpus's rows, this profile.

## Finding 2: the mapping include closure is walked on every lookup (2.5%)

`MappingDefinition.withIncludes` (`core/src/main/java/com/legend/model/MappingDefinition.java`) walks the includes
transitively — a hash set of names, a `find` per include, the package-relative retry — every time `ofClasses` asks for
the bindings of some classes: 93 inclusive samples, 65 of them `HashMap.putVal`, 13 `HashMap.resize`. The closure is a
fact of the built model; it changes only when the model does. Beside it, `ModelBuilder.findMapping` resolves the
mapping's FQN string through `SymbolTable.resolveId` on each call (87 samples, 25 of them `String.equals`): identity by
spelling, the plan's rule 0b.8's subject (no string identity for a declaration).

## What is not a hot spot

The store resolver's 12.7% and the lowering's 3.7% are spread over many frames; the biggest, `resolveObject`, is 3.4%
inclusive and its leaves are its own logic. Nothing in the back half re-derives a whole-model fact per call the way the
names and the islands do. Its cost is its structure (the one-pass 36k-line resolver), which is the plan's W4.3 and the
D11 question, not a profile fix.
