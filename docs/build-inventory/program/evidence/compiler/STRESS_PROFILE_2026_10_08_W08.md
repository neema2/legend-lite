# The executed stress run before and after W0.8, on one quiet desk (2026-10-08)

Two runs of `//core:stress_tool` over the stress corpus on DuckDB, minutes apart on the same desk, each started by a poll
that waited for the load average to fall under 5 with no other Java or Bazel process above 100% CPU (before: load 2.9;
after: load 4.5), each under `-XX:StartFlightRecording=settings=profile,dumponexit=true` with a writable repository;
aggregated by `jfr_phases.py`. The verdicts are the same in both (4,705 pass, 15 fail, 16 skipped of 4,736).

| | before (main 2494bfb59) | after (W0.8) |
|---|---|---|
| the model's parse+build (7,610 elements) | 5,645 ms | 1,516 ms |
| the run's wall clock (model + tests) | 19.3 s | 15.7 s |
| execution samples | 808 | 488 |
| parse share | 45.5% (368) | 3.9% (19) |

`//core:compile_latency --corpus stress --passes 1`, three quiet runs after: parse+build 1,848 ms, 1,622 ms, 1,451 ms (the
baseline receipt of the same morning, before: 6,550 ms; the receipt's loaded runs 16 to 55 s). The per-query latency is
unchanged within noise (the fix touches the model's build only).

The 2026-10-07 profile (`STRESS_PROFILE_2026_10_07.md`, 3,669 samples) attributed 14.4% of a longer, busier run to
`readIsland`; on a quiet desk the model build is a larger share of a shorter run, which is why "before" reads 45.5% here.
The numbers to compare are the two rows above, taken alike.

## Before, by phase

samples: 808 over 1 recording(s)

| phase | samples | share |
|---|---|---|
| parse | 368 | 45.5% |
| other | 162 | 20.0% |
| names | 77 | 9.5% |
| store-resolve | 75 | 9.3% |
| type | 50 | 6.2% |
| model | 31 | 3.8% |
| lower | 22 | 2.7% |
| normalize | 14 | 1.7% |
| inline | 9 | 1.1% |

| hottest frame at the top of the stack | samples |
|---|---|
| `java.lang.AbstractStringBuilder.needsNewBuffer` | 171 |
| `com.legend.lexer.Lexer.skipWhitespace` | 108 |
| `com.legend.lexer.TokenStream.lineStarts` | 61 |
| `java.util.HashMap.getNode` | 54 |
| `java.util.stream.ReferencePipeline$3$1.accept` | 15 |
| `java.util.HashMap.put` | 12 |
| `java.lang.StringLatin1.hashCode` | 10 |
| `com.legend.parser.TokenStreamCursor.peek` | 8 |
| `java.util.HashMap.resize` | 8 |
| `com.legend.compiler.NameResolver.resolveVs` | 7 |
| `java.util.ImmutableCollections$SetN.probe` | 7 |
| `java.util.ArrayList.addAll` | 7 |

| hottest legend frame, inclusive | samples | share |

## After, by phase

samples: 488 over 1 recording(s)

| phase | samples | share |
|---|---|---|
| other | 180 | 36.9% |
| names | 81 | 16.6% |
| store-resolve | 74 | 15.2% |
| type | 48 | 9.8% |
| model | 38 | 7.8% |
| lower | 22 | 4.5% |
| parse | 19 | 3.9% |
| normalize | 12 | 2.5% |
| inline | 12 | 2.5% |
| probe/io | 2 | 0.4% |

| hottest frame at the top of the stack | samples |
|---|---|
| `java.util.HashMap.getNode` | 53 |
| `java.util.HashMap.putVal` | 24 |
| `java.util.HashMap.resize` | 19 |
| `java.lang.StringLatin1.hashCode` | 13 |
| `com.legend.compiler.NameResolver.resolveVs` | 10 |
| `java.util.Arrays.copyOf` | 10 |
| `java.util.stream.ReferencePipeline$2$1.accept` | 9 |
| `java.util.stream.ReferencePipeline$3$1.accept` | 8 |
| `java.util.ArrayList.addAll` | 6 |
| `java.lang.StringLatin1.replace` | 5 |
| `java.lang.StringLatin1.lastIndexOf` | 5 |
| `java.util.ImmutableCollections$AbstractImmutableList.iterator` | 4 |

