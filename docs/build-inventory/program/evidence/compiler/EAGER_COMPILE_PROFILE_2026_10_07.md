# The eager corpus compile under Flight Recorder (2026-10-07)

The whole corpus world's bodies typed up front (`bazel build //spec:eager_corpus_compile`), run three times as the action's own command under `-XX:StartFlightRecording=settings=profile` (with `-XX:FlightRecorderOptions=repository=<a writable folder>`: JFR's repository defaults to `java.io.tmpdir`, which the action sets to a read-only folder under bazel-out). The design that reads this: `docs/COMPILER_RIGHT_DESIGN_2026_10_07.md` §0. Aggregated by `jfr_phases.py` beside this file.

## The probe's own summary, per run

    # eager corpus compile — bodies=9942 failed=1620 build=1799ms typeAll=2091ms
    # by reason: {kernel/other=464, overload=56, unknown-function=611, unknown-property=3, unknown-type=486}
    # eager corpus compile — bodies=9942 failed=1620 build=1811ms typeAll=2041ms
    # by reason: {kernel/other=464, overload=56, unknown-function=611, unknown-property=3, unknown-type=486}
    # eager corpus compile — bodies=9942 failed=1620 build=1827ms typeAll=2139ms
    # by reason: {kernel/other=464, overload=56, unknown-function=611, unknown-property=3, unknown-type=486}

## The samples

samples: 809 over 3 recording(s)

| phase | samples | share |
|---|---|---|
| names | 322 | 39.8% |
| type | 194 | 24.0% |
| other | 135 | 16.7% |
| parse | 84 | 10.4% |
| model | 35 | 4.3% |
| normalize | 27 | 3.3% |
| probe/io | 10 | 1.2% |
| inline | 2 | 0.2% |

| hottest frame at the top of the stack | samples |
|---|---|
| `java.util.HashMap.getNode` | 89 |
| `java.util.ArrayList.removeIf` | 37 |
| `com.legend.compiler.BareNames.catalogTiered` | 35 |
| `com.legend.compiler.NameResolver.resolveVs` | 27 |
| `java.util.HashMap.putVal` | 26 |
| `java.util.HashMap.resize` | 23 |
| `java.util.HashMap.hash` | 18 |
| `com.legend.compiler.NameResolver.resolveCallCandidates` | 17 |
| `java.util.ImmutableCollections$SetN.probe` | 14 |
| `java.util.Collections$UnmodifiableMap.get` | 14 |
| `java.lang.StringLatin1.lastIndexOf` | 13 |
| `java.util.LinkedHashMap.afterNodeInsertion` | 12 |

| hottest legend frame, inclusive | samples | share |
|---|---|---|
| `com.legend.compiler.BareNames.catalogTiered` | 195 | 24.1% |
| `com.legend.compiler.BareNames.catalog` | 163 | 20.1% |
| `com.legend.compiler.NameResolver.resolveVs` | 106 | 13.1% |
| `com.legend.compiler.ResolvedNames.referents` | 88 | 10.9% |
| `com.legend.compiler.BareNames.tiered` | 87 | 10.8% |
| `com.legend.compiler.NameResolver.lambda$resolveVsList$0` | 77 | 9.5% |
| `com.legend.compiler.NameResolver.resolveList` | 74 | 9.1% |
| `com.legend.compiler.spec.Typer.synth` | 61 | 7.5% |
| `com.legend.compiler.NameResolver.resolveCallCandidates` | 54 | 6.7% |
| `com.legend.compiler.spec.Typer.applyFunction` | 54 | 6.7% |
| `com.legend.compiler.NameResolver.resolveVsList` | 48 | 5.9% |
| `com.legend.compiler.spec.Overloads.checkGeneric` | 40 | 4.9% |
| `com.legend.builtin.Pure.nativeFunctionsAt` | 31 | 3.8% |
| `com.legend.compiler.NameResolver.addKnown` | 27 | 3.3% |
| `com.legend.compiler.ResolvedNames.names` | 24 | 3.0% |
| `com.legend.parser.ElementParser.parse` | 23 | 2.8% |
| `com.legend.compiler.spec.Overloads.synth` | 23 | 2.8% |
| `com.legend.compiler.spec.Typer.applyCore` | 20 | 2.5% |
| `com.legend.compiler.spec.TdsDesugars.tdsSchemaDesugars` | 20 | 2.5% |
| `com.legend.lexer.Lexer.run` | 19 | 2.3% |
| `com.legend.lexer.Lexer.tokenize` | 18 | 2.2% |
| `com.legend.rcorpus.MinimalCorpus.<init>` | 18 | 2.2% |
| `com.legend.parser.ElementParser.parseSingleElement` | 18 | 2.2% |
| `com.legend.compiler.NameResolver.resolveNameMulti` | 18 | 2.2% |
| `com.legend.compiler.spec.Overloads.checkWithDeferred` | 18 | 2.2% |
| `com.legend.lexer.Lexer.scanNormalToken` | 17 | 2.1% |
| `com.legend.compiler.spec.Typer.accessProperty` | 17 | 2.1% |
| `com.legend.compiler.spec.InferenceKernel.resolveOverload` | 16 | 2.0% |
| `com.legend.rcorpus.EagerCorpusCompileProbe.main` | 14 | 1.7% |
| `com.legend.parser.ElementParser.parseModel` | 14 | 1.7% |
| `com.legend.compiler.spec.Overloads.checkGenericTyped` | 14 | 1.7% |
| `com.legend.Compiler.parseSources` | 13 | 1.6% |
| `com.legend.compiler.spec.Overloads.applyGeneric` | 13 | 1.6% |
| `com.legend.builtin.TdsLegacy.bare` | 13 | 1.6% |
| `com.legend.compiler.spec.InferenceKernel.resolveChosen` | 13 | 1.6% |
| `com.legend.compiler.ResolvedNames.declaredNatives` | 12 | 1.5% |
| `com.legend.compiler.spec.ReceiverOwnedFunctions.of` | 12 | 1.5% |
| `com.legend.Compiler.buildModule` | 11 | 1.4% |
| `com.legend.compiler.spec.Overloads.bindDeferredAndBuild` | 11 | 1.4% |
| `com.legend.compiler.spec.Overloads.typeLambda` | 11 | 1.4% |
