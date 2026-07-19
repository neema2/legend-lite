# Audit PCT-A — Overfit / Hardcoding, range 6ddcf1bb..HEAD (PCT-native arc)

Auditor: PCT-A (overfit/hardcoding). Range: A0 census through A3 assert vocabulary +
'#TDS' presentation (`dc6086d1..80be7659`; diff read `dc6086d1..HEAD` hunk by hunk).
Ground truth: /Users/neemsandv/legend/legend-pure and /Users/neemsandv/legend/legend-engine
checkouts, primarily `core_functions_relation/relation/functions/{toString,s,tdsEquivalent}.pure`,
`platform/pure/essential/tests/{assertError,pct_core}.pure`,
`legend-pure-m4/.../grammar/StringEscape.java` (+ commons-lang3 translate semantics),
`AntlrContextToM3CoreInstance.java` (sliceExpression), `M3CoreParser.g4`, `M4Fragment.g4`,
interpreted `AssertError.java`, `PureTestBuilderInterpreted.java`.

## Verdict

No wrong-layer HIGH of the 20a-COALESCE kind, but the range's presentation and
equivalence natives are **subset ports calibrated to the pinned grids**: `tdsCell`
implements exactly the three arms the pinned '#TDS' texts exercise and silently drops
s.pure's Variant arms, the type-directed `'null'` spelling, the header quote/trim rule,
and the empty-relation blank line (F1); `tdsEquivalent` keys its delta/date arms on
runtime VALUE classes where the real function keys on DECLARED column types — the one
false-PASS channel found in the range — and uses fractional seconds where real pure's
`dateDiff(...,SECONDS)` truncates (F5). The native-replaces-stub dedupe is faithful to
real pure's rule for `native function` stubs but silently extends it to CONCRETE
reference bodies (coalesce.pure et al.), which real pure executes and lite discards —
any lite-native divergence from a discarded body is permanently silent, and an
identical-signature user redefinition is silently dropped where real pure raises a
loud duplicate error; misaligned signatureKeys, by contrast, land in the overload set
and die loudly at the tie throw (F2). assertError's message-exact policy matches the
interpreted native, but line/column pins are silently unverified per-test (documented
P3 debt) and compile-time errors are conflated with runtime raises (F3). The escape
port is exact for octal, near-exact for unicode (misses commons' `\u+XXXX` arm, F4).
The range-literal desugar, the identity-adapter binding, the lexer date/float rules,
the zero-arg multi-statement lambda typing, and the whole consume-and-drop parser set
audit clean against the grammar and upstream mechanisms.

**Counts: HIGH 0, MED 4 (F1, F2, F3, F5), LOW 5 (F4, F6, F7, F8, F9).**

---

## F1 — MED — CONFIRMED-OVERFIT (subset port): relationToString/tdsCell implement the pinned grid arms, not toString.pure + s.pure

`core/src/main/java/com/legend/StatementExecutor.java:149-162` (interception arm),
`:219-255` (relationToString), `:257-265` (tdsCell);
`core/src/main/java/com/legend/builtin/Pure.java:1228-1233` + `core/src/test/resources/native-catalog.txt:509-510`
(both arities cataloged); `PlatformTypes.java:51-54,75-82` (platform-owned suppression).

Real mechanism: `meta::pure::functions::relation::toString` is a CONCRETE
`<<PCT.function, PCT.platformOnly>>` body (engine `core_functions_relation/relation/functions/toString.pure:19-36`)
— `toString(rel)` delegates to `toString(rel,false)`, which builds the header from
`columns()` with **`$c.name->trim()` and quoting for non-simple names**
(`isSimple = startsWith('\'') || matches('^[a-zA-Z0-9_]+$')`), and spells each cell via
**`s($val, $columnType)`** (`s.pure:23-38`). The code comment says "the way real pure's
TestTDS toString does" — the actual owner is toString.pure's body; the spelling label
is wrong even though the pinned text shape coincides.

Divergences where no pin looks (all fail-visible when a test does pin — string-exact
assertEquals — so MED, not HIGH):
1. **Header**: lite prints `t.columns().get(c).name()` raw — no trim, no quoting. A
   non-simple column name (spaces, quotes) renders unquoted where real pure quotes it.
2. **Empty relation**: real emits `header\n` + `''` + `\n#` → a BLANK line before `#`
   (`joinStrings` over zero rows); lite emits `header\n#`. No pinned empty-grid test
   found in the relation suite — purely silent.
3. **tdsCell vs s.pure**: s.pure's `Variant[1]` arm ALWAYS quotes (with `\\`/`\'`
   escapes) and its empty-cell arm spells **`'null'` (quoted) when the column type is
   Variant** (`s.pure:26-31`); lite has neither — every null prints bare `null`, and a
   variant cell is quoted only by the accident of containing `{`/`[`.
4. **Cell fallback**: real is pure `toString()` semantics; lite is Java
   `String.valueOf` on the JDBC object. Temporals diverge (Timestamp
   `2000-10-25 06:30:00.0` vs pure `2000-10-25T06:30:00.000+0000`) — VISIBLE today as
   the four asOfJoin FAILs in the generated `docs/PCT_CORPUS.md` (honest direction),
   plus latent float-spelling edges (`Double.toString` scientific notation vs pure).
5. **`toString(rel,true)`** is cataloged and TYPES but has no implementation anywhere
   (the interception arm requires `args.size()==1`); it dies at
   `Scalars.lower`'s "no scalar lowering registered" (`Scalars.java:2176-2184`) — loud,
   defensible, but the catalog promises an arity the platform cannot execute.

Principled shape: port toString.pure's header rule and s.pure's match arms (they are
five lines of logic each), and emit the blank line for the zero-row case.

## F2 — MED — DEFENSIBLE-DIVERGENCE with a silent edge: native-replaces-stub extends real pure's rule from native stubs to concrete reference bodies

`core/src/main/java/com/legend/compiler/element/FunctionCompiler.java:41-67`;
key = `Function.signatureKey()` (`core/src/main/java/com/legend/model/Function.java:51-65`
— FQN + per-param `TypeExpression.toString` + multiplicity, no param names, no return).

Real mechanism, verified: for `native function` declarations the runtime supplies the
implementation and the declaration is only the signature (compiled runtime
`NativeFunctionProcessor.java:298-320` registers Natives by function id) — for THOSE
corpus declarations the dedupe is faithful. But the PCT suites also carry CONCRETE
`<<PCT.function>>` implementations (e.g. all six `flow::coalesce` overloads,
engine `core_functions_unclassified/flow/coalesce.pure:18-46`, are real bodies real
pure EXECUTES). Lite catalogs natives at those exact signatures, so the dedupe
discards the reference bodies and dispatches lite's own lowering. Consequences:
1. **Silent-divergence channel**: wherever a lite native's semantics stray from the
   discarded body, no error can ever surface outside a pinned assert — the ground-truth
   implementation never runs. This is the audit-20a keying concern at the dispatch
   layer: correctness now rests on each native having been verified against the body
   it shadows, and nothing enforces that at the merge point.
2. **User shadowing is silent**: an identical-signature user redefinition of any
   catalog native is dropped with NO diagnostic (contrast the platform-owned arm's
   stderr at :69-73). Real pure raises a loud duplicate-function compile error for the
   same spelling. Tenet-3 inversion (silent where the reference is loud).
3. **Misalignment is loud (good)**: user functions are prelude-resolved to catalog
   FQNs before `findFunction`, so aligned keys dedupe; when keys DON'T align (type-param
   name drift, unresolved spellings) both candidates survive and any call hits the
   ambiguity throw (`InferenceKernel.java:697-743` "candidates tie") — an accounted
   ERROR, never a silent winner. The feared "silent duplicate survives" does not occur.

Minimum fix: stderr-once on every dedupe drop (parity with the platform-owned arm);
principled fix: dedupe only `NativeFunctionDefinition` corpus stubs, and route concrete
collisions through a verify-or-wall decision rather than unconditional native
preference.

## F3 — MED — DEFENSIBLE-DIVERGENCE (documented): assertError verifies message only; positions silently unchecked, compile-time raises conflated with runtime

`core/src/main/java/com/legend/harness/TestBody.java:639-670`.

Real mechanism, verified: `assertError.pure:22-35` — the 4-arg form asserts
`assertEquals($message,$msg)` (exact) and, WHEN the line/column args are non-empty,
asserts them against the raised error's real `SourceInformation`; the interpreted
native (`AssertError.java:75-95`) catches only `PureExecutionException` from
EXECUTING the thunk and passes `e.getInfo()` (position-stripped) to the matcher.
Lite matches: exact message equality on `e.getMessage()`, matcher-lambda form →
SHAPE, `NotImplementedException` rethrown → SHAPE (honest), no-raise → FAIL. Two gaps:
1. Line/column args are accepted and IGNORED — `testSimpleAssertErrorLine`-shaped
   tests count PASS on message alone; the positional half of the pin is silently
   unverified and the scoreboard does not distinguish these from fully-verified
   passes. Documented as P3 debt in `docs/PCT_NATIVE_PLAN.md` (and the plan's end
   state requires line to become real, column advisory) — but per-test the drop is
   silent today.
2. Lite catches ANY `RuntimeException | SQLException` from `evalScalar`, including
   its own compile/type-time errors; real pure only treats EXECUTION errors as the
   asserted raise (a thunk compile error fails the test outright). A lite type-error
   message that coincidentally equals the expected runtime message passes by the
   wrong mechanism. Narrow, but it is a mechanism conflation, not a port.

## F4 — LOW — CLEAN core, one missing arm: escape decoding vs StringEscape.UNESCAPE_PURE

`core/src/main/java/com/legend/parser/SpecParser.java:769-830`;
ground truth `legend-pure-m4/.../StringEscape.java:39-42` (commons-**lang3**
translate, not commons-text as the comment says — same semantics).

Verified exact: octal arm reproduces OctalUnescaper precisely (up to 3 digits, 3rd
only when the FIRST digit ≤ '3', max `\377`; `'\477'` → `'7` pinned at
`SpecParserTest.java:136-146`); repeated-`u` handling matches UnicodeUnescaper; the
terminal drop-backslash rule and `[btnfr"'\\]` set match the aggregate's lookup
tables; malformed hex and short escapes are loud in both. One divergence: commons'
UnicodeUnescaper also skips an optional **`+` after the u's** (`\u+0041` → 'A');
lite feeds `+004` to `parseInt(hex,16)` (Java accepts the sign) yielding ``
plus a literal `1` — silently different, no corpus pin. Add the `+` skip or go loud
on it.

## F5 — MED — CONFIRMED-OVERFIT (keying layer): assertTdsEquivalent keys tolerance arms on runtime values, not declared column types

`core/src/main/java/com/legend/harness/TestBody.java:615-637` (assert),
`:1575-1585` (literalNumber), `:1588-1634` (tdsEquivalent), `:1636-1650` (epochSeconds);
ground truth engine `relation/functions/tdsEquivalent.pure:26-50`.

Faithful parts: ordered column-NAME equality (types deliberately not compared —
upstream `testDifferentNumTypes` pins Number≡Integer), row-count equality, ordered
row-wise cells, both-empty equal, `<= abs(delta)` inclusive bound, non-literal
delta args → SHAPE (loud). Divergences:
1. **Arm keying**: real selects the Number/Date/Variant tolerant arms by the
   DECLARED column type (`subTypeOf(Number)` etc. on `classifierGenericType`); lite
   selects by `instanceof Number` / temporal on the CELL VALUE. On an Any-typed
   column holding numbers, real demands exact `==` while lite grants delta
   tolerance — a **false-PASS channel** (the only silent-pass direction found in
   this range). Unpinned today; will bite exactly where no test looks.
2. **Sub-second truncation**: real compares `dateDiff(...,SECONDS)` — an INTEGER,
   truncating sub-second differences (upstream `testAssertTdsEquivalentWithTimeDelta`
   passes a 1ms difference at timeDelta 0); lite compares fractional epoch seconds
   and would FAIL that shape. Visible direction, but it is the real function's
   documented behavior traded for a stricter invention.
3. No Variant arm (real compares variant columns by `toString()` equality); lite
   falls to `wireEquals` on whatever the wire holds — coincidence-dependent.

## F6 — LOW — CLEAN: range-literal desugar is arity-faithful to sliceExpression

`core/src/main/java/com/legend/parser/SpecParser.java:949-981`.
Verified against `AntlrContextToM3CoreInstance.java:712-760` and grammar
`M3CoreParser.g4:375` — exactly three forms (`[:end]` → `range(end)`,
`[start:end]` → `range(start,end)`, `[start:end:step]` → `range(start,end,step)`),
no `[start:]` form upstream either, simple-name `"range"` resolves to the cataloged
1/2/3-arg overloads (`native-catalog.txt:460-462`). Mixing with commas is a loud
parse error in both. No divergence found.

## F7 — LOW — CLEAN/DEFENSIBLE: the identity-adapter binding is β-equivalent to the upstream in-memory adapter; the expected-failure ledger is (honestly) absent

`pct-corpus/src/test/java/com/legend/pctcorpus/PctCorpusRunner.java:153-186`
(runTest + identityAdapter), `:189-201` (importScopeOf), `:203-222` (classify),
`:256-265` (isPctTest).

Ground truth: the reference adapter is literally
`testAdapterForInMemoryExecution<X|o>(f:Function<{->X[o]}>[1]):X[o] { $f->eval(); }`
(`pct_core.pure:29-37`); upstream `buildPCTTestSuite`
(`PureTestBuilderInterpreted.java:80-118`) wraps the adapter function instance as the
single test argument. Lite's `let f = {g|$g->eval()}` prefix is semantically
identical for every conforming payload (`$f->eval(|expr)` β-reduces to `expr` in
both), 0-param tests get no binding (matches `isPCTTest` gating), ≠1-param tests go
SHAPE (loud). Two honest deltas, neither overfit: (1) upstream consults a per-adapter
expectedFailures ledger (`explodeExpectedFailures`) — lite counts those as plain
FAIL/ERROR, so the scoreboard's PASS total is not directly comparable to the official
adapter's 1109 (the plan's "denominator is what's on disk" note covers this);
(2) `isPctTest` accepts the bare `"PCT"` profile spelling alongside the exact FQN —
bounded, but a user profile whose simple name is `PCT` would false-match (tenet-4
nit; corpus-only surface today).

## F8 — LOW — CLEAN: lexer date/float extensions are grammar-grounded

`core/src/main/java/com/legend/lexer/Lexer.java:380-386` (negative-year date),
`:572-577` (leading-dot float). `M4Fragment.g4:21` spells
`Date: '%' ('-')? (Digit)+ ...` and `:17` `Float: (Digit)* '.' (Digit)+ ...` —
both forms are real pure's lexer, not pin accommodations; the `DOT_DOT` check
precedes the float arm so `1..2` is unaffected, and property names cannot begin
with a digit.

## F9 — LOW — CLEAN with two nits: the consume-and-drop parser set is grammar-verified

`core/src/main/java/com/legend/parser/ElementParser.java:340-349`+`:448-460`
(Measure skip → absent element), `:391-403` (class type variables),
`:469-489` (`<T|m>`/`<|m>` multiplicity params), `:809-811` (profile
stereotypes/taggedValues before name), `:1599-1617` (taggedValue `+` concatenation);
`SpecParser.java:1421-1438` (`^X name(...)` + `^X(10)(...)` type-variable values),
`:1564-1575` (`<|*>`); `TokenStreamCursor.java:641-652` (`(?:?)` wildcard).

Every construct verified against `M3CoreParser.g4`: `:33` (classDefinition —
typeVariableParameters), `:47`, `:188`/`:359-360` (instance name +
typeVariableValues), `:234-238` (`taggedValue: ... STRING (PLUS STRING)*`),
`:249` (profile: `PROFILE stereotypes? taggedValues? qualifiedName` — and
`pct_core.pure:23` is the live use), `:457` (`<|*>`), `:481-484`
(`mayColumnType: (QUESTION | type)`). Dropped constructs fail loud downstream
(unknown unit/type-var references; `?` maps onto the existing
`InferenceKernel.UNKNOWN_COLUMN_TYPE` machinery). The `parseSingle` fix (a bare
`error(...)` call that never threw) turns a silent no-op into a real rejection —
an anti-overfit improvement. Nits: (a) lite parses `<T>` BEFORE the `(vars)`
group — the grammar puts typeVariableParameters first, so the combined real
spelling `Class X(x:Integer[1])<T>` fails loud in lite while lite accepts a
reversed spelling real pure rejects; (b) tagged-value strings are unquoted but
never unescaped (pre-existing, shared with the old single-string path).

---

Cross-cutting note: the toString-tail change in `TestBody.java:1054-1071` (toString
no longer stripped; compare is string-exact) is the right ownership move and makes
every F1 divergence fail-visible where pinned; the residual silent surface is
exactly the unpinned spellings enumerated above.
