# Parser completeness

> **New here?** Read [HANDOFF.md](HANDOFF.md) first — it is written for someone with no
> context and covers state, findings, the work queue and the traps. This file is design.

A harness that answers one question: **is legend-lite's parser complete, and does it refuse
exactly what legend-engine refuses?**

## Why this exists separately from the corpus

The stress corpus tests what happens *after* parsing — mappings, execution paths, results
against real databases. It is very good at that and structurally blind to this: a construct
nobody happened to write is invisible to it. Thirty-plus files, 89 services, 210 tables, and
it had never written `let`, `Schema`, `or`, or `is null`.

There is also a keyword census in parser-equivalence, and it answers a different question —
*does this keyword appear anywhere in legend-engine's own corpus*. That is the right
denominator for "has the parity harness seen this grammar arm" and the wrong one for "is the
parser complete": ninety percent of upstream's fixtures says nothing about the other ten
percent of the grammar.

This measures the **grammar** directly, and grades coverage on fixtures that parse.

## Running it

```
cd scripts/parser
python3 fixtures.py            # coverage + both harnesses; exit non-zero on any problem
python3 fixtures.py --gaps     # what is still missing, by grammar
bazel run //scripts/parser:keywords -- --tier1   # the raw keyword census over the pinned grammars (Bazel)
python3 mutants.py             # regenerate the mutation manifest
python3 mutants.py --check     # fail on drift instead of overwriting
python3 mutants.py --accepted  # the review queue: mutations legend-engine tolerated
```

Needs `tools/engine-runner` built (see its README). Everything runs in one JVM.

## What counts as coverage

A keyword counts when a fixture **parses** and claims it:

```
// COVERS RelationalLexerGrammar: Schema, TabularFunction, MultiGrainFilter
```

The claim is checked three ways — the grammar must exist, the keyword must be one of *its*
keywords, and the fixture's own code must contain it outside comments and string literals. A
declaration that lies fails before it can inflate anything.

`COVERS-EMBEDDED` is the same thing for sub-grammars whose input *is* a string literal.
FlatData is written inside the quoted `content:` of a SchemaSet and its `.g4` re-declares
`STRING` and `BRACE_OPEN` precisely so it can parse standalone; for that grammar the string
body is not prose around code, it is the code.

Existing sources count too, attributed by `###Section`, so nobody re-authors a fixture for
`Database` because the stress corpus happens to spell it. Section attribution is not an
approximation of legend-engine's routing — the section is where it dispatches to a grammar.

**Parse, not compile.** A fixture exercising `~distinct` does not need a runtime, a
connection and seeded data to prove the parser accepts `~distinct`. Demanding compilation
would make every fixture ten times the size and make the coverage number quietly depend on
the compiler, which is the next stage's problem.

## Negative fixtures

`negative/` holds constructs that **must** be rejected, and each declares why:

```
// REJECTS: Field 'auth' should be specified only once
```

checked as a substring of the parser's actual message. This is not ceremony. A negative
fixture asserting only "this was rejected" is satisfiable by a typo, and would keep passing
forever while the construct it guards quietly started working.

It also guards a failure mode that is easy to miss: **a missing grammar extension makes any
content in that section look correctly rejected.** Four vendor grammar extensions really
were absent from the runner classpath, and `specification: Athena` was failing with
"Unsupported Data Source Specification type" — a packaging gap wearing a grammar limit's
clothes. Pinning the message is what distinguishes the two.

## The denominator, and what is excluded from it

Not every declared keyword is reachable. Five categories are excluded, each mechanically
detected or empirically confirmed, with the evidence recorded in `tiers.py` rather than
asserted:

- **Out of scope** — grammars no user can reach by typing into a `.pure` file. Haskell,
  Protobuf3, MongoDBQuery, SqlBase: none registers a `SectionParser` or an
  `EmbeddedPureParser`. Each was traced to its only non-test caller.
- **Dead tokens** — declared by a lexer, referenced by no parser rule. Found mechanically
  via ANTLR's `tokenVocab` link. `AuthenticationStrategyLexerGrammar` has ten.
- **Walker-rejected** — a parser rule exists, the tree builds, the walker refuses. `native`,
  `projects`, `allVersionsInRange`, GraphQL `extend`, `EqualToTDS`, plus two island types
  whose processor has zero implementors anywhere (`HostedService.actions`,
  `DataQualityValidation.persistenceStrategy`). Every one confirmed by running it.
- **Unshipped section** — the tokens are referenced by parser rules and the rules parse, but
  the `SectionParser` that would route text to them lives in `src/test/java`.
  `###AuthenticationDemo`. Invisible to both checks above: nothing is dead, nothing is
  refused — the routing simply is not there.
- **Version skew** — declared by the `.g4` working copy at git HEAD but absent from the
  runner's released jars. `mappingProvider`, and DataQuality's `testSuites`/`data`/`tests`/
  `asserts`. See below; this one nearly invalidated the whole number.

A target containing keywords no fixture can cover makes 100% impossible and turns the number
into a permanent accusation instead of a goal. They are pinned by negative fixtures instead — legend-lite must refuse them too, and if upstream ever makes one *work*,
the negative fixture fails and says so.

## Things that went wrong, kept here so they do not recur

Every correction to this measurement has moved the number **down**. That is the direction to
expect when a measurement stops flattering itself, and a correction that moves it up
deserves suspicion.

| Bug | Effect |
|---|---|
| counted keywords in comments and `###Data` payloads | 186 → 122 |
| credited a keyword to a grammar whose section did not define it | 97 → 83 |
| word boundaries applied unconditionally | `->subType(@` uncoverable by construction |
| composite token `'include '[a-z]+' '` kept only its first fragment | `include` uncoverable |
| GraphQL byte-order marks harvested as keywords | demanded fixtures for a file encoding |
| grammars keyed by file stem | two Snowflake grammars merged into one bucket |
| `//` inside a string literal stripped as a comment | swallowed real code to end of file |
| census read `.g4` at git HEAD, oracle ran released jars | measured a parser nobody runs |

That last one is the one to remember. The denominator came from a legend-engine **working
copy**; every verdict came from **4.138.2 jars**. Five keywords existed only in the former,
so no fixture could ever cover them, and "100%" would have been a claim about a parser
nobody was running. The fix takes the surface from the same artifact that parses:
`perf.TokenDump` reads the `Vocabulary` ANTLR bakes into every generated lexer — the same
table the parser uses to produce its "Valid alternatives: [...]" messages — into
`tools/engine-runner/vocab.tsv`, a build output now: **after a release bump,** `bazel run
//tools/engine-runner:update_vocab` (and `//:generated` fails while it is stale).

If that file is missing the harness says so rather than quietly skipping the check. Note
only simple-literal tokens are comparable: a composite like
`CONSTRAINT_OWNER: '~owner' CONSTRAINT_SEPARATOR;` has no literal name in the vocabulary and
would otherwise read as skew while parsing perfectly — five of those in Domain alone.

One tool was **built and discarded**: a rule-reachability detector meant to find dead parser
rules. It claimed `UserNamePassword` was unreachable, which a passing fixture disproves —
these grammars have no `definition` rule, their entry points are invoked from Java, so
reachability needs roots that cannot be derived from the `.g4` alone. A false "unreachable"
silently shrinks the denominator, which is the one direction this harness must never move on
its own. The token-level `tokenVocab` check survives because it only claims dead when no
parser text mentions the token at all.

Also worth knowing: `$s->toBytes()` in a function body parses, but lexes as an ordinary
function call rather than M3's `TO_BYTES` token, which is reachable only from
`primitiveValueAtomic` — test parameters and `EqualTo` expected values. The `COVERS` check
proves the text is present, not which token it became. That residual gap is real; fixtures
narrow it because they are one section each, but they do not close it.

## Layout

```
keywords.py        harvest the grammar surface; dead-token and version-skew detection
tiers.py           what is in scope and why; the exclusions and their evidence
fixtures.py        run both harnesses, recompute coverage from fixtures that parse
mutants.py         damage every positive fixture; record what legend-engine does
mutants.tsv        the manifest -- one row per mutant, committed and diffable
PERMISSIVENESS.md  where legend-engine accepts more than it looks like it should
fixtures/          positive fixtures, each declaring COVERS
negative/          must-be-rejected fixtures, each declaring REJECTS
```

## Three harnesses, three strengths of claim

**Positive fixtures** prove a construct is reachable: it parsed, so the surface exists.

**Negative fixtures** prove a construct is refused *and why* — each pins the message, so it
cannot pass for an accidental reason.

**Mutants** prove nothing on their own. They record behaviour across ~1600 damaged inputs
nobody would write by hand, and their value is differential: legend-lite must agree verdict
for verdict. The weakest claim of the three, and the only one with the breadth to have found
F19 — 16 of 51 fixtures tolerated a deleted final delimiter and all 16 were `###Connection`,
which no hand-written negative would have discovered because nobody tries the same deletion
in eight different sections.

See PERMISSIVENESS.md for what the accepted mutants revealed. That file matters more than it
looks: rejecting what legend-engine accepts is the divergence direction nobody plans for,
because it looks like rigour.

`ParseMain` (in `tools/engine-runner`) parses each file individually and reports a per-file
verdict; `--expect-fail` inverts it for the negative corpus. An empty run exits non-zero,
because a mistyped path reporting "0 wrong" would read as success.
