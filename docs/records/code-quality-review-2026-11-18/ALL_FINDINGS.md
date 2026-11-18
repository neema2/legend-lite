# Every finding of the review, with its verified severity

Reviewer severity is what the area reviewer set; "Verified as" and "Final severity" are the adversarial verifier's. Details: `reviews/<AREA>.md` (the finding) and `verification/<AREA>.md` (the check).

| ID | Reviewer | Verified as | Final severity | Finding |
|---|---|---|---|---|
| ARCH-1 | S1 | OVERSTATED | S2 | The plan schema is hand-maintained in three languages and five codecs, versioned by exact match |
| ARCH-2 | S1 | CONFIRMED-WITH-CORRECTION | S2 | Four representations of "a program of steps" and two Java walkers that re-derive the same rules |
| ARCH-3 | S1 | OVERSTATED | S2 | The guard layer is about the size of the code it guards, and it has shaped the product's API |
| ARCH-4 | S1 | CONFIRMED-WITH-CORRECTION | S1 | The plan's type model admits states the runners refuse at run time |
| ARCH-5 | S1 | CONFIRMED-WITH-CORRECTION | S2 | Session routing is spread across two source hierarchies and four routers over denormalised targets |
| ARCH-6 | S1 | OVERSTATED | S2 | Three host runners and three walls re-implement plan semantics; the host boundary is an RPC spelled in five places |
| ARCH-7 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The test road is as large as the product road, and the judges multiply every assert by about 14 files |
| ARCH-8 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Code comments and the doc layer read as a process log |
| ARCH-9 | S2 | CONFIRMED | S2 | Measurement metadata is threaded through exec's core API |
| ARCH-10 | S2 | CONFIRMED | S2 | Positional many-field records with telescoping constructors make every plan field addition a sweep |
| ARCH-11 | S3 | CONFIRMED | S3 | Inline fully-qualified names instead of imports (pattern) |
| PLANMODEL-1 | S1 | CONFIRMED-WITH-CORRECTION | S1 | Five hand-maintained codecs, three conventions, untyped post-processing |
| PLANMODEL-2 | S1 | OVERSTATED | S2 | Every statement embeds its full Target; each Load points back to its holder's target |
| PLANMODEL-3 | S2 | CONFIRMED | S2 | Typed facts spelled as strings inside the typed plan, parsed back at run |
| PLANMODEL-4 | S2 | CONFIRMED | S2 | Sum types flattened into nullable fields, sentinel ints, prose rules and `default` arms |
| PLANMODEL-5 | S2 | CONFIRMED-WITH-CORRECTION | S2 (the highest-value S2) | The plan carries the planner's MIR tree, which nothing runs from |
| PLANMODEL-6 | S2 | CONFIRMED-WITH-CORRECTION | S2 | `testexecutionplan` is "data alone" in name only; edge data and grammar live inside it |
| PLANMODEL-7 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The same concept implemented several times across the pair and against the model |
| PLANMODEL-8 | S2 | OVERSTATED | S3 | The reader's strictness and errors are inconsistent; a misspelled optional key becomes a default |
| PLANMODEL-9 | S2 | OVERSTATED | S3 | Versioning is a lockstep fingerprint with a prose changelog in three places |
| PLANMODEL-10 | S3 | (S3 pattern, spot-checked) | S3 | Javadoc reads as process history, and some of it is stale or wrong |
| PLANMODEL-11 | S3 | (S3 pattern, spot-checked) | S3 | Name collisions, residue names and forwarding wrappers |
| PLANMODEL-12 | S3 | (S3 pattern, spot-checked) | S3 | God files, telescoping constructors and positional nulls |
| PLANMODEL-13 | S3 | (S3 pattern, spot-checked) | S3 | Inline fully qualified names instead of imports |
| EXEC-1 | S1 | CONFIRMED | S1 | Column types are strings with a grammar, re-parsed and matched by name in six classes (partly per cell) |
| EXEC-2 | S1 | CONFIRMED | S1 | Two runners for "steps on one session", with duplicated orchestration, diverging rules and a lossy re-encoding of the test plan |
| EXEC-3 | S1 | CONFIRMED-WITH-CORRECTION | S1 | Session acquisition: five overlapping abstractions, three parallel opening ladders, process-global caches with no owner |
| EXEC-4 | S2 | CONFIRMED | S2 | `PlanRunner` is a 1,209-line bag of package-private statics; out-parameters and one-element arrays |
| EXEC-5 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Two JDBC transaction implementations, and two bracket APIs whose exception contracts force tunnelling |
| EXEC-6 | S2 | CONFIRMED | S2 | The `Statements` port is JDBC-shaped and fat; a decorator exists only to carry the census label, paired by hand at 17 sites |
| EXEC-7 | S2 | CONFIRMED | S2 | `JdbcStatements.send` trades static types for a single choke point: an enum mode, a sealed answer and four runtime downcasts |
| EXEC-8 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The plan type admits states exec must refuse at run time: about 16 impossible-arm throws |
| EXEC-9 | S2 | CONFIRMED | S2 | Dead and vestigial API |
| EXEC-10 | S2 | CONFIRMED | S2 (low end) | A second hand-written JSON parser |
| EXEC-11 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Error handling: one exception type for plan defects, user refusals and bugs; two ways to wrap `SQLException` |
| EXEC-12 | S2 | CONFIRMED-WITH-CORRECTION | S2 | `RunScope`: simple and readable, sound as a single-thread tripwire, but with a shared close token and conflated scopes |
| EXEC-13 | S2 | CONFIRMED | S2 | `PureV1Writer` is two writers in one class; `ColumnType` admits invalid states |
| EXEC-14 | S2 | CONFIRMED | S2 | Session state: five concurrency mechanisms, public mutators on exec-internal state, instanceof walkers over its own subclasses |
| EXEC-15 | S3 | (S3 pattern, spot-checked) | S3 | Comments narrate process history instead of explaining the code (pattern) |
| EXEC-16 | S3 | (S3 pattern, spot-checked) | S3 | Inline fully-qualified names instead of imports (pattern) |
| EXEC-17 | S3 | (S3 pattern, spot-checked) | S3 | Opaque names, vocabulary sprawl, positional booleans (pattern) |
| EXEC-18 | S3 | (S3 pattern, spot-checked) | S3 | Tests: behavioural and real, but coupled to internals and to message fragments (pattern) |
| PLANNER-1 | S1 | OVERSTATED | S2 | The planner's API is an allowlist of signature strings, not a type |
| PLANNER-2 | S1 | CONFIRMED-WITH-CORRECTION | S2 | Program planning is flag-driven, runs twice for a writing query, and still special-cases the test door |
| PLANNER-3 | S2 | CONFIRMED-WITH-CORRECTION | S2 | "Lower a prepared statement for its target" is rebuilt by hand at six or more sites, and pulls are planned twice |
| PLANNER-4 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The root and the planner depend on each other, the root keeps a second lowering road, and three types are called `Target` |
| PLANNER-5 | S2 | CONFIRMED | S2 | The series encoded payload identity in the MIR as URL strings and magic names |
| PLANNER-6 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The raw-SQL refusal is policed door by door, and its exception class is a trap for catch sites |
| PLANNER-7 | S2 | CONFIRMED-WITH-CORRECTION | S2 | testdatagen and setup duplicate the same logic, use a boolean mode flag, and build SQL with an 11-argument constructor |
| PLANNER-8 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The test-data reload added modal Lowerer state and SQL-carrying HIR nodes to core layers |
| PLANNER-9 | S2 | CONFIRMED | S2 | Payload ordering is a planner-level MIR repair, and DML traversal is hand-switched twice |
| PLANNER-10 | S2 | CONFIRMED | S2 | Catalog registration depends on call order and is checked after the fact |
| PLANNER-11 | S2 | CONFIRMED | S2 (low) | SystemCatalog rolls its own hash although the planner already has a portable SHA-256 |
| PLANNER-12 | S2 | CONFIRMED-WITH-CORRECTION | S2 (low) | Empty declared tables are made by building a CSV text and feeding it to the CSV parser |
| PLANNER-13 | S3 | (S3 pattern, spot-checked) | S3 | Inline fully qualified names are everywhere |
| PLANNER-14 | S3 | (S3 pattern, spot-checked) | S3 | Comments narrate process history, and some are stale or orphaned |
| PLANNER-15 | S3 | (S3 pattern, spot-checked) | S3 | Positional booleans and telescoping overloads |
| PLANNER-16 | S3 | (S3 pattern, spot-checked) | S3 | Some tests pin text and counts, and nine planted classes exist only for guards |
| TESTPLAN-1 | S1 | CONFIRMED-WITH-CORRECTION | S1 | Two parallel assert classifiers (and a one-implementation strategy interface) |
| TESTPLAN-2 | S1 | CONFIRMED | S1 (one redesign with -1) | The database and engine-text planners are evaluators re-run under mutable recording collectors |
| TESTPLAN-3 | S1 | CONFIRMED-WITH-CORRECTION | S1 (borderline S2) | The body walk is re-implemented four times |
| TESTPLAN-4 | S1 | CONFIRMED-WITH-CORRECTION | S1 | BodyClass predicts what the planners can do, one accreted rule at a time |
| TESTPLAN-5 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Planning failures, Java defects included, are caught broadly and replayed as test outcomes |
| TESTPLAN-6 | S2 | CONFIRMED-WITH-CORRECTION | S2 | God classes and flag-laden telescoping signatures |
| TESTPLAN-7 | S2 | CONFIRMED | S2 | ExecEnv looks immutable but carries shared mutable planning state, and some parameters are redundant |
| TESTPLAN-8 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Comments explain present behaviour by a deleted subsystem, and many are stale |
| TESTPLAN-9 | S2 | CONFIRMED-WITH-CORRECTION | S2 (low) | Dead code, residue and stubs kept alive for a guard |
| TESTPLAN-10 | S2 | OVERSTATED | S3 | Planning writes process-global counters and reads system properties |
| TESTPLAN-11 | S2 | CONFIRMED | S2 | Stringly-typed forms and cross-layer naming contracts |
| TESTPLAN-12 | S2 | CONFIRMED | S2 | Dialect capability is probed with exceptions, including one fallback |
| TESTPLAN-13 | S2 | CONFIRMED | S2 | Test-suite structure: pinning, legacy names, thread-local state |
| TESTPLAN-14 | S3 | (S3 pattern, spot-checked) | S3 | Inline fully qualified names and unused imports (pattern) |
| TESTPLAN-15 | S3 | (S3 pattern, spot-checked) | S3 | Process history in comments, identifiers and runtime error messages (pattern) |
| TESTPLAN-16 | S3 | (S3 pattern, spot-checked) | S3 | Hand-rolled tree rewrites, let chasing and duplicated small types (pattern) |
| TESTEXEC-1 | S1 | CONFIRMED-WITH-CORRECTION | S1 (borderline S2) | The runner works out a verdict's kind from nullable fields; judge-mode dispatch is spread over two classes |
| TESTEXEC-2 | S1 | OVERSTATED | S2 | A verdict is `@Nullable TypedSide` plus exceptions: three outcomes carried by a dummy value, null and throw |
| TESTEXEC-3 | S2 | CONFIRMED (reachability now verified by reading, not by running) | S2, flagged as a likely verdict bug | The decision and its message are computed twice with different kinds, and in one arm the message overturns the decision |
| TESTEXEC-4 | S2 | CONFIRMED-WITH-CORRECTION | S2 | A measurement instrument for a deleted byte channel runs inside production verdicts; four instrumentation mechanisms are woven into decision code |
| TESTEXEC-5 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The listener and census reporting protocol is copied five times, each copy with its own `settled` flag |
| TESTEXEC-6 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Kinds, forms and carriers are dispatched on strings, including Java class names written into the portable plan |
| TESTEXEC-7 | S2 | CONFIRMED | S2 | Three decodes of a constant's text, admitting different types; exec's javadoc calls its own "the one decode" |
| TESTEXEC-8 | S2 | CONFIRMED | S2 | `TestRunner.Moved` is a god class with a history name |
| TESTEXEC-9 | S2 | CONFIRMED (none of the four is a register row) | S2; (a) is a fidelity defect | Value equality is defined well once, but several neighbours diverge from it in subtle ways |
| TESTEXEC-10 | S2 | CONFIRMED | S2 | Logic copied from other layers into the run |
| TESTEXEC-11 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Error handling: an exception nobody catches, one exception type for two meanings, a catch-all that can rescue a text, and messages that name deleted internals |
| TESTEXEC-12 | S2 | OVERSTATED | S3, plus one worthwhile Appeals/router test | The run-side judges have no focused tests; guards pin their structure instead |
| TESTEXEC-13 | S2 | CONFIRMED-WITH-CORRECTION | S2 (low) | State and control flow: holders, a reader pushed through a setter, three index walkers, nested lambdas, long parameter lists |
| TESTEXEC-14 | S3 | (S3 pattern, spot-checked) | S3 | Inline fully-qualified names everywhere (pattern) |
| TESTEXEC-15 | S3 | (S3 pattern, spot-checked) | S3 | Process history in comments and identifiers (pattern) |
| TESTEXEC-16 | S3 | (S3 pattern, spot-checked) | S3 | Stale links, orphan javadoc and small residue (pattern) |
| GUARDS-1 | S1 | CONFIRMED-WITH-CORRECTION | S1 | Exact inventories of incidental structure make ordinary edits fail the build |
| GUARDS-2 | S1 | CONFIRMED-WITH-CORRECTION | S2 | String-keyed rules silently stop guarding when code moves; two are vacuous now |
| GUARDS-3 | S1 | CONFIRMED-WITH-CORRECTION | S2 | Hand-written source parsers are the analysis engine: fragile, duplicated, and they dictate code layout |
| GUARDS-4 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Registries stand in for access control and types (planner entries, root doors, request fields) |
| GUARDS-5 | S2 | CONFIRMED-WITH-CORRECTION | S2 (weak) | The same invariant is encoded two or three times (Bazel deps, core-layers.txt, ArchUnit lists, source regex) |
| GUARDS-6 | S2 | CONFIRMED | S2 | Guards measure names, spellings and size as stand-ins for design |
| GUARDS-7 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The guards dictate product code shape to suit their own analysis |
| GUARDS-8 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Guard code quality: one 4,407-line class, structural duplication, repeated import cost, a silent skip |
| GUARDS-9 | S3 | (S3 pattern, spot-checked) | S3 | Process history and stale or misattached documentation dominate the guard files (pattern) |
| GUARDS-10 | S3 | (S3 pattern, spot-checked) | S3 | Untyped tuples and if/else ladders in the guard code (pattern) |
| PYTHON-1 | S1 | CONFIRMED-WITH-CORRECTION | S1 (borderline S2) | The guard layer is out of proportion to the code it guards, and its count pins tax routine edits |
| PYTHON-2 | S1 | CONFIRMED-WITH-CORRECTION | S1 | The plan format is re-implemented by hand per host, as untyped dicts, with a hand-copied version |
| PYTHON-3 | S2 | CONFIRMED | S2 | Two representations of "a plan", two `read_plan`s, a parse-dump-parse round trip, and dicts used as records |
| PYTHON-4 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The half-finished move: re-export shims kept, and consumed by internal code |
| PYTHON-5 | S2 | OVERSTATED | S3 | The public API regressed: `Frames.connection` became a wrapper with no capabilities, so tests reach `_con` |
| PYTHON-6 | S2 | CONFIRMED-WITH-CORRECTION | S2 (low; S3 defensible) | Small concepts duplicated: two "model refused" exceptions, a set written twice, a version check pasted twice |
| PYTHON-7 | S2 | OVERSTATED | S3 | Error design: string-typed kinds, an unnamed refusal, and no hierarchy |
| PYTHON-8 | S2 | OVERSTATED | S3 | Module cohesion: `_planned.py` holds four concerns, and the line between `_reads` and `_runs` is arbitrary |
| PYTHON-9 | S2 | OVERSTATED | S3 | The bridge allowlist's membership test has side effects and matches keyed bodies by brute force |
| PYTHON-10 | S3 | (S3 pattern, spot-checked) | S3 | Comments and docstrings are mostly process history, and several are stale (pattern) |
| PYTHON-11 | S3 | (S3 pattern, spot-checked) | S3 | Typing: the package ships `py.typed`, no type checker runs, and the hints are loose or wrong (pattern) |
| PYTHON-12 | S3 | (S3 pattern, spot-checked) | S3 | Residue and dead code (pattern) |
| PYTHON-13 | S3 | (S3 pattern, spot-checked) | S3 | A few tests pin structure or absence rather than behaviour (pattern) |
| TYPESCRIPT-1 | S1 | CONFIRMED-WITH-CORRECTION | S1 (borderline) | The plan format is decoded by hand in every host, and the TS decoder works by casts, not by narrowing |
| TYPESCRIPT-2 | S1 | OVERSTATED | S2 | Layer separation is enforced by a hand-written regex analyser of TypeScript where the build and lint could do it |
| TYPESCRIPT-3 | S2 | CONFIRMED-WITH-CORRECTION | S2 | `Boundary.java` is a grab bag: inline fully qualified names, JSON decoded by hand, JSON round trips |
| TYPESCRIPT-4 | S2 | CONFIRMED | S2 | `HostOperationsJson` keeps each operation's keys in two places, so a source-reading test exists to hold them equal |
| TYPESCRIPT-5 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Two planner clients implement one protocol, and a new request kind touches about 11 places |
| TYPESCRIPT-6 | S2 | CONFIRMED | S2 | `runPlan` is one 89-line closure that mixes validation with execution and duplicates its neighbours |
| TYPESCRIPT-7 | S2 | CONFIRMED | S2 (low end) | The runner answers JSON text built by template strings, and its callers parse it back |
| TYPESCRIPT-8 | S2 | OVERSTATED | S3 | The run-frame witness is called by hand, opened at two levels, and blind in production |
| TYPESCRIPT-9 | S2 | CONFIRMED | S2 | `snap.ts` checks capabilities by casting, and duplicates its state machine |
| TYPESCRIPT-10 | S3 | (S3 pattern, spot-checked) | S3 | Comments narrate process history (148 dates, about 193 audit and ruling references in engine-client/src) |
| TYPESCRIPT-11 | S3 | (S3 pattern, spot-checked) | S3 | Half-migration residue: re-export shims, a stale export, an unused variant (about 9 sites) |
| TYPESCRIPT-12 | S3 | (S3 pattern, spot-checked) | S3 | Small duplications (each a few lines, about 8 patterns) |
| TYPESCRIPT-13 | S3 | (S3 pattern, spot-checked) | S3 | Muddled vocabulary: "door" means two things in the same files |
| TYPESCRIPT-14 | S3 | (S3 pattern, spot-checked) | S3 | Tests pin text where structure is available |
