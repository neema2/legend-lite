# PRODUCT-HOSTS: verdict SOUND-WITH-DEBT

## Summary (at most 10 lines)
All four fixes in my share do what they claim at the fix head `eec3c95c7`. I found no blocker.
- F6: on every host, lite's store routes answer a malformed request 400 `request` in lite's words. A fault in the store's own files still answers 500 `internal`. The refusal is raised where the request is read; `served`'s catch is unchanged. I probed 21 inputs against both heads: every edge answer (legend-engine's path) is byte-identical to the PR head.
- N3: Java's `LiteV1Api.refusal` matches spec §3. The new shared case is bound to the server alone, so no host now runs a case it fails.
- N1: the refusal sits in the one resolver that every user name passes through (calls, references, signature ids, imports, lambdas, bodies, mappings, islands). The platform's own writers build typed nodes after the resolver.
- N4: the route moved cleanly. Edge off answers 404, and edge on answers it byte for byte as before. But the wheel test no longer asks the route at all, so it does not prove the shipped wheel's edge-off 404.
Debt to queue:
- a bad percent-escape is still 500 `internal` on every host;
- the product store still carries legend-engine's statuses and two new exception-name literals for the edge;
- four hand-kept copies of the kind sets;
- `trustOne` (named in F32) is still callable by its full name;
- stale "pending the user" javadocs.

## Per commit (sha, claim, verdict)
- `f9aa0a2fe` F6: malformed request is 400 `request` in lite's words. **Partly** at that commit: its new catch arm turned store-file faults into 400. That was corrected in `4fda95dc8`.
- `4fda95dc8` F6 follow-up: refused where the request is read; any other failure stays 500. **Fixes it** (probed).
- `129696cab` F6 correction: the corrupt store file's case held by a test. **Fixes it** (test only, in the `core` lane).
- `c99cbcb5b` N3: a data refusal carries its site's `where`; `raise` is a raise's kind alone. **Fixes it** (Java).
- `d076566c7` N3 correction: spec §3's `data`/`internal` rows match its paragraph. **Fixes it**.
- `eb65b5cd4` N3: spec §3's paragraph is the user's ruling. **Fixes it** for the doc; the code's javadocs are left stale (PH-8).
- `2b59369fe` N1: user calls to the compiler's own natives refused by name. **Partly**: it missed references and signature ids, which `a0760b7e1` closed.
- `a0760b7e1` N1 correction: refused by base name, reference and signature id too. **Fixes it** for the three natives in `COMPILER_OWN`.
- `8fe8c4cf8` N4: Python's legend-engine `currentUser` served by the edge alone. **Fixes it**; the wheel test proves less than asked (PH-11).

## Blockers
None.

## Findings

### PH-1 [WRONG-ANSWER] [FIX-AFTER] A malformed percent-escape is still a 500 `internal` on lite's store route, on every host
- **Where:**
  - `core/src/main/java/com/legend/queries/SavedQueries.java:203` and `:321,323` (`queryParams`);
  - `SavedCubes.java:89`. Each calls `URLDecoder.decode`.
  - The `IllegalArgumentException` is caught by `served` (`SavedQueries.java:236`) as `MALFORMED`.
- **What:** the probe gives, on lite's route:
  - `GET /api/lite/v1/queries/%zz` → 500 `internal` "the request could not be served: URLDecoder: Illegal hex characters...";
  - `GET .../batch?queryIds=%zz` → the same;
  - `GET .../dataCube/%zz` → the same.

  The Java server passes the raw path (`LegendHttpServer` ModuleHandler, `rawPath()`). The tab passes `url.pathname`, which keeps escapes. Python passes `urlsplit(self.path).path`. So all three hosts reach the same decode and give the same 500.
- **Why it matters:** it is the same class of request fault that F6 fixed. Spec §3 says a malformed request is `request`, 400. The hosts agree with each other, so this is not a parity break, and the finding did not name this input.
- **Instead:** decode the path segment and query parameters in one store helper that raises `UNREADABLE` ("its path segment '%zz' is not percent-encoded"). The edge's words stay the same as today, "IllegalArgumentException: URLDecoder: ...". About 10 lines, plus one case in `query-store/test/conformance.ts`.
- **Confidence:** verified (probe output: `audit/fixreview/probe-ph/run-fix.txt`).

### PH-2 [WRONG-DESIGN] [FIX-AFTER] The product store still computes legend-engine's status and exception text; four mounts override it from hand-kept sets
- **Where:**
  - `SavedQueries.java:245,256`: `unread`/`unreadable` set status 500, legend-engine's.
  - `:272`: `"IllegalArgumentException"` plus "Expected top-level JSON object, got " copied from the JSON library "so the edge's answer is unchanged".
  - `:348`: `"IllegalArgumentException", "Unknown sort-by value"`.
  - `FIELD_UNREAD`'s operands are legend-engine's class names (`QuerySearchSpecification`, ...).
  - The overrides: `LegendHttpServer.java:255-262` (`MALFORMED_REQUEST`), `python/legend_lite/engine.py:422`, `query-store/src/wasm-server.ts:79`, `datacube/src/cube-store.ts:206`. `MISSING` is copied the same four times.
- **What:**
  - The literal `"IllegalArgumentException"` in `SavedQueries` went from 0 to 2.
  - The class names were in the product before (as exception messages). The batch keeps them as operands.
  - Lite's mounts ignore the store's 500/404 by matching kind names in four copies across three languages. No guard compares the copies; only the conformance cases they each exercise would catch a drift.
- **Why it matters:** this is the meaning rule's "a product method that computes upstream's alternative and hands it to a hook". It is also V-PRODUCT's own fix (`StoreWords` should have its own status map), which was not taken. A new refusal kind needs four edits, or it silently answers by legend-engine's status.
- **Instead:**
  - The product side:
    - Each `Refusal.Kind` carries lite's kind (`request`/`notFound`/`internal`).
    - `refused` writes lite's status.
    - `UNREADABLE` carries facts (not JSON plus the parser's text; not an object plus the node type; a bad number; an unknown sort option).
  - The edge side (`StoreWords`):
    - maps each kind to legend-engine's status;
    - spells "IllegalArgumentException: ...".
  - The four wrappers then pass the store's answer through.
  - About 60 lines.
- **Confidence:** verified.

### PH-3 [WRONG-ANSWER] [NOTE] A store "bug" outside IAE/CCE/UncheckedIO is not a 500 on the Java server
- **Where:**
  - `SavedQueries.served` (`:231-239`) catches only `UncheckedIOException | IllegalArgumentException | ClassCastException`.
  - `LegendHttpServer.ModuleHandler.handle` (`:350-381`) and `route` (`:170-180`) have no catch-all.
- **What:**
  - On the Java server, an NPE or IllegalStateException in a store route leaves the handler uncaught. As far as I know, the JDK's HttpServer then closes the exchange without a response.
  - On the tab, the module traps and `wasm-server.ts` answers 500 `internal`.
  - On Python, `_stored` catches `Exception` and answers 500.

  This is the same at the PR head and unchanged by F6.
- **Why it matters:** the brief asks whether "a bug still answers 500". For the corrupt file and I/O cases it does, but for the server's other runtime faults it does not.
- **Instead:** a catch-all in `ModuleHandler` that answers 500 `internal` naming the class. About 6 lines.
- **Confidence:**
  - the code: verified;
  - the JDK closing the connection: suspected (not run).

### PH-4 [WRONG-ANSWER] [NOTE] Two old status divergences on lite's store route
- **What:**
  - `PATCH /api/lite/v1/queries/q1`: Java and the tab give 400 `request` (`NO_SUCH_ROUTE`, probed). Python's HTTP server has no `do_PATCH` (`engine.py:2297-2412`), so the standard library answers 501 HTML. In-process, `_saved_queries` answers 405 (`engine.py:849`).
  - `NOT_OWNER` (403) is answered with kind `request` and status 403 on every mount. Spec §3 says `request` is 400.

  Both are the same at the PR head, and neither is in the finding.
- **Instead:** queue both with Q9.
- **Confidence:** verified by reading; PATCH on Java probed.

### PH-5 [COSMETIC] [NOTE] Coverage gaps in the shared store cases
- **What:**
  - The query-store suite covers every new kind on all three hosts: `field_unread`, `unreadable` (not JSON, not an object, version, sort), `no_search_term` and `no_such_route`. It runs on the tab (LITE), the server (LITE and ENGINE) and Python (LITE, `datacube/test/python-engine/saved-queries.test.ts`).
  - DataCube's suite (`datacube/test/saved-pages/conformance.ts:124-145`) has no "body not JSON" case and no `DataCubeQuery.description is not a string` case.
  - The corrupt store file case is Java-only (`SavedQueriesTest:64-76`).
- **Instead:** add the two pages cases (about 8 lines).
- **Confidence:** verified.

### PH-6 [BROKEN-RULE] [FIX-AFTER] N1 covers three internal natives; F32's `trustOne` and 35 others still resolve by full name
- **Where:** `builtin/Pure.java:634-635` (`COMPILER_OWN` = typeRaise, cellText, assertFailedText) against `INTERNAL_DESUGAR` (`:644-656`, 38 names).
- **What:**
  - F32 named "typeRaise, cellText, and presumably trustOne, which skips toOne's guard".
  - `trustOne` cannot be refused at the resolver as it stands. The Cypher island writes it as a plain `AppliedFunction` with no record (`protocol/cypher/CypherToPure.java:10273-10458`, through `SqlToPure.call:5945`), and that call then passes through `NameResolver.resolveQuery`. So the exclusion is correct for this design, but the hole F32 named is still open. `trustOne` predates the PR.
- **Instead:**
  - Have the island translators give their internal calls a record (referents = [fqn]). The resolver's idempotent arm (`NameResolver.java:1914-1918`) then passes them.
  - Then refuse every `INTERNAL_DESUGAR` name on a call with no record.
  - Pin it with InternalNativeCallTest. About 15 lines.
- **Confidence:**
  - the gap: verified;
  - the suggested mechanism: suspected.

### PH-7 [COSMETIC] [NOTE] Stale counts in N1's docs
- `Pure.java:630-633` says the set is "the type check's raise and a TDS cell's text", but it holds three natives.
- `InternalNativeCallTest`'s class javadoc names two.
- **Confidence:** verified.

### PH-8 [COSMETIC] [NOTE] N3's code still says "pending the user"; the spec paragraph repeats itself
- **Where:**
  - `server/LiteV1Api.java` `dataRefusal` javadoc: "ruled by the orchestrator pending the user".
  - `LiteDataRefusalTest`'s class javadoc: the same.
  - Against these, `docs/records/lite-api-v1-2026-11-17.md:59-64` says "ruled by the user 2026-11-18". That paragraph also states the rule twice, and names the enum spellings (`DATABASE_ERROR`, ...) that the same section calls internal.
- **Instead:**
  - Say "ruled by the user, 2026-11-18" in both javadocs.
  - Keep one sentence of the paragraph, in wire terms.
- **Confidence:** verified.

### PH-9 [WRONG-DESIGN] [NOTE] Kind-less run failures answer `data`, which spec §3 does not list
- **Where:**
  - `LiteV1Api.dataRefusal`'s last arm, pinned by `LiteDataRefusalTest.aFailureOfNoKindIsDataWithNoRaise`.
  - The kind-less sources:
    - a site-less `fail` (`exec/RaisedErrors.java:261`);
    - `DataLoads.java:76,79` and `SourceAdmission.java:104-169`;
    - `JdbcStatements.java:709` (a list parameter);
    - `Sessions.java:401-497` (a JDBC `getAutoCommit`/`setAutoCommit` failure, which is a connection's fault).
- **What:** all of these answer 422 `data` with no `raise`. Spec §3's `data` row lists raises, the database's refusal and a too-large file. Its `internal` row is "anything else", so the spec says nothing about these failures. A site-less `fail` also answers without `raise`, while a sited one answers `userFail`. All of this predates the PR and is outside N3's three named kinds.
- **Instead:** add a line to the spec, or reclassify:
  - `Sessions`' JDBC failures through `RaisedErrors.connectionError`;
  - the list parameter as `parameters`.
- **Confidence:** verified.

### PH-10 [COSMETIC] [NOTE] The Python runners would not hold `raise`/`where`
- **Where:** `python/tests/test_engine.py:566-568` and `test_compiler.py:187-191` compare `kind` alone. The test_engine runner only checks that the status is ≥ 400.
- **What:** the TS server runner holds them (`engine-client/test/lite-api-server.test.ts:146-148`, `includes(c.refusal, refusal)`). When Q9 binds Python to the new case, the Python runners must hold `raise`, `where` and the status the same way, or the case proves nothing there.
- **Confidence:** verified.

### PH-11 [COSMETIC] [FIX-AFTER] N4: the wheel test does not prove the shipped wheel's edge-off 404
- **Where:** `python/tests/test_wheel.py:133-136,413`.
- **What:**
  - The probe now asks `/sdlc/api/projects` for the cookie check and never asks `/api/server/v1/currentUser`.
  - The edge-off proof is in-process only: `test_edge.test_a_product_engine_serves_none_of_legend_engines_routes` (`test_edge.py:59-66`).
  - The layer rule is source-level (`test_run_closure.py`).
- **Instead:** one line in the wheel probe, `"engine_user": get(link, "/api/server/v1/currentUser", jar)[0]`, asserted as 404.
- **Confidence:** verified.

### PH-12 [COSMETIC] [NOTE] One long javadoc line
`LegendHttpServer.java:271` is 195 characters (the F6 edit). No guard checks this. Verified.

## Strengths
- **F6, the edge is untouched:**
  - 21 inputs were probed at both heads (16 query, 5 pages): bodies of the wrong shape, not JSON, not an object, `-`, a bad version, a version past `int`, an unknown sort, `projectCoordinates:[5]`, an unknown path, PATCH, and bad escapes.
  - Every legend-engine-path answer is byte-identical. Only lite's lines changed, each to 400 `request` in lite's words.
  - `StoreWords`' new arms rebuild exactly the old `MALFORMED` text.
- **F6, server faults stay internal:** store files holding `{}`, `[1]` and `{bad` answer 500 `internal`. `FileRecords`/`MemoryRecords` never call `requestBody`.
- **F6, refused at the read site:**
  - `requestBody` catches only `Json.parse`'s IAE; `version` catches only NFE.
  - `served` is unchanged, so `CATCH_RETURNS_VALUE` stays 32 and the broad-catch census is untouched (IAE and NFE do not match `BROAD_TYPE`).
  - Valid requests behave as before: `requestBody` is `parseObject`'s acceptance, and `parseInt` is `valueOf`'s.
- **F6, tests:** the tests flip the earlier wrong-direction pins. They run in the `ui`, `datacube`, `warehouse` and `core` lanes.
- **N1:**
  - Every user name passes the one resolver:
    - queries;
    - function, derived-property and constraint bodies;
    - M2M mappings, filters and XStore expressions;
    - service bodies;
    - the grammar carrier's lambdas;
    - the SQL, Cypher and #TS islands after translation (each through `NameResolver.resolveQuery`).
  - A signature id is refused by `SignatureMangle`'s own base cut. A bare id through an import is refused too.
  - Tolerant mode walls the refusal: `ResolutionException` is caught at `NameResolver.java:239`.
  - The platform's writers build typed nodes after the resolver, so its own uses never reach the refusal (`TdsDesugars:162,167`, `MatchChecker:138`, `ChainDispatch:226`, `LambdaBodies:132`).
  - The parser, `ProtocolReader` and the island translators never set a record, so no user call takes the idempotent bypass.
  - No test, fixture or `.pure` file calls these natives by name.
- **N3:**
  - The classifier matches spec §3's table and paragraph. Each of the five shapes has a test.
  - No client reads the dropped names `databaseError`, `connectionError` or `fileTooLarge`.
  - `where` is planning text (for example "[1]", the multiplicity), never a run-time value.
- **N4:**
  - `THIS_USER == _QUERY_USER` (`engine.py:132`), so edge on answers the same 200, `application/json`, `"this user"`.
  - `private()` still requires the token or cookie under `/api/`.
  - No product module imports the edge.
  - Query asks lite's `POST /api/lite/v1/user` (`query-store/src/client.ts:94`). Studio's `currentUser` is the SDLC client's.
- **No pins moved:** no pin, ceiling, allowlist or ledger moved in my share's commits.

## Measurements
- Store probe (scratch javac of `json/`, `base/Nullable`, `core/.../queries/*`, `edge/StoreWords` at each head; `probe/Main.java`): `audit/fixreview/probe-ph/run-{pr,fix}.txt`.
  - `diff run-pr.txt run-fix.txt`: 19 lite lines differ and 0 edge lines differ.
  - The bad-escape lines (`%zz` in a path or `queryIds`) are 500 `internal` at both heads.
- `lite-api-cases.json` (`python3 -I` over the fixture):
  - 26 cases at the fix head (25 at the PR head);
  - by binding: 19 all, 6 `[server, python]`, 1 `[server]` (the new data case);
  - 1 refusal names keys beyond `kind`.
- Hand-kept copies of lite's kind sets (`rg`): `MALFORMED_REQUEST` 4, `MISSING` 4 (Java, Python, two in TS).
- `"IllegalArgumentException"` literals in `SavedQueries.java`: 0 at the PR head, 2 at the fix head (`grep -c`).
- `COMPILER_OWN` 3, `INTERNAL_DESUGAR` 38 (`Pure.java:634-656`).
- The changed Python files parse (`python3 -I` with `ast.parse`, read-only); `THIS_USER` is used in `edge/_pure_v1.py`, and `json` is imported in `test_edge.py`.
