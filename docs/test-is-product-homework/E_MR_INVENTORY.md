# E. MR's inventory: mutable state, coupling and concerns on the program road (2026-11-01)

This is an independent, read-only audit of the code at 96a1dbde, made for the user's ruling R20 ("killing mutable state
and fixing everything expertly and also split by concern"). Brief B14 is designed from it.

Path legend: P = `core/src/main/java/com/legend/planner/`, T = `.../testplan/`, E = `.../exec/`,
S = `.../compiler/spec/`. Line numbers are those of 96a1dbde.

## 1. Mutable state

### ProgramFrames (P/ProgramFrames.java)

Lifetime: one per program (ProgramState:41) or per query (PlanMaker:69). Every 5-argument `prepare` makes a
throwaway one (ProductStatements:56). TestKinds.effect uses that overload (TestKinds:168-169), so an effect
statement never sees the program's frames.

Program-lived state:
- `frames` (50). Written by `let` 86 (TestKinds:120) and by `alias` 102 (PlanMaker:259). Read by `relationFrame`
  106, `renames` 186-190, `readAgainWithCtes` 145, the hook's `frame()` 197 and `relationalActivitySql` 240.
- `planRows` (52). Handed out raw by `planRows()` 112 and shared into every statement's PlanEnv (ProductStatements:72,
  TestKinds:99). Written by `inlineExecute` 231 (via PlanAllocations:304), by TestKinds:104, and by the resolver's
  handle registrar (BodyPlanner:33 → PlanAllocations:199-200, 434, 500). `startStatement` 129 evicts from it.
- `stamps` (54). `newStamp` 78-82 makes the name `"#stamp"+size` (TestKinds:103, `inlineExecute` 224). Read by
  `stampSlots` 117-125, which every lowering binds (PlanMaker.withStamps 98-103).
- `consumed` (66). Written at 91-93 (TestKinds:102). Read at ProductStatements:93-97.

Per-statement state, cleared by `startStatement` 128-135:
- `ownStamp` (56). Written at 224 and restored at 152-154. Read at PlanMaker:94, 293 and 341.
- `asCte` (58). Written at 155, read at 202.
- `reads` (60). Written at 201. Its counts are never read: line 143 uses only its names, so the counts are dead.
- `definitions` (62). Written at 208. Read through ProgramState.lowered:101 and PlanMaker:289.
- `inlineRuns` (72). Written at 230, read at 129.

Hazard: these per-statement fields always belong to whichever `prepare` ran last. TestKinds.frame runs two prepares
(107, 114) and reads `definitions` between them (109).

### ProgramState (P/ProgramState.java)

Lifetime: one per program, created only at PlanMaker:239.
- `lets` 37, `steps` 38, `staged` 39 and `sites` 36 are package-private and aliased into locals (PlanMaker:243-246).
  PlanMaker mutates them directly (269, 299-301, 311, 318, 327-330, 340). TestKinds adds through `add` 110 (109, 153,
  205, 215).
- `lets()` 80-82 returns a LIVE unmodifiable view. TestKinds:104 captures it in a deferred LazyRows closure
  (PlanAllocations:304-306), which therefore sees lets appended later.
- `door()` 65 has no callers.

### ObjectSites (lowering/ObjectSites.java:38-45)

It mints `prefix + (size + 1)` on a key's first sight, so ids depend on call order. A program's `sites` is called
by every lowering: ProgramState:99-100, PlanMaker:285 (staged), 326 (store write), 330→370 (count), TestKinds:109
then 116. PlanEnv.of also makes a per-statement `new ObjectSites("i")` (PlanEnv:45). On the program road only
BodyPlanner.lowererFor:153 reads it.

### PlanEnv (P/PlanEnv.java)

A record whose mutable components `queryLets` 24, `planRows` 27 and `instanceIds` 26 are shared by every `withX`
copy (52-95). `tableReplace` is passed without a copy (BodyPlanner:242).

`queryLets` is a fresh LinkedHashMap per `PlanEnv.of` (42).
- Writers: FramePlanner:80-81 and 139-140 (as `queryLetsSink` into ExecuteChainAssembly:476, `putAll`, while the
  inliner's hook runs); ProductStatements:89 (`put`) and 95 (`putIfAbsent`).
- Readers: StoreResolver copies it at construction (104-105); `seeded` 232; EnginePlans:757.
- Sharing: within one statement by `env`, `widened`, `Prepared.env`, every `FrameChain.env`, and the LazyRows closures
  registered during inlining. It is not shared across statements.

### LazyRows (plan/LazyRows.java:36-50)

It memoizes its rows on first read (ConstructedInstances:122-130).

### PlanMaker.program's locals

- `effectMemo` 242 is a memo for LetPlanner:134. On the test door it duplicates `QueryDoor.Scan.effects`.
- `unreadStampsDropped` rewrites `steps` in place (139).

### TestKinds (T/TestKinds.java)

Lifetime: one per `TestDoor.plan` call (TestDoor:28). It is a stateful object behind a `@FunctionalInterface`
(ProgramKinds).
- `script` (38): written by `effect` 197, cleared by `close` 218, read by `seed` 144 and `close` 211-217. Ordering:
  close (57) runs before seed (60).
- `established` (40): added at 136, cleared at 71.
- `generated` (42): a pure memo.
- `scan` (36): pure caches.

### PlanRunner.program (E/PlanRunner.java:159-344)

All of its state lives for one run.
- `target`, `results`, `changes` and `snapshot` (164-167) are written by the pre-scan (168-247).
- `held` 276, and `answered[]` 277, a one-element array whose only purpose is to escape the lambda.
- `live` 284 is written by `update` 790 (counts) and `stamp` 939, and read by `valuesOf` 944-951.

Ordering:
- a counted Update runs before the result that binds COUNT;
- a frame's Check puts its `#stampN` before its readers;
- HOST sides are read before the DATABASE fused statement (352 vs 379).

The StatementOrigin ThreadLocal is entered at 428 and 846.

### SessionState (E/SessionState.java)

It lives as long as its session and is synchronized. Fields: `seeded` 21, `sessionStatementsRun` 29, `dirty` 31.
Its key type is `Record`, which holds both `SeedKey` (PlanRunner:863) and `Seeds.Sources` (BodyDriver:699).

### Static mutable state in scope

None. The only global is StatementOrigin's ThreadLocal.

## 2. Hidden coupling

1. **The inliner's hook mutates the planner's state.** UserCallInliner calls the hook at S/UserCallInliner:1348-1352
   and 1551-1556 (wired at ResultEnvelopeSplice:155-157). The rewrite asks `frame(name)` several times per node
   (ResultEnvelopeSplice:305, 403, 426, 448, 501, 543, 642), so the read counts mean nothing, and
   `readAgainWithCtes` counts pastes by identity instead.
2. **`inlineExecute` has three side effects** (ProgramFrames:216-236).
   - It writes the chain's consumed lets into `env.queryLets` mid-inlining.
   - It mints `ownStamp` on its first call, BEFORE the `containsKey` test, so a call site already registered still
     mints a stamp.
   - It registers a LazyRows that captures `env`, whose `queryLets` ProductStatements:87-97 fills afterwards. The
     activity SQL is computed at the resolver's first read, during `resolve` (102).
3. **`relationalActivitySql` (238-253) resolves mid-inlining** (`BodyPlanner.resolver` via EnginePlans) over the
   partial `queryLets` and `planRows` of that moment.
4. **`prepare` calls `startStatement`** (ProductStatements:68). That evicts the previous statement's inline-run rows
   from the shared `planRows`, and this eviction is how B7d's "each statement its own stamp" is implemented.
5. **The second inline pass.** `readAgainWithCtes` is `startStatement`, plus restoring `ownStamp`, plus setting
   `asCte` (141-157). Inline runs are re-registered under the same stamp. Pass 1's `env.queryLets` is not cleared.
6. **The order of `queryLets` decides SQL bytes:** the seeded let prefix's order (ProductStatements:231-237), and so
   the alias numbers. That order is:
   - the chain-consumed lets, in hook-call order (pass 1, then pass 2);
   - then the inliner's read lets (`put` keeps an existing position);
   - then the frames' consumed lets (`putIfAbsent`).
7. **Chain-consumed lets skip the `read` filter** (86-91), contrary to the comment at 83-85. Preserve this.
8. **"The last prepare wins."** PlanMaker reads `ownStamp()` and `definitions()` after a prepare (293, 341;
   ProgramState:101). TestKinds.frame's order is fixed: rows (104), eager prepare (107), lower (109), CTE prepare
   (113-118), `frames.let` (120). Changing it changes ids and stamps.
9. **Stamp slots grow over the program.** Every lowering binds every stamp minted so far, so `withStamps` must run
   after the statement's prepare.

## 3. Long methods and mixed concerns

### PlanMaker.program, 217-352 (136 lines)

Concerns, in order:
1. target and dialect decision, 221-233 (a duplicate of `plan` 81-89);
2. refusal, 235;
3. state and its aliases, 239-246;
4. the test-kinds hook, 250;
5. let-kind dispatch, 254-275;
6. staged lets, 276-303;
7. statements (constant, handle, store write and count, test-door refusal, planned), 305-341;
8. the stamp post-pass, 345;
9. drops, 346-350.

Proposed units:
- `decide(ctx, runtime, serverVersion)`, shared with `plan`;
- `planLet`, returning a sealed `Alias | Value | Staged`;
- `planStatement`;
- a pure `withoutUnreadStamps`;
- `drops`.

### PlanMaker.plan, 54-95

Refuse, divert writes, prepare, constant, lower, decide, metamodel, read plus stamp.

### ProductStatements.prepare, 63-112

Reset, env, inline twice, merge `queryLets` from two sources, temporal check, stage, resolve, guard, widen renames,
classify. Proposed: `inline(...)` returning `(body, queryLets, StatementReads)`, then `resolve(...)`.

### TestKinds

- `plan` 50-85: scan, close, seed, dispatch.
- `frame` 93-122: env, chain, consumed, stamp, rows, eager run, CTE, register.
- `effect` 158-207: guard, prepare, effect check, literal folds, statements, last answer.

### PlanRunner.program, 159-344

Concerns, in order:
1. parameters;
2. the pre-scan, 168-247, which duplicates `sequence()` 654-710 with deliberate differences (an Effect does not set
   `changes`; Setup is allowed);
3. the result count;
4. the no-session branch, 252-274;
5. session and Work, 278-312;
6. the transaction choice, 313-335;
7. the answer's stream, 337-343, which duplicates 266-272.

Proposed: a pure `shape`, `runSteps`, `bracket`, `stream`.

## 4. The immutable design proposed

- **A statement's facts as a returned value:** `StatementReads(ownStamp, definitions, inline runs, stamps minted)`
  in `ProductStatements.Statement`. `ProgramState.lowered` reads it, not `frames`.
- **Definitions** are the names pass 2 read, in first-read order, filtered to `asCte`.
- **The program's facts as one owner's value:** frames, consumed, the stamp count, the program's rows.
- **The hook:** a single-owner per-statement recorder local to `inline(...)`, frozen into `StatementReads`.
  - Threading immutable state through the re-entrant 1,701-line inliner is not worth its cost.
  - `queryLets` stays an accumulating map with one owner during inlining: resolvers built mid-inlining observe it.
  - Freeze with `unmodifiableMap(new LinkedHashMap<>(…))`, never `Map.copyOf`, which loses order.
- **ProgramState:** a builder confined to `program()`. Its fields are private, there are no aliased locals,
  `List.copyOf` is what goes out, and TestKinds gets a view plus `add`.
- **ObjectSites stays a mutable minter.** The Lowerer calls `idOf` deep inside lowering.
- **TestKinds:** its `script` and `established` are its own private state. `generated` stays a private cache,
  because seed generation is expensive.
- **PlanRunner:** `shape` becomes pure; the run is a single-owner `Run{live, held}`; `answered` becomes
  `held.answer != null`. `live` is a legitimate local builder: its values are the database's, produced during the run.
- **SessionState** is mutable by nature. Type its key as `SeedKey` once the walk is gone.

## 5. Other defects

### Inline fully-qualified names

Counts are `com.legend` / `java.*`, approximate:

| file | com.legend | java.* |
|---|---|---|
| PlanMaker | 164 | 32 |
| PlanAllocations | 159 | 48 |
| BodyPlanner | 82 | 51 |
| EffectPlanner | 77 | 42 |
| ProductStatements | 59 | 15 |
| FramePlanner | 46 | 17 |
| PlanRunner | 37 | 39 |
| TestKinds | 36 | 7 |
| SidePlanner | 29 | 4 |
| Seeds | 27 | 1 |
| PlanEnv | 20 | 23 |
| QueryDoor | 19 | 4 |
| LetPlanner | 17 | 10 |
| ProgramFrames | 15 | 13 |
| ProgramState | 11 | 4 |

`@com.legend.base.Nullable` is spelled in full in most files.

### Orphan or stacked javadoc

- BodyPlanner 38-45 and 92-97;
- PlanAllocations 208-210;
- PlanRunner 616-617;
- ResultEnvelopeSplice:140-146 documents a `toSQLString` that does not exist.

### Stale comment

SessionState:13-15 claims a per-statement column-types memo that does not exist.

### Dead code

- the `reads` counts;
- `ProgramState.door()`;
- the 6-argument `registerActivityRows`, whose only caller is the walk (BodyDriver:511);
- `comment`, always null from in-scope callers;
- `DdlTexts`' `List<Object>`, re-checked as String (ProductStatements:122-126);
- the constant `ADMITTED.contains(...)` guards.

### Duplicated logic

- the target decision (`plan` vs `program`);
- `declared…declaration(ctx)` (63, 73, 93, 192);
- three lowering pipelines: ProgramState.lowered; the staged path (PlanMaker:287-289, which adds `namedColumns`
  BEFORE `EnumValueTables`); and `ProductStatements.lower`;
- the rename merge (BodyPlanner:234-242 vs FramePlanner:257-272);
- the protocol-let scans (PlanAllocations:389-405 vs 454-471);
- `FreeVars.ofBody`, computed twice;
- two effect memos;
- PlanEnv construction (ProductStatements:71 vs TestKinds:98);
- `executesOn`, called twice.

### Misleading names

- `reads`, which is both a field and a method in ProgramFrames;
- `PlanMaker.reads(Sql, String)`;
- `inTransaction = !ownsTransaction()`.

### Nullable-heavy APIs

- `prepare(runtime, mark)`;
- `constant()`, which returns null;
- `FramePlanner.renames`, where null means none;
- `View` and `Cte`, three nullable fields each;
- `registerActivityRows(comment, commentParameter)`.

### Unchecked downcasts

- PlanAllocations:193-194, 398-399, 463-464;
- TestKinds:65, 94-95.

### Swallowed exceptions returning null

PlanAllocations:201-205, 242-254, 435-438, 501-504; FramePlanner:98-105. There is also an environment-gated debug
print at PlanAllocations:248-252.

### Possible latent defects (unverified; brief B14 item 5 verifies them)

- The eager run and the CTE in TestKinds.frame are prepared on a fresh env, without `fc.env().tableReplace()`.
- `FrameCtes.attach` runs after `postProcessed`, so CTE bodies escape the table renames.
- A stamp minted by the eager run's own inline read is never bound.
- `SessionState.establish(A)` clears `dirty`, so other keys read as established again after a write.

### Guard constraints

- A new file joins `JavaEvalLedgerTest`'s `PLANNER_CLASSES`, an exact set, with a dated reason.
- `ArchitectureTest` pins `PlanMaker.testProgram` by name and the library's dependencies.

## 6. Byte-identity risks

- The ObjectSites call order: the eager run before the CTE; DML before the count.
- Stamp numbering: frame-let order, then the first inline call per statement, even for a registered scope. Stamps are
  never re-minted in pass 2.
- The order of `queryLets` (§2.6).
- The order of `definitions` (it decides the WITH order).
- Names: `frame_<let>`, `AliasPrefix.frameBody`, `StagedRows.table`, `#stamp<n>`.
- The `frames` LinkedHashMap order (the rename union and the conflict message).
- QueryDoor's EnumSet order (which refusal is told).
- `planRows`: when its entries exist, and what the closures capture.
- The second pass fires only on identity-counted pastes.
- Refusal order.
- The deliberate pre-scan differences between `program()` and `sequence()`.
- `unreadStampsDropped` rebuilds the Sql without its stamp.
