# M10a brief — the system database on the test road (the narrow M4; 2026-11-01)

The plan's §7 M10a (the user, 2026-11-01, on M4 narrow: "fine for now but needs a product follow up"; §9 "The system
database: a product follow-up"): a step's target names the graph's system database, seeded once per graph; the test
library plans it, the runner opens it; no product form changes. B7's one code path for the product is outside this
program. Written before the code (R15); its audit checks the build against it.

## Today

- The walk routes a BODY or a SIDE that reads the system metamodel store to the graph's system database
  (`BodyDriver.openRoute`: `SystemRoutes.of` the planner's rule -- the one store read is the system store, a mix with
  a user store refused by name; `SystemDatabase.of(ctx).routeFor(...)` the driver's per-graph session, opened and
  written once from `MetamodelSeeds`, its tables renamed to temporary names on a server engine and not at all in
  process). The side's plan then runs on that session (`PlanSessions.of(on)` ignores the plan's target).
- The program road runs a moved body on ONE session (`BodyRouter`: `PlanSessions.of(session)`), and the runner's
  `ProgramShape` requires one target over every step and side. So the class rule refuses a system read
  (`BodyClass.sideKind` → `Kind.SYSTEM_READ`), and the judges' planners refuse a side the walk routed elsewhere
  (`HostAsserts`: `sides.on() != env.session()`; `DatabaseAsserts`: `g.on() != env.session()`). The DuckDB corpus
  shows 392 `assert,system-read` bodies on each pass, plus mixed ones (`let-execute,system-read`,
  `effect,system-read`, `frame-system-read`).

## The design

1. **The plan names the system database.** `ExecutionPlan.Database.System(DatabaseType type)`: the graph's system
   database on this engine (its tables the metamodel's, written once by the one who opens it). PlanJson tag `system`,
   VERSION 25 -> 26. The runner is unchanged: a System target opens through the sessions source like any other, and
   `Statements.check` compares the engine type, which it carries.
2. **The test door plans a routed body's program on that target.** `BodyClass.systemRouted(body, runtime)`: every
   store the body's statements read (through the user functions they call) is the system store, no frame, no effect,
   no store write, and the runtime executes on an IN-PROCESS engine (DuckDB, H2, SQLite: the system tables keep their
   names there; a server engine's renamed temporary tables wait for the program road's renames -- refused by name,
   counted). `TestDoor.planned(..., systemRouted)` → `PlanMaker.testProgram(..., systemEngine)`: the program's Target
   is `System(engine)` with the dialect's session setup and no seeds.
3. **The sides carry the same target.** The asserts' sides are planned by the walk's planner on the system session
   as today (`openRoute`); `JudgePlans.target(env)` answers the System target for an environment whose session is the
   graph's system database (`SystemDatabase.engineOf(session)`), so the program's shape sees one target. On the walk
   this changes the side plan's target METADATA alone (`PlanSessions.of(on)` ignores it).
4. **The router opens the system session for the System target.** `PlanSessions.of(session, trace, systemOpener)`:
   a System target answers the opener's session (a non-closing view, as the body's), every other target the body's.
   The router's opener is the driver's `SystemDatabase.of(ctx).routeFor(engine, dialect, store, MetamodelSeeds::rows,
   options.systemConnections())` -- the same call `openRoute` makes, so the walk's side session and the program's are
   the same object, seeded once per graph.
5. **The guards compare against the program's session.** `HostAsserts.plan`/`DatabaseAsserts.plan` take the session
   the program will run on (the system session when routed, else the body's) and refuse a side on any other.
6. **The class rule admits the system read where the body is routed.** `sideKind` keeps refusing `SYSTEM_READ`
   unless the body is system-routed (step 2); `FRAME_PROGRAM`s with system reads stay on the walk (a frame's eager run
   targets its runtime's database: two targets), as do mixed bodies.

## What moves and what is measured

The 392 `assert,system-read` bodies per DuckDB pass (host and database judges); H2 likewise. Outcomes identical;
the statement-origin census may move its `system=` count (a side's statements sent on the system session under the
BODY origin): recorded, not an outcome. The parity test gains system-read bodies on the frame model (`x::M`, `x::DB`
read through the metamodel), run under both judges.

## Residue, expected

Server engines (Postgres, the rest): the system tables' temporary names are not applied on the program road → the
class rule refuses them by name. Frame programs over the system store (`let r = execute(..., systemRuntime)`): two
targets → walk. Mixed bodies (a user-store frame or effect beside system reads): walk. The product's own metamodel
carriage (`PlanMaker.metamodel`) unchanged (the follow-up the user named).
