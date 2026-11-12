// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.cache.Hash;
import com.legend.executionplan.ExecutionPlan;

import java.util.List;

/**
 * THE LOAD PLANNER (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.2 and P2, L2, 2026-11-09): the one place a plan's
 * {@link ExecutionPlan.Load} is made, and the one identity scheme every load takes -- the SHA-256 of a canonical
 * encoding of its steps (each field length-prefixed, a cell's absence marked, a file by its reference's digest, size
 * and format and its delivery), carried in the plan and never recomputed at the run. Equal steps name one load; steps
 * that load differently never do. A target's declared test data ({@link ExecutionPlan.Load.Cadence#PER_ACQUISITION}),
 * a program's seeds ({@code BEFORE_STEP}) and its own loads ({@code AT_STEP}) are all made here.
 */
public final class LoadPlanner {

    private LoadPlanner() {
    }

    /** {@code steps} as a load of {@code cadence} into {@code target}, named by its content ({@link #identity}). */
    public static ExecutionPlan.Load load(ExecutionPlan.Load.Cadence cadence, List<ExecutionPlan.LoadStep> steps,
            ExecutionPlan.Target target) {
        return new ExecutionPlan.Load(identity(cadence, steps), cadence, steps, target, java.util.Map.of());
    }

    /** {@code steps} as a load of {@code cadence} into {@code target} whose identity was digested already
     *  ({@link #identity}, memoized by its caller: a project's fill, per model). */
    static ExecutionPlan.Load load(String identity, ExecutionPlan.Load.Cadence cadence,
            List<ExecutionPlan.LoadStep> steps, ExecutionPlan.Target target) {
        return new ExecutionPlan.Load(identity, cadence, steps, target, java.util.Map.of());
    }

    /** {@link #load(String, ExecutionPlan.Load.Cadence, List, ExecutionPlan.Target)} reading {@code content}, the
     *  bundle its CONTENT file steps name (L6c). */
    static ExecutionPlan.Load load(String identity, ExecutionPlan.Load.Cadence cadence,
            List<ExecutionPlan.LoadStep> steps, ExecutionPlan.Target target, java.util.Map<String, String> content) {
        return new ExecutionPlan.Load(identity, cadence, steps, target, content);
    }

    /** {@code steps} as a load of {@code cadence} into {@code target} reading {@code content}, named by its steps'
     *  content (the digests stand for the bundle's bytes). */
    static ExecutionPlan.Load load(ExecutionPlan.Load.Cadence cadence, List<ExecutionPlan.LoadStep> steps,
            ExecutionPlan.Target target, java.util.Map<String, String> content) {
        return new ExecutionPlan.Load(identity(cadence, steps), cadence, steps, target, content);
    }

    /** {@code steps} as a load of {@code cadence} into {@code target}, reading their bundle (A2: a table's rows are a
     *  content blob of inserts), named by their content. */
    public static ExecutionPlan.Load load(ExecutionPlan.Load.Cadence cadence, com.legend.setup.LoadSteps steps,
            ExecutionPlan.Target target) {
        return load(cadence, steps.steps(), target, steps.content());
    }

    /**
     * A PROJECT'S DATA ROWS as the ONCE load its own database is made with, alone as a plan, or null for a model with
     * none (B3, 2026-11-11, docs/TOTAL_WALL_START_HERE_2026_11_11.md §5.2: "the hosts run the plan's loads"; census
     * M14): the load a server's project plan names its database with (the same steps, content and identity:
     * {@code PlanMaker}'s project fill), for a host that makes a project's database itself -- the tab's DuckDB,
     * Python's -- and runs it there, its statements blob checked against its digest. Until then the host was handed the
     * fill's statements as text ({@code ProjectData.fill}) and ran them outside any plan. A model whose Data rows cannot
     * make the database is refused by name ({@code ProjectData.refusal}).
     */
    public static @com.legend.base.Nullable ExecutionPlan projectData(com.legend.compiler.element.ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect, com.legend.executionplan.DatabaseType type) {
        com.legend.setup.LoadSteps fill = com.legend.setup.CsvSeed.planned(
                com.legend.setup.ProjectData.fillSteps(ctx, dialect), dialect);
        if (fill.isEmpty()) {
            return null;
        }
        ExecutionPlan.Target database = Targets.of(new ExecutionPlan.Database.Platform(type,
                new ExecutionPlan.Project(Hash.ofUtf8(com.legend.setup.ProjectData.contentKey(ctx)).hex(), null)),
                dialect);
        return ExecutionPlan.writing(List.of(), load(ExecutionPlan.Load.Cadence.ONCE, fill, database));
    }

    /** Where a remote file is, as a host names it (B3's Q3): an address that carries no token, written as it is, or a
     *  BEARER URL (a presigned link, its query string the token) the host holds by name, never handed to the planner. */
    public sealed interface RemoteLocation permits PlainLocation, BearerLocation {
    }

    /** An address that carries no token ({@code s3://}, {@code https://} with no query string): the plan names it. */
    public record PlainLocation(String url) implements RemoteLocation {
        public PlainLocation {
            java.util.Objects.requireNonNull(url, "url");
            if (url.indexOf('?') >= 0) {
                throw new IllegalArgumentException("a URL with a query string carries its token: the host holds it as a"
                        + " bearer URL, and the plan names it");
            }
            // a user in the address is a credential (the B checkpoint's tenets audit, LOW 5, 2026-11-11)
            com.legend.sql.dialect.HostOperation.Location.noUserinfo(url);
        }
    }

    /** A bearer URL the host holds under {@code name} (its field {@code URL}); {@code shown}, its address without the
     *  query string -- what a secret's scope is cut from, never the token. */
    public record BearerLocation(String name, String shown) implements RemoteLocation {
        public BearerLocation {
            java.util.Objects.requireNonNull(name, "name");
            java.util.Objects.requireNonNull(shown, "shown");
            if (shown.indexOf('?') >= 0) {
                throw new IllegalArgumentException("a bearer URL's shown address is its address without the query"
                        + " string, which carries the token");
            }
            com.legend.sql.dialect.HostOperation.Location.noUserinfo(shown);
        }
    }

    /** An S3 secret the host holds under {@code name}: the fields it holds (KEY_ID, SECRET, ...), never their values. */
    public record RemoteCredentials(String name, List<String> fields) {
        public RemoteCredentials {
            java.util.Objects.requireNonNull(name, "name");
            fields = List.copyOf(fields);
        }
    }

    /** A remote file mounted: the view it is read through ({@code null}: its access alone, no view), where it is, its
     *  format, and the credentials it is read with, if any. */
    public record RemoteMount(@com.legend.base.Nullable String view, RemoteLocation location,
            com.legend.sql.dialect.HostOperation.RemoteFormat format, @com.legend.base.Nullable RemoteCredentials credentials) {
        public RemoteMount {
            java.util.Objects.requireNonNull(location, "location");
            java.util.Objects.requireNonNull(format, "format");
        }
    }

    /** The field a bearer URL is held under in the host's store. */
    public static final String URL_FIELD = "URL";

    /**
     * A REMOTE FILE MOUNTED, as a plan (B3's Q3, 2026-11-11, the user's ruling of that day: "Yes do option 2";
     * docs/data-delivery-homework/B3_REMOTE_MOUNT_DESIGN_2026_11_11.md): one
     * {@link ExecutionPlan.Load.Cadence#HOST_SESSION} load, kept until the person lets the file go -- the extensions its
     * format reads with; its S3 secret, each field bound from the host's store by name and the scope, the plan's own
     * value, cut from its address; a bearer URL bound to a session variable the dialect names for it (a step of the
     * CONNECTION, which the host replays on every connection it opens: a variable is one connection's, measured); and the
     * view, reading the address as it is or the variable. No credential, no token, in any statement or in the plan:
     * each is named, the host binds it. Until then the host ran the mount as operations outside any plan, the token in
     * the view's text (census M14). A bearer Iceberg table is refused by name (a table is a folder: there is no
     * presigned link to it).
     */
    public static ExecutionPlan remoteMount(com.legend.sql.dialect.SqlDialect dialect,
            com.legend.executionplan.DatabaseType type, RemoteMount mount) {
        com.legend.sql.dialect.HostOperations host = com.legend.sql.dialect.HostOperations.of(dialect);
        boolean iceberg = mount.format() == com.legend.sql.dialect.HostOperation.RemoteFormat.ICEBERG;
        com.legend.setup.LoadSteps.Builder steps = new com.legend.setup.LoadSteps.Builder();
        for (String sql : host.statements(new com.legend.sql.dialect.HostOperation.Extensions(
                iceberg ? List.of("httpfs", "iceberg") : List.of("httpfs")))) {
            steps.statement(sql);
        }
        String address = switch (mount.location()) {
            case PlainLocation p -> p.url();
            case BearerLocation b -> {
                if (iceberg) {
                    throw new IllegalArgumentException("an Iceberg table is a folder, read by its address: a bearer URL"
                            + " names one file (" + b.shown() + ")");
                }
                yield b.shown();
            }
        };
        if (mount.credentials() != null && !mount.credentials().fields().isEmpty()) {
            RemoteCredentials c = mount.credentials();
            com.legend.sql.dialect.HostOperation.Secret secret = new com.legend.sql.dialect.HostOperation.Secret(
                    c.name(), scope(address), c.fields());
            List<ExecutionPlan.LoadStep.Bind> binds = new java.util.ArrayList<>();
            for (String field : c.fields()) {
                binds.add(com.legend.setup.LoadSteps.held(c.name(), field));
            }
            binds.add(com.legend.setup.LoadSteps.value(secret.scope()));
            steps.add(com.legend.setup.LoadSteps.bound(host.statements(secret), binds,
                    ExecutionPlan.Reach.DATABASE));
        }
        com.legend.sql.dialect.HostOperation.Location at = switch (mount.location()) {
            case PlainLocation p -> new com.legend.sql.dialect.HostOperation.Location.Url(p.url());
            case BearerLocation b -> {
                steps.add(com.legend.setup.LoadSteps.bound(host.statements(
                        new com.legend.sql.dialect.HostOperation.SetVariable(b.name())),
                        List.of(com.legend.setup.LoadSteps.held(b.name(), URL_FIELD)),
                        ExecutionPlan.Reach.CONNECTION));
                yield new com.legend.sql.dialect.HostOperation.Location.Variable(b.name());
            }
        };
        if (mount.view() != null) {
            for (String sql : host.statements(new com.legend.sql.dialect.HostOperation.MakeRelation(
                    com.legend.sql.dialect.HostOperation.Kind.VIEW,
                    com.legend.sql.dialect.HostOperation.Name.of(mount.view()), true, null, List.of(),
                    new com.legend.sql.dialect.HostOperation.Source.Remote(at, mount.format())))) {
                steps.statement(sql);
            }
        }
        ExecutionPlan.Target target = Targets.of(new ExecutionPlan.Database.Platform(type, null), dialect);
        return ExecutionPlan.writing(List.of(), load(ExecutionPlan.Load.Cadence.HOST_SESSION, steps.build(), target));
    }

    /**
     * A SNAPSHOT'S STREAM, as a host asks for its plan: {@code plane}, the name the host holds the remote plane under
     * (the warehouse a grid reads live); {@code sql}, the statement that plane runs (the snap's source query, written by
     * the planner for the plane's own database); {@code schema} (none: the database's own) and {@code table}, the table
     * of the host's database it lands in, a name of the host's own (quoted, whatever it holds).
     */
    public record SnapStream(String plane, String sql, @com.legend.base.Nullable String schema, String table) {
        public SnapStream {
            java.util.Objects.requireNonNull(plane, "plane");
            java.util.Objects.requireNonNull(sql, "sql");
            java.util.Objects.requireNonNull(table, "table");
        }
    }

    /**
     * A SNAPSHOT FROM A REMOTE PLANE, as a plan (the user's ruling of 2026-11-11, "This is the right plan"; the
     * orchestrator's simplification of 2026-11-12: no hash, the count and the done step guard a stream cut short): one
     * {@link ExecutionPlan.Load.Cadence#HOST_SESSION} load whose one step streams the plane's answer into the table --
     * its schema made and the table there before dropped first, the database counting what landed, all in one
     * transaction the host commits only when the stream completed and the count is the plane's own. The plan names the
     * plane, its statement and the table; never a row. Until then the tab's DuckDbEngine.loadArrow and Python's landing
     * table took the rows outside any plan, with no transaction (census, the snapshot rows).
     */
    public static ExecutionPlan snapStream(com.legend.sql.dialect.SqlDialect dialect,
            com.legend.executionplan.DatabaseType type, SnapStream snap) {
        com.legend.sql.dialect.HostOperations host = com.legend.sql.dialect.HostOperations.of(dialect);
        com.legend.sql.dialect.HostOperation.Name table = new com.legend.sql.dialect.HostOperation.Name(null,
                snap.schema(), snap.table(), false);
        List<String> prepare = new java.util.ArrayList<>();
        if (snap.schema() != null) {
            prepare.addAll(host.statements(new com.legend.sql.dialect.HostOperation.MakeSchema(null, snap.schema())));
        }
        prepare.addAll(host.statements(new com.legend.sql.dialect.HostOperation.Drop(
                com.legend.sql.dialect.HostOperation.Kind.TABLE, table)));
        List<String> count = host.statements(new com.legend.sql.dialect.HostOperation.Count(
                new com.legend.sql.dialect.HostOperation.Source.Relation(table)));
        if (count.size() != 1) {
            throw new IllegalStateException("a stream's table is counted by one statement, not " + count.size());
        }
        com.legend.setup.LoadSteps steps = com.legend.setup.LoadSteps.stream(snap.plane(), snap.sql(), snap.schema(),
                snap.table(), prepare, count.get(0), transaction(host, com.legend.sql.dialect.HostOperation.Transaction.Step.BEGIN),
                transaction(host, com.legend.sql.dialect.HostOperation.Transaction.Step.COMMIT),
                transaction(host, com.legend.sql.dialect.HostOperation.Transaction.Step.ROLLBACK));
        return ExecutionPlan.writing(List.of(), load(ExecutionPlan.Load.Cadence.HOST_SESSION, steps,
                Targets.of(new ExecutionPlan.Database.Platform(type, null), dialect)));
    }

    private static List<String> transaction(com.legend.sql.dialect.HostOperations host,
            com.legend.sql.dialect.HostOperation.Transaction.Step step) {
        return host.statements(new com.legend.sql.dialect.HostOperation.Transaction(step));
    }

    /**
     * A HOST'S VALUE READ AS A RELATION (a Python frame), as a host asks for its plan: {@code held}, the name the host
     * holds the value under (the frame's); {@code as}, the relation it is attached as; {@code live}, read at each query
     * (a view over the attached value, kept on every connection) or once (a table copied from it, let go); and the
     * {@code schema} (none: the database's own) and {@code relation} it is read through.
     */
    public record FrameLoad(String held, String as, boolean live, @com.legend.base.Nullable String schema,
            String relation) {
        public FrameLoad {
            java.util.Objects.requireNonNull(held, "held");
            java.util.Objects.requireNonNull(as, "as");
            java.util.Objects.requireNonNull(relation, "relation");
        }
    }

    /**
     * A HOST'S VALUE READ AS A RELATION, as a plan (the user's ruling of 2026-11-11, "This is the right plan": a Python
     * frame is named, attached at run): one {@link ExecutionPlan.Load.Cadence#HOST_SESSION} load -- the value attached
     * by name ({@link ExecutionPlan.Reach#CONNECTION} for a live frame, which the host keeps and attaches on every
     * connection and cursor it opens, so every query reads the frame as it is then; {@link ExecutionPlan.Reach#DATABASE}
     * for one read once, let go when the load ends), the relation there before dropped, and the view over it or the
     * table copied from it. The plan names the value, never its bytes. Until then Python registered the frame's Arrow
     * on the connection and on each read's cursor outside any plan (census, the frames row).
     */
    public static ExecutionPlan frameLoad(com.legend.sql.dialect.SqlDialect dialect,
            com.legend.executionplan.DatabaseType type, FrameLoad frame) {
        com.legend.sql.dialect.HostOperations host = com.legend.sql.dialect.HostOperations.of(dialect);
        com.legend.sql.dialect.HostOperation.Kind kind = frame.live() ? com.legend.sql.dialect.HostOperation.Kind.VIEW
                : com.legend.sql.dialect.HostOperation.Kind.TABLE;
        com.legend.sql.dialect.HostOperation.Name target = new com.legend.sql.dialect.HostOperation.Name(null,
                frame.schema(), frame.relation(), false);
        com.legend.setup.LoadSteps.Builder steps = new com.legend.setup.LoadSteps.Builder();
        steps.add(com.legend.setup.LoadSteps.attach(frame.held(), frame.as(),
                frame.live() ? ExecutionPlan.Reach.CONNECTION : ExecutionPlan.Reach.DATABASE));
        for (String sql : host.statements(new com.legend.sql.dialect.HostOperation.Drop(kind, target))) {
            steps.statement(sql);
        }
        for (String sql : host.statements(new com.legend.sql.dialect.HostOperation.MakeRelation(kind, target, false,
                null, List.of(), new com.legend.sql.dialect.HostOperation.Source.Relation(
                        com.legend.sql.dialect.HostOperation.Name.of(frame.as()))))) {
            steps.statement(sql);
        }
        return ExecutionPlan.writing(List.of(), load(ExecutionPlan.Load.Cadence.HOST_SESSION, steps.build(),
                Targets.of(new ExecutionPlan.Database.Platform(type, null), dialect)));
    }

    /** Where a secret's keys apply: its address as a prefix of the URLs it is used for -- cut before a glob, an Iceberg
     *  table's metadata folder its table's (its data files lie beside it). */
    static String scope(String address) {
        String scope = address;
        int glob = -1;
        for (int i = 0; i < scope.length(); i++) {
            char ch = scope.charAt(i);
            if (ch == '*' || ch == '?' || ch == '[') {
                glob = i;
                break;
            }
        }
        if (glob >= 0) {
            scope = scope.substring(0, glob);
        }
        return scope.replaceFirst("(?i)/metadata/?$", "");
    }

    /** Where a host's own file goes: a table of its own (a person's upload), or a model's declared table (a person's
     *  file put into Studio's workspace, over the model's own rows). */
    public sealed interface HostFileInto permits NewTable, DeclaredTable {
    }

    /** A table of the file's own, {@code table} (a person's name, quoted), made or replaced from the file's columns. */
    public record NewTable(String table) implements HostFileInto {
        public NewTable {
            java.util.Objects.requireNonNull(table, "table");
        }
    }

    /** The table {@code schema.table} the Database {@code database} of {@code ctx} declares, made as declared and filled
     *  from the file, each declared column from the file's column of its name. */
    public record DeclaredTable(com.legend.compiler.element.ModelContext ctx, String database, String schema,
            String table) implements HostFileInto {
    }

    /**
     * A HOST'S OWN FILE READ INTO ITS DATABASE, as a plan (B3, 2026-11-11, the orchestrator's go: "uploads and Studio's
     * file puts as planned loads, the file a pointer"; the design's B3 row: "a dropped file is a pointer DuckDB reads"):
     * one {@link ExecutionPlan.Load.Cadence#HOST_SESSION} load -- kept until the person resets it -- whose file step names
     * the host's file by reference ({@link com.legend.executionplan.DataRef.Namespace#HOST}: its name, digest, size and
     * format) and is delivered as its database reads it, the statement's {@code ?} the file as the host's store binds it.
     * A table of its own is made or replaced from the file (its types the reader's: a CSV detected, Parquet's, JSON's
     * records); a declared table is made as the model declares it (its DDL the seed's, a header alone) and filled from
     * the file (a CSV's cells as text, a Parquet's as typed, the database converting each to its column's type). The
     * host runs the plan (engine-client's runLoad); exec has no store of a host's files, so it refuses one by name.
     */
    public static ExecutionPlan hostFile(com.legend.sql.dialect.SqlDialect dialect,
            com.legend.executionplan.DatabaseType type, com.legend.executionplan.DataRef file, HostFileInto into) {
        if (file.namespace() != com.legend.executionplan.DataRef.Namespace.HOST) {
            throw new IllegalArgumentException("a host's file load names a HOST file: " + file);
        }
        com.legend.sql.dialect.HostOperations host = com.legend.sql.dialect.HostOperations.of(dialect);
        List<ExecutionPlan.LoadStep> steps = new java.util.ArrayList<>();
        java.util.Map<String, String> content = java.util.Map.of();
        com.legend.sql.dialect.HostOperation read = switch (into) {
            case NewTable n -> new com.legend.sql.dialect.HostOperation.MakeRelation(
                    com.legend.sql.dialect.HostOperation.Kind.TABLE, com.legend.sql.dialect.HostOperation.Name.of(n.table()),
                    true, null, List.of(), new com.legend.sql.dialect.HostOperation.Source.HostFile(switch (file.format()) {
                        case com.legend.executionplan.DataRef.DetectedCsv d -> com.legend.sql.dialect.HostOperation.FileFormat.CSV;
                        case com.legend.executionplan.DataRef.Parquet p -> com.legend.sql.dialect.HostOperation.FileFormat.PARQUET;
                        case com.legend.executionplan.DataRef.Json j -> com.legend.sql.dialect.HostOperation.FileFormat.JSON;
                        case com.legend.executionplan.DataRef.Csv c -> throw new IllegalArgumentException("a table of a"
                                + " file's own detects a CSV's columns: its format is detectedCsv, not a declared CSV");
                        case com.legend.executionplan.DataRef.Statements st -> throw new IllegalArgumentException(
                                "a host's file is no statements blob");
                    }));
            case DeclaredTable d -> {
                com.legend.model.DatabaseDefinition.TableDefinition def = d.ctx().findTableDefinition(d.database(),
                        "default".equals(d.schema()) ? d.table() : d.schema() + "." + d.table()).orElseThrow(() ->
                        new IllegalArgumentException("a file fills " + d.schema() + "." + d.table() + ", which the"
                                + " Database " + d.database() + " declares not"));
                // the table as the model declares it, empty: the seed's statements for its header alone
                com.legend.setup.LoadSteps made = com.legend.setup.CsvSeed.planned(com.legend.setup.CsvSeed.steps(
                        d.schema() + "\n" + d.table() + "\n" + String.join(",", def.columns().stream()
                                .map(com.legend.model.DatabaseDefinition.ColumnDefinition::name).toList()),
                        d.database(), d.ctx(), dialect), dialect);
                steps.addAll(made.steps());
                content = made.content();
                yield new com.legend.sql.dialect.HostOperation.Insert(
                        com.legend.sql.dialect.HostOperation.Name.declared(d.schema(), d.table()),
                        def.columns().stream().map(c -> new com.legend.sql.dialect.HostOperation.Column(c.name(), c.name()))
                                .toList(),
                        new com.legend.sql.dialect.HostOperation.Source.HostFile(switch (file.format()) {
                            case com.legend.executionplan.DataRef.Csv c -> com.legend.sql.dialect.HostOperation.FileFormat.CSV_TEXT;
                            case com.legend.executionplan.DataRef.Parquet p -> com.legend.sql.dialect.HostOperation.FileFormat.PARQUET;
                            case com.legend.executionplan.DataRef.DetectedCsv dc -> throw new IllegalArgumentException(
                                    "a declared table's CSV is read as text, cast into its declared columns: its format"
                                            + " is csv, not detectedCsv");
                            case com.legend.executionplan.DataRef.Json j -> throw new IllegalArgumentException(
                                    "a declared table is filled from a .csv or a .parquet file, not JSON");
                            case com.legend.executionplan.DataRef.Statements st -> throw new IllegalArgumentException(
                                    "a host's file is no statements blob");
                        }));
            }
        };
        // the host's own file, read by its database's statements (setup.LoadSteps: the planner's makers make a step)
        steps.addAll(com.legend.setup.LoadSteps.hostFile(file, host.statements(read)).steps());
        ExecutionPlan.Target target = Targets.of(new ExecutionPlan.Database.Platform(type, null), dialect);
        return ExecutionPlan.writing(List.of(), load(ExecutionPlan.Load.Cadence.HOST_SESSION, steps, target, content));
    }

    /** {@code steps} as the loads a target establishes on acquisition: none for no step, else one load into
     *  {@code target}'s own database and session. */
    public static List<ExecutionPlan.Load> targetLoads(com.legend.setup.LoadSteps steps,
            ExecutionPlan.Target target) {
        return steps.isEmpty() ? List.of()
                : List.of(load(ExecutionPlan.Load.Cadence.PER_ACQUISITION, steps, target.bare()));
    }

    /** {@code target} establishing {@code steps} on acquisition (none: the target as it is). */
    public static ExecutionPlan.Target withLoads(ExecutionPlan.Target target, com.legend.setup.LoadSteps steps) {
        return new ExecutionPlan.Target(target.database(), target.session(), targetLoads(steps, target),
                target.raiseReadBack());
    }

    /** {@link #withLoads(ExecutionPlan.Target, com.legend.setup.LoadSteps)} for steps that read no blob. */
    public static ExecutionPlan.Target withLoads(ExecutionPlan.Target target, List<ExecutionPlan.LoadStep> steps) {
        return withLoads(target, com.legend.setup.LoadSteps.of(steps));
    }

    /** A load's name: the SHA-256 of its canonical content ({@link ExecutionPlan.Load#canonical}: its cadence and its
     *  steps' content), so equal loads share it and loads that run differently never do. Exec recomputes it before it
     *  runs a load and refuses one whose identity is not its content's (the M1 tenets audit's note, 2026-11-09). */
    public static String identity(ExecutionPlan.Load.Cadence cadence, List<ExecutionPlan.LoadStep> steps) {
        return ExecutionPlan.Load.IDENTITY_PREFIX + Hash.ofUtf8(ExecutionPlan.Load.canonical(cadence, steps)).hex();
    }

    /** The test data {@code runtime}'s connections declare, as load steps final for {@code dialect}'s database: a plan's
     *  target's acquisition load. Its steps are {@code CsvSeed.declaredSteps}', which {@link #seeds} plans too, beside a
     *  statement's other seeds, in one load (B2a, 2026-11-11: until then the target's in {@code PlanMaker}, the seeds'
     *  in the test planner's {@code Seeds.planned}; the B2a audit's note: the javadoc said seeds called this). */
    public static com.legend.setup.LoadSteps declared(String runtime,
            com.legend.compiler.element.ModelContext ctx, com.legend.sql.dialect.SqlDialect dialect) {
        return com.legend.setup.CsvSeed.planned(com.legend.setup.CsvSeed.declaredSteps(runtime, ctx, dialect), dialect);
    }

    /**
     * {@code statement}'s SEEDS as a program's load steps, final for {@code dialect}'s database (B2a, 2026-11-11: the
     * load planner's, moved from the test planner's {@code Seeds.planned}), with the referee's own copy of them, written
     * in {@code referee}'s dialect (A2; the edge's, until B2b replays the plan's loads): each {@code from()}'s inline test
     * data and named runtime's declared data, then the program's {@code runtime}'s -- each runtime's once, and each
     * from()'s inline data once (the correctness audit's C3, 2026-11-10: a from()'s runtime that is the program's too was
     * planned twice, its setup SQL failing "already exists", its rows doubled; the A delta audit's note 10, 2026-11-11:
     * two from()s of one instance runtime too).
     */
    public static com.legend.setup.CsvSeed.Planned seeds(com.legend.compiler.spec.typed.TypedSpec statement,
            @com.legend.base.Nullable String runtime, com.legend.compiler.element.ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect, com.legend.sql.dialect.SqlDialect referee) {
        List<com.legend.setup.CsvSeed.Step> setups = new java.util.ArrayList<>();
        java.util.Set<String> declared = new java.util.HashSet<>();
        java.util.Set<List<Object>> inline = new java.util.HashSet<>();
        for (com.legend.compiler.spec.typed.TypedFrom fr : com.legend.compiler.spec.typed.ExecutionContext.froms(
                statement)) {
            // (the setup SQL's language asked only where there is setup SQL: a connection with none may declare none)
            if (inline.add(List.of(fr.sqlSetups(), fr.csvSetups(),
                    fr.sqlSetups().isEmpty() ? "" : String.valueOf(fr.context().rawLanguage())))) {
                for (String sql : fr.sqlSetups()) {
                    // written in the database the from()'s connection declares (decision D2)
                    setups.add(new com.legend.setup.CsvSeed.Step.Sql(sql, fr.context().rawLanguage()));
                }
                setups.addAll(com.legend.setup.CsvSeed.setupSteps(fr, ctx, dialect));
            }
            // an ELEMENT runtime named by the from(): its declared connections' test data (the instance form above
            // carries the same facts on the instance)
            fr.context().runtime().ifPresent(ref -> {
                if (declared.add(ref.fullPath())) {
                    setups.addAll(com.legend.setup.CsvSeed.declaredSteps(ref.fullPath(), ctx, dialect));
                }
            });
        }
        // the program's runtime: its declared connections' test data seeds the session the same way
        if (runtime != null && declared.add(runtime)) {
            setups.addAll(com.legend.setup.CsvSeed.declaredSteps(runtime, ctx, dialect));
        }
        return com.legend.setup.CsvSeed.planned(setups, dialect, referee);
    }
}
