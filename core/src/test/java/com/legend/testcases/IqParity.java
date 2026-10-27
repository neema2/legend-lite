// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlSyntaxException;
import com.legend.protocol.sql.iq.IqDialect;
import com.legend.protocol.sql.iq.IqLexer;
import com.legend.protocol.sql.iq.IqParser;
import com.legend.protocol.sql.iq.IqProfile;
import com.legend.testbackend.TestBackend;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The {@code #IQ{...}#} island's parity cases (docs/IQ_SQL_STUDY_2026_10_13.md §6): legend-lite's own, in
 * {@code core/src/test/resources/iq-parity/lite/*.yaml}, over the tables of {@code iq-parity/schema.yaml}, each run
 * under each of its profiles as an island over those tables, and judged against SQL Anywhere 17's answer snapped by
 * {@code tools/iq-oracle} into {@code iq-answers.jsonl} -- or, for a case that names a provisional decision, against
 * the rows the decision gives ({@code decision}, {@code expect}). The judging, the statuses, the ratchet and the ledger
 * are {@link SqlParity}'s; this class has what differs: the case keys, the profiles, the island's spelling, the FROM
 * rewrite (through {@link IqLexer}), the IQ types of the schema.
 *
 * <p>The snapped answers are {@link IqAnswers}', which also guards them against the cases.
 *
 * <p>IQ island, landing 4 (WP1), 2026-10-14: real SAP IQ 17.1 is the oracle of the {@code iq17-*} profiles. Every case
 * that runs under an {@code iq16-*} profile runs under the matching {@code iq17-*} one too ({@link #runs}), judged by
 * the live IQ's snapped answer ({@code snap.sh --oracle iq}); the {@code iq16-*} and {@code sqla17-*} runs stay judged
 * by SQL Anywhere. Where lite follows SQL Anywhere's semantics and IQ's differ, an {@code iq17-*} run fails: each such
 * run is named, with its cause, in the known-divergence allowlist {@code iq-parity/iq17-divergences.tsv}, which only
 * shrinks ({@link #divergences}).
 */
public final class IqParity {

    /** The database every case's tables are in, and the runtime that reaches it. */
    public static final String DATABASE = "e2e::DB";
    public static final String RUNTIME = "e2e::RT";

    /** The keys a case may carry (tools/iq-oracle/README.md): any other is an error. */
    private static final Set<String> CASE_KEYS = Set.of("id", "sql", "profile", "engine", "skip", "sa_defect",
            "decision", "expect", "params", "settings", "nondeterministic", "member");

    private static final Pattern DECISION = Pattern.compile("D-[A-Z]+[0-9]+[a-z]?");

    private IqParity() {
    }

    // ------------------------------------------------------------------ the cases

    /** A case: its file (relative to iq-parity/, {@code lite/...}), its id, its SQL, its profiles, the engine it pins
     *  (null for auto), why it is not run (a skip or an SA defect), and the provisional decision that judges it, with
     *  the decided rows (one line of JSON); IQ island, 2026-10-14 (review 7, the user's ruling on B1): why its answer
     *  may be one plan's choice ({@code nondeterministic}: it tests a refusal or an error, {@link #determinism}); IQ
     *  island, 2026-10-15 (the user's rule: any answer IQ could give is right): why a string it shows is one value of a
     *  group of values IQ's collation holds equal, IQ's choice of which ({@code member}: its strings are compared by
     *  that collation, {@link #compare}). */
    public record Case(String file, String id, String sql, List<String> profiles, @com.legend.base.Nullable String engine,
            @com.legend.base.Nullable String skip, @com.legend.base.Nullable String decision,
            @com.legend.base.Nullable String expect, @com.legend.base.Nullable String params,
            @com.legend.base.Nullable String settings, @com.legend.base.Nullable String nondeterministic,
            @com.legend.base.Nullable String member) {

        /** A case whose strings are compared as they are. */
        public Case(String file, String id, String sql, List<String> profiles,
                @com.legend.base.Nullable String engine, @com.legend.base.Nullable String skip,
                @com.legend.base.Nullable String decision, @com.legend.base.Nullable String expect,
                @com.legend.base.Nullable String params, @com.legend.base.Nullable String settings,
                @com.legend.base.Nullable String nondeterministic) {
            this(file, id, sql, profiles, engine, skip, decision, expect, params, settings, nondeterministic, null);
        }

        /** A case without settings (IQ island, 2026-10-14, the options framework: {@code settings}, IQ's settings by
         *  level as written, {@code database.<name>=<value>, query.<name>=<value>, ...}) that does not opt out of the
         *  determinism lint. */
        public Case(String file, String id, String sql, List<String> profiles,
                @com.legend.base.Nullable String engine, @com.legend.base.Nullable String skip,
                @com.legend.base.Nullable String decision, @com.legend.base.Nullable String expect,
                @com.legend.base.Nullable String params) {
            this(file, id, sql, profiles, engine, skip, decision, expect, params, null, null, null);
        }

        /** A case that binds no variables. */
        public Case(String file, String id, String sql, List<String> profiles,
                @com.legend.base.Nullable String engine, @com.legend.base.Nullable String skip,
                @com.legend.base.Nullable String decision, @com.legend.base.Nullable String expect) {
            this(file, id, sql, profiles, engine, skip, decision, expect, null, null, null, null);
        }

        /** The case's key in the answers file: {@code lite/<file>/<id>}. */
        public String key() {
            return file.replaceAll("\\.yaml$", "") + "/" + id;
        }
    }

    /** IQ island, 2026-10-14 (M1e): a variable a case binds ({@code params}: {@code name = value; ...}), for its host
     *  variables ({@code @name}, {@code :name}): its name, its Pure type, its value as a Pure literal (a {@code let}
     *  before the island), and the SQL Anywhere variable the referee holds it in (tools/iq-oracle/snap.py reads the same
     *  spelling): its type and its value as SQL Anywhere's constant, null for a NULL. IQ island, 2026-10-14 (review 5's
     *  M5): the referee binds a real variable of that type on the case's connection (it wrote the constant into the SQL
     *  before, which SQL Anywhere reads otherwise in measured places); {@code null:<type>} is a NULL, a Pure empty value
     *  of that type. */
    public record Param(String name, String type, String pure, String saType, @com.legend.base.Nullable String saValue) {

        /** Whether the value is a NULL (an empty Pure value). */
        public boolean isNull() {
            return saValue == null;
        }
    }

    private static final Pattern PARAM = Pattern.compile("([A-Za-z_][A-Za-z0-9_]*)\\s*=\\s*(.+)");
    private static final Pattern PARAM_INTEGER = Pattern.compile("-?[0-9]+");
    private static final Pattern PARAM_FLOAT = Pattern.compile("-?[0-9]+\\.[0-9]+");
    private static final Pattern PARAM_DECIMAL = Pattern.compile("-?[0-9]+\\.([0-9]+)D");
    private static final Pattern PARAM_DATE = Pattern.compile("%([0-9]{4}-[0-9]{2}-[0-9]{2})");
    private static final Pattern PARAM_TIMESTAMP = Pattern.compile(
            "%([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,6})?)");
    private static final Pattern PARAM_NULL = Pattern.compile(
            "null:(Integer|Float|String|StrictDate|DateTime|Decimal\\(38,([0-9]+)\\))");

    /** A case's {@code params}, in order: {@code name = 5} (an Integer, SQL Anywhere's BIGINT), {@code 1.5} (a Float, a
     *  DOUBLE), {@code 1.50D} (a Decimal of scale 2, a NUMERIC(38,2)), {@code 'text'} (a String, a LONG VARCHAR; no
     *  quote or backslash inside, which the two readers would read otherwise), {@code %2026-03-04} (a StrictDate, a
     *  DATE), {@code %2026-03-04T05:06:07.123456} (a DateTime, a TIMESTAMP), {@code null:Integer} (a NULL of that type:
     *  Integer, Float, String, StrictDate, DateTime or Decimal(38,s)). Names are distinct ignoring case (SQL Anywhere's
     *  variable names are). */
    public static List<Param> params(@com.legend.base.Nullable String params) {
        List<Param> out = new ArrayList<>();
        if (params == null) {
            return out;
        }
        Set<String> seen = new java.util.HashSet<>();
        for (String entry : params.split(";")) {
            if (entry.strip().isEmpty()) {
                continue;
            }
            Matcher m = PARAM.matcher(entry.strip());
            if (!m.matches()) {
                throw new IllegalStateException("a param is name = value, not " + entry);
            }
            String name = m.group(1);
            if (!seen.add(name.toLowerCase(Locale.ROOT))) {
                throw new IllegalStateException("the param " + name + " twice (SQL Anywhere's variable names ignore"
                        + " case)");
            }
            String v = m.group(2).strip();
            Matcher d;
            if (PARAM_INTEGER.matcher(v).matches()) {
                out.add(new Param(name, "Integer", v, "BIGINT", v));
            } else if (PARAM_FLOAT.matcher(v).matches()) {
                out.add(new Param(name, "Float", v, "DOUBLE", "CAST(" + v + " AS DOUBLE)"));
            } else if ((d = PARAM_DECIMAL.matcher(v)).matches()) {
                int scale = d.group(1).length();
                // IQ island, 2026-10-14 (review 5's m6): Pure types a negative decimal literal (-0.50D, a minus of
                // 0.50D) a plain Decimal, which the island refuses: the engine-neutrality check reads it so too
                out.add(new Param(name, v.startsWith("-") ? "Decimal" : "Decimal(38," + scale + ")", v,
                        "NUMERIC(38," + scale + ")", v.substring(0, v.length() - 1)));
            } else if (v.length() >= 2 && v.startsWith("'") && v.endsWith("'")
                    && v.indexOf('\'', 1) == v.length() - 1) {
                if (v.indexOf('\\') >= 0) {
                    throw new IllegalStateException("a string param holds no backslash (SQL Anywhere reads one as an"
                            + " escape, Pure's let does not), not " + v);
                }
                out.add(new Param(name, "String", v, "LONG VARCHAR", v));
            } else if ((d = PARAM_DATE.matcher(v)).matches()) {
                out.add(new Param(name, "StrictDate", v, "DATE", "CAST('" + d.group(1) + "' AS DATE)"));
            } else if ((d = PARAM_TIMESTAMP.matcher(v)).matches()) {
                out.add(new Param(name, "DateTime", v, "TIMESTAMP", "CAST('" + d.group(1) + " " + d.group(2)
                        + "' AS TIMESTAMP)"));
            } else if ((d = PARAM_NULL.matcher(v)).matches()) {
                String pure = d.group(2) != null ? "Decimal" : d.group(1);
                String sa = switch (d.group(1)) {
                    case "Integer" -> "BIGINT";
                    case "Float" -> "DOUBLE";
                    case "String" -> "LONG VARCHAR";
                    case "StrictDate" -> "DATE";
                    case "DateTime" -> "TIMESTAMP";
                    default -> "NUMERIC(38," + d.group(2) + ")";
                };
                out.add(new Param(name, d.group(2) != null ? "Decimal(38," + d.group(2) + ")" : pure,
                        "[]->cast(@" + pure + ")", sa, null));
            } else {
                throw new IllegalStateException("a param's value is an integer, a float, a decimal (1.5D), a 'string',"
                        + " a %date, a %date-time or null:<type>, not " + v);
            }
        }
        return out;
    }

    /** One run of a case: the case under one of its profiles. Its key in the ledger is the case's,
     *  {@code @} and the profile. */
    public record Run(Case c, IqProfile profile) {

        public String key() {
            return c.key() + "@" + profile.spelling();
        }

        /** IQ island, 2026-10-14 (the options framework): the settings the run's SQL is read under -- its profile's,
         *  with its case's settings given ({@link IqParity#settings}, {@code IqSettings.resolve}) -- before the SET
         *  statements its SQL may begin with; the profile's when the settings are IQ's error (the run raises it). */
        public com.legend.protocol.sql.iq.IqSettings settings() {
            if (c.settings() == null) {
                return profile.settings();
            }
            try {
                return com.legend.protocol.sql.iq.IqSettings.resolve(profile, IqParity.settings(
                        java.util.Objects.requireNonNull(c.settings())));
            } catch (com.legend.protocol.sql.SqlTranslationException e) {
                return profile.settings();
            }
        }

        /** The run's SELECT as the island reads it: its text with the SET statements before it blanked, and the
         *  settings they make; {@link SqlSyntaxException} or {@code SqlTranslationException} when they are not read. */
        public com.legend.protocol.sql.iq.IqSetPrefix.Prefix select(String sql) {
            com.legend.protocol.sql.iq.IqSetPrefix.Prefix p = com.legend.protocol.sql.iq.IqSetPrefix.read(sql,
                    settings());
            return p != null ? p : new com.legend.protocol.sql.iq.IqSetPrefix.Prefix(settings(), sql);
        }

        /** The run as SqlParity's ratchet keeps one: its file and an id that holds the profile. */
        SqlParity.Case asSqlParityCase() {
            return new SqlParity.Case(c.file(), c.id() + "@" + profile.spelling(), c.sql(), c.skip());
        }
    }

    /** Every case of the case files under {@code liteDir}, in file order (files by path). */
    public static List<Case> cases(Path liteDir) throws IOException {
        List<Path> files;
        try (java.util.stream.Stream<Path> walk = Files.walk(liteDir)) {
            files = walk.filter(p -> p.toString().endsWith(".yaml"))
                    .sorted(java.util.Comparator.comparing(p -> liteDir.relativize(p).toString().replace('\\', '/')))
                    .toList();
        }
        List<Case> cases = new ArrayList<>();
        Set<String> keys = new java.util.HashSet<>();
        for (Path f : files) {
            String file = "lite/" + liteDir.relativize(f).toString().replace('\\', '/');
            for (Map<String, Object> m : SqlParity.Yaml.tests(Files.readString(f, StandardCharsets.UTF_8), file)) {
                Case c = toCase(file, m);
                if (!keys.add(c.key())) {
                    throw new IllegalStateException(file + ": the id " + c.id() + " twice");
                }
                cases.add(c);
            }
        }
        return cases;
    }

    private static Case toCase(String file, Map<String, Object> m) {
        String where = file + ": case " + m.get("id");
        for (String k : m.keySet()) {
            if (!CASE_KEYS.contains(k)) {
                throw new IllegalStateException(where + ": unknown key " + k + " (known: " + CASE_KEYS + ")");
            }
        }
        if (!(m.get("id") instanceof String id) || !(m.get("sql") instanceof String sql)) {
            throw new IllegalStateException(where + ": a case needs an id and its sql as text");
        }
        List<String> profiles = new ArrayList<>();
        Object written = m.get("profile");
        for (String p : (written == null ? IqProfile.DEFAULT.spelling() : written.toString()).split(",")) {
            String name = p.strip();
            if (IqProfile.named(name) == null || profiles.contains(name)) {
                throw new IllegalStateException(where + ": an unknown profile, or one named twice: " + name);
            }
            profiles.add(name);
        }
        String engine = (String) m.get("engine");
        if (engine != null && !Set.of("iq", "sa", "auto").contains(engine)) {
            throw new IllegalStateException(where + ": engine is iq, sa or auto, not " + engine);
        }
        String decision = (String) m.get("decision");
        String expect = (String) m.get("expect");
        if (decision != null && !DECISION.matcher(decision).matches() || (decision == null) != (expect == null)) {
            throw new IllegalStateException(where + ": a decision is D-<letters><number>, and comes with expect");
        }
        String skip = (String) m.get("skip");
        if (skip == null && m.get("sa_defect") != null) {
            skip = "sa_defect: " + m.get("sa_defect");
        }
        String params = (String) m.get("params");
        params(params);
        String settings = (String) m.get("settings");
        if (settings != null) {
            settings(settings);
        }
        Object nondeterministic = m.get("nondeterministic");
        if (nondeterministic != null && (!(nondeterministic instanceof String why) || why.strip().isEmpty())) {
            throw new IllegalStateException(where + ": nondeterministic states its reason (the case tests a refusal or"
                    + " an error)");
        }
        Object member = m.get("member");
        if (member != null && (!(member instanceof String why) || why.strip().isEmpty())) {
            throw new IllegalStateException(where + ": member states its reason (which value of a group of equal"
                    + " strings the query shows is IQ's choice)");
        }
        return new Case(file, id, sql, profiles, engine == null || engine.equals("auto") ? null : engine, skip,
                decision, expect, params, settings, (String) nondeterministic, (String) member);
    }

    /** Every run of {@code cases}: each case under each of its profiles, in order, then (IQ island, landing 4, WP1,
     *  2026-10-14) under the {@code iq17-*} twin of each {@code iq16-*} one it does not name (tools/iq-oracle's
     *  {@code twins} derives the same runs). */
    public static List<Run> runs(List<Case> cases) {
        List<Run> runs = new ArrayList<>();
        for (Case c : cases) {
            List<String> profiles = new ArrayList<>(c.profiles());
            for (String p : c.profiles()) {
                String twin = p.startsWith("iq16-") ? "iq17-" + p.substring("iq16-".length()) : null;
                if (twin != null && !profiles.contains(twin)) {
                    profiles.add(twin);
                }
            }
            for (String p : profiles) {
                runs.add(new Run(c, java.util.Objects.requireNonNull(IqProfile.named(p))));
            }
        }
        return runs;
    }

    /** Whether a profile's runs are judged by the live IQ (the {@code iq17-*} profiles: IQ island, landing 4, WP1),
     *  not by SQL Anywhere. */
    public static boolean liveIq(IqProfile profile) {
        return profile.spelling().startsWith("iq17-");
    }

    // ------------------------------------------------------------------ the schema, the model and the data

    /** The tables of {@code schema.yaml}. */
    public static List<SqlParity.Table> schema(Path schemaFile) throws IOException {
        return SqlParity.Yaml.schema(Files.readString(schemaFile, StandardCharsets.UTF_8));
    }

    private static final Pattern SIZED = Pattern.compile("(char|varchar)\\((\\d+)\\)");
    private static final Pattern SCALED = Pattern.compile("(numeric|decimal)\\((\\d+),\\s*(\\d+)\\)");

    /** The Pure model of the schema: one database, each column at the relational type of its IQ type, a connection
     *  and a runtime. */
    public static String model(List<SqlParity.Table> tables, String connection) {
        StringBuilder m = new StringBuilder("###Relational\nDatabase " + DATABASE + "\n(\n");
        for (SqlParity.Table t : tables) {
            m.append("    Table ").append(quoted(t.name())).append("\n    (\n");
            for (int i = 0; i < t.columns().size(); i++) {
                SqlParity.Column c = t.columns().get(i);
                m.append("        ").append(quoted(c.name())).append(' ').append(relationalType(c.type()))
                        .append(i + 1 < t.columns().size() ? ",\n" : "\n");
            }
            m.append("    )\n");
        }
        m.append(")\n###Connection\nRelationalDatabaseConnection e2e::Conn\n{\n    ").append(connection)
                .append("\n}\n###Runtime\nRuntime ").append(RUNTIME).append("\n{\n    mappings: [];\n    connections: [ ")
                .append(DATABASE).append(": [ environment: e2e::Conn ] ];\n}\n");
        return m.toString();
    }

    /** A table's or a column's name as the model and the DDL write it: as it is when it is a plain identifier, else in
     *  double quotes (IQ island, 2026-10-15, the corpus lane: Tableau's "Number of Records"). */
    static String quoted(String name) {
        return name.matches("[A-Za-z_][A-Za-z0-9_]*") && !RESERVED.contains(name.toLowerCase(Locale.ROOT)) ? name
                : "\"" + name.replace("\"", "\"\"") + "\"";
    }

    /** The words the lanes' databases reserve, which a name the corpora use must be quoted as. */
    private static final Set<String> RESERVED = Set.of("all", "and", "any", "as", "asc", "between", "both", "by", "case",
            "cast", "check", "column", "constraint", "create", "cross", "current_date", "current_time", "default",
            "desc", "distinct", "do", "else", "end", "except", "false", "fetch", "for", "foreign", "from", "full",
            "grant", "group", "having", "in", "inner", "intersect", "into", "is", "join", "leading", "left", "like",
            "limit", "natural", "not", "null", "offset", "on", "only", "or", "order", "outer", "primary", "references",
            "right", "select", "table", "then", "to", "trailing", "true", "union", "unique", "user", "using", "when",
            "where", "window", "with");

    /** An IQ type of the schema as the relational store spells it (the store type the island reads). */
    static String relationalType(String iqType) {
        String t = iqType.toLowerCase(Locale.ROOT).strip();
        Matcher sized = SIZED.matcher(t);
        if (sized.matches()) {
            return sized.group(1).toUpperCase(Locale.ROOT) + "(" + sized.group(2) + ")";
        }
        Matcher scaled = SCALED.matcher(t);
        if (scaled.matches()) {
            return scaled.group(1).toUpperCase(Locale.ROOT) + "(" + scaled.group(2) + "," + scaled.group(3) + ")";
        }
        return switch (t) {
            case "smallint", "bigint", "tinyint", "double", "real", "float", "date", "timestamp", "bit" ->
                    t.toUpperCase(Locale.ROOT);
            case "integer", "int" -> "INTEGER";
            default -> throw new IllegalStateException("no relational type for the schema's " + iqType);
        };
    }

    /** An IQ type of the schema as the lane's table declares it, for its dialect to spell: a BIT is a boolean (the
     *  relational store's BIT is Pure's Boolean), a CHAR(n) a VARCHAR(n), holding the value as written (a padded CHAR
     *  is the IQ engine's rule to apply -- D-S1, the island's read -- not the store's), a TINYINT a SMALLINT, a FLOAT a
     *  DOUBLE, a NUMERIC a DECIMAL. */
    private static com.legend.sql.SqlDdl.ColumnType columnType(String iqType) {
        String t = relationalType(iqType);
        Matcher sized = Pattern.compile("(CHAR|VARCHAR)\\((\\d+)\\)").matcher(t);
        if (sized.matches()) {
            return new com.legend.sql.SqlDdl.ColumnType.Sized("VARCHAR", Integer.parseInt(sized.group(2)));
        }
        Matcher scaled = Pattern.compile("(NUMERIC|DECIMAL)\\((\\d+),(\\d+)\\)").matcher(t);
        if (scaled.matches()) {
            return new com.legend.sql.SqlDdl.ColumnType.Scaled("DECIMAL", Integer.parseInt(scaled.group(2)),
                    Integer.parseInt(scaled.group(3)));
        }
        return new com.legend.sql.SqlDdl.ColumnType.Plain(switch (t) {
            case "BIT" -> com.legend.sql.SqlDdl.ColumnType.Kind.BIT;
            case "DOUBLE", "FLOAT" -> com.legend.sql.SqlDdl.ColumnType.Kind.DOUBLE;
            case "TINYINT", "SMALLINT" -> com.legend.sql.SqlDdl.ColumnType.Kind.SMALLINT;
            case "INTEGER" -> com.legend.sql.SqlDdl.ColumnType.Kind.INTEGER;
            case "BIGINT" -> com.legend.sql.SqlDdl.ColumnType.Kind.BIGINT;
            case "REAL" -> com.legend.sql.SqlDdl.ColumnType.Kind.REAL;
            case "DATE" -> com.legend.sql.SqlDdl.ColumnType.Kind.DATE;
            case "TIMESTAMP" -> com.legend.sql.SqlDdl.ColumnType.Kind.TIMESTAMP;
            default -> throw new IllegalStateException("no column type for the schema's " + iqType);
        });
    }

    /** Whether {@code name} is declared quoted: a name that is not a plain identifier, or that a lane's database
     *  reserves ({@link #quoted}'s rule). */
    private static boolean declaredQuoted(String name) {
        return !quoted(name).equals(name);
    }

    /** Creates and fills the schema's tables on {@code backend}'s database, each rendered by its dialect: each value
     *  bound by its column's type, a quoted value read by it. */
    public static void seed(TestBackend backend, Connection c, List<SqlParity.Table> tables) throws SQLException {
        com.legend.sql.dialect.SqlDialect dialect = backend.dialect();
        for (SqlParity.Table t : tables) {
            com.legend.sql.NameQuoting quoting = new com.legend.sql.NameQuoting(false, declaredQuoted(t.name()));
            List<com.legend.sql.SqlDdl.Column> columns = new ArrayList<>();
            for (SqlParity.Column col : t.columns()) {
                columns.add(new com.legend.sql.SqlDdl.Column(col.name(), declaredQuoted(col.name()),
                        columnType(col.type()), false, false));
            }
            try (Statement s = c.createStatement()) {
                s.execute(dialect.render(new com.legend.sql.SqlDdl.DropTable(null, t.name(), quoting)));
                s.execute(dialect.render(new com.legend.sql.SqlDdl.CreateTable(null, t.name(), quoting, columns)));
            }
            if (t.rows().isEmpty()) {
                continue;
            }
            String marks = String.join(", ", java.util.Collections.nCopies(t.columns().size(), "?"));
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + quoted(t.name()) + " VALUES (" + marks
                    + ")")) {
                for (List<Object> row : t.rows()) {
                    for (int i = 0; i < row.size(); i++) {
                        bind(ps, i + 1, row.get(i), relationalType(t.columns().get(i).type()));
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }

    private static void bind(PreparedStatement ps, int i, @com.legend.base.Nullable Object v, String type)
            throws SQLException {
        if (v == null) {
            ps.setNull(i, java.sql.Types.NULL);
            return;
        }
        String text = v.toString();
        if (type.equals("BIT")) {
            ps.setBoolean(i, !text.equals("0"));
        } else if (type.equals("SMALLINT") || type.equals("INTEGER") || type.equals("TINYINT")) {
            ps.setInt(i, Integer.parseInt(text));
        } else if (type.equals("BIGINT")) {
            ps.setLong(i, Long.parseLong(text));
        } else if (type.startsWith("NUMERIC") || type.startsWith("DECIMAL")) {
            ps.setBigDecimal(i, new BigDecimal(text));
        } else if (type.equals("DOUBLE") || type.equals("FLOAT") || type.equals("REAL")) {
            ps.setDouble(i, Double.parseDouble(text));
        } else if (type.equals("DATE")) {
            ps.setDate(i, java.sql.Date.valueOf(text));
        } else if (type.equals("TIMESTAMP")) {
            ps.setTimestamp(i, java.sql.Timestamp.valueOf(text));
        } else {
            ps.setString(i, text);
        }
    }

    /** The names of the schema's tables, as the rewrite matches them. */
    public static Set<String> tableNames(List<SqlParity.Table> tables) {
        return SqlParity.tableNames(tables);
    }

    // ------------------------------------------------------------------ the rewrite

    /** The names a leading {@code WITH [RECURSIVE] name [(...)] AS (...), ...} defines, in lower case. */
    private static Set<String> cteNames(List<IqLexer.Token> tokens) {
        Set<String> out = new java.util.HashSet<>();
        if (tokens.isEmpty() || !tokens.get(0).is("with")) {
            return out;
        }
        int i = 1;
        if (i < tokens.size() && tokens.get(i).isWord("recursive")) {
            i++;
        }
        while (i < tokens.size() && tokens.get(i).isName()) {
            out.add(tokens.get(i).text().toLowerCase(Locale.ROOT));
            i++;
            // the optional column list, AS, then the CTE's parenthesized query
            for (int round = 0; round < 2 && i < tokens.size(); round++) {
                if (tokens.get(i).is("as")) {
                    i++;
                }
                if (i < tokens.size() && tokens.get(i).isOp("(")) {
                    int depth = 0;
                    do {
                        IqLexer.Token x = tokens.get(i);
                        depth += x.isOp("(") ? 1 : x.isOp(")") ? -1 : 0;
                        i++;
                    } while (depth > 0 && i < tokens.size());
                }
            }
            if (i < tokens.size() && tokens.get(i).isOp(",")) {
                i++;
            } else {
                break;
            }
        }
        return out;
    }

    /** {@code sql} with each schema table named in FROM, after JOIN or after a comma in FROM ({@code t}, {@code [t]},
     *  {@code T}) replaced by its Legend source, {@code tb('e2e::DB.t')}, aliased by the name as written when no alias
     *  follows. The text is read by {@link IqLexer} under the run's profile; text that does not lex is returned as it
     *  is, for the island to refuse. */
    public static String rewrite(String sql, Set<String> tables, IqProfile profile) {
        return rewrite(sql, tables, profile.settings());
    }

    /** {@link #rewrite(String, Set, IqProfile)} under {@code settings} (IQ island, 2026-10-14, the options framework:
     *  a run's, its case's settings given over its profile's). */
    public static String rewrite(String sql, Set<String> tables, com.legend.protocol.sql.iq.IqSettings settings) {
        List<IqLexer.Token> tokens;
        try {
            tokens = IqLexer.tokenize(sql, settings);
        } catch (SqlSyntaxException e) {
            return sql;
        }
        StringBuilder out = new StringBuilder();
        int copied = 0;
        Deque<Boolean> inFrom = new ArrayDeque<>();
        inFrom.push(false);
        // IQ island, 2026-10-15: a common table expression's name hides a table's, so it is no table to rewrite
        Set<String> ctes = cteNames(tokens);
        for (int i = 0; i < tokens.size(); i++) {
            IqLexer.Token t = tokens.get(i);
            if (t.isOp("(")) {
                // IQ island, 2026-10-14 (M1b): a parenthesis at a table's place (a parenthesized join, a derived table)
                // is in the FROM clause too; any other (a call's, a subquery's in a condition) is not
                IqLexer.Token before = i == 0 ? t : tokens.get(i - 1);
                inFrom.push(Boolean.TRUE.equals(inFrom.peek()) && (before.is("from") || before.is("join")
                        || before.isOp(",") || before.isOp("(")));
                continue;
            }
            if (t.isOp(")")) {
                if (inFrom.size() > 1) {
                    inFrom.pop();
                }
                continue;
            }
            if (t.kind() == IqLexer.Kind.KEYWORD) {
                boolean was = Boolean.TRUE.equals(inFrom.pop());
                inFrom.push(t.is("from") || t.is("join") || was && !CLAUSES.contains(t.text().toLowerCase(Locale.ROOT)));
                if (t.is("from") || t.is("join")) {
                    continue;
                }
            }
            if (i == 0) {
                continue;
            }
            IqLexer.Token prev = tokens.get(i - 1);
            boolean tablePlace = prev.is("from") || prev.is("join")
                    || (prev.isOp(",") || prev.isOp("(")) && Boolean.TRUE.equals(inFrom.peek());
            IqLexer.Token after = tokens.get(Math.min(i + 1, tokens.size() - 1));
            if (!tablePlace || !t.isName() || !tables.contains(t.text().toLowerCase(Locale.ROOT))
                    || ctes.contains(t.text().toLowerCase(Locale.ROOT)) || after.isOp(".") || after.isOp("(")) {
                continue;
            }
            boolean aliased = after.is("as") || after.isName();
            IqLexer.Token alias = after.is("as") ? tokens.get(Math.min(i + 2, tokens.size() - 1)) : after;
            if (aliased && alias.isName() && alias.text().equalsIgnoreCase(t.text())) {
                // IQ island, 2026-10-14 (review 7's m4): FROM t AS t is left to the island, which binds t to the
                // model's table and keeps the alias (IQ's -845 for DBA.t.id there); tb('...t') AS t is the island's
                // spelling of t unaliased
                continue;
            }
            out.append(sql, copied, t.start()).append("tb('").append(DATABASE).append('.')
                    .append(t.text().toLowerCase(Locale.ROOT)).append("')");
            if (!aliased) {
                out.append(" AS ").append(sql, t.start(), t.end());
            }
            copied = t.end();
        }
        return out.append(sql, copied, sql.length()).toString();
    }

    /** The keywords that end a FROM clause (IQ island, 2026-10-14, M1b: not ON, whose condition a comma or a JOIN
     *  ends, both back in the FROM clause). */
    private static final Set<String> CLAUSES = Set.of("where", "group", "having", "order", "union", "except",
            "intersect", "window", "for", "option", "into", "select", "using");

    /** The island a run is: {@code #IQ{...}#} under the default profile, {@code #IQ:<profile>{...}#} under another. */
    public static String island(Run run, Set<String> tables) {
        String sql = rewrite(run.c().sql(), tables, run.profile());
        String island = run.profile() == IqProfile.DEFAULT ? "#IQ{" + sql + "}#"
                : "#IQ:" + run.profile().spelling() + "{" + sql + "}#";
        // IQ island, 2026-10-14 (M1e): the variables a case binds, each a let before the island
        List<Param> params = params(run.c().params());
        if (params.isEmpty()) {
            return island;
        }
        StringBuilder lets = new StringBuilder("|");
        params.forEach(p -> lets.append("let ").append(p.name()).append(" = ").append(p.pure()).append("; "));
        return lets.append(island).append(';').toString();
    }

    /** A run whose case pins an engine: the engine IQ runs the island in must be it (an island cannot choose one). */
    public static @com.legend.base.Nullable String engineMismatch(Run run, Set<String> tables) {
        String pinned = run.c().engine();
        if (pinned == null) {
            return null;
        }
        SqlAst.Statement statement;
        com.legend.protocol.sql.iq.IqSetPrefix.Prefix select;
        try {
            select = run.select(rewrite(run.c().sql(), tables, run.settings()));
            statement = IqParser.parse(select.select(), select.settings());
        } catch (SqlSyntaxException | com.legend.protocol.sql.SqlTranslationException e) {
            return null;
        }
        String engine = IqDialect.engineOf(statement, select.settings()).name().toLowerCase(Locale.ROOT);
        return engine.equals(pinned) ? null : "ENGINE: " + run.key() + " pins engine=" + pinned + ", and its island"
                + " runs in engine=" + engine;
    }

    /** A run whose case SQL Anywhere judges (no decision, no pinned engine) over tables, translated under the IQ
     *  engine's rules: those must be SQL Anywhere's too (the study's §6.3, engine neutrality), else the case names the
     *  decision that judges it, or pins {@code engine: sa}. A translation the IQ engine's rules refuse is never judged
     *  by SQL Anywhere, so it is no breach; one they answer and SQL Anywhere's rules do not, or answer otherwise, is. */
    public static @com.legend.base.Nullable String engineNeutrality(Run run, List<SqlParity.Table> tables,
            Set<String> names) {
        Case c = run.c();
        if (liveIq(run.profile())) {
            // IQ island, landing 4 (WP1): real IQ judges an iq17-* run, by the IQ engine's own semantics
            return null;
        }
        if ("iq".equals(c.engine()) && c.decision() == null && c.skip() == null) {
            // IQ island, 2026-10-14 (the second review's m2): a pin to the IQ engine would escape this guard while
            // SQL Anywhere still judges the case
            return "ENGINE NEUTRALITY: " + run.key() + " pins engine=iq and names no decision: SQL Anywhere judges it,"
                    + " so drop the pin (the guard then checks it translates alike under both engines' rules), or name"
                    + " the decision that judges it";
        }
        if (c.engine() != null || c.decision() != null || c.skip() != null) {
            return null;
        }
        String sql = rewrite(c.sql(), names, run.settings());
        SqlAst.Statement statement;
        com.legend.protocol.sql.iq.IqSetPrefix.Prefix select;
        try {
            select = run.select(sql);
            statement = IqParser.parse(select.select(), select.settings());
        } catch (SqlSyntaxException | com.legend.protocol.sql.SqlTranslationException e) {
            return null;
        }
        if (IqDialect.engineOf(statement, select.settings()) != IqDialect.Engine.IQ) {
            return null;
        }
        if (run.profile().spelling().startsWith("iq16-") && liveJudgesItsSettings(c)
                && select.select().strip().equals(sql.strip())) {
            // IQ island, 2026-10-15 (landing 4, the research's D-N3: IQ's engine widens integer arithmetic, so most
            // integer arithmetic over IQ tables translates otherwise under its rules): this guard predates the live
            // IQ. An iq16-* run now has an iq17-* twin (runs) that the live IQ 17.1 judges by the IQ engine's own
            // semantics, so the IQ engine's translation is judged where it differs; SQL Anywhere stays a second judge,
            // whose disagreement fails the run (which then names its decision). Kept for a case with SET statements or
            // settings other than the query's own (temporary options), whose twin the live IQ may not judge
            // (not_judged: another database property or PUBLIC option).
            return null;
        }
        List<Param> params = params(c.params());
        String iq = pureOf(sql, tables, IqDialect.of(run.settings(), IqDialect.Engine.IQ), params);
        if (iq == null) {
            return null;
        }
        String sa = pureOf(sql, tables, IqDialect.of(run.settings(), IqDialect.Engine.SA), params);
        return iq.equals(sa) ? null : "ENGINE NEUTRALITY: " + run.key() + " translates otherwise under the IQ engine's"
                + " rules than under SQL Anywhere's, which judges it: name its decision, or pin engine: sa";
    }

    /** Whether the live IQ judges {@code c}'s iq17-* twin whatever its settings: none, or the query's own only (the
     *  temporary options a connection sets; IQ island, 2026-10-15, {@link #engineNeutrality}). */
    private static boolean liveJudgesItsSettings(Case c) {
        String written = c.settings();
        if (written == null) {
            return true;
        }
        for (com.legend.protocol.sql.iq.IqSettings.Setting setting : settings(written)) {
            if (setting.level() != com.legend.protocol.sql.iq.IqSettings.Level.QUERY) {
                return false;
            }
        }
        return true;
    }

    /** The skip a case takes when its answer depends on the order IQ evaluates a search condition's operands in (the
     *  study's provisional decision D-E2). */
    public static final String PLAN_DEPENDENT = "plan-dependent run-time error (D-E2)";

    /** A run whose WHERE puts an operation that can raise (arithmetic or NULLIF over a column) beside AND or OR
     *  (outside a CASE's or an IF's condition): SQL Anywhere evaluates such an operand, or not, as its plan has it, and
     *  lite's databases short-circuit, so the answer is no evidence either way (D-E2). Such a case is skipped
     *  ({@link #PLAN_DEPENDENT}) or rewritten; null for any other. */
    public static @com.legend.base.Nullable String evaluationOrder(Run run) {
        if (run.c().skip() != null) {
            return null;
        }
        SqlAst.Statement statement;
        try {
            com.legend.protocol.sql.iq.IqSetPrefix.Prefix select = run.select(run.c().sql());
            statement = IqParser.parse(select.select(), select.settings());
        } catch (SqlSyntaxException | com.legend.protocol.sql.SqlTranslationException e) {
            return null;
        }
        // IQ island, 2026-10-14 (M1b): every query's WHERE (a subquery's, a branch's, a derived table's) and every
        // join's ON is such a search condition; (review 3's M4) and every HAVING
        List<SqlAst.Expr> conditions = new ArrayList<>();
        for (SqlAst.Statement q : IqDialect.queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                if (s.where() != null) {
                    conditions.add(java.util.Objects.requireNonNull(s.where()));
                }
                if (s.having() != null) {
                    conditions.add(java.util.Objects.requireNonNull(s.having()));
                }
                s.from().forEach(f -> onConditions(f, conditions));
            }
        }
        if (conditions.stream().noneMatch(IqParity::raisesBesideAndOr)) {
            return null;
        }
        return "EVALUATION ORDER: " + run.key() + " puts an operation that can raise beside AND or OR in a WHERE, an"
                + " ON or a HAVING: whether IQ raises depends on its plan (D-E2), so skip it (skip: " + PLAN_DEPENDENT
                + ") or rewrite it";
    }

    private static void onConditions(SqlAst.FromItem f, List<SqlAst.Expr> out) {
        if (f instanceof SqlAst.Join j) {
            if (j.on() != null) {
                out.add(java.util.Objects.requireNonNull(j.on()));
            }
            onConditions(j.left(), out);
            onConditions(j.right(), out);
        }
    }

    /** Whether a search condition has an AND or OR (outside a CASE) one of whose operands holds an operation that
     *  can raise. */
    private static boolean raisesBesideAndOr(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.Binary b when b.op() == SqlAst.BinaryOp.AND || b.op() == SqlAst.BinaryOp.OR ->
                    canRaise(b.left()) || canRaise(b.right()) || raisesBesideAndOr(b.left())
                            || raisesBesideAndOr(b.right());
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.NOT -> raisesBesideAndOr(u.operand());
            // IQ island, 2026-10-14 (M1a): a truth test's condition is the condition the AND or OR stands in
            case SqlAst.IsTruth t -> raisesBesideAndOr(t.operand());
            default -> false;
        };
    }

    /** Whether {@code e} holds integer arithmetic, a minus or NULLIF over a column (a CASE's or an IF's branches
     *  included: IQ evaluates the one it takes). */
    private static boolean canRaise(SqlAst.Expr e) {
        boolean here = switch (e) {
            case SqlAst.Binary b -> switch (b.op()) {
                case PLUS, MINUS, TIMES, DIVIDE, MODULO -> readsAColumn(e);
                default -> false;
            };
            case SqlAst.Unary u -> u.op() == SqlAst.UnaryOp.MINUS && readsAColumn(e);
            // IQ island, 2026-10-14 (M1d): the functions and the CASTs that can raise IQ's errors over a column
            case SqlAst.FunctionCall f -> RAISING_FUNCTIONS.contains(f.name().get(f.name().size() - 1).text()
                    .toLowerCase(Locale.ROOT)) && readsAColumn(e);
            case SqlAst.Cast c -> readsAColumn(e);
            default -> false;
        };
        return here || subexpressions(e).stream().anyMatch(IqParity::canRaise);
    }

    /** The functions whose result can be IQ's run-time error (NULLIF's conversion; M1d's: SUBSTRING's negative length,
     *  MOD's zero divisor, ABS's and ROUND's overflow, the IQ engine's domain errors; review 3's M4: SUM's and AVG's
     *  overflow, D-E3). */
    private static final Set<String> RAISING_FUNCTIONS = Set.of("nullif", "substring", "substr", "mod", "remainder",
            "abs", "round", "sqrt", "exp", "ln", "log", "log10", "sum", "avg");

    private static boolean readsAColumn(SqlAst.Expr e) {
        return e instanceof SqlAst.ColumnRef || subexpressions(e).stream().anyMatch(IqParity::readsAColumn);
    }

    /** The expressions directly inside {@code e}, for the forms IQ's reader makes. */
    private static List<SqlAst.Expr> subexpressions(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.Binary b -> List.of(b.left(), b.right());
            case SqlAst.Unary u -> List.of(u.operand());
            case SqlAst.IsNull n -> List.of(n.operand());
            // IQ island, 2026-10-14 (M1a): the predicates IQ's reader now makes
            case SqlAst.IsTruth t -> List.of(t.operand());
            case SqlAst.IsDistinct d -> List.of(d.left(), d.right());
            case SqlAst.Between b -> List.of(b.operand(), b.low(), b.high());
            case SqlAst.InList in -> {
                List<SqlAst.Expr> all = new ArrayList<>(List.of(in.operand()));
                all.addAll(in.values());
                yield all;
            }
            case SqlAst.Like l -> l.escape() == null ? List.of(l.operand(), l.pattern())
                    : List.of(l.operand(), l.pattern(), java.util.Objects.requireNonNull(l.escape()));
            case SqlAst.FunctionCall f -> f.args();
            // IQ island, 2026-10-14 (M1b): a subquery's own expressions are its query's
            case SqlAst.InSubquery q -> List.of(q.operand());
            case SqlAst.Quantified q -> List.of(q.left());
            case SqlAst.Case c -> {
                List<SqlAst.Expr> all = new ArrayList<>();
                if (c.operand() != null) {
                    all.add(java.util.Objects.requireNonNull(c.operand()));
                }
                for (SqlAst.When w : c.whens()) {
                    all.add(w.condition());
                    all.add(w.result());
                }
                if (c.otherwise() != null) {
                    all.add(java.util.Objects.requireNonNull(c.otherwise()));
                }
                yield all;
            }
            // IQ island, 2026-10-14 (M1d): CAST and CONVERT
            case SqlAst.Cast c -> List.of(c.operand());
            // IQ island, 2026-10-15 (windows): a window call's arguments and keys
            case SqlAst.Window w -> {
                List<SqlAst.Expr> all = new ArrayList<>(w.call().args());
                all.addAll(w.spec().partitionBy());
                w.spec().orderBy().forEach(o -> all.add(o.expr()));
                yield all;
            }
            default -> List.of();
        };
    }

    // ------------------------------------------------------------------ determinism (IQ island, 2026-10-14)

    /** The column every schema table holds once per row, never NULL ({@link #determinism}: checked against the rows). */
    public static final String KEY = "id";

    /** The aggregates the island reads: a query of one of them and no GROUP BY has at most one row. */
    private static final Set<String> AGGREGATES = Set.of("count", "sum", "avg", "min", "max", "median", "list");

    /**
     * IQ island, 2026-10-14 (review 7's B1, the user's ruling): a committed case is deterministic. Where IQ's own answer
     * is not determined -- which rows a TOP, FIRST or START AT keeps where its ORDER BY keys tie at the cut, the order
     * of a LIST's values -- any answer IQ could return is right, and lite returns its database's; but a case judged so
     * would pin one plan's choice, SQL Anywhere's snapped against DuckDB's or Postgres's. So a run fails here when its
     * SQL has
     * <ul>
     *   <li>a row limit (TOP, FIRST, START AT) without ORDER BY;</li>
     *   <li>a row limit whose ORDER BY reaches no column unique in its source: a table's {@link #KEY},
     *       {@code sa_rowgenerator}'s {@code row_num}, a derived table's column unique in its query, a grouped query's
     *       keys (all of them), or every item of a SELECT DISTINCT -- for each FROM item, the ORDER BY keys need one
     *       of its unique columns, unless an inner join's ON or the WHERE equates one with a column of an item they
     *       order;</li>
     *   <li>a LIST without its own ORDER BY, unless it lists a GROUP BY key of its query or a constant (its values
     *       are one);</li>
     *   <li>IQ island, 2026-10-15 (windows): a window function that reads its window's rows in its order (ROW_NUMBER,
     *       NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, an aggregate over a ROWS frame) whose PARTITION BY and ORDER BY
     *       keys reach no unique column of a FROM item, as a row limit's must ({@link #unorderedWindows}).</li>
     * </ul>
     * A limit over at most one row (no FROM, DUMMY, an aggregate without GROUP BY) needs no ORDER BY, nor does one
     * inside EXISTS, which reads only whether a row is kept (SQL Anywhere rejects an ORDER BY there, -131). A case
     * may opt out ({@code nondeterministic: <reason>}) only when it tests a refusal or an error: then the run must not
     * PASS, and a case with nothing to opt out of may not. A skipped case is not run, and a statement IQ's reader
     * rejects keeps no rows. Returns the run's problem, or null.
     */
    public static @com.legend.base.Nullable String determinism(Run run, List<SqlParity.Table> tables,
            SqlParity.Status status) {
        Case c = run.c();
        if (c.skip() != null) {
            return null;
        }
        List<String> found;
        try {
            // the SELECT after the SET statements a run may begin with, read under the settings they make; a
            // statement the reader rejects, or settings IQ raises an error for, keeps no rows
            com.legend.protocol.sql.iq.IqSetPrefix.Prefix select = run.select(c.sql());
            found = nondeterminism(IqParser.parse(select.select(), select.settings()), tables);
        } catch (SqlSyntaxException | com.legend.protocol.sql.SqlTranslationException e) {
            found = List.of();
        }
        String why = c.nondeterministic();
        if (why == null) {
            return found.isEmpty() ? null : "NONDETERMINISTIC: " + run.key() + ": " + String.join("; ", found)
                    + " -- add the source's unique key (" + KEY + ") as the last ORDER BY key, or, for a case that"
                    + " tests a refusal or an error, say so (nondeterministic: <reason>)";
        }
        if (found.isEmpty()) {
            return "NONDETERMINISTIC: " + run.key() + " opts out (" + why + "), but its SQL has nothing to opt out of";
        }
        return status == SqlParity.Status.PASS ? "NONDETERMINISTIC: " + run.key() + " opts out (" + why + "), but it"
                + " PASSes: its answer may be one plan's choice (" + String.join("; ", found) + ")" : null;
    }

    /** What makes {@code statement}'s answer one plan's choice ({@link #determinism}), each in a few words. */
    static List<String> nondeterminism(SqlAst.Statement statement, List<SqlParity.Table> tables) {
        Map<String, SqlParity.Table> byName = new LinkedHashMap<>();
        for (SqlParity.Table t : tables) {
            byName.put(t.name().toLowerCase(Locale.ROOT), t);
        }
        Set<SqlAst.Statement> underExists = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        List<SqlAst.Statement> queries = IqDialect.queries(statement);
        for (SqlAst.Statement q : queries) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : clauses(s)) {
                    existsQueries(e, underExists);
                }
            }
        }
        List<String> found = new ArrayList<>();
        for (SqlAst.Statement q : queries) {
            String limited = limitProblem(q, byName, underExists);
            if (limited != null) {
                found.add(limited);
            }
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : clauses(s)) {
                    unorderedLists(e, s, byName, found);
                    unorderedWindows(e, s, byName, found);
                }
            }
        }
        return found;
    }

    /** IQ island, 2026-10-15 (windows): the window functions that read their window's rows in its order. */
    private static final Set<String> ORDERED_WINDOWS = Set.of("row_number", "ntile", "lag", "lead", "first_value",
            "last_value");

    /**
     * IQ island, 2026-10-15 (windows): each window call under {@code e} (in {@code s}) whose value is one plan's choice,
     * in a few words, into {@code out}: ROW_NUMBER, NTILE, LAG, LEAD, FIRST_VALUE and LAST_VALUE, and an aggregate over
     * a ROWS frame other than the whole partition, read rows in the window's order, so its PARTITION BY and ORDER BY
     * keys together must reach a unique column of each FROM item, as a row limit's ORDER BY must ({@link #limitProblem}:
     * a grouped query's keys, all of them). RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST and RANGE frames give ties one
     * value.
     */
    private static void unorderedWindows(SqlAst.Expr e, SqlAst.Select s, Map<String, SqlParity.Table> tables,
            List<String> out) {
        if (e instanceof SqlAst.Window w) {
            SqlAst.WindowSpec spec = w.spec();
            for (int guard = 0; spec.base() != null && guard <= s.windows().size(); guard++) {
                String base = java.util.Objects.requireNonNull(spec.base()).text().toLowerCase(Locale.ROOT);
                SqlAst.WindowSpec named = null;
                for (SqlAst.NamedWindow n : s.windows()) {
                    if (named == null && n.name().text().toLowerCase(Locale.ROOT).equals(base)) {
                        named = n.spec();
                    }
                }
                if (named == null) {
                    break;
                }
                spec = new SqlAst.WindowSpec(named.base(), named.partitionBy(), spec.orderBy().isEmpty()
                        ? named.orderBy() : spec.orderBy(), spec.frame() != null ? spec.frame() : named.frame(),
                        spec.pos());
            }
            String fn = w.call().name().get(w.call().name().size() - 1).text().toLowerCase(Locale.ROOT);
            SqlAst.Frame frame = spec.frame();
            boolean rows = frame != null && frame.unit() == SqlAst.FrameUnit.ROWS
                    && !(frame.start().kind() == SqlAst.BoundKind.UNBOUNDED_PRECEDING
                    && frame.end().kind() == SqlAst.BoundKind.UNBOUNDED_FOLLOWING);
            if (ORDERED_WINDOWS.contains(fn) || rows) {
                String why = windowOrder(spec, s, tables);
                if (why != null) {
                    out.add("the window at line " + w.pos().line() + ", column " + w.pos().column() + " " + why
                            + " (" + fn.toUpperCase(Locale.ROOT) + (rows && !ORDERED_WINDOWS.contains(fn)
                            ? " over a ROWS frame" : "") + " reads its rows in the window's order)");
                }
            }
        }
        for (SqlAst.Expr child : subexpressions(e)) {
            unorderedWindows(child, s, tables, out);
        }
    }

    /** Why {@code spec}'s keys order no row of {@code s} totally ({@link #unorderedWindows}); null when they do. */
    private static @com.legend.base.Nullable String windowOrder(SqlAst.WindowSpec spec, SqlAst.Select s,
            Map<String, SqlParity.Table> tables) {
        if (oneRow(s, tables)) {
            return null;
        }
        List<Source> sources = sources(s, tables);
        List<String> keys = new ArrayList<>();
        for (SqlAst.Expr k : spec.partitionBy()) {
            String r = resolve(k, s, sources, true);
            if (r != null) {
                keys.add(r);
            }
        }
        for (SqlAst.SortItem o : spec.orderBy()) {
            String r = resolve(o.expr(), s, sources, true);
            if (r != null) {
                keys.add(r);
            }
        }
        if (!s.groupBy().isEmpty()) {
            for (SqlAst.Expr g : s.groupBy()) {
                String k = resolve(g, s, sources, true);
                if (k == null || !keys.contains(k)) {
                    return "orders a grouped query by fewer than its GROUP BY keys";
                }
            }
            return null;
        }
        boolean[] reached = new boolean[sources.size()];
        for (int i = 0; i < sources.size(); i++) {
            Source src = sources.get(i);
            reached[i] = src.oneRow();
            for (String u : src.unique()) {
                reached[i] |= keys.contains(i + "." + u);
            }
        }
        List<SqlAst.Expr> equalities = new ArrayList<>();
        if (s.where() != null) {
            conjuncts(java.util.Objects.requireNonNull(s.where()), equalities);
        }
        for (SqlAst.FromItem f : s.from()) {
            innerOns(f, equalities);
        }
        for (boolean more = true; more; ) {
            more = false;
            for (SqlAst.Expr eq : equalities) {
                if (eq instanceof SqlAst.Binary b && b.op() == SqlAst.BinaryOp.EQ) {
                    more |= determines(resolve(b.left(), s, sources, false), resolve(b.right(), s, sources, false),
                            sources, reached) | determines(resolve(b.right(), s, sources, false),
                            resolve(b.left(), s, sources, false), sources, reached);
                }
            }
        }
        for (int i = 0; i < sources.size(); i++) {
            if (!reached[i]) {
                return "orders by no unique column of " + sources.get(i).label();
            }
        }
        return null;
    }

    /** Whether every item {@code s} shows is a constant (a literal, or a sign or arithmetic of literals): its rows
     *  all show the same values (IQ island, 2026-10-15, row limits without ORDER BY). */
    private static boolean constantsOnly(SqlAst.Select s) {
        for (SqlAst.SelectItem item : s.items()) {
            if (!(item instanceof SqlAst.SelectExpr x) || !literal(x.expr())) {
                return false;
            }
        }
        return !s.items().isEmpty();
    }

    private static boolean literal(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> true;
            case SqlAst.NumericLit n -> true;
            case SqlAst.StringLit t -> true;
            case SqlAst.Unary u -> u.op() != SqlAst.UnaryOp.NOT && literal(u.operand());
            case SqlAst.Binary b -> switch (b.op()) {
                case PLUS, MINUS, TIMES -> literal(b.left()) && literal(b.right());
                default -> false;
            };
            default -> false;
        };
    }

    /** A query's row limit that keeps rows of its plan's choosing, in a few words; null for none (IQ's reader puts a
     *  row limit on a SELECT only: a set operation's TOP is its first branch's). */
    private static @com.legend.base.Nullable String limitProblem(SqlAst.Statement q, Map<String, SqlParity.Table> tables,
            Set<SqlAst.Statement> underExists) {
        if (!(q instanceof SqlAst.Select s) || s.limit() == null && s.offset() == null || underExists.contains(q)
                || oneRow(q, tables)) {
            return null;
        }
        String at = "the row limit at line " + q.pos().line() + ", column " + q.pos().column();
        if (constantsOnly(s)) {
            // IQ island, 2026-10-15: every row it keeps shows the same values, whichever rows they are
            return null;
        }
        if (s.orderBy().isEmpty()) {
            return at + " has no ORDER BY";
        }
        List<Source> sources = sources(s, tables);
        List<String> keys = new ArrayList<>();
        for (SqlAst.SortItem i : s.orderBy()) {
            String k = resolve(i.expr(), s, sources, true);
            if (k != null) {
                keys.add(k);
            }
        }
        if (!s.groupBy().isEmpty()) {
            // IQ island, 2026-10-16 (checklist 2.16b): a ROLLUP's or a CUBE's keys are its arguments, and a group of a
            // NULL key ties with a subtotal unless the ORDER BY reads a GROUPING for each of them
            List<SqlAst.Expr> groupKeys = new ArrayList<>();
            int rolled = 0;
            for (SqlAst.Expr g : s.groupBy()) {
                if (g instanceof SqlAst.FunctionCall f && f.name().size() == 1 && Set.of("rollup", "cube").contains(
                        f.name().get(0).text().toLowerCase(Locale.ROOT))) {
                    groupKeys.addAll(f.args());
                    rolled += f.args().size();
                } else {
                    groupKeys.add(g);
                }
            }
            for (SqlAst.Expr g : groupKeys) {
                String k = resolve(g, s, sources, true);
                if (k == null || !keys.contains(k)) {
                    return at + " orders a grouped query by fewer than its GROUP BY keys";
                }
            }
            if (groupingsOrdered(s) < rolled) {
                return at + " orders a ROLLUP or a CUBE by fewer GROUPINGs than its keys (a NULL key's group ties with"
                        + " a subtotal)";
            }
            return null;
        }
        if (s.distinct()) {
            boolean every = true;
            for (int i = 0; i < s.items().size() && every; i++) {
                every = s.items().get(i) instanceof SqlAst.SelectExpr x && keys.contains(itemKey(x, i, s, sources));
            }
            if (every) {
                return null;
            }
        }
        // a FROM item is ordered where the ORDER BY reaches a unique column of it, or where an inner join's ON or the
        // WHERE equates a unique column of it with a column of an item that is ordered (one row of it for each row of
        // that one: SELECT ... FROM t JOIN (... GROUP BY t.id) a ON a.tid = t.id ORDER BY t.id)
        boolean[] reached = new boolean[sources.size()];
        for (int i = 0; i < sources.size(); i++) {
            Source src = sources.get(i);
            reached[i] = src.oneRow();
            for (String u : src.unique()) {
                reached[i] |= keys.contains(i + "." + u);
            }
        }
        List<SqlAst.Expr> equalities = new ArrayList<>();
        if (s.where() != null) {
            conjuncts(java.util.Objects.requireNonNull(s.where()), equalities);
        }
        for (SqlAst.FromItem f : s.from()) {
            innerOns(f, equalities);
        }
        for (boolean more = true; more; ) {
            more = false;
            for (SqlAst.Expr e : equalities) {
                if (e instanceof SqlAst.Binary b && b.op() == SqlAst.BinaryOp.EQ) {
                    more |= determines(resolve(b.left(), s, sources, false), resolve(b.right(), s, sources, false),
                            sources, reached) | determines(resolve(b.right(), s, sources, false),
                            resolve(b.left(), s, sources, false), sources, reached);
                }
            }
        }
        for (int i = 0; i < sources.size(); i++) {
            if (!reached[i]) {
                return at + " orders by no unique column of " + sources.get(i).label();
            }
        }
        return null;
    }

    /** How many of {@code s}'s ORDER BY keys are a GROUPING call (written there, or an item's by its alias or its
     *  position). */
    private static int groupingsOrdered(SqlAst.Select s) {
        int n = 0;
        for (SqlAst.SortItem o : s.orderBy()) {
            SqlAst.Expr e = o.expr();
            for (int i = 0; i < s.items().size(); i++) {
                if (s.items().get(i) instanceof SqlAst.SelectExpr x && (e instanceof SqlAst.IntegerLit p
                        && p.value().intValue() == i + 1 || x.alias() != null && e instanceof SqlAst.ColumnRef r
                        && r.parts().size() == 1 && r.parts().get(0).text().equalsIgnoreCase(
                        java.util.Objects.requireNonNull(x.alias()).text()))) {
                    e = x.expr();
                    break;
                }
            }
            if (e instanceof SqlAst.FunctionCall f && f.name().size() == 1
                    && f.name().get(0).text().equalsIgnoreCase("grouping")) {
                n++;
            }
        }
        return n;
    }

    /** Marks {@code unique}'s source ordered where it is a unique column of a source not yet ordered, and
     *  {@code other} a column of another, ordered source; whether it did. */
    private static boolean determines(@com.legend.base.Nullable String unique, @com.legend.base.Nullable String other,
            List<Source> sources, boolean[] reached) {
        if (unique == null || other == null || !unique.contains(".") || !other.contains(".")) {
            return false;
        }
        int i = Integer.parseInt(unique.substring(0, unique.indexOf('.')));
        int j = Integer.parseInt(other.substring(0, other.indexOf('.')));
        if (i == j || reached[i] || !reached[j] || !sources.get(i).unique().contains(unique.substring(
                unique.indexOf('.') + 1))) {
            return false;
        }
        reached[i] = true;
        return true;
    }

    /** The operands of the ANDs of {@code e}, into {@code out}. */
    private static void conjuncts(SqlAst.Expr e, List<SqlAst.Expr> out) {
        if (e instanceof SqlAst.Binary b && b.op() == SqlAst.BinaryOp.AND) {
            conjuncts(b.left(), out);
            conjuncts(b.right(), out);
        } else {
            out.add(e);
        }
    }

    /** The conjuncts of the ON conditions of the inner joins of {@code f}, into {@code out}. */
    private static void innerOns(SqlAst.FromItem f, List<SqlAst.Expr> out) {
        if (f instanceof SqlAst.Join j) {
            if (j.type() == SqlAst.JoinType.INNER && j.on() != null) {
                conjuncts(java.util.Objects.requireNonNull(j.on()), out);
            }
            innerOns(j.left(), out);
            innerOns(j.right(), out);
        }
    }

    /** A FROM item as the lint knows it: its label, the name that qualifies its columns (lower case), its columns
     *  (null when not known), those unique in its rows, and whether it has at most one row. */
    private record Source(String label, String qualifier, @com.legend.base.Nullable List<String> columns,
            Set<String> unique, boolean oneRow) {
    }

    private static List<Source> sources(SqlAst.Select s, Map<String, SqlParity.Table> tables) {
        List<Source> out = new ArrayList<>();
        for (SqlAst.FromItem f : s.from()) {
            for (SqlAst.FromItem leaf : leaves(f)) {
                out.add(source(leaf, tables));
            }
        }
        return out;
    }

    private static Source source(SqlAst.FromItem leaf, Map<String, SqlParity.Table> tables) {
        switch (leaf) {
            case SqlAst.TableName t -> {
                String name = t.name().get(t.name().size() - 1).text().toLowerCase(Locale.ROOT);
                String q = t.alias() != null ? java.util.Objects.requireNonNull(t.alias()).text()
                        .toLowerCase(Locale.ROOT) : name;
                if (name.equals("dummy")) {
                    return new Source("DUMMY", q, List.of("dummy_col"), Set.of("dummy_col"), true);
                }
                return table(name, q, tables);
            }
            case SqlAst.TableFunction f -> {
                String fn = f.name().get(f.name().size() - 1).text().toLowerCase(Locale.ROOT);
                String q = f.alias() != null ? java.util.Objects.requireNonNull(f.alias()).text()
                        .toLowerCase(Locale.ROOT) : fn;
                if (fn.equals("sa_rowgenerator")) {
                    return new Source("sa_rowgenerator", q, List.of("row_num"), Set.of("row_num"), false);
                }
                if (fn.equals("tb") && f.args().size() == 1 && f.args().get(0).value() instanceof SqlAst.StringLit p) {
                    String path = p.value();
                    return table(path.substring(path.lastIndexOf('.') + 1).toLowerCase(Locale.ROOT), q, tables);
                }
                return new Source(fn, q, null, Set.of(), false);
            }
            case SqlAst.SubqueryFrom d -> {
                String q = d.alias() != null ? java.util.Objects.requireNonNull(d.alias()).text()
                        .toLowerCase(Locale.ROOT) : "";
                List<Output> outputs = outputs(d.query(), tables);
                if (outputs == null) {
                    return new Source("the derived table " + q, q, null, Set.of(), oneRow(d.query(), tables));
                }
                List<String> names = new ArrayList<>();
                Set<String> unique = new java.util.HashSet<>();
                for (int i = 0; i < outputs.size(); i++) {
                    String name = i < d.columnAliases().size() ? d.columnAliases().get(i).text()
                            .toLowerCase(Locale.ROOT) : outputs.get(i).name();
                    names.add(name == null ? "" : name);
                    if (name != null && outputs.get(i).unique()) {
                        unique.add(name);
                    }
                }
                return new Source("the derived table " + q, q, names, unique, oneRow(d.query(), tables));
            }
            case SqlAst.Join j -> throw new IllegalStateException("a join is no FROM leaf");
        }
    }

    /** A schema table's source: its columns, and {@link #KEY} unique where its rows hold it once each. */
    private static Source table(String name, String qualifier, Map<String, SqlParity.Table> tables) {
        SqlParity.Table t = tables.get(name);
        if (t == null) {
            return new Source(name, qualifier, null, Set.of(), false);
        }
        List<String> columns = t.columns().stream().map(c -> c.name().toLowerCase(Locale.ROOT)).toList();
        int key = columns.indexOf(KEY);
        if (key < 0) {
            return new Source(name, qualifier, columns, Set.of(), false);
        }
        Set<Object> seen = new java.util.HashSet<>();
        for (List<Object> row : t.rows()) {
            Object v = row.get(key);
            if (v == null || !seen.add(v)) {
                throw new IllegalStateException("the schema's table " + name + " holds its " + KEY + " " + v
                        + " twice, or NULL: the determinism lint's key is no key");
            }
        }
        return new Source(name, qualifier, columns, Set.of(KEY), false);
    }

    /** A query's output column: its name (lower case; null for none) and whether no two of its rows have one value. */
    private record Output(@com.legend.base.Nullable String name, boolean unique) {
    }

    /** {@code q}'s output columns, in order; null where a {@code *} reads a source whose columns are not known. */
    private static @com.legend.base.Nullable List<Output> outputs(SqlAst.Statement q,
            Map<String, SqlParity.Table> tables) {
        if (q instanceof SqlAst.SetOperation o) {
            List<Output> left = outputs(first(o), tables);
            if (left == null) {
                return null;
            }
            boolean single = left.size() == 1 && !o.all();
            return left.stream().map(x -> new Output(x.name(), single)).toList();
        }
        SqlAst.Select s = (SqlAst.Select) q;
        List<Source> sources = sources(s, tables);
        boolean one = oneRow(s, tables);
        List<String> group = new ArrayList<>();
        for (SqlAst.Expr g : s.groupBy()) {
            group.add(resolve(g, s, sources, true));
        }
        List<Output> out = new ArrayList<>();
        int multi = 0;
        for (Source src : sources) {
            multi += src.oneRow() ? 0 : 1;
        }
        for (int i = 0; i < s.items().size(); i++) {
            SqlAst.SelectItem item = s.items().get(i);
            if (item instanceof SqlAst.AllColumns a) {
                for (int k = 0; k < sources.size(); k++) {
                    Source src = sources.get(k);
                    if (a.qualifier() != null && !src.qualifier().equals(java.util.Objects.requireNonNull(
                            a.qualifier()).text().toLowerCase(Locale.ROOT))) {
                        continue;
                    }
                    if (src.columns() == null) {
                        return null;
                    }
                    for (String col : java.util.Objects.requireNonNull(src.columns())) {
                        out.add(new Output(col, one || s.groupBy().isEmpty() && !s.distinct() && multi <= 1
                                && src.unique().contains(col)));
                    }
                }
                continue;
            }
            SqlAst.SelectExpr x = (SqlAst.SelectExpr) item;
            String name = x.alias() != null ? java.util.Objects.requireNonNull(x.alias()).text()
                    .toLowerCase(Locale.ROOT) : x.expr() instanceof SqlAst.ColumnRef r
                    ? r.parts().get(r.parts().size() - 1).text().toLowerCase(Locale.ROOT) : null;
            String k = resolve(x.expr(), s, sources, false);
            boolean unique;
            if (one) {
                unique = true;
            } else if (!s.groupBy().isEmpty()) {
                unique = group.size() == 1 && k != null && k.equals(group.get(0));
            } else if (s.distinct()) {
                unique = s.items().size() == 1;
            } else {
                int dot = k == null ? -1 : k.indexOf('.');
                unique = dot > 0 && multi <= 1 && sources.get(Integer.parseInt(k.substring(0, dot))).unique()
                        .contains(k.substring(dot + 1)) && !sources.get(Integer.parseInt(k.substring(0, dot)))
                        .oneRow();
            }
            out.add(new Output(name, unique));
        }
        return out;
    }

    /** Whether {@code q} has at most one row: no FROM, only one-row FROM items (DUMMY), or an aggregate without
     *  GROUP BY. */
    private static boolean oneRow(SqlAst.Statement q, Map<String, SqlParity.Table> tables) {
        if (!(q instanceof SqlAst.Select s)) {
            return false;
        }
        if (s.from().isEmpty()) {
            return true;
        }
        if (s.groupBy().isEmpty()) {
            for (SqlAst.SelectItem item : s.items()) {
                if (item instanceof SqlAst.SelectExpr x && aggregated(x.expr())) {
                    return true;
                }
            }
        }
        return sources(s, tables).stream().allMatch(Source::oneRow);
    }

    private static boolean aggregated(SqlAst.Expr e) {
        if (e instanceof SqlAst.FunctionCall f && AGGREGATES.contains(f.name().get(f.name().size() - 1).text()
                .toLowerCase(Locale.ROOT))) {
            return true;
        }
        return subexpressions(e).stream().anyMatch(IqParity::aggregated);
    }

    /** What an ORDER BY or GROUP BY key (or a select item, {@code aliases} false) reads in {@code s}: a source's
     *  column, {@code <source index>.<column>}; a select item that is no column, {@code item <index>}; null otherwise.
     *  A position and a name of a select item's alias read that item, an alias before a column (as IQ reads them). */
    private static @com.legend.base.Nullable String resolve(SqlAst.Expr e, SqlAst.Select s, List<Source> sources,
            boolean aliases) {
        if (aliases && e instanceof SqlAst.IntegerLit n) {
            int i = n.value().intValue() - 1;
            return i >= 0 && i < s.items().size() && s.items().get(i) instanceof SqlAst.SelectExpr x
                    ? itemKey(x, i, s, sources) : null;
        }
        if (!(e instanceof SqlAst.ColumnRef r)) {
            return null;
        }
        List<String> parts = r.parts().stream().map(p -> p.text().toLowerCase(Locale.ROOT)).toList();
        String column = parts.get(parts.size() - 1);
        if (aliases && parts.size() == 1) {
            for (int i = 0; i < s.items().size(); i++) {
                if (s.items().get(i) instanceof SqlAst.SelectExpr x && x.alias() != null
                        && java.util.Objects.requireNonNull(x.alias()).text().toLowerCase(Locale.ROOT).equals(column)) {
                    return itemKey(x, i, s, sources);
                }
            }
        }
        int found = -1;
        for (int i = 0; i < sources.size(); i++) {
            Source src = sources.get(i);
            boolean here = parts.size() == 1 ? src.columns() == null ? sources.size() == 1
                    : java.util.Objects.requireNonNull(src.columns()).contains(column)
                    : src.qualifier().equals(parts.get(parts.size() - 2));
            if (here) {
                if (found >= 0) {
                    return null;
                }
                found = i;
            }
        }
        return found < 0 ? null : found + "." + column;
    }

    /** The key a select item reads ({@link #resolve}): its column's, else the item's own. */
    private static String itemKey(SqlAst.SelectExpr x, int i, SqlAst.Select s, List<Source> sources) {
        String k = resolve(x.expr(), s, sources, false);
        return k != null ? k : "item " + i;
    }

    /** A query's clauses' expressions: its items', WHERE's, GROUP BY's, HAVING's, ORDER BY's and its joins' ON
     *  conditions (not its subqueries'). */
    private static List<SqlAst.Expr> clauses(SqlAst.Select s) {
        List<SqlAst.Expr> all = new ArrayList<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                all.add(x.expr());
            }
        }
        if (s.where() != null) {
            all.add(java.util.Objects.requireNonNull(s.where()));
        }
        all.addAll(s.groupBy());
        if (s.having() != null) {
            all.add(java.util.Objects.requireNonNull(s.having()));
        }
        s.orderBy().forEach(o -> all.add(o.expr()));
        for (SqlAst.FromItem f : s.from()) {
            List<SqlAst.Expr> on = new ArrayList<>();
            onConditions(f, on);
            all.addAll(on);
        }
        return all;
    }

    /** The queries an EXISTS under {@code e} reads, and a set operation's branches there, into {@code out}. */
    private static void existsQueries(SqlAst.Expr e, Set<SqlAst.Statement> out) {
        if (e instanceof SqlAst.Exists x) {
            Deque<SqlAst.Statement> todo = new ArrayDeque<>(List.of(x.query()));
            while (!todo.isEmpty()) {
                SqlAst.Statement q = todo.pop();
                out.add(q);
                if (q instanceof SqlAst.SetOperation o) {
                    todo.push(o.left());
                    todo.push(o.right());
                }
            }
        }
        for (SqlAst.Expr child : subexpressions(e)) {
            existsQueries(child, out);
        }
    }

    /** Each LIST under {@code e} (in {@code s}, not in its subqueries) without its own ORDER BY that lists no GROUP BY
     *  key of {@code s}, in a few words, into {@code out}. */
    private static void unorderedLists(SqlAst.Expr e, SqlAst.Select s, Map<String, SqlParity.Table> tables,
            List<String> out) {
        if (e instanceof SqlAst.FunctionCall f && f.name().get(f.name().size() - 1).text().equalsIgnoreCase("list")
                && f.orderBy().isEmpty()) {
            List<Source> sources = sources(s, tables);
            String listed = f.args().isEmpty() ? null : resolve(f.args().get(0), s, sources, false);
            // a constant's values are one value too
            boolean key = !f.args().isEmpty() && (f.args().get(0) instanceof SqlAst.IntegerLit
                    || f.args().get(0) instanceof SqlAst.NumericLit || f.args().get(0) instanceof SqlAst.StringLit
                    || f.args().get(0) instanceof SqlAst.NullLit);
            for (SqlAst.Expr g : s.groupBy()) {
                String grouped = resolve(g, s, sources, true);
                key |= listed != null && listed.equals(grouped);
                // IQ island, 2026-10-15: grouped by a unique column of its one source, each group is one row
                if (sources.size() == 1 && grouped != null) {
                    for (String u : sources.get(0).unique()) {
                        key |= grouped.equals("0." + u);
                    }
                }
            }
            if (!key) {
                out.add("the LIST at line " + f.pos().line() + ", column " + f.pos().column() + " has no ORDER BY");
            }
        }
        for (SqlAst.Expr child : subexpressions(e)) {
            unorderedLists(child, s, tables, out);
        }
    }

    /** A FROM item's tables, functions and derived tables, through its joins. */
    private static List<SqlAst.FromItem> leaves(SqlAst.FromItem item) {
        if (item instanceof SqlAst.Join j) {
            List<SqlAst.FromItem> out = new ArrayList<>(leaves(j.left()));
            out.addAll(leaves(j.right()));
            return out;
        }
        return List.of(item);
    }

    /** The first SELECT of a query. */
    private static SqlAst.Select first(SqlAst.Statement q) {
        return switch (q) {
            case SqlAst.Select s -> s;
            case SqlAst.SetOperation o -> first(o.left());
            case SqlAst.With w -> throw new IllegalStateException("IQ's reader makes no WITH");
            case SqlAst.Write w -> throw new IllegalStateException("IQ's reader makes no write");
        };
    }

    /** {@code sql}'s Pure under {@code dialect}, the schema's tables its sources; null when the dialect refuses it,
     *  the refusal's kind and message when it rejects it. */
    private static @com.legend.base.Nullable String pureOf(String sql, List<SqlParity.Table> tables,
            IqDialect dialect, List<Param> params) {
        try {
            return com.legend.protocol.PureComposer.valueSpecification(com.legend.protocol.sql.SqlToPure.translate(sql,
                    sources(tables, params), dialect), com.legend.protocol.PureComposer.Style.STANDARD);
        } catch (com.legend.protocol.sql.SqlTranslationException e) {
            return e.kind() == com.legend.protocol.sql.SqlTranslationException.Kind.UNSUPPORTED ? null
                    : e.kind() + ": " + e.getMessage();
        }
    }

    /**
     * IQ island, 2026-10-14 (the options framework): the query a run is, as the lanes run it -- its island; for a case
     * with settings, which reach no island yet (their Pure plumbing, tagged values of the IQ profile on a Database and a
     * function, lands separately), the translation of its SQL under them ({@code IqSettings.resolve}) done here as the
     * island's seam does it (SqlIslands: the schema's tables its sources, each column's store type, the case's
     * variables), printed, its variables let before it. A translation's refusal or error is thrown as the
     * {@code SqlTranslationException} it is.
     */
    public static String query(Run run, List<SqlParity.Table> schema, Set<String> tables) {
        if (run.c().settings() == null) {
            return island(run, tables);
        }
        com.legend.protocol.sql.iq.IqSettings settings = com.legend.protocol.sql.iq.IqSettings.resolve(run.profile(),
                settings(java.util.Objects.requireNonNull(run.c().settings())));
        List<Param> params = params(run.c().params());
        String pure = parsedTds(com.legend.protocol.PureComposer.valueSpecification(
                com.legend.protocol.sql.SqlToPure.translate(rewrite(run.c().sql(), tables, settings),
                        sources(schema, params), IqDialect.of(settings)), com.legend.protocol.PureComposer.Style.STANDARD));
        if (params.isEmpty()) {
            return pure;
        }
        StringBuilder lets = new StringBuilder("|");
        params.forEach(p -> lets.append("let ").append(p.name()).append(" = ").append(p.pure()).append("; "));
        return lets.append(pure).append(';').toString();
    }

    /** {@code pure} with each TDS literal the printer writes {@code #TDS{...}#} written {@code #TDS...#}, the spelling
     *  the parser reads back as the same rows (it reads the braced one's braces as rows: a FROM-less SELECT's one row
     *  read back as three). */
    private static String parsedTds(String pure) {
        StringBuilder out = new StringBuilder();
        int at = 0;
        for (int i = pure.indexOf("#TDS{"); i >= 0; i = pure.indexOf("#TDS{", at)) {
            int end = pure.indexOf("}#", i);
            out.append(pure, at, i).append("#TDS").append(pure, i + "#TDS{".length(), end).append('#');
            at = end + 2;
        }
        return out.append(pure.substring(at)).toString();
    }

    /**
     * IQ island, 2026-10-14 (the options framework): a case's {@code settings}, as {@code IqSettings.resolve} takes
     * them -- each {@code database.<name>=<value>} (a Database element's tagged value: a database property, a PUBLIC
     * option, CommProtocol) or {@code query.<name>=<value>} (the enclosing function's: the connection's temporary
     * value), IQ's names, a value as text (quoted, {@code ''} a quote in it, or bare up to the next comma), separated by
     * commas.
     */
    public static List<com.legend.protocol.sql.iq.IqSettings.Setting> settings(String text) {
        List<com.legend.protocol.sql.iq.IqSettings.Setting> out = new ArrayList<>();
        int i = 0;
        int n = text.length();
        while (true) {
            while (i < n && text.charAt(i) == ' ') {
                i++;
            }
            int dot = text.indexOf('.', i);
            int eq = text.indexOf('=', i);
            if (dot < 0 || eq < dot) {
                throw new IllegalStateException("a setting is database.<name>=<value> or query.<name>=<value>: "
                        + text.substring(i));
            }
            String level = text.substring(i, dot).strip().toLowerCase(Locale.ROOT);
            String name = text.substring(dot + 1, eq).strip();
            if (!level.equals("database") && !level.equals("query") || name.isEmpty()) {
                throw new IllegalStateException("a setting is database.<name>=<value> or query.<name>=<value>: "
                        + text.substring(i));
            }
            i = eq + 1;
            while (i < n && text.charAt(i) == ' ') {
                i++;
            }
            StringBuilder value = new StringBuilder();
            if (i < n && text.charAt(i) == '\'') {
                i++;
                while (true) {
                    if (i >= n) {
                        throw new IllegalStateException("a setting's quoted value is not closed: " + text);
                    }
                    if (text.charAt(i) == '\'' && i + 1 < n && text.charAt(i + 1) == '\'') {
                        value.append('\'');
                        i += 2;
                    } else if (text.charAt(i) == '\'') {
                        i++;
                        break;
                    } else {
                        value.append(text.charAt(i++));
                    }
                }
            } else {
                while (i < n && text.charAt(i) != ',') {
                    value.append(text.charAt(i++));
                }
                if (value.toString().strip().isEmpty()) {
                    throw new IllegalStateException("a setting has a value: " + text);
                }
            }
            out.add(new com.legend.protocol.sql.iq.IqSettings.Setting(level.equals("database")
                    ? com.legend.protocol.sql.iq.IqSettings.Level.DATABASE
                    : com.legend.protocol.sql.iq.IqSettings.Level.QUERY, name, value.toString().strip()));
            while (i < n && text.charAt(i) == ' ') {
                i++;
            }
            if (i >= n) {
                return out;
            }
            if (text.charAt(i) != ',') {
                throw new IllegalStateException("',' expected between settings: " + text.substring(i));
            }
            i++;
        }
    }

    /** The translator's sources for a run: the schema's tables (in the model's default schema), each column of its
     *  Pure type and store type, and the case's variables. */
    private static com.legend.protocol.sql.SqlToPure.Sources sources(List<SqlParity.Table> tables,
            List<Param> params) {
        // IQ island, 2026-10-14 (M1e): the model's tables (in its default schema) and the case's variables
        return new com.legend.protocol.sql.SqlToPure.Sources() {
            @Override
            public void columns(com.legend.protocol.spec.ValueSpecification source,
                    com.legend.protocol.sql.SqlToPure.Column column) {
                String printed = com.legend.protocol.PureComposer.valueSpecification(source,
                        com.legend.protocol.PureComposer.Style.STANDARD);
                String table = printed.substring(printed.lastIndexOf('.') + 1, printed.lastIndexOf("}#"));
                for (SqlParity.Table t : tables) {
                    if (t.name().equals(table)) {
                        t.columns().forEach(col -> column.column(col.name(), pureType(col.type()), true,
                                relationalType(col.type())));
                    }
                }
            }

            @Override
            public void tables(com.legend.protocol.sql.SqlToPure.Table table) {
                tables.forEach(t -> table.table(DATABASE, "default", t.name()));
            }

            @Override
            public boolean variable(String name, com.legend.protocol.sql.SqlToPure.Scoped type) {
                for (Param p : params) {
                    if (p.name().equals(name)) {
                        type.variable(p.type(), p.isNull(), false);
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void variables(java.util.function.Consumer<String> names) {
                params.forEach(p -> names.accept(p.name()));
            }
        };
    }

    /** The Pure type of a schema column of IQ type {@code iqType}, as the typer reports it to the island. */
    private static String pureType(String iqType) {
        String t = relationalType(iqType);
        if (t.startsWith("CHAR") || t.startsWith("VARCHAR")) {
            return "String";
        }
        if (t.startsWith("NUMERIC") || t.startsWith("DECIMAL")) {
            return "Decimal";
        }
        return switch (t) {
            case "BIT" -> "Boolean";
            case "DOUBLE", "FLOAT", "REAL" -> "Float";
            case "DATE" -> "StrictDate";
            case "TIMESTAMP" -> "DateTime";
            default -> "Integer";
        };
    }

    // ------------------------------------------------------------------ one run's outcome

    /** One run in one lane: its island run through legend-lite, judged against its judge by this harness's exact
     *  comparison ({@link #compare}). An error is a both-error only when it is the island's own (its syntax error, its
     *  refusal, IQ's error it raises) or the database's; any other exception in legend-lite is an ERROR. */
    public static SqlParity.Outcome outcome(Run run, IqAnswers.@com.legend.base.Nullable Snap snap,
            List<SqlParity.Table> schema, Set<String> tables, SqlParity.Legend legend) {
        SqlParity.Outcome o = judged(run, snap, schema, tables, legend);
        // IQ island, landing 4 (WP1): an iq17-* run's judge is the live IQ, and its report says so
        return liveIq(run.profile()) ? new SqlParity.Outcome(o.status(), o.detail().replace("SQL Anywhere",
                "the live IQ")) : o;
    }

    private static SqlParity.Outcome judged(Run run, IqAnswers.@com.legend.base.Nullable Snap snap,
            List<SqlParity.Table> schema, Set<String> tables, SqlParity.Legend legend) {
        if (run.c().skip() != null) {
            return new SqlParity.Outcome(SqlParity.Status.SKIP, java.util.Objects.requireNonNull(run.c().skip()));
        }
        if (snap == null) {
            return new SqlParity.Outcome(SqlParity.Status.ERROR, "no snapped answer (UNSNAPPED)");
        }
        if (snap.notJudged() != null) {
            // IQ island, landing 4 (WP1): a run the live IQ cannot judge here (snap.sh --oracle iq says why)
            return new SqlParity.Outcome(SqlParity.Status.SKIP, "not judged by the live IQ: " + snap.notJudged());
        }
        // IQ island, 2026-10-15 (windows): a decision written in full judges by its own columns (IqAnswers.judged)
        snap = IqAnswers.judged(run, snap);
        SqlParity.Answer expected = IqAnswers.expected(run, snap);
        com.legend.exec.ExecutionResult result;
        try {
            result = legend.run(query(run, schema, tables));
        } catch (com.legend.protocol.sql.SqlTranslationException e) {
            // IQ island, 2026-10-14 (the options framework): a case with settings, translated here: a refusal by
            // name, or the island's own error (its syntax error, IQ's error it raises)
            if (e.kind() == com.legend.protocol.sql.SqlTranslationException.Kind.UNSUPPORTED) {
                return expected instanceof SqlParity.Rejected r ? bothError(e, r)
                        : new SqlParity.Outcome(SqlParity.Status.REFUSED, oneLine(e));
            }
            return expected instanceof SqlParity.Rejected r ? bothError(e, r)
                    : new SqlParity.Outcome(SqlParity.Status.ERROR, oneLine(e));
        } catch (com.legend.error.NotImplementedException e) {
            return expected instanceof SqlParity.Rejected r ? bothError(e, r)
                    : new SqlParity.Outcome(SqlParity.Status.REFUSED, oneLine(e));
        } catch (Exception | StackOverflowError e) {
            if (!liteError(e)) {
                return new SqlParity.Outcome(SqlParity.Status.ERROR, "a Java exception in legend-lite: " + oneLine(e));
            }
            // IQ island, 2026-10-14 (review 7's m3): an error the island raises in the database by name is a refusal
            // whose test the database computes -- REFUSED where SQL Anywhere answers, never a PASS; any other error
            // of the database's where SQL Anywhere answers stays an ERROR
            return expected instanceof SqlParity.Rejected r ? bothError(e, r) : refusedAtRunTime(e)
                    ? new SqlParity.Outcome(SqlParity.Status.REFUSED, oneLine(e))
                    : new SqlParity.Outcome(SqlParity.Status.ERROR, oneLine(e));
        }
        SqlParity.Grid actual = SqlParity.grid(result);
        List<String> types = result.columns().stream().map(c -> c.pureType().typeName()).toList();
        return switch (expected) {
            case SqlParity.Rejected r -> new SqlParity.Outcome(SqlParity.Status.BUG, "SQL Anywhere rejects it ("
                    + r.message() + ") and legend-lite answered " + actual.rows().size() + " rows");
            case SqlParity.Rows rows -> {
                List<String> diffs = compare(run, rows.grid(), snap, actual, types);
                yield diffs.isEmpty() ? new SqlParity.Outcome(SqlParity.Status.PASS, "")
                        : new SqlParity.Outcome(SqlParity.Status.FAIL, String.join("; ", diffs));
            }
        };
    }

    private static final Pattern LITE_CODE = Pattern.compile("\\(IQ: SQLCODE (-?\\d+), SQLSTATE ([0-9A-Z]{5})\\)");
    private static final Pattern SA_CODE = Pattern.compile("^SQLCODE (-?\\d+), SQLSTATE ([0-9A-Z]{5})");

    /** Both reject the run: a BOTH_ERROR, unless legend-lite's error names IQ's SQLCODE ({@code (IQ: SQLCODE n,
     *  SQLSTATE s)}, as the island's own errors do) and SQL Anywhere raised another one -- then lite's claim is
     *  wrong, a FAIL. An error without a code (a refusal, a database's run-time error) is judged by being one. */
    public static SqlParity.Outcome bothError(Throwable e, SqlParity.Rejected sa) {
        Matcher theirs = SA_CODE.matcher(sa.message());
        String code = theirs.find() ? theirs.group(1) : null;
        for (Throwable t = e; t != null && code != null; t = t.getCause() == t ? null : t.getCause()) {
            Matcher ours = LITE_CODE.matcher(String.valueOf(t.getMessage()));
            if (ours.find()) {
                if (!ours.group(1).equals(code)) {
                    return new SqlParity.Outcome(SqlParity.Status.FAIL, "both reject it, but legend-lite names IQ's"
                            + " SQLCODE " + ours.group(1) + " where SQL Anywhere raised " + code + " (" + sa.message()
                            + "): " + oneLine(e));
                }
                break;
            }
        }
        return new SqlParity.Outcome(SqlParity.Status.BOTH_ERROR, oneLine(e));
    }

    /** Whether {@code e} is the database's error that the island raises by name ({@link IqDialect#REFUSED_AT_RUN_TIME},
     *  a {@code DataError}): its refusal of a value the database tests. */
    static boolean refusedAtRunTime(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof com.legend.error.DataError && t.getMessage() != null
                    && t.getMessage().contains(IqDialect.REFUSED_AT_RUN_TIME)) {
                return true;
            }
        }
        return false;
    }

    /** Whether {@code e} is an error legend-lite reports on purpose: the island's syntax error or IQ's error it raises
     *  in translation (its message begins with the island's opening), or the database's (a {@code DataError}). */
    private static boolean liteError(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof com.legend.error.IslandSyntaxException || t instanceof com.legend.error.DataError) {
                return true;
            }
            if (t.getClass().getSimpleName().equals("TypeInferenceException") && t.getMessage() != null
                    && t.getMessage().startsWith("#IQ")) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }

    /** The judge's comparison: column names exactly as SQL Anywhere reports them (the island names a column as IQ
     *  does, so case counts), each column's type family against SQL Anywhere's type, a NUMERIC column's scale against
     *  each of lite's decimal cells ({@link #scale}), each cell exactly by SQL
     *  Anywhere's type ({@link #cellEquals}); rows in order when the query orders them (the order among rows tied on
     *  keys that are all output columns is the database's), else as a set. The run's ORDER BY is read by IQ's
     *  reader. Public for its test (IQ island, 2026-10-14, review 4's B2). */
    public static List<String> compare(Run run, SqlParity.Grid expected, IqAnswers.Snap snap, SqlParity.Grid actual,
            List<String> liteTypes) {
        List<String> diffs = new ArrayList<>();
        if (expected.columns().size() != actual.columns().size()) {
            diffs.add("Column count mismatch: expected " + expected.columns().size() + " but got "
                    + actual.columns().size());
            return diffs;
        }
        for (int i = 0; i < expected.columns().size(); i++) {
            if (!expected.columns().get(i).equals(actual.columns().get(i))) {
                diffs.add("Column name mismatch at index " + i + ": expected '" + expected.columns().get(i)
                        + "' but got '" + actual.columns().get(i) + "'");
            }
            String family = family(snap.types().get(i), i < snap.scales().size() ? snap.scales().get(i) : 0,
                    liteTypes.get(i));
            if (family != null) {
                diffs.add("Column '" + expected.columns().get(i) + "': " + family);
            }
            String scale = scale(snap.types().get(i), i < snap.scales().size() ? snap.scales().get(i) : 0, actual, i);
            if (scale != null) {
                diffs.add("Column '" + expected.columns().get(i) + "': " + scale);
            }
        }
        if (!diffs.isEmpty()) {
            return diffs;
        }
        if (run.c().member() != null) {
            // IQ island, 2026-10-15: a string shown is any value of its group of equal strings
            expected = collated(expected, snap.types(), run.settings());
            actual = collated(actual, snap.types(), run.settings());
        }
        List<SqlAst.SortItem> orderBy;
        SqlAst.Statement st;
        try {
            com.legend.protocol.sql.iq.IqSetPrefix.Prefix select = run.select(run.c().sql());
            st = IqParser.parse(select.select(), select.settings());
            // IQ island, 2026-10-14 (M1b): a set operation's ORDER BY is its own
            orderBy = st instanceof SqlAst.Select s ? s.orderBy() : st instanceof SqlAst.SetOperation o ? o.orderBy()
                    : List.of();
        } catch (SqlSyntaxException | com.legend.protocol.sql.SqlTranslationException e) {
            boolean ordered = run.c().sql().toUpperCase(Locale.ROOT).contains("ORDER BY");
            return rows(ordered, null, expected, actual, snap.types(), run.settings());
        }
        if (orderBy.isEmpty()) {
            return rows(false, null, expected, actual, snap.types(), run.settings());
        }
        if (rounded(st)) {
            // IQ island, 2026-10-14 (review 4's B2): the tie rule reads the shown keys; a value of a quotient or an
            // AVG is shown rounded where SQL Anywhere sorts by the unrounded one (.9990 and .9990 sort as 997/998 <
            // 998/999), so where any item of the statement is (or reads) one, the rows are compared in their order
            return rows(true, null, expected, actual, snap.types(), run.settings());
        }
        List<Integer> keys = new ArrayList<>();
        for (SqlAst.SortItem item : orderBy) {
            int found = -1;
            if (item.expr() instanceof SqlAst.IntegerLit n && n.value().bitLength() > 31) {
                // an integer past an INT's range is a constant, which orders nothing (measured; 2026-10-16)
                continue;
            }
            if (item.expr() instanceof SqlAst.IntegerLit n && n.value().signum() > 0
                    && n.value().compareTo(java.math.BigInteger.valueOf(expected.columns().size())) <= 0) {
                found = n.value().intValue() - 1;
            } else if (item.expr() instanceof SqlAst.ColumnRef ref && ref.parts().size() == 1) {
                for (int i = 0; i < expected.columns().size(); i++) {
                    if (expected.columns().get(i).equalsIgnoreCase(ref.parts().get(0).text())) {
                        found = found == -1 ? i : -2;
                    }
                }
            }
            if (found < 0) {
                return rows(true, null, expected, actual, snap.types(), run.settings());
            }
            keys.add(found);
        }
        return rows(true, keys, expected, actual, snap.types(), run.settings());
    }

    /** Whether a select item of any query of {@code statement} is, or reads, a quotient ({@code /}) or an AVG or a
     *  MEDIAN (a quotient of its values): values SQL Anywhere shows rounded (IQ island, 2026-10-14, review 4's B2). */
    private static boolean rounded(SqlAst.Statement statement) {
        for (SqlAst.Statement q : IqDialect.queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.SelectItem item : s.items()) {
                    if (item instanceof SqlAst.SelectExpr x && roundedValue(x.expr())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean roundedValue(SqlAst.Expr e) {
        boolean here = e instanceof SqlAst.Binary b && b.op() == SqlAst.BinaryOp.DIVIDE
                || e instanceof SqlAst.FunctionCall f && Set.of("avg", "median").contains(
                f.name().get(f.name().size() - 1).text().toLowerCase(Locale.ROOT));
        return here || subexpressions(e).stream().anyMatch(IqParity::roundedValue);
    }

    /** The rows: in order ({@code ordered}, no {@code keys}), as a set, or as a set whose {@code keys} columns are in
     *  order -- a string key by IQ's collation under {@code profile} (IQ island, 2026-10-14: rows whose string keys
     *  differ only in trailing spaces, or in a {@code sqla17} profile in the ASCII letters' case, tie, and their order
     *  is the plan's, measured on SQL Anywhere 17). */
    private static List<String> rows(boolean ordered, @com.legend.base.Nullable List<Integer> keys,
            SqlParity.Grid expected, SqlParity.Grid actual, List<String> types,
            com.legend.protocol.sql.iq.IqSettings profile) {
        List<String> diffs = new ArrayList<>();
        if (expected.rows().size() != actual.rows().size()) {
            diffs.add("Row count mismatch: expected " + expected.rows().size() + " but got " + actual.rows().size());
            return diffs;
        }
        List<List<Object>> e = ordered && keys == null ? expected.rows() : sorted(expected.rows());
        List<List<Object>> a = ordered && keys == null ? actual.rows() : sorted(actual.rows());
        for (int r = 0; r < e.size() && diffs.size() < 10; r++) {
            for (int c = 0; c < types.size() && diffs.size() < 10; c++) {
                if (!cellEquals(e.get(r).get(c), a.get(r).get(c), types.get(c))) {
                    diffs.add("Row " + r + ", column '" + expected.columns().get(c) + "': expected '"
                            + e.get(r).get(c) + "' but got '" + a.get(r).get(c) + "'");
                }
            }
        }
        if (keys != null && diffs.isEmpty()) {
            for (int r = 0; r < expected.rows().size() && diffs.isEmpty(); r++) {
                for (int k : keys) {
                    Object ev = collationKey(expected.rows().get(r).get(k), types.get(k), profile);
                    Object av = collationKey(actual.rows().get(r).get(k), types.get(k), profile);
                    if (!cellEquals(ev, av, types.get(k))) {
                        diffs.add("Row " + r + ", sort key '" + expected.columns().get(k) + "': expected '" + ev
                                + "' but got '" + av + "'");
                    }
                }
            }
        }
        return diffs;
    }

    /** A sort key's cell as IQ orders it: a string of SQL Anywhere's type {@code saType} by its collation under
     *  {@code profile} -- its ASCII letters in lower case in a case-insensitive database, its trailing spaces dropped in
     *  a blank-padding one -- any other cell itself. */
    static @com.legend.base.Nullable Object collationKey(@com.legend.base.Nullable Object cell, String saType,
            com.legend.protocol.sql.iq.IqSettings profile) {
        if (!(cell instanceof String s) || !(saType.equals("char") || saType.equals("varchar")
                || saType.equals("long varchar"))) {
            return cell;
        }
        StringBuilder b = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            b.append(!profile.caseSensitive() && c >= 'A' && c <= 'Z' ? (char) (c + ('a' - 'A')) : c);
        }
        int end = b.length();
        while (profile.blankPadding() && end > 0 && b.charAt(end - 1) == ' ') {
            end--;
        }
        return b.substring(0, end);
    }

    /** {@code grid} with each string cell its collation key under {@code profile} ({@link #collationKey}): the
     *  strings of a case that shows one value of each group of equal strings (IQ island, 2026-10-15, {@code member}). */
    private static SqlParity.Grid collated(SqlParity.Grid grid, List<String> types,
            com.legend.protocol.sql.iq.IqSettings profile) {
        List<List<Object>> rows = new ArrayList<>();
        for (List<Object> row : grid.rows()) {
            List<Object> keyed = new ArrayList<>();
            for (int c = 0; c < row.size(); c++) {
                keyed.add(collationKey(row.get(c), types.get(c), profile));
            }
            rows.add(keyed);
        }
        return new SqlParity.Grid(grid.columns(), rows);
    }

    /** Rows in one order whatever their source: by each cell's canonical text ({@link #canonical}). */
    private static List<List<Object>> sorted(List<List<Object>> rows) {
        List<List<Object>> out = new ArrayList<>(rows);
        out.sort(java.util.Comparator.comparing(r -> String.join("\001", r.stream().map(IqParity::canonical)
                .toList())));
        return out;
    }

    /** A cell's text for ordering a set of rows: a number by its exact value, whatever its Java type. */
    private static String canonical(@com.legend.base.Nullable Object v) {
        if (v == null) {
            return "\000";
        }
        if (v instanceof Double || v instanceof Float) {
            return "d" + v;
        }
        BigDecimal n = exact(v);
        return n != null ? "n" + n.stripTrailingZeros().toPlainString() : "s" + v;
    }

    /** An exact number's value: a Long, an Integer, a Short, a Byte, a BigInteger, a BigDecimal; null otherwise. */
    private static @com.legend.base.Nullable BigDecimal exact(Object v) {
        return switch (v) {
            case BigDecimal d -> d;
            case java.math.BigInteger i -> new BigDecimal(i);
            case Long l -> BigDecimal.valueOf(l);
            case Integer i -> BigDecimal.valueOf(i);
            case Short i -> BigDecimal.valueOf(i);
            case Byte i -> BigDecimal.valueOf(i);
            default -> null;
        };
    }

    /** How close two doubles must be when SQL Anywhere's column is a DOUBLE: a few units in the last place. */
    private static final double RELATIVE = 1e-15;

    /** A cell against SQL Anywhere's, by SQL Anywhere's type {@code saType}: an integer exactly (as a whole number, of
     *  any Java integer type), a numeric exactly by value (its scale is the column's, {@link #scale}), a double within
     *  {@link #RELATIVE} of it (a NaN or an infinity exactly), anything else as the same text. */
    public static boolean cellEquals(@com.legend.base.Nullable Object expected, @com.legend.base.Nullable Object actual,
            String saType) {
        if (expected == null || actual == null) {
            return expected == actual;
        }
        if (IqAnswers.INTEGER_TYPES.contains(saType) || saType.equals("numeric") || saType.equals("decimal")) {
            BigDecimal e = exact(expected);
            BigDecimal a = exact(actual);
            return e != null && a != null && e.compareTo(a) == 0
                    && (!IqAnswers.INTEGER_TYPES.contains(saType) || !(actual instanceof BigDecimal));
        }
        if (saType.equals("double") || saType.equals("float") || saType.equals("real")) {
            if (!(expected instanceof Double e) || !(actual instanceof Double || actual instanceof Float)) {
                return false;
            }
            double a = ((Number) actual).doubleValue();
            if (Double.isNaN(e) || Double.isInfinite(e) || Double.isNaN(a) || Double.isInfinite(a)) {
                return e.equals(a);
            }
            return Math.abs(e - a) <= RELATIVE * Math.max(Math.abs(e), Math.abs(a));
        }
        return !(actual instanceof Number) && !(actual instanceof Boolean) && expected.toString().equals(
                actual.toString());
    }

    /** Whether lite's column type (its Pure type's name) is of SQL Anywhere's type's family: null when it is, else
     *  the difference. An integer type (a BIT too) is an Integer; a numeric a Decimal (one of scale 0 an Integer too);
     *  a double a Float; text a String; a date a StrictDate; a timestamp a DateTime. */
    private static @com.legend.base.Nullable String family(String saType, int scale, String liteType) {
        String lite = liteType.contains("(") ? liteType.substring(0, liteType.indexOf('(')) : liteType;
        Set<String> allowed;
        if (IqAnswers.INTEGER_TYPES.contains(saType)) {
            allowed = Set.of("Integer");
        } else if (saType.equals("numeric") || saType.equals("decimal")) {
            allowed = scale == 0 ? Set.of("Decimal", "Integer") : Set.of("Decimal");
        } else if (saType.equals("double") || saType.equals("float") || saType.equals("real")) {
            allowed = Set.of("Float");
        } else if (saType.equals("char") || saType.equals("varchar") || saType.equals("long varchar")) {
            allowed = Set.of("String");
        } else if (saType.equals("date")) {
            allowed = Set.of("StrictDate", "Date");
        } else if (saType.equals("timestamp")) {
            allowed = Set.of("DateTime");
        } else {
            return null;
        }
        return allowed.contains(lite) ? null : "SQL Anywhere's type " + saType + " is " + allowed + ", legend-lite's is "
                + liteType;
    }

    /** Whether lite's numbers in column {@code i} carry SQL Anywhere's scale where its type is a NUMERIC of
     *  {@code scale} places (IQ shows {@code 1.50} as NUMERIC(2,1) {@code 1.5}): null when every decimal cell does (an
     *  integer cell is a NUMERIC of scale 0 read as one), else the difference. */
    public static @com.legend.base.Nullable String scale(String saType, int scale, SqlParity.Grid actual, int i) {
        if (!saType.equals("numeric") && !saType.equals("decimal")) {
            return null;
        }
        for (List<Object> row : actual.rows()) {
            Object v = row.get(i);
            int places = v instanceof BigDecimal d ? d.scale() : 0;
            if (v != null && places != scale) {
                return "SQL Anywhere's NUMERIC has " + scale + " places, legend-lite's value " + v + " has " + places;
            }
        }
        return null;
    }

    private static String oneLine(Throwable e) {
        String m = e.getClass().getSimpleName() + ": " + e.getMessage();
        m = m.replace('\n', ' ').replace('\t', ' ');
        return m.length() > 300 ? m.substring(0, 300) + "..." : m;
    }

    // ------------------------------------------------------------------ the ledger

    private static final String LEDGER_HEADER = "# the #IQ{...}# island's parity cases (iq-parity/lite), each case's"
            + " status per profile and lane: the ratchet IqParityTest and\n# IqParityPostgresTest hold"
            + " (core/src/test/java/com/legend/testcases/IqParity.java; the statuses are SqlParity's)\n";

    /** Each run's outcome against the ledger's column for {@code lane} (SqlParity's ratchet: REGRESSION, FIX
     *  DETECTED, STATUS MISMATCH, NEW, GONE); with {@code update}, the ledger's column is written instead. */
    public static List<String> ratchet(List<Run> runs, Map<String, SqlParity.Outcome> outcomes, Path ledger,
            String lane, @com.legend.base.Nullable Path update) throws IOException {
        List<SqlParity.Case> cases = new ArrayList<>();
        Map<String, SqlParity.Outcome> byKey = new LinkedHashMap<>();
        for (Run r : runs) {
            SqlParity.Outcome o = outcomes.get(r.key());
            if (o != null && (o.status() == SqlParity.Status.FAIL || o.status() == SqlParity.Status.BUG
                    || o.status() == SqlParity.Status.ERROR || System.getProperty("iq.parity.verbose") != null)) {
                // the line the ledger cannot hold: why a run is not as SQL Anywhere answers
                System.out.println("[iq-parity] " + lane + " " + o.status() + " " + r.key() + ": " + o.detail());
            }
            SqlParity.Case c = r.asSqlParityCase();
            cases.add(c);
            byKey.put(SqlParity.key(c), outcomes.get(r.key()));
        }
        return SqlParity.ratchet("the #IQ{...}# island's parity cases", "iq-parity", LEDGER_HEADER, cases, byKey,
                ledger, lane, update);
    }

    // ------------------------------------------------------------------ the known divergences (IQ island, landing 4)

    /** The statuses of a judged run whose answer is not its judge's: a wrong answer, an answer where the judge rejects
     *  it, an error where the judge answers. */
    private static final Set<SqlParity.Status> DIVERGING = Set.of(SqlParity.Status.FAIL, SqlParity.Status.BUG,
            SqlParity.Status.ERROR);

    /** One entry of the known-divergence allowlist: an {@code iq17-*} run, the lanes it diverges in, and the cause
     *  (a cause id the file declares). */
    public record Divergence(String run, Set<String> lanes, String cause) {
    }

    /** The allowlist {@code iq17-divergences.tsv}: its causes (id to what) and its entries, by run. Lines are
     *  {@code cause<TAB>id<TAB>what} and {@code run<TAB>key<TAB>cause id<TAB>lanes (duckdb,postgres)}; a line starting
     *  with {@code #} is a comment. A cause declared twice, an entry of an undeclared cause, a run listed twice, a lane
     *  of no lane, and an entry of no {@code iq17-*} run are errors. */
    public static Map<String, Divergence> divergences(Path file, Map<String, String> causes) throws IOException {
        Map<String, Divergence> out = new LinkedHashMap<>();
        int n = 0;
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            n++;
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] f = line.split("\t", -1);
            String where = file.getFileName() + ":" + n + ": ";
            if (f[0].equals("cause") && f.length == 3 && !f[1].isEmpty() && !f[2].isEmpty()) {
                if (causes.put(f[1], f[2]) != null) {
                    throw new IllegalStateException(where + "the cause " + f[1] + " is declared twice");
                }
            } else if (f[0].equals("run") && f.length == 4) {
                if (!causes.containsKey(f[2])) {
                    throw new IllegalStateException(where + "the cause " + f[2] + " is not declared (cause<TAB>id<TAB>"
                            + "what, above its runs)");
                }
                Set<String> lanes = new java.util.LinkedHashSet<>(List.of(f[3].split(",")));
                if (lanes.isEmpty() || !SqlParity.LANES.containsAll(lanes)) {
                    throw new IllegalStateException(where + "the lanes are some of " + SqlParity.LANES + ": " + f[3]);
                }
                if (!f[1].contains("@iq17-")) {
                    throw new IllegalStateException(where + f[1] + " is no iq17-* run: a run SQL Anywhere judges"
                            + " diverges never (no FAIL, BUG or ERROR is allowed it)");
                }
                if (out.put(f[1], new Divergence(f[1], lanes, f[2])) != null) {
                    throw new IllegalStateException(where + f[1] + " is listed twice");
                }
            } else {
                throw new IllegalStateException(where + "a line is cause<TAB>id<TAB>what or run<TAB>key<TAB>cause"
                        + "<TAB>lanes: " + line);
            }
        }
        return out;
    }

    /**
     * IQ island, landing 4 (WP1), 2026-10-14: the lane's outcomes against the known-divergence allowlist. A run whose
     * status is FAIL, BUG or ERROR in {@code lane} must be listed for the lane (an iq17-* run whose lite answer is not
     * the live IQ's, for a cause the file names); a listed run must still diverge in each lane it lists -- one that
     * now PASSes, or is refused or both-errors or skipped, must leave the list (shrink-only), and so must a run that is
     * gone; a declared cause no entry names is stale too. Returns the problems.
     */
    public static List<String> divergences(List<Run> runs, Map<String, SqlParity.Outcome> outcomes, Path file,
            String lane) throws IOException {
        Map<String, String> causes = new LinkedHashMap<>();
        Map<String, Divergence> listed = divergences(file, causes);
        List<String> problems = new ArrayList<>();
        Set<String> live = new java.util.HashSet<>();
        Map<String, Integer> counts = new java.util.TreeMap<>();
        for (Run r : runs) {
            live.add(r.key());
            SqlParity.Outcome o = outcomes.get(r.key());
            Divergence d = listed.get(r.key());
            boolean here = d != null && d.lanes().contains(lane);
            if (o != null && DIVERGING.contains(o.status()) && !here) {
                problems.add("UNLISTED DIVERGENCE: " + r.key() + " is " + o.status() + " in the " + lane + " lane and"
                        + " not in iq17-divergences.tsv" + (liveIq(r.profile()) ? " (list it with its cause, in a dated"
                        + " block naming the task, or fix it)" : " (a run SQL Anywhere judges may not diverge: fix it)")
                        + (o.detail().isEmpty() ? "" : ": " + o.detail()));
            } else if (here && (o == null || !DIVERGING.contains(o.status()))) {
                problems.add("ALLOWLIST SHRINKS: " + r.key() + " is listed in iq17-divergences.tsv (" + d.cause()
                        + ") for the " + lane + " lane, and is now " + (o == null ? "not run" : o.status()) + ": remove"
                        + " it (the list only shrinks)");
            }
            if (here) {
                counts.merge(d.cause(), 1, Integer::sum);
            }
        }
        for (Divergence d : listed.values()) {
            if (!live.contains(d.run())) {
                problems.add("ALLOWLIST SHRINKS: " + d.run() + " is listed in iq17-divergences.tsv and is no run: remove"
                        + " it");
            }
        }
        Set<String> used = new java.util.HashSet<>();
        listed.values().forEach(d -> used.add(d.cause()));
        for (String cause : causes.keySet()) {
            if (!used.contains(cause)) {
                problems.add("ALLOWLIST SHRINKS: the cause " + cause + " of iq17-divergences.tsv names no run: remove"
                        + " it");
            }
        }
        System.out.println("iq17 known divergences, " + lane + " lane: " + counts);
        return problems;
    }
}
