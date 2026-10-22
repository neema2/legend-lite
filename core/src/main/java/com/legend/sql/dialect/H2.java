// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.List;
import java.util.Set;

/**
 * The H2 EXECUTION dialect (H2_BACKEND.md §12 step 8) — a sibling of
 * {@link DuckDb} extending {@link AnsiSqlRenderer}, NEVER
 * {@link EngineStyleH2} (whose output is byte-pinned to the engine's
 * golden TEXT and stays quarantined). This dialect's contract is
 * EXECUTABLE H2 2.1.214 SQL: every spelling here was probed against the
 * real jar or read from its function catalog; anything H2 cannot express
 * inherits the base renderer's LOUD walls and graduates into the
 * declared-gap registry (§9), never a silent wrong answer.
 *
 * <p>Capability notes (probed, see the doc's verification addendum):
 * native QUALIFY; NO correlated table-function arguments in any version
 * through 2.4.240 (LATERAL absent — the collection-carrier family walls);
 * NO typed-JSON navigation on 2.1.214 (the engine uses a Java UDF, a
 * route this dialect bans) — 2.3+ HAS it ((j)."field" quoted-key,
 * 1-based [i], typed-JSON values only): {@link H2Modern}, selected by
 * CONNECTED VERSION, spells it natively.
 */
public class H2 extends AnsiSqlRenderer {

    /** This database's name for each scalar type, a refusal where it has none (the dialect program's Phase 1,
     *  2026-10-13: an exhaustive switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN -> "BOOLEAN";
            case INTEGER -> "INTEGER";
            case BIGINT -> "BIGINT";
            case HUGEINT -> "NUMERIC(38)";
            case DOUBLE -> "DOUBLE PRECISION";
            case VARCHAR, LITERAL, TEMPORAL_TEXT -> "VARCHAR";
            case DATE -> "DATE";
            case TIMESTAMP -> "TIMESTAMP";
            case TIMESTAMPTZ -> "TIMESTAMPTZ";
            case JSON -> "JSON";
            // to the nanosecond: H2's TIME keeps no fraction of a second (its default precision is 0, measured
            // 2.1.214; the independent review, 2026-10-18)
            case TIME -> "TIME(9)";
            case DECIMAL_TEXT -> 
                    throw new IllegalStateException(s + " cast reached a dialect without " + s + " support");
        };
    }

    /** No STRUCT type here: a struct cast is refused. */
    @Override
    protected String structTypeName(com.legend.sql.SqlType.Struct st) {
        throw new DialectCapability("a STRUCT type reached a dialect without struct support");
    }

    /** The base passes plus the lateral explode (H2 2.1 has no LATERAL;
     * see LateralExplodeToUnion). */
    @Override
    protected java.util.List<com.legend.sql.SqlRewriter> passes() {
        java.util.List<com.legend.sql.SqlRewriter> ps = new java.util.ArrayList<>();
        ps.add(new CarrierStrategies(CAPS));
        if (!supportsQualify()) {
            ps.add(new QualifyToSubselect());
        }
        // a lateral over a constant list first: its rows need no correlation (ConstantLateralsAsRows)
        ps.add(new ConstantLateralsAsRows());
        ps.add(new LateralExplodeToUnion());
        ps.add(new AggregatesDeliverDouble(java.util.Set.of(com.legend.sql.SqlAgg.Fn.AVG)));
        // LAST: every source the passes above introduced is in scope when
        // references take their source's spelling (Phase 1, batch 135)
        ps.add(new SourceSpelling());
        return java.util.List.copyOf(ps);
    }

    /** H2 names the failing statement of a script ({@code SQL statement: <text>};
     *  a later failure quotes the script from that statement on): the index of the
     *  statement the quote starts with. */
    @Override
    public java.util.OptionalInt failingStatement(String message, java.util.List<String> statements) {
        int at = message.indexOf("SQL statement: ");
        if (at < 0) {
            return java.util.OptionalInt.empty();
        }
        String quoted = message.substring(at + "SQL statement: ".length()).strip();
        for (int i = 0; i < statements.size(); i++) {
            if (quoted.startsWith(statements.get(i).strip())) {
                return java.util.OptionalInt.of(i);
            }
        }
        return java.util.OptionalInt.empty();
    }

    /** H2 2.1.214 reserved words — the ENGINE's own list
     * (h2Extension2_1_214.pure:180-192). This is the GOLDEN TEXT
     * channel's set ({@code Ddl.createTable ENGINE_TEXT}'s
     * column-name rule quotes exactly what the engine quotes); the
     * EXECUTION lexicon {@link #LEXICON} derives from it. */
    public static final Lexicon ENGINE_LEXICON = new Lexicon('"', Set.of(
            "all", "and", "array", "as", "between", "case", "check",
            "constraint", "cross", "current_catalog", "current_date",
            "current_schema", "current_time", "current_timestamp",
            "current_user", "distinct", "except", "exists", "false", "fetch",
            "for", "foreign", "from", "full", "group", "having", "if", "in",
            "inner", "intersect", "interval", "is", "join", "left", "like",
            "limit", "localtime", "localtimestamp", "minus", "natural", "not",
            "null", "offset", "on", "or", "order", "primary", "qualify",
            "row", "rownum", "select", "table", "true", "union", "unique",
            "unknown", "using", "values", "where", "window", "with",
            "_rowid_", "both", "groups", "ilike", "leading", "over",
            "partition", "range", "regexp", "rows", "top", "trailing"));

    /** Stock H2 2.1.214 EXECUTION lexicon: the engine's list plus the
     * words the STOCK parser rejects in alias position (witnessed:
     * {@code AS right} &rarr; syntax error) that the engine's laxer
     * FORK omits. Execution-dialect-only — golden text channels use
     * {@link #ENGINE_LEXICON} or {@link EngineStyleH2#LEXICON}. */
    public static final Lexicon LEXICON = new Lexicon('"',
            java.util.stream.Stream.concat(
                    ENGINE_LEXICON.reservedWords().stream(),
                    java.util.stream.Stream.of("right"))
                    .collect(java.util.stream.Collectors.collectingAndThen(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new), java.util.Collections::unmodifiableSet)));

    /** H2: no native lists, no correlated explosion (the ONLY probed
     * backend without it), JSON constructors only. */
    public static final CarrierStrategies.Caps CAPS = new CarrierStrategies.Caps(false, false, false, false);

    public H2() {
        super(LEXICON);
    }

    @Override
    protected boolean supportsQualify() {
        return true;
    }

    /** SCHEMA-QUALIFIED references spell like the engine (convergence
     * batch B, 2026-08-29): the engine renders schema and table parts
     * through its identifierProcessor — bare unless pre-quoted,
     * reserved, or space-bearing — and its schema/table DDL spells the
     * SAME way, so bare references resolve on a case-sensitive
     * session (witness: calendarAggregation's
     * LegendCalendarSchema.NY_Calendar — created bare, referenced
     * quoted by the ANSI base's unconditional per-part quote → whole
     * family dead under engine casing). Single-part names already ride
     * {@link #ident} (bare-unless-needed); this brings the dotted
     * branch to the same rule. */
    @Override
    protected String tableName(String name, com.legend.sql.NameQuoting quoting) {
        int dot = name.indexOf('.');
        if (dot <= 0) {
            return super.tableName(name, quoting);
        }
        // a part the store declared quoted keeps its case (delimited); a bare one by the reference rule
        return (quoting.schema() ? delimited(name.substring(0, dot)) : execPart(name.substring(0, dot))) + "."
                + (quoting.table() ? delimited(name.substring(dot + 1)) : execPart(name.substring(dot + 1)));
    }

    /** EXCEPT names are DERIVED labels of the framed source (TDS/pivot
     * frames — the only EXCEPT producers) — they spell like every other
     * derived name: the alias rule. */
    @Override
    protected String starExceptName(String name) {
        return aliasIdent(name);
    }

    /** {@code RAWTOHEX(HASH('<algorithm>', x))}: a string's digest in lower-case hex. */
    private static SqlWriter digest(SqlWriter writer, String algorithm, List<SqlExpr> a) {
        return writer.append("RAWTOHEX(HASH('").append(algorithm).append("', ").expr(a.get(0), 0).append("))");
    }

    /** {@inheritDoc} H2 keeps the SQL standard's {@code information_schema}: the corpus's catalog-grid tests prove
     *  it, the primary keys' {@code key_column_usage} and {@code table_constraints} pair included (H2 2.1.214, probed
     *  2026-09-05: ordinal positions, quoted names). */
    @Override
    protected SqlWriter catalog(SqlWriter writer, com.legend.sql.SqlSource.Catalog c) {
        return InformationSchemaCatalog.write(writer, c, this, CATALOG_GRID_TYPES);
    }

    /** {@inheritDoc} H2 runs the standard spelling: the test-data generator's sessions prove it (the corpus's
     *  generator tests, ReservedNamesGenerationTest). */
    @Override
    protected String tempTableAs(com.legend.sql.SqlDml.CreateTempTableAs ct) {
        return createTemporaryTableAs(ct);
    }

    /** H2's catalog names a column's type by its own name ({@code CHARACTER VARYING}, {@code DOUBLE PRECISION}) and
     *  keeps the declared one beside it: its JDBC driver reads the code by the declared name where there is one
     *  (a FLOAT is FLOAT, a DECIMAL DECIMAL, though both are stored as another) and reports a DECIMAL by that name.
     *  Measured 2026-10-16 against {@code DatabaseMetaData.getColumns} on H2 2.1.214 (CatalogGridTest). */
    private static final InformationSchemaCatalog.Types CATALOG_GRID_TYPES = new InformationSchemaCatalog.Types(
            "CASE WHEN declared_data_type = 'DECIMAL' THEN 'DECIMAL' ELSE data_type END",
            "COALESCE(declared_data_type, data_type)", gridTypeCodes());

    private static java.util.Map<String, InformationSchemaCatalog.TypeCode> gridTypeCodes() {
        java.util.Map<String, InformationSchemaCatalog.TypeCode> m = new java.util.LinkedHashMap<>();
        m.put("INTEGER", InformationSchemaCatalog.TypeCode.INTEGER);
        m.put("CHARACTER VARYING", InformationSchemaCatalog.TypeCode.VARCHAR);
        m.put("BIGINT", InformationSchemaCatalog.TypeCode.BIGINT);
        m.put("SMALLINT", InformationSchemaCatalog.TypeCode.SMALLINT);
        m.put("TINYINT", InformationSchemaCatalog.TypeCode.TINYINT);
        m.put("DOUBLE PRECISION", InformationSchemaCatalog.TypeCode.DOUBLE);
        m.put("FLOAT", InformationSchemaCatalog.TypeCode.FLOAT);
        m.put("REAL", InformationSchemaCatalog.TypeCode.REAL);
        m.put("DECIMAL", InformationSchemaCatalog.TypeCode.DECIMAL);
        m.put("NUMERIC", InformationSchemaCatalog.TypeCode.NUMERIC);
        m.put("DATE", InformationSchemaCatalog.TypeCode.DATE);
        m.put("TIMESTAMP", InformationSchemaCatalog.TypeCode.TIMESTAMP);
        m.put("BOOLEAN", InformationSchemaCatalog.TypeCode.BOOLEAN);
        m.put("CHARACTER", InformationSchemaCatalog.TypeCode.CHAR);
        m.put("TIME", InformationSchemaCatalog.TypeCode.TIME);
        m.put("TIMESTAMP WITH TIME ZONE", InformationSchemaCatalog.TypeCode.TIMESTAMP_WITH_TIMEZONE);
        m.put("BINARY VARYING", InformationSchemaCatalog.TypeCode.VARBINARY);
        m.put("BINARY", InformationSchemaCatalog.TypeCode.BINARY);
        return m;
    }

    /** Aliases and every DERIVED label quote UNCONDITIONALLY — the
     * engine's own convention (as "root", as "legalName"): on the
     * case-sensitive session a bare invented name would uppercase in
     * result-set LABELS and break label-reading consumers (the PCT
     * minted-name decode saw 'ID' for 'id' — the batch-C blocker). */
    @Override
    protected String aliasIdent(String name) {
        if (name.length() > 1 && name.charAt(0) == '"'
                && name.endsWith("\"")) {
            return name;
        }
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    /** Alias-less projections label EXPLICITLY from the projection's
     * DECLARED output (engine convention; outputs-from-projections —
     * the output rides the projection itself, no positional pairing):
     * on this case-sensitive session an implicit label folds to the
     * PHYSICAL case while downstream layers reference the DECLARED
     * name (witness: the calendar aggregate's bare hireType labeling
     * HIRETYPE under the csv wrap's "hireType" read). Stars and
     * deliberately output-less projections pass labels through. */
    @Override
    protected SqlWriter projection(SqlWriter writer, com.legend.sql.SqlSelect.Projection p) {
        if (p.alias() != null || p.out() == null) {
            return super.projection(writer, p);
        }
        return writer.expr(p.expr(), 0).append(" AS ").append(aliasIdent(p.out().name()));
    }

    /** The origin-driven reference rule (convergence slice 3): DERIVED
     * spells exactly like its definition ({@link #aliasIdent});
     * PHYSICAL spells as the DDL spelled it (bare-unless-special —
     * pre-quoted declarations ride their quotes in the name);
     * origin-LESS walls loudly — a guessed spelling on a
     * case-sensitive session is a silent wrong answer. */
    @Override
    protected String columnName(SqlExpr.Column c) {
        if (c.origin() == null) {
            throw new DialectCapability("origin-less column reference"
                    + " reached the case-sensitive H2 renderer: "
                    + (c.table() == null ? "" : c.table() + ".")
                    + c.name() + " — stamp the construction site"
                    + " (Column.derived/physical or an OutputCol door)");
        }
        return switch (c.origin()) {
            case DERIVED -> aliasIdent(c.name());
            // declared quoted: delimited, its case kept
            case PHYSICAL_QUOTED -> delimited(c.name());
            case PHYSICAL -> execPart(c.name());
            // a per-key name is written with its key's position by the expression writer (AnsiSqlRenderer.perKeyName)
            case PER_KEY -> throw new IllegalStateException("a per-key name '" + c.name() + "' spelled without its"
                    + " key's position: the expression writer writes it inside its per-key region");
        };
    }

    /** A stored name as this dialect's queries reference a physical column or table part. */
    @Override
    public String physicalName(String name) {
        return execPart(name);
    }

    private String execPart(String part) {
        if (part.length() > 1 && part.charAt(0) == '"'
                && part.endsWith("\"")) {
            return part;
        }
        if (!part.matches("[A-Za-z_][A-Za-z0-9_$]*")
                || reservedWords()
                        .contains(part.toLowerCase(java.util.Locale.ROOT))) {
            return '"' + part + '"';
        }
        return part;
    }

    /** A store column's declared type as H2 spells it in DDL (until 2026-10-15 the base renderer's, which every
     *  dialect overrode a few of: the dialect program's owner guard). */
    @Override
    protected String ddlType(com.legend.sql.SqlDdl.ColumnType t) {
        return switch (t) {
            case com.legend.sql.SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case BIGINT -> "BIGINT";
                case SMALLINT -> "SMALLINT";
                case TINYINT -> "TINYINT";
                case INTEGER -> "INTEGER";
                case FLOAT -> "FLOAT";
                case DOUBLE -> "DOUBLE";
                case REAL -> "REAL";
                case BIT -> "BIT";
                case TIMESTAMP -> "TIMESTAMP";
                case DATE -> "DATE";
                case TIME -> "TIME(9)"; // (a fraction of a second kept, as the cast's)
                case TIMESTAMP_NS -> "TIMESTAMP(9)";
                case JSON -> "JSON";
                case VARCHAR -> "VARCHAR";
                case OTHER, DISTINCT, ARRAY, OBJECT -> throw DdlSpelling.unspelled(p.kind());
            };
            case com.legend.sql.SqlDdl.ColumnType.Sized z -> DdlSpelling.sized(z);
            case com.legend.sql.SqlDdl.ColumnType.Scaled s -> DdlSpelling.scaled(s);
        };
    }

    /** The engine's H2 text is H2's own. */
    @Override
    public String fromEngineText(String statement) {
        return statement;
    }

    /** The server versions this dialect writes for, as prefixes of the version a session reports: 2.1 and 2.2, the
     *  engine-parity target (the product's H2 is 2.1.214). A plan written in this dialect records them, and its runner
     *  refuses a session of another version (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 2's landing 2). */
    public static final java.util.List<String> SERVERS = java.util.List.of("2.1", "2.2");

    /** H2 types a parameter when it prepares the statement — by its neighbour (a decimal beside an integer column is
     *  read as an integer: {@code ID * ?} with 1.5 answers {@code [2, 6]}), or not at all when it stands alone
     *  ({@code Unknown data type}) — so every placeholder is written typed, as a literal of the parameter's value is,
     *  which answers as the literal does (docs/execution-plan-boundary-2026-10-05/probes/literal-results.txt). A
     *  parameter of one type is cast to it, a date-time to the nanosecond (a plain {@code TIMESTAMP} keeps six digits
     *  and rounds the rest, where the literal keeps nine). A parameter whose value decides its type (a Float's, a
     *  Decimal's, a Date's, a Number's) is cast to a TYPE HOLE the runner fills when the value is known, with the type
     *  H2 gives a literal of that value: a decimal of its own precision and scale, a floating decimal of its own digits,
     *  a date, a date-time to the nanosecond; and a whole number a BIGINT, as an Integer parameter is (the literal of a
     *  small one is an INTEGER: the same text, and Pure's Integer is 64 bits). A date-time is passed as its text, as on
     *  every database. Measured, every value against its literal, alone, in arithmetic, compared and in the answer's
     *  JSON (probes/value-typed-cast-results.txt). */
    @Override
    protected SqlWriter placeholder(SqlWriter writer, com.legend.sql.SqlExpr.PlanParam p) {
        com.legend.sql.ValueTyping byValue = p.byValue();
        if (byValue != null) {
            java.util.Map<com.legend.sql.ValueKind, RenderedStatement.TypeSpelling> types =
                    new java.util.EnumMap<>(com.legend.sql.ValueKind.class);
            for (com.legend.sql.ValueKind kind : byValue.kinds()) {
                types.put(kind, holeType(kind));
            }
            return writer.append("CAST(").bind(scalarBind(p)).append(" AS ").typeHole(types, byValue.absent())
                    .append(")");
        }
        if (!(p.type() instanceof com.legend.sql.TypeFact.Typed t)) {
            throw new DialectCapability("plan parameter '" + p.name() + "' has no type and is not typed by its value,"
                    + " and H2 types a parameter when it prepares the statement: not bound on H2");
        }
        return writer.append("CAST(").bind(scalarBind(p)).append(" AS ").append(parameterTypeName(t.type()))
                .append(")");
    }

    /** A parameter's cast type: its type's, a date-time's to the nanosecond. */
    private String parameterTypeName(com.legend.sql.SqlType t) {
        return t == com.legend.sql.SqlType.Scalar.TIMESTAMP ? "TIMESTAMP(9)" : castTypeName(t);
    }

    /** The type H2 gives a literal of a value of {@code kind}, spelled with the value's own digits where it takes them:
     *  every kind, H2 typing every placeholder when it prepares it. */
    @Override
    protected RenderedStatement.TypeSpelling holeType(com.legend.sql.ValueKind kind) {
        return switch (kind) {
            case INTEGER -> new RenderedStatement.TypeSpelling("BIGINT", RenderedStatement.Digits.NONE);
            case DECIMAL -> new RenderedStatement.TypeSpelling("NUMERIC", RenderedStatement.Digits.PRECISION_AND_SCALE);
            case FLOATING -> new RenderedStatement.TypeSpelling("DECFLOAT", RenderedStatement.Digits.PRECISION);
            case DATE -> new RenderedStatement.TypeSpelling("DATE", RenderedStatement.Digits.NONE);
            case DATE_TIME, DATE_TIME_NANOS -> new RenderedStatement.TypeSpelling("TIMESTAMP(9)",
                    RenderedStatement.Digits.NONE);
        };
    }

    /** CAPABILITY BY CONNECTED VERSION: H2 2.3+ has typed-JSON navigation ({@code (j)."f"},
     *  1-based {@code [i]}), which {@link H2Modern} spells natively; 2.1 and 2.2 -- the engine-parity
     *  target, {@link #SERVERS} -- keep this dialect's walls. */
    @Override
    public SqlDialect forServer(String version) {
        return SERVERS.stream().anyMatch(version::startsWith) ? this : new H2Modern();
    }

    /** {@inheritDoc} {@link #SERVERS}: another server's H2 takes {@link H2Modern} ({@link #forServer}). */
    @Override
    public java.util.List<String> serverVersions() {
        return SERVERS;
    }

    /** {@inheritDoc} Yes: an embedded engine. */
    @Override
    public boolean inProcess() {
        return true;
    }

    /** Native QUALIFY (probed capability note above) — the clause
     * renders like DuckDB's; the base default walls, which stranded
     * the one PCT qualify test despite supportsQualify(). */
    @Override
    protected SqlWriter appendQualify(SqlWriter writer, com.legend.sql.SqlSelect s,
            int depth) {
        return nl(writer, depth).append("QUALIFY ").expr(java.util.Objects.requireNonNull(s.qualify(),
                "appendQualify without a qualify clause"), 0);
    }

    /** No native dynamic PIVOT — the two-phase staticization pre-pass
     * pins the key values, then the static emulation strategy runs. */
    // the probe plan's facts (2026-10-20): measured 2026-10-20 (2.1.214): 16,384 columns; REPEATABLE READ accepted
    @Override
    public KeysSnapshot keysSnapshot() {
        return KeysSnapshot.REPEATABLE_READ;
    }

    @Override
    public @com.legend.base.Nullable Integer resultColumnLimit() {
        return 16384;
    }

    @Override
    public boolean needsStaticPivot() {
        return true;
    }

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        // every function, each by name: the switch is this dialect's whole table (the dialect program's Phase 1,
        // 2026-10-13: an exhaustive switch, so a new function asks this dialect too)
        return switch (c.fn()) {
            case FORMAT -> formatArm(writer, c);
            case EQUAL -> equalArm(writer, c, parentPrec);
            case DATE_DIFF -> dateDiffArm(writer, c);
            case SPLIT_PART -> splitPartArm(writer, c);
            case REGEXP_EXTRACT -> regexpExtractArm(writer, c);
            // ---- this dialect's own spellings
            // integer division: DuckDB spells `a // b`,
            // and `//` is a LINE COMMENT on H2 — `SELECT 7 // 2` returns 7
            // silently (H2_BACKEND.md H5.2). H2's `/` over INT operands IS
            // integer division; CAST pins the operand type against widening.
            case INT_DIVIDE ->
                    writer.append("CAST(").expr(a.get(0), 6).append(" / ").expr(a.get(1), 6).append(" AS BIGINT)");
            // date arithmetic: DuckDB spells its interval idiom
            // `d + to_days(n)`; H2's form is dateadd(UNIT, n, d) — probed
            // 2.1.214 for DAY/MONTH/MICROSECOND, negative amounts, and
            // CAST-around composition.
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> writer.append("dateadd(")
                    .append(dateUnit(part(a.get(0)))).append(", ").expr(a.get(1), 0)
                    .append(", ").expr(a.get(2), 0).append(")");
            // H2 has no date_part — the SQL-standard extract(UNIT FROM x)
            // form (probed 2.1.214: HOUR over TIMESTAMP and TIME)
            case EXTRACT -> writer.append("extract(").append(datePart(part(a.get(0))))
                    .append(" FROM ").expr(a.get(1), 0).append(")");
            // H2 has no starts_with — LEFT/CHAR_LENGTH equality (probed,
            // incl. '%' in the prefix: no LIKE-escaping hazard)
            case STARTS_WITH -> writer.append("(LEFT(").expr(a.get(0), 0).append(", CHAR_LENGTH(").expr(a.get(1), 0)
                    .append(")) = ").expr(a.get(1), 0).append(")");
            // strpos (P3): H2 spells LOCATE with SWAPPED args — probed
            // 2.1.214 parity on all edges (1-based, miss 0, empty needle 1).
            case STRPOS -> writer.append("LOCATE(").expr(a.get(1), 0).append(", ").expr(a.get(0), 0).append(")");
            // ends_with (P3): the RIGHT/CHAR_LENGTH twin of STARTS_WITH
            // (probed incl. '%' suffix and needle-longer-than-string).
            case ENDS_WITH -> writer.append("(RIGHT(").expr(a.get(0), 0).append(", CHAR_LENGTH(").expr(a.get(1), 0)
                    .append(")) = ").expr(a.get(1), 0).append(")");
            // boolean text (P7): H2 CAST prints 'TRUE' — the reference
            // prints 'true'; NULL stays NULL through the CASE.
            case BOOL_TO_TEXT -> writer.append("CASE WHEN ").expr(a.get(0), 0).append(" THEN 'true' WHEN NOT ")
                    .expr(a.get(0), 0).append(" THEN 'false' END");
            // full-regexp match (P7): REGEXP_LIKE anchored '^(?:p)$' —
            // probed partial-vs-full parity; literal patterns anchor at
            // build time, dynamic ones through CONCAT.
            case REGEXP_FULL_MATCH -> regexpFullMatch(writer, c);
            // error(msg) (P3): SIGNAL raises with the message and is LAZY
            // under CASE (probed 2.1.214: guarded branches do not fire,
            // taken branches raise, THEN-arm types unify). B7: the U+001F
            // provenance sentinel rides both ends (RaisedErrors).
            case ERROR -> errorArm(writer, c);
            // epoch(ts) / epoch_ms(ts): H2 has neither — the standard
            // EXTRACT(EPOCH FROM ts) (seconds with the subsecond fraction;
            // probed 2.1.214: 1417706543.123) and its millisecond scaling.
            case EPOCH_SECONDS -> writer.append("EXTRACT(EPOCH FROM ").expr(a.get(0), 0).append(")");
            case EPOCH_NS, FROM_EPOCH_NS -> throw new DialectCapability("a nanosecond epoch: H2 keeps"
                    + " microseconds (the Cypher island's nanosecond leg is DuckDB's)");
            case EPOCH_MS -> writer.append("CAST(EXTRACT(EPOCH FROM ").expr(a.get(0), 0).append(") * 1000 AS BIGINT)");
            // ---- each function as this dialect spells it, by name (the dialect program's Phase 1, D1, 2026-10-13:
            // an exhaustive switch; a spelling several databases share is a named helper of the base)
            // the ANSI operators
            case AND, OR, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, PLUS, MINUS, TIMES ->
                    infix(writer, c, parentPrec);
            // the shared spellings (the base's named helpers)
            case NOT -> notPrefix(writer, c.args(), parentPrec);
            // Pure's divide is a Float division: both operands DOUBLE
            case DIVIDE -> divideAsDoubles(writer, c.args());
            case MOD -> modFloored(writer, c.args());
            case REM -> modTruncated(writer, c.args());
            case NEGATE -> negation(writer, c.args());
            case IS_NULL -> isNullTest(writer, c.args());
            case IS_NOT_NULL -> isNotNullTest(writer, c.args());
            case IN -> inList(writer, c.args(), parentPrec);
            // 64-bit parse (PCT Long.MIN/MAX round-trips)
            case PARSE_INT -> castAs(writer, c.args().get(0), "BIGINT");
            // NULL-skipping flat concat (the database's concat skips NULL arguments)
            case CONCAT -> concatFlat(writer, c.args());
            // joinStrings' interleaved concat, never flattened
            case CONCAT_JOIN -> writer.function("concat", c.args());
            case PI -> writer.append("pi()");
            case CEILING -> bigintOf(writer, "ceil", c.args());
            case FLOOR -> bigintOf(writer, "floor", c.args());
            case SIGN -> bigintOf(writer, "sign", c.args());
            case XOR -> xorAsAndOr(writer, c.args(), parentPrec);
            case LPAD -> padWithSpace(writer, "lpad", c.args());
            case RPAD -> padWithSpace(writer, "rpad", c.args());
            case UC_FIRST -> firstCharCased(writer, "upper", c.args(), parentPrec);
            case LC_FIRST -> firstCharCased(writer, "lower", c.args(), parentPrec);
            // generateGuid's contract is text: the uuid as VARCHAR
            case GUID -> writer.append("CAST(uuid() AS VARCHAR)");
            case RANDOM -> writer.append("RAND()");
            case TODAY -> writer.append("current_date");
            case NOW -> writer.append("now()");
            case DATE_TRUNC_DAY -> castAs(writer, c.args().get(0), "DATE");
            case DATE_TRUNC -> dateTruncDateGrained(writer, c.args(),
                    stringLit(datePart(part(a.get(0))).toLowerCase(java.util.Locale.ROOT)));
            case CURRENT_USER_FN -> writer.append("current_user");
            case IS_DISTINCT_FROM, NULL_SAFE_NOT_EQUAL -> distinctFrom(writer, c.args());
            case NULL_SAFE_EQUAL -> notDistinctFrom(writer, c.args());
            // parseDate(text): the ISO text as a timestamp
            case PARSE_DATE -> castAs(writer, c.args().get(0), "TIMESTAMP");
            // Pure's divide-with-scale is HALF_UP: plain ROUND (half away from zero)
            case ROUND_HALF_UP -> writer.function("ROUND", c.args());
            case FLOAT_CANON -> floatCanonByText(writer, c.args(), parentPrec);
            case FLOAT_PRINT, FLOAT_PRINT_ENGINE -> floatPrintByText(writer, c, parentPrec);
            // the hooks: this dialect's own (or the base's refusal)
            case LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET, LIST_POSITION, LIST_ZIP, LIST_DISTINCT,
                 LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX, LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT,
                 LIST_SORT_DESC, LIST_TAIL, LIST_INIT, RANGE_FN, LIST_PRODUCT, LIST_REDUCE, LIST_SLICE, REPEAT_VALUE,
                 LIST_BOOL_AND, LIST_BOOL_OR, LIST_REVERSE, TYPEOF -> listCall(writer, c.fn(), c.args());
            case LIST_EXISTS -> listExists(writer, c.args());
            case LIST_FOR_ALL -> listForAll(writer, c.args());
            case STRUCT_INSERT -> structInsert(writer, c.args());
            case UNNEST -> unnestProjection(writer, c.args());
            case ROUND -> roundHalfEven(writer, c.args());
            case BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT -> bitOp(writer, c.fn(), c.args());
            case PURE_SPLIT_PART -> splitPartCall(writer, c.args());
            case HASH -> hashSigned(writer, c.args());
            case ALL_DISTINCT -> allDistinct(writer, c.args());
            case TO_VARIANT -> variantConstruct(writer, c.args());
            case VARIANT_ELEMENTS -> variantElements(writer, c.args());
            case VARIANT_GET -> variantGet(writer, c.args());
            // this database's plain spellings: name(args)
            case ABS -> writer.function("abs", c.args());
            case COALESCE -> writer.function("coalesce", c.args());
            case LENGTH -> writer.function("length", c.args());
            case UPPER -> writer.function("upper", c.args());
            case LOWER -> writer.function("lower", c.args());
            case GREATEST -> writer.function("greatest", c.args());
            case LEAST -> writer.function("least", c.args());
            case LIST_FLATTEN -> writer.function("flatten", c.args());
            case SQRT -> writer.function("sqrt", c.args());
            case CBRT -> writer.function("cbrt", c.args());
            case EXP -> writer.function("exp", c.args());
            case LN -> writer.function("ln", c.args());
            case LOG10 -> writer.function("log10", c.args());
            case POW -> writer.function("power", c.args());
            case SIN -> writer.function("sin", c.args());
            case COS -> writer.function("cos", c.args());
            case TAN -> writer.function("tan", c.args());
            case ASIN -> writer.function("asin", c.args());
            case ACOS -> writer.function("acos", c.args());
            case ATAN -> writer.function("atan", c.args());
            case ATAN2 -> writer.function("atan2", c.args());
            case SINH -> writer.function("sinh", c.args());
            case COSH -> writer.function("cosh", c.args());
            case TANH -> writer.function("tanh", c.args());
            case REGEXP_EXTRACT_ALL -> writer.function("regexp_extract_all", c.args());
            case REGEXP_REPLACE -> writer.function("regexp_replace", c.args());
            case MAP_FROM_LISTS -> writer.function("map", c.args());
            case MAP_FROM_ENTRIES -> writer.function("map_from_entries", c.args());
            case MAP_EXTRACT -> writer.function("map_extract", c.args());
            case MAP_KEYS -> writer.function("map_keys", c.args());
            case MAP_VALUES -> writer.function("map_values", c.args());
            case MAP_CONCAT -> writer.function("map_concat", c.args());
            case SUBSTRING -> writer.function("substr", c.args());
            case MATCHES -> writer.function("regexp_like", c.args());
            case LEFT -> writer.function("left", c.args());
            case RIGHT -> writer.function("right", c.args());
            case TRIM -> writer.function("trim", c.args());
            case LTRIM -> writer.function("ltrim", c.args());
            case RTRIM -> writer.function("rtrim", c.args());
            case REPLACE -> writer.function("replace", c.args());
            case TRANSLATE -> translate(writer, c.args());
            case SPLIT -> writer.function("string_split", c.args());
            case REVERSE_STRING -> writer.function("reverse", c.args());
            case ASCII_CODE -> writer.function("ascii", c.args());
            case CHR -> writer.function("chr", c.args());
            case LEVENSHTEIN -> writer.function("levenshtein", c.args());
            // H2 has no md5/sha1/sha256: its HASH digests, RAWTOHEX spells the digest in lower-case hex (2026-10-16,
            // the dialect program's Phase 3: the test-data generator's string hashing reached H2's sha256 and H2
            // refused it; DigestTest asks H2 and DuckDB for the same digests)
            case MD5 -> digest(writer, "MD5", c.args());
            case SHA1 -> digest(writer, "SHA-1", c.args());
            case SHA256 -> digest(writer, "SHA-256", c.args());
            case MAKE_DATE -> writer.function("make_date", c.args());
            case TIMEZONE -> writer.function("timezone", c.args());
            // H2 adds a date and a time to a timestamp (its datetime arithmetic; designed, not measured here)
            case DATE_AT_TIME -> op(writer, parentPrec, () -> writer.expr(a.get(0), 5).append(" + ")
                    .expr(a.get(1), 6));
            case FROM_EPOCH_SECONDS -> writer.function("to_timestamp", c.args());
            case DAYNAME -> writer.function("dayname", c.args());
            case MONTHNAME -> writer.function("monthname", c.args());
            case COT -> writer.function("cot", c.args());
            case RADIANS -> writer.function("radians", c.args());
            case DEGREES -> writer.function("degrees", c.args());
            case REPEAT_STR -> writer.function("repeat", c.args());
            case JARO_WINKLER -> writer.function("jaro_winkler_similarity", c.args());
            case LIST_LENGTH -> writer.function("len", c.args());
            case STRFTIME -> writer.function("formatdatetime", c.args());
            case STRPTIME -> writer.function("parsedatetime", c.args());
            case FLOOR_RAW -> writer.function("floor", c.args());
            case JSON_TYPE -> writer.function("json_type", c.args());
            case JSON_ARRAY_LENGTH -> writer.function("json_array_length", c.args());
            case JSON_PRETTY -> writer.function("json_pretty", c.args());
            // refused by name (D1, measured 2026-10-13: H2 cannot run DuckDB's spelling)
            case JSON_MERGE_PATCH ->
                    throw new DialectCapability("a JSON merge reached H2, which has no json_merge_patch()");
            case BIT_NOT -> throw new DialectCapability("bitNot reached H2, which has no xor()");
            case MAP_EMPTY -> throw new DialectCapability("an empty map reached H2, which has no map type");
            case ENCODE_BASE64 -> throw new DialectCapability("encodeBase64 reached H2, which has no to_base64()");
            case MAKE_TIMESTAMP ->
                    throw new DialectCapability("a date-time constructor reached H2, which has no make_timestamp()");
            case FROM_EPOCH_MS ->
                    throw new DialectCapability("a date from epoch milliseconds reached H2, which has no epoch_ms()");
            case DECODE_BASE64 -> throw new DialectCapability("decodeBase64 reached H2, which has no from_base64()");
            case TIME_BUCKET -> throw new DialectCapability("a time bucket reached H2, which has no time_bucket()");
        };
    }

    /** format(template, args…) over a LITERAL template: H2 has no printf (probed absent, 2026-07-31) — the template
     * is a concatenation of its text segments with each %d / %s slot CAST to VARCHAR (the metamodel's
     * dataTypeToSqlText: 'VARCHAR(%d)', 'DECIMAL(%d, %d)'); any other specifier is a capability gap, loud. */
    private SqlWriter formatArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!a.isEmpty() && a.get(0) instanceof SqlExpr.StringLit tpl) {
            return formatAsConcat(writer, tpl.value(), a.subList(1, a.size()));
        }
        return writer.function("printf", c.args());
    }

    /** bool = text (P8): the reference COERCES the string to bool ('N' -> false, probed) where strict H2 rejects the
     * compare — a parseable literal becomes the boolean it names; anything else falls through (the type error stays
     * loud). */
    private SqlWriter equalArm(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        if (a.size() == 2) {
            for (int i = 0; i < 2; i++) {
                if (booleanShaped(a.get(i))
                        && a.get(1 - i) instanceof SqlExpr.StringLit bs) {
                    String t = bs.value().toLowerCase(java.util.Locale.ROOT);
                    Boolean b = switch (t) {
                        case "true", "t", "1", "yes", "y" -> Boolean.TRUE;
                        case "false", "f", "0", "no", "n" -> Boolean.FALSE;
                        default -> null;
                    };
                    if (b != null) {
                        return writer.append("(").expr(a.get(i), 4).append(" = ").append((b ? "TRUE" : "FALSE"))
                                .append(")");
                    }
                }
            }
        }
        return infix(writer, c, parentPrec);
    }

    /** date_diff (P7): DATEDIFF(UNIT, a, b) — probed sign parity (10 / -10) with the reference
     * date_diff('unit', a, b). */
    private SqlWriter dateDiffArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        return writer.append("DATEDIFF(").append(datePart(part(a.get(0)))).append(", ")
                .expr(a.get(1), 0).append(", ").expr(a.get(2), 0).append(")");
    }

    /** A date part as H2's {@code extract} and {@code DATEDIFF} name it (and, in lower case, its {@code date_trunc}). */
    private static String datePart(com.legend.sql.DatePart p) {
        return switch (p) {
            case YEAR -> "YEAR";
            case QUARTER -> "QUARTER";
            case MONTH -> "MONTH";
            case WEEK -> "WEEK";
            case DAY -> "DAY";
            case HOUR -> "HOUR";
            case MINUTE -> "MINUTE";
            case SECOND -> "SECOND";
            case MILLISECOND -> "MILLISECOND";
            case MICROSECOND -> "MICROSECOND";
            case NANOSECOND -> throw new DialectCapability("a nanosecond part has no measured H2 spelling (the"
                    + " Cypher island's nanosecond leg is DuckDB's)");
            case DAY_OF_YEAR -> "DOY";
            case ISO_DAY_OF_WEEK -> "ISODOW";
        };
    }

    /** split_part (R5c): H2 has no token pick — the probed EXACT spelling (empty tokens KEPT, missing token -> '',
     * matching DuckDB split_part on all 6 probed edges): separator-count CASE guard + REGEXP_REPLACE with
     * \Q\E-quoted separator (H2 regexes are java.util.regex). Literal single-char separator + literal positive index
     * only — the strategy rule emits nothing else. */
    private SqlWriter splitPartArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.get(1) instanceof SqlExpr.StringLit sep
                && sep.value().length() == 1
                && a.get(2) instanceof SqlExpr.IntLit ix && ix.value() >= 1) {
            String sepLit = stringLit(sep.value());
            String q = "\\Q" + sep.value() + "\\E";
            String cls = "[^" + q + "]*";
            String pat = stringLit("^(?:" + cls + q + "){"
                    + (ix.value() - 1) + "}(" + cls + ").*$");
            return writer.append("CASE WHEN (CHAR_LENGTH(").expr(a.get(0), 0).append(") - CHAR_LENGTH(REPLACE(")
                    .expr(a.get(0), 0).append(", ").append(sepLit).append(", ''))) < ").append(ix.value() - 1)
                    .append(" THEN '' ELSE REGEXP_REPLACE(").expr(a.get(0), 0).append(", ").append(pat)
                    .append(", '$1') END");
        }
        return writer.function("split_part", c.args());
    }

    /** regexp_extract(s, p[, g]): H2 spells REGEXP_SUBSTR(s, p, 1, 1, NULL, g) — probed 2.1.214: group g returns the
     * capture ('-1.3421E-8' -> '1.3421'); a MISS is NULL where DuckDB yields '' — COALESCE keeps the contract the
     * callers read ('' = the group did not match). */
    private SqlWriter regexpExtractArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.size() == 2 || a.size() == 3) {
            writer.append("COALESCE(REGEXP_SUBSTR(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0)
                    .append(", 1, 1, NULL, ");
            return (a.size() == 3 ? writer.expr(a.get(2), 0) : writer.append("0")).append("), '')");
        }
        return writer.function("regexp_extract", c.args());
    }

    /** The {@code REGEXP_FULL_MATCH} arm of {@link #call}. */
    private SqlWriter regexpFullMatch(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        writer.append("REGEXP_LIKE(").expr(a.get(0), 0).append(", ");
        return (a.get(1) instanceof SqlExpr.StringLit pl
                ? writer.append(stringLit("^(?:" + pl.value() + ")$"))
                : writer.append("CONCAT('^(?:', ").expr(a.get(1), 0).append(", ')$')")).append(")");
    }

    /** The {@code ERROR} arm of {@link #call}. */
    private SqlWriter errorArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        // optional second arg = 'line:col' provenance (PureSql.raise) —
        // same in-envelope U+001E convention as raiseEnvelope
        writer.append("SIGNAL('45000', CHAR(31) || ");
        if (c.args().size() > 1) {
            writer.expr(c.args().get(1), 0).append(" || CHAR(30) || ");
        }
        return writer.append("(").expr(a.get(0), 0).append(") || CHAR(31))");
    }

    /** H2 window-frame interval bounds want the SQL-standard QUOTED
     * SINGULAR form ({@code INTERVAL '3' DAY} — probed; DuckDB's bare
     * plural {@code INTERVAL 3 DAYS} is a syntax error). WEEKS has no
     * standard interval unit — 7-day multiples. */
    @Override
    protected String bound(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p ->
                    h2Interval(p.n(), p.unit()) + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing f ->
                    h2Interval(f.n(), f.unit()) + " FOLLOWING";
            default -> super.bound(b);
        };
    }

    private static String h2Interval(long n, String unit) {
        String u = unit.toUpperCase(java.util.Locale.ROOT);
        if (u.equals("WEEKS") || u.equals("WEEK")) {
            return "INTERVAL '" + (n * 7) + "' DAY";
        }
        return "INTERVAL '" + n + "' "
                + (u.endsWith("S") ? u.substring(0, u.length() - 1) : u);
    }

    /** A DurationUnit's part -> H2 dateadd unit keyword (the
     * engine's extensionDefaults mapToDBUnitType; duplicated from the
     * QUARANTINED EngineStyleH2 deliberately — semantic unit data, not
     * golden formatting). */
    private static String dateUnit(com.legend.sql.DatePart unit) {
        return switch (unit) {
            case YEAR -> "YEAR";
            case MONTH -> "MONTH";
            case WEEK -> "WEEK";
            case DAY -> "DAY";
            case HOUR -> "HOUR";
            case MINUTE -> "MINUTE";
            case SECOND -> "SECOND";
            case MILLISECOND -> "MILLISECOND";
            case MICROSECOND -> "MICROSECOND";
            case QUARTER, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability(
                    "interval unit '" + unit + "' reached a dialect without a dateadd unit");
        };
    }

    /** Collection VALUES ride the JSON carrier (§2b, probed 2.1.214):
     * {@code JSON_ARRAY(e1, ..., eN NULL ON NULL)} — NULL ON NULL keeps
     * DuckDB's [null] element semantics (default ABSENT drops them);
     * dates serialize as ISO strings. The executor's unwrap parses the
     * JSON text back to the element list. */
    @Override
    protected SqlWriter arrayLit(SqlWriter writer, java.util.List<SqlExpr> elements) {
        if (elements.isEmpty()) {
            return writer.append("JSON_ARRAY()");
        }
        return writer.append("JSON_ARRAY(").list(elements).append(" NULL ON NULL)");
    }

    /** Scalar -> variant JSON: {@code CAST(x AS JSON)} — probed
     * 2.1.214: numbers stay JSON numbers, booleans true/false, strings
     * quote; exact toVariant semantics. */
    @Override
    protected SqlWriter variantConstruct(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("CAST(").expr(a.get(0), 0).append(" AS JSON)");
    }

    /** {@code SELECT * EXCEPT (...)} (probed 2.1.214, qualified and
     * bare — DuckDB's EXCLUDE is a syntax error here). */
    @Override
    protected String starExceptKeyword() {
        return "EXCEPT";
    }

    /** H2's row-order pseudo-column (probed 2.1.214: bare and inside
     * STRING_AGG's ORDER BY). */
    @Override
    protected String rowOrderColumn() {
        return "_ROWID_";
    }

    /** {@code format} with a literal template as {@code ||} concatenation
     * (see {@link #call}). */
    private SqlWriter formatAsConcat(SqlWriter writer, String template, List<SqlExpr> args) {
        writer.append("(");
        StringBuilder text = new StringBuilder();
        int slot = 0;
        boolean first = true;
        for (int i = 0; i < template.length(); i++) {
            char ch = template.charAt(i);
            if (ch != '%') {
                text.append(ch);
                continue;
            }
            if (i + 1 >= template.length()) {
                throw new DialectCapability("format template ends in a bare"
                        + " '%' — no H2 spelling");
            }
            char spec = template.charAt(++i);
            if (spec == '%') {
                text.append('%');
                continue;
            }
            if (spec != 'd' && spec != 's') {
                throw new DialectCapability("format specifier '%" + spec
                        + "' reached H2, which has no printf — only %d / %s"
                        + " concatenate");
            }
            if (slot >= args.size()) {
                throw new DialectCapability("format template has more slots"
                        + " than arguments");
            }
            if (text.length() > 0) {
                writer.append(first ? "" : " || ").append("'")
                        .append(text.toString().replace("'", "''")).append("'");
                first = false;
                text.setLength(0);
            }
            writer.append(first ? "" : " || ").append("CAST(").expr(args.get(slot++), 0).append(" AS VARCHAR)");
            first = false;
        }
        if (text.length() > 0 || first) {
            writer.append(first ? "" : " || ").append("'")
                    .append(text.toString().replace("'", "''")).append("'");
        }
        return writer.append(")");
    }

    // (The NULLS-LAST bare-key override is GONE — §7 slice-2 burn,
    // 2026-09-01: it conformed H2 to DuckDB's default for
    // self-consistency between our two executions, which inverted the
    // ENGINE's nulls-low ASC semantics on both. The base renderer now
    // pins engine placement explicitly on every bare key, so both
    // executions agree AND match the engine's own asserts.)

    /** FORMATDATETIME wants java.time patterns, not %-codes (probed
     * 2.1.214 byte-equal to DuckDB strftime on iso-micro, date, month/
     * weekday names, 12-hour + AM/PM). Literal text quotes ALWAYS —
     * the 'T' separator would otherwise parse as an era directive; the
     * enclosing {@code stringLit} doubles the quotes for SQL. */
    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        StringBuilder out = new StringBuilder();
        for (com.legend.sql.DateFmt p : fl.parts()) {
            out.append(switch (p) {
                case com.legend.sql.DateFmt.Text t ->
                        "'" + t.s().replace("'", "''") + "'";
                case com.legend.sql.DateFmt.Part part -> switch (part) {
                    case YEAR4 -> "yyyy";
                    case MONTH2 -> "MM";
                    case DAY2 -> "dd";
                    case HOUR2 -> "HH";
                    case MIN2 -> "mm";
                    case SEC2 -> "ss";
                    case SUBSEC_MICRO -> "SSSSSS";
                    /* nine-digit fractional seconds (the value-read
                     * wire decode, disagree-9 burn) — java.time nanos */
                    case SUBSEC_NANO -> "nnnnnnnnn";
                    // DuckDB %g trims trailing zeros — java.time has no
                    // trim token; the shape stays loud until witnessed.
                    case SUBSEC_MIN -> throw new DialectCapability(
                            "minimal-fraction date format reached a"
                            + " dialect without a trim-zeros token");
                    case MONTH_ABBREV -> "MMM";
                    case MONTH_NAME -> "MMMM";
                    case WEEKDAY_NAME -> "EEEE";
                    case HOUR12 -> "hh";
                    case HOUR12_NOPAD -> "h";
                    case AMPM -> "a";
                };
            });
        }
        return out.toString();
    }

    /** DuckDB's QUANTILE_CONT(v, q) has no H2 spelling — the SQL-standard
     * inverse-distribution form PERCENTILE_CONT(q) WITHIN GROUP (ORDER BY
     * v), probed 2.1.214. The LIST collect rides the JSON carrier:
     * {@code JSON_ARRAYAGG(x [ORDER BY ... NULLS ...] NULL ON NULL)} —
     * probed grammar (order clause BEFORE the null clause), zero rows ->
     * NULL and [null] element parity with DuckDB list(). NULLS placement
     * renders EXPLICITLY (the base reducer drops nullOrder). Other
     * reducers render on the base. */
    @Override
    protected SqlWriter reducer(SqlWriter writer, com.legend.sql.SqlAgg.Reducer r) {
        return switch (r.fn()) {
            case SUM -> h2Aggregate(writer, "SUM", r);
            case COUNT -> h2Aggregate(writer, "COUNT", r);
            case AVG -> h2Aggregate(writer, "AVG", r);
            case MIN -> h2Aggregate(writer, "MIN", r);
            case MAX -> h2Aggregate(writer, "MAX", r);
            // Pure's first() over a group is ANY non-empty value (empties vanish from a collection); H2 2.1.214 has no
            // ANY_VALUE, so a comparable scalar takes MIN (anyValue)
            case ANY_VALUE -> r.args().size() == 1 && !r.distinct() && r.orderBy().isEmpty() ? anyValue(writer, r)
                    : h2Aggregate(writer, "ANY_VALUE", r);
            case STDDEV_SAMP -> h2Aggregate(writer, "STDDEV_SAMP", r);
            case STDDEV_POP -> h2Aggregate(writer, "STDDEV_POP", r);
            case VAR_SAMP -> h2Aggregate(writer, "VAR_SAMP", r);
            case VAR_POP -> h2Aggregate(writer, "VAR_POP", r);
            case MEDIAN -> h2Aggregate(writer, "MEDIAN", r);
            case MODE -> h2Aggregate(writer, "MODE", r);
            case STRING_AGG -> h2Aggregate(writer, "STRING_AGG", r);
            case LIST -> r.args().size() == 1 && !r.distinct() ? listOfValues(writer, r)
                    : h2Aggregate(writer, "LIST", r);
            // the reducer's single order key IS the within-group order (a descending percentile); none = ascending
            case QUANTILE_CONT -> withinGroupShaped(r) ? percentile(writer, "PERCENTILE_CONT", r)
                    : h2Aggregate(writer, "QUANTILE_CONT", r);
            case QUANTILE_DISC -> withinGroupShaped(r) ? percentile(writer, "PERCENTILE_DISC", r)
                    : h2Aggregate(writer, "QUANTILE_DISC", r);
            case CORR -> h2Aggregate(writer, "CORR", r);
            case COVAR_SAMP -> h2Aggregate(writer, "COVAR_SAMP", r);
            case COVAR_POP -> h2Aggregate(writer, "COVAR_POP", r);
            case ARG_MAX -> h2Aggregate(writer, "ARG_MAX", r);
            case ARG_MIN -> h2Aggregate(writer, "ARG_MIN", r);
            case BOOL_AND -> h2Aggregate(writer, "BOOL_AND", r);
            case BOOL_OR -> h2Aggregate(writer, "BOOL_OR", r);
            case VARIANCE -> h2Aggregate(writer, "VARIANCE", r);
            case STDDEV -> h2Aggregate(writer, "STDDEV", r);
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY -> throw loweringMarker(r);
            // a window function is a ranking or value function, never an aggregate (windowCall writes it)
            case ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE,
                 NTH_VALUE -> throw windowFunction(r);
        };
    }

    /** An aggregate by its name; an ordered one pins the reference NULL placement too (the sortKey pin's
     *  aggregate-internal twin — witnessed: sorted joinStrings led with TDSNull under H2's NULLS-FIRST default). */
    private SqlWriter h2Aggregate(SqlWriter writer, String name, com.legend.sql.SqlAgg.Reducer r) {
        if (r.orderBy().isEmpty()) {
            return aggregate(writer, name, r);
        }
        writer.append(name).append("(").append(r.distinct() ? "DISTINCT " : "");
        if (r.args().isEmpty()) {
            writer.append("*");
        } else {
            writer.list(r.args());
        }
        return writer.append(" ORDER BY ").join(r.orderBy(), ", ", (w, k) -> w.expr(k.expr(), 0)
                .append(k.ascending() ? " ASC" : " DESC")
                .append(k.nullOrder() == null
                        ? (k.ascending() ? " NULLS LAST" : " NULLS FIRST")
                        : k.nullOrder() == com.legend.sql.SqlSelect.SortKey.NullOrder.NULLS_FIRST
                                ? " NULLS FIRST" : " NULLS LAST"))
                .append(")");
    }

    /** Pure's first() over a group is ANY non-empty value (empties
     * vanish from a collection); H2 2.1.214 has no ANY_VALUE, so a
     * comparable scalar takes MIN, which is such a value. A JSON or
     * array carrier has no order to take a minimum over: a wall,
     * never a guess (rebuild W0.6 push 13, E5). */
    private SqlWriter anyValue(SqlWriter writer, com.legend.sql.SqlAgg.Reducer r) {
        SqlExpr arg = r.args().get(0);
        if (arg.type() instanceof com.legend.sql.TypeFact.Typed t
                && t.type() instanceof com.legend.sql.SqlType.Scalar sc
                && sc != com.legend.sql.SqlType.Scalar.JSON) {
            return writer.append("MIN(").expr(arg, 0).append(")");
        }
        throw new DialectCapability("first() over a group of "
                + (arg.type() instanceof com.legend.sql.TypeFact.Typed t2 ? t2.type() : arg.type())
                + " values has no H2 spelling: this H2 has no ANY_VALUE and MIN needs a comparable scalar");
    }

    /** A percentile H2 writes WITHIN GROUP: a value and a level, at most one order key. */
    private static boolean withinGroupShaped(com.legend.sql.SqlAgg.Reducer r) {
        return r.args().size() == 2 && !r.distinct() && r.orderBy().size() <= 1;
    }

    /** {@code name(level) WITHIN GROUP (ORDER BY value[ DESC])}. */
    private SqlWriter percentile(SqlWriter writer, String name, com.legend.sql.SqlAgg.Reducer r) {
        boolean desc = !r.orderBy().isEmpty()
                && !r.orderBy().get(0).ascending();
        return writer.append(name).append("(")
                .expr(r.args().get(1), 0).append(") WITHIN GROUP (ORDER BY ").expr(r.args().get(0), 0)
                .append(desc ? " DESC" : "").append(")");
    }

    /** A list of the group's values: {@code JSON_ARRAYAGG(x[ ORDER BY keys] NULL ON NULL)}. */
    private SqlWriter listOfValues(SqlWriter writer, com.legend.sql.SqlAgg.Reducer r) {
        writer.append("JSON_ARRAYAGG(").expr(r.args().get(0), 0);
        if (!r.orderBy().isEmpty()) {
            writer.append(" ORDER BY ").join(r.orderBy(), ", ", (w, k) -> w.expr(k.expr(), 0)
                    .append(k.ascending() ? " ASC" : " DESC")
                    .append(k.nullOrder() == null ? ""
                            : k.nullOrder() == com.legend.sql.SqlSelect.SortKey.NullOrder.NULLS_FIRST
                                    ? " NULLS FIRST" : " NULLS LAST"));
        }
        return writer.append(" NULL ON NULL)");
    }

    /** Banker's ROUND (probed on 2.1.214 against DuckDB ROUND_EVEN over
     * 2.5/3.5/-2.5/0.125/0.135/2.4 — all six equal): exact-.5 fractions
     * steer to the even integer of the scaled value, everything else is
     * plain ROUND. */
    @Override
    protected SqlWriter roundHalfEven(SqlWriter writer, List<SqlExpr> a) {
        SqlWriter.Piece p = a.size() == 1 ? w -> w.append("1")
                : w -> w.append("POWER(10, ").expr(a.get(1), 0).append(")");
        SqlWriter.Piece scaled = w -> w.append("(").expr(a.get(0), 6).append(" * ").piece(p).append(")");
        return writer.append("(CASE WHEN ").piece(scaled).append(" - FLOOR(").piece(scaled).append(") = 0.5")
                .append(" THEN (CASE WHEN MOD(CAST(FLOOR(").piece(scaled).append(") AS BIGINT), 2) = 0 THEN FLOOR(")
                .piece(scaled).append(") ELSE FLOOR(").piece(scaled).append(") + 1 END)").append(" ELSE ROUND(")
                .piece(scaled).append(") END / ").piece(p).append(")");
    }

    /** SQL-standard constructor, probed on 2.1.214:
     * {@code JSON_OBJECT('k': v, ...)}. The kv list alternates
     * key-expression, value-expression. */
    @Override
    protected SqlWriter jsonArray(SqlWriter writer, SqlExpr.JsonArray j) {
        return writer.append("JSON_ARRAY(").list(j.elements()).append(")");
    }

    @Override
    protected SqlWriter jsonObject(SqlWriter writer, SqlExpr.JsonObject j) {
        writer.append("JSON_OBJECT(");
        List<SqlExpr> kv = j.kv();
        for (int i = 0; i + 1 < kv.size(); i += 2) {
            if (i > 0) {
                writer.append(", ");
            }
            writer.expr(kv.get(i), 0).append(": ").expr(kv.get(i + 1), 0);
        }
        return writer.append(')');
    }

    /** SQL-standard aggregate, probed on 2.1.214:
     * {@code COALESCE(JSON_ARRAYAGG(v ORDER BY ...), JSON '[]')} — the
     * COALESCE keeps the graph contract's empty-array-over-zero-rows. */
    @Override
    protected SqlWriter jsonArrayAgg(SqlWriter writer, SqlExpr.JsonArrayAgg j) {
        writer.append("COALESCE(JSON_ARRAYAGG(").expr(j.value(), 0);
        if (!j.orderKeys().isEmpty()) {
            writer.append(" ORDER BY ").join(j.orderKeys(), ", ", (w, k) -> w.expr(k.expr(), 0)
                    .append(k.desc() ? " DESC" : " ASC").append(" NULLS LAST"));
        }
        return writer.append("), JSON '[]')");
    }

    /** Boolean-to-text diverges: H2 CAST(bool AS VARCHAR) prints
     * 'TRUE', the reference 'true' (witnessed: assertEquals true vs
     * TRUE). A STRUCTURALLY BOOLEAN value casting to VARCHAR spells the
     * reference text via CASE — detection is by node shape (EXISTS,
     * literals, comparisons, logic, null tests, membership), never by
     * guessing a column's type. */
    @Override
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        // leg 3.1b (the H2 grid canon spelled a BOOLEAN COLUMN as 'FALSE' —
        // the two rows host mode lost when the byte channel judged): a
        // value whose TYPE FACT is BOOLEAN takes the reference spelling
        // too, not only a boolean-SHAPED expression
        boolean booleanFact = c.value().type() instanceof com.legend.sql.TypeFact.Typed tf
                && tf.type() == com.legend.sql.SqlType.Scalar.BOOLEAN;
        if (c.target() == com.legend.sql.SqlType.Scalar.VARCHAR
                && (booleanFact || booleanShaped(c.value()))) {
            return writer.append("CASE WHEN ").expr(c.value(), 0).append(" THEN 'true' WHEN NOT ").expr(c.value(), 0)
                    .append(" THEN 'false' END");
        }
        return super.variantAwareCast(writer, c);
    }

    private static boolean booleanShaped(SqlExpr e) {
        return switch (e) {
            case SqlExpr.BoolLit ignored -> true;
            case SqlExpr.Exists ignored -> true;
            case SqlExpr.Membership ignored -> true;
            case SqlExpr.Group g -> booleanShaped(g.inner());
            case SqlExpr.Call c -> switch (c.fn()) {
                case AND, OR, NOT, XOR, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL,
                     GREATER, GREATER_EQUAL, IS_NULL, IS_NOT_NULL, IN,
                     IS_DISTINCT_FROM, NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL,
                     STARTS_WITH, ENDS_WITH -> true;
                default -> false;
            };
            default -> false;
        };
    }

    /** translate, where H2's means the same: its TRANSLATE keeps a character of {@code from} past {@code to}'s end
     *  ({@code translate('abcabc', 'abc', 'x')} is {@code xbcxbc} on 2.4.240, measured 2026-10-15), where the function
     *  removes it ({@code xx}). So it is spelled only when {@code to}, a literal, is at least as long as {@code from}, a
     *  literal, and refused otherwise: H2's own answer would be a different one. */
    private static SqlWriter translate(SqlWriter writer, java.util.List<SqlExpr> a) {
        if (a.get(1) instanceof SqlExpr.StringLit from && a.get(2) instanceof SqlExpr.StringLit to
                && to.value().codePointCount(0, to.value().length())
                        >= from.value().codePointCount(0, from.value().length())) {
            return writer.function("translate", a);
        }
        throw new DialectCapability("TRANSLATE reached H2 with a to shorter than its from, or either not a literal:"
                + " H2's TRANSLATE keeps the characters translate removes");
    }

    /** H2 hands JSON values back as {@code byte[]} (UTF-8 text) — the
     * canonical envelope representation is the STRING (probed 2.1.214;
     * H2_BACKEND.md §12 step 11's first codec row). */
    @Override
    public @com.legend.base.Nullable Object normalize(@com.legend.base.Nullable Object jdbcValue,
            com.legend.sql.@com.legend.base.Nullable SqlType type) {
        // a JSON-carried list's text too (the list-less carrier, §2b): until 2026-10-15 the executor decoded H2's
        // byte[] there itself
        if (jdbcValue instanceof byte[] b
                && (type == com.legend.sql.SqlType.Scalar.JSON || type instanceof com.legend.sql.SqlType.Array)) {
            return new String(b, java.nio.charset.StandardCharsets.UTF_8);
        }
        // LEGACY-mode H2 hands DOUBLE aggregates back as BigDecimal
        // ('1E+1' for SUM over DOUBLE — probed; DuckDB hands Double):
        // the DECLARED column type drives the codec, and every float
        // print/compare downstream expects the double carrier.
        if (jdbcValue instanceof java.math.BigDecimal bd
                && type == com.legend.sql.SqlType.Scalar.DOUBLE) {
            return bd.doubleValue();
        }
        return jdbcValue;
    }

    /** Pure's splitPart with the engine's meaning (commons split: the separator is a SET of
     *  characters, adjacent separators collapse, 1-based part, NULL past the end) in plain H2:
     *  the part-th maximal run of non-separator characters, {@code REGEXP_SUBSTR(s, '[^…]+', 1, part)},
     *  which is NULL when there is no such run. Each separator character is written as
     *  {@code \x{h…}}, so no character is special inside the class. It used to call
     *  {@code legend_h2_extension_split_part}, a function only the engine's H2 (and our test
     *  harness) installs, so product SQL failed on a plain H2 (execution plan W0.2(a)); that
     *  spelling stays where it belongs, in {@link EngineStyleH2}'s golden text. A separator that
     *  is not a literal has no spelling here and is refused. */
    @Override
    protected SqlWriter splitPartCall(SqlWriter writer, java.util.List<SqlExpr> a) {
        if (!(a.get(1) instanceof SqlExpr.StringLit sep) || sep.value().isEmpty()) {
            throw new DialectCapability(
                    "splitPart on H2 needs a literal, non-empty separator");
        }
        StringBuilder cls = new StringBuilder("[^");
        sep.value().codePoints().forEach(cp ->
                cls.append("\\x{").append(Integer.toHexString(cp)).append('}'));
        cls.append("]+");
        return writer.append("REGEXP_SUBSTR(").expr(a.get(0), 0).append(", ").append(stringLit(cls.toString()))
                .append(", 1, ").expr(a.get(2), 0).append(")");
    }

    /** {@inheritDoc} Its JSON_TYPE answers DuckDB's names (its json_type, run on DuckDB's names). */
    @Override
    protected String jsonKindName(com.legend.sql.JsonKind kind) {
        return switch (kind) {
            case OBJECT -> "OBJECT";
            case ARRAY -> "ARRAY";
            case NULL -> "NULL";
            case STRING -> "VARCHAR";
            case INTEGER -> "BIGINT";
            case UNSIGNED_INTEGER -> "UBIGINT";
            case FLOAT -> "DOUBLE";
            case BOOLEAN -> "BOOLEAN";
        };
    }

    /** {@inheritDoc} A DATE is {@code DATE}; no other type's name is compared. */
    @Override
    protected String typeofName(com.legend.sql.SqlType.Scalar type) {
        if (type == com.legend.sql.SqlType.Scalar.DATE) {
            return "DATE";
        }
        throw new DialectCapability("a TYPEOF compared with " + type + "'s name reached H2");
    }
}
