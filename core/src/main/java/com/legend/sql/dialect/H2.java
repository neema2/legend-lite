// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.List;

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

    /** The base passes plus the lateral explode (H2 2.1 has no LATERAL;
     * see LateralExplodeToUnion). */
    @Override
    protected java.util.List<com.legend.sql.SqlRewriter> passes() {
        java.util.List<com.legend.sql.SqlRewriter> ps =
                new java.util.ArrayList<>(super.passes());
        ps.add(new LateralExplodeToUnion());
        ps.add(new H2AvgDelivers());
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

    public H2() {
        super(Lexicon.H2, TypeNames.H2, Spellings.H2);
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
    protected String tableName(String name) {
        int dot = name.indexOf('.');
        if (dot <= 0) {
            return super.tableName(name);
        }
        return execPart(name.substring(0, dot)) + "."
                + execPart(name.substring(dot + 1));
    }

    /** EXCEPT names are DERIVED labels of the framed source (TDS/pivot
     * frames — the only EXCEPT producers) — they spell like every other
     * derived name: the alias rule. */
    @Override
    protected String starExceptName(String name) {
        return aliasIdent(name);
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
    protected String projection(com.legend.sql.SqlSelect.Projection p) {
        if (p.alias() != null || p.out() == null) {
            return super.projection(p);
        }
        return expr(p.expr(), 0) + " AS " + aliasIdent(p.out().name());
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
        };
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

    @Override
    public boolean rawH2IsNative() {
        return true;
    }

    /** Native QUALIFY (probed capability note above) — the clause
     * renders like DuckDB's; the base default walls, which stranded
     * the one PCT qualify test despite supportsQualify(). */
    @Override
    protected void appendQualify(StringBuilder sb, com.legend.sql.SqlSelect s,
            int depth) {
        nl(sb, depth).append("QUALIFY ").append(expr(
                java.util.Objects.requireNonNull(s.qualify(),
                        "appendQualify without a qualify clause"), 0));
    }

    /** No native dynamic PIVOT — the two-phase staticization pre-pass
     * pins the key values, then the static emulation strategy runs. */
    @Override
    public boolean needsStaticPivot() {
        return true;
    }

    @Override
    protected String call(SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        // integer division: the base renderer spells DuckDB's `a // b`,
        // and `//` is a LINE COMMENT on H2 — `SELECT 7 // 2` returns 7
        // silently (H2_BACKEND.md H5.2). H2's `/` over INT operands IS
        // integer division; CAST pins the operand type against widening.
        if (c.fn() == SqlFn.INT_DIVIDE) {
            return "CAST(" + expr(a.get(0), 6) + " / " + expr(a.get(1), 6)
                    + " AS BIGINT)";
        }
        // date arithmetic: the ANSI base spells DuckDB's interval idiom
        // `d + to_days(n)`; H2's form is dateadd(UNIT, n, d) — probed
        // 2.1.214 for DAY/MONTH/MICROSECOND, negative amounts, and
        // CAST-around composition.
        if (c.fn() == SqlFn.ADD_INTERVAL || c.fn() == SqlFn.ADD_INTERVAL_TEMPORAL) {
            return "dateadd(" + dateUnit(((SqlExpr.StringLit) a.get(0))
                    .value()) + ", " + expr(a.get(1), 0) + ", "
                    + expr(a.get(2), 0) + ")";
        }
        // H2 has no date_part — the SQL-standard extract(UNIT FROM x)
        // form (probed 2.1.214: HOUR over TIMESTAMP and TIME)
        if (c.fn() == SqlFn.EXTRACT) {
            return "extract(" + ((SqlExpr.StringLit) a.get(0)).value()
                    .toUpperCase(java.util.Locale.ROOT) + " FROM "
                    + expr(a.get(1), 0) + ")";
        }
        // format(template, args…) over a LITERAL template: H2 has no printf
        // (probed absent, 2026-07-31) — the template is a concatenation of
        // its text segments with each %d / %s slot CAST to VARCHAR (the
        // metamodel's dataTypeToSqlText: 'VARCHAR(%d)', 'DECIMAL(%d, %d)');
        // any other specifier is a capability gap, loud.
        if (c.fn() == SqlFn.FORMAT && !a.isEmpty()
                && a.get(0) instanceof SqlExpr.StringLit tpl) {
            return formatAsConcat(tpl.value(), a.subList(1, a.size()));
        }
        // H2 has no starts_with — LEFT/CHAR_LENGTH equality (probed,
        // incl. '%' in the prefix: no LIKE-escaping hazard)
        if (c.fn() == SqlFn.STARTS_WITH) {
            return "(LEFT(" + expr(a.get(0), 0) + ", CHAR_LENGTH("
                    + expr(a.get(1), 0) + ")) = " + expr(a.get(1), 0) + ")";
        }
        // strpos (P3): H2 spells LOCATE with SWAPPED args — probed
        // 2.1.214 parity on all edges (1-based, miss 0, empty needle 1).
        if (c.fn() == SqlFn.STRPOS) {
            return "LOCATE(" + expr(a.get(1), 0) + ", " + expr(a.get(0), 0)
                    + ")";
        }
        // ends_with (P3): the RIGHT/CHAR_LENGTH twin of STARTS_WITH
        // (probed incl. '%' suffix and needle-longer-than-string).
        if (c.fn() == SqlFn.ENDS_WITH) {
            return "(RIGHT(" + expr(a.get(0), 0) + ", CHAR_LENGTH("
                    + expr(a.get(1), 0) + ")) = " + expr(a.get(1), 0) + ")";
        }
        // bool = text (P8): the reference COERCES the string to bool
        // ('N' -> false, probed) where strict H2 rejects the compare —
        // a parseable literal becomes the boolean it names; anything
        // else falls through (the type error stays loud).
        if (c.fn() == SqlFn.EQUAL && a.size() == 2) {
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
                        return "(" + expr(a.get(i), 4) + " = "
                                + (b ? "TRUE" : "FALSE") + ")";
                    }
                }
            }
        }
        // boolean text (P7): H2 CAST prints 'TRUE' — the reference
        // prints 'true'; NULL stays NULL through the CASE.
        if (c.fn() == SqlFn.BOOL_TO_TEXT) {
            String v = expr(a.get(0), 0);
            return "CASE WHEN " + v + " THEN 'true' WHEN NOT " + v
                    + " THEN 'false' END";
        }
        // date_diff (P7): DATEDIFF(UNIT, a, b) — probed sign parity
        // (10 / -10) with the reference date_diff('unit', a, b).
        if (c.fn() == SqlFn.DATE_DIFF
                && a.get(0) instanceof SqlExpr.StringLit du) {
            return "DATEDIFF(" + du.value()
                    .toUpperCase(java.util.Locale.ROOT) + ", "
                    + expr(a.get(1), 0) + ", " + expr(a.get(2), 0) + ")";
        }
        // full-regexp match (P7): REGEXP_LIKE anchored '^(?:p)$' —
        // probed partial-vs-full parity; literal patterns anchor at
        // build time, dynamic ones through CONCAT.
        if (c.fn() == SqlFn.REGEXP_FULL_MATCH) {
            String pat = a.get(1) instanceof SqlExpr.StringLit pl
                    ? stringLit("^(?:" + pl.value() + ")$")
                    : "CONCAT('^(?:', " + expr(a.get(1), 0) + ", ')$')";
            return "REGEXP_LIKE(" + expr(a.get(0), 0) + ", " + pat + ")";
        }
        // error(msg) (P3): SIGNAL raises with the message and is LAZY
        // under CASE (probed 2.1.214: guarded branches do not fire,
        // taken branches raise, THEN-arm types unify). B7: the U+001F
        // provenance sentinel rides both ends (RaisedErrors).
        if (c.fn() == SqlFn.ERROR) {
            // optional second arg = 'line:col' provenance (PureSql.raise) —
            // same in-envelope U+001E convention as the ANSI arm
            String position = c.args().size() > 1
                    ? expr(c.args().get(1), 0) + " || CHAR(30) || " : "";
            return "SIGNAL('45000', CHAR(31) || " + position + "("
                    + expr(a.get(0), 0) + ") || CHAR(31))";
        }
        // split_part (R5c): H2 has no token pick — the probed EXACT
        // spelling (empty tokens KEPT, missing token -> '', matching
        // DuckDB split_part on all 6 probed edges): separator-count CASE
        // guard + REGEXP_REPLACE with \Q\E-quoted separator (H2 regexes
        // are java.util.regex). Literal single-char separator + literal
        // positive index only — the strategy rule emits nothing else.
        if (c.fn() == SqlFn.SPLIT_PART
                && a.get(1) instanceof SqlExpr.StringLit sep
                && sep.value().length() == 1
                && a.get(2) instanceof SqlExpr.IntLit ix && ix.value() >= 1) {
            String str = expr(a.get(0), 0);
            String sepLit = stringLit(sep.value());
            String q = "\\Q" + sep.value() + "\\E";
            String cls = "[^" + q + "]*";
            String pat = stringLit("^(?:" + cls + q + "){"
                    + (ix.value() - 1) + "}(" + cls + ").*$");
            return "CASE WHEN (CHAR_LENGTH(" + str
                    + ") - CHAR_LENGTH(REPLACE(" + str + ", " + sepLit
                    + ", ''))) < " + (ix.value() - 1)
                    + " THEN '' ELSE REGEXP_REPLACE(" + str + ", " + pat
                    + ", '$1') END";
        }
        // regexp_extract(s, p[, g]): H2 spells REGEXP_SUBSTR(s, p, 1, 1,
        // NULL, g) — probed 2.1.214: group g returns the capture ('-1.3421E-8'
        // -> '1.3421'); a MISS is NULL where DuckDB yields '' — COALESCE
        // keeps the contract the callers read ('' = the group did not match).
        if (c.fn() == SqlFn.REGEXP_EXTRACT && (a.size() == 2 || a.size() == 3)) {
            return "COALESCE(REGEXP_SUBSTR(" + expr(a.get(0), 0) + ", "
                    + expr(a.get(1), 0) + ", 1, 1, NULL, "
                    + (a.size() == 3 ? expr(a.get(2), 0) : "0") + "), '')";
        }
        // epoch(ts) / epoch_ms(ts): H2 has neither — the standard
        // EXTRACT(EPOCH FROM ts) (seconds with the subsecond fraction;
        // probed 2.1.214: 1417706543.123) and its millisecond scaling.
        if (c.fn() == SqlFn.EPOCH_SECONDS) {
            return "EXTRACT(EPOCH FROM " + expr(a.get(0), 0) + ")";
        }
        if (c.fn() == SqlFn.EPOCH_MS) {
            return "CAST(EXTRACT(EPOCH FROM " + expr(a.get(0), 0)
                    + ") * 1000 AS BIGINT)";
        }
        return super.call(c, parentPrec);
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

    /** DurationUnit interval-fn name -> H2 dateadd unit keyword (the
     * engine's extensionDefaults mapToDBUnitType; duplicated from the
     * QUARANTINED EngineStyleH2 deliberately — semantic unit data, not
     * golden formatting). */
    private static String dateUnit(String unitFn) {
        return switch (unitFn) {
            case "to_years" -> "YEAR";
            case "to_months" -> "MONTH";
            case "to_weeks" -> "WEEK";
            case "to_days" -> "DAY";
            case "to_hours" -> "HOUR";
            case "to_minutes" -> "MINUTE";
            case "to_seconds" -> "SECOND";
            case "to_milliseconds" -> "MILLISECOND";
            case "to_microseconds" -> "MICROSECOND";
            default -> throw new DialectCapability(
                    "interval unit '" + unitFn
                    + "' reached a dialect without a dateadd unit");
        };
    }

    /** Collection VALUES ride the JSON carrier (§2b, probed 2.1.214):
     * {@code JSON_ARRAY(e1, ..., eN NULL ON NULL)} — NULL ON NULL keeps
     * DuckDB's [null] element semantics (default ABSENT drops them);
     * dates serialize as ISO strings. The executor's unwrap parses the
     * JSON text back to the element list. */
    @Override
    protected String arrayLit(java.util.List<SqlExpr> elements) {
        if (elements.isEmpty()) {
            return "JSON_ARRAY()";
        }
        return "JSON_ARRAY(" + elements.stream()
                .map(e -> expr(e, 0))
                .collect(java.util.stream.Collectors.joining(", "))
                + " NULL ON NULL)";
    }

    /** Scalar -> variant JSON: {@code CAST(x AS JSON)} — probed
     * 2.1.214: numbers stay JSON numbers, booleans true/false, strings
     * quote; exact toVariant semantics. */
    @Override
    protected String variantConstruct(List<SqlExpr> a) {
        return "CAST(" + expr(a.get(0), 0) + " AS JSON)";
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
    private String formatAsConcat(String template, List<SqlExpr> args) {
        StringBuilder out = new StringBuilder("(");
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
                out.append(first ? "" : " || ").append("'")
                        .append(text.toString().replace("'", "''")).append("'");
                first = false;
                text.setLength(0);
            }
            out.append(first ? "" : " || ").append("CAST(")
                    .append(expr(args.get(slot++), 0)).append(" AS VARCHAR)");
            first = false;
        }
        if (text.length() > 0 || first) {
            out.append(first ? "" : " || ").append("'")
                    .append(text.toString().replace("'", "''")).append("'");
        }
        return out.append(")").toString();
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
    protected String reducer(com.legend.sql.SqlAgg.Reducer r) {
        if ((r.fn() == com.legend.sql.SqlAgg.Fn.QUANTILE_CONT
                    || r.fn() == com.legend.sql.SqlAgg.Fn.QUANTILE_DISC)
                && r.args().size() == 2 && !r.distinct()
                && r.orderBy().size() <= 1) {
            // the reducer's single order key IS the within-group order
            // (a descending percentile); none = ascending
            boolean desc = !r.orderBy().isEmpty()
                    && !r.orderBy().get(0).ascending();
            return (r.fn() == com.legend.sql.SqlAgg.Fn.QUANTILE_CONT
                    ? "PERCENTILE_CONT(" : "PERCENTILE_DISC(")
                    + expr(r.args().get(1), 0)
                    + ") WITHIN GROUP (ORDER BY " + expr(r.args().get(0), 0)
                    + (desc ? " DESC" : "") + ")";
        }
        if (r.fn() == com.legend.sql.SqlAgg.Fn.LIST && r.args().size() == 1
                && !r.distinct()) {
            StringBuilder sb = new StringBuilder("JSON_ARRAYAGG(")
                    .append(expr(r.args().get(0), 0));
            if (!r.orderBy().isEmpty()) {
                sb.append(" ORDER BY ");
                for (int i = 0; i < r.orderBy().size(); i++) {
                    var k = r.orderBy().get(i);
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(expr(k.expr(), 0))
                            .append(k.ascending() ? " ASC" : " DESC");
                    if (k.nullOrder() != null) {
                        sb.append(k.nullOrder()
                                == com.legend.sql.SqlSelect.SortKey
                                        .NullOrder.NULLS_FIRST
                                ? " NULLS FIRST" : " NULLS LAST");
                    }
                }
            }
            return sb.append(" NULL ON NULL)").toString();
        }
        // ordered aggregates pin the reference NULL placement too (the
        // sortKey pin's aggregate-internal twin — witnessed: sorted
        // joinStrings led with TDSNull under H2's NULLS-FIRST default)
        if (!r.orderBy().isEmpty()) {
            String args = r.args().isEmpty() ? "*" : list(r.args());
            String order = " ORDER BY " + r.orderBy().stream()
                    .map(k -> expr(k.expr(), 0)
                            + (k.ascending() ? " ASC" : " DESC")
                            + (k.nullOrder() == null
                                    ? (k.ascending() ? " NULLS LAST"
                                            : " NULLS FIRST")
                                    : k.nullOrder() == com.legend.sql
                                            .SqlSelect.SortKey.NullOrder
                                            .NULLS_FIRST
                                            ? " NULLS FIRST"
                                            : " NULLS LAST"))
                    .collect(java.util.stream.Collectors.joining(", "));
            return r.fn() + "(" + (r.distinct() ? "DISTINCT " : "") + args
                    + order + ")";
        }
        return super.reducer(r);
    }

    /** Banker's ROUND (probed on 2.1.214 against DuckDB ROUND_EVEN over
     * 2.5/3.5/-2.5/0.125/0.135/2.4 — all six equal): exact-.5 fractions
     * steer to the even integer of the scaled value, everything else is
     * plain ROUND. */
    @Override
    protected String roundHalfEven(List<SqlExpr> a) {
        String p = a.size() == 1 ? "1"
                : "POWER(10, " + expr(a.get(1), 0) + ")";
        String scaled = "(" + expr(a.get(0), 6) + " * " + p + ")";
        return "(CASE WHEN " + scaled + " - FLOOR(" + scaled + ") = 0.5"
                + " THEN (CASE WHEN MOD(CAST(FLOOR(" + scaled
                + ") AS BIGINT), 2) = 0 THEN FLOOR(" + scaled
                + ") ELSE FLOOR(" + scaled + ") + 1 END)"
                + " ELSE ROUND(" + scaled + ") END / " + p + ")";
    }

    /** SQL-standard constructor, probed on 2.1.214:
     * {@code JSON_OBJECT('k': v, ...)}. The kv list alternates
     * key-expression, value-expression. */
    @Override
    protected String jsonArray(SqlExpr.JsonArray j) {
        return "JSON_ARRAY(" + j.elements().stream()
                .map(e -> expr(e, 0)).collect(java.util.stream.Collectors.joining(", ")) + ")";
    }

    @Override
    protected String jsonObject(SqlExpr.JsonObject j) {
        StringBuilder sb = new StringBuilder("JSON_OBJECT(");
        List<SqlExpr> kv = j.kv();
        for (int i = 0; i + 1 < kv.size(); i += 2) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(expr(kv.get(i), 0)).append(": ")
                    .append(expr(kv.get(i + 1), 0));
        }
        return sb.append(')').toString();
    }

    /** SQL-standard aggregate, probed on 2.1.214:
     * {@code COALESCE(JSON_ARRAYAGG(v ORDER BY ...), JSON '[]')} — the
     * COALESCE keeps the graph contract's empty-array-over-zero-rows. */
    @Override
    protected String jsonArrayAgg(SqlExpr.JsonArrayAgg j) {
        StringBuilder sb = new StringBuilder("COALESCE(JSON_ARRAYAGG(")
                .append(expr(j.value(), 0));
        if (!j.orderKeys().isEmpty()) {
            sb.append(" ORDER BY ");
            for (int i = 0; i < j.orderKeys().size(); i++) {
                var k = j.orderKeys().get(i);
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(expr(k.expr(), 0))
                        .append(k.desc() ? " DESC" : " ASC")
                        .append(" NULLS LAST");
            }
        }
        return sb.append("), JSON '[]')").toString();
    }

    /** Boolean-to-text diverges: H2 CAST(bool AS VARCHAR) prints
     * 'TRUE', the reference 'true' (witnessed: assertEquals true vs
     * TRUE). A STRUCTURALLY BOOLEAN value casting to VARCHAR spells the
     * reference text via CASE — detection is by node shape (EXISTS,
     * literals, comparisons, logic, null tests, membership), never by
     * guessing a column's type. */
    @Override
    protected String variantAwareCast(SqlExpr.Cast c) {
        // leg 3.1b (the H2 grid canon spelled a BOOLEAN COLUMN as 'FALSE' —
        // the two rows host mode lost when the byte channel judged): a
        // value whose TYPE FACT is BOOLEAN takes the reference spelling
        // too, not only a boolean-SHAPED expression
        boolean booleanFact = c.value().type() instanceof com.legend.sql.TypeFact.Typed tf
                && tf.type() == com.legend.sql.SqlType.Scalar.BOOLEAN;
        if (c.target() == com.legend.sql.SqlType.Scalar.VARCHAR
                && (booleanFact || booleanShaped(c.value()))) {
            String v = expr(c.value(), 0);
            return "CASE WHEN " + v + " THEN 'true' WHEN NOT " + v
                    + " THEN 'false' END";
        }
        return super.variantAwareCast(c);
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

    /** H2 hands JSON values back as {@code byte[]} (UTF-8 text) — the
     * canonical envelope representation is the STRING (probed 2.1.214;
     * H2_BACKEND.md §12 step 11's first codec row). */
    @Override
    public @com.legend.base.Nullable Object normalize(@com.legend.base.Nullable Object jdbcValue,
            com.legend.sql.@com.legend.base.Nullable SqlType type) {
        if (jdbcValue instanceof byte[] b
                && type == com.legend.sql.SqlType.Scalar.JSON) {
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

    /** splitPart = the engine's own H2 extension function (commons split:
     *  adjacent separators collapse, past the end -> NULL) — the golden's spelling. */
    @Override
    protected String splitPartCall(java.util.List<SqlExpr> a) {
        return "legend_h2_extension_split_part(" + expr(a.get(0), 0) + ", "
                + expr(a.get(1), 0) + ", " + expr(a.get(2), 0) + ")";
    }
}
