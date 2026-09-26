package com.legend.exec;

import com.legend.sql.Json;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.element.type.ExprType;
import com.legend.error.DataError;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlQuery;
import com.legend.values.PureDateLiteral;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Executes rendered SQL and shapes the rows per the ROOT's classification.
 * Cell values are raw JDBC objects; column Pure types come from the query's
 * typed outputs. ONE registered exception (documented-debts 2026-08-18 —
 * the audit's §5 caught this header claiming "never" while the exception
 * lived below it): a schema rebuilt DOWNSTREAM of a dynamic pivot loses
 * the aggregate templates, and {@code pivotColumnType}'s last fallback
 * derives the Pure type from the JDBC type name through the exact-match
 * {@code pureOfSqlType} table (loud on unknown names). Everywhere else,
 * JDBC metadata never types a column.
 */
public final class Executor {

    private Executor() {
    }

    /**
     * Raw-statement execution — the K-native {@code executeInDb} boundary:
     * one already-dialect-adapted statement, no plan, no result shaping.
     */
    /** Runs one raw statement; true when it produced a result set (a
     * query) — the statement's KIND, from execution. */
    public static boolean executeRaw(Connection connection, String statement) {
        try (Statement st = connection.createStatement()) {
            Census.inc(Census.Key.SQL_ROUND_TRIPS);   // the raw-SQL boundary counts too
            StatementOrigin.count();
            Census.add(Census.Key.SQL_CHARS, statement.length());
            return st.execute(statement);
        } catch (SQLException e) {
            // THE SEAM (user directive 2026-09-01): java.sql stops at
            // the executor boundary — platform vocabulary above
            // cause = the UNWRAPPED exception (Positioned when the raise
            // was ours) — assertError's position adjudication reads it
            java.sql.SQLException un = RaisedErrors.unwrapped(e);
            throw new DataError(String.valueOf(un.getMessage()), un);
        }
    }

    /** The engines' own bulk-load APIs, found beside the drivers. */
    private static final List<BulkLoad> BULK_LOADS = java.util.ServiceLoader.load(BulkLoad.class)
            .stream().map(java.util.ServiceLoader.Provider::get).toList();

    /** Loads {@code load}'s rows: through the engine's bulk API when one is
     * present, else as ONE multi-row insert. The database types every cell
     * on both paths ({@link BulkLoad}'s contract); {@code dialect} renders
     * every statement either path sends. */
    public static void load(Connection connection, com.legend.sql.dialect.SqlDialect dialect,
            RowLoad load) {
        if (load.rows().isEmpty()) {
            return;
        }
        try {
            for (BulkLoad bulk : BULK_LOADS) {
                if (bulk.accepts(connection)) {
                    List<com.legend.sql.SqlDdl.Column> text = new ArrayList<>(load.width());
                    for (int c = 0; c < load.width(); c++) {
                        text.add(new com.legend.sql.SqlDdl.Column("c" + c, false,
                                new com.legend.sql.SqlDdl.ColumnType.Plain(
                                        com.legend.sql.SqlDdl.ColumnType.Kind.VARCHAR),
                                false, false));
                    }
                    BulkLoad.Staging staging = new BulkLoad.Staging(STAGE,
                            dialect.render(new com.legend.sql.SqlDdl.CreateTable(null, STAGE, text, true)),
                            dialect.render(load.fromStage(STAGE)),
                            dialect.render(new com.legend.sql.SqlDdl.DropTable(null, STAGE)));
                    Census.inc(Census.Key.SQL_ROUND_TRIPS);
                    Census.inc(Census.Key.BULK_LOADS);
                    StatementOrigin.count();
                    bulk.load(connection, load, staging);
                    return;
                }
            }
        } catch (SQLException e) {
            java.sql.SQLException un = RaisedErrors.unwrapped(e);
            throw new DataError(String.valueOf(un.getMessage()), un);
        }
        executeRaw(connection, dialect.render(load.values()));
    }

    /** A bulk load's staging table: temporary, so one per session, created
     *  and dropped inside the one load. */
    private static final String STAGE = "legend_row_load";

    /** ONE round trip for a multi-statement SCRIPT (block-compiler stage 3): the
     * engine runs it sequentially and stops at the first failing statement, the
     * earlier ones applied (probed on both engines, homework §19); result sets are
     * not read (a verdict statement is its own send). */
    public static void executeScript(Connection connection, String script) {
        try (Statement st = connection.createStatement()) {
            Census.inc(Census.Key.SQL_ROUND_TRIPS);
            StatementOrigin.count();
            Census.add(Census.Key.SQL_CHARS, script.length());
            if (System.getenv("LEGEND_LITE_DUMP_SQL") != null) {
                System.err.println("[script] " + script);
            }
            st.execute(script);
        } catch (SQLException e) {
            java.sql.SQLException un = RaisedErrors.unwrapped(e);
            throw new DataError(String.valueOf(un.getMessage()), un);
        }
    }

    /** Does this executor OWN the transaction a script would run in? A caller that
     * turned autocommit off (the corpus harness brackets an effect body as one
     * attempt, committed on a pass and rolled back on a failure) holds it; the
     * dialect's bracket must not nest inside, and a failure is the caller's to
     * unwind. */
    public static boolean ownsTransaction(Connection connection) {
        try {
            return connection.getAutoCommit();
        } catch (SQLException e) {
            throw new DataError(String.valueOf(e.getMessage()), e);
        }
    }

    public static ExecutionResult execute(String sql, SqlQuery plan, ExprType rootType,
                                          Connection connection,
                                          com.legend.sql.dialect.SqlDialect dialect,
                                          @com.legend.base.Nullable ExecutionTrace trace) {
        return execute(sql, plan, rootType, ResultShape.of(rootType), connection, dialect, trace);
    }

    /**
     * Shape-explicit entry: the driver decides the shape from the RESOLVED
     * root NODE (a class-typed root is GRAPH only under the resolver's
     * serialize envelope; bare it is an instance VALUE — the type alone
     * cannot tell them apart).
     */
    public static ExecutionResult execute(String sql, SqlQuery plan, ExprType rootType,
                                          ResultShape shape, Connection connection,
                                          com.legend.sql.dialect.SqlDialect dialect,
                                          @com.legend.base.Nullable ExecutionTrace trace) {
        return execute(sql, plan, rootType, shape, connection, dialect, null, trace);
    }

    /** V11 rider entry: when {@code rider} is a wrapped canon carrier,
     * the SCALAR/COLLECTION arms harvest the appended canon columns
     * row-aligned with the value decode — one execution serves both
     * the value fetch and the byte verdict. */
    public static ExecutionResult execute(String sql, SqlQuery plan, ExprType rootType,
                                          ResultShape shape, Connection connection,
                                          com.legend.sql.dialect.SqlDialect dialect,
                                          @com.legend.base.Nullable CanonRider rider,
                                          @com.legend.base.Nullable ExecutionTrace trace) {
        // trace (Phase 2b, batch 137): the caller's execution trace — the
        // stamp this execution publishes lands there, not on the thread.
        // TYPED-IR Slice 1: the label-lie census — every executed plan's
        // declared labels vs the bottom-up judgment (measurement only)
        SqlTypeCensus.probe(plan);
        try {
            return execute0(sql, plan, rootType, shape, connection, dialect,
                    rider, trace);
        } catch (SQLException e) {
            // B7 (RaisedErrors) + THE SEAM: a message WE raised in SQL
            // surfaces clean of the driver's transport envelope — HERE,
            // once, for every consumer — and java.sql STOPS HERE
            // (platform vocabulary above the boundary; the original
            // SQLException rides as cause)
            // cause = the UNWRAPPED exception (Positioned when the raise
            // was ours) — assertError's position adjudication reads it
            java.sql.SQLException un = RaisedErrors.unwrapped(e);
            throw new DataError(String.valueOf(un.getMessage()), un);
        }
    }

    private static ExecutionResult execute0(String sql, SqlQuery plan, ExprType rootType,
                                          ResultShape shape, Connection connection,
                                          com.legend.sql.dialect.SqlDialect dialect,
                                          @com.legend.base.Nullable CanonRider rider,
                                          @com.legend.base.Nullable ExecutionTrace trace)
            throws SQLException {
        // a TDSNull-TYPED root ([^TDSNull(), ^TDSNull()] — the grid
        // convention's null-cell VALUE, whose scalar form IS the SQL NULL):
        // every cell is the value; it rides the Any lane's decoded
        // null-slot egress, never the lowering-defect wall
        boolean anyRoot = PlatformTypes.isAny(rootType.type())
                || rootType.type()
                        instanceof com.legend.compiler.element.type.Type.ClassType nct
                && PlatformTypes.TDS_NULL_FQN.equals(nct.fqn());
        boolean variantRoot = rootType.type()
                instanceof com.legend.compiler.element.type.Type.ClassType vct
                && PlatformTypes.isVariant(vct);
        dumpSql(sql);
        // prepareStatement, not createStatement: DuckDB JDBC 1.5 masks a
        // direct Statement's real error behind 'Attempting to execute an
        // unsuccessful or closed pending query result' (audit: 74 corpus
        // errors were unreadable); prepare() surfaces the actual message
        try {
            return executePrepared(connection, sql, shape, plan, rootType,
                    dialect, anyRoot, variantRoot, rider, trace);
        } catch (SQLException e) {
            // error-path echo under the same diagnostic flag: a sweep's
            // failing statement is otherwise invisible (pre-exec dump
            // interleaves; the FAILING sql is the one worth reading)
            if (System.getenv("LEGEND_LITE_DUMP_SQL") != null) {
                System.err.println("[sql-fail] " + sql);
            }
            throw e;
        }
    }

    /**
     * E5 (JAVA_EVICTION_PLAN): ONE plan-rendered text value (the wire
     * column) — pure byte transport, the database composed the text.
     */
    public static String wireText(String sql, Connection connection) {
        dumpSql(sql);
        try (java.sql.PreparedStatement st = connection.prepareStatement(sql);
             ResultSet rs = st.executeQuery()) {
            if (!rs.next()) {
                throw new IllegalStateException(
                        "wire render produced no row");
            }
            String s = rs.getString(1);
            return s == null ? "" : s;
        } catch (SQLException e) {
            // cause = the UNWRAPPED exception (Positioned when the raise
            // was ours) — assertError's position adjudication reads it
            java.sql.SQLException un = RaisedErrors.unwrapped(e);
            throw new DataError(String.valueOf(un.getMessage()), un);
        }
    }

    /**
     * E5: stream the plan-rendered JSON rows (one {@code _wire_row}
     * object text per JDBC row — {@code Render.jsonWireRows}); Java
     * writes only the array punctuation, flushing per row so downstream
     * buffers release bytes as rows arrive. {@code out} is never closed.
     */
    public static void streamWireRows(String sql, Connection connection,
            java.io.Writer out) throws java.io.IOException {
        dumpSql(sql);
        try (java.sql.PreparedStatement st = connection.prepareStatement(sql);
             ResultSet rs = st.executeQuery()) {
            out.write('[');
            boolean first = true;
            while (rs.next()) {
                if (!first) {
                    out.write(',');
                }
                first = false;
                String row = rs.getString(1);
                out.write(row != null ? row : "null");
                out.flush();
            }
            out.write(']');
            out.flush();
        } catch (SQLException e) {
            // cause = the UNWRAPPED exception (Positioned when the raise
            // was ours) — assertError's position adjudication reads it
            java.sql.SQLException un = RaisedErrors.unwrapped(e);
            throw new DataError(String.valueOf(un.getMessage()), un);
        }
    }

    /** GRAPH streaming: the streaming lowering's one {@code json_object}
     * per JDBC row ({@code Lowerer#withStreamingGraphRoot}), each row's
     * JSON written verbatim inside an enclosing array. */
    public static void streamGraph(String sql, Connection connection,
            com.legend.sql.dialect.SqlDialect dialect, java.io.Writer out)
            throws java.io.IOException {
        dumpSql(sql);
        try (java.sql.PreparedStatement st = connection.prepareStatement(sql);
             ResultSet rs = st.executeQuery()) {
            out.write('[');
            boolean first = true;
            while (rs.next()) {
                if (!first) {
                    out.write(',');
                }
                first = false;
                // each cell is one complete json_object — normalized through
                // the dialect's JSON codec (H2 hands JSON back as byte[]),
                // then written verbatim: no parsing, no re-escaping.
                Object cell = dialect.normalize(rs.getObject(1),
                        com.legend.sql.SqlType.Scalar.JSON);
                out.write(cell != null ? String.valueOf(cell) : "null");
                out.flush();
            }
            out.write(']');
            out.flush();
        } catch (SQLException e) {
            // cause = the UNWRAPPED exception (Positioned when the raise
            // was ours) — assertError's position adjudication reads it
            java.sql.SQLException un = RaisedErrors.unwrapped(e);
            throw new DataError(String.valueOf(un.getMessage()), un);
        }
    }

    /** Opt-in diagnostic: every executed statement to stderr. Also the
     * ROUND-TRIP count (leg 3.0, DATABASE_MODE_HOMEWORK §4a): every
     * statement this executor sends passes here first — measurement
     * only, printed by the corpus lanes, read by no verdict. */
    private static void dumpSql(String sql) {
        Census.inc(Census.Key.SQL_ROUND_TRIPS);
        StatementOrigin.count();
        Census.add(Census.Key.SQL_CHARS, sql.length());
        if (System.getenv("LEGEND_LITE_DUMP_SQL") != null) {
            // the dump names the statement's ORIGIN mark (census: which sends are sides,
            // probes, seeds …) — the same fact the statement-origin census counts
            System.err.println("[sql:" + StatementOrigin.current().name().toLowerCase(java.util.Locale.ROOT) + "] " + sql);
        }
    }


    private static ExecutionResult executePrepared(Connection connection,
            String sql, ResultShape shape, SqlQuery plan, ExprType rootType,
            com.legend.sql.dialect.SqlDialect dialect, boolean anyRoot,
            boolean variantRoot, @com.legend.base.Nullable CanonRider rider,
            @com.legend.base.Nullable ExecutionTrace trace)
            throws SQLException {
        // the engine's execution-trace comment rides the statement the
        // database receives (ExecutionTrace, batch 83); the stamp lands on
        // the caller's trace (batch 137)
        try (java.sql.PreparedStatement st = PrepTrace.prepared(connection,
                trace != null ? trace.stamp(sql) : ExecutionTrace.stampOnly(sql), sql);
             ResultSet rs = PrepTrace.executed(st, sql)) {
            // CONTRACT PROGRAM: the wire census — label vs the result's
            // own metadata (rides with the data; no extra round trip).
            // Int-or-null columns are WATCHED, not counted: the row
            // reads below supply the value evidence and the finally
            // settles them (D1 — metadata alone cannot split a real
            // integer wire from an all-NULL column).
            SqlTypeCensus.probeWire(plan, rs,
                    dialect.getClass().getSimpleName());
            try {
                return runShape(shape, rs, plan, rootType, dialect,
                        anyRoot, variantRoot, rider);
            } finally {
                SqlTypeCensus.settleWire();
            }
        }
    }

    private static ExecutionResult runShape(ResultShape shape, ResultSet rs,
            SqlQuery plan, ExprType rootType,
            com.legend.sql.dialect.SqlDialect dialect, boolean anyRoot,
            boolean variantRoot, @com.legend.base.Nullable CanonRider rider)
            throws SQLException {
        return switch (shape) {
                case TABULAR -> tabular(rs, plan, rootType, dialect, rider);
                case SCALAR -> {
                    boolean hasRow = rs.next();
                    // ZERO ROWS under a REQUIRED declared bound raises —
                    // the engine's resultSizeRange enforcement (its Java
                    // executor checks the finished result's row count;
                    // multiplicity audit follow-up, egress slice A).
                    // Distinguishable from one-row-holding-NULL, so the
                    // TDSNull mapping-lane convention is untouched.
                    if (!hasRow && rootType.multiplicity()
                            .requireBounded("SCALAR shaping").lower() >= 1) {
                        throw new IllegalStateException(
                                "Cannot cast a collection of size 0 to"
                                + " multiplicity "
                                + rootType.multiplicity().text());
                    }
                    Object v = hasRow
                            ? cell(rs, plan, dialect, anyRoot, variantRoot)
                            : null;
                    if (hasRow) {
                        harvestCanon(rs, rider);
                    }
                    // a SECOND row under a scalar-shaped root is a resolver/
                    // lowering bug (e.g. a to-one stand-in leaking rows) —
                    // reading only the first would be a SILENT wrong value
                    // (audit 20b: this arm never checked)
                    if (rs.next()) {
                        throw new IllegalStateException("scalar-shaped result"
                                + " returned more than one row — the to-one"
                                + " contract was not enforced upstream");
                    }
                    if (v instanceof java.sql.Array arr) {
                        // the LIST WIRE arriving as ONE JDBC array cell under a
                        // scalar-shaped root: the collection IS the value — decoded
                        // HERE, at the one JDBC seam (F1.3b's named shrink, 2026-09-22:
                        // the router's flatten moved behind the exec seam)
                        yield new ExecutionResult.Collection(arrayCell(arr, dialect), rootType.type());
                    }
                    yield new ExecutionResult.Scalar(v, rootType.type());
                }
                case COLLECTION -> {
                    List<Object> values = new ArrayList<>();
                    boolean anyRow = false;
                    while (rs.next()) {
                        anyRow = true;
                        Cell c = cellRead(rs, plan, dialect, anyRoot, variantRoot);
                        Object v = c.value();
                        harvestCanon(rs, rider);
                        // THE LOWERER OWNS THE NULL-DROP (shortcut audit §5):
                        // pure's "a collection holds no empties" is compiled
                        // — the resolver filters optional-cell projections,
                        // the lowerer filters relation-map cells and strips
                        // value-lane carriers at the explode — so the SQL
                        // collection IS the pure collection and size()/at()/
                        // toOne() agree with what arrives here. A NULL cell
                        // reaching a non-variant COLLECTION egress is a
                        // lowering defect and WALLS; the silent one-line drop
                        // that used to sit here masked exactly that defect
                        // class. On the variant/Any lane the WIRE decides
                        // (TDSNull-is-a-value slice): a wire NULL is an
                        // EMPTY and decays by variant-decay semantics — a
                        // rule of the lane, not a mask; a PRESENT wire cell
                        // that decodes to null is the JSON null VALUE — the
                        // TDSNull slot the lowerer's variant value law
                        // emitted for a [1]-stamped cell (grid convention:
                        // TDSNull is DATA) — and stays in the collection as
                        // the host null slot (PureAsserts' direction-aware
                        // sentinel equivalence adjudicates it).
                        if (v == null) {
                            // a TDSNull-TYPED root: every cell IS the
                            // TDSNull value — the wire's ONE spelling of
                            // it (the [1..1] cell-read convention, the
                            // referee's sentinel), never a null slot
                            if (rootType.type()
                                    instanceof com.legend.compiler.element.type.Type.ClassType nct
                                    && PlatformTypes.TDS_NULL_FQN.equals(nct.fqn())) {
                                values.add(PlatformTypes.TDS_NULL_CELL);
                                continue;
                            }
                            if (!anyRoot && !variantRoot) {
                                throw new IllegalStateException("NULL cell"
                                        + " reached COLLECTION egress — the"
                                        + " lowerer owns the null-drop"
                                        + " (COMPILER_SHORTCUT_AUDIT §5); a"
                                        + " NULL here is a lowering defect,"
                                        + " never an empty");
                            }
                            if (c.wirePresent()) {
                                values.add(null);
                            }
                        } else {
                            values.add(v);
                        }
                    }
                    long lower = rootType.multiplicity()
                            .requireBounded("COLLECTION shaping").lower();
                    // ZERO ROWS under a required lower bound is the
                    // engine's finish-line resultSizeRange check (egress
                    // slice A) — a USER error with pure's own message,
                    // not a defect: the query legitimately emptied.
                    if (!anyRow && lower >= 1) {
                        throw new IllegalStateException(
                                "Cannot cast a collection of size 0 to"
                                + " multiplicity "
                                + rootType.multiplicity().text());
                    }
                    // the declared lower bound is the fact with teeth: a
                    // drop that shrinks a [1..*]-typed collection below its
                    // bound is a mapping/lowering defect, never a quiet count
                    if (values.size() < lower) {
                        throw new IllegalStateException("collection-shaped"
                                + " result holds " + values.size()
                                + " values, below its declared lower bound "
                                + lower + " — NULL cells were dropped past"
                                + " the type's own contract");
                    }
                    yield new ExecutionResult.Collection(values, rootType.type());
                }
                case GRAPH -> new ExecutionResult.Graph(
                        // the envelope is JSON TEXT by contract — the
                        // dialect codec canonicalizes driver-flavored
                        // carriers (H2 hands JSON back as byte[])
                        rs.next() ? String.valueOf(dialect.normalize(
                                rs.getObject(1),
                                com.legend.sql.SqlType.Scalar.JSON)) : "[]",
                        rootType.type());
            };
    }

    /** THE ONE canon read (V11 + V7 §8 leg 1): every appended canon
     * column — scalar candidates at 2..1+k, or a grid's single
     * per-row canon at the LAST column — reads through this single
     * choke point, row-aligned with the value decode. A wrapped rider
     * implies a non-variant scalar shape, so no value row is ever
     * dropped out of alignment (the COLLECTION null wall). */
    private static void harvestCanon(ResultSet rs,
            @com.legend.base.Nullable CanonRider rider) throws SQLException {
        int base;
        int count;
        if (rider == null) {
            return;
        } else if (rider.tdsWrapped()) {
            // position derives from the WRAP FRAME (CanonRider, the one
            // home) — never re-asked of the result set
            base = rider.rowCanonPosition();
            count = 1;
        } else if (rider.wrapped()) {
            base = 2;
            count = rider.kinds().size();
        } else {
            return;
        }
        String[] cs = new String[count];
        for (int i = 0; i < count; i++) {
            cs[i] = rs.getString(base + i);
        }
        rider.rows().add(cs);
    }

    /** A decoded cell PLUS the wire-presence fact: {@code wirePresent}
     * is whether the SQL cell itself held a value BEFORE decoding — a
     * JSON null is a PRESENT wire cell that decodes to host null (the
     * TDSNull slot), while an absent wire cell is an EMPTY. One fetch;
     * the distinction is read off the already-fetched value, never a
     * second accessor (tenet C1.2 ratchet). */
    record Cell(@com.legend.base.Nullable Object value, boolean wirePresent) {
    }

    private static @com.legend.base.Nullable Object cell(ResultSet rs, SqlQuery plan,
                               com.legend.sql.dialect.SqlDialect dialect, boolean anyRoot,
                               boolean variantRoot)
            throws SQLException {
        return cellRead(rs, plan, dialect, anyRoot, variantRoot).value();
    }

    private static Cell cellRead(ResultSet rs, SqlQuery plan,
                               com.legend.sql.dialect.SqlDialect dialect, boolean anyRoot,
                               boolean variantRoot)
            throws SQLException {
        Object fetched = fetch(rs, 1, sqlTypeOf(plan, 0));
        // presence is the RAW wire fact — read before unwrap (a JSON
        // null node is a present cell; unwrap/decode may host-null it)
        boolean present = fetched != null;
        Object v = unwrap(fetched, sqlTypeOf(plan, 0), dialect);
        // a LITERAL-labeled cell is FULLY decoded by unwrap (the label
        // IS the decode instruction — LiteralText); feeding the typed
        // result through decodeAny would RE-TYPE it by the json grammar
        // (the '3'->Long double-decode, gate-caught in the reference
        // channel). Only the JSON carrier decodes as variant.
        if (anyRoot
                && sqlTypeOf(plan, 0) != com.legend.sql.SqlType.Scalar.LITERAL) {
            return new Cell(decodeAny(v), present);
        }
        // A JSON-carrier CELL under a non-Any VALUE root (a variant-list
        // read narrowed by cast(@Float): $row.values->at(1)->cast(@Float) —
        // the cast re-roots the declared type, but the plan still types
        // the column JSON). Decided by that PLANNED type, never by the
        // driver's class: DuckDB's driver hands a JSON node, another
        // (the warehouse's) the text — the same wire either way, and
        // decoding it is recovery, never value guessing. NEVER for a
        // VARIANT root (audit 22 self-catch): a Variant result's contract
        // IS the JSON text — decoding it would change the wire.
        if (!variantRoot && v != null
                && sqlTypeOf(plan, 0) == com.legend.sql.SqlType.Scalar.JSON) {
            return new Cell(decodeAny(v), present);
        }
        return new Cell(v, present);
    }

    // latticeKind (the print-form kind sniffer for NUMBER-rooted mixed
    // identities) DELETED BY CENSUS (F10 slice 2b): mixed selections and
    // collections now carry the LITERAL label and parse in unwrap
    // (values/LiteralText); the instrumented firing count read ZERO across
    // the full PCT lane and the full corpus before deletion.


    /**
     * An ANY-typed value travels as variant JSON (the heterogeneous-list
     * carrier); at the boundary each element decodes back to its own runtime
     * kind — a number is a Number again, not the string {@code "1"}. Variant
     * results are NOT decoded (their contract is the JSON text itself); only
     * the Any root takes this path.
     *
     * <p>V1.8 adjudication (Phase 8): these scalar arms are the ANY-boundary
     * decode CONTRACT, not a duplicate JSON reader — the string arm already
     * delegates to the one unescape table (F3.1d), and the number arm must
     * NOT delegate to {@code sql/Json.num}: a decimal-form JSON number under
     * an Any root is a pure Float (host {@code Double}), while the strict
     * JSON bridge deliberately reads {@code BigDecimal} (audit 18 —
     * wireEquals-grade exactness). Same grammar, different target kinds by
     * design.
     */
    /** A class VALUE riding a JSON slot inside a struct (a polymorphic
     * field: {@code parameters : RelationalOperationElement[*]}, {@code
     * relationalElement : RelationalOperationElement[1]}) arrives as
     * object/array text — the field's declared kind says it is a
     * structure, so it decodes to the map/list the verdict's key
     * restriction and the wire consumers read (WORLD_MAP §4: the value
     * carries its class as {@code __type}). Scalars pass through
     * {@link #decodeAny} untouched; a Variant ROOT never comes here. */
    public static @com.legend.base.Nullable Object structured(@com.legend.base.Nullable Object v) {
        if (v instanceof String s) {
            String t = s.trim();
            if (t.startsWith("{") || t.startsWith("[")) {
                return Json.parse(t);
            }
        }
        return v;
    }

    /** A JDBC array cell's elements as wire values through THE one {@code unwrap}
     * (no declared element type: the driver object alone decides the carrier —
     * the one-carrier rule's own arm, not a second copy of it). */
    private static List<Object> arrayCell(java.sql.Array arr, com.legend.sql.dialect.SqlDialect dialect)
            throws SQLException {
        Object[] elements = (Object[]) arr.getArray();
        List<Object> out = new ArrayList<>(elements.length);
        for (Object el : elements) {
            out.add(unwrap(el, null, dialect));
        }
        return out;
    }

    private static @com.legend.base.Nullable Object decodeAny(@com.legend.base.Nullable Object v) {
        // Drivers hand JSON cells back as their own node type (DuckDB:
        // org.duckdb.JsonNode) or as text — matched by FULL class name.
        // The REASON is optional-dependency isolation, not guard-dodging
        // (documented-debts 2026-08-18; the audit read the old comment as
        // compliance-avoidance policy): exec MAY import driver packages
        // (F1.3 funnel), but a hard `instanceof org.duckdb.JsonNode`
        // links a class that is ABSENT on H2/SQLite-only deployments —
        // NoClassDefFoundError at first result read. The full-FQN string
        // is the exact-match, no-sniffing form of the same test; the
        // node's toString IS the JSON text.
        String s;
        if (v instanceof String str) {
            s = str;
        } else if (v != null && v.getClass().getName().equals("org.duckdb.JsonNode")) {
            s = v.toString();
        } else {
            return v;
        }
        String t = s.trim();
        // F10 slice-3 AUDIT (2026-08-24): the spelling arms that lived
        // here were WIRE SNIFFING — the engine's rule is DECLARED TYPE
        // DECIDES (relationalMappingExecution's narrow conversion
        // table; the '4'-as-Long witness showed sniffing mis-types raw
        // text). Spelling decode is LABEL-DRIVEN ONLY: the LITERAL arm
        // in unwrap. This reader keeps its ORIGINAL contract — the
        // variant-JSON carrier at Any roots.
        if (t.length() >= 2 && t.startsWith("\"") && t.endsWith("\"")) {
            return jsonUnescape(t.substring(1, t.length() - 1));
        }
        if (t.equals("true") || t.equals("false")) {
            return Boolean.valueOf(t);
        }
        if (t.equals("null")) {
            return null;
        }
        try {
            return Long.valueOf(t);
        } catch (NumberFormatException ignored) {
            // fall through
        }
        try {
            return Double.valueOf(t);
        } catch (NumberFormatException ignored) {
            return s;
        }
    }

    /**
     * JSON string-escape decoding — the variant carrier emits PROPER JSON, so
     * a value like {@code he said "hi"} arrives as {@code "he said \"hi\""};
     * a raw quote-strip would keep the backslashes (audit finding).
     */
    private static String jsonUnescape(String s) {
        // F3.1d: this was a keep-the-backslash TWIN of the platform
        // reader's table (sql/Json drops it — the same terminal rule as
        // the Pure unescape family); the twin lost the adjudication
        return com.legend.sql.Json.unescapeString(s);
    }

    /**
     * A composite JDBC cell unwraps by its DECLARED layout: a struct cell
     * becomes an ordered field map (names from the plan's {@link SqlType.Struct}
     * — the model's canonical layout, positional values), an array cell a list;
     * leaves normalize through the dialect. Attribute-count drift from the
     * declared layout is a contract violation — loud, never zipped short.
     */
    /**
     * Typed cell retrieval. TIMESTAMP columns fetch through {@code java.time}:
     * the driver's {@code java.sql.Timestamp} construction DROPS the BC era
     * (year -21457 surfaces as +21458 — irrecoverably, the epoch itself is
     * wrong). Timestamp stays the carrier where it is faithful (AD years);
     * a BC value keeps its LocalDateTime.
     */
    private static @com.legend.base.Nullable Object fetch(ResultSet rs, int i,
            com.legend.sql.@com.legend.base.Nullable SqlType type)
            throws SQLException {
        Object o = rs.getObject(i);
        if (o != null) {
            // D1 value evidence: a non-null driver object settles a
            // watched int-or-null column (no-op when nothing is
            // watched — the common statement)
            SqlTypeCensus.wireValueSeen(i);
        } else {
            // converse tripwire (E2E audit): a wire NULL under a
            // required-label column is a breach sighting
            SqlTypeCensus.wireNullSeen(i);
        }
        if (o instanceof java.sql.Timestamp) {
            // (a TIMESTAMP-typed output may still surface a VARCHAR cell —
            // the precision-faithful string convention — so gate on the
            // actual driver object, not the declared type.)
            // ONE carrier, chosen by KIND, never by value (documented-
            // debts 2026-08-18; the old `getYear() < 1` read a value's
            // MAGNITUDE to pick the box — C2.2's shape, the surviving
            // sibling of the deleted midnight heuristic): every
            // timestamp cell re-fetches as java.time, the BC-faithful
            // carrier — Timestamp's epoch is WRONG for BC years
            java.time.LocalDateTime ldt =
                    rs.getObject(i, java.time.LocalDateTime.class);
            return ldt != null ? ldt : o;
        }
        return o;
    }

    private static @com.legend.base.Nullable Object unwrap(@com.legend.base.Nullable Object v,
            com.legend.sql.@com.legend.base.Nullable SqlType type,
                                 com.legend.sql.dialect.SqlDialect dialect) throws SQLException {
        if (v == null) {
            return null;
        }
        // the KIND-FAITHFUL CARRIER (F10 proper): a LITERAL-declared
        // cell holds a pure-literal spelling — the label IS the decode
        // instruction, the text carries its own kind (values/LiteralText,
        // the host half of lowering/LiteralSpelling's grammar)
        if (type == com.legend.sql.SqlType.Scalar.LITERAL
                && v instanceof String ls) {
            return com.legend.values.LiteralText.parse(ls);
        }
        if (type instanceof com.legend.sql.SqlType.Struct st && v instanceof java.sql.Struct s) {
            Object[] attrs = s.getAttributes();
            if (attrs.length != st.fields().size()) {
                throw new IllegalStateException("struct cell has " + attrs.length
                        + " attribute(s) but the declared layout has " + st.fields().size());
            }
            java.util.LinkedHashMap<String, Object> m = new java.util.LinkedHashMap<>();
            for (int i = 0; i < attrs.length; i++) {
                Object fv = unwrap(attrs[i], st.fields().get(i).type(), dialect);
                // an Any-typed FIELD (a Pair<String, Any> second slot: a
                // string or ^TDSNull() sibling) rides the JSON carrier; the
                // field's VALUE is the decoded scalar ("Firm X" is the
                // string, null the TDSNull slot) — only a Variant ROOT
                // keeps its JSON text contract
                if (st.fields().get(i).type() == com.legend.sql.SqlType.Scalar.JSON) {
                    fv = structured(decodeAny(fv));
                }
                // a polymorphic TO-MANY field (JSON[]: the RelationalOperationElement
                // parameters of a DynaFunction) decodes per element the same way
                if (st.fields().get(i).type() instanceof com.legend.sql.SqlType.Array fa
                        && fa.element() == com.legend.sql.SqlType.Scalar.JSON
                        && fv instanceof List<?> fl) {
                    List<Object> decoded = new ArrayList<>(fl.size());
                    for (Object e : fl) {
                        decoded.add(structured(decodeAny(e)));
                    }
                    fv = decoded;
                }
                m.put(st.fields().get(i).name(), fv);
            }
            return m;
        }
        // the JSON carrier (H2 and any list-less backend, §2b): an
        // Array-typed cell arriving as JSON TEXT parses back to the
        // element list — the declared type drives the decode, never the
        // value's runtime class alone
        if (type instanceof com.legend.sql.SqlType.Array
                && (v instanceof byte[] || v instanceof String)) {
            String text = v instanceof byte[] b
                    ? new String(b, java.nio.charset.StandardCharsets.UTF_8)
                    : (String) v;
            if (text.startsWith("[")) {
                return Json.parse(text);
            }
        }
        if (type instanceof com.legend.sql.SqlType.Array at && v instanceof java.sql.Array a) {
            Object[] elements = (Object[]) a.getArray();
            List<Object> out = new ArrayList<>(elements.length);
            for (Object e : elements) {
                out.add(unwrap(e, at.element(), dialect));
            }
            return out;
        }
        // the ONE-CARRIER rule at every LEAF (documented-debts
        // 2018-08-18, HARDENED 2026-08-21 user directive): the wire's
        // temporal type is PureDateLiteral, FULL STOP — java.sql and
        // java.time temporals never escape this seam. Driver objects
        // convert in ONE hop here (java.time appears below only as the
        // driver's extraction vehicle); a declared-temporal VARCHAR
        // cell is the precision-faithful string convention and PARSES,
        // so written precision finally survives onto the wire.
        v = dialect.normalize(v, type);
        // the DECLARED type drives the codec (the H2 DOUBLE-arm
        // doctrine): an INTEGRAL-declared cell arriving as a scale-0
        // BigDecimal (DuckDB types beyond-int64 literals DECIMAL)
        // decodes to its EXACT integral carrier — the pure Integer kind
        // never blurs into Decimal on the wire (X-audit decode guard)
        // (the VALUE decides: a fractional cell under an integral slot is a
        // declared/physical mismatch — decoded as what it is, counted by the
        // census, never asserted — the wire-slot leg)
        if (v instanceof java.math.BigDecimal bd
                && (type == com.legend.sql.SqlType.Scalar.BIGINT
                        || type == com.legend.sql.SqlType.Scalar.INTEGER
                        || type == com.legend.sql.SqlType.Scalar.HUGEINT)
                && bd.stripTrailingZeros().scale() <= 0) {
            java.math.BigInteger bi = bd.toBigIntegerExact();
            return bi.bitLength() < 63 ? (Object) bi.longValue() : bi;
        }
        return switch (v) {
            case java.sql.Timestamp ts ->
                    // struct/array leaves never pass fetch()'s BC-safe
                    // re-fetch; Timestamp is faithful for AD years
                    PureDateLiteral.fromLocalDateTime(ts.toLocalDateTime());
            case java.sql.Date d ->
                    PureDateLiteral.fromLocalDate(d.toLocalDate());
            case java.time.LocalDate ld -> PureDateLiteral.fromLocalDate(ld);
            case java.time.LocalDateTime ldt ->
                    PureDateLiteral.fromLocalDateTime(ldt);
            case java.time.OffsetDateTime odt ->
                    PureDateLiteral.fromLocalDateTime(odt.withOffsetSameInstant(
                            java.time.ZoneOffset.UTC).toLocalDateTime());
            case String s when isTemporalType(type) ->
                    PureDateLiteral.parse(s.trim().replace(' ', 'T'));
            // the wire-decoded decimal carrier (DECIMAL_TEXT, the B3
            // pattern's twin): the label IS the decode instruction —
            // the cell holds the value-lane canonical D-suffixed
            // spelling, decoded to its exact BigDecimal
            case String s when type
                    == com.legend.sql.SqlType.Scalar.DECIMAL_TEXT ->
                    new java.math.BigDecimal(
                            s.endsWith("D") || s.endsWith("d")
                                    ? s.substring(0, s.length() - 1) : s);
            case null, default -> v;
        };
    }

    private static boolean isTemporalType(com.legend.sql.@com.legend.base.Nullable SqlType type) {
        return type == com.legend.sql.SqlType.Scalar.DATE
                || type == com.legend.sql.SqlType.Scalar.TIMESTAMP
                || type == com.legend.sql.SqlType.Scalar.TIMESTAMPTZ
                // the B3 carrier: temporal-in-text-carriage — the label
                // IS the decode instruction (the LITERAL doctrine)
                || type == com.legend.sql.SqlType.Scalar.TEMPORAL_TEXT;
    }

    /**
     * PURE column types come from the TYPED HIR ROOT's schema (the frontend's
     * truth); SQL types for driver normalization come from the plan's
     * outputs. The two type systems meet only here, each on its own side.
     */
    private static ExecutionResult.Tabular tabular(ResultSet rs, SqlQuery plan, ExprType rootType,
                                                    com.legend.sql.dialect.SqlDialect dialect,
                                                    @com.legend.base.Nullable CanonRider rider)
            throws SQLException {
        final Type.RelationType schema = tabularSchema(rootType);
        // V7 §8 leg 1 — a grid-wrapped rider rode the plan's LAST
        // column (__rowcanon): strip it from the value decode and
        // harvest it row-aligned (shapeRow is 1:1 by construction)
        boolean grid = rider != null && rider.tdsWrapped();
        // the value decode reads the data columns only (the wrap frame's
        // appended canon columns: CanonRider, the one home)
        int n = rs.getMetaData().getColumnCount() - (rider == null ? 0 : rider.canonColumns());
        List<Column> columns = resolveColumns(rs, plan, schema, n);
        List<Row> rows = new ArrayList<>();
        while (rs.next()) {
            rows.addAll(shapeRow(rs, n, plan, dialect, schema, columns));
            harvestCanon(rs, rider);
        }
        return new ExecutionResult.Tabular(columns, rows, rootType.type());
    }

    /** The relation schema of a TABULAR root, struct columns flattened. */
    private static Type.RelationType tabularSchema(ExprType rootType) {
        // a wrapped table's schema, or a bare struct (a ROW root reads
        // as a one-row table view) — Row-vs-Relation
        Type.RelationType typedSchema = Type.schemaView(rootType.type());
        if (typedSchema == null) {
            throw new IllegalStateException("TABULAR result without a relation root type: "
                    + rootType.type().typeName());
        }
        // A ROW-STRUCT column (a user navigate's slot) is typed nesting over
        // a FLAT physical reality — expand to the prefixed columns the join
        // emitted (alias_COL), mirroring the lowerer's output flattening.
        return flattenStructColumns(typedSchema);
    }

    private static List<Column> resolveColumns(ResultSet rs, SqlQuery plan,
            Type.RelationType schema, int n) throws SQLException {
        List<Column> columns = new ArrayList<>();
        if (schema.isLateBound()) {
            // SINGLE-QUERY RULE (P3-2): an UNDEMANDED raw grid skipped
            // the LIMIT-0 probe — the ONE executed query is its own
            // schema authority. Adopt the result-set headers as trusted
            // columns (Any[0..1], the trust-name rule); the wire KIND
            // drives cell decode (the plan carries no outputs to
            // consult). Gate is the TYPE (schema.isLateBound()), never
            // an outputs.isEmpty() proxy.
            for (int i = 1; i <= n; i++) {
                Type.Column tc = Type.RelationType.trustedColumn(
                        rs.getMetaData().getColumnName(i));
                columns.add(new Column(tc.name(), tc.type(),
                        tc.multiplicity()));
            }
            return columns;
        }
        if (n == schema.columns().size()) {
            // POSITIONAL on both sides (schemas are ordered); no null types.
            for (int i = 1; i <= n; i++) {
                Type.Column sc = schema.columns().get(i - 1);
                columns.add(new Column(sc.name(), sc.type(),
                        sc.multiplicity()));
            }
        } else if (hasPivot(plan)) {
            // DYNAMIC PIVOT: one result column per pivoted VALUE — the static
            // schema cannot enumerate them (the checker keeps only the
            // group-by half). Statically known names match by NAME; a
            // pivot-generated '<value>__|__<agg>' column inherits its
            // aggregate TEMPLATE's type (schema.dynamicColumns(), the
            // engine-lite DynamicPivotColumn design) — the name is
            // data-dependent, the type is not. SQL-type derivation remains
            // only for schemas rebuilt downstream of the pivot, where the
            // templates no longer ride.
            for (int i = 1; i <= n; i++) {
                // ENGINE presentation: a separator-bearing pivot name
                // presents quote-wrapped (Type.RelationType
                // .presentPivotName — the physical SQL column stays bare)
                String name = Type.RelationType.presentPivotName(
                        rs.getMetaData().getColumnName(i));
                String sqlType = rs.getMetaData().getColumnTypeName(i);
                columns.add(new Column(name, pivotColumnType(schema, name, sqlType)));
            }
        } else {
            throw new IllegalStateException("result has " + n + " columns but the typed"
                    + " schema has " + schema.columns().size() + " — plan/schema mismatch");
        }
        return columns;
    }

    /**
     * Shape ONE JDBC row into 1..k result rows (shared by the materialized
     * and streaming tabular paths — the shaping rules must never diverge).
     */
    private static List<Row> shapeRow(ResultSet rs, int n, SqlQuery plan,
            com.legend.sql.dialect.SqlDialect dialect, Type.RelationType schema,
            List<Column> columns) throws SQLException {
        List<Object> cells = new ArrayList<>(n);
        boolean lateBound = schema.isLateBound();
        for (int i = 1; i <= n; i++) {
            Object cell = unwrap(fetch(rs, i, sqlTypeOf(plan, i - 1, lateBound)),
                    sqlTypeOf(plan, i - 1, lateBound), dialect);
            // E2 (JAVA_EVICTION_PLAN): the host-side row explosion is
            // DEAD — the scalar-stream projection explodes IN SQL
            // (LEFT LATERAL UNNEST at project lowering; probe: zero
            // firings on the full sweep) and the declared to-one slot
            // matches the emitted one. A list cell in a primitive
            // schema slot is a lowering defect, never repaired here.
            if ((cell instanceof List<?> || cell instanceof java.sql.Array)
                    && columns.get(i - 1).pureType()
                            instanceof Type.Primitive) {
                throw new IllegalStateException("a many-valued cell"
                        + " reached a scalar TDS slot ('"
                        + columns.get(i - 1).name()
                        + "') — the lowering must explode scalar"
                        + " streams in SQL (E2)");
            }
            cells.add(cell);
        }
        return List.of(new Row(cells));
    }

    private static com.legend.sql.@com.legend.base.Nullable SqlType sqlTypeOf(SqlQuery plan, int index) {
        return sqlTypeOf(plan, index, false);
    }

    /** {@code lateBound} is threaded from the TYPED schema
     * ({@code schema.isLateBound()} — P3-2's explicit gate, never an
     * outputs-emptiness proxy): an undemanded raw grid's zero-output
     * star-select has no static SQL type per column, and the wire KIND
     * drives decode. */
    private static com.legend.sql.@com.legend.base.Nullable SqlType sqlTypeOf(SqlQuery plan, int index,
            boolean lateBound) {
        List<OutputCol> outputs = plan.outputs();
        if (index >= outputs.size()) {
            if (lateBound) {
                return null; // late-bound grid column: the wire kind decides
            }
            if (hasPivot(plan)) {
                return null; // dynamic pivot column: no static SQL type exists
            }
            throw new IllegalStateException("result column " + index
                    + " has no plan output — plan/result mismatch");
        }
        return outputs.get(index).type();
    }

    /** Whether the plan's source tree contains a (dynamic-columned) PIVOT. */
    /** Package-visible for the wire census (D2): a pivot-bearing plan's
     * result columns are data-dependent, so a probe shape mismatch
     * there is the pivot's contract, not an unknown. */
    static boolean hasPivot(SqlQuery plan) {
        return plan instanceof com.legend.sql.SqlSelect s && hasPivot(s.from());
    }

    private static boolean hasPivot(com.legend.sql.SqlSource src) {
        return switch (src) {
            case null -> false;
            case com.legend.sql.SqlSource.Pivot p -> true;
            case com.legend.sql.SqlSource.Subselect sub -> hasPivot(sub.inner());
            case com.legend.sql.SqlSource.Join j -> hasPivot(j.left()) || hasPivot(j.right());
            default -> false;
        };
    }
    /**
     * The Pure type of one column of a pivot result. Static (group-by) names
     * match the schema; a dynamic {@code <value>__|__<agg>} name inherits its
     * aggregate template's type. A suffixed name that matches NO template while
     * templates are present is a naming-contract bug — loud, never guessed.
     */
    private static Type pivotColumnType(Type.RelationType schema, String name, String sqlType) {
        // the ONE matching rule lives on Type.RelationType (the
        // deferred-TDS resolver shares it); the SQL-type decode is THIS
        // caller's fallback — schemas rebuilt downstream of the pivot,
        // where the templates no longer ride
        Type t = schema.pivotColumnType(name);
        return t != null ? t : pureOfSqlType(sqlType);
    }

    /** The Pure primitive a DYNAMIC (pivot-generated) SQL column carries.
     * Every known name is EXPLICIT — an unrecognized SQL type is a gap in
     * this table, not a String (audit 15: the silent String default
     * corrupted result typing invisibly). */
    public static Type pureOfSqlType(String sqlType) {
        Type t = pureOfSqlTypeOrNull(sqlType);
        if (t == null) {
            throw new IllegalStateException(
                    "no Pure primitive mapped for SQL type '" + sqlType
                    + "' (pivot-generated column) — add it to"
                    + " Executor.pureOfSqlType");
        }
        return t;
    }

    /** The LOOKUP variant — null for an unmapped SQL type (the
     * late-bound grid stamp's fallback-to-Any door; the pivot path
     * keeps the loud variant above). */
    public static @com.legend.base.Nullable Type pureOfSqlTypeOrNull(
            String sqlType) {
        // V1.9 (Phase 8): the parameter suffix strips ONCE
        // ('DECIMAL(38,9)' -> 'DECIMAL'), then the table is EXACT-match
        // — no prefix matching (the audited startsWith arms could never
        // say what they excluded).
        String t = sqlType.toUpperCase();
        int paren = t.indexOf('(');
        if (paren > 0) {
            t = t.substring(0, paren).strip();
        }
        return switch (t) {
            case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT" ->
                    Type.Primitive.INTEGER;
            case "FLOAT", "DOUBLE", "REAL" -> Type.Primitive.FLOAT;
            case "BOOLEAN" -> Type.Primitive.BOOLEAN;
            case "DATE" -> Type.Primitive.STRICT_DATE;
            case "TIMESTAMP" -> Type.Primitive.DATE_TIME;
            case "DECIMAL", "NUMERIC" -> Type.Primitive.DECIMAL;
            case "VARCHAR", "CHAR", "TEXT", "STRING", "BPCHAR" ->
                    Type.Primitive.STRING;
            default -> null;
        };
    }

    /** Expand row-struct columns (navigate slots) to their prefixed flat set. */
    private static Type.RelationType flattenStructColumns(Type.RelationType schema) {
        if (schema.columns().stream().noneMatch(c ->
                c.type() instanceof Type.RelationType)) {
            return schema;
        }
        List<Type.Column> flat = new ArrayList<>();
        for (Type.Column c : schema.columns()) {
            // a slot column types the target's bare row-struct
            Type.RelationType sub =
                    c.type() instanceof Type.RelationType r0 ? r0 : null;
            if (sub != null) {
                for (Type.Column sc : sub.columns()) {
                    flat.add(new Type.Column(c.name() + "_" + sc.name(),
                            sc.type(), sc.multiplicity()));
                }
            } else {
                flat.add(c);
            }
        }
        return new Type.RelationType(flat);
    }

}
