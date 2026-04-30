package com.gs.legend.plan.lowering.relation;

import com.gs.legend.ast.TdsLiteral;
import com.gs.legend.compiler.PureCompileException;
import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.typed.TypedGetAll;
import com.gs.legend.compiler.typed.TypedTableReference;
import com.gs.legend.compiler.typed.TypedTdsLiteral;
import com.gs.legend.model.m3.Type;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * Root relation sources: mapped-class root ({@link TypedGetAll}), raw physical
 * table ({@link TypedTableReference}), and inline TDS literals ({@link TypedTdsLiteral}).
 */
public final class SourceLowering {
    private SourceLowering() {}

    // ---------------- TypedTableReference ----------------

    /**
     * Raw physical table reference: {@code #>{Db.TABLE}#}. The HIR node carries
     * the resolved physical table name; the schema lives on {@code info()}.
     */
    public static SqlRelation lower(TypedTableReference n, LoweringContext ctx) {
        String alias = ctx.nextAlias();
        return new SqlRelation.TableRef(null, n.tableName(), alias,
                outputsFromSchema(n.info().schema()));
    }

    // ---------------- TypedSourceUrl ----------------

    /**
     * External data-source root: {@code sourceUrl(<url>)}. The dialect renders
     * the URL into a complete subquery (scheme-dependent — {@code data:} → JSON
     * unnest, {@code file:} → {@code read_json_objects}, etc.). Everything
     * outside this method is dialect-agnostic; URL-scheme dispatch lives in
     * {@link com.gs.legend.sqlgen.SQLDialect#renderSourceUrl}.
     */
    public static SqlRelation lower(com.gs.legend.compiler.typed.TypedSourceUrl n,
                                    LoweringContext ctx) {
        String alias = ctx.nextAlias();
        return new SqlRelation.SourceUrl(
                n.url(), alias,
                outputsFromSchema(n.info().schema()));
    }

    // ---------------- TypedGetAll ----------------

    /**
     * {@code ClassName.all()} root. Two paths, in priority order:
     * <ol>
     *   <li><strong>Mapping-function body</strong> — recursively lower
     *       {@code mappingFn.body().hir()}. This is the typed HIR equivalent of the
     *       legacy {@code store.sourceSpec()} chain (source terminal + filter + distinct
     *       + joins) and covers all relational mappings (physical-table-backed
     *       and external-URL-backed via {@link com.gs.legend.compiler.typed.TypedSourceUrl})
     *       plus M2M (model-to-model) mappings.</li>
     *   <li><strong>Identity fallback</strong> — plain {@code SELECT *} from
     *       {@code store.tableName()} with a fresh alias.</li>
     * </ol>
     */
    public static SqlRelation lower(TypedGetAll n, LoweringContext ctx) {
        // Missing mapping is a back-end / link-time error — surfaced here at
        // the use site rather than at type-check (front-end). TypeChecker
        // tolerates absence so a query can be well-typed even when a class
        // it references has no mapping in the active scope; we draw the line
        // at the moment lowering actually needs the materialization.
        var mappingFn = ctx.compiledMappingFunction(n.className()).orElse(null);
        if (mappingFn == null) {
            throw new PureCompileException(
                    "No mapping in scope for class '" + n.className() + "' — "
                            + "required to lower getAll. Add a mapping for this class to the active runtime.");
        }

        StoreResolution store = ctx.storeFor(n);
        if (store == null) {
            throw new PureCompileException(
                    "getAll: no StoreResolution for class " + n.className()
                            + ". MappingResolver must stamp every TypedGetAll.");
        }

        // Mapping-function body carries the compiled source-relation HIR — the
        // synth source terminal is either a {@link TypedTableReference} (physical
        // tables) or a {@link com.gs.legend.compiler.typed.TypedSourceUrl}
        // (external URL-backed classes, e.g. {@code JsonModelConnection}), with
        // optional filter/distinct/joins/extends layered on top. Recurse to
        // lower it; MappingResolver has already stamped StoreResolutions on
        // the inner relational nodes.
        if (mappingFn.body() != null) {
            var body = mappingFn.body().hir();
            return Lowerer.lowerRelation(body, ctx);
        }

        // Identity fallback: bare SELECT * FROM <table>.
        if (store.tableName() == null) {
            throw new PureCompileException(
                    "getAll: StoreResolution for " + n.className() + " has no tableName and no mappingFn body.");
        }
        String alias = ctx.nextAlias();
        return new SqlRelation.TableRef(null, store.tableName(), alias,
                outputsFromSchema(n.info().schema()));
    }

    // ---------------- TypedTdsLiteral ----------------

    /**
     * Inline {@code #TDS ... #} literal. Compiles to a {@code VALUES (...)} table
     * with one row per TDS row. Empty TDS literals compile to a single all-null
     * row gated by {@code WHERE 1 = 0} so the column schema survives even when
     * the row set is empty (a bare {@code VALUES ()} is invalid SQL).
     */
    public static SqlRelation lower(TypedTdsLiteral n, LoweringContext ctx) {
        TdsLiteral tds = n.data();
        List<TdsLiteral.TdsColumn> columns = tds.columns();
        List<String> columnNames = columns.stream().map(TdsLiteral.TdsColumn::name).toList();
        List<SqlRelation.OutputCol> outputs = outputsFromSchema(n.info().schema());
        String alias = ctx.nextAlias();

        if (tds.rows().isEmpty()) {
            // Preserve schema via one all-null row; caller wraps in WHERE 1=0.
            // SqlRelation.Values has no predicate slot; we emit a Filter wrapping it.
            List<SqlExpr> nullRow = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) nullRow.add(new SqlExpr.NullLiteral());
            var values = new SqlRelation.Values(List.of(nullRow), columnNames, alias, outputs);
            return new SqlRelation.Filter(values,
                    new SqlExpr.Binary(
                            new SqlExpr.NumericLiteral(1), "=", new SqlExpr.NumericLiteral(0)));
        }

        List<List<SqlExpr>> rows = new ArrayList<>(tds.rows().size());
        for (List<Object> row : tds.rows()) {
            List<SqlExpr> cells = new ArrayList<>(row.size());
            for (int i = 0; i < row.size(); i++) {
                Object val = row.get(i);
                TdsLiteral.TdsColumn col = i < columns.size() ? columns.get(i) : null;
                cells.add(renderTdsCell(val, col));
            }
            rows.add(cells);
        }
        return new SqlRelation.Values(rows, columnNames, alias, outputs);
    }

    // ---------------- Helpers ----------------

    private static SqlExpr renderTdsCell(Object val, TdsLiteral.TdsColumn col) {
        SqlExpr cell;
        String colType = col != null ? col.type() : null;
        if (val instanceof String sv && colType != null
                && ("StrictDate".equals(colType) || "Date".equals(colType))) {
            cell = new SqlExpr.DateLiteral(stripPercent(sv));
        } else if (val instanceof String sv && "DateTime".equals(colType)) {
            cell = new SqlExpr.TimestampLiteral(stripPercent(sv));
        } else if (val == null) {
            cell = new SqlExpr.NullLiteral();
        } else if (val instanceof Boolean b) {
            cell = new SqlExpr.BoolLiteral(b);
        } else if (val instanceof Number num) {
            cell = new SqlExpr.NumericLiteral(num);
        } else {
            cell = new SqlExpr.StringLiteral(val.toString());
        }
        // Variant columns want a dialect-specific variant wrapper.
        if (col != null && col.isVariant()) cell = new SqlExpr.VariantLiteral(cell);
        return cell;
    }

    private static String stripPercent(String s) {
        return s.startsWith("%") ? s.substring(1) : s;
    }

    /** Build the MIR output-column list from a {@link Type.Schema}. */
    private static List<SqlRelation.OutputCol> outputsFromSchema(Type.Schema schema) {
        if (schema == null) return List.of();
        List<SqlRelation.OutputCol> out = new ArrayList<>(schema.columns().size());
        for (var e : schema.columns().entrySet()) {
            out.add(new SqlRelation.OutputCol(e.getKey(), e.getValue()));
        }
        return out;
    }
}
