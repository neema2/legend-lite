// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;

import java.util.ArrayList;
import java.util.List;

/**
 * The engine's temp-table IN protocol
 * ({@code processInOperation.pure}) over the PLAN-TEXT surface: an
 * {@code in} whose collection exceeds the connection's threshold — or
 * carries a many-valued plan parameter of unknown size — becomes the
 * block plan: {@code RelationalBlockExecutionNode} wrapping
 * Allocation / CreateAndPopulateTempTable /
 * FreeMarkerConditionalExecutionNode ahead of the Relational node,
 * whose SQL splices the temp-table select (over threshold) or the
 * {@code ${inFilterClause_X}} wrapper variable (conditional).
 *
 * <p>Thresholds are the engine's
 * {@code getCollectionThresholdLimitForDatabaseType}: TEST connections
 * 50; DB2 32767; other real connections have NO limit (the H2
 * timezone golden pins protocol-off for a real H2 connection).
 */
public final class InProtocol {

    private InProtocol() {
    }

    /** ENGINE-ORDINAL PARITY: the engine's transform() hands each
     * matched {@code in} a uniqueId from its metamodel walk; for the
     * canonical single-site mixed-collection shape that ordinal is 4
     * (testExecutionPlanGenerationForInWithVarAndConstantInputs pins
     * {@code tempVarForIn_4}). Sites with any literal member number
     * from here; all-parameter sites name by their parameters. */
    private static final int MIXED_SUFFIX_BASE = 4;

    public record Site(String suffix, boolean conditional,
            List<SqlExpr.PlanParam> vars, List<SqlExpr> constants,
            String tempPrefix) {
        /** The engine's procesTempTableName — DB-SPECIFIC: DB2 prefixes
         * {@code SESSION.} (its temp-table schema convention). */
        String tempTable() {
            return tempPrefix + suffix;
        }

        String tempVar() {
            return "tempVarForIn_" + suffix;
        }

        String wrapper() {
            return "inFilterClause_" + suffix;
        }
    }

    public record Result(SqlQuery plan, List<Site> sites) {
    }

    /** Scan-and-rewrite; null when no site triggers. */
    public static @com.legend.base.Nullable Result apply(SqlQuery plan,
            @com.legend.base.Nullable Integer threshold,
            @com.legend.base.Nullable String dbType) {
        if (threshold == null) {
            return null;
        }
        String tempPrefix = "DB2".equals(dbType)
                ? "SESSION.tempTableForIn_" : "tempTableForIn_";
        List<Site> sites = new ArrayList<>();
        int[] mixedId = {MIXED_SUFFIX_BASE};
        SqlRewriter rw = new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                if (!(e instanceof SqlExpr.Call c) || c.fn() != SqlFn.IN
                        || c.args().size() < 2) {
                    return e;
                }
                List<SqlExpr> tail = c.args().subList(1, c.args().size());
                List<SqlExpr.PlanParam> vars = new ArrayList<>();
                List<SqlExpr> constants = new ArrayList<>();
                for (SqlExpr t : tail) {
                    if (t instanceof SqlExpr.PlanParam p) {
                        vars.add(p);
                    } else {
                        constants.add(t);
                    }
                }
                // the Lowerer emits a MANY collection param as the
                // WHOLE in-tail (the renderCollection form); scalar
                // params ride among literals and never trigger alone,
                // and a [0..1] OPTIONAL param is never a collection
                // (the engine requires ZeroMany —
                // testFilterEqualsWithOptionalParameter pins the plain
                // Sequence for optional-param equality filters)
                boolean manyVar = tail.size() == 1 && vars.size() == 1
                        && !vars.get(0).optional();
                boolean overThreshold = tail.size() > threshold;
                if (!overThreshold && !(manyVar && !vars.isEmpty())) {
                    return e;
                }
                String suffix = constants.isEmpty()
                        ? vars.stream().map(SqlExpr.PlanParam::name)
                                .reduce((a, b) -> a + "_" + b).orElseThrow()
                        : String.valueOf(mixedId[0]++);
                Site s = new Site(suffix, !overThreshold, vars, constants,
                        tempPrefix);
                sites.add(s);
                return new SqlExpr.Call(SqlFn.IN, List.of(c.args().get(0),
                        overThreshold
                                ? new SqlExpr.TempTableInSplice(
                                        s.tempTable())
                                : new SqlExpr.PlanParam(s.wrapper(),
                                        SqlExpr.PlanParam.Kind.RAW)));
            }
        };
        SqlQuery rewritten = rw.rewrite(plan);
        return sites.isEmpty() ? null : new Result(rewritten, sites);
    }

    /** The engine threshold rule (see class doc). */
    public static @com.legend.base.Nullable Integer thresholdFor(
            @com.legend.base.Nullable String connName,
            @com.legend.base.Nullable String dbType) {
        if (connName != null
                && connName.startsWith("TestDatabaseConnection")) {
            return 50;
        }
        return "DB2".equals(dbType) ? 32767 : null;
    }

    /** Every site's node texts in emission order — the caller-facing
     * form (a triggered protocol implies a connection spelling: the
     * threshold rule only fires with one). */
    public static List<String> allNodeTexts(Result inp,
            @com.legend.base.Nullable String connName,
            @com.legend.base.Nullable String dbType,
            java.util.function.Function<SqlExpr.PlanParam, String> splice) {
        String conn = java.util.Objects.requireNonNull(connName,
                "in-protocol threshold without a connection spelling");
        Integer th = java.util.Objects.requireNonNull(
                thresholdFor(connName, dbType));
        boolean isTest = conn.startsWith("TestDatabaseConnection");
        List<String> out = new ArrayList<>();
        for (Site site : inp.sites()) {
            out.addAll(nodeTexts(site, conn, isTest, th,
                    site.conditional()
                            ? splice.apply(site.vars().get(0)) : ""));
        }
        return out;
    }

    /** The site's plan-node texts, in emission order (before the
     * Relational node). */
    public static List<String> nodeTexts(Site s, String connName,
            boolean isTest, int threshold, String falseText) {
        List<String> out = new ArrayList<>();
        String colType = "VARCHAR(1024)";   // String collections — the
        // only element kind the goldens pin; others wall at the caller
        String createTemp = PlanText.createAndPopulateTempTable(
                inputVarNames(s), s.tempTable(), colType, connName);
        if (!s.conditional()) {
            if (!s.constants().isEmpty()) {
                out.add(PlanText.allocation(s.tempVar(),
                        "  type = String\n",
                        PlanText.constantBare("String",
                                constantsText(s))));
            }
            out.add(createTemp);
            return out;
        }
        StringBuilder checks = new StringBuilder("(");
        for (SqlExpr.PlanParam v : s.vars()) {
            checks.append("instanceOf(").append(v.name())
                    .append(", \"Stream\")");
            if (isTest) {
                checks.append(" || instanceOf(").append(v.name())
                        .append(", \"StreamingResult\")");
            }
            checks.append(" || ");
        }
        checks.append("((").append(s.vars().stream()
                        .map(v -> "collectionSize(" + v.name() + "![])?number")
                        .reduce((a, b) -> a + " + " + b).orElseThrow())
                .append(s.constants().isEmpty()
                        ? "" : " + " + s.constants().size())
                .append(") > ").append(threshold).append("))");
        String condition = "${" + checks + "?c}";
        String trueBlock = PlanText.sequence("  type = String\n",
                List.of(createTemp, PlanText.constantBare("String",
                        tempSelectSql(s.tempTable()))));
        String falseBlock = PlanText.constantBare("String", falseText);
        out.add(PlanText.allocation(s.wrapper(), "  type = String\n",
                PlanText.freeMarkerConditional(condition, trueBlock,
                        falseBlock)));
        return out;
    }

    private static List<String> inputVarNames(Site s) {
        List<String> names = new ArrayList<>();
        s.vars().forEach(v -> names.add(v.name()));
        if (!s.constants().isEmpty()) {
            names.add(s.tempVar());
        }
        return names;
    }

    /** Constant values spell RAW (no quotes) — the engine prints the
     * Java values of the ConstantExecutionNode list. */
    private static String constantsText(Site s) {
        StringBuilder sb = new StringBuilder();
        for (SqlExpr c : s.constants()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(switch (c) {
                case SqlExpr.StringLit sl -> sl.value();
                case SqlExpr.IntLit il -> String.valueOf(il.value());
                default -> throw new com.legend.error
                        .NotImplementedException("in-protocol constant"
                        + " spelling for "
                        + c.getClass().getSimpleName() + " pending");
            });
        }
        return sb.toString();
    }

    /** The engine's generateTempTableSelectSQLQuery template text. */
    public static String tempSelectSql(String tempTable) {
        String a = tempTable.toLowerCase(java.util.Locale.ROOT) + "_0";
        return "select \"" + a + "\".ColumnForStoringInCollection as"
                + " ColumnForStoringInCollection from " + tempTable
                + " as \"" + a + "\"";
    }
}
