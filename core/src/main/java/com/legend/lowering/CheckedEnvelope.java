// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.ArrayList;
import java.util.List;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import java.util.function.Function;

/**
 * The CHECKED serialize envelope ({@code graphFetchChecked}): each object
 * wraps as {@code {defects: [...], value: obj}} where the defects array
 * collects one engine-shaped defect object per class constraint that does
 * NOT hold. A predicate that evaluates to SQL NULL (a required property
 * absent on the wire) is the SQL analog of the engine's generated
 * checker THROWING mid-evaluation, and produces the engine's
 * catch-branch defect — {@code Unable to evaluate constraint [id]:
 * data not available - check your mappings} (referee:
 * legendJavaPlatformBinding shared/constraints.pure
 * generateConstraintMethod, the NullPointerException catch). Before
 * this arm a NULL predicate produced NO defect — an object violating
 * every constraint reported clean (type audit D102). The engine's
 * defect shape is the dataQuality {@code Defect} serialization —
 * id/externalId/message/enforcementLevel/ruleType/ruleDefinerPath/path.
 */
final class CheckedEnvelope {

    private CheckedEnvelope() {
    }

    /** {@code render} lowers one constraint predicate/message lambda into
     * the envelope base select's terms (the Lowerer's own scalar path). */
    /** A checked CHILD's envelope column on the parent's frame: the
     * child's own {defects, value} (per element for a to-many), evaluated
     * ONCE as a lateral join — value and hoisted defects both read it. */
    /** Install a checked child's select as a LATERAL join of the live frame
     * {@code fr[0]} (CROSS for a to-many — its aggregate always yields one
     * row; LEFT ON TRUE for a to-one, which may yield none) and return the
     * child's envelope column. */
    static SqlExpr.CheckedDefects.Hoist attach(SqlSelect[] fr, SqlSelect childSelect, List<String> path,
            boolean toMany, String alias) {
        fr[0] = fr[0].withFrom(new SqlSource.Join(fr[0].from(),
                new SqlSource.Subselect(childSelect, alias, null),
                toMany ? SqlSource.Join.Kind.CROSS_LATERAL : SqlSource.Join.Kind.LEFT_LATERAL,
                toMany ? null : new SqlExpr.BoolLit(true)));
        return new SqlExpr.CheckedDefects.Hoist(path,
                SqlExpr.Column.of(alias, childSelect.outputs(), "result"), toMany);
    }

    /** A nested child's term in the parent object: a plain scalar subquery,
     * or — for a CHECKED child — its lateral envelope's value, the hoist
     * recorded for {@link #wrap}. */
    static SqlExpr childTerm(SqlSelect[] fr, SqlSelect childSelect,
            TypedSerializeGraph.Child child, List<SqlExpr.CheckedDefects.Hoist> hoists,
            java.util.function.Supplier<String> alias, List<String> prefix) {
        if (child.node().checkedConstraints() == null) {
            return new SqlExpr.ScalarSubquery(childSelect);
        }
        List<String> path = new ArrayList<>(prefix);
        path.add(child.property());
        SqlExpr.CheckedDefects.Hoist h = attach(fr, childSelect, path, child.node().arrayWrap(), alias.get());
        hoists.add(h);
        return new SqlExpr.CheckedChildValue(h.envelope(), h.toMany());
    }

    static SqlExpr wrap(TypedSerializeGraph g, SqlExpr obj,
            Function<TypedFuncCol, SqlExpr> render, List<SqlExpr.CheckedDefects.Hoist> hoists) {
        List<SqlExpr> cases = new ArrayList<>();
        for (var cc : java.util.Objects.requireNonNull(
                g.checkedConstraints(), "wrap() requires checked constraints")) {
            SqlExpr pred = render.apply(cc.predicate());
            SqlExpr msg = render.apply(cc.message());
            cases.add(new SqlExpr.Case(List.of(
                    new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.IS_NULL, pred),
                            // CONSTRAINT_UNEVALUABLE, its constraint's id the operand: the product's defect names the
                            // kind, the edge spells the engine's sentence (D1.1, the orchestrator's Q3 ruling)
                            defect(cc, new SqlExpr.RaiseText(com.legend.sql.RaiseKind.CONSTRAINT_UNEVALUABLE,
                                    List.of(new SqlExpr.StringLit(cc.id()))))),
                    new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.NOT, pred),
                            defect(cc, msg))),
                    new SqlExpr.NullLit(), SqlExpr.Reason.CARRIER));
        }
        SqlExpr own = cases.isEmpty() ? new SqlExpr.ArrayLit(List.of())
                : SqlExpr.Call.of(SqlFn.LIST_FILTER,
                        new SqlExpr.ArrayLit(cases),
                        new SqlExpr.Lambda(List.of("x"),
                                SqlExpr.Call.of(SqlFn.IS_NOT_NULL,
                                        SqlExpr.Column.derived(null, "x"))));
        // checked CHILDREN hoist through the semantic node — spelled by the
        // dialect strategy (CheckedDefectsToLists); absent, the own list stands
        SqlExpr defects = SqlExpr.Call.of(SqlFn.TO_VARIANT,
                hoists.isEmpty() ? own : new SqlExpr.CheckedDefects(own, hoists));
        return new SqlExpr.JsonObject(List.of(
                new SqlExpr.StringLit("defects"), defects,
                new SqlExpr.StringLit("value"), obj));
    }

    /** One engine-shaped defect object with the given message term. */
    private static SqlExpr defect(
            TypedSerializeGraph.CheckedConstraint cc, SqlExpr msg) {
        return new SqlExpr.JsonObject(List.of(
                new SqlExpr.StringLit("id"),
                new SqlExpr.StringLit(cc.id()),
                new SqlExpr.StringLit("externalId"),
                new SqlExpr.NullLit(),
                new SqlExpr.StringLit("message"), msg,
                new SqlExpr.StringLit("enforcementLevel"),
                new SqlExpr.StringLit(cc.level()),
                new SqlExpr.StringLit("ruleType"),
                new SqlExpr.StringLit("ClassConstraint"),
                new SqlExpr.StringLit("ruleDefinerPath"),
                new SqlExpr.StringLit(cc.definerFqn()),
                new SqlExpr.StringLit("path"),
                SqlExpr.Call.of(SqlFn.TO_VARIANT,
                        new SqlExpr.ArrayLit(List.of()))));
    }
}
