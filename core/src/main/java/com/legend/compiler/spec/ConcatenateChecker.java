package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedRename;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Relation {@code concatenate} (engine {@code ConcatenateChecker}, SQL
 * {@code UNION ALL}).
 *
 * <p>POSITIONAL rule (user ruling 2026-09-05, batch 72a): two relation
 * operands unite BY POSITION — same arity, position-wise compatible column
 * types, the LEFT operand's names — exactly what the engine's relational
 * lowering does ({@code processConcatenate}, pureToSQLQuery.pure:2709:
 * the arms align into one {@code UNION ALL}; SQL names the union by its
 * first select). The in-memory {@code tds.pure:483-487} name assert is
 * that implementation's own runtime check, not the type rule of the lane
 * this compiles to. A right operand whose names differ is spelled as a
 * {@link TypedRename} onto the left's names, so the lowering and every
 * downstream schema read stay name-aligned. The relation rule is applied
 * here, never by loosening the generic {@code T}-binding (the
 * InferenceKernel wall stays for every other same-{@code T} signature).
 *
 * <p>The COLLECTION overload ({@code set1:T[*], set2:T[*]}) is a plain
 * value operation (SQL list concat), typed generically as before.
 */
final class ConcatenateChecker {

    private ConcatenateChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() == 2
                && af.parameters().stream().noneMatch(Typer::deferredArg)) {
            // each argument types exactly ONCE (a second synth would
            // re-register typer state); the typed pair then picks the rule
            TypedSpec left = t.synth(af.parameters().get(0), env);
            TypedSpec right = t.synth(af.parameters().get(1), env);
            Type.RelationType ls = Type.relationSchema(left.info().type());
            if (ls != null) {
                // n-ary TDS concatenate (ledger cluster 22): p1->concatenate(
                // [p2, p3, p4]) — a relation-typed collection RHS (its own
                // info reads as the element relation at [*]) folds into a
                // LEFT-ASSOCIATIVE chain (engine tds.pure:480-496 returns
                // TabularDataSet[1]). Guard on ALL elements relation-typed so
                // a genuine scalar-collection concatenate is never captured.
                if (right instanceof TypedCollection tc) {
                    if (!tc.elements().isEmpty()
                            && tc.elements().stream().allMatch(e ->
                                    Type.relationSchema(e.info().type()) != null)) {
                        TypedSpec acc = left;
                        for (TypedSpec e : tc.elements()) {
                            acc = positional(acc, e);
                        }
                        return acc;
                    }
                } else if (Type.relationSchema(right.info().type()) != null) {
                    return positional(left, right);
                }
            }
            return generic(t.checkGenericTyped(af, List.of(left, right)));
        }
        return generic(t.checkGeneric(af, env));
    }

    /** The COLLECTION overload (set1:T[*], set2:T[*]) is a plain value
     * operation (SQL list concat), not the relation set-op node. */
    private static TypedSpec generic(Application a) {
        if (!Type.isRelation(a.out().type())) {
            return Typer.emitCall(a.chosen(), a.args(), a.out());
        }
        return new TypedConcatenate(a.args().get(0), a.args().get(1), a.out());
    }

    /** {@code left UNION ALL right} by position: arity and column types
     * must agree slot for slot; the right operand renames onto the left's
     * names when they differ. The result carries the LEFT schema at [1]. */
    private static TypedSpec positional(TypedSpec left, TypedSpec right) {
        Type.RelationType ls = java.util.Objects.requireNonNull(
                Type.relationSchema(left.info().type()));
        Type.RelationType rs = java.util.Objects.requireNonNull(
                Type.relationSchema(right.info().type()));
        if (ls.columns().size() != rs.columns().size()) {
            throw new TypeInferenceException("concatenate: " + ls.columns().size()
                    + " column(s) " + names(ls) + " cannot unite with "
                    + rs.columns().size() + " column(s) " + names(rs)
                    + " (relation concatenate is positional: same arity)");
        }
        List<TypedRename.ColRename> renames = new ArrayList<>();
        for (int i = 0; i < ls.columns().size(); i++) {
            Type.Column lc = ls.columns().get(i);
            Type.Column rc = rs.columns().get(i);
            if (!compatible(lc.type(), rc.type())) {
                throw new TypeInferenceException("concatenate: column " + (i + 1)
                        + " ('" + lc.name() + "' " + lc.type().typeName()
                        + ") cannot unite with '" + rc.name() + "' "
                        + rc.type().typeName() + " (position-wise types must agree)");
            }
            if (!lc.name().equals(rc.name())) {
                renames.add(new TypedRename.ColRename(rc.name(), lc.name()));
            }
        }
        ExprType one = new ExprType(left.info().type(), Multiplicity.Bounded.ONE);
        TypedSpec aligned = renames.isEmpty() ? right : new TypedRename(right, renames, one);
        return new TypedConcatenate(left, aligned, one);
    }

    /** Same type, or two numerics (the database widens the union slot). */
    private static boolean compatible(Type a, Type b) {
        if (a.equals(b)) {
            return true;
        }
        return a instanceof Type.Primitive pa && b instanceof Type.Primitive pb
                && pa.isNumeric() && pb.isNumeric();
    }

    private static List<String> names(Type.RelationType rt) {
        return rt.columns().stream().map(Type.Column::name).toList();
    }
}
