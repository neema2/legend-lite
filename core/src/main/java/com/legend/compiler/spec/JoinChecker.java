package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedJoinSlot;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.ColSpec;
import com.legend.model.spec.AppliedProperty;
import com.legend.model.spec.CString;
import com.legend.model.spec.EnumValue;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;

import java.util.List;

import java.util.Optional;

/**
 * Relation {@code join} (engine {@code JoinChecker}) &mdash; checked generically
 * against {@code join<T,V>(rel1, rel2, joinKind:JoinKind[1],
 * f:{T[1],V[1]->Boolean[1]}):Relation<T+V>[1]}: the condition lambda sees one row
 * of each side; the output schema is the union {@code T+V} (a name collision is
 * a loud error &mdash; real legend-pure's rule).
 *
 * <p>The 5-argument {@code prefix} overload exists exactly to resolve such
 * collisions: EVERY right-side column is renamed {@code prefix + name} in the
 * output (engine's behavior). The prefix renaming is beyond the signature's
 * {@code T+V} algebra, so that path validates each argument against the
 * registered 5-arity signature and computes the prefixed union bespoke.
 */
final class JoinChecker {

    private JoinChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        TypedSpec shared = sharedKeyLegacyJoin(t, af, env);
        if (shared != null) {
            return shared;
        }
        af = tdsLegacyToModern(af);
        if (af.parameters().size() == 3) {
            return slot(t, af, env);
        }
        if (af.parameters().size() == 5) {
            return withPrefix(t, af, env);
        }
        Application a = t.checkGeneric(af, env);
        if (a.args().size() != 4 || !(a.args().get(2) instanceof TypedEnumValue kind)
                || !(a.args().get(3) instanceof TypedLambda cond)) {
            throw new TypeInferenceException(
                    "join expects (rel1, rel2, JoinKind, {t,v|cond} [, 'prefix'])");
        }
        return new TypedJoin(a.args().get(0), a.args().get(1), kind, cond, Optional.empty(), a.out());
    }

    /**
     * Desugar the legacy TDS join spellings: the {@code JoinType} enum maps
     * to {@code JoinKind} (INNER/LEFT_OUTER/RIGHT_OUTER/FULL_OUTER →
     * INNER/LEFT/RIGHT/FULL), and the string-pair condition
     * {@code join(tds2, kind, 'lhsCol', 'rhsCol')} becomes the modern
     * condition lambda {@code {a,b|$a.lhsCol == $b.rhsCol}}.
     */
    private static AppliedFunction tdsLegacyToModern(AppliedFunction af) {
        List<ValueSpecification> ps = af.parameters();
        if (ps.size() < 3 || !(ps.get(2) instanceof EnumValue kind)
                // EXACT identification: the FQN, or the bare import
                // spelling — a suffix match would also capture a user enum
                // named ...::JoinType
                || !(kind.fullPath().equals("meta::relational::metamodel::join::JoinType")
                        || kind.fullPath().equals("JoinType"))) {
            return af;
        }
        String mapped = switch (kind.value()) {
            case "INNER" -> "INNER";
            case "LEFT_OUTER" -> "LEFT";
            case "RIGHT_OUTER" -> "RIGHT";
            case "FULL_OUTER" -> "FULL";
            default -> throw new TypeInferenceException(
                    "unknown JoinType value '" + kind.value() + "'");
        };
        EnumValue joinKind = new EnumValue(
                "meta::pure::functions::relation::JoinKind", mapped);
        List<String> lhsCols = columnNames(ps.size() >= 4 ? ps.get(3) : null);
        List<String> rhsCols = ps.size() == 5 ? columnNames(ps.get(4)) : lhsCols;
        if (lhsCols != null && rhsCols != null && lhsCols.size() == rhsCols.size()
                && (ps.size() == 4 || ps.size() == 5)) {
            Variable a = new Variable("a");
            Variable b = new Variable("b");
            ValueSpecification cond = null;
            for (int i = 0; i < lhsCols.size(); i++) {
                ValueSpecification eq = new AppliedFunction("equal", List.of(
                        new AppliedProperty(a, lhsCols.get(i)),
                        new AppliedProperty(b, rhsCols.get(i))));
                cond = cond == null ? eq : new AppliedFunction("and", List.of(cond, eq));
            }
            LambdaFunction condLam = new LambdaFunction(List.of(a, b), List.of(cond));
            return new AppliedFunction(af.function(),
                    List.of(ps.get(0), ps.get(1), joinKind, condLam));
        }
        List<ValueSpecification> out = new java.util.ArrayList<>(ps);
        out.set(2, joinKind);
        return new AppliedFunction(af.function(), out);
    }

    /**
     * The legacy TDS SHARED-KEY join {@code join(tds2, JoinType, ['id'])}:
     * both sides carry the key columns under the SAME names, and the engine
     * keeps exactly ONE copy in the output. The modern {@code T+V} algebra
     * would (rightly) reject the collision, so: rename the right side's keys
     * to synthetic names, run the modern join on the renamed condition, then
     * SELECT the synthetic copies away — the whole dedup is visible in the
     * typed tree, no schema-algebra bypass.
     */
    private static TypedSpec sharedKeyLegacyJoin(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> ps = af.parameters();
        if (ps.size() != 4 || !(ps.get(2) instanceof EnumValue kind)
                || !(kind.fullPath().equals("meta::relational::metamodel::join::JoinType")
                        || kind.fullPath().equals("JoinType"))) {
            return null;
        }
        List<String> keys = columnNames(ps.get(3));
        if (keys == null) {
            return null;
        }
        // WHICH side's key values survive is join-type-dependent (engine
        // tds.pure requiredLeftCols/requiredRightCols + the RightOuter
        // golden: fID carries the RIGHT side's values, no TDSNull): the
        // OUTER-PRESERVED side keeps its keys — RIGHT_OUTER renames the
        // LEFT copies away; everything else keeps the left.
        boolean rightKeeps = kind.value().equals("RIGHT_OUTER");
        // collision-safe synthetic prefix (ordinal bump against BOTH
        // sides' columns — a real __jk_* column must survive untouched)
        java.util.Set<String> taken = new java.util.LinkedHashSet<>();
        for (ValueSpecification side : List.of(ps.get(0), ps.get(1))) {
            if (t.synth(side, env).info().type()
                    instanceof Type.RelationType srt) {
                srt.columns().forEach(c -> taken.add(c.name()));
            }
        }
        String jkPrefix = "__jk_";
        int ordinal = 2;
        while (hasJkCollision(jkPrefix, taken)) {
            jkPrefix = "__jk" + ordinal++ + "_";
        }
        ValueSpecification left = ps.get(0);
        ValueSpecification right = ps.get(1);
        Variable a = new Variable("a");
        Variable b = new Variable("b");
        ValueSpecification cond = null;
        List<String> synthetic = new java.util.ArrayList<>(keys.size());
        for (String k : keys) {
            String s = jkPrefix + k;
            synthetic.add(s);
            ValueSpecification eq;
            if (rightKeeps) {
                left = new AppliedFunction("rename",
                        List.of(left, new ColSpec(k), new ColSpec(s)));
                eq = new AppliedFunction("equal", List.of(
                        new AppliedProperty(a, s), new AppliedProperty(b, k)));
            } else {
                right = new AppliedFunction("rename",
                        List.of(right, new ColSpec(k), new ColSpec(s)));
                eq = new AppliedFunction("equal", List.of(
                        new AppliedProperty(a, k), new AppliedProperty(b, s)));
            }
            cond = cond == null ? eq : new AppliedFunction("and", List.of(cond, eq));
        }
        AppliedFunction modern = new AppliedFunction("join", List.of(
                left, right,
                new EnumValue("meta::pure::functions::relation::JoinKind",
                        joinKindNameOf(kind)),
                new LambdaFunction(List.of(a, b), List.of(cond))));
        TypedSpec joined = check(t, modern, env);
        Type.RelationType rt = (Type.RelationType) joined.info().type();
        List<Type.Column> kept = rt.columns().stream()
                .filter(c -> !synthetic.contains(c.name())).toList();
        return new com.legend.compiler.spec.typed.TypedSelect(joined,
                kept.stream().map(Type.Column::name).toList(),
                new ExprType(new Type.RelationType(kept),
                        joined.info().multiplicity()));
    }

    private static boolean hasJkCollision(String prefix, java.util.Set<String> taken) {
        for (String t : taken) {
            if (t.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static String joinKindNameOf(EnumValue kind) {
        return switch (kind.value()) {
            case "INNER" -> "INNER";
            case "LEFT_OUTER" -> "LEFT";
            case "RIGHT_OUTER" -> "RIGHT";
            case "FULL_OUTER" -> "FULL";
            default -> throw new TypeInferenceException(
                    "unknown JoinType value '" + kind.value() + "'");
        };
    }

    /** String or [strings] column-name argument of the legacy TDS join, else null. */
    private static List<String> columnNames(ValueSpecification v) {
        if (v instanceof CString c) {
            return List.of(c.value());
        }
        if (v instanceof com.legend.model.spec.PureCollection pc
                && !pc.values().isEmpty()
                && pc.values().stream().allMatch(x -> x instanceof CString)) {
            return pc.values().stream().map(x -> ((CString) x).value()).toList();
        }
        return null;
    }

    /**
     * The pipeline SLOT join {@code rel->join(~alias: #>{db.T}#, {s,t|cond})}
     * (lite; the mapping normalizer's join-chain step — no real pure
     * counterpart). Mirrors {@code NavigateChecker.preMap}: validate against
     * the registered lite signature; the thunk's table reference binds
     * {@code T}, the cond lambda types over one row of each side, and only
     * {@code Z} — the sub-row column {@code (alias:TargetRow[1])} — is
     * bespoke (the sub-row is to-one per output row, which the signature's
     * algebra cannot spell).
     */
    private static TypedSpec slot(Typer t, AppliedFunction af, Env env) {
        TypedFunction sig = t.model().findFunction(CoreFn.JOIN.parseName()).stream()
                .filter(c -> c.parameters().size() == 3)
                .findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "no 3-argument (slot) join overload is registered"));
        if (!(af.parameters().get(1) instanceof ColSpec cs)
                || cs.function1() == null || !cs.function1().parameters().isEmpty()
                || !(af.parameters().get(2) instanceof LambdaFunction condLam)) {
            throw new TypeInferenceException(
                    "join expects (rel1, rel2, JoinKind, {t,v|cond} [, 'prefix']) — or"
                            + " the pipeline slot form (rel, ~alias: <table>, {s,t|cond})");
        }
        Bindings b = new Bindings();
        TypedSpec source = t.synth(af.parameters().get(0), env);
        t.kernel().unify(sig.parameters().get(0).type(), source.info().type(), b);
        t.kernel().unifyMult(sig.parameters().get(0).multiplicity(),
                source.info().multiplicity(), source.info().type(), b);

        // The slot thunk {->Relation<T>[1]} — typing it binds T to the target row.
        Type.GenericType slotParam = (Type.GenericType) sig.parameters().get(1).type();
        TypedLambda thunk = (TypedLambda) t.typeLambda(cs.function1(),
                slotParam.arguments().get(0), b, env);
        Type targetRow = ((Type.FunctionType) thunk.info().type()).result().type();
        if (!(targetRow instanceof Type.RelationType)) {
            throw new TypeInferenceException(
                    "join slot target must be a relation (a table reference), got "
                            + targetRow.typeName());
        }
        // Z = (alias : TargetRow[1]) — the joined sub-row column.
        String zVar = ((Type.TypeVar) slotParam.arguments().get(1)).name();
        b.bindType(zVar, new Type.RelationType(List.of(
                new Type.Column(cs.name(), targetRow, Multiplicity.Bounded.ONE))));
        TypedLambda cond = (TypedLambda) t.typeLambda(condLam, sig.parameters().get(2).type(), b, env);

        ExprType out = t.kernel().resolveOutput(sig.returnType(), sig.returnMultiplicity(), b);
        return new TypedJoinSlot(source, cs.name(), thunk.body().get(0), cond, out);
    }

    private static TypedSpec withPrefix(Typer t, AppliedFunction af, Env env) {
        TypedFunction sig = t.model().findFunction(CoreFn.JOIN.parseName()).stream()
                .filter(c -> c.parameters().size() == 5)
                .findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "no 5-argument join overload is registered"));

        // Validate every argument against the registered signature (never bypassed);
        // the condition lambda types against the signature's function parameter with
        // T and V already bound from the two sides.
        Bindings b = new Bindings();
        TypedSpec left = Checkers.unifiedArg(t, sig, 0, af, b, env);
        TypedSpec right = Checkers.unifiedArg(t, sig, 1, af, b, env);
        TypedSpec kindArg = Checkers.unifiedArg(t, sig, 2, af, b, env);
        if (!(af.parameters().get(3) instanceof LambdaFunction condLam)
                || !(kindArg instanceof TypedEnumValue kind)) {
            throw new TypeInferenceException(
                    "join expects (rel1, rel2, JoinKind, {t,v|cond}, 'prefix')");
        }
        TypedLambda cond = (TypedLambda) t.typeLambda(condLam, sig.parameters().get(3).type(), b, env);
        String prefix = Checkers.stringLiteralArg(t, af, 4, env, "join prefix");
        // The prefix SEPARATES with an underscore ('r' -> r_id, corpus
        // semantics; no real-pure counterpart exists). A caller-supplied
        // trailing underscore is already the separator.
        if (!prefix.endsWith("_")) {
            prefix = prefix + "_";
        }

        // Bespoke output: left columns + EVERY right column renamed prefix+name.
        Type.RelationType schema = Checkers.prefixedUnion(left, right, prefix, c -> true);
        return new TypedJoin(left, right, kind, cond, Optional.of(prefix),
                new ExprType(schema, sig.returnMultiplicity()));
    }
}
