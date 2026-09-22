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
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

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
        // the pipeline SLOT form is lite-INTERNAL vocabulary: it exists
        // ONLY under its exact spelling (normalizer emissions); a
        // user-written bare 'join' must never reach it
        boolean liteSlotSpelling = af.function()
                .equals(com.legend.builtin.Pure.Lite.JOIN_SLOT);
        // FQN spellings canonicalize to the parse name up front (the
        // ProjectChecker lesson): rebuilds and generic resolution key on
        // the name, and an FQN finds only its own narrow catalog entry.
        if (af.function().contains("::")) {
            af = new AppliedFunction("join", af.parameters());
        }
        af = resolveLetBoundArgs(af, env);
        TypedSpec shared = sharedKeyLegacyJoin(t, af, env);
        if (shared != null) {
            return shared;
        }
        af = tdsLegacyToModern(af);
        if (af.parameters().size() == 3 && liteSlotSpelling) {
            return slot(t, af, env);
        }
        if (af.parameters().size() == 5) {
            return withPrefix(t, af, env);
        }
        if (af.parameters().size() == 4
                && af.parameters().get(3) instanceof LambdaFunction lf0
                && lf0.parameters().size() == 2) {
            af = sideAgnosticTdsCond(t, af, env, lf0);
        }
        Application a = t.checkGeneric(af, env);
        if (a.args().size() != 4 || !(a.args().get(2) instanceof TypedEnumValue kind)
                || !(a.args().get(3) instanceof TypedLambda cond)) {
            throw new TypeInferenceException(
                    "join expects (rel1, rel2, JoinKind, {t,v|cond} [, 'prefix'])");
        }
        return new TypedJoin(a.args().get(0), a.args().get(1), kind, cond,
                Optional.empty(), null, a.out(), true /* USER lambda */);
    }

    /** The legacy TDS join's KIND through the let-alias channel: the
     * corpus parameterizes {@code let type = JoinType.LEFT_OUTER; ...
     * ->join(tds2, $type, {a,b|...})} (23 tdsJoin tests). The legacy
     * desugars below match a literal EnumValue only, so the let-bound
     * spelling fell to the modern signature with an untyped row
     * parameter and walled "unknown function 'getInteger'".
     * {@link Env#resolveAlias} is the ONE lookup mechanism (bind-once
     * leg); the kind position is read-only structural consumption, so
     * adoption is sound (pure lets are single-assignment). NOT the
     * condition: a let-bound {@code {a:TDSRow[1], b:TDSRow[1]|...}}
     * lambda walls at its OWN let (the declared nominal TDSRow has no
     * columns to read — it only means something against the consuming
     * join's rows), so it is a deferred-kind candidate for the bind-once
     * charter, not an alias chase here (2 tests, named). */
    private static AppliedFunction resolveLetBoundArgs(AppliedFunction af, Env env) {
        List<ValueSpecification> ps = af.parameters();
        List<ValueSpecification> np = new java.util.ArrayList<>(ps);
        boolean changed = false;
        if (ps.size() >= 3 && ps.get(2) instanceof Variable kindVar) {
            ValueSpecification r = env.resolveAlias(kindVar);
            if (r != kindVar && r instanceof EnumValue) {
                np.set(2, r);
                changed = true;
            }
        }
        // a let-bound CONDITION lambda (`let jc = {a:TDSRow[1], b:TDSRow[1]
        // | ...}; ->join(..., $jc)`): the join re-types the lambda's reads
        // against its OWN rows — the alias chase binds the raw lambda here
        if (ps.size() >= 4 && ps.get(3) instanceof Variable condVar) {
            ValueSpecification r = env.resolveAlias(condVar);
            if (r != condVar && r instanceof LambdaFunction) {
                np.set(3, r);
                changed = true;
            }
        }
        return changed ? af.withParameters(np) : af;
    }

    /** ENGINE-LEGACY tolerance: a TDS join condition's {@code get*('col')}
     * reads resolve BY NAME across both rows (the corpus spells
     * {@code {a,b|$a.getInteger('aID') == $b.getInteger('faID')}} with the
     * sides swapped and the engine accepts it — testJoinAfterGroupByAfter-
     * JoinInner). A read whose column is ABSENT on its own side and
     * PRESENT on the other re-points to the other param before typing.
     * Ambiguity (present on both) keeps the spelled side. */
    private static AppliedFunction sideAgnosticTdsCond(Typer t,
            AppliedFunction af, Env env, LambdaFunction lf) {
        TypedSpec l = t.synth(af.parameters().get(0), env);
        TypedSpec r = t.synth(af.parameters().get(1), env);
        Type.RelationType lr = Type.relationSchema(l.info().type());
        Type.RelationType rr = Type.relationSchema(r.info().type());
        if (lr == null || rr == null) {
            return af;
        }
        java.util.Set<String> lc = new java.util.LinkedHashSet<>();
        lr.columns().forEach(c -> lc.add(c.name()));
        java.util.Set<String> rc = new java.util.LinkedHashSet<>();
        rr.columns().forEach(c -> rc.add(c.name()));
        String pa = lf.parameters().get(0).name();
        String pb = lf.parameters().get(1).name();
        ValueSpecification body = swapMisplacedReads(
                lf.body().get(lf.body().size() - 1), pa, pb, lc, rc);
        if (body == lf.body().get(lf.body().size() - 1)) {
            return af;
        }
        java.util.List<ValueSpecification> nb =
                new java.util.ArrayList<>(lf.body());
        nb.set(nb.size() - 1, body);
        java.util.List<ValueSpecification> np =
                new java.util.ArrayList<>(af.parameters());
        np.set(3, new LambdaFunction(lf.parameters(), nb));
        return af.withParameters(np);
    }

    private static ValueSpecification swapMisplacedReads(ValueSpecification n,
            String pa, String pb, java.util.Set<String> lc,
            java.util.Set<String> rc) {
        if (n instanceof AppliedFunction gf
                && com.legend.builtin.NativeFn.RowGetter.of(gf.function()).isPresent()
                && gf.parameters().size() == 2
                && gf.parameters().get(0) instanceof Variable v
                && gf.parameters().get(1) instanceof CString col) {
            boolean onA = v.name().equals(pa);
            boolean onB = v.name().equals(pb);
            if (onA && !lc.contains(col.value()) && rc.contains(col.value())) {
                return gf.withParameters(java.util.List.of(
                        new Variable(pb), col));
            }
            if (onB && !rc.contains(col.value()) && lc.contains(col.value())) {
                return gf.withParameters(java.util.List.of(
                        new Variable(pa), col));
            }
            return n;
        }
        if (n instanceof AppliedFunction fn2) {
            boolean changed = false;
            java.util.List<ValueSpecification> args = new java.util.ArrayList<>();
            for (ValueSpecification c : fn2.parameters()) {
                ValueSpecification c2 = swapMisplacedReads(c, pa, pb, lc, rc);
                changed |= c2 != c;
                args.add(c2);
            }
            return changed ? fn2.withParameters(args) : n;
        }
        if (n instanceof AppliedProperty ap) {
            ValueSpecification rcv = swapMisplacedReads(ap.receiver(), pa, pb, lc, rc);
            return rcv == ap.receiver() ? n
                    : new AppliedProperty(rcv, ap.property());
        }
        return n;
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
            return af.withParameters(List.of(ps.get(0), ps.get(1), joinKind, condLam));
        }
        List<ValueSpecification> out = new java.util.ArrayList<>(ps);
        out.set(2, joinKind);
        return af.withParameters(out);
    }

    /**
     * The legacy TDS SHARED-KEY join {@code join(tds2, JoinType, ['id'])}:
     * both sides carry the key columns under the SAME names, and the engine
     * keeps exactly ONE copy in the output — MERGE BY NAME (engine tds.pure
     * join/5 + processTdsJoinOnColumns: a qualified equality per key pair,
     * the left's columns then the right's minus the shared names;
     * RIGHT_OUTER keeps the right's). Typed like the prefix form: every
     * argument against the modern join's registered signature, the
     * condition with T and V bound, the merged schema stated here (the
     * generic's T+V algebra would rightly reject the shared names). The
     * lowering projects the merged list explicitly where sides overlap.
     * (Until 2026-09-12 the right keys were renamed to a synthetic __jk_
     * copy and selected away — row-equal, one subselect too many.)
     */
    private static @com.legend.base.Nullable TypedSpec sharedKeyLegacyJoin(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> ps = af.parameters();
        if ((ps.size() != 4 && ps.size() != 5)
                || !(ps.get(2) instanceof EnumValue kind)
                || !(kind.fullPath().equals("meta::relational::metamodel::join::JoinType")
                        || kind.fullPath().equals("JoinType"))) {
            return null;
        }
        List<String> keys = columnNames(ps.get(3));
        if (keys == null) {
            return null;
        }
        // the EXPLICIT-pair spelling join(l, r, kind, ['id'], ['id']) with
        // IDENTICAL lists is the shared-key join by another name — distinct
        // names go to the modern desugar (no collision there)
        if (ps.size() == 5 && !keys.equals(columnNames(ps.get(4)))) {
            return null;
        }
        // DUPLICATE key entries (corpus DupeJoinKeys: ['tradeDate','tradeDate'])
        // are redundant equalities — one rename + one condition per distinct key
        keys = new java.util.ArrayList<>(new java.util.LinkedHashSet<>(keys));
        boolean rightKeeps = kind.value().equals("RIGHT_OUTER");
        Variable a = new Variable("a");
        Variable b = new Variable("b");
        ValueSpecification cond = null;
        for (String k : keys) {
            ValueSpecification eq = new AppliedFunction("equal", List.of(
                    new AppliedProperty(a, k), new AppliedProperty(b, k)));
            cond = cond == null ? eq : new AppliedFunction("and", List.of(cond, eq));
        }
        AppliedFunction modern = new AppliedFunction("join", List.of(
                ps.get(0), ps.get(1),
                new EnumValue("meta::pure::functions::relation::JoinKind",
                        joinKindNameOf(kind)),
                new LambdaFunction(List.of(a, b), List.of(cond))));
        TypedFunction sig = t.model().findFunction(com.legend.builtin.Pure
                        .JOIN__RELATION_1__RELATION_1__JOIN_KIND_1__FUNCTION_1.qualifiedName())
                .stream().filter(f -> f.parameters().size() == 4).findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "the relation join is not registered"));
        Bindings bnd = new Bindings();
        TypedSpec left = Checkers.unifiedArg(t, sig, 0, modern, bnd, env);
        TypedSpec right = Checkers.unifiedArg(t, sig, 1, modern, bnd, env);
        TypedSpec kindArg = Checkers.unifiedArg(t, sig, 2, modern, bnd, env);
        if (!(kindArg instanceof TypedEnumValue tk)) {
            throw new TypeInferenceException("join expects a JoinKind");
        }
        TypedLambda tc = (TypedLambda) t.typeLambda(
                (LambdaFunction) modern.parameters().get(3),
                sig.parameters().get(3).type(), bnd, env);
        Type.RelationType lrt = Type.requireRelationSchema(left.info().type());
        Type.RelationType rrt = Type.requireRelationSchema(right.info().type());
        List<Type.Column> kept = new java.util.ArrayList<>();
        for (Type.Column c : lrt.columns()) {
            if (!(rightKeeps && keys.contains(c.name()))) {
                kept.add(c);
            }
        }
        for (Type.Column c : rrt.columns()) {
            if (!(!rightKeeps && keys.contains(c.name()))) {
                kept.add(c);
            }
        }
        return new TypedJoin(left, right, tk, tc, Optional.empty(), null,
                new ExprType(Type.relation(new Type.RelationType(kept)),
                        sig.returnMultiplicity()),
                true /* USER condition: the legacy keys, equated */);
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
    private static @com.legend.base.Nullable List<String> columnNames(
            @com.legend.base.Nullable ValueSpecification v) {
        if (v instanceof CString c) {
            return List.of(c.value());
        }
        if (v instanceof com.legend.protocol.spec.PureCollection pc
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
        // the slot overload is lite-INTERNAL vocabulary (not in the
        // user bare-name namespace) — resolve by exact identity
        TypedFunction sig = t.model().findFunction(com.legend.builtin.Pure.Lite.JOIN_SLOT).stream()
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
        Type targetRow = thunk.functionType().result().type();
        Type.RelationType targetSchema = Type.relationSchema(targetRow);
        if (targetSchema == null) {
            throw new TypeInferenceException(
                    "join slot target must be a relation (a table reference), got "
                            + targetRow.typeName());
        }
        // Z = (alias : TargetROW[1]) — the joined sub-row column IS one
        // row of the target: the bare schema struct (Row-vs-Relation —
        // the navigate slot's reads are per-row BY TYPE).
        String zVar = ((Type.TypeVar) slotParam.arguments().get(1)).name();
        b.bindType(zVar, new Type.RelationType(List.of(
                new Type.Column(cs.name(), targetSchema,
                        Multiplicity.Bounded.ONE))));
        TypedLambda cond = (TypedLambda) t.typeLambda(condLam, sig.parameters().get(2).type(), b, env);

        ExprType out = t.kernel().resolveOutput(sig.returnType(), sig.returnMultiplicity(), b);
        return new TypedJoinSlot(source, cs.name(), thunk.body().get(0), cond,
                cs.alias(), out);
    }

    private static TypedSpec withPrefix(Typer t, AppliedFunction af, Env env) {
        // the PREFIX form is lite surface (Pure.Lite.JOIN_WITH_PREFIX, USER 2026-09-11):
        // the user's join(l, r, kind, cond, 'p_') spelling routes here by arity
        TypedFunction sig = t.model().findFunction(com.legend.builtin.Pure.Lite.JOIN_WITH_PREFIX).stream()
                .findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "the lite prefix join is not registered"));

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
        return new TypedJoin(left, right, kind, cond, Optional.of(prefix), null,
                new ExprType(Type.relation(schema), sig.returnMultiplicity()), true);
    }
}
