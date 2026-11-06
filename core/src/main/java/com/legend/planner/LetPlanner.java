// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.base.Nullable;
import com.legend.builtin.NativeFn;
import com.legend.builtin.Pure;
import com.legend.compiler.spec.ResultEnvelopeSplice;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.StatementEffects;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedRawSqlRelation;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.model.FunctionId;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A LET's kind, planned (the planner-driver seam program's S2b, 2026-10-31; the body driver's loop decided it until
 * then): what a non-last {@code let} of a test body is, read off its right-hand side and the statements after it, with
 * no session. D75: a let is a staged table or a compile-time splice, never a value Java holds -- so every kind but
 * the value let says where its rows live: an ALIAS reads a frame already planned, STAGED rows and a STAGED GRID are a
 * temporary table of the session made at the let's turn, a FRAME is an execute call whose chain its readers splice
 * ({@link FramePlanner}), and a VALUE let (a handle's included) is spliced into its readers at planning, a handle's
 * lineage rows registered ({@link PlanAllocations#registerHandlesIn}). The driver dispatches on the kind and runs
 * what runs.
 */
public final class LetPlanner {

    private LetPlanner() {
    }

    /** The kinds of a non-last let, in the order they are decided. */
    public enum Kind {
        /** {@code $r.values(->at(0)/->toOne()/->first())} over a relation-rooted frame: the same frame. */
        ALIAS,
        /** {@code rows->stage()}: the rows computed at the let's turn into a temporary table read by name after. */
        STAGED,
        /** {@code execute(...)} or {@code executeLegendQuery(...)}: a frame. */
        FRAME,
        /** A raw grid ({@code executeInDb('select ...', $c)}) a statement reads after a later effect: the engine runs
         *  it AT the let, so its rows are held in a temporary table at the let's turn (D75). Read before any effect,
         *  it stays a splice: the state it read is the state its readers see. */
        STAGED_GRID,
        /** Any other binding, a handle's included: spliced into its readers. */
        VALUE
    }

    /** The kind of {@code let}, statement {@code at} of {@code stmts}; {@code relationFrame} says whether a name is a
     *  relation-rooted frame planned before it. */
    public static Kind kind(TypedLet let, List<TypedSpec> stmts, int at, SpecCompiler specs,
            Map<FunctionId, Boolean> effectMemo,
            Predicate<String> relationFrame) {
        if (aliasOf(let.value(), relationFrame) != null) {
            return Kind.ALIAS;
        }
        TypedSpec rhs = rhs(let);
        if (rhs instanceof TypedNativeCall sc
                && Pure.Lite.STAGE.equals(sc.callee().qualifiedName())) {
            return Kind.STAGED;
        }
        if (rhs instanceof TypedNativeCall ec && NativeFn.Handle.runsAFrame(ec.callee().id())) {
            return Kind.FRAME;
        }
        if (rhs instanceof TypedRawSqlRelation
                && readAfterEffect(stmts, at, let.name(), specs, effectMemo)) {
            return Kind.STAGED_GRID;
        }
        return Kind.VALUE;
    }

    /**
     * {@code body} with each UNREAD let as the statement it binds (the user's ruling, 2026-11-02, the walker-deletion
     * program): a {@code let x = e} that no later statement reads -- by name, through the trees it carries, shadowing
     * respected ({@code FreeVars.ofBodyReadingTrees}) -- is the statement {@code e}, at the same place. Pure evaluates
     * a let at its turn, so {@code let x = e;} with {@code x} unread IS {@code e;}: it runs, and what it raises fails
     * the test (legend-pure; until this rule lite sent nothing for such a let). The body's LAST let is one (the user's
     * "let at the end just runs as normal statement", 2026-11-02; applied to the last statement 2026-11-03): nothing
     * reads it, and a body's last {@code let x = e} answers {@code e}, the value it binds.
     * Kept as lets: a staged let ({@code rows->stage()}: its rows made at its turn already) and an alias of a frame's
     * values ({@code $r.values->at(0)}: the same frame, nothing to run). A read let is evaluated where it is read
     * ({@code docs/SEMANTICS_REGISTER.md}).
     */
    public static List<TypedSpec> unreadAsStatements(List<TypedSpec> body,
            com.legend.protocol.spec.@Nullable ValueSpecification source,
            com.legend.compiler.element.ModelContext ctx) {
        Map<String, List<Boolean>> readInSource = readInSource(source);
        Map<String, Integer> seen = new java.util.HashMap<>();
        List<TypedSpec> out = null;
        for (int i = 0; i < body.size(); i++) {
            if (!(body.get(i) instanceof TypedLet let)) {
                continue;
            }
            int occurrence = seen.merge(let.name(), 1, Integer::sum) - 1;
            List<Boolean> reads = readInSource.get(let.name());
            if (reads == null || occurrence >= reads.size() || reads.get(occurrence)) {
                // read in the source (the typer substitutes a let into the lambdas that read it, so the typed tree
                // alone can miss a read), or no source let to ask: read
                continue;
            }
            TypedSpec rhs = rhs(let);
            if (rhs instanceof TypedNativeCall sc && Pure.Lite.STAGE.equals(sc.callee().qualifiedName())
                    || badAliasIndex(let.value(), n -> true) || aliasOf(let.value(), n -> true) != null
                    || doesNoWork(let.value(), body.subList(0, i), ctx)
                    || rhs instanceof com.legend.compiler.spec.typed.TypedTestDataGen) {
                // the generator's carrier stays a let until the generator is planned (T8): the walk folds it where a
                // statement reads it, and no road runs it as a statement yet
                continue;
            }
            if (com.legend.compiler.spec.typed.FreeVars.ofBodyReadingTrees(body.subList(i + 1, body.size()))
                    .contains(let.name())) {
                continue;
            }
            if (out == null) {
                out = new java.util.ArrayList<>(body);
            }
            out.set(i, let.value());
        }
        return out == null ? body : List.copyOf(out);
    }

    /** For each let name of {@code source}'s statements (a parameterless lambda's body), whether each of its lets, in
     *  order, is read by a statement after it -- the source's free variables, scoping respected
     *  ({@code SourceSubst.freeVarsOfBody}); the carriers' captured arguments are source too. Empty for no source. */
    private static Map<String, List<Boolean>> readInSource(
            com.legend.protocol.spec.@Nullable ValueSpecification source) {
        Map<String, List<Boolean>> out = new java.util.HashMap<>();
        if (!(source instanceof com.legend.protocol.spec.LambdaFunction lf)) {
            return out;
        }
        List<com.legend.protocol.spec.ValueSpecification> statements = lf.body();
        for (int j = 0; j < statements.size(); j++) {
            com.legend.protocol.spec.CString name = com.legend.compiler.spec.SourceSubst.letName(statements.get(j));
            if (name == null) {
                continue;
            }
            boolean read = com.legend.compiler.spec.SourceSubst.freeVarsOfBody(
                    statements.subList(j + 1, statements.size())).contains(name.value());
            out.computeIfAbsent(name.value(), k -> new java.util.ArrayList<>()).add(read);
        }
        return out;
    }

    /** Whether {@code value}, bound by a let after {@code lets}, does no work when evaluated -- it cannot raise and
     *  touches no database, so running it as a statement changes nothing: a function value (a lambda), an element
     *  reference, a variable, a constant ({@code Literals.fold}), an orchestration handle (a connection, a runtime:
     *  {@link BodyPlanner#handleType}), or the census carrier the checker captured from the model. */
    public static boolean doesNoWork(TypedSpec value, List<TypedSpec> lets,
            com.legend.compiler.element.ModelContext ctx) {
        return value instanceof com.legend.compiler.spec.typed.TypedLambda
                || value instanceof com.legend.compiler.spec.typed.TypedPackageableRef
                || value instanceof com.legend.compiler.spec.typed.TypedVariable
                || value instanceof com.legend.compiler.spec.typed.TypedCsvCensus
                || com.legend.compiler.spec.typed.Literals.fold(value, lets) != null
                || BodyPlanner.handleType(value, ctx) != null;
    }

    /** {@code let}'s right-hand side under its execution contexts (a {@code from} only binds where it runs). */
    public static TypedSpec rhs(TypedLet let) {
        TypedSpec rhs = let.value();
        while (rhs instanceof TypedFrom rf) {
            rhs = rf.source();
        }
        return rhs;
    }

    /**
     * {@code let tds = $r.values(->at(0)/->toOne()/->first())} over a RELATION-rooted
     * frame: the wrappers are the Result envelope and the alias IS the same
     * frame ({@code $tds->size()} keeps ONE-TDS semantics). {@code at(k>0)}
     * is loud — the envelope holds one TDS. Class/scalar roots return null:
     * their at/toOne are REAL selections and the binding is an ordinary let.
     * The name of the frame aliased, or null.
     */
    public static @Nullable String aliasOf(TypedSpec rhs,
            Predicate<String> relationFrame) {
        Unwrapped u = unwrap(rhs, relationFrame);
        if (u.frame() != null && u.badIndex()) {
            throw new IllegalStateException("Result.values->at(k>0) on a"
                    + " relation-rooted query — the values envelope holds"
                    + " one TDS");
        }
        return u.frame();
    }

    /** Whether {@link #aliasOf} would RAISE on {@code rhs} ({@code $frame.values->at(k)} with k > 0 over a
     *  relation frame): the shape named without raising, so a rule can leave the body to the walk, which raises it at
     *  the let's turn (the test-is-the-product plan's M9a audit, S1). */
    public static boolean badAliasIndex(TypedSpec rhs, Predicate<String> relationFrame) {
        Unwrapped u = unwrap(rhs, relationFrame);
        return u.frame() != null && u.badIndex();
    }

    /** {@code rhs} with its Result-envelope wrappers off: the relation frame at its root (null where the root is no
     *  relation frame) and whether an {@code at(k)} with k > 0 was among the wrappers. */
    private record Unwrapped(@Nullable String frame, boolean badIndex) {
    }

    private static Unwrapped unwrap(TypedSpec rhs, Predicate<String> relationFrame) {
        TypedSpec cur = rhs;
        boolean badIndex = false;
        while (true) {
            if (cur instanceof TypedPropertyAccess pa
                    && pa.property().equals("values")) {
                cur = pa.source();
                continue;
            }
            if (cur instanceof TypedNativeCall nc
                    && (ResultEnvelopeSplice.AT_FQN.equals(nc.callee().qualifiedName())
                            || ResultEnvelopeSplice.TO_ONE_FQN.equals(nc.callee().qualifiedName())
                            || ResultEnvelopeSplice.FIRST_FQN.equals(nc.callee().qualifiedName()))
                    && !nc.args().isEmpty()) {
                if (ResultEnvelopeSplice.AT_FQN.equals(nc.callee().qualifiedName())
                        && !(nc.args().size() == 2 && nc.args().get(1)
                                instanceof TypedCInteger k
                                && k.value().longValue() == 0)) {
                    badIndex = true;
                }
                cur = nc.args().get(0);
                continue;
            }
            break;
        }
        return new Unwrapped(cur instanceof TypedVariable v && relationFrame.test(v.name()) ? v.name() : null,
                badIndex);
    }

    /** Whether a statement after {@code at} reads the let {@code name} -- itself or through a let that reads it --
     *  once an effect has run after it (an effect's own statement included: it may read the let, its change after). */
    public static boolean readAfterEffect(List<TypedSpec> stmts, int at, String name, SpecCompiler specs,
            Map<FunctionId, Boolean> effectMemo) {
        Set<String> names = new HashSet<>(List.of(name));
        boolean effectSeen = false;
        for (int j = at + 1; j < stmts.size(); j++) {
            TypedSpec s = stmts.get(j);
            boolean reads = reads(s, names);
            if (effectSeen && reads) {
                return true;
            }
            if (s instanceof TypedLet l && reads) {
                names.add(l.name());
            }
            effectSeen |= StatementEffects.containsEffect(s, specs, effectMemo);
        }
        return false;
    }

    /** Whether {@code n} reads a variable named in {@code names}. */
    private static boolean reads(TypedSpec n, Set<String> names) {
        if (n instanceof TypedVariable v && names.contains(v.name())) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (reads(c, names)) {
                return true;
            }
        }
        return false;
    }
}
