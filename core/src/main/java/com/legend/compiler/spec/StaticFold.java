// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Type;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * COMPILE-TIME evaluation of SCHEMA computations inside a
 * {@code <<functionType.NormalizeRequiredFunction>>} body (engine doctrine:
 * these functions are normalized away before routing — their bodies COMPUTE
 * the plan, they are not part of it). After β-substitution the schema
 * positions are expressions over literals and column metadata
 * ({@code $cols->map(vc|pair($vc,$vc+'_1'))},
 * {@code $tds.columns->filter(...)->map(col|col({r|...}, $col.name+'_x'))});
 * this folder evaluates that vocabulary to LITERAL arguments so the
 * ordinary checkers (rename pairs, restrict names, extend colspecs) see
 * static facts.
 *
 * <p>Two mutually recursive halves:
 * <ul>
 *   <li>{@link #eval} — the static interpreter. Values: {@code String},
 *       {@code Long}, {@code Double}, {@code Boolean}, {@code List<Object>}
 *       (pure collections FLATTEN), {@link Col} (a TDSColumn metadata fact,
 *       from typing a relation receiver's {@code .columns}), {@link Pair}
 *       and {@link TypeToken}. {@code null} = not static.</li>
 *   <li>{@link #fold} — the AST rebuild. Static subtrees reify to literal
 *       nodes; a {@code map} over a static collection whose lambda body is
 *       NOT static (it reads the runtime row) UNROLLS — one folded body per
 *       element with the parameter bound in scope; {@code if} over a static
 *       condition folds to the taken branch. Everything else rebuilds
 *       children and stays for the ordinary Typer walk.</li>
 * </ul>
 *
 * <p>Unfoldable schema positions stay as-written and the downstream checker
 * walls stay LOUD — the folder never guesses.
 */
final class StaticFold {

    private final Typer typer;
    private final Env env;

    StaticFold(Typer typer, Env env) {
        this.typer = Objects.requireNonNull(typer, "typer");
        this.env = env;
    }

    /** TDS column metadata: name + SIMPLE pure type name (String, Integer…). */
    record Col(String name, String type) {
    }

    record Pair(Object first, Object second) {
    }

    /** A bare type reference in expression position ({@code $col.type == Integer}). */
    record TypeToken(String simpleName) {
    }

    // =====================================================================
    // fold — AST rebuild
    // =====================================================================

    ValueSpecification fold(ValueSpecification v) {
        return fold(v, Map.of());
    }

    /** The expression as a fully static LITERAL, or null — the Typer's
     * TDSColumn-metadata fold ({@code X.columns->map(c|$c.name...)} asserts)
     * only rewrites when the whole computation is schema facts. */
    @com.legend.Nullable ValueSpecification foldToLiteral(ValueSpecification v) {
        return reify(eval(v, Map.of()));
    }

    private ValueSpecification fold(ValueSpecification v, Map<String, Object> scope) {
        // a lambda literal is an opaque VALUE to eval (a pair list of
        // accessors filters statically) but its body still FOLDS here
        ValueSpecification lit = v instanceof LambdaFunction ? null : reify(eval(v, scope));
        if (lit != null) {
            // a reified value may carry lambda LITERALS (the accessor a
            // pair list selected): their bodies fold under the same scope
            return lit == v ? lit : structural(lit, scope);
        }
        return structural(v, scope);
    }

    private ValueSpecification structural(ValueSpecification v, Map<String, Object> scope) {
        return switch (v) {
            case AppliedFunction af -> foldCall(af, scope);
            case AppliedProperty ap -> new AppliedProperty(
                    fold(ap.receiver(), scope), ap.property());
            case LambdaFunction lf -> {
                Map<String, Object> inner = shadow(scope, lf.parameters());
                yield new LambdaFunction(lf.parameters(),
                        lf.body().stream().map(b -> fold(b, inner)).toList());
            }
            case PureCollection pc -> new PureCollection(
                    pc.values().stream().map(x -> fold(x, scope)).toList());
            case ColSpec cs -> new ColSpec(cs.name(),
                    cs.function1() == null ? null
                            : (LambdaFunction) fold(cs.function1(), scope),
                    cs.function2() == null ? null
                            : (LambdaFunction) fold(cs.function2(), scope),
                    cs.alias(),
                    cs.args().stream().map(a -> fold(a, scope)).toList());
            case ColSpecArray ca -> new ColSpecArray(ca.colSpecs().stream()
                    .map(c -> (ColSpec) fold(c, scope)).toList());
            default -> v.mapChildren(x -> fold(x, scope));
        };
    }

    /** Every function-valued helper call mentioning {@code pv}, replaced
     * by its raw expansion ({@link Typer#rawSchemaErasedExpansion}). */
    private ValueSpecification expandHelperCallsOver(ValueSpecification v, String pv) {
        if (v instanceof AppliedFunction call && mentions(call, pv)
                && typer.functionValuedHelperCall(call)) {
            ValueSpecification ex = typer.rawSchemaErasedExpansion(call);
            if (ex != null) {
                return ex;
            }
        }
        return v.mapChildren(x -> expandHelperCallsOver(x, pv));
    }

    private static boolean mentions(ValueSpecification v, String name) {
        if (v instanceof Variable var) {
            return var.name().equals(name);
        }
        boolean[] found = {false};
        v.mapChildren(x -> {
            if (!found[0] && mentions(x, name)) {
                found[0] = true;
            }
            return x;
        });
        return found[0];
    }

    private ValueSpecification foldCall(AppliedFunction af, Map<String, Object> scope) {
        List<ValueSpecification> ps = af.parameters();
        // map over a STATIC collection with a runtime lambda body: UNROLL —
        // one folded body per element, the parameter bound as a scope fact
        // (a Col element's .name/.type reads fold to literals inside).
        if (com.legend.compiler.ResolvedNames.names(af, com.legend.compiler.element.type.PlatformTypes.MAP) && ps.size() == 2
                && ps.get(1) instanceof LambdaFunction lam
                && lam.parameters().size() == 1) {
            List<Object> coll = evalList(ps.get(0), scope);
            if (coll != null) {
                String pv = lam.parameters().get(0).name();
                // a function-valued helper CALL over the element
                // (toStringForColAccessor($col)->eval($row)) expands to
                // its literal FIRST, so the element's .name/.type reads
                // inside it fold like the body's own — the binder is gone
                // after the unroll, so a whole-value use would dangle
                ValueSpecification body = expandHelperCallsOver(single(lam), pv);
                List<ValueSpecification> parts = new ArrayList<>(coll.size());
                for (Object e : coll) {
                    Map<String, Object> inner = new LinkedHashMap<>(scope);
                    inner.put(pv, e);
                    parts.add(fold(body, inner));
                }
                return parts.size() == 1 ? parts.get(0) : new PureCollection(parts);
            }
        }
        // if over a static condition: fold the taken branch's body
        if (FoldOp.of(af) == FoldOp.IF && ps.size() == 3
                && eval(ps.get(0), scope) instanceof Boolean cond
                && ps.get(1) instanceof LambdaFunction thenL
                && ps.get(2) instanceof LambdaFunction elseL) {
            return fold(single(cond ? thenL : elseL), scope);
        }
        // a USER (Pure-bodied) function called with at least one STATIC
        // argument: engine preval EVALUATES the normalize-required body, so
        // every Pure call inside it runs too (rowValueDifference's private
        // extendMatchColumns($tds, $diffCols) over the filtered TDSColumn
        // facts). β-inline the callee and fold its body under this scope —
        // static arguments as scope facts, runtime ones by source
        // substitution. Null = not a unique bodied callee of this arity.
        ValueSpecification inlined = inlineUserCall(af, scope);
        if (inlined != null) {
            return inlined;
        }
        return af.withParameters(ps.stream().map(p -> fold(p, scope)).toList());
    }

    /** Callees being inlined on this fold's stack (a recursive program
     * never terminates statically — leave it to the ordinary path). */
    private final java.util.ArrayDeque<String> inlining = new java.util.ArrayDeque<>();

    private @com.legend.Nullable ValueSpecification inlineUserCall(AppliedFunction af,
            Map<String, Object> scope) {
        List<ValueSpecification> ps = af.parameters();
        List<com.legend.compiler.element.TypedFunction> bodied = new ArrayList<>();
        for (var f : typer.functionCandidates(af.function())) {
            if (f.body().isPresent() && f.parameters().size() == ps.size()) {
                bodied.add(f);
            }
        }
        if (bodied.size() != 1 || inlining.contains(bodied.get(0).signatureKey())) {
            return null;
        }
        var callee = bodied.get(0);
        Map<String, Object> inner = new LinkedHashMap<>(scope);
        Map<String, ValueSpecification> subst = new LinkedHashMap<>();
        boolean anyStatic = false;
        for (int i = 0; i < ps.size(); i++) {
            String name = callee.parameters().get(i).name();
            Object v = eval(ps.get(i), inner);
            if (v != null) {
                inner.put(name, v);
                anyStatic = true;
            } else {
                inner.remove(name);
                subst.put(name, fold(ps.get(i), scope));
            }
        }
        if (!anyStatic) {
            return null;
        }
        ValueSpecification single = singleBody(callee);
        if (single == null) {
            return null;
        }
        ValueSpecification body = SourceSubst.substitute(typer.alphaRename(single), subst);
        inlining.push(callee.signatureKey());
        try {
            return fold(body, inner);
        } finally {
            inlining.pop();
        }
    }

    /** The callee's body as ONE expression (its lets inlined through the shared
     * funnel), or null when it has non-let intermediate statements. */
    private static @com.legend.Nullable ValueSpecification singleBody(
            com.legend.compiler.element.TypedFunction callee) {
        LambdaFunction lets = SourceSubst.inlineLets(new LambdaFunction(List.of(),
                callee.body().orElseThrow()));
        return lets == null ? null : lets.body().get(0);
    }

    /** A call to the ONE bodied callee of this arity, every argument static:
     * its body evaluates under those bindings (the interpreter's rule for a
     * library function with a body — natives evaluate by their fold row).
     * Null = not such a call, or not static; a callee already on the stack is
     * a recursive program and is left to the ordinary path. */
    private @com.legend.Nullable Object evalUserCall(AppliedFunction af, Map<String, Object> scope) {
        List<ValueSpecification> ps = af.parameters();
        List<com.legend.compiler.element.TypedFunction> bodied = new ArrayList<>();
        for (var f : typer.functionCandidates(af.function())) {
            if (f.body().isPresent() && f.parameters().size() == ps.size()) {
                bodied.add(f);
            }
        }
        if (bodied.size() != 1 || inlining.contains(bodied.get(0).signatureKey())) {
            return null;
        }
        var callee = bodied.get(0);
        Map<String, Object> inner = new LinkedHashMap<>(scope);
        for (int i = 0; i < ps.size(); i++) {
            Object v = eval(ps.get(i), inner);
            if (v == null) {
                return null;
            }
            inner.put(callee.parameters().get(i).name(), v);
        }
        ValueSpecification single = singleBody(callee);
        if (single == null) {
            return null;
        }
        inlining.push(callee.signatureKey());
        try {
            return eval(typer.alphaRename(single), inner);
        } finally {
            inlining.pop();
        }
    }

    // =====================================================================
    // eval — the static interpreter (null = not static)
    // =====================================================================

    private @com.legend.Nullable Object eval(ValueSpecification v, Map<String, Object> scope) {
        if (v instanceof LambdaFunction) {
            // a lambda LITERAL is an opaque static value: a pair list of
            // (type, accessor lambda) filters statically to the one
            // accessor (toStringForColAccessor), which then evals inline
            return v;
        }
        return switch (v) {
            case CString s -> s.value();
            case CInteger i -> i.value().longValue();
            case com.legend.protocol.spec.CFloat f -> f.value();
            case CBoolean b -> b.value();
            case Variable var -> scope.get(var.name());
            case PureCollection pc -> {
                List<Object> out = new ArrayList<>(pc.values().size());
                for (ValueSpecification x : pc.values()) {
                    Object e = eval(x, scope);
                    if (e == null) {
                        yield null;
                    }
                    flattenInto(out, e);
                }
                yield out;
            }
            case PackageableElementPtr p -> new TypeToken(simple(p.fullPath()));
            case AppliedProperty ap -> evalProperty(ap, scope);
            case AppliedFunction af -> evalCall(af, scope);
            default -> null;
        };
    }

    private @com.legend.Nullable Object evalProperty(AppliedProperty ap, Map<String, Object> scope) {
        if (ap.property().equals("columns")) {
            Object recv = eval(ap.receiver(), scope);
            if (recv == null) {
                return relationColumns(ap.receiver());
            }
            return null;
        }
        Object recv = eval(ap.receiver(), scope);
        return switch (recv) {
            case Col c -> switch (ap.property()) {
                case "name" -> c.name();
                case "type" -> new TypeToken(c.type());
                default -> null;
            };
            case Pair p -> switch (ap.property()) {
                case "first" -> p.first();
                case "second" -> p.second();
                default -> null;
            };
            // collection property nav auto-maps ($diffCols.name)
            case List<?> l -> {
                List<Object> out = new ArrayList<>(l.size());
                for (Object e : l) {
                    Object r = switch (e) {
                        case Col c when ap.property().equals("name") -> c.name();
                        case Col c when ap.property().equals("type") -> new TypeToken(c.type());
                        case Pair p when ap.property().equals("first") -> p.first();
                        case Pair p when ap.property().equals("second") -> p.second();
                        default -> null;
                    };
                    if (r == null) {
                        yield null;
                    }
                    out.add(r);
                }
                yield out;
            }
            case null, default -> null;
        };
    }

    /** The TDSColumn facts of a relation-typed receiver — TYPED speculatively
     * (the receiver re-types when the folded body synths; the Typer is
     * effect-free on failure). Null when it does not type to a relation. */
    private @com.legend.Nullable List<Object> relationColumns(ValueSpecification receiver) {
        try {
            var typed = typer.synth(receiver, env);
            if (Type.schemaView(typed.info().type()) instanceof Type.RelationType rt) {
                List<Object> cols = new ArrayList<>(rt.columns().size());
                for (Type.RelationType.Column c : rt.columns()) {
                    cols.add(new Col(c.name(), simple(c.type().typeName())));
                }
                return cols;
            }
        } catch (RuntimeException ignored) {
            // not typable here (free row vars, unresolved store) — not static
        }
        return null;
    }

    /** THE OPERATIONS THE FOLD KNOWS, each by the declarations it names: a call
     *  folds by what it REFERS TO ({@link com.legend.compiler.ResolvedNames#referents}),
     *  never by its spelling — a qualified name and a bare one carrying
     *  candidates fold alike (untangle 4b.1). */
    enum FoldOp {
        AND(Pure.AND__BOOLEAN_1__BOOLEAN_1.qualifiedName(), Pure.AND__BOOLEAN_MANY.qualifiedName()),
        AT(Pure.AT__T_MANY__INTEGER_1.qualifiedName()),
        CONCATENATE(Pure.CONCATENATE__RELATION_1__RELATION_1.qualifiedName(), Pure.CONCATENATE__T_MANY__T_MANY.qualifiedName()),
        CONTAINS(Pure.CONTAINS__ANY_MANY__ANY_1.qualifiedName(), Pure.CONTAINS__Z_MANY__Z_1__FUNCTION_1.qualifiedName()),
        ELEMENT_TO_PATH(Pure.ELEMENT_TO_PATH__3.qualifiedName(), Pure.ELEMENT_TO_PATH__FUNCTION_1.qualifiedName(), Pure.ELEMENT_TO_PATH__PACKAGEABLEELEMENT_1.qualifiedName(), Pure.ELEMENT_TO_PATH__PACKAGEABLEELEMENT_1__BOOLEAN_1.qualifiedName(), Pure.ELEMENT_TO_PATH__PACKAGEABLEELEMENT_1__STRING_1.qualifiedName(), Pure.ELEMENT_TO_PATH__TYPE_1.qualifiedName(), Pure.ELEMENT_TO_PATH__TYPE_1__STRING_1.qualifiedName()),
        EQUAL(Pure.EQUAL__ANY_MANY__ANY_MANY.qualifiedName()),
        FILTER(Pure.FILTER__RELATION_1__FUNCTION_1.qualifiedName(), Pure.FILTER__T_MANY__FUNCTION_1.qualifiedName(), Pure.TDS_FILTER__TDS_1__FUNCTION_1.qualifiedName()),
        IF(Pure.IF__BOOLEAN_1__FUNCTION_1__FUNCTION_1.qualifiedName(), Pure.IF__PAIR_MANY__FUNCTION_1.qualifiedName()),
        IN(Pure.IN__ANY_0_1__ANY_MANY.qualifiedName(), Pure.IN__ANY_1__ANY_MANY.qualifiedName(), Pure.IN__U_0_1__RELATION_1.qualifiedName()),
        INDEX_OF(Pure.INDEX_OF__STRING_1__STRING_1.qualifiedName(), Pure.INDEX_OF__STRING_1__STRING_1__INTEGER_1.qualifiedName(), Pure.INDEX_OF__T_MANY__T_1.qualifiedName()),
        IS_EMPTY(Pure.IS_EMPTY__T_MANY.qualifiedName()),
        IS_NOT_EMPTY(Pure.IS_NOT_EMPTY__ANY_0_1.qualifiedName(), Pure.IS_NOT_EMPTY__ANY_MANY.qualifiedName()),
        JOIN_STRINGS(Pure.JOIN_STRINGS__RELATION_1__COL_SPEC_1__STRING_1.qualifiedName(), Pure.JOIN_STRINGS__RELATION_1__COL_SPEC_1__STRING_1__SORT_INFO_MANY.qualifiedName(), Pure.JOIN_STRINGS__RELATION_1__FUNCTION_1__STRING_1.qualifiedName(), Pure.JOIN_STRINGS__RELATION_1__FUNCTION_1__STRING_1__SORT_INFO_MANY.qualifiedName(), Pure.JOIN_STRINGS__STRING_MANY.qualifiedName(), Pure.JOIN_STRINGS__STRING_MANY__STRING_1.qualifiedName(), Pure.JOIN_STRINGS__STRING_MANY__STRING_1__STRING_1__STRING_1.qualifiedName()),
        MAKE_STRING(Pure.MAKE_STRING__ANY_MANY.qualifiedName(), Pure.MAKE_STRING__ANY_MANY__STRING_1.qualifiedName(), Pure.MAKE_STRING__ANY_MANY__STRING_1__STRING_1__STRING_1.qualifiedName()),
        MAP(Pure.MAP__RELATION_1__FUNCTION_1.qualifiedName(), Pure.MAP__T_0_1__FUNCTION_1.qualifiedName(), Pure.MAP__T_MANY__FUNCTION_1.qualifiedName(), Pure.MAP__T_M__FUNCTION_1.qualifiedName()),
        MINUS(Pure.MINUS__DECIMAL_MANY.qualifiedName(), Pure.MINUS__FLOAT_MANY.qualifiedName(), Pure.MINUS__INTEGER_MANY.qualifiedName(), Pure.MINUS__NUMBER_MANY.qualifiedName()),
        NOT(Pure.NOT__BOOLEAN_1.qualifiedName()),
        OR(Pure.OR__BOOLEAN_1__BOOLEAN_1.qualifiedName(), Pure.OR__BOOLEAN_MANY.qualifiedName()),
        PAIR(Pure.PAIR__U_1__V_1.qualifiedName()),
        PLUS(Pure.PLUS__DECIMAL_MANY.qualifiedName(), Pure.PLUS__FLOAT_MANY.qualifiedName(), Pure.PLUS__INTEGER_MANY.qualifiedName(), Pure.PLUS__NUMBER_MANY.qualifiedName(), Pure.STRING_PLUS__STRING_MANY.qualifiedName()),
        REMOVE_DUPLICATES(Pure.REMOVE_DUPLICATES__T_MANY.qualifiedName(), Pure.REMOVE_DUPLICATES__T_MANY__FUNCTION_0_1__FUNCTION_0_1.qualifiedName(), Pure.REMOVE_DUPLICATES__T_MANY__FUNCTION_1.qualifiedName()),
        SORT_BY(Pure.SORT_BY__T_m__FUNCTION_0_1.qualifiedName()),
        TO_ONE(Pure.TO_ONE__T_MANY.qualifiedName(), Pure.TO_ONE__T_MANY__STRING_1.qualifiedName()),
        TO_ONE_MANY(Pure.TO_ONE_MANY__T_MANY.qualifiedName(), Pure.TO_ONE_MANY__T_MANY__STRING_1.qualifiedName()),
        TO_STRING(Pure.TO_STRING__ANY_1.qualifiedName(), Pure.TO_STRING__RELATION.qualifiedName(), Pure.TO_STRING__RELATION_BOOL.qualifiedName()),
        ZIP(Pure.ZIP__T_MANY__U_MANY.qualifiedName());

        private final java.util.Set<String> fqns;

        FoldOp(String... fqns) {
            // several overload constants spell one FQN: copyOf tolerates the repeats
            this.fqns = java.util.Set.copyOf(java.util.Arrays.asList(fqns));
        }

        /** The operation {@code af} names, or null when it names none of them. */
        static @com.legend.Nullable FoldOp of(AppliedFunction af) {
            java.util.List<String> referents = com.legend.compiler.ResolvedNames.referents(af);
            for (FoldOp op : values()) {
                for (String r : referents) {
                    if (op.fqns.contains(r)) {
                        return op;
                    }
                }
            }
            return null;
        }
    }

    private @com.legend.Nullable Object evalCall(AppliedFunction af, Map<String, Object> scope) {
        List<ValueSpecification> ps = af.parameters();
        FoldOp op = FoldOp.of(af);
        if (op == null) {
            // not a native the folder evaluates by a row: a BODIED library
            // function evaluates by its body (removeAll: filter + contains)
            return evalUserCall(af, scope);
        }
        switch (op) {
            // arithmetic is VARIADIC (upstream's plus(Number[*]) & co.): the
            // infix run is the parser's one-collection carrier, whose
            // operands fold — sum / concatenation, left-fold subtraction
            // (one operand negates), product
            case PLUS -> {
                List<Object> args = evalAll(operands(af), scope);
                if (args == null) {
                    return null;
                }
                if (args.stream().allMatch(a -> a instanceof String)) {
                    StringBuilder sb = new StringBuilder();
                    args.forEach(a -> sb.append((String) a));
                    return sb.toString();
                }
                if (args.stream().allMatch(a -> a instanceof Long)) {
                    return args.stream().mapToLong(a -> (Long) a).sum();
                }
                return null;
            }
            case MINUS -> {
                List<Object> args = evalAll(operands(af), scope);
                if (args == null || !args.stream().allMatch(a -> a instanceof Long)) {
                    return null;
                }
                if (args.size() == 1) {
                    return -(Long) args.get(0);
                }
                long acc = (Long) args.get(0);
                for (int i = 1; i < args.size(); i++) {
                    acc -= (Long) args.get(i);
                }
                return acc;
            }
            case PAIR -> {
                List<Object> args = evalAll(ps, scope);
                return args != null && args.size() == 2
                        ? new Pair(args.get(0), args.get(1)) : null;
            }
            // zip over two static lists: the pairs by position, to the
            // shorter length (the tdsExtension programs pair their column
            // lists with their output-column lists — iqrClassify/zScore:
            // `$cols->zip($outputCols)->map(colPair|…)`; batch 76)
            case ZIP -> {
                if (ps.size() != 2) {
                    return null;
                }
                List<Object> a = evalList(ps.get(0), scope);
                List<Object> b = evalList(ps.get(1), scope);
                if (a == null || b == null) {
                    return null;
                }
                List<Object> out = new ArrayList<>();
                for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
                    out.add(new Pair(a.get(i), b.get(i)));
                }
                return out;
            }
            case EQUAL -> {
                List<Object> args = evalAll(ps, scope);
                return args != null && args.size() == 2
                        ? staticEquals(args.get(0), args.get(1)) : null;
            }
            case NOT -> {
                Object a = ps.size() == 1 ? eval(ps.get(0), scope) : null;
                return a instanceof Boolean b ? !b : null;
            }
            // ledger cluster 19 part (1) — the cheap-probe slice of the
            // NormalizeRequired fold vocabulary:
            case TO_ONE_MANY -> {
                // identity on the evaluated list (multiplicity assertion)
                Object a = ps.size() == 1 ? eval(ps.get(0), scope) : null;
                return a instanceof List<?> l && !l.isEmpty() ? a : null;
            }
            case TO_STRING -> {
                Object a = ps.size() == 1 ? eval(ps.get(0), scope) : null;
                return a instanceof String || a instanceof Long
                        || a instanceof Boolean || a instanceof Double
                        ? String.valueOf(a) : null;
            }
            // isEmpty/isNotEmpty over a foldable value: an if() whose
            // condition is isEmpty([]) must fold so the DEAD branch never
            // reaches the Typer (adjudication ledger cluster 2 — joinWith-
            // OptionalColumns' else-branch is ill-typed when $cols is []).
            case IS_EMPTY, IS_NOT_EMPTY -> {
                Object a = ps.size() == 1 ? eval(ps.get(0), scope) : null;
                if (a == null) {
                    return null;
                }
                boolean empty = a instanceof List<?> l && l.isEmpty();
                return op == FoldOp.IS_EMPTY ? empty : !empty;
            }
            case IN -> {
                if (ps.size() != 2) {
                    return null;
                }
                Object x = eval(ps.get(0), scope);
                List<Object> coll = evalList(ps.get(1), scope);
                return x == null || coll == null ? null : coll.contains(x);
            }
            case CONCATENATE -> {
                List<Object> args = evalAll(ps, scope);
                return args;   // evalAll already flattens collections
            }
            case REMOVE_DUPLICATES -> {
                List<Object> coll = ps.size() == 1 ? evalList(ps.get(0), scope) : null;
                return coll == null ? null : new ArrayList<>(new LinkedHashSet<>(coll));
            }
            case CONTAINS -> {
                // the platform's native (lowered to SQL IN), evaluated statically
                // over static values; the comparator overload is not static
                if (ps.size() != 2) {
                    return null;
                }
                List<Object> coll = evalList(ps.get(0), scope);
                Object x = eval(ps.get(1), scope);
                return coll == null || x == null ? null : coll.contains(x);
            }
            case INDEX_OF -> {
                if (ps.size() != 2) {
                    return null;
                }
                List<Object> coll = evalList(ps.get(0), scope);
                Object x = eval(ps.get(1), scope);
                return coll == null || x == null ? null : (long) coll.indexOf(x);
            }
            case MAP -> {
                if (ps.size() != 2 || !(ps.get(1) instanceof LambdaFunction lam)
                        || lam.parameters().size() != 1) {
                    return null;
                }
                List<Object> coll = evalList(ps.get(0), scope);
                if (coll == null) {
                    return null;
                }
                List<Object> out = new ArrayList<>(coll.size());
                for (Object e : coll) {
                    Object r = evalWith(lam, e, scope);
                    if (r == null) {
                        return null;
                    }
                    flattenInto(out, r);
                }
                return out;
            }
            case FILTER -> {
                if (ps.size() != 2 || !(ps.get(1) instanceof LambdaFunction lam)
                        || lam.parameters().size() != 1) {
                    return null;
                }
                List<Object> coll = evalList(ps.get(0), scope);
                if (coll == null) {
                    return null;
                }
                List<Object> out = new ArrayList<>();
                for (Object e : coll) {
                    Object keep = evalWith(lam, e, scope);
                    if (!(keep instanceof Boolean b)) {
                        return null;
                    }
                    if (b) {
                        out.add(e);
                    }
                }
                return out;
            }
            case SORT_BY -> {
                if (ps.size() != 2 || !(ps.get(1) instanceof LambdaFunction lam)
                        || lam.parameters().size() != 1) {
                    return null;
                }
                List<Object> coll = evalList(ps.get(0), scope);
                if (coll == null) {
                    return null;
                }
                List<Map.Entry<Object, Object>> keyed = new ArrayList<>(coll.size());
                for (Object e : coll) {
                    Object k = evalWith(lam, e, scope);
                    if (!(k instanceof Comparable)) {
                        return null;
                    }
                    keyed.add(Map.entry(e, k));
                }
                @SuppressWarnings({"unchecked", "rawtypes"})
                Comparator<Map.Entry<Object, Object>> cmp =
                        Comparator.comparing(en -> (Comparable) en.getValue());
                keyed.sort(cmp);
                return keyed.stream().map(Map.Entry::getKey).toList();
            }
            case IF -> {
                if (ps.size() == 3 && eval(ps.get(0), scope) instanceof Boolean c
                        && ps.get(1) instanceof LambdaFunction thenL
                        && ps.get(2) instanceof LambdaFunction elseL) {
                    LambdaFunction taken = c ? thenL : elseL;
                    return taken.body().size() == 1
                            ? eval(taken.body().get(0), scope) : null;
                }
                return null;
            }
            case AND, OR -> {
                List<Object> args = evalAll(ps, scope);
                if (args == null || !args.stream().allMatch(a -> a instanceof Boolean)) {
                    return null;
                }
                boolean and = op == FoldOp.AND;
                return args.stream().map(a -> (Boolean) a)
                        .reduce(and, (x, y) -> and ? x && y : x || y);
            }
            case MAKE_STRING, JOIN_STRINGS -> {
                if (ps.isEmpty() || ps.size() > 2) {
                    return null;
                }
                List<Object> coll = evalList(ps.get(0), scope);
                Object sep = ps.size() == 2 ? eval(ps.get(1), scope) : "";
                if (coll == null || !(sep instanceof String s)) {
                    return null;
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < coll.size(); i++) {
                    String piece = stringify(coll.get(i));
                    if (piece == null) {
                        return null;
                    }
                    sb.append(i > 0 ? s : "").append(piece);
                }
                return sb.toString();
            }
            case ELEMENT_TO_PATH -> {
                Object a = ps.size() == 1 ? eval(ps.get(0), scope) : null;
                // primitives' path IS the simple name (Integer, String…)
                return a instanceof TypeToken t ? t.simpleName() : null;
            }
            case TO_ONE, AT -> {
                if (op == FoldOp.TO_ONE && ps.size() == 1) {
                    Object a = eval(ps.get(0), scope);
                    return a instanceof List<?> l && l.size() == 1 ? l.get(0) : a;
                }
                // toOne(value, message): the ASSERTING spelling unwraps
                // when statically singular (ledger cluster 19 part 1)
                if (op == FoldOp.TO_ONE && ps.size() == 2) {
                    Object a = eval(ps.get(0), scope);
                    if (a instanceof List<?> l) {
                        return l.size() == 1 ? l.get(0) : null;
                    }
                    return a;
                }
                if (ps.size() == 2 && eval(ps.get(1), scope) instanceof Long ix) {
                    List<Object> coll = evalList(ps.get(0), scope);
                    return coll != null && ix >= 0 && ix < coll.size()
                            ? coll.get((int) (long) ix) : null;
                }
                return null;
            }
            default -> {
                return null;
            }
        }
    }

    /** A single-expression lambda's body; multi-statement bodies fold via
     * {@link SourceSubst#inlineLets} first, loud when that fails. */
    private static ValueSpecification single(LambdaFunction lam) {
        if (lam.body().size() == 1) {
            return lam.body().get(0);
        }
        LambdaFunction folded = SourceSubst.inlineLets(lam);
        if (folded == null) {
            throw new TypeInferenceException(
                    "static fold: multi-statement lambda with non-let statements");
        }
        return folded.body().get(0);
    }

    private @com.legend.Nullable Object evalWith(LambdaFunction lam, Object arg, Map<String, Object> scope) {
        if (lam.body().size() != 1) {
            return null;
        }
        Map<String, Object> inner = new LinkedHashMap<>(scope);
        inner.put(lam.parameters().get(0).name(), arg);
        return eval(lam.body().get(0), inner);
    }

    /** The operands of an operator application: the n-ary carrier's run
     *  ({@code plus[Collection[a,b]]}) or the parameters themselves. */
    private static List<ValueSpecification> operands(AppliedFunction af) {
        return af.parameters().size() == 1
                && af.parameters().get(0) instanceof PureCollection run
                ? run.values() : af.parameters();
    }

    private @com.legend.Nullable List<Object> evalAll(List<ValueSpecification> ps, Map<String, Object> scope) {
        List<Object> out = new ArrayList<>(ps.size());
        for (ValueSpecification p : ps) {
            Object e = eval(p, scope);
            if (e == null) {
                return null;
            }
            flattenInto(out, e);
        }
        return out;
    }

    private @com.legend.Nullable List<Object> evalList(ValueSpecification v, Map<String, Object> scope) {
        Object e = eval(v, scope);
        return switch (e) {
            case List<?> l -> new ArrayList<>(l);
            case null -> null;
            default -> {
                List<Object> one = new ArrayList<>(1);
                one.add(e);
                yield one;
            }
        };
    }

    private static @com.legend.Nullable String stringify(@com.legend.Nullable Object v) {
        return switch (v) {
            case String s -> s;
            case Long l -> String.valueOf(l);
            case Double d -> String.valueOf(d);
            case Boolean b -> String.valueOf(b);
            case TypeToken t -> t.simpleName();
            case null, default -> null;
        };
    }

    private static Boolean staticEquals(Object a, Object b) {
        if (a instanceof TypeToken || b instanceof TypeToken) {
            String an = a instanceof TypeToken t ? t.simpleName() : String.valueOf(a);
            String bn = b instanceof TypeToken t ? t.simpleName() : String.valueOf(b);
            return an.equals(bn);
        }
        return a.equals(b);
    }

    private static void flattenInto(List<Object> out, Object e) {
        if (e instanceof List<?> l) {
            out.addAll(l);
        } else {
            out.add(e);
        }
    }

    // =====================================================================
    // reify — static value back to a literal AST (null = keep the AST)
    // =====================================================================

    private static @com.legend.Nullable ValueSpecification reify(@com.legend.Nullable Object v) {
        return switch (v) {
            case String s -> new CString(s);
            case Long l -> new CInteger(l);
            case Double d -> new com.legend.protocol.spec.CFloat(d);
            case Boolean b -> new CBoolean(b);
            case Pair p -> {
                ValueSpecification f = reify(p.first());
                ValueSpecification s = reify(p.second());
                yield f == null || s == null ? null
                        : new AppliedFunction("pair", List.of(f, s));
            }
            case List<?> l -> {
                List<ValueSpecification> vs = new ArrayList<>(l.size());
                for (Object e : l) {
                    ValueSpecification r = reify(e);
                    if (r == null) {
                        yield null;
                    }
                    vs.add(r);
                }
                yield new PureCollection(vs);
            }
            case LambdaFunction lf -> lf;
            case null, default -> null;
        };
    }

    private static Map<String, Object> shadow(Map<String, Object> scope, List<Variable> params) {
        if (scope.isEmpty()) {
            return scope;
        }
        Map<String, Object> inner = new LinkedHashMap<>(scope);
        params.forEach(p -> inner.remove(p.name()));
        return inner;
    }

    private static String simple(String qn) {
        int cut = qn.lastIndexOf("::");
        return cut < 0 ? qn : qn.substring(cut + 2);
    }

}
