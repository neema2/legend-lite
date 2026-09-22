// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

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
    @com.legend.base.Nullable ValueSpecification foldToLiteral(ValueSpecification v) {
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
        if (com.legend.protocol.spec.AppliedFunction.isIf(af) && ps.size() == 3
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

    private @com.legend.base.Nullable ValueSpecification inlineUserCall(AppliedFunction af,
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
        LambdaFunction lets = SourceSubst.inlineLets(new LambdaFunction(List.of(),
                callee.body().orElseThrow()));
        if (lets == null) {
            return null;
        }
        ValueSpecification body = SourceSubst.substitute(
                typer.alphaRename(lets.body().get(0)), subst);
        inlining.push(callee.signatureKey());
        try {
            return fold(body, inner);
        } finally {
            inlining.pop();
        }
    }

    // =====================================================================
    // eval — the static interpreter (null = not static)
    // =====================================================================

    private @com.legend.base.Nullable Object eval(ValueSpecification v, Map<String, Object> scope) {
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

    private @com.legend.base.Nullable Object evalProperty(AppliedProperty ap, Map<String, Object> scope) {
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
    private @com.legend.base.Nullable List<Object> relationColumns(ValueSpecification receiver) {
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

    private @com.legend.base.Nullable Object evalCall(AppliedFunction af, Map<String, Object> scope) {
        List<ValueSpecification> ps = af.parameters();
        switch (af.function()) {
            // arithmetic is VARIADIC (upstream's plus(Number[*]) & co.): the
            // infix run is the parser's one-collection carrier, whose
            // operands fold — sum / concatenation, left-fold subtraction
            // (one operand negates), product
            case "plus" -> {
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
            case "minus" -> {
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
            case "pair" -> {
                List<Object> args = evalAll(ps, scope);
                return args != null && args.size() == 2
                        ? new Pair(args.get(0), args.get(1)) : null;
            }
            // zip over two static lists: the pairs by position, to the
            // shorter length (the tdsExtension programs pair their column
            // lists with their output-column lists — iqrClassify/zScore:
            // `$cols->zip($outputCols)->map(colPair|…)`; batch 76)
            case "zip" -> {
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
            case "equal" -> {
                List<Object> args = evalAll(ps, scope);
                return args != null && args.size() == 2
                        ? staticEquals(args.get(0), args.get(1)) : null;
            }
            case "not" -> {
                Object a = ps.size() == 1 ? eval(ps.get(0), scope) : null;
                return a instanceof Boolean b ? !b : null;
            }
            // ledger cluster 19 part (1) — the cheap-probe slice of the
            // NormalizeRequired fold vocabulary:
            case "toOneMany" -> {
                // identity on the evaluated list (multiplicity assertion)
                Object a = ps.size() == 1 ? eval(ps.get(0), scope) : null;
                return a instanceof List<?> l && !l.isEmpty() ? a : null;
            }
            case "toString" -> {
                Object a = ps.size() == 1 ? eval(ps.get(0), scope) : null;
                return a instanceof String || a instanceof Long
                        || a instanceof Boolean || a instanceof Double
                        ? String.valueOf(a) : null;
            }
            // isEmpty/isNotEmpty over a foldable value: an if() whose
            // condition is isEmpty([]) must fold so the DEAD branch never
            // reaches the Typer (adjudication ledger cluster 2 — joinWith-
            // OptionalColumns' else-branch is ill-typed when $cols is []).
            case "isEmpty", "isNotEmpty" -> {
                Object a = ps.size() == 1 ? eval(ps.get(0), scope) : null;
                if (a == null) {
                    return null;
                }
                boolean empty = a instanceof List<?> l && l.isEmpty();
                return af.function().equals("isEmpty") ? empty : !empty;
            }
            case "in" -> {
                if (ps.size() != 2) {
                    return null;
                }
                Object x = eval(ps.get(0), scope);
                List<Object> coll = evalList(ps.get(1), scope);
                return x == null || coll == null ? null : coll.contains(x);
            }
            case "concatenate" -> {
                List<Object> args = evalAll(ps, scope);
                return args;   // evalAll already flattens collections
            }
            case "removeDuplicates" -> {
                List<Object> coll = ps.size() == 1 ? evalList(ps.get(0), scope) : null;
                return coll == null ? null : new ArrayList<>(new LinkedHashSet<>(coll));
            }
            case "removeAll" -> {
                if (ps.size() != 2) {
                    return null;
                }
                List<Object> a = evalList(ps.get(0), scope);
                List<Object> b = evalList(ps.get(1), scope);
                if (a == null || b == null) {
                    return null;
                }
                List<Object> out = new ArrayList<>(a);
                out.removeAll(b);
                return out;
            }
            case "indexOf" -> {
                if (ps.size() != 2) {
                    return null;
                }
                List<Object> coll = evalList(ps.get(0), scope);
                Object x = eval(ps.get(1), scope);
                return coll == null || x == null ? null : (long) coll.indexOf(x);
            }
            case "map" -> {
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
            case "filter" -> {
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
            case "sortBy" -> {
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
            case "if" -> {
                if (ps.size() == 3 && eval(ps.get(0), scope) instanceof Boolean c
                        && ps.get(1) instanceof LambdaFunction thenL
                        && ps.get(2) instanceof LambdaFunction elseL) {
                    LambdaFunction taken = c ? thenL : elseL;
                    return taken.body().size() == 1
                            ? eval(taken.body().get(0), scope) : null;
                }
                return null;
            }
            case "and", "or" -> {
                List<Object> args = evalAll(ps, scope);
                if (args == null || !args.stream().allMatch(a -> a instanceof Boolean)) {
                    return null;
                }
                boolean and = af.function().equals("and");
                return args.stream().map(a -> (Boolean) a)
                        .reduce(and, (x, y) -> and ? x && y : x || y);
            }
            case "makeString", "joinStrings" -> {
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
            case "elementToPath" -> {
                Object a = ps.size() == 1 ? eval(ps.get(0), scope) : null;
                // primitives' path IS the simple name (Integer, String…)
                return a instanceof TypeToken t ? t.simpleName() : null;
            }
            case "toOne", "at" -> {
                if (af.function().equals("toOne") && ps.size() == 1) {
                    Object a = eval(ps.get(0), scope);
                    return a instanceof List<?> l && l.size() == 1 ? l.get(0) : a;
                }
                // toOne(value, message): the ASSERTING spelling unwraps
                // when statically singular (ledger cluster 19 part 1)
                if (af.function().equals("toOne") && ps.size() == 2) {
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

    private @com.legend.base.Nullable Object evalWith(LambdaFunction lam, Object arg, Map<String, Object> scope) {
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

    private @com.legend.base.Nullable List<Object> evalAll(List<ValueSpecification> ps, Map<String, Object> scope) {
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

    private @com.legend.base.Nullable List<Object> evalList(ValueSpecification v, Map<String, Object> scope) {
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

    private static @com.legend.base.Nullable String stringify(@com.legend.base.Nullable Object v) {
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

    private static @com.legend.base.Nullable ValueSpecification reify(@com.legend.base.Nullable Object v) {
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
