// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedCopyInstance;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMatchRuntime;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTypeRef;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * TIER 1 RECURSION (user green light 2026-09-03): a recursive Pure function
 * applied to a LITERAL instance tree unrolls at compile time — M2M
 * composition over a literal source. The inliner re-enters the recursive
 * call while its literal argument DESCENDS (strictly smaller each level,
 * so the unroll is well-founded with no depth constant), and these folds
 * decide, over literal structure only, what the body's operations mean: a
 * {@code match} picks its arm by the literal's class, a property read is
 * the literal's field, a cast over a literal of the target class is the
 * literal, and {@code map}/{@code filter}/{@code at}/{@code slice}/
 * {@code in}/… over a literal collection are their literal results. Every
 * fold is EXACT or ABSENT: {@link #fold} returns the node itself when the
 * unroll does not own it. Nothing here reads data — a literal is a value
 * the program spelled.
 *
 * <p>THE COMPILER COMPARES, THE DATABASE COMPUTES (docs/WORLD_MAP.md §4,
 * TENET_CHARTER Clause 6.2): every fold here decides something visible in
 * the program text — an arm by a literal's class, a spelled field, list
 * shape over a spelled list, identity of two spelled scalars of ONE kind,
 * a short-circuit on a spelled boolean. No fold produces a NEW value: a
 * {@code toLower}, a {@code +}, an arithmetic result is the database's and
 * stays in the tree as a residual the lowering spells as SQL. The fold set
 * is pinned by LiteralUnrollLedgerTest.
 */
final class LiteralUnroll {

    private LiteralUnroll() {
    }

    /** A value the compiler holds in full: an instance literal (its class)
     * or a scalar literal (its primitive/enum and value). */
    sealed interface Literal permits Instance, Scalar {
        String cls();
    }

    record Instance(String cls, TypedSpec node) implements Literal {
    }

    record Scalar(String cls, Object value) implements Literal {
    }

    static Optional<Literal> literal(TypedSpec s) {
        return switch (s) {
            case TypedNewInstance ni -> Optional.of(new Instance(ni.classFqn(), ni));
            case TypedCopyInstance cp -> literalStructure(cp.source())
                    ? Optional.of(new Instance(cp.classFqn(), cp)) : Optional.empty();
            case TypedCString c -> Optional.of(new Scalar(Type.Primitive.STRING.qualifiedName(), c.value()));
            case TypedCInteger i -> Optional.of(new Scalar(Type.Primitive.INTEGER.qualifiedName(),
                    i.value().longValue()));
            case TypedCBoolean b -> Optional.of(new Scalar(Type.Primitive.BOOLEAN.qualifiedName(), b.value()));
            case TypedCFloat f -> Optional.of(new Scalar(Type.Primitive.FLOAT.qualifiedName(), f.value()));
            case TypedEnumValue ev -> Optional.of(new Scalar(ev.enumFqn(), ev.enumFqn() + "." + ev.value()));
            case com.legend.compiler.spec.typed.TypedCDate d when d.info().type() instanceof Type.Primitive p ->
                    Optional.of(new Scalar(p.qualifiedName(), String.valueOf(d.value())));
            case com.legend.compiler.spec.typed.TypedCDecimal d when d.info().type() instanceof Type.Primitive p ->
                    Optional.of(new Scalar(p.qualifiedName(), d.value()));
            case com.legend.compiler.spec.typed.TypedCTime d when d.info().type() instanceof Type.Primitive p ->
                    Optional.of(new Scalar(p.qualifiedName(), String.valueOf(d.value())));
            case com.legend.compiler.spec.typed.TypedCLatestDate d when d.info().type() instanceof Type.Primitive p ->
                    Optional.of(new Scalar(p.qualifiedName(), "%latest"));
            // a spelled pair is an instance literal of Pair (first/second)
            case TypedNativeCall c when is(c, Pure.AT_COLLECTION_PAIR) && c.args().size() == 2
                    && c.args().stream().allMatch(LiteralUnroll::literalStructure) ->
                    Optional.of(new Instance(PlatformTypes.PAIR, c));
            default -> Optional.empty();
        };
    }

    /** A SPELLED list: a collection whose elements are each exactly one
     * value (any expression — lambdas, standing calls, store reads: their
     * multiplicity is [1..1]); its size and positions are structural. A
     * conditional-membership element (if(c,|e,|[]) — [0..1]) is not. */
    static boolean spelledList(TypedSpec s) {
        if (!(s instanceof TypedCollection tc)) {
            // a single SPELLED value IS a one-element list (pure's collection
            // model: `parameters = ^Literal(…)` spells one) — a computed
            // scalar is not (a query-level toOne/first over it keeps its
            // shape: the float canon keys on the wrapper)
            return (literal(s).isPresent() || s instanceof TypedLambda)
                    && s.info().multiplicity() instanceof Multiplicity.Bounded b
                    && b.lower() == 1 && Integer.valueOf(1).equals(b.upper());
        }
        for (TypedSpec e : tc.elements()) {
            if (e instanceof TypedCollection) {
                if (!spelledList(e)) {
                    return false;
                }
            } else if (!(e.info().multiplicity() instanceof Multiplicity.Bounded b)
                    || b.lower() != 1 || !Integer.valueOf(1).equals(b.upper())) {
                return false;
            }
        }
        return true;
    }

    /** A value whose STRUCTURE the compiler holds in full (a literal or a
     * collection of literal structures). */
    static boolean literalStructure(TypedSpec s) {
        return s instanceof TypedCollection tc
                ? tc.elements().stream().allMatch(LiteralUnroll::literalStructure)
                : literal(s).isPresent();
    }

    /** Node count of a literal structure — the unroll's descent measure. */
    static int size(TypedSpec s) {
        return switch (s) {
            case TypedNewInstance ni -> 1 + ni.properties().values().stream().mapToInt(LiteralUnroll::size).sum();
            case TypedCopyInstance cp -> 1 + size(cp.source())
                    + cp.overrides().values().stream().mapToInt(LiteralUnroll::size).sum();
            case TypedCollection tc -> tc.elements().stream().mapToInt(LiteralUnroll::size).sum();
            case TypedNativeCall c when is(c, Pure.AT_COLLECTION_PAIR) -> 1 + c.args().stream().mapToInt(LiteralUnroll::size).sum();
            default -> literal(s).isPresent() ? 1 : 0;
        };
    }

    private static boolean accepts(ModelContext ctx, String cls, String armType) {
        if (armType.equals(PlatformTypes.ANY) || armType.equals(cls)) {
            return true;
        }
        String simpleArm = armType.substring(armType.lastIndexOf(':') + 1);
        String simpleCls = cls.substring(cls.lastIndexOf(':') + 1);
        if (!armType.contains("::") && simpleArm.equals(simpleCls)) {
            return true;
        }
        // an enumeration VALUE is an instance of the m3 Enum metaclass
        if (armType.equals("meta::pure::metamodel::type::Enum") && ctx.findEnum(cls).isPresent()) {
            return true;
        }
        return ctx.isSubtype(cls, armType);
    }

    /** The literal field of an instance literal: the SPELLED value (or the
     * copy's override). A property the literal does not spell is not
     * folded — it may be derived, defaulted, or association-backed, and
     * the resolver owns those reads. */
    static Optional<TypedSpec> field(TypedSpec inst, String prop) {
        // a spelled enum's NAME is structural (WORLD_MAP §4)
        if (inst instanceof TypedEnumValue ev && prop.equals("name")) {
            return Optional.of(new com.legend.compiler.spec.typed.TypedCString(ev.value(),
                    new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE)));
        }
        // a field read over a spelled COLLECTION is the collection of the
        // elements' fields (pure's dot rule over [*]), when every element folds
        if (inst instanceof TypedCollection tc && literalStructure(tc) && !elements(tc).isEmpty()) {
            List<TypedSpec> out = new ArrayList<>();
            for (TypedSpec e : elements(tc)) {
                Optional<TypedSpec> f = field(e, prop);
                if (f.isEmpty()) {
                    return Optional.empty();
                }
                out.addAll(elements(f.get()));   // pure's dot rule CONCATENATES the elements' values
            }
            ExprType first = out.get(0).info();
            return Optional.of(new TypedCollection(out, new ExprType(first.type(),
                    Multiplicity.Bounded.ZERO_MANY)));
        }
        if (inst instanceof TypedNewInstance ni) {
            return Optional.ofNullable(ni.properties().get(prop));
        }
        if (inst instanceof TypedNativeCall c && is(c, Pure.AT_COLLECTION_PAIR) && c.args().size() == 2) {
            return prop.equals("first") ? Optional.of(c.args().get(0))
                    : prop.equals("second") ? Optional.of(c.args().get(1)) : Optional.empty();
        }
        if (inst instanceof TypedCopyInstance cp && literalStructure(cp.source())) {
            TypedSpec v = cp.overrides().get(prop);
            return v != null ? Optional.of(v) : field(cp.source(), prop);
        }
        return Optional.empty();
    }

    /** An UNSPELLED property of an instance literal is the class's default
     * — empty when its multiplicity admits empty and it declares no default
     * value (WORLD_MAP §4: "an unspelled field is the class's default/empty").
     * A declared default (whose expression the definition does not carry)
     * and an unknown class stay unfolded. */
    private static Optional<TypedSpec> unspelledDefault(TypedPropertyAccess pa, ModelContext ctx) {
        if (!(pa.source() instanceof TypedNewInstance ni)
                || !(pa.info().multiplicity() instanceof Multiplicity.Bounded b) || b.lower() != 0) {
            return Optional.empty();
        }
        Optional<com.legend.model.ClassDefinition.PropertyDefinition> decl =
                declaredProperty(ctx, ni.classFqn(), pa.property(), 0);
        if (decl.isEmpty() || decl.get().hasDefault()) {
            return Optional.empty();
        }
        return Optional.of(new TypedCollection(List.of(), pa.info()));
    }

    private static Optional<com.legend.model.ClassDefinition.PropertyDefinition> declaredProperty(
            ModelContext ctx, String cls, String prop, int depth) {
        if (depth > 32) {
            return Optional.empty();
        }
        Optional<com.legend.model.ClassDefinition> cd = ctx.findClassDefinition(cls);
        if (cd.isEmpty()) {
            return Optional.empty();
        }
        for (var p : cd.get().properties()) {
            if (p.name().equals(prop)) {
                return Optional.of(p);
            }
        }
        for (com.legend.protocol.TypeExpression sup : cd.get().superClasses()) {
            String name = sup instanceof com.legend.protocol.TypeExpression.NameRef nr ? nr.name()
                    : sup instanceof com.legend.protocol.TypeExpression.Generic g ? g.name() : null;
            if (name != null) {
                Optional<com.legend.model.ClassDefinition.PropertyDefinition> r =
                        declaredProperty(ctx, name, prop, depth + 1);
                if (r.isPresent()) {
                    return r;
                }
            }
        }
        return Optional.empty();
    }

    /** The FLAT element list of a literal structure — pure collections
     * are flat: a nested or empty literal collection compacts away
     * ({@code [[]->first(), 'a']} has one element). */
    static List<TypedSpec> elements(TypedSpec s) {
        if (!(s instanceof TypedCollection tc)) {
            return List.of(s);
        }
        List<TypedSpec> out = new ArrayList<>();
        for (TypedSpec e : tc.elements()) {
            out.addAll(elements(e));
        }
        return out;
    }

    /** Every element is a pair whose KEY is a spelled scalar (values may be
     * anything — lambdas, standing calls: a lookup never needs them). */
    private static boolean spelledKeys(TypedSpec pairs) {
        if (!(pairs instanceof TypedCollection)) {
            return false;
        }
        for (TypedSpec p : elements(pairs)) {
            if (!(p instanceof TypedNativeCall pc && is(pc, Pure.AT_COLLECTION_PAIR) && pc.args().size() == 2
                    && literal(pc.args().get(0)).filter(l -> l instanceof Scalar).isPresent())) {
                return false;
            }
        }
        return true;
    }

    /** The spelled scalar value of a literal, for the inliner's key folds. */
    static Optional<Object> scalarValue(TypedSpec s) {
        return scalar(s);
    }

    private static Optional<Object> scalar(TypedSpec s) {
        return literal(s).filter(l -> l instanceof Scalar).map(l -> ((Scalar) l).value());
    }

    private static TypedCollection sub(TypedCollection coll, int lo, int hi, ExprType info) {
        List<TypedSpec> el = elements(coll);
        int from = Math.max(0, lo);
        int to = Math.min(el.size(), hi);
        return new TypedCollection(from < to ? el.subList(from, to) : List.of(), info);
    }

    private static TypedCBoolean bool(boolean v) {
        return new TypedCBoolean(v, ExprType.one(Type.Primitive.BOOLEAN));
    }

    /**
     * One fold step over an already-rewritten node: the literal result when
     * the node is a literal-structure operation the unroll owns, else the
     * node ITSELF. Constructs that bind a parameter (match arms, map/filter
     * lambdas, if branches) are unrolled by the inliner BEFORE their bodies
     * are rewritten — see UserCallInliner.rewriteSwitch.
     */
    static TypedSpec fold(TypedSpec n, ModelContext ctx) {
        return switch (n) {
            case TypedPropertyAccess pa -> field(pa.source(), pa.property())
                    .or(() -> unspelledDefault(pa, ctx)).orElse(n);
            case TypedCast tc -> literal(tc.source())
                    .filter(lit -> tc.target() instanceof Type.ClassType target
                            && accepts(ctx, lit.cls(), target.fqn())
                            // a spelled SCALAR cast to its own primitive (or a
                            // supertype of it): `$lit.value->cast(@String)`
                            || lit instanceof Scalar && tc.target() instanceof Type.Primitive prim
                            && ctx.isSubtype(lit.cls(), prim.qualifiedName()))
                    .<TypedSpec>map(lit -> tc.source())
                    // a cast over a SPELLED collection is element-wise: the
                    // collection itself when every element is a literal the
                    // target accepts (toPostgresModel's converted-parameter
                    // lists, `->cast(@Expression)` before their fold); the
                    // EMPTY spelled collection is trivially every class's
                    .or(() -> tc.source() instanceof TypedCollection coll
                            && tc.target() instanceof Type.ClassType target
                            && literalStructure(coll)
                            && elements(coll).stream().allMatch(e -> literal(e)
                                    .filter(lit -> accepts(ctx, lit.cls(), target.fqn())).isPresent())
                            ? Optional.of(elements(coll).isEmpty()
                                    ? new TypedCollection(List.of(), tc.info()) : coll)
                            : Optional.empty())
                    .orElse(n);
            case TypedNativeCall c -> nativeFold(c, ctx);
            // a copy of a literal IS a literal: the source's fields with the
            // overrides applied (one instance shape downstream)
            case TypedCopyInstance cp when cp.source() instanceof TypedNewInstance src -> {
                java.util.Map<String, TypedSpec> props = new java.util.LinkedHashMap<>(src.properties());
                props.putAll(cp.overrides());
                yield new TypedNewInstance(cp.classFqn(), props, cp.info());
            }
            // a WELL-FORMED spelled slice only: an inverted range is the
            // engine's error, raised by the lowering (PCT testSliceError)
            case TypedSlice sl when sl.source() instanceof TypedCollection coll && literalStructure(coll)
                    && sl.start() instanceof TypedCInteger lo && sl.stop() instanceof TypedCInteger hi
                    && lo.value().intValue() >= 0 && lo.value().intValue() <= hi.value().intValue() ->
                    sub(coll, lo.value().intValue(), hi.value().intValue(), sl.info());
            case TypedLimit lm when lm.source() instanceof TypedCollection coll && literalStructure(coll)
                    && lm.count() instanceof TypedCInteger k ->
                    sub(coll, 0, k.value().intValue(), lm.info());
            case TypedDrop dr when dr.source() instanceof TypedCollection coll && literalStructure(coll)
                    && dr.count() instanceof TypedCInteger k ->
                    sub(coll, k.value().intValue(), Integer.MAX_VALUE, dr.info());
            case TypedConcatenate cc when literalStructure(cc.left()) && literalStructure(cc.right()) -> {
                List<TypedSpec> out = new ArrayList<>(elements(cc.left()));
                out.addAll(elements(cc.right()));
                yield new TypedCollection(out, cc.info());
            }
            default -> n;
        };
    }

    /** The arm a literal input dispatches to (the inliner applies it
     * before the arms are rewritten); empty for a non-literal input. */
    static Optional<TypedMatchRuntime.Arm> arm(TypedMatchRuntime mr, TypedSpec input, ModelContext ctx) {
        return literal(input).flatMap(lit -> mr.arms().stream()
                .filter(a -> accepts(ctx, lit.cls(), a.typeFqn())).findFirst());
    }

    /** Whether {@code c}'s callee is one of the declarations in {@code groups}. */
    @SafeVarargs
    private static boolean is(TypedNativeCall c, java.util.List<com.legend.model.FunctionId>... groups) {
        com.legend.model.FunctionId id = c.callee().id();
        for (java.util.List<com.legend.model.FunctionId> g : groups) {
            if (g.contains(id)) {
                return true;
            }
        }
        return false;
    }

    /** {@code equal}/{@code eq} over spelled operands — element references,
     * TDS null carriers, same-kind scalars; anything else stays the
     * database's compare. */
    private static TypedSpec equalityFold(TypedNativeCall c, List<TypedSpec> a) {
        // ELEMENT references (a class or enumeration used as a value —
        // `$v != TDSNull` over `[TDSNull, 1, 2]`): two references are
        // equal exactly when they name the same element; a reference
        // against a scalar literal is never equal (pure: different
        // kinds — compare-only, no value is computed)
        if (a.get(0) instanceof TypedPackageableRef lr
                && a.get(1) instanceof TypedPackageableRef rr) {
            return bool(lr.fullPath().equals(rr.fullPath()));
        }
        // the TDS null cell: the engine's TDSNull is ONE constant, so two
        // null carriers (^TDSNull() / the bare-ref sqlNull() funnel) are
        // equal and a scalar literal never equals it (tds.pure
        // firstNotNull: `[TDSNull, TDSNull]->filter(v | $v != TDSNull)` is [])
        boolean ln = isTdsNullCarrier(a.get(0));
        boolean rn = isTdsNullCarrier(a.get(1));
        if (ln && rn) {
            return bool(true);
        }
        if ((ln && literal(a.get(1)).filter(r -> r instanceof Scalar).isPresent())
                || (rn && literal(a.get(0)).filter(l -> l instanceof Scalar).isPresent())) {
            return bool(false);
        }
        if ((a.get(0) instanceof TypedPackageableRef
                        && literal(a.get(1)).filter(r -> r instanceof Scalar).isPresent())
                || (a.get(1) instanceof TypedPackageableRef
                        && literal(a.get(0)).filter(l -> l instanceof Scalar).isPresent())) {
            return bool(false);
        }
        return literal(a.get(0)).flatMap(l -> literal(a.get(1))
                .filter(r -> l instanceof Scalar && r instanceof Scalar && l.cls().equals(r.cls()))
                .<TypedSpec>map(r -> bool(((Scalar) l).value().equals(((Scalar) r).value()))))
                .orElse(c);
    }

    /** The TDS null cell in either spelling: the instance {@code ^TDSNull()}
     * or the bare reference's {@code sqlNull()} funnel (Typer). */
    private static boolean isTdsNullCarrier(TypedSpec s) {
        return (s instanceof TypedNewInstance ni
                        && ni.classFqn().equals(com.legend.compiler.element.type.PlatformTypes.TDS_NULL_FQN))
                || (s instanceof TypedNativeCall c && c.args().isEmpty() && is(c, Pure.AT_RELATIONAL_FUNCTIONS_SQL_QUERY_TO_STRING_SQL_NULL));
    }

    private static TypedSpec nativeFold(TypedNativeCall c, ModelContext ctx) {
        List<TypedSpec> a = c.args();
        // SPELLED-INTEGER compares (WORLD_MAP §4: "size() == 1" and kin) —
        // same-kind identity over two spelled integers, never a new value
        if ((is(c, Pure.AT_BOOLEAN_GREATER_THAN) || is(c, Pure.AT_BOOLEAN_LESS_THAN) || is(c, Pure.AT_BOOLEAN_GREATER_THAN_EQUAL)
                || is(c, Pure.AT_BOOLEAN_LESS_THAN_EQUAL)) && a.size() == 2
                && a.get(0) instanceof TypedCInteger l && a.get(1) instanceof TypedCInteger r) {
            long x = l.value().longValue();
            long y = r.value().longValue();
            return bool(is(c, Pure.AT_BOOLEAN_GREATER_THAN) ? x > y : is(c, Pure.AT_BOOLEAN_LESS_THAN) ? x < y
                    : is(c, Pure.AT_BOOLEAN_GREATER_THAN_EQUAL) ? x >= y : x <= y);
        }
        // LIST SHAPE over a spelled collection (WORLD_MAP §4): its size, and
        // membership of a spelled scalar (same-kind identity, as `in`)
        if (is(c, Pure.AT_RELATION_SIZE, Pure.AT_COLLECTION_SIZE) && a.size() == 1 && spelledList(a.get(0))) {
            return new com.legend.compiler.spec.typed.TypedCInteger(elements(a.get(0)).size(),
                    new ExprType(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE));
        }
        if (is(c, Pure.AT_COLLECTION_DEFAULT_IF_EMPTY) && a.size() == 2 && spelledList(a.get(0))) {
            return elements(a.get(0)).isEmpty() ? a.get(1) : a.get(0);
        }
        // the COLLECTION overload only: pure's [x] == x law resolves
        // ['ACTIVE']->contains('TIV') to string::contains (substring)
        if (is(c, Pure.AT_COLLECTION_CONTAINS, Pure.AT_STRING_CONTAINS)
                && c.callee().qualifiedName().equals("meta::pure::functions::collection::contains")
                && a.size() == 2 && literalStructure(a.get(0))) {
            Optional<Literal> needle = literal(a.get(1)).filter(l -> l instanceof Scalar);
            if (needle.isPresent()) {
                Scalar n = (Scalar) needle.get();
                boolean allScalar = elements(a.get(0)).stream()
                        .allMatch(e -> literal(e).filter(l -> l instanceof Scalar
                                && l.cls().equals(n.cls())).isPresent());
                if (allScalar) {
                    return bool(elements(a.get(0)).stream().anyMatch(e ->
                            literal(e).map(l -> ((Scalar) l).value().equals(n.value())).orElse(false)));
                }
            }
        }
        // isTrue over a spelled boolean or a spelled empty
        if (is(c, Pure.AT_BOOLEAN_IS_TRUE) && a.size() == 1) {
            if (a.get(0) instanceof TypedCBoolean b) {
                return bool(b.value());
            }
            if (a.get(0) instanceof TypedCollection tc && literalStructure(tc) && elements(tc).isEmpty()) {
                return bool(false);
            }
        }
        // a spelled boolean's assert is a no-op (assert(true, …)); a false
        // one raises and stays the database's
        if (com.legend.builtin.NativeFn.Verdict.of(c.callee().id()).orElse(null) == com.legend.builtin.NativeFn.Verdict.ASSERT && !a.isEmpty() && a.get(0) instanceof TypedCBoolean tb && tb.value()) {
            return bool(true);
        }
        // an enumeration's values are its declaration (spelled)
        if (com.legend.builtin.NativeFn.LiteralForm.of(c.callee().id()).orElse(null) == com.legend.builtin.NativeFn.LiteralForm.ENUM_VALUES && a.size() == 1) {
            Optional<String> fqn = switch (a.get(0)) {
                case TypedTypeRef tr -> tr.target() instanceof Type.EnumType et
                        ? Optional.of(et.fqn()) : Optional.empty();
                case TypedPackageableRef pr -> Optional.of(pr.fullPath());
                default -> Optional.empty();
            };
            Optional<com.legend.compiler.element.TypedEnum> en = fqn.flatMap(ctx::findEnum);
            if (en.isPresent()) {
                ExprType one = new ExprType(new Type.EnumType(en.get().qualifiedName()),
                        Multiplicity.Bounded.ONE);
                return new TypedCollection(en.get().values().stream()
                        .<TypedSpec>map(v -> new TypedEnumValue(en.get().qualifiedName(), v, one))
                        .toList(), c.info());
            }
        }
        // dynamicNew(Class, [^KeyValue(key, value)…]) over spelled keys IS the
        // instance literal ^Class(key = value, …)
        if (com.legend.builtin.NativeFn.LiteralForm.of(c.callee().id()).orElse(null) == com.legend.builtin.NativeFn.LiteralForm.DYNAMIC_NEW && a.size() == 2 && literalStructure(a.get(1))) {
            String cls = typeTargetFqn(a.get(0)).orElse(null);
            java.util.Map<String, TypedSpec> props = new java.util.LinkedHashMap<>();
            boolean spelled = cls != null;
            for (TypedSpec kv : elements(a.get(1))) {
                TypedSpec value = kv instanceof TypedNewInstance ni ? ni.properties().get("value") : null;
                if (kv instanceof TypedNewInstance ni && ni.properties().get("key") instanceof TypedCString k
                        && value != null) {
                    props.put(k.value(), value);
                } else {
                    spelled = false;
                }
            }
            if (spelled && cls != null) {
                return new TypedNewInstance(cls, props,
                        new ExprType(new Type.ClassType(cls), Multiplicity.Bounded.ONE));
            }
        }
        // SPELLED MAPS: newMap over spelled pairs is a structure the compiler
        // holds — its key/value pairs, and a lookup by a spelled key
        if (com.legend.builtin.NativeFn.LiteralForm.of(c.callee().id()).orElse(null) == com.legend.builtin.NativeFn.LiteralForm.KEY_VALUES && a.size() == 1 && a.get(0) instanceof TypedNativeCall nm
                && is(nm, Pure.AT_COLLECTION_NEW_MAP) && nm.args().size() == 1 && spelledKeys(nm.args().get(0))) {
            return new TypedCollection(elements(nm.args().get(0)), c.info());
        }
        if (is(c, Pure.AT_VARIANT_NAVIGATION_GET, Pure.AT_COLLECTION_GET) && a.size() == 2 && a.get(0) instanceof TypedNativeCall nm
                && is(nm, Pure.AT_COLLECTION_NEW_MAP) && nm.args().size() == 1 && spelledKeys(nm.args().get(0))) {
            Optional<Literal> key = literal(a.get(1)).filter(l -> l instanceof Scalar);
            if (key.isPresent()) {
                Scalar k = (Scalar) key.get();
                for (TypedSpec p : elements(nm.args().get(0))) {
                    Optional<Literal> pk = field(p, "first").flatMap(LiteralUnroll::literal);
                    if (pk.isEmpty() || !(pk.get() instanceof Scalar ps)) {
                        return c;   // an unspelled key: the database's lookup
                    }
                    if (ps.cls().equals(k.cls()) && ps.value().equals(k.value())) {
                        return field(p, "second").orElse(c);
                    }
                }
                return new TypedCollection(List.of(), c.info());
            }
        }
        if (is(c, Pure.AT_META_INSTANCE_OF) && a.size() == 2) {
            return literal(a.get(0)).flatMap(lit -> typeTargetFqn(a.get(1))
                    .<TypedSpec>map(fqn -> bool(accepts(ctx, lit.cls(), fqn)))).orElse(c);
        }
        // assertInstanceOf(literal, T) over a CONFORMING literal is the
        // spelled-true assert (a non-conforming one raises — the database's)
        if (is(c, Pure.AT_ASSERTS_ASSERT_INSTANCE_OF) && a.size() >= 2) {
            return literal(a.get(0)).flatMap(lit -> typeTargetFqn(a.get(1))
                    .filter(fqn -> accepts(ctx, lit.cls(), fqn))
                    .<TypedSpec>map(fqn -> bool(true))).orElse(c);
        }
        // scalar equality of the SAME kind only: a cross-kind compare
        // (1 == 1.0) is the database's verdict (SQL numeric coercion —
        // EqualityWorldsConformanceTest's declared divergence)
        if ((is(c, Pure.AT_BOOLEAN_EQUAL) || is(c, Pure.AT_BOOLEAN_EQ)) && a.size() == 2) {
            return equalityFold(c, a);
        }
        if (is(c, Pure.AT_BOOLEAN_NOT) && a.size() == 1 && a.get(0) instanceof TypedCBoolean b) {
            return bool(!b.value());
        }
        // and/or: both literal, or the short-circuit side literal (pure
        // semantics: the right operand is not evaluated)
        if ((is(c, Pure.AT_BOOLEAN_AND, Pure.AT_COLLECTION_AND) || is(c, Pure.AT_BOOLEAN_OR, Pure.AT_COLLECTION_OR)) && a.size() == 2) {
            boolean isAnd = is(c, Pure.AT_BOOLEAN_AND, Pure.AT_COLLECTION_AND);
            if (a.get(0) instanceof TypedCBoolean l) {
                if (l.value() != isAnd) {
                    return bool(l.value());
                }
                return a.get(1);
            }
            if (a.get(1) instanceof TypedCBoolean r && r.value() == isAnd) {
                return a.get(0);
            }
            return c;
        }
        if (is(c, Pure.AT_COLLECTION_IN, Pure.AT_RELATION_IN) && a.size() == 2 && a.get(1) instanceof TypedCollection coll
                && coll.elements().stream().allMatch(e -> scalar(e).isPresent())) {
            return scalar(a.get(0)).<TypedSpec>map(needle -> bool(coll.elements().stream()
                    .anyMatch(e -> scalar(e).filter(needle::equals).isPresent()))).orElse(c);
        }
        if ((is(c, Pure.AT_COLLECTION_IS_EMPTY) || is(c, Pure.AT_COLLECTION_IS_NOT_EMPTY)) && a.size() == 1
                && spelledList(a.get(0))) {
            boolean empty = elements(a.get(0)).isEmpty();
            return bool(is(c, Pure.AT_COLLECTION_IS_EMPTY) == empty);
        }
        if (is(c, Pure.AT_COLLECTION_AT) && a.size() == 2 && spelledList(a.get(0))
                && a.get(1) instanceof TypedCInteger k
                && k.value().intValue() >= 0 && k.value().intValue() < elements(a.get(0)).size()) {
            return elements(a.get(0)).get(k.value().intValue());
        }
        // toOne / toOneMany of a SPELLED one (a lambda from a spelled map
        // lookup, an instance literal): the value itself — never a computed
        // expression (a query-level toOne over an aggregate keeps its shape;
        // the float canon of calendarAggregations keys on it)
        if ((is(c, Pure.AT_MULTIPLICITY_TO_ONE) || is(c, Pure.AT_MULTIPLICITY_TO_ONE_MANY)) && a.size() >= 1
                && (a.get(0) instanceof TypedLambda || a.get(0) instanceof TypedNewInstance)
                && a.get(0).info().multiplicity() instanceof Multiplicity.Bounded ob
                && ob.lower() == 1 && Integer.valueOf(1).equals(ob.upper())) {
            return a.get(0);
        }
        // first/last of the EMPTY spelled list is the empty list (pure:
        // `[]->first()` is []); toOne over it is an error and stays
        if ((is(c, Pure.AT_RELATION_FIRST, Pure.AT_COLLECTION_FIRST) || is(c, Pure.AT_RELATION_LAST, Pure.AT_COLLECTION_LAST)) && a.size() == 1
                && spelledList(a.get(0)) && elements(a.get(0)).isEmpty()) {
            // element type from the ARGUMENT (a generic callee's `T[0..1]`
            // stamp never reaches the lowering)
            ExprType ei = c.info().type() instanceof Type.TypeVar
                    ? new ExprType(a.get(0).info().type(), c.info().multiplicity()) : c.info();
            return new TypedCollection(List.of(), ei);
        }
        if ((is(c, Pure.AT_MULTIPLICITY_TO_ONE) || is(c, Pure.AT_MULTIPLICITY_TO_ONE_MANY) || is(c, Pure.AT_RELATION_FIRST, Pure.AT_COLLECTION_FIRST) || is(c, Pure.AT_RELATION_LAST, Pure.AT_COLLECTION_LAST))
                && a.size() >= 1 && spelledList(a.get(0)) && !elements(a.get(0)).isEmpty()) {
            List<TypedSpec> el = elements(a.get(0));
            if (is(c, Pure.AT_MULTIPLICITY_TO_ONE_MANY)) {
                return a.get(0);
            }
            if (el.size() == 1 || is(c, Pure.AT_RELATION_FIRST, Pure.AT_COLLECTION_FIRST)) {
                return el.get(0);
            }
            if (is(c, Pure.AT_RELATION_LAST, Pure.AT_COLLECTION_LAST)) {
                return el.get(el.size() - 1);
            }
            return c;
        }
        // the concatenation of two SPELLED lists is list shape (WORLD_MAP
        // §4): the elements in order, whatever each element is — a
        // preOrderTraversal's `$r->concatenate($r->children()->map(..))`
        // over a spelled tree is the spelled node list (the TypedConcatenate
        // node form folds above; this is the plain native call)
        if (is(c, Pure.AT_COLLECTION_CONCATENATE, Pure.AT_RELATION_CONCATENATE) && a.size() == 2) {
            if (spelledList(a.get(0)) && spelledList(a.get(1))) {
                List<TypedSpec> out = new ArrayList<>(elements(a.get(0)));
                out.addAll(elements(a.get(1)));
                return new TypedCollection(out, c.info());
            }
            // an EMPTY spelled side is the identity: the other side, whatever
            // it is (the value is unchanged; only the list shape was decided)
            if (spelledList(a.get(0)) && elements(a.get(0)).isEmpty()) {
                return a.get(1);
            }
            if (spelledList(a.get(1)) && elements(a.get(1)).isEmpty()) {
                return a.get(0);
            }
        }
        // zip over two SPELLED lists is list shape: the pairs by position,
        // to the shorter length (a convertJoinTreeNode's nodes zipped with
        // their converted relations); a pair is an instance literal
        if (is(c, Pure.AT_COLLECTION_ZIP) && a.size() == 2 && spelledList(a.get(0)) && spelledList(a.get(1))) {
            List<TypedSpec> l = elements(a.get(0));
            List<TypedSpec> r = elements(a.get(1));
            var pairFn = ctx.findFunction("meta::pure::functions::collection::pair").get(0);
            ExprType pairInfo = new ExprType(c.info().type(), Multiplicity.Bounded.ONE);
            List<TypedSpec> out = new ArrayList<>();
            for (int i = 0; i < Math.min(l.size(), r.size()); i++) {
                out.add(new TypedNativeCall(pairFn, List.of(l.get(i), r.get(i)), pairInfo));
            }
            return new TypedCollection(out, c.info());
        }
        // the tail of a spelled list is its shape minus the head
        // (toPostgresModel's binary-expression chains fold over it)
        if (is(c, Pure.AT_COLLECTION_TAIL) && a.size() == 1 && a.get(0) instanceof TypedCollection coll
                && spelledList(coll)) {
            return sub(coll, 1, Integer.MAX_VALUE, c.info());
        }
        // and its init is its shape minus the last (WORLD_MAP §4 lists
        // both; convertJoinStrings interleaves separators over init/last)
        if (is(c, Pure.AT_COLLECTION_INIT) && a.size() == 1 && a.get(0) instanceof TypedCollection coll
                && spelledList(coll)) {
            int n = elements(coll).size();
            return n == 0 ? new TypedCollection(List.of(), c.info())
                    : sub(coll, 0, n - 1, c.info());
        }
        return c;
    }

    private static Optional<String> typeTargetFqn(TypedSpec typeArg) {
        return switch (typeArg) {
            case TypedTypeRef tr -> tr.target() instanceof Type.ClassType c
                    ? Optional.of(c.fqn()) : Optional.empty();
            case TypedPackageableRef pr -> Optional.of(pr.fullPath());
            default -> Optional.empty();
        };
    }

}
