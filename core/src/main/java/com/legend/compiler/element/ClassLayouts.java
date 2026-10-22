package com.legend.compiler.element;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The CANONICAL value layout of a class — the field list an instance VALUE
 * carries when it lowers to a SQL struct. The layout is the MODEL's, never
 * the instance's: declared stored properties in declaration order, inherited
 * first (superclass walk), with a parameterized receiver's type arguments
 * substituted into generic property types. Two instances of one class always
 * share one layout; an instance that omits an optional property still has the
 * field (NULL-valued).
 *
 * <p>Classes with no stored properties (the body-less native carriers:
 * List, SortInfo, _Window, …) have NO layout — {@link Optional#empty()} —
 * so they keep hitting the loud lowering walls rather than becoming empty
 * structs.
 */
public final class ClassLayouts {

    /** F13 — the ONE owner of the synthetic identity field's spelling:
     * the {@code __id} column an identity layout appends to a keyless
     * class (every producer and consumer compares against this). */
    public static final String SYNTHETIC_ID = "__id";
    /** WORLD_MAP §4 — the synthetic CLASS field beside the identity: a
     * value in a polymorphic slot (a {@code RelationalOperationElement[*]}
     * key carrying DynaFunctions and Literals) is judged by ITS OWN
     * class's keys, and the wire can only carry that class as data.
     * Rides the identity layouts only, like {@link #SYNTHETIC_ID}. */
    public static final String SYNTHETIC_TYPE = "__type";
    /** The synthetic CANON field (F10 proper — the kind-tagged carrier):
     * a constructed instance's canonical key text, computed at its
     * CONSTRUCTION site from its own fields (children are built before
     * parents, so a parent's canon references its children's — finite
     * for recursive polymorphic shapes where a static expansion is not)
     * and carried on the wire as JSON, so a JSON-carried nested instance
     * is judged by the classifier that built it. Rides the identity
     * layouts only, like {@link #SYNTHETIC_ID}. */
    public static final String SYNTHETIC_CANON = "__canon";

    private ClassLayouts() {
    }

    /** The layout of a class-typed VALUE, or empty when {@code t} is not a layoutable class. */
    public static Optional<List<Type.Column>> layoutOf(ModelContext ctx, Type t) {
        return layoutOf(ctx, t, false);
    }

    /**
     * F13/F13c — {@code withIdentity} appends the synthetic {@code __id}
     * identity field to a model class's layout: engine {@code eq()} is
     * INSTANCE IDENTITY for every non-primitive (keyed classes
     * included — keys govern {@code equal}, never {@code eq}), and a
     * keyless class's {@code equal} is identity too; the wire can only
     * carry identity as data. Identity layouts ride ONLY the verdict
     * lanes (canon rider + assert-condition sides) — the golden-SQL
     * text lanes and corpus value lanes keep the plain layout, so their
     * pinned texts never see the field. Platform CARRIERS (Pair/List/
     * Map) are excluded: their constructors short-circuit to fixed
     * carrier shapes before the layout, and an appended field would rip
     * the layout from the emitted value (the Executor's attr-count
     * wall). Every producer of an identity layout's struct owns the
     * field: constructor/copy sites MINT (each copy is a NEW instance);
     * store-mapped reads would project NULL (no such producer exists in
     * the identity lanes today — the attr-count wall guards it).
     */
    /** THE LAYOUT MEMO — a compile-time fact of the immutable model, cached
     * ON the model context (ModelContext.derived: the compile artifact,
     * never a static sink — instrument-state-matches-fact-lifetime). The
     * batch-170 profile: the lowering asked for the same class layouts on
     * every use, each a supertype walk per declared property
     * (isFunctionCarrier → isSubtype), 3× the samples of batch 164 once
     * the module's classes grew — the corpus lane 69 s → 83 s. Get-then-put,
     * not computeIfAbsent: a layout derivation reads other layouts. */
    private static final class LayoutMemo {
        final java.util.Map<Type, Optional<List<Type.Column>>> plain =
                new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.Map<Type, Optional<List<Type.Column>>> identity =
                new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.Map<Type, Optional<List<Type.Column>>> object =
                new java.util.concurrent.ConcurrentHashMap<>();
    }

    public static Optional<List<Type.Column>> layoutOf(ModelContext ctx, Type t,
                                                      boolean withIdentity) {
        LayoutMemo memo = ctx.derived(LayoutMemo.class, c -> new LayoutMemo());
        java.util.Map<Type, Optional<List<Type.Column>>> table = withIdentity ? memo.identity : memo.plain;
        Optional<List<Type.Column>> hit = table.get(t);
        if (hit != null) {
            return hit;
        }
        Optional<List<Type.Column>> computed = layoutOfUncached(ctx, t, withIdentity);
        table.putIfAbsent(t, computed);
        return computed;
    }

    private static Optional<List<Type.Column>> layoutOfUncached(ModelContext ctx, Type t,
                                                              boolean withIdentity) {
        Optional<List<Type.Column>> base = plainLayoutOf(ctx, t);
        if (!withIdentity || base.isEmpty()) {
            return base;
        }
        String fqn = EqualityKeys.fqnOf(t);
        if (fqn == null || PlatformTypes.isPairCarrier(t)
                || PlatformTypes.isListCarrier(t)
                || PlatformTypes.isMapCarrier(t)) {
            return base;   // platform carriers own their SQL shape
        }
        if (base.get().stream().anyMatch(c -> SYNTHETIC_ID.equals(c.name()))) {
            throw new IllegalStateException("class '" + fqn + "' declares a"
                    + " property named __id — colliding with the synthetic"
                    + " identity field");
        }
        List<Type.Column> out = new ArrayList<>(base.get());
        var opt = new com.legend.compiler.element.type.Multiplicity.Bounded(0, 1);
        out.add(new Type.Column(SYNTHETIC_ID, Type.Primitive.STRING, opt));
        out.add(new Type.Column(SYNTHETIC_TYPE, Type.Primitive.STRING, opt));
        out.add(new Type.Column(SYNTHETIC_CANON, new Type.ClassType(PlatformTypes.VARIANT), opt));
        return Optional.of(List.copyOf(out));
    }

    /** THE OBJECT LAYOUT (docs/OBJECT_IDENTITY_2026_10_20.md §3.1): a model class's declared properties plus its
     *  identity, {@code __id}, and its own class, {@code __type} -- every object carries both on every lane, so
     *  {@code eq} and a keyless class's {@code equal} compare identity, never the properties. No {@code __canon}:
     *  that is the verdict lanes' byte judge's ({@link #layoutOf(ModelContext, Type, boolean)}). Platform carriers
     *  keep their own shapes; empty for a type that is no layoutable class. */
    public static Optional<List<Type.Column>> objectLayoutOf(ModelContext ctx, Type t) {
        LayoutMemo memo = ctx.derived(LayoutMemo.class, c -> new LayoutMemo());
        Optional<List<Type.Column>> hit = memo.object.get(t);
        if (hit != null) {
            return hit;
        }
        Optional<List<Type.Column>> computed = layoutOf(ctx, t, true).map(cols -> List.copyOf(
                cols.stream().filter(c -> !SYNTHETIC_CANON.equals(c.name())).toList()));
        memo.object.putIfAbsent(t, computed);
        return computed;
    }

    /** {@link #syntheticOnlyLayout(boolean)} for the object layout: a property-less class's object is its identity and
     *  its class. */
    public static List<Type.Column> objectSyntheticOnlyLayout() {
        var opt = new com.legend.compiler.element.type.Multiplicity.Bounded(0, 1);
        return List.of(new Type.Column(SYNTHETIC_ID, Type.Primitive.STRING, opt),
                new Type.Column(SYNTHETIC_TYPE, Type.Primitive.STRING, opt));
    }

    /** The layout of a constructed value of a class with NO stored
     * properties (toPostgresModel's {@code ^NullLiteral()}, {@code
     * ^SQLNull()}): its only data is its class and, in an identity lane,
     * its identity — the synthetic fields alone (a plain lane carries
     * {@code __type} only; an empty struct is not a SQL value). */
    public static List<Type.Column> syntheticOnlyLayout(boolean withIdentity) {
        var opt = new com.legend.compiler.element.type.Multiplicity.Bounded(0, 1);
        Type.Column type = new Type.Column(SYNTHETIC_TYPE, Type.Primitive.STRING, opt);
        return withIdentity
                ? List.of(new Type.Column(SYNTHETIC_ID, Type.Primitive.STRING, opt), type,
                        new Type.Column(SYNTHETIC_CANON, new Type.ClassType(PlatformTypes.VARIANT), opt))
                : List.of(type);
    }

    private static Optional<List<Type.Column>> plainLayoutOf(ModelContext ctx, Type t) {
        return switch (t) {
            case Type.ClassType ct when !PlatformTypes.isVariant(ct) ->
                    ctx.findClass(ct.fqn()).flatMap(c -> layout(ctx, c, Map.of()));
            case Type.GenericType g -> ctx.findClass(g.rawFqn()).flatMap(c -> {
                if (c.typeParameters().size() != g.arguments().size()) {
                    return Optional.empty();   // malformed parameterization — let the caller stay loud
                }
                Map<String, Type> args = new LinkedHashMap<>();
                for (int i = 0; i < c.typeParameters().size(); i++) {
                    args.put(c.typeParameters().get(i), g.arguments().get(i));
                }
                return layout(ctx, c, args);
            });
            default -> Optional.empty();
        };
    }

    private static Optional<List<Type.Column>> layout(ModelContext ctx, TypedClass cls,
                                                      Map<String, Type> typeArgs) {
        LinkedHashMap<String, Type.Column> fields = new LinkedHashMap<>();
        collect(ctx, cls, typeArgs, fields, false);
        return fields.isEmpty() ? Optional.empty()
                : Optional.of(List.copyOf(fields.values()));
    }

    /**
     * Inherited stored properties first (super walk), then locally declared
     * ones. A SUBCLASS redeclaration overrides its inherited field (in the
     * inherited position); two SUPERS declaring the same name with different
     * types is a genuine conflict — LOUD, never first-wins (audit).
     */
    /** A nominal whose class is a {@code Function} subclass (m3: Property,
     * QualifiedProperty, FunctionDefinition, …) is CODE like the bare
     * function type — Class.properties: Property<T,Any|*>[*] has no slot
     * (batch 149: the metaclass shapes gained their m3 properties). */
    /** A REFLECTION-typed property ({@code Any.classifierGenericType :
     * GenericType[0..1]}, {@code Any.elementOverride : ElementOverride[0..1]},
     * m3.pure) is the metamodel's view of the value, not data the value
     * carries: no SQL carrier, no slot — the function-carrier rule's twin.
     * With it m3's {@code Any} comes into the prelude whole (batch 163);
     * without it every instance struct grew two fields (batch 147: 173
     * tests in one lane run). */
    private static boolean isReflectionCarrier(Type declared) {
        String raw = declared instanceof Type.ClassType ct ? ct.fqn()
                : declared instanceof Type.GenericType g ? g.rawFqn() : null;
        return PlatformTypes.GENERIC_TYPE.equals(raw) || PlatformTypes.ELEMENT_OVERRIDE.equals(raw);
    }

    private static boolean isFunctionCarrier(ModelContext ctx, Type declared) {
        String raw = declared instanceof Type.ClassType ct ? ct.fqn()
                : declared instanceof Type.GenericType g ? g.rawFqn() : null;
        return raw != null && !raw.equals(PlatformTypes.FUNCTION)
                && ctx.isSubtype(raw, PlatformTypes.FUNCTION);
    }

    private static void collect(ModelContext ctx, TypedClass cls, Map<String, Type> typeArgs,
                                LinkedHashMap<String, Type.Column> out, boolean isSuper) {
        for (String superFqn : cls.superClassFqns()) {
            ctx.findClass(superFqn).ifPresent(s -> collect(ctx, s, typeArgs, out, true));
        }
        for (Property p : cls.properties()) {
            if (!(p instanceof Property.Stored stored)) {
                continue;
            }
            Type declared = substitute(stored.type(), typeArgs);
            if (declared instanceof Type.FunctionType
                    || (declared instanceof Type.GenericType g
                            && g.rawFqn().equals(PlatformTypes.FUNCTION))
                    || isFunctionCarrier(ctx, declared)
                    || isReflectionCarrier(declared)) {
                // a FUNCTION-typed property is CODE, not data (the engine's
                // Runtime.preprocessFunction hook, Function<{FunctionDefinition,
                // Runtime -> FunctionDefinition}>): it has no SQL carrier and
                // no slot in the instance's canonical layout — a constructed
                // ^Runtime(connectionStores=...) lowers without it (batch 69c,
                // fetchDbPrimaryKeysMetaData's connection chain)
                continue;
            }
            Type.Column col = new Type.Column(stored.name(), declared, stored.multiplicity());
            Type.Column prev = out.get(stored.name());
            if (prev != null && isSuper) {
                if (!prev.type().equals(col.type())) {
                    throw new IllegalStateException("class '" + cls.qualifiedName()
                            + "' inherits conflicting declarations of property '"
                            + stored.name() + "' (" + prev.type().typeName() + " vs "
                            + col.type().typeName() + ")");
                }
                // D94 (layout row): a DIAMOND duplicate keeps the FIRST
                // super's declaration — multiplicity included — the same
                // extends-order rule findProperty resolves by, so the
                // layout and the property surface can never disagree
                // (last-super-wins laid a [*] field under a [1] property:
                // a list under an Integer[1] stamp)
                continue;
            }
            out.put(stored.name(), col);   // keeps the first position
        }
    }

    /** Positional generic instantiation: replace type-parameter occurrences with the receiver's arguments. */
    private static Type substitute(Type t, Map<String, Type> typeArgs) {
        return switch (t) {
            case Type.TypeVar v -> typeArgs.getOrDefault(v.name(), t);
            case Type.GenericType g -> new Type.GenericType(g.rawFqn(),
                    g.arguments().stream().map(a -> substitute(a, typeArgs)).toList());
            default -> t;
        };
    }
}
