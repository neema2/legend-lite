// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.base.Nullable;
import com.legend.compiler.element.EqualityKeys;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.testexecutionplan.HostJudgment;
import com.legend.testexecutionplan.KeyTree;
import com.legend.testexecutionplan.KindClass;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE HOST JUDGMENT'S FACTS, READ AT PLANNING (docs/PLAN_AS_DATA_2026_11_07.md 2.2, the design's step 4, 2026-11-08):
 * from the typed arguments and the model, into the test plan's vocabulary -- a side's static facts, its kind class, the
 * key trees an instance pair restricts to. Planning's half: the run reads the facts, never the model.
 */
final class JudgmentFacts {

    private JudgmentFacts() {
    }

    /** An equality pair as the database judge reads it: how its sides compare is the product's equality's
     *  ({@code lowering.SideEqualities.Pair}); here, the judge's own facts about it -- whether it is compared at all in
     *  the literal channel, and the family whose record the host's builders write. */
    record TypedPair(com.legend.lowering.SideEqualities.Pair pair) {

        /** Whether both sides carry their values in ONE scalar SQL type (the literal channel's Any-stamped or mixed
         *  side whose plan holds one typed column: compared as typed values all the same) -- never an instance's kind
         *  but a packageable element's, carried by its path (an instance carried as its text is its own member). */
        boolean oneScalar(TypedSpec eSpec, com.legend.sql.SqlQuery ePlan, TypedSpec aSpec, com.legend.sql.SqlQuery aPlan,
                ModelContext ctx) {
            // (each side's declared type too: a rider's kind is no proof -- the audit of 2026-11-22's metamodel pair, a
            // SetImplementation carried as its JSON text under a rider naming a Mapping)
            return pair.form() == com.legend.lowering.SideEqualities.Pair.Form.VALUES && byValue(pair.e(), ctx)
                    && byValue(pair.a(), ctx) && byValue(eSpec.info().type(), ctx)
                    && byValue(aSpec.info().type(), ctx) && !ePlan.outputs().isEmpty()
                    && !aPlan.outputs().isEmpty() && ePlan.outputs().get(0).type() instanceof com.legend.sql.SqlType.Scalar t
                    && t != com.legend.sql.SqlType.Scalar.JSON && t.equals(aPlan.outputs().get(0).type());
        }

        private static boolean byValue(Type k, ModelContext ctx) {
            return EqualityKeys.fqnOf(k) instanceof String c && !PlatformTypes.isAny(k) && !PlatformTypes.isNil(k)
                    ? ctx.isSubtype(c, PACKAGEABLE) : true;
        }

        boolean kindsDiffer() {
            return pair.kindsDiffer();
        }

        /** The family whose record the host's builders write: an instance's identity assert ({@code assertEq},
         *  {@code assertIs}) names it by its class and identity, as the host's does; else the equality's. */
        com.legend.testexecutionplan.RootLoop.Family family(boolean multiset, String name) {
            return pair.form() == com.legend.lowering.SideEqualities.Pair.Form.INSTANCES
                    && (name.equals("assertEq") || name.equals("assertIs"))
                    ? com.legend.testexecutionplan.RootLoop.Family.EQ
                    : instanceTexts() && !multiset ? com.legend.testexecutionplan.RootLoop.Family.EQUALS_DOCUMENTS
                    : com.legend.testexecutionplan.RootLoop.Family.equality(multiset, tagged());
        }

        boolean dates() {
            return pair.form() == com.legend.lowering.SideEqualities.Pair.Form.DATES;
        }

        /** Whether the pair is of plain values compared as values (no date, no tag, no instance, no "empty only"). */
        boolean plainValues() {
            return pair.form() == com.legend.lowering.SideEqualities.Pair.Form.VALUES;
        }

        boolean instances() {
            return pair.form() == com.legend.lowering.SideEqualities.Pair.Form.INSTANCES;
        }

        boolean tagged() {
            return pair.form() == com.legend.lowering.SideEqualities.Pair.Form.TAGGED;
        }

        boolean instanceTexts() {
            return pair.form() == com.legend.lowering.SideEqualities.Pair.Form.INSTANCE_TEXTS;
        }

        /** The pair's typed member over the two sides' plans: the product's comparison of them under the assert's
         *  polarity, each side's values riding the failure; null where its values have no typed column. */
        com.legend.lowering.VerdictSql.@Nullable TypedRow row(com.legend.sql.SqlQuery ePlan,
                com.legend.sql.SqlQuery aPlan, boolean multiset, boolean wantEqual) {
            return com.legend.lowering.VerdictSql.typed(pair.compared(ePlan, aPlan, multiset), wantEqual);
        }
    }

    private static final String PACKAGEABLE = "meta::pure::metamodel::PackageableElement";

    /** The pair of an equality's two sides ({@code lowering.SideEqualities.pair}), or null where it has no typed
     *  comparison (the caller refuses it by name). */
    static @Nullable TypedPair typedPair(TypedSpec eSpec, Type eKind, com.legend.sql.SqlQuery ePlan, TypedSpec aSpec,
            Type aKind, com.legend.sql.SqlQuery aPlan, ModelContext ctx) {
        com.legend.lowering.SideEqualities.Pair pair = com.legend.lowering.SideEqualities.pair(eSpec, eKind, ePlan,
                aSpec, aKind, aPlan, ctx);
        return pair == null ? null : new TypedPair(pair);
    }

    /** Whether no Variant is among either side's values (after {@link #refuseIdentityOnly}'s refusal). */
    static boolean computed(TypedSpec e, TypedSpec a) {
        return com.legend.lowering.SideEqualities.refuseIdentityOnly(e, a) == com.legend.compiler.spec.typed.VariantEquality.Verdict.COMPUTED;
    }

    /** Whether a side's Variants meet values of other types alone (after {@link #refuseIdentityOnly}'s refusal). */
    static boolean variantDisjoint(TypedSpec e, TypedSpec a) {
        return com.legend.lowering.SideEqualities.refuseIdentityOnly(e, a) == com.legend.compiler.spec.typed.VariantEquality.Verdict.DISJOINT;
    }

    /** A membership of or in a Variant (after {@link #refuseIdentityOnly}'s refusal): the database judge's has no
     *  typed form. */
    static boolean variantMembership(TypedSpec coll, TypedSpec val) {
        return variantDisjoint(coll, val) || PlatformTypes.isVariant(coll.info().type())
                || PlatformTypes.isVariant(val.info().type());
    }

    /** {@code s}'s static facts: the keyed class it declares, its kind class, whether it is Any-stamped, Nil-typed, a
     *  map carrier; its type's name for the census and the declared type's name its values are judged by. */
    static HostJudgment.SideFacts sideFacts(TypedSpec s) {
        Type t = sideType(s);
        return new HostJudgment.SideFacts(EqualityKeys.fqnOf(t), kindOf(t), AssertVerdicts.isAnyStamped(s),
                PlatformTypes.isNil(t), PlatformTypes.isMapCarrier(t), AssertVerdicts.typeName(s), t.typeName());
    }

    /** A side's type for the judges: its own, or where it is declared Any and is a cast of a Variant (written
     *  {@code v->cast(@Any)}), the Variant it is -- a fact the plan reads off the expression, so both judges read the
     *  side as one (the audit of 8477c703, 2026-11-23: reading every Any side as a Variant made an Integer one). */
    static Type sideType(TypedSpec s) {
        return com.legend.compiler.spec.typed.VariantEquality.sideType(s);
    }

    /** The kind class of a type; null for a shape with no equality kind. */
    static @Nullable KindClass kindOf(Type t) {
        if (t == Type.Primitive.INTEGER || t == Type.Primitive.FLOAT
                || t == Type.Primitive.DECIMAL
                // NUMBER is the numeric tower's supertype -- the concrete render refines from the PLAN's SQL type
                || t == Type.Primitive.NUMBER || t instanceof Type.PrecisionDecimal) {
            return KindClass.Primitive.NUMERIC;
        }
        if (t instanceof Type.EnumType et) {
            return new KindClass.Enum(et.fqn());
        }
        if (t == Type.Primitive.STRING) {
            return KindClass.Primitive.STRING;
        }
        if (t == Type.Primitive.BOOLEAN) {
            return KindClass.Primitive.BOOLEAN;
        }
        if (t == Type.Primitive.STRICT_DATE || t == Type.Primitive.DATE_TIME || t == Type.Primitive.DATE) {
            return KindClass.Primitive.TEMPORAL;
        }
        String fqn = EqualityKeys.fqnOf(t);
        return fqn != null ? new KindClass.Instance(fqn) : null;
    }

    /** The compiler's key tree in the plan's vocabulary. */
    private static void classesHeld(Type t, ModelContext ctx, Map<String, KeyTree> byClass) {
        if (!(t instanceof Type.GenericType g) || !(PlatformTypes.isMapCarrier(t) || PlatformTypes.isListCarrier(t))) {
            return;
        }
        for (Type held : g.arguments()) {
            String fqn = EqualityKeys.fqnOf(held);
            if (held instanceof Type.ClassType && fqn != null && !PlatformTypes.isAny(held)
                    && !PlatformTypes.isVariant(held) && !PlatformTypes.isNil(held)) {
                KeyTree keys = keyTree(EqualityKeys.resolve(ctx, held));
                if (keys != null) {
                    byClass.putIfAbsent(fqn, keys);
                }
            }
            classesHeld(held, ctx, byClass);
        }
    }

    static @Nullable KeyTree keyTree(@Nullable EqualityKeys k) {
        if (k == null) {
            return null;
        }
        List<KeyTree.Key> keys = new java.util.ArrayList<>(k.keys().size());
        for (EqualityKeys.Key key : k.keys()) {
            keys.add(new KeyTree.Key(key.name(), key.many(), keyTree(key.nested()), key.objectOnly(),
                    key.declaredClass()));
        }
        return new KeyTree(k.classFqn(), keys);
    }

    /**
     * The keyed-class facts a pair's equality restricts by (X5): each side's declared class's keys where the classes
     * agree or one side declares a supertype of the other, the subtype relation between them, and the keys of every
     * keyed class a value in a key slot that declares no keyed class may carry as its own -- read once, here.
     */
    static HostJudgment.Keys keys(TypedSpec eSpec, TypedSpec aSpec, ModelContext ctx) {
        // the host's comparison families (equals, contains, sameElements) all read their two sides' facts here
        com.legend.lowering.SideEqualities.refuseIdentityOnly(eSpec, aSpec);
        String ef = EqualityKeys.fqnOf(eSpec.info().type());
        String af = EqualityKeys.fqnOf(aSpec.info().type());
        boolean eSubA = false;
        boolean aSubE = false;
        if (ef != null && af != null && !ef.equals(af)) {
            eSubA = ctx.isSubtype(ef, af);
            aSubE = ctx.isSubtype(af, ef);
        }
        EqualityKeys e = ef != null && (ef.equals(af) || eSubA) ? EqualityKeys.resolve(ctx, eSpec.info().type())
                : null;
        EqualityKeys a = aSubE ? EqualityKeys.resolve(ctx, aSpec.info().type()) : null;
        // the two sides' own classes: a key declared Variant compares their Variants (register S401) -- never the classes
        // an Any slot may hold, read below (one keyed class with a Variant key in the model is no Pair's)
        com.legend.compiler.spec.typed.VariantEquality.refuseVariantKeys(e);
        com.legend.compiler.spec.typed.VariantEquality.refuseVariantKeys(a);
        Map<String, KeyTree> byClass = new LinkedHashMap<>();
        Set<String> seen = new java.util.HashSet<>();
        ownClassesOfTheSlots(e, ctx, byClass, seen);
        ownClassesOfTheSlots(a, ctx, byClass, seen);
        // (a map or a List that holds objects: the keyed classes it is declared to hold, each instance of which the
        // host restricts to its keys where it meets it inside the container -- step 6B part 2: compared whole, a
        // map to keyed objects was "not equal" to one whose objects differ in a field that is no key)
        classesHeld(eSpec.info().type(), ctx, byClass);
        classesHeld(aSpec.info().type(), ctx, byClass);
        return new HostJudgment.Keys(keyTree(e), keyTree(a), eSubA, aSubE, byClass);
    }

    /** The keyed classes a value may carry as its own in a key slot of {@code k} that declares no keyed class: the
     *  declared class and its subtree -- the model's whole universe for Any -- each class that DECLARES a key resolved
     *  once; a keyed class found contributes its own slots the same way. */
    private static void ownClassesOfTheSlots(@Nullable EqualityKeys k, ModelContext ctx, Map<String, KeyTree> byClass,
            Set<String> seen) {
        if (k == null) {
            return;
        }
        for (EqualityKeys.Key key : k.keys()) {
            if (key.nested() != null) {
                ownClassesOfTheSlots(key.nested(), ctx, byClass, seen);
                continue;
            }
            String declared = key.declaredClass();
            if (declared == null || !seen.add(declared + "[*]")) {
                continue;
            }
            Iterable<String> candidates = declared.equals(PlatformTypes.ANY) ? ctx.resolutionUniverse()
                    : withSubtree(declared, ctx);
            for (String fqn : candidates) {
                if (!seen.add(fqn) || !EqualityKeys.declaresKeys(ctx, fqn)) {
                    continue;
                }
                EqualityKeys own = EqualityKeys.resolve(ctx, fqn);
                if (own != null) {
                    byClass.put(fqn, java.util.Objects.requireNonNull(keyTree(own)));
                    ownClassesOfTheSlots(own, ctx, byClass, seen);
                }
            }
        }
    }

    private static Set<String> withSubtree(String declared, ModelContext ctx) {
        Set<String> out = new java.util.LinkedHashSet<>();
        out.add(declared);
        out.addAll(ctx.subtree(declared));
        return out;
    }

    /**
     * The enumerations an engine-text appeal's referee decodes, read from the model now, at planning (the plan-as-data
     * design's slice 4, hole 1, 2026-11-08: the referee read them from the compiled model at the appeal's decision):
     * every enumeration among our rows' column types ({@code rows}' shape, the plan's spelling; none where the side's
     * planning failed) and among the properties of
     * {@code classFqn} (inherited ones too, the most derived winning), each decoded under {@code mappingFqn} where there
     * is one -- the mapping's enumeration mapping (its own, then its includes'), a source value to the value's name; a
     * mapping that maps none decodes BY NAME, the identity over the enumeration's values (real pure's); a source value
     * naming another enumeration's value decodes nothing (no entry).
     */
    static com.legend.testexecutionplan.EnumFacts enumsOf(ModelContext ctx, @Nullable String mappingFqn,
            @Nullable String classFqn, com.legend.testexecutionplan.@Nullable SideShape shape) {
        java.util.Set<String> enums = new java.util.LinkedHashSet<>();
        for (com.legend.testexecutionplan.SideShape.Column column : shape == null
                ? java.util.List.<com.legend.testexecutionplan.SideShape.Column>of() : shape.columns()) {
            if (ctx.findEnum(column.type()).isPresent()) {
                enums.add(column.type());
            }
        }
        Map<String, String> properties = new LinkedHashMap<>();
        if (mappingFqn != null && classFqn != null) {
            enumProperties(ctx, classFqn, properties, new java.util.HashSet<>());
            enums.addAll(properties.values());
        }
        Map<String, Map<String, String>> decodes = new LinkedHashMap<>();
        if (mappingFqn != null) {
            for (String e : enums) {
                Map<String, String> decode = decodeOf(ctx, mappingFqn, e);
                if (decode != null) {
                    decodes.put(e, decode);
                }
            }
        }
        return new com.legend.testexecutionplan.EnumFacts(java.util.List.copyOf(enums), decodes, properties);
    }

    /** {@code classFqn}'s properties of an enumeration type into {@code out} (a name already there is a more derived
     *  class's), then its superclasses'. */
    private static void enumProperties(ModelContext ctx, String classFqn, Map<String, String> out,
            java.util.Set<String> visited) {
        if (!visited.add(classFqn)) {
            return;
        }
        ctx.findClass(classFqn).ifPresent(c -> {
            for (com.legend.compiler.element.Property p : c.properties()) {
                if (p.type() instanceof Type.EnumType et) {
                    out.putIfAbsent(p.name(), et.fqn());
                }
            }
            for (String superFqn : c.superClassFqns()) {
                enumProperties(ctx, superFqn, out, visited);
            }
        });
    }

    /** {@code enumFqn} decoded under {@code mappingFqn}: a source value to the value's name; null where the mapping's
     *  enumeration mapping reads another enumeration's value, or the enumeration is unknown. */
    private static @Nullable Map<String, String> decodeOf(ModelContext ctx, String mappingFqn, String enumFqn) {
        var em = com.legend.plan.EnumMappings.of(ctx, mappingFqn, enumFqn);
        if (em == null) {
            var en = ctx.findEnum(enumFqn).orElse(null);
            if (en == null) {
                return null;
            }
            Map<String, String> id = new LinkedHashMap<>();
            for (String v : en.values()) {
                id.put(v, v);
            }
            return id;
        }
        Map<String, String> dec = new LinkedHashMap<>();
        for (var vm : em.valueMappings()) {
            for (var sv : vm.sourceValues()) {
                switch (sv) {
                    case com.legend.model.EnumerationMapping.SourceValue.StringValue str ->
                            dec.put(str.value(), vm.enumValue());
                    case com.legend.model.EnumerationMapping.SourceValue.IntegerValue n ->
                            dec.put(String.valueOf(n.value()), vm.enumValue());
                    case com.legend.model.EnumerationMapping.SourceValue.EnumRef ignored -> {
                        return null;
                    }
                }
            }
        }
        return dec;
    }
}
