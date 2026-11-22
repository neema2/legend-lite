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

    /** An equality pair's TYPED kinds (item 3's S4): the kinds the database judge compares its two sides as -- each
     *  side's typed class ({@code VerdictSql.typedClass}; two number kinds then the same only where both are empty,
     *  register S127), a date side's its kind ({@link #dateKind}) -- or null where the pair stays the seven-column row:
     *  a side with no typed class, or a partial date's literal. A Decimal literal is typed: its written scale rides its
     *  SQL type, which the reader restores where a driver drops it (SQLite's, as BigQuery's). */
    record TypedPair(Type e, Type a, Form form) {
        /** How the pair's values compare: as typed values, dates (by kind and instant), the same only where both are
         *  empty (an empty side, Nil), tagged values (each its kind and its value), a generic class's instances (each by
         *  its fields, the struct the database compares). */
        enum Form { VALUES, DATES, EMPTY_ONLY, TAGGED, INSTANCES }

        boolean dates() {
            return form == Form.DATES;
        }

        boolean tagged() {
            return form == Form.TAGGED;
        }

        /** The pair's typed member over the two sides' plans, or null where its values have no typed column. */
        com.legend.lowering.VerdictSql.@Nullable TypedRow row(com.legend.sql.SqlQuery ePlan,
                com.legend.sql.SqlQuery aPlan, boolean multiset, boolean wantEqual) {
            return switch (form) {
                case TAGGED -> com.legend.lowering.VerdictSql.typedTagged(ePlan, e, aPlan, a, multiset, wantEqual);
                case VALUES, DATES, EMPTY_ONLY, INSTANCES -> com.legend.lowering.VerdictSql.typedEquals(ePlan, e,
                        aPlan, a, multiset, wantEqual, form == Form.EMPTY_ONLY);
            };
        }
    }

    static @Nullable TypedPair typedPair(TypedSpec eSpec, Type eKind, com.legend.sql.SqlQuery ePlan, TypedSpec aSpec,
            Type aKind, com.legend.sql.SqlQuery aPlan) {
        if (tagged(ePlan) || tagged(aPlan)) {
            // a value whose kind the database decides among candidates (a Number's max, a mixed list): each value its
            // kind and its value, against a side of one typed class or another tagged one (VerdictSql.typedTagged)
            return (tagged(ePlan) || sideKind(eSpec, eKind, ePlan) != null)
                    && (tagged(aPlan) || sideKind(aSpec, aKind, aPlan) != null)
                    ? new TypedPair(eKind, aKind, TypedPair.Form.TAGGED) : null;
        }
        if (PlatformTypes.isNil(eKind) || PlatformTypes.isNil(aKind)) {
            // an empty side (Nil, the empty collection's kind): the two the same only where both are empty, the other
            // side's values typed by its own kind for the record
            Type e = PlatformTypes.isNil(eKind) ? eKind : sideKind(eSpec, eKind, ePlan);
            Type a = PlatformTypes.isNil(aKind) ? aKind : sideKind(aSpec, aKind, aPlan);
            return e == null || a == null ? null : new TypedPair(e, a, TypedPair.Form.EMPTY_ONLY);
        }
        Type e = dateKind(eSpec, eKind, ePlan);
        Type a = dateKind(aSpec, aKind, aPlan);
        if (e != null && a != null) {
            // a date literal's side rides the literal channel (its written text), its value column still the
            // database's DATE or TIMESTAMP, which the pair compares (S399)
            return new TypedPair(e, a, TypedPair.Form.DATES);
        }
        if (eKind.equals(aKind) && com.legend.lowering.VerdictSql.instances(ePlan)
                && com.legend.lowering.VerdictSql.instances(aPlan)) {
            // two sides of one generic class carried as structs (a Pair): each instance by its fields, the database
            // comparing the structs (VerdictSql.instances)
            return new TypedPair(eKind, aKind, TypedPair.Form.INSTANCES);
        }
        e = com.legend.lowering.VerdictSql.typedClass(eKind);
        a = com.legend.lowering.VerdictSql.typedClass(aKind);
        return e == null || a == null ? null : new TypedPair(e, a, TypedPair.Form.VALUES);
    }

    /** Whether a side's plan carries a tagged value (its first column a {@code SqlType.Tagged}: the kind the
     *  database decided among candidates). */
    private static boolean tagged(com.legend.sql.SqlQuery plan) {
        return !plan.outputs().isEmpty() && plan.outputs().get(0).type() instanceof com.legend.sql.SqlType.Tagged;
    }

    /** A side's typed kind: its date kind ({@link #dateKind}), else its typed class; null where it has neither. */
    private static @Nullable Type sideKind(TypedSpec spec, Type kind, com.legend.sql.SqlQuery plan) {
        Type d = dateKind(spec, kind, plan);
        return d != null ? d : com.legend.lowering.VerdictSql.typedClass(kind);
    }

    /** A date side's kind under register S399 (the user's ruling of 2026-11-22: the product compares a date by its
     *  kind and its instant, the database comparing): a literal's own -- a day a StrictDate, a date-time of any written
     *  precision a DateTime, a partial date none (it stays the seven-column row) --, a StrictDate's or DateTime's side
     *  its own, a {@code Date} side's by its column's SQL type (a DATE holds days, a TIMESTAMP date-times); null for any
     *  other side. A fact of the types, fixed at planning. */
    private static @Nullable Type dateKind(TypedSpec spec, Type kind, com.legend.sql.SqlQuery plan) {
        if (spec instanceof com.legend.compiler.spec.typed.TypedCDate d) {
            return d.value() instanceof com.legend.values.PureDateLiteral.StrictDate ? Type.Primitive.STRICT_DATE
                    : d.value().precision().atLeast(com.legend.values.PureDateLiteral.Precision.HOUR)
                            ? Type.Primitive.DATE_TIME : null;
        }
        if (kind == Type.Primitive.STRICT_DATE || kind == Type.Primitive.DATE_TIME) {
            return kind;
        }
        if (kind != Type.Primitive.DATE || plan.outputs().isEmpty()) {
            return null;
        }
        com.legend.sql.SqlType sql = plan.outputs().get(0).type();
        return sql == com.legend.sql.SqlType.Scalar.DATE ? Type.Primitive.STRICT_DATE
                : sql == com.legend.sql.SqlType.Scalar.TIMESTAMP || sql == com.legend.sql.SqlType.Scalar.TIMESTAMPTZ
                        ? Type.Primitive.DATE_TIME : null;
    }

    /** {@code s}'s static facts: the keyed class it declares, its kind class, whether it is Any-stamped, Nil-typed, a
     *  map carrier; its type's name for the census and the declared type's name its values are judged by. */
    static HostJudgment.SideFacts sideFacts(TypedSpec s) {
        Type t = s.info().type();
        return new HostJudgment.SideFacts(EqualityKeys.fqnOf(t), kindOf(t), AssertVerdicts.isAnyStamped(s),
                PlatformTypes.isNil(t), PlatformTypes.isMapCarrier(t), AssertVerdicts.typeName(s), t.typeName());
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
        Map<String, KeyTree> byClass = new LinkedHashMap<>();
        Set<String> seen = new java.util.HashSet<>();
        ownClassesOfTheSlots(e, ctx, byClass, seen);
        ownClassesOfTheSlots(a, ctx, byClass, seen);
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
