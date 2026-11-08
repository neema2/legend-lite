// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

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
}
