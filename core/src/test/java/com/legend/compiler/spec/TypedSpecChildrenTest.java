package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.FoldStrategy;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGraphTree;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedOver;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedRename;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.parser.spec.PureDateLiteral;
import com.legend.parser.spec.PureTimeLiteral;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code TypedSpec.children()} completeness: for EVERY permitted HIR node, every
 * {@code TypedSpec} reachable through its record components (directly, through
 * {@code List}/{@code Optional}, or through the composite carriers
 * {@code TypedFuncCol}/{@code TypedAggCol}/{@code GroupKey}) must appear in
 * {@code children()}. This is the residual risk the abstract method can't catch:
 * the compiler forces every node to IMPLEMENT {@code children()}, but a node that
 * forgets one of its own sub-expressions would silently break every traversal
 * (callee collection, future lowering walks). Built by reflection so a NEW node
 * is covered automatically.
 */
class TypedSpecChildrenTest {

    private static final ModelContext CTX = Compiler.compileModel("");

    @Test
    void everyNodesChildrenCoverItsTypedSpecComponents() throws Exception {
        List<String> failures = new ArrayList<>();
        for (Class<?> node : TypedSpec.class.getPermittedSubclasses()) {
            TypedSpec instance = (TypedSpec) build(node);
            var expected = new ArrayList<TypedSpec>();
            for (RecordComponent rc : node.getRecordComponents()) {
                collectTypedSpecs(rc.getAccessor().invoke(instance), expected);
            }
            var children = new IdentityHashMap<TypedSpec, Boolean>();
            instance.children().forEach(c -> children.put(c, true));
            for (TypedSpec want : expected) {
                if (!children.containsKey(want)) {
                    failures.add(node.getSimpleName() + " omits a "
                            + want.getClass().getSimpleName() + " component from children()");
                }
            }
        }
        assertTrue(failures.isEmpty(), String.join("\n", failures));
    }

    /** Flatten a component value into the TypedSpecs it carries. */
    private static void collectTypedSpecs(Object value, List<TypedSpec> out) {
        switch (value) {
            case null -> { }
            case TypedSpec ts -> out.add(ts);
            case List<?> l -> l.forEach(e -> collectTypedSpecs(e, out));
            case Optional<?> o -> o.ifPresent(e -> collectTypedSpecs(e, out));
            case java.util.Map<?, ?> m -> m.values().forEach(e -> collectTypedSpecs(e, out));
            case TypedFuncCol fc -> out.add(fc.fn());
            case TypedAggCol ac -> {
                out.add(ac.map());
                out.add(ac.reduce());
            }
            case TypedGroupBy.GroupKey gk -> gk.fn().ifPresent(out::add);
            case FoldStrategy.MapReduce mr -> {
                out.add(mr.transform());
                out.add(mr.reducer());
            }
            default -> { }   // names, types, flags, info — not expression children
        }
    }

    // ==================== generic dummy-instance builder ====================

    private static Object build(Class<?> record) throws Exception {
        RecordComponent[] rcs = record.getRecordComponents();
        Class<?>[] types = new Class<?>[rcs.length];
        Object[] args = new Object[rcs.length];
        for (int i = 0; i < rcs.length; i++) {
            types[i] = rcs[i].getType();
            args[i] = dummy(rcs[i].getType(), rcs[i].getGenericType());
        }
        Constructor<?> ctor = record.getDeclaredConstructor(types);
        ctor.setAccessible(true);
        return ctor.newInstance(args);
    }

    private static Object dummy(Class<?> type, java.lang.reflect.Type generic) throws Exception {
        if (type == List.class) {
            return List.of(dummyOfGenericArg(generic, 0));
        }
        if (type == Optional.class) {
            return Optional.of(dummyOfGenericArg(generic, 0));
        }
        if (type == java.util.Map.class) {
            return java.util.Map.of("k", dummyOfGenericArg(generic, 1));
        }
        if (type == TypedSpec.class) {
            return leaf();
        }
        if (type == TypedFunction.class) {
            return CTX.findFunction("size").get(0);   // any real native signature
        }
        if (type == TypedLambda.class) {
            return new TypedLambda(List.of("x"), List.of(leaf()), one());
        }
        if (type == TypedEnumValue.class) {
            return new TypedEnumValue("test::E", "V", one());
        }
        if (type == TypedPackageableRef.class) {
            return new TypedPackageableRef("test::P", one());
        }
        if (type == TypedOver.class) {
            return new TypedOver(List.of("c"), List.of(), Optional.empty(), one());
        }
        if (type == TypedFuncCol.class) {
            return new TypedFuncCol("c", (TypedLambda) dummy(TypedLambda.class, null));
        }
        if (type == TypedAggCol.class) {
            return new TypedAggCol("c", (TypedLambda) dummy(TypedLambda.class, null),
                    (TypedLambda) dummy(TypedLambda.class, null));
        }
        if (type == TypedGroupBy.GroupKey.class) {
            return new TypedGroupBy.GroupKey("c",
                    Optional.of((TypedLambda) dummy(TypedLambda.class, null)));
        }
        if (type == TypedSort.TypedSortKey.class) {
            return new TypedSort.TypedSortKey("c", true);
        }
        if (type == TypedRename.ColRename.class) {
            return new TypedRename.ColRename("a", "b");
        }
        if (type == TypedGraphTree.class) {
            return new TypedGraphTree("p", List.of());
        }
        if (type == FoldStrategy.class) {
            return new FoldStrategy.MapReduce(leaf(), leaf(), "a", "m");
        }
        if (type == PureDateLiteral.class) {
            return new PureDateLiteral.StrictDate(2020, 1, 1);
        }
        if (type == PureTimeLiteral.class) {
            return new PureTimeLiteral.TimeWithMinute(10, 30);
        }
        if (type == ExprType.class) {
            return one();
        }
        if (type == Type.class) {
            return Type.Primitive.INTEGER;
        }
        if (type == Multiplicity.class) {
            return Multiplicity.Bounded.ONE;
        }
        if (type == String.class) {
            return "x";
        }
        if (type == BigDecimal.class) {
            return BigDecimal.ONE;
        }
        if (type.isEnum()) {
            return type.getEnumConstants()[0];
        }
        if (type == boolean.class || type == Boolean.class) {
            return true;
        }
        if (type == long.class || type == Long.class || type == Number.class) {
            return 1L;
        }
        if (type == int.class || type == Integer.class) {
            return 1;
        }
        if (type == double.class || type == Double.class) {
            return 1.0;
        }
        throw new IllegalStateException("TypedSpecChildrenTest needs a dummy rule for "
                + type.getName() + " — add one when introducing new node component types");
    }

    private static Object dummyOfGenericArg(java.lang.reflect.Type generic, int i) throws Exception {
        if (generic instanceof ParameterizedType pt) {
            java.lang.reflect.Type arg = pt.getActualTypeArguments()[i];
            if (arg instanceof Class<?> c) {
                return dummy(c, null);
            }
            if (arg instanceof ParameterizedType nested) {
                return dummy((Class<?>) nested.getRawType(), nested);
            }
        }
        throw new IllegalStateException("cannot determine element type of " + generic);
    }

    private static TypedSpec leaf() {
        return new TypedVariable("v", one());
    }

    private static ExprType one() {
        return ExprType.one(Type.Primitive.INTEGER);
    }
}
