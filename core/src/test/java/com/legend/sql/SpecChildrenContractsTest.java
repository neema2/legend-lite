// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import com.legend.model.RelationalOperation;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The TypedSpecChildrenTest pattern extended to the three Phase-2a
 * contracts (NULL_GATE_VERIFICATION G1.2): for every permitted variant
 * of {@link SqlExpr}, {@link ValueSpecification} and
 * {@link RelationalOperation}, {@code children()} must cover every
 * same-hierarchy value reachable through the variant's record
 * components, and {@code withChildren(children())} must round-trip to
 * an equal node. This is the reflective test that catches an
 * incomplete children declaration — the WindowCall {@code fn} omission
 * (G1.1) is exactly the class it exists for.
 *
 * <p>Documented contract exemptions the expectation collector honors:
 * SqlExpr's QUERY-carrying nodes ({@code Exists}, {@code ScalarSubquery})
 * own their inner traversal, so {@link SqlQuery}-typed components are
 * not expected children; {@code LambdaFunction.parameters} are excluded
 * by the shadow-stop doctrine (children are the BODY only).
 */
class SpecChildrenContractsTest {

    @Test
    void sqlExprChildrenCoverComponents() throws Exception {
        checkHierarchy(SqlExpr.class,
                n -> ((SqlExpr) n).children(),
                (n, cs) -> ((SqlExpr) n).withChildren(cast(cs)));
    }

    @Test
    void valueSpecificationChildrenCoverComponents() throws Exception {
        checkHierarchy(ValueSpecification.class,
                n -> ((ValueSpecification) n).children(),
                (n, cs) -> ((ValueSpecification) n).withChildren(cast(cs)));
    }

    @Test
    void relationalOperationChildrenCoverComponents() throws Exception {
        checkHierarchy(RelationalOperation.class,
                n -> ((RelationalOperation) n).children(),
                (n, cs) -> ((RelationalOperation) n).withChildren(cast(cs)));
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> cast(List<?> l) {
        return (List<T>) l;
    }

    private interface Kids {
        List<?> of(Object node);
    }

    private interface Rebuild {
        Object with(Object node, List<?> children);
    }

    private static void checkHierarchy(Class<?> iface, Kids kids,
            Rebuild rebuild) throws Exception {
        List<String> failures = new ArrayList<>();
        for (Class<?> node : permittedClosure(iface)) {
            Object instance = build(node);
            var expected = new ArrayList<>();
            if (com.legend.protocol.spec.TypeAnnotation.class
                    .isAssignableFrom(node)) {
                // TYPE METADATA is a leaf for expression traversal: a
                // RelationShape's per-column annotations carry types, not
                // expressions — nothing to substitute or collect inside
                expected.clear();
                Object rb = rebuild.with(instance, kids.of(instance));
                if (!instance.equals(rb)) {
                    failures.add(node.getSimpleName()
                            + " withChildren(children()) != this");
                }
                continue;
            }
            for (RecordComponent rc : node.getRecordComponents()) {
                if (SqlQuery.class.isAssignableFrom(rc.getType())) {
                    continue;   // query-carrying: owns its traversal
                }
                if (node == LambdaFunction.class
                        && rc.getName().equals("parameters")) {
                    continue;   // shadow-stop doctrine
                }
                collect(iface, rc.getAccessor().invoke(instance), expected);
            }
            var children = new IdentityHashMap<Object, Boolean>();
            kids.of(instance).forEach(c -> children.put(c, true));
            for (Object want : expected) {
                if (!children.containsKey(want)) {
                    failures.add(node.getSimpleName() + " omits a "
                            + want.getClass().getSimpleName()
                            + " component from children()");
                }
            }
            Object rebuilt = rebuild.with(instance, kids.of(instance));
            if (!instance.equals(rebuilt)) {
                failures.add(node.getSimpleName()
                        + " withChildren(children()) != this");
            }
        }
        assertTrue(failures.isEmpty(), String.join("\n", failures));
    }

    /** Permitted subclasses, flattened through sealed sub-interfaces
     * (ColumnInstance, TypeAnnotation). */
    private static List<Class<?>> permittedClosure(Class<?> iface) {
        List<Class<?>> out = new ArrayList<>();
        for (Class<?> c : iface.getPermittedSubclasses()) {
            if (c.isInterface()) {
                out.addAll(permittedClosure(c));
            } else {
                out.add(c);
            }
        }
        return out;
    }

    /** Flatten a component value into the hierarchy instances it carries
     * (directly, in lists/maps/optionals, or in known carrier records). */
    private static void collect(Class<?> iface, Object value, List<Object> out)
            throws Exception {
        switch (value) {
            case null -> { }
            case List<?> l -> {
                for (Object e : l) {
                    collect(iface, e, out);
                }
            }
            case Optional<?> o -> {
                if (o.isPresent()) {
                    collect(iface, o.get(), out);
                }
            }
            case Map<?, ?> m -> {
                for (Object e : m.values()) {
                    collect(iface, e, out);
                }
            }
            default -> {
                if (iface.isInstance(value)) {
                    out.add(value);
                    return;
                }
                // carrier records (SortKey, When, Field, Key,
                // KeyExpression, Using...): recurse one level through
                // THEIR components
                if (value.getClass().isRecord()
                        && value.getClass().getPackageName()
                                .startsWith("com.legend")) {
                    for (RecordComponent rc
                            : value.getClass().getRecordComponents()) {
                        collect(iface, rc.getAccessor().invoke(value), out);
                    }
                }
            }
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

    private static Object dummy(Class<?> type,
            java.lang.reflect.Type generic) throws Exception {
        if (type == List.class) {
            return List.of(genericArg(generic, 0));
        }
        if (type == Optional.class) {
            return Optional.of(genericArg(generic, 0));
        }
        if (type == Map.class) {
            return Map.of(genericArg(generic, 0), genericArg(generic, 1));
        }
        if (type == String.class) {
            return "x";
        }
        if (type == boolean.class || type == Boolean.class) {
            return false;
        }
        if (type == int.class || type == Integer.class) {
            return 1;
        }
        if (type == long.class || type == Long.class) {
            return 1L;
        }
        if (type == double.class || type == Double.class) {
            return 1.0d;
        }
        if (type == Number.class) {
            return 1;
        }
        if (type == Object.class) {
            return "x";
        }
        if (type == java.math.BigDecimal.class) {
            return java.math.BigDecimal.ONE;
        }
        if (type.isEnum()) {
            return type.getEnumConstants()[0];
        }
        // pinned concrete choices for interface-typed components; the
        // SqlAgg dummy is a Reducer WITH expression args — the shape that
        // makes an omitted WindowCall.fn visible (G1.1)
        if (type == SqlExpr.class) {
            return new SqlExpr.Column(null, "c");
        }
        if (type == SqlAgg.class) {
            return new SqlAgg.Reducer(SqlAgg.Fn.SUM,
                    List.of(new SqlExpr.Column(null, "a")), false, List.of());
        }
        if (type == SqlQuery.class) {
            return build(SqlSelect.class);
        }
        if (type == SqlSource.class) {
            return new SqlSource.Table("T", "t0", List.of(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE);
        }
        if (type == SqlType.class) {
            return SqlType.Scalar.values()[0];
        }
        if (type == SqlExpr.WindowCall.Frame.Bound.class) {
            return new SqlExpr.WindowCall.Frame.Bound.CurrentRow();
        }
        if (type == ValueSpecification.class) {
            return new CString("x");
        }
        if (type == com.legend.protocol.spec.Variable.class) {
            return new com.legend.protocol.spec.Variable("v", null, null);
        }
        if (type == LambdaFunction.class) {
            return new LambdaFunction(
                    List.of(new com.legend.protocol.spec.Variable("v", null, null)),
                    List.of(new CString("b")));
        }
        if (type == com.legend.protocol.spec.ColSpec.class) {
            return new com.legend.protocol.spec.ColSpec("n");
        }
        if (type == com.legend.protocol.TypeExpression.class) {
            return new com.legend.protocol.TypeExpression.NameRef("T");
        }
        if (type == com.legend.protocol.Multiplicity.class) {
            return com.legend.protocol.Multiplicity.Concrete.PURE_ONE;
        }
        if (type == com.legend.values.PureDateLiteral.class) {
            return new com.legend.values.PureDateLiteral.StrictDate(2020, 1, 1);
        }
        if (type == com.legend.values.PureTimeLiteral.class) {
            return (com.legend.values.PureTimeLiteral) build(
                    com.legend.values.PureTimeLiteral.class
                            .getPermittedSubclasses()[0]);
        }
        if (type == RelationalOperation.class) {
            return new RelationalOperation.Literal("x");
        }
        // self-recursive record (Node holds List<Node>) — the generic builder
        // would recurse forever; pin a leaf
        if (type == com.legend.protocol.spec.Gql.Selection.class) {
            // Field nests selections of itself — spread is the leaf form
            return new com.legend.protocol.spec.Gql.FragmentSpread(
                    "f", List.of());
        }
        if (type == com.legend.protocol.spec.GraphFetchLiteral.Node.class) {
            return new com.legend.protocol.spec.GraphFetchLiteral.Node(
                    "p", null, List.of(), null, null, List.of());
        }
        if (type.isRecord()) {
            return build(type);
        }
        if (type.isSealed() && type.getPermittedSubclasses().length > 0) {
            for (Class<?> c : type.getPermittedSubclasses()) {
                if (c.isRecord()) {
                    return build(c);
                }
            }
        }
        throw new IllegalStateException("no dummy for " + type);
    }

    private static Object genericArg(java.lang.reflect.Type generic, int i)
            throws Exception {
        Class<?> arg = (Class<?>)
                ((ParameterizedType) generic).getActualTypeArguments()[i];
        return dummy(arg, arg);
    }
}
