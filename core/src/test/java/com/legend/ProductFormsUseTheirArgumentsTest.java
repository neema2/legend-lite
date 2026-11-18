// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * GUARD L-A, ITS FIRST HALF (the orchestrator's order of 2026-11-18): a two-form hook FED upstream's form. A hook's
 * product form is called by the product with a function per form it may pick -- {@code executeValues(array, oneBare)},
 * {@code computedPruning(pruned, kept)}, {@code existsJoin(filtered, joinForm)} -- and a function the product form never
 * calls is code the product builds for the edge alone: upstream's form, written in the product and handed in. This guard
 * calls every product form of every hook type (HookLambdasTest.CLASS_HOOKS: each product or host class implementing a
 * hook, two-form kind or style, pinned there exactly) through every method that takes a function, with a sentinel per
 * function and a sample per other parameter (every constant of an enum, both Booleans), and reports each function the
 * form never calls in any call. Each is pinned with its ruling ({@link #UNCALLED}); a new one fails the build.
 */
@Tag("guardrail")
class ProductFormsUseTheirArgumentsTest {

    /** Each function a product form never calls, "Form#method(params) arg N" -> its ruling. Found 2026-11-18. */
    static final Map<String, String> UNCALLED = Map.of();

    /** Every product form of every hook type: the instances of each class HookLambdasTest pins (an enum's constants, a
     *  static field holding one, else its no-argument constructor), with each hook type it implements. */
    static Map<Object, Set<Class<?>>> productForms(Map<String, Set<String>> classHooks) {
        Map<Object, Set<Class<?>>> out = new java.util.LinkedHashMap<>();
        classHooks.forEach((cls, hooks) -> {
            Class<?> c = load(cls);
            Set<Class<?>> types = hooks.stream().map(ProductFormsUseTheirArgumentsTest::load)
                    .collect(Collectors.toCollection(java.util.LinkedHashSet::new));
            for (Object form : instances(c, types)) {
                out.put(form, types);
            }
        });
        return out;
    }

    static Class<?> load(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new AssertionError("no class " + name, e);
        }
    }

    /** {@code c}'s instances: its enum constants, else every static field of {@code c}, its enclosing class or a hook
     *  type holding an instance of exactly {@code c}, else one made by its no-argument constructor. None: a failure. */
    static List<Object> instances(Class<?> c, Set<Class<?>> hooks) {
        if (c.isEnum()) {
            return List.of(c.getEnumConstants());
        }
        List<Class<?>> holders = new ArrayList<>(List.of(c));
        if (c.getEnclosingClass() != null) {
            holders.add(c.getEnclosingClass());
        }
        holders.addAll(hooks);
        List<Object> out = new ArrayList<>();
        for (Class<?> h : holders) {
            for (Field f : h.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers()) && !f.getType().isPrimitive()) {
                    try {
                        f.setAccessible(true);
                        Object v = f.get(null);
                        if (v != null && v.getClass() == c && out.stream().noneMatch(o -> o == v)) {
                            out.add(v);
                        }
                    } catch (IllegalAccessException | RuntimeException e) {
                        throw new AssertionError("cannot read " + h.getName() + "#" + f.getName(), e);
                    }
                }
            }
        }
        if (!out.isEmpty()) {
            return out;
        }
        try {
            var ctor = c.getDeclaredConstructor();
            ctor.setAccessible(true);
            return List.of(ctor.newInstance());
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(c.getName() + ": no instance this guard can reach -- an enum constant, a static"
                    + " field or a no-argument constructor", e);
        }
    }

    /** Whether {@code t} is a function: an interface of java.util.function, Runnable or Callable, or one annotated
     *  as functional. */
    static boolean isFunction(Class<?> t) {
        return t.isInterface() && (t.getName().startsWith("java.util.function.") || t == Runnable.class
                || t == java.util.concurrent.Callable.class || t.isAnnotationPresent(FunctionalInterface.class));
    }

    /** One sentinel call record: which function argument was called. */
    static final class Calls {
        final boolean[] called;

        Calls(int n) {
            called = new boolean[n];
        }
    }

    /** The samples for a parameter of {@code type}: a function's sentinel is made per call. */
    static List<Object> samples(Class<?> type) {
        if (type == boolean.class || type == Boolean.class) {
            return List.of(false, true);
        }
        if (type.isEnum()) {
            return List.of(type.getEnumConstants());
        }
        return List.of(sample(type));
    }

    /** One value of {@code type}, or a failure naming it: the guard stays total (a new parameter type is added here). */
    static Object sample(Type type) {
        Class<?> raw = raw(type);
        if (raw == SqlExpr.class) {
            return new SqlExpr.StringLit("L-A sentinel");
        }
        if (raw == SqlSelect.class || raw == SqlQuery.class) {
            return SqlSelect.starOf(new SqlSource.Dual());
        }
        if (raw == String.class) {
            return "L-A sentinel";
        }
        if (raw == int.class || raw == Integer.class) {
            return 0;
        }
        if (raw == long.class || raw == Long.class) {
            return 0L;
        }
        if (raw == Class.class) {
            return Object.class;
        }
        if (raw == RuntimeException.class) {
            return new RuntimeException("L-A sentinel");
        }
        if (raw == Object.class) {
            return "L-A sentinel";
        }
        throw new AssertionError("no sample of " + type.getTypeName() + ": add one to ProductFormsUseTheirArgumentsTest");
    }

    static Class<?> raw(Type t) {
        if (t instanceof Class<?> c) {
            return c;
        }
        if (t instanceof ParameterizedType p) {
            return (Class<?>) p.getRawType();
        }
        if (t instanceof WildcardType w) {
            return raw(w.getUpperBounds()[0]);
        }
        if (t instanceof java.lang.reflect.TypeVariable<?> v) {
            return raw(v.getBounds()[0]);
        }
        throw new AssertionError("no raw type of " + t.getTypeName());
    }

    /** A sentinel for the function parameter {@code type} at {@code index}: marks its call, answers its own argument
     *  (an operator) or a sample of what it returns. */
    static Object sentinel(Type type, int index, Calls calls) {
        Class<?> fn = raw(type);
        return Proxy.newProxyInstance(fn.getClassLoader(), new Class<?>[] {fn}, (proxy, m, args) -> {
            if (m.getDeclaringClass() == Object.class) {
                return switch (m.getName()) {
                    case "equals" -> proxy == args[0];
                    case "hashCode" -> System.identityHashCode(proxy);
                    default -> "L-A sentinel " + fn.getSimpleName();
                };
            }
            if (m.isDefault()) {
                return java.lang.reflect.InvocationHandler.invokeDefault(proxy, m, args);
            }
            calls.called[index] = true;
            if (m.getReturnType() == void.class) {
                return null;
            }
            if (m.getReturnType() == boolean.class) {
                return false;
            }
            if (fn == java.util.function.UnaryOperator.class || fn == java.util.function.BinaryOperator.class) {
                return args[0];
            }
            Type ret = returned(type);
            return sample(ret);
        });
    }

    /** What the function {@code type} returns: its last type argument (Supplier's T, Function's R). */
    static Type returned(Type type) {
        if (type instanceof ParameterizedType p) {
            Type[] a = p.getActualTypeArguments();
            return a[a.length - 1];
        }
        throw new AssertionError("a raw function type: " + type.getTypeName());
    }

    /** "Form#method(params) arg N" for every function argument {@code form} never calls through {@code hook}. */
    static Set<String> uncalled(Object form, Class<?> hook, Map<String, String> threw) {
        Set<String> out = new TreeSet<>();
        for (Method m : hook.getMethods()) {
            if (Modifier.isStatic(m.getModifiers()) || m.getDeclaringClass() == Object.class) {
                continue;
            }
            Class<?>[] ps = m.getParameterTypes();
            Type[] gs = m.getGenericParameterTypes();
            if (Arrays.stream(ps).noneMatch(ProductFormsUseTheirArgumentsTest::isFunction)) {
                continue;
            }
            Method impl;
            try {
                impl = form.getClass().getMethod(m.getName(), ps);
                impl.setAccessible(true);
            } catch (ReflectiveOperationException | RuntimeException e) {
                throw new AssertionError(form.getClass().getName() + " does not answer " + m, e);
            }
            boolean[] ever = new boolean[ps.length];
            List<List<Object>> options = new ArrayList<>();
            for (Class<?> p : ps) {
                options.add(isFunction(p) ? List.of(FN) : samples(p));
            }
            for (List<Object> combo : combos(options)) {
                Calls calls = new Calls(ps.length);
                Object[] args = new Object[ps.length];
                for (int i = 0; i < ps.length; i++) {
                    args[i] = combo.get(i) == FN ? sentinel(gs[i], i, calls) : combo.get(i);
                }
                try {
                    impl.invoke(form, args);
                } catch (InvocationTargetException e) {
                    threw.merge(name(form, m), e.getCause().getClass().getSimpleName(), (a, b) -> a);
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
                for (int i = 0; i < ps.length; i++) {
                    ever[i] |= calls.called[i];
                }
            }
            for (int i = 0; i < ps.length; i++) {
                if (isFunction(ps[i]) && !ever[i]) {
                    out.add(name(form, m) + " arg " + i);
                }
            }
        }
        return out;
    }

    private static final Object FN = new Object();

    static String name(Object form, Method m) {
        String cls = form.getClass().getName().substring(form.getClass().getPackageName().length() + 1);
        String self = form instanceof Enum<?> e ? cls + "." + e.name() : cls;
        return self + "#" + m.getName() + Arrays.stream(m.getParameterTypes()).map(Class::getSimpleName)
                .collect(Collectors.joining(", ", "(", ")"));
    }

    static List<List<Object>> combos(List<List<Object>> options) {
        List<List<Object>> out = new ArrayList<>(List.of(List.of()));
        for (List<Object> o : options) {
            List<List<Object>> next = new ArrayList<>();
            for (List<Object> prefix : out) {
                for (Object v : o) {
                    List<Object> c = new ArrayList<>(prefix);
                    c.add(v);
                    next.add(c);
                }
            }
            out = next;
        }
        return out;
    }

    static Set<String> found(Map<String, Set<String>> classHooks, Map<String, String> threw) {
        Set<String> out = new TreeSet<>();
        productForms(classHooks).forEach((form, hooks) -> hooks.forEach(h -> out.addAll(uncalled(form, h, threw))));
        return out;
    }

    @Test
    void everyProductFormCallsTheFunctionsItIsHanded() {
        Map<String, String> threw = new TreeMap<>();
        Set<String> found = found(HookLambdasTest.CLASS_HOOKS, threw);
        assertEquals(new TreeSet<>(UNCALLED.keySet()), found, "a product form is handed a function it never calls --"
                + " a form the product builds for the edge alone (upstream's, handed in): move it to the edge, or rule"
                + " it here (forms that threw on the samples, which may hide a call: " + threw + ")");
    }

    /** The planted kind: a product form handed two forms that calls one. */
    interface PlantedTwoForm {
        String pick(java.util.function.Supplier<String> ours, java.util.function.Supplier<String> upstreams);

        String keep(String text, java.util.function.UnaryOperator<String> rewrite);
    }

    static final class PlantedProductForm implements PlantedTwoForm {
        static final PlantedProductForm INSTANCE = new PlantedProductForm();

        @Override
        public String pick(java.util.function.Supplier<String> ours, java.util.function.Supplier<String> upstreams) {
            return ours.get();
        }

        @Override
        public String keep(String text, java.util.function.UnaryOperator<String> rewrite) {
            return text;
        }
    }

    @Test
    void theGuardBites() {
        Set<String> found = found(Map.of(PlantedProductForm.class.getName(), Set.of(PlantedTwoForm.class.getName())),
                new TreeMap<>());
        assertEquals(Set.of("ProductFormsUseTheirArgumentsTest$PlantedProductForm#keep(String, UnaryOperator) arg 1",
                "ProductFormsUseTheirArgumentsTest$PlantedProductForm#pick(Supplier, Supplier) arg 1"), found);
    }
}
