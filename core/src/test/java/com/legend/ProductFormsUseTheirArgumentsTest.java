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
 * {@code computedPruning(pruned, kept)}, {@code existsJoin(filtered, joinForm)} -- and a function the product form
 * never calls is code the product builds for the edge alone: upstream's form, written in the product and handed in.
 * This guard calls every product form of every hook type (HookLambdasTest.CLASS_HOOKS: each product or host class implementing a
 * hook, two-form kind or style, pinned there exactly) through every method that takes a function, with a sentinel per
 * function and a sample per other parameter (every constant of an enum, both Booleans), and reports each function the
 * form never calls in any call. Each is pinned with its ruling ({@link #UNCALLED}); a new one fails the build.
 */
@Tag("guardrail")
class ProductFormsUseTheirArgumentsTest {

    /** Each function a product form never calls that is upstream's alternative, computed by the product and handed in:
     *  "Form#method(params) arg N" -> "census line OWNER (date): why". Counted, one line per form (the orchestrator's
     *  ruling of 2026-11-18, 06:40: E-SQL's L9 and L6, inside the withShape design (i)). */
    static final Map<String, String> UNCALLED = Map.of();
            // (existsJoin's and computedPruning's: CLOSED 2026-11-18 by E-SQL's 63d2ad6ed and 4b39cdd82 -- the lowering
            // hands its statement and sites, the edge's form composes legend-engine's form itself)
            // (executeValues': CLOSED 2026-11-18 by E-SQL -- the form takes the product's own serialization alone,
            // executeValues(SqlSelect own); the edge folds it itself; L-A.LoweringForms closes)
    // (RefusedExecutedValues#relation and #classes: ruled no finding 2026-11-18, below -- L-A.ExecutedValues closes)

    /** Each function a product form never calls that is no finding (the orchestrator's ruling of 2026-11-18, 06:40: a
     *  hook's own input from the product's generic code, not upstream's alternative), with the reason. Not counted. */
    static final Map<String, String> RULED = Map.of(
            "ProductLoweringForms#relations(Supplier, Supplier) arg 1", "2026-11-18: () -> lower(root), the product's"
                    + " own plain lowering, which it runs itself when sharing is off; only the choice is upstream's",
            "ProductLoweringForms#view(SqlSelect, Supplier, String) arg 1", "2026-11-18: this::nextAlias, the product's"
                    + " alias counter, handed in so the edge can name its frame",
            // (executeValues' arg 1: GONE 2026-11-18 -- E-SQL's executeValues(SqlSelect own), the product's own
            // serialization alone, no function handed)
            // the functions inside ExistsSites (seen since the GUARD stream's widening of 2026-11-18, train 18, on E-SQL's
            // fold move, 612b194708): the lowering's own alias minter and resolver-zone lookup, handed to every form (the
            // orchestrator's ruling on L6, item 1: the product hands its statement and sites, never upstream's
            // composition); the product's form keeps the EXISTS as lowered -- view's arg 1's case
            "ProductLoweringForms#existsJoin(SqlSelect, ExistsSites) arg 1.alias", "2026-11-18: this::nextAlias, the"
                    + " product's alias counter, handed in so the edge can name its join's keys (view's arg 1's case)",
            "ProductLoweringForms#existsJoin(SqlSelect, ExistsSites) arg 1.zones", "2026-11-18: the lowering's own"
                    + " resolver zones per conjunct, handed in so the edge can split its join (the L6 ruling's sites)",
            "NoEdgeRun$Hearing#decide(Session, Leases, Function) arg 2", "2026-11-18: the run's session lookup; the"
                    + " product's run has no appeals to decide",
            "NoEdgeRun$Hearing#afterFailure(RuntimeException, Session, Leases, Function) arg 3", "2026-11-18: the run's"
                    + " session lookup; the product's run has no appeals to decide",
            // the orchestrator's ruling of 2026-11-18 (train 18, E-Java's item 2b, 11bb6517d3): the supplier computes lite's
            // own activity SQL (S310), not upstream's alternative, and the product's form refuses legend-engine's
            // envelopes before it would need it -- a refusal's consequence, as NoEdgeRun$Hearing's
            "RefusedExecutedValues.REFUSED#relation(TypedSpec, Supplier) arg 1", "2026-11-18: the activity's SQL, lite's"
                    + " own (S310); the product's form refuses legend-engine's tdsBuilder envelope before it would need it",
            "RefusedExecutedValues.REFUSED#classes(TypedSpec, Supplier) arg 1", "2026-11-18: the activity's SQL, lite's"
                    + " own (S310); the product's form refuses legend-engine's classBuilder envelope before it would need it");

    /** The census lines this guard holds: one per form with a finding. */
    static Set<String> lines() {
        Set<String> out = new TreeSet<>();
        UNCALLED.values().forEach(v -> out.add(v.split(" ")[0]));
        return out;
    }

    /** Every product form of every hook type taking a function: the instances of each class HookLambdasTest pins (an
     *  enum's constants, a static field holding one, else its no-argument constructor), with each such hook type it
     *  implements. */
    static Map<Object, Set<Class<?>>> productForms(Map<String, Set<String>> classHooks) {
        Map<Object, Set<Class<?>>> out = new java.util.LinkedHashMap<>();
        classHooks.forEach((cls, hooks) -> {
            Class<?> c = load(cls);
            // only a hook type with a method taking a function (or a record carrying one) is called: a form of none
            // needs no instance
            Set<Class<?>> types = hooks.stream().map(ProductFormsUseTheirArgumentsTest::load)
                    .filter(h -> Arrays.stream(h.getMethods()).anyMatch(m -> !Modifier.isStatic(m.getModifiers())
                            && Arrays.stream(m.getParameterTypes())
                                    .anyMatch(ProductFormsUseTheirArgumentsTest::carriesFunction)))
                    .collect(Collectors.toCollection(java.util.LinkedHashSet::new));
            if (types.isEmpty()) {
                return;
            }
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
            // a no-argument constructor, else the class's one constructor with a sample per parameter (a record
            // the product's form makes per run, as NoEdgeRun$Hearing)
            java.lang.reflect.Constructor<?>[] ctors = c.getDeclaredConstructors();
            if (ctors.length != 1) {
                var ctor = c.getDeclaredConstructor();
                ctor.setAccessible(true);
                return List.of(ctor.newInstance());
            }
            ctors[0].setAccessible(true);
            Object[] args = new Object[ctors[0].getParameterCount()];
            for (int i = 0; i < args.length; i++) {
                args[i] = sample(ctors[0].getGenericParameterTypes()[i]);
            }
            return List.of(ctors[0].newInstance(args));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(c.getName() + ": no instance this guard can reach -- an enum constant, a static"
                    + " field, a no-argument constructor or its one constructor", e);
        }
    }

    /** Whether {@code t} is a function: an interface of java.util.function, Runnable or Callable, or one annotated
     *  as functional. */
    static boolean isFunction(Class<?> t) {
        return t.isInterface() && (t.getName().startsWith("java.util.function.") || t == Runnable.class
                || t == java.util.concurrent.Callable.class || t.isAnnotationPresent(FunctionalInterface.class));
    }

    /** Whether {@code t} hands a form a function: a function, or a record with a function component (the GUARD
     *  stream's widening of 2026-11-18, train 18, on E-SQL's fold move: the lowering now hands its forms a record of
     *  its own functions, ExistsSites, and a function inside a record was invisible to this guard). */
    static boolean carriesFunction(Class<?> t) {
        return isFunction(t) || (t.isRecord() && !STYLE_RECORDS.containsKey(t.getName())
                && Arrays.stream(t.getRecordComponents()).anyMatch(c -> isFunction(c.getType())));
    }

    /** The records that ARE the style, handed whole to a form, not inputs a caller makes for it: their function
     *  components are the style's facets (the product's refusing, the edge's answering), never upstream's form computed
     *  by the product. Pinned exactly, shrink-only; each must hold a hook (theStyleRecordsAreStyles). */
    static final Map<String, String> STYLE_RECORDS = Map.of(
            "com.legend.lowering.Styled", "2026-11-18 (the GUARD stream's widening, train 18): the plan style's text"
                    + " facets as the lowering reads them; pureOnly is the PureOnlyTexts facet -- Styled.PRODUCT's throws"
                    + " the call's refusal, the edge's style answers legend-pure's texts -- so ListTexts#mixedList's and"
                    + " VerdictWords#elementText's product forms rightly never ask it");

    /** One sentinel call record: which function was called, by its slot ("arg N", or "arg N.component" for a
     *  function inside a record argument). */
    static final class Calls {
        final Set<String> called = new TreeSet<>();
    }

    /** The samples for a parameter of {@code type}: a function's sentinel is made per call. */
    static List<Object> samples(Class<?> type) {
        if (type == boolean.class || type == Boolean.class) {
            return List.of(false, true);
        }
        if (type.isEnum()) {
            return List.of(type.getEnumConstants());
        }
        return java.util.Collections.singletonList(sample(type));
    }

    /** One value of {@code type}: the plan's and the MIR's own samples, an interface's poison proxy, else none (a
     *  form that reads it throws, reported); a primitive with no sample fails, naming it. */
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
        if (raw.isPrimitive()) {
            throw new AssertionError("no sample of " + type.getTypeName()
                    + ": add one to ProductFormsUseTheirArgumentsTest");
        }
        if (raw.isInterface() && !raw.isSealed()) {
            // a poison: the form that uses it throws, and the run reports the throw beside what it found (a SEALED
            // interface cannot be proxied -- TypedSpec, handed to ExecutedValues' forms since E-Java's item 2b,
            // 11bb6517d3, train 18, 2026-11-18 -- so it takes the null below, which a form that reads it throws on)
            return Proxy.newProxyInstance(raw.getClassLoader(), new Class<?>[] {raw}, (proxy, m, args) -> {
                if (m.getDeclaringClass() == Object.class) {
                    return switch (m.getName()) {
                        case "equals" -> proxy == args[0];
                        case "hashCode" -> System.identityHashCode(proxy);
                        default -> "L-A poison " + raw.getSimpleName();
                    };
                }
                throw new IllegalStateException("L-A poison " + raw.getSimpleName() + "#" + m.getName());
            });
        }
        // any other reference: none (a form that reads it throws, and the run reports the throw)
        return null;
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

    /** A sentinel for the function parameter {@code type} in {@code slot}: marks its call, answers its own argument
     *  (an operator) or a sample of what it returns. */
    static Object sentinel(Type type, String slot, Calls calls) {
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
            calls.called.add(slot);
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
            if (Arrays.stream(ps).noneMatch(ProductFormsUseTheirArgumentsTest::carriesFunction)) {
                continue;
            }
            Method impl;
            try {
                impl = form.getClass().getMethod(m.getName(), ps);
                impl.setAccessible(true);
            } catch (ReflectiveOperationException | RuntimeException e) {
                throw new AssertionError(form.getClass().getName() + " does not answer " + m, e);
            }
            Set<String> ever = new TreeSet<>();
            List<List<Object>> options = new ArrayList<>();
            for (Class<?> p : ps) {
                options.add(carriesFunction(p) ? List.of(FN) : samples(p));
            }
            for (List<Object> combo : combos(options)) {
                Calls calls = new Calls();
                Object[] args = new Object[ps.length];
                for (int i = 0; i < ps.length; i++) {
                    args[i] = combo.get(i) != FN ? combo.get(i)
                            : isFunction(ps[i]) ? sentinel(gs[i], "arg " + i, calls)
                            : carrying(ps[i], "arg " + i, calls);
                }
                try {
                    impl.invoke(form, args);
                } catch (InvocationTargetException e) {
                    threw.merge(name(form, m), e.getCause().getClass().getSimpleName(), (a, b) -> a);
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
                ever.addAll(calls.called);
            }
            for (String slot : slots(ps)) {
                if (!ever.contains(slot)) {
                    out.add(name(form, m) + " " + slot);
                }
            }
        }
        return out;
    }

    /** A record argument carrying functions, made by its canonical constructor: a sentinel in each function
     *  component's slot ("arg N.component"), a sample in every other. */
    static Object carrying(Class<?> record, String slot, Calls calls) {
        java.lang.reflect.RecordComponent[] cs = record.getRecordComponents();
        Class<?>[] types = Arrays.stream(cs).map(java.lang.reflect.RecordComponent::getType).toArray(Class<?>[]::new);
        Object[] args = new Object[cs.length];
        for (int i = 0; i < cs.length; i++) {
            args[i] = isFunction(types[i]) ? sentinel(cs[i].getGenericType(), slot + "." + cs[i].getName(), calls)
                    : samples(types[i]).get(0);
        }
        try {
            java.lang.reflect.Constructor<?> ctor = record.getDeclaredConstructor(types);
            ctor.setAccessible(true);
            return ctor.newInstance(args);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("cannot make " + record.getName() + " with sentinels", e);
        }
    }

    /** The function slots of a method's parameters: "arg N" per function, "arg N.component" per function component
     *  of a record argument. */
    static List<String> slots(Class<?>[] ps) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < ps.length; i++) {
            if (isFunction(ps[i])) {
                out.add("arg " + i);
            } else if (carriesFunction(ps[i])) {
                for (java.lang.reflect.RecordComponent c : ps[i].getRecordComponents()) {
                    if (isFunction(c.getType())) {
                        out.add("arg " + i + "." + c.getName());
                    }
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
        Set<String> pinned = new TreeSet<>(UNCALLED.keySet());
        pinned.addAll(RULED.keySet());
        assertEquals(pinned, found, "a product form is handed a function it never calls --"
                + " a form the product builds for the edge alone (upstream's, handed in): move it to the edge, or rule"
                + " it here (forms that threw on the samples, which may hide a call: " + threw + ")");
    }

    /** Each style record is a record holding a hook (a two-form kind or a facet kind): the exemption cannot cover a
     *  record of a caller's inputs. */
    @Test
    void theStyleRecordsAreStyles() {
        Set<Class<?>> hooks = new java.util.HashSet<>(com.legend.testplan.PlanStyleFacets.TWO_FORMS);
        hooks.addAll(com.legend.testplan.PlanStyleFacets.KINDS);
        for (String name : STYLE_RECORDS.keySet()) {
            Class<?> c = load(name);
            assertTrue(c.isRecord() && Arrays.stream(c.getRecordComponents()).anyMatch(rc -> hooks.contains(rc.getType())),
                    name + " is a record holding a hook");
        }
        assertTrue(Arrays.stream(PlantedSites.class.getRecordComponents()).noneMatch(rc -> hooks.contains(rc.getType())),
                "the planted sites hold no hook");
    }

    /** The planted kind: a product form handed two forms that calls one. */
    interface PlantedTwoForm {
        String pick(java.util.function.Supplier<String> ours, java.util.function.Supplier<String> upstreams);

        String keep(String text, java.util.function.UnaryOperator<String> rewrite);

        String join(String text, PlantedSites sites);
    }

    /** A record of the caller's functions handed to a form (as lowering.ExistsSites). */
    record PlantedSites(java.util.function.Supplier<String> alias, java.util.function.UnaryOperator<String> zones,
            int depth) {
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

        @Override
        public String join(String text, PlantedSites sites) {
            return text + sites.alias().get();
        }
    }

    @Test
    void theGuardBites() {
        Set<String> found = found(Map.of(PlantedProductForm.class.getName(), Set.of(PlantedTwoForm.class.getName())),
                new TreeMap<>());
        assertEquals(Set.of("ProductFormsUseTheirArgumentsTest$PlantedProductForm#keep(String, UnaryOperator) arg 1",
                "ProductFormsUseTheirArgumentsTest$PlantedProductForm#pick(Supplier, Supplier) arg 1",
                "ProductFormsUseTheirArgumentsTest$PlantedProductForm#join(String, PlantedSites) arg 1.zones"), found);
    }
}
