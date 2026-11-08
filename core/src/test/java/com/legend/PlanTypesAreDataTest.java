// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.TestPlan;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * GUARD 1: THE PLAN'S TYPES ARE DATA (docs/PLAN_AS_DATA_2026_11_07.md 2.10, the design's step 3, 2026-11-08; the user,
 * 2026-11-08: "the thing I really want to ensure is that there is no code in the plan it's all data"). Every type
 * reachable from the product's plan ({@link ExecutionPlan}) and a test body's ({@link TestPlan}) -- record components,
 * a sealed interface's permitted subtypes, the elements of a list, a set or a map -- is a primitive or its box, a
 * String, a BigDecimal or BigInteger, a date or time value, an enum, or a record or sealed interface of the plans' own
 * libraries or the SQL tree ({@code com.legend.sql}, pure data by AGENTS.md 3a). A functional type, a Throwable,
 * Object, a wildcard, a type variable or a type of any other library is a violation. The violations found when the
 * guard was introduced are pinned below, shrink-only: a pin leaves with a dated note; a new violation fails.
 */
@Tag("guardrail")
class PlanTypesAreDataTest {

    /** The violations at the guard's introduction (2026-11-08), each the offending type and the first path to it. */
    private static final Set<String> PINNED = Set.<String>of(
            // (2026-11-08, the plan-as-data design's step 7: the model's ConnectionDefinition at Declared.connection and
            // its DatabaseType enum at Platform.type left the pins -- the plan holds its own faithful copy of the
            // connection description, executionplan.Connection and the records it names)
            // (2026-11-08, the plan-as-data design's step 9: the window frame bound's abstract Number left the pins --
            // the SQL tree's offset is a closed data type, Frame.Offset: an integer, a float or a decimal. Guard 1 is at
            // zero: a new violation fails, nothing is pinned)
            );

    private static final Set<String> DATA_PACKAGES = Set.of("com.legend.executionplan", "com.legend.testexecutionplan",
            "com.legend.sql",
            // 2026-11-08, the plan-as-data design's step 8 (2.10's guard 1 names them): the edge's extension records,
            // reached only through the test plan's Extension -- every implementation on the classpath, walked as data
            "com.legend.edge");

    private static final Set<Class<?>> LEAVES = Set.of(String.class, Boolean.class, Byte.class, Short.class,
            Integer.class, Long.class, Float.class, Double.class, Character.class, java.math.BigDecimal.class,
            java.math.BigInteger.class, java.time.LocalDate.class, java.time.LocalDateTime.class,
            java.time.LocalTime.class, java.time.Instant.class, java.time.OffsetDateTime.class,
            java.time.ZonedDateTime.class);

    /** Every class on the classpath that implements the test plan's {@code Extension} (the edge's records). */
    private static List<Class<?>> extensionImplementations() {
        List<Class<?>> out = new java.util.ArrayList<>();
        for (var c : new com.tngtech.archunit.core.importer.ClassFileImporter().importPackages("com.legend")) {
            if (!c.isInterface() && c.isAssignableTo(com.legend.testexecutionplan.Extension.class)) {
                out.add(c.reflect());
            }
        }
        if (out.isEmpty()) {
            throw new IllegalStateException("no implementation of the test plan's Extension on the classpath to check");
        }
        return out;
    }

    @Test
    void everyTypeAPlanHoldsIsData() {
        Set<String> violations = new TreeSet<>();
        Set<Type> seen = new HashSet<>();
        Deque<Object[]> todo = new ArrayDeque<>();
        todo.add(new Object[] {ExecutionPlan.class, "ExecutionPlan"});
        todo.add(new Object[] {TestPlan.class, "TestPlan"});
        while (!todo.isEmpty()) {
            Object[] next = todo.poll();
            Type type = (Type) next[0];
            String path = (String) next[1];
            // every (type, path) is judged; only the walk INTO a type is done once (the slice 2 invariant audit,
            // 2026-11-08: the seen-check stood before the judgment, so a second field of an already-seen bad type --
            // Following.n beside Preceding.n -- was never reported)
            switch (type) {
                case ParameterizedType p -> {
                    Class<?> raw = (Class<?>) p.getRawType();
                    if (raw == List.class || raw == Set.class || raw == Map.class) {
                        if (seen.add(type)) {
                            for (Type arg : p.getActualTypeArguments()) {
                                todo.add(new Object[] {arg, path + "<>"});
                            }
                        }
                    } else {
                        violations.add(p.getTypeName() + " at " + path);
                    }
                }
                case WildcardType w -> violations.add("wildcard " + w.getTypeName() + " at " + path);
                case TypeVariable<?> v -> violations.add("type variable " + v.getName() + " at " + path);
                case Class<?> c -> {
                    if (c.isPrimitive() || LEAVES.contains(c)) {
                        continue;
                    }
                    if (c.isEnum()) {
                        // an enum carries no state of a type the walk would see (fields, constant bodies): one of the
                        // plans' own libraries, the SQL tree's or the error vocabulary's (a failure's phase) alone
                        // (the slice 1 audit, 2026-11-08)
                        if (!DATA_PACKAGES.contains(c.getPackageName()) && !c.getPackageName().equals("com.legend.error")) {
                            violations.add("enum " + c.getName() + " at " + path);
                        }
                        // its own state and its constants walked too (the slice 3 invariant audit, 2026-11-08: an enum
                        // was taken on trust, so a field holding a lambda would have passed): every instance field
                        // judged as a component, and a constant with a body -- a class of its own, code -- refused
                        if (seen.add(c)) {
                            for (java.lang.reflect.Field f : c.getDeclaredFields()) {
                                if (!f.isEnumConstant() && !f.isSynthetic()
                                        && !java.lang.reflect.Modifier.isStatic(f.getModifiers())) {
                                    todo.add(new Object[] {f.getGenericType(), c.getSimpleName() + "." + f.getName()});
                                }
                            }
                            for (Object constant : c.getEnumConstants()) {
                                if (constant.getClass() != c) {
                                    violations.add("enum constant with a body " + c.getName() + "." + constant + " at "
                                            + path);
                                }
                            }
                        }
                        continue;
                    }
                    if (!seen.add(c) && DATA_PACKAGES.contains(c.getPackageName())) {
                        // a data type already walked: its components were judged on its first path
                        continue;
                    }
                    if (c == com.legend.testexecutionplan.Extension.class) {
                        // the edge's records (2.10, guard 1): every implementation on the classpath, each walked as
                        // the plan's data -- an extension point implemented by anything but data fails here
                        for (Class<?> impl : extensionImplementations()) {
                            todo.add(new Object[] {impl, "Extension implemented by"});
                        }
                        continue;
                    }
                    if (!DATA_PACKAGES.contains(c.getPackageName())) {
                        violations.add(c.getName() + " at " + path);
                        continue;
                    }
                    if (c.isRecord()) {
                        for (RecordComponent rc : c.getRecordComponents()) {
                            todo.add(new Object[] {rc.getGenericType(), c.getSimpleName() + "." + rc.getName()});
                        }
                    } else if (c.isSealed()) {
                        for (Class<?> sub : c.getPermittedSubclasses()) {
                            todo.add(new Object[] {sub, c.getSimpleName() + " permits"});
                        }
                    } else {
                        violations.add(c.getName() + " (neither a record nor a sealed interface) at " + path);
                    }
                }
                default -> violations.add(type.getTypeName() + " at " + path);
            }
        }
        assertEquals(new TreeSet<>(PINNED), violations, "the plans' types are data: a new violation is a type a plan"
                + " must not hold; a pin that no longer holds leaves PINNED with a dated note");
    }
}
