// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A plan's parameter values checked against its declarations and converted for binding
 * (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3), with no model: the plan declares each parameter's name, Pure
 * type, multiplicity and an enumeration's names. A caller hands each value as its Pure type's Java value -- an Integer a
 * {@code Long}, a Float a {@code Double}, a Decimal a {@code BigDecimal}, a Number any of those three, a Boolean a
 * {@code Boolean}, a String a {@code String}, a StrictDate a {@code LocalDate}, a DateTime a {@code LocalDateTime} in
 * UTC, a Date either of those two, an enumeration value its name; a collection a {@code List}; none (null, or an empty
 * list) for no value. A request's values are made so by their reader (the server's execute reads legend-engine's
 * protocol values, step 4); the runner reads no text. They leave as their slots bind them: as they came, but a Float as
 * its literal is typed (the numeric charter's Rule 1) and a Decimal of a negative scale as its plain digits.
 */
public final class PlanParameters {

    private PlanParameters() {
    }

    /** One parameter's checked value: one value, a list, or none (an optional one's absence, an empty list). */
    public sealed interface Checked permits One, Many, None {
    }

    /** One value, as its slot binds it. */
    public record One(Object value) implements Checked {
    }

    /** A list's values, each as an element of the slot's array binds it. */
    public record Many(List<Object> values) implements Checked {
        public Many {
            values = List.copyOf(values);
        }
    }

    /** No value: an optional parameter's absence (a typed null), or a list's absence (an empty array). */
    public record None() implements Checked {
    }

    /** One value's conversion: the value its slot binds, or why it is refused. */
    private sealed interface Conversion permits Converted, Refused {
    }

    private record Converted(Object value) implements Conversion {
    }

    private record Refused(String why) implements Conversion {
    }

    /**
     * {@code values} checked against {@code declared} and converted, by name: every declared parameter, its checked
     * value. A value for no declared parameter is ignored.
     *
     * @throws IllegalArgumentException naming every problem at once: {@code Missing external parameter(s): ...} for each
     *                                  required parameter given no value, {@code Invalid provided parameter(s): [...]}
     *                                  for each value that is not one its parameter takes
     */
    public static Map<String, Checked> check(List<ExecutionPlan.Parameter> declared, Map<String, ?> values) {
        List<String> missing = new ArrayList<>();
        List<String> invalid = new ArrayList<>();
        Map<String, Checked> out = new LinkedHashMap<>();
        for (ExecutionPlan.Parameter p : declared) {
            Object value = values.get(p.name());
            if (value == null || value instanceof List<?> list && list.isEmpty()) {
                if (p.multiplicity().lower() > 0) {
                    missing.add(p.name() + ":" + p.type() + "[" + multiplicity(p.multiplicity()) + "]");
                } else {
                    out.put(p.name(), new None());
                }
                continue;
            }
            String problem = checked(p, value, out);
            if (problem != null) {
                invalid.add("parameter '" + p.name() + "' (" + p.type() + "[" + multiplicity(p.multiplicity()) + "]): "
                        + problem);
            }
        }
        List<String> problems = new ArrayList<>();
        if (!missing.isEmpty()) {
            problems.add("Missing external parameter(s): " + String.join(", ", missing));
        }
        if (!invalid.isEmpty()) {
            problems.add("Invalid provided parameter(s): [" + String.join("; ", invalid) + "]");
        }
        if (!problems.isEmpty()) {
            throw new IllegalArgumentException(String.join("; ", problems));
        }
        return out;
    }

    /** {@code value} (one value or a non-empty list) of {@code p} checked: its checked form put in {@code out}, or what
     *  is wrong with it. */
    private static @com.legend.base.Nullable String checked(ExecutionPlan.Parameter p, Object value,
            Map<String, Checked> out) {
        ExecutionPlan.Multiplicity m = p.multiplicity();
        if (Integer.valueOf(1).equals(m.upper())) {
            if (value instanceof List<?> list) {
                return "takes one value, given a list of " + list.size();
            }
            return switch (convert(p, value)) {
                case Converted c -> {
                    out.put(p.name(), new One(c.value()));
                    yield null;
                }
                case Refused r -> r.why();
            };
        }
        if (!(value instanceof List<?> list)) {
            return "takes a list, given one value";
        }
        Integer upper = m.upper();
        if (list.size() < m.lower() || upper != null && list.size() > upper) {
            return "takes " + multiplicity(m) + " values, given " + list.size();
        }
        List<Object> converted = new ArrayList<>(list.size());
        for (Object v : list) {
            if (v == null) {
                return "a list holds no null";
            }
            switch (convert(p, v)) {
                case Converted c -> converted.add(c.value());
                case Refused r -> {
                    return r.why();
                }
            }
        }
        out.put(p.name(), new Many(converted));
        return null;
    }

    /** {@code v} (one value, not null) of {@code p} as its slot binds it, or why it is not one {@code p} takes. */
    private static Conversion convert(ExecutionPlan.Parameter p, Object v) {
        if (!p.enumValues().isEmpty()) {
            return v instanceof String name && p.enumValues().contains(name) ? new Converted(name)
                    : new Refused(shown(v) + " is not a value of " + p.type() + " " + p.enumValues());
        }
        return switch (p.type()) {
            case "Integer" -> v instanceof Long ? new Converted(v) : notA(v, "an Integer is a Long");
            case "Float" -> v instanceof Double d ? floatValue(d) : notA(v, "a Float is a Double");
            case "Decimal" -> v instanceof BigDecimal d ? new Converted(plain(d)) : notA(v, "a Decimal is a BigDecimal");
            case "Number" -> switch (v) {
                case Long l -> new Converted(l);
                case Double d -> floatValue(d);
                case BigDecimal d -> new Converted(plain(d));
                default -> notA(v, "a Number is a Long, a Double or a BigDecimal");
            };
            case "Boolean" -> v instanceof Boolean ? new Converted(v) : notA(v, "a Boolean is a Boolean");
            case "String" -> v instanceof String ? new Converted(v) : notA(v, "a String is a String");
            case "StrictDate" -> v instanceof LocalDate ? new Converted(v) : notA(v, "a StrictDate is a LocalDate");
            case "DateTime" -> v instanceof LocalDateTime t ? dateTime(t)
                    : notA(v, "a DateTime is a LocalDateTime (in UTC)");
            case "Date" -> v instanceof LocalDate ? new Converted(v) : v instanceof LocalDateTime t ? dateTime(t)
                    : notA(v, "a Date is a LocalDate or a LocalDateTime (in UTC)");
            default -> new Refused("a " + p.type() + " value is not bound by a plan (PARK-21)");
        };
    }

    private static Refused notA(Object v, String expected) {
        return new Refused("given " + shown(v) + " (" + v.getClass().getSimpleName() + "): " + expected);
    }

    private static String shown(Object v) {
        return v instanceof String s ? "\"" + s + "\"" : String.valueOf(v);
    }

    /** A Float as its literal is typed (the numeric charter's Rule 1, {@code SqlTyping.floatDecimal}): its plain digits
     *  as a decimal, an extreme magnitude as a double; a value that is not finite has no literal, and is refused. */
    private static Conversion floatValue(double d) {
        if (!Double.isFinite(d)) {
            return new Refused(d + " is not finite: no SQL literal stands for it");
        }
        BigDecimal plain = com.legend.sql.SqlTyping.floatDecimal(d);
        return new Converted(plain != null ? plain : (Object) d);
    }

    /** A date-time, which a slot passes as its text: a year outside 1 to 9999 has none a database's cast reads. */
    private static Conversion dateTime(LocalDateTime t) {
        return t.getYear() >= 1 && t.getYear() <= 9999 ? new Converted(t)
                : new Refused(t + " is outside the years 1 to 9999: no date-time text a database reads stands for it");
    }

    /** A decimal as its literal is written: its plain digits ({@code 1E+3} is {@code 1000}). */
    private static BigDecimal plain(BigDecimal d) {
        return d.scale() < 0 ? d.setScale(0) : d;
    }

    /** {@code [lower..upper]} as Pure spells it: {@code *}, {@code 1}, {@code 1..*}, {@code 0..1}. */
    private static String multiplicity(ExecutionPlan.Multiplicity m) {
        Integer upper = m.upper();
        if (upper == null) {
            return m.lower() == 0 ? "*" : m.lower() + "..*";
        }
        return m.lower() == upper ? String.valueOf(m.lower()) : m.lower() + ".." + upper;
    }
}
