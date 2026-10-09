// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A plan's parameter values checked against its declarations and converted for binding, as legend-engine checks them
 * before it runs a plan (its {@code FunctionParametersParametersValidation}, {@code FunctionParameterTypeValidator} and
 * {@code FunctionParametersNormalizer}; docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3), with no model: the plan
 * declares each parameter's name, Pure type, multiplicity and an enumeration's names. A value arrives as legend-engine's
 * execute API makes it from a request's protocol value — a {@code String}, a {@code Long}, a {@code Double}, a
 * {@code BigDecimal}, a {@code Boolean}, a date as its text, an enumeration value as its name, a {@code List} for a
 * collection — and leaves as what its slot binds: a {@code long}, a {@code BigDecimal} (a Float's too, its plain
 * digits, or a {@code double} at an extreme magnitude: the numeric charter's Rule 1, as its literal is typed), a
 * {@code LocalDate}, a {@code LocalDateTime} in UTC, a name, a list of them, or none.
 */
public final class PlanParameters {

    private PlanParameters() {
    }

    /** legend-engine's StrictDate format. */
    private static final List<String> STRICT_DATE_FORMATS = List.of("yyyy-MM-dd");

    /** legend-engine's DateTime formats, in its order ({@code PlanDateParameterDateFormat}): a fraction of 1 to 9
     *  digits where a pattern says {@code .SSS}, an offset ({@code Z}: {@code +hhmm}) converted to UTC. */
    private static final List<String> DATE_TIME_FORMATS = List.of("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSSZ", "yyyy-MM-dd'T'HH:mm:ssZ");

    private static final List<DateTimeFormatter> DATE_TIME_PARSERS = DATE_TIME_FORMATS.stream()
            .map(PlanParameters::parser).toList();

    /** The Pure types legend-engine validates a parameter value of, in the order it declares them (it prints them in its
     *  map's hash order, which no test of its asserts): an unknown type's message names them. */
    private static final List<String> ENGINE_TYPES = List.of("StrictDate", "DateTime", "Date", "Integer", "Float",
            "Decimal", "Boolean", "String", "Byte", "meta::pure::metamodel::variant::Variant");

    /** The types the runner binds: legend-engine's, less a Byte and a Variant, which no plan binds yet (PARK-21). */
    private static final List<String> BOUND_TYPES = List.of("StrictDate", "DateTime", "Date", "Integer", "Float",
            "Decimal", "Boolean", "String");

    /** One value's conversion: the value its slot binds, not a value of the type (legend-engine's failure), or refused
     *  by the runner for a reason of its own. */
    private sealed interface Conversion permits Converted, NotOfType, Refused {
    }

    private record Converted(Object value) implements Conversion {
    }

    private record NotOfType() implements Conversion {
    }

    private record Refused(String why) implements Conversion {
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

    /**
     * {@code values} checked against {@code declared} and converted, by name: every declared parameter, its checked
     * value. A value for no declared parameter is ignored, as legend-engine ignores it.
     *
     * @throws IllegalArgumentException with legend-engine's message: {@code Missing external parameter(s): ...} for a
     *                                  required parameter with no value, {@code Invalid provided parameter(s): [...]}
     *                                  for values that fail their checks, every failure collected
     */
    public static Map<String, Checked> check(List<ExecutionPlan.Parameter> declared, Map<String, ?> values) {
        // a required parameter given no value, or a null one, is missing: legend-engine lets a null one through to its
        // template, which then fails to write the statement (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3)
        List<String> missing = new ArrayList<>();
        for (ExecutionPlan.Parameter p : declared) {
            if (p.multiplicity().lower() > 0 && values.get(p.name()) == null) {
                missing.add(p.name() + ":" + p.type() + "[" + multiplicity(p.multiplicity()) + "]");
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("Missing external parameter(s): " + String.join(",", missing));
        }
        Map<String, Checked> out = new LinkedHashMap<>();
        List<String> invalid = new ArrayList<>();
        for (ExecutionPlan.Parameter p : declared) {
            Object value = values.get(p.name());
            if (value == null) {
                out.put(p.name(), new None());
                continue;
            }
            String failure = checkOne(p, value, out);
            if (failure != null) {
                invalid.add(failure);
            }
        }
        if (!invalid.isEmpty()) {
            throw new IllegalArgumentException("Invalid provided parameter(s): [" + String.join(",", invalid) + "]");
        }
        return out;
    }

    /** {@code value} of {@code p} checked: its checked form put in {@code out}, or its failure's message. */
    private static @com.legend.base.Nullable String checkOne(ExecutionPlan.Parameter p, Object value,
            Map<String, Checked> out) {
        if (p.enumValues().isEmpty() && !BOUND_TYPES.contains(p.type())) {
            return ENGINE_TYPES.contains(p.type())
                    ? "parameter '" + p.name() + "' of type " + p.type() + " is not bound by a plan (PARK-21)"
                    // legend-engine has no validator for it (a Number among them): one more failure
                    : "Unknown external parameter type: " + p.type() + ", valid external parameter types: ["
                            + String.join(", ", ENGINE_TYPES) + "]";
        }
        boolean one = Integer.valueOf(1).equals(p.multiplicity().upper());
        List<?> given = value instanceof List<?> list ? list : List.of(value);
        if (one && given.size() > 1) {
            // legend-engine passes it on to its template, which writes no SQL a database runs
            return "parameter '" + p.name() + "' (" + p.type() + "[" + multiplicity(p.multiplicity())
                    + "]) takes one value, given " + given.size();
        }
        List<Object> converted = new ArrayList<>(given.size());
        for (Object v : given) {
            switch (convert(p, v)) {
                case Converted c -> converted.add(c.value());
                // legend-engine names the failing VALUE, a list's failing element; an enumeration's, the whole value
                case NotOfType n -> {
                    return p.enumValues().isEmpty() ? message(p.type(), v)
                            : "Invalid enum value " + value + " for " + p.type() + ", valid enum values: "
                                    + p.enumValues();
                }
                case Refused r -> {
                    return r.why();
                }
            }
        }
        out.put(p.name(), converted.isEmpty() ? new None() : one ? new One(converted.get(0)) : new Many(converted));
        return null;
    }

    /** {@code v} as its slot binds it. */
    private static Conversion convert(ExecutionPlan.Parameter p, Object v) {
        if (!p.enumValues().isEmpty()) {
            String name = v.toString();
            return p.enumValues().contains(name) ? new Converted(name) : new NotOfType();
        }
        Object c = switch (p.type()) {
            case "Integer" -> v instanceof Long || v instanceof Integer ? (Object) ((Number) v).longValue()
                    : v instanceof String s ? parse(() -> Long.parseLong(s)) : null;
            case "Float" -> v instanceof Double || v instanceof Float || v instanceof Integer || v instanceof Long
                    ? (Double) ((Number) v).doubleValue()
                    : v instanceof String s ? parse(() -> Double.parseDouble(s)) : null;
            case "Decimal" -> v instanceof BigDecimal d ? d : v instanceof String s ? parse(() -> new BigDecimal(s)) : null;
            case "Boolean" -> v instanceof Boolean b ? b
                    : v instanceof String s && (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false"))
                    ? Boolean.valueOf(s) : null;
            case "String" -> v instanceof String s ? s : null;
            case "StrictDate" -> v instanceof LocalDate d ? d : v instanceof String s ? strictDate(s) : null;
            case "DateTime" -> dateTime(v);
            case "Date" -> v instanceof LocalDate d ? d : date(v);
            default -> throw new IllegalStateException("parameter '" + p.name() + "' of type " + p.type()
                    + " reached conversion: its type is not one the runner binds");
        };
        if (c == null) {
            return new NotOfType();
        }
        return c instanceof Double d ? floatValue(p, d) : new Converted(c);
    }

    /** A Float as its literal is typed (the numeric charter's Rule 1, {@code SqlTyping.floatDecimal}): its plain digits
     *  as a decimal, an extreme magnitude as a double; a value that is not finite has no SQL literal and is refused by
     *  name (legend-engine parses it and writes {@code NaN} into its statement). */
    private static Conversion floatValue(ExecutionPlan.Parameter p, double d) {
        if (!Double.isFinite(d)) {
            return new Refused("parameter '" + p.name() + "' (Float) is " + d + ": a value that is not finite has no"
                    + " SQL literal, and is not bound");
        }
        BigDecimal plain = com.legend.sql.SqlTyping.floatDecimal(d);
        return new Converted(plain != null ? plain : (Object) d);
    }

    /** A Date's value: a StrictDate's form, else a DateTime's. */
    private static @com.legend.base.Nullable Object date(Object v) {
        LocalDate strict = v instanceof String s ? strictDate(s) : null;
        return strict != null ? strict : dateTime(v);
    }

    private static @com.legend.base.Nullable Object dateTime(Object v) {
        return switch (v) {
            case LocalDateTime t -> t;
            case ZonedDateTime z -> z.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime();
            case Instant i -> LocalDateTime.ofInstant(i, ZoneOffset.UTC);
            case String s -> {
                for (int i = 0; i < DATE_TIME_PARSERS.size(); i++) {
                    try {
                        yield DATE_TIME_FORMATS.get(i).endsWith("Z")
                                ? OffsetDateTime.parse(s, DATE_TIME_PARSERS.get(i)).atZoneSameInstant(ZoneOffset.UTC)
                                        .toLocalDateTime()
                                : LocalDateTime.parse(s, DATE_TIME_PARSERS.get(i));
                    } catch (DateTimeParseException e) {
                        // the next format
                    }
                }
                yield null;
            }
            default -> null;
        };
    }

    private static @com.legend.base.Nullable LocalDate strictDate(String s) {
        try {
            return LocalDate.parse(s, DateTimeFormatter.ofPattern(STRICT_DATE_FORMATS.get(0), java.util.Locale.ROOT));
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /** legend-engine's parser for {@code pattern}: a {@code .SSS} fraction takes 1 to 9 digits. */
    private static DateTimeFormatter parser(String pattern) {
        int fraction = pattern.indexOf(".SSS");
        if (fraction < 0) {
            return DateTimeFormatter.ofPattern(pattern, java.util.Locale.ROOT);
        }
        DateTimeFormatterBuilder b = new DateTimeFormatterBuilder().appendPattern(pattern.substring(0, fraction))
                .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true);
        if (pattern.endsWith("Z")) {
            b.appendPattern("Z");
        }
        return b.toFormatter(java.util.Locale.ROOT);
    }

    private interface Parse {
        Object parse();
    }

    private static @com.legend.base.Nullable Object parse(Parse p) {
        try {
            return p.parse();
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** legend-engine's message for a value of {@code type} that fails its check. */
    private static String message(String type, Object value) {
        StringBuilder b = new StringBuilder("Unable to process '").append(type).append("' parameter, value: ");
        if (value instanceof String) {
            b.append('\'').append(value).append('\'');
            if (!"String".equals(type)) {
                b.append(" is not parsable.");
            }
        } else {
            b.append(value).append('.');
        }
        switch (type) {
            case "StrictDate" -> b.append(" Expected formats: [").append(String.join(",", STRICT_DATE_FORMATS)).append(']');
            case "DateTime" -> b.append(" Expected formats: [").append(String.join(",", DATE_TIME_FORMATS)).append(']');
            case "Date" -> b.append(" Expected formats: [").append(String.join(",", STRICT_DATE_FORMATS)).append(',')
                    .append(String.join(",", DATE_TIME_FORMATS)).append(']');
            default -> {
                // no formats to name
            }
        }
        return b.toString();
    }

    /** {@code [lower..upper]} as legend-engine spells it: {@code *}, {@code 1}, {@code 1..*}, {@code 0..1}. */
    private static String multiplicity(ExecutionPlan.Multiplicity m) {
        if (m.upper() == null) {
            return m.lower() == 0 ? "*" : m.lower() + "..*";
        }
        return m.lower() == m.upper() ? String.valueOf(m.lower()) : m.lower() + ".." + m.upper();
    }
}
