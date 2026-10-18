// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

import com.legend.compiler.graphql.GqlSchema.EnumType;
import com.legend.compiler.graphql.GqlSchema.InputObjectType;
import com.legend.compiler.graphql.GqlSchema.InputValueDef;
import com.legend.compiler.graphql.GqlSchema.ScalarType;
import com.legend.compiler.graphql.GqlSchema.TypeDef;
import com.legend.compiler.graphql.GqlValidator.Frame;
import com.legend.protocol.spec.Gql;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A value against an input type (the specification's input coercion, §6.3): literals by scalar, enum values by
 * name, lists item by item (a lone value is a list of one), input objects field by field, variables by their
 * declared type's compatibility with the position. Dates are checked for their ISO shape here, since the schema
 * types them (G5). An {@code *OrderBy} object carries one key (G3).
 */
final class ValueCoercion {

    private static final Pattern DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private static final Pattern DATE_TIME = Pattern.compile("\\d{4}-\\d{2}-\\d{2}(T\\d{2}:\\d{2}(:\\d{2}(\\.\\d+)?)?"
            + "(Z|[+-]\\d{2}:?\\d{2})?)?");
    private static final Pattern TIME = Pattern.compile("\\d{2}:\\d{2}(:\\d{2}(\\.\\d+)?)?");
    private static final Set<String> BUILTIN = Set.of("Int", "Float", "String", "Boolean", "ID");

    private ValueCoercion() {
    }

    static boolean isBuiltinScalar(String name) {
        return BUILTIN.contains(name);
    }

    /** Checks {@code value} against {@code type}, reporting at {@code path}. */
    static void coerce(Frame f, Gql.Value value, GqlSchema.TypeRef type, String path) {
        if (value instanceof Gql.VariableRef v) {
            variable(f, v.name(), type, path);
            return;
        }
        if (value instanceof Gql.NullValue) {
            if (!nullable(type)) {
                f.error(path, "null where " + GqlSchema.spell(type) + " is required");
            }
            return;
        }
        switch (type) {
            case GqlSchema.ListOf l -> {
                if (value instanceof Gql.ListValue lv) {
                    int i = 0;
                    for (Gql.Value item : lv.values()) {
                        coerce(f, item, l.item(), path + "[" + i++ + "]");
                    }
                } else {
                    coerce(f, value, l.item(), path);
                }
            }
            case GqlSchema.Named n -> named(f, value, n.name(), path);
        }
    }

    private static void named(Frame f, Gql.Value value, String typeName, String path) {
        TypeDef t = f.schema.find(typeName);
        switch (t) {
            case InputObjectType in -> inputObject(f, value, in, path);
            case EnumType e -> {
                if (value instanceof Gql.EnumValue ev) {
                    if (!e.values().contains(ev.value())) {
                        f.error(path, ev.value() + " is not a value of " + e.name() + "; the values are "
                                + String.join(", ", e.values()));
                    }
                } else if (value instanceof Gql.StringValue sv) {
                    f.error(path, "\"" + sv.value() + "\" is a string where the enum " + e.name()
                            + " is expected: write " + sv.value() + " without quotes");
                } else {
                    f.error(path, literalKind(value) + " where the enum " + e.name() + " is expected");
                }
            }
            case ScalarType s -> scalar(f, value, s.name(), path);
            case null -> {
                if (isBuiltinScalar(typeName)) {
                    scalar(f, value, typeName, path);
                } else {
                    f.error(path, "no input type " + typeName + " in the schema");
                }
            }
            default -> f.error(path, typeName + " is an output type, not an input type");
        }
    }

    private static void scalar(Frame f, Gql.Value value, String scalar, String path) {
        boolean ok = switch (scalar) {
            case "Int" -> value instanceof Gql.IntValue iv && iv.value() >= Integer.MIN_VALUE
                    && iv.value() <= Integer.MAX_VALUE;
            case SchemaGenerator.BIG_INT -> value instanceof Gql.IntValue;
            case "Float" -> value instanceof Gql.IntValue || value instanceof Gql.FloatValue;
            case SchemaGenerator.DECIMAL -> value instanceof Gql.IntValue || value instanceof Gql.FloatValue
                    || value instanceof Gql.StringValue;
            case "String", "ID" -> value instanceof Gql.StringValue;
            case "Boolean" -> value instanceof Gql.BooleanValue;
            case SchemaGenerator.DATE -> value instanceof Gql.StringValue sv && DATE.matcher(sv.value()).matches();
            case SchemaGenerator.DATE_TIME ->
                    value instanceof Gql.StringValue sv && DATE_TIME.matcher(sv.value()).matches();
            case SchemaGenerator.TIME -> value instanceof Gql.StringValue sv && TIME.matcher(sv.value()).matches();
            default -> throw new IllegalStateException("no coercion rule for the scalar " + scalar);
        };
        if (!ok) {
            f.error(path, literalKind(value) + " where " + scalar + " is expected" + hint(scalar, value));
        }
    }

    /** The spelling hint a scalar's refusal carries; nothing for the scalars whose literal is obvious. */
    private static String hint(String scalar, Gql.Value value) {
        if (SchemaGenerator.DATE.equals(scalar)) {
            return ": write an ISO date, \"2024-01-31\"";
        }
        if (SchemaGenerator.DATE_TIME.equals(scalar)) {
            return ": write an ISO date-time, \"2024-01-31T10:30:00\"";
        }
        if (SchemaGenerator.TIME.equals(scalar)) {
            return ": write an ISO time, \"10:30:00\"";
        }
        if (SchemaGenerator.INT.equals(scalar) && value instanceof Gql.IntValue) {
            return ": Int is 32-bit; the model's integers are BigInt";
        }
        return "";
    }

    private static void inputObject(Frame f, Gql.Value value, InputObjectType in, String path) {
        if (!(value instanceof Gql.ObjectValue ov)) {
            String extra = value instanceof Gql.IntValue || value instanceof Gql.StringValue
                    || value instanceof Gql.FloatValue || value instanceof Gql.BooleanValue
                    ? "; a bare value is not a comparison: write { equals: " + spell(value) + " }" : "";
            f.error(path, literalKind(value) + " where the input object " + in.name() + " is expected" + extra);
            return;
        }
        Set<String> seen = new LinkedHashSet<>();
        for (Gql.Argument field : ov.fields()) {
            if (!seen.add(field.name())) {
                f.error(path + "." + field.name(), "the field " + field.name() + " is given twice");
                continue;
            }
            InputValueDef def = in.field(field.name());
            if (def == null) {
                f.error(path + "." + field.name(), in.name() + " has no field " + field.name() + "; its fields are "
                        + fieldNames(in) + nearest(in, field.name()));
                continue;
            }
            coerce(f, field.value(), def.type(), path + "." + field.name());
        }
        for (InputValueDef def : in.fields()) {
            if (!nullable(def.type()) && !seen.contains(def.name())) {
                f.error(path, in.name() + " requires " + def.name());
            }
        }
        if (in.name().endsWith("OrderBy") && ov.fields().size() > 1) {
            f.error(path, "an " + in.name() + " object carries one key; several keys are a list of objects,"
                    + " [{ a: ASC }, { b: DESC }], whose order is the sort order (G3)");
        }
    }

    private static void variable(Frame f, String name, GqlSchema.TypeRef location, String path) {
        Gql.VariableDef def = f.variables.get(name);
        if (def == null) {
            f.error(path, "variable $" + name + " is not declared by the operation");
            return;
        }
        f.variablesUsed.add(name);
        GqlSchema.TypeRef declared = GqlSchema.TypeRef.of(def.type());
        if (!usable(declared, location, def.defaultValue() != null)) {
            f.error(path, "variable $" + name + " of type " + GqlSchema.spell(declared) + " used where "
                    + GqlSchema.spell(location) + " is expected");
        }
    }

    /** The specification's variable-usage rule: the same named type, lists matching, a nullable variable
     *  allowed in a non-null position only with a default. */
    private static boolean usable(GqlSchema.TypeRef variable, GqlSchema.TypeRef location, boolean hasDefault) {
        if (location instanceof GqlSchema.ListOf ll) {
            if (!(variable instanceof GqlSchema.ListOf vl)) {
                return false;
            }
            if (!ll.nullable() && vl.nullable() && !hasDefault) {
                return false;
            }
            return usable(vl.item(), ll.item(), false);
        }
        GqlSchema.Named ln = (GqlSchema.Named) location;
        if (!(variable instanceof GqlSchema.Named vn) || !vn.name().equals(ln.name())) {
            return false;
        }
        return ln.nullable() || !vn.nullable() || hasDefault;
    }

    static boolean nullable(GqlSchema.TypeRef type) {
        return switch (type) {
            case GqlSchema.Named n -> n.nullable();
            case GqlSchema.ListOf l -> l.nullable();
        };
    }

    static String literalKind(Gql.Value value) {
        return switch (value) {
            case Gql.IntValue v -> "the integer " + v.value();
            case Gql.FloatValue v -> "the number " + v.value();
            case Gql.StringValue v -> "the string \"" + v.value() + "\"";
            case Gql.BooleanValue v -> "the boolean " + v.value();
            case Gql.NullValue v -> "null";
            case Gql.EnumValue v -> "the name " + v.value();
            case Gql.VariableRef v -> "the variable $" + v.name();
            case Gql.ListValue v -> "a list";
            case Gql.ObjectValue v -> "an object";
        };
    }

    /** A scalar literal as written; a composite as an ellipsis. */
    private static String spell(Gql.Value value) {
        return switch (value) {
            case Gql.IntValue v -> Long.toString(v.value());
            case Gql.FloatValue v -> Double.toString(v.value());
            case Gql.StringValue v -> "\"" + v.value() + "\"";
            case Gql.BooleanValue v -> Boolean.toString(v.value());
            case Gql.NullValue v -> "null";
            case Gql.EnumValue v -> v.value();
            case Gql.VariableRef v -> "$" + v.name();
            case Gql.ListValue v -> "[...]";
            case Gql.ObjectValue v -> "{...}";
        };
    }

    static String fieldNames(InputObjectType in) {
        StringBuilder b = new StringBuilder();
        for (InputValueDef d : in.fields()) {
            if (b.length() > 0) {
                b.append(", ");
            }
            b.append(d.name());
        }
        return b.toString();
    }

    /** "; did you mean X" for the closest field name, when one is close. */
    static String nearest(InputObjectType in, String name) {
        String best = null;
        int bestD = 3;
        for (InputValueDef d : in.fields()) {
            int dist = Names.distance(d.name(), name);
            if (dist < bestD) {
                bestD = dist;
                best = d.name();
            }
        }
        return best == null ? "" : "; did you mean " + best;
    }
}
