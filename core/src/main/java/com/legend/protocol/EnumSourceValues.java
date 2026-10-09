// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * An enum value mapping's source values, in every format legend-engine 4.145.0 reads -- part of {@link MappingReader}.
 * The engine reads the older formats as plain values and makes sense of them only when it compiles
 * ({@code HelperMappingBuilder.convertSourceValues}); lite reads each, in the engine's order, straight to the typed
 * source value that compile makes of it (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 2 step 2):
 *
 * <ol>
 *   <li>today's typed values ({@code stringSourceValue}, {@code integerSourceValue}, {@code enumSourceValue}), when
 *       every value is one;</li>
 *   <li>protocol 1.5 and before: one object typed by a literal's {@code _type} ({@code string} or {@code integer}
 *       with {@code values}, an {@code enumValue}, or a {@code collection} of those);</li>
 *   <li>protocol 1.10: plain values read by the enumeration mapping's {@code sourceType} ({@code STRING},
 *       {@code INTEGER} -- a string spelling an integer is that integer -- or an enumeration's path);</li>
 *   <li>protocol 1.6 to 1.9: plain values, a string or an integer each.</li>
 * </ol>
 */
final class EnumSourceValues {

    private EnumSourceValues() {
    }

    static List<Protocol.PEnumSourceValue> read(List<Json.Node> values, @com.legend.base.Nullable String sourceType) {
        List<Protocol.PEnumSourceValue> out = new ArrayList<>(values.size());
        if (values.stream().allMatch(EnumSourceValues::typed)) {
            for (Json.Node v : values) {
                out.add(MappingReader.sourceValue(v));
            }
            return out;
        }
        if (values.size() == 1 && values.get(0) instanceof Json.Obj flagged) {
            flagged(flagged, out);
            return out;
        }
        for (Json.Node v : values) {
            out.add(sourceType != null ? withSourceType(v, sourceType) : plain(v));
        }
        return out;
    }

    /** Today's typed source value: an object whose {@code _type} is one of the three. */
    private static boolean typed(Json.Node v) {
        if (!(v instanceof Json.Obj o)) {
            return false;
        }
        String type = o.getStringOr("_type", null);
        return "stringSourceValue".equals(type) || "integerSourceValue".equals(type) || "enumSourceValue".equals(type);
    }

    /** {@code processSourceValuesWithTypeFlagOnEnumValueMapping}: a collection's members flattened, as Pure does. */
    private static void flagged(Json.Obj node, List<Protocol.PEnumSourceValue> out) {
        Wire w = Wire.of(node, "enum source value (protocol 1.5)");
        String type = w.type();
        if ("string".equals(type)) {
            for (Json.Node v : w.arr("values")) {
                out.add(new Protocol.PEnumSourceValue(null, w.asStr(v, "values[]")));
            }
        } else if ("integer".equals(type)) {
            for (Json.Node v : w.arr("values")) {
                out.add(new Protocol.PEnumSourceValue(null, integer(v)));
            }
        } else if ("enumValue".equals(type)) {
            out.add(new Protocol.PEnumSourceValue(w.str("fullPath"), w.str("value")));
        } else if ("collection".equals(type)) {
            for (Json.Node v : w.arr("values")) {
                flagged((Json.Obj) Wire.of(v, "enum source value (protocol 1.5)").json(), out);
            }
        } else {
            throw Wire.refuse("an enum source value (protocol 1.5) of _type '" + type
                    + "': the engine reads string, integer, enumValue and collection");
        }
        w.done(out);
    }

    /** {@code processSourceValuesWithSourceType}. */
    private static Protocol.PEnumSourceValue withSourceType(Json.Node v, String sourceType) {
        String kind = sourceType.toUpperCase(Locale.ROOT);
        if (kind.equals("STRING")) {
            return plain(v);
        }
        if (kind.equals("INTEGER")) {
            if (v instanceof Json.Str s) {
                try {
                    return new Protocol.PEnumSourceValue(null, Long.parseLong(s.value()));
                } catch (NumberFormatException e) {
                    throw Wire.refuse("an INTEGER enum source value '" + s.value() + "' that is not an integer");
                }
            }
            return new Protocol.PEnumSourceValue(null, integer(v));
        }
        if (!(v instanceof Json.Str s)) {
            throw Wire.refuse("an enum source value of enumeration " + sourceType + " that is not a value name: "
                    + Wire.abbreviate(v));
        }
        return new Protocol.PEnumSourceValue(sourceType, s.value());
    }

    /** {@code processSimpleSourceValue}: a string stays a string, an integer is a long. */
    private static Protocol.PEnumSourceValue plain(Json.Node v) {
        if (v instanceof Json.Str s) {
            return new Protocol.PEnumSourceValue(null, s.value());
        }
        return new Protocol.PEnumSourceValue(null, integer(v));
    }

    private static long integer(Json.Node v) {
        if (v instanceof Json.Num n && n.isInteger()) {
            return n.longValue();
        }
        throw Wire.refuse("an enum source value that is neither a string nor an integer: " + Wire.abbreviate(v));
    }
}
