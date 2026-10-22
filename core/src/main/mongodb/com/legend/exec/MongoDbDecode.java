// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.values.LiteralText;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * THE DOCUMENT DECODE (the plan path program's step 7, docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §3 and §7,
 * 2026-10-18): a typed row's cells read from a MongoDB document by the plan's columns -- each field by its column's
 * name, each value by the slot first and the Pure type then, the rules of {@link RowDecode} and {@link JdbcDecode}
 * over BSON's own kinds: an int32 or an int64 an integral, a double a double, a decimal128 an exact decimal, a date
 * a date-time in UTC (a day under a DATE slot), a string a text (the column's own text under a numeric slot unless
 * the Pure type is numeric), a boolean a boolean, a document or an array its JSON read as a document's cell. Nothing
 * is computed; a value no arm reads is refused by name.
 */
final class MongoDbDecode {

    /** A nested document's or array's JSON: numbers and strings plain; a date or a decimal128 inside one keeps
     *  BSON's own {@code $}-form, which a nested read refuses by name. */
    private static final JsonWriterSettings RELAXED = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();

    private MongoDbDecode() {
    }

    /** The document's cells, one per column of {@code columns}, each from the field of the column's name. */
    static List<Object> row(BsonDocument doc, List<ExecutionPlan.Column> columns) {
        List<Object> out = new ArrayList<>(columns.size());
        for (ExecutionPlan.Column c : columns) {
            BsonValue v = doc.get(c.name());
            if (v == null) {
                throw new IllegalStateException("the document has no field for the plan's column '" + c.name()
                        + "' (it has " + doc.keySet() + "): the pipeline does not write the plan's columns");
            }
            out.add(cell(v, c));
        }
        return out;
    }

    static @com.legend.base.Nullable Object cell(BsonValue v, ExecutionPlan.Column c) {
        if (v.isNull()) {
            return null;
        }
        String type = c.type();
        switch (RowDecode.slotWord(c.slot())) {
            case "LITERAL" -> {
                return LiteralText.parse(text(v, c));
            }
            case "DECIMAL_TEXT" -> {
                return RowDecode.decimalText(text(v, c));
            }
            case "TEMPORAL_TEXT" -> {
                return PureDateLiteral.parse(text(v, c).trim().replace(' ', 'T'));
            }
            case "DATE" -> {
                if (v.isDateTime()) {
                    return PureDateLiteral.fromLocalDate(utc(v).toLocalDate());
                }
                return PureDateLiteral.parse(text(v, c).trim().replace(' ', 'T'));
            }
            case "TIMESTAMP", "TIMESTAMPTZ" -> {
                return timed(v, c);
            }
            case "TIME" -> {
                return PureTimeLiteral.parse(text(v, c).trim());
            }
            case "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT" -> {
                return v.isString() && !RowDecode.numericPure(type) ? v.asString().getValue() : integral(v, c);
            }
            case "DOUBLE", "FLOAT", "REAL" -> {
                return v.isString() && !RowDecode.numericPure(type) ? v.asString().getValue() : floating(v, c);
            }
            case "DECIMAL", "NUMERIC" -> {
                return v.isString() && !RowDecode.numericPure(type) ? v.asString().getValue() : decimal(v, c);
            }
            case "BOOLEAN" -> {
                if (v.isBoolean()) {
                    return v.asBoolean().getValue();
                }
                throw refused(v, c);
            }
            case "VARCHAR", "CHAR", "TEXT" -> {
                return text(v, c);
            }
            case "JSON", "ARRAY", "STRUCT", "MAP" -> {
                return RowDecode.cell(json(v), c);
            }
            default -> {
                // no slot: the Pure type decides
            }
        }
        if (type.equals("Integer")) {
            return integral(v, c);
        }
        if (type.equals("Float")) {
            return floating(v, c);
        }
        if (type.equals("Decimal") || type.startsWith("Decimal(")) {
            return decimal(v, c);
        }
        if (type.equals("Number")) {
            if (v.isInt32() || v.isInt64() || (v.isDecimal128() && scale(v) <= 0)) {
                return integral(v, c);
            }
            if (v.isDouble()) {
                return v.asDouble().getValue();
            }
            return decimal(v, c);
        }
        if (type.equals("String")) {
            return text(v, c);
        }
        if (type.equals("Boolean")) {
            if (v.isBoolean()) {
                return v.asBoolean().getValue();
            }
            throw refused(v, c);
        }
        if (type.equals("StrictDate")) {
            return v.isDateTime() ? PureDateLiteral.fromLocalDate(utc(v).toLocalDate())
                    : PureDateLiteral.parse(text(v, c).trim().replace(' ', 'T'));
        }
        if (type.equals("DateTime") || type.equals("Date")) {
            return timed(v, c);
        }
        if (type.equals("Time")) {
            return PureTimeLiteral.parse(text(v, c).trim());
        }
        // a class instance, an enumeration's value, Any: the document's own kind, read as a document's cell
        return RowDecode.cell(json(v), c);
    }

    /** A BSON date under a timestamp: nine digits, as a database timestamp is read everywhere
     *  ({@link PureDateLiteral#fromDatabaseTimestamp}). */
    private static Object timed(BsonValue v, ExecutionPlan.Column c) {
        if (v.isDateTime()) {
            return PureDateLiteral.fromDatabaseTimestamp(utc(v));
        }
        return PureDateLiteral.parse(text(v, c).trim().replace(' ', 'T'));
    }

    private static LocalDateTime utc(BsonValue v) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(v.asDateTime().getValue()), ZoneOffset.UTC);
    }

    private static int scale(BsonValue v) {
        return v.asDecimal128().getValue().bigDecimalValue().stripTrailingZeros().scale();
    }

    private static Object integral(BsonValue v, ExecutionPlan.Column c) {
        BigInteger whole;
        if (v.isInt32()) {
            return (long) v.asInt32().getValue();
        } else if (v.isInt64()) {
            return v.asInt64().getValue();
        } else if (v.isDecimal128() && scale(v) <= 0) {
            whole = v.asDecimal128().getValue().bigDecimalValue().toBigIntegerExact();
        } else if (v.isDouble() && BigDecimal.valueOf(v.asDouble().getValue()).stripTrailingZeros().scale() <= 0) {
            // a whole value the server computed as a double (the JSON decode reads such a token the same way)
            whole = BigDecimal.valueOf(v.asDouble().getValue()).toBigIntegerExact();
        } else if (v.isString()) {
            whole = new BigInteger(v.asString().getValue().trim());
        } else {
            throw refused(v, c);
        }
        return whole.bitLength() < 63 ? (Object) whole.longValue() : whole;
    }

    private static Object floating(BsonValue v, ExecutionPlan.Column c) {
        if (v.isDouble()) {
            return v.asDouble().getValue();
        }
        if (v.isInt32() || v.isInt64()) {
            return (double) v.asNumber().longValue();
        }
        if (v.isDecimal128()) {
            return v.asDecimal128().getValue().bigDecimalValue().doubleValue();
        }
        if (v.isString()) {
            return Double.valueOf(v.asString().getValue().trim());
        }
        throw refused(v, c);
    }

    private static BigDecimal decimal(BsonValue v, ExecutionPlan.Column c) {
        if (v.isDecimal128()) {
            return v.asDecimal128().getValue().bigDecimalValue();
        }
        if (v.isInt32() || v.isInt64()) {
            return BigDecimal.valueOf(v.asNumber().longValue());
        }
        if (v.isDouble()) {
            return BigDecimal.valueOf(v.asDouble().getValue());
        }
        if (v.isString()) {
            return new BigDecimal(v.asString().getValue().trim());
        }
        throw refused(v, c);
    }

    /** A text slot's cell: the string, or a number's own digits where the document holds a number under it. */
    private static String text(BsonValue v, ExecutionPlan.Column c) {
        if (v.isString()) {
            return v.asString().getValue();
        }
        if (v.isInt32() || v.isInt64()) {
            return Long.toString(v.asNumber().longValue());
        }
        if (v.isDecimal128()) {
            return v.asDecimal128().getValue().bigDecimalValue().toPlainString();
        }
        if (v.isDouble()) {
            return Double.toString(v.asDouble().getValue());
        }
        throw refused(v, c);
    }

    /** {@code v} as a JSON node: a document's or an array's own JSON, a scalar its token. */
    private static Json.Node json(BsonValue v) {
        Json.Node wrapped = Json.parse(new BsonDocument("v", v).toJson(RELAXED));
        Json.Node node = ((Json.Obj) wrapped).fields().get("v");
        if (node == null) {
            throw new IllegalStateException("the document's value " + v.getBsonType() + " has no JSON");
        }
        return node;
    }

    private static IllegalStateException refused(BsonValue v, ExecutionPlan.Column c) {
        return new IllegalStateException("column '" + c.name() + "' is declared " + c.type()
                + (c.slot() == null ? "" : " in slot " + c.slot()) + ", and the document's value, a "
                + v.getBsonType() + ", is not of that kind");
    }
}
