// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE DRIVER DECODE (the plan path program's step 7, docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §3 and §4,
 * 2026-10-18): a typed row's cell read from a JDBC driver's value by the plan's column -- the SLOT first (its kind
 * is the carrier's), the Pure type where the slot is none or the cell is a structure's, the driver's own kind where
 * both are silent. The twin of {@link com.legend.result.RowDecode}, which reads the same cells off a JSON object (a document's row):
 * the rules are the same, the carrier differs -- here the driver's Java object, there a JSON token. Each conversion
 * is one hop from the driver's object to the one carrier the host adjudication layer reads (a date kind
 * {@link PureDateLiteral}, a time {@link PureTimeLiteral}, a Decimal its exact {@link BigDecimal}, an Integer a
 * {@code long}, a Float a {@code double}, a structure its ordered map or list); nothing is computed (Charter C1.2),
 * and a value no arm reads is refused by name, never guessed (AGENTS.md invariant 4).
 *
 * <p>The rules the legacy executor's fetch and unwrap held, kept with their reasons: a timestamp or a time is
 * re-read from the driver as {@code java.time} (a {@code java.sql.Timestamp} drops the BC era: year -21457
 * surfaced as +21458); a text the database holds under a numeric slot is the text unless the Pure type is itself
 * numeric (the engine's test store declares {@code interactionTable.ID INT} over a VARCHAR column; step 6's
 * finding); an integral slot's scale-0 {@code BigDecimal} (DuckDB types a literal past int64 DECIMAL) is its exact
 * integral; a double slot's {@code BigDecimal} (H2's LEGACY mode hands a DOUBLE aggregate back so) is the double; a
 * JSON cell arrives as text, as H2's UTF-8 bytes or as a driver's own node object, read to its text beside the
 * driver ({@link DriverCells}); a list or a struct the database carries as JSON text (H2's list, Postgres's jsonb
 * struct) decodes by the slot's declared layout; a boolean SQLite answers as 1 or 0 is the boolean.
 */
final class JdbcDecode {

    private JdbcDecode() {
    }

    /** What reading typed rows needs of a session's driver beyond the plan's columns, its database's facts
     *  ({@code SqlDialect}): {@code javaTime}, the driver re-reads a timestamp cell as
     *  {@code java.time} ({@code SqlDialect.refetchesTimestampAsJavaTime});
     *  {@code dropsDecimalZeros}, it hands a decimal back with its trailing zeros dropped, padded back here to its
     *  slot's declared scale, never rounded ({@code SqlDialect.dropsDecimalTrailingZeros});
     *  {@code failedWithoutColumns}, it opens a failing statement's result with no columns, its error raised on the
     *  first read ({@code SqlDialect.opensFailedResultsWithoutColumns}); {@code nonFiniteJson}, its JSON spells a double
     *  that is not finite as a bare token ({@code SqlDialect.jsonSpellsNonFiniteNumbers}, read by
     *  {@link NonFiniteJson}). The session's statements read them off the dialect ({@link JdbcStatements#check}). */
    record Driver(boolean javaTime, boolean dropsDecimalZeros, boolean failedWithoutColumns, boolean nonFiniteJson) {
    }

    /** The cells of the row {@code rs} stands on, one per column of {@code columns}, the i-th column read from the
     *  i-th result column: a typed-rows statement answers the plan's columns in their order. */
    static List<Object> row(ResultSet rs, List<ExecutionPlan.Column> columns, Driver driver) throws SQLException {
        List<Object> out = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            ExecutionPlan.Column c = columns.get(i);
            if (com.legend.result.RowDecode.slotWord(c.slot()).equals("TAGGED")) {
                // a tagged column (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a): its cell is the tag, the slot
                // index, read as the integer it is; the value stands in that slot's column beside it, read with
                // the slot's own type
                Object tag = cell(rs, i + 1, com.legend.result.RowDecode.tagOf(c), driver);
                if (tag == null) {
                    out.add(null);
                    continue;
                }
                int index = ((Number) tag).intValue();
                ExecutionPlan.Column slot = com.legend.result.RowDecode.taggedSlot(c, index);
                Object value = cell(rs, rs.findColumn(slot.name()), slot, driver);
                if (value == null) {
                    throw new IllegalStateException("the row's tagged column '" + c.name() + "' names slot "
                            + slot.name() + ", and that slot's column is null");
                }
                // the tag rides into the batch beside the carrier (TaggedCell; track F, 2026-10-29)
                out.add(new com.legend.result.TaggedCell(index, java.util.Objects.requireNonNull(
                        com.legend.result.RowDecode.presented(value, slot))));
                continue;
            }
            out.add(cell(rs, i + 1, c, driver));
        }
        return out;
    }

    /** Result column {@code i} as the cell of {@code c}. */
    static @com.legend.base.Nullable Object cell(ResultSet rs, int i, ExecutionPlan.Column c, Driver driver)
            throws SQLException {
        Object v;
        switch (com.legend.result.RowDecode.slotWord(c.slot())) {
            // the texts: a label whose text carries its own kind, and a text slot (a number the database holds
            // under a text slot keeps its digits, as its JSON token did)
            case "DECIMAL_TEXT", "TEMPORAL_TEXT", "VARCHAR", "CHAR", "TEXT" -> v = rs.getString(i);
            default -> {
                v = rs.getObject(i);
                // a timestamp or a time re-read as java.time: BC-faithful, and a time with the microseconds its
                // column holds (the legacy executor's fetch); a driver that answers the re-read with nothing is
                // refused, never kept. A database whose driver re-reads no timestamp keeps the driver's Timestamp,
                // as the legacy executor did (Snowflake's refuses the re-read and Databricks' answers it with text,
                // both measured 2026-10-11; on the typed seam Snowflake's refusal failed 48 channel B tests,
                // measured on its lane 2026-10-21)
                if (v instanceof java.sql.Timestamp && driver.javaTime()) {
                    v = reread(rs, i, LocalDateTime.class, c);
                } else if (v instanceof java.sql.Date && driver.javaTime()) {
                    // a date too (SqlDialect.refetchesTimestampAsJavaTime): java.sql.Date drops a BC date's era, 44 BC
                    // read as 44 AD (the legacy executor re-read it; PostgresArmTest, 2026-10-27)
                    v = reread(rs, i, LocalDate.class, c);
                } else if (v instanceof java.sql.Time) {
                    v = reread(rs, i, LocalTime.class, c);
                }
            }
        }
        return decode(v, c, driver);
    }

    private static Object reread(ResultSet rs, int i, Class<? extends java.time.temporal.Temporal> as,
            ExecutionPlan.Column c) throws SQLException {
        Object t = rs.getObject(i, as);
        if (t == null) {
            throw new IllegalStateException("column '" + c.name() + "': the driver answered the re-read of its cell as "
                    + as.getSimpleName() + " with nothing");
        }
        return t;
    }

    /** {@code v}, a driver's value (a cell, or a structure's element), as the cell of {@code c}: by the slot first,
     *  then by the Pure type. */
    static @com.legend.base.Nullable Object decode(@com.legend.base.Nullable Object v, ExecutionPlan.Column c,
            Driver driver) throws SQLException {
        if (v == null) {
            return null;
        }
        String type = c.type();
        switch (com.legend.result.RowDecode.slotWord(c.slot())) {
            // the slot labels that ARE the decode instruction: a text-carried temporal or decimal parses (the
            // LITERAL spelling carrier's arm, a cell parsed by its Pure-literal text, left with track L's L2,
            // 2026-10-29: a mixed value is a tagged value, read in its slot)
            case "TAGGED" -> throw new IllegalStateException("column '" + c.name() + "': a tagged cell is read with"
                    + " its row (its slot columns stand beside it), never alone");
            case "DECIMAL_TEXT" -> {
                return com.legend.result.RowDecode.decimalText(text(v, c));
            }
            case "TEMPORAL_TEXT" -> {
                return com.legend.result.RowDecode.temporalText(text(v, c), c);
            }
            // the SQL kinds: the carrier is the slot's -- but a text the database holds is the column's own text
            // unless the Pure type is numeric (RowDecode's rule, the step-6 finding)
            case "DATE", "TIMESTAMP", "TIMESTAMPTZ" -> {
                return temporal(v, c);
            }
            case "TIME" -> {
                return time(v, c);
            }
            case "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT" -> {
                return v instanceof String s && !com.legend.result.RowDecode.numericPure(type) ? s : integral(v, c);
            }
            case "DOUBLE", "FLOAT", "REAL" -> {
                return v instanceof String s && !com.legend.result.RowDecode.numericPure(type) ? s : floating(v, c);
            }
            case "DECIMAL", "NUMERIC" -> {
                if (v instanceof String s && !com.legend.result.RowDecode.numericPure(type)) {
                    return s;
                }
                BigDecimal d = decimal(v, c);
                // a driver that drops a decimal's trailing zeros (BigQuery's): the slot's declared scale is the
                // value's own, its digits padded back -- setScale to a greater scale never rounds
                int scale = driver.dropsDecimalZeros() ? declaredScale(c) : -1;
                return scale > d.scale() ? d.setScale(scale) : d;
            }
            case "BOOLEAN" -> {
                return bool(v, c);
            }
            case "VARCHAR", "CHAR", "TEXT" -> {
                return text(v, c);
            }
            // a JSON cell: its text, read by the Pure type as a document's cell is
            case "JSON" -> {
                String json = jsonText(v, c);
                Json.Node node = driver.nonFiniteJson() ? NonFiniteJson.parse(json) : Json.parse(json);
                // a Variant's cell is its JSON text, a JSON null included: the SQL cell is not NULL, so the null is
                // the Variant's value (toJSON of it is 'null'), never an absent one
                return type.equals(com.legend.result.RowDecode.VARIANT) ? Json.toCompact(node) : com.legend.result.RowDecode.cell(node, c);
            }
            case "ARRAY" -> {
                return array(v, c, driver);
            }
            case "STRUCT" -> {
                return struct(v, c, driver);
            }
            case "MAP" -> {
                return map(v, c, driver);
            }
            default -> {
                // no slot (a late-bound grid's column, a pivot's generated column): the Pure type decides
            }
        }
        return byPureType(v, c, driver);
    }

    /** A timestamp as the engine reads one from a database: nine subsecond digits
     *  ({@link PureDateLiteral#fromDatabaseTimestamp}); a date a day; a text its written precision. */
    private static Object temporal(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case LocalDateTime t -> PureDateLiteral.fromDatabaseTimestamp(t);
            // a structure's leaf, never re-read, or a driver that re-reads no timestamp: its wall clock, its era kept
            case java.sql.Timestamp ts -> PureDateLiteral.fromDatabaseTimestamp(wallClock(ts));
            case OffsetDateTime o -> PureDateLiteral.fromDatabaseTimestamp(
                    o.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
            // a DATE the database holds is a day, whichever slot declares it (the canon's own dispatch: a column
            // declared DATE may hold timestamps, and the driver's kind says which)
            case LocalDate d -> PureDateLiteral.fromLocalDate(d);
            case java.sql.Date d -> PureDateLiteral.fromLocalDate(day(d));
            // a database that holds its dates as text (SQLite)
            case String s -> PureDateLiteral.parse(s.trim().replace(' ', 'T'));
            default -> throw refused(v, c);
        };
    }

    /** A driver's Timestamp as the wall clock it holds, its era kept: {@code Timestamp.toLocalDateTime} reads the
     *  year of the era, so a BC year surfaced positive (year -21457 as 21458, its month and day kept: Databricks'
     *  driver, measured by its lane 2026-10-27, which re-reads no timestamp as java.time); the calendar's own ERA field
     *  says which, and an astronomical year is 1 minus a BC year. */
    static LocalDateTime wallClock(java.sql.Timestamp ts) {
        LocalDateTime t = ts.toLocalDateTime();
        return beforeChrist(ts) ? t.withYear(1 - t.getYear()) : t;
    }

    /** A driver's {@link java.sql.Date} as the day it holds, its era kept ({@link #wallClock}'s rule). */
    static LocalDate day(java.sql.Date d) {
        LocalDate day = d.toLocalDate();
        return beforeChrist(d) ? day.withYear(1 - day.getYear()) : day;
    }

    private static boolean beforeChrist(java.util.Date d) {
        java.util.GregorianCalendar calendar = new java.util.GregorianCalendar();
        calendar.setTime(d);
        return calendar.get(java.util.Calendar.ERA) == java.util.GregorianCalendar.BC;
    }

    private static Object time(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case LocalTime t -> PureTimeLiteral.fromLocalTime(t);
            case java.sql.Time t -> PureTimeLiteral.fromLocalTime(t.toLocalTime());
            case String s -> PureTimeLiteral.parse(s.trim());
            default -> throw refused(v, c);
        };
    }

    private static Object integral(Object v, ExecutionPlan.Column c) {
        BigInteger whole = switch (v) {
            case Long l -> BigInteger.valueOf(l);
            case Integer n -> BigInteger.valueOf(n);
            case Short n -> BigInteger.valueOf(n);
            case Byte n -> BigInteger.valueOf(n);
            case BigInteger b -> b;
            case BigDecimal d when d.stripTrailingZeros().scale() <= 0 -> d.toBigIntegerExact();
            case String s -> new BigInteger(s.trim());
            default -> throw refused(v, c);
        };
        // 63 bits beside the sign fit a long (the track A audit's finding 2, 2026-10-29: 63 exclusive until then)
        return whole.bitLength() <= 63 ? (Object) whole.longValue() : whole;
    }

    private static Object floating(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case Double d -> d;
            // a REAL's own digits, as the JSON writer's shortest text spelled them, never the widened binary
            case Float f -> Double.parseDouble(Float.toString(f));
            case BigDecimal d -> d.doubleValue();
            case Long l -> (double) l;
            case Integer n -> (double) n;
            case BigInteger b -> b.doubleValue();
            case String s -> Double.valueOf(s.trim());
            default -> throw refused(v, c);
        };
    }

    private static BigDecimal decimal(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case BigDecimal d -> d;
            case Long l -> BigDecimal.valueOf(l);
            case Integer n -> BigDecimal.valueOf(n);
            case Short n -> BigDecimal.valueOf(n);
            case BigInteger b -> new BigDecimal(b);
            case Double d -> BigDecimal.valueOf(d);
            case Float f -> new BigDecimal(Float.toString(f));
            case String s -> new BigDecimal(s.trim());
            default -> throw refused(v, c);
        };
    }

    /** The scale a {@code DECIMAL(p,s)} slot declares; -1 for a slot that declares none. */
    private static int declaredScale(ExecutionPlan.Column c) {
        String slot = c.slot() == null ? "" : c.slot();
        int comma = slot.indexOf(',');
        return slot.startsWith("DECIMAL(") && comma > 0 && slot.endsWith(")")
                ? Integer.parseInt(slot.substring(comma + 1, slot.length() - 1).trim()) : -1;
    }

    private static Boolean bool(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case Boolean b -> b;
            // SQLite answers a boolean as 1 or 0
            case Integer n -> n != 0;
            case Long n -> n != 0;
            default -> throw refused(v, c);
        };
    }

    /** A text slot's cell: the text, or a number's own digits where the database holds a number under it. */
    private static String text(Object v, ExecutionPlan.Column c) {
        return switch (v) {
            case String s -> s;
            case BigDecimal d -> d.toPlainString();
            case Long n -> Long.toString(n);
            case Integer n -> Integer.toString(n);
            case Short n -> Short.toString(n);
            case Byte n -> Byte.toString(n);
            case BigInteger b -> b.toString();
            case Double d -> Double.toString(d);
            case Float f -> Float.toString(f);
            default -> throw refused(v, c);
        };
    }

    /** A JSON cell's text: as the driver hands it (a String), H2's UTF-8 bytes, or a driver's own node object read
     *  beside the driver. */
    private static String jsonText(Object v, ExecutionPlan.Column c) {
        if (v instanceof String s) {
            return s;
        }
        if (v instanceof byte[] b) {
            return new String(b, StandardCharsets.UTF_8);
        }
        String s = DriverCells.jsonTextOf(v);
        if (s == null) {
            throw refused(v, c);
        }
        return s;
    }

    private static Object array(Object v, ExecutionPlan.Column c, Driver driver) throws SQLException {
        ExecutionPlan.Column element = com.legend.result.RowDecode.element(c);
        // a driver's Array, or a list inside ClickHouse's Tuple, which its driver hands as a java.util.List (channel
        // B's testHeadComplex and testFirstComplex, 2026-10-20)
        Object[] items = v instanceof java.sql.Array a ? (Object[]) a.getArray()
                : v instanceof List<?> l ? l.toArray() : null;
        if (items != null) {
            List<Object> out = new ArrayList<>(items.length);
            for (Object item : items) {
                out.add(decode(item, element, driver));
            }
            return out;
        }
        // a list the database carries as JSON text (the list-less carrier: H2's), each element by the element slot
        if (v instanceof String || v instanceof byte[]) {
            Json.Node node = Json.parse(jsonText(v, c));
            if (node instanceof Json.Arr arr) {
                List<Object> out = new ArrayList<>(arr.items().size());
                for (Json.Node item : arr.items()) {
                    out.add(com.legend.result.RowDecode.cell(item, element));
                }
                return out;
            }
        }
        throw refused(v, c);
    }

    private static @com.legend.base.Nullable Object struct(Object v, ExecutionPlan.Column c, Driver driver)
            throws SQLException {
        List<ExecutionPlan.Column> fields = com.legend.result.RowDecode.fields(c);
        Map<String, Object> out = new LinkedHashMap<>();
        // a struct's attributes in order: a driver's Struct, or ClickHouse's Tuple, which its driver hands as the
        // Object[] of its fields (the legacy executor's ClickHouse.normalize; channel B's find, head and zip tests,
        // 2026-10-20)
        Object[] attrs = v instanceof java.sql.Struct s ? s.getAttributes() : v instanceof Object[] tuple ? tuple : null;
        if (attrs != null) {
            if (attrs.length != fields.size()) {
                throw new IllegalStateException("column '" + c.name() + "': a struct cell of " + attrs.length
                        + " attributes under a slot of " + fields.size() + " fields (" + c.slot() + ")");
            }
            for (int i = 0; i < attrs.length; i++) {
                out.put(fields.get(i).name(), decode(attrs[i], fields.get(i), driver));
            }
            return out;
        }
        // a struct the database carries as JSON (Postgres's jsonb, SQLite's text, H2's bytes): its fields by the
        // slot's declared layout; a JSON null is an absent instance
        Json.Node node = Json.parse(jsonText(v, c));
        if (node instanceof Json.Null) {
            return null;
        }
        if (node instanceof Json.Obj o) {
            for (ExecutionPlan.Column f : fields) {
                Json.Node fv = o.fields().get(f.name());
                if (fv == null && com.legend.result.CellText.synthetic(f.name())) {
                    continue;   // an identity field the JSON does not carry: not a null property (RowDecode's rule)
                }
                out.put(f.name(), fv == null ? null : com.legend.result.RowDecode.cell(fv, f));
            }
            return out;
        }
        throw refused(v, c);
    }

    private static @com.legend.base.Nullable Object map(Object v, ExecutionPlan.Column c, Driver driver)
            throws SQLException {
        ExecutionPlan.Column[] kv = com.legend.result.RowDecode.mapSlots(c);
        ExecutionPlan.Column key = kv[0];
        ExecutionPlan.Column value = kv[1];
        Map<Object, Object> out = new LinkedHashMap<>();
        if (v instanceof Map<?, ?> m) {
            for (Map.Entry<?, ?> e : m.entrySet()) {
                out.put(decode(e.getKey(), key, driver), decode(e.getValue(), value, driver));
            }
            return out;
        }
        // a map carried as its entries: a list of (key, value) structs, or of JSON objects {"key", "value"}
        // (Postgres's jsonb[]; found by channel B on Postgres, step 9, 2026-10-19)
        if (v instanceof java.sql.Array a) {
            for (Object entry : (Object[]) a.getArray()) {
                if (entry instanceof java.sql.Struct s && s.getAttributes().length == 2) {
                    Object[] attrs = s.getAttributes();
                    out.put(decode(attrs[0], key, driver), decode(attrs[1], value, driver));
                    continue;
                }
                Json.Node node = Json.parse(jsonText(entry, c));
                if (node instanceof Json.Obj o && o.fields().containsKey("key") && o.fields().containsKey("value")) {
                    out.put(com.legend.result.RowDecode.cell(o.fields().get("key"), key), com.legend.result.RowDecode.cell(o.fields().get("value"), value));
                    continue;
                }
                throw new IllegalStateException("column '" + c.name() + "': a map cell's entry is neither a"
                        + " (key, value) struct nor a {\"key\", \"value\"} object: " + entry);
            }
            return out;
        }
        // a map the database carries as JSON (SQLite's text, MariaDB's and MySQL's JSON, Postgres's jsonb, as a
        // struct is): its entries by the slot's key and value, the JSON row's own map cell (RowDecode); a JSON null
        // is an absent map. Found by channel B on SQLite (step 9, 2026-10-19: the six map put and putAll cases
        // refused since the typed rows of step 6, which the DuckDB and H2 lanes, whose drivers deliver a Map, never
        // met).
        Json.Node node = Json.parse(jsonText(v, c));
        if (node instanceof Json.Null) {
            return null;
        }
        if (node instanceof Json.Obj) {
            return com.legend.result.RowDecode.cell(node, c);
        }
        throw refused(v, c);
    }

    /** A cell with no slot: the Pure type decides, and where it is a class, an enumeration or Any, the driver's own
     *  kind (a structure's JSON text parsed as a document's cell is). */
    private static @com.legend.base.Nullable Object byPureType(Object v, ExecutionPlan.Column c, Driver driver)
            throws SQLException {
        String type = c.type();
        if (type.equals(com.legend.result.RowDecode.VARIANT)) {
            // a variant keeps its JSON text
            return jsonText(v, c);
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
            return switch (v) {
                case Long l -> l;
                case Integer n -> (long) n;
                case Short n -> (long) n;
                case BigInteger b -> b.bitLength() <= 63 ? (Object) b.longValue() : b;
                case BigDecimal d -> d;
                case Double d -> d;
                case Float f -> Double.parseDouble(Float.toString(f));
                case String s -> s.contains(".") || s.contains("e") || s.contains("E") ? new BigDecimal(s.trim())
                        : integral(s, c);
                default -> throw refused(v, c);
            };
        }
        if (type.equals("String")) {
            return text(v, c);
        }
        if (type.equals("Boolean")) {
            return bool(v, c);
        }
        if (type.equals("StrictDate") || type.equals("DateTime") || type.equals("Date")) {
            return temporal(v, c);
        }
        if (type.equals("Time")) {
            return time(v, c);
        }
        // a class instance, an enumeration's value, Any: the driver's own kind -- a structure's JSON text read as a
        // document's cell (the declared kind, a class or Any, says it is a structure), a scalar as it is
        return switch (v) {
            case String s -> {
                String t = s.trim();
                yield t.startsWith("{") || t.startsWith("[") ? com.legend.result.RowDecode.cell(Json.parse(t), c) : s;
            }
            case byte[] b -> com.legend.result.RowDecode.cell(Json.parse(new String(b, StandardCharsets.UTF_8)), c);
            case Boolean b -> b;
            case Long l -> l;
            case Integer n -> (long) n;
            case Short n -> (long) n;
            case BigInteger b -> b.bitLength() <= 63 ? (Object) b.longValue() : b;
            case BigDecimal d -> d;
            case Double d -> d;
            case Float f -> Double.parseDouble(Float.toString(f));
            case LocalDateTime t -> temporal(t, c);
            case java.sql.Timestamp t -> temporal(t, c);
            case LocalDate d -> temporal(d, c);
            case java.sql.Date d -> temporal(d, c);
            case LocalTime t -> time(t, c);
            case java.sql.Time t -> time(t, c);
            case java.sql.Array a -> {
                Object[] items = (Object[]) a.getArray();
                List<Object> out = new ArrayList<>(items.length);
                for (Object item : items) {
                    out.add(decode(item, c, driver));
                }
                yield out;
            }
            default -> {
                String json = DriverCells.jsonTextOf(v);
                if (json == null) {
                    throw refused(v, c);
                }
                yield com.legend.result.RowDecode.cell(Json.parse(json), c);
            }
        };
    }

    private static IllegalStateException refused(Object v, ExecutionPlan.Column c) {
        String text = String.valueOf(v);
        return new IllegalStateException("column '" + c.name() + "' is declared " + c.type()
                + (c.slot() == null ? "" : " in slot " + c.slot()) + ", and the driver's value, a "
                + v.getClass().getName() + " " + (text.length() > 120 ? text.substring(0, 120) + "..." : text)
                + ", is not of that kind");
    }
}
