// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.deephaven;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE MESSAGES lite writes to a Deephaven server and reads back (docs/DEEPHAVEN_PLAN_2026_10_22.md DH11): each one's
 * fields, as Deephaven's protocol numbers and types them, and nothing lite does not write or read. A protocol's
 * facts, not Deephaven code: {@code DeephavenMessagesTest} checks every row against the {@code .proto} definitions
 * of the pinned server, so a number or a type held wrong (the measured case: a table reference's offset is a zigzag
 * {@code sint32}) fails a test, not a request. A message is named as its definition nests it
 * ({@code BatchTableRequest.Operation}), without the package.
 */
public final class DeephavenMessages {

    private DeephavenMessages() {
    }

    /** How a field's value is written. */
    public enum Kind { MESSAGE, STRING, BYTES, BOOL, INT32, INT64, UINT32, UINT64, SINT32, SINT64, DOUBLE, ENUM }

    /** How many values a field holds: one, written only when it is not its type's default (proto3's implicit
     *  presence); one, written whenever it is set (a oneof's member, an {@code optional}); or any number. */
    public enum Card { SINGLE, PRESENT, REPEATED }

    /** A field: its JSON name (the protocol's JSON form, as a plan writes it), number, kind, the message or enum it
     *  holds (null for a scalar) and cardinality. */
    public record Field(String json, int number, Kind kind, @com.legend.base.Nullable String type, Card card) {
    }

    /** A message: its fields by JSON name, in number order. */
    public record Message(String name, Map<String, Field> fields) {
    }

    /** UpdateBy's operation specs, nested where the protocol nests them. */
    private static final String SPEC = "UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.";

    private static final Map<String, Message> MESSAGES = index(List.of(
            m("BatchTableRequest",
                    f("ops", 1, Kind.MESSAGE, "BatchTableRequest.Operation", Card.REPEATED)),
            m("BatchTableRequest.Operation",
                    f("emptyTable", 1, Kind.MESSAGE, "EmptyTableRequest", Card.PRESENT),
                    f("dropColumns", 3, Kind.MESSAGE, "DropColumnsRequest", Card.PRESENT),
                    f("update", 4, Kind.MESSAGE, "SelectOrUpdateRequest", Card.PRESENT),
                    f("view", 6, Kind.MESSAGE, "SelectOrUpdateRequest", Card.PRESENT),
                    f("updateView", 7, Kind.MESSAGE, "SelectOrUpdateRequest", Card.PRESENT),
                    f("select", 8, Kind.MESSAGE, "SelectOrUpdateRequest", Card.PRESENT),
                    f("selectDistinct", 9, Kind.MESSAGE, "SelectDistinctRequest", Card.PRESENT),
                    f("unstructuredFilter", 11, Kind.MESSAGE, "UnstructuredFilterTableRequest", Card.PRESENT),
                    f("sort", 12, Kind.MESSAGE, "SortTableRequest", Card.PRESENT),
                    f("head", 13, Kind.MESSAGE, "HeadOrTailRequest", Card.PRESENT),
                    f("merge", 18, Kind.MESSAGE, "MergeTablesRequest", Card.PRESENT),
                    f("crossJoin", 23, Kind.MESSAGE, "CrossJoinTablesRequest", Card.PRESENT),
                    f("naturalJoin", 24, Kind.MESSAGE, "NaturalJoinTablesRequest", Card.PRESENT),
                    f("createInputTable", 31, Kind.MESSAGE, "CreateInputTableRequest", Card.PRESENT),
                    f("updateBy", 32, Kind.MESSAGE, "UpdateByRequest", Card.PRESENT),
                    f("aggregate", 35, Kind.MESSAGE, "AggregateRequest", Card.PRESENT),
                    f("slice", 44, Kind.MESSAGE, "SliceRequest", Card.PRESENT)),
            m("EmptyTableRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("size", 2, Kind.SINT64, null, Card.SINGLE)),
            m("Ticket",
                    f("ticket", 1, Kind.BYTES, null, Card.SINGLE)),
            m("DropColumnsRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("sourceId", 2, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("columnNames", 3, Kind.STRING, null, Card.REPEATED)),
            m("TableReference",
                    f("ticket", 1, Kind.MESSAGE, "Ticket", Card.PRESENT),
                    f("batchOffset", 2, Kind.SINT32, null, Card.PRESENT)),
            m("SelectOrUpdateRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("sourceId", 2, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("columnSpecs", 3, Kind.STRING, null, Card.REPEATED)),
            m("SelectDistinctRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("sourceId", 2, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("columnNames", 3, Kind.STRING, null, Card.REPEATED)),
            m("UnstructuredFilterTableRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("sourceId", 2, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("filters", 3, Kind.STRING, null, Card.REPEATED)),
            m("SortTableRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("sourceId", 2, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("sorts", 3, Kind.MESSAGE, "SortDescriptor", Card.REPEATED)),
            m("SortDescriptor",
                    f("columnName", 1, Kind.STRING, null, Card.SINGLE),
                    f("isAbsolute", 2, Kind.BOOL, null, Card.SINGLE),
                    f("direction", 3, Kind.ENUM, "SortDescriptor.SortDirection", Card.SINGLE)),
            m("HeadOrTailRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("sourceId", 2, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("numRows", 3, Kind.SINT64, null, Card.SINGLE)),
            m("MergeTablesRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("sourceIds", 2, Kind.MESSAGE, "TableReference", Card.REPEATED),
                    f("keyColumn", 3, Kind.STRING, null, Card.SINGLE)),
            m("CrossJoinTablesRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("leftId", 2, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("rightId", 3, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("columnsToMatch", 4, Kind.STRING, null, Card.REPEATED),
                    f("columnsToAdd", 5, Kind.STRING, null, Card.REPEATED),
                    f("reserveBits", 6, Kind.INT32, null, Card.SINGLE)),
            m("NaturalJoinTablesRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("leftId", 2, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("rightId", 3, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("columnsToMatch", 4, Kind.STRING, null, Card.REPEATED),
                    f("columnsToAdd", 5, Kind.STRING, null, Card.REPEATED),
                    f("joinType", 6, Kind.ENUM, "NaturalJoinTablesRequest.JoinType", Card.SINGLE)),
            m("CreateInputTableRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("sourceTableId", 2, Kind.MESSAGE, "TableReference", Card.PRESENT),
                    f("kind", 4, Kind.MESSAGE, "CreateInputTableRequest.InputTableKind", Card.SINGLE)),
            m("CreateInputTableRequest.InputTableKind",
                    f("inMemoryAppendOnly", 1, Kind.MESSAGE,
                            "CreateInputTableRequest.InputTableKind.InMemoryAppendOnly", Card.PRESENT),
                    f("inMemoryKeyBacked", 2, Kind.MESSAGE,
                            "CreateInputTableRequest.InputTableKind.InMemoryKeyBacked", Card.PRESENT)),
            m("CreateInputTableRequest.InputTableKind.InMemoryAppendOnly"),
            m("CreateInputTableRequest.InputTableKind.InMemoryKeyBacked",
                    f("keyColumns", 1, Kind.STRING, null, Card.REPEATED)),
            m("AggregateRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("sourceId", 2, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("initialGroupsId", 3, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("preserveEmpty", 4, Kind.BOOL, null, Card.SINGLE),
                    f("aggregations", 5, Kind.MESSAGE, "Aggregation", Card.REPEATED),
                    f("groupByColumns", 6, Kind.STRING, null, Card.REPEATED)),
            m("Aggregation",
                    f("columns", 1, Kind.MESSAGE, "Aggregation.AggregationColumns", Card.PRESENT),
                    f("count", 2, Kind.MESSAGE, "Aggregation.AggregationCount", Card.PRESENT),
                    f("countWhere", 7, Kind.MESSAGE, "Aggregation.AggregationCountWhere", Card.PRESENT)),
            m("Aggregation.AggregationColumns",
                    f("spec", 1, Kind.MESSAGE, "AggSpec", Card.SINGLE),
                    f("matchPairs", 2, Kind.STRING, null, Card.REPEATED)),
            m("AggSpec",
                    f("avg", 3, Kind.MESSAGE, "AggSpec.AggSpecAvg", Card.PRESENT),
                    f("countDistinct", 4, Kind.MESSAGE, "AggSpec.AggSpecCountDistinct", Card.PRESENT),
                    f("first", 6, Kind.MESSAGE, "AggSpec.AggSpecFirst", Card.PRESENT),
                    f("group", 9, Kind.MESSAGE, "AggSpec.AggSpecGroup", Card.PRESENT),
                    f("last", 10, Kind.MESSAGE, "AggSpec.AggSpecLast", Card.PRESENT),
                    f("max", 11, Kind.MESSAGE, "AggSpec.AggSpecMax", Card.PRESENT),
                    f("median", 12, Kind.MESSAGE, "AggSpec.AggSpecMedian", Card.PRESENT),
                    f("min", 13, Kind.MESSAGE, "AggSpec.AggSpecMin", Card.PRESENT),
                    f("sortedFirst", 15, Kind.MESSAGE, "AggSpec.AggSpecSorted", Card.PRESENT),
                    f("sortedLast", 16, Kind.MESSAGE, "AggSpec.AggSpecSorted", Card.PRESENT),
                    f("std", 17, Kind.MESSAGE, "AggSpec.AggSpecStd", Card.PRESENT),
                    f("sum", 18, Kind.MESSAGE, "AggSpec.AggSpecSum", Card.PRESENT),
                    f("unique", 20, Kind.MESSAGE, "AggSpec.AggSpecUnique", Card.PRESENT),
                    f("var", 23, Kind.MESSAGE, "AggSpec.AggSpecVar", Card.PRESENT)),
            m("AggSpec.AggSpecAvg"),
            m("AggSpec.AggSpecCountDistinct",
                    f("countNulls", 1, Kind.BOOL, null, Card.SINGLE)),
            m("AggSpec.AggSpecFirst"),
            // a group's values as one vector, which a formula after the group reads (a correlation, a covariance, an
            // interpolated percentile: the library's cor, cov, sort and nth over it)
            m("AggSpec.AggSpecGroup"),
            // a group's last row's value: a window's running figure at the last of each tie, which every tie takes (K6)
            m("AggSpec.AggSpecLast"),
            m("AggSpec.AggSpecMax"),
            m("AggSpec.AggSpecMedian",
                    f("averageEvenlyDivided", 1, Kind.BOOL, null, Card.SINGLE)),
            m("AggSpec.AggSpecMin"),
            // a value at the first or last row of an order (ARG_MIN, ARG_MAX: a candle's open and close, K3a)
            m("AggSpec.AggSpecSorted",
                    f("columns", 1, Kind.MESSAGE, "AggSpec.AggSpecSortedColumn", Card.REPEATED)),
            m("AggSpec.AggSpecSortedColumn",
                    f("columnName", 1, Kind.STRING, null, Card.SINGLE)),
            m("AggSpec.AggSpecStd"),
            m("AggSpec.AggSpecSum"),
            // a group's one value, or null when it has more than one (G1: DataCube's unique column); its nulls are
            // never a value, and the server asks the sentinel for more than one of every request
            m("AggSpec.AggSpecUnique",
                    f("includeNulls", 1, Kind.BOOL, null, Card.SINGLE),
                    f("nonUniqueSentinel", 2, Kind.MESSAGE, "AggSpec.AggSpecNonUniqueSentinel", Card.SINGLE)),
            m("AggSpec.AggSpecNonUniqueSentinel",
                    f("nullValue", 1, Kind.ENUM, "NullValue", Card.PRESENT)),
            m("AggSpec.AggSpecVar"),
            // A WINDOW'S VALUES (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K6): a row-based operation over each group of
            // the partition's keys, in the table's order -- running (cumulative) or over a window of rows or of time
            m("UpdateByRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("sourceId", 2, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("operations", 4, Kind.MESSAGE, "UpdateByRequest.UpdateByOperation", Card.REPEATED),
                    f("groupByColumns", 5, Kind.STRING, null, Card.REPEATED)),
            m("UpdateByRequest.UpdateByOperation",
                    f("column", 1, Kind.MESSAGE, "UpdateByRequest.UpdateByOperation.UpdateByColumn", Card.PRESENT)),
            m("UpdateByRequest.UpdateByOperation.UpdateByColumn",
                    f("spec", 1, Kind.MESSAGE, "UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec", Card.SINGLE),
                    f("matchPairs", 2, Kind.STRING, null, Card.REPEATED)),
            m("UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec",
                    f("sum", 1, Kind.MESSAGE, SPEC + "UpdateByCumulativeSum", Card.PRESENT),
                    f("min", 2, Kind.MESSAGE, SPEC + "UpdateByCumulativeMin", Card.PRESENT),
                    f("max", 3, Kind.MESSAGE, SPEC + "UpdateByCumulativeMax", Card.PRESENT),
                    f("rollingSum", 7, Kind.MESSAGE, SPEC + "UpdateByRollingSum", Card.PRESENT),
                    f("rollingGroup", 8, Kind.MESSAGE, SPEC + "UpdateByRollingGroup", Card.PRESENT),
                    f("rollingAvg", 9, Kind.MESSAGE, SPEC + "UpdateByRollingAvg", Card.PRESENT),
                    f("rollingMin", 10, Kind.MESSAGE, SPEC + "UpdateByRollingMin", Card.PRESENT),
                    f("rollingMax", 11, Kind.MESSAGE, SPEC + "UpdateByRollingMax", Card.PRESENT),
                    f("rollingCount", 18, Kind.MESSAGE, SPEC + "UpdateByRollingCount", Card.PRESENT),
                    f("countWhere", 23, Kind.MESSAGE, SPEC + "UpdateByCumulativeCountWhere", Card.PRESENT)),
            m(SPEC + "UpdateByCumulativeSum"),
            m(SPEC + "UpdateByCumulativeMin"),
            m(SPEC + "UpdateByCumulativeMax"),
            rolling("UpdateByRollingSum"),
            rolling("UpdateByRollingGroup"),
            rolling("UpdateByRollingAvg"),
            rolling("UpdateByRollingMin"),
            rolling("UpdateByRollingMax"),
            rolling("UpdateByRollingCount"),
            m(SPEC + "UpdateByCumulativeCountWhere",
                    f("resultColumn", 1, Kind.STRING, null, Card.SINGLE),
                    f("filters", 2, Kind.STRING, null, Card.REPEATED)),
            // a window's size: in rows (ticks, the current one among them), or in time along a date-time column
            m("UpdateByWindowScale",
                    f("ticks", 1, Kind.MESSAGE, "UpdateByWindowScale.UpdateByWindowTicks", Card.PRESENT),
                    f("time", 2, Kind.MESSAGE, "UpdateByWindowScale.UpdateByWindowTime", Card.PRESENT)),
            m("UpdateByWindowScale.UpdateByWindowTicks",
                    f("ticks", 1, Kind.DOUBLE, null, Card.SINGLE)),
            m("UpdateByWindowScale.UpdateByWindowTime",
                    f("column", 1, Kind.STRING, null, Card.SINGLE),
                    f("nanos", 2, Kind.SINT64, null, Card.PRESENT)),
            m("Aggregation.AggregationCount",
                    f("columnName", 1, Kind.STRING, null, Card.SINGLE)),
            m("Aggregation.AggregationCountWhere",
                    f("columnName", 1, Kind.STRING, null, Card.SINGLE),
                    f("filters", 2, Kind.STRING, null, Card.REPEATED)),
            m("SliceRequest",
                    f("resultId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("sourceId", 2, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("firstPositionInclusive", 3, Kind.SINT64, null, Card.SINGLE),
                    f("lastPositionExclusive", 4, Kind.SINT64, null, Card.SINGLE)),
            m("ExportedTableCreationResponse",
                    f("resultId", 1, Kind.MESSAGE, "TableReference", Card.SINGLE),
                    f("success", 2, Kind.BOOL, null, Card.SINGLE),
                    f("errorInfo", 3, Kind.STRING, null, Card.SINGLE),
                    f("isStatic", 5, Kind.BOOL, null, Card.SINGLE),
                    f("size", 6, Kind.SINT64, null, Card.SINGLE)),
            m("PublishRequest",
                    f("sourceId", 1, Kind.MESSAGE, "Ticket", Card.SINGLE),
                    f("resultId", 2, Kind.MESSAGE, "Ticket", Card.SINGLE)),
            m("ReleaseRequest",
                    f("id", 1, Kind.MESSAGE, "Ticket", Card.SINGLE))));

    private static final Map<String, Map<String, Integer>> ENUMS = enumIndex(
            e("SortDescriptor.SortDirection", "UNKNOWN", 0, "DESCENDING", -1, "ASCENDING", 1, "REVERSE", 2),
            e("NullValue", "NULL_VALUE", 0),
            e("NaturalJoinTablesRequest.JoinType", "JOIN_TYPE_NOT_SPECIFIED", 0, "ERROR_ON_DUPLICATE", 1,
                    "FIRST_MATCH", 2, "LAST_MATCH", 3, "EXACTLY_ONE_MATCH", 4));

    /** The message {@code name}; one lite does not write or read is refused by name. */
    public static Message message(String name) {
        Message m = MESSAGES.get(name);
        if (m == null) {
            throw new IllegalArgumentException("Deephaven message '" + name + "' is not one lite writes or reads");
        }
        return m;
    }

    /** Every message, in the order the table lists them (for the definitions check). */
    public static List<Message> all() {
        return List.copyOf(MESSAGES.values());
    }

    /** The number of {@code value} in the enum {@code type}; a value the enum does not have is refused by name. */
    public static int enumNumber(String type, String value) {
        Map<String, Integer> values = ENUMS.get(type);
        if (values == null) {
            throw new IllegalArgumentException("Deephaven enum '" + type + "' is not one lite writes");
        }
        Integer n = values.get(value);
        if (n == null) {
            throw new IllegalArgumentException("Deephaven enum " + type + " has no value '" + value + "' (it has "
                    + values.keySet() + ")");
        }
        return n;
    }

    /** The name of {@code number} in the enum {@code type}; a number the enum does not have is refused by name. */
    public static String enumName(String type, int number) {
        Map<String, Integer> values = ENUMS.get(type);
        if (values == null) {
            throw new IllegalArgumentException("Deephaven enum '" + type + "' is not one lite reads");
        }
        for (Map.Entry<String, Integer> v : values.entrySet()) {
            if (v.getValue() == number) {
                return v.getKey();
            }
        }
        throw new IllegalArgumentException("Deephaven enum " + type + " has no value numbered " + number);
    }

    /** Every enum's values by name, in the table's order (for the definitions check). */
    public static Map<String, Map<String, Integer>> enums() {
        return ENUMS;
    }

    private static Field f(String json, int number, Kind kind, @com.legend.base.Nullable String type, Card card) {
        return new Field(json, number, kind, type, card);
    }

    /** A rolling operation's spec (K6): its window behind the row, the row among it, and ahead. */
    private static Message rolling(String name) {
        return m(SPEC + name,
                f("reverseWindowScale", 1, Kind.MESSAGE, "UpdateByWindowScale", Card.SINGLE),
                f("forwardWindowScale", 2, Kind.MESSAGE, "UpdateByWindowScale", Card.SINGLE));
    }

    private static Message m(String name, Field... fields) {
        Map<String, Field> byName = new LinkedHashMap<>();
        for (Field field : fields) {
            byName.put(field.json(), field);
        }
        return new Message(name, java.util.Collections.unmodifiableMap(byName));
    }

    private static Map<String, Message> index(List<Message> messages) {
        Map<String, Message> byName = new LinkedHashMap<>();
        for (Message message : messages) {
            byName.put(message.name(), message);
        }
        return java.util.Collections.unmodifiableMap(byName);
    }

    @SafeVarargs
    private static Map<String, Map<String, Integer>> enumIndex(Map.Entry<String, Map<String, Integer>>... enums) {
        Map<String, Map<String, Integer>> byName = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, Integer>> e : enums) {
            byName.put(e.getKey(), e.getValue());
        }
        return java.util.Collections.unmodifiableMap(byName);
    }

    private static Map.Entry<String, Map<String, Integer>> e(String type, Object... pairs) {
        Map<String, Integer> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            values.put((String) pairs[i], (Integer) pairs[i + 1]);
        }
        return Map.entry(type, java.util.Collections.unmodifiableMap(values));
    }
}
