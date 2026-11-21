// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.error;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A FAILED ASSERT'S CODE, AND LITE'S SENTENCE OF IT: ONE CATALOG FOR EVERY ROUTE (item 3 of the compatibility program,
 * 2026-11-21: assert verdicts as records; the user's Q15 = A of 2026-11-20, "lite's own assert-failure words on every
 * route"). The fourth family of the one error model ({@link ErrorCode}, beside {@link CompileKind}, {@link ErrorKind}
 * and {@link FrameKind}): a failed assert is an {@link ErrorRecord} of one of these and its values, never a sentence --
 * not in a plan, not in a test plan; the sentence is made from the record at the last step. Until item 3 this was
 * {@code AssertSentence}, the product's table beside legend-pure's words at the edge; the edge now words a record by
 * this table too (one catalog).
 *
 * <p>Each row's template names its slots in braces; a record's values are the slots' texts IN THE TEMPLATE'S ORDER
 * ({@link #slots}), each a value as lite's value printer writes it (a Float in six decimals, S311; a DateTime in ISO 8601
 * UTC; a Decimal's plain digits; a String QUOTED JSON-style, inside a collection or a row too, so a String is never
 * mistaken for a list; a collection {@code [a, b]}, a row {@code [1, "a"]}). Build a record by its slots' names
 * ({@link #record}), never by position. A quantified assert's element is named by the {@link FrameKind#AT_INDEX} frame
 * around the element's own record (Pure's zero-based index, the one {@code ->at(i)} takes; the user's direction,
 * 2026-11-17). Data only: no rule here decides a verdict.
 *
 * <p>The last rows are not a test's asserts but a platform function's own (legend-engine's test-data generator and
 * CSV seed assert their inputs): their sentences are legend-engine's, read from its sources -- lite has none of its own
 * for them, and the one error model words an error upstream's way where upstream has words (the user's ruling of
 * 2026-11-21 ~02:40).
 */
public enum AssertKind implements ErrorCode {
    EQUALS("expected {expected}, got {actual}"),
    /** Two values whose texts tie (the orchestrator's ruling, 2026-11-17): each side's kind after it. */
    EQUALS_KINDS("expected {expected} ({expectedKind}), got {actual} ({actualKind})"),
    NOT_EQUALS("expected a value other than {actual}"),
    SAME_ELEMENTS("expected the elements {expected} in any order, got {actual}"),
    SIZE("expected size {expected}, got {actual}"),
    NOT_SIZE("expected a size other than {size}"),
    CONTAINS("expected {collection} to contain {value}"),
    NOT_CONTAINS("expected {collection} not to contain {value}"),
    EMPTY("expected no values, got {collection}"),
    NOT_EMPTY("expected at least one value, got none"),
    TRUE("expected true, got false"),
    FALSE("expected false, got true"),
    IS("expected the same instance as {expected}, got {actual}"),
    IS_NOT("expected an instance other than {actual}"),
    /** assertIs over two element references, no operand to name (the orchestrator's ruling, 2026-11-17). */
    SAME_INSTANCE("expected the same instance"),
    INSTANCE_OF("expected an instance of {type}, got {actual}"),
    /** assertTdsEquivalent: rows zipped IN ORDER, numbers within the delta (and times within the time delta), every
     *  other cell equal -- so no "in any order". */
    TDS_EQUIVALENT("expected the rows {expected} within {delta}, got {actual}"),
    WITHIN_TOLERANCE("expected {expected} within {delta}, got {actual}"),
    JSON_EQUALS("expected the JSON {expected}, got {actual}"),
    ERROR("expected the error {expected}, got {actual}"),
    NO_ERROR("expected the error {expected}, got none"),
    // census J29 (2026-11-18): assertError's line and column, lite's own
    ERROR_LINE("expected the error at line {expected}, got line {actual}"),
    ERROR_COLUMN("expected the error at column {expected}, got column {actual}"),
    /** assertError's line or column asked of an error that carries no source position (a native database error, or a
     *  raise emission without provenance): item 3, 2026-11-21, until then a sentence outside the catalog, the same on
     *  both judges. */
    ERROR_NO_POSITION("expected the error at a source position, got an error with none"),
    /** The caller's own message ({@code assert(c, 'm')}, {@code assertEquals(e, a, 'm')}, ...): a value the program
     *  computed, the whole sentence. */
    MESSAGE("{message}"),
    // ---- a platform function's own asserts, in legend-engine's words (the class comment's last paragraph) ----
    /** legend-engine's test-data generator: a query with more than one root, a view's (its relation tree). */
    SEED_SINGLE_ROOT_VIEW("Seed data generation currently supports queries with a single root table/view (unions are"
            + " not supported)"),
    /** legend-engine's test-data generator: a query with {@code count} roots. */
    SEED_SINGLE_ROOT("Seed data generation currently supports queries with a single root (got: {count})"),
    /** legend-engine's test-data generator: a view at the root (the relation tree node's class, Pure's toString). */
    SEED_ROOT_TABLE("Seed data generation currently supports queries with a root table (unions/views are not"
            + " supported, got: RelationTree)"),
    /** legend-engine's test-data generator: a table with no key it can carry. */
    SEED_TABLE_KEYS("Table : {table} must have atleast 1 primary key or non-null column to utilise this feature"),
    /** legend-engine's CSV seed: a schema none of the runtime's databases declares. */
    CSV_SCHEMA_NOT_FOUND("There should be exactly 1 schema with name {schema} found in the provided runtime"
            + " database(s) [{databases}] at the given connection."),
    /** legend-engine's CSV seed: a table the schema does not declare. */
    CSV_TABLE_NOT_FOUND("Table \"{table}\" not found in Schema \"{schema}\" in Database(s) {databases}"),
    /** legend-engine's CSV seed: a column the table does not declare. */
    CSV_COLUMN_NOT_FOUND("Column \"{column}\" not found in Table \"{table}\" in Database(s) {database}");

    private final String template;

    AssertKind(String template) {
        this.template = template;
    }

    @Override
    public String family() {
        return "assert";
    }

    /** The number of values a record of this kind holds: its slots'. */
    public int arity() {
        return slots().size();
    }

    /** Lite's sentence of this kind over {@code values}, its slots' texts in the template's order ({@link #slots}).
     *  A count other than the slots' is refused by name. */
    @Override
    public String sentence(List<String> values) {
        List<String> slots = slots();
        if (values.size() != slots.size()) {
            throw new IllegalArgumentException(name() + ": slots " + slots + ", given " + values.size() + " values");
        }
        Map<String, String> byName = new java.util.LinkedHashMap<>();
        for (int i = 0; i < slots.size(); i++) {
            byName.put(slots.get(i), values.get(i));
        }
        return fill(byName);
    }

    /** A record of this kind, its values given by their slots' names (each slot once; a missing or extra name is
     *  refused by name), stored in the template's order. */
    public ErrorRecord record(Map<String, String> values) {
        if (!values.keySet().equals(new java.util.HashSet<>(slots()))) {
            throw new IllegalArgumentException(name() + ": slots " + slots() + ", given " + values.keySet());
        }
        List<String> ordered = new ArrayList<>();
        for (String slot : slots()) {
            ordered.add(values.get(slot));
        }
        return ErrorRecord.of(this, ordered);
    }

    /** A record of a kind with no slots. */
    public ErrorRecord record() {
        return record(Map.of());
    }

    /** A condition's failure ({@code assert}, {@code assertFalse}): the caller's own literal message, else the
     *  condition's record ({@link #TRUE}, {@link #FALSE}). */
    public static ErrorRecord condition(boolean wantTrue, @com.legend.base.Nullable String callerMessage) {
        return callerMessage != null ? MESSAGE.record(Map.of("message", callerMessage))
                : (wantTrue ? TRUE : FALSE).record();
    }

    /** The row's sentence with its slots in braces. */
    public String template() {
        return template;
    }

    /** The row's pieces in order: each literal text, and each slot as {@link Slot}. One parse, for both judges: Java
     *  joins the pieces ({@link #fill}); the database judge's SQL concatenates them. */
    public List<Piece> pieces() {
        List<Piece> out = new ArrayList<>();
        int at = 0;
        while (at < template.length()) {
            int open = template.indexOf('{', at);
            if (open < 0) {
                out.add(new Text(template.substring(at)));
                break;
            }
            if (open > at) {
                out.add(new Text(template.substring(at, open)));
            }
            int close = template.indexOf('}', open);
            if (close < 0) {
                throw new IllegalStateException(name() + ": an unclosed slot in '" + template + "'");
            }
            out.add(new Slot(template.substring(open + 1, close)));
            at = close + 1;
        }
        return List.copyOf(out);
    }

    /** The row's slot names, in order. */
    public List<String> slots() {
        List<String> out = new ArrayList<>();
        for (Piece p : pieces()) {
            if (p instanceof Slot s) {
                out.add(s.name());
            }
        }
        return List.copyOf(out);
    }

    /** The sentence with each slot replaced by its value's text (written by the product's value writer). Every slot
     *  must be given, and no other: a missing or extra value is refused by name. */
    public String fill(Map<String, String> values) {
        if (!values.keySet().equals(new java.util.HashSet<>(slots()))) {
            throw new IllegalArgumentException(name() + ": slots " + slots() + ", given " + values.keySet());
        }
        StringBuilder sb = new StringBuilder();
        for (Piece p : pieces()) {
            switch (p) {
                case Text t -> sb.append(t.text());
                case Slot s -> sb.append(values.get(s.name()));
            }
        }
        return sb.toString();
    }

    /** One piece of a sentence. */
    public sealed interface Piece permits Text, Slot {
    }

    /** Literal text. */
    public record Text(String text) implements Piece {
    }

    /** A named slot. */
    public record Slot(String name) implements Piece {
    }
}
