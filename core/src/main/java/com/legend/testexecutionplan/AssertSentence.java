// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A FAILED ASSERT'S SENTENCE IN THE PRODUCT, ONE TABLE FOR BOTH JUDGES (the user's judge model, 2026-11-17: the host and
 * database judges are interchangeable in the product, so they must agree on the message as well as the verdict). Each
 * row is one assert kind's sentence, its slots named in braces. The host judge fills a row in Java; the database judge's
 * SQL is generated from the same row at planning. A slot's value is written by the product's own value writer (a Float
 * in six decimals, S311; a DateTime as the product writes one; a collection as {@code [a, b]}), so a failure message
 * shows a value exactly as the product shows it. Data only: no rule here decides a verdict.
 *
 * <p>Compatibility mode (PCT, the corpus) keeps legend-pure's words, the edge's; this table is the product's.
 */
public enum AssertSentence {
    EQUALS("expected {expected}, got {actual}"),
    EQUALS_ROWS("expected the rows {expected}, got {actual}"),
    NOT_EQUALS("expected a value other than {actual}"),
    SAME_ELEMENTS("expected the elements {expected} in any order, got {actual}"),
    SIZE("expected {expected} values, got {actual}"),
    NOT_SIZE("expected a size other than {size}"),
    CONTAINS("expected {collection} to contain {value}"),
    NOT_CONTAINS("expected {collection} not to contain {value}"),
    EMPTY("expected no values, got {collection}"),
    NOT_EMPTY("expected at least one value, got none"),
    TRUE("expected true, got false"),
    FALSE("expected false, got true"),
    IS("expected the same instance as {expected}, got {actual}"),
    IS_NOT("expected an instance other than {actual}"),
    WITHIN_TOLERANCE("expected {expected} within {delta}, got {actual}"),
    JSON_EQUALS("expected the JSON {expected}, got {actual}"),
    ERROR("expected the error \"{expected}\", got \"{actual}\""),
    NO_ERROR("expected the error \"{expected}\", got none"),
    AT_ELEMENT("{sentence} at element {index}");

    private final String template;

    AssertSentence(String template) {
        this.template = template;
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
