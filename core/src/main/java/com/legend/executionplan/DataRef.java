// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A REFERENCE TO DATA in a plan (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.1, the user's go of 2026-11-09): where the name
 * is meaningful to any runner ({@code namespace}), the name inside it, and the SHA-256 digest and size of the bytes the
 * plan was planned against, so the same plan loads the same data on every machine. Never a machine's path: an absolute
 * name, a drive letter, a backslash, an empty segment or a {@code .}/{@code ..} segment is refused here, so no plan can
 * hold one.
 *
 * @param namespace where {@code name} resolves (a runner's store for it: {@link DataStores})
 * @param name      a relative, {@code /}-separated name inside the namespace
 * @param digest    the bytes' SHA-256, lowercase hex
 * @param size      the bytes' length
 * @param format    how the bytes are read
 */
public record DataRef(Namespace namespace, String name, String digest, long size, Format format) {

    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");

    public DataRef {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(format, "format");
        requireName(name);
        Objects.requireNonNull(digest, "digest");
        if (!SHA256.matcher(digest).matches()) {
            throw new IllegalArgumentException("a data reference's digest is a SHA-256 in lowercase hex: " + digest);
        }
        if (size < 0) {
            throw new IllegalArgumentException("a data reference's size is never negative: " + size);
        }
    }

    /** Where a reference's name resolves. More arrive with their slices (the design's 2.1: content, bundle, url). */
    public enum Namespace {
        /** A repository's test resources, by their name under its resource root. */
        RESOURCE,
        /** The model's own text above the inline threshold (L6c, 2026-11-10, decision 1): bytes the plan's BUNDLE
         *  carries, named by their digest, beside the plan and never in it -- a serialized plan travels as its JSON plus
         *  one blob per digest, each verified on read ({@code PlanJson.Bundle}). */
        CONTENT
    }

    /** How the referenced bytes are read: a closed set of declared formats, one meaning on every database
     *  (docs/DATA_IN_THE_PLAN_2026_11_09.md decision 6, ruled 2026-11-09: a file's cells mean what the platform's
     *  format says, never what a database's reader makes of them). */
    public sealed interface Format permits Csv, Statements {
    }

    /**
     * SQL STATEMENTS as a JSON array of strings, UTF-8 (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §1.1, A2: "model data
     * is SQL in the plan"): the model's own data rendered by the target's dialect -- chunked multi-row INSERTs whose
     * cells are the dialect's typed literals -- each statement sent as it is written. The form all model data takes in
     * the content store; until A2 it was the rows' text cells (a format the runner bound itself).
     */
    public record Statements() implements Format {

        /** The one statements format. */
        public static final Statements JSON = new Statements();

        /** {@code statements} as this format's text: one JSON array of strings, compact. The one writer of it. */
        public static String encode(java.util.List<String> statements) {
            if (statements.isEmpty()) {
                throw new IllegalArgumentException("a statements blob of no statement (a load of no rows is no step)");
            }
            return com.legend.json.Json.toCompact(statements);
        }

        /** This format's text as its statements, in order: a text that is not a non-empty JSON array of strings is
         *  refused by name. The one reader of it. */
        public static java.util.List<String> decode(String text) {
            if (!(com.legend.json.Json.parse(text) instanceof com.legend.json.Json.Arr arr) || arr.items().isEmpty()) {
                throw new IllegalArgumentException("a statements blob is a non-empty JSON array of strings");
            }
            java.util.List<String> out = new java.util.ArrayList<>(arr.items().size());
            for (com.legend.json.Json.Node n : arr.items()) {
                if (!(n instanceof com.legend.json.Json.Str s)) {
                    throw new IllegalArgumentException("a statements blob holds a " + n.getClass().getSimpleName()
                            + " where every item is a statement's text");
                }
                out.add(s.value());
            }
            return java.util.List.copyOf(out);
        }
    }

    /**
     * Comma-separated text, UTF-8, its first record a header (the user's ruling, 2026-11-09, decision 6): the cells by
     * upstream's rule (legend-pure's {@code loadCsvToDbTable}: Commons CSV 1.5's {@code EXCEL}, an empty cell NULL),
     * read by the database's own reader (A3; the referee's too since B2b). A record whose width is not the header's is refused, never
     * padded or cut. Its options are a small closed set, each added when a real file needs one with the default's
     * value as its default (decision 6); today the delimiter and the quote, each the default's.
     *
     * @param delimiter the cell separator
     * @param quote     the quote that opens and closes a quoted cell, doubled inside one
     */
    public record Csv(char delimiter, char quote) implements Format {

        /** The default, upstream's: a comma, a double quote. */
        public static final Csv DEFAULT = new Csv(',', '"');

        public Csv {
            if (delimiter == quote || delimiter == '\n' || delimiter == '\r' || quote == '\n' || quote == '\r') {
                throw new IllegalArgumentException("a CSV format's delimiter and quote are two characters, neither a"
                        + " line break: '" + delimiter + "', '" + quote + "'");
            }
        }
    }

    /** {@code name} refused unless it is a relative, {@code /}-separated name with no empty, {@code .} or {@code ..}
     *  segment, no backslash and no drive letter: never a machine's path (the design's P5). */
    public static String requireName(String name) {
        Objects.requireNonNull(name, "name");
        if (name.isEmpty() || name.startsWith("/") || name.indexOf('\\') >= 0 || name.indexOf(':') >= 0
                || name.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("a data reference's name is relative to its namespace, never a"
                    + " machine's path: '" + name + "'");
        }
        for (String segment : name.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("a data reference's name has an empty, '.' or '..' segment: '"
                        + name + "'");
            }
        }
        return name;
    }
}
