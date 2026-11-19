// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

/**
 * LITE'S ARROW METADATA KEYS, ONE SOURCE (H4's P10, the user's CP-13 of 2026-11-19): every key a product Arrow answer
 * carries in its schema's or a field's metadata, written here once and GENERATED for the hosts that write or read them
 * (engine-client's lite-facts.ts {@code ARROW_KEYS}, Python's _type_facts.py {@code ARROW_KEY_*}: TypeFacts, diff-tested)
 * -- until then a literal typed in each language. legend-engine's keys (its builder, activities and columns) are the
 * edge's, never here.
 */
public final class ArrowKeys {

    private ArrowKeys() {
    }

    /** The stream's: the answer's shape (lite's query/run, {@code server.LiteV1Planning.ShapedArrow}). */
    public static final String SHAPE = "legend.shape";

    /** A field's: its Pure type, as the plan types the column ({@link ArrowWriter#facts}). */
    public static final String TYPE = "legend.type";

    /** A field's: its Pure multiplicity ({@code [1]}, {@code [0..1]}, ...). */
    public static final String MULTIPLICITY = "legend.multiplicity";

    /** A field's: the database's slot the column was read from, where the plan names one. */
    public static final String SLOT = "legend.slot";

    /** A field's: a temporal column's precision, where the plan has one. */
    public static final String PRECISION = "legend.precision";

    /** A tagged column's member's: its kind. */
    public static final String KIND = "legend.kind";

    /** The stream's, on a host's grid rows (a page's plan-and-run on Python's host): the planner's answer they are of. */
    public static final String PLAN = "legend.plan";

    /** The stream's, on a host's grid rows read elsewhere (a warehouse's): who read them, and where. */
    public static final String RECEIPT = "legend.receipt";

    /** Every key, by its constant's name, in the order above: what the generator writes for the hosts. */
    public static java.util.Map<String, String> all() {
        java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
        out.put("SHAPE", SHAPE);
        out.put("TYPE", TYPE);
        out.put("MULTIPLICITY", MULTIPLICITY);
        out.put("SLOT", SLOT);
        out.put("PRECISION", PRECISION);
        out.put("KIND", KIND);
        out.put("PLAN", PLAN);
        out.put("RECEIPT", RECEIPT);
        return out;
    }
}
