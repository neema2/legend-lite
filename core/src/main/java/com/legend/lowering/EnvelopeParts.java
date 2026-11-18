// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

/**
 * THE PRODUCT'S PARTS OF A STYLED STRING ENTRY, LOWERED WHEN ASKED (L-B.TypedJsonResult, 2026-11-18): the root read as
 * a relation (its column names, their types' simple names and each row's cells, the product's own toJSON vocabulary)
 * or as a class root's objects (the graph emission's JSON array). The style's facet asks for one; the product lowers it
 * and wraps the envelope over it.
 */
public interface EnvelopeParts {

    /** The root read as a relation: its parts. */
    RelationParts relation();

    /** The root read as a class root's objects: the JSON array of them. */
    SqlExpr objects();
}
