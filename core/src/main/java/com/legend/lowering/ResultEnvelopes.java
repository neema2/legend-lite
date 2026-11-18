// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;

/**
 * THE STRING ENTRY'S STYLED VALUE, A LOWERING FACET OF THE PLAN STYLE (Phase 6's B5, 2026-11-17; L-B.TypedJsonResult,
 * the orchestrator's ruling (A) of 2026-11-18): a {@code TypedJsonResult} of kind STYLED is written by the style --
 * what to read of the root, and the envelope around it, are the facet's. The product hands its own parts on demand
 * ({@link EnvelopeParts}: a relation's names, types and cells, or a class root's objects) and the text the style
 * recorded when it planned the value; the product's style never plans a STYLED value (its planning form refuses it)
 * and its facet refuses by name. Types only (HookRulingTest).
 */
@com.legend.base.OpenLine("B5") public interface ResultEnvelopes {

    /** The STYLED value over a root of type {@code rootType}: the envelope, over the parts it asks of {@code parts},
     *  with {@code recorded}, the text the style recorded at planning (or none). */
    SqlExpr styled(Type rootType, @com.legend.base.Nullable String recorded, EnvelopeParts parts);
}
