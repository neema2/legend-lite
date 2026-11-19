// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

import java.util.List;

/** A relation root's parts (L-B.TypedJsonResult, 2026-11-18): its columns' {@code names}, their types' simple
 *  {@code typeNames}, and each row's {@code cells} in column order -- the product's own toJSON vocabulary. */
public record RelationParts(List<String> names, List<String> typeNames, List<SqlExpr> cells) {

    public RelationParts {
        names = List.copyOf(names);
        typeNames = List.copyOf(typeNames);
        cells = List.copyOf(cells);
    }
}
