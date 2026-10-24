// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.compiler.spec.typed;

/**
 * Lite's store writes over a store table (2026-10-18, docs/WRITES_DESIGN_2026_10_18.md §4, L1): {@code delete},
 * {@code update} and {@code merge}, each the database's statement ({@code DELETE}, {@code UPDATE}, {@code MERGE}), its
 * value the rows the database affected. Upstream's {@code write} is {@link TypedWrite}, its own node.
 */
public sealed interface TypedStoreWrite extends TypedSpec permits TypedDelete, TypedUpdate, TypedMerge {

    /** The store table the statement writes. */
    TypedTableReference table();
}
