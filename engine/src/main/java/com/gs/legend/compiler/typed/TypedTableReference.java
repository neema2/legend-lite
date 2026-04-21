package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * Direct table reference: {@code #>{StoreName.TABLE_NAME}#} Pure syntax.
 *
 * <p>Resolved to the physical table during type-checking. Acts as a relation
 * source in the same position as {@link TypedGetAll}, but bypasses any
 * class-level mapping \u2014 the schema comes directly from the store's
 * table declaration.
 *
 * @param storeName Fully qualified store name, e.g. {@code "model::MyDatabase"}.
 * @param tableName Physical table name, e.g. {@code "T_PERSON"}.
 */
public record TypedTableReference(
        String storeName,
        String tableName,
        ExpressionType info
) implements TypedSpec {}
