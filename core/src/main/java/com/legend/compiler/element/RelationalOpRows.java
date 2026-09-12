// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.element;

import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;
import com.legend.model.RelationalOperation;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Relational-operation TREES as metamodel-store rows (group F burn,
 * 2026-09-02): one {@code relational_elements} row per node (parent + ordinal,
 * the engine's node kinds), each stamped with the compiler's inferred SQL
 * type as a {@code data_types} row ({@link RelationalTypeInference}). ONE
 * builder serves both row sources: the model's mapping / view expressions
 * (the metamodel seeds) and a query's CONSTRUCTED instances
 * ({@code ^DynaFunction(...)}, the resolver's side-output rows) — the same
 * fact shape whoever authored the tree.
 */
public final class RelationalOpRows {

    /**
     * THE ONE ROW LAYOUT of {@code metamodel.relational_elements} — the
     * whole {@code RelationalOperationElement} hierarchy (tables, views,
     * columns, table aliases, expression nodes) as rows of ONE table with a
     * {@code kind} discriminator and an {@code id} primary key (the store's
     * single-table-hierarchy idiom, user ruling 2026-09-02: the extent is an
     * indexed filtered scan, never a UNION ALL over five tables). Column
     * order = the store DDL's; every factory below fills it.
     */
    public static final int ELEMENT_COLUMNS = 21;

    private static List<String> element(String id, String kind) {
        List<String> row = new ArrayList<>(ELEMENT_COLUMNS);
        for (int i = 0; i < ELEMENT_COLUMNS; i++) {
            row.add(null);
        }
        row.set(0, id);
        row.set(1, kind);
        return row;
    }

    /** ELEMENT IDS: every relation kind has a deterministic id, so a
     * reference to a table, view or column is ONE id column (a join on
     * the primary key), never a name triple. */
    public static String tableId(String dbFqn, String schema, String name) {
        return "tbl:" + dbFqn + "|" + schema + "|" + name;
    }

    public static String viewId(String dbFqn, String schema, String name) {
        return "view:" + dbFqn + "|" + schema + "|" + name;
    }

    public static String columnId(String dbFqn, String schema, String table, String name) {
        return "column:" + dbFqn + "|" + schema + "|" + table + "|" + name;
    }

    /** A store table: {@code (name, db_fqn, schema_name)}. */
    public static List<String> tableRow(String dbFqn, String schema, String name) {
        List<String> r = element(tableId(dbFqn, schema, name), "Table");
        r.set(2, name);
        r.set(3, dbFqn);
        r.set(4, schema);
        return r;
    }

    /** A store view: {@code (name, db_fqn, schema_name)}. */
    public static List<String> viewRow(String dbFqn, String schema, String name) {
        List<String> r = element(viewId(dbFqn, schema, name), "View");
        r.set(2, name);
        r.set(3, dbFqn);
        r.set(4, schema);
        return r;
    }

    /** A table column: {@code (name, db_fqn, schema_name, table_name, dtype_id)}. */
    public static List<String> columnRow(String dbFqn, String schema, String table,
            String name, String dtypeId, int ordinal) {
        List<String> r = element(columnId(dbFqn, schema, table, name), "Column");
        r.set(2, name);
        r.set(3, dbFqn);
        r.set(4, schema);
        r.set(5, table);
        r.set(6, dtypeId);
        r.set(8, String.valueOf(ordinal));   // declaration order — Table.columns is an ORDERED collection
        return r;
    }

    /** A main-table alias owned by {@code (mapping_fqn, set_id)}: its
     * name and three element REFERENCES — the main relation it names
     * (a table's or a view's id), the view it belongs to when it is a
     * view's own alias, and the base TABLE behind it. */
    public static List<String> aliasRow(String mappingFqn, String setId, String name,
            @com.legend.Nullable String mainElementId,
            @com.legend.Nullable String viewElementId,
            @com.legend.Nullable String baseElementId) {
        List<String> r = element("alias:" + mappingFqn + "|" + setId, "TableAlias");
        r.set(2, name);
        r.set(16, mappingFqn);
        r.set(17, setId);
        r.set(18, mainElementId);
        r.set(19, viewElementId);
        r.set(20, baseElementId);
        return r;
    }

    /** An expression NODE (the op-tree kinds). */
    private static List<String> opRow(String id, String kind, @com.legend.Nullable String parent,
            @com.legend.Nullable Integer ordinal, @com.legend.Nullable String dynaName,
            @com.legend.Nullable String literal, @com.legend.Nullable String colElementId,
            @com.legend.Nullable String colName, @com.legend.Nullable String typeId,
            @com.legend.Nullable String pkMapping, @com.legend.Nullable String pkSet) {
        List<String> r = element(id, kind);
        r.set(7, parent);
        r.set(8, ordinal == null ? null : Integer.toString(ordinal));
        r.set(9, dynaName);
        r.set(10, literal);
        r.set(11, colElementId);
        r.set(12, colName);
        r.set(13, typeId);
        r.set(14, pkMapping);
        r.set(15, pkSet);
        return r;
    }

    public final List<List<String>> ops = new ArrayList<>();
    public final List<List<String>> dataTypes = new ArrayList<>();
    private final ModelContext ctx;
    private final Set<String> typeIds = new LinkedHashSet<>();

    public RelationalOpRows(ModelContext ctx) {
        this.ctx = ctx;
    }

    /** One data-type row per id (a column's declared type, a node's
     * inferred type): the m3 subclass simple name + its size/precision/
     * scale. */
    public void dataType(String id, RelationalDataType t) {
        if (!typeIds.add(id)) {
            return;
        }
        String size = null;
        String precision = null;
        String scale = null;
        switch (t) {
            case RelationalDataType.Varchar v -> size = Integer.toString(v.size());
            case RelationalDataType.Char_ c -> size = Integer.toString(c.size());
            case RelationalDataType.Binary b -> size = Integer.toString(b.size());
            case RelationalDataType.Varbinary v -> size = Integer.toString(v.size());
            case RelationalDataType.Decimal d -> {
                precision = Integer.toString(d.precision());
                scale = Integer.toString(d.scale());
            }
            case RelationalDataType.Numeric n -> {
                precision = Integer.toString(n.precision());
                scale = Integer.toString(n.scale());
            }
            default -> {
            }
        }
        dataTypes.add(java.util.Arrays.asList(id, kindOf(t), size, precision, scale));
    }

    /** The m3 datatype class simple name of a store type (the store
     * mapping's per-kind filter reads it). */
    public static String kindOf(RelationalDataType t) {
        return switch (t) {
            case RelationalDataType.BigInt ignored -> "BigInt";
            case RelationalDataType.SmallInt ignored -> "SmallInt";
            case RelationalDataType.TinyInt ignored -> "TinyInt";
            case RelationalDataType.Integer_ ignored -> "Integer";
            case RelationalDataType.Float_ ignored -> "Float";
            case RelationalDataType.Double_ ignored -> "Double";
            case RelationalDataType.Real ignored -> "Real";
            case RelationalDataType.Bit ignored -> "Bit";
            case RelationalDataType.Timestamp ignored -> "Timestamp";
            case RelationalDataType.Date_ ignored -> "Date";
            case RelationalDataType.Distinct ignored -> "Distinct";
            case RelationalDataType.Other ignored -> "Other";
            case RelationalDataType.SemiStructured ignored -> "SemiStructured";
            case RelationalDataType.Varchar ignored -> "Varchar";
            case RelationalDataType.Char_ ignored -> "Char";
            case RelationalDataType.Binary ignored -> "Binary";
            case RelationalDataType.Varbinary ignored -> "Varbinary";
            case RelationalDataType.Decimal ignored -> "Decimal";
            case RelationalDataType.Numeric ignored -> "Numeric";
            case RelationalDataType.Array ignored -> "Array";
            case RelationalDataType.Object_ ignored -> "Object";
        };
    }

    /** One node row (+ its inferred-type row) and, recursively, its
     * children. {@code scopeDb}/{@code scopeTable} resolve bare column
     * references (a view reads its own store; a set's expressions its
     * main table). A parenthesized group is transparent (no engine node). */
    public void node(RelationalOperation op, String id, @com.legend.Nullable String parent,
            @com.legend.Nullable Integer ordinal, @com.legend.Nullable String scopeDbFqn,
            @com.legend.Nullable DatabaseDefinition scopeDb, @com.legend.Nullable String scopeTable) {
        node(op, id, parent, ordinal, scopeDbFqn, scopeDb, scopeTable, null, null);
    }

    public void node(RelationalOperation op, String id, @com.legend.Nullable String parent,
            @com.legend.Nullable Integer ordinal, @com.legend.Nullable String scopeDbFqn,
            @com.legend.Nullable DatabaseDefinition scopeDb, @com.legend.Nullable String scopeTable,
            @com.legend.Nullable String pkMapping, @com.legend.Nullable String pkSet) {
        if (op instanceof RelationalOperation.Group g) {
            node(g.inner(), id, parent, ordinal, scopeDbFqn, scopeDb, scopeTable, pkMapping, pkSet);
            return;
        }
        String kind;
        String dynaName = null;
        String literal = null;
        String colDb = null;
        String colSchema = null;
        String colTable = null;
        String colName = null;
        List<RelationalOperation> children = List.of();
        String childDbFqn = scopeDbFqn;
        DatabaseDefinition childDb = scopeDb;
        switch (op) {
            case RelationalOperation.ColumnRef c -> {
                kind = "TableAliasColumn";
                colDb = c.databaseName() != null ? c.databaseName() : scopeDbFqn;
                DatabaseDefinition db = colDb == null ? null
                        : ctx.findDatabase(colDb).orElse(scopeDb);
                String[] st = splitTable(db, c.table());
                colSchema = st[0];
                colTable = st[1];
                colName = c.column();
            }
            case RelationalOperation.TargetColumnRef t -> {
                kind = "TableAliasColumn";
                colDb = scopeDbFqn;
                if (scopeTable != null) {
                    String[] st = splitTable(scopeDb, scopeTable);
                    colSchema = st[0];
                    colTable = st[1];
                }
                colName = t.column();
            }
            // 4.145.0 relational lambdas: parsed and carried; their
            // metamodel rows are the lowering leg's — refused loudly here
            case RelationalOperation.Lambda lam -> throw new IllegalStateException(
                    "relational lambda " + lam.parameters() + " | ... has no metamodel rows yet"
                            + " (4.145.0 construct; a lowering leg)");
            case RelationalOperation.LambdaParam p -> throw new IllegalStateException(
                    "relational lambda parameter $" + p.name() + " has no metamodel rows yet"
                            + " (4.145.0 construct; a lowering leg)");
            case RelationalOperation.Literal l -> {
                kind = "Literal";
                literal = String.valueOf(l.value());
            }
            case RelationalOperation.FunctionCall f -> {
                kind = "DynaFunction";
                dynaName = f.name();
                children = f.args();
            }
            case RelationalOperation.Comparison c -> {
                kind = "DynaFunction";
                dynaName = switch (c.op()) {
                    case EQ -> "equal";
                    case NEQ -> "notEqual";
                    case LT -> "lessThan";
                    case LTE -> "lessThanEqual";
                    case GT -> "greaterThan";
                    case GTE -> "greaterThanEqual";
                };
                children = List.of(c.left(), c.right());
            }
            case RelationalOperation.BooleanOp b -> {
                kind = "DynaFunction";
                dynaName = b.op().keyword();
                children = List.of(b.left(), b.right());
            }
            case RelationalOperation.IsNull n -> {
                kind = "DynaFunction";
                dynaName = "isNull";
                children = List.of(n.operand());
            }
            case RelationalOperation.IsNotNull n -> {
                kind = "DynaFunction";
                dynaName = "isNotNull";
                children = List.of(n.operand());
            }
            case RelationalOperation.ArrayLiteral a -> {
                kind = "LiteralList";
                children = a.elements();
            }
            case RelationalOperation.JoinNavigation j -> {
                kind = "RelationalOperationElementWithJoin";
                if (j.databaseName() != null) {
                    childDbFqn = j.databaseName();
                    childDb = ctx.findDatabase(j.databaseName()).orElse(scopeDb);
                }
                children = j.terminal() == null ? List.of() : List.of(j.terminal());
            }
            case RelationalOperation.Group ignored -> throw new IllegalStateException("unreachable");
        }
        RelationalDataType t = RelationalTypeInference.infer(op, scopeDb, ctx);
        String typeId = null;
        if (t != null) {
            typeId = "op:" + id;
            dataType(typeId, t);
        }
        // a column reference is the Column element's id (the join is on
        // the primary key); the column NAME stays for TableAliasColumn.columnName
        String colElementId = colDb != null && colSchema != null && colTable != null
                && colName != null ? columnId(colDb, colSchema, colTable, colName) : null;
        ops.add(opRow(id, kind, parent, ordinal, dynaName, literal,
                colElementId, colName, typeId, pkMapping, pkSet));
        for (int i = 0; i < children.size(); i++) {
            node(children.get(i), id + "/" + i, id, i, childDbFqn, childDb, scopeTable);
        }
    }

    /** {schema, table} of a table spelling: {@code schema.table} as
     * written; a bare name is the top-level ({@code default}) table or
     * view when one exists, else the declared schema holding it. */
    private static String[] splitTable(@com.legend.Nullable DatabaseDefinition db, String spelling) {
        int dot = spelling.indexOf('.');
        if (dot >= 0) {
            return new String[] {spelling.substring(0, dot), spelling.substring(dot + 1)};
        }
        // the parser lists a schema's tables in the flat list too, so a
        // declared schema holding the name wins over the flat entry
        if (db != null) {
            for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
                if (s.tables().stream().anyMatch(t -> t.name().equals(spelling))
                        || s.views().stream().anyMatch(v -> v.name().equals(spelling))) {
                    return new String[] {s.name(), spelling};
                }
            }
        }
        return new String[] {"default", spelling};
    }

}
