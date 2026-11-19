// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.setup;

import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;

/**
 * DDL rendering from the COMPILED store model — the K-native
 * {@code dropAndCreateTableInDb} boundary (the real engine's
 * {@code toDDL.pure} walks the Database metamodel; legend-lite renders
 * from {@link DatabaseDefinition}). Lives in the SETUP library (the plan
 * side's setup text, moved out of exec 2026-10-08,
 * docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9) — the SQL layer
 * ({@code com.legend.sql}) stays standalone and never sees the store model.
 * Model-derived DDL is spelled correctly for its
 * TARGET the first time (F7.4, audit S4): the type switch carries both
 * flavors, and the {@code edge.RawSqlBoundary} translator serves
 * HAND-WRITTEN corpus text only — text whose origin really is another
 * dialect.
 *
 * <p>Constraints: the {@code dropAndCreateTableInDb} native emits the
 * declared {@code PRIMARY KEY(...)} and {@code NULL}/{@code NOT NULL}
 * exactly like the engine (its {@code applyConstraints} defaults true —
 * batch 71, measured across the corpus: zero seed failures, so no
 * fixture re-seeds a keyed table it created through the native; the one
 * failure was declared-quoted key names, now spelled per flavor). The
 * HARNESS's ambient CSV seed ({@code CsvSeed}) still creates its tables
 * without constraints: milestoned test data holds several versions of
 * one id, and that seeding has no engine counterpart to match.
 */
public final class Ddl {

    private Ddl() {
    }

    /** The render TARGETS of the ONE generator (ratified E4 design:
     * engine-exact text is a FLAVOR of the single speller, never a
     * second one). {@code H2_EXEC}/{@code DUCK_EXEC} are the EXECUTION
     * forms — full-name quoting, no constraints (the deliberate DuckDB
     * re-seed divergence in this file's header); {@code ENGINE_TEXT} is
     * the engine's {@code translateCreateTableStatementDefault}
     * (extensionDefaults.pure:609-620) — reserved-word column quoting,
     * engine type spellings (INT), NULL / NOT NULL nullability,
     * trailing {@code , PRIMARY KEY(...)} with RAW pk names. */
    /** {@code Drop table if exists s.T;} — the engine's
     * dropTableStatement spelling, identical across every flavor. */
    public static com.legend.sql.SqlDdl.DropTable dropTable(@com.legend.base.Nullable String schema,
            String table) {
        return new com.legend.sql.SqlDdl.DropTable(schema, table, com.legend.sql.NameQuoting.BARE);
    }

    /** {@link #dropTable(String, String)} of a declared table: its names, and which the store declared quoted
     *  ({@link #quoting}). */
    public static com.legend.sql.SqlDdl.DropTable dropDeclaredTable(DatabaseDefinition.TableDefinition def,
            @com.legend.base.Nullable String schema) {
        return new com.legend.sql.SqlDdl.DropTable(schema, def.name(), quoting(def, schema));
    }

    /** Which of a declared table's names, in schema {@code schema} (null, or {@code default}, for none), its store
     *  declared quoted ({@code Table "Orders"}, {@code Schema "Sales"}): data the dialect spells (sql.NameQuoting), never
     *  quote characters in the names. */
    public static com.legend.sql.NameQuoting quoting(DatabaseDefinition.TableDefinition def,
            @com.legend.base.Nullable String schema) {
        boolean named = schema != null && !schema.isEmpty() && !"default".equals(schema);
        return new com.legend.sql.NameQuoting(named && def.schemaQuoted(), def.quoted());
    }


    /** {@code constraints}: emit the engine's {@code NULL}/{@code NOT NULL}
     * and trailing {@code PRIMARY KEY(...)} in an EXECUTION flavor too —
     * the {@code dropAndCreateTableInDb} native (engine parity: its
     * {@code applyConstraints} defaults true, so the physical table the
     * engine's test creates CARRIES its declared key and the live catalog
     * answers {@code fetchDbPrimaryKeysMetaData}); the ambient seed stays
     * unconstrained (this file's header). */
    /** THE store's declared shape as a DDL node: every column with its
     *  declared type, nullability and key membership (the engine's
     *  applyConstraints defaults true — the physical table CARRIES its
     *  declared key and the live catalog answers fetchDbPrimaryKeysMetaData);
     *  the dialect renders it ({@link com.legend.sql.dialect.SqlDialect#render(com.legend.sql.SqlDdl)}). */
    public static com.legend.sql.SqlDdl.CreateTable createTable(
            DatabaseDefinition.TableDefinition def, @com.legend.base.Nullable String schema) {
        java.util.List<com.legend.sql.SqlDdl.Column> cols = new java.util.ArrayList<>();
        for (DatabaseDefinition.ColumnDefinition col : def.columns()) {
            cols.add(new com.legend.sql.SqlDdl.Column(col.name(), col.quoted(), com.legend.compiler.element.StoreCompiler.declaredType(col.dataType()),
                    col.notNull(), col.primaryKey()));
        }
        return new com.legend.sql.SqlDdl.CreateTable(schema, def.name(), quoting(def, schema), cols);
    }


    /** The SYSTEM metamodel seed's DDL (METAMODEL_STORE_HANDOFF.md &sect;5):
     * schema + drop/create through the ONE generator. Its rows are
     * {@link #metamodelRows}. Idempotent per context &mdash; the content is
     * a pure function of the active model context, so overlays simply
     * re-seed. */
    public static java.util.List<String> metamodelSeed(
            DatabaseDefinition.TableDefinition def, String schema,
            com.legend.sql.dialect.SqlDialect dialect) {
        return java.util.List.of(
                dialect.render(new com.legend.sql.SqlDdl.CreateSchema(schema, false)),
                dialect.render(dropTable(schema, def.name())),
                dialect.render(createTable(def, schema)));
    }

    /** The seed's rows for one store table (all-string cells, pre-sorted by
     * the registry's deterministic-order contract; null an absent optional
     * fact), every column in declared order, each typed by its declared type
     * (the load writes the dialect's typed literals, A2). */
    public static RowLoad metamodelRows(DatabaseDefinition.TableDefinition def, String schema,
            java.util.List<java.util.List<String>> rows) {
        return new RowLoad(schema, def.name(), quoting(def, schema), java.util.List.of(), java.util.Set.of(),
                def.columns().size(), rows, declaredTypes(def));
    }

    /** {@code def}'s columns' declared types, in declared order: a load's typed literals. */
    public static java.util.List<com.legend.sql.SqlDdl.ColumnType> declaredTypes(DatabaseDefinition.TableDefinition def) {
        java.util.List<com.legend.sql.SqlDdl.ColumnType> out = new java.util.ArrayList<>(def.columns().size());
        for (DatabaseDefinition.ColumnDefinition col : def.columns()) {
            out.add(com.legend.compiler.element.StoreCompiler.declaredType(col.dataType()));
        }
        return out;
    }

    /** One store table's seed in an IN-PROCESS engine as a plan's setup steps: its DDL ({@link #metamodelSeed}),
     *  then its rows ({@link #metamodelRows}) as a content blob of inserts ({@link LoadSteps#rows}); a table of no rows
     *  adds no rows step. The one owner of that form for the plan's target and the system database's session (the
     *  doors, 2026-10-29; until then {@code PlanMaker} and {@code SystemDatabase} each made it). */
    public static LoadSteps metamodelSteps(DatabaseDefinition.TableDefinition def, String schema,
            java.util.List<java.util.List<String>> rows, com.legend.sql.dialect.SqlDialect dialect) {
        LoadSteps.Builder out = new LoadSteps.Builder();
        for (String statement : metamodelSeed(def, schema, dialect)) {
            out.statement(statement);
        }
        return out.add(metamodelRowSteps(def, schema, rows, dialect)).build();
    }

    /** {@link #metamodelSteps}' rows alone, no DDL: a model's registration in the process's catalog, whose tables are
     *  made once with the platform's rows (docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md 4.3, L4b) -- inserts only, so a
     *  registration stays inside its transaction (H2 commits on DDL); none for no row. */
    public static LoadSteps metamodelRowSteps(DatabaseDefinition.TableDefinition def, String schema,
            java.util.List<java.util.List<String>> rows, com.legend.sql.dialect.SqlDialect dialect) {
        return LoadSteps.rows(metamodelRows(def, schema, rows), dialect);
    }

    /** The FLAVORED type spelling: the deltas from the H2 base are the
     * ONLY per-target lines — DuckDB where H2's type SEMANTICS differ
     * from its name (FLOAT is an 8-byte double, BIT a boolean — spelled
     * from the TYPE, never recovered from text, F7.4); engine TEXT is
     * {@link #dataTypeToSqlText}, the ONE engine spelling. */

    /** THE engine {@code dataTypeToSqlText} spelling
     * (platform_store_relational/functions.pure:68-96), spelled ONCE —
     * the ENGINE_TEXT DDL flavor and the metamodel walk's
     * dataTypeToSqlText native both read here (ratified E4 design: no
     * type text is spelled twice). Deltas from the EXECUTION base:
     * Integer spells INT; Other spells OTHER (execution walls — a
     * column of type Other cannot be created). */
    public static String dataTypeToSqlText(RelationalDataType t) {
        return com.legend.sql.dialect.DdlSpelling.engineText(com.legend.compiler.element.StoreCompiler.declaredType(t));
    }

}
