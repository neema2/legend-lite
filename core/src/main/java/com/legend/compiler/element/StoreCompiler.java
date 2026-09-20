package com.legend.compiler.element;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Phase F's <strong>store-element</strong> compiler: a database's physical
 * table definition &rarr; its Pure row-struct ({@link Type.RelationType}) &mdash;
 * what gives {@code #>{db.TABLE}#} a typed schema. Contains the single
 * SQL-type&rarr;Pure-type boundary ({@link #columnType}, engine
 * {@code SqlDataType.toGenericType}): integer widths collapse to
 * {@code Integer}, real/double to {@code Float}, {@code DATE}&rarr;{@code StrictDate},
 * {@code TIMESTAMP}&rarr;{@code DateTime}, {@code SEMISTRUCTURED}&rarr;{@code Variant};
 * decimal carries its precision/scale. A type with no Pure spelling throws
 * (no fallback, AGENTS.md invariant 4).
 */
final class StoreCompiler {

    private StoreCompiler() {
    }

    /** The schema of {@code name} within {@code db} — top-level tables first, then each schema's. */
    static Optional<Type.RelationType> resolveTable(DatabaseDefinition db, String name) {
        Optional<Type.RelationType> table =
                findTableDef(db, name).map(StoreCompiler::tableSchema);
        if (table.isPresent()) {
            return table;
        }
        // A TabularFunction resolves like a table -- it is a named
        // relation with declared columns -- and differs only in how the
        // SOURCE renders. Before this it parsed, round-tripped through
        // the protocol, and then failed at reference with "unknown
        // table", which is the worst of both: the grammar accepted
        // something the compiler denied existed.
        Optional<Type.RelationType> fn =
                findTabularFunctionDef(db, name).map(StoreCompiler::tableSchema);
        if (fn.isPresent()) {
            return fn;
        }
        // #>{db.View}# — views resolve like tables, schema derived from
        // each projected column's underlying physical column; a view with
        // a non-column-ref projection stays unresolved (same outcome as
        // an unknown name, never a wrong schema)
        return findViewDef(db, name).flatMap(v -> viewSchema(db, v));
    }

    /**
     * A tabular function by name, with the same schema-qualification
     * rules as a table.
     *
     * Deliberately a SEPARATE lookup rather than a flag on the table
     * search: the two render differently, so a caller that wanted a
     * table must never silently receive a function.
     */
    static Optional<DatabaseDefinition.TableDefinition> findTabularFunctionDef(
            DatabaseDefinition db, String name) {
        int dot = name.indexOf('.');
        if (dot > 0) {
            String schemaName = name.substring(0, dot);
            String fnName = name.substring(dot + 1);
            if (schemaName.equals("default")) {
                for (var t : db.tabularFunctions()) {
                    if (t.name().equals(fnName)) {
                        return Optional.of(t);
                    }
                }
            }
            for (var sch : db.schemas()) {
                if (!sch.name().equals(schemaName)) {
                    continue;
                }
                for (var t : sch.tabularFunctions()) {
                    if (t.name().equals(fnName)) {
                        return Optional.of(t);
                    }
                }
            }
            return Optional.empty();
        }
        for (var t : db.tabularFunctions()) {
            if (t.name().equals(name)) {
                return Optional.of(t);
            }
        }
        for (var sch : db.schemas()) {
            for (var t : sch.tabularFunctions()) {
                if (t.name().equals(name)) {
                    return Optional.of(t);
                }
            }
        }
        return Optional.empty();
    }

    /** Whether {@code name} names a tabular function in {@code db}. */
    static boolean isTabularFunction(DatabaseDefinition db, String name) {
        return findTabularFunctionDef(db, name).isPresent();
    }

    static Optional<DatabaseDefinition.ViewDefinition> findViewDef(
            DatabaseDefinition db, String name) {
        int dot = name.indexOf('.');
        if (dot > 0) {
            String schemaName = name.substring(0, dot);
            String viewName = name.substring(dot + 1);
            if (schemaName.equals("default")) {
                for (var v : db.views()) {
                    if (v.name().equals(viewName)) {
                        return Optional.of(v);
                    }
                }
            }
            for (var s : db.schemas()) {
                if (!s.name().equals(schemaName)) {
                    continue;
                }
                for (var v : s.views()) {
                    if (v.name().equals(viewName)) {
                        return Optional.of(v);
                    }
                }
            }
            return Optional.empty();
        }
        for (var v : db.views()) {
            if (v.name().equals(name)) {
                return Optional.of(v);
            }
        }
        for (var s : db.schemas()) {
            for (var v : s.views()) {
                if (v.name().equals(name)) {
                    return Optional.of(v);
                }
            }
        }
        return Optional.empty();
    }

    private static Optional<Type.RelationType> viewSchema(
            DatabaseDefinition db, DatabaseDefinition.ViewDefinition view) {
        List<Type.Column> columns =
                new ArrayList<>(view.columnMappings().size());
        for (var cm : view.columnMappings()) {
            if (!(cm.expression()
                    instanceof com.legend.model.RelationalOperation.ColumnRef cr)) {
                return Optional.empty();
            }
            var td = findTableDef(db, cr.table());
            if (td.isEmpty()) {
                return Optional.empty();
            }
            // a ColumnRef keeps its wire QUOTES (RelOpFromProtocol passes
            // the column verbatim) while the declaration stores the name
            // BARE + quoted flag — match both spellings (the sibling
            // tableSchema's quote-bearing identity; ledger cluster 7)
            var col = td.get().columns().stream()
                    .filter(c -> c.name().equals(cr.column())
                            || (c.quoted() && ("\"" + c.name() + "\"")
                                    .equals(cr.column())))
                    .findFirst();
            if (col.isEmpty()) {
                return Optional.empty();
            }
            Multiplicity mult = (col.get().notNull()
                    || col.get().primaryKey() || cm.primaryKey())
                    ? Multiplicity.Bounded.ONE : Multiplicity.Bounded.ZERO_ONE;
            columns.add(new Type.Column(cm.name(),
                    columnType(col.get().dataType()), mult));
        }
        return Optional.of(new Type.RelationType(columns));
    }

    static Optional<DatabaseDefinition.TableDefinition> findTableDef(
            DatabaseDefinition db, String name) {
        // A DOTTED name is schema-qualified (~mainTable [db] hr.EMPLOYEES):
        // match the named schema's table only.
        int dot = name.indexOf('.');
        if (dot > 0) {
            String schemaName = name.substring(0, dot);
            String tableName = name.substring(dot + 1);
            if (schemaName.equals("default")) {
                // ENGINE PARITY (RelationalParseTreeWalker:149): a
                // database's top-level tables ARE schema 'default' — the
                // qualified spelling resolves ONLY those (audit 22b F4:
                // the bare-name fallback found same-named tables in OTHER
                // schemas where the engine's schema('default')->table()
                // fails loud). An EXPLICIT Schema default(...) block
                // counts too.
                for (var t : db.tables()) {
                    if (t.name().equals(tableName)) {
                        return Optional.of(t);
                    }
                }
            }
            for (var s : db.schemas()) {
                if (!s.name().equals(schemaName)) {
                    continue;
                }
                for (var t : s.tables()) {
                    if (t.name().equals(tableName)) {
                        return Optional.of(t);
                    }
                }
            }
            return Optional.empty();
        }
        for (var t : db.tables()) {
            if (t.name().equals(name)) {
                return Optional.of(t);
            }
        }
        for (var s : db.schemas()) {
            for (var t : s.tables()) {
                if (t.name().equals(name)) {
                    return Optional.of(t);
                }
            }
        }
        return Optional.empty();
    }

    /** A table's columns as a bare {@link Type.RelationType} row-struct (doc §G-α). */
    private static Type.RelationType tableSchema(DatabaseDefinition.TableDefinition table) {
        List<Type.Column> columns = new ArrayList<>(table.columns().size());
        for (var col : table.columns()) {
            Multiplicity mult = (col.notNull() || col.primaryKey())
                    ? Multiplicity.Bounded.ONE : Multiplicity.Bounded.ZERO_ONE;
            // a QUOTED declaration carries its quotes IN the column
            // identity (the Typer's quote-bearing RelationType
            // convention) — renderers emit the spelling as-is
            columns.add(new Type.Column(
                    col.quoted() ? "\"" + col.name() + "\"" : col.name(),
                    columnType(col.dataType()), mult));
        }
        return new Type.RelationType(columns);
    }

    private static Type columnType(RelationalDataType dt) {
        return switch (dt) {
            case RelationalDataType.Bit b -> Type.Primitive.BOOLEAN;
            case RelationalDataType.TinyInt i -> Type.Primitive.INTEGER;
            case RelationalDataType.SmallInt i -> Type.Primitive.INTEGER;
            case RelationalDataType.Integer_ i -> Type.Primitive.INTEGER;
            case RelationalDataType.BigInt i -> Type.Primitive.INTEGER;
            case RelationalDataType.Float_ f -> Type.Primitive.FLOAT;
            case RelationalDataType.Double_ f -> Type.Primitive.FLOAT;
            case RelationalDataType.Real f -> Type.Primitive.FLOAT;
            case RelationalDataType.Decimal d -> new Type.PrecisionDecimal(d.precision(), d.scale());
            case RelationalDataType.Numeric n -> new Type.PrecisionDecimal(n.precision(), n.scale());
            case RelationalDataType.Varchar v -> Type.Primitive.STRING;
            case RelationalDataType.Char_ c -> Type.Primitive.STRING;
            case RelationalDataType.Binary b -> Type.Primitive.BYTE;
            case RelationalDataType.Varbinary b -> Type.Primitive.BYTE;
            case RelationalDataType.Date_ d -> Type.Primitive.STRICT_DATE;
            case RelationalDataType.Timestamp t -> Type.Primitive.DATE_TIME;
            case RelationalDataType.Distinct d -> throw unsupportedColumnType(dt);
            case RelationalDataType.Other o -> throw unsupportedColumnType(dt);
            // Semi-structured (JSON) columns are Variant — the get()/to(@Type)
            // navigation surface (engine GetChecker's source shape).
            case RelationalDataType.SemiStructured s ->
                    new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.VARIANT);
            case RelationalDataType.Array a -> throw unsupportedColumnType(dt);
            case RelationalDataType.Object_ o -> throw unsupportedColumnType(dt);
        };
    }

    private static com.legend.error.ModelException unsupportedColumnType(RelationalDataType dt) {
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL, 
                "SQL column type '" + dt.getClass().getSimpleName() + "' has no scalar Pure type");
    }
}
