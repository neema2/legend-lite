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
        return findTableDef(db, name).map(StoreCompiler::tableSchema);
    }

    static Optional<DatabaseDefinition.TableDefinition> findTableDef(
            DatabaseDefinition db, String name) {
        // A DOTTED name is schema-qualified (~mainTable [db] hr.EMPLOYEES):
        // match the named schema's table only.
        int dot = name.indexOf('.');
        if (dot > 0) {
            String schemaName = name.substring(0, dot);
            String tableName = name.substring(dot + 1);
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
            columns.add(new Type.Column(col.name(), columnType(col.dataType()), mult));
        }
        return new Type.RelationType(columns);
    }

    private static Type columnType(RelationalDataType dt) {
        return switch (dt) {
            case RelationalDataType.Bool b -> Type.Primitive.BOOLEAN;
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
                    new Type.ClassType(Pure.VARIANT.qualifiedName());
            case RelationalDataType.Array a -> throw unsupportedColumnType(dt);
            case RelationalDataType.Object_ o -> throw unsupportedColumnType(dt);
        };
    }

    private static com.legend.error.ModelException unsupportedColumnType(RelationalDataType dt) {
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL, 
                "SQL column type '" + dt.getClass().getSimpleName() + "' has no scalar Pure type");
    }
}
