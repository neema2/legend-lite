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
public final class StoreCompiler {

    private StoreCompiler() {
    }

    /** A table's columns as a bare {@link Type.RelationType} row-struct (doc §G-α). */
    static Type.RelationType tableSchema(DatabaseDefinition.TableDefinition table) {
        List<Type.Column> columns = new ArrayList<>(table.columns().size());
        for (var col : table.columns()) {
            Multiplicity mult = (col.notNull() || col.primaryKey())
                    ? Multiplicity.Bounded.ONE : Multiplicity.Bounded.ZERO_ONE;
            // the BARE name: relation space names columns as Pure does
            // (the engine strips a declaration's quotes where a table becomes
            // a relation). Quoting is a spelling, carried to the SQL as a
            // rendering fact (TypedTableReference.quotedColumns)
            columns.add(new Type.Column(col.name(), columnType(col.dataType()), mult));
        }
        return new Type.RelationType(columns);
    }

    /**
     * THE store column type: a relational data type as its Pure type, for every kind
     * (docs/STORE_TYPES_HOMEWORK_2026_10_02.md, ruled 2026-10-02). A type Pure cannot name --
     * OTHER, and DISTINCT (the SQL-standard user-defined distinct type) -- is a String, as upstream
     * types them (dataTypeToCompatiblePureType), and is READ as text (the dialect's stored read); a
     * nested value -- ARRAY, OBJECT, SEMISTRUCTURED -- is a Variant, as DuckDB's catalog declares
     * every nested type.
     */
    static Type columnType(RelationalDataType dt) {
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
            case RelationalDataType.Time t -> Type.Primitive.STRICT_TIME;
            case RelationalDataType.TimestampNs t -> Type.Primitive.DATE_TIME;
            case RelationalDataType.Distinct d -> Type.Primitive.STRING;
            case RelationalDataType.Other o -> Type.Primitive.STRING;
            // nested values are Variant — the get()/to(@Type) navigation
            // surface (engine GetChecker's source shape)
            case RelationalDataType.SemiStructured s -> VARIANT;
            case RelationalDataType.Array a -> VARIANT;
            case RelationalDataType.Object_ o -> VARIANT;
        };
    }

    private static final Type VARIANT = new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.VARIANT);

    /**
     * The store model's column type as the SQL layer's DECLARED type: the one crossing from store
     * model to SQL IR (the SQL layer stands alone). One owner for DDL and for a scan's stored types
     * (docs/STORE_TYPES_HOMEWORK_2026_10_02.md, step 2), moved here from exec.Ddl.
     */
    public static com.legend.sql.SqlDdl.ColumnType declaredType(RelationalDataType t) {
        return switch (t) {
            case RelationalDataType.BigInt ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.BIGINT);
            case RelationalDataType.SmallInt ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.SMALLINT);
            case RelationalDataType.TinyInt ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.TINYINT);
            case RelationalDataType.Integer_ ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.INTEGER);
            case RelationalDataType.Float_ ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.FLOAT);
            case RelationalDataType.Double_ ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.DOUBLE);
            case RelationalDataType.Real ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.REAL);
            case RelationalDataType.Bit ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.BIT);
            case RelationalDataType.Timestamp ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.TIMESTAMP);
            case RelationalDataType.Date_ ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.DATE);
            case RelationalDataType.Time ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.TIME);
            case RelationalDataType.TimestampNs ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.TIMESTAMP_NS);
            case RelationalDataType.SemiStructured ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.JSON);
            case RelationalDataType.Other ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.OTHER);
            case RelationalDataType.Distinct ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.DISTINCT);
            case RelationalDataType.Array ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.ARRAY);
            case RelationalDataType.Object_ ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.OBJECT);
            case RelationalDataType.Varchar v -> new com.legend.sql.SqlDdl.ColumnType.Sized("VARCHAR", v.size());
            case RelationalDataType.Char_ c -> new com.legend.sql.SqlDdl.ColumnType.Sized("CHAR", c.size());
            case RelationalDataType.Binary b -> new com.legend.sql.SqlDdl.ColumnType.Sized("BINARY", b.size());
            case RelationalDataType.Varbinary v -> new com.legend.sql.SqlDdl.ColumnType.Sized("VARBINARY", v.size());
            case RelationalDataType.Decimal d -> new com.legend.sql.SqlDdl.ColumnType.Scaled("DECIMAL", d.precision(), d.scale());
            case RelationalDataType.Numeric n -> new com.legend.sql.SqlDdl.ColumnType.Scaled("NUMERIC", n.precision(), n.scale());
        };
    }

    private static com.legend.sql.SqlDdl.ColumnType plain(com.legend.sql.SqlDdl.ColumnType.Kind k) {
        return new com.legend.sql.SqlDdl.ColumnType.Plain(k);
    }
}
