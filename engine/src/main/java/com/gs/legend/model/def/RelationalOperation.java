package com.gs.legend.model.def;

import java.util.List;
import java.util.Objects;

/**
 * Unified expression tree for all relational contexts: join conditions, filter conditions,
 * view expressions, mapping property expressions, {@code ~groupBy}, and {@code ~primaryKey}.
 * 
 * Used in both {@code ###Relational} (Database) and {@code ###Mapping} contexts. The mapping
 * context uses additional variants like {@link ScopeBlock}, {@link EnumTransform}, and
 * {@link BindingTransform}, plus optional database qualifiers on {@link ColumnRef}.
 * 
 * Maps to the Pure grammar's {@code dbOperation} rule hierarchy:
 * <pre>
 * dbOperation        → dbBooleanOperation | dbJoinOperation
 * dbBooleanOperation → dbAtomicOperation dbBooleanOperationRight?
 * dbAtomicOperation  → dbGroupOperation | dbFunctionOperation | dbColumnOperation | dbJoinOperation | dbConstant
 * </pre>
 * 
 * Examples (Database context):
 * <pre>
 * T_PERSON.ID = T_ADDRESS.PERSON_ID
 *   → Comparison(ColumnRef(null,"T_PERSON","ID"), "=", ColumnRef(null,"T_ADDRESS","PERSON_ID"))
 * 
 * T1.A = T2.B and T1.C is not null
 *   → BooleanOp(Comparison(...), "and", IsNotNull(ColumnRef(null,"T1","C")))
 * 
 * {target}.id = T1.parent_id
 *   → Comparison(TargetColumnRef("id"), "=", ColumnRef(null,"T1","parent_id"))
 * </pre>
 * 
 * Examples (Mapping context):
 * <pre>
 * [store::DB] T_PERSON.FIRST_NAME
 *   → ColumnRef("store::DB", "T_PERSON", "FIRST_NAME")
 * 
 * concat([DB] T.FIRST, ' ', [DB] T.LAST)
 *   → FunctionCall("concat", [ColumnRef("DB","T","FIRST"), Literal(" "), ColumnRef("DB","T","LAST")])
 * 
 * [DB]@PersonFirm > @FirmCountry | COUNTRY.NAME
 *   → JoinNavigation("DB", [JoinChainElement("PersonFirm"), JoinChainElement("FirmCountry")], ColumnRef(...))
 * </pre>
 */
public sealed interface RelationalOperation {

    /**
     * A column reference: {@code TABLE.COLUMN} or {@code [DB] TABLE.COLUMN}.
     * 
     * In Database context, {@code databaseName} is null (implicit from enclosing Database).
     * In Mapping context, {@code databaseName} is the {@code [DB]} qualifier.
     * 
     * @param databaseName Optional database qualifier (null in DB context)
     * @param table        The table name
     * @param column       The column name
     */
    record ColumnRef(String databaseName, String table, String column) implements RelationalOperation {
        public ColumnRef {
            Objects.requireNonNull(table, "Table cannot be null");
            Objects.requireNonNull(column, "Column cannot be null");
        }

        /** Convenience for Database context (no database qualifier). */
        public static ColumnRef of(String table, String column) {
            return new ColumnRef(null, table, column);
        }

        /** Convenience for Mapping context (with database qualifier). */
        public static ColumnRef of(String databaseName, String table, String column) {
            return new ColumnRef(databaseName, table, column);
        }
    }

    /**
     * A target column reference for self-joins: {@code {target}.COLUMN}.
     * 
     * Grammar: {@code dbTableAliasColumnOperationWithTarget → TARGET DOT relationalIdentifier}
     * 
     * @param column The column name on the target alias
     */
    record TargetColumnRef(String column) implements RelationalOperation {
        public TargetColumnRef {
            Objects.requireNonNull(column, "Column cannot be null");
        }
    }

    /**
     * A literal value: string, integer, or float.
     * 
     * Grammar: {@code dbConstant → STRING | INTEGER | FLOAT}
     * 
     * @param value The literal value (String, Integer, Long, Double)
     */
    record Literal(Object value) implements RelationalOperation {
        public Literal {
            Objects.requireNonNull(value, "Literal value cannot be null");
        }

        public static Literal string(String s) { return new Literal(s); }
        public static Literal integer(long n) { return new Literal(n); }
        public static Literal decimal(double d) { return new Literal(d); }
    }

    /**
     * A function call: {@code functionName(arg1, arg2, ...)}.
     * 
     * Grammar: {@code dbFunctionOperation → identifier PAREN_OPEN (args ...)? PAREN_CLOSE}
     * 
     * @param name The function name (e.g., "concat", "case", "toUpper")
     * @param args The function arguments
     */
    record FunctionCall(String name, List<RelationalOperation> args) implements RelationalOperation {
        public FunctionCall {
            Objects.requireNonNull(name, "Function name cannot be null");
            Objects.requireNonNull(args, "Args cannot be null");
            args = List.copyOf(args);
        }

        public static FunctionCall of(String name, RelationalOperation... args) {
            return new FunctionCall(name, List.of(args));
        }
    }

    /**
     * A comparison: {@code left op right}.
     * 
     * Where op is: {@code = | != | <> | > | < | >= | <=}
     * 
     * @param left  The left operand
     * @param op    The operator string
     * @param right The right operand
     */
    record Comparison(RelationalOperation left, String op, RelationalOperation right) implements RelationalOperation {
        public Comparison {
            Objects.requireNonNull(left, "Left operand cannot be null");
            Objects.requireNonNull(op, "Operator cannot be null");
            Objects.requireNonNull(right, "Right operand cannot be null");
        }

        public static Comparison eq(RelationalOperation left, RelationalOperation right) {
            return new Comparison(left, "=", right);
        }
    }

    /**
     * A boolean operation: {@code left AND/OR right}.
     * 
     * @param left  The left operand
     * @param op    "and" or "or"
     * @param right The right operand
     */
    record BooleanOp(RelationalOperation left, String op, RelationalOperation right) implements RelationalOperation {
        public BooleanOp {
            Objects.requireNonNull(left, "Left operand cannot be null");
            Objects.requireNonNull(op, "Operator cannot be null");
            Objects.requireNonNull(right, "Right operand cannot be null");
        }

        public static BooleanOp and(RelationalOperation left, RelationalOperation right) {
            return new BooleanOp(left, "and", right);
        }

        public static BooleanOp or(RelationalOperation left, RelationalOperation right) {
            return new BooleanOp(left, "or", right);
        }
    }

    /**
     * IS NULL check: {@code operand is null}.
     */
    record IsNull(RelationalOperation operand) implements RelationalOperation {
        public IsNull {
            Objects.requireNonNull(operand, "Operand cannot be null");
        }
    }

    /**
     * IS NOT NULL check: {@code operand is not null}.
     */
    record IsNotNull(RelationalOperation operand) implements RelationalOperation {
        public IsNotNull {
            Objects.requireNonNull(operand, "Operand cannot be null");
        }
    }

    /**
     * A grouped (parenthesized) expression: {@code (inner)}.
     */
    record Group(RelationalOperation inner) implements RelationalOperation {
        public Group {
            Objects.requireNonNull(inner, "Inner expression cannot be null");
        }
    }

    /**
     * An array literal for function arguments like {@code in(col, [1, 2, 3])}.
     */
    record ArrayLiteral(List<RelationalOperation> elements) implements RelationalOperation {
        public ArrayLiteral {
            Objects.requireNonNull(elements, "Elements cannot be null");
            elements = List.copyOf(elements);
        }
    }

    /**
     * Join navigation: {@code [DB]@Join1 > (INNER) @Join2 | TABLE.COLUMN}.
     * 
     * Used in both DB context (view column expressions) and mapping context (property mappings).
     * The {@link JoinChainElement} captures join type (INNER, LEFT_OUTER, etc.) for each hop.
     * 
     * @param databaseName Optional database qualifier (null if not specified)
     * @param joinChain    The ordered list of join chain elements
     * @param terminal     Optional terminal expression after the pipe (null if bare join reference)
     */
    record JoinNavigation(
            String databaseName,
            List<JoinChainElement> joinChain,
            RelationalOperation terminal
    ) implements RelationalOperation {
        public JoinNavigation {
            Objects.requireNonNull(joinChain, "Join chain cannot be null");
            if (joinChain.isEmpty()) throw new IllegalArgumentException("Join chain cannot be empty");
            joinChain = List.copyOf(joinChain);
        }

        /** Convenience for single-hop join (no terminal). */
        public static JoinNavigation single(String databaseName, String joinName) {
            return new JoinNavigation(databaseName, List.of(JoinChainElement.of(databaseName, joinName)), null);
        }
    }

    // ==================== Mapping-Context Variants ====================

    /**
     * A scope block that provides a default table context for nested property mappings.
     * 
     * Pure syntax: {@code scope([DB]TABLE) (prop1 : COL1, prop2 : COL2)}
     * 
     * Only appears in Mapping context.
     * 
     * @param databaseName The database name
     * @param tableName    The default table name within the scope
     * @param properties   The property mappings within this scope
     */
    record ScopeBlock(
            String databaseName,
            String tableName,
            List<MappingDefinition.PropertyMappingDefinition> properties
    ) implements RelationalOperation {
        public ScopeBlock {
            Objects.requireNonNull(databaseName, "Database name cannot be null");
            Objects.requireNonNull(tableName, "Table name cannot be null");
            Objects.requireNonNull(properties, "Properties cannot be null");
            properties = List.copyOf(properties);
        }
    }

    /**
     * An enumeration mapping transformer: {@code EnumerationMapping ID : expression}.
     * 
     * Only appears in Mapping context (property values).
     * 
     * @param enumMappingId The enumeration mapping identifier
     * @param inner         The inner expression being transformed
     */
    record EnumTransform(String enumMappingId, RelationalOperation inner) implements RelationalOperation {
        public EnumTransform {
            Objects.requireNonNull(enumMappingId, "Enum mapping ID cannot be null");
            Objects.requireNonNull(inner, "Inner expression cannot be null");
        }
    }

    /**
     * A binding transformer: {@code Binding qualifiedName : expression}.
     * 
     * Only appears in Mapping context (property values).
     * 
     * @param bindingName The binding qualified name
     * @param inner       The inner expression being transformed
     */
    record BindingTransform(String bindingName, RelationalOperation inner) implements RelationalOperation {
        public BindingTransform {
            Objects.requireNonNull(bindingName, "Binding name cannot be null");
            Objects.requireNonNull(inner, "Inner expression cannot be null");
        }
    }

}
