package com.gs.legend.model;

import com.gs.legend.ast.*;
import com.gs.legend.model.def.RelationalOperation;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts {@link RelationalOperation} AST (from database filter definitions)
 * into {@link ValueSpecification} AST (for the compiler pipeline).
 *
 * <p>This is a model-layer transformation: it converts the relational definition
 * format into the Pure expression format that TypeChecker can compile.
 * Column references become {@code $row.COLUMN} property accesses.
 *
 * <p>Used by {@link com.gs.legend.compiler.MappingNormalizer} to convert
 * {@code ~filter} conditions into compilable expressions.
 */
public final class RelationalMappingConverter {

    private RelationalMappingConverter() {}

    /**
     * Converts a RelationalOperation to a ValueSpecification.
     * Column references become {@code $row.COLUMN}.
     *
     * @param op The relational operation to convert
     * @return The equivalent ValueSpecification
     * @throws IllegalArgumentException if the operation type is not supported
     */
    public static ValueSpecification convert(RelationalOperation op) {
        return convert(op, null);
    }

    /**
     * Converts a RelationalOperation to a ValueSpecification with table-to-variable mapping.
     * When {@code tableToParam} is provided, {@code ColumnRef(db, "T_PERSON", "ID")} becomes
     * {@code $prev.ID} if tableToParam maps "T_PERSON" to "prev".
     * When null, all columns map to {@code $row.COLUMN}.
     *
     * @param op           The relational operation to convert
     * @param tableToParam Map from table name to lambda parameter name (null for default "row")
     */
    public static ValueSpecification convert(RelationalOperation op, Map<String, String> tableToParam) {
        return convert(op, tableToParam, "__target__");
    }

    /**
     * Converts a RelationalOperation to a ValueSpecification with table-to-variable mapping
     * and explicit target parameter name for {@link RelationalOperation.TargetColumnRef}.
     *
     * @param op              The relational operation to convert
     * @param tableToParam    Map from table name to lambda parameter name (null for default "row")
     * @param targetParamName Variable name for TargetColumnRef nodes (e.g., "tgt" for traverse lambda)
     */
    public static ValueSpecification convert(RelationalOperation op, Map<String, String> tableToParam,
                                              String targetParamName) {
        return switch (op) {
            case RelationalOperation.ColumnRef ref -> {
                String param;
                if (tableToParam != null) {
                    param = tableToParam.get(ref.table());
                    if (param == null) throw new IllegalArgumentException(
                            "Unknown table '" + ref.table() + "' in join condition. Known tables: " + tableToParam.keySet());
                } else {
                    param = "row";
                }
                yield new AppliedProperty(ref.column(), List.of(new Variable(param)));
            }

            case RelationalOperation.TargetColumnRef ref ->
                    new AppliedProperty(ref.column(), List.of(new Variable(targetParamName)));

            case RelationalOperation.Literal lit -> convertLiteral(lit);

            case RelationalOperation.Comparison cmp ->
                    new AppliedFunction(comparisonOp(cmp.op()),
                            List.of(convert(cmp.left(), tableToParam, targetParamName), convert(cmp.right(), tableToParam, targetParamName)));

            case RelationalOperation.BooleanOp bool ->
                    new AppliedFunction(bool.op(),
                            List.of(convert(bool.left(), tableToParam, targetParamName), convert(bool.right(), tableToParam, targetParamName)));

            case RelationalOperation.IsNull isNull ->
                    new AppliedFunction("isEmpty", List.of(convert(isNull.operand(), tableToParam, targetParamName)));

            case RelationalOperation.IsNotNull isNotNull ->
                    new AppliedFunction("isNotEmpty", List.of(convert(isNotNull.operand(), tableToParam, targetParamName)));

            case RelationalOperation.FunctionCall func -> {
                    var convertedArgs = new java.util.ArrayList<>(
                            func.args().stream().map(a -> convert(a, tableToParam, targetParamName)).toList());
                    // DynaFunction if(cond, then, else) → Pure if(cond, |then, |else)
                    if (func.name().equals("if") && convertedArgs.size() >= 3) {
                        convertedArgs.set(1, new LambdaFunction(List.of(), convertedArgs.get(1)));
                        convertedArgs.set(2, new LambdaFunction(List.of(), convertedArgs.get(2)));
                    }
                    yield new AppliedFunction(func.name(), convertedArgs);
            }

            case RelationalOperation.Group grp -> convert(grp.inner(), tableToParam, targetParamName);

            default -> throw new IllegalArgumentException(
                    "Unsupported RelationalOperation: " + op.getClass().getSimpleName());
        };
    }

    /**
     * Collects all table names referenced by {@link RelationalOperation.ColumnRef} nodes
     * in the given operation tree.
     */
    public static Set<String> collectTableNames(RelationalOperation op) {
        Set<String> tables = new HashSet<>();
        collectTableNamesRecursive(op, tables);
        return tables;
    }

    private static void collectTableNamesRecursive(RelationalOperation op, Set<String> tables) {
        switch (op) {
            case RelationalOperation.ColumnRef ref -> tables.add(ref.table());
            case RelationalOperation.Comparison cmp -> {
                collectTableNamesRecursive(cmp.left(), tables);
                collectTableNamesRecursive(cmp.right(), tables);
            }
            case RelationalOperation.BooleanOp bool -> {
                collectTableNamesRecursive(bool.left(), tables);
                collectTableNamesRecursive(bool.right(), tables);
            }
            case RelationalOperation.IsNull n -> collectTableNamesRecursive(n.operand(), tables);
            case RelationalOperation.IsNotNull n -> collectTableNamesRecursive(n.operand(), tables);
            case RelationalOperation.FunctionCall f -> f.args().forEach(a -> collectTableNamesRecursive(a, tables));
            case RelationalOperation.Group g -> collectTableNamesRecursive(g.inner(), tables);
            case RelationalOperation.ArrayLiteral arr -> arr.elements().forEach(e -> collectTableNamesRecursive(e, tables));
            default -> {} // Literal, TargetColumnRef, etc.
        }
    }

    private static ValueSpecification convertLiteral(RelationalOperation.Literal lit) {
        Object value = lit.value();
        if (value instanceof String s) return new CString(s);
        if (value instanceof Long l) return new CInteger(l);
        if (value instanceof Integer i) return new CInteger(i.longValue());
        if (value instanceof Double d) return new CFloat(d);
        throw new IllegalArgumentException("Unsupported literal type: " + value.getClass());
    }

    private static String comparisonOp(String op) {
        return switch (op) {
            case "=" -> "equal";
            case "!=" , "<>" -> "notEqual";
            case ">" -> "greaterThan";
            case "<" -> "lessThan";
            case ">=" -> "greaterThanEqual";
            case "<=" -> "lessThanEqual";
            default -> throw new IllegalArgumentException("Unsupported comparison operator: " + op);
        };
    }
}
