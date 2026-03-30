package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.CString;
import com.gs.legend.ast.TdsLiteral;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Checker for {@code tds(CString("TDS"), CString(raw))}.
 *
 * <p>Parses the raw TDS text into structured columns + rows, builds a
 * Relation schema from the column definitions, and stamps TypeInfo with
 * both the schema and the parsed {@link TdsLiteral} for PlanGenerator.
 *
 * <p>Column types are resolved from explicit type annotations (e.g.,
 * {@code name:String}) or inferred from the first non-null data value.
 *
 * <p>Parser emits: {@code tds(CString("TDS"), CString(rawText))}
 */
public class TdsChecker extends AbstractChecker {

    public TdsChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        // tds(CString("TDS"), CString(raw)) — raw is param[1]
        String raw = ((CString) af.parameters().get(1)).value();
        TdsLiteral tds = TdsLiteral.parse(raw);

        // Build Relation schema — TdsLiteral.parse() guarantees every column has a type
        Map<String, GenericType> columns = new LinkedHashMap<>();
        for (var col : tds.columns()) {
            columns.put(col.name(), mapTdsColumnType(col.type()));
        }

        var schema = GenericType.Relation.Schema.withoutPivot(columns);
        return TypeInfo.builder()
                .expressionType(ExpressionType.one(new GenericType.Relation(schema)))
                .tdsLiteral(tds)
                .build();
    }

    /** Maps a TDS type annotation string to a GenericType. Never null — TdsLiteral guarantees type. */
    private static GenericType mapTdsColumnType(String typeStr) {
        return switch (typeStr) {
            case "Integer" -> GenericType.Primitive.INTEGER;
            case "Float", "Number" -> GenericType.Primitive.FLOAT;
            case "Decimal" -> GenericType.DEFAULT_DECIMAL;
            case "Boolean" -> GenericType.Primitive.BOOLEAN;
            case "String" -> GenericType.Primitive.STRING;
            case "Date", "StrictDate" -> GenericType.Primitive.STRICT_DATE;
            case "DateTime" -> GenericType.Primitive.DATE_TIME;
            default -> GenericType.Primitive.STRING;
        };
    }
}
