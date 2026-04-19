package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.CString;
import com.gs.legend.ast.TdsLiteral;
import com.gs.legend.compiler.*;
import com.gs.legend.model.m3.Type;

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
        // Validate arity against registered signature: tds(String[1], String[1])
        resolveOverload("tds", af.parameters(), source);
        // tds(CString("TDS"), CString(raw)) — raw is param[1]
        String raw = ((CString) af.parameters().get(1)).value();
        TdsLiteral tds = TdsLiteral.parse(raw);

        // Build Relation schema — TdsLiteral.parse() guarantees every column has a type
        Map<String, Type> columns = new LinkedHashMap<>();
        for (var col : tds.columns()) {
            columns.put(col.name(), mapTdsColumnType(col.type()));
        }

        var schema = Type.Schema.withoutPivot(columns);
        return TypeInfo.builder()
                .expressionType(ExpressionType.one(new Type.Relation(schema)))
                .tdsLiteral(tds)
                .build();
    }

    /** Maps a TDS type annotation string to a Type. Never null — TdsLiteral guarantees type. */
    private static Type mapTdsColumnType(String typeStr) {
        return switch (typeStr) {
            case "Integer" -> Type.Primitive.INTEGER;
            case "Float", "Number" -> Type.Primitive.FLOAT;
            case "Decimal" -> Type.DEFAULT_DECIMAL;
            case "Boolean" -> Type.Primitive.BOOLEAN;
            case "String" -> Type.Primitive.STRING;
            case "Date", "StrictDate" -> Type.Primitive.STRICT_DATE;
            case "DateTime" -> Type.Primitive.DATE_TIME;
            default -> Type.Primitive.STRING;
        };
    }
}
