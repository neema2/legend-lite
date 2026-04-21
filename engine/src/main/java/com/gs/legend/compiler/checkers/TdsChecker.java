package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.CString;
import com.gs.legend.ast.TdsLiteral;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.compiler.typed.TypedTdsLiteral;
import com.gs.legend.model.m3.Primitive;
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
    public TypedTdsLiteral check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        // Validate arity + arg types against the signature: tds(String[1], String[1]).
        resolveOverload("tds", af.parameters(), source);
        // tds(CString("TDS"), CString(raw)) — raw is param[1]
        String raw = ((CString) af.parameters().get(1)).value();
        TdsLiteral tds = TdsLiteral.parse(raw);

        // Build Relation schema — TdsLiteral.parse() guarantees every column has a type.
        Map<String, Type> columns = new LinkedHashMap<>();
        for (var col : tds.columns()) {
            columns.put(col.name(), mapTdsColumnType(col.type()));
        }
        var schema = Type.Schema.withoutPivot(columns);
        return new TypedTdsLiteral(tds,
                ExpressionType.one(new Type.Relation(schema)));
    }

    /**
     * Maps a TDS type annotation string to a {@link Type}. Accepts both simple
     * Pure names ({@code "Integer"}) and fully qualified paths
     * ({@code "meta::pure::metamodel::variant::Variant"}) — TDS literals in PCT tests use
     * FQNs because the TDS text lives outside normal import scope.
     *
     * <p>Throws on unrecognised types rather than defaulting to {@code String}; silent
     * coercion to String was masking bugs (variant columns typed as String, then get()
     * failing downstream with "expected Variant, got String").
     */
    private static Type mapTdsColumnType(String typeStr) {
        // Named-type fast path.
        switch (typeStr) {
            case "Integer":                  return Primitive.INTEGER;
            case "Float", "Number":          return Primitive.FLOAT;
            case "Decimal":                  return Type.DEFAULT_DECIMAL;
            case "Boolean":                  return Primitive.BOOLEAN;
            case "String":                   return Primitive.STRING;
            case "Date", "StrictDate":       return Primitive.STRICT_DATE;
            case "DateTime":                 return Primitive.DATE_TIME;
            default:
                // Try FQN lookup against the built-in primitive catalog — handles
                // "meta::pure::metamodel::variant::Variant" and other full-path references.
                var fqnMatch = Primitive.findByFqn(typeStr);
                if (fqnMatch.isPresent()) return fqnMatch.get();
                // Also tolerate the simple-name form of a FQN so "Variant" resolves even
                // though its {@link Primitive#pureName} is "JSON".
                for (Primitive p : Primitive.ALL) {
                    if (p.simpleName().equals(typeStr)) return p;
                }
                throw new PureCompileException(
                        "TDS column type '" + typeStr + "' is not a known primitive — "
                        + "add a case to TdsChecker.mapTdsColumnType or fix the TDS literal");
        }
    }
}
