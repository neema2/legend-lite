package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.CString;
import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.PureCompileException;
import com.gs.legend.compiler.TypeCheckEnv;
import com.gs.legend.compiler.TypeChecker;
import com.gs.legend.compiler.typed.TypedSourceUrl;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.Type;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Checker for {@code sourceUrl(CString url)}.
 *
 * <p>Models an external data-source root: rows come from a URL that the
 * dialect inlines as a SQL subquery (e.g. {@code data:application/json,...}
 * → DuckDB {@code SELECT unnest(CAST('...' AS JSON[])) AS "data"}).
 *
 * <p>The relation type is fixed: a single {@code data} VARIANT column whose
 * rows are the unnested top-level elements of the payload. Property-bearing
 * extends (synthesised by {@code MappingNormalizer.variantIdentity}) fan
 * that one column out into named property columns via {@code get($row.data, '<prop>')}.
 *
 * <p>{@code MappingNormalizer} emits: {@code sourceUrl(CString("data:application/json,..."))}
 */
public class SourceUrlChecker extends AbstractChecker {

    public SourceUrlChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypedSourceUrl check(AppliedFunction af, TypedSpec source,
                                TypeChecker.CompilationContext ctx) {
        resolveOverload("sourceUrl", af.parameters(), source);
        if (!(af.parameters().get(0) instanceof CString urlLit)) {
            throw new PureCompileException(
                    "sourceUrl: expected a string literal URL, got " + af.parameters().get(0));
        }
        Map<String, Type> columns = new LinkedHashMap<>();
        columns.put("data", Primitive.VARIANT);
        Type.Schema schema = Type.Schema.withoutPivot(columns);
        return new TypedSourceUrl(
                urlLit.value(),
                ExpressionType.one(new Type.Relation(schema)));
    }
}
