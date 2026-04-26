package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * External data-source root: {@code sourceUrl(CString url)} Pure syntax.
 *
 * <p>Models a relation whose rows come from a URL the dialect knows how to
 * inline as a SQL subquery — e.g. a {@code data:application/json,...}
 * literal that DuckDB renders as {@code SELECT unnest(CAST('...' AS JSON[])) AS "data"}
 * — instead of a physical table or inline {@code VALUES} literal.
 *
 * <p>Sits in the same position as {@link TypedTableReference} / {@link TypedTdsLiteral}
 * (relation-source terminal). Used by {@code MappingNormalizer.variantIdentity}
 * for {@code JsonModelConnection}s so the synthesised mapping body honestly
 * names its source as a URL instead of a fake {@code _json_<Class>} table.
 *
 * @param url External resource locator (currently {@code data:} URIs;
 *            {@code file:} / {@code http(s):} are dialect-extensible).
 * @param info Carries the relation type — typically a single
 *             {@code data} VARIANT/SEMISTRUCTURED column whose rows are the
 *             unnested top-level elements of the JSON payload. Downstream
 *             extends fan that single column out into property columns via
 *             {@code get($row.data, '<prop>')}.
 */
public record TypedSourceUrl(
        String url,
        ExpressionType info
) implements TypedSpec {}
