package com.legend.protocol.spec;

import java.util.Objects;

/**
 * A {@code #SQL{ select ... }#} expression island (the sql-expression
 * extension). Wire shape (ZTailProbe "sql-island"): a {@code classInstance}
 * of type {@code SQL} whose value is {@code {"sql": <content>}}; the span
 * covers the whole literal.
 *
 * <p>Carried for parse coverage — legend-lite's compiler refuses it loudly
 * (an inline SQL expression bypasses the typed lowering pipeline, which is
 * the #1 tenet's job).
 */
public record SqlIsland(String sql,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos)
        implements ValueSpecification {

    public SqlIsland {
        Objects.requireNonNull(sql, "sql");
    }
}
