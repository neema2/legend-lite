package com.legend.protocol.spec;

import java.util.Objects;

/**
 * A query-text island: {@code #SQL{ select ... }#} (the sql-expression extension), {@code #IQ{ select ... }#}
 * and {@code #IQ:<profile>{ select ... }#} (legend-lite's SAP IQ island, docs/IQ_SQL_STUDY_2026_10_13.md §4), or
 * {@code #Cypher{ MATCH ... }#} (legend-lite's Cypher island, docs/CYPHER_STUDY_2026_10_15.md §4). Wire
 * shape (ZTailProbe "sql-island"): a {@code classInstance} of type {@code SQL} whose value is
 * {@code {"sql": <content>}}, of type {@code IQ} whose value is {@code {"sql": <content>}} with
 * {@code "profile": <name>} when the island names one, or of type {@code Cypher} whose value is
 * {@code {"cypher": <content>}}; the span covers the whole literal.
 *
 * <p>The SQL island's text is Postgres SQL; the IQ island's is SAP IQ 16.2's, read under the named profile
 * ({@code iq16-default} when none is named). The typer translates it into the Pure Relation expression it means
 * ({@code com.legend.protocol.sql.SqlToPure}, through the language's dialect) and types that, so the island runs
 * through the same typed pipeline as any query ({@code compiler/spec/SqlIslands}).
 *
 * @param language the island's language ({@link IslandLanguage}: the registry of every island language's facts)
 * @param profile  the profile an {@code #IQ:<profile>{...}#} island names, as written; null when it names none (and
 *                 always for a language that takes none)
 */
public record SqlIsland(String sql,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos,
        IslandLanguage language,
        @com.legend.base.Nullable String profile)
        implements ValueSpecification {

    /** Position is excluded from equality, as on every value specification ({@code ValueSpecEqualityTest}). */
    @Override
    public boolean equals(Object o) {
        return o instanceof SqlIsland other && sql.equals(other.sql()) && language == other.language()
                && Objects.equals(profile, other.profile());
    }

    @Override
    public int hashCode() {
        // the opener, the language's name as the string field held it: a hash as before, stable across runs
        return Objects.hash(sql, language.opener(), profile);
    }

    public SqlIsland {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(language, "language");
    }

    /** A {@code #SQL{...}#} island. */
    public SqlIsland(String sql, @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) {
        this(sql, pos, IslandLanguage.SQL, null);
    }

    /** The island's opening as written, up to its first brace: #SQL, #IQ or #IQ:profile, then the brace. */
    public String opening() {
        return "#" + language.opener() + (profile == null ? "" : ":" + profile) + "{";
    }
}
