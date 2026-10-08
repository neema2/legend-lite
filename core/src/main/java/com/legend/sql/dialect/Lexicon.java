package com.legend.sql.dialect;

import java.util.Set;

/**
 * Dialect LEXICON &mdash; pure DATA, the first extraction of the
 * rewrite-then-render migration (remediation T3.2): what a backend quotes
 * and how, with no behavior attached. A backend that differs ONLY here is
 * not a dialect subclass &mdash; it is a {@code Lexicon} row handed to the
 * base renderer (SQLite was exactly that and its class is gone).
 *
 * @param quoteChar     the identifier quote character
 * @param reservedWords lower-cased words that force quoting of a
 *                      plain-looking identifier
 */
public record Lexicon(char quoteChar, Set<String> reservedWords) {

    public Lexicon {
        reservedWords = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(reservedWords));
    }

    /** DuckDB (the execution backend): every keyword DuckDB will not take as a table or column name (its
     *  {@code reserved} and {@code type_function} categories, {@code duckdb_keywords()}), held so by
     *  DuckDbKeywordsTest against the pinned DuckDB; a few more DML words besides. */
    public static final Lexicon DUCKDB = new Lexicon('"', Set.of(
            "all", "analyse", "analyze", "and", "anti", "any", "array", "as", "asc", "asof", "asymmetric", "at",
            "authorization", "between", "binary", "both", "by", "case", "cast", "check", "collate", "collation",
            "column", "columns", "concurrently", "constraint", "create", "cross", "default", "deferrable", "delete",
            "desc", "describe", "distinct", "do", "drop", "else", "end", "except", "exists", "false", "fetch", "for",
            "foreign", "freeze", "from", "full", "generated", "glob", "group", "having", "ilike", "in", "initially",
            "inner", "insert", "intersect", "into", "is", "isnull", "join", "lambda", "lateral", "leading", "left",
            "like", "limit", "map", "natural", "not", "notnull", "null", "offset", "on", "only", "or", "order",
            "outer", "overlaps", "pivot", "pivot_longer", "pivot_wider", "placing", "positional", "primary",
            "qualify", "references", "returning", "right", "select", "semi", "show", "similar", "some", "struct",
            "summarize", "symmetric", "table", "tablesample", "then", "to", "trailing", "true", "try_cast", "union",
            "unique", "unpack", "unpivot", "update", "using", "values", "variadic", "verbose", "when", "where",
            "window", "with"));

    /**
     * SQLite (<a href="https://sqlite.org/lang_keywords.html">lang_keywords</a>)
     * &mdash; the ANSI baseline rendering with SQLite's reserved-word list.
     * SQLite accepts the relational core the base renderer emits (SELECT/
     * JOIN/WHERE/GROUP BY/ORDER BY/LIMIT-OFFSET, TRUE/FALSE since 3.23).
     * KNOWN GAPS that fail LOUD at execution, pending SQLite-specific
     * passes: aliased VALUES sources (TDS literals &mdash; SQLite lacks
     * {@code (VALUES ...) AS t(c1, c2)}), typed DATE/TIMESTAMP literals,
     * and everything with no ANSI encoding at all (list lambdas, QUALIFY,
     * ASOF, native PIVOT) on the base renderer's loud defaults.
     */
    public static final Lexicon SQLITE = new Lexicon('"', Set.of(
            "abort", "action", "add", "after", "all", "alter", "analyze", "and", "as",
            "asc", "attach", "autoincrement", "before", "begin", "between", "by",
            "cascade", "case", "cast", "check", "collate", "column", "commit",
            "conflict", "constraint", "create", "cross", "current_date",
            "current_time", "current_timestamp", "database", "default", "deferrable",
            "deferred", "delete", "desc", "detach", "distinct", "drop", "each",
            "else", "end", "escape", "except", "exclusive", "exists", "explain",
            "fail", "for", "foreign", "from", "full", "glob", "group", "having",
            "if", "ignore", "immediate", "in", "index", "indexed", "initially",
            "inner", "insert", "instead", "intersect", "into", "is", "isnull",
            "join", "key", "left", "like", "limit", "match", "natural", "no", "not",
            "notnull", "null", "of", "offset", "on", "or", "order", "outer", "plan",
            "pragma", "primary", "query", "raise", "recursive", "references",
            "regexp", "reindex", "release", "rename", "replace", "restrict", "right",
            "rollback", "row", "savepoint", "select", "set", "table", "temp",
            "temporary", "then", "to", "transaction", "trigger", "union", "unique",
            "update", "using", "vacuum", "values", "view", "virtual", "when",
            "where", "with", "without"));

    /** Engine-text lowering (H2/DB2 golden style): minimal quoting. */

    /** H2 2.1.214 reserved words — the ENGINE's own list
     * (h2Extension2_1_214.pure:180-192). This is the GOLDEN TEXT
     * channel's set ({@code Ddl.createTable ENGINE_TEXT}'s
     * column-name rule quotes exactly what the engine quotes); the
     * EXECUTION lexicon {@link #H2} derives from it. */
    public static final Lexicon H2_ENGINE_TEXT = new Lexicon('"', Set.of(
            "all", "and", "array", "as", "between", "case", "check",
            "constraint", "cross", "current_catalog", "current_date",
            "current_schema", "current_time", "current_timestamp",
            "current_user", "distinct", "except", "exists", "false", "fetch",
            "for", "foreign", "from", "full", "group", "having", "if", "in",
            "inner", "intersect", "interval", "is", "join", "left", "like",
            "limit", "localtime", "localtimestamp", "minus", "natural", "not",
            "null", "offset", "on", "or", "order", "primary", "qualify",
            "row", "rownum", "select", "table", "true", "union", "unique",
            "unknown", "using", "values", "where", "window", "with",
            "_rowid_", "both", "groups", "ilike", "leading", "over",
            "partition", "range", "regexp", "rows", "top", "trailing"));

    /** Stock H2 2.1.214 EXECUTION lexicon: the engine's list plus the
     * words the STOCK parser rejects in alias position (witnessed:
     * {@code AS right} &rarr; syntax error) that the engine's laxer
     * FORK omits. Execution-dialect-only — golden text channels use
     * {@link #H2_ENGINE_TEXT} or {@link #ENGINE_STYLE}. */
    public static final Lexicon H2 = new Lexicon('"',
            java.util.stream.Stream.concat(
                    H2_ENGINE_TEXT.reservedWords().stream(),
                    java.util.stream.Stream.of("right"))
                    .collect(java.util.stream.Collectors.collectingAndThen(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new), java.util.Collections::unmodifiableSet)));

    public static final Lexicon ENGINE_STYLE = new Lexicon('"', Set.of());

    /** PostgreSQL 16+ (2026-10-01 W5.5/P1 Postgres dialect). The {@code Postgres} dialect
     * QUOTES EVERY identifier (Postgres folds an unquoted name to lowercase, so a bare
     * {@code NAME} reads {@code name}), so this list never decides a spelling there; it
     * is the documented reserved set (SQL key words appendix, "reserved" in the
     * PostgreSQL column) for any reader that asks. */
    public static final Lexicon POSTGRES = new Lexicon('"', Set.of(
            "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric",
            "authorization", "binary", "both", "case", "cast", "check", "collate", "collation",
            "column", "concurrently", "constraint", "create", "cross", "current_catalog",
            "current_date", "current_role", "current_schema", "current_time",
            "current_timestamp", "current_user", "default", "deferrable", "desc", "distinct",
            "do", "else", "end", "except", "false", "fetch", "for", "foreign", "freeze", "from",
            "full", "grant", "group", "having", "ilike", "in", "initially", "inner", "intersect",
            "into", "is", "isnull", "join", "lateral", "leading", "left", "like", "limit",
            "localtime", "localtimestamp", "natural", "not", "notnull", "null", "offset", "on",
            "only", "or", "order", "outer", "overlaps", "placing", "primary", "references",
            "returning", "right", "select", "session_user", "similar", "some", "symmetric",
            "system_user", "table", "tablesample", "then", "to", "trailing", "true", "union",
            "unique", "user", "using", "variadic", "verbose", "when", "where", "window", "with"));
}
