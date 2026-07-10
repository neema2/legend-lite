package com.legend.sql.dialect;

import java.util.Set;

/**
 * SQLite &mdash; the ANSI baseline rendering with SQLite's reserved-word
 * list (<a href="https://sqlite.org/lang_keywords.html">lang_keywords</a>).
 * SQLite accepts the relational core this renderer emits (SELECT/JOIN/
 * WHERE/GROUP BY/ORDER BY/LIMIT-OFFSET, TRUE/FALSE since 3.23). KNOWN
 * GAPS that fail LOUD at execution, pending SQLite-specific overrides:
 * aliased VALUES sources (TDS literals &mdash; SQLite lacks
 * {@code (VALUES ...) AS t(c1, c2)}), typed DATE/TIMESTAMP literals, and
 * everything with no ANSI encoding at all (list lambdas, QUALIFY, ASOF,
 * native PIVOT) on the base class's loud defaults.
 */
public final class Sqlite extends AnsiSqlRenderer {

    private static final Set<String> RESERVED = Set.of(
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
            "where", "with", "without");

    @Override
    protected Set<String> reservedWords() {
        return RESERVED;
    }
}
