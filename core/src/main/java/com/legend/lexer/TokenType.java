package com.legend.lexer;

/**
 * Token types for the hand-rolled Pure lexer ({@link Lexer}).
 *
 * Ordering is arbitrary — the model parser dispatches on enum values,
 * not ordinals.
 */
public enum TokenType {

    // ==================== Literals ====================
    STRING,             // 'text'
    BOOLEAN,            // true | false (also matched by TRUE/FALSE)
    TRUE,               // 'true'
    FALSE,              // 'false'
    INTEGER,            // [0-9]+
    FLOAT,              // [0-9]*.[0-9]+(e[+-]?[0-9]+)?[fF]?
    DECIMAL,            // like FLOAT but ends with d/D
    DATE,               // %2024-01-15 / %2024-01-15T10:30:00
    STRICTTIME,         // %10:30:45
    LATEST_DATE,        // %latest
    QUOTED_STRING,      // "text" (relational identifiers)

    // ==================== Punctuation ====================
    AND,                // &&
    OR,                 // ||
    NOT,                // !
    COMMA,              // ,
    EQUAL,              // =
    TEST_EQUAL,         // ==
    TEST_NOT_EQUAL,     // !=
    ARROW,              // ->
    BRACE_OPEN,         // {
    BRACE_CLOSE,        // }
    BRACKET_OPEN,       // [
    BRACKET_CLOSE,      // ]
    PAREN_OPEN,         // (
    PAREN_CLOSE,        // )
    COLON,              // :
    DOT,                // .
    DOLLAR,             // $
    DOT_DOT,            // ..
    SEMI_COLON,         // ;
    NEW_SYMBOL,         // ^
    PIPE,               // |
    TILDE,              // ~
    SUBSET,             // ⊆
    QUESTION,           // ?
    AT,                 // @
    PLUS,               // +
    STAR,               // *
    MINUS,              // -
    DIVIDE,             // /
    LESS_THAN,          // <
    LESS_OR_EQUAL,      // <=
    GREATER_THAN,       // >
    GREATER_OR_EQUAL,   // >=
    NOT_EQUAL,          // <>
    PATH_SEPARATOR,     // ::

    // ==================== M3 Keywords ====================
    ALL,                // all
    LET,                // let
    COMPARATOR,         // comparator
    ALL_VERSIONS,       // allVersions
    ALL_VERSIONS_IN_RANGE,  // allVersionsInRange

    // ==================== Domain Keywords ====================
    IMPORT,             // import
    CLASS,              // Class
    ASSOCIATION,        // Association
    PROFILE,            // Profile
    ENUM,               // Enum
    FUNCTION,           // function
    EXTENDS,            // extends
    STEREOTYPES,        // stereotypes
    TAGS,               // tags
    NATIVE,             // native
    AS,                 // as

    // ==================== Constraint Tilde Commands ====================

    // ==================== Mapping Keywords ====================
    MAPPING,            // Mapping
    INCLUDE,            // include
    MAPPING_TESTABLE_SUITES,  // testSuites
    MAPPING_TESTS_QUERY,      // query

    // ==================== Service Keywords ====================
    SERVICE,            // Service
    SERVICE_PATTERN,    // pattern
    SERVICE_OWNERS,     // owners
    SERVICE_DOCUMENTATION, // documentation
    SERVICE_AUTO_ACTIVATE_UPDATES, // autoActivateUpdates
    SERVICE_EXEC,       // execution
    SERVICE_SINGLE,     // Single
    SERVICE_MAPPING,    // mapping
    SERVICE_RUNTIME,    // runtime

    // ==================== Runtime Keywords ====================
    RUNTIME,            // Runtime
    SINGLE_CONNECTION_RUNTIME, // SingleConnectionRuntime
    MAPPINGS,           // mappings
    CONNECTIONS,        // connections
    CONNECTION,         // connection

    // ==================== Relational/Database Keywords ====================
    DATABASE,           // Database
    TABLE,              // Table
    SCHEMA,             // Schema
    VIEW,               // View
    FILTER,             // Filter
    MULTIGRAIN_FILTER,  // MultiGrainFilter
    JOIN,               // Join

    // Tilde commands (relational)
    FILTER_CMD,         // ~filter
    DISTINCT_CMD,       // ~distinct
    GROUP_BY_CMD,       // ~groupBy
    MAIN_TABLE_CMD,     // ~mainTable
    PRIMARY_KEY_CMD,    // ~primaryKey
    SRC_CMD,            // ~src

    // Composite tokens
    TARGET,             // {target}
    PRIMARY_KEY,        // PRIMARY KEY
    NOT_NULL,           // NOT NULL
    IS_NULL,            // is null
    IS_NOT_NULL,        // is not null
    RELATIONAL_AND,     // and
    RELATIONAL_OR,      // or
    RELATIONAL,         // Relational
    PURE_MAPPING,       // Pure

    // Milestoning

    // Mapping modifiers
    ASSOCIATION_MAPPING, // AssociationMapping
    ENUMERATION_MAPPING, // EnumerationMapping
    OTHERWISE,          // Otherwise
    INLINE,             // Inline

    // ==================== Connection Keywords ====================
    RELATIONAL_DATABASE_CONNECTION, // RelationalDatabaseConnection
    STORE,              // store
    TYPE,               // type
    RELATIONAL_DATASOURCE_SPEC, // specification
    RELATIONAL_AUTH_STRATEGY,   // auth
    H2,                 // H2

    // ==================== Island Mode ====================
    ISLAND_OPEN,
    // Emitted INSIDE islands for nested '#{' / bare '#' / free content — the
    // DSL collector consumes their TEXT generically; depth tracking counts
    // ISLAND_START as an open.
    ISLAND_START,
    ISLAND_HASH,
    ISLAND_CONTENT,
    // THE lexer-error token: unlexable input. The cursor THROWS on contact —
    // it must never flow silently into a parse (audit: it used to).
    INVALID,        // #...{
    ISLAND_END,         // }#
    ISLAND_ARROW_EXIT,  // }->
    ISLAND_BRACE_OPEN,  // { (inside island)
    ISLAND_BRACE_CLOSE, // } (inside island)
    TDS_LITERAL,        // #TDS...#

    // ==================== Identifiers ====================
    VALID_STRING,       // [A-Za-z_$][A-Za-z0-9_$]*

    // ==================== Special ====================
    EOF                 // end of input
}
