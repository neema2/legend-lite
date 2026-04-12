package com.gs.legend.parser;

/**
 * Token types for the hand-rolled Pure lexer ({@link PureLexer2}).
 *
 * Mirrors the token definitions in PureLexer.g4.
 * Order does NOT need to match ANTLR's auto-assigned integer IDs —
 * the edge-case harness compares by token text, and the model parser
 * dispatches on these enum values directly.
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
    PERCENT,            // %
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
    FILE_NAME,          // ?[filename
    FILE_NAME_END,      // ]?

    // ==================== M3 Keywords ====================
    ALL,                // all
    LET,                // let
    COMPARATOR,         // comparator
    ALL_VERSIONS,       // allVersions
    ALL_VERSIONS_IN_RANGE,  // allVersionsInRange
    TO_BYTES_FUNCTION,  // toBytes
    NAVIGATION_PATH_BLOCK,  // #/...#
    SUBTYPE_START,      // ->subType(@

    // ==================== Domain Keywords ====================
    IMPORT,             // import
    CLASS,              // Class
    ASSOCIATION,        // Association
    PROFILE,            // Profile
    ENUM,               // Enum
    MEASURE,            // Measure
    FUNCTION,           // function
    EXTENDS,            // extends
    STEREOTYPES,        // stereotypes
    TAGS,               // tags
    NATIVE,             // native
    PROJECTS,           // projects
    AS,                 // as
    CONSTRAINT_ENFORCEMENT_LEVEL_ERROR, // Error
    CONSTRAINT_ENFORCEMENT_LEVEL_WARN,  // Warn
    AGGREGATION_TYPE_COMPOSITE, // composite
    AGGREGATION_TYPE_SHARED,    // shared
    AGGREGATION_TYPE_NONE,      // none

    // ==================== Constraint Tilde Commands ====================
    CONSTRAINT_OWNER,       // ~owner
    CONSTRAINT_EXTERNAL_ID, // ~externalId
    CONSTRAINT_FUNCTION,    // ~function
    CONSTRAINT_MESSAGE,     // ~message
    CONSTRAINT_ENFORCEMENT, // ~enforcementLevel

    // ==================== Mapping Keywords ====================
    MAPPING,            // Mapping
    INCLUDE,            // include
    TESTS,              // MappingTests
    MAPPING_TESTABLE_DATA,    // data
    MAPPING_TESTABLE_ASSERT,  // assert
    MAPPING_TESTABLE_DOC,     // doc
    MAPPING_TESTABLE_SUITES,  // testSuites
    MAPPING_TEST_ASSERTS,     // asserts
    MAPPING_TESTS,            // tests
    MAPPING_TESTS_QUERY,      // query

    // ==================== Service Keywords ====================
    SERVICE,            // Service
    SERVICE_PATTERN,    // pattern
    SERVICE_OWNERS,     // owners
    SERVICE_DOCUMENTATION, // documentation
    SERVICE_AUTO_ACTIVATE_UPDATES, // autoActivateUpdates
    SERVICE_EXEC,       // execution
    SERVICE_SINGLE,     // Single
    SERVICE_MULTI,      // Multi
    SERVICE_MAPPING,    // mapping
    SERVICE_RUNTIME,    // runtime

    // ==================== Runtime Keywords ====================
    RUNTIME,            // Runtime
    SINGLE_CONNECTION_RUNTIME, // SingleConnectionRuntime
    MAPPINGS,           // mappings
    CONNECTIONS,        // connections
    CONNECTION,         // connection
    CONNECTIONSTORES,   // connectionStores

    // ==================== Relational/Database Keywords ====================
    DATABASE,           // Database
    TABLE,              // Table
    SCHEMA,             // Schema
    VIEW,               // View
    TABULAR_FUNC,       // TabularFunction
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
    MILESTONING,        // milestoning
    BUSINESS_MILESTONING, // business
    BUSINESS_MILESTONING_FROM, // BUS_FROM
    BUSINESS_MILESTONING_THRU, // BUS_THRU
    THRU_IS_INCLUSIVE,  // THRU_IS_INCLUSIVE
    BUS_SNAPSHOT_DATE,  // BUS_SNAPSHOT_DATE
    PROCESSING_MILESTONING, // processing
    PROCESSING_MILESTONING_IN, // PROCESSING_IN
    PROCESSING_MILESTONING_OUT, // PROCESSING_OUT
    OUT_IS_INCLUSIVE,    // OUT_IS_INCLUSIVE
    INFINITY_DATE,       // INFINITY_DATE
    PROCESSING_SNAPSHOT_DATE, // PROCESSING_SNAPSHOT_DATE

    // Mapping modifiers
    ASSOCIATION_MAPPING, // AssociationMapping
    ENUMERATION_MAPPING, // EnumerationMapping
    OTHERWISE,          // Otherwise
    INLINE,             // Inline
    BINDING,            // Binding
    SCOPE,              // scope

    // ==================== Connection Keywords ====================
    RELATIONAL_DATABASE_CONNECTION, // RelationalDatabaseConnection
    STORE,              // store
    TYPE,               // type
    MODE,               // mode
    RELATIONAL_DATASOURCE_SPEC, // specification
    RELATIONAL_AUTH_STRATEGY,   // auth
    RELATIONAL_POST_PROCESSORS, // postProcessors
    QUERY_TIMEOUT,      // queryTimeOutInSeconds
    DB_TIMEZONE,        // timezone
    QUOTE_IDENTIFIERS,  // quoteIdentifiers
    QUERY_GENERATION_CONFIGS, // queryGenerationConfigs
    DUCKDB,             // DuckDB
    SQLITE,             // SQLite
    POSTGRES,           // Postgres
    H2,                 // H2
    SNOWFLAKE,          // Snowflake
    NOAUTH,             // NoAuth
    INMEMORY,           // InMemory
    LOCALDUCKDB,        // LocalDuckDB

    // ==================== Island Mode ====================
    ISLAND_OPEN,        // #...{
    ISLAND_START,       // #{
    ISLAND_END,         // }#
    ISLAND_ARROW_EXIT,  // }->
    ISLAND_HASH,        // # (inside island)
    ISLAND_BRACE_OPEN,  // { (inside island)
    ISLAND_BRACE_CLOSE, // } (inside island)
    ISLAND_CONTENT,     // text inside island
    TDS_LITERAL,        // #TDS...#

    // ==================== Identifiers ====================
    VALID_STRING,       // [A-Za-z_$][A-Za-z0-9_$]*

    // ==================== Special ====================
    INVALID,            // catch-all
    EOF                 // end of input
}
