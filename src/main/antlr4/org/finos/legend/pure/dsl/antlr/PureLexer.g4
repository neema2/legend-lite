// =============================================================================
// PureLexer.g4 - Merged Pure Grammar Lexer
// =============================================================================
// This file merges the following legend-engine grammar files:
//   - CoreFragmentGrammar.g4
//   - CoreLexerGrammar.g4
//   - M3LexerGrammar.g4
//   - DomainLexerGrammar.g4
//   - MappingLexerGrammar.g4
//   - RuntimeLexerGrammar.g4
//   - ConnectionLexerGrammar.g4
//   - RelationalLexerGrammar.g4
//   - RelationalDatabaseConnectionLexerGrammar.g4
//
// Raw source files are in: src/main/antlr4-raw/
// =============================================================================

lexer grammar PureLexer;


// =============================================================================
// FRAGMENT RULES (from CoreFragmentGrammar)
// =============================================================================

fragment True:                          'true';
fragment False:                         'false';
fragment Letter:                        [A-Za-z];
fragment Digit:                         [0-9];
fragment HexDigit:                      [0-9a-fA-F];

fragment Whitespace:                    [ \r\t\n]+;

fragment Comment:                       '/*' .*? '*/';
fragment LineComment:                   '//' ~[\r\n]*;

fragment UnicodeEsc:                    'u' (HexDigit (HexDigit (HexDigit HexDigit?)?)?)?;
fragment Esc:                           '\\';
fragment EscSeq:                        Esc
                                        (
                                            [btnfr"'\\]
                                            | UnicodeEsc
                                            | .
                                            | EOF
                                        );
fragment EscAny:                        Esc .;

fragment TimeZone:                      String | (('+' | '-')(Digit)(Digit)(Digit)(Digit));
fragment ValidString:                   (Letter | Digit | '_' ) (Letter | Digit | '_' | '$')*;
fragment FileName:                      '?[' (Letter | Digit | '_' | '.' | '/')+;
fragment FileNameEnd:                   ']?';
fragment Assign:                        ([ \r\t\n])* '=';
fragment PathSeparator:                 '::';

fragment String:                        ('\'' ( EscSeq | ~['\r\n\\] )*  '\'' );
fragment Boolean:                       True | False;
fragment Integer:                       (Digit)+;
fragment Float:                         (Digit)* '.' (Digit)+ ( ('e' | 'E') ('+' | '-')? (Digit)+)? ('f' | 'F')?;
fragment Decimal:                       ((Digit)* '.' (Digit)+ | (Digit)+) ( ('e' | 'E') ('+' | '-')? (Digit)+)? ('d' | 'D');
fragment Date:                          '%' ('-')? (Digit)+ ('-'(Digit)+ ('-'(Digit)+ ('T' DateTime TimeZone?)?)?)?;
fragment DateTime:                      (Digit)+ (':'(Digit)+ (':'(Digit)+ ('.'(Digit)+)?)?)?;
fragment StrictTime:                    '%' (Digit)+ (':'(Digit)+ (':'(Digit)+ ('.'(Digit)+)?)?)?;

fragment Invalid:                       .;


// =============================================================================
// ACTION RULES (from CoreLexerGrammar)
// =============================================================================

WHITESPACE:                             Whitespace      -> skip;
COMMENT:                                Comment         -> skip;
LINE_COMMENT:                           LineComment     -> skip;
ISLAND_OPEN:                            '#'  (~[#{])* '{'-> pushMode (ISLAND_MODE);


// =============================================================================
// TYPE TOKENS (from CoreLexerGrammar)
// =============================================================================

STRING:                                 String;
BOOLEAN:                                Boolean;
TRUE:                                   True;
FALSE:                                  False;
INTEGER:                                Integer;
FLOAT:                                  Float;
DECIMAL:                                Decimal;
DATE:                                   Date;
STRICTTIME:                             StrictTime;
LATEST_DATE:                            '%latest';


// =============================================================================
// BUILDING BLOCK TOKENS (from CoreLexerGrammar)
// =============================================================================

FILE_NAME:                              FileName;
FILE_NAME_END:                          FileNameEnd;
PATH_SEPARATOR:                         PathSeparator;

AND:                                    '&&';
OR:                                     '||';
NOT:                                    '!';
COMMA:                                  ',';
EQUAL:                                  '=';
TEST_EQUAL:                             '==';
TEST_NOT_EQUAL:                         '!=';
PERCENT:                                '%';
ARROW:                                  '->';
BRACE_OPEN:                             '{';
BRACE_CLOSE:                            '}';
BRACKET_OPEN:                           '[';
BRACKET_CLOSE:                          ']';
PAREN_OPEN:                             '(';
PAREN_CLOSE:                            ')';
COLON:                                  ':';
DOT:                                    '.';
DOLLAR:                                 '$';
DOT_DOT:                                '..';
SEMI_COLON:                             ';';
NEW_SYMBOL:                             '^';
PIPE:                                   '|';
TILDE:                                  '~';

AT:                                     '@';
PLUS:                                   '+';
STAR:                                   '*';
MINUS:                                  '-';
DIVIDE:                                 '/';
LESS_THAN:                              '<';
LESS_OR_EQUAL:                          '<=';
GREATER_THAN:                           '>';
GREATER_OR_EQUAL:                       '>=';


// =============================================================================
// M3 KEYWORDS (from M3LexerGrammar)
// =============================================================================

ALL:                                    'all';
LET:                                    'let';
ALL_VERSIONS:                           'allVersions';
ALL_VERSIONS_IN_RANGE:                  'allVersionsInRange';
TO_BYTES_FUNCTION:                      'toBytes';
NAVIGATION_PATH_BLOCK:                  '#/' (~[#])*  '#';

// GraphFetch subtype token (from GraphFetchTreeLexerGrammar)
SUBTYPE_START:                          '->subType(@';


// =============================================================================
// DOMAIN KEYWORDS (from DomainLexerGrammar)
// =============================================================================

IMPORT:                                 'import';
CLASS:                                  'Class';
ASSOCIATION:                            'Association';
PROFILE:                                'Profile';
ENUM:                                   'Enum';
MEASURE:                                'Measure';
FUNCTION:                               'function';
EXTENDS:                                'extends';
STEREOTYPES:                            'stereotypes';
TAGS:                                   'tags';
CONSTRAINT_OWNER:                       '~owner';
CONSTRAINT_EXTERNAL_ID:                 '~externalId';
CONSTRAINT_FUNCTION:                    '~function';
CONSTRAINT_MESSAGE:                     '~message';
CONSTRAINT_ENFORCEMENT:                 '~enforcementLevel';
CONSTRAINT_ENFORCEMENT_LEVEL_ERROR:     'Error';
CONSTRAINT_ENFORCEMENT_LEVEL_WARN:      'Warn';
NATIVE:                                 'native';
PROJECTS:                               'projects';
AS:                                     'as';
AGGREGATION_TYPE_COMPOSITE:             'composite';
AGGREGATION_TYPE_SHARED:                'shared';
AGGREGATION_TYPE_NONE:                  'none';


// =============================================================================
// MAPPING KEYWORDS (from MappingLexerGrammar)
// =============================================================================

MAPPING:                                'Mapping';
INCLUDE:                                'include';
TESTS:                                  'MappingTests';
// MAPPING_TESTABLE_FUNCTION is same as FUNCTION - use FUNCTION token
MAPPING_TESTABLE_DATA:                  'data';
MAPPING_TESTABLE_ASSERT:                'assert';
MAPPING_TESTABLE_DOC:                   'doc';
// MAPPING_TESTABLE_TYPE is same as TYPE - reorder so TYPE comes first
MAPPING_TESTABLE_SUITES:                'testSuites';
MAPPING_TEST_ASSERTS:                   'asserts';
MAPPING_TESTS:                          'tests';
MAPPING_TESTS_QUERY:                    'query';


// =============================================================================
// SERVICE KEYWORDS (from ServiceLexerGrammar)
// =============================================================================

SERVICE:                                'Service';
SERVICE_PATTERN:                        'pattern';
SERVICE_OWNERS:                         'owners';
SERVICE_DOCUMENTATION:                  'documentation';
SERVICE_AUTO_ACTIVATE_UPDATES:          'autoActivateUpdates';
SERVICE_EXEC:                           'execution';
SERVICE_SINGLE:                         'Single';
SERVICE_MULTI:                          'Multi';
SERVICE_MAPPING:                        'mapping';
SERVICE_RUNTIME:                        'runtime';


// =============================================================================
// RUNTIME KEYWORDS (from RuntimeLexerGrammar)
// =============================================================================

RUNTIME:                                'Runtime';
SINGLE_CONNECTION_RUNTIME:              'SingleConnectionRuntime';
MAPPINGS:                               'mappings';
CONNECTIONS:                            'connections';
CONNECTION:                             'connection';
CONNECTIONSTORES:                       'connectionStores';


// =============================================================================
// RELATIONAL/DATABASE KEYWORDS (from RelationalLexerGrammar)
// =============================================================================

DATABASE:                               'Database';
TABLE:                                  'Table';
SCHEMA:                                 'Schema';
VIEW:                                   'View';
TABULAR_FUNC:                           'TabularFunction';
FILTER:                                 'Filter';
MULTIGRAIN_FILTER:                      'MultiGrainFilter';
JOIN:                                   'Join';

FILTER_CMD:                             '~filter';
DISTINCT_CMD:                           '~distinct';
GROUP_BY_CMD:                           '~groupBy';
MAIN_TABLE_CMD:                         '~mainTable';
PRIMARY_KEY_CMD:                        '~primaryKey';

TARGET:                                 '{target}';
PRIMARY_KEY:                            'PRIMARY KEY';
NOT_NULL:                               'NOT NULL';
IS_NULL:                                'is null';
IS_NOT_NULL:                            'is not null';
RELATIONAL_AND:                         'and';
RELATIONAL_OR:                          'or';

// Milestoning
MILESTONING:                            'milestoning';
BUSINESS_MILESTONING:                   'business';
BUSINESS_MILESTONING_FROM:              'BUS_FROM';
BUSINESS_MILESTONING_THRU:              'BUS_THRU';
THRU_IS_INCLUSIVE:                      'THRU_IS_INCLUSIVE';
BUS_SNAPSHOT_DATE:                      'BUS_SNAPSHOT_DATE';
PROCESSING_MILESTONING:                 'processing';
PROCESSING_MILESTONING_IN:              'PROCESSING_IN';
PROCESSING_MILESTONING_OUT:             'PROCESSING_OUT';
OUT_IS_INCLUSIVE:                       'OUT_IS_INCLUSIVE';
INFINITY_DATE:                          'INFINITY_DATE';
PROCESSING_SNAPSHOT_DATE:               'PROCESSING_SNAPSHOT_DATE';

// Mapping
ASSOCIATION_MAPPING:                    'AssociationMapping';
ENUMERATION_MAPPING:                    'EnumerationMapping';
OTHERWISE:                              'Otherwise';
INLINE:                                 'Inline';
BINDING:                                'Binding';
SCOPE:                                  'scope';
RELATIONAL:                             'Relational';
PURE_MAPPING:                           'Pure';
SRC_CMD:                                '~src';

NOT_EQUAL:                              '<>';
QUOTED_STRING:                          ('"' ( EscSeq | ~["\r\n] )*  '"');


// =============================================================================
// CONNECTION KEYWORDS (from RelationalDatabaseConnectionLexerGrammar)
// =============================================================================

RELATIONAL_DATABASE_CONNECTION:         'RelationalDatabaseConnection';
STORE:                                  'store';
TYPE:                                   'type';
MODE:                                   'mode';
RELATIONAL_DATASOURCE_SPEC:             'specification';
RELATIONAL_AUTH_STRATEGY:               'auth';
RELATIONAL_POST_PROCESSORS:             'postProcessors';
QUERY_TIMEOUT:                          'queryTimeOutInSeconds';
DB_TIMEZONE:                            'timezone';
TIMEZONE_VALUE:                         ('+' | '-') Digit Digit Digit Digit;
QUOTE_IDENTIFIERS:                      'quoteIdentifiers';
QUERY_GENERATION_CONFIGS:               'queryGenerationConfigs';

// Database connection types (commonly used)
DUCKDB:                                 'DuckDB';
SQLITE:                                 'SQLite';
POSTGRES:                               'Postgres';
H2:                                     'H2';
SNOWFLAKE:                              'Snowflake';

// Auth types
NOAUTH:                                 'NoAuth';

// Specification types  
INMEMORY:                               'InMemory';
LOCALDUCKDB:                            'LocalDuckDB';


// =============================================================================
// VALID_STRING must come after all keywords
// =============================================================================

VALID_STRING:                           ValidString;


// =============================================================================
// INVALID (catch-all, must be last)
// =============================================================================

INVALID:                                Invalid;


// =============================================================================
// ISLAND MODE (from CoreLexerGrammar)
// =============================================================================

mode ISLAND_MODE;
ISLAND_START:                           '#{'        -> pushMode (ISLAND_MODE);
ISLAND_END:                             '}#'        -> popMode;
ISLAND_HASH:                            '#';
ISLAND_BRACE_OPEN:                      '{';
ISLAND_BRACE_CLOSE:                     '}';
ISLAND_CONTENT:                         (~[{}#])+;
