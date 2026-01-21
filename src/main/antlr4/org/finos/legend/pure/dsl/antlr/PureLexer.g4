lexer grammar PureLexer;

// =============================================================================
// Lexer Grammar for Pure Query Language (Legend-Lite)
// Compatible with ANTLR 4.13.1 and GraalVM Native Image
// =============================================================================

// Two-character operators (must come before single-char)
ARROW           : '->' ;
DOUBLE_EQUALS   : '==' ;
NOT_EQUALS      : '!=' ;
LESS_THAN_EQ    : '<=' ;
GREATER_THAN_EQ : '>=' ;
DOUBLE_AND      : '&&' ;
DOUBLE_OR       : '||' ;
HASH_GREATER    : '#>' ;
HASH_LBRACE     : '#{' ;
RBRACE_HASH     : '}#' ;
DOUBLE_COLON    : '::' ;

// Single-character operators and delimiters
DOT             : '.' ;
PIPE            : '|' ;
DOLLAR          : '$' ;
LESS_THAN       : '<' ;
GREATER_THAN    : '>' ;
NOT             : '!' ;
LPAREN          : '(' ;
RPAREN          : ')' ;
LBRACKET        : '[' ;
RBRACKET        : ']' ;
LBRACE          : '{' ;
RBRACE          : '}' ;
COMMA           : ',' ;
TILDE           : '~' ;
COLON           : ':' ;
CARET           : '^' ;
ASSIGN          : '=' ;
STAR            : '*' ;
PLUS            : '+' ;
MINUS           : '-' ;
SLASH           : '/' ;

// Keywords
TRUE            : 'true' ;
FALSE           : 'false' ;

// Identifiers
IDENTIFIER
    : [a-zA-Z_] [a-zA-Z0-9_]*
    ;

// String literals (single-quoted)
STRING_LITERAL
    : '\'' ( ~['\\\r\n] | '\\' . )* '\''
    ;

// Numeric literals
INTEGER_LITERAL
    : '-'? DIGIT+
    ;

FLOAT_LITERAL
    : '-'? DIGIT+ '.' DIGIT+
    ;

fragment DIGIT : [0-9] ;

// Whitespace (skip)
WS : [ \t\r\n]+ -> skip ;

// Comments (skip)
LINE_COMMENT    : '//' ~[\r\n]* -> skip ;
BLOCK_COMMENT   : '/*' .*? '*/' -> skip ;
