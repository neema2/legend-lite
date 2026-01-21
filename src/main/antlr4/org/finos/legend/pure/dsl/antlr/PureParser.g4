parser grammar PureParser;

options { tokenVocab = PureLexer; }

// =============================================================================
// Parser Grammar for Pure Query Language (Legend-Lite)
// Compatible with ANTLR 4.13.1 and GraalVM Native Image
// =============================================================================

// Entry point - a Pure query expression
query
    : expression EOF
    ;

// Expression with method chaining
expression
    : primaryExpression (ARROW methodCall)*
    ;

// Primary expressions
primaryExpression
    : classAllExpression        // Person.all()
    | relationLiteral           // #>{store::DB.TABLE}
    | graphFetchExpression      // #{...}#
    | variableAccess            // $p.property
    | instanceExpression        // ^Person(firstName='John')
    | literal                   // 'text', 42, true
    | lambdaExpression          // {x | ...}
    | LPAREN expression RPAREN  // (expr)
    ;

// Class.all() expression
classAllExpression
    : qualifiedName DOT IDENTIFIER LPAREN RPAREN   // Person.all()
    ;

// Qualified name with package path
qualifiedName
    : IDENTIFIER (DOUBLE_COLON IDENTIFIER)*
    ;

// Relation literal: #>{store::DB.TABLE_NAME}
relationLiteral
    : HASH_GREATER LBRACE qualifiedName DOT IDENTIFIER RBRACE
    ;

// GraphFetch tree: #{Person { firstName, lastName, address { city } }}#
graphFetchExpression
    : HASH_LBRACE graphFetchTree RBRACE_HASH
    ;

graphFetchTree
    : qualifiedName graphFetchPropertyList?
    ;

graphFetchPropertyList
    : LBRACE graphFetchProperty (COMMA graphFetchProperty)* RBRACE
    ;

graphFetchProperty
    : IDENTIFIER graphFetchPropertyList?   // Simple or nested property
    | STAR                                  // Wildcard all properties
    ;

// Variable access: $p, $p.firstName, $x.address.city
variableAccess
    : DOLLAR IDENTIFIER (DOT IDENTIFIER)*
    ;

// Instance expression: ^Person(firstName='John', lastName='Doe')  
instanceExpression
    : CARET qualifiedName LPAREN instancePropertyList? RPAREN
    ;

instancePropertyList
    : instanceProperty (COMMA instanceProperty)*
    ;

instanceProperty
    : IDENTIFIER ASSIGN expression
    ;

// Lambda expression: {x | $x.firstName == 'John'}
lambdaExpression
    : LBRACE lambdaParams? PIPE lambdaBody RBRACE
    ;

lambdaParams
    : IDENTIFIER (COMMA IDENTIFIER)*
    ;

lambdaBody
    : orExpression
    ;

// Operators (precedence from low to high)
orExpression
    : andExpression (DOUBLE_OR andExpression)*
    ;

andExpression
    : comparisonExpression (DOUBLE_AND comparisonExpression)*
    ;

comparisonExpression
    : additiveExpression (comparisonOperator additiveExpression)?
    ;

comparisonOperator
    : DOUBLE_EQUALS
    | NOT_EQUALS
    | LESS_THAN
    | LESS_THAN_EQ
    | GREATER_THAN
    | GREATER_THAN_EQ
    ;

additiveExpression
    : multiplicativeExpression ((PLUS | MINUS) multiplicativeExpression)*
    ;

multiplicativeExpression
    : unaryExpression ((STAR | SLASH) unaryExpression)*
    ;

unaryExpression
    : NOT unaryExpression
    | MINUS unaryExpression
    | chainExpression
    ;

chainExpression
    : atom (ARROW methodCall)*
    ;

atom
    : variableAccess
    | literal
    | qualifiedName LPAREN argumentList? RPAREN   // Function call
    | columnReference                              // ~columnName
    | LPAREN orExpression RPAREN
    ;

// Column reference: ~columnName
columnReference
    : TILDE IDENTIFIER
    ;

// Literals
literal
    : STRING_LITERAL
    | INTEGER_LITERAL
    | FLOAT_LITERAL
    | TRUE
    | FALSE
    ;

// Method calls (after ->)
methodCall
    : IDENTIFIER LPAREN argumentList? RPAREN
    ;

// Argument list
argumentList
    : argument (COMMA argument)*
    ;

argument
    : expression
    | lambdaExpression
    | listExpression
    | extendExpression
    | columnReference
    ;

// List expression: [expr1, expr2, ...]
listExpression
    : LBRACKET (argument (COMMA argument)*)? RBRACKET
    ;

// Extend expression for calculated columns: ~newCol : x | $x.col1 + $x.col2
extendExpression
    : TILDE IDENTIFIER COLON IDENTIFIER PIPE orExpression
    | TILDE IDENTIFIER COLON windowFunctionCall
    ;

// Window function: row_number()->over(~col1, ~col2)
windowFunctionCall
    : IDENTIFIER LPAREN (columnReference (COMMA columnReference)*)? RPAREN ARROW overClause
    ;

overClause
    : IDENTIFIER LPAREN (columnReference (COMMA columnReference)*)? RPAREN
    ;
