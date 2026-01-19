package org.finos.legend.pure.dsl;

/**
 * Represents a token in the Pure language lexer.
 * 
 * @param type     The token type
 * @param value    The token value (for identifiers, literals, etc.)
 * @param position The position in the source string
 */
public record Token(TokenType type, String value, int position) {

    public enum TokenType {
        // Identifiers and literals
        IDENTIFIER, // Person, lastName, filter
        STRING_LITERAL, // 'Smith'
        INTEGER_LITERAL, // 42
        FLOAT_LITERAL, // 3.14

        // Keywords
        TRUE, // true
        FALSE, // false

        // Operators
        ARROW, // ->
        DOT, // .
        PIPE, // |
        DOLLAR, // $
        EQUALS, // ==
        NOT_EQUALS, // !=
        LESS_THAN, // <
        LESS_THAN_EQ, // <=
        GREATER_THAN, // >
        GREATER_THAN_EQ, // >=
        AND, // &&
        OR, // ||
        NOT, // !

        // Delimiters
        LPAREN, // (
        RPAREN, // )
        LBRACKET, // [
        RBRACKET, // ]
        LBRACE, // {
        RBRACE, // }
        COMMA, // ,

        // Relation syntax
        HASH_GREATER, // #> (start of Relation literal)
        TILDE, // ~ (column reference)
        COLON, // : (for extend expressions)
        DOUBLE_COLON, // :: (qualified name separator)

        // Special
        EOF, // End of input
    }

    @Override
    public String toString() {
        return type + (value != null ? "(" + value + ")" : "") + "@" + position;
    }
}
