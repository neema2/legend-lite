package org.finos.legend.pure.dsl;

import org.finos.legend.pure.dsl.Token.TokenType;

import java.util.ArrayList;
import java.util.List;

/**
 * Lexer for the Pure language.
 * Converts a Pure query string into a list of tokens.
 */
public final class PureLexer {

    private final String input;
    private int position;

    public PureLexer(String input) {
        this.input = input;
        this.position = 0;
    }

    /**
     * Tokenizes the entire input string.
     * 
     * @return List of tokens
     */
    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();

        while (position < input.length()) {
            skipWhitespace();
            if (position >= input.length())
                break;

            Token token = nextToken();
            if (token != null) {
                tokens.add(token);
            }
        }

        tokens.add(new Token(TokenType.EOF, null, position));
        return tokens;
    }

    private void skipWhitespace() {
        while (position < input.length() && Character.isWhitespace(input.charAt(position))) {
            position++;
        }
    }

    private Token nextToken() {
        char c = input.charAt(position);
        int start = position;

        // Two-character operators
        if (position + 1 < input.length()) {
            String twoChar = input.substring(position, position + 2);
            TokenType twoCharType = switch (twoChar) {
                case "->" -> TokenType.ARROW;
                case "==" -> TokenType.EQUALS;
                case "!=" -> TokenType.NOT_EQUALS;
                case "<=" -> TokenType.LESS_THAN_EQ;
                case ">=" -> TokenType.GREATER_THAN_EQ;
                case "&&" -> TokenType.AND;
                case "||" -> TokenType.OR;
                case "#>" -> TokenType.HASH_GREATER; // Relation literal start
                case "#{" -> TokenType.HASH_LBRACE; // graphFetch tree start
                case "}#" -> TokenType.RBRACE_HASH; // graphFetch tree end
                case "::" -> TokenType.DOUBLE_COLON; // Qualified name separator
                default -> null;
            };
            if (twoCharType != null) {
                position += 2;
                return new Token(twoCharType, twoChar, start);
            }
        }

        // Single-character operators and delimiters
        TokenType singleCharType = switch (c) {
            case '.' -> TokenType.DOT;
            case '|' -> TokenType.PIPE;
            case '$' -> TokenType.DOLLAR;
            case '<' -> TokenType.LESS_THAN;
            case '>' -> TokenType.GREATER_THAN;
            case '!' -> TokenType.NOT;
            case '(' -> TokenType.LPAREN;
            case ')' -> TokenType.RPAREN;
            case '[' -> TokenType.LBRACKET;
            case ']' -> TokenType.RBRACKET;
            case '{' -> TokenType.LBRACE;
            case '}' -> TokenType.RBRACE;
            case ',' -> TokenType.COMMA;
            case '~' -> TokenType.TILDE; // Column reference
            case ':' -> TokenType.COLON; // Extend expression separator
            case '^' -> TokenType.CARET; // Instance construction
            case '=' -> TokenType.ASSIGN; // Property assignment
            default -> null;
        };
        if (singleCharType != null) {
            position++;
            return new Token(singleCharType, String.valueOf(c), start);
        }

        // String literal
        if (c == '\'') {
            return readStringLiteral();
        }

        // Number literal
        if (Character.isDigit(c)
                || (c == '-' && position + 1 < input.length() && Character.isDigit(input.charAt(position + 1)))) {
            return readNumberLiteral();
        }

        // Identifier or keyword
        if (Character.isLetter(c) || c == '_') {
            return readIdentifierOrKeyword();
        }

        throw new PureParseException("Unexpected character: '" + c + "' at position " + position);
    }

    private Token readStringLiteral() {
        int start = position;
        position++; // skip opening quote

        StringBuilder sb = new StringBuilder();
        while (position < input.length() && input.charAt(position) != '\'') {
            char c = input.charAt(position);
            if (c == '\\' && position + 1 < input.length()) {
                position++;
                c = input.charAt(position);
                sb.append(switch (c) {
                    case 'n' -> '\n';
                    case 't' -> '\t';
                    case '\'' -> '\'';
                    case '\\' -> '\\';
                    default -> c;
                });
            } else {
                sb.append(c);
            }
            position++;
        }

        if (position >= input.length()) {
            throw new PureParseException("Unterminated string literal at position " + start);
        }

        position++; // skip closing quote
        return new Token(TokenType.STRING_LITERAL, sb.toString(), start);
    }

    private Token readNumberLiteral() {
        int start = position;
        StringBuilder sb = new StringBuilder();

        if (input.charAt(position) == '-') {
            sb.append('-');
            position++;
        }

        boolean hasDecimal = false;
        while (position < input.length()) {
            char c = input.charAt(position);
            if (Character.isDigit(c)) {
                sb.append(c);
                position++;
            } else if (c == '.' && !hasDecimal) {
                hasDecimal = true;
                sb.append(c);
                position++;
            } else {
                break;
            }
        }

        return new Token(
                hasDecimal ? TokenType.FLOAT_LITERAL : TokenType.INTEGER_LITERAL,
                sb.toString(),
                start);
    }

    private Token readIdentifierOrKeyword() {
        int start = position;
        StringBuilder sb = new StringBuilder();

        while (position < input.length()) {
            char c = input.charAt(position);
            if (Character.isLetterOrDigit(c) || c == '_') {
                sb.append(c);
                position++;
            } else {
                break;
            }
        }

        String value = sb.toString();

        // Check for keywords
        return switch (value) {
            case "true" -> new Token(TokenType.TRUE, value, start);
            case "false" -> new Token(TokenType.FALSE, value, start);
            default -> new Token(TokenType.IDENTIFIER, value, start);
        };
    }
}
