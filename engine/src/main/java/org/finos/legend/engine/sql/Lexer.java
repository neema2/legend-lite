package org.finos.legend.engine.sql;

/**
 * SQL Lexer with SavePoint backtracking, inspired by Alibaba Druid.
 * 
 * Features:
 * - SavePoint for backtracking: mark(), reset()
 * - FNV-1a hash for O(1) keyword lookup
 * - PostgreSQL dialect support (quoted identifiers, :: cast)
 * - Legend-Engine extensions (SERVICE, TABLE, CLASS)
 */
public final class Lexer {
    
    private final String text;
    private int pos;
    private char ch;
    
    // Current token state
    private Token token;
    private String stringVal;
    private long hash;
    private int tokenPos;
    
    public Lexer(String sql) {
        this.text = sql;
        this.pos = 0;
        this.ch = pos < text.length() ? text.charAt(pos) : '\0';
        nextToken(); // Prime the lexer
    }

    // ==================== SavePoint for Backtracking ====================

    public record SavePoint(int pos, Token token, String stringVal, long hash, int tokenPos) {}

    public SavePoint mark() {
        return new SavePoint(pos, token, stringVal, hash, tokenPos);
    }

    public void reset(SavePoint sp) {
        this.pos = sp.pos;
        this.token = sp.token;
        this.stringVal = sp.stringVal;
        this.hash = sp.hash;
        this.tokenPos = sp.tokenPos;
        this.ch = pos < text.length() ? text.charAt(pos) : '\0';
    }

    // ==================== Token Access ====================

    public Token token() {
        return token;
    }

    public String stringVal() {
        return stringVal;
    }

    public long hash() {
        return hash;
    }

    public int tokenPos() {
        return tokenPos;
    }

    public String info() {
        return "pos " + tokenPos + ": " + token + (stringVal != null ? "(" + stringVal + ")" : "");
    }

    // ==================== Scanning ====================

    public void nextToken() {
        skipWhitespaceAndComments();
        
        tokenPos = pos;
        stringVal = null;
        hash = 0;

        if (ch == '\0') {
            token = Token.EOF;
            return;
        }

        // Identifier or keyword
        if (isIdentifierStart(ch)) {
            scanIdentifier();
            return;
        }

        // Quoted identifier: "name"
        if (ch == '"') {
            scanQuotedIdentifier();
            return;
        }

        // String literal: 'value'
        if (ch == '\'') {
            scanString();
            return;
        }

        // Number
        if (isDigit(ch)) {
            scanNumber();
            return;
        }

        // Operators and punctuation
        scanOperator();
    }

    private void scanIdentifier() {
        int start = pos;
        long h = Token.FNV_OFFSET;
        
        while (isIdentifierPart(ch)) {
            char c = ch;
            if (c >= 'A' && c <= 'Z') {
                c = (char) (c + 32); // lowercase for hash
            }
            h ^= c;
            h *= Token.FNV_PRIME;
            advance();
        }
        
        stringVal = text.substring(start, pos);
        hash = h;
        
        // Check if it's a keyword
        Token kw = Token.keyword(h);
        token = (kw != null) ? kw : Token.IDENTIFIER;
    }

    private void scanQuotedIdentifier() {
        advance(); // skip opening "
        int start = pos;
        
        while (ch != '\0' && ch != '"') {
            if (ch == '"' && peek() == '"') {
                advance(); // escaped quote
            }
            advance();
        }
        
        stringVal = text.substring(start, pos);
        if (ch == '"') advance(); // skip closing "
        token = Token.QUOTED_IDENTIFIER;
    }

    private void scanString() {
        advance(); // skip opening '
        StringBuilder sb = new StringBuilder();
        
        while (ch != '\0') {
            if (ch == '\'') {
                if (peek() == '\'') {
                    sb.append('\'');
                    advance();
                    advance();
                } else {
                    break;
                }
            } else {
                sb.append(ch);
                advance();
            }
        }
        
        if (ch == '\'') advance(); // skip closing '
        stringVal = sb.toString();
        token = Token.STRING;
    }

    private void scanNumber() {
        int start = pos;
        boolean isDecimal = false;
        
        while (isDigit(ch)) advance();
        
        if (ch == '.' && isDigit(peek())) {
            isDecimal = true;
            advance(); // .
            while (isDigit(ch)) advance();
        }
        
        // Scientific notation: 1e10, 1E-5
        if (ch == 'e' || ch == 'E') {
            isDecimal = true;
            advance();
            if (ch == '+' || ch == '-') advance();
            while (isDigit(ch)) advance();
        }
        
        stringVal = text.substring(start, pos);
        token = isDecimal ? Token.DECIMAL : Token.INTEGER;
    }

    private void scanOperator() {
        switch (ch) {
            case '(' -> { advance(); token = Token.LPAREN; }
            case ')' -> { advance(); token = Token.RPAREN; }
            case '[' -> { advance(); token = Token.LBRACKET; }
            case ']' -> { advance(); token = Token.RBRACKET; }
            case ',' -> { advance(); token = Token.COMMA; }
            case ';' -> { advance(); token = Token.SEMICOLON; }
            case '.' -> { advance(); token = Token.DOT; }
            case '+' -> { advance(); token = Token.PLUS; }
            case '-' -> {
                if (peek() == '-') {
                    skipLineComment();
                    nextToken();
                } else {
                    advance();
                    token = Token.MINUS;
                }
            }
            case '*' -> { advance(); token = Token.STAR; }
            case '/' -> {
                if (peek() == '*') {
                    skipBlockComment();
                    nextToken();
                } else {
                    advance();
                    token = Token.SLASH;
                }
            }
            case '%' -> { advance(); token = Token.PERCENT; }
            case '=' -> { advance(); token = Token.EQ; }
            case '<' -> {
                advance();
                if (ch == '=') { advance(); token = Token.LE; }
                else if (ch == '>') { advance(); token = Token.NE; }
                else { token = Token.LT; }
            }
            case '>' -> {
                advance();
                if (ch == '=') { advance(); token = Token.GE; }
                else { token = Token.GT; }
            }
            case '!' -> {
                advance();
                if (ch == '=') { advance(); token = Token.NE; }
                else throw new SQLParseException("Expected = after !", pos);
            }
            case '|' -> {
                advance();
                if (ch == '|') { advance(); token = Token.CONCAT; }
                else throw new SQLParseException("Expected | after |", pos);
            }
            case ':' -> {
                advance();
                if (ch == ':') { advance(); token = Token.DOUBLE_COLON; }
                else { token = Token.COLON; }
            }
            default -> throw new SQLParseException("Unexpected character: " + ch, pos);
        }
    }

    // ==================== Helpers ====================

    private void advance() {
        pos++;
        ch = pos < text.length() ? text.charAt(pos) : '\0';
    }

    private char peek() {
        return pos + 1 < text.length() ? text.charAt(pos + 1) : '\0';
    }

    private void skipWhitespaceAndComments() {
        while (true) {
            while (Character.isWhitespace(ch)) advance();
            
            if (ch == '-' && peek() == '-') {
                skipLineComment();
            } else if (ch == '/' && peek() == '*') {
                skipBlockComment();
            } else {
                break;
            }
        }
    }

    private void skipLineComment() {
        while (ch != '\0' && ch != '\n') advance();
        if (ch == '\n') advance();
    }

    private void skipBlockComment() {
        advance(); // /
        advance(); // *
        while (ch != '\0') {
            if (ch == '*' && peek() == '/') {
                advance();
                advance();
                break;
            }
            advance();
        }
    }

    private static boolean isIdentifierStart(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
    }

    private static boolean isIdentifierPart(char c) {
        return isIdentifierStart(c) || (c >= '0' && c <= '9');
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }
}
