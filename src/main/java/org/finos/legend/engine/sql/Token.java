package org.finos.legend.engine.sql;

/**
 * SQL Token types with pre-computed hash codes for O(1) lookup.
 * Inspired by Alibaba Druid's FnvHash approach.
 */
public enum Token {
    // Literals
    EOF,
    IDENTIFIER,
    QUOTED_IDENTIFIER,  // "identifier"
    STRING,             // 'string'
    INTEGER,            // 123
    DECIMAL,            // 45.67

    // Keywords - SELECT
    SELECT, DISTINCT, ALL, AS,
    
    // Keywords - FROM/JOIN
    FROM, JOIN, INNER, LEFT, RIGHT, FULL, OUTER, CROSS, ON, USING,
    
    // Keywords - WHERE
    WHERE, AND, OR, NOT, IN, BETWEEN, LIKE, ILIKE, IS, NULL, TRUE, FALSE,
    EXISTS, CASE, WHEN, THEN, ELSE, END,
    
    // Keywords - GROUP/ORDER
    GROUP, BY, HAVING, ORDER, ASC, DESC, NULLS, FIRST, LAST,
    
    // Keywords - LIMIT
    LIMIT, OFFSET, FETCH, NEXT, ROWS, ONLY, ROW,
    
    // Keywords - SET operations
    UNION, INTERSECT, EXCEPT,
    
    // Keywords - Window functions
    OVER, PARTITION, RANGE, UNBOUNDED, PRECEDING, FOLLOWING, CURRENT,
    
    // Keywords - CAST
    CAST,
    
    // Legend-Engine extensions
    SERVICE, TABLE, CLASS,

    // Operators - Comparison
    EQ,         // =
    NE,         // <> or !=
    LT,         // <
    LE,         // <=
    GT,         // >
    GE,         // >=

    // Operators - Arithmetic
    PLUS,       // +
    MINUS,      // -
    STAR,       // *
    SLASH,      // /
    PERCENT,    // %

    // Operators - Other
    CONCAT,     // ||
    DOUBLE_COLON, // :: (Postgres cast)
    DOT,        // .
    COMMA,      // ,
    SEMICOLON,  // ;
    COLON,      // :
    
    // Brackets
    LPAREN,     // (
    RPAREN,     // )
    LBRACKET,   // [
    RBRACKET,   // ]
    ;

    /**
     * FNV-1a 64-bit hash constant (prime).
     */
    public static final long FNV_PRIME = 0x100000001b3L;
    public static final long FNV_OFFSET = 0xcbf29ce484222325L;

    /**
     * Pre-computed hash code for this token (lowercase).
     */
    private final long hash;

    Token() {
        this.hash = fnv1a64(this.name().toLowerCase());
    }

    public long hash() {
        return hash;
    }

    /**
     * FNV-1a 64-bit hash (case-insensitive via lowercase).
     */
    public static long fnv1a64(String s) {
        long h = FNV_OFFSET;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                c = (char) (c + 32); // lowercase
            }
            h ^= c;
            h *= FNV_PRIME;
        }
        return h;
    }

    /**
     * Lookup keyword by hash. Returns null if not a keyword.
     */
    public static Token keyword(long hash) {
        // Binary search or switch on popular hashes
        for (Token t : KEYWORDS) {
            if (t.hash == hash) return t;
        }
        return null;
    }

    /**
     * Keywords that can be looked up by hash.
     */
    private static final Token[] KEYWORDS = {
        SELECT, DISTINCT, ALL, AS,
        FROM, JOIN, INNER, LEFT, RIGHT, FULL, OUTER, CROSS, ON, USING,
        WHERE, AND, OR, NOT, IN, BETWEEN, LIKE, ILIKE, IS, NULL, TRUE, FALSE,
        EXISTS, CASE, WHEN, THEN, ELSE, END,
        GROUP, BY, HAVING, ORDER, ASC, DESC, NULLS, FIRST, LAST,
        LIMIT, OFFSET, FETCH, NEXT, ROWS, ONLY, ROW,
        UNION, INTERSECT, EXCEPT,
        OVER, PARTITION, RANGE, UNBOUNDED, PRECEDING, FOLLOWING, CURRENT,
        CAST,
        SERVICE, TABLE, CLASS
    };

    public boolean isKeyword() {
        return ordinal() >= SELECT.ordinal() && ordinal() <= CLASS.ordinal();
    }

    public boolean isComparisonOp() {
        return this == EQ || this == NE || this == LT || this == LE || this == GT || this == GE;
    }
}
