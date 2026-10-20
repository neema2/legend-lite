// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlSyntaxException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * SAP IQ 16.2 SQL text to tokens (the research's GRAMMAR L1-L31, docs/IQ_SQL_STUDY_2026_10_13.md §5.2), read under a
 * profile's options: whitespace is the six ASCII spaces; {@code --} and {@code //} comment to the end of the line and
 * {@code /* ... * /} comments nest (one left open runs to the end); an identifier is plain, {@code [bracketed]},
 * {@code `back-quoted`} or, with {@code quoted_identifier} On, {@code "double-quoted"}, and none has a doubling escape;
 * with {@code quoted_identifier} Off a double-quoted text is a string, {@code ""} standing for one {@code "}; a string
 * is single-quoted, {@code ''} standing for one {@code '}, and with {@code escape_character} On {@code \n}, {@code \\}
 * and {@code \xHH} are escapes in it (any other backslash is itself; a backslash never escapes the quote); a number is
 * digits with an optional point and exponent; operators are IQ's ({@code != !< !> *= =* ||} and the rest). A token
 * ends where the next character cannot extend it, so {@code 1abc} is the number 1 then the name {@code abc}, as in IQ.
 *
 * <p>A word is a keyword when IQ reserves it ({@link #RESERVED}): SQL Anywhere 17's reserved words less the ten it
 * reads as names in every select position. Forms IQ reads that this reader does not yet are refused here by name:
 * money ({@code $5}) and binary ({@code 0x1F}) literals, {@code N'...'} strings, an exponent with no digits, and, in an
 * ISO-8859-1 database's profile, a character that charset has not.
 */
public final class IqLexer {

    /** What a token is. */
    public enum Kind {
        /** A plain identifier that is no reserved word. */
        WORD,
        /** A reserved word (its text as written). */
        KEYWORD,
        /** A delimited identifier: {@code [x]}, {@code `x`}, or {@code "x"} under {@code quoted_identifier} On. */
        QUOTED,
        /** A string: its value, quoting and escapes undone. */
        STRING,
        /** A number as written. */
        NUMBER,
        /** An operator or punctuation, as written. */
        OP,
        /** The end of the text. */
        EOF
    }

    /**
     * A token: its kind, its text (a string's or a delimited identifier's value; anything else as written), where it
     * starts (line and column), and the characters of the source it spans ({@code start} to {@code end}).
     */
    public record Token(Kind kind, String text, SqlAst.Pos pos, int start, int end) {

        /** Whether this is the reserved word {@code word} (in any case). */
        public boolean is(String word) {
            return kind == Kind.KEYWORD && text.equalsIgnoreCase(word);
        }

        /** Whether this is the plain word {@code word} (in any case), reserved or not. */
        public boolean isWord(String word) {
            return (kind == Kind.KEYWORD || kind == Kind.WORD) && text.equalsIgnoreCase(word);
        }

        /** Whether this is the operator {@code op}. */
        public boolean isOp(String op) {
            return kind == Kind.OP && text.equals(op);
        }

        /** Whether this names something: a plain identifier or a delimited one. */
        public boolean isName() {
            return kind == Kind.WORD || kind == Kind.QUOTED;
        }
    }

    /** IQ's reserved words, lower case: SQL Anywhere 17's (which IQ's front end is), less the context words it reads as
     *  names (dynamic, inout, insensitive, limit, no, out, scroll, sensitive, timestamp, tran), the research's
     *  reserved-word table. IQ's own four (aes_decrypt, clear, iq, timeline) are names to SQL Anywhere, and here. */
    static final Set<String> RESERVED = Set.of("add", "all", "alter", "and", "any", "array", "as", "asc", "attach",
            "backup", "begin", "between", "bigint", "binary", "bit", "bottom", "break", "by", "call", "capability",
            "cascade", "case", "cast", "char", "char_convert", "character", "check", "checkpoint", "close", "comment",
            "commit", "compressed", "conflict", "connect", "constraint", "contains", "continue", "convert", "create",
            "cross", "cube", "current", "current_timestamp", "current_user", "cursor", "date", "datetimeoffset",
            "datetimex", "dbspace", "deallocate", "dec", "decimal", "declare", "default", "delete", "deleting", "desc",
            "detach", "distinct", "do", "double", "drop", "else", "elseif", "encrypted", "end", "endif", "escape",
            "except", "exception", "exec", "execute", "executing", "executing_user", "existing", "exists",
            "externlogin", "extract", "fetch", "first", "float", "for", "force", "foreign", "forward", "from", "full",
            "goto", "grant", "group", "having", "holdlock", "identified", "if", "in", "index", "inner", "insert",
            "inserting", "install", "instead", "int", "integer", "integrated", "intersect", "into", "invoking",
            "invoking_user", "is", "isolation", "join", "json", "kerberos", "key", "lateral", "left", "like", "lock",
            "login", "long", "match", "membership", "merge", "message", "mode", "modify", "natural", "nchar", "new",
            "noholdlock", "not", "notify", "null", "numeric", "nvarchar", "of", "off", "on", "open", "openstring",
            "openxml", "option", "options", "or", "order", "others", "outer", "over", "passthrough", "pivot",
            "precision", "prepare", "primary", "print", "privileges", "proc", "procedure", "procedure_owner",
            "publication", "raiserror", "readtext", "real", "reference", "references", "refresh", "release", "remote",
            "remove", "rename", "reorganize", "resource", "restore", "restrict", "return", "revoke", "right",
            "rollback", "rollup", "row", "rowtype", "save", "savepoint", "select", "session", "session_user", "set",
            "setuser", "share", "smallint", "some", "spatial", "sqlcode", "sqlstate", "start", "stop", "subtrans",
            "subtransaction", "synchronize", "table", "temporary", "then", "time", "tinyint", "to", "top", "treat",
            "trigger", "truncate", "tsequal", "unbounded", "union", "unique", "uniqueidentifier", "unknown", "unnest",
            "unpivot", "unsigned", "update", "updating", "user", "using", "validate", "values", "varbinary", "varbit",
            "varchar", "variable", "varray", "varying", "view", "wait", "waitfor", "when", "where", "while", "window",
            "with", "within", "work", "writetext", "xml");

    /** IQ island, 2026-10-17 (the corpus lane's SEDE: {@code AS Timeline}; measured on IQ 17.1, its
     *  sa_reserved_words() against SQL Anywhere 17's): the words IQ reserves beyond SQL Anywhere's that it reads as no
     *  name anywhere -- an alias, a table's alias, a column read (-131). Its other extra words (true, false,
     *  current_database_schema as an alias only; dynamic, limit, no, out, timestamp ... nowhere) are names here. The
     *  words of an IQ profile of IQ 17.1's UTF-8 database (the iq17 presets); IQ 16's, whose stand-in is SQL Anywhere
     *  17, and SQL Anywhere's are SQL Anywhere's. */
    static final Set<String> IQ_RESERVED = Set.of("aes_decrypt", "clear", "iq", "timeline");

    /** IQ island, 2026-10-17 (the corpus lane's {@code AS row}; measured on IQ 17.1): words SQL Anywhere 17 reserves
     *  that IQ 17.1 reads as names everywhere -- an alias with AS or without, a column read alone or after a dot, a
     *  table's alias -- though its sa_reserved_words() lists them (array, json, row, rowtype, unnest and varray of its
     *  sixteen alias-able words; the other ten are names to SQL Anywhere too). The iq17 presets' names; IQ 16's
     *  stand-in, SQL Anywhere 17, reserves them. */
    static final Set<String> IQ_NAMES = Set.of("array", "json", "row", "rowtype", "unnest", "varray");

    /** IQ island, 2026-10-17 (BIRD's THEN TRUE ELSE FALSE; measured on IQ 17.1): TRUE and FALSE, IQ 17.1's
     *  constants 1 and 0 (SMALLINTs, as the literals are; TRUE + 1 is 2), no name; in IQ 16's stand-in, SQL Anywhere
     *  17, names. */
    static final Set<String> IQ_CONSTANTS = Set.of("true", "false");

    /** IQ's two- and one-character operators, longest first where one begins another. */
    private static final List<String> OPERATORS = List.of("<>", "<=", ">=", "!=", "!<", "!>", "*=", "=*", "||", "=",
            "<", ">", "+", "-", "*", "/", "%", "&", "|", "^", "~", "(", ")", ",", ".", ";", "?", ":", "{", "}");

    private final String text;
    private final IqSettings settings;
    private int at;
    private int line = 1;
    private int column = 1;

    private IqLexer(String text, IqSettings settings) {
        this.text = text;
        this.settings = settings;
    }

    /** Every token of {@code text} under {@code settings}'s options, ending with one {@link Kind#EOF}. */
    public static List<Token> tokenize(String text, IqSettings settings) {
        return new IqLexer(text, settings).all();
    }

    /** Whether IQ reserves {@code word} (in any case). */
    public static boolean reserved(String word) {
        return RESERVED.contains(lower(word));
    }

    private List<Token> all() {
        List<Token> out = new ArrayList<>();
        while (true) {
            skipSpaceAndComments();
            SqlAst.Pos pos = new SqlAst.Pos(line, column);
            int start = at;
            if (at >= text.length()) {
                out.add(new Token(Kind.EOF, "", pos, start, start));
                return out;
            }
            out.add(next(pos, start));
        }
    }

    private Token next(SqlAst.Pos pos, int start) {
        char c = text.charAt(at);
        if (c == '\'') {
            return new Token(Kind.STRING, string(pos), pos, start, at);
        }
        if (c == '"') {
            if (settings.quotedIdentifier()) {
                return new Token(Kind.QUOTED, delimited('"', pos), pos, start, at);
            }
            return new Token(Kind.STRING, doubleQuotedString(pos), pos, start, at);
        }
        if (c == '[' && peek(1) == '[' || c == ']' && peek(1) == ']') {
            // an array subscript's brackets (the research's GRAMMAR E9): no delimited identifier holds a bracket
            advance(2);
            return new Token(Kind.OP, text.substring(start, at), pos, start, at);
        }
        if (c == '[') {
            return new Token(Kind.QUOTED, delimited(']', pos), pos, start, at);
        }
        if (c == '`') {
            return new Token(Kind.QUOTED, delimited('`', pos), pos, start, at);
        }
        if (isDigit(c) || c == '.' && isDigit(peek(1))) {
            return number(pos, start);
        }
        if (c == '$' && (isDigit(peek(1)) || peek(1) == '.' || (peek(1) == '+' || peek(1) == '-') && isDigit(peek(2)))) {
            throw SqlSyntaxException.unsupported("money literals ($5) are not supported yet", pos);
        }
        if (c == 'N' && peek(1) == '\'') {
            // IQ island, 2026-10-15 (the corpus lane: 120 of SEDE's queries; measured on IQ 17.1): a national string,
            // N right before the quote (n'...' and N '...' are not one), is a string of the database's own charset,
            // UTF-8 (IQ's engine types N'abc' a VARCHAR(3), its value 'abc')
            advance(1);
            return new Token(Kind.STRING, string(pos), pos, start, at);
        }
        if (isIdentifierStart(c)) {
            while (at < text.length() && isIdentifierPart(text.charAt(at))) {
                advance(1);
            }
            String word = text.substring(start, at);
            boolean iq17 = settings.iq17();
            String lowered = lower(word);
            boolean keyword = reserved(word) && !(iq17 && IQ_NAMES.contains(lowered))
                    || iq17 && (IQ_RESERVED.contains(lowered) || IQ_CONSTANTS.contains(lowered));
            return new Token(keyword ? Kind.KEYWORD : Kind.WORD, word, pos, start, at);
        }
        for (String op : OPERATORS) {
            if (text.startsWith(op, at)) {
                advance(op.length());
                return new Token(Kind.OP, op, pos, start, at);
            }
        }
        throw new SqlSyntaxException("the character '" + c + "' (U+" + hex4(c) + ") is not IQ SQL here"
                + IqDialect.syntaxCode(), pos);
    }

    /** Spaces and comments: the six ASCII spaces (a no-break space is none, in IQ), {@code --} and {@code //} to the end
     *  of the line, {@code /* ... * /} nesting, an unclosed one running to the end of the text. */
    private void skipSpaceAndComments() {
        while (at < text.length()) {
            char c = text.charAt(at);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\u000B') {
                advance(1);
            } else if (c == '-' && peek(1) == '-' || c == '/' && peek(1) == '/') {
                while (at < text.length() && text.charAt(at) != '\n' && text.charAt(at) != '\r') {
                    advance(1);
                }
            } else if (c == '/' && peek(1) == '*') {
                int depth = 0;
                while (at < text.length()) {
                    if (text.startsWith("/*", at)) {
                        depth++;
                        advance(2);
                    } else if (text.startsWith("*/", at)) {
                        depth--;
                        advance(2);
                        if (depth == 0) {
                            break;
                        }
                    } else {
                        advance(1);
                    }
                }
            } else {
                return;
            }
        }
    }

    /** A {@code '...'} string's value: {@code ''} is one quote; with {@code escape_character} On, {@code \n} a line
     *  feed, {@code \\} a backslash and {@code \xHH} (or {@code \XHH}) that character, any other backslash itself. */
    private String string(SqlAst.Pos pos) {
        advance(1);
        StringBuilder value = new StringBuilder();
        while (true) {
            if (at >= text.length()) {
                throw new SqlSyntaxException("unterminated string" + IqDialect.syntaxCode(), pos);
            }
            char c = text.charAt(at);
            if (c == '\'') {
                if (peek(1) == '\'') {
                    value.append('\'');
                    advance(2);
                    continue;
                }
                advance(1);
                return charset(value.toString(), pos);
            }
            if (c == '\\' && settings.escapeCharacter()) {
                char e = peek(1);
                if (e == 'n') {
                    value.append('\n');
                    advance(2);
                    continue;
                }
                if (e == '\\') {
                    value.append('\\');
                    advance(2);
                    continue;
                }
                if ((e == 'x' || e == 'X') && isHex(peek(2)) && isHex(peek(3))) {
                    int code = Integer.parseInt(text.substring(at + 2, at + 4), 16);
                    if (code == 0) {
                        throw SqlSyntaxException.unsupported("a NUL character (\\x00) in a string is not supported: not"
                                + " every database lite runs on can hold one", pos);
                    }
                    if (code > 0x7F && !settings.latin1()) {
                        // a UTF-8 database stores the escape's byte as it is, which is no character of its own
                        throw SqlSyntaxException.unsupported("the escape \\x" + text.substring(at + 2, at + 4)
                                + " of a byte above 0x7F is not supported in profile " + settings.spelling()
                                + "'s UTF-8 database", pos);
                    }
                    value.append((char) code);
                    advance(4);
                    continue;
                }
            }
            value.append(c);
            advance(1);
        }
    }

    /** A {@code "..."} string under {@code quoted_identifier} Off: {@code ""} is one {@code "}, no other escape. */
    private String doubleQuotedString(SqlAst.Pos pos) {
        advance(1);
        StringBuilder value = new StringBuilder();
        while (true) {
            if (at >= text.length()) {
                throw new SqlSyntaxException("unterminated string" + IqDialect.syntaxCode(), pos);
            }
            char c = text.charAt(at);
            if (c == '"') {
                if (peek(1) == '"') {
                    value.append('"');
                    advance(2);
                    continue;
                }
                advance(1);
                return charset(value.toString(), pos);
            }
            value.append(c);
            advance(1);
        }
    }

    /** A delimited identifier's name, up to {@code close}: no doubling escape, none of the delimiters, a backslash or a
     *  control character inside, and never empty (each a syntax error in IQ). */
    private String delimited(char close, SqlAst.Pos pos) {
        advance(1);
        int from = at;
        while (at < text.length() && text.charAt(at) != close) {
            char c = text.charAt(at);
            if (c == '"' || c == '[' || c == ']' || c == '`' || c == '\\' || c < 0x20) {
                throw new SqlSyntaxException("the character U+" + hex4(c) + " cannot stand in a delimited identifier"
                        + IqDialect.syntaxCode(), pos);
            }
            advance(1);
        }
        if (at >= text.length()) {
            throw new SqlSyntaxException("unterminated delimited identifier" + IqDialect.syntaxCode(), pos);
        }
        String name = text.substring(from, at);
        advance(1);
        if (name.isEmpty()) {
            throw new SqlSyntaxException("an empty delimited identifier" + IqDialect.syntaxCode(), pos);
        }
        if (close == '"' && peek(0) == '"') {
            throw new SqlSyntaxException("a delimited identifier has no doubled-quote escape" + IqDialect.syntaxCode(),
                    pos);
        }
        return charset(name, pos);
    }

    /** {@code 42}, {@code 4.5}, {@code .5}, {@code 5.}, {@code 1e10}, {@code 1.5E-3}; a hexadecimal binary literal
     *  ({@code 0x...}) is refused by name, and so is an exponent with no digits (IQ reads {@code 1e} as 1). */
    private Token number(SqlAst.Pos pos, int start) {
        if (text.charAt(at) == '0' && (peek(1) == 'x' || peek(1) == 'X')) {
            throw SqlSyntaxException.unsupported("binary literals (0x...) are not supported yet", pos);
        }
        while (isDigit(peek(0))) {
            advance(1);
        }
        if (peek(0) == '.') {
            advance(1);
            while (isDigit(peek(0))) {
                advance(1);
            }
        }
        if (peek(0) == 'e' || peek(0) == 'E') {
            int sign = peek(1) == '+' || peek(1) == '-' ? 1 : 0;
            if (!isDigit(peek(1 + sign))) {
                throw SqlSyntaxException.unsupported("an exponent with no digits (" + text.substring(start, at + 1)
                        + ") is not supported", pos);
            }
            advance(1 + sign);
            while (isDigit(peek(0))) {
                advance(1);
            }
        }
        return new Token(Kind.NUMBER, text.substring(start, at), pos, start, at);
    }

    /** {@code value}, refused by name when the profile's database cannot hold one of its characters (IQ's ISO-8859-1
     *  turns any other into something else). */
    private String charset(String value, SqlAst.Pos pos) {
        if (settings.latin1()) {
            for (int i = 0; i < value.length(); i++) {
                if (value.charAt(i) > 0xFF) {
                    throw SqlSyntaxException.unsupported("the character U+" + hex4(value.charAt(i)) + " is not in"
                            + " ISO-8859-1, the charset of profile " + settings.spelling() + "'s database", pos);
                }
            }
        }
        return value;
    }

    private boolean isIdentifierStart(char c) {
        return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c == '@' || c == '#' || c == '$'
                || c > 0x7F && Character.isLetter(c) && (c <= 0xFF || !settings.latin1());
    }

    private boolean isIdentifierPart(char c) {
        return isIdentifierStart(c) || isDigit(c);
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isHex(char c) {
        return isDigit(c) || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F';
    }

    private char peek(int ahead) {
        return at + ahead < text.length() ? text.charAt(at + ahead) : '\0';
    }

    private void advance(int n) {
        for (int i = 0; i < n; i++) {
            if (text.charAt(at) == '\n') {
                line++;
                column = 1;
            } else {
                column++;
            }
            at++;
        }
    }

    private static String hex4(char c) {
        String h = Integer.toHexString(c).toUpperCase(java.util.Locale.ROOT);
        return "0000".substring(h.length()) + h;
    }

    /** ASCII lower case: IQ's keywords and identifiers fold A-Z only. */
    static String lower(String s) {
        char[] chars = s.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] >= 'A' && chars[i] <= 'Z') {
                chars[i] = (char) (chars[i] + ('a' - 'A'));
            }
        }
        return new String(chars);
    }
}
