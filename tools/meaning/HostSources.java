// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * THE HOSTS' SOURCES, READ (census B2, 2026-11-19; the reusable piece the presentation program's G4 reads too): a
 * TypeScript or JavaScript file, or a Python file, as the tokens a meaning guard asks about -- each string literal (a
 * template's text between its substitutions; a Python docstring marked as one), each name in code (a property read
 * after {@code .} or {@code ?.} marked as one), each comment, each punctuation mark, each import (static, type-only,
 * dynamic or guarded, with the names it takes) and each top-level export -- every one with its line. Comments and
 * literals are never code, so prose about a shape is no read of it.
 *
 * <p>A lexer of its own, not the languages' parsers: the repository's TypeScript is 7.0.2, whose JavaScript API is an
 * unstable bridge to its Go binary (HostSqlTest's reason, 2026-10-13), and a Python parser would make the JVM guards
 * a Python program. What it cannot see, by construction: a name built at run time ({@code obj[`${a}Columns`]}), a
 * regular expression right after a closing bracket ({@code if (x) /re/}, read as a division), and a Python f-string's
 * nested quotes of its own kind (3.12's PEP 701), read as the string's end.
 */
public final class HostSources {

    private HostSources() {
    }

    /** The language a file is read as, by its extension. */
    public enum Lang {
        TYPESCRIPT, PYTHON;

        /** The language of {@code path}, or null for a file neither reader reads (a {@code .d.ts} declares only). */
        public static Lang of(String path) {
            String p = path.toLowerCase(Locale.ROOT);
            if (p.endsWith(".d.ts")) {
                return null;
            }
            if (p.endsWith(".ts") || p.endsWith(".mts") || p.endsWith(".cts") || p.endsWith(".tsx") || p.endsWith(".js")
                    || p.endsWith(".mjs") || p.endsWith(".cjs")) {
                return TYPESCRIPT;
            }
            return p.endsWith(".py") ? PYTHON : null;
        }
    }

    /** A token's kind. */
    public enum Kind {
        /** a string literal's text (a template's text between substitutions, a Python string's body) */
        LITERAL,
        /** a Python docstring, or any string expression standing alone as a statement: prose, never a value */
        DOCSTRING,
        /** a name in code */
        NAME,
        /** a name read as a property: after {@code .} or {@code ?.} */
        PROPERTY,
        /** a comment's text, its markers stripped */
        COMMENT,
        /** one punctuation character */
        PUNCT
    }

    /** One token: its kind, its text, its 1-based line. */
    public record Token(Kind kind, String text, int line) {

        boolean is(String s) {
            return text.equals(s) && (kind == Kind.NAME || kind == Kind.PUNCT);
        }
    }

    /** How an import binds its module. */
    public enum ImportKind {
        /** at load: {@code import x from 'm'}, {@code export ... from 'm'}, a top-level Python import */
        STATIC,
        /** a type alone ({@code import type ...}): erased, never loaded */
        TYPE_ONLY,
        /** when the code runs: {@code import('m')}, {@code require('m')}, a Python import inside a def */
        DYNAMIC,
        /** a Python import inside a top-level {@code try}: loaded when it can be */
        GUARDED
    }

    /** One import: the module as written (a Python relative one with its dots), the names it takes from the module
     *  ({@code default} for a default import, {@code *} for a namespace or star import, empty for a whole-module
     *  Python import or a side effect), its kind and its line. */
    public record Import(String module, List<String> names, ImportKind kind, int line) {
    }

    /** A file read: its path (as given), its language, its tokens in order, its imports and its top-level exports. */
    public record Source(String path, Lang lang, List<Token> tokens, List<Import> imports, List<Token> exports) {

        /** The tokens of {@code kind}. */
        public List<Token> of(Kind kind) {
            return tokens.stream().filter(t -> t.kind() == kind).toList();
        }

        /** The string literals (docstrings left out). */
        public List<Token> literals() {
            return of(Kind.LITERAL);
        }

        /** Every name in code: plain names and property reads. */
        public List<Token> names() {
            return tokens.stream().filter(t -> t.kind() == Kind.NAME || t.kind() == Kind.PROPERTY).toList();
        }
    }

    /** Reads {@code text} as the file {@code path}'s source, in the language its extension names. */
    public static Source read(String path, String text) {
        Lang lang = Lang.of(path);
        if (lang == null) {
            throw new IllegalArgumentException(path + ": neither TypeScript nor Python");
        }
        return lang == Lang.TYPESCRIPT ? typescript(path, text) : python(path, text);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // TypeScript and JavaScript

    /** Keywords after which a {@code /} begins a regular expression, not a division (HostSqlTest's). */
    private static final Set<String> BEFORE_EXPRESSION = Set.of("return", "typeof", "instanceof", "in", "of", "new",
            "delete", "void", "throw", "case", "do", "else", "yield", "await");

    private static Source typescript(String path, String src) {
        List<Token> tokens = new ArrayList<>();
        Deque<Integer> substitutions = new ArrayDeque<>();
        int depth = 0;
        int line = 1;
        boolean expressionMayBegin = true;
        boolean afterDot = false;
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (c == '\n') {
                line++;
                i++;
            } else if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '/' && i + 1 < n && src.charAt(i + 1) == '/') {
                int start = i + 2;
                while (i < n && src.charAt(i) != '\n') {
                    i++;
                }
                tokens.add(new Token(Kind.COMMENT, src.substring(start, i).strip(), line));
            } else if (c == '/' && i + 1 < n && src.charAt(i + 1) == '*') {
                int end = src.indexOf("*/", i + 2);
                end = end < 0 ? n : end + 2;
                tokens.add(new Token(Kind.COMMENT, src.substring(i + 2, Math.max(i + 2, end - 2)), line));
                line += count(src, i, end);
                i = end;
            } else if (c == '\'' || c == '"') {
                int start = i;
                i = skipQuoted(src, i, c);
                tokens.add(new Token(Kind.LITERAL, src.substring(start + 1, Math.max(start + 1, i - 1)), line));
                line += count(src, start, i);
                expressionMayBegin = false;
                afterDot = false;
            } else if (c == '`' || (c == '}' && !substitutions.isEmpty() && substitutions.peek() == depth)) {
                boolean head = c == '`';
                if (!head) {
                    substitutions.pop();
                }
                int start = ++i;
                while (i < n && src.charAt(i) != '`' && !(src.charAt(i) == '$' && i + 1 < n && src.charAt(i + 1) == '{')) {
                    i += src.charAt(i) == '\\' ? 2 : 1;
                }
                tokens.add(new Token(Kind.LITERAL, src.substring(start, Math.min(i, n)), line));
                line += count(src, start, Math.min(i, n));
                if (i < n && src.charAt(i) == '$') {
                    substitutions.push(depth);
                    i += 2;
                    expressionMayBegin = true;
                } else {
                    i++;
                    expressionMayBegin = false;
                }
                afterDot = false;
            } else if (c == '/' && expressionMayBegin) {
                i = skipRegex(src, i);
                expressionMayBegin = false;
                afterDot = false;
            } else if (Character.isJavaIdentifierStart(c)) {
                int start = i;
                while (i < n && Character.isJavaIdentifierPart(src.charAt(i))) {
                    i++;
                }
                String word = src.substring(start, i);
                tokens.add(new Token(afterDot ? Kind.PROPERTY : Kind.NAME, word, line));
                expressionMayBegin = !afterDot && BEFORE_EXPRESSION.contains(word);
                afterDot = false;
            } else if (Character.isDigit(c)) {
                while (i < n && (Character.isLetterOrDigit(src.charAt(i)) || src.charAt(i) == '.' || src.charAt(i) == '_')) {
                    i++;
                }
                expressionMayBegin = false;
                afterDot = false;
            } else {
                if (c == '{') {
                    depth++;
                } else if (c == '}') {
                    depth--;
                }
                tokens.add(new Token(Kind.PUNCT, String.valueOf(c), line));
                // `.name` and `?.name` read a property; `...name` spreads a value
                afterDot = c == '.' && !(i + 1 < n && src.charAt(i + 1) == '.') && !(i > 0 && src.charAt(i - 1) == '.');
                expressionMayBegin = c != ')' && c != ']' && c != '}' && !afterDot;
                i++;
            }
        }
        List<Import> imports = new ArrayList<>();
        List<Token> exports = new ArrayList<>();
        tsModuleShape(tokens.stream().filter(t -> t.kind() != Kind.COMMENT).toList(), imports, exports);
        return new Source(path, Lang.TYPESCRIPT, List.copyOf(tokens), List.copyOf(imports), List.copyOf(exports));
    }

    private static final Set<String> DECLARES = Set.of("function", "class", "const", "let", "var", "type", "interface",
            "enum", "namespace");

    /** The imports and top-level exports a TypeScript token stream declares. */
    private static void tsModuleShape(List<Token> code, List<Import> imports, List<Token> exports) {
        int depth = 0;
        for (int k = 0; k < code.size(); k++) {
            Token t = code.get(k);
            if (t.is("{")) {
                depth++;
            } else if (t.is("}")) {
                depth--;
            }
            if (t.kind() != Kind.NAME) {
                continue;
            }
            Token next = k + 1 < code.size() ? code.get(k + 1) : null;
            if ((t.text().equals("import") || t.text().equals("require")) && next != null && next.is("(")) {
                // import('m') / require('m'): when the code runs
                Token arg = k + 2 < code.size() ? code.get(k + 2) : null;
                if (arg != null && arg.kind() == Kind.LITERAL) {
                    imports.add(new Import(arg.text(), List.of("*"), ImportKind.DYNAMIC, t.line()));
                }
            } else if (t.text().equals("import") && next != null && next.kind() == Kind.LITERAL) {
                imports.add(new Import(next.text(), List.of(), ImportKind.STATIC, t.line()));
            } else if (t.text().equals("import") && depth == 0 && !(k > 0 && code.get(k - 1).is("."))) {
                importClause(code, k, false, imports, exports);
            } else if (t.text().equals("export") && depth == 0) {
                int j = k + 1;
                while (j < code.size() && Set.of("declare", "async", "abstract").contains(code.get(j).text())) {
                    j++;
                }
                if (j >= code.size()) {
                    continue;
                }
                Token d = code.get(j);
                if (d.is("default")) {
                    exports.add(new Token(Kind.NAME, "default", d.line()));
                    Token named = j + 2 < code.size() && DECLARES.contains(code.get(j + 1).text()) ? code.get(j + 2) : null;
                    if (named != null && named.kind() == Kind.NAME) {
                        exports.add(named);
                    }
                } else if (d.is("type") && j + 1 < code.size() && code.get(j + 1).is("{")) {
                    importClause(code, k, true, imports, exports);
                } else if (DECLARES.contains(d.text())) {
                    int name = j + 1;
                    if (name < code.size() && code.get(name).is("*")) {
                        name++;
                    }
                    if (name < code.size() && code.get(name).kind() == Kind.NAME) {
                        exports.add(code.get(name));
                    }
                } else if (d.is("{") || d.is("*") || d.is("type")) {
                    importClause(code, k, true, imports, exports);
                }
            }
        }
    }

    /** {@code import ... from 'm'} or {@code export {..} [from 'm']} starting at {@code k}: the names it takes (an
     *  import's imported names; an export's exported names), and the import, if it names a module. */
    private static void importClause(List<Token> code, int k, boolean export, List<Import> imports,
            List<Token> exports) {
        boolean typeOnly = k + 1 < code.size() && code.get(k + 1).is("type")
                && !(k + 2 < code.size() && (code.get(k + 2).is("from") || code.get(k + 2).is(",")));
        List<String> taken = new ArrayList<>();
        List<Token> exported = new ArrayList<>();
        int j = k + 1;
        boolean braces = false;
        for (; j < code.size(); j++) {
            Token x = code.get(j);
            if (x.is("from") || x.is(";") || x.kind() == Kind.LITERAL
                    || (j > k + 1 && x.kind() == Kind.NAME && (DECLARES.contains(x.text()) || x.is("import")
                            || x.is("export")))) {
                break;
            }
            if (x.is("{")) {
                braces = true;
            } else if (x.is("}")) {
                braces = false;
                if (export && !(j + 1 < code.size() && code.get(j + 1).is("from"))) {
                    j++;
                    break;
                }
            } else if (x.is("*")) {
                taken.add("*");
            } else if (x.kind() == Kind.NAME && !x.text().equals("type")) {
                boolean renamed = j + 2 < code.size() && code.get(j + 1).is("as");
                if (x.text().equals("as")) {
                    continue;
                }
                if (j > 0 && code.get(j - 1).is("as")) {
                    // the local or the exported name of `a as b`
                    if (export) {
                        exported.add(x);
                    }
                    continue;
                }
                taken.add(braces || export ? x.text() : "default");
                if (export && !renamed) {
                    exported.add(x);
                }
            } else if (x.kind() == Kind.NAME && x.text().equals("type") && export) {
                continue;
            }
        }
        boolean hasModule = j + 1 < code.size() && code.get(j).is("from") && code.get(j + 1).kind() == Kind.LITERAL;
        if (hasModule) {
            imports.add(new Import(code.get(j + 1).text(), List.copyOf(taken),
                    typeOnly ? ImportKind.TYPE_ONLY : ImportKind.STATIC, code.get(k).line()));
        }
        if (export) {
            exports.addAll(exported);
            if (taken.contains("*") && hasModule) {
                exports.add(new Token(Kind.NAME, "*", code.get(k).line()));
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Python

    private static Source python(String path, String src) {
        List<Token> tokens = new ArrayList<>();
        List<Import> imports = new ArrayList<>();
        List<Token> exports = new ArrayList<>();
        // {indent, kind}: 0 def/class, 1 try/except, 2 another block, 3 `if TYPE_CHECKING:` (its imports types alone)
        Deque<int[]> blocks = new ArrayDeque<>();
        List<Token> logical = new ArrayList<>();
        int line = 1;
        int i = 0;
        int n = src.length();
        int paren = 0;
        boolean lineStart = true;
        int indent = 0;
        int logicalIndent = 0;
        while (true) {
            char c = i < n ? src.charAt(i) : '\n';
            if (i >= n || (c == '\n' && paren == 0)) {
                if (!logical.isEmpty()) {
                    pythonStatement(logical, logicalIndent, blocks, imports, exports, tokens);
                }
                logical = new ArrayList<>();
                if (i >= n) {
                    break;
                }
                line++;
                i++;
                lineStart = true;
                indent = 0;
                continue;
            }
            if (c == '\n') {
                line++;
                i++;
                continue;
            }
            if (c == '\\' && i + 1 < n && src.charAt(i + 1) == '\n') {
                line++;
                i += 2;
                continue;
            }
            if (c == ' ' || c == '\t' || c == '\r' || c == '\f') {
                if (lineStart) {
                    indent += c == '\t' ? 8 : 1;
                }
                i++;
                continue;
            }
            if (c == '#') {
                int start = i + 1;
                while (i < n && src.charAt(i) != '\n') {
                    i++;
                }
                tokens.add(new Token(Kind.COMMENT, src.substring(start, i).strip(), line));
                continue;
            }
            if (lineStart) {
                logicalIndent = indent;
                lineStart = false;
            }
            int prefixEnd = i;
            while (prefixEnd < n && prefixEnd - i < 2 && "rRbBuUfF".indexOf(src.charAt(prefixEnd)) >= 0) {
                prefixEnd++;
            }
            if (prefixEnd < n && (src.charAt(prefixEnd) == '\'' || src.charAt(prefixEnd) == '"')
                    && (i == 0 || !Character.isJavaIdentifierPart(src.charAt(i - 1)))) {
                String prefix = src.substring(i, prefixEnd).toLowerCase(Locale.ROOT);
                char q = src.charAt(prefixEnd);
                String quote3 = String.valueOf(q).repeat(3);
                boolean triple = src.startsWith(quote3, prefixEnd);
                int bodyStart = prefixEnd + (triple ? 3 : 1);
                int j = bodyStart;
                while (j < n) {
                    char d = src.charAt(j);
                    if (d == '\\') {
                        j += 2;
                        continue;
                    }
                    if (triple ? src.startsWith(quote3, j) : (d == q || d == '\n')) {
                        break;
                    }
                    j++;
                }
                int bodyEnd = Math.min(j, n);
                String body = src.substring(bodyStart, bodyEnd);
                int startLine = line;
                line += count(src, i, bodyEnd);
                i = Math.min(n, bodyEnd + (triple ? 3 : 1));
                if (prefix.contains("f")) {
                    fString(body, startLine, logical);
                } else {
                    logical.add(new Token(Kind.LITERAL, body, startLine));
                }
                continue;
            }
            if (Character.isJavaIdentifierStart(c)) {
                int start = i;
                while (i < n && Character.isJavaIdentifierPart(src.charAt(i))) {
                    i++;
                }
                boolean property = precededByDot(src, start) && !logical.isEmpty()
                        && !logical.get(0).is("from") && !logical.get(0).is("import");
                logical.add(new Token(property ? Kind.PROPERTY : Kind.NAME, src.substring(start, i), line));
                continue;
            }
            if (Character.isDigit(c)) {
                while (i < n && (Character.isLetterOrDigit(src.charAt(i)) || src.charAt(i) == '.' || src.charAt(i) == '_')) {
                    i++;
                }
                continue;
            }
            if (c == '(' || c == '[' || c == '{') {
                paren++;
            } else if (c == ')' || c == ']' || c == '}') {
                paren = Math.max(0, paren - 1);
            }
            logical.add(new Token(Kind.PUNCT, String.valueOf(c), line));
            i++;
        }
        tokens.sort(Comparator.comparingInt(Token::line));
        return new Source(path, Lang.PYTHON, List.copyOf(tokens), List.copyOf(imports), List.copyOf(exports));
    }

    private static boolean precededByDot(String src, int at) {
        int k = at - 1;
        while (k >= 0 && (src.charAt(k) == ' ' || src.charAt(k) == '\t')) {
            k--;
        }
        return k >= 0 && src.charAt(k) == '.' && !(k > 0 && Character.isDigit(src.charAt(k - 1)));
    }

    /** An f-string's body: its text parts literals, its {expressions}' names code. */
    private static void fString(String body, int line, List<Token> logical) {
        StringBuilder text = new StringBuilder();
        StringBuilder expr = new StringBuilder();
        int depth = 0;
        int ln = line;
        for (int k = 0; k < body.length(); k++) {
            char d = body.charAt(k);
            if (depth == 0 && (d == '{' || d == '}') && k + 1 < body.length() && body.charAt(k + 1) == d) {
                text.append(d);
                k++;
                continue;
            }
            if (d == '{') {
                if (depth == 0) {
                    logical.add(new Token(Kind.LITERAL, text.toString(), ln));
                    text.setLength(0);
                    expr.setLength(0);
                } else {
                    expr.append(d);
                }
                depth++;
            } else if (d == '}' && depth > 0) {
                depth--;
                if (depth == 0) {
                    String[] words = expr.toString().split("[^A-Za-z0-9_.]+");
                    for (String w : words) {
                        String[] parts = w.split("\\.");
                        for (int p = 0; p < parts.length; p++) {
                            if (!parts[p].isEmpty() && Character.isJavaIdentifierStart(parts[p].charAt(0))) {
                                logical.add(new Token(p == 0 ? Kind.NAME : Kind.PROPERTY, parts[p], ln));
                            }
                        }
                    }
                } else {
                    expr.append(d);
                }
            } else if (depth > 0) {
                expr.append(d);
            } else {
                text.append(d);
            }
            if (d == '\n') {
                ln++;
            }
        }
        logical.add(new Token(Kind.LITERAL, text.toString(), ln));
    }

    private static final Set<String> NOT_ASSIGNED = Set.of("if", "else", "elif", "for", "while", "with", "try",
            "except", "finally", "return", "global", "nonlocal", "assert", "raise", "del", "pass", "print", "yield");

    /** One logical Python line: its tokens recorded (a statement of strings alone as prose), its import or its
     *  top-level definition read. */
    private static void pythonStatement(List<Token> logical, int indent, Deque<int[]> blocks, List<Import> imports,
            List<Token> exports, List<Token> tokens) {
        while (!blocks.isEmpty() && indent <= blocks.peek()[0]) {
            blocks.pop();
        }
        boolean inDef = blocks.stream().anyMatch(b -> b[0] < indent && b[1] == 0);
        boolean inTry = blocks.stream().anyMatch(b -> b[0] < indent && b[1] == 1);
        boolean prose = logical.stream().allMatch(t -> t.kind() == Kind.LITERAL);
        for (Token t : logical) {
            tokens.add(prose ? new Token(Kind.DOCSTRING, t.text(), t.line()) : t);
        }
        Token first = logical.get(0);
        boolean opensBlock = logical.stream().anyMatch(t -> t.is(":"))
                && logical.get(logical.size() - 1).is(":");
        if (first.is("def") || first.is("class") || (first.is("async") && logical.size() > 1 && logical.get(1).is("def"))
                || first.is("@")) {
            int name = first.is("async") ? 2 : 1;
            if (indent == 0 && !first.is("@") && name < logical.size()) {
                exports.add(new Token(Kind.NAME, logical.get(name).text(), first.line()));
            }
            if (!first.is("@")) {
                blocks.push(new int[] {indent, 0});
            }
        } else if ((first.is("try") || first.is("except")) && opensBlock) {
            blocks.push(new int[] {indent, 1});
        } else if (first.is("if") && logical.size() > 1 && logical.get(1).text().endsWith("TYPE_CHECKING") && opensBlock) {
            blocks.push(new int[] {indent, 3});
        } else if (opensBlock) {
            blocks.push(new int[] {indent, 2});
        }
        // the statement after a block opener on the same line (`try: import x`) is read past its colon
        int at = 0;
        boolean typing = blocks.stream().anyMatch(b -> b[0] < indent && b[1] == 3);
        ImportKind kind = inDef ? ImportKind.DYNAMIC : inTry ? ImportKind.GUARDED
                : typing ? ImportKind.TYPE_ONLY : ImportKind.STATIC;
        for (int k = 0; k + 1 < logical.size(); k++) {
            if (logical.get(k).is(":") && (logical.get(k + 1).is("import") || logical.get(k + 1).is("from"))) {
                at = k + 1;
                if (first.is("try") || first.is("except")) {
                    kind = inDef ? ImportKind.DYNAMIC : ImportKind.GUARDED;
                } else if (first.is("def")) {
                    kind = ImportKind.DYNAMIC;
                }
            }
        }
        Token head = logical.get(at);
        if (head.is("import")) {
            StringBuilder module = new StringBuilder();
            for (int k = at + 1; k <= logical.size(); k++) {
                Token t = k < logical.size() ? logical.get(k) : null;
                if (t == null || t.is(",") || t.is("as") || t.is(";")) {
                    if (module.length() > 0) {
                        imports.add(new Import(module.toString(), List.of(), kind, head.line()));
                        module.setLength(0);
                    }
                    if (t != null && t.is("as")) {
                        k++;
                    }
                    if (t == null || t.is(";")) {
                        break;
                    }
                } else if (t.is(".")) {
                    module.append('.');
                } else if (t.kind() == Kind.NAME || t.kind() == Kind.PROPERTY) {
                    module.append(t.text());
                }
            }
        } else if (head.is("from") && at + 1 < logical.size()) {
            StringBuilder module = new StringBuilder();
            int k = at + 1;
            while (k < logical.size() && !logical.get(k).is("import")) {
                Token t = logical.get(k);
                module.append(t.is(".") ? "." : t.text());
                k++;
            }
            List<String> names = new ArrayList<>();
            for (k = k + 1; k < logical.size(); k++) {
                Token t = logical.get(k);
                if (t.is("as")) {
                    k++;
                } else if (t.is("*")) {
                    names.add("*");
                } else if (t.kind() == Kind.NAME) {
                    names.add(t.text());
                } else if (t.is(";")) {
                    break;
                }
            }
            imports.add(new Import(module.toString(), List.copyOf(names), kind, head.line()));
        } else if (indent == 0 && first.kind() == Kind.NAME && logical.size() > 1 && !NOT_ASSIGNED.contains(first.text())
                && (logical.get(1).is("=") || logical.get(1).is(":")) && !(logical.size() > 2 && logical.get(2).is("="))) {
            exports.add(first);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static int count(String src, int from, int to) {
        int k = 0;
        for (int i = from; i < to && i < src.length(); i++) {
            if (src.charAt(i) == '\n') {
                k++;
            }
        }
        return k;
    }

    private static int skipQuoted(String src, int at, char quote) {
        int i = at + 1;
        while (i < src.length() && src.charAt(i) != quote && src.charAt(i) != '\n') {
            i += src.charAt(i) == '\\' ? 2 : 1;
        }
        return Math.min(i + 1, src.length());
    }

    private static int skipRegex(String src, int at) {
        int i = at + 1;
        boolean inClass = false;
        while (i < src.length() && src.charAt(i) != '\n') {
            char c = src.charAt(i);
            if (c == '\\') {
                i += 2;
                continue;
            }
            if (c == '[') {
                inClass = true;
            } else if (c == ']') {
                inClass = false;
            } else if (c == '/' && !inClass) {
                i++;
                break;
            }
            i++;
        }
        while (i < src.length() && Character.isLetter(src.charAt(i))) {
            i++;
        }
        return i;
    }
}
