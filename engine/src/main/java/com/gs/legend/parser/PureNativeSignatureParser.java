package com.gs.legend.parser;

import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.compiler.PType;
import com.gs.legend.model.m3.Multiplicity;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Hand-rolled parser for Pure native function signatures.
 *
 * <p>Parses strings like:
 * <pre>
 *   native function filter&lt;T&gt;(rel:Relation&lt;T&gt;[1], f:Function&lt;{T[1]-&gt;Boolean[1]}&gt;[1]):Relation&lt;T&gt;[1];
 *   native function sort&lt;T|m&gt;(col:T[m]):T[m];
 *   native function rename&lt;T,Z,K,V&gt;(r:Relation&lt;T&gt;[1], old:ColSpec&lt;Z=(?:K)⊆T&gt;[1], new:ColSpec&lt;V=(?:K)&gt;[1]):Relation&lt;T-Z+V&gt;[1];
 * </pre>
 *
 * <p>Tokens come from {@link PureLexer2}; production is fully structural
 * (no text round-trips through the model layer).
 *
 * <p>Supports:
 * <ul>
 *   <li>Type parameters {@code <T,V,K>} and multiplicity parameters {@code <T|m>}</li>
 *   <li>Nested generics {@code Relation<T>}, {@code Function<{T[1]->Boolean[1]}>}</li>
 *   <li>Schema algebra {@code T-Z+V}, {@code Z⊆T}, {@code Z=K⊆T}</li>
 *   <li>Inline relation types {@code (col:Integer[1], name:String[1])}</li>
 *   <li>Multiplicity variables {@code [m]}, {@code [n]}</li>
 * </ul>
 */
public final class PureNativeSignatureParser {

    private final String source;
    private final PureLexer2 lexer;
    private final int tokenCount;
    private int pos;
    private Set<String> typeParams = Set.of();

    /**
     * Parses a Pure native function signature string into a {@link NativeFunctionDef}.
     */
    public static NativeFunctionDef parse(String signature) {
        return new PureNativeSignatureParser(signature).parseTopLevel();
    }

    private PureNativeSignatureParser(String source) {
        this.source = source;
        this.lexer = new PureLexer2(source);
        this.lexer.tokenize();
        this.tokenCount = lexer.tokenCount();
        this.pos = 0;
    }

    // ==================== Top-level ====================

    private NativeFunctionDef parseTopLevel() {
        expect(TokenType.NATIVE);
        expect(TokenType.FUNCTION);
        String qualifiedName = parseQualifiedName();
        String name = qualifiedName.contains("::")
                ? qualifiedName.substring(qualifiedName.lastIndexOf("::") + 2)
                : qualifiedName;

        List<String> typeParamList = new ArrayList<>();
        List<String> multParamList = new ArrayList<>();
        if (check(TokenType.LESS_THAN)) {
            parseTypeAndMultParams(typeParamList, multParamList);
        }
        typeParams = new HashSet<>(typeParamList);

        expect(TokenType.PAREN_OPEN);
        List<PType.Param> params = new ArrayList<>();
        if (!check(TokenType.PAREN_CLOSE)) {
            params.add(parseParam());
            while (match(TokenType.COMMA)) {
                params.add(parseParam());
            }
        }
        expect(TokenType.PAREN_CLOSE);

        expect(TokenType.COLON);
        PType returnType = parsePType();
        Multiplicity returnMult = parseMultiplicity();

        // Optional trailing ';' (Pure grammar requires it)
        match(TokenType.SEMI_COLON);

        return new NativeFunctionDef(
                name, typeParamList, multParamList, params, returnType, returnMult, source);
    }

    /** Parses {@code <T>}, {@code <T,V>}, {@code <T|m>}, {@code <T,V|m,n>}. */
    private void parseTypeAndMultParams(List<String> typeParamList, List<String> multParamList) {
        expect(TokenType.LESS_THAN);
        // Type parameters, until PIPE or GREATER_THAN
        if (!check(TokenType.PIPE) && !check(TokenType.GREATER_THAN)) {
            typeParamList.add(parseIdentifier());
            while (match(TokenType.COMMA)) {
                typeParamList.add(parseIdentifier());
            }
        }
        // Optional multiplicity parameters after PIPE
        if (match(TokenType.PIPE)) {
            multParamList.add(parseIdentifier());
            while (match(TokenType.COMMA)) {
                multParamList.add(parseIdentifier());
            }
        }
        expect(TokenType.GREATER_THAN);
    }

    private PType.Param parseParam() {
        String name = parseIdentifier();
        expect(TokenType.COLON);
        PType type = parsePType();
        Multiplicity mult = parseMultiplicity();
        return new PType.Param(name, type, mult);
    }

    // ==================== Types ====================

    /** Parses a single PType (no enclosing multiplicity). */
    private PType parsePType() {
        // Function type: {T[1]->Boolean[1]} or {T[1],V[1]->Any[*]}
        if (check(TokenType.BRACE_OPEN)) {
            return parseFunctionType();
        }
        // Inline relation type: (col:Type[mult], ...)
        if (check(TokenType.PAREN_OPEN)) {
            return parseRelationType();
        }
        // Named type (possibly generic): Name or Name<...>
        String typeName = parseTypeName();
        if (match(TokenType.LESS_THAN)) {
            List<PType> typeArgs = new ArrayList<>();
            typeArgs.add(parseTypeWithOperation());
            while (match(TokenType.COMMA)) {
                typeArgs.add(parseTypeWithOperation());
            }
            expect(TokenType.GREATER_THAN);
            String simpleName = simpleName(typeName);
            return new PType.Parameterized(simpleName, typeArgs);
        }
        // No type args — either a type variable or a concrete named type
        String simpleName = simpleName(typeName);
        if (typeParams.contains(simpleName)) {
            return new PType.TypeVar(simpleName);
        }
        return new PType.Concrete(typeName);
    }

    /** {@code {T[1],V[1]->R[1]}} — one or more param types, then {@code ->}, then return. */
    private PType.FunctionType parseFunctionType() {
        expect(TokenType.BRACE_OPEN);
        List<PType.Param> params = new ArrayList<>();
        if (!check(TokenType.ARROW)) {
            // At least one param type
            PType firstType = parsePType();
            Multiplicity firstMult = parseMultiplicity();
            params.add(new PType.Param("", firstType, firstMult));
            while (match(TokenType.COMMA)) {
                PType t = parsePType();
                Multiplicity m = parseMultiplicity();
                params.add(new PType.Param("", t, m));
            }
        }
        expect(TokenType.ARROW);
        PType retType = parsePType();
        Multiplicity retMult = parseMultiplicity();
        expect(TokenType.BRACE_CLOSE);
        return new PType.FunctionType(params, retType, retMult);
    }

    /** {@code (col:Type[mult], ...)} — inline RelationType columns. */
    private PType.RelationTypeVar parseRelationType() {
        expect(TokenType.PAREN_OPEN);
        List<PType.RelationTypeVar.Column> columns = new ArrayList<>();
        if (!check(TokenType.PAREN_CLOSE)) {
            columns.add(parseRelationColumn());
            while (match(TokenType.COMMA)) {
                columns.add(parseRelationColumn());
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new PType.RelationTypeVar(columns);
    }

    private PType.RelationTypeVar.Column parseRelationColumn() {
        // mayColumnName: QUESTION | columnName
        String colName = match(TokenType.QUESTION) ? "?" : parseIdentifier();
        expect(TokenType.COLON);
        // mayColumnType: QUESTION | type
        PType colType = match(TokenType.QUESTION) ? new PType.Concrete("Any") : parsePType();
        Multiplicity mult = check(TokenType.BRACKET_OPEN) ? parseMultiplicity() : Multiplicity.ONE;
        return new PType.RelationTypeVar.Column(colName, colType, mult);
    }

    /**
     * Parses a single type argument, possibly wrapped in schema-algebra operators:
     * {@code T}, {@code T-Z+V}, {@code Z⊆T}, {@code Z=K⊆T}, {@code Z=(?:K)⊆T}.
     */
    private PType parseTypeWithOperation() {
        PType result = parsePType();

        // Equal: T=K  (matches legend-pure's equalType)
        if (match(TokenType.EQUAL)) {
            PType eqType = parsePType();
            result = new PType.SchemaAlgebra(result, eqType, PType.SchemaAlgebra.OpType.Equal);
        }

        // Add/subtract chain: T-Z+V
        while (check(TokenType.PLUS) || check(TokenType.MINUS)) {
            PType.SchemaAlgebra.OpType op =
                    match(TokenType.PLUS)
                            ? PType.SchemaAlgebra.OpType.Union
                            : consumeAs(TokenType.MINUS, PType.SchemaAlgebra.OpType.Difference);
            PType right = parsePType();
            result = new PType.SchemaAlgebra(result, right, op);
        }

        // Subset: Z⊆T
        if (match(TokenType.SUBSET)) {
            PType superSet = parsePType();
            result = new PType.SchemaAlgebra(result, superSet, PType.SchemaAlgebra.OpType.Subset);
        }

        return result;
    }

    /**
     * Parses a type name (qualified or simple).
     * Keywords like {@code function} and certain pseudo-types ({@code _Window}, {@code _Traversal})
     * count as identifiers here.
     */
    private String parseTypeName() {
        StringBuilder sb = new StringBuilder(parseIdentifier());
        while (match(TokenType.PATH_SEPARATOR)) {
            sb.append("::").append(parseIdentifier());
        }
        return sb.toString();
    }

    // ==================== Multiplicity ====================

    /**
     * Parses a multiplicity at the token level, supporting the {@code [m]} variable
     * form used by native signatures. Mirrors {@code PureQueryParser.parseMultiplicity}.
     */
    private Multiplicity parseMultiplicity() {
        expect(TokenType.BRACKET_OPEN);
        Multiplicity result;
        if (match(TokenType.STAR)) {
            result = Multiplicity.MANY;
        } else if (check(TokenType.INTEGER)) {
            int lower = Integer.parseInt(consume(TokenType.INTEGER));
            if (match(TokenType.DOT_DOT)) {
                Integer upper = match(TokenType.STAR)
                        ? null
                        : Integer.parseInt(consume(TokenType.INTEGER));
                result = new Multiplicity.Bounded(lower, upper);
            } else {
                result = new Multiplicity.Bounded(lower, lower);
            }
        } else {
            // Multiplicity variable: identifier like m, n
            String name = parseIdentifier();
            result = new Multiplicity.Var(name);
        }
        expect(TokenType.BRACKET_CLOSE);
        return result;
    }

    // ==================== Token plumbing ====================

    private String parseIdentifier() {
        TokenType t = peek();
        if (t == null) {
            error("Expected identifier, got EOF");
        }
        // Native signatures freely reuse keyword-ish tokens as param names or type-arg
        // names (e.g., {@code type:T[1]}, {@code value:...}, {@code function:...}). Accept
        // any token whose text is a plain alphanumeric identifier — lexer already coalesces
        // keywords, and PureLexer2 distinguishes operators from identifiers via token type,
        // so a simple text check is safe here.
        String text = text();
        if (text != null && !text.isEmpty() && isIdentifierText(text)) {
            advance();
            return text;
        }
        error("Expected identifier, got " + t + " '" + text() + "'");
        return null;
    }

    private static boolean isIdentifierText(String text) {
        char c0 = text.charAt(0);
        if (!Character.isLetter(c0) && c0 != '_') return false;
        for (int i = 1; i < text.length(); i++) {
            char c = text.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') return false;
        }
        return true;
    }

    private String parseQualifiedName() {
        StringBuilder sb = new StringBuilder(parseIdentifier());
        while (match(TokenType.PATH_SEPARATOR)) {
            sb.append("::").append(parseIdentifier());
        }
        return sb.toString();
    }

    private TokenType peek() {
        return pos < tokenCount ? lexer.tokenType(pos) : null;
    }

    private String text() {
        return pos < tokenCount ? lexer.tokenText(pos) : "";
    }

    private boolean check(TokenType t) {
        return peek() == t;
    }

    private boolean match(TokenType t) {
        if (check(t)) {
            advance();
            return true;
        }
        return false;
    }

    private void advance() {
        pos++;
    }

    private String consume(TokenType t) {
        if (!check(t)) error("Expected " + t + ", got " + peek() + " '" + text() + "'");
        String s = text();
        advance();
        return s;
    }

    private void expect(TokenType t) {
        consume(t);
    }

    /** Consume a token and return a fixed value (used for constant-returning branches). */
    private <T> T consumeAs(TokenType t, T value) {
        consume(t);
        return value;
    }

    private static String simpleName(String qualifiedName) {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }

    private void error(String message) {
        throw new PureParseException(
                "PureNativeSignatureParser: " + message + " (in: " + source + ")");
    }
}
