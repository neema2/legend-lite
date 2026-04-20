package com.gs.legend.parser;

import com.gs.legend.compiler.BuiltinRegistry;
import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.model.m3.LClass;
import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.Type;

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
        List<Type.Parameter> params = new ArrayList<>();
        if (!check(TokenType.PAREN_CLOSE)) {
            params.add(parseParam());
            while (match(TokenType.COMMA)) {
                params.add(parseParam());
            }
        }
        expect(TokenType.PAREN_CLOSE);

        expect(TokenType.COLON);
        Type returnType = parsePType();
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

    private Type.Parameter parseParam() {
        String name = parseIdentifier();
        expect(TokenType.COLON);
        Type type = parsePType();
        Multiplicity mult = parseMultiplicity();
        return new Type.Parameter(name, type, mult);
    }

    // ==================== Types ====================

    /** Parses a single Type (no enclosing multiplicity). */
    private Type parsePType() {
        // Function type: {T[1]->Boolean[1]} or {T[1],V[1]->Any[*]}
        if (check(TokenType.BRACE_OPEN)) {
            return parseFunctionType();
        }
        // Inline relation type: (col:Type[mult], ...)
        if (check(TokenType.PAREN_OPEN)) {
            return parseRelationType();
        }
        // Named type (possibly generic): FQN or FQN<...>. Native signatures must use
        // fully-qualified names for every named type except bare type-parameter letters
        // declared in the enclosing <...> header (T, V, K, ...). The former "unqualified
        // residual accepted as a signature-layer pseudo-type" fallback is gone — every
        // structural class (Relation, Function, ColSpec, _Window, _Range, _Traversal, ...)
        // is now a real entry in BuiltinClassRegistry and must be referenced by FQN.
        String typeName = parseTypeName();
        if (match(TokenType.LESS_THAN)) {
            List<Type> typeArgs = new ArrayList<>();
            typeArgs.add(parseTypeWithOperation());
            while (match(TokenType.COMMA)) {
                typeArgs.add(parseTypeWithOperation());
            }
            expect(TokenType.GREATER_THAN);
            // Catalog class reference: emit GenericType(LClass.XYZ, typeArgs). The platform class
            // enum ({@link LClass}) carries the FQN identity; downstream dispatch pattern-matches
            // against specific variants (LClass.RELATION, LClass.COL_SPEC, etc.). findByFqn is
            // the sole membership check — O(1) HashMap lookup; no separate "contains" pre-check.
            LClass rawType = LClass.findByFqn(typeName).orElseThrow(() -> new PureParseException(
                    "PureNativeSignatureParser: '" + typeName + "' is not a platform class in "
                            + "BuiltinClassRegistry. Native signatures must reference catalog "
                            + "classes (Relation, Function, ColSpec, _Window, ...) by fully "
                            + "qualified name. Signature was: '" + source + "'."));
            return new Type.GenericType(rawType, typeArgs);
        }
        // No type args — one of:
        //   (a) bare type-variable letter declared in the <> header (T, V, K, ...)
        //   (b) full FQN of a primitive, platform enum, or catalog class
        String simpleName = simpleName(typeName);
        if (typeParams.contains(simpleName)) {
            return new Type.TypeVar(simpleName);
        }
        Primitive prim = Primitive.findByFqn(typeName).orElse(null);
        if (prim != null) {
            // Legend-lite convention: bare "Decimal" in a native signature means
            // DECIMAL(38, 18) — the widest SQL decimal compatible with DuckDB's INT128.
            // Legend-pure's sigs write bare Decimal and leave precision to the dialect;
            // legend-lite pins it at the Pure type layer so downstream type inference
            // sees Decimal(38,18) instead of unparameterized Decimal. Tests assert this.
            if (prim == Primitive.DECIMAL) return Type.DEFAULT_DECIMAL;
            return prim;
        }
        if (BuiltinRegistry.findPlatformEnum(typeName).isPresent()) {
            return new Type.EnumType(typeName);
        }
        // Zero-arg catalog class (_Range, Rows, _Traversal, UnboundedFrameValue, FrameValue, ...).
        // The LClass enum constant IS the Type — no GenericType wrapper needed when there are
        // no type arguments. Direct O(1) HashMap lookup; absent ⇒ unknown type (throw below).
        var lc = LClass.findByFqn(typeName);
        if (lc.isPresent()) return lc.get();
        throw new PureParseException(
                "PureNativeSignatureParser: unknown type name '" + typeName + "' in signature '"
                        + source + "'. Native signatures must reference primitives, platform enums, "
                        + "and platform classes by fully qualified name, or declared type parameters "
                        + "by bare letter.");
    }

    /**
     * {@code {T[1],V[1]->R[1]}} — one or more param types, then {@code ->}, then return.
     *
     * <p>Function-type literals have no parameter identifiers; all params are
     * constructed with {@code ""} as the name, matching legend-pure's convention
     * (see {@link Type.Parameter} javadoc).
     */
    private Type.FunctionType parseFunctionType() {
        expect(TokenType.BRACE_OPEN);
        List<Type.Parameter> params = new ArrayList<>();
        if (!check(TokenType.ARROW)) {
            // At least one param type
            Type firstType = parsePType();
            Multiplicity firstMult = parseMultiplicity();
            params.add(new Type.Parameter("", firstType, firstMult));
            while (match(TokenType.COMMA)) {
                Type t = parsePType();
                Multiplicity m = parseMultiplicity();
                params.add(new Type.Parameter("", t, m));
            }
        }
        expect(TokenType.ARROW);
        Type retType = parsePType();
        Multiplicity retMult = parseMultiplicity();
        expect(TokenType.BRACE_CLOSE);
        return new Type.FunctionType(params, retType, retMult);
    }

    /** {@code (col:Type[mult], ...)} — inline RelationType columns. */
    private Type.RelationTypeVar parseRelationType() {
        expect(TokenType.PAREN_OPEN);
        List<Type.RelationTypeVar.Column> columns = new ArrayList<>();
        if (!check(TokenType.PAREN_CLOSE)) {
            columns.add(parseRelationColumn());
            while (match(TokenType.COMMA)) {
                columns.add(parseRelationColumn());
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new Type.RelationTypeVar(columns);
    }

    private Type.RelationTypeVar.Column parseRelationColumn() {
        // mayColumnName: QUESTION | columnName
        String colName = match(TokenType.QUESTION) ? "?" : parseIdentifier();
        expect(TokenType.COLON);
        // mayColumnType: QUESTION | type
        Type colType = match(TokenType.QUESTION) ? Primitive.ANY : parsePType();
        Multiplicity mult = check(TokenType.BRACKET_OPEN) ? parseMultiplicity() : Multiplicity.ONE;
        return new Type.RelationTypeVar.Column(colName, colType, mult);
    }

    /**
     * Parses a single type argument, possibly wrapped in schema-algebra operators:
     * {@code T}, {@code T-Z+V}, {@code Z⊆T}, {@code Z=K⊆T}, {@code Z=(?:K)⊆T}.
     */
    private Type parseTypeWithOperation() {
        Type result = parsePType();

        // Equal: T=K  (matches legend-pure's equalType)
        if (match(TokenType.EQUAL)) {
            Type eqType = parsePType();
            result = new Type.SchemaAlgebra(result, Type.SchemaAlgebra.Op.EQUAL, eqType);
        }

        // Add/subtract chain: T-Z+V
        while (check(TokenType.PLUS) || check(TokenType.MINUS)) {
            Type.SchemaAlgebra.Op op =
                    match(TokenType.PLUS)
                            ? Type.SchemaAlgebra.Op.UNION
                            : consumeAs(TokenType.MINUS, Type.SchemaAlgebra.Op.DIFFERENCE);
            Type right = parsePType();
            result = new Type.SchemaAlgebra(result, op, right);
        }

        // Subset: Z⊆T
        if (match(TokenType.SUBSET)) {
            Type superSet = parsePType();
            result = new Type.SchemaAlgebra(result, Type.SchemaAlgebra.Op.SUBSET, superSet);
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
