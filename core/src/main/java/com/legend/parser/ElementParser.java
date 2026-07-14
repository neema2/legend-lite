package com.legend.parser;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.parser.element.AssociationDefinition;
import com.legend.parser.element.AssociationDefinition.AssociationEndDefinition;
import com.legend.parser.element.AssociationMapping;
import com.legend.parser.element.AssociationPropertyMapping;
import com.legend.parser.element.AuthenticationSpec;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.ClassDefinition.ConstraintDefinition;
import com.legend.parser.element.ClassDefinition.DerivedPropertyDefinition;
import com.legend.parser.element.ClassDefinition.ParameterDefinition;
import com.legend.parser.element.ConnectionDefinition;
import com.legend.parser.element.ConnectionSpecification;
import com.legend.parser.element.DatabaseDefinition;
import com.legend.parser.element.EnumDefinition;
import com.legend.parser.element.EnumerationMapping;
import com.legend.parser.element.ClassMapping;
import com.legend.parser.element.FilterMapping;
import com.legend.parser.element.FilterPointer;
import com.legend.parser.element.FunctionDefinition;
import com.legend.parser.element.NativeFunctionDefinition;
import com.legend.parser.element.LegacyMappingDefinition;
import com.legend.parser.element.MappingDefinition;
import com.legend.parser.element.Realization;
import com.legend.parser.element.MappingInclude;
import com.legend.parser.element.PropertyMapping;
import com.legend.parser.spec.PackageableElementPtr;
import com.legend.parser.element.JsonModelConnection;
import com.legend.parser.element.PackageableElement;
import com.legend.parser.element.ComparisonOp;
import com.legend.parser.element.RelationalDataType;
import com.legend.parser.element.JoinChainElement;
import com.legend.parser.element.JoinType;
import com.legend.parser.element.LogicalOp;
import com.legend.parser.element.ProfileDefinition;
import com.legend.parser.element.RelationalOperation;
import com.legend.parser.element.RuntimeDefinition;
import com.legend.parser.element.ServiceDefinition;
import com.legend.parser.element.StereotypeApplication;
import com.legend.parser.element.TaggedValue;
import com.legend.parser.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hand-rolled recursive-descent parser for Pure top-level element
 * declarations &mdash; the driver for step B of the pipeline.
 *
 * <h2>Usage</h2>
 * <pre>
 *   ParsedModel model = ElementParser.parse(pureSource);    // text overload
 *   ParsedModel model = ElementParser.parse(tokenStream);   // pre-lexed overload
 * </pre>
 *
 * <h2>Status (Phase B.4a)</h2>
 * Handles:
 * <ul>
 *   <li>{@code import} statements (wildcard and specific).</li>
 *   <li>{@code Class} (regular and {@code native}) with stereotypes, tagged
 *       values, generic type parameters, superclasses, properties,
 *       derived properties (body parsed eagerly into {@link ValueSpecification}
 *       statements), and class-level constraints (expression parsed eagerly).</li>
 *   <li>{@code Association} &mdash; exactly two ends.</li>
 *   <li>{@code Enum} &mdash; one or more value names.</li>
 *   <li>{@code Profile} &mdash; stereotypes and tags.</li>
 *   <li>{@code function} &mdash; signature plus body parsed eagerly into a
 *       {@code List<ValueSpecification>} via {@link SpecParser#parseCodeBlock}.</li>
 *   <li>{@code Service} &mdash; pattern, doc, execution (query parsed eagerly as a
 *       single {@link ValueSpecification}; mapping/runtime as FQN refs),
 *       and {@code testSuites} block captured as raw text (decision D-3 &mdash;
 *       still deferred until the test-suite grammar lands).</li>
 *   <li>{@code Runtime} (and {@code SingleConnectionRuntime}) &mdash; mappings,
 *       connection bindings, embedded {@code JsonModelConnection} islands.</li>
 *   <li>{@code RelationalDatabaseConnection} &mdash; store, type, specification,
 *       authentication.</li>
 * </ul>
 *
 * <p>B.4a (this slice) adds {@code Database} with the full relational
 * expression sub-AST &mdash; column refs, target columns, literals,
 * function calls, comparisons, boolean combinations, null tests, groups,
 * array literals, and join-navigation chains. Used by {@code Join},
 * {@code Filter}, {@code MultiGrainFilter}, and view column expressions.
 *
 * <p>NOT yet supported (sub-slices B.4b-f):
 * <ul>
 *   <li>{@code Mapping} (next: B.4b Relational class mappings, then
 *       Association / Enum / M2M class mappings / test-suites).</li>
 * </ul>
 * Encountering an unsupported keyword raises a {@link ParseException}.
 *
 * <h2>Deliberate divergences from engine</h2>
 * <ul>
 *   <li>{@link FunctionDefinition} drops engine's compiler-cache fields
 *       ({@code resolvedBody}, {@code parsedReturnType}, parameter
 *       {@code functionType}/{@code parsedType}) &mdash; those bridge parser
 *       output and compiler output via {@code withResolvedBody}, which is
 *       the F-must-not-trigger-G violation AGENTS.md invariant 1 forbids.
 *       Compile-side output lives on {@code compiler.element.TypedFunction}
 *       (Phase F, future).</li>
 *   <li>{@link ServiceDefinition} drops engine's {@code toRegexPattern()},
 *       {@code extractPathParams()}, and convenience factories &mdash;
 *       those are REST-runtime concerns, not parser data.</li>
 *   <li><strong>Unknown keys inside {@code Runtime} / {@code Connection} /
 *       {@code Service} bodies throw</strong> (engine silently
 *       {@code skipToSemicolon}'s them). Matches AGENTS.md invariant 4
 *       (no fallbacks). May fire on forward-compat Pure syntax &mdash;
 *       tracked as decision D-2 in core's README.</li>
 * </ul>
 *
 * <h2>Stateful only during parse</h2>
 * A parser instance is allocated per call to {@link #parse(TokenStream)},
 * driven to completion, then discarded. The resulting {@link ParsedModel}
 * is the only object that survives.
 *
 * <p>Ported from engine's {@code com.gs.legend.parser.PureModelParser},
 * keeping the same recursive-descent shape and identifier-token set
 * for parity testing.
 */
public final class ElementParser implements TokenStreamCursor {

    // ============================================================
    // Instance state (transient during parse)
    // ============================================================

    private final TokenStream tokens;
    private int pos;

    /**
     * When non-null, parsing is inside a class mapping body and bare
     * identifiers in relational expressions resolve eagerly to columns of
     * this main table (matching FINOS engine's ScopeInfo behavior). When
     * null (Database context), bare identifiers throw per D-7. Set/cleared
     * around mapping-body parsing in {@link #parseRelationalClassMappingBody}.
     */
    private LegacyMappingDefinition.TableReference currentMappingScope;

    private ElementParser(TokenStream tokens) {
        this.tokens = tokens;
        this.pos = 0;
    }

    // ============================================================
    // Public API
    // ============================================================

    /**
     * Tokenize and parse a Pure source string into a {@link ParsedModel}.
     *
     * <p>Linear eager parse: every declared element is parsed in source
     * order. The result is the batch compiler's parser-stage output.
     *
     * <p>Demand-driven element loading (parse only the elements an
     * incremental client touches) is provided by the dormant IDE layer in
     * {@code com.legend.ide}; it is not used by the batch compiler.
     */
    public static ParsedModel parse(String source) {
        return parse(Lexer.tokenize(Objects.requireNonNull(source, "source")));
    }

    /** Parse a pre-lexed token stream into a {@link ParsedModel}. */
    public static ParsedModel parse(TokenStream tokens) {
        return new ElementParser(Objects.requireNonNull(tokens, "tokens")).parseModel();
    }

    /**
     * Parse exactly one packageable element from a token-stream slice.
     *
     * <p>The slice is expected to contain the element's full token range
     * (header + body); the first token must be the element's leading
     * keyword (or {@code native} for native classes). After parsing the
     * element the slice should be fully consumed &mdash; trailing tokens
     * cause a {@link ParseException}.
     *
     * <p>Exposed for the IDE layer ({@code com.legend.ide}), which uses it
     * to deep-parse a single element sliced out of a larger token stream
     * via the shallow indexer. Not used by the batch compiler.
     */
    public static PackageableElement parseSingle(TokenStream slice) {
        ElementParser parser = new ElementParser(slice);
        PackageableElement element = parser.parseSingleElement();
        if (!parser.atEnd()) {
            parser.error("trailing tokens after element body: expected slice to be fully consumed");
        }
        return element;
    }

    /** Parse one {@code import path::* ;} statement from a token-stream slice. */
    public static String parseSingleImport(TokenStream slice) {
        ElementParser parser = new ElementParser(slice);
        String imp = parser.parseImportStatement();
        if (!parser.atEnd()) {
            parser.error("trailing tokens after import statement");
        }
        return imp;
    }

    // ============================================================
    // Top-level
    // ============================================================

    private ParsedModel parseModel() {
        List<PackageableElement> elements = new ArrayList<>();
        ImportScope.Builder imports = new ImportScope.Builder();
        Map<String, Integer> offsets = new HashMap<>();
        // SECTION-scoped imports (real pure): an import following elements
        // opens a NEW section scope; each element records the scope active
        // where it was declared.
        Map<String, ImportScope> elementImports = new HashMap<>();
        ImportScope.Builder sectionImports = new ImportScope.Builder();
        boolean sawElementSinceImport = false;

        while (!atEnd()) {
            if (peek() == TokenType.IMPORT) {
                if (sawElementSinceImport) {
                    sectionImports = new ImportScope.Builder();
                    sawElementSinceImport = false;
                }
                String imp = parseImportStatement();
                imports.add(imp);
                sectionImports.add(imp);
            } else if (skipTopLevelNonElement()) {
                sawElementSinceImport = true;
            } else {
                int at = tokens.start(pos);
                PackageableElement e = parseSingleElement();
                sawElementSinceImport = true;
                elementImports.putIfAbsent(e.qualifiedName(), sectionImports.build());
                offsets.putIfAbsent(e.qualifiedName(), at);
                elements.add(e);
            }
        }

        return new ParsedModel(elements, imports.build(), tokens.source(), offsets, elementImports);
    }

    /**
     * Non-model top-level artifacts real pure files carry — {@code Diagram
     * fqn(w,h) { ... }} blocks and top-level {@code ^Instance(...)}
     * declarations. They define no queryable element; consumed and DROPPED
     * so the elements around them load (previously each sank its whole
     * file's parse). Returns false (nothing consumed) for anything else.
     */
    private boolean skipTopLevelNonElement() {
        if (isIdentifierToken(peek()) && "Diagram".equals(text())) {
            advance();
            parseQualifiedName();       // the diagram's name — anything else
                                        // after it is a parse error, never an
                                        // unbounded token skip (audit 8 S9)
            if (peek() == TokenType.PAREN_OPEN) {
                skipBalancedBlock();    // (width=..., height=...)
            }
            if (peek() == TokenType.BRACE_OPEN) {
                skipBalancedBlock();    // { TypeView ... }
            }
            return true;
        }
        if (peek() == TokenType.NEW_SYMBOL) {
            advance();
            parseQualifiedName();       // the instance's type reference
            if (peek() != TokenType.PAREN_OPEN) {
                throw error("top-level ^Instance must be followed by (...)");
            }
            skipBalancedBlock();
            return true;
        }
        return false;
    }

    /**
     * Dispatch on the current token (an element-starting keyword) and
     * parse exactly one packageable element. Used both by
     * {@link #parseModel} when iterating a full file and by
     * {@link #parseSingle} when the IDE layer's shallow scanner
     * ({@code com.legend.ide.ModelIndexer}) has sliced one element out of a
     * larger stream.
     */
    private PackageableElement parseSingleElement() {
        TokenType t = peek();
        return switch (t) {
            case CLASS -> parseClassDefinition(false);
            case NATIVE -> {
                advance(); // consume 'native'
                yield switch (peek()) {
                    case CLASS -> parseClassDefinition(true);
                    case FUNCTION -> parseNativeFunction();
                    default -> throw error("expected 'Class' or 'function' after 'native', got "
                            + peek() + " ('" + safeText() + "')");
                };
            }
            case ASSOCIATION -> parseAssociation();
            case ENUM -> parseEnumDefinition();
            case PROFILE -> parseProfile();
            case FUNCTION -> parseFunctionDefinition();
            case SERVICE -> parseServiceDefinition();
            case RUNTIME -> parseRuntime();
            case SINGLE_CONNECTION_RUNTIME -> parseSingleConnectionRuntime();
            case RELATIONAL_DATABASE_CONNECTION -> parseConnection();
            case DATABASE -> parseDatabase();
            case MAPPING -> parseMapping();
            // Primitive my::Ext extends Base [constraint]? — precise primitive
            case VALID_STRING -> {
                if ("Primitive".equals(safeText())) {
                    yield parsePrimitiveExtension();
                }
                throw error("unsupported top-level keyword: " + t + " ('" + safeText() + "')");
            }
            default -> throw error("unsupported top-level keyword: " + t + " ('" + safeText() + "')");
        };
    }

    // ============================================================
    // Import statement
    // ============================================================

    /** {@code import packagePath::* ;} or {@code import packagePath::Type ;} */
    private String parseImportStatement() {
        expect(TokenType.IMPORT);
        StringBuilder sb = new StringBuilder();
        sb.append(parseIdentifier());
        while (match(TokenType.PATH_SEPARATOR)) {
            sb.append("::");
            if (match(TokenType.STAR)) {
                sb.append("*");
                break;
            }
            sb.append(parseIdentifier());
        }
        expect(TokenType.SEMI_COLON);
        return sb.toString();
    }

    // ============================================================
    // Class declaration
    // ============================================================

    private ClassDefinition parseClassDefinition(boolean isNative) {
        expect(TokenType.CLASS);
        List<StereotypeApplication> stereotypes = parseStereotypes();
        List<TaggedValue> taggedValues = parseTaggedValues();
        String qualifiedName = parseQualifiedName();

        List<String> typeParams = parseClassTypeParams();

        List<TypeExpression> superClasses = new ArrayList<>();
        if (match(TokenType.EXTENDS)) {
            superClasses.add(parseType());
            while (match(TokenType.COMMA)) {
                superClasses.add(parseType());
            }
        }

        List<ConstraintDefinition> constraints = peek() == TokenType.BRACKET_OPEN
                ? parseConstraints()
                : List.of();

        expect(TokenType.BRACE_OPEN);

        List<ClassDefinition.PropertyDefinition> properties = new ArrayList<>();
        List<DerivedPropertyDefinition> derivedProperties = new ArrayList<>();
        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            if (isDerivedPropertyStart()) {
                derivedProperties.add(parseDerivedProperty());
            } else {
                properties.add(parseProperty());
            }
        }
        expect(TokenType.BRACE_CLOSE);

        return new ClassDefinition(
                qualifiedName,
                typeParams,
                superClasses,
                properties,
                derivedProperties,
                constraints,
                stereotypes,
                taggedValues,
                isNative);
    }

    /** Optional generic type parameters: {@code <T>}, {@code <U, V>}, ... */
    private List<String> parseClassTypeParams() {
        if (peek() != TokenType.LESS_THAN) return List.of();
        advance(); // consume <
        List<String> params = new ArrayList<>();
        params.add(parseIdentifier());
        while (match(TokenType.COMMA)) {
            params.add(parseIdentifier());
        }
        expect(TokenType.GREATER_THAN);
        return params;
    }

    /**
     * Property starts as {@code identifier (':' ...)}; derived property starts
     * as {@code identifier '(' ...}. Distinguish by lookahead at offset +1
     * across allowed stereotype/tag annotations.
     */
    private boolean isDerivedPropertyStart() {
        int saved = pos;
        // skip optional stereotypes <<...>>
        if (peek() == TokenType.LESS_THAN && peek(1) == TokenType.LESS_THAN) {
            int depth = 2;
            pos += 2;
            while (pos < tokens.count() && depth > 0) {
                if (peek() == TokenType.GREATER_THAN) depth--;
                else if (peek() == TokenType.LESS_THAN) depth++;
                pos++;
            }
        }
        // skip optional tagged values { ... }
        if (peek() == TokenType.BRACE_OPEN) {
            int depth = 1;
            pos++;
            while (pos < tokens.count() && depth > 0) {
                if (peek() == TokenType.BRACE_OPEN) depth++;
                else if (peek() == TokenType.BRACE_CLOSE) depth--;
                pos++;
            }
        }
        boolean derived = isIdentifierToken(peek()) && peek(1) == TokenType.PAREN_OPEN;
        pos = saved;
        return derived;
    }

    /**
     * Parse a derived (computed) property:
     * {@code <<stereos>> {tags} name(params) { body }: Type[mult];}.
     * The body between {@code &lcub;...&rcub;} is parsed eagerly via
     * {@link SpecParser#parseCodeBlock} and stored as a
     * {@code List<ValueSpecification>} on the resulting
     * {@link DerivedPropertyDefinition}.
     *
     * <p>NOTE: stereotypes and tagged values on derived properties are
     * parsed-and-discarded for engine parity (engine drops them too).
     * If we want to preserve them later, this is the place.
     */
    private DerivedPropertyDefinition parseDerivedProperty() {
        parseStereotypes();   // parity: engine consumes and drops
        parseTaggedValues();  // parity: engine consumes and drops
        String name = parseIdentifier();

        expect(TokenType.PAREN_OPEN);
        List<ParameterDefinition> params = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            params.add(parseDerivedPropertyParameter());
            while (match(TokenType.COMMA)) {
                params.add(parseDerivedPropertyParameter());
            }
        }
        expect(TokenType.PAREN_CLOSE);

        List<ValueSpecification> body = List.of();
        if (peek() == TokenType.BRACE_OPEN) {
            advance(); // consume {
            int bodyStart = pos;
            int depth = 1;
            while (!atEnd() && depth > 0) {
                TokenType t = peek();
                if (t == TokenType.BRACE_OPEN) depth++;
                else if (t == TokenType.BRACE_CLOSE) depth--;
                if (depth > 0) advance();
            }
            body = SpecParser.parseCodeBlock(tokens.slice(bodyStart, pos));
            expect(TokenType.BRACE_CLOSE);
        }

        expect(TokenType.COLON);
        TypeExpression type = parseType();
        Multiplicity mult = parseMultiplicity();
        expect(TokenType.SEMI_COLON);

        // Door 4: a bare function-reference body binds the derived property to a
        // user function; any other expression is the sugar (inline) form.
        return new DerivedPropertyDefinition(
                name, params, realizationOf(body), type, mult);
    }

    /**
     * Classify a parsed body as a {@link Realization}: a single bare element
     * reference ({@link PackageableElementPtr}) is a function ref (Door 1/4);
     * anything else is an inline expression body (sugar / Door 3). Shared by
     * mapping bindings and the class/service hats.
     */
    private static Realization realizationOf(List<ValueSpecification> body) {
        if (body.size() == 1 && body.get(0) instanceof PackageableElementPtr ptr) {
            return new Realization.Ref(ptr.fullPath());
        }
        return new Realization.Inline(body);
    }

    private ParameterDefinition parseDerivedPropertyParameter() {
        String name = parseIdentifier();
        expect(TokenType.COLON);
        TypeExpression type = parseType();
        Multiplicity mult = parseMultiplicity();
        return new ParameterDefinition(name, type, mult);
    }

    // ============================================================
    // Constraint declarations (class-level)
    // ============================================================

    /**
     * {@code [ constraint (, constraint)* ]} after the class header. Each
     * constraint may be {@code name: expression} or just {@code expression}
     * (named {@code "unnamed"} for parity with engine). The expression is
     * parsed eagerly via {@link SpecParser#parse(com.legend.lexer.TokenStream)}
     * into a single {@link ValueSpecification}.
     */
    private List<ConstraintDefinition> parseConstraints() {
        expect(TokenType.BRACKET_OPEN);
        List<ConstraintDefinition> result = new ArrayList<>();
        if (peek() != TokenType.BRACKET_CLOSE) {
            result.add(parseConstraint());
            while (match(TokenType.COMMA)) {
                result.add(parseConstraint());
            }
        }
        expect(TokenType.BRACKET_CLOSE);
        return result;
    }

    private ConstraintDefinition parseConstraint() {
        String name = "unnamed";
        // EXTENDED form (real m3): name( ~function: expr ~enforcementLevel: X
        // ~message: expr ) — the predicate is the ~function expression;
        // enforcement level and message are instantiation-time concerns,
        // parsed and dropped (engine parity for query compilation).
        // Dispatch on `name( ~` — a bare function-call constraint like
        // [ eq($this.a, 1) ] is the SIMPLE form (the real grammar allows any
        // expression there); only `name(~...)` opens the extended clause block.
        if (isIdentifierToken(peek()) && peek(1) == TokenType.PAREN_OPEN
                && peek(2) == TokenType.TILDE) {
            name = parseIdentifier();
            expect(TokenType.PAREN_OPEN);
            expect(TokenType.TILDE);
            String kw = parseIdentifier();
            // real clause order: ~owner? ~externalId? ~function
            // ~enforcementLevel? ~message? — leading clauses skip to the next ~
            while (kw.equals("owner") || kw.equals("externalId")) {
                expect(TokenType.COLON);
                while (!atEnd() && peek() != TokenType.TILDE) {
                    advance();
                }
                expect(TokenType.TILDE);
                kw = parseIdentifier();
            }
            if (!kw.equals("function")) {
                throw error("extended constraint must lead with ~function:, got ~" + kw);
            }
            expect(TokenType.COLON);
            int fnStart = pos;
            int d = 0;
            while (!atEnd()) {
                TokenType t = peek();
                if (t == TokenType.TILDE && d == 0) {
                    break;
                }
                if (t == TokenType.BRACKET_OPEN || t == TokenType.PAREN_OPEN
                        || t == TokenType.BRACE_OPEN) {
                    d++;
                } else if (t == TokenType.BRACKET_CLOSE || t == TokenType.PAREN_CLOSE
                        || t == TokenType.BRACE_CLOSE) {
                    if (d == 0) {
                        break;
                    }
                    d--;
                }
                advance();
            }
            ValueSpecification fn = SpecParser.parse(tokens.slice(fnStart, pos));
            // consume remaining ~key: value sections up to the closing paren
            int dd = 0;
            while (!atEnd()) {
                TokenType t = peek();
                if (t == TokenType.PAREN_CLOSE && dd == 0) {
                    break;
                }
                if (t == TokenType.BRACKET_OPEN || t == TokenType.PAREN_OPEN
                        || t == TokenType.BRACE_OPEN) {
                    dd++;
                } else if (t == TokenType.BRACKET_CLOSE || t == TokenType.PAREN_CLOSE
                        || t == TokenType.BRACE_CLOSE) {
                    dd--;
                }
                advance();
            }
            expect(TokenType.PAREN_CLOSE);
            return new ConstraintDefinition(name, realizationOf(List.of(fn)));
        }
        if (isIdentifierToken(peek()) && peek(1) == TokenType.COLON) {
            name = parseIdentifier();
            advance(); // consume :
        }

        // Scan until top-level ',' or matching ']' &mdash; balance brackets,
        // parens, and braces so embedded expressions don't fool us.
        int bodyStart = pos;
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.BRACKET_OPEN || t == TokenType.PAREN_OPEN || t == TokenType.BRACE_OPEN) {
                depth++;
            } else if (t == TokenType.BRACKET_CLOSE || t == TokenType.PAREN_CLOSE || t == TokenType.BRACE_CLOSE) {
                if (depth == 0) break;
                depth--;
            } else if (t == TokenType.COMMA && depth == 0) {
                break;
            }
            advance();
        }
        if (pos == bodyStart) {
            throw error("empty constraint expression for '" + name + "'");
        }
        ValueSpecification expression = SpecParser.parse(tokens.slice(bodyStart, pos));
        // Door 4: `[name: some::fn]` binds the constraint to a predicate
        // function; any other expression is the sugar (inline) predicate.
        return new ConstraintDefinition(name, realizationOf(List.of(expression)));
    }

    /** {@code Primitive fqn extends Base} with an optional dropped constraint block. */
    private PackageableElement parsePrimitiveExtension() {
        advance();   // 'Primitive'
        String fqn = parseQualifiedName();
        expect(TokenType.EXTENDS);
        String base = parseQualifiedName();
        // optional (args) on the base (e.g. Decimal(10,2)) — dropped
        if (peek() == TokenType.PAREN_OPEN) {
            skipBalancedBlock();
        }
        // optional [constraints] — instantiation-time; dropped
        if (peek() == TokenType.BRACKET_OPEN) {
            skipBalancedBlock();
        }
        return new com.legend.parser.element.PrimitiveExtensionDefinition(fqn, base);
    }

    // ============================================================
    // Association
    // ============================================================

    /** {@code Association <<stereos>> {tags} qualifiedName { end1; end2; }} */
    private AssociationDefinition parseAssociation() {
        expect(TokenType.ASSOCIATION);
        parseStereotypes();   // parity: engine consumes and drops
        parseTaggedValues();  // parity: engine consumes and drops
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        List<AssociationEndDefinition> ends = new ArrayList<>();
        List<ClassDefinition.DerivedPropertyDefinition> derived = new ArrayList<>();
        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            // real pure allows QUALIFIED properties in associations — they
            // are alternate accessors of one end, owned by the OPPOSITE
            // end's class (adopted there during normalization)
            if (isDerivedPropertyStart()) {
                derived.add(parseDerivedProperty());
                continue;
            }
            String name = parseIdentifier();
            expect(TokenType.COLON);
            TypeExpression type = parseType();
            Multiplicity mult = parseMultiplicity();
            expect(TokenType.SEMI_COLON);
            ends.add(new AssociationEndDefinition(name, type, mult));
        }
        expect(TokenType.BRACE_CLOSE);

        if (ends.size() != 2) {
            throw error("Association must have exactly 2 properties, found: " + ends.size());
        }
        return new AssociationDefinition(qualifiedName, ends.get(0), ends.get(1), derived);
    }

    // ============================================================
    // Enum
    // ============================================================

    /** {@code Enum <<stereos>> {tags} qualifiedName { VAL (, VAL)* }} */
    private EnumDefinition parseEnumDefinition() {
        expect(TokenType.ENUM);
        parseStereotypes();   // parity: engine consumes and drops
        parseTaggedValues();  // parity: engine consumes and drops
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        List<String> values = new ArrayList<>();
        if (peek() != TokenType.BRACE_CLOSE) {
            parseStereotypes();   // per-value, dropped
            parseTaggedValues();  // per-value, dropped
            values.add(parseIdentifier());
            while (match(TokenType.COMMA)) {
                parseStereotypes();
                parseTaggedValues();
                values.add(parseIdentifier());
            }
        }
        expect(TokenType.BRACE_CLOSE);

        if (values.isEmpty()) {
            throw error("Enum '" + qualifiedName + "' must have at least one value");
        }
        return new EnumDefinition(qualifiedName, values);
    }

    // ============================================================
    // Profile
    // ============================================================

    /**
     * {@code Profile qualifiedName { (stereotypes: [...]; | tags: [...];)* }}
     */
    private ProfileDefinition parseProfile() {
        expect(TokenType.PROFILE);
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        List<String> stereotypes = new ArrayList<>();
        List<String> tags = new ArrayList<>();

        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            if (peek() == TokenType.STEREOTYPES) {
                advance();
                expect(TokenType.COLON);
                expect(TokenType.BRACKET_OPEN);
                if (peek() != TokenType.BRACKET_CLOSE) {
                    stereotypes.add(parseIdentifier());
                    while (match(TokenType.COMMA)) {
                        stereotypes.add(parseIdentifier());
                    }
                }
                expect(TokenType.BRACKET_CLOSE);
                expect(TokenType.SEMI_COLON);
            } else if (peek() == TokenType.TAGS) {
                advance();
                expect(TokenType.COLON);
                expect(TokenType.BRACKET_OPEN);
                if (peek() != TokenType.BRACKET_CLOSE) {
                    tags.add(parseIdentifier());
                    while (match(TokenType.COMMA)) {
                        tags.add(parseIdentifier());
                    }
                }
                expect(TokenType.BRACKET_CLOSE);
                expect(TokenType.SEMI_COLON);
            } else {
                throw error("expected 'stereotypes' or 'tags' inside Profile, found " + peek()
                        + " ('" + safeText() + "')");
            }
        }

        expect(TokenType.BRACE_CLOSE);
        return new ProfileDefinition(qualifiedName, stereotypes, tags);
    }

    // ============================================================
    // Function declaration
    // ============================================================

    /**
     * {@code function <<stereos>> {tags} qualifiedName(params) : returnType[mult]
     * [optional-constraints] { body }}.
     * Body parsed eagerly via {@link SpecParser#parseCodeBlock} into a
     * {@code List<ValueSpecification>} (sequence of statements).
     */
    /**
     * The signature grammar shared by concrete and native functions (audit
     * M2 found it duplicated verbatim): stereotypes, tags, FQN,
     * {@code <T,V|m,n>}, parameters, {@code :R[m]}.
     */
    private record FunctionSignature(
            String qualifiedName,
            List<String> typeParams,
            List<String> multParams,
            List<FunctionDefinition.ParameterDefinition> params,
            TypeExpression returnType,
            Multiplicity returnMult,
            List<StereotypeApplication> stereotypes,
            List<TaggedValue> taggedValues) {
    }

    private FunctionSignature parseFunctionSignature() {
        expect(TokenType.FUNCTION);
        List<StereotypeApplication> stereotypes = parseStereotypes();
        List<TaggedValue> taggedValues = parseTaggedValues();
        String qualifiedName = parseQualifiedName();
        List<String> typeParams = new ArrayList<>();
        List<String> multParams = new ArrayList<>();
        parseTypeAndMultiplicityParameters(typeParams, multParams);
        expect(TokenType.PAREN_OPEN);
        List<FunctionDefinition.ParameterDefinition> params = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            params.add(parseFunctionParameter());
            while (match(TokenType.COMMA)) {
                params.add(parseFunctionParameter());
            }
        }
        expect(TokenType.PAREN_CLOSE);
        expect(TokenType.COLON);
        TypeExpression returnType = parseType();
        Multiplicity returnMult = parseMultiplicity();
        return new FunctionSignature(qualifiedName, List.copyOf(typeParams),
                List.copyOf(multParams), params, returnType, returnMult,
                stereotypes, taggedValues);
    }

    private FunctionDefinition parseFunctionDefinition() {
        FunctionSignature sig = parseFunctionSignature();

        // Optional constraints block — engine accepts then discards.
        // We accept and discard for parity; future work may attach these.
        if (peek() == TokenType.BRACKET_OPEN) {
            parseConstraints();
        }

        expect(TokenType.BRACE_OPEN);
        int bodyStart = pos;
        int depth = 1;
        while (!atEnd() && depth > 0) {
            TokenType t = peek();
            if (t == TokenType.BRACE_OPEN) depth++;
            else if (t == TokenType.BRACE_CLOSE) depth--;
            if (depth > 0) advance();
        }
        List<ValueSpecification> body = SpecParser.parseCodeBlock(tokens.slice(bodyStart, pos));
        expect(TokenType.BRACE_CLOSE);

        return new FunctionDefinition(
                sig.qualifiedName(), sig.typeParams(), sig.multParams(),
                sig.params(), sig.returnType(), sig.returnMult(),
                body, sig.stereotypes(), sig.taggedValues());
    }

    /**
     * Parse a {@code native function ...;} declaration. {@code native} has
     * already been consumed by the caller. Mirrors Pure's
     * {@code nativeFunction} grammar rule: same signature shape as
     * {@link #parseFunctionDefinition()}, but no body block &mdash; the
     * declaration is terminated by a semicolon.
     *
     * <p>Pure syntax:
     * <pre>
     *   native function &lt;&lt;stereo&gt;&gt; {tag=v}
     *       my::pkg::fn&lt;T,V|m,n&gt;(p1:T1[m1], p2:T2[m2]):R[m];
     * </pre>
     */
    private NativeFunctionDefinition parseNativeFunction() {
        FunctionSignature sig = parseFunctionSignature();
        expect(TokenType.SEMI_COLON);
        return new NativeFunctionDefinition(
                sig.qualifiedName(), sig.typeParams(), sig.multParams(),
                sig.params(), sig.returnType(), sig.returnMult(),
                sig.stereotypes(), sig.taggedValues());
    }

    private FunctionDefinition.ParameterDefinition parseFunctionParameter() {
        String name = parseIdentifier();
        expect(TokenType.COLON);
        TypeExpression type = parseType();
        Multiplicity mult = parseMultiplicity();
        return new FunctionDefinition.ParameterDefinition(name, type, mult);
    }

    /**
     * Optional {@code <T,V|m,n>} block declaring generic type and/or
     * multiplicity parameters on a function (concrete or native).
     *
     * <p>Mirrors Pure's M3 grammar:
     * <pre>
     *   typeAndMultiplicityParameters: '<' ( typeParameters ('|' multParameters)? | multParameters ) '>'
     * </pre>
     * Either side may be omitted &mdash; e.g. {@code <T>}, {@code <T,V>},
     * {@code <T|m>}, {@code <T,V|m,n>}, or just {@code <|m>}.
     *
     * <p>Caller passes empty mutable lists; this method appends discovered
     * names. Does nothing if no leading {@code <}.
     */
    private void parseTypeAndMultiplicityParameters(List<String> typeParams,
                                                     List<String> multParams) {
        if (peek() != TokenType.LESS_THAN) return;
        advance(); // consume '<'

        // Type-parameter side (may be empty if next token is PIPE).
        if (peek() != TokenType.PIPE) {
            typeParams.add(parseIdentifier());
            while (match(TokenType.COMMA)) {
                typeParams.add(parseIdentifier());
            }
        }

        // Optional multiplicity side: '|' multParam (',' multParam)*
        if (match(TokenType.PIPE)) {
            multParams.add(parseIdentifier());
            while (match(TokenType.COMMA)) {
                multParams.add(parseIdentifier());
            }
        }

        expect(TokenType.GREATER_THAN);
    }

    // ============================================================
    // Service declaration
    // ============================================================

    /**
     * {@code Service qualifiedName { pattern: ...; documentation: ...;
     * execution: Single { query: |...; mapping: ...; runtime: ...; }
     * testSuites { ... } }}.
     *
     * <p>Unknown top-level keys throw (no silent skip). The query body
     * is parsed eagerly into a {@link ValueSpecification}; the
     * {@code testSuites} block is still captured as raw text (D-3),
     * pending a test-suite grammar.
     */
    private ServiceDefinition parseServiceDefinition() {
        expect(TokenType.SERVICE);
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        String pattern = null;
        String documentation = null;
        ValueSpecification functionBody = null;
        String mappingRef = null;
        String runtimeRef = null;
        String testSuitesSource = null;

        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            // Dispatch on the MINTED TOKEN TYPES — the lexer keyword-izes
            // every service body key; re-reading them as identifier strings
            // was the audit's H2 (two sources of truth for one keyword).
            TokenType key = peek();
            String keyText = safeText();
            advance();
            expect(TokenType.COLON);
            switch (key) {
                case SERVICE_PATTERN -> {
                    pattern = unquoteString(consume(TokenType.STRING));
                    expect(TokenType.SEMI_COLON);
                }
                case SERVICE_DOCUMENTATION -> {
                    documentation = unquoteString(consume(TokenType.STRING));
                    expect(TokenType.SEMI_COLON);
                }
                case SERVICE_AUTO_ACTIVATE_UPDATES -> {
                    // A BOOLEAN TOKEN, not a text re-read (re-audit L5).
                    if (peek() != TokenType.TRUE && peek() != TokenType.FALSE) {
                        throw error("autoActivateUpdates expects true or false, got '"
                                + safeText() + "'");
                    }
                    advance();
                    expect(TokenType.SEMI_COLON);
                }
                case SERVICE_OWNERS -> {
                    expect(TokenType.BRACKET_OPEN);
                    if (peek() != TokenType.BRACKET_CLOSE) {
                        consume(TokenType.STRING);
                        while (match(TokenType.COMMA)) consume(TokenType.STRING);
                    }
                    expect(TokenType.BRACKET_CLOSE);
                    expect(TokenType.SEMI_COLON);
                }
                case SERVICE_EXEC -> {
                    // Only the Single flavor is wired; anything else (Multi,
                    // typos) previously parsed AS Single silently (audit M13a).
                    expect(TokenType.SERVICE_SINGLE);
                    expect(TokenType.BRACE_OPEN);
                    while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
                        TokenType execKey = peek();
                        String execKeyText = safeText();
                        if (execKey != TokenType.MAPPING_TESTS_QUERY
                                && execKey != TokenType.SERVICE_MAPPING
                                && execKey != TokenType.SERVICE_RUNTIME) {
                            // Validate BEFORE consuming: the error points AT
                            // the offender, by TEXT (re-audit M4).
                            throw error("unknown key '" + execKeyText
                                    + "' inside Service.execution (Phase B.3 strict mode; see D-2)");
                        }
                        advance();
                        expect(TokenType.COLON);
                        switch (execKey) {
                            case MAPPING_TESTS_QUERY -> {
                                match(TokenType.PIPE); // optional leading '|'
                                int bs = pos;
                                int d = 0;
                                while (!atEnd()) {
                                    TokenType tk = peek();
                                    if (tk == TokenType.PAREN_OPEN) d++;
                                    else if (tk == TokenType.PAREN_CLOSE) d--;
                                    else if (tk == TokenType.SEMI_COLON && d <= 0) break;
                                    advance();
                                }
                                if (pos == bs) {
                                    throw error("empty query expression in Service '"
                                            + qualifiedName + "'");
                                }
                                functionBody = SpecParser.parse(tokens.slice(bs, pos));
                                expect(TokenType.SEMI_COLON);
                            }
                            case SERVICE_MAPPING -> {
                                mappingRef = parseQualifiedName();
                                expect(TokenType.SEMI_COLON);
                            }
                            case SERVICE_RUNTIME -> {
                                runtimeRef = parseQualifiedName();
                                expect(TokenType.SEMI_COLON);
                            }
                            default -> throw new IllegalStateException(
                                    "unreachable: execution keys validated above");
                        }
                    }
                    expect(TokenType.BRACE_CLOSE);
                }
                case MAPPING_TESTABLE_SUITES -> {
                    // Capture the entire balanced block as raw text — D-3.
                    // testSuites may be followed by '{' or '['; both balance the same way.
                    TokenType opener = peek();
                    if (opener != TokenType.BRACE_OPEN && opener != TokenType.BRACKET_OPEN) {
                        throw error("expected '{' or '[' after testSuites:, got " + opener);
                    }
                    int bs = pos;
                    skipBalancedContent(opener,
                            opener == TokenType.BRACE_OPEN ? TokenType.BRACE_CLOSE : TokenType.BRACKET_CLOSE);
                    testSuitesSource = reconstructText(bs, pos);
                    match(TokenType.SEMI_COLON);
                }
                default -> throw error("unknown key '" + keyText + "' inside Service '"
                        + qualifiedName + "' (Phase B.3 strict mode; see D-2)");
            }
        }
        expect(TokenType.BRACE_CLOSE);

        if (functionBody == null) {
            throw error("Service '" + qualifiedName + "' has no query expression");
        }
        return new ServiceDefinition(
                qualifiedName,
                pattern != null ? pattern : "/",
                functionBody,
                documentation,
                mappingRef,
                runtimeRef,
                testSuitesSource);
    }

    // ============================================================
    // Runtime declaration
    // ============================================================

    /**
     * {@code Runtime qualifiedName { mappings: [...]; connections: [...]; }}.
     * Embedded {@code JsonModelConnection} islands ({@code #{ ... }#}) are
     * captured and parsed via regex (engine parity).
     */
    private RuntimeDefinition parseRuntime() {
        expect(TokenType.RUNTIME);
        String qualifiedName = parseQualifiedName();
        return parseRuntimeBody(qualifiedName);
    }

    /**
     * {@code SingleConnectionRuntime qualifiedName { ... }} &mdash; engine's
     * implementation skips the body and returns an empty runtime. We match
     * that here pending real grammar support.
     */
    private RuntimeDefinition parseSingleConnectionRuntime() {
        expect(TokenType.SINGLE_CONNECTION_RUNTIME);
        String qualifiedName = parseQualifiedName();
        // skipBalancedContent consumes the opening '{' itself, then closing '}'.
        skipBalancedContent(TokenType.BRACE_OPEN, TokenType.BRACE_CLOSE);
        return new RuntimeDefinition(qualifiedName, List.of(), Map.of(), List.of());
    }

    private RuntimeDefinition parseRuntimeBody(String qualifiedName) {
        expect(TokenType.BRACE_OPEN);
        List<String> mappings = new ArrayList<>();
        // HashMap (not LinkedHashMap) — bindings are looked up by store name;
        // iteration order is never observed semantically. Saves the per-put
        // linked-list bookkeeping cost.
        Map<String, String> connectionBindings = new HashMap<>();
        List<JsonModelConnection> jsonConnections = new ArrayList<>();

        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            TokenType key = peek();   // minted token, not a re-read string (audit H2)
            String keyText = safeText();
            advance();
            expect(TokenType.COLON);
            switch (key) {
                case MAPPINGS -> {
                    expect(TokenType.BRACKET_OPEN);
                    if (peek() != TokenType.BRACKET_CLOSE) {
                        mappings.add(parseQualifiedName());
                        while (match(TokenType.COMMA)) mappings.add(parseQualifiedName());
                    }
                    expect(TokenType.BRACKET_CLOSE);
                    match(TokenType.SEMI_COLON);
                }
                case CONNECTIONS -> parseRuntimeConnections(connectionBindings, jsonConnections);
                default -> throw error("unknown key '" + keyText + "' inside Runtime '"
                        + qualifiedName + "' (Phase B.3 strict mode; see D-2)");
            }
        }
        expect(TokenType.BRACE_CLOSE);
        return new RuntimeDefinition(qualifiedName, mappings, connectionBindings, jsonConnections);
    }

    private void parseRuntimeConnections(Map<String, String> bindings,
                                         List<JsonModelConnection> jsonConnections) {
        expect(TokenType.BRACKET_OPEN);
        while (peek() != TokenType.BRACKET_CLOSE && !atEnd()) {
            String storeName = parseQualifiedName();
            expect(TokenType.COLON);
            expect(TokenType.BRACKET_OPEN);
            while (peek() != TokenType.BRACKET_CLOSE && !atEnd()) {
                parseIdentifier();      // tag (e.g. "id", "json") — engine doesn't keep this
                expect(TokenType.COLON);
                if (peek() == TokenType.ISLAND_OPEN) {
                    advance(); // consume ISLAND_OPEN ('#{' or '#name{')
                    int embStart = pos;
                    while (peek() != TokenType.ISLAND_END && !atEnd()) advance();
                    String embText = reconstructText(embStart, pos);
                    if (peek() == TokenType.ISLAND_END) advance();
                    // LOUD: an unrecognized embedded connection island was
                    // silently consumed and DISCARDED (audit H1). Strict
                    // mode names what it cannot parse.
                    jsonConnections.add(parseEmbeddedJsonModelConnection(embText));
                } else {
                    if (bindings.put(storeName, parseQualifiedName()) != null) {
                        throw error("duplicate connection binding for store '"
                                + storeName + "'");
                    }
                }
                match(TokenType.COMMA);
            }
            expect(TokenType.BRACKET_CLOSE);
            match(TokenType.COMMA);
        }
        expect(TokenType.BRACKET_CLOSE);
        match(TokenType.SEMI_COLON);
    }

    // Compiled once, used by parseEmbeddedJsonModelConnection.
    private static final Pattern JMC_CLASS_PATTERN =
            Pattern.compile("class\\s*:\\s*([\\w:]+)\\s*;");
    private static final Pattern JMC_URL_PATTERN =
            Pattern.compile("url\\s*:\\s*'([^']*)'\\s*;");

    /**
     * Parse an embedded {@code JsonModelConnection { class: ...; url: '...'; }}
     * block via regex against its raw source text. LOUD on anything else —
     * only JsonModelConnection islands are supported, and a typo'd one must
     * not vanish (audit H1; the old null-return silently dropped it).
     */
    private JsonModelConnection parseEmbeddedJsonModelConnection(String raw) {
        raw = raw.trim();
        if (!raw.startsWith("JsonModelConnection")) {
            throw error("unsupported embedded connection flavor (only"
                    + " JsonModelConnection is supported): "
                    + raw.substring(0, Math.min(40, raw.length())));
        }
        Matcher cm = JMC_CLASS_PATTERN.matcher(raw);
        Matcher um = JMC_URL_PATTERN.matcher(raw);
        if (!cm.find() || !um.find()) {
            throw error("malformed JsonModelConnection (expected class: ...;"
                    + " url: '...';): " + raw.substring(0, Math.min(60, raw.length())));
        }
        return new JsonModelConnection(cm.group(1), um.group(1));
    }

    // ============================================================
    // RelationalDatabaseConnection
    // ============================================================

    /**
     * {@code RelationalDatabaseConnection qualifiedName { store: ...; type: ...;
     * specification: ...; auth: ...; }}.
     * Unknown keys throw (strict mode; D-2).
     */
    private ConnectionDefinition parseConnection() {
        expect(TokenType.RELATIONAL_DATABASE_CONNECTION);
        String qualifiedName = parseQualifiedName();
        expect(TokenType.BRACE_OPEN);

        String storeName = null;
        ConnectionDefinition.DatabaseType dbType = null;
        ConnectionSpecification specification = null;
        AuthenticationSpec authentication = null;

        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            TokenType key = peek();   // minted token, not a re-read string (audit H2)
            String keyText = safeText();
            advance();
            expect(TokenType.COLON);
            switch (key) {
                case STORE -> {
                    storeName = parseQualifiedName();
                    expect(TokenType.SEMI_COLON);
                }
                case TYPE -> {
                    String typeStr = parseIdentifier();
                    try {
                        dbType = ConnectionDefinition.DatabaseType.valueOf(typeStr);
                    } catch (IllegalArgumentException ex) {
                        throw error("unknown database type '" + typeStr + "' (expected one of "
                                + java.util.Arrays.toString(ConnectionDefinition.DatabaseType.values()) + ")");
                    }
                    expect(TokenType.SEMI_COLON);
                }
                case RELATIONAL_DATASOURCE_SPEC -> {
                    String specType = parseIdentifier();
                    expect(TokenType.BRACE_OPEN);
                    Map<String, String> props = parseKeyValueBlock();
                    expect(TokenType.BRACE_CLOSE);
                    specification = switch (specType) {
                        case "InMemory" -> new ConnectionSpecification.InMemory();
                        case "LocalFile" -> new ConnectionSpecification.LocalFile(props.get("path"));
                        case "Static" -> new ConnectionSpecification.StaticDatasource(
                                props.get("host"),
                                props.containsKey("port") ? Integer.parseInt(props.get("port")) : 0,
                                props.get("database"));
                        case "LocalH2" -> new ConnectionSpecification.LocalH2(props.get("url"));
                        default -> throw error("unknown specification flavor '" + specType
                                + "' (expected InMemory / LocalFile / LocalH2 / Static)");
                    };
                    expect(TokenType.SEMI_COLON);
                }
                case RELATIONAL_AUTH_STRATEGY -> {
                    String authType = parseIdentifier();
                    Map<String, String> props = Map.of();
                    if (match(TokenType.BRACE_OPEN)) {
                        props = parseKeyValueBlock();
                        expect(TokenType.BRACE_CLOSE);
                    }
                    authentication = switch (authType) {
                        case "NoAuth" -> new AuthenticationSpec.NoAuth();
                        case "DefaultH2" -> new AuthenticationSpec.DefaultH2();
                        case "UsernamePassword" -> new AuthenticationSpec.UsernamePassword(
                                props.get("username"), props.get("passwordVaultRef"));
                        default -> throw error("unknown auth flavor '" + authType
                                + "' (expected NoAuth / DefaultH2 / UsernamePassword)");
                    };
                    expect(TokenType.SEMI_COLON);
                }
                default -> throw error("unknown key '" + keyText
                        + "' inside RelationalDatabaseConnection '" + qualifiedName
                        + "' (Phase B.3 strict mode; see D-2)");
            }
        }
        expect(TokenType.BRACE_CLOSE);

        if (dbType == null) error("RelationalDatabaseConnection '" + qualifiedName + "' missing required 'type:' key");
        if (specification == null) error("RelationalDatabaseConnection '" + qualifiedName + "' missing required 'specification:' key");
        // auth: defaults to NoAuth for LOCAL specs only (LocalFile /
        // InMemory / LocalH2 — the engine's 'mode: local' shape); a Static
        // (remote) connection without auth stays the loud error the real
        // engine gives (audit).
        if (authentication == null) {
            if (specification instanceof ConnectionSpecification.StaticDatasource) {
                error("RelationalDatabaseConnection '" + qualifiedName
                        + "' missing required 'auth:' key");
            }
            authentication = new AuthenticationSpec.NoAuth();
        }

        return new ConnectionDefinition(qualifiedName, storeName, dbType, specification, authentication);
    }

    // ============================================================
    // Shared key/value-block + balanced-content helpers
    // ============================================================

    /**
     * Parse a sequence of {@code key: value;} pairs inside an already-opened
     * brace block, stopping at (but not consuming) the closing {@code }}.
     * Values are either {@code STRING}, {@code INTEGER}, identifiers, or
     * qualified names; all are stored as their raw text.
     */
    private Map<String, String> parseKeyValueBlock() {
        // HashMap: connection properties are looked up by key, never iterated
        // positionally; JDBC URL construction doesn't depend on order either.
        Map<String, String> props = new HashMap<>();
        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            String key = parseIdentifier();
            expect(TokenType.COLON);
            String value;
            if (peek() == TokenType.STRING) {
                value = unquoteString(consume(TokenType.STRING));
            } else if (peek() == TokenType.QUOTED_STRING) {
                // Double-quoted values appear in connection specs
                // (path: "/tmp/db.duckdb") — same string payload, the
                // OTHER quote character.
                String raw = consume(TokenType.QUOTED_STRING);
                value = raw.length() >= 2 && raw.charAt(0) == '"'
                        && raw.charAt(raw.length() - 1) == '"'
                        ? raw.substring(1, raw.length() - 1) : raw;
            } else if (peek() == TokenType.INTEGER) {
                value = consume(TokenType.INTEGER);
            } else {
                value = parseQualifiedName();
            }
            if (props.put(key, value) != null) {
                throw error("duplicate key '" + key + "' in property block");
            }
            // The last pair's semicolon is optional in corpus sources
            // (LocalH2 { url: '...' }); a following key still needs one.
            if (peek() != TokenType.BRACE_CLOSE) {
                expect(TokenType.SEMI_COLON);
            }
        }
        return props;
    }

    /**
     * Skip over a balanced {@code open..close} region. The opener at the
     * current position is consumed; advance until the matching close is
     * consumed too. Handles arbitrary nesting of the same open/close pair.
     */
    private void skipBalancedContent(TokenType open, TokenType close) {
        expect(open);
        int depth = 1;
        while (!atEnd() && depth > 0) {
            TokenType t = peek();
            if (t == open) depth++;
            else if (t == close) depth--;
            advance();
            if (depth == 0) return;
        }
    }

    /** Strip the leading and trailing {@code '} from a {@code STRING} token's raw text. */
    private String unquoteString(String raw) {
        // Routes through THE shared decoder (which throws on malformed
        // input) — no legacy fallback: an unterminated literal at EOF used
        // to flow through with its leading quote intact (re-audit M2).
        return TokenStreamCursor.unquoteAndUnescape(raw, this);
    }

    // ============================================================
    // Database declaration
    // ============================================================

    /**
     * {@code Database <<stereos>> {tags} qualifiedName ( includes? (Schema | Table | View | Join | Filter | MultiGrainFilter)* )}.
     */
    private DatabaseDefinition parseDatabase() {
        expect(TokenType.DATABASE);
        parseStereotypes();   // parity: engine consumes and drops
        parseTaggedValues();  // parity: engine consumes and drops
        String qualifiedName = parseQualifiedName();
        String dbScope = qualifiedName;
        expect(TokenType.PAREN_OPEN);

        List<String> includes = new ArrayList<>();
        List<DatabaseDefinition.SchemaDefinition> schemas = new ArrayList<>();
        List<DatabaseDefinition.TableDefinition> tables = new ArrayList<>();
        List<DatabaseDefinition.ViewDefinition> views = new ArrayList<>();
        List<DatabaseDefinition.JoinDefinition> joins = new ArrayList<>();
        List<DatabaseDefinition.FilterDefinition> filters = new ArrayList<>();
        List<DatabaseDefinition.FilterDefinition> multiGrainFilters = new ArrayList<>();

        while (peek() != TokenType.PAREN_CLOSE && !atEnd()) {
            if (peek() == TokenType.INCLUDE) {
                advance();
                includes.add(parseQualifiedName());
            } else if (peek() == TokenType.SCHEMA) {
                schemas.add(parseDbSchema(dbScope, tables, views));
            } else if (peek() == TokenType.TABLE) {
                tables.add(parseDbTable());
            } else if (peek() == TokenType.VIEW) {
                views.add(parseDbView(dbScope));
            } else if (peek() == TokenType.JOIN) {
                joins.add(parseDbJoin(dbScope));
            } else if (peek() == TokenType.FILTER) {
                filters.add(parseDbFilter(dbScope));
            } else if (peek() == TokenType.MULTIGRAIN_FILTER) {
                multiGrainFilters.add(parseDbMultiGrainFilter(dbScope));
            } else {
                throw error("unknown Database element '" + safeText()
                        + "' in '" + qualifiedName + "' (expected include / Schema / Table / View / Join / Filter / MultiGrainFilter)");
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition(qualifiedName, includes, schemas, tables, views,
                joins, filters, multiGrainFilters);
    }

    /**
     * {@code Schema name ( (Table | View)* )}. Schema tables and views are
     * also mirrored into the database-level flat lists for callers that want
     * a flat view of all tables &mdash; matches engine.
     */
    private DatabaseDefinition.SchemaDefinition parseDbSchema(
            String dbScope,
            List<DatabaseDefinition.TableDefinition> flatTables,
            List<DatabaseDefinition.ViewDefinition> flatViews) {
        advance(); // "Schema"
        String schemaName = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        List<DatabaseDefinition.TableDefinition> schemaTables = new ArrayList<>();
        List<DatabaseDefinition.ViewDefinition> schemaViews = new ArrayList<>();
        while (peek() != TokenType.PAREN_CLOSE && !atEnd()) {
            if (peek() == TokenType.TABLE) {
                DatabaseDefinition.TableDefinition t = parseDbTable();
                schemaTables.add(t);
                flatTables.add(t);
            } else if (peek() == TokenType.VIEW) {
                DatabaseDefinition.ViewDefinition v = parseDbView(dbScope);
                schemaViews.add(v);
                flatViews.add(v);
            } else {
                throw error("unknown Schema element '" + safeText()
                        + "' inside '" + schemaName + "' (expected Table or View)");
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.SchemaDefinition(schemaName, schemaTables, schemaViews);
    }

    private DatabaseDefinition.TableDefinition parseDbTable() {
        advance(); // "Table"
        String tableName = parseRelationalIdentifier();
        expect(TokenType.PAREN_OPEN);
        // milestoning(business(BUS_FROM=..., BUS_THRU=...),
        // processing(PROCESSING_IN=..., PROCESSING_OUT=...)) — the table's
        // temporal columns, captured for milestoned-fetch filters
        DatabaseDefinition.TableDefinition.Milestoning milestoning = null;
        if (isIdentifierToken(peek()) && "milestoning".equals(text())
                && peek(1) == TokenType.PAREN_OPEN) {
            advance();
            expect(TokenType.PAREN_OPEN);
            DatabaseDefinition.TableDefinition.Milestoning.Business business = null;
            DatabaseDefinition.TableDefinition.Milestoning.Processing processing = null;
            while (peek() != TokenType.PAREN_CLOSE && !atEnd()) {
                String kind = parseIdentifier();
                expect(TokenType.PAREN_OPEN);
                java.util.Map<String, String> kv = new java.util.LinkedHashMap<>();
                while (peek() != TokenType.PAREN_CLOSE && !atEnd()) {
                    String k = parseIdentifier();
                    if (match(TokenType.EQUAL)) {
                        // values are column names, date literals
                        // (INFINITY_DATE=%9999-12-31...) or booleans
                        // (THRU_IS_INCLUSIVE=true) — ALL are load-bearing
                        // (the engine's %latest filter and range-boundary
                        // operators derive from them); capture verbatim.
                        if (isIdentifierToken(peek())) {
                            kv.put(k, parseRelationalIdentifier());
                        } else {
                            kv.put(k, text());
                            advance();
                        }
                    }
                    match(TokenType.COMMA);
                }
                expect(TokenType.PAREN_CLOSE);
                if (kind.equalsIgnoreCase("business")) {
                    business = new DatabaseDefinition.TableDefinition
                            .Milestoning.Business(
                            kv.get("BUS_FROM"), kv.get("BUS_THRU"),
                            "true".equalsIgnoreCase(kv.get("THRU_IS_INCLUSIVE")),
                            kv.get("BUS_SNAPSHOT_DATE"),
                            kv.get("INFINITY_DATE"));
                } else if (kind.equalsIgnoreCase("processing")) {
                    processing = new DatabaseDefinition.TableDefinition
                            .Milestoning.Processing(
                            kv.get("PROCESSING_IN"), kv.get("PROCESSING_OUT"),
                            "true".equalsIgnoreCase(kv.get("OUT_IS_INCLUSIVE")),
                            kv.get("PROCESSING_SNAPSHOT_DATE"),
                            kv.get("INFINITY_DATE"));
                }
                match(TokenType.COMMA);
            }
            expect(TokenType.PAREN_CLOSE);
            milestoning = new DatabaseDefinition.TableDefinition.Milestoning(
                    business, processing);
        }
        List<DatabaseDefinition.ColumnDefinition> columns = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            columns.add(parseColumnDefinition());
            while (match(TokenType.COMMA)) {
                if (peek() == TokenType.PAREN_CLOSE) break;
                columns.add(parseColumnDefinition());
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.TableDefinition(tableName, columns, milestoning);
    }

    private DatabaseDefinition.ColumnDefinition parseColumnDefinition() {
        String columnName = parseRelationalIdentifier();
        RelationalDataType dataType = parseColumnDataType();
        boolean primaryKey = match(TokenType.PRIMARY_KEY);
        // PRIMARY KEY implies NOT NULL (engine parity).
        boolean notNull = primaryKey || match(TokenType.NOT_NULL);
        return new DatabaseDefinition.ColumnDefinition(columnName, dataType, primaryKey, notNull);
    }

    /**
     * Parse a column data type, dispatching the source identifier (plus
     * optional {@code (size)} or {@code (precision, scale)} arguments) to
     * the appropriate {@link RelationalDataType} record.
     *
     * <p>Sized types ({@code VARCHAR}, {@code CHAR}, {@code BINARY},
     * {@code VARBINARY}) consume one int. Precision/scale types
     * ({@code DECIMAL}, {@code NUMERIC}) consume two. Other types must not
     * carry parens. Unknown identifiers throw per AGENTS.md invariant 4.
     */
    private RelationalDataType parseColumnDataType() {
        String name = parseIdentifier();
        String upper = name.toUpperCase();
        if (peek() == TokenType.PAREN_OPEN) {
            advance();
            int first = Integer.parseInt(consume(TokenType.INTEGER));
            RelationalDataType sized;
            if (match(TokenType.COMMA)) {
                int second = Integer.parseInt(consume(TokenType.INTEGER));
                sized = switch (upper) {
                    case "DECIMAL" -> new RelationalDataType.Decimal(first, second);
                    case "NUMERIC" -> new RelationalDataType.Numeric(first, second);
                    default -> throw new IllegalArgumentException(
                            "type '" + name + "' does not take (precision, scale)");
                };
            } else {
                sized = switch (upper) {
                    case "VARCHAR"   -> new RelationalDataType.Varchar(first);
                    case "CHAR"      -> new RelationalDataType.Char_(first);
                    case "BINARY"    -> new RelationalDataType.Binary(first);
                    case "VARBINARY" -> new RelationalDataType.Varbinary(first);
                    // Engine grammar permits DECIMAL(p) with implicit scale=0;
                    // Numeric likewise. Mirror that.
                    case "DECIMAL"   -> new RelationalDataType.Decimal(first, 0);
                    case "NUMERIC"   -> new RelationalDataType.Numeric(first, 0);
                    default -> throw new IllegalArgumentException(
                            "type '" + name + "' does not take a (size) argument");
                };
            }
            expect(TokenType.PAREN_CLOSE);
            return sized;
        }
        return RelationalDataType.fromName(name);
    }

    private DatabaseDefinition.JoinDefinition parseDbJoin(String dbScope) {
        advance(); // "Join"
        String joinName = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        RelationalOperation operation = parseDbOperation(dbScope);
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.JoinDefinition(joinName, operation);
    }

    private DatabaseDefinition.FilterDefinition parseDbFilter(String dbScope) {
        advance(); // "Filter"
        String filterName = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        RelationalOperation condition = parseDbOperation(dbScope);
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.FilterDefinition(filterName, condition);
    }

    private DatabaseDefinition.FilterDefinition parseDbMultiGrainFilter(String dbScope) {
        advance(); // "MultiGrainFilter"
        String filterName = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        RelationalOperation condition = parseDbOperation(dbScope);
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.FilterDefinition(filterName, condition);
    }

    private DatabaseDefinition.ViewDefinition parseDbView(String dbScope) {
        advance(); // "View"
        String viewName = parseRelationalIdentifier();
        expect(TokenType.PAREN_OPEN);

        FilterMapping filter = null;
        List<RelationalOperation> groupBy = new ArrayList<>();
        boolean distinct = false;

        while (peek() == TokenType.FILTER_CMD
                || peek() == TokenType.GROUP_BY_CMD
                || peek() == TokenType.DISTINCT_CMD) {
            TokenType cmd = peek();
            advance();
            if (cmd == TokenType.FILTER_CMD) {
                filter = parseViewFilterClause(dbScope);
            } else if (cmd == TokenType.GROUP_BY_CMD) {
                expect(TokenType.PAREN_OPEN);
                if (peek() != TokenType.PAREN_CLOSE) {
                    groupBy.add(parseDbOperation(dbScope));
                    while (match(TokenType.COMMA)) groupBy.add(parseDbOperation(dbScope));
                }
                expect(TokenType.PAREN_CLOSE);
            } else { // DISTINCT_CMD
                distinct = true;
            }
        }

        List<DatabaseDefinition.ViewDefinition.ViewColumnMapping> columnMappings = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            columnMappings.add(parseViewColumnMapping(dbScope));
            while (match(TokenType.COMMA)) {
                if (peek() == TokenType.PAREN_CLOSE) break;
                columnMappings.add(parseViewColumnMapping(dbScope));
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.ViewDefinition(viewName, filter, groupBy, distinct, columnMappings);
    }

    /**
     * Parse the body of a view's {@code ~filter ...} clause. Caller has
     * already consumed the {@code ~filter} command token. Four forms:
     * <pre>
     *   ~filter F                                  → Direct(Local("F"))
     *   ~filter [DB] F                             → Direct(Cross("DB", "F"))
     *   ~filter [DB1] @J1 > @J2 | F                → JoinMediated("DB1", [..], Local("F"))
     *   ~filter [DB1] @J1 > @J2 | [DB2] F          → JoinMediated("DB1", [..], Cross("DB2", "F"))
     * </pre>
     *
     * <p>The fork between Direct and JoinMediated happens after seeing the
     * first {@code [DB]} (or the absence thereof): if the next token is
     * {@code @}, we're on the join-mediated path; otherwise it's a direct
     * filter reference.
     */
    private FilterMapping parseViewFilterClause(String dbScope) {
        String firstDb = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            firstDb = parseQualifiedName();
            expect(TokenType.BRACKET_CLOSE);
        }
        if (peek() == TokenType.AT) {
            // Join-mediated form: firstDb is the source db (required by grammar)
            if (firstDb == null) {
                throw error("Join-mediated filter must start with a [DB] qualifier");
            }
            List<JoinChainElement> joins = parseJoinChain(firstDb);
            expect(TokenType.PIPE);
            FilterPointer target = parseFilterPointer();
            return new FilterMapping.JoinMediated(firstDb, joins, target);
        }
        // Direct form: firstDb (possibly null) qualifies the filter name itself.
        String name = parseIdentifier();
        FilterPointer pointer = firstDb == null
                ? new FilterPointer.Local(name)
                : new FilterPointer.Cross(firstDb, name);
        return new FilterMapping.Direct(pointer);
    }

    /**
     * Parse a {@code [DB]?} filter-name pair into a {@link FilterPointer}.
     * Used for the target side of a join-mediated filter (post-pipe).
     */
    private FilterPointer parseFilterPointer() {
        String db = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            db = parseQualifiedName();
            expect(TokenType.BRACKET_CLOSE);
        }
        String name = parseIdentifier();
        return db == null ? new FilterPointer.Local(name) : new FilterPointer.Cross(db, name);
    }

    /**
     * Parse {@code @J1 > @J2 > ... > @Jn} into a list of {@link JoinChainElement}.
     * Caller has consumed any leading {@code [DB]}. The {@code initialDb}
     * argument seeds the chain's source database when no per-hop {@code [DB]}
     * is specified.
     */
    /**
     * THE join-chain grammar, engine-uniform (audit M1 found it TRIPLICATED
     * with capability drift):
     * <pre>  (joinType)? @Join ( '&gt;' (joinType)? ('[' db ']')? @Join )*  </pre>
     */
    private List<JoinChainElement> parseJoinChain(String defaultDb) {
        List<JoinChainElement> chain = new ArrayList<>();
        JoinType firstJoinType = optionalJoinType();
        expect(TokenType.AT);
        chain.add(new JoinChainElement(parseIdentifier(), firstJoinType, defaultDb, false));
        parseJoinChainHops(chain, defaultDb);
        return chain;
    }

    /** The {@code ('&gt;' (type)? ([db])? @Join)*} hop loop shared by all chain forms. */
    private void parseJoinChainHops(List<JoinChainElement> chain, String defaultDb) {
        while (match(TokenType.GREATER_THAN)) {
            JoinType joinType = optionalJoinType();
            String hopDb = defaultDb;
            if (peek() == TokenType.BRACKET_OPEN) {
                advance();
                hopDb = parseQualifiedName();
                expect(TokenType.BRACKET_CLOSE);
            }
            expect(TokenType.AT);
            chain.add(new JoinChainElement(parseIdentifier(), joinType, hopDb, false));
        }
    }

    /** {@code (INNER)}-style parenthesised join type, if present. */
    private JoinType optionalJoinType() {
        if (peek() != TokenType.PAREN_OPEN) {
            return null;
        }
        advance();
        JoinType t = JoinType.fromIdentifier(parseIdentifier());
        expect(TokenType.PAREN_CLOSE);
        return t;
    }

    private DatabaseDefinition.ViewDefinition.ViewColumnMapping parseViewColumnMapping(String dbScope) {
        String colName = parseIdentifier();
        String targetSetId = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            targetSetId = parseIdentifier();
            expect(TokenType.BRACKET_CLOSE);
        }
        expect(TokenType.COLON);
        RelationalOperation expression = parseDbOperation(dbScope);
        boolean pk = match(TokenType.PRIMARY_KEY);
        return new DatabaseDefinition.ViewDefinition.ViewColumnMapping(
                colName, targetSetId, expression, pk);
    }

    // ============================================================
    // Mapping declaration (B.4b — Relational class mappings)
    // ============================================================

    /**
     * {@code Mapping qualifiedName ( includes? (classMapping | associationMapping
     * | enumerationMapping | testSuites)* )}.
     *
     * <p>Current slice supports relational class mappings only. Encountering
     * an association mapping, enumeration mapping, M2M (Pure-instance) class
     * mapping, or a {@code testSuites:} block raises a {@code ParseException}
     * naming the unsupported construct &mdash; better than silently
     * dropping data (D-2 strictness).
     */
    private PackageableElement parseMapping() {
        expect(TokenType.MAPPING);
        String qualifiedName = parseQualifiedName();
        expect(TokenType.PAREN_OPEN);

        List<MappingInclude> includes = new ArrayList<>();
        MappingAccum accum = new MappingAccum();
        String testSuitesSource = null;

        while (peek() != TokenType.PAREN_CLOSE) {
            if (peek() == TokenType.INCLUDE) {
                advance();
                includes.add(parseMappingInclude());
                continue;
            }
            if (isIdentifierToken(peek()) && peek() == TokenType.MAPPING_TESTABLE_SUITES) {
                // B.4f / D-3: capture the testSuites block verbatim and
                // hand it to Phase C for lazy parsing. Engine grammar
                // wraps suites in '[ ... ]'.
                if (testSuitesSource != null) {
                    throw error("duplicate 'testSuites' block in Mapping '"
                            + qualifiedName + "'");
                }
                advance(); // consume 'testSuites' identifier
                expect(TokenType.COLON);
                TokenType opener = peek();
                if (opener != TokenType.BRACKET_OPEN && opener != TokenType.BRACE_OPEN) {
                    throw error("expected '[' or '{' after testSuites:, got " + opener);
                }
                int bs = pos;
                skipBalancedContent(opener,
                        opener == TokenType.BRACKET_OPEN
                                ? TokenType.BRACKET_CLOSE
                                : TokenType.BRACE_CLOSE);
                testSuitesSource = reconstructText(bs, pos);
                continue;
            }
            // Class/association/enumeration mappings share an outer shape
            // (`path: MappingType { body }`). parseMappingElement dispatches by
            // the MappingType keyword and (for a kind block) by the first body
            // tokens: a legacy DSL body (`~directive` / `prop:`) goes to the
            // legacy lists; a clean-sheet function-ref body (a bare FQN) goes
            // to the canonical binding lists (CLEAN_SHEET_INVERSION §5.1).
            parseMappingElement(accum);
        }
        expect(TokenType.PAREN_CLOSE);

        boolean hasLegacy = !accum.classMappings.isEmpty()
                || !accum.associationMappings.isEmpty();
        boolean hasCanonical = !accum.classBindings.isEmpty()
                || !accum.associationBindings.isEmpty();
        if (hasLegacy && hasCanonical) {
            throw error("Mapping '" + qualifiedName + "' mixes legacy DSL bodies with "
                + "function-form bindings; a mapping must be all-legacy or "
                + "all-clean-sheet (convert the whole mapping)");
        }
        // Whole-element split (CLEAN_SHEET_INVERSION §1.5.2): clean-sheet text
        // parses straight to the canonical binding table; legacy DSL to the
        // legacy surface tree (rewritten to canonical by MappingNormalizer).
        if (hasCanonical) {
            return new MappingDefinition(qualifiedName, includes,
                    accum.classBindings, accum.associationBindings,
                    accum.enumerationMappings, testSuitesSource);
        }
        return new LegacyMappingDefinition(qualifiedName, includes,
                accum.classMappings, accum.associationMappings,
                accum.enumerationMappings, testSuitesSource);
    }

    /** Per-mapping body accumulator: legacy surface records vs canonical bindings. */
    private static final class MappingAccum {
        final List<ClassMapping> classMappings = new ArrayList<>();
        final List<AssociationMapping> associationMappings = new ArrayList<>();
        final List<EnumerationMapping> enumerationMappings = new ArrayList<>();
        final List<MappingDefinition.ClassBinding> classBindings = new ArrayList<>();
        final List<MappingDefinition.AssociationBinding> associationBindings = new ArrayList<>();
    }

    /**
     * {@code includedPath ([oldStore -> newStore (, oldStore -> newStore)*])?}.
     * The {@code include} keyword has already been consumed by the caller.
     */
    private MappingInclude parseMappingInclude() {
        String includedPath = parseQualifiedName();
        List<MappingInclude.StoreSubstitution> subs = new ArrayList<>();
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            if (peek() != TokenType.BRACKET_CLOSE) {
                subs.add(parseStoreSubstitution());
                while (match(TokenType.COMMA)) subs.add(parseStoreSubstitution());
            }
            expect(TokenType.BRACKET_CLOSE);
        }
        return new MappingInclude(includedPath, subs);
    }

    private MappingInclude.StoreSubstitution parseStoreSubstitution() {
        String original = parseQualifiedName();
        // engine grammar uses `->` between the two store names
        if (peek() != TokenType.ARROW) {
            throw error("expected '->' in store substitution but found " + peek());
        }
        advance();
        String replacement = parseQualifiedName();
        return new MappingInclude.StoreSubstitution(original, replacement);
    }

    /**
     * Parse one element of a Mapping body. Both class and association
     * mappings share the outer header shape:
     * <pre>
     *   [*]? path ([setId])? (extends [parentId])? : MappingType { body }
     * </pre>
     * After consuming the header and opening brace, we peek inside the
     * body to decide which kind this is &mdash; an association mapping
     * begins with the {@code AssociationMapping} keyword; everything
     * else is a class mapping. Engine parity: this is exactly how
     * {@code RelationalParseTreeWalker} disambiguates.
     *
     * <p>The result is added to whichever output list corresponds to
     * its kind.
     */
    private void parseMappingElement(MappingAccum accum) {
        boolean root = match(TokenType.STAR);
        String elementPath = parseQualifiedName();

        String setId = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            setId = parseIdentifier();
            expect(TokenType.BRACKET_CLOSE);
        }
        String extendsSetId = null;
        if (peek() == TokenType.EXTENDS) {
            advance();
            expect(TokenType.BRACKET_OPEN);
            extendsSetId = parseIdentifier();
            expect(TokenType.BRACKET_CLOSE);
        }
        expect(TokenType.COLON);

        TokenType mappingTypeToken = peek();
        if (mappingTypeToken == TokenType.ENUMERATION_MAPPING) {
            if (root) {
                throw error("Enumeration mappings cannot be marked root (the leading '*' "
                        + "is only valid for class mappings)");
            }
            if (setId != null || extendsSetId != null) {
                throw error("Enumeration mappings do not accept [setId] or extends [parentId]");
            }
            advance(); // consume EnumerationMapping keyword
            accum.enumerationMappings.add(parseEnumerationMappingBody(elementPath));
            return;
        }
        // Clean-sheet: AssociationMapping is its own kind tag (not nested inside
        // Relational). Body is a bare predicate-function FQN.
        if (mappingTypeToken == TokenType.ASSOCIATION_MAPPING) {
            if (root) {
                throw error("Association mappings cannot be marked root (the leading '*' "
                        + "is only valid for class mappings)");
            }
            if (setId != null || extendsSetId != null) {
                throw error("Association mappings do not accept [setId] or extends [parentId]");
            }
            advance(); // consume AssociationMapping keyword
            expect(TokenType.BRACE_OPEN);
            accum.associationBindings.add(new MappingDefinition.AssociationBinding(
                    elementPath, parseCleanSheetBody(elementPath)));
            return;
        }
        if (mappingTypeToken == TokenType.RELATIONAL || mappingTypeToken == TokenType.PURE_MAPPING) {
            boolean pure = mappingTypeToken == TokenType.PURE_MAPPING;
            advance();
            expect(TokenType.BRACE_OPEN);
            // Legacy nested AssociationMapping (`Relational { AssociationMapping (...) }`).
            if (!pure && peek() == TokenType.ASSOCIATION_MAPPING) {
                if (root) {
                    throw error("Association mappings cannot be marked root (the leading '*' "
                            + "is only valid for class mappings)");
                }
                if (setId != null || extendsSetId != null) {
                    throw error("Association mappings do not accept [setId] or extends [parentId] "
                            + "on the header");
                }
                advance(); // consume AssociationMapping keyword
                AssociationMapping am = parseAssociationMappingBody(elementPath);
                expect(TokenType.BRACE_CLOSE);
                accum.associationMappings.add(am);
                return;
            }
            // Disambiguate clean-sheet body vs legacy DSL body
            // (CLEAN_SHEET_INVERSION §5.1): a legacy body opens with a
            // `~directive` or a `prop:` property mapping; anything else is a
            // clean-sheet Pure expression (a function ref or an inline body).
            if (isCleanSheetBody()) {
                accum.classBindings.add(new MappingDefinition.ClassBinding(
                        elementPath,
                        pure ? MappingDefinition.Kind.PURE : MappingDefinition.Kind.RELATIONAL,
                        setId, extendsSetId, root, parseCleanSheetBody(elementPath)));
                return;
            }
            ClassMapping cm = pure
                    ? parsePureClassMappingBody(elementPath, setId, extendsSetId, root)
                    : parseRelationalClassMappingBody(elementPath, setId, extendsSetId, root);
            expect(TokenType.BRACE_CLOSE);
            accum.classMappings.add(cm);
            return;
        }
        // Operation STORE-UNION captures its member sets (synthesized as a
        // projected UNION ALL); identification is by EXACT FQN — the engine
        // walker (OperationClassMappingParseTreeWalker) dispatches
        // union_* = STORE_UNION vs special_union_* = ROUTER_UNION vs
        // inheritance_*/merge_*, and the latter three have different
        // semantics (a contains-match would silently run a router union as
        // a store union). Non-store-union operations and AggregationAware
        // stay parse-and-skip so the surrounding mapping loads — a query
        // against the class stays LOUD at resolution ("no mapping for
        // class"). The skip is a BALANCED-BRACE consume: merge bodies carry
        // set LISTS + a validation lambda that the member-list parse would
        // choke on (audit F6 — one merge mapping sank its whole model).
        if (isIdentifierToken(peek()) && "Operation".equals(text())) {
            advance();
            expect(TokenType.BRACE_OPEN);
            String fn = parseQualifiedName();
            if ("meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_"
                    .equals(fn)) {
                // members are IMPLICIT (the mapped subclass sets)
                if (peek() == TokenType.PAREN_OPEN) {
                    skipBalancedBlock();
                }
                expect(TokenType.BRACE_CLOSE);
                accum.classMappings.add(new ClassMapping.Inheritance(
                        elementPath, setId, extendsSetId, root));
                return;
            }
            // union_ = STORE union (one SQL); special_union_ = ROUTER union
            // (per-member execution, results concatenated). ROW CONTENT is
            // identical — both synthesize as the member union here.
            if (!"meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_"
                    .equals(fn)
                    && !"meta::pure::router::operations::special_union_OperationSetImplementation_1__SetImplementation_MANY_"
                            .equals(fn)) {
                int depth = 1;
                while (depth > 0 && !atEnd()) {
                    if (peek() == TokenType.BRACE_OPEN) {
                        depth++;
                    } else if (peek() == TokenType.BRACE_CLOSE) {
                        depth--;
                    }
                    advance();
                }
                return;
            }
            List<String> members = new ArrayList<>();
            if (peek() == TokenType.PAREN_OPEN) {
                advance();
                if (peek() != TokenType.PAREN_CLOSE) {
                    members.add(parseIdentifier());
                    while (match(TokenType.COMMA)) {
                        members.add(parseIdentifier());
                    }
                }
                expect(TokenType.PAREN_CLOSE);
            }
            expect(TokenType.BRACE_CLOSE);
            if (members.isEmpty()) {
                throw error("Operation union for '" + elementPath
                        + "' names no member sets");
            }
            accum.classMappings.add(new ClassMapping.Union(
                    elementPath, setId, extendsSetId, root, members));
            return;
        }
        // Relation (~func-sourced) class mapping: the class's extent is the
        // relation a zero-arg function returns; properties bind to columns.
        if (isIdentifierToken(peek()) && "Relation".equals(text())) {
            advance();
            expect(TokenType.BRACE_OPEN);
            accum.classMappings.add(parseRelationFunctionBody(
                    elementPath, setId, extendsSetId, root));
            expect(TokenType.BRACE_CLOSE);
            return;
        }
        // XStore ASSOCIATION mapping: each end binds via a pure boolean
        // expression over $this/$that (Relation-function mapping keys).
        if (isIdentifierToken(peek()) && "XStore".equals(text())) {
            advance();
            expect(TokenType.BRACE_OPEN);
            List<AssociationMapping.Cross.XStoreProperty> xprops = new ArrayList<>();
            while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
                String prop = parseIdentifier();
                String srcSet = null;
                String tgtSet = null;
                if (match(TokenType.BRACKET_OPEN)) {
                    srcSet = parseIdentifier();
                    expect(TokenType.COMMA);
                    tgtSet = parseIdentifier();
                    expect(TokenType.BRACKET_CLOSE);
                }
                expect(TokenType.COLON);
                int exprStart = pos;
                int depth = 0;
                while (!atEnd()) {
                    TokenType t = peek();
                    if (t == TokenType.PAREN_OPEN || t == TokenType.BRACKET_OPEN
                            || t == TokenType.BRACE_OPEN) {
                        depth++;
                    } else if (t == TokenType.PAREN_CLOSE
                            || t == TokenType.BRACKET_CLOSE) {
                        depth--;
                    } else if (t == TokenType.BRACE_CLOSE) {
                        if (depth == 0) {
                            break;
                        }
                        depth--;
                    } else if (t == TokenType.COMMA && depth == 0) {
                        break;
                    }
                    advance();
                }
                List<com.legend.parser.spec.ValueSpecification> exprs =
                        SpecParser.parseCodeBlock(tokens.slice(exprStart, pos));
                if (exprs.size() != 1) {
                    throw error("XStore property '" + prop + "' must be a single"
                            + " expression; got " + exprs.size() + " statements");
                }
                xprops.add(new AssociationMapping.Cross.XStoreProperty(
                        prop, srcSet, tgtSet, exprs.get(0)));
                match(TokenType.COMMA);
            }
            expect(TokenType.BRACE_CLOSE);
            accum.associationMappings.add(new AssociationMapping.Cross(
                    elementPath, xprops));
            return;
        }
        // ModelJoin ASSOCIATION mapping: one typed lambda over the two ends
        if (isIdentifierToken(peek()) && "ModelJoin".equals(text())) {
            advance();
            expect(TokenType.BRACE_OPEN);
            int lamStart = pos;
            skipBalancedBlock();    // the {params | cond} lambda block
            List<com.legend.parser.spec.ValueSpecification> lam =
                    SpecParser.parseCodeBlock(tokens.slice(lamStart, pos));
            if (lam.size() != 1
                    || !(lam.get(0) instanceof com.legend.parser.spec.LambdaFunction lf)) {
                throw error("ModelJoin body for '" + elementPath
                        + "' must be a single typed lambda");
            }
            expect(TokenType.BRACE_CLOSE);
            accum.associationMappings.add(
                    new AssociationMapping.ModelJoin(elementPath, lf));
            return;
        }
        if (isIdentifierToken(peek()) && "AggregationAware".equals(text())) {
            advance();
            skipBalancedBlock();
            return;
        }
        throw error("unsupported class mapping type: '" + safeText() + "'");
    }

    /**
     * Body of a {@code : Relation} class mapping (opening brace consumed;
     * stops AT the closing brace): {@code ~func <ref>} then comma-separated
     * column bindings — {@code prop: COL}, {@code prop: 'QUOTED COL'}, or
     * mapping-local {@code +prop: Type[m]: COL} (XStore association keys).
     */
    private ClassMapping.RelationFunction parseRelationFunctionBody(String className,
            String setId, String extendsSetId, boolean root) {
        expect(TokenType.TILDE);
        String kw = parseIdentifier();
        if (!"func".equals(kw)) {
            throw error("expected ~func in Relation class mapping, got ~" + kw);
        }
        String ref = parseQualifiedName();
        if (peek() == TokenType.PAREN_OPEN) {
            // signature spelling f():Relation<Any>[1] — the FQN identifies
            // the function; the signature tokens are redundant here
            skipBalancedBlock();
            if (match(TokenType.COLON)) {
                parseQualifiedName();
                skipTypeArgsAndMultiplicity();
            }
        }
        List<ClassMapping.RelationFunction.Col> cols = new ArrayList<>();
        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            boolean local = match(TokenType.PLUS);
            String prop = parseIdentifier();
            expect(TokenType.COLON);
            if (local) {
                parseQualifiedName();       // declared local-property type
                skipTypeArgsAndMultiplicity();
                expect(TokenType.COLON);
            }
            // enum-decoded column: prop: EnumerationMapping <id> : COL
            String enumId = null;
            if (isIdentifierToken(peek()) && "EnumerationMapping".equals(text())) {
                advance();
                enumId = parseIdentifier();
                expect(TokenType.COLON);
            }
            String col = parseIdentifier();
            cols.add(new ClassMapping.RelationFunction.Col(prop, col, local, enumId));
            if (!match(TokenType.COMMA)) {
                break;
            }
        }
        return new ClassMapping.RelationFunction(className, setId, extendsSetId,
                root, ref, cols);
    }

    /** Consume {@code <...>} type arguments and a {@code [..]} multiplicity, if present. */
    private void skipTypeArgsAndMultiplicity() {
        if (peek() == TokenType.LESS_THAN) {
            int depth = 0;
            while (!atEnd()) {
                if (peek() == TokenType.LESS_THAN) {
                    depth++;
                } else if (peek() == TokenType.GREATER_THAN) {
                    depth--;
                    if (depth == 0) {
                        advance();
                        break;
                    }
                }
                advance();
            }
        }
        if (peek() == TokenType.BRACKET_OPEN) {
            while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) {
                advance();
            }
            match(TokenType.BRACKET_CLOSE);
        }
    }

    /** Consume a balanced {@code {...}} or {@code (...)} block (strings skipped by the lexer). */
    private void skipBalancedBlock() {
        // the body opens with '{' (kind block) — consume to its close,
        // balancing every bracket kind
        int depth = 0;
        boolean started = false;
        while (!atEnd()) {
            TokenType t = peek();
            if (t == TokenType.BRACE_OPEN || t == TokenType.PAREN_OPEN
                    || t == TokenType.BRACKET_OPEN) {
                depth++;
                started = true;
            } else if (t == TokenType.BRACE_CLOSE || t == TokenType.PAREN_CLOSE
                    || t == TokenType.BRACKET_CLOSE) {
                depth--;
            }
            advance();
            if (started && depth == 0) {
                return;
            }
        }
    }

    /**
     * True if the kind block body (after the opening brace) is a clean-sheet
     * Pure expression rather than a legacy DSL body. Two-token lookahead, no
     * backtracking (CLEAN_SHEET_INVERSION §5.1): a legacy body opens with a
     * {@code ~command} ({@code ~mainTable}/{@code ~src}/&hellip;, each its own
     * token) or an {@code identifier ':'} property mapping; everything else is
     * a clean-sheet expression (a function ref or an inline body, distinguished
     * after parsing by {@link #parseCleanSheetBody}).
     */
    private boolean isCleanSheetBody() {
        if (peek() == TokenType.BRACE_CLOSE) return false;                 // {} — empty LEGACY body (extends inherits everything)
        if (isLegacyMappingCommand(peek())) return false;                  // ~mainTable / ~filter / ~src / ...
        if (isIdentifierToken(peek()) && peek(1) == TokenType.COLON) return false;  // prop: legacy PM
        if (isIdentifierToken(peek()) && "scope".equals(text())
                && peek(1) == TokenType.PAREN_OPEN) return false;          // scope([db]...)( legacy PMs )
        if (isIdentifierToken(peek()) && peek(1) == TokenType.PAREN_OPEN) return false;  // prop( embedded )
        if (isIdentifierToken(peek()) && peek(1) == TokenType.BRACKET_OPEN) return false;  // prop[setId]: / prop[setId](
        if (peek() == TokenType.PLUS) return false;                        // +localProp:
        return true;
    }

    /** The {@code ~command} tokens that open a legacy class-mapping body. */
    private static boolean isLegacyMappingCommand(TokenType t) {
        return t == TokenType.MAIN_TABLE_CMD
            || t == TokenType.FILTER_CMD
            || t == TokenType.DISTINCT_CMD
            || t == TokenType.GROUP_BY_CMD
            || t == TokenType.PRIMARY_KEY_CMD
            || t == TokenType.SRC_CMD;
    }

    /**
     * Parse a clean-sheet kind-block body (the opening brace already consumed)
     * into a {@link Realization}, then consume the closing
     * brace. The body is parsed as a Pure expression and classified by shape
     * (CLEAN_SHEET_INVERSION §5.3):
     * <ul>
     *   <li>a single bare element reference ({@code acme::funcs::personMapping})
     *       &mdash; a {@link PackageableElementPtr} &mdash; is a function
     *       reference ({@link Realization.Ref}, Door 1);</li>
     *   <li>anything else (a call/pipeline/lambda) is an inline body
     *       ({@link Realization.Inline}, Door 3), lifted to a
     *       function by Phase E.</li>
     * </ul>
     */
    private Realization parseCleanSheetBody(String elementPath) {
        int bodyStart = pos;
        int depth = 1;
        while (!atEnd() && depth > 0) {
            TokenType t = peek();
            if (t == TokenType.BRACE_OPEN) depth++;
            else if (t == TokenType.BRACE_CLOSE) depth--;
            if (depth > 0) advance();
        }
        List<ValueSpecification> body = SpecParser.parseCodeBlock(tokens.slice(bodyStart, pos));
        expect(TokenType.BRACE_CLOSE);
        if (body.isEmpty()) {
            throw error("clean-sheet mapping binding for '" + elementPath
                + "' has an empty body");
        }
        return realizationOf(body);
    }

    /**
     * Parse the body of a {@code Relational} class mapping (the {@code { ... }}
     * after the {@code : Relational} prefix). The opening brace has already
     * been consumed by the caller; we stop at the matching close brace.
     *
     * <h3>Grammar (legend-engine canonical, from
     * {@code RelationalParserGrammar.g4:244-251})</h3>
     * <pre>
     *   classMapping : mappingFilter?       // ~filter    (0..1)
     *                  DISTINCT_CMD?        // ~distinct  (0..1)
     *                  mappingGroupBy?      // ~groupBy   (0..1)
     *                  mappingPrimaryKey?   // ~primaryKey (0..1)
     *                  mappingMainTable?    // ~mainTable (0..1)
     *                  (propertyMapping (COMMA propertyMapping)*)?
     * </pre>
     *
     * <p><b>Order is fixed.</b> Each {@code ~command} appears at most once,
     * in the sequence shown. Writing {@code ~mainTable} before
     * {@code ~primaryKey} is a grammar violation (even though it reads
     * more naturally to an English speaker). Legend-engine's ANTLR
     * parser enforces the order; we match. Engine-lite is more lax
     * (any order), but that is an engine-lite-ism, not canonical.
     *
     * <p><b>{@code ~mainTable} is optional.</b> When present, its
     * database becomes the default scope for unqualified table/column
     * references in property-mapping bodies (the "scope-info" mechanism
     * in legend-engine's {@code ScopeInfo}). When absent, every
     * reference in property bodies must carry its own {@code [DB]}
     * qualifier or the downstream resolver will fail.
     *
     * <p>{@code ~filter}, {@code ~groupBy}, {@code ~primaryKey} each
     * parse their expressions BEFORE the main-table scope is known
     * (the grammar orders them before {@code mappingMainTable}), so
     * their expressions must self-qualify. {@link #parseDbOperation}
     * accepts a {@code null} scope and defers the error to the
     * identifier-resolution layer.
     */
    private ClassMapping parseRelationalClassMappingBody(
            String className, String setId, String extendsSetId, boolean root) {

        FilterMapping filter = null;
        boolean distinct = false;
        List<RelationalOperation> groupBy = new ArrayList<>();
        List<RelationalOperation> primaryKey = new ArrayList<>();
        LegacyMappingDefinition.TableReference mainTable = null;
        List<PropertyMapping> propertyMappings = new ArrayList<>();
        java.util.Map<String, String> savedTargetSets = currentTargetSets;
        currentTargetSets = new java.util.LinkedHashMap<>();

        LegacyMappingDefinition.TableReference savedScope = currentMappingScope;
        try {
            // 1. Optional ~filter (pre-mainTable; null scope).
            if (peek() == TokenType.FILTER_CMD) {
                advance();
                filter = parseViewFilterClause(null);
            }
            // 2. Optional ~distinct.
            if (peek() == TokenType.DISTINCT_CMD) {
                advance();
                distinct = true;
            }
            // 3. Optional ~groupBy.
            if (peek() == TokenType.GROUP_BY_CMD) {
                advance();
                expect(TokenType.PAREN_OPEN);
                if (peek() != TokenType.PAREN_CLOSE) {
                    groupBy.add(parseDbOperation(null));
                    while (match(TokenType.COMMA)) groupBy.add(parseDbOperation(null));
                }
                expect(TokenType.PAREN_CLOSE);
            }
            // 4. Optional ~primaryKey.
            if (peek() == TokenType.PRIMARY_KEY_CMD) {
                advance();
                expect(TokenType.PAREN_OPEN);
                if (peek() != TokenType.PAREN_CLOSE) {
                    primaryKey.add(parseDbOperation(null));
                    while (match(TokenType.COMMA)) primaryKey.add(parseDbOperation(null));
                }
                expect(TokenType.PAREN_CLOSE);
            }
            // 5. Optional ~mainTable. Sets the scope for subsequent
            //    property-mapping bodies.
            if (peek() == TokenType.MAIN_TABLE_CMD) {
                advance();
                mainTable = parseMappingMainTable();
                currentMappingScope = mainTable;
            }
            // 6. Zero-or-more property mappings. Property-mapping
            //    expressions may rely on mainTable's scope when
            //    mainTable is present; otherwise they must self-qualify.
            while (peek() != TokenType.BRACE_CLOSE) {
                // Any stray ~command here is an out-of-order grammar
                // violation. Legend-engine's ANTLR rejects; we match
                // with a specific diagnostic naming the offending
                // command and the canonical clause order, which is
                // more useful than "unexpected token ~primaryKey".
                TokenType t = peek();
                if (t == TokenType.FILTER_CMD || t == TokenType.DISTINCT_CMD
                        || t == TokenType.GROUP_BY_CMD
                        || t == TokenType.PRIMARY_KEY_CMD
                        || t == TokenType.MAIN_TABLE_CMD) {
                    throw error("'" + tildeCommandText(t)
                            + "' out of order or duplicated in Relational class"
                            + " mapping for '" + className + "'; legend-engine order is:"
                            + " ~filter, ~distinct, ~groupBy, ~primaryKey, ~mainTable,"
                            + " then property mappings");
                }
                if (isIdentifierToken(t) && "scope".equals(text())
                        && peek(1) == TokenType.PAREN_OPEN) {
                    parseScopeBlock(mainTable, propertyMappings);
                    match(TokenType.COMMA);
                    continue;
                }
                propertyMappings.add(parsePropertyMapping(mainTable));
                match(TokenType.COMMA);
            }
        } finally {
            currentMappingScope = savedScope;
        }

        java.util.Map<String, String> targetSets = currentTargetSets;
        currentTargetSets = savedTargetSets;
        return new ClassMapping.Relational(
                className, setId, extendsSetId, root,
                mainTable, filter, distinct, groupBy, primaryKey, propertyMappings,
                /* sourceUrl */ null, targetSets);
    }

    /** {@code prop[setId]} routings of the class mapping being parsed. */
    private java.util.Map<String, String> currentTargetSets;

    /**
     * Surface spelling of a {@code ~}-command token, for error messages.
     * Callers must pass only the five class-mapping tilde commands
     * &mdash; any other token is a programmer error, not a user error,
     * and throwing here surfaces that bug immediately rather than
     * producing a confusing diagnostic with a raw token name in it
     * (AGENTS.md invariant 4: NO FALLBACKS).
     */
    private static String tildeCommandText(TokenType t) {
        return switch (t) {
            case FILTER_CMD      -> "~filter";
            case DISTINCT_CMD    -> "~distinct";
            case GROUP_BY_CMD    -> "~groupBy";
            case PRIMARY_KEY_CMD -> "~primaryKey";
            case MAIN_TABLE_CMD  -> "~mainTable";
            default -> throw new IllegalStateException(
                    "tildeCommandText called with non-class-mapping token: " + t);
        };
    }

    /** {@code scope([db] path[.path2]) ( propertyMappings )} — see {@link ScopeBlock}. */
    private void parseScopeBlock(LegacyMappingDefinition.TableReference mainTable,
                                 List<PropertyMapping> out) {
        advance();   // 'scope'
        expect(TokenType.PAREN_OPEN);
        String db = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            db = parseQualifiedName();
            expect(TokenType.BRACKET_CLOSE);
        }
        String path = null;
        if (peek() != TokenType.PAREN_CLOSE) {
            path = parseRelationalIdentifier();
            while (peek() == TokenType.DOT) {
                advance();
                path = path + "." + parseRelationalIdentifier();
            }
        }
        expect(TokenType.PAREN_CLOSE);
        expect(TokenType.PAREN_OPEN);
        ScopeBlock saved = currentScopeBlock;
        currentScopeBlock = new ScopeBlock(db, path);
        try {
            out.add(parsePropertyMapping(mainTable));
            while (match(TokenType.COMMA)) {
                if (peek() == TokenType.PAREN_CLOSE) {
                    break;
                }
                out.add(parsePropertyMapping(mainTable));
            }
        } finally {
            currentScopeBlock = saved;
        }
        expect(TokenType.PAREN_CLOSE);
    }

    /**
     * Parse {@code [db::DB] TABLE_NAME} or {@code [db::DB] SCHEMA.TABLE}.
     * The {@code ~mainTable} command has already been consumed.
     */
    /**
     * An active {@code scope([db]path)(...)} block (real mapping grammar):
     * inside it, BARE identifiers are columns of the scoped table and
     * single-segment scopes prefix dotted refs as a schema. Resolution is
     * deferred to each use site — no store lookup at parse time.
     */
    private record ScopeBlock(String db, String path) { }

    private ScopeBlock currentScopeBlock;

    private LegacyMappingDefinition.TableReference parseMappingMainTable() {
        expect(TokenType.BRACKET_OPEN);
        String db = parseQualifiedName();
        expect(TokenType.BRACKET_CLOSE);
        String table = parseRelationalIdentifier();
        if (peek() == TokenType.DOT) {
            advance();
            table = table + "." + parseRelationalIdentifier();
        }
        return new LegacyMappingDefinition.TableReference(db, table);
    }

    /**
     * Parse one property-to-source binding inside a relational class mapping.
     * Nine grammar forms collapse onto the {@link PropertyMapping} sealed
     * type:
     * <pre>
     *   prop: TABLE.COL                                    → Column
     *   prop: [db::DB] TABLE.COL                           → Column (db override)
     *   prop: EnumerationMapping enumId : [db::DB] T.COL   → EnumeratedColumn
     *   prop: [db::DB] @J1 > @J2                           → Join
     *   prop: [db::DB] @J1 > @J2 | T.COL                   → JoinTerminalColumn
     *   prop: someFunc(T.A, T.B)                           → Expression
     *   prop ( subProp: ..., subProp: ... )                → Embedded            (B.4g)
     *   prop() Inline[setId]                               → InlineEmbedded      (B.4g)
     *   prop ( subs ) Otherwise ([setId]: body)            → OtherwiseEmbedded   (B.4g)
     *   +localProp: Type[mult]: body                       → LocalProperty       (B.4g)
     * </pre>
     *
     * <p>Dispatch order: the leading {@code +} marks a {@link
     * PropertyMapping.LocalProperty}. Otherwise a property name is
     * parsed; if the next token is {@code (}, this is an embedded family
     * (empty parens → Inline, body + {@code Otherwise} → Otherwise,
     * body alone → Embedded). The colon-introduced forms fall through
     * to {@link #parsePropertyMappingBody}.
     */
    private PropertyMapping parsePropertyMapping(LegacyMappingDefinition.TableReference mainTable) {
        if (peek() == TokenType.PLUS) {
            return parseLocalPropertyMapping(mainTable);
        }
        String propName = parseIdentifier();
        // prop[targetSetId] : ... routes the property to a SPECIFIC mapping
        // set of the target class; prop[sourceSetId, targetSetId] (union
        // member bodies) also names the OWNING set. RECORDED
        // (currentTargetSets): the normalizer poisons the class when the
        // target id names a non-root set — silently navigating the root
        // set instead would be wrong rows.
        String targetSetId = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            targetSetId = parseIdentifier();
            if (match(TokenType.COMMA)) {
                targetSetId = parseIdentifier();    // [source, TARGET]
            }
            expect(TokenType.BRACKET_CLOSE);
            // prop[id]( ... ) — an EMBEDDED mapping's [id] names its OWN
            // set implementation (extends-override bookkeeping), not a
            // target-set route; recording it as one would drop the property.
            if (peek() == TokenType.PAREN_OPEN) {
                targetSetId = null;
            } else if (currentTargetSets != null) {
                currentTargetSets.put(propName, targetSetId);
            }
        }
        if (peek() == TokenType.PAREN_OPEN) {
            return parseEmbeddedPropertyMapping(propName, mainTable);
        }
        expect(TokenType.COLON);
        PropertyMapping pm = parsePropertyMappingBody(propName, mainTable);
        // the route rides the PM itself (per-duplicate fidelity — the
        // name-keyed currentTargetSets map overwrites same-named routes)
        if (targetSetId != null && pm instanceof PropertyMapping.Join j) {
            pm = new PropertyMapping.Join(j.propertyName(), j.database(),
                    j.joins(), targetSetId);
        }
        return pm;
    }

    /**
     * Parse a local mapping property: {@code +name: Type[mult]: body}.
     * The leading {@code +} has not yet been consumed.
     */
    private PropertyMapping parseLocalPropertyMapping(
            LegacyMappingDefinition.TableReference mainTable) {
        expect(TokenType.PLUS);
        String propName = parseIdentifier();
        expect(TokenType.COLON);
        TypeExpression type = parseType();
        Multiplicity mult = parseMultiplicity();
        expect(TokenType.COLON);
        PropertyMapping body = parsePropertyMappingBody(propName, mainTable);
        return new PropertyMapping.LocalProperty(propName, type, mult, body);
    }

    /**
     * Parse the embedded family. The property name has been consumed; the
     * next token is the opening {@code (}.
     *
     * <ul>
     *   <li>{@code propName() Inline[setId]} &mdash; empty parens → inline ref</li>
     *   <li>{@code propName ( subs ) Otherwise ([setId]: body)} → otherwise</li>
     *   <li>{@code propName ( subs )} → embedded</li>
     * </ul>
     *
     * <p>Sub-mappings inherit the parent's scope (same main table), so
     * {@code mainTable} threads through unchanged.
     */
    private PropertyMapping parseEmbeddedPropertyMapping(
            String propName, LegacyMappingDefinition.TableReference mainTable) {
        expect(TokenType.PAREN_OPEN);
        if (peek() == TokenType.PAREN_CLOSE) {
            // Inline embedded: propName() Inline[setId]
            advance();
            expect(TokenType.INLINE);
            expect(TokenType.BRACKET_OPEN);
            String setId = parseIdentifier();
            expect(TokenType.BRACKET_CLOSE);
            return new PropertyMapping.InlineEmbedded(propName, setId);
        }
        // Non-empty body — parse sub-property mappings.
        List<PropertyMapping> subs = new ArrayList<>();
        subs.add(parsePropertyMapping(mainTable));
        while (match(TokenType.COMMA)) {
            if (peek() == TokenType.PAREN_CLOSE) break; // trailing comma tolerated
            subs.add(parsePropertyMapping(mainTable));
        }
        expect(TokenType.PAREN_CLOSE);

        if (peek() == TokenType.OTHERWISE) {
            advance();
            expect(TokenType.PAREN_OPEN);
            expect(TokenType.BRACKET_OPEN);
            String fallbackSetId = parseIdentifier();
            expect(TokenType.BRACKET_CLOSE);
            expect(TokenType.COLON);
            PropertyMapping fallback = parsePropertyMappingBody(propName, mainTable);
            expect(TokenType.PAREN_CLOSE);
            return new PropertyMapping.OtherwiseEmbedded(
                    propName, subs, fallbackSetId, fallback);
        }
        return new PropertyMapping.Embedded(propName, subs);
    }

    /**
     * Parse a property mapping body (everything after {@code propName:}).
     * Accepts a nullable {@code mainTable} so it can be shared between
     * class mappings (mainTable non-null, bare-id resolved to it) and
     * association mappings (mainTable null, bare-id forbidden, db must
     * come from explicit {@code [DB]} or a join's per-hop {@code [DB]}).
     *
     * <p>Engine parity: this is the same code path the FINOS engine
     * runs for both contexts &mdash; the only difference is the
     * presence/absence of ScopeInfo.
     */
    private PropertyMapping parsePropertyMappingBody(
            String propName, LegacyMappingDefinition.TableReference mainTable) {
        // Optional EnumerationMapping prefix:
        //   prop: EnumerationMapping enumId : ...
        //   prop: EnumerationMapping : ...        (ANONYMOUS — resolved by
        //                                          the property's enum type)
        String enumMappingId = null;
        boolean anonymousEnumMapping = false;
        if (peek() == TokenType.ENUMERATION_MAPPING) {
            advance();
            if (peek() == TokenType.COLON) {
                anonymousEnumMapping = true;
            } else {
                enumMappingId = parseIdentifier();
            }
            expect(TokenType.COLON);
        }

        // Optional leading [DB] qualifier.
        String explicitDb = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            explicitDb = parseQualifiedName();
            expect(TokenType.BRACKET_CLOSE);
        }
        String db = explicitDb != null
                ? explicitDb
                : currentScopeBlock != null && currentScopeBlock.db() != null
                        ? currentScopeBlock.db()
                        : (mainTable != null ? mainTable.database() : null);

        // Branch by what follows the optional [DB].
        if (peek() == TokenType.AT) {
            // Join navigation: @J1 > @J2 ( | terminalColumn )?
            requirePropertyMappingDb(propName, db, "join navigation");
            List<JoinChainElement> joins = parseJoinChain(db);
            if (match(TokenType.PIPE)) {
                RelationalOperation terminal = parseDbAtomicOperation(db);
                // EnumerationMapping over a JOIN-reached column (engine:
                // classificationType : EnumerationMapping m : [db]@J | T.C):
                // the terminal read decodes through the enum mapping
                return new PropertyMapping.JoinTerminalColumn(propName, db,
                        joins, terminal, enumMappingId,
                        enumMappingId != null || anonymousEnumMapping);
            }
            if (enumMappingId != null || anonymousEnumMapping) {
                throw error("EnumerationMapping on a join property mapping"
                        + " requires a terminal column (| T.C)");
            }
            return new PropertyMapping.Join(propName, db, joins);
        }

        // Column reference or structured expression.
        // Inside a scope block, a BARE identifier is a column of the
        // scoped table (scope([db]schemaA.firmSet)( legalName: name )).
        if (currentScopeBlock != null && currentScopeBlock.path() != null
                && isIdentifierToken(peek())
                // a single-quoted STRING here is a CONSTANT literal binding
                // (issuer( name: 'test' )), not a quoted column name —
                // relational identifiers quote with double quotes
                && peek() != TokenType.STRING
                && peek(1) != TokenType.DOT && peek(1) != TokenType.PAREN_OPEN
                && peek(1) != TokenType.ARROW) {
            requirePropertyMappingDb(propName, db, "scoped column reference");
            String column = parseRelationalIdentifier();
            if (enumMappingId != null || anonymousEnumMapping) {
                return new PropertyMapping.EnumeratedColumn(propName, enumMappingId,
                        db, currentScopeBlock.path(), column);
            }
            return new PropertyMapping.Column(propName, db, currentScopeBlock.path(), column);
        }
        // We peek a couple of tokens ahead to distinguish a simple
        // TABLE.COL column read from a function call / expression.
        if (looksLikeBareColumnRef()) {
            requirePropertyMappingDb(propName, db, "column reference");
            String tablePart = parseRelationalIdentifier();
            // Support SCHEMA.TABLE.COL — collapse first two into the table
            // string, matching engine's TablePtr handling.
            expect(TokenType.DOT);
            String second = parseRelationalIdentifier();
            String table;
            String column;
            if (peek() == TokenType.DOT) {
                advance();
                table = tablePart + "." + second;
                column = parseRelationalIdentifier();
            } else {
                // a SINGLE-segment scope is a schema prefix for dotted refs
                // (scope([db]productSchema)( name: synonymTable.NAME ))
                table = currentScopeBlock != null
                        && currentScopeBlock.path() != null
                        && !currentScopeBlock.path().contains(".")
                        ? currentScopeBlock.path() + "." + tablePart
                        : tablePart;
                column = second;
            }
            // An ARROW continues into a dynafunction chain over the column
            // (DATA->get('k', @String)) — an EXPRESSION binding, not a
            // plain column read.
            if (!atEnd() && peek() == TokenType.ARROW) {
                if (enumMappingId != null || anonymousEnumMapping) {
                    throw error("EnumerationMapping cannot be applied to an expression mapping");
                }
                RelationalOperation chained = parseArrowChain(
                        new RelationalOperation.ColumnRef(db, table, column), db);
                return new PropertyMapping.Expression(propName, chained);
            }
            if (enumMappingId != null || anonymousEnumMapping) {
                return new PropertyMapping.EnumeratedColumn(propName, enumMappingId, db, table, column);
            }
            return new PropertyMapping.Column(propName, db, table, column);
        }

        // Structured expression: parse via the existing relational
        // expression grammar with mapping-scope bare-id resolution.
        RelationalOperation expr = parseDbOperation(db);
        if (enumMappingId != null || anonymousEnumMapping) {
            // faithful parse; a loud resolution wall (enum decode over
            // constants/expressions is unbuilt)
            return new PropertyMapping.EnumeratedExpression(propName, enumMappingId, expr);
        }
        return new PropertyMapping.Expression(propName, expr);
    }

    /**
     * Lookahead: does the current position look like a bare {@code TABLE.COL}
     * column reference (as opposed to a function call or a more complex
     * expression)? A bare column ref is an identifier followed by a dot,
     * with no parenthesis after the first identifier and no comparison
     * operator before the dot.
     */
    private boolean looksLikeBareColumnRef() {
        if (!isIdentifierToken(peek()) && peek() != TokenType.QUOTED_STRING) {
            return false;
        }
        return peek(1) == TokenType.DOT;
    }

    /**
     * Parse a join chain in mapping property context:
     * {@code (joinType)? @J1 ( > (joinType)? ([DB])? @Jn )*}.
     * Per-hop optional {@code (joinType)} annotations are preserved on
     * {@link JoinChainElement#joinType()}. Per-hop optional {@code [DB]}
     * overrides the chain's default database.
     */

    /**
     * Enforce that a property mapping that needs an explicit database (join
     * navigations and column references) has one. In class-mapping context
     * the main table supplies it implicitly; in association-mapping context
     * the user must write {@code [db::DB]}.
     */
    private void requirePropertyMappingDb(String propName, String db, String kind) {
        if (db == null) {
            throw error("property mapping '" + propName + "': " + kind
                    + " requires a database. Write `[db::DB] ...` "
                    + "or place the mapping inside a class mapping with ~mainTable.");
        }
    }

    /**
     * Parse the body of an {@code AssociationMapping ( ... )} block. The
     * {@code AssociationMapping} keyword has already been consumed by the
     * caller; this method handles the parenthesized comma-separated list
     * of property mappings and stops at the closing paren.
     *
     * <p>Engine grammar (per FINOS RelationalParseTreeWalker):
     * <pre>
     *   AssociationMapping ( propMapping (, propMapping)* )
     * </pre>
     * Each {@code propMapping} is either:
     * <pre>
     *   propName: body                            // unbracketed (most common)
     *   propName[srcSetId, dstSetId]: body        // disambiguating brackets
     * </pre>
     * where {@code body} is the same property-mapping body grammar used
     * for class mappings (joins, column refs, expressions). Association
     * mappings have no main table, so bare identifiers and unqualified
     * {@code T.COL} (without explicit {@code [DB]}) error out via the
     * existing {@code currentMappingScope == null} path.
     */
    private AssociationMapping parseAssociationMappingBody(String associationName) {
        expect(TokenType.PAREN_OPEN);
        List<AssociationPropertyMapping> propertyMappings = new ArrayList<>();
        if (peek() != TokenType.PAREN_CLOSE) {
            propertyMappings.add(parseAssociationPropertyMapping());
            while (match(TokenType.COMMA)) {
                propertyMappings.add(parseAssociationPropertyMapping());
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new AssociationMapping.Relational(associationName, propertyMappings);
    }

    /**
     * Parse one association property mapping entry. Handles the optional
     * {@code [srcSetId, dstSetId]} disambiguating brackets and delegates
     * the rest to {@link #parsePropertyMappingBody} with a null
     * {@code mainTable} (no scope &mdash; bare identifiers must error).
     */
    private AssociationPropertyMapping parseAssociationPropertyMapping() {
        String propName = parseIdentifier();
        String sourceSetId = null;
        String targetSetId = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            sourceSetId = parseIdentifier();
            expect(TokenType.COMMA);
            targetSetId = parseIdentifier();
            expect(TokenType.BRACKET_CLOSE);
        }
        expect(TokenType.COLON);
        PropertyMapping body = parsePropertyMappingBody(propName, null);
        return new AssociationPropertyMapping(sourceSetId, targetSetId, body);
    }

    /**
     * Parse an {@code EnumerationMapping} body. The {@code EnumerationMapping}
     * keyword has been consumed; this method handles the optional mapping
     * ID, the brace-delimited list of {@code enumValue: source} entries,
     * and stops at the closing brace.
     *
     * <p>Grammar:
     * <pre>
     *   EnumerationMapping mappingId? { enumValueEntry (, enumValueEntry)* }
     *   enumValueEntry  := enumValueName : sourceValueList
     *   sourceValueList := sourceValue                       // single value
     *                    | [ sourceValue (, sourceValue)* ]  // bracketed list
     *   sourceValue     := QUOTED_STRING                     // 'PENDING'
     *                    | INTEGER                           // 1
     *                    | qualifiedName . identifier        // other::Enum.VALUE
     * </pre>
     */
    private EnumerationMapping parseEnumerationMappingBody(String enumName) {
        // Optional mapping ID: present when the next token is an
        // identifier (or identifier-like keyword) AND not the body open.
        String mappingId = null;
        if (peek() != TokenType.BRACE_OPEN && isIdentifierToken(peek())) {
            mappingId = parseIdentifier();
        }
        expect(TokenType.BRACE_OPEN);
        List<EnumerationMapping.EnumValueMapping> valueMappings = new ArrayList<>();
        if (peek() != TokenType.BRACE_CLOSE) {
            valueMappings.add(parseEnumValueMapping());
            while (match(TokenType.COMMA)) {
                // Trailing comma tolerated (engine accepts it).
                if (peek() == TokenType.BRACE_CLOSE) break;
                valueMappings.add(parseEnumValueMapping());
            }
        }
        expect(TokenType.BRACE_CLOSE);
        return new EnumerationMapping(enumName, mappingId, valueMappings);
    }

    private EnumerationMapping.EnumValueMapping parseEnumValueMapping() {
        String enumValue = parseIdentifier();
        expect(TokenType.COLON);
        List<EnumerationMapping.SourceValue> sources = new ArrayList<>();
        if (match(TokenType.BRACKET_OPEN)) {
            if (peek() == TokenType.BRACKET_CLOSE) {
                throw error("EnumerationMapping value '" + enumValue
                        + "' must list at least one source value");
            }
            sources.add(parseEnumSourceValue());
            while (match(TokenType.COMMA)) sources.add(parseEnumSourceValue());
            expect(TokenType.BRACKET_CLOSE);
        } else {
            sources.add(parseEnumSourceValue());
        }
        return new EnumerationMapping.EnumValueMapping(enumValue, sources);
    }

    private EnumerationMapping.SourceValue parseEnumSourceValue() {
        if (peek() == TokenType.STRING) {
            String raw = text();
            advance();
            // Strip surrounding single quotes (engine grammar uses ' ').
            String value = raw.length() >= 2
                    && raw.charAt(0) == '\''
                    && raw.charAt(raw.length() - 1) == '\''
                    ? raw.substring(1, raw.length() - 1)
                    : raw;
            return new EnumerationMapping.SourceValue.StringValue(value);
        }
        if (peek() == TokenType.INTEGER) {
            long value = Long.parseLong(text());
            advance();
            return new EnumerationMapping.SourceValue.IntegerValue(value);
        }
        // Otherwise: cross-enum reference  pkg::Enum.VALUE
        String path = parseQualifiedName();
        expect(TokenType.DOT);
        String valueName = parseIdentifier();
        return new EnumerationMapping.SourceValue.EnumRef(path, valueName);
    }

    /**
     * Parse the body of a {@code Pure} (model-to-model) class mapping. The
     * opening brace has been consumed; we stop at the matching close brace.
     *
     * <p>Grammar:
     * <pre>
     *   pureClassMappingBody := ~src qualifiedName
     *                           (~filter pureExpression)?
     *                           propBinding (, propBinding)*
     *   propBinding         := identifier : pureExpression
     * </pre>
     *
     * <h2>Eager Pure-expression parsing</h2>
     * The filter expression and each property RHS are full Pure value
     * expressions. {@link #scanPureExpression} finds the token range for
     * each; {@link SpecParser#parse} then parses the sliced range into a
     * {@link ValueSpecification} which is stored directly on
     * {@link ClassMapping.Pure#filter()} / {@link ClassMapping.Pure.PropertyBinding#expression()}.
     */
    private ClassMapping parsePureClassMappingBody(
            String className, String setId, String extendsSetId, boolean root) {
        expect(TokenType.SRC_CMD);
        String sourceClass = parseQualifiedName();

        // Optional ~filter <pure expression> — runs until the first
        // property binding starts.
        ValueSpecification filter = null;
        if (peek() == TokenType.FILTER_CMD) {
            advance();
            int start = pos;
            scanPureExpression(/*stopOnPropertyBindingStart=*/ true);
            if (pos == start) {
                throw error("~filter clause in Pure class mapping for '"
                        + className + "' is empty");
            }
            filter = SpecParser.parse(tokens.slice(start, pos));
        }

        List<ClassMapping.Pure.PropertyBinding> bindings = new ArrayList<>();
        while (peek() != TokenType.BRACE_CLOSE && !atEnd()) {
            String propName = parseIdentifier();
            expect(TokenType.COLON);
            int start = pos;
            scanPureExpression(/*stopOnPropertyBindingStart=*/ false);
            if (pos == start) {
                throw error("Pure class mapping property '" + propName
                        + "' has an empty body");
            }
            ValueSpecification expression = SpecParser.parse(tokens.slice(start, pos));
            bindings.add(new ClassMapping.Pure.PropertyBinding(propName, expression));
            // Properties are comma-separated; trailing comma tolerated.
            match(TokenType.COMMA);
        }

        return new ClassMapping.Pure(
                className, setId, extendsSetId, root,
                sourceClass, filter, bindings);
    }

    /**
     * Advance past a Pure value expression, stopping at the first
     * top-level (depth-0) terminator. The caller then slices the consumed
     * range and hands it to {@link SpecParser} for eager parsing. The
     * matched tokens are NOT consumed beyond the expression itself; on
     * return, {@code pos} points at the terminator.
     *
     * <p>Terminators:
     * <ul>
     *   <li>{@code COMMA} or {@code BRACE_CLOSE} at depth 0
     *       &mdash; always end an expression.</li>
     *   <li>When {@code stopOnPropertyBindingStart} is true, also
     *       an identifier-followed-by-colon at depth 0 &mdash; used by
     *       {@code ~filter} where no separator precedes the next
     *       property binding.</li>
     * </ul>
     *
     * <p>Nesting is tracked through paren/brace/bracket pairs so embedded
     * commas (e.g. inside function calls or lambdas) do not terminate.
     */
    private void scanPureExpression(boolean stopOnPropertyBindingStart) {
        int depth = 0;
        while (!atEnd()) {
            TokenType t = peek();
            if (depth == 0) {
                if (t == TokenType.BRACE_CLOSE || t == TokenType.COMMA) break;
                if (stopOnPropertyBindingStart
                        && isIdentifierToken(t)
                        && peek(1) == TokenType.COLON) {
                    break;
                }
            }
            if (t == TokenType.PAREN_OPEN
                    || t == TokenType.BRACE_OPEN
                    || t == TokenType.BRACKET_OPEN) {
                depth++;
            } else if (t == TokenType.PAREN_CLOSE
                    || t == TokenType.BRACE_CLOSE
                    || t == TokenType.BRACKET_CLOSE) {
                if (depth == 0) break;
                depth--;
            }
            advance();
        }
    }

    // ============================================================
    // Relational expression sub-grammar
    // ============================================================

    /** {@code dbOperation: dbJoinOperation | dbBooleanOperation}. */
    private RelationalOperation parseDbOperation(String dbScope) {
        if (peek() == TokenType.AT
                || (peek() == TokenType.BRACKET_OPEN && lookAheadIsJoin())) {
            return parseDbJoinOperation(dbScope);
        }
        return parseDbBooleanOperation(dbScope);
    }

    /** {@code dbBooleanOperation: dbAtomicOperation (("and" | "or") dbOperation)?}. */
    private RelationalOperation parseDbBooleanOperation(String dbScope) {
        RelationalOperation left = parseDbAtomicOperation(dbScope);
        if (!atEnd() && isIdentifierToken(peek())
                && (peek() == TokenType.RELATIONAL_AND || peek() == TokenType.RELATIONAL_OR)) {
            LogicalOp op = LogicalOp.fromKeyword(text());
            advance();
            RelationalOperation right = parseDbOperation(dbScope);
            return new RelationalOperation.BooleanOp(left, op, right);
        }
        return left;
    }

    /**
     * {@code dbAtomicOperation: group | functionCall | columnRef | joinNav
     *                         | targetColumn | literal
     *                         (comparison | IS_NULL | IS_NOT_NULL)?}.
     */
    private RelationalOperation parseDbAtomicOperation(String dbScope) {
        RelationalOperation expr;

        if (peek() == TokenType.PAREN_OPEN) {
            advance();
            expr = new RelationalOperation.Group(parseDbOperation(dbScope));
            expect(TokenType.PAREN_CLOSE);
        } else if (peek() == TokenType.AT
                || (peek() == TokenType.BRACKET_OPEN && lookAheadIsJoin())) {
            expr = parseDbJoinOperation(dbScope);
        } else if (peek() == TokenType.BRACKET_OPEN) {
            // '[db]TABLE.COL' or '[db]SCHEMA.TABLE.COL' self-qualified
            // column reference. Required in legend-engine's canonical
            // ~groupBy / ~primaryKey clauses because those clauses are
            // parsed BEFORE ~mainTable (per grammar order), so no
            // enclosing scope is available. Real legend-engine .pure
            // corpora use this form heavily -- e.g.
            //   ~primaryKey([testDatabase]ABC.aName)
            // Without this arm, null-scope clauses cannot carry column
            // references at all, which would reject every real
            // legend-engine mapping that has ~primaryKey or ~groupBy.
            advance(); // consume '['
            String db = parseQualifiedName();
            expect(TokenType.BRACKET_CLOSE);
            String firstId = parseRelationalIdentifier();
            expect(TokenType.DOT);
            String second = parseRelationalIdentifier();
            if (peek() == TokenType.DOT) {
                advance();
                String third = parseRelationalIdentifier();
                // [db]SCHEMA.TABLE.COL
                expr = new RelationalOperation.ColumnRef(db, firstId + "." + second, third);
            } else {
                // [db]TABLE.COL
                expr = new RelationalOperation.ColumnRef(db, firstId, second);
            }
        } else if (peek() == TokenType.STRING) {
            expr = RelationalOperation.Literal.string(unquoteString(text()));
            advance();
        } else if (peek() == TokenType.INTEGER) {
            expr = RelationalOperation.Literal.integer(Long.parseLong(text()));
            advance();
        } else if (peek() == TokenType.FLOAT || peek() == TokenType.DECIMAL) {
            expr = RelationalOperation.Literal.decimal(Double.parseDouble(text()));
            advance();
        } else if (peek() == TokenType.MINUS) {
            // negative literals in relational operations (join conditions:
            // personExtensionTable.ID != -99999999)
            advance();
            if (peek() == TokenType.INTEGER) {
                expr = RelationalOperation.Literal.integer(-Long.parseLong(text()));
            } else if (peek() == TokenType.FLOAT || peek() == TokenType.DECIMAL) {
                expr = RelationalOperation.Literal.decimal(-Double.parseDouble(text()));
            } else {
                throw error("expected a numeric literal after '-'");
            }
            advance();
        } else if (peek() == TokenType.TARGET) {
            advance();
            expect(TokenType.DOT);
            expr = new RelationalOperation.TargetColumnRef(parseRelationalIdentifier());
        } else {
            String firstId = parseRelationalIdentifier();
            if (peek() == TokenType.PAREN_OPEN && !firstId.contains(".")) {
                advance(); // '('
                List<RelationalOperation> args = new ArrayList<>();
                if (peek() != TokenType.PAREN_CLOSE) {
                    args.add(parseDbFunctionArg(dbScope));
                    while (match(TokenType.COMMA)) args.add(parseDbFunctionArg(dbScope));
                }
                expect(TokenType.PAREN_CLOSE);
                expr = new RelationalOperation.FunctionCall(firstId, args);
            } else if (peek() == TokenType.DOT) {
                advance();
                String second = parseRelationalIdentifier();
                // Qualified T.COL: database is ambiguous at parse time
                // (T may live in the enclosing scope's database OR in any
                // of its includes). Leave db null; Phase D resolves it
                // using the enclosing element's scope. Matches engine,
                // which fills in TableAlias.database during binding, not
                // parsing.
                if (peek() == TokenType.DOT) {
                    advance();
                    String third = parseRelationalIdentifier();
                    expr = new RelationalOperation.ColumnRef(null, firstId + "." + second, third);
                } else {
                    expr = new RelationalOperation.ColumnRef(null, firstId, second);
                }
            } else if (currentScopeBlock != null && currentScopeBlock.path() != null) {
                // Bare identifier inside a scope BLOCK's expression
                // (scope([db]TRADE)( quantity: sum(QTY) )) — the scoped
                // table's column (engine ScopeInfo parity)
                expr = new RelationalOperation.ColumnRef(
                        currentScopeBlock.db(), currentScopeBlock.path(), firstId);
            } else if (currentMappingScope != null) {
                // Bare identifier in mapping context: unambiguously the
                // class mapping's main table's column. Both table AND
                // database are known at parse time (FINOS engine parity
                // via ScopeInfo). Eager resolution applies only here
                // because this is the ONLY case where parse-time info
                // determines the database without further lookup.
                expr = new RelationalOperation.ColumnRef(
                        currentMappingScope.database(),
                        currentMappingScope.table(),
                        firstId);
            } else {
                // Database-context bare identifier (Filter / Join /
                // MultiGrainFilter / view filter). FINOS engine rejects this
                // at parse time with this exact message
                // (RelationalParseTreeWalker.generateTableAlias). Match that
                // behavior: no implicit-table column refs in the AST.
                throw error("Missing table or alias for column '" + firstId + "'");
            }
        }

        expr = parseArrowChain(expr, dbScope);

        // Optional right side: comparison or null-test.
        if (!atEnd()) {
            TokenType next = peek();
            if (next == TokenType.EQUAL || next == TokenType.TEST_EQUAL
                    || next == TokenType.TEST_NOT_EQUAL || next == TokenType.LESS_THAN
                    || next == TokenType.GREATER_THAN || next == TokenType.LESS_OR_EQUAL
                    || next == TokenType.GREATER_OR_EQUAL || next == TokenType.NOT_EQUAL) {
                ComparisonOp op = ComparisonOp.fromToken(next);
                advance();
                RelationalOperation right = parseDbAtomicOperation(dbScope);
                expr = new RelationalOperation.Comparison(expr, op, right);
            } else if (next == TokenType.IS_NULL) {
                advance();
                expr = new RelationalOperation.IsNull(expr);
            } else if (next == TokenType.IS_NOT_NULL) {
                advance();
                expr = new RelationalOperation.IsNotNull(expr);
            }
        }
        return expr;
    }

    /**
     * Postfix ARROW chain: {@code col->get('k', @String)} is the dynafunction
     * with the receiver as its first argument (engine's method-call spelling
     * in mapping operations).
     */
    private RelationalOperation parseArrowChain(RelationalOperation receiver, String dbScope) {
        RelationalOperation expr = receiver;
        while (!atEnd() && peek() == TokenType.ARROW) {
            advance();
            String fn = parseRelationalIdentifier();
            expect(TokenType.PAREN_OPEN);
            List<RelationalOperation> chainArgs = new ArrayList<>();
            chainArgs.add(expr);
            if (peek() != TokenType.PAREN_CLOSE) {
                chainArgs.add(parseDbFunctionArg(dbScope));
                while (match(TokenType.COMMA)) {
                    chainArgs.add(parseDbFunctionArg(dbScope));
                }
            }
            expect(TokenType.PAREN_CLOSE);
            expr = new RelationalOperation.FunctionCall(fn, chainArgs);
        }
        return expr;
    }

    private RelationalOperation parseDbFunctionArg(String dbScope) {
        // '@Type' in ARGUMENT position is a TYPE REFERENCE (get(col,'k',@String))
        // — a bare join navigation never terminates at ',' or ')'.
        if (peek() == TokenType.AT) {
            int save = pos;
            advance();
            String name = parseQualifiedName();
            if (!atEnd() && (peek() == TokenType.COMMA || peek() == TokenType.PAREN_CLOSE)) {
                return new RelationalOperation.TypeRef(name);
            }
            pos = save;
        }
        return parseDbFunctionArgTail(dbScope);
    }

    private RelationalOperation parseDbFunctionArgTail(String dbScope) {
        // R4.4 prerequisite: distinguish [db]@joinName (a self-qualified
        // JoinNavigation) from [a, b, c] (an array literal). Without the
        // lookahead, every '[' was consumed as array-literal opening and
        // a function argument like concat([db::DB] @J | T.X, ...) failed
        // to parse. Defer to parseDbOperation when the bracket region is
        // a database qualifier on a join chain — or on a COLUMN REF: the
        // dynafunction corpus writes isNull([store::DB] TN1.VAL) with a
        // per-argument database bracket (the atomic grammar's
        // self-qualified arm parses it once we defer).
        if (peek() == TokenType.BRACKET_OPEN && !lookAheadIsJoin()
                && !lookAheadIsDbQualifiedColumn()) {
            advance();
            List<RelationalOperation> elements = new ArrayList<>();
            if (peek() != TokenType.BRACKET_CLOSE) {
                elements.add(parseDbFunctionArg(dbScope));
                while (match(TokenType.COMMA)) elements.add(parseDbFunctionArg(dbScope));
            }
            expect(TokenType.BRACKET_CLOSE);
            return new RelationalOperation.ArrayLiteral(elements);
        }
        return parseDbOperation(dbScope);
    }

    /**
     * {@code dbJoinOperation: ([DB])? @joinName ( '>' ( '(' joinType ')' )? ([DB])? @joinName )* ( '|' dbBooleanOperation )?}.
     */
    private RelationalOperation parseDbJoinOperation(String dbScope) {
        String dbName = null;
        if (peek() == TokenType.BRACKET_OPEN) {
            advance();
            dbName = parseQualifiedName();
            expect(TokenType.BRACKET_CLOSE);
        }
        String db = dbName != null ? dbName : dbScope;

        List<JoinChainElement> chain = parseJoinChain(db);

        RelationalOperation terminal = null;
        if (match(TokenType.PIPE)) {
            terminal = parseDbBooleanOperation(dbScope);
        }
        return new RelationalOperation.JoinNavigation(dbName, chain, terminal);
    }

    /**
     * Look ahead past a {@code [...]} bracket region and report whether the
     * very next token is {@code @} (i.e. this bracket region is a database
     * qualifier on a join chain, not a generic indexing operator).
     */
    private boolean lookAheadIsJoin() {
        int saved = pos;
        advance(); // skip '['
        while (!atEnd() && peek() != TokenType.BRACKET_CLOSE) advance();
        if (!atEnd()) advance(); // skip ']'
        boolean isJoin = !atEnd() && peek() == TokenType.AT;
        pos = saved;
        return isJoin;
    }

    /**
     * Lookahead: is this bracket a DATABASE QUALIFIER on a column reference
     * ({@code [store::DB] TN1.VAL}) rather than an array literal? A
     * qualifier bracket contains only a qualified name (identifiers joined
     * by {@code ::} — never dots, commas, or literals) and is followed by
     * an identifier; an array literal's contents or successor break one of
     * those. The {@code @}-successor case is {@link #lookAheadIsJoin}.
     */
    private boolean lookAheadIsDbQualifiedColumn() {
        int saved = pos;
        advance(); // skip '['
        boolean qualifiedName = !atEnd() && isIdentifierToken(peek());
        if (qualifiedName) {
            advance();
            while (!atEnd() && peek() == TokenType.PATH_SEPARATOR) {
                advance();
                if (atEnd() || !isIdentifierToken(peek())) {
                    qualifiedName = false;
                    break;
                }
                advance();
            }
        }
        boolean isQualifier = qualifiedName && !atEnd()
                && peek() == TokenType.BRACKET_CLOSE;
        if (isQualifier) {
            advance(); // skip ']'
            isQualifier = !atEnd() && (isIdentifierToken(peek())
                    || peek() == TokenType.QUOTED_STRING);
        }
        pos = saved;
        return isQualifier;
    }

    /**
     * Parse a relational identifier &mdash; either a quoted identifier
     * {@code "T_PERSON"} (returned without the surrounding quotes) or a bare
     * {@link #parseIdentifier()}. Quoted identifiers carry case-sensitive
     * names in source-dialect-style relational grammars.
     */
    private String parseRelationalIdentifier() {
        if (peek() == TokenType.QUOTED_STRING) {
            String quoted = text();
            advance();
            return quoted.length() >= 2 ? quoted.substring(1, quoted.length() - 1) : quoted;
        }
        return parseIdentifier();
    }

    // ============================================================
    // Source-text reconstruction (for lazy expression bodies)
    // ============================================================

    /**
     * Reconstruct the verbatim source text spanning tokens
     * {@code [startToken, endToken)}. Returns empty string if the range is
     * empty. Still used to capture {@code testSuites} blocks (D-3) and
     * embedded JSON-island raw text; expression bodies are now sliced and
     * handed to {@link SpecParser} instead of being kept as text.
     */
    private String reconstructText(int startToken, int endToken) {
        if (startToken >= endToken) return "";
        int charStart = tokens.start(startToken);
        int charEnd = tokens.end(endToken - 1);
        return tokens.source().substring(charStart, charEnd);
    }

    // ============================================================
    // Property declaration (regular)
    // ============================================================

    private ClassDefinition.PropertyDefinition parseProperty() {
        List<StereotypeApplication> stereotypes = parseStereotypes();
        List<TaggedValue> taggedValues = parseTaggedValues();
        String name = parseIdentifier();
        expect(TokenType.COLON);
        TypeExpression type = parseType();
        Multiplicity mult = parseMultiplicity();
        expect(TokenType.SEMI_COLON);
        return new ClassDefinition.PropertyDefinition(
                name, type, mult, stereotypes, taggedValues);
    }

    // ============================================================
    // Shared helpers (engine-parity)
    // ============================================================

    // -----------------------------------------------------------------
    // TokenStreamCursor accessors.
    //
    // Implementing the interface gives us the entire lexical layer
    // (peek/match/expect/consume/advance/text/safeText/textEquals/error,
    // parseIdentifier, parseQualifiedName) plus the type-expression
    // grammar (parseType / parseTypeArgument / parseMultiplicity and
    // their sub-grammars) as inherited defaults.
    //
    // Previously ElementParser carried its own private duplicates of
    // every primitive AND a bridge that allocated a standalone helper
    // class for each type expression. Both layers are now gone; the
    // interface is the single source of truth.
    // -----------------------------------------------------------------

    @Override
    public TokenStream tokens() {
        return tokens;
    }

    @Override
    public int pos() {
        return pos;
    }

    @Override
    public void setPos(int pos) {
        this.pos = pos;
    }

    /** {@code <<profile.stereotype, ...>>}; returns empty list if absent. */
    private List<StereotypeApplication> parseStereotypes() {
        if (peek() != TokenType.LESS_THAN || peek(1) != TokenType.LESS_THAN) {
            return List.of();
        }
        advance();
        advance();
        List<StereotypeApplication> result = new ArrayList<>();
        result.add(parseStereotype());
        while (match(TokenType.COMMA)) {
            result.add(parseStereotype());
        }
        expect(TokenType.GREATER_THAN);
        expect(TokenType.GREATER_THAN);
        return result;
    }

    private StereotypeApplication parseStereotype() {
        String profile = parseQualifiedName();
        expect(TokenType.DOT);
        String name = parseIdentifier();
        return new StereotypeApplication(profile, name);
    }

    /** {@code { profile.tag = 'value', ... }}; returns empty list if not a tag block. */
    private List<TaggedValue> parseTaggedValues() {
        if (peek() != TokenType.BRACE_OPEN) return List.of();
        if (!looksLikeTaggedValueBlock(tokens, pos)) return List.of();

        advance(); // skip {
        List<TaggedValue> result = new ArrayList<>();
        result.add(parseTaggedValue());
        while (match(TokenType.COMMA)) {
            result.add(parseTaggedValue());
        }
        expect(TokenType.BRACE_CLOSE);
        return result;
    }

    /**
     * Heuristic: does the {@code {} block starting at {@code bracePos} look
     * like a tagged-value block (e.g.
     * {@code {meta::pure::profiles::doc.doc = 'desc'}}) rather than the
     * element body?
     *
     * <p>Pattern matched: {@code '{' IDENT ('::' IDENT)* '.' IDENT '='}.
     * Shared by the parser (which dispatches on this when consuming tagged
     * values inline) and the shallow scanner in the IDE layer
     * ({@code com.legend.ide.ModelIndexer}), which
     * needs to skip a tagged-value block to reach the FQN. <strong>Both
     * must agree on the heuristic</strong>: if the scanner says yes but
     * the parser says no (or vice versa), token offsets drift and
     * downstream parses fail confusingly. Keeping the predicate in one
     * place prevents that drift.
     */
    public static boolean looksLikeTaggedValueBlock(TokenStream tokens, int bracePos) {
        int n = tokens.count();
        if (bracePos >= n || tokens.type(bracePos) != TokenType.BRACE_OPEN) return false;
        int p = bracePos + 1;
        if (p >= n || !IDENTIFIER_TOKENS.contains(tokens.type(p))) return false;
        while (p < n && IDENTIFIER_TOKENS.contains(tokens.type(p))) {
            p++;
            if (p >= n) return false;
            if (tokens.type(p) == TokenType.PATH_SEPARATOR) p++;
            else break;
        }
        if (p >= n || tokens.type(p) != TokenType.DOT) return false;
        p++;
        if (p >= n || !IDENTIFIER_TOKENS.contains(tokens.type(p))) return false;
        p++;
        return p < n && tokens.type(p) == TokenType.EQUAL;
    }

    private TaggedValue parseTaggedValue() {
        String profile = parseQualifiedName();
        expect(TokenType.DOT);
        String tag = parseIdentifier();
        expect(TokenType.EQUAL);
        String rawValue = consume(TokenType.STRING);
        String value = rawValue;
        if (value.startsWith("'") && value.endsWith("'") && value.length() >= 2) {
            value = value.substring(1, value.length() - 1);
        }
        return new TaggedValue(profile, tag, value);
    }

    // ============================================================
    // Token cursor: peek/peek(int)/text/safeText/textEquals/advance/
    // atEnd/match/expect/consume/error/parseIdentifier/parseQualifiedName
    // all live on TokenStreamCursor as default methods. The local
    // duplicates that used to sit here were removed when this class
    // started implementing the interface.
    // ============================================================
}
