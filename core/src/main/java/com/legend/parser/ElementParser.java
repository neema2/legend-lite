package com.legend.parser;

import com.legend.model.Multiplicity;
import com.legend.model.TypeExpression;
import com.legend.model.ParsedModel;
import com.legend.model.ImportScope;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationDefinition.AssociationEndDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.AssociationPropertyMapping;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassDefinition.ConstraintDefinition;
import com.legend.model.ClassDefinition.DerivedPropertyDefinition;
import com.legend.model.ClassDefinition.ParameterDefinition;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;
import com.legend.model.DatabaseDefinition;
import com.legend.model.EnumDefinition;
import com.legend.model.EnumerationMapping;
import com.legend.model.ClassMapping;
import com.legend.model.FilterMapping;
import com.legend.model.FilterPointer;
import com.legend.model.FunctionDefinition;
import com.legend.model.NativeFunctionDefinition;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.MappingDefinition;
import com.legend.model.Realization;
import com.legend.model.MappingInclude;
import com.legend.model.PropertyMapping;
import com.legend.model.spec.PackageableElementPtr;
import com.legend.model.JsonModelConnection;
import com.legend.model.PackageableElement;
import com.legend.model.ComparisonOp;
import com.legend.model.RelationalDataType;
import com.legend.model.JoinChainElement;
import com.legend.model.JoinType;
import com.legend.model.LogicalOp;
import com.legend.model.ProfileDefinition;
import com.legend.model.RelationalOperation;
import com.legend.model.RuntimeDefinition;
import com.legend.model.ServiceDefinition;
import com.legend.model.StereotypeApplication;
import com.legend.model.TaggedValue;
import com.legend.model.spec.ValueSpecification;

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

    final TokenStream tokens;
    int pos;

    /**
     * When non-null, parsing is inside a class mapping body and bare
     * identifiers in relational expressions resolve eagerly to columns of
     * this main table (matching FINOS engine's ScopeInfo behavior). When
     * null (Database context), bare identifiers throw per D-7. Set/cleared
     * around mapping-body parsing in {@link #parseRelationalClassMappingBody}.
     */
    LegacyMappingDefinition.TableReference currentMappingScope;

    /** {@code prop[setId]} routings of the class mapping being parsed. */
    java.util.Map<String, String> currentTargetSets;

    /**
     * An active {@code scope([db]path)(...)} block (real mapping grammar):
     * inside it, BARE identifiers are columns of the scoped table and
     * single-segment scopes prefix dotted refs as a schema. Resolution is
     * deferred to each use site — no store lookup at parse time.
     */
    record ScopeBlock(String db, String path) { }

    ScopeBlock currentScopeBlock;

    /** Grammar-section parsers sharing this parser's cursor and scope state. */
    final MappingGrammarParser mappingGrammar = new MappingGrammarParser(this);
    final RelationalGrammarParser relationalGrammar = new RelationalGrammarParser(this);

    ElementParser(TokenStream tokens) {
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
                mappingGrammar.skipBalancedBlock();    // (width=..., height=...)
            }
            if (peek() == TokenType.BRACE_OPEN) {
                mappingGrammar.skipBalancedBlock();    // { TypeView ... }
            }
            return true;
        }
        if (peek() == TokenType.NEW_SYMBOL) {
            advance();
            parseQualifiedName();       // the instance's type reference
            if (peek() != TokenType.PAREN_OPEN) {
                throw error("top-level ^Instance must be followed by (...)");
            }
            mappingGrammar.skipBalancedBlock();
            return true;
        }
        // a STRAY top-level closer: the corpus\u0027s own
        // m2m2rExecutionPlanTests.pure carries an unbalanced extra )
        // (opens=4, closes=5) that the engine tolerates; skip exactly
        // this token, never other junk
        if (peek() == TokenType.PAREN_CLOSE) {
            advance();
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
            case DATABASE -> relationalGrammar.parseDatabase();
            case MAPPING -> mappingGrammar.parseMapping();
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
    static Realization realizationOf(List<ValueSpecification> body) {
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
            mappingGrammar.skipBalancedBlock();
        }
        // optional [constraints] — instantiation-time; dropped
        if (peek() == TokenType.BRACKET_OPEN) {
            mappingGrammar.skipBalancedBlock();
        }
        return new com.legend.model.PrimitiveExtensionDefinition(fqn, base);
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
    void skipBalancedContent(TokenType open, TokenType close) {
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
    String unquoteString(String raw) {
        // Routes through THE shared decoder (which throws on malformed
        // input) — no legacy fallback: an unterminated literal at EOF used
        // to flow through with its leading quote intact (re-audit M2).
        return TokenStreamCursor.unquoteAndUnescape(raw, this);
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
    String reconstructText(int startToken, int endToken) {
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
        // property DEFAULT VALUE (real pure: prop: Boolean[1] = false;) —
        // parsed and DROPPED for now: defaults apply at ^construction,
        // which no supported path exercises for default-bearing classes.
        // A deliberate, documented divergence until construction demands it.
        if (match(TokenType.EQUAL)) {
            int depth = 0;
            while (!atEnd()) {
                TokenType t = peek();
                if (depth == 0 && t == TokenType.SEMI_COLON) {
                    break;
                }
                if (t == TokenType.PAREN_OPEN || t == TokenType.BRACKET_OPEN
                        || t == TokenType.BRACE_OPEN) {
                    depth++;
                } else if (t == TokenType.PAREN_CLOSE
                        || t == TokenType.BRACKET_CLOSE
                        || t == TokenType.BRACE_CLOSE) {
                    depth--;
                }
                advance();
            }
        }
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
    List<StereotypeApplication> parseStereotypes() {
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
    List<TaggedValue> parseTaggedValues() {
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
