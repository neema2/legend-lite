// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.model.Multiplicity;
import com.legend.model.TypeExpression;

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
 * Mapping-declaration grammar (B.4b — Relational class mappings), split
 * from ElementParser; shares its token cursor and scope state via {@code p}.
 */
final class MappingGrammarParser {

    private final ElementParser p;

    MappingGrammarParser(ElementParser p) {
        this.p = p;
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
    PackageableElement parseMapping() {
        p.expect(TokenType.MAPPING);
        String qualifiedName = p.parseQualifiedName();
        p.expect(TokenType.PAREN_OPEN);

        List<MappingInclude> includes = new ArrayList<>();
        MappingAccum accum = new MappingAccum();
        String testSuitesSource = null;

        while (p.peek() != TokenType.PAREN_CLOSE) {
            if (p.peek() == TokenType.INCLUDE) {
                p.advance();
                includes.add(parseMappingInclude());
                continue;
            }
            if (p.isIdentifierToken(p.peek()) && p.peek() == TokenType.MAPPING_TESTABLE_SUITES) {
                // B.4f / D-3: capture the testSuites block verbatim and
                // hand it to Phase C for lazy parsing. Engine grammar
                // wraps suites in '[ ... ]'.
                if (testSuitesSource != null) {
                    throw p.error("duplicate 'testSuites' block in Mapping '"
                            + qualifiedName + "'");
                }
                p.advance(); // consume 'testSuites' identifier
                p.expect(TokenType.COLON);
                TokenType opener = p.peek();
                if (opener != TokenType.BRACKET_OPEN && opener != TokenType.BRACE_OPEN) {
                    throw p.error("expected '[' or '{' after testSuites:, got " + opener);
                }
                int bs = p.pos;
                p.skipBalancedContent(opener,
                        opener == TokenType.BRACKET_OPEN
                                ? TokenType.BRACKET_CLOSE
                                : TokenType.BRACE_CLOSE);
                testSuitesSource = p.reconstructText(bs, p.pos);
                continue;
            }
            // Class/association/enumeration mappings share an outer shape
            // (`path: MappingType { body }`). parseMappingElement dispatches by
            // the MappingType keyword and (for a kind block) by the first body
            // p.tokens: a legacy DSL body (`~directive` / `prop:`) goes to the
            // legacy lists; a clean-sheet function-ref body (a bare FQN) goes
            // to the canonical binding lists (CLEAN_SHEET_INVERSION §5.1).
            parseMappingElement(accum);
        }
        p.expect(TokenType.PAREN_CLOSE);

        boolean hasLegacy = !accum.classMappings.isEmpty()
                || !accum.associationMappings.isEmpty();
        boolean hasCanonical = !accum.classBindings.isEmpty()
                || !accum.associationBindings.isEmpty();
        if (hasLegacy && hasCanonical) {
            throw p.error("Mapping '" + qualifiedName + "' mixes legacy DSL bodies with "
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
    static final class MappingAccum {
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
    MappingInclude parseMappingInclude() {
        String includedPath = p.parseQualifiedName();
        List<MappingInclude.StoreSubstitution> subs = new ArrayList<>();
        if (p.peek() == TokenType.BRACKET_OPEN) {
            p.advance();
            if (p.peek() != TokenType.BRACKET_CLOSE) {
                subs.add(parseStoreSubstitution());
                while (p.match(TokenType.COMMA)) subs.add(parseStoreSubstitution());
            }
            p.expect(TokenType.BRACKET_CLOSE);
        }
        return new MappingInclude(includedPath, subs);
    }

    MappingInclude.StoreSubstitution parseStoreSubstitution() {
        String original = p.parseQualifiedName();
        // engine grammar uses `->` between the two store names
        if (p.peek() != TokenType.ARROW) {
            throw p.error("expected '->' in store substitution but found " + p.peek());
        }
        p.advance();
        String replacement = p.parseQualifiedName();
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
    void parseMappingElement(MappingAccum accum) {
        boolean root = p.match(TokenType.STAR);
        String elementPath = p.parseQualifiedName();

        String setId = null;
        if (p.peek() == TokenType.BRACKET_OPEN) {
            p.advance();
            setId = p.parseIdentifier();
            p.expect(TokenType.BRACKET_CLOSE);
        }
        String extendsSetId = null;
        if (p.peek() == TokenType.EXTENDS) {
            p.advance();
            p.expect(TokenType.BRACKET_OPEN);
            extendsSetId = p.parseIdentifier();
            p.expect(TokenType.BRACKET_CLOSE);
        }
        p.expect(TokenType.COLON);

        TokenType mappingTypeToken = p.peek();
        if (mappingTypeToken == TokenType.ENUMERATION_MAPPING) {
            if (root) {
                throw p.error("Enumeration mappings cannot be marked root (the leading '*' "
                        + "is only valid for class mappings)");
            }
            if (setId != null || extendsSetId != null) {
                throw p.error("Enumeration mappings do not accept [setId] or extends [parentId]");
            }
            p.advance(); // consume EnumerationMapping keyword
            accum.enumerationMappings.add(parseEnumerationMappingBody(elementPath));
            return;
        }
        // Clean-sheet: AssociationMapping is its own kind tag (not nested inside
        // Relational). Body is a bare predicate-function FQN.
        if (mappingTypeToken == TokenType.ASSOCIATION_MAPPING) {
            if (root) {
                throw p.error("Association mappings cannot be marked root (the leading '*' "
                        + "is only valid for class mappings)");
            }
            if (setId != null || extendsSetId != null) {
                throw p.error("Association mappings do not accept [setId] or extends [parentId]");
            }
            p.advance(); // consume AssociationMapping keyword
            p.expect(TokenType.BRACE_OPEN);
            accum.associationBindings.add(new MappingDefinition.AssociationBinding(
                    elementPath, parseCleanSheetBody(elementPath)));
            return;
        }
        if (mappingTypeToken == TokenType.RELATIONAL || mappingTypeToken == TokenType.PURE_MAPPING) {
            boolean pure = mappingTypeToken == TokenType.PURE_MAPPING;
            p.advance();
            p.expect(TokenType.BRACE_OPEN);
            // Legacy nested AssociationMapping (`Relational { AssociationMapping (...) }`).
            // The ENGINE grammar tolerates both a leading '*' and a [setId]
            // on the header (corpus: testInheritanceRelationalMilestoned's
            // *Vehicle_VehicleOwner, modelJoins' Trade_LegalEntity
            // [trade_legal]) — the star is meaningless for associations and
            // the id names the mapping element; neither changes binding
            // semantics here (bindings key by association FQN), so both are
            // ACCEPTED and dropped, matching the reference parser.
            if (!pure && p.peek() == TokenType.ASSOCIATION_MAPPING) {
                p.advance(); // consume AssociationMapping keyword
                AssociationMapping am = parseAssociationMappingBody(elementPath);
                p.expect(TokenType.BRACE_CLOSE);
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
            p.expect(TokenType.BRACE_CLOSE);
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
        if (p.isIdentifierToken(p.peek()) && "Operation".equals(p.text())) {
            p.advance();
            p.expect(TokenType.BRACE_OPEN);
            String fn = p.parseQualifiedName();
            if ("meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_"
                    .equals(fn)) {
                // members are IMPLICIT (the mapped subclass sets)
                if (p.peek() == TokenType.PAREN_OPEN) {
                    skipBalancedBlock();
                }
                p.match(TokenType.SEMI_COLON);
                p.expect(TokenType.BRACE_CLOSE);
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
                while (depth > 0 && !p.atEnd()) {
                    if (p.peek() == TokenType.BRACE_OPEN) {
                        depth++;
                    } else if (p.peek() == TokenType.BRACE_CLOSE) {
                        depth--;
                    }
                    p.advance();
                }
                return;
            }
            List<String> members = new ArrayList<>();
            if (p.peek() == TokenType.PAREN_OPEN) {
                p.advance();
                if (p.peek() != TokenType.PAREN_CLOSE) {
                    members.add(p.parseIdentifier());
                    while (p.match(TokenType.COMMA)) {
                        members.add(p.parseIdentifier());
                    }
                }
                p.expect(TokenType.PAREN_CLOSE);
            }
            // the corpus writes union_...(a, b); with a trailing semicolon
            p.match(TokenType.SEMI_COLON);
            p.expect(TokenType.BRACE_CLOSE);
            if (members.isEmpty()) {
                throw p.error("Operation union for '" + elementPath
                        + "' names no member sets");
            }
            accum.classMappings.add(new ClassMapping.Union(
                    elementPath, setId, extendsSetId, root, members));
            return;
        }
        // Relation (~func-sourced) class mapping: the class's extent is the
        // relation a zero-arg function returns; properties bind to columns.
        if (p.isIdentifierToken(p.peek()) && "Relation".equals(p.text())) {
            p.advance();
            p.expect(TokenType.BRACE_OPEN);
            accum.classMappings.add(parseRelationFunctionBody(
                    elementPath, setId, extendsSetId, root));
            p.expect(TokenType.BRACE_CLOSE);
            return;
        }
        // XStore ASSOCIATION mapping: each end binds via a pure boolean
        // expression over $this/$that (Relation-function mapping keys).
        if (p.isIdentifierToken(p.peek()) && "XStore".equals(p.text())) {
            parseXStoreAssociationMapping(accum, elementPath);
            return;
        }
        parseMappingElementRest(accum, elementPath, setId, extendsSetId, root);
    }

    /** {@code my::A: XStore { prop[src, tgt]: <boolean expr>, ... }}. */
    private void parseXStoreAssociationMapping(MappingAccum accum, String elementPath) {
        p.advance();
        p.expect(TokenType.BRACE_OPEN);
        List<AssociationMapping.Cross.XStoreProperty> xprops = new ArrayList<>();
            while (p.peek() != TokenType.BRACE_CLOSE && !p.atEnd()) {
                String prop = p.parseIdentifier();
                String srcSet = null;
                String tgtSet = null;
                if (p.match(TokenType.BRACKET_OPEN)) {
                    srcSet = p.parseIdentifier();
                    p.expect(TokenType.COMMA);
                    tgtSet = p.parseIdentifier();
                    p.expect(TokenType.BRACKET_CLOSE);
                }
                p.expect(TokenType.COLON);
                int exprStart = p.pos;
                int depth = 0;
                while (!p.atEnd()) {
                    TokenType t = p.peek();
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
                    } else if (depth == 0 && p.isIdentifierToken(t)
                            && p.peek(1) == TokenType.BRACKET_OPEN
                            && p.isIdentifierToken(p.peek(2))
                            && p.pos > exprStart) {
                        // the NEXT xprop's `name[src, tgt]:` head — the
                        // corpus's testMappingCrossStore misses a comma
                        // between entries
                        break;
                    }
                    p.advance();
                }
                List<com.legend.model.spec.ValueSpecification> exprs =
                        SpecParser.parseCodeBlock(p.tokens.slice(exprStart, p.pos));
                if (exprs.size() != 1) {
                    throw p.error("XStore property '" + prop + "' must be a single"
                            + " expression; got " + exprs.size() + " statements");
                }
                xprops.add(new AssociationMapping.Cross.XStoreProperty(
                        prop, srcSet, tgtSet, exprs.get(0)));
                if (!p.match(TokenType.COMMA)
                        && p.peek() != TokenType.BRACE_CLOSE && !p.atEnd()) {
                    // ENGINE PARITY (audit 21a §4b): the engine's XStore rule
                    // (`mappingLine (COMMA mappingLine)*`, no EOF anchor)
                    // COMPLETES at a missing comma and silently discards the
                    // remaining entries; the compiled mapping keeps only the
                    // entries before the gap (the corpus golden for
                    // crossStoreUnionTestMapping corroborates). Match the
                    // compiled model: drop the rest of the block.
                    int skipDepth = 0;
                    while (!p.atEnd()) {
                        TokenType t = p.peek();
                        if (t == TokenType.PAREN_OPEN || t == TokenType.BRACKET_OPEN
                                || t == TokenType.BRACE_OPEN) {
                            skipDepth++;
                        } else if (t == TokenType.PAREN_CLOSE
                                || t == TokenType.BRACKET_CLOSE) {
                            skipDepth--;
                        } else if (t == TokenType.BRACE_CLOSE) {
                            if (skipDepth == 0) {
                                break;
                            }
                            skipDepth--;
                        }
                        p.advance();
                    }
                    break;
                }
            }
            p.expect(TokenType.BRACE_CLOSE);
            accum.associationMappings.add(new AssociationMapping.Cross(
                    elementPath, xprops));
    }

    /** The non-XStore tail of a mapping element (guardrail split; the
     * branch order is unchanged). */
    private void parseMappingElementRest(MappingAccum accum, String elementPath,
            String setId, String extendsSetId, boolean root) {
        // ModelJoin ASSOCIATION mapping: one typed lambda over the two ends
        if (p.isIdentifierToken(p.peek()) && "ModelJoin".equals(p.text())) {
            p.advance();
            p.expect(TokenType.BRACE_OPEN);
            int lamStart = p.pos;
            skipBalancedBlock();    // the {params | cond} lambda block
            List<com.legend.model.spec.ValueSpecification> lam =
                    SpecParser.parseCodeBlock(p.tokens.slice(lamStart, p.pos));
            if (lam.size() != 1
                    || !(lam.get(0) instanceof com.legend.model.spec.LambdaFunction lf)) {
                throw p.error("ModelJoin body for '" + elementPath
                        + "' must be a single typed lambda");
            }
            p.expect(TokenType.BRACE_CLOSE);
            accum.associationMappings.add(
                    new AssociationMapping.ModelJoin(elementPath, lf));
            return;
        }
        if (p.isIdentifierToken(p.peek()) && "AggregationAware".equals(p.text())) {
            p.advance();
            skipBalancedBlock();
            return;
        }
        throw p.error("unsupported class mapping type: '" + p.safeText() + "'");
    }

    /**
     * Body of a {@code : Relation} class mapping (opening brace consumed;
     * stops AT the closing brace): {@code ~func <ref>} then comma-separated
     * column bindings — {@code prop: COL}, {@code prop: 'QUOTED COL'}, or
     * mapping-local {@code +prop: Type[m]: COL} (XStore association keys).
     */
    ClassMapping.RelationFunction parseRelationFunctionBody(String className,
            String setId, String extendsSetId, boolean root) {
        p.expect(TokenType.TILDE);
        String kw = p.parseIdentifier();
        if (!"func".equals(kw)) {
            throw p.error("expected ~func in Relation class mapping, got ~" + kw);
        }
        String ref = p.parseQualifiedName();
        if (p.peek() == TokenType.PAREN_OPEN) {
            // signature spelling f():Relation<Any>[1] — the FQN identifies
            // the function; the signature p.tokens are redundant here
            skipBalancedBlock();
            if (p.match(TokenType.COLON)) {
                p.parseQualifiedName();
                skipTypeArgsAndMultiplicity();
            }
        }
        List<ClassMapping.RelationFunction.Col> cols =
                parseRelationCols(TokenType.BRACE_CLOSE);
        return new ClassMapping.RelationFunction(className, setId, extendsSetId,
                root, ref, cols);
    }

    /** Comma-separated Relation-mapping column bindings up to (not
     * consuming) {@code stop}: {@code prop: COL}, local {@code +prop},
     * enum-decoded, and EMBEDDED blocks {@code prop ( sub: COL, ... )}
     * (audit: one embedded block killed the whole relation setup file). */
    private List<ClassMapping.RelationFunction.Col> parseRelationCols(
            TokenType stop) {
        List<ClassMapping.RelationFunction.Col> cols = new ArrayList<>();
        while (p.peek() != stop && !p.atEnd()) {
            boolean local = p.match(TokenType.PLUS);
            String prop = p.parseIdentifier();
            // EMBEDDED block: prop ( sub: COL, ... ) — class-typed property
            // whose sub-bindings share this row
            if (p.peek() == TokenType.PAREN_OPEN) {
                p.advance();
                List<ClassMapping.RelationFunction.Col> sub =
                        parseRelationCols(TokenType.PAREN_CLOSE);
                p.expect(TokenType.PAREN_CLOSE);
                // INLINE-embedded: prop () Inline [setId] — delegates the
                // sub-bindings to a sibling set of this mapping
                String inlineSet = null;
                if (p.isIdentifierToken(p.peek()) && "Inline".equals(p.text())) {
                    p.advance();
                    p.expect(TokenType.BRACKET_OPEN);
                    inlineSet = p.parseIdentifier();
                    p.expect(TokenType.BRACKET_CLOSE);
                }
                cols.add(new ClassMapping.RelationFunction.Col(prop, null,
                        local, null, sub, inlineSet));
                if (!p.match(TokenType.COMMA)) {
                    break;
                }
                continue;
            }
            p.expect(TokenType.COLON);
            if (local) {
                p.parseQualifiedName();       // declared local-property type
                skipTypeArgsAndMultiplicity();
                p.expect(TokenType.COLON);
            }
            // enum-decoded column: prop: EnumerationMapping <id> : COL
            String enumId = null;
            if (p.isIdentifierToken(p.peek()) && "EnumerationMapping".equals(p.text())) {
                p.advance();
                enumId = p.parseIdentifier();
                p.expect(TokenType.COLON);
            }
            String col = p.parseIdentifier();
            cols.add(new ClassMapping.RelationFunction.Col(prop, col, local, enumId));
            if (!p.match(TokenType.COMMA)) {
                break;
            }
        }
        return cols;
    }

    /** Consume {@code <...>} type arguments and a {@code [..]} multiplicity, if present. */
    void skipTypeArgsAndMultiplicity() {
        if (p.peek() == TokenType.LESS_THAN) {
            int depth = 0;
            while (!p.atEnd()) {
                if (p.peek() == TokenType.LESS_THAN) {
                    depth++;
                } else if (p.peek() == TokenType.GREATER_THAN) {
                    depth--;
                    if (depth == 0) {
                        p.advance();
                        break;
                    }
                }
                p.advance();
            }
        }
        if (p.peek() == TokenType.BRACKET_OPEN) {
            while (!p.atEnd() && p.peek() != TokenType.BRACKET_CLOSE) {
                p.advance();
            }
            p.match(TokenType.BRACKET_CLOSE);
        }
    }

    /** Consume a balanced {@code {...}} or {@code (...)} block (strings skipped by the lexer). */
    void skipBalancedBlock() {
        // the body opens with '{' (kind block) — consume to its close,
        // balancing every bracket kind
        int depth = 0;
        boolean started = false;
        while (!p.atEnd()) {
            TokenType t = p.peek();
            if (t == TokenType.BRACE_OPEN || t == TokenType.PAREN_OPEN
                    || t == TokenType.BRACKET_OPEN) {
                depth++;
                started = true;
            } else if (t == TokenType.BRACE_CLOSE || t == TokenType.PAREN_CLOSE
                    || t == TokenType.BRACKET_CLOSE) {
                depth--;
            }
            p.advance();
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
    boolean isCleanSheetBody() {
        if (p.peek() == TokenType.BRACE_CLOSE) return false;                 // {} — empty LEGACY body (extends inherits everything)
        if (isLegacyMappingCommand(p.peek())) return false;                  // ~mainTable / ~filter / ~src / ...
        if (p.isIdentifierToken(p.peek()) && p.peek(1) == TokenType.COLON) return false;  // prop: legacy PM
        if (p.isIdentifierToken(p.peek()) && "scope".equals(p.text())
                && p.peek(1) == TokenType.PAREN_OPEN) return false;          // scope([db]...)( legacy PMs )
        if (p.isIdentifierToken(p.peek()) && p.peek(1) == TokenType.PAREN_OPEN) return false;  // prop( embedded )
        if (p.isIdentifierToken(p.peek()) && p.peek(1) == TokenType.BRACKET_OPEN) return false;  // prop[setId]: / prop[setId](
        if (p.peek() == TokenType.PLUS) return false;                        // +localProp:
        return true;
    }

    /** The {@code ~command} p.tokens that open a legacy class-mapping body. */
    static boolean isLegacyMappingCommand(TokenType t) {
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
    Realization parseCleanSheetBody(String elementPath) {
        int bodyStart = p.pos;
        int depth = 1;
        while (!p.atEnd() && depth > 0) {
            TokenType t = p.peek();
            if (t == TokenType.BRACE_OPEN) depth++;
            else if (t == TokenType.BRACE_CLOSE) depth--;
            if (depth > 0) p.advance();
        }
        List<ValueSpecification> body = SpecParser.parseCodeBlock(p.tokens.slice(bodyStart, p.pos));
        p.expect(TokenType.BRACE_CLOSE);
        if (body.isEmpty()) {
            throw p.error("clean-sheet mapping binding for '" + elementPath
                + "' has an empty body");
        }
        return p.realizationOf(body);
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
    ClassMapping parseRelationalClassMappingBody(
            String className, String setId, String extendsSetId, boolean root) {

        FilterMapping filter = null;
        boolean distinct = false;
        List<RelationalOperation> groupBy = new ArrayList<>();
        List<RelationalOperation> primaryKey = new ArrayList<>();
        LegacyMappingDefinition.TableReference mainTable = null;
        List<PropertyMapping> propertyMappings = new ArrayList<>();
        java.util.Map<String, String> savedTargetSets = p.currentTargetSets;
        java.util.Map<String, String> targetSets;
        p.currentTargetSets = new java.util.LinkedHashMap<>();

        LegacyMappingDefinition.TableReference savedScope = p.currentMappingScope;
        try {
            // 1. Optional ~filter (pre-mainTable; null scope).
            if (p.peek() == TokenType.FILTER_CMD) {
                p.advance();
                filter = p.relationalGrammar.parseViewFilterClause(null);
            }
            // 2. Optional ~distinct.
            if (p.peek() == TokenType.DISTINCT_CMD) {
                p.advance();
                distinct = true;
            }
            // 3. Optional ~groupBy.
            if (p.peek() == TokenType.GROUP_BY_CMD) {
                p.advance();
                p.expect(TokenType.PAREN_OPEN);
                if (p.peek() != TokenType.PAREN_CLOSE) {
                    groupBy.add(p.relationalGrammar.parseDbOperation(null));
                    while (p.match(TokenType.COMMA)) groupBy.add(p.relationalGrammar.parseDbOperation(null));
                }
                p.expect(TokenType.PAREN_CLOSE);
            }
            // 4. Optional ~primaryKey.
            if (p.peek() == TokenType.PRIMARY_KEY_CMD) {
                p.advance();
                p.expect(TokenType.PAREN_OPEN);
                if (p.peek() != TokenType.PAREN_CLOSE) {
                    primaryKey.add(p.relationalGrammar.parseDbOperation(null));
                    while (p.match(TokenType.COMMA)) primaryKey.add(p.relationalGrammar.parseDbOperation(null));
                }
                p.expect(TokenType.PAREN_CLOSE);
            }
            // 5. Optional ~mainTable. Sets the scope for subsequent
            //    property-mapping bodies.
            if (p.peek() == TokenType.MAIN_TABLE_CMD) {
                p.advance();
                mainTable = parseMappingMainTable();
                p.currentMappingScope = mainTable;
            }
            // 6. Zero-or-more property mappings. Property-mapping
            //    expressions may rely on mainTable's scope when
            //    mainTable is present; otherwise they must self-qualify.
            while (p.peek() != TokenType.BRACE_CLOSE) {
                // Any stray ~command here is an out-of-order grammar
                // violation. Legend-engine's ANTLR rejects; we match
                // with a specific diagnostic naming the offending
                // command and the canonical clause order, which is
                // more useful than "unexpected token ~primaryKey".
                TokenType t = p.peek();
                if (t == TokenType.FILTER_CMD || t == TokenType.DISTINCT_CMD
                        || t == TokenType.GROUP_BY_CMD
                        || t == TokenType.PRIMARY_KEY_CMD
                        || t == TokenType.MAIN_TABLE_CMD) {
                    throw p.error("'" + tildeCommandText(t)
                            + "' out of order or duplicated in Relational class"
                            + " mapping for '" + className + "'; legend-engine order is:"
                            + " ~filter, ~distinct, ~groupBy, ~primaryKey, ~mainTable,"
                            + " then property mappings");
                }
                if (p.isIdentifierToken(t) && "scope".equals(p.text())
                        && p.peek(1) == TokenType.PAREN_OPEN) {
                    parseScopeBlock(mainTable, propertyMappings);
                    p.match(TokenType.COMMA);
                    continue;
                }
                propertyMappings.add(parsePropertyMapping(mainTable));
                p.match(TokenType.COMMA);
            }
        } finally {
            p.currentMappingScope = savedScope;
            // restore INSIDE the finally with the scope (audit 15: the
            // out-of-finally restore was benign only because a throwing
            // parse discards the parser instance)
            targetSets = p.currentTargetSets;
            p.currentTargetSets = savedTargetSets;
        }
        return new ClassMapping.Relational(
                className, setId, extendsSetId, root,
                mainTable, filter, distinct, groupBy, primaryKey, propertyMappings,
                /* sourceUrl */ null, targetSets);
    }


    /**
     * Surface spelling of a {@code ~}-command token, for error messages.
     * Callers must pass only the five class-mapping tilde commands
     * &mdash; any other token is a programmer error, not a user error,
     * and throwing here surfaces that bug immediately rather than
     * producing a confusing diagnostic with a raw token name in it
     * (AGENTS.md invariant 4: NO FALLBACKS).
     */
    static String tildeCommandText(TokenType t) {
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

    /** {@code scope([db] path[.path2]) ( propertyMappings )} — see {@link ElementParser.ScopeBlock}. */
    void parseScopeBlock(LegacyMappingDefinition.TableReference mainTable,
                                 List<PropertyMapping> out) {
        p.advance();   // 'scope'
        p.expect(TokenType.PAREN_OPEN);
        String db = null;
        if (p.peek() == TokenType.BRACKET_OPEN) {
            p.advance();
            db = p.parseQualifiedName();
            p.expect(TokenType.BRACKET_CLOSE);
        }
        String path = null;
        if (p.peek() != TokenType.PAREN_CLOSE) {
            path = p.relationalGrammar.parseRelationalIdentifier();
            while (p.peek() == TokenType.DOT) {
                p.advance();
                path = path + "." + p.relationalGrammar.parseRelationalIdentifier();
            }
        }
        p.expect(TokenType.PAREN_CLOSE);
        p.expect(TokenType.PAREN_OPEN);
        ElementParser.ScopeBlock saved = p.currentScopeBlock;
        p.currentScopeBlock = new ElementParser.ScopeBlock(db, path);
        try {
            out.add(parsePropertyMapping(mainTable));
            while (p.match(TokenType.COMMA)) {
                if (p.peek() == TokenType.PAREN_CLOSE) {
                    break;
                }
                out.add(parsePropertyMapping(mainTable));
            }
        } finally {
            p.currentScopeBlock = saved;
        }
        p.expect(TokenType.PAREN_CLOSE);
    }

    /**
     * Parse {@code [db::DB] TABLE_NAME} or {@code [db::DB] SCHEMA.TABLE}.
     * The {@code ~mainTable} command has already been consumed.
     */

    LegacyMappingDefinition.TableReference parseMappingMainTable() {
        p.expect(TokenType.BRACKET_OPEN);
        String db = p.parseQualifiedName();
        p.expect(TokenType.BRACKET_CLOSE);
        String table = p.relationalGrammar.parseRelationalIdentifier();
        if (p.peek() == TokenType.DOT) {
            p.advance();
            table = table + "." + p.relationalGrammar.parseRelationalIdentifier();
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
    PropertyMapping parsePropertyMapping(LegacyMappingDefinition.TableReference mainTable) {
        if (p.peek() == TokenType.PLUS) {
            return parseLocalPropertyMapping(mainTable);
        }
        String propName = p.parseIdentifier();
        // prop[targetSetId] : ... routes the property to a SPECIFIC mapping
        // set of the target class; prop[sourceSetId, targetSetId] (union
        // member bodies) also names the OWNING set. RECORDED
        // (p.currentTargetSets): the normalizer poisons the class when the
        // target id names a non-root set — silently navigating the root
        // set instead would be wrong rows.
        String targetSetId = null;
        if (p.peek() == TokenType.BRACKET_OPEN) {
            p.advance();
            targetSetId = p.parseIdentifier();
            if (p.match(TokenType.COMMA)) {
                targetSetId = p.parseIdentifier();    // [source, TARGET]
            }
            p.expect(TokenType.BRACKET_CLOSE);
            // prop[id]( ... ) — an EMBEDDED mapping's [id] names its OWN
            // set implementation (extends-override bookkeeping), not a
            // target-set route; recording it as one would drop the property.
            if (p.peek() == TokenType.PAREN_OPEN) {
                targetSetId = null;
            } else if (p.currentTargetSets != null) {
                p.currentTargetSets.put(propName, targetSetId);
            }
        }
        if (p.peek() == TokenType.PAREN_OPEN) {
            return parseEmbeddedPropertyMapping(propName, mainTable);
        }
        p.expect(TokenType.COLON);
        PropertyMapping pm = parsePropertyMappingBody(propName, mainTable);
        // the route rides the PM itself (per-duplicate fidelity — the
        // name-keyed p.currentTargetSets map overwrites same-named routes)
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
    PropertyMapping parseLocalPropertyMapping(
            LegacyMappingDefinition.TableReference mainTable) {
        p.expect(TokenType.PLUS);
        String propName = p.parseIdentifier();
        p.expect(TokenType.COLON);
        TypeExpression type = p.parseType();
        Multiplicity mult = p.parseMultiplicity();
        p.expect(TokenType.COLON);
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
    PropertyMapping parseEmbeddedPropertyMapping(
            String propName, LegacyMappingDefinition.TableReference mainTable) {
        p.expect(TokenType.PAREN_OPEN);
        if (p.peek() == TokenType.PAREN_CLOSE) {
            // Inline embedded: propName() Inline[setId]
            p.advance();
            p.expect(TokenType.INLINE);
            p.expect(TokenType.BRACKET_OPEN);
            String setId = p.parseIdentifier();
            p.expect(TokenType.BRACKET_CLOSE);
            return new PropertyMapping.InlineEmbedded(propName, setId);
        }
        // Non-empty body — parse sub-property mappings.
        List<PropertyMapping> subs = new ArrayList<>();
        subs.add(parsePropertyMapping(mainTable));
        while (p.match(TokenType.COMMA)) {
            if (p.peek() == TokenType.PAREN_CLOSE) break; // trailing comma tolerated
            subs.add(parsePropertyMapping(mainTable));
        }
        p.expect(TokenType.PAREN_CLOSE);

        if (p.peek() == TokenType.OTHERWISE) {
            p.advance();
            p.expect(TokenType.PAREN_OPEN);
            p.expect(TokenType.BRACKET_OPEN);
            String fallbackSetId = p.parseIdentifier();
            p.expect(TokenType.BRACKET_CLOSE);
            p.expect(TokenType.COLON);
            PropertyMapping fallback = parsePropertyMappingBody(propName, mainTable);
            p.expect(TokenType.PAREN_CLOSE);
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
    PropertyMapping parsePropertyMappingBody(
            String propName, LegacyMappingDefinition.TableReference mainTable) {
        // Optional EnumerationMapping prefix:
        //   prop: EnumerationMapping enumId : ...
        //   prop: EnumerationMapping : ...        (ANONYMOUS — resolved by
        //                                          the property's enum type)
        String enumMappingId = null;
        boolean anonymousEnumMapping = false;
        if (p.peek() == TokenType.ENUMERATION_MAPPING) {
            p.advance();
            if (p.peek() == TokenType.COLON) {
                anonymousEnumMapping = true;
            } else {
                enumMappingId = p.parseIdentifier();
            }
            p.expect(TokenType.COLON);
        }

        // Optional leading [DB] qualifier.
        String explicitDb = null;
        if (p.peek() == TokenType.BRACKET_OPEN) {
            p.advance();
            explicitDb = p.parseQualifiedName();
            p.expect(TokenType.BRACKET_CLOSE);
        }
        String db = explicitDb != null
                ? explicitDb
                : p.currentScopeBlock != null && p.currentScopeBlock.db() != null
                        ? p.currentScopeBlock.db()
                        : (mainTable != null ? mainTable.database() : null);

        // Branch by what follows the optional [DB].
        if (p.peek() == TokenType.AT) {
            // Join navigation: @J1 > @J2 ( | terminalColumn )?
            requirePropertyMappingDb(propName, db, "join navigation");
            List<JoinChainElement> joins = p.relationalGrammar.parseJoinChain(db);
            if (p.match(TokenType.PIPE)) {
                RelationalOperation terminal = p.relationalGrammar.parseDbAtomicOperation(db);
                // EnumerationMapping over a JOIN-reached column (engine:
                // classificationType : EnumerationMapping m : [db]@J | T.C):
                // the terminal read decodes through the enum mapping
                return new PropertyMapping.JoinTerminalColumn(propName, db,
                        joins, terminal, enumMappingId,
                        enumMappingId != null || anonymousEnumMapping);
            }
            if (enumMappingId != null || anonymousEnumMapping) {
                throw p.error("EnumerationMapping on a join property mapping"
                        + " requires a terminal column (| T.C)");
            }
            return new PropertyMapping.Join(propName, db, joins);
        }

        // Column reference or structured expression.
        // Inside a scope block, a BARE identifier is a column of the
        // scoped table (scope([db]schemaA.firmSet)( legalName: name )).
        if (p.currentScopeBlock != null && p.currentScopeBlock.path() != null
                && p.isIdentifierToken(p.peek())
                // a single-quoted STRING here is a CONSTANT literal binding
                // (issuer( name: 'test' )), not a quoted column name —
                // relational identifiers quote with double quotes
                && p.peek() != TokenType.STRING
                && p.peek(1) != TokenType.DOT && p.peek(1) != TokenType.PAREN_OPEN
                && p.peek(1) != TokenType.ARROW) {
            requirePropertyMappingDb(propName, db, "scoped column reference");
            String column = p.relationalGrammar.parseRelationalIdentifier();
            if (enumMappingId != null || anonymousEnumMapping) {
                return new PropertyMapping.EnumeratedColumn(propName, enumMappingId,
                        db, p.currentScopeBlock.path(), column);
            }
            return new PropertyMapping.Column(propName, db, p.currentScopeBlock.path(), column);
        }
        // We peek a couple of p.tokens ahead to distinguish a simple
        // TABLE.COL column read from a function call / expression.
        if (looksLikeBareColumnRef()) {
            requirePropertyMappingDb(propName, db, "column reference");
            String tablePart = p.relationalGrammar.parseRelationalIdentifier();
            // Support SCHEMA.TABLE.COL — collapse first two into the table
            // string, matching engine's TablePtr handling.
            p.expect(TokenType.DOT);
            String second = p.relationalGrammar.parseRelationalIdentifier();
            String table;
            String column;
            if (p.peek() == TokenType.DOT) {
                p.advance();
                table = tablePart + "." + second;
                column = p.relationalGrammar.parseRelationalIdentifier();
            } else {
                // a SINGLE-segment scope is a schema prefix for dotted refs
                // (scope([db]productSchema)( name: synonymTable.NAME ))
                table = p.currentScopeBlock != null
                        && p.currentScopeBlock.path() != null
                        && !p.currentScopeBlock.path().contains(".")
                        ? p.currentScopeBlock.path() + "." + tablePart
                        : tablePart;
                column = second;
            }
            // An ARROW continues into a dynafunction chain over the column
            // (DATA->get('k', @String)) — an EXPRESSION binding, not a
            // plain column read.
            if (!p.atEnd() && p.peek() == TokenType.ARROW) {
                if (enumMappingId != null || anonymousEnumMapping) {
                    throw p.error("EnumerationMapping cannot be applied to an expression mapping");
                }
                RelationalOperation chained = p.relationalGrammar.parseArrowChain(
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
        RelationalOperation expr = p.relationalGrammar.parseDbOperation(db);
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
    boolean looksLikeBareColumnRef() {
        if (!p.isIdentifierToken(p.peek()) && p.peek() != TokenType.QUOTED_STRING) {
            return false;
        }
        return p.peek(1) == TokenType.DOT;
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
    void requirePropertyMappingDb(String propName, String db, String kind) {
        if (db == null) {
            throw p.error("property mapping '" + propName + "': " + kind
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
     * existing {@code p.currentMappingScope == null} path.
     */
    AssociationMapping parseAssociationMappingBody(String associationName) {
        p.expect(TokenType.PAREN_OPEN);
        List<AssociationPropertyMapping> propertyMappings = new ArrayList<>();
        if (p.peek() != TokenType.PAREN_CLOSE) {
            propertyMappings.add(parseAssociationPropertyMapping());
            while (p.match(TokenType.COMMA)) {
                propertyMappings.add(parseAssociationPropertyMapping());
            }
        }
        p.expect(TokenType.PAREN_CLOSE);
        return new AssociationMapping.Relational(associationName, propertyMappings);
    }

    /**
     * Parse one association property mapping entry. Handles the optional
     * {@code [srcSetId, dstSetId]} disambiguating brackets and delegates
     * the rest to {@link #parsePropertyMappingBody} with a null
     * {@code mainTable} (no scope &mdash; bare identifiers must error).
     */
    AssociationPropertyMapping parseAssociationPropertyMapping() {
        String propName = p.parseIdentifier();
        String sourceSetId = null;
        String targetSetId = null;
        if (p.peek() == TokenType.BRACKET_OPEN) {
            p.advance();
            sourceSetId = p.parseIdentifier();
            p.expect(TokenType.COMMA);
            targetSetId = p.parseIdentifier();
            p.expect(TokenType.BRACKET_CLOSE);
        }
        p.expect(TokenType.COLON);
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
    EnumerationMapping parseEnumerationMappingBody(String enumName) {
        // Optional mapping ID: present when the next token is an
        // identifier (or identifier-like keyword) AND not the body open.
        String mappingId = null;
        if (p.peek() != TokenType.BRACE_OPEN && p.isIdentifierToken(p.peek())) {
            mappingId = p.parseIdentifier();
        }
        p.expect(TokenType.BRACE_OPEN);
        List<EnumerationMapping.EnumValueMapping> valueMappings = new ArrayList<>();
        if (p.peek() != TokenType.BRACE_CLOSE) {
            valueMappings.add(parseEnumValueMapping());
            while (p.match(TokenType.COMMA)) {
                // Trailing comma tolerated (engine accepts it).
                if (p.peek() == TokenType.BRACE_CLOSE) break;
                valueMappings.add(parseEnumValueMapping());
            }
        }
        p.expect(TokenType.BRACE_CLOSE);
        return new EnumerationMapping(enumName, mappingId, valueMappings);
    }

    EnumerationMapping.EnumValueMapping parseEnumValueMapping() {
        String enumValue = p.parseIdentifier();
        p.expect(TokenType.COLON);
        List<EnumerationMapping.SourceValue> sources = new ArrayList<>();
        if (p.match(TokenType.BRACKET_OPEN)) {
            if (p.peek() == TokenType.BRACKET_CLOSE) {
                throw p.error("EnumerationMapping value '" + enumValue
                        + "' must list at least one source value");
            }
            sources.add(parseEnumSourceValue());
            while (p.match(TokenType.COMMA)) sources.add(parseEnumSourceValue());
            p.expect(TokenType.BRACKET_CLOSE);
        } else {
            sources.add(parseEnumSourceValue());
        }
        return new EnumerationMapping.EnumValueMapping(enumValue, sources);
    }

    EnumerationMapping.SourceValue parseEnumSourceValue() {
        if (p.peek() == TokenType.STRING) {
            String raw = p.text();
            p.advance();
            // Strip surrounding single quotes (engine grammar uses ' ').
            String value = raw.length() >= 2
                    && raw.charAt(0) == '\''
                    && raw.charAt(raw.length() - 1) == '\''
                    ? raw.substring(1, raw.length() - 1)
                    : raw;
            return new EnumerationMapping.SourceValue.StringValue(value);
        }
        if (p.peek() == TokenType.INTEGER) {
            long value = Long.parseLong(p.text());
            p.advance();
            return new EnumerationMapping.SourceValue.IntegerValue(value);
        }
        // Otherwise: cross-enum reference  pkg::Enum.VALUE
        String path = p.parseQualifiedName();
        p.expect(TokenType.DOT);
        String valueName = p.parseIdentifier();
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
    ClassMapping parsePureClassMappingBody(
            String className, String setId, String extendsSetId, boolean root) {
        p.expect(TokenType.SRC_CMD);
        String sourceClass = p.parseQualifiedName();

        // Optional ~filter <pure expression> — runs until the first
        // property binding starts.
        ValueSpecification filter = null;
        if (p.peek() == TokenType.FILTER_CMD) {
            p.advance();
            int start = p.pos;
            scanPureExpression(/*stopOnPropertyBindingStart=*/ true);
            if (p.pos == start) {
                throw p.error("~filter clause in Pure class mapping for '"
                        + className + "' is empty");
            }
            filter = SpecParser.parse(p.tokens.slice(start, p.pos));
        }

        List<ClassMapping.Pure.PropertyBinding> bindings = new ArrayList<>();
        while (p.peek() != TokenType.BRACE_CLOSE && !p.atEnd()) {
            // The three optional heads of an engine mappingLine
            // (M3CoreParser.g4:81-85) — RECORDED on the binding, never
            // dropped (audit 21a: the [targetSetId] route is the audit-11
            // wrong-rows shape mirrored on the Pure side; the normalizer
            // poisons routes/markers it cannot honor, loudly).
            // +localProp : Type[m] : expr — mapping-LOCAL property (engine
            // keeps it distinct from class properties).
            boolean local = p.match(TokenType.PLUS);
            String propName = p.parseIdentifier();
            // prop* : expr — the M2M EXPLOSION marker (index-aligned zip
            // fan-out over collection-valued sources; a row-count semantic).
            // Engine grammar places STAR after the [set] group / local
            // annotations; the corpus also spells it right after the name.
            boolean explode = p.match(TokenType.STAR);
            if (local) {
                p.expect(TokenType.COLON);
                p.parseType();
                p.parseMultiplicity();
            }
            // prop[targetSetId] : expr / prop[src, tgt] : expr — the M2M
            // target-set route (single id = target, engine walker parity).
            String sourceSetId = null;
            String targetSetId = null;
            if (p.peek() == TokenType.BRACKET_OPEN) {
                p.advance();
                String first = p.parseIdentifier();
                if (p.match(TokenType.COMMA)) {
                    sourceSetId = first;
                    targetSetId = p.parseIdentifier();
                } else {
                    targetSetId = first;
                }
                p.expect(TokenType.BRACKET_CLOSE);
            }
            explode = p.match(TokenType.STAR) || explode;
            p.expect(TokenType.COLON);
            int start = p.pos;
            scanPureExpression(/*stopOnPropertyBindingStart=*/ false);
            if (p.pos == start) {
                throw p.error("Pure class mapping property '" + propName
                        + "' has an empty body");
            }
            ValueSpecification expression = SpecParser.parse(p.tokens.slice(start, p.pos));
            bindings.add(new ClassMapping.Pure.PropertyBinding(propName, expression,
                    sourceSetId, targetSetId, explode, local));
            // Properties are comma-separated; trailing comma tolerated.
            p.match(TokenType.COMMA);
        }

        return new ClassMapping.Pure(
                className, setId, extendsSetId, root,
                sourceClass, filter, bindings);
    }

    /**
     * Advance past a Pure value expression, stopping at the first
     * top-level (depth-0) terminator. The caller then slices the consumed
     * range and hands it to {@link SpecParser} for eager parsing. The
     * matched p.tokens are NOT consumed beyond the expression itself; on
     * return, {@code p.pos} points at the terminator.
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
    void scanPureExpression(boolean stopOnPropertyBindingStart) {
        int depth = 0;
        while (!p.atEnd()) {
            TokenType t = p.peek();
            if (depth == 0) {
                if (t == TokenType.BRACE_CLOSE || t == TokenType.COMMA) break;
                if (stopOnPropertyBindingStart
                        && p.isIdentifierToken(t)
                        && p.peek(1) == TokenType.COLON) {
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
            p.advance();
        }
    }

}
