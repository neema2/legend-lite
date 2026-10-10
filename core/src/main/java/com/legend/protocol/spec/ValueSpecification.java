package com.legend.protocol.spec;

/**
 * Root of all Pure expression AST nodes &mdash; the output of
 * {@link com.legend.parser.SpecParser}.
 *
 * <p>Sealed hierarchy mirroring the engine protocol
 * {@code org.finos.legend.engine.protocol.pure.m3.valuespecification.ValueSpecification},
 * implemented as Java records for immutability and pattern matching. Record
 * names and field names match the engine's verbatim (see
 * {@code core/README.md} &sect; "Element / Spec symmetry"); this maximises
 * test-corpus portability and lets downstream layers swap between core's
 * standalone parser output and the engine's protocol shapes by mechanical
 * renaming.
 *
 * <h2>Scope &mdash; C.1 (leaf expressions)</h2>
 * The {@code permits} clause currently lists only the variants emitted by
 * {@link com.legend.parser.SpecParser} as of Phase C.1: literals,
 * {@link Variable}, and {@link PureCollection}. Subsequent phases extend
 * the clause:
 * <ul>
 *   <li><strong>C.2</strong> &mdash; {@code AppliedProperty}, {@code AppliedFunction}</li>
 *   <li><strong>C.3</strong> &mdash; operator-desugared {@code AppliedFunction},
 *       {@code NewInstance}</li>
 *   <li><strong>C.4</strong> &mdash; {@code LambdaFunction}, code-block
 *       statement carrier (TBD)</li>
 *   <li><strong>C.5</strong> &mdash; milestoning carriers, {@code TypeAnnotation},
 *       {@code PackageableElementPtr}, {@code EnumValue}, {@code CDecimal},
 *       {@code CByteArray}, {@code UnitInstance}, {@code ColumnInstance}</li>
 * </ul>
 *
 * <p>Adding a variant in a later phase is an additive edit to this
 * {@code permits} clause plus a new record file in this package; existing
 * pattern matches surface the addition as an unhandled-case compiler
 * warning (or error in switch expressions), which is the desired behaviour.
 *
 * <h2>Pure data contract</h2>
 * The parser produces ONLY these types. No semantic interpretation, no
 * type checking, no name resolution. Function calls are generic
 * {@code AppliedFunction} nodes regardless of native vs user-defined
 * status &mdash; the compiler interprets them. Variables carry their
 * source-level name (without the {@code $} prefix); typing arrives later
 * if/when introduced via lambda parameter annotations.
 */
public sealed interface ValueSpecification permits
        AppliedFunction,
        AppliedProperty,
        CBoolean,
        CDate,
        CDecimal,
        CFloat,
        CInteger,
        CLatestDate,
        GraphFetchLiteral,
        QuotedTreeCall,
        QuotedGrammarCall,
        PathLiteral,
        SqlIsland,
        GqlIsland,
        TdsLiteral,
        CString,
        CByteArray,
        CTime,
        ColumnInstance,
        EnumValue,
        LambdaFunction,
        NewInstance,
        NewInstanceCast,
        PackageableElementPtr,
        PureCollection,
        TypeAnnotation,
        Variable {

    /**
     * The DIRECT {@link ValueSpecification} children, in rebuild order —
     * the ONE structural-recursion contract every spec walker shares.
     * Both switches are EXHAUSTIVE with no default arm: a new variant
     * fails compilation HERE until it declares its children, and every
     * walker inherits correct traversal.
     *
     * <p>{@link LambdaFunction} children are its BODY only — walkers
     * enter through bodies, never through the parameter Variables (the
     * shadow-stop doctrine; rewriters that substitute through lambdas
     * handle shadowing in their own explicit arm).
     */
    default java.util.List<ValueSpecification> children() {
        return switch (this) {
            case PathLiteral pl -> java.util.List.of(pl.desugared());
            case GraphFetchLiteral gf -> gf.arguments();
            case QuotedTreeCall q ->
                    java.util.List.of(q.original(), q.tree());
            case QuotedGrammarCall q -> {
                java.util.List<ValueSpecification> cs =
                        new java.util.ArrayList<>(q.functions().size() + 1);
                cs.add(q.original());
                cs.addAll(q.functions());
                yield cs;
            }
            case CBoolean ignored -> java.util.List.of();
            case CByteArray ignored -> java.util.List.of();
            case CDate ignored -> java.util.List.of();
            case CDecimal ignored -> java.util.List.of();
            case CFloat ignored -> java.util.List.of();
            case CInteger ignored -> java.util.List.of();
            case CLatestDate ignored -> java.util.List.of();
            case CString ignored -> java.util.List.of();
            case CTime ignored -> java.util.List.of();
            case EnumValue ignored -> java.util.List.of();
            case PackageableElementPtr ignored -> java.util.List.of();
            case Variable ignored -> java.util.List.of();
            case TypeAnnotation ignored -> java.util.List.of();
            case SqlIsland ignored -> java.util.List.of();
            case GqlIsland ignored2 -> java.util.List.of();
            case TdsLiteral tl -> java.util.List.of(tl.desugared());
            case AppliedFunction af -> af.parameters();
            case AppliedProperty ap -> java.util.List.of(ap.receiver());
            case LambdaFunction lf -> lf.body();
            case NewInstance ni -> ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::expression).toList().stream()
                    .map(KeyExpression::value)
                    .map(v -> (ValueSpecification) v).toList();
            case NewInstanceCast nc -> java.util.List.of(nc.src());
            case PureCollection pc -> pc.values();
            case ColSpecArray ca -> java.util.List.copyOf(ca.colSpecs());
            case ColSpec cs -> {
                java.util.List<ValueSpecification> out =
                        new java.util.ArrayList<>();
                if (cs.function1() != null) {
                    out.add(cs.function1());
                }
                if (cs.function2() != null) {
                    out.add(cs.function2());
                }
                out.addAll(cs.args());
                yield out;
            }
        };
    }

    /**
     * This node with its direct children replaced ({@code cs.size() ==
     * children().size()}, same order) — the other half of the recursion
     * contract; see {@link #children()}.
     */
    default ValueSpecification withChildren(
            java.util.List<ValueSpecification> cs) {
        return switch (this) {
            case CByteArray b -> b;                 // leaf
            case PathLiteral pl -> new PathLiteral(pl.startType(), pl.segments(),
                    (LambdaFunction) cs.get(0), pl.alias(), pl.hasDatedSegment(),
                    pl.pos(), pl.literalLength(), pl.lineStarts());
            case GraphFetchLiteral gf -> gf.withArguments(cs);
            case QuotedTreeCall q -> new QuotedTreeCall(
                    (AppliedFunction) cs.get(0), cs.get(1), q.pos());
            case QuotedGrammarCall q -> new QuotedGrammarCall(
                    (AppliedFunction) cs.get(0),
                    cs.subList(1, cs.size()).stream()
                            .map(LambdaFunction.class::cast).toList(),
                    q.pos());
            case CBoolean ignored -> this;
            case CDate ignored -> this;
            case CDecimal ignored -> this;
            case CFloat ignored -> this;
            case CInteger ignored -> this;
            case CLatestDate ignored -> this;
            case CString ignored -> this;
            case CTime ignored -> this;
            case EnumValue ignored -> this;
            case PackageableElementPtr ignored -> this;
            case Variable ignored -> this;
            case TypeAnnotation ignored -> this;
            case SqlIsland ignored -> this;
            case GqlIsland ignored2 -> this;
            case TdsLiteral tl -> new TdsLiteral(tl.tdsString(),
                    (AppliedFunction) cs.get(0), tl.pos());
            case AppliedFunction af -> af.withParameters(cs);
            // the owner class (older JSON's written detail) rides through; the position is W1.2's known defect
            case AppliedProperty ap ->
                    new AppliedProperty(cs.get(0), ap.property(), null, ap.ownerClass());
            case LambdaFunction lf ->
                    new LambdaFunction(lf.parameters(), cs);
            case NewInstance ni -> {
                java.util.List<NewInstance.KeyBinding> props =
                        new java.util.ArrayList<>();
                int i = 0;
                for (var e : ni.properties()) {
                    props.add(new NewInstance.KeyBinding(e.key(),
                            new KeyExpression(cs.get(i++),
                                    e.expression().isAdd(),
                                    e.expression().isLocal())));
                }
                yield new NewInstance(ni.className(), ni.typeArguments(),
                        ni.typeMultiplicityArguments(),
                        java.util.List.copyOf(props));
            }
            case NewInstanceCast nc -> new NewInstanceCast(nc.className(),
                    nc.typeArguments(), cs.get(0), nc.targetSetId());
            case PureCollection old -> new PureCollection(cs, old.pos());
            case ColSpecArray ignored -> new ColSpecArray(
                    cs.stream().map(x -> (ColSpec) x).toList());
            case ColSpec old -> {
                int i = 0;
                LambdaFunction f1 = old.function1() == null ? null
                        : (LambdaFunction) cs.get(i++);
                LambdaFunction f2 = old.function2() == null ? null
                        : (LambdaFunction) cs.get(i++);
                yield new ColSpec(old.name(), f1, f2, old.alias(),
                        cs.subList(i, cs.size()), old.qualified(), old.pos(),
                        old.colType(), old.colTypeMult());
            }
        };
    }

    /**
     * Identity-preserving one-level rewrite: {@code f} over each direct
     * child, reassembled through {@link #withChildren} only when a child
     * actually changed. Walkers recurse by calling this from their own
     * default arm — a variant they do not special-case still traverses
     * correctly by construction.
     */
    default ValueSpecification mapChildren(
            java.util.function.UnaryOperator<ValueSpecification> f) {
        java.util.List<ValueSpecification> cs = children();
        if (cs.isEmpty()) {
            return this;
        }
        java.util.List<ValueSpecification> rw =
                new java.util.ArrayList<>(cs.size());
        boolean same = true;
        for (ValueSpecification c : cs) {
            ValueSpecification r = f.apply(c);
            same = same && r == c;
            rw.add(r);
        }
        return same ? this : withChildren(rw);
    }
}
