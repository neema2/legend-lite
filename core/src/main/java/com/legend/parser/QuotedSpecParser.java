package com.legend.parser;

import com.legend.protocol.spec.ValueSpecification;

/**
 * Parses QUOTED CODE — the string inside
 * {@code compileLegendValueSpecification('#{...}#')} — into a spec tree
 * at the {@code payloadDialect} THE CALLER states. Pure mechanism: this
 * class holds no dialect and knows no regime. (Why callers differ is
 * their business, documented at their call sites: SpecParser passes the
 * host parse's own level, the corpus inliners pass LEGEND_ENGINE.)
 */
public final class QuotedSpecParser {

    private QuotedSpecParser() {
    }

    /** Both spellings of the grammar quote/eval native (real engine
     * devUtils.pure:18) — the parse-time recognizer's whole vocabulary. */
    private static final java.util.Set<String> COMPILE_LEGEND_GRAMMAR =
            java.util.Set.of("compileLegendGrammar",
                    "meta::legend::compileLegendGrammar");

    /** The quote/eval fold, AT PARSE TIME: when {@code call} is
     * {@code compileLegendValueSpecification('<literal>')} (either
     * spelling, argument a string literal or a literal {@code +} chain)
     * and the string parses as a tree literal, wrap it as the two-faced
     * {@link com.legend.protocol.spec.QuotedTreeCall} — wire face the
     * original call, pipeline face the parsed tree. The parse level is
     * LEGEND_ENGINE: the engine's own native routes the string through
     * {@code PureGrammarParser} (LegendCompile.java:57). Returns
     * {@code null} when the call is not foldable — it then stays a plain
     * {@code AppliedFunction}, typed {@code Any[1]} like the engine's
     * native. */
    public static com.legend.protocol.spec.@com.legend.base.Nullable QuotedTreeCall
            fold(com.legend.protocol.spec.AppliedFunction call,
                    GrammarLevel payloadDialect) {
        return fold(call, payloadDialect, name -> null);
    }

    /** As {@link #fold(com.legend.protocol.spec.AppliedFunction, GrammarLevel)}
     * with {@code constants} resolving a {@code $variable} operand to the
     * literal string its {@code let} bound IN THE SAME BODY (SpecParser's
     * scope stack): {@code let treeString = '#{' + ... ; compileLegend
     * ValueSpecification($treeString)} — the corpus's subType-family
     * spelling — is the same quote/eval literal as the inline form, and
     * folds at the same place. A name the scope does not carry (or one
     * shadowed by a lambda parameter) resolves to null = not foldable. */
    public static com.legend.protocol.spec.@com.legend.base.Nullable QuotedTreeCall
            fold(com.legend.protocol.spec.AppliedFunction call,
                    GrammarLevel payloadDialect,
                    java.util.function.Function<String, @com.legend.base.Nullable String>
                            constants) {
        if (!(call.function().equals("compileLegendValueSpecification")
                || call.function().equals(
                        "meta::legend::compileLegendValueSpecification"))
                || call.parameters().size() != 1) {
            return null;
        }
        String src = foldStringConcat(call.parameters().get(0), constants);
        if (src == null) {
            return null;
        }
        ValueSpecification tree = parseTree(src, payloadDialect);
        return tree == null ? null
                : new com.legend.protocol.spec.QuotedTreeCall(call, tree,
                        call.pos());
    }

    /** The GRAMMAR quote/eval fold, at parse time: when {@code call} is
     * {@code compileLegendGrammar('<literal>')} (either spelling; a
     * literal, a literal {@code +} chain, or a let-bound constant through
     * {@code constants}) and the payload parses as FUNCTIONS ONLY, wrap it
     * as the two-faced {@link com.legend.protocol.spec.QuotedGrammarCall}
     * — wire face the original call, pipeline face each function as its
     * lambda (declared parameters + body). The payload parses at
     * LEGEND_ENGINE: the engine's {@code LegendCompile} routes it through
     * the USER grammar. Any other payload (a model in a string) or a
     * parse refusal returns {@code null} — the call stays a plain
     * {@code AppliedFunction} and walls loudly as the unported native. */
    public static com.legend.protocol.spec.@com.legend.base.Nullable QuotedGrammarCall
            foldGrammar(com.legend.protocol.spec.AppliedFunction call,
                    java.util.function.Function<String, @com.legend.base.Nullable String>
                            constants) {
        if (!COMPILE_LEGEND_GRAMMAR.contains(call.function())
                || call.parameters().size() != 1) {
            return null;
        }
        String src = foldStringConcat(call.parameters().get(0), constants);
        if (src == null) {
            return null;
        }
        // the payload's records (the parser returns protocol only): functions, each as its lambda
        com.legend.protocol.Protocol.PureModelContextData parsed;
        try {
            parsed = PmcdParser.parse(src, GrammarLevel.LEGEND_ENGINE).model();
        } catch (ParseException notGrammar) {
            return null;
        }
        java.util.List<com.legend.protocol.spec.LambdaFunction> fns =
                new java.util.ArrayList<>();
        for (com.legend.protocol.Protocol.Element el : parsed.elements()) {
            if (el instanceof com.legend.protocol.Protocol.PSectionIndex) {
                continue;   // the index the engine appends, not an element
            }
            if (!(el instanceof com.legend.protocol.Protocol.PFunction fd)) {
                return null;
            }
            java.util.List<com.legend.protocol.spec.Variable> params =
                    new java.util.ArrayList<>(fd.parameters().size());
            for (com.legend.protocol.ParameterDefinition p : fd.parameters()) {
                params.add(new com.legend.protocol.spec.Variable(p.name(),
                        p.type(), p.multiplicity(), null));
            }
            fns.add(new com.legend.protocol.spec.LambdaFunction(params,
                    fd.body()));
        }
        return fns.isEmpty() ? null
                : new com.legend.protocol.spec.QuotedGrammarCall(call, fns,
                        call.pos());
    }

    /** Fold a literal string-concatenation chain ('a' + 'b' + ...) to its
     * value, or null when any operand is not a literal string (a
     * {@code $variable} operand is a literal when {@code constants} says
     * so — a let-bound constant of the same body). */
    public static @com.legend.base.Nullable String foldStringConcat(ValueSpecification v,
            java.util.function.Function<String, @com.legend.base.Nullable String> constants) {
        if (v instanceof com.legend.protocol.spec.CString cs) {
            return cs.value();
        }
        if (v instanceof com.legend.protocol.spec.Variable var) {
            return constants.apply(var.name());
        }
        if (v instanceof com.legend.protocol.spec.AppliedFunction pf
                && (pf.function().equals("plus")
                        || pf.function().equals("meta::pure::functions::math::plus"))) {
            StringBuilder sb = new StringBuilder();
            for (ValueSpecification p : pf.parameters()) {
                if (p instanceof com.legend.protocol.spec.PureCollection pc) {
                    for (ValueSpecification e : pc.values()) {
                        String part = foldStringConcat(e, constants);
                        if (part == null) {
                            return null;
                        }
                        sb.append(part);
                    }
                    continue;
                }
                String part = foldStringConcat(p, constants);
                if (part == null) {
                    return null;
                }
                sb.append(part);
            }
            return sb.toString();
        }
        return null;
    }

    /** Parse tree-literal SOURCE ({@code #{Class{...}}#}) to its
     * {@link com.legend.protocol.spec.GraphFetchLiteral}, or {@code null} when the
     * text is not a tree literal the grammar carries (callers keep their own loud
     * walls). */
    public static @com.legend.base.Nullable ValueSpecification parseTree(String source,
            GrammarLevel dialect) {
        try {
            ValueSpecification v = SpecParser.parse(source.trim(), dialect);
            return v instanceof com.legend.protocol.spec.GraphFetchLiteral ? v : null;
        } catch (ParseException notATree) {
            // ONLY a parse refusal means "not a tree literal" — any other
            // RuntimeException here is a parser defect and must surface, not
            // silently downgrade the call to an untyped fold (adversarial
            // audit F33: the broad catch masked exactly that regression once)
            return null;
        }
    }
}
