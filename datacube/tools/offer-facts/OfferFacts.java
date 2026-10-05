// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package offerfacts;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.TypedParameter;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.plan.UpstreamRelationType;
import com.legend.protocol.ProtocolReader;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * What DataCube may offer a column of each type, as legend-lite's COMPILER answers it
 * (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T5): DataCube's own queries (written by
 * {@code emit.ts} with the product's query builder) compiled, and for each probe column
 * each aggregate's answer -- the type the level query gives its measure, or refused; each
 * filter operator's -- whether its condition compiles (the query names the function the
 * operator means where Pure's name is ambiguous: query.ts's text "contains" is
 * {@code string::contains}, refused on a number); and
 * for each function the calculated-column editor offers, the function as the compiler
 * resolves its name and every declared overload of it (the signatures the editor shows),
 * once its example has compiled (the example is the proof: one that does not compile
 * fails generation).
 *
 * <p>Facts are keyed by the type the compiler gave the probe; two probes of one type must
 * agree (a fact of the type, not of the column), or generation fails.
 *
 * <p>Usage: {@code OfferFacts <model> <queries.tsv> <out.ts>}. Built by
 * {@code //datacube:offer_facts}; the committed copy is diff-tested
 * ({@code bazel run //datacube:update_generated}).
 */
public final class OfferFacts {

    private OfferFacts() {
    }

    /** One probe's answers: aggregate to result type (null: refused); operator to whether it compiles. */
    private record Facts(Map<String, String> aggregates, Map<String, Boolean> operators) {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            throw new IllegalArgumentException("usage: OfferFacts <model> <queries.tsv> <out.ts>");
        }
        String model = Files.readString(Path.of(args[0]), StandardCharsets.UTF_8);
        Map<String, String> declared = new LinkedHashMap<>();
        Map<String, Facts> byProbe = new LinkedHashMap<>();
        Map<String, String> compiled = new LinkedHashMap<>();
        Map<String, Calc> calcs = new LinkedHashMap<>();
        ModelContext ctx = Compiler.compileModel(model);
        for (String line : Files.readAllLines(Path.of(args[1]), StandardCharsets.UTF_8)) {
            if (line.isEmpty()) {
                continue;
            }
            String[] f = line.split("\t", 4);
            String kind = f[0];
            String probe = f[1];
            String name = f[2];
            String query = f[3];
            switch (kind) {
                case "column" -> declared.put(probe, name);
                case "type" -> {
                    for (Type.Column c : UpstreamRelationType.columns(Compiler.query(ctx, lambda(query)).resultType())) {
                        compiled.put(c.name(), UpstreamRelationType.typePath(c.type()));
                    }
                }
                case "agg" -> facts(byProbe, probe).aggregates().put(name, measureType(ctx, query));
                case "op" -> facts(byProbe, probe).operators().put(name, compiles(ctx, query));
                case "calc" -> calcs.put(name, calc(ctx, name, query));
                default -> throw new IllegalArgumentException("unknown line kind: " + kind);
            }
        }

        // the literals were built for the declared type: the compiler must have given it
        Map<String, Facts> byType = new TreeMap<>();
        Map<String, String> firstProbe = new LinkedHashMap<>();
        for (Map.Entry<String, String> d : declared.entrySet()) {
            String probe = d.getKey();
            String reported = compiled.get(probe);
            // keyed by the type's own name, as DataCube's plainType names it (Variant, not its path)
            String type = reported == null ? null : reported.substring(reported.lastIndexOf(':') + 1);
            if (!d.getValue().equals(type)) {
                throw new IllegalStateException("probe '" + probe + "' was built as " + d.getValue()
                        + " but the compiler typed it " + reported + ": correct emit.ts");
            }
            Facts facts = Objects.requireNonNull(byProbe.get(probe), probe);
            Facts before = byType.putIfAbsent(type, facts);
            if (before != null && !before.equals(facts)) {
                throw new IllegalStateException("probes '" + firstProbe.get(type) + "' and '" + probe
                        + "' are both " + type + " but the compiler answers them differently: "
                        + before + " / " + facts);
            }
            firstProbe.putIfAbsent(type, probe);
        }
        Files.writeString(Path.of(args[2]), render(byType, firstProbe, calcs), StandardCharsets.UTF_8);
    }

    /** A curated function as the compiler knows it: its path and every overload's declaration. */
    private record Calc(String path, List<String> signatures) {
    }

    /**
     * The function an example proves: its example compiled as a calculated column over
     * TRADES (a refusal fails generation), its name resolved as the compiler resolves a call
     * to it -- the one path the example's typed tree used, when the name has several -- and
     * each overload declared under that path.
     */
    private static Calc calc(ModelContext ctx, String name, String example) {
        // the example is text a person reads and types: parsed, the query around it built as protocol
        String extend = "|#>{offer::DB.TRADES}#->extend(~calc:" + "x|" + example + ")";
        TypedSpec typed;
        try {
            // typed once: its result type checked, its expression read
            com.legend.TypedQuery q = Compiler.query(ctx, extend);
            q.resultType();
            typed = q.expression();
        } catch (RuntimeException refused) {
            throw new IllegalStateException("the example of '" + name + "' does not compile -- fix it in"
                    + " src/calc.ts or stop offering the function: " + example + "\n" + refused.getMessage(), refused);
        }
        AppliedFunction call = (AppliedFunction) NameResolver.resolveQuery(
                new AppliedFunction(name, List.of(), List.of()));
        List<String> candidates = call.candidateFqns().isEmpty() ? List.of(call.function()) : call.candidateFqns();
        String path;
        if (candidates.size() == 1) {
            path = candidates.get(0);
        } else {
            List<String> used = new ArrayList<>();
            ArrayDeque<TypedSpec> work = new ArrayDeque<>(List.of(typed));
            while (!work.isEmpty()) {
                TypedSpec n = work.poll();
                if (n instanceof TypedNativeCall c && candidates.contains(c.callee().qualifiedName())
                        && !used.contains(c.callee().qualifiedName())) {
                    used.add(c.callee().qualifiedName());
                }
                work.addAll(n.children());
            }
            if (used.size() != 1) {
                throw new IllegalStateException("'" + name + "' names " + candidates + " and its example used "
                        + used + ": write an example that calls exactly one");
            }
            path = used.get(0);
        }
        List<String> signatures = new ArrayList<>();
        for (TypedFunction f : ctx.findFunction(path)) {
            signatures.add(signature(f));
        }
        if (signatures.isEmpty()) {
            throw new IllegalStateException("'" + name + "' resolved to " + path + ", which declares nothing");
        }
        return new Calc(path, List.copyOf(new java.util.TreeSet<>(signatures)));
    }

    /** A declaration as Pure writes it, without its package: {@code toUpper(source:String[1]):String[1]}. */
    private static String signature(TypedFunction f) {
        StringBuilder out = new StringBuilder(f.qualifiedName().substring(f.qualifiedName().lastIndexOf(':') + 1));
        if (!f.typeParameters().isEmpty() || !f.multiplicityParameters().isEmpty()) {
            out.append('<').append(String.join(",", f.typeParameters()));
            if (!f.multiplicityParameters().isEmpty()) {
                out.append('|').append(String.join(",", f.multiplicityParameters()));
            }
            out.append('>');
        }
        out.append('(');
        for (int i = 0; i < f.parameters().size(); i++) {
            TypedParameter p = f.parameters().get(i);
            out.append(i == 0 ? "" : ", ").append(p.name()).append(':').append(shown(p.type()))
                    .append(p.multiplicity().text());
        }
        return out.append("):").append(shown(f.returnType()))
                .append(f.returnMultiplicity().text()).toString();
    }

    /** A type by its own name, as Pure writes it in a signature (`Any`, not its path). */
    private static String shown(Type t) {
        String name = t.typeName();
        return name.indexOf('<') < 0 ? name.substring(name.lastIndexOf(':') + 1) : name;
    }

    private static Facts facts(Map<String, Facts> byProbe, String probe) {
        return byProbe.computeIfAbsent(probe, p -> new Facts(new LinkedHashMap<>(), new LinkedHashMap<>()));
    }

    private static LambdaFunction lambda(String json) {
        return ProtocolReader.lambda(json);
    }

    /** Whether the compiler accepts a query. */
    private static boolean compiles(ModelContext ctx, String query) {
        try {
            Compiler.query(ctx, lambda(query)).resultType();
            return true;
        } catch (RuntimeException refused) {
            return false;
        }
    }

    /** The measure's type in a level query, or null when the compiler refuses the query. */
    private static String measureType(ModelContext ctx, String query) {
        try {
            for (Type.Column c : UpstreamRelationType.columns(Compiler.query(ctx, lambda(query)).resultType())) {
                if (c.name().equals("m")) {
                    return UpstreamRelationType.typePath(c.type());
                }
            }
            throw new IllegalStateException("the level query answers no measure column: " + query);
        } catch (RuntimeException refused) {
            if (refused instanceof IllegalStateException) {
                throw refused;
            }
            return null;
        }
    }

    private static String render(Map<String, Facts> byType, Map<String, String> probe, Map<String, Calc> calcs) {
        StringBuilder out = new StringBuilder();
        out.append("// GENERATED by datacube/tools/offer-facts (emit.ts writes DataCube's own queries, OfferFacts.java\n")
                .append("// compiles them with legend-lite) -- DO NOT EDIT. Regenerate: bazel run //datacube:update_generated;\n")
                .append("// its diff test fails the build if this copy drifts from the compiler.\n")
                .append("// docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T5.\n\n")
                .append("import type { AggregateFn, FilterOperator } from '../snapshot.ts';\n\n")
                .append("/** What the compiler answered DataCube's queries over a column of one type. */\n")
                .append("export interface OfferFact {\n")
                .append("  /** Each aggregate: the type the level query gives the measure; null, refused. */\n")
                .append("  readonly aggregates: Readonly<Record<AggregateFn, string | null>>;\n")
                .append("  /** Each filter operator: whether its condition compiles. */\n")
                .append("  readonly operators: Readonly<Record<FilterOperator, boolean>>;\n")
                .append("}\n\n")
                .append("/** By the type the compiler gives a column (the probe it was measured on in a comment). */\n")
                .append("export const OFFER_FACTS: Readonly<Record<string, OfferFact>> = {\n");
        for (Map.Entry<String, Facts> e : byType.entrySet()) {
            out.append("  ").append(quote(e.getKey())).append(": { // ").append(probe.get(e.getKey())).append('\n');
            out.append("    aggregates: {\n");
            for (Map.Entry<String, String> a : e.getValue().aggregates().entrySet()) {
                out.append("      ").append(a.getKey()).append(": ")
                        .append(a.getValue() == null ? "null" : quote(a.getValue())).append(",\n");
            }
            out.append("    },\n    operators: {\n");
            for (Map.Entry<String, Boolean> o : e.getValue().operators().entrySet()) {
                out.append("      ").append(o.getKey()).append(": ").append(o.getValue()).append(",\n");
            }
            out.append("    },\n  },\n");
        }
        out.append("};\n\n")
                .append("/** Each function the calculated-column editor offers: its path, as the compiler resolves\n")
                .append(" *  the name, and every overload declared there. */\n")
                .append("export const CALC_FACTS: Readonly<Record<string, { readonly path: string; readonly signatures: readonly string[] }>> = {\n");
        for (Map.Entry<String, Calc> c : calcs.entrySet()) {
            out.append("  ").append(c.getKey()).append(": {\n    path: ").append(quote(c.getValue().path()))
                    .append(",\n    signatures: [\n");
            for (String sig : c.getValue().signatures()) {
                out.append("      ").append(text(sig)).append(",\n");
            }
            out.append("    ],\n  },\n");
        }
        return out.append("};\n").toString();
    }

    /** A signature as a TypeScript string: its characters are Pure's, none needing escape but the quote. */
    private static String text(String s) {
        if (s.indexOf('\'') >= 0 || s.indexOf('\\') >= 0 || s.indexOf('\n') >= 0) {
            throw new IllegalArgumentException("a signature to quote: " + s);
        }
        return "'" + s + "'";
    }

    private static String quote(String s) {
        if (!s.matches("[A-Za-z0-9_:]+")) {
            throw new IllegalArgumentException("not a plain name: " + s);
        }
        return "'" + s + "'";
    }
}
