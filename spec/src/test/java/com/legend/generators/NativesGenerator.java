// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.model.FunctionDefinition;
import com.legend.model.NativeFunctionDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.parser.Dialect;
import com.legend.parser.ElementParser;
import com.legend.protocol.Multiplicity;
import com.legend.protocol.TypeExpression;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * GENERATES the signature text of every membership constant in {@code Pure.java}
 * from the pinned checkouts' declarations (the batch-5 contract: membership is
 * OURS — {@code native-membership.tsv} — the spelling is UPSTREAM's), and REFUSES,
 * loudly, a membership row no upstream declaration matches, an upstream type name
 * that does not resolve, and a rendered signature that does not round-trip.
 * Checked by {@link NativeSignatureGeneratorTest}.
 *
 * <pre>
 *   NativesGenerator &lt;legend-engine root&gt; &lt;legend-pure root&gt; &lt;native-membership.tsv&gt;
 *                    &lt;Pure.java&gt; &lt;output&gt;
 * </pre>
 */
public final class NativesGenerator {

    private NativesGenerator() {}

    public static void main(String[] args) throws IOException {
        if (args.length != 5) {
            throw new IllegalArgumentException("usage: NativesGenerator <legend-engine root> <legend-pure root>"
                    + " <native-membership.tsv> <Pure.java> <output>");
        }
        List<Row> rows = readMembership(Path.of(args[2]));
        Map<String, Decl> upstream = upstreamDeclarations(rows, Path.of(args[0]), Path.of(args[1]));
        Result r = compute(rows, upstream, Files.readAllLines(Path.of(args[3]), StandardCharsets.UTF_8), true);
        refuseOrphans(r.orphans());
        printSummary(r);
        Files.writeString(Path.of(args[4]), String.join("\n", r.lines()) + "\n", StandardCharsets.UTF_8);
    }

    /** A membership constant's declaration line in Pure.java: the text
     *  inside {@code signature("…")} is the generated part; the line's
     *  position and the comments around it are ours. */
    static final Pattern CONSTANT_LINE = Pattern.compile(
            "^(\\s*public static final NativeFunctionDefinition (\\w+) = signature\\(\")(.*?)(\"\\);\\s*(?://.*)?)$");

    /** Shrink-only: membership rows whose signature is not upstream's.
     *  MEASURED 2026-09-11 at the generator's landing (after the
     *  parseSources overload fix): 137 of 789 — 38 widened to Any, 25
     *  invented arities, 17 wildcard subsets dropped, 6 TDS-as-Relation,
     *  9 TDSRow getters (qualified properties upstream), 3 type-parameter
     *  names, 39 mixed. Leg 1 (the simple rows) adopts ~30. */
    public static final int DIVERGENT_MAX = 0;

    /** One membership row: OUR constant name, the FQN, the overload's key. */
    public record Row(String constant, String fqn, String key) {
    }

    /** One upstream declaration, canonically rendered. */
    public record Decl(String fqn, String key, String text, String file) {
    }

    /** Everything one pass over Pure.java finds: the rewritten lines, and the
     *  receipts the check and the program both report. */
    public record Result(List<String> lines, List<String> orphans, List<String> drift,
            List<String> missing, int same, int wanted) {
    }

    /** The signature text the membership + upstream say each constant has,
     *  applied to Pure.java's lines. {@code addMissing}: a membership
     *  constant with no declaration line joins after the last signature
     *  (generation); otherwise it is reported as missing (the check). */
    public static Result compute(List<Row> rows, Map<String, Decl> upstream, List<String> pureJava,
            boolean addMissing) {
        Map<String, String> wanted = new LinkedHashMap<>();
        List<String> orphans = new ArrayList<>();
        for (Row r : rows) {
            Decl d = upstream.get(r.key());
            if (d == null) {
                // the two orphan kinds: SAME FQN, DIFFERENT SIGNATURE (upstream
                // declares the name with other overloads — the silent
                // divergence D4 exists to surface) vs NO UPSTREAM DECLARATION
                List<String> sameFqn = new ArrayList<>();
                for (Decl u : upstream.values()) {
                    if (u.fqn().equals(r.fqn())) {
                        sameFqn.add(u.key());
                    }
                }
                orphans.add(r.constant() + "\t" + r.key() + (sameFqn.isEmpty()
                        ? "\n      NO upstream declaration of " + r.fqn()
                        : "\n      upstream declares: " + String.join("\n                        ", sameFqn)));
                continue;
            }
            wanted.put(r.constant(), d.text());
        }
        List<String> lines = new ArrayList<>(pureJava);
        List<String> drift = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        int same = 0;
        for (int i = 0; i < lines.size(); i++) {
            Matcher m = CONSTANT_LINE.matcher(lines.get(i));
            if (!m.matches()) {
                continue;
            }
            String text = wanted.get(m.group(2));
            if (text == null) {
                continue;
            }
            seen.add(m.group(2));
            if (text.equals(m.group(3))) {
                same++;
                continue;
            }
            drift.add(m.group(2) + "\n    was: " + m.group(3) + "\n    now: " + text);
            lines.set(i, m.group(1) + text + m.group(4));
        }
        List<String> missing = new ArrayList<>(wanted.keySet());
        missing.removeAll(seen);
        if (addMissing && !missing.isEmpty()) {
            // a NEW membership constant (an upstream overload adopted whole):
            // its declaration line joins Pure.java after the last signature
            int last = -1;
            for (int i = 0; i < lines.size(); i++) {
                if (CONSTANT_LINE.matcher(lines.get(i)).matches()) {
                    last = i;
                }
            }
            for (String c : missing) {
                lines.add(++last, "    public static final NativeFunctionDefinition " + c
                        + " = signature(\"" + wanted.get(c) + "\");");
                drift.add(c + "\n    was: (absent)\n    now: " + wanted.get(c));
            }
            missing.clear();
        }
        return new Result(lines, orphans, drift, missing, same, wanted.size());
    }

    /** The receipt both report: the divergent rows, then the refusal when they
     *  exceed the pin — "the new thing we cannot parse yet": fix the platform. */
    public static void refuseOrphans(List<String> orphans) {
        System.out.println("[natives] " + orphans.size() + " divergent rows (pin " + DIVERGENT_MAX + ")");
        for (String o : orphans) {
            System.out.println("[natives]   " + o.replace("\n", "\n[natives]   "));
        }
        if (orphans.size() > DIVERGENT_MAX) {
            throw new AssertionError("membership rows whose signature is not"
                    + " upstream's GREW: " + orphans.size() + " > " + DIVERGENT_MAX + " — adopt upstream's"
                    + " text (or move an invention to Lite), never widen the pin:\n  "
                    + String.join("\n  ", orphans));
        }
    }

    public static void printSummary(Result r) {
        System.out.println("[natives] " + r.wanted() + " signatures generated: " + r.same()
                + " byte-identical, " + r.drift().size() + " respelled");
        for (String d : r.drift()) {
            System.out.println("[natives] " + d);
        }
    }

    public static List<Row> readMembership(Path membership) throws IOException {
        List<Row> out = new ArrayList<>();
        for (String line : Files.readAllLines(membership, StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] c = line.split("\t", -1);
            if (c.length != 3) {
                throw new IllegalStateException("native-membership.tsv: expected 3 columns: " + line);
            }
            out.add(new Row(c[0], c[1], c[2]));
        }
        return out;
    }

    /** A declaration-position function header: {@code [native] function
     *  [<<…>>] [{…}] a::b::c} — the cheap index of which FILE declares an
     *  FQN (the parser then reads the file whole; a file that does not
     *  parse is a wall, never a partial copy). */
    private static final Pattern FUNC_HEADER = Pattern.compile(
            "(?m)^\\s*(?:native\\s+)?function\\s+(?:<<[^>]*>>\\s*)*(?:\\{[^}]*\\}\\s*)?"
            + "([A-Za-z0-9_$]+(?:::[A-Za-z0-9_$]+)+)\\s*[<(]");

    /** Every upstream declaration of a membership FQN, keyed by signature
     *  key, canonically rendered. Only the files declaring a membership FQN
     *  are parsed; the type universe for resolution is the whole spec index
     *  (every Class/Enum FQN in both checkouts plus m3's). */
    public static Map<String, Decl> upstreamDeclarations(List<Row> rows, Path engine, Path pure) throws IOException {
        List<Path> roots = new ArrayList<>();
        for (String r : PreludeGenerator.ENGINE_SPEC_ROOTS) {
            roots.add(engine.resolve(r));
        }
        roots.add(pure);
        Set<String> wanted = new LinkedHashSet<>();
        for (Row r : rows) {
            wanted.add(r.fqn());
        }
        Set<String> universe = new LinkedHashSet<>();
        Set<Path> files = new LinkedHashSet<>();
        for (Path root : roots) {
            if (!Files.isDirectory(root)) {
                throw new IllegalStateException("spec root missing (upstream moved it?): " + root);
            }
            try (Stream<Path> s = Files.walk(root)) {
                for (Path f : s.filter(p -> p.toString().endsWith(".pure")).sorted().toList()) {
                    String text = Files.readString(f, StandardCharsets.UTF_8);
                    Matcher m = PreludeGenerator.DECL_HEADER.matcher(text);
                    while (m.find()) {
                        universe.add(m.group(2));
                    }
                    Matcher fm = FUNC_HEADER.matcher(text);
                    while (fm.find()) {
                        if (wanted.contains(fm.group(1))) {
                            files.add(f);
                        }
                    }
                }
            }
        }
        Path m3File = pure.resolve(PreludeGenerator.M3_PURE);
        universe.addAll(PreludeGenerator.m3Declarations(
                Files.readString(m3File, StandardCharsets.UTF_8)).keySet());

        Map<String, Decl> out = new LinkedHashMap<>();
        List<String> walls = new ArrayList<>();
        List<String> unresolved = new ArrayList<>();
        List<String> unrenderable = new ArrayList<>();
        for (Path f : files) {
            List<String> parseWalls = new ArrayList<>();
            ParsedModel parsed = Compiler.parseSources(
                    List.of(new Compiler.ModelSource(f.toString(),
                            Files.readString(f, StandardCharsets.UTF_8))),
                    (n, err) -> parseWalls.add(n + ": " + err), Dialect.LEGEND_PLATFORM).model();
            if (!parseWalls.isEmpty()) {
                walls.add(parseWalls.get(0));
                continue;
            }
            Map<String, String> resolveWalls = new LinkedHashMap<>();
            ParsedModel resolved = NameResolver.resolve(parsed, universe, resolveWalls);
            String dbg = System.getProperty("natives.debug");
            if (dbg != null) {
                long a = parsed.elements().stream().filter(e -> e.qualifiedName().equals(dbg)).count();
                long b = resolved.elements().stream().filter(e -> e.qualifiedName().equals(dbg)).count();
                if (a > 0 || b > 0) {
                    System.out.println("[natives-debug] " + f + ": parsed " + a + " resolved " + b
                            + " walls " + resolveWalls.keySet());
                }
            }
            for (Map.Entry<String, String> w : resolveWalls.entrySet()) {
                if (wanted.contains(w.getKey())) {
                    unresolved.add(w.getKey() + " => " + w.getValue());
                }
            }
            for (PackageableElement el : resolved.elements()) {
                if (!wanted.contains(el.qualifiedName())) {
                    continue;
                }
                String text;
                try {
                    text = switch (el) {
                    case NativeFunctionDefinition n -> render(n.qualifiedName(), n.typeParameters(),
                            n.multiplicityParameters(), n.parameters(), n.returnType(),
                            n.returnMultiplicity());
                    case FunctionDefinition d -> render(d.qualifiedName(), d.typeParameters(),
                            d.multiplicityParameters(), d.parameters(), d.returnType(),
                            d.returnMultiplicity());
                    default -> null;
                    };
                } catch (IllegalStateException ex) {
                    // an upstream overload this renderer cannot spell (a unit
                    // type, a name outside the indexed universe) cannot match a
                    // membership key either: REPORTED, and any membership row it
                    // was the partner of surfaces as an orphan below
                    unrenderable.add(el.qualifiedName() + " in " + f + ": " + ex.getMessage());
                    continue;
                }
                if (text == null) {
                    continue;
                }
                NativeFunctionDefinition back = parseSignature(text);
                String key = canonicalKey(back);
                Decl prior = out.get(key);
                if (prior != null && !prior.text().equals(text)) {
                    throw new IllegalStateException("upstream declares the same overload twice with"
                            + " different signatures:\n  " + prior.file() + ": " + prior.text()
                            + "\n  " + f + ": " + text);
                }
                out.putIfAbsent(key, new Decl(el.qualifiedName(), key, text, f.toString()));
            }
        }
        if (!unresolved.isEmpty()) {
            throw new IllegalStateException("upstream declarations of membership FQNs whose type"
                    + " names do not resolve:\n  " + String.join("\n  ", unresolved));
        }
        // a wall is REPORTED, never silent: a membership FQN declared only in
        // a walled file surfaces as an orphan row with the wall named
        if (!unrenderable.isEmpty()) {
            System.out.println("[natives] " + unrenderable.size() + " upstream overloads not rendered:");
            for (String u : unrenderable) {
                System.out.println("[natives]   " + u);
            }
        }
        if (!walls.isEmpty()) {
            System.out.println("[natives] " + walls.size() + " spec files unparsed (load walls):");
            for (String w : walls) {
                System.out.println("[natives]   " + w);
            }
        }
        return out;
    }

    /** THE overload identity on both sides: the FQN and the canonically
     *  rendered parameter list (position-free, FQN-spelled) — Pure's
     *  overloads differ by parameters only, never by return type. */
    static String canonicalKey(NativeFunctionDefinition d) {
        Set<String> tps = new LinkedHashSet<>(d.typeParameters());
        StringBuilder sb = new StringBuilder(d.qualifiedName()).append('(');
        for (int i = 0; i < d.parameters().size(); i++) {
            var p = d.parameters().get(i);
            if (i > 0) {
                sb.append(',');
            }
            sb.append(type(p.type(), tps)).append(p.multiplicity());
        }
        return sb.append(')').toString();
    }

    static NativeFunctionDefinition parseSignature(String text) {
        ParsedModel one = ElementParser.parse(text, Dialect.LEGEND_PLATFORM);
        if (one.elements().size() != 1
                || !(one.elements().get(0) instanceof NativeFunctionDefinition nfd)) {
            throw new IllegalStateException("rendered signature does not parse to one native: " + text);
        }
        return nfd;
    }

    // ------------------------------------------------------------------
    // the canonical rendering — FQN-spelled, the catalog's own spelling
    // ------------------------------------------------------------------

    private static final Set<String> PRIMITIVES = Set.of(
            "Number", "Integer", "Float", "Decimal", "String", "Boolean",
            "Byte", "Date", "StrictDate", "DateTime", "LatestDate", "StrictTime");

    static String render(String fqn, List<String> typeParams, List<String> multParams,
            List<FunctionDefinition.ParameterDefinition> params, TypeExpression ret,
            Multiplicity retMult) {
        Set<String> tps = new LinkedHashSet<>(typeParams);
        StringBuilder sb = new StringBuilder("native function ").append(fqn);
        if (!typeParams.isEmpty() || !multParams.isEmpty()) {
            sb.append('<').append(String.join(",", typeParams));
            if (!multParams.isEmpty()) {
                sb.append('|').append(String.join(",", multParams));
            }
            sb.append('>');
        }
        sb.append('(');
        for (int i = 0; i < params.size(); i++) {
            var p = params.get(i);
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(p.name()).append(':').append(type(p.type(), tps)).append(p.multiplicity());
        }
        return sb.append("):").append(type(ret, tps)).append(retMult).append(';').toString();
    }

    static String type(TypeExpression t, Set<String> typeParams) {
        return switch (t) {
            case TypeExpression.NameRef n -> qualify(n.name(), typeParams);
            case TypeExpression.Generic g -> {
                if (!g.typeVariableValues().isEmpty()) {
                    throw new IllegalStateException("type-variable values are not rendered: " + g);
                }
                StringBuilder sb = new StringBuilder(qualify(g.name(), typeParams)).append('<');
                for (int i = 0; i < g.arguments().size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(type(g.arguments().get(i), typeParams));
                }
                if (!g.multiplicityArguments().isEmpty()) {
                    sb.append('|').append(String.join(",", g.multiplicityArguments()));
                }
                yield sb.append('>').toString();
            }
            case TypeExpression.FunctionType f -> {
                StringBuilder sb = new StringBuilder("{");
                for (int i = 0; i < f.parameters().size(); i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    var p = f.parameters().get(i);
                    sb.append(type(p.type(), typeParams)).append(p.multiplicity());
                }
                yield sb.append("->").append(type(f.result().type(), typeParams))
                        .append(f.result().multiplicity()).append('}').toString();
            }
            case TypeExpression.RelationType r -> {
                StringBuilder sb = new StringBuilder("(");
                for (int i = 0; i < r.columns().size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    var c = r.columns().get(i);
                    sb.append(c.name()).append(':').append(type(c.type(), typeParams));
                    if (c.multiplicityDeclared()) {
                        sb.append(c.multiplicity());
                    }
                }
                yield sb.append(')').toString();
            }
            case TypeExpression.SchemaAlgebra a -> type(a.left(), typeParams)
                    + switch (a.op()) {
                        case EQUAL -> "=";
                        case UNION -> "+";
                        case DIFFERENCE -> "-";
                        case SUBSET -> "⊆";
                    }
                    + type(a.right(), typeParams);
        };
    }

    static String qualify(String name, Set<String> typeParams) {
        // "?" is the relation-type WILDCARD column spelling (upstream's
        // SortInfo<(?:?)⊆T>), a literal, not a name
        if (name.contains("::") || typeParams.contains(name) || name.equals("?")) {
            return name;
        }
        if (PRIMITIVES.contains(name)) {
            return "meta::pure::metamodel::type::" + name;
        }
        throw new IllegalStateException("unresolved type name in an upstream signature: " + name);
    }
}
