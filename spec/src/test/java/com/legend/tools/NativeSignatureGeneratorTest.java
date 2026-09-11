package com.legend.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.builtin.Pure;
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
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * UPSTREAM BOUNDARY, batch 5 (program §3 C / D4): {@code Pure.java}'s
 * signature TEXT is generated — membership is OURS ({@code
 * native-membership.tsv}: constant, fqn, signature key), the spelling is
 * UPSTREAM's (the pinned checkouts' {@code native function} and bodied
 * {@code function} declarations, resolved to FQNs and rendered
 * canonically). Drift between the two is impossible by construction: the
 * parity test regenerates the block between the markers and asserts it
 * byte-equal, exactly the {@code prelude.pure} contract.
 *
 * <p>Modes: {@code -Dnatives.generate=1} rewrites the generated block of
 * {@code Pure.java} (and prints the divergence receipt: rows whose spelling
 * changed); the default asserts.
 *
 * <p>What the generator refuses, loudly: a membership row no upstream
 * declaration matches (the signature key is the join — a row with no
 * partner is a claim the spec does not make: move it to the Lite section
 * or delete it), an upstream declaration whose type names do not resolve
 * (a spec root moved), and a rendered signature that does not round-trip
 * through the catalog's own parser.
 */
class NativeSignatureGeneratorTest {

    static final Path MEMBERSHIP = CoreTree.resource("com/legend/builtin/native-membership.tsv");
    static final Path PURE_JAVA = CoreTree.main("com/legend/builtin/Pure.java");
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
    static final int DIVERGENT_MAX = 0;

    /** One membership row: OUR constant name, the FQN, the overload's key. */
    record Row(String constant, String fqn, String key) {
    }

    /** One upstream declaration, canonically rendered. */
    record Decl(String fqn, String key, String text, String file) {
    }

    @Test
    @DisplayName("every membership constant's signature text in Pure.java is the generator's current output (regenerate with -Dnatives.generate=1)")
    void signatureTextIsCurrent() throws IOException {
        if ("1".equals(System.getProperty("natives.bootstrap"))) {
            Files.writeString(MEMBERSHIP, membershipFromCatalog(), StandardCharsets.UTF_8);
            System.out.println("[natives] bootstrapped " + MEMBERSHIP + " from the catalog");
            return;
        }
        List<Row> rows = readMembership();
        Map<String, Decl> upstream = upstreamDeclarations(rows);
        String dump = System.getProperty("natives.dump");
        if (dump != null) {
            // every upstream declaration of a membership FQN, one per line
            // (fqn, canonical key, canonical text, SOURCE FILE) — the input a
            // re-keying leg and the provenance review read instead of scraping
            // this test's report
            StringBuilder sb = new StringBuilder();
            for (Decl d : upstream.values()) {
                sb.append(d.fqn()).append('\t').append(d.key()).append('\t').append(d.text())
                        .append('\t').append(d.file()).append('\n');
            }
            Files.writeString(Path.of(dump), sb.toString(), StandardCharsets.UTF_8);
            System.out.println("[natives] dumped " + upstream.size() + " upstream declarations to " + dump);
            return;
        }
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
        // DIVERGENT rows — membership rows whose signature is not upstream's:
        // a shrink-only pin (each leg of batch 5 adopts upstream's text for a
        // bucket and lowers it), never a ledger of reasons. Zero is the done
        // criterion (program §4 row 5, D4): then the pin goes and this is a
        // plain parity assert.
        System.out.println("[natives] " + orphans.size() + " divergent rows (pin " + DIVERGENT_MAX + ")");
        for (String o : orphans) {
            System.out.println("[natives]   " + o.replace("\n", "\n[natives]   "));
        }
        assertTrue(orphans.size() <= DIVERGENT_MAX, () -> "membership rows whose signature is not"
                + " upstream's GREW: " + orphans.size() + " > " + DIVERGENT_MAX + " — adopt upstream's"
                + " text (or move an invention to Lite), never widen the pin:\n  "
                + String.join("\n  ", orphans));
        List<String> lines = Files.readAllLines(PURE_JAVA, StandardCharsets.UTF_8);
        List<String> drift = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        boolean generate = "1".equals(System.getProperty("natives.generate"));
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
        if (generate && !missing.isEmpty()) {
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
        assertTrue(missing.isEmpty(), () -> "membership constants with no declaration line in"
                + " Pure.java: " + missing + " — regenerate with -Dnatives.generate=1");
        System.out.println("[natives] " + wanted.size() + " signatures generated: " + same
                + " byte-identical, " + drift.size() + " respelled");
        for (String d : drift) {
            System.out.println("[natives] " + d);
        }
        if (generate) {
            Files.writeString(PURE_JAVA, String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
            return;
        }
        assertTrue(drift.isEmpty(), () -> "Pure.java's signature text drifted from the membership"
                + " + the pinned checkouts (" + drift.size() + " constants) — regenerate with"
                + " -Dnatives.generate=1");
    }

    @Test
    @DisplayName("every generated constant is claimed (membership = the implemented surface) and every catalog native outside the block is a Lite invention")
    void membershipIsTheCatalog() throws IOException {
        Set<String> members = new LinkedHashSet<>();
        for (Row r : readMembership()) {
            members.add(r.constant());
        }
        List<String> stray = new ArrayList<>();
        for (Map.Entry<String, NativeFunctionDefinition> c : constants().entrySet()) {
            if (members.contains(c.getKey())) {
                continue;
            }
            if (!c.getValue().qualifiedName().startsWith(Pure.Lite.PKG)) {
                stray.add(c.getKey() + " = " + c.getValue().qualifiedName());
            }
        }
        assertTrue(stray.isEmpty(), () -> "Pure.java constants outside the membership that are"
                + " not Lite inventions (hand-typed spec claims — add the row, or move to Lite):\n  "
                + String.join("\n  ", stray));
    }

    // ------------------------------------------------------------------
    // membership
    // ------------------------------------------------------------------

    static List<Row> readMembership() throws IOException {
        List<Row> out = new ArrayList<>();
        for (String line : Files.readAllLines(MEMBERSHIP, StandardCharsets.UTF_8)) {
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

    /** Pure.java's public native-function constants by name (reflection:
     *  the constant names are ours and the code references them). */
    static Map<String, NativeFunctionDefinition> constants() {
        Map<String, NativeFunctionDefinition> out = new TreeMap<>();
        for (Field f : Pure.class.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers()) && Modifier.isPublic(f.getModifiers())
                    && f.getType() == NativeFunctionDefinition.class) {
                try {
                    out.put(f.getName(), (NativeFunctionDefinition) f.get(null));
                } catch (IllegalAccessException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }
        return out;
    }

    // ------------------------------------------------------------------
    // upstream
    // ------------------------------------------------------------------

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
    static Map<String, Decl> upstreamDeclarations(List<Row> rows) throws IOException {
        Path engine = PreludeGeneratorTest.engineRoot();
        Path pure = PreludeGeneratorTest.pureRoot();
        List<Path> roots = new ArrayList<>();
        for (String r : PreludeGeneratorTest.ENGINE_SPEC_ROOTS) {
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
                    Matcher m = PreludeGeneratorTest.DECL_HEADER.matcher(text);
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
        Path m3File = pure.resolve(PreludeGeneratorTest.M3_PURE);
        universe.addAll(PreludeGeneratorTest.m3Declarations(
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

    /** Bootstrap helper (run once at the batch-5 landing, then never): the
     *  membership TSV derived from today's hand-typed catalog. */
    static String membershipFromCatalog() {
        StringBuilder sb = new StringBuilder("# native-membership.tsv — the platform's implemented"
                + " surface (upstream boundary program, batch 5).\n"
                + "# Membership is OURS; the signature text is generated from the pinned checkouts.\n"
                + "# constant\tfqn\tsignatureKey — sorted by fqn, then key.\n");
        List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, NativeFunctionDefinition> c : constants().entrySet()) {
            NativeFunctionDefinition d = c.getValue();
            if (d.qualifiedName().startsWith(Pure.Lite.PKG)) {
                continue;
            }
            rows.add(new Row(c.getKey(), d.qualifiedName(), canonicalKey(d)));
        }
        rows.sort(java.util.Comparator.comparing(Row::fqn).thenComparing(Row::key));
        for (Row r : rows) {
            sb.append(r.constant()).append('\t').append(r.fqn()).append('\t').append(r.key()).append('\n');
        }
        return sb.toString();
    }
}
