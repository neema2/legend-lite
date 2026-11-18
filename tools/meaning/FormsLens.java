// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning;

import com.legend.tools.meaning.ClassFiles.Bootstrap;
import com.legend.tools.meaning.ClassFiles.ClassFile;
import com.legend.tools.meaning.ClassFiles.Constant;
import com.legend.tools.meaning.ClassFiles.Insn;
import com.legend.tools.meaning.ClassFiles.Method;
import com.legend.tools.meaning.ClassFiles.Ref;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * LENS M3, THE FORMS (census B2, 2026-11-19; the audit's GUARDS-1, -2 and -4): what the product computes for the
 * edge, read from the bytecode of the product's and the edge's classes. A product DECLARATION with a product
 * implementation and an edge implementation (a hook, a two-form kind, any overridden product method; a lambda
 * implements one too) is a seam, and three shapes across it are upstream meaning in the product:
 * <ol>
 *   <li>UNUSED: a parameter the product's implementation never loads and the edge's does -- a value the product's
 *       callers compute for the edge alone (GUARDS-4: an eagerly computed upstream value handed to a hook);</li>
 *   <li>ANSWER: the product's implementation answers a constant (null, false, 0, a literal, an empty collection) where
 *       the edge's answers otherwise; every product method whose conditional tests that answer -- directly, through a
 *       local, through a parameter it is handed, or through a method that returns it -- branches on an upstream answer
 *       (GUARDS-1/-2: a union frame named only at the edge, its framing decided in the product);</li>
 *   <li>EDGE_LIVE: a product method whose product callers all pass the absent value (null, false, 0) at a parameter
 *       that an edge caller passes live -- the parameter only the edge passes non-null (GUARDS-2's SubselectPrune).</li>
 * </ol>
 * Plus REFUSED: a product implementation that throws where the edge's answers, and each product call of it (code the
 * product runs only for the edge). Every finding is reported, never judged: the census classifies.
 */
public final class FormsLens {

    /** One finding: its shape, the declaration or method it is about, where (class and line), and what. */
    public record Finding(String shape, String subject, String where, String what) {
    }

    private final Map<String, ClassFile> classes;
    private final Predicate<String> isEdge;
    private final Map<String, Set<String>> supers = new HashMap<>();

    /** A seam's implementation: the class and method holding the body, and the mapping from the declaration's
     *  parameter positions to the body's local slots (-1 when the body does not take it). */
    record Impl(ClassFile owner, Method body, int[] slotOf, boolean edge, String label) {
    }

    /** @param classes every class read, by internal name: the product's and the edge's
     *  @param isEdge whether an internal class name is the edge's */
    public FormsLens(Map<String, ClassFile> classes, Predicate<String> isEdge) {
        this.classes = classes;
        this.isEdge = isEdge;
    }

    /** Every supertype of {@code name} read (itself included), by internal name. */
    Set<String> supertypes(String name) {
        Set<String> known = supers.get(name);
        if (known != null) {
            return known;
        }
        Set<String> out = new LinkedHashSet<>();
        out.add(name);
        ClassFile c = classes.get(name);
        if (c != null) {
            if (c.superName() != null) {
                out.addAll(supertypes(c.superName()));
            }
            for (String i : c.interfaces()) {
                out.addAll(supertypes(i));
            }
        }
        supers.put(name, out);
        return out;
    }

    private static boolean overridable(Method m) {
        return !m.name().startsWith("<") && (m.access() & 0x0008) == 0 && (m.access() & 0x0002) == 0;
    }

    /** "owner#name(desc)" of a product declaration. */
    private static String key(String owner, String name, String desc) {
        return owner + "#" + name + desc;
    }

    /** The findings. */
    public List<Finding> findings() {
        // the product's declarations, by name+descriptor -> declaring types
        Map<String, Set<String>> declarers = new HashMap<>();
        for (ClassFile c : classes.values()) {
            if (isEdge.test(c.name())) {
                continue;
            }
            for (Method m : c.methods()) {
                if (overridable(m) && !m.isBridge()) {
                    declarers.computeIfAbsent(m.name() + m.descriptor(), k -> new TreeSet<>()).add(c.name());
                }
            }
        }
        // each declaration's implementations: every class's concrete methods, and every lambda
        Map<String, List<Impl>> impls = new TreeMap<>();
        for (ClassFile c : classes.values()) {
            for (Method m : c.methods()) {
                if (!overridable(m) || m.isAbstract()) {
                    continue;
                }
                Set<String> ds = declarers.get(m.name() + m.descriptor());
                if (ds == null) {
                    continue;
                }
                Method body = m.isBridge() ? bridged(c, m) : m;
                if (body == null) {
                    continue;
                }
                int[] slots = ClassFiles.paramSlots(body);
                for (String t : supertypes(c.name())) {
                    if (ds.contains(t)) {
                        impls.computeIfAbsent(key(t, m.name(), m.descriptor()), k -> new ArrayList<>())
                                .add(new Impl(c, body, slots, isEdge.test(c.name()), c.binaryName() + "#" + m.name()));
                    }
                }
            }
            lambdas(c, declarers, impls);
        }
        List<Finding> out = new ArrayList<>();
        Map<String, String> seeds = new TreeMap<>();
        Map<String, String> refused = new TreeMap<>();
        for (Map.Entry<String, List<Impl>> e : impls.entrySet()) {
            List<Impl> product = e.getValue().stream().filter(i -> !i.edge()).toList();
            List<Impl> edge = e.getValue().stream().filter(Impl::edge).toList();
            if (product.isEmpty() || edge.isEmpty()) {
                continue;
            }
            String d = e.getKey();
            String desc = d.substring(d.indexOf('('));
            int params = ClassFiles.params(desc).size();
            // UNUSED
            for (int k = 0; k < params; k++) {
                final int p = k;
                boolean edgeLoads = edge.stream().anyMatch(i -> loads(i, p));
                if (!edgeLoads) {
                    continue;
                }
                for (Impl i : product) {
                    if (!loads(i, p)) {
                        out.add(new Finding("UNUSED", pretty(d), where(i), "parameter " + (k + 1) + " of "
                                + params + " (" + simple(ClassFiles.params(desc).get(k)) + ") is never read by "
                                + i.label() + "; the edge's " + edge.stream().filter(x -> loads(x, p))
                                        .map(Impl::label).findFirst().orElse("?") + " reads it"));
                    }
                }
            }
            // ANSWER and REFUSED
            List<String> answers = product.stream().map(this::answer).toList();
            List<String> edgeAnswers = edge.stream().map(this::answer).toList();
            boolean allConstant = answers.stream().allMatch(a -> a != null && !a.startsWith("identity")
                    && !a.equals("refuses"));
            if (allConstant && edgeAnswers.stream().anyMatch(a -> a == null || !answers.contains(a))) {
                // a scalar or an enumeration's answer is a decision the product branches on; a form or a collection is
                // a strategy the product hands its work to (the designed hook), reported but no seed
                if (scalar(ClassFiles.returns(desc))) {
                    seeds.put(d, String.join(", ", new LinkedHashSet<>(answers)));
                }
                for (Impl i : product) {
                    out.add(new Finding("ANSWER", pretty(d), where(i), i.label() + " answers " + answer(i)
                            + "; the edge's " + edge.get(0).label() + " answers "
                            + (edgeAnswers.get(0) == null ? "a computed value" : edgeAnswers.get(0))));
                }
            }
            if (answers.stream().allMatch("refuses"::equals) && edgeAnswers.stream().anyMatch(a -> !"refuses".equals(a))) {
                refused.put(d, product.get(0).label());
            }
        }
        // the branches on an answer: the product's methods, run with the seeds, to a fixpoint over parameters handed a
        // tainted value and methods that return one
        Map<String, Set<Integer>> taintedParams = new HashMap<>();
        Map<String, String> why = new HashMap<>();
        Set<String> returning = new HashSet<>();
        Map<String, Flow.Result> results = new HashMap<>();
        Predicate<Ref> seeded = r -> isSeed(r, seeds, declarers) || returning.contains(resolve(r));
        for (int pass = 0; pass < 8; pass++) {
            boolean changed = false;
            for (ClassFile c : classes.values()) {
                if (isEdge.test(c.name())) {
                    continue;
                }
                for (Method m : c.methods()) {
                    if (m.code().isEmpty()) {
                        continue;
                    }
                    String mk = key(c.name(), m.name(), m.descriptor());
                    Set<Integer> mine = taintedParams.getOrDefault(mk, Set.of());
                    Flow.Result r = Flow.run(c, m, seeded, mine);
                    results.put(mk, r);
                    // a method returns an upstream answer when it does with no parameter tainted: a helper handed one
                    // by a caller returns it to that caller alone, not to every caller (context, not a seed)
                    boolean intrinsic = r.returnsTaint()
                            && (mine.isEmpty() || Flow.run(c, m, seeded, Set.of()).returnsTaint());
                    if (intrinsic && returning.add(mk)) {
                        changed = true;
                    }
                    for (Flow.Call call : r.taintedCalls()) {
                        String target = resolve(call.target());
                        if (target == null || isEdge.test(target.substring(0, target.indexOf('#')))) {
                            continue;
                        }
                        for (int k = 0; k < call.args().size(); k++) {
                            if (call.args().get(k).taint()
                                    && taintedParams.computeIfAbsent(target, x -> new TreeSet<>()).add(k)) {
                                changed = true;
                                why.putIfAbsent(target + "@" + k, mk + "|" + call.line());
                            }
                        }
                    }
                }
            }
            if (!changed) {
                break;
            }
        }
        for (ClassFile c : classes.values()) {
            if (isEdge.test(c.name())) {
                continue;
            }
            for (Method m : c.methods()) {
                String mk = key(c.name(), m.name(), m.descriptor());
                Flow.Result r = results.get(mk);
                if (r == null || r.taintedBranchLines().isEmpty()) {
                    continue;
                }
                String source = sourceOf(r, c, m, seeds, declarers, why, taintedParams.get(mk), results,
                        taintedParams);
                out.add(new Finding("BRANCH", c.binaryName() + "#" + m.name(), where(c, r.taintedBranchLines().get(0)),
                        r.taintedBranchLines().size() + " conditional(s) at line(s) "
                                + new TreeSet<>(r.taintedBranchLines()) + " test " + source));
            }
        }
        // REFUSED: the product's calls of a declaration its product forms refuse
        for (ClassFile c : classes.values()) {
            if (isEdge.test(c.name())) {
                continue;
            }
            for (Method m : c.methods()) {
                Flow.Result r = results.get(key(c.name(), m.name(), m.descriptor()));
                if (r == null) {
                    continue;
                }
                for (Flow.Call call : r.calls()) {
                    for (String d : declarationsOf(call.target(), declarers)) {
                        if (refused.containsKey(d)) {
                            out.add(new Finding("REFUSED", pretty(d), where(c, call.line()), c.binaryName() + "#"
                                    + m.name() + " calls it; the product's " + refused.get(d) + " refuses"));
                        }
                    }
                }
            }
        }
        out.addAll(edgeLive(results));
        return out;
    }

    /** EDGE_LIVE: a product method whose product callers all pass the same absent value at a parameter an edge caller
     *  passes live. */
    private List<Finding> edgeLive(Map<String, Flow.Result> productResults) {
        Map<String, List<Flow.Call>> productSites = new TreeMap<>();
        Map<String, List<String>> productWhere = new HashMap<>();
        Map<String, List<Flow.Call>> edgeSites = new TreeMap<>();
        Map<String, List<String>> edgeWhere = new HashMap<>();
        for (ClassFile c : classes.values()) {
            boolean edge = isEdge.test(c.name());
            for (Method m : c.methods()) {
                if (m.code().isEmpty()) {
                    continue;
                }
                Flow.Result r = edge ? Flow.run(c, m, x -> false, Set.of())
                        : productResults.get(key(c.name(), m.name(), m.descriptor()));
                if (r == null) {
                    continue;
                }
                for (Flow.Call call : r.calls()) {
                    if (call.op() == 186) {
                        continue;
                    }
                    String target = resolve(call.target());
                    if (target == null || isEdge.test(target.substring(0, target.indexOf('#')))) {
                        continue;
                    }
                    (edge ? edgeSites : productSites).computeIfAbsent(target, k -> new ArrayList<>()).add(call);
                    (edge ? edgeWhere : productWhere).computeIfAbsent(target, k -> new ArrayList<>())
                            .add(c.binaryName() + ":" + call.line());
                }
            }
        }
        List<Finding> out = new ArrayList<>();
        for (Map.Entry<String, List<Flow.Call>> e : productSites.entrySet()) {
            List<Flow.Call> edge = edgeSites.get(e.getKey());
            if (edge == null) {
                continue;
            }
            int n = e.getValue().get(0).args().size();
            for (int k = 0; k < n; k++) {
                final int p = k;
                String absent = e.getValue().get(0).args().get(k).constant();
                boolean allAbsent = e.getValue().stream().allMatch(c -> c.args().size() > p && c.args().get(p).isAbsent()
                        && c.args().get(p).constant().equals(absent));
                if (!allAbsent) {
                    continue;
                }
                List<Integer> live = new ArrayList<>();
                for (int s = 0; s < edge.size(); s++) {
                    Flow.V v = edge.get(s).args().get(p);
                    if (v.constant() == null || !v.constant().equals(absent)) {
                        live.add(s);
                    }
                }
                if (!live.isEmpty()) {
                    String desc = e.getKey().substring(e.getKey().indexOf('('));
                    out.add(new Finding("EDGE_LIVE", pretty(e.getKey()), productWhere.get(e.getKey()).get(0),
                            "parameter " + (k + 1) + " (" + simple(ClassFiles.params(desc).get(k)) + "): every product"
                                    + " call (" + e.getValue().size() + ", e.g. " + productWhere.get(e.getKey()).get(0)
                                    + ") passes " + absent + "; the edge passes it live ("
                                    + edgeWhere.get(e.getKey()).get(live.get(0)) + ")"));
                }
            }
        }
        return out;
    }

    /** The concrete method a call resolves to ("owner#name(desc)" of the class declaring it), or null. */
    private String resolve(Ref r) {
        ClassFile c = classes.get(r.owner());
        while (c != null) {
            for (Method m : c.methods()) {
                if (m.name().equals(r.name()) && m.descriptor().equals(r.descriptor())) {
                    return key(c.name(), m.name(), m.descriptor());
                }
            }
            c = c.superName() == null ? null : classes.get(c.superName());
        }
        // an interface's method (a default, or abstract): its first declarer among the supertypes
        for (String t : supertypes(r.owner())) {
            ClassFile s = classes.get(t);
            if (s == null) {
                continue;
            }
            for (Method m : s.methods()) {
                if (m.name().equals(r.name()) && m.descriptor().equals(r.descriptor())) {
                    return key(s.name(), m.name(), m.descriptor());
                }
            }
        }
        return null;
    }

    private Set<String> declarationsOf(Ref r, Map<String, Set<String>> declarers) {
        Set<String> ds = declarers.get(r.name() + r.descriptor());
        if (ds == null) {
            return Set.of();
        }
        Set<String> out = new TreeSet<>();
        for (String t : supertypes(r.owner())) {
            if (ds.contains(t)) {
                out.add(key(t, r.name(), r.descriptor()));
            }
        }
        return out;
    }

    private boolean isSeed(Ref r, Map<String, String> seeds, Map<String, Set<String>> declarers) {
        for (String d : declarationsOf(r, declarers)) {
            if (seeds.containsKey(d)) {
                return true;
            }
        }
        return false;
    }

    /** What a tainted method tests: the seeds it calls, else the parameters it was handed, followed back hop by hop
     *  (a few hops, no cycle) to the method that called a seed. */
    private String sourceOf(Flow.Result r, ClassFile c, Method m, Map<String, String> seeds,
            Map<String, Set<String>> declarers, Map<String, String> why, Set<Integer> params,
            Map<String, Flow.Result> results, Map<String, Set<Integer>> taintedParams) {
        Set<String> direct = directSeeds(r, seeds, declarers);
        if (!direct.isEmpty()) {
            return String.join("; ", direct);
        }
        if (params == null || params.isEmpty()) {
            return "a method returning an upstream answer";
        }
        String mk = key(c.name(), m.name(), m.descriptor());
        List<String> out = new ArrayList<>();
        for (int p : params) {
            StringBuilder chain = new StringBuilder("parameter " + (p + 1));
            String at = mk + "@" + p;
            Set<String> visited = new HashSet<>();
            for (int hop = 0; hop < 6 && why.containsKey(at) && visited.add(at); hop++) {
                String[] w = why.get(at).split("\\|");
                String caller = w[0];
                chain.append(" <- ").append(pretty(caller)).append(":").append(w[1]);
                Flow.Result cr = results.get(caller);
                Set<String> seeded = cr == null ? Set.of() : directSeeds(cr, seeds, declarers);
                if (!seeded.isEmpty()) {
                    chain.append(" <- ").append(String.join("; ", seeded));
                    break;
                }
                Set<Integer> cp = taintedParams.get(caller);
                if (cp == null || cp.isEmpty()) {
                    break;
                }
                at = caller + "@" + cp.iterator().next();
            }
            out.add(chain.toString());
        }
        return String.join("; ", out);
    }

    private Set<String> directSeeds(Flow.Result r, Map<String, String> seeds, Map<String, Set<String>> declarers) {
        Set<String> direct = new TreeSet<>();
        for (Flow.Call call : r.calls()) {
            for (String d : declarationsOf(call.target(), declarers)) {
                if (seeds.containsKey(d)) {
                    direct.add(pretty(d) + " (the product's answer: " + seeds.get(d) + ")");
                }
            }
        }
        return direct;
    }

    private final Map<Method, Set<Integer>> loadedCache = new java.util.IdentityHashMap<>();

    /** Whether {@code i}'s body reads its declaration's parameter {@code k}. */
    private boolean loads(Impl i, int k) {
        if (k >= i.slotOf().length || i.slotOf()[k] < 0) {
            return true;
        }
        return loadedCache.computeIfAbsent(i.body(), b -> Flow.run(i.owner(), b, x -> false, Set.of()).loadedSlots())
                .contains(i.slotOf()[k]);
    }

    /** The constant a body answers, "identity N" for its parameter N handed back, "refuses" when it only throws,
     *  else null. */
    String answer(Impl i) {
        List<Insn> code = i.body().code();
        String ret = ClassFiles.returns(i.body().descriptor());
        boolean returns = code.stream().anyMatch(x -> x.op() >= 172 && x.op() <= 176);
        if (!returns && code.stream().anyMatch(x -> x.op() == 191)) {
            return "refuses";
        }
        if (code.size() != 2 || code.get(1).op() < 172 || code.get(1).op() > 176) {
            return null;
        }
        Insn first = code.get(0);
        ClassFile c = i.owner();
        int op = first.op();
        if (op == 1) {
            return "null";
        }
        if (op >= 2 && op <= 8) {
            int v = op - 3;
            return ret.equals("Z") ? (v == 0 ? "false" : "true") : String.valueOf(v);
        }
        if (op == 9 || op == 10 || op == 11 || op == 12 || op == 13 || op == 14 || op == 15) {
            return "a number constant";
        }
        if (op == 16 || op == 17) {
            return String.valueOf(first.operand());
        }
        if (op == 18 || op == 19 || op == 20) {
            Constant k = c.pool()[first.operand()];
            return k.tag() == 8 ? "\"" + c.utf8(k.a()) + "\"" : String.valueOf(k.value());
        }
        if (op == 178) {
            Ref f = c.ref(first.operand());
            return "the constant " + f.owner().substring(f.owner().lastIndexOf('/') + 1) + "." + f.name();
        }
        if (op == 184) {
            Ref f = c.ref(first.operand());
            if (f.descriptor().startsWith("()") && (f.owner().startsWith("java/util/"))) {
                return "an empty " + f.owner().substring(f.owner().lastIndexOf('/') + 1) + " (" + f.name() + "())";
            }
            return null;
        }
        int slot = switch (op) {
            case 25, 21, 22, 23, 24 -> first.operand();
            case 42, 43, 44, 45 -> op - 42;
            case 26, 27, 28, 29 -> op - 26;
            default -> -1;
        };
        if (slot >= 0) {
            for (int k = 0; k < i.slotOf().length; k++) {
                if (i.slotOf()[k] == slot) {
                    return "identity " + (k + 1);
                }
            }
        }
        return null;
    }

    /** Whether a return type is a decision's: a primitive, a string, a boxed primitive or an enumeration read. */
    private boolean scalar(String type) {
        if (type.length() == 1) {
            return !type.equals("V");
        }
        if (type.matches("Ljava/lang/(String|Boolean|Integer|Long|Double|Float|Short|Byte|Character);")) {
            return true;
        }
        if (type.startsWith("L") && type.endsWith(";")) {
            ClassFile c = classes.get(type.substring(1, type.length() - 1));
            return c != null && (c.access() & 0x4000) != 0;
        }
        return false;
    }

    /** The method a bridge calls: the same name, in the same class, by invokevirtual or invokeinterface. */
    private static Method bridged(ClassFile c, Method bridge) {
        for (Insn i : bridge.code()) {
            if (i.op() == 182 || i.op() == 185 || i.op() == 183) {
                Ref r = c.ref(i.operand());
                if (r.name().equals(bridge.name()) && r.owner().equals(c.name())) {
                    for (Method m : c.methods()) {
                        if (m.name().equals(r.name()) && m.descriptor().equals(r.descriptor())) {
                            return m;
                        }
                    }
                }
            }
        }
        return null;
    }

    /** Every lambda and method reference of {@code c} implementing a product declaration. */
    private void lambdas(ClassFile c, Map<String, Set<String>> declarers, Map<String, List<Impl>> impls) {
        for (Method m : c.methods()) {
            for (Insn i : m.code()) {
                if (i.op() != 186) {
                    continue;
                }
                Constant indy = c.pool()[i.operand()];
                Bootstrap b = c.bootstraps().get(indy.a());
                if (!b.handle().owner().equals("java/lang/invoke/LambdaMetafactory") || b.args().size() < 3) {
                    continue;
                }
                Ref site = c.indy(i.operand());
                String ret = ClassFiles.returns(site.descriptor());
                if (!ret.startsWith("L")) {
                    continue;
                }
                String iface = ret.substring(1, ret.length() - 1);
                Constant samType = c.pool()[b.args().get(0)];
                String samDesc = c.utf8(samType.a());
                Set<String> ds = declarers.get(site.name() + samDesc);
                if (ds == null) {
                    continue;
                }
                Constant handle = c.pool()[b.args().get(1)];
                Ref impl = c.ref(handle.b());
                int kind = handle.a();
                ClassFile owner = classes.get(impl.owner());
                Method body = null;
                if (owner != null) {
                    for (Method x : owner.methods()) {
                        if (x.name().equals(impl.name()) && x.descriptor().equals(impl.descriptor())) {
                            body = x;
                        }
                    }
                }
                if (body == null || body.code().isEmpty()) {
                    continue;
                }
                int captured = ClassFiles.params(site.descriptor()).size();
                // the body's arguments: a static (6) or a constructor (8) takes its parameters; an instance method its
                // receiver first
                List<Integer> argSlots = new ArrayList<>();
                int slot = 0;
                if (kind != 6 && kind != 8) {
                    argSlots.add(0);
                    slot = 1;
                }
                for (String p : ClassFiles.params(body.descriptor())) {
                    argSlots.add(slot);
                    slot += ClassFiles.size(p);
                }
                int samParams = ClassFiles.params(samDesc).size();
                int[] slots = new int[samParams];
                for (int k = 0; k < samParams; k++) {
                    int a = captured + k;
                    slots[k] = kind == 8 || a >= argSlots.size() ? -1 : argSlots.get(a);
                }
                for (String t : supertypes(iface)) {
                    if (ds.contains(t)) {
                        impls.computeIfAbsent(key(t, site.name(), samDesc), k -> new ArrayList<>()).add(new Impl(owner,
                                body, slots, isEdge.test(c.name()), c.binaryName() + "#" + m.name() + "'s lambda ("
                                        + owner.binaryName() + "#" + body.name() + ")"));
                    }
                }
            }
        }
    }

    private String where(Impl i) {
        return where(i.owner(), i.body().lineAt(i.body().code().isEmpty() ? 0 : i.body().code().get(0).pc()));
    }

    /** "a.b.C:line" -- the class's source file by its package and SourceFile attribute. */
    static String where(ClassFile c, int line) {
        String pkg = c.name().contains("/") ? c.name().substring(0, c.name().lastIndexOf('/') + 1) : "";
        String file = c.sourceFile() == null ? c.name().substring(pkg.length()) + ".java" : c.sourceFile();
        return pkg + file + ":" + line;
    }

    static String pretty(String key) {
        int hash = key.indexOf('#');
        String owner = key.substring(0, hash).replace('/', '.');
        String rest = key.substring(hash + 1);
        String name = rest.substring(0, rest.indexOf('('));
        List<String> ps = ClassFiles.params(rest.substring(rest.indexOf('(')));
        List<String> simple = new ArrayList<>();
        for (String p : ps) {
            simple.add(simple(p));
        }
        return owner.substring(owner.lastIndexOf('.') + 1) + "#" + name + "(" + String.join(", ", simple) + ")";
    }

    static String simple(String type) {
        String t = type;
        int dims = 0;
        while (t.startsWith("[")) {
            t = t.substring(1);
            dims++;
        }
        String base = switch (t) {
            case "Z" -> "boolean";
            case "I" -> "int";
            case "J" -> "long";
            case "D" -> "double";
            case "F" -> "float";
            case "B" -> "byte";
            case "C" -> "char";
            case "S" -> "short";
            default -> {
                String n = t.substring(1, t.length() - 1);
                n = n.substring(n.lastIndexOf('/') + 1);
                yield n.substring(n.lastIndexOf('$') + 1);
            }
        };
        return base + "[]".repeat(dims);
    }

    /** A map's text for a report, sorted. */
    static String describe(Map<String, ?> m) {
        return new LinkedHashMap<>(m).toString();
    }
}
