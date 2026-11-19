// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compatspike;

import com.legend.Compiler;
import com.legend.model.ConnectionDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * H5, SCRATCH (the compatibility program's translator spike, 2026-11-19; never merges as it is): the ENGINE audience's
 * MODEL-LEVEL, TARGET-AWARE input translator. Its input is the parsed model and the target database (read from the
 * runtime's connection); its output is a rewritten parsed model plus the compatibility library's source, handed to the
 * normal compiler by the caller through its public doors ({@link Compiler#parseSources}, {@link Compiler#buildModule},
 * the test door). It lives in the spec tests' source tree on purpose: from here only the core's PUBLIC API is
 * reachable, so "it needed no new hook" is checked by javac, not claimed.
 *
 * <p>It rewrites one family: legend-engine's {@code substring} and {@code indexOf} (registers S130, S134; H3 (a)),
 * over a core whose own meaning is Pure's (R4). Each rewritten call is renamed to its compatibility twin in
 * {@code meta::legend::compat::engine::}, a lite program written for the target ({@code compat-engine-<target>.pure},
 * H3's library); the call keeps its arguments and its source position.
 *
 * <p>Scopes ({@code -Dcompat.translator}):
 * <ul>
 *   <li>{@code identity}: every record of the model rebuilt, nothing renamed (the pass-through check: a translator in
 *       place with no rewrite registered must leave every answer as it was);</li>
 *   <li>{@code everywhere}: every call in every place code lives (function bodies, derived properties, constraints,
 *       mapping property expressions, services, test bodies): the DEFINITION-SITE rewrite;</li>
 *   <li>{@code queries}: only the code legend-engine sends to the database: the query lambda of a store-routed call
 *       ({@link #ROUTED}) and the lambda a helper forwards into one, found by a one-level-at-a-time fixpoint over the
 *       model's functions: the USE-SITE rewrite. Every other call keeps Pure's meaning, as legend-pure's interpreter
 *       gives it in legend-engine's own runs (a test body's host-side code, an M2M mapping run in memory).</li>
 * </ul>
 * A store-routed call whose context carries {@code CORRECT_SQL_SUBSTRING_INDEXING} keeps Pure's {@code substring} (the
 * flag is the translator's input; pureToSQLQuery.pure:4660-4666: the flag corrects substring only, indexOf never).
 */
public final class EngineTranslator {

    /** legend-engine's store-routed entries that take the query as their first argument (counted over the corpus). */
    static final Set<String> ROUTED = Set.of("execute", "executionPlan", "toSQLString", "toSQL", "toSQLStringPretty",
            "toNonExecutableSQLString", "executeInternal", "printDebug", "relationalMapperSqlQuery",
            "executeLegendQuery");

    private static final String COMPAT = "meta::legend::compat::engine::";
    private static final String FLAG = "CORRECT_SQL_SUBSTRING_INDEXING";
    public static final String LIBRARY_SOURCE_PREFIX = "compat/";

    public enum Scope { OFF, IDENTITY, EVERYWHERE, QUERIES }

    public static Scope scope() {
        return Scope.valueOf(System.getProperty("compat.translator", "off").trim().toUpperCase(Locale.ROOT));
    }

    /** What one translation did: counts by rewritten function and place, the sites left alone and why. */
    public static final class Report {
        public final Map<String, Integer> rewritten = new TreeMap<>();
        public final Map<String, Integer> keptPure = new TreeMap<>();
        public final Set<String> queryHelpers = new LinkedHashSet<>();
        public final List<String> unresolvedRoutedArguments = new ArrayList<>();
        public final List<String> notes = new ArrayList<>();
        public int recordsRebuilt;
        public long nanos;

        void count(Map<String, Integer> m, String k) {
            m.merge(k, 1, Integer::sum);
        }

        public String summary() {
            return "compat translator: rewritten=" + rewritten + " keptPure=" + keptPure + " queryHelpers="
                    + queryHelpers + " unresolvedRoutedArguments=" + unresolvedRoutedArguments.size()
                    + " recordsRebuilt=" + recordsRebuilt + " ms=" + nanos / 1_000_000 + " notes=" + notes;
        }
    }

    private EngineTranslator() {
    }

    /** The compatibility library for the target the runtime's connection names: a source the caller parses with the
     *  model. Refused by name for a database it has no library for. */
    public static Compiler.ModelSource library(ConnectionDefinition connection) throws IOException {
        String target = switch (connection.databaseType().name()) {
            case "H2" -> "h2";
            case "DuckDB" -> "duckdb";
            default -> throw new UnsupportedOperationException("the ENGINE translator has no substring/indexOf "
                    + "library for " + connection.databaseType() + " (H5 spike: H2 and DuckDB only)");
        };
        String name = "compat-engine-" + target + ".pure";
        try (InputStream in = EngineTranslator.class.getResourceAsStream("/compatspike/" + name)) {
            if (in == null) {
                throw new IOException("missing resource /compatspike/" + name);
            }
            return new Compiler.ModelSource(LIBRARY_SOURCE_PREFIX + name,
                    new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    /** The model with legend-engine's calls renamed, by {@code scope}; the library's own elements are left alone. */
    public static Compiler.ParsedModule translate(Compiler.ParsedModule parsed, Scope scope, Report report) {
        long t0 = System.nanoTime();
        ParsedModel m = parsed.model();
        Map<String, String> sources = m.elementSources();
        long th = System.nanoTime();
        Set<String> helpers = scope == Scope.QUERIES ? queryHelpers(m.elements(), report) : Set.of();
        report.notes.add("helpers ms=" + (System.nanoTime() - th) / 1_000_000 + " elements=" + m.elements().size());
        List<PackageableElement> out = new ArrayList<>(m.elements().size());
        for (PackageableElement el : m.elements()) {
            String src = sources.getOrDefault(ParsedModel.keyOf(el), sources.getOrDefault(el.qualifiedName(), ""));
            if (src.startsWith(LIBRARY_SOURCE_PREFIX) || el.qualifiedName().startsWith(COMPAT) || scope == Scope.OFF) {
                out.add(el);
                continue;
            }
            Walker w = new Walker(scope, helpers, report, el.qualifiedName());
            out.add((PackageableElement) w.map(el, scope == Scope.EVERYWHERE));
        }
        ParsedModel model = new ParsedModel(out, m.imports(), m.source(), m.elementOffsets(), m.elementImports(),
                m.elementSources(), m.unclaimedSections(), m.elementSpans());
        report.nanos += System.nanoTime() - t0;
        return new Compiler.ParsedModule(model, parsed.duplicateElements(), parsed.sourceTexts());
    }

    /** A request's query (pure/v1's lambda, a probe's text): all of it is code legend-engine sends to the store. */
    public static ValueSpecification translateQuery(ValueSpecification query, Report report) {
        long t0 = System.nanoTime();
        Object out = new Walker(Scope.QUERIES, ROUTED, report, "<query>").map(query, true);
        report.nanos += System.nanoTime() - t0;
        return (ValueSpecification) out;
    }

    // ---- which helpers forward a query into a routed call (scope QUERIES) ----------------------------------------

    /** Simple function name to the parameter positions that reach a routed call's query argument, to a fixpoint. */
    private static Set<String> queryHelpers(List<PackageableElement> elements, Report report) {
        Map<String, Set<Integer>> forwarding = new LinkedHashMap<>();
        for (String r : ROUTED) {
            forwarding.put(r, Set.of(0));
        }
        boolean changed = true;
        while (changed) {
            changed = false;
            for (PackageableElement el : elements) {
                if (!(el instanceof com.legend.model.FunctionDefinition f)) {
                    continue;
                }
                List<String> params = f.parameters().stream().map(p -> p.name()).toList();
                Set<Integer> found = new LinkedHashSet<>();
                for (ValueSpecification stmt : f.body()) {
                    collectForwarded(stmt, params, forwarding, found);
                }
                if (found.isEmpty()) {
                    continue;
                }
                String simple = simpleName(f.qualifiedName());
                Set<Integer> had = forwarding.getOrDefault(simple, Set.of());
                if (!had.containsAll(found)) {
                    Set<Integer> merged = new LinkedHashSet<>(had);
                    merged.addAll(found);
                    forwarding.put(simple, merged);
                    report.queryHelpers.add(f.qualifiedName() + found);
                    changed = true;
                }
            }
        }
        return forwarding.keySet();
    }

    /** The parameters of the enclosing function that a call in {@code v} passes in a forwarding position. */
    private static void collectForwarded(Object v, List<String> params, Map<String, Set<Integer>> forwarding,
            Set<Integer> found) {
        if (v instanceof AppliedFunction af) {
            Set<Integer> positions = forwarding.get(simpleName(af.function()));
            if (positions != null) {
                for (int i : positions) {
                    if (i < af.parameters().size() && af.parameters().get(i) instanceof Variable var) {
                        int at = params.indexOf(var.name());
                        if (at >= 0) {
                            found.add(at);
                        }
                    }
                }
            }
        }
        for (Object child : children(v)) {
            collectForwarded(child, params, forwarding, found);
        }
    }

    // ---- the walker ----------------------------------------------------------------------------------------------

    /** A deep map over the model's records: every record whose components changed is rebuilt by its canonical
     *  constructor, so a rename keeps every other field (positions included). */
    private static final class Walker {
        private final Scope scope;
        private final Set<String> routed;
        private final Report report;
        private final String element;
        private final IdentityHashMap<Object, Object> done = new IdentityHashMap<>();
        /** inside a routed call whose context carries CORRECT_SQL_SUBSTRING_INDEXING: substring stays Pure's */
        private final boolean flagged;

        Walker(Scope scope, Set<String> routed, Report report, String element) {
            this(scope, routed, report, element, false);
        }

        Walker(Scope scope, Set<String> routed, Report report, String element, boolean flagged) {
            this.scope = scope;
            this.routed = routed;
            this.report = report;
            this.element = element;
            this.flagged = flagged;
        }

        /** {@code inQuery}: the code is one legend-engine sends to the database (always, for EVERYWHERE). */
        Object map(Object o, boolean inQuery) {
            if (o == null) {
                return null;
            }
            if (o instanceof List<?> l) {
                List<Object> out = null;
                // a statement list: a let-bound lambda a routed call names later is a query too
                Set<String> queryLets = inQuery ? Set.of() : queryLets(l);
                for (int i = 0; i < l.size(); i++) {
                    Object e = l.get(i);
                    boolean q = inQuery || isQueryLet(e, queryLets);
                    Object m = map(e, q);
                    if (m != e && out == null) {
                        out = new ArrayList<>(l.subList(0, i));
                    }
                    if (out != null) {
                        out.add(m);
                    }
                }
                return out == null ? l : out;
            }
            if (o instanceof Map<?, ?> mp) {
                Map<Object, Object> out = null;
                int i = 0;
                for (Map.Entry<?, ?> e : mp.entrySet()) {
                    Object v = map(e.getValue(), inQuery);
                    if (v != e.getValue() && out == null) {
                        out = new LinkedHashMap<>();
                        int j = 0;
                        for (Map.Entry<?, ?> e2 : mp.entrySet()) {
                            if (j++ >= i) {
                                break;
                            }
                            out.put(e2.getKey(), e2.getValue());
                        }
                    }
                    if (out != null) {
                        out.put(e.getKey(), v);
                    }
                    i++;
                }
                return out == null ? mp : out;
            }
            Class<?> c = o.getClass();
            if (!c.isRecord()) {
                return o;
            }
            if (!inQuery && done.containsKey(o)) {
                return done.get(o);
            }
            Shape shape = SHAPES.get(c);
            RecordComponent[] rc = shape.components();
            Object[] values = new Object[rc.length];
            boolean changed = scope == Scope.IDENTITY;
            // a routed call: its query argument is a query; a flagged one keeps Pure's substring
            boolean routedCall = scope == Scope.QUERIES && o instanceof AppliedFunction af0
                    && routed.contains(simpleName(af0.function()));
            boolean flaggedCall = o instanceof AppliedFunction af1 && ROUTED.contains(simpleName(af1.function()))
                    && mentionsFlag(af1);
            for (int i = 0; i < rc.length; i++) {
                Object v = shape.get(i, o);
                Object m;
                if (routedCall && rc[i].getName().equals("parameters") && v instanceof List<?> ps && !ps.isEmpty()) {
                    List<Object> out = new ArrayList<>(ps.size());
                    for (int k = 0; k < ps.size(); k++) {
                        Object p = ps.get(k);
                        if (k == 0) {
                            if (!(p instanceof LambdaFunction) && !(p instanceof Variable)) {
                                report.unresolvedRoutedArguments.add(element + ": " + p.getClass().getSimpleName());
                            }
                            out.add(flaggedCall ? withFlag(p) : map(p, true));
                        } else {
                            out.add(map(p, inQuery));
                        }
                    }
                    m = out;
                    if (!out.equals(ps)) {
                        changed = true;
                    } else {
                        m = ps;
                    }
                } else {
                    m = map(v, inQuery);
                }
                values[i] = m;
                changed |= m != v;
            }
            Object rebuilt = changed ? construct(c, values) : o;
            if (changed) {
                report.recordsRebuilt++;
            }
            if (rebuilt instanceof AppliedFunction af) {
                rebuilt = rename(af, inQuery);
            }
            if (!inQuery) {
                done.put(o, rebuilt);
            }
            return rebuilt;
        }

        /** A flagged routed call's query: walked as a query with substring left as Pure's. */
        private Object withFlag(Object p) {
            return new Walker(scope, routed, report, element, true).map(p, true);
        }

        private AppliedFunction rename(AppliedFunction af, boolean inQuery) {
            String name = af.function();
            int arity = af.parameters().size();
            String simple = simpleName(name);
            boolean ours = name.equals(simple) || name.equals("meta::pure::functions::string::" + simple);
            String key;
            if (ours && simple.equals("substring") && (arity == 2 || arity == 3)) {
                key = "substring/" + arity;
                if (flagged) {
                    report.count(report.keptPure, key + " (flagged)");
                    return af;
                }
            } else if (ours && simple.equals("indexOf") && arity == 2) {
                key = "indexOf/2";
            } else {
                if (ours && simple.equals("indexOf") && arity == 3) {
                    // legend-engine has no SQL translation for it (E-R2: lite answers, in Pure's meaning)
                    report.count(report.keptPure, "indexOf/3 (no upstream translation)");
                }
                return af;
            }
            if (scope == Scope.IDENTITY || !inQuery) {
                report.count(report.keptPure, key);
                return af;
            }
            report.count(report.rewritten, key);
            return new AppliedFunction(COMPAT + simple, af.parameters(), List.of(), af.pos(), af.propertyCall(),
                    af.grouped(), af.infix(), af.island(), af.fControl(), af.ownerClass());
        }
    }

    /** The names a statement list binds by {@code let} to a lambda that a routed call in the same list takes. */
    private static Set<String> queryLets(List<?> statements) {
        Set<String> lets = new LinkedHashSet<>();
        for (Object s : statements) {
            if (s instanceof AppliedFunction af && af.function().equals("letFunction") && af.parameters().size() == 2
                    && af.parameters().get(1) instanceof LambdaFunction
                    && af.parameters().get(0) instanceof com.legend.protocol.spec.CString name) {
                lets.add(name.value());
            }
        }
        if (lets.isEmpty()) {
            return lets;
        }
        Set<String> used = new LinkedHashSet<>();
        for (Object s : statements) {
            collectRoutedVariables(s, used);
        }
        lets.retainAll(used);
        return lets;
    }

    private static void collectRoutedVariables(Object v, Set<String> out) {
        if (v instanceof AppliedFunction af && ROUTED.contains(simpleName(af.function()))
                && !af.parameters().isEmpty() && af.parameters().get(0) instanceof Variable var) {
            out.add(var.name());
        }
        for (Object c : children(v)) {
            collectRoutedVariables(c, out);
        }
    }

    private static boolean isQueryLet(Object s, Set<String> queryLets) {
        return !queryLets.isEmpty() && s instanceof AppliedFunction af && af.function().equals("letFunction")
                && af.parameters().size() == 2 && af.parameters().get(0) instanceof com.legend.protocol.spec.CString n
                && queryLets.contains(n.value());
    }

    private static boolean mentionsFlag(AppliedFunction af) {
        for (int i = 1; i < af.parameters().size(); i++) {
            if (mentions(af.parameters().get(i))) {
                return true;
            }
        }
        return false;
    }

    private static boolean mentions(Object v) {
        if (v instanceof com.legend.protocol.spec.EnumValue e && e.value().equals(FLAG)) {
            return true;
        }
        if (v instanceof com.legend.protocol.spec.AppliedProperty p && p.property().equals(FLAG)) {
            return true;
        }
        for (Object c : children(v)) {
            if (mentions(c)) {
                return true;
            }
        }
        return false;
    }

    // ---- reflection over records (each record class's shape read once) -------------------------------------------

    /** A record class's components, accessors and canonical constructor, made accessible once. */
    private record Shape(RecordComponent[] components, java.lang.reflect.Method[] accessors, Constructor<?> ctor) {
        Object get(int i, Object o) {
            try {
                return accessors[i].invoke(o);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(components[i].toString(), e);
            }
        }
    }

    private static final ClassValue<Shape> SHAPES = new ClassValue<>() {
        @Override
        protected Shape computeValue(Class<?> c) {
            RecordComponent[] rc = c.getRecordComponents();
            java.lang.reflect.Method[] acc = new java.lang.reflect.Method[rc.length];
            Class<?>[] types = new Class<?>[rc.length];
            for (int i = 0; i < rc.length; i++) {
                acc[i] = rc[i].getAccessor();
                acc[i].setAccessible(true);
                types[i] = rc[i].getType();
            }
            try {
                Constructor<?> k = c.getDeclaredConstructor(types);
                k.setAccessible(true);
                return new Shape(rc, acc, k);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("no canonical constructor: " + c.getName(), e);
            }
        }
    };

    private static List<Object> children(Object v) {
        List<Object> out = new ArrayList<>();
        if (v instanceof List<?> l) {
            out.addAll(l);
        } else if (v instanceof Map<?, ?> m) {
            out.addAll(m.values());
        } else if (v != null && v.getClass().isRecord()) {
            Shape sh = SHAPES.get(v.getClass());
            for (int i = 0; i < sh.components().length; i++) {
                Object c = sh.get(i, v);
                if (c != null) {
                    out.add(c);
                }
            }
        }
        return out;
    }

    private static Object construct(Class<?> c, Object[] values) {
        try {
            return SHAPES.get(c).ctor().newInstance(values);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("rebuilding " + c.getName(), e);
        }
    }

    static String simpleName(String fqn) {
        int at = fqn.lastIndexOf("::");
        return at < 0 ? fqn : fqn.substring(at + 2);
    }
}
