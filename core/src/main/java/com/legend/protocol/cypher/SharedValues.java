// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.cypher;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PROTOTYPE (2026-10-19): a value the translation spells more than once, computed once. The translator builds its
 * expressions as Java objects that share their parts (a temporal helper reads its input in every use), and SQL is
 * text, so each use is written out in full: lite/temporal/nanos_arithmetic was one 33.7 MB statement, 130 s in
 * DuckDB's prepare. Over each {@code select(extend(src, cols), sel)} -- a projection over rows -- the large
 * sub-expressions its columns repeat, reading only the row, are moved into hidden columns of extends before it (one
 * extend per level of nesting) and read from there by name: each stage a nested SELECT, each value the database's
 * once. The select drops the hidden columns, so the projection's columns are unchanged.
 *
 * <p>A value is computed in its column exactly where the projection computed it: one the projection reads on every
 * row (an occurrence under no condition) as it is; one it reads only under conditions -- an {@code if}'s branch, the
 * right side of an {@code and} or an {@code or} -- the same conditions at every occurrence, under those conditions
 * (its column empty elsewhere, read as {@code toOne}); any other (conditions that differ, or a lambda over elements)
 * where it is. A cast a type test guards, a raise its check guards, stay guarded (found: a numeric cast moved out of
 * an ordering's type test raised on a list).
 */
final class SharedValues {

    /** A hidden column's name: the translator's synthetic prefix. */
    private static final String PREFIX = "_cypher_shared_";

    /** A sub-expression smaller than this (its nodes, written out) stays where it is. */
    private static final long MIN_SIZE = 24;

    /** A call whose arguments a database evaluates when it plans (Postgres's generate_series: the planner estimates its
     *  rows from them, so the dialect writes a raise there volatile, Postgres.raise): nothing inside one is moved to a
     *  column, which the planner's pull-up would bring back into the arguments without that mark (found: a range's
     *  null check raised at plan time on Postgres). */
    private static final String PLANNED = "range";

    /** The most paths (after {@link Analysis#simplify}) a value is computed under; past it, it stays. */
    private static final int MAX_GUARDS = 4;

    /** The most distinct condition paths a node's occurrences are followed along; past it, the node stays. */
    private static final int MAX_PATHS = 8;

    /** Calls whose value is a relation or a collection: never a column's value. */
    private static final Set<String> NOT_A_VALUE = Set.of("getAll", "project", "extend", "select", "filter",
            "groupBy", "aggregate", "concatenate", "distinct", "sort", "limit", "take", "drop", "slice", "join",
            "rename", "map", "range", "split", "from", "fold", "zip", "tail", "init", "reverse", "removeDuplicates",
            "union", "list", "keys", "values", "toMany");

    /** One condition on the way to an occurrence: the condition's digest, and whether it held. */
    private record Step(String condition, boolean holds) {
    }

    /** A step past which nothing is known (a lambda over elements, a lazy argument of another call). */
    private static final Step UNKNOWN = new Step("?", false);

    /** The paths of a node reached along more than {@link #MAX_PATHS}. */
    private static final Set<List<Step>> TOO_MANY = Set.of(List.of(new Step("!", false)));

    private int next;

    private SharedValues() {
    }

    /** {@code query} with each projection's repeated large values computed once. */
    static ValueSpecification hoist(ValueSpecification query) {
        return new SharedValues().rewrite(query, new IdentityHashMap<>());
    }

    private ValueSpecification rewrite(ValueSpecification n, IdentityHashMap<ValueSpecification,
            ValueSpecification> done) {
        ValueSpecification seen = done.get(n);
        if (seen != null) {
            return seen;
        }
        List<ValueSpecification> kids = n.children();
        List<ValueSpecification> out = new ArrayList<>(kids.size());
        boolean changed = false;
        for (ValueSpecification k : kids) {
            ValueSpecification r = rewrite(k, done);
            changed |= r != k;
            out.add(r);
        }
        ValueSpecification rebuilt = changed ? n.withChildren(out) : n;
        ValueSpecification result = rebuilt;
        if (rebuilt instanceof AppliedFunction sel && sel.function().equals("select") && sel.parameters().size() == 2
                && sel.parameters().get(0) instanceof AppliedFunction ext && ext.function().equals("extend")
                && ext.parameters().size() == 2 && ext.parameters().get(1) instanceof ColSpecArray cols) {
            AppliedFunction staged = stage(ext, cols);
            if (staged != ext) {
                List<ValueSpecification> ps = new ArrayList<>(sel.parameters());
                ps.set(0, staged);
                result = sel.withParameters(ps);
            }
        }
        done.put(n, result);
        return result;
    }

    // ------------------------------------------------------------------ one projection

    /** A chosen value: its column, and the paths of conditions it is computed under (one empty path: on every row;
     *  else wherever one of them holds). */
    private record Chosen(ValueSpecification node, String column, List<List<Step>> guards) {

        boolean plain() {
            return guards.size() == 1 && guards.get(0).isEmpty();
        }
    }

    /** {@code extend(src, cols)} with its repeated large values in extends before it, or itself when none. */
    private AppliedFunction stage(AppliedFunction ext, ColSpecArray cols) {
        // every column a lambda of one row parameter, the same name throughout (else: as it is)
        String row = null;
        for (ColSpec c : cols.colSpecs()) {
            LambdaFunction f = c.function1();
            if (f == null || c.function2() != null || f.parameters().size() != 1 || f.body().size() != 1) {
                return ext;
            }
            String p = f.parameters().get(0).name();
            if (row == null) {
                row = p;
            } else if (!row.equals(p)) {
                return ext;
            }
        }
        if (row == null) {
            return ext;
        }
        Analysis a = new Analysis(row);
        List<ValueSpecification> roots = new ArrayList<>();
        for (ColSpec c : cols.colSpecs()) {
            roots.add(java.util.Objects.requireNonNull(c.function1()).body().get(0));
        }
        a.count(roots);
        Map<String, Chosen> chosen = new LinkedHashMap<>();
        for (Map.Entry<String, Long> e : a.occurrences.entrySet()) {
            ValueSpecification rep = java.util.Objects.requireNonNull(a.representative.get(e.getKey()));
            if (e.getValue() < 2 || a.size(rep) < MIN_SIZE || !a.hoistable(rep)) {
                continue;
            }
            List<List<Step>> guards = a.guardsOf(e.getKey());
            if (guards != null) {
                chosen.put(e.getKey(), new Chosen(rep, PREFIX + next++, guards));
            }
        }
        if (chosen.isEmpty()) {
            return ext;
        }
        // the levels; a value that needs itself (through a condition of its own guard) stays where it is, and the
        // levels are taken again without it
        Map<String, Integer> level = new HashMap<>();
        while (true) {
            level.clear();
            String cyclic = null;
            for (String d : chosen.keySet()) {
                cyclic = levelOf(d, chosen, a, level, new HashSet<>());
                if (cyclic != null) {
                    break;
                }
            }
            if (cyclic == null) {
                break;
            }
            chosen.remove(cyclic);
        }
        if (chosen.isEmpty()) {
            return ext;
        }
        int depth = level.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        ValueSpecification src = ext.parameters().get(0);
        String r = row;
        for (int l = 1; l <= depth; l++) {
            List<ColSpec> specs = new ArrayList<>();
            for (Map.Entry<String, Chosen> e : chosen.entrySet()) {
                if (java.util.Objects.requireNonNull(level.get(e.getKey())) != l) {
                    continue;
                }
                specs.add(new ColSpec(e.getValue().column(), lambda(r, definition(e.getKey(), e.getValue(), a, chosen,
                        r)), null, null));
            }
            src = call("extend", src, new ColSpecArray(specs, null));
        }
        List<ColSpec> rewritten = new ArrayList<>();
        IdentityHashMap<ValueSpecification, ValueSpecification> memo = new IdentityHashMap<>();
        for (ColSpec c : cols.colSpecs()) {
            LambdaFunction f = java.util.Objects.requireNonNull(c.function1());
            ValueSpecification body = replace(f.body().get(0), a, chosen, r, null, memo);
            rewritten.add(new ColSpec(c.name(), new LambdaFunction(f.parameters(), List.of(body), f.pos()), null,
                    c.alias(), c.args(), c.qualified(), c.pos(), c.colType(), c.colTypeMult(), c.stereotypes(),
                    c.taggedValues()));
        }
        return ext.withParameters(List.of(src, new ColSpecArray(rewritten, cols.pos())));
    }

    /** A chosen value's column's value: the value (its chosen insides read from their columns) -- on every row, or
     *  where one of its paths is reached, each path a Boolean that reads its conditions outermost first, each only
     *  where the one before it held as the projection needed (so no condition is read where it was not), the value
     *  written once. */
    private static ValueSpecification definition(String d, Chosen c, Analysis a, Map<String, Chosen> chosen,
            String row) {
        ValueSpecification body = replace(c.node(), a, chosen, row, d, new IdentityHashMap<>());
        if (c.plain()) {
            return body;
        }
        ValueSpecification reached = null;
        for (List<Step> path : c.guards()) {
            ValueSpecification b = new CBoolean(true, null);
            for (int i = path.size() - 1; i >= 0; i--) {
                Step s = path.get(i);
                ValueSpecification condition = replace(java.util.Objects.requireNonNull(a.representative.get(
                        s.condition())), a, chosen, row, null, new IdentityHashMap<>());
                b = s.holds() ? call("if", condition, thunk(b), thunk(new CBoolean(false, null)))
                        : call("if", condition, thunk(new CBoolean(false, null)), thunk(b));
            }
            reached = reached == null ? b : call("or", reached, b);
        }
        return call("if", java.util.Objects.requireNonNull(reached), thunk(body), thunk(empty()));
    }

    /** A chosen value's level (into {@code level}): one more than the deepest chosen value inside it or inside its
     *  guards' conditions; the digest of a value met again on its own way down (a cycle), else null. */
    private static @com.legend.base.Nullable String levelOf(String d, Map<String, Chosen> chosen, Analysis a,
            Map<String, Integer> level, Set<String> visiting) {
        if (level.containsKey(d)) {
            return null;
        }
        if (!visiting.add(d)) {
            return d;
        }
        Chosen c = java.util.Objects.requireNonNull(chosen.get(d));
        Set<String> inside = new HashSet<>(a.chosenInside(c.node(), chosen.keySet()));
        for (Step s : c.guards().stream().flatMap(List::stream).toList()) {
            if (chosen.containsKey(s.condition())) {
                inside.add(s.condition());
            } else {
                inside.addAll(a.chosenInside(java.util.Objects.requireNonNull(a.representative.get(s.condition())),
                        chosen.keySet()));
            }
        }
        inside.remove(d);
        int inner = 0;
        for (String i : inside) {
            String cyclic = levelOf(i, chosen, a, level, visiting);
            if (cyclic != null) {
                return cyclic;
            }
            inner = Math.max(inner, java.util.Objects.requireNonNull(level.get(i)));
        }
        visiting.remove(d);
        level.put(d, inner + 1);
        return null;
    }

    /** {@code n} with every chosen value (but {@code self}, the value being defined) read from its column -- under
     *  {@code toOne} where its column is guarded; under a lambda that binds the row's name again, nothing is replaced
     *  (the name there is another). */
    private static ValueSpecification replace(ValueSpecification n, Analysis a, Map<String, Chosen> chosen,
            String row, @com.legend.base.Nullable String self,
            IdentityHashMap<ValueSpecification, ValueSpecification> memo) {
        ValueSpecification seen = memo.get(n);
        if (seen != null) {
            return seen;
        }
        String d = a.digest(n);
        Chosen c = d.equals(self) ? null : chosen.get(d);
        ValueSpecification result;
        if (c != null) {
            ValueSpecification read = new AppliedProperty(new Variable(row, null, null, null), c.column(), null, null);
            result = c.plain() ? read : call("toOne", read);
        } else if (n instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(p -> p.name().equals(row))
                || n instanceof AppliedFunction f && f.function().equals(PLANNED)) {
            result = n;
        } else {
            List<ValueSpecification> kids = n.children();
            List<ValueSpecification> out = new ArrayList<>(kids.size());
            boolean changed = false;
            for (ValueSpecification k : kids) {
                ValueSpecification r = replace(k, a, chosen, row, self, memo);
                changed |= r != k;
                out.add(r);
            }
            result = changed ? n.withChildren(out) : n;
        }
        memo.put(n, result);
        return result;
    }

    // ------------------------------------------------------------------ the analysis of one projection's columns

    /** Each node's digest (equal for equal sub-expressions), size written out, free names, occurrences, and the
     *  conditions each is reached under. */
    private static final class Analysis {

        private final String row;
        private final IdentityHashMap<ValueSpecification, String> digests = new IdentityHashMap<>();
        private final IdentityHashMap<ValueSpecification, Long> sizes = new IdentityHashMap<>();
        private final IdentityHashMap<ValueSpecification, Set<String>> free = new IdentityHashMap<>();
        private final IdentityHashMap<ValueSpecification, Boolean> shadowed = new IdentityHashMap<>();
        final Map<String, Long> occurrences = new LinkedHashMap<>();
        final Map<String, ValueSpecification> representative = new HashMap<>();
        private final Map<String, Set<List<Step>>> pathsOf = new HashMap<>();
        private int unique;

        Analysis(String row) {
            this.row = row;
        }

        /** Each node's occurrences in the columns written out (a shared node once per path to it), and the
         *  distinct condition paths it is reached along. */
        void count(List<ValueSpecification> roots) {
            List<ValueSpecification> order = new ArrayList<>();
            IdentityHashMap<ValueSpecification, Boolean> visited = new IdentityHashMap<>();
            for (ValueSpecification r : roots) {
                post(r, visited, order);
            }
            java.util.Collections.reverse(order);
            IdentityHashMap<ValueSpecification, Long> paths = new IdentityHashMap<>();
            IdentityHashMap<ValueSpecification, Set<List<Step>>> steps = new IdentityHashMap<>();
            for (ValueSpecification r : roots) {
                paths.merge(r, 1L, SharedValues::add);
                steps.computeIfAbsent(r, k -> new HashSet<>()).add(List.of());
            }
            for (ValueSpecification n : order) {
                long p = paths.getOrDefault(n, 0L);
                if (p == 0) {
                    continue;
                }
                // a node under a lambda that rebinds the row's name is not the row's: not followed there
                if (n instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(v -> v.name().equals(row))) {
                    continue;
                }
                String d = digest(n);
                occurrences.merge(d, p, SharedValues::add);
                representative.putIfAbsent(d, n);
                Set<List<Step>> mine = steps.getOrDefault(n, TOO_MANY);
                pathsOf.merge(d, mine, Analysis::union);
                List<ValueSpecification> kids = n.children();
                for (int i = 0; i < kids.size(); i++) {
                    ValueSpecification k = kids.get(i);
                    paths.merge(k, p, SharedValues::add);
                    List<Step> edge = edge(n, i, kids);
                    Set<List<Step>> theirs = steps.get(k);
                    if (mine == TOO_MANY || theirs == TOO_MANY) {
                        steps.put(k, TOO_MANY);
                        continue;
                    }
                    Set<List<Step>> added = theirs == null ? new HashSet<>() : theirs;
                    for (List<Step> path : mine) {
                        List<Step> longer = new ArrayList<>(path);
                        longer.addAll(edge);
                        added.add(List.copyOf(longer));
                    }
                    steps.put(k, added.size() > MAX_PATHS ? TOO_MANY : added);
                }
            }
        }

        private static Set<List<Step>> union(Set<List<Step>> a, Set<List<Step>> b) {
            if (a == TOO_MANY || b == TOO_MANY) {
                return TOO_MANY;
            }
            Set<List<Step>> out = new HashSet<>(a);
            out.addAll(b);
            return out.size() > MAX_PATHS ? TOO_MANY : out;
        }

        /** The conditions on the way from {@code n} to its child {@code i}: an {@code if}'s condition into its
         *  branches, an {@code and}'s or an {@code or}'s first operand into its second; past a lambda over elements,
         *  or into a lambda another call takes, nothing known. */
        private List<Step> edge(ValueSpecification n, int i, List<ValueSpecification> kids) {
            if (n instanceof AppliedFunction f) {
                String fn = f.function();
                if (fn.equals(PLANNED)) {
                    return List.of(UNKNOWN);
                }
                if (fn.equals("if") && kids.size() == 3 && i > 0) {
                    return List.of(new Step(digest(kids.get(0)), i == 1));
                }
                if ((fn.equals("and") || fn.equals("or")) && kids.size() == 2 && i == 1) {
                    return List.of(new Step(digest(kids.get(0)), fn.equals("and")));
                }
                if (kids.get(i) instanceof LambdaFunction && !fn.equals("if")) {
                    return List.of(UNKNOWN);
                }
                return List.of();
            }
            if (n instanceof LambdaFunction lf && !lf.parameters().isEmpty()) {
                return List.of(UNKNOWN);
            }
            return List.of();
        }

        /** Where a repeated value may be computed: on every row (an empty guard) when some occurrence is under no
         *  condition; under one path's conditions when every occurrence is under the same ones, each condition a
         *  value of the row; null (it stays where it is) otherwise. */
        @com.legend.base.Nullable List<List<Step>> guardsOf(String d) {
            Set<List<Step>> ps = pathsOf.get(d);
            if (ps == null || ps == TOO_MANY) {
                return null;
            }
            Set<List<Step>> simple = simplify(ps);
            if (simple.contains(List.of())) {
                return List.of(List.of());
            }
            if (simple.size() > MAX_GUARDS) {
                return null;
            }
            for (List<Step> path : simple) {
                for (Step s : path) {
                    if (s.condition().equals("?")) {
                        return null;
                    }
                    ValueSpecification c = representative.get(s.condition());
                    if (c == null) {
                        return null;
                    }
                    Set<String> names = freeNames(c);
                    if (!(names.isEmpty() || names.equals(Set.of(row))) || shadows(c)) {
                        return null;
                    }
                }
            }
            List<List<Step>> out = new ArrayList<>(simple);
            out.sort(java.util.Comparator.comparing(Object::toString));
            return out;
        }

        /** The same occurrences' paths, fewer: a path that extends another is covered by it (where the shorter holds
         *  the value is read), and the two branches of one condition after the same path are that path (one of them
         *  is taken wherever the condition is read). */
        private static Set<List<Step>> simplify(Set<List<Step>> in) {
            Set<List<Step>> s = new HashSet<>(in);
            boolean changed = true;
            while (changed) {
                changed = false;
                for (List<Step> p : List.copyOf(s)) {
                    for (List<Step> q : s) {
                        if (q.size() < p.size() && p.subList(0, q.size()).equals(q)) {
                            s.remove(p);
                            changed = true;
                            break;
                        }
                    }
                }
                for (List<Step> p : List.copyOf(s)) {
                    if (p.isEmpty() || !s.contains(p)) {
                        continue;
                    }
                    Step last = p.get(p.size() - 1);
                    if (last.condition().equals("?")) {
                        continue;
                    }
                    List<Step> prefix = p.subList(0, p.size() - 1);
                    List<Step> other = new ArrayList<>(prefix);
                    other.add(new Step(last.condition(), !last.holds()));
                    if (s.contains(other)) {
                        s.remove(p);
                        s.remove(other);
                        s.add(List.copyOf(prefix));
                        changed = true;
                    }
                }
            }
            return s;
        }

        private static void post(ValueSpecification n, IdentityHashMap<ValueSpecification, Boolean> visited,
                List<ValueSpecification> order) {
            // iterative post-order (the trees are deep)
            java.util.ArrayDeque<Object[]> stack = new java.util.ArrayDeque<>();
            if (visited.put(n, Boolean.TRUE) != null) {
                return;
            }
            stack.push(new Object[] {n, 0});
            while (!stack.isEmpty()) {
                Object[] top = stack.peek();
                ValueSpecification v = (ValueSpecification) top[0];
                List<ValueSpecification> kids = v.children();
                int i = (Integer) top[1];
                if (i < kids.size()) {
                    top[1] = i + 1;
                    ValueSpecification k = kids.get(i);
                    if (visited.put(k, Boolean.TRUE) == null) {
                        stack.push(new Object[] {k, 0});
                    }
                } else {
                    stack.pop();
                    order.add(v);
                }
            }
        }

        /** Whether a node may be a column's value: a call or a property read, of a value (not a relation or a
         *  collection), reading no name but the row's, nowhere rebinding the row's name. */
        boolean hoistable(ValueSpecification n) {
            if (!(n instanceof AppliedFunction || n instanceof AppliedProperty)) {
                return false;
            }
            if (n instanceof AppliedFunction f && NOT_A_VALUE.contains(f.function())) {
                return false;
            }
            Set<String> names = freeNames(n);
            if (!(names.isEmpty() || names.equals(Set.of(row)))) {
                return false;
            }
            return !shadows(n);
        }

        /** The chosen values directly inside {@code n} (not inside another chosen value). */
        Set<String> chosenInside(ValueSpecification n, Set<String> chosen) {
            Set<String> out = new HashSet<>();
            IdentityHashMap<ValueSpecification, Boolean> seen = new IdentityHashMap<>();
            java.util.ArrayDeque<ValueSpecification> work = new java.util.ArrayDeque<>(n.children());
            while (!work.isEmpty()) {
                ValueSpecification v = work.pop();
                if (seen.put(v, Boolean.TRUE) != null) {
                    continue;
                }
                if (v instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(p -> p.name().equals(row))) {
                    continue;
                }
                String d = digest(v);
                if (chosen.contains(d)) {
                    out.add(d);
                    continue;
                }
                work.addAll(v.children());
            }
            return out;
        }

        long size(ValueSpecification n) {
            Long s = sizes.get(n);
            if (s != null) {
                return s;
            }
            long total = 1;
            for (ValueSpecification k : n.children()) {
                total = add(total, size(k));
            }
            sizes.put(n, total);
            return total;
        }

        Set<String> freeNames(ValueSpecification n) {
            Set<String> s = free.get(n);
            if (s != null) {
                return s;
            }
            Set<String> out = new HashSet<>();
            if (n instanceof Variable v) {
                out.add(v.name());
            } else {
                for (ValueSpecification k : n.children()) {
                    out.addAll(freeNames(k));
                }
                if (n instanceof LambdaFunction lf) {
                    lf.parameters().forEach(p -> out.remove(p.name()));
                }
            }
            free.put(n, out);
            return out;
        }

        /** Whether a lambda inside {@code n} binds the row's name again. */
        boolean shadows(ValueSpecification n) {
            Boolean s = shadowed.get(n);
            if (s != null) {
                return s;
            }
            boolean out = n instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(p -> p.name()
                    .equals(row));
            if (!out) {
                for (ValueSpecification k : n.children()) {
                    if (shadows(k)) {
                        out = true;
                        break;
                    }
                }
            }
            shadowed.put(n, out);
            return out;
        }

        /** Equal for equal sub-expressions (their spelling, their source positions aside); a node of a kind not
         *  spelled out here is equal only to itself, so an unknown kind can only miss a saving, never merge two
         *  different values. */
        String digest(ValueSpecification n) {
            String d = digests.get(n);
            if (d != null) {
                return d;
            }
            String own = local(n);
            if (own == null) {
                d = "U" + unique++;
            } else {
                StringBuilder b = new StringBuilder(own).append('(');
                for (ValueSpecification k : n.children()) {
                    b.append(digest(k)).append(',');
                }
                b.append(')');
                d = sha(b.toString());
            }
            digests.put(n, d);
            return d;
        }

        /** A node's own spelling, its children aside; null for a kind whose meaning this does not spell out. */
        private static @com.legend.base.Nullable String local(ValueSpecification n) {
            return switch (n) {
                case AppliedFunction f -> "F:" + f.function() + ":" + f.propertyCall() + ":" + f.referents() + ":"
                        + f.fControl() + ":" + f.ownerClass();
                case AppliedProperty p -> "P:" + p.property() + ":" + p.ownerClass();
                case Variable v -> "V:" + v.name();
                case LambdaFunction l -> "L:" + l.parameters().stream().map(Variable::name).toList();
                case ColSpec c -> "CS:" + c.name() + ":" + c.alias() + ":" + c.qualified();
                case ColSpecArray ignored -> "CA";
                case PureCollection ignored -> "C";
                case NewInstance ni -> "N:" + ni.className() + ":" + ni.typeArguments() + ":"
                        + ni.typeMultiplicityArguments() + ":" + ni.properties().stream().map(b -> b.key() + "/"
                        + b.expression().isAdd() + "/" + b.expression().isLocal()).toList();
                case CString s -> "S:" + s.value();
                case CInteger i -> "I:" + i.value();
                case CFloat f -> "D:" + f.value();
                case CDecimal f -> "M:" + f.value();
                case CBoolean b -> "B:" + b.value();
                case EnumValue e -> "E:" + e.fullPath() + "." + e.value();
                case PackageableElementPtr p -> "R:" + p.fullPath();
                default -> null;
            };
        }

        private static String sha(String s) {
            try {
                byte[] h = MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8));
                return java.util.HexFormat.of().formatHex(h, 0, 16);
            } catch (java.security.NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static long add(long a, long b) {
        long s = a + b;
        return s < 0 ? Long.MAX_VALUE : s;
    }

    private static AppliedFunction call(String fn, ValueSpecification... args) {
        return com.legend.protocol.sql.SqlToPure.call(fn, args);
    }

    private static LambdaFunction lambda(String parameter, ValueSpecification body) {
        return new LambdaFunction(List.of(new Variable(parameter, null, null, null)), List.of(body), null);
    }

    private static LambdaFunction thunk(ValueSpecification body) {
        return new LambdaFunction(List.of(), List.of(body), null);
    }

    private static PureCollection empty() {
        return new PureCollection(List.of(), null);
    }
}
