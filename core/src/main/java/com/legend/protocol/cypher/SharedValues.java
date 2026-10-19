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
import com.legend.protocol.spec.PackageableElementPtr;
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
 */
final class SharedValues {

    /** A hidden column's name: the translator's synthetic prefix. */
    private static final String PREFIX = "_cypher_shared_";

    /** A sub-expression smaller than this (its nodes, written out) stays where it is. */
    private static final long MIN_SIZE = 24;

    /** Calls whose value is a relation or a collection: never a column's value. */
    private static final Set<String> NOT_A_VALUE = Set.of("getAll", "project", "extend", "select", "filter",
            "groupBy", "aggregate", "concatenate", "distinct", "sort", "limit", "take", "drop", "slice", "join",
            "rename", "map", "range", "split", "from", "fold", "zip", "tail", "init", "reverse", "removeDuplicates",
            "union", "list", "keys", "values", "toMany", "meta::pure::functions::collection::range");

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
        // the candidates: repeated, large, reading only the row, a value
        Map<String, ValueSpecification> chosen = new LinkedHashMap<>();
        for (Map.Entry<String, Long> e : a.occurrences.entrySet()) {
            ValueSpecification rep = java.util.Objects.requireNonNull(a.representative.get(e.getKey()));
            if (e.getValue() >= 2 && a.size(rep) >= MIN_SIZE && a.hoistable(rep)) {
                chosen.put(e.getKey(), rep);
            }
        }
        if (chosen.isEmpty()) {
            return ext;
        }
        // each chosen value's column, and its level: one more than the deepest chosen value inside it
        Map<String, String> column = new HashMap<>();
        for (String d : chosen.keySet()) {
            column.put(d, PREFIX + next++);
        }
        Map<String, Integer> level = new HashMap<>();
        for (String d : chosen.keySet()) {
            levelOf(d, chosen, a, level);
        }
        int depth = level.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        ValueSpecification src = ext.parameters().get(0);
        String r = row;
        for (int l = 1; l <= depth; l++) {
            List<ColSpec> specs = new ArrayList<>();
            for (Map.Entry<String, ValueSpecification> e : chosen.entrySet()) {
                if (java.util.Objects.requireNonNull(level.get(e.getKey())) != l) {
                    continue;
                }
                // its definition: its own node with the chosen values inside it read from their columns
                ValueSpecification body = replace(e.getValue(), a, column, r, e.getKey(), new IdentityHashMap<>());
                specs.add(new ColSpec(java.util.Objects.requireNonNull(column.get(e.getKey())), lambda(r, body),
                        null, null));
            }
            src = call("extend", src, new ColSpecArray(specs, null));
        }
        List<ColSpec> rewritten = new ArrayList<>();
        IdentityHashMap<ValueSpecification, ValueSpecification> memo = new IdentityHashMap<>();
        for (ColSpec c : cols.colSpecs()) {
            LambdaFunction f = java.util.Objects.requireNonNull(c.function1());
            ValueSpecification body = replace(f.body().get(0), a, column, r, null, memo);
            rewritten.add(new ColSpec(c.name(), new LambdaFunction(f.parameters(), List.of(body), f.pos()), null,
                    c.alias(), c.args(), c.qualified(), c.pos(), c.colType(), c.colTypeMult(), c.stereotypes(),
                    c.taggedValues()));
        }
        return ext.withParameters(List.of(src, new ColSpecArray(rewritten, cols.pos())));
    }

    private static int levelOf(String d, Map<String, ValueSpecification> chosen, Analysis a,
            Map<String, Integer> level) {
        Integer known = level.get(d);
        if (known != null) {
            return known;
        }
        int inner = 0;
        for (String c : a.chosenInside(java.util.Objects.requireNonNull(chosen.get(d)), chosen.keySet())) {
            inner = Math.max(inner, levelOf(c, chosen, a, level));
        }
        level.put(d, inner + 1);
        return inner + 1;
    }

    /** {@code n} with every chosen value (but {@code self}, the node being defined) read from its column; under a
     *  lambda that binds the row's name again, nothing is replaced (the name there is another). */
    private static ValueSpecification replace(ValueSpecification n, Analysis a, Map<String, String> column,
            String row, @com.legend.base.Nullable String self, IdentityHashMap<ValueSpecification, ValueSpecification> memo) {
        ValueSpecification seen = memo.get(n);
        if (seen != null) {
            return seen;
        }
        String d = a.digest(n);
        ValueSpecification result;
        String col = d.equals(self) ? null : column.get(d);
        if (col != null) {
            result = new AppliedProperty(new Variable(row, null, null, null), col, null, null);
        } else if (n instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(p -> p.name().equals(row))) {
            result = n;
        } else {
            List<ValueSpecification> kids = n.children();
            List<ValueSpecification> out = new ArrayList<>(kids.size());
            boolean changed = false;
            for (ValueSpecification k : kids) {
                ValueSpecification r = replace(k, a, column, row, self, memo);
                changed |= r != k;
                out.add(r);
            }
            result = changed ? n.withChildren(out) : n;
        }
        memo.put(n, result);
        return result;
    }

    // ------------------------------------------------------------------ the analysis of one projection's columns

    /** Each node's digest (equal for equal sub-expressions), size written out, free names and occurrences. */
    private static final class Analysis {

        private final String row;
        private final IdentityHashMap<ValueSpecification, String> digests = new IdentityHashMap<>();
        private final IdentityHashMap<ValueSpecification, Long> sizes = new IdentityHashMap<>();
        private final IdentityHashMap<ValueSpecification, Set<String>> free = new IdentityHashMap<>();
        private final IdentityHashMap<ValueSpecification, Boolean> shadowed = new IdentityHashMap<>();
        final Map<String, Long> occurrences = new LinkedHashMap<>();
        final Map<String, ValueSpecification> representative = new HashMap<>();

        Analysis(String row) {
            this.row = row;
        }

        /** Each node's number of occurrences in the columns written out (a shared node once per path to it). */
        void count(List<ValueSpecification> roots) {
            // the nodes in an order where each comes before its children
            List<ValueSpecification> order = new ArrayList<>();
            IdentityHashMap<ValueSpecification, Boolean> visited = new IdentityHashMap<>();
            for (ValueSpecification r : roots) {
                post(r, visited, order);
            }
            java.util.Collections.reverse(order);
            IdentityHashMap<ValueSpecification, Long> paths = new IdentityHashMap<>();
            for (ValueSpecification r : roots) {
                paths.merge(r, 1L, SharedValues::add);
            }
            for (ValueSpecification n : order) {
                long p = paths.getOrDefault(n, 0L);
                if (p == 0) {
                    continue;
                }
                // a node under a lambda that rebinds the row's name is not the row's: not counted there
                if (n instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(v -> v.name().equals(row))) {
                    continue;
                }
                String d = digest(n);
                occurrences.merge(d, p, SharedValues::add);
                representative.putIfAbsent(d, n);
                for (ValueSpecification k : n.children()) {
                    paths.merge(k, p, SharedValues::add);
                }
            }
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

        /** Equal for equal sub-expressions (their spelling, their source positions aside). */
        String digest(ValueSpecification n) {
            String d = digests.get(n);
            if (d != null) {
                return d;
            }
            StringBuilder b = new StringBuilder();
            b.append(local(n)).append('(');
            for (ValueSpecification k : n.children()) {
                b.append(digest(k)).append(',');
            }
            b.append(')');
            d = sha(b.toString());
            digests.put(n, d);
            return d;
        }

        private static String local(ValueSpecification n) {
            return switch (n) {
                case AppliedFunction f -> "F:" + f.function() + ":" + f.propertyCall() + ":" + f.referents() + ":"
                        + f.fControl() + ":" + f.ownerClass();
                case AppliedProperty p -> "P:" + p.property() + ":" + p.ownerClass();
                case Variable v -> "V:" + v.name();
                case LambdaFunction l -> "L:" + l.parameters().stream().map(Variable::name).toList();
                case ColSpec c -> "CS:" + c.name() + ":" + c.alias() + ":" + c.qualified();
                case ColSpecArray ignored -> "CA";
                case CString s -> "S:" + s.value();
                case CInteger i -> "I:" + i.value();
                case CFloat f -> "D:" + f.value();
                case CDecimal f -> "M:" + f.value();
                case CBoolean b -> "B:" + b.value();
                case EnumValue e -> "E:" + e.fullPath() + "." + e.value();
                case PackageableElementPtr p -> "R:" + p.fullPath();
                // (a leaf's own spelling; a composite node's is its kind, its children's digests beside it -- never
                // its toString, which writes the whole tree out)
                default -> n.children().isEmpty() ? "X:" + n.getClass().getSimpleName() + ":" + n
                        : "X:" + n.getClass().getSimpleName();
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
}
