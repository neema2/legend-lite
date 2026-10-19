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
 * A value the translation reads more than once, computed once: the translator's own let (2026-10-19). SQL has no
 * scalar let, so a value a helper reads in several places is written out at each (the temporal helpers read their
 * inputs over and over: lite/temporal/nanos_arithmetic was one 33.7 MB statement, 130 s in DuckDB's prepare).
 *
 * <p>A helper marks a value it reads more than once with {@link #share}, where it knows the value is NULL-safe: its
 * SQL on NULL inputs is NULL, never a raise (the temporal parts, a zone's offset, an instant). At the end of the
 * translation, {@link #bind} computes each marked value of a projection over rows ({@code select(extend(src, cols),
 * sel)}) once, in a hidden column of an extend below it, where the projection computes it on every row anyway: on a
 * path from the column that passes no condition but a NULL guard ({@code if(isEmpty(x) or ..., |[], |body)}, whose
 * body is a marked value's NULL on NULL inputs). A marked value only a condition reaches -- an {@code if}'s branch,
 * an {@code and}'s or an {@code or}'s right side, a lambda, a {@code range}'s arguments (which Postgres evaluates
 * when it plans) -- stays where it is, as does one read once; every mark left is then removed. So the SQL of a
 * translation with nothing to share is what it was.
 */
final class Shares {

    /** The mark: never a function of the platform, so a mark left behind fails compilation, by name. */
    static final String MARK = "meta::legend::lite::cypher::shared";

    /** A hidden column's name: the translator's synthetic prefix. */
    private static final String PREFIX = "_cypher_let_";

    /** A call whose arguments Postgres evaluates when it plans (generate_series, Postgres.raise): never moved. */
    private static final String PLANNED = "range";

    private int next;

    private Shares() {
    }

    /** {@code v}, marked as a value its reader reads more than once and NULL-safe; a variable, a constant or a
     *  column read as it is (nothing to share). */
    static ValueSpecification share(ValueSpecification v) {
        if (v instanceof AppliedFunction f && f.function().equals(MARK) || v.children().isEmpty()
                || v instanceof AppliedProperty p && p.receiver() instanceof Variable) {
            return v;
        }
        return com.legend.protocol.sql.SqlToPure.call(MARK, v);
    }

    /** {@code query} with each projection's shared values computed once, and every mark removed. */
    static ValueSpecification bind(ValueSpecification query) {
        Shares s = new Shares();
        ValueSpecification bound = s.rewrite(query, new IdentityHashMap<>());
        return strip(bound, new IdentityHashMap<>());
    }

    private ValueSpecification rewrite(ValueSpecification n,
            IdentityHashMap<ValueSpecification, ValueSpecification> done) {
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
        ValueSpecification result = changed ? n.withChildren(out) : n;
        if (result instanceof AppliedFunction sel && sel.function().equals("select") && sel.parameters().size() == 2
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

    /** Every mark removed. */
    private static ValueSpecification strip(ValueSpecification n,
            IdentityHashMap<ValueSpecification, ValueSpecification> done) {
        ValueSpecification seen = done.get(n);
        if (seen != null) {
            return seen;
        }
        ValueSpecification result;
        if (n instanceof AppliedFunction f && f.function().equals(MARK)) {
            result = strip(f.parameters().get(0), done);
        } else {
            List<ValueSpecification> kids = n.children();
            List<ValueSpecification> out = new ArrayList<>(kids.size());
            boolean changed = false;
            for (ValueSpecification k : kids) {
                ValueSpecification r = strip(k, done);
                changed |= r != k;
                out.add(r);
            }
            result = changed ? n.withChildren(out) : n;
        }
        done.put(n, result);
        return result;
    }

    // ------------------------------------------------------------------ one projection

    /** {@code extend(src, cols)} with its shared values in extends below it, or itself when there are none. */
    private AppliedFunction stage(AppliedFunction ext, ColSpecArray cols) {
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
        a.walk(roots);
        // the marked values to compute once: read more than once, on every row, reading only the row
        Map<String, String> column = new LinkedHashMap<>();
        for (String d : a.marks) {
            ValueSpecification m = java.util.Objects.requireNonNull(a.representative.get(d));
            if (a.occurrences.getOrDefault(d, 0L) >= 2 && a.always.contains(d) && a.readsOnlyRow(m)) {
                column.put(d, PREFIX + next++);
            }
        }
        if (column.isEmpty()) {
            return ext;
        }
        Map<String, Integer> level = new HashMap<>();
        for (String d : column.keySet()) {
            levelOf(d, column, a, level);
        }
        int depth = level.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        ValueSpecification src = ext.parameters().get(0);
        for (int l = 1; l <= depth; l++) {
            List<ColSpec> specs = new ArrayList<>();
            for (Map.Entry<String, String> e : column.entrySet()) {
                if (java.util.Objects.requireNonNull(level.get(e.getKey())) != l) {
                    continue;
                }
                AppliedFunction mark = (AppliedFunction) java.util.Objects.requireNonNull(a.representative.get(
                        e.getKey()));
                ValueSpecification value = replace(mark.parameters().get(0), a, column, row,
                        new IdentityHashMap<>());
                specs.add(new ColSpec(e.getValue(), new LambdaFunction(List.of(new Variable(row, null, null, null)),
                        List.of(value), null), null, null));
            }
            src = com.legend.protocol.sql.SqlToPure.call("extend", src, new ColSpecArray(specs, null));
        }
        List<ColSpec> rewritten = new ArrayList<>();
        IdentityHashMap<ValueSpecification, ValueSpecification> memo = new IdentityHashMap<>();
        for (ColSpec c : cols.colSpecs()) {
            LambdaFunction f = java.util.Objects.requireNonNull(c.function1());
            ValueSpecification body = replace(f.body().get(0), a, column, row, memo);
            rewritten.add(new ColSpec(c.name(), new LambdaFunction(f.parameters(), List.of(body), f.pos()), null,
                    c.alias(), c.args(), c.qualified(), c.pos(), c.colType(), c.colTypeMult(), c.stereotypes(),
                    c.taggedValues()));
        }
        return ext.withParameters(List.of(src, new ColSpecArray(rewritten, cols.pos())));
    }

    /** A shared value's stage: one past the deepest shared value inside it (values nest, so there is no cycle). */
    private static int levelOf(String d, Map<String, String> column, Analysis a, Map<String, Integer> level) {
        Integer known = level.get(d);
        if (known != null) {
            return known;
        }
        AppliedFunction mark = (AppliedFunction) java.util.Objects.requireNonNull(a.representative.get(d));
        int inner = 0;
        for (String i : a.marksInside(mark.parameters().get(0), column.keySet())) {
            inner = Math.max(inner, levelOf(i, column, a, level));
        }
        level.put(d, inner + 1);
        return inner + 1;
    }

    /** {@code n} with every shared value read from its column (under a lambda that binds the row's name again,
     *  nothing: the name there is another). */
    private static ValueSpecification replace(ValueSpecification n, Analysis a, Map<String, String> column,
            String row, IdentityHashMap<ValueSpecification, ValueSpecification> memo) {
        ValueSpecification seen = memo.get(n);
        if (seen != null) {
            return seen;
        }
        ValueSpecification result;
        String col = n instanceof AppliedFunction f && f.function().equals(MARK) ? column.get(a.digest(n)) : null;
        if (col != null) {
            result = new AppliedProperty(new Variable(row, null, null, null), col, null, null);
        } else if (n instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(p -> p.name().equals(row))) {
            result = n;
        } else {
            List<ValueSpecification> kids = n.children();
            List<ValueSpecification> out = new ArrayList<>(kids.size());
            boolean changed = false;
            for (ValueSpecification k : kids) {
                ValueSpecification r = replace(k, a, column, row, memo);
                changed |= r != k;
                out.add(r);
            }
            result = changed ? n.withChildren(out) : n;
        }
        memo.put(n, result);
        return result;
    }

    // ------------------------------------------------------------------ the analysis of one projection's columns

    /** The projection's marks: each one's digest (equal for equal values), occurrences written out, and whether a
     *  path from a column reaches it through no condition but a NULL guard. */
    private static final class Analysis {

        private final String row;
        private final IdentityHashMap<ValueSpecification, String> digests = new IdentityHashMap<>();
        private final IdentityHashMap<ValueSpecification, Set<String>> free = new IdentityHashMap<>();
        final Set<String> marks = new java.util.LinkedHashSet<>();
        final Map<String, Long> occurrences = new HashMap<>();
        final Set<String> always = new HashSet<>();
        final Map<String, ValueSpecification> representative = new HashMap<>();
        private int unique;

        Analysis(String row) {
            this.row = row;
        }

        void walk(List<ValueSpecification> roots) {
            List<ValueSpecification> order = new ArrayList<>();
            IdentityHashMap<ValueSpecification, Boolean> visited = new IdentityHashMap<>();
            for (ValueSpecification r : roots) {
                post(r, visited, order);
            }
            java.util.Collections.reverse(order);
            IdentityHashMap<ValueSpecification, Long> paths = new IdentityHashMap<>();
            IdentityHashMap<ValueSpecification, Boolean> reached = new IdentityHashMap<>();
            for (ValueSpecification r : roots) {
                paths.merge(r, 1L, Shares::add);
                reached.put(r, Boolean.TRUE);
            }
            for (ValueSpecification n : order) {
                long p = paths.getOrDefault(n, 0L);
                if (p == 0) {
                    continue;
                }
                if (n instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(v -> v.name().equals(row))) {
                    continue;
                }
                boolean everyRow = reached.getOrDefault(n, Boolean.FALSE);
                if (n instanceof AppliedFunction f && f.function().equals(MARK)) {
                    String d = digest(n);
                    marks.add(d);
                    occurrences.merge(d, p, Shares::add);
                    representative.putIfAbsent(d, n);
                    if (everyRow) {
                        always.add(d);
                    }
                }
                List<ValueSpecification> kids = n.children();
                for (int i = 0; i < kids.size(); i++) {
                    ValueSpecification k = kids.get(i);
                    paths.merge(k, p, Shares::add);
                    if (everyRow && unconditional(n, i, kids)) {
                        reached.put(k, Boolean.TRUE);
                    }
                }
            }
        }

        /** Whether the projection computes child {@code i} of {@code n} wherever it computes {@code n}: not in an
         *  {@code if}'s branches (but a NULL guard's body), not past an {@code and}'s, an {@code or}'s or a {@code coalesce}'s first argument,
         *  not in a lambda another call takes or a lambda over elements, not in a {@code range}'s arguments. */
        private static boolean unconditional(ValueSpecification n, int i, List<ValueSpecification> kids) {
            if (n instanceof AppliedFunction f) {
                String fn = f.function();
                if (fn.equals(PLANNED)) {
                    return false;
                }
                if (fn.equals("if") && kids.size() == 3) {
                    return i == 0 || i == 2 && nullGuard(kids.get(0), kids.get(1));
                }
                if (fn.equals("and") || fn.equals("or") || fn.equals("coalesce")) {
                    // (the database reads what follows the first only where the first leaves it open)
                    return i == 0;
                }
                return !(kids.get(i) instanceof LambdaFunction);
            }
            if (n instanceof LambdaFunction lf) {
                return lf.parameters().isEmpty();
            }
            return true;
        }

        /** {@code if(condition, |[], ...)} with a condition that only tests emptiness: a NULL guard (SqlToPure's
         *  strict operations), whose other branch a marked value may be computed beside, its NULL on NULL inputs. */
        private static boolean nullGuard(ValueSpecification condition, ValueSpecification then) {
            return then instanceof LambdaFunction t && t.parameters().isEmpty() && t.body().size() == 1
                    && t.body().get(0) instanceof PureCollection pc && pc.values().isEmpty() && emptinessTest(condition);
        }

        private static boolean emptinessTest(ValueSpecification c) {
            if (c instanceof AppliedFunction f) {
                if (f.function().equals("isEmpty") && f.parameters().size() == 1) {
                    return true;
                }
                if (f.function().equals("or")) {
                    return f.parameters().stream().allMatch(Analysis::emptinessTest);
                }
            }
            return false;
        }

        private static void post(ValueSpecification n, IdentityHashMap<ValueSpecification, Boolean> visited,
                List<ValueSpecification> order) {
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

        /** Whether a value reads no name but the row's, nowhere binding the row's name again. */
        boolean readsOnlyRow(ValueSpecification n) {
            Set<String> names = freeNames(n);
            return (names.isEmpty() || names.equals(Set.of(row))) && !rebinds(n, new IdentityHashMap<>());
        }

        private boolean rebinds(ValueSpecification n, IdentityHashMap<ValueSpecification, Boolean> seen) {
            if (seen.put(n, Boolean.TRUE) != null) {
                return false;
            }
            if (n instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(p -> p.name().equals(row))) {
                return true;
            }
            for (ValueSpecification k : n.children()) {
                if (rebinds(k, seen)) {
                    return true;
                }
            }
            return false;
        }

        /** The shared values directly inside {@code n} (not inside another one). */
        Set<String> marksInside(ValueSpecification n, Set<String> shared) {
            Set<String> out = new HashSet<>();
            IdentityHashMap<ValueSpecification, Boolean> seen = new IdentityHashMap<>();
            java.util.ArrayDeque<ValueSpecification> work = new java.util.ArrayDeque<>(n.children());
            work.push(n);
            while (!work.isEmpty()) {
                ValueSpecification v = work.pop();
                if (seen.put(v, Boolean.TRUE) != null) {
                    continue;
                }
                if (v instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(p -> p.name().equals(row))) {
                    continue;
                }
                if (v instanceof AppliedFunction f && f.function().equals(MARK) && shared.contains(digest(v))) {
                    out.add(digest(v));
                    continue;
                }
                work.addAll(v.children());
            }
            return out;
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

        /** Equal for equal values (their spelling, source positions aside); a node of a kind not spelled out here is
         *  equal only to itself, so an unknown kind can only miss a saving, never merge two different values. */
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
                d = sha(b.append(')').toString());
            }
            digests.put(n, d);
            return d;
        }

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
}
