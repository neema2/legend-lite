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
 * SQL on NULL inputs is NULL, never a raise (every strict operation's operand, which it reads in its NULL guard and in
 * its value; the temporal parts; an instant). At the end of the translation, {@link #bind} computes each marked value
 * of a projection over rows ({@code select(extend(src, cols), sel)}) once, in a hidden column of an extend below it
 * (fenced by an {@code OFFSET 0}, which Postgres's planner does not flatten), where the projection computes it on
 * every row anyway: anticipated, as a compiler's code motion has it -- computed unconditionally, or in both of an
 * {@code if}'s branches, or in a NULL guard's body ({@code if(isEmpty(x) or ..., |[], |body)}, where a marked value is
 * NULL on the NULL inputs the guard tests). A marked value only a condition reaches -- one branch of an {@code if},
 * what follows an {@code and}'s, an {@code or}'s or a {@code coalesce}'s first argument, a lambda, a {@code range}'s
 * arguments (which Postgres evaluates when it plans) -- stays where it is, as does one read once; every mark left is
 * then removed. A projection over a class's instances ({@code project(getAll(C), cols)}) is staged the same way once
 * the property chains its columns read are projected into columns of their own (each one value, as the translator's
 * model says: {@link Paths}). So the SQL of a translation with nothing to share is what it was.
 */
final class Shares {

    /** The mark: a name no function has, so a mark left behind fails compilation, by name. */
    static final String MARK = "cypher shared value";

    /** A hidden column's name: the translator's synthetic prefix. */
    private static final String PREFIX = "_cypher_let_";

    /** The nodes a value must save to be computed once, as many as the Lowerer's fold lets a read of a column write
     *  back into its readers (Fold.Budget.BLOAT): a smaller saving is not worth a column and a subquery. */
    private static final long WORTH = 100;

    /** A call whose arguments Postgres evaluates when it plans (generate_series, Postgres.raise): never moved. */
    private static final String PLANNED = "range";

    /** Whether a chain of properties from a class reads one value a relation's column holds: each step to-one, the
     *  last of no class (what the translator, which holds the model, knows; {@link #stageClass}). */
    interface Paths {
        boolean scalar(String classPath, List<String> chain);
    }

    private final Paths paths;

    private int next;

    private Shares(Paths paths) {
        this.paths = paths;
    }

    /** Whether {@code n} is a call of {@code function}. The binder reads back the Pure the translator wrote, before
     *  any resolution: a name of the translator's own writing (a mark, a projection's shape, a construct the database
     *  evaluates lazily), never a resolved declaration's -- read here alone. */
    private static boolean named(ValueSpecification n, String function) {
        return n instanceof AppliedFunction f && f.function().equals(function);
    }

    /** {@code v}, marked as a value its reader reads more than once and NULL-safe; a variable, a constant or a
     *  column read as it is (nothing to share). */
    static ValueSpecification share(ValueSpecification v) {
        if (named(v, MARK) || v.children().isEmpty()
                || v instanceof AppliedProperty p && p.receiver() instanceof Variable) {
            return v;
        }
        return com.legend.protocol.sql.SqlToPure.call(MARK, v);
    }

    /** {@code query} with each projection's shared values computed once, and every mark removed. */
    static ValueSpecification bind(ValueSpecification query, Paths paths) {
        Shares s = new Shares(paths);
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
        if (named(result, "project") && result instanceof AppliedFunction proj
                && proj.parameters().size() == 2 && proj.parameters().get(1) instanceof ColSpecArray cols) {
            result = stageClass(proj, cols);
        } else if (named(result, "select") && result instanceof AppliedFunction sel && sel.parameters().size() == 2
                && named(sel.parameters().get(0), "extend") && sel.parameters().get(0) instanceof AppliedFunction ext
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
        if (named(n, MARK) && n instanceof AppliedFunction f) {
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

    /** The one name every column's function names its row by, each a function of the row alone; null when not. */
    private static @com.legend.base.Nullable String rowOf(ColSpecArray cols) {
        String row = null;
        for (ColSpec c : cols.colSpecs()) {
            LambdaFunction f = c.function1();
            if (f == null || c.function2() != null || f.parameters().size() != 1 || f.body().size() != 1) {
                return null;
            }
            String p = f.parameters().get(0).name();
            if (row == null) {
                row = p;
            } else if (!row.equals(p)) {
                return null;
            }
        }
        return row;
    }

    private static List<ValueSpecification> bodies(ColSpecArray cols) {
        List<ValueSpecification> roots = new ArrayList<>();
        for (ColSpec c : cols.colSpecs()) {
            roots.add(java.util.Objects.requireNonNull(c.function1()).body().get(0));
        }
        return roots;
    }

    /** The marked values to compute once: read more than once, on every row, reading only the row. */
    private static List<String> bindable(Analysis a) {
        List<String> out = new ArrayList<>();
        for (String d : a.marks) {
            ValueSpecification m = java.util.Objects.requireNonNull(a.representative.get(d));
            long occurrences = a.occurrences.getOrDefault(d, 0L);
            ValueSpecification value = ((AppliedFunction) m).parameters().get(0);
            if (value instanceof AppliedProperty p && p.receiver() instanceof Variable) {
                // (a column read as it is: a property chain a projection over instances has read into a column)
                continue;
            }
            if (occurrences >= 2 && a.always.contains(d) && a.readsOnlyRow(m)
                    && a.size(value) > WORTH / (occurrences - 1)) {
                out.add(d);
            }
        }
        return out;
    }

    /** {@code extend(src, cols)} with its shared values in extends below it, or itself when there are none. */
    private AppliedFunction stage(AppliedFunction ext, ColSpecArray cols) {
        String row = rowOf(cols);
        if (row == null) {
            return ext;
        }
        Analysis a = new Analysis(row);
        a.walk(bodies(cols));
        Map<String, String> column = new LinkedHashMap<>();
        for (String d : bindable(a)) {
            column.put(d, PREFIX + next++);
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
            // (dropping no row: an OFFSET 0, which Postgres's planner does not pull up through, so it keeps the stage
            // a subquery rather than writing each column's value back into every read of it)
            src = com.legend.protocol.sql.SqlToPure.call("drop", com.legend.protocol.sql.SqlToPure.call("extend", src,
                    new ColSpecArray(specs, null)), new CInteger(0L));
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

    /** {@code project(src, cols)} over a class's instances ({@code getAll}, filtered or not) with shared values to
     *  compute once, as a projection over rows: each property chain the columns read from the instance (each one a
     *  relation's value, {@link Paths#scalar}) projected once into a column, then the columns over those
     *  ({@code select(extend(...))}), staged as any such projection is. Itself where an instance is read whole, a
     *  chain reads more than one value or a class, or there is nothing to share. */
    private ValueSpecification stageClass(AppliedFunction proj, ColSpecArray cols) {
        ValueSpecification src = proj.parameters().get(0);
        String cls = classOf(src);
        String row = rowOf(cols);
        if (cls == null || row == null) {
            return proj;
        }
        Analysis a = new Analysis(row);
        a.walk(bodies(cols));
        if (bindable(a).isEmpty()) {
            return proj;
        }
        // each chain the columns read, by its spelling: the column holding it, the chain itself
        Map<String, String> chainColumn = new LinkedHashMap<>();
        Map<String, ValueSpecification> chainRead = new LinkedHashMap<>();
        IdentityHashMap<ValueSpecification, Boolean> seen = new IdentityHashMap<>();
        for (ValueSpecification b : bodies(cols)) {
            if (!chains(b, row, cls, chainColumn, chainRead, seen)) {
                return proj;
            }
        }
        if (chainColumn.isEmpty()) {
            // (the columns read nothing of an instance: no relation to stage over)
            return proj;
        }
        List<ColSpec> reads = new ArrayList<>();
        for (Map.Entry<String, String> e : chainColumn.entrySet()) {
            reads.add(new ColSpec(e.getValue(), new LambdaFunction(List.of(new Variable(row, null, null, null)),
                    List.of(java.util.Objects.requireNonNull(chainRead.get(e.getKey()))), null), null, null));
        }
        List<ColSpec> over = new ArrayList<>();
        List<ColSpec> selected = new ArrayList<>();
        IdentityHashMap<ValueSpecification, ValueSpecification> memo = new IdentityHashMap<>();
        for (ColSpec c : cols.colSpecs()) {
            LambdaFunction f = java.util.Objects.requireNonNull(c.function1());
            ValueSpecification body = readColumns(f.body().get(0), row, chainColumn, memo);
            over.add(new ColSpec(c.name(), new LambdaFunction(f.parameters(), List.of(body), f.pos()), null,
                    c.alias(), c.args(), c.qualified(), c.pos(), c.colType(), c.colTypeMult(), c.stereotypes(),
                    c.taggedValues()));
            selected.add(new ColSpec(c.name(), null, null, null));
        }
        ColSpecArray overCols = new ColSpecArray(over, cols.pos());
        AppliedFunction ext = (AppliedFunction) com.legend.protocol.sql.SqlToPure.call("extend",
                com.legend.protocol.sql.SqlToPure.call("project", src, new ColSpecArray(reads, null)), overCols);
        AppliedFunction staged = stage(ext, overCols);
        if (staged == ext) {
            return proj;
        }
        return com.legend.protocol.sql.SqlToPure.call("select", staged, new ColSpecArray(selected, null));
    }

    /** The class a {@code getAll}, filtered or not, reads the instances of; null for any other source. */
    private static @com.legend.base.Nullable String classOf(ValueSpecification src) {
        if (named(src, "getAll") && src instanceof AppliedFunction f && f.parameters().size() == 1
                && f.parameters().get(0) instanceof PackageableElementPtr p) {
            return p.fullPath();
        }
        if (named(src, "filter") && src instanceof AppliedFunction f && f.parameters().size() == 2) {
            return classOf(f.parameters().get(0));
        }
        return null;
    }

    /** The properties of {@code row} read in {@code n} as chains (each its column, under the next hidden name);
     *  false where the row is read otherwise: whole, or through a chain {@link #paths} does not hold one value of. */
    private boolean chains(ValueSpecification n, String row, String cls, Map<String, String> chainColumn,
            Map<String, ValueSpecification> chainRead, IdentityHashMap<ValueSpecification, Boolean> seen) {
        if (seen.put(n, Boolean.TRUE) != null) {
            return true;
        }
        if (n instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(p -> p.name().equals(row))) {
            return true;
        }
        List<String> chain = chainOf(n, row);
        if (chain != null) {
            if (!paths.scalar(cls, chain)) {
                return false;
            }
            String key = String.join(".", chain);
            if (!chainColumn.containsKey(key)) {
                chainColumn.put(key, PREFIX + next++);
                chainRead.put(key, n);
            }
            return true;
        }
        if (n instanceof Variable v && v.name().equals(row)) {
            return false;
        }
        for (ValueSpecification k : n.children()) {
            if (!chains(k, row, cls, chainColumn, chainRead, seen)) {
                return false;
            }
        }
        return true;
    }

    /** The property names of a chain of property reads from {@code row} ({@code $row.a.b}), outermost last; null for
     *  anything else. */
    private static @com.legend.base.Nullable List<String> chainOf(ValueSpecification n, String row) {
        java.util.LinkedList<String> names = new java.util.LinkedList<>();
        ValueSpecification at = n;
        while (at instanceof AppliedProperty p) {
            names.addFirst(p.property());
            at = p.receiver();
        }
        return !names.isEmpty() && at instanceof Variable v && v.name().equals(row) ? names : null;
    }

    /** {@code n} with each chain read from {@code row} its column's read. */
    private static ValueSpecification readColumns(ValueSpecification n, String row, Map<String, String> chainColumn,
            IdentityHashMap<ValueSpecification, ValueSpecification> memo) {
        ValueSpecification seen = memo.get(n);
        if (seen != null) {
            return seen;
        }
        ValueSpecification result;
        List<String> chain = chainOf(n, row);
        if (chain != null) {
            result = new AppliedProperty(new Variable(row, null, null, null), java.util.Objects.requireNonNull(
                    chainColumn.get(String.join(".", chain))), null, null);
        } else if (n instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(p -> p.name().equals(row))) {
            result = n;
        } else {
            List<ValueSpecification> kids = n.children();
            List<ValueSpecification> out = new ArrayList<>(kids.size());
            boolean changed = false;
            for (ValueSpecification k : kids) {
                ValueSpecification r = readColumns(k, row, chainColumn, memo);
                changed |= r != k;
                out.add(r);
            }
            result = changed ? n.withChildren(out) : n;
        }
        memo.put(n, result);
        return result;
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
        String col = named(n, MARK) ? column.get(a.digest(n)) : null;
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
        private final IdentityHashMap<ValueSpecification, Long> sizes = new IdentityHashMap<>();
        /** Each spelling seen (a node's own and its children's digests): its digest. */
        private final Map<String, String> spellings = new HashMap<>();
        final Set<String> marks = new java.util.LinkedHashSet<>();
        final Map<String, Long> occurrences = new HashMap<>();
        final Set<String> always = new HashSet<>();
        final Map<String, ValueSpecification> representative = new HashMap<>();

        Analysis(String row) {
            this.row = row;
        }

        void walk(List<ValueSpecification> roots) {
            List<ValueSpecification> order = new ArrayList<>();
            IdentityHashMap<ValueSpecification, Boolean> visited = new IdentityHashMap<>();
            for (ValueSpecification r : roots) {
                post(r, visited, order);
            }
            // what each node surely computes, its children's first: the marks every row computes are the columns'
            IdentityHashMap<ValueSpecification, Set<String>> sure = new IdentityHashMap<>();
            for (ValueSpecification n : order) {
                sure.put(n, sure(n, sure));
            }
            for (ValueSpecification r : roots) {
                always.addAll(java.util.Objects.requireNonNull(sure.get(r)));
            }
            java.util.Collections.reverse(order);
            IdentityHashMap<ValueSpecification, Long> paths = new IdentityHashMap<>();
            for (ValueSpecification r : roots) {
                paths.merge(r, 1L, Shares::add);
            }
            for (ValueSpecification n : order) {
                long p = paths.getOrDefault(n, 0L);
                if (p == 0) {
                    continue;
                }
                if (n instanceof LambdaFunction lf && lf.parameters().stream().anyMatch(v -> v.name().equals(row))) {
                    continue;
                }
                if (named(n, MARK)) {
                    String d = digest(n);
                    marks.add(d);
                    occurrences.merge(d, p, Shares::add);
                    representative.putIfAbsent(d, n);
                }
                for (ValueSpecification k : n.children()) {
                    paths.merge(k, p, Shares::add);
                }
            }
        }

        /** The marked values computed wherever {@code n} is (anticipated): its own, and those of what it computes
         *  unconditionally ({@link #unconditional}); an {@code if}'s those both its branches compute, or a NULL
         *  guard's those of its body; nothing for a lambda over elements (computed once per element, or never). */
        private Set<String> sure(ValueSpecification n, IdentityHashMap<ValueSpecification, Set<String>> sure) {
            if (n instanceof LambdaFunction lf && !lf.parameters().isEmpty()) {
                return Set.of();
            }
            List<ValueSpecification> kids = n.children();
            Set<String> out = Set.of();
            if (named(n, "if") && kids.size() == 3) {
                Set<String> elseSure = java.util.Objects.requireNonNull(sure.get(kids.get(2)));
                out = union(java.util.Objects.requireNonNull(sure.get(kids.get(0))), nullGuard(kids.get(0),
                        kids.get(1)) ? elseSure : intersection(java.util.Objects.requireNonNull(sure.get(kids.get(1))),
                        elseSure));
            } else {
                for (int i = 0; i < kids.size(); i++) {
                    if (unconditional(n, i, kids)) {
                        out = union(out, java.util.Objects.requireNonNull(sure.get(kids.get(i))));
                    }
                }
            }
            if (named(n, MARK)) {
                Set<String> with = new HashSet<>(out);
                with.add(digest(n));
                out = with;
            }
            return out;
        }

        /** The two sets' union, either one itself where it holds the other (the sets are only read). */
        private static Set<String> union(Set<String> a, Set<String> b) {
            if (b.isEmpty() || a.containsAll(b)) {
                return a;
            }
            if (a.isEmpty() || b.containsAll(a)) {
                return b;
            }
            Set<String> u = new HashSet<>(a);
            u.addAll(b);
            return u;
        }

        private static Set<String> intersection(Set<String> a, Set<String> b) {
            if (a.isEmpty() || b.isEmpty()) {
                return Set.of();
            }
            if (b.containsAll(a)) {
                return a;
            }
            Set<String> i = new HashSet<>(a);
            i.retainAll(b);
            return i;
        }

        /** Whether the projection computes child {@code i} of {@code n} wherever it computes {@code n}: not in an
         *  {@code if}'s branches (but a NULL guard's body), not past an {@code and}'s, an {@code or}'s or a {@code coalesce}'s first argument,
         *  not in a lambda another call takes or a lambda over elements, not in a {@code range}'s arguments. */
        private static boolean unconditional(ValueSpecification n, int i, List<ValueSpecification> kids) {
            if (n instanceof AppliedFunction) {
                if (named(n, PLANNED)) {
                    return false;
                }
                if (named(n, "if") && kids.size() == 3) {
                    return i == 0 || i == 2 && nullGuard(kids.get(0), kids.get(1));
                }
                if (named(n, "and") || named(n, "or") || named(n, "coalesce")) {
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
                if (named(c, "isEmpty") && f.parameters().size() == 1) {
                    return true;
                }
                if (named(c, "or")) {
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

        /** The nodes {@code n} is written out as (each read of a shared node again), at most Long.MAX_VALUE. */
        long size(ValueSpecification n) {
            Long known = sizes.get(n);
            if (known != null) {
                return known;
            }
            long s = 1;
            for (ValueSpecification k : n.children()) {
                s = add(s, size(k));
            }
            sizes.put(n, s);
            return s;
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
                if (named(v, MARK) && shared.contains(digest(v))) {
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

        /** Equal for equal values (their spelling, source positions aside): each node's own spelling and its
         *  children's digests, numbered as first seen (hash-consing: two values share a digest exactly when they are
         *  spelled alike, with no hash to collide); a node of a kind not spelled out here is equal only to itself, so
         *  an unknown kind can only miss a saving, never merge two different values. */
        String digest(ValueSpecification n) {
            String d = digests.get(n);
            if (d != null) {
                return d;
            }
            String own = local(n);
            if (own == null) {
                // (each node of no spelling its own: the digests so far numbered it apart)
                d = "U" + digests.size();
            } else {
                StringBuilder b = new StringBuilder(own).append('(');
                for (ValueSpecification k : n.children()) {
                    b.append(digest(k)).append(',');
                }
                d = spellings.computeIfAbsent(b.append(')').toString(), k -> "H" + spellings.size());
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

    }

    private static long add(long a, long b) {
        long s = a + b;
        return s < 0 ? Long.MAX_VALUE : s;
    }
}
