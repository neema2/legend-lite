package com.legend.integration;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.type.Type;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.GraphFetchLiteral;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A stress service's TDS query as the GraphQL document a client would write for the same business question
 * ({@code filter -> project -> [filter] -> [groupBy] -> sort -> [limit]} over to-one paths): the columns become a
 * selection, the filters a {@code where}, the sort an {@code orderBy}, the limit a {@code limit}, a {@code groupBy}
 * the class's {@code Groups} root. The answer flattens back to the service's rows by each column's path, so the
 * oracle's expectation for the service judges the GraphQL answer too. A query outside that shape is {@link
 * Unsupported}, with its reason.
 */
final class ServiceTwins {

    private ServiceTwins() {
    }

    // ---- the GraphQL side: a schema scoped to the twin's classes, the document translated ----

    /** The document translated over a schema generated for {@code scope}: the root's query lambda, as the GraphQL
     *  route runs it. A document the schema refuses throws, naming the validator's errors. */
    static LambdaFunction translate(ModelContext ctx, Set<String> scope, String document) {
        var schema = com.legend.compiler.graphql.SchemaGenerator.generate(ctx,
                com.legend.compiler.graphql.Scope.of(scope));
        var parsed = com.legend.parser.GqlParser.parseDocument(document, 1, 1);
        var v = com.legend.compiler.graphql.GqlValidator.validate(parsed, schema,
                com.legend.compiler.graphql.GqlValidator.Limits.DEFAULT, null);
        if (!v.valid()) {
            throw new IllegalStateException("the document is invalid: " + v.errors());
        }
        var t = com.legend.protocol.graphql.GqlToPure.translate(parsed, schema, null, Map.of());
        if (t.roots().size() != 1) {
            throw new IllegalStateException("one root expected, got " + t.roots().size());
        }
        return t.roots().get(0).asLambda();
    }

    /** {@code seeds} closed under superclasses and the classes their own properties reach: the smallest scope
     *  the generator accepts for them (a property's class must be in scope, a superclass with its subclass). A
     *  realistic model has same-named classes in different packages, so the whole model is no scope. */
    static Set<String> closure(ModelContext ctx, Set<String> seeds) {
        Set<String> out = new TreeSet<>();
        ArrayDeque<String> todo = new ArrayDeque<>(seeds);
        while (!todo.isEmpty()) {
            String fqn = todo.pop();
            if (!out.add(fqn)) {
                continue;
            }
            var c = ctx.findClass(fqn).orElseThrow(() -> new Unsupported("no class " + fqn));
            todo.addAll(c.superClassFqns());
            for (Property p : c.properties()) {
                if (p.type() instanceof Type.ClassType ct) {
                    todo.add(ct.fqn());
                }
            }
        }
        return out;
    }

    /** The TDS twin's scope: its root and every class its columns navigate, closed. */
    static Set<String> scope(ModelContext ctx, Twin t) {
        Set<String> seeds = new TreeSet<>();
        seeds.add(t.rootClass());
        for (Column c : t.columns()) {
            String owner = t.rootClass();
            for (Step st : c.path()) {
                Property p = ctx.findProperty(owner, st.name()).orElseThrow();
                if (p.type() instanceof Type.ClassType ct) {
                    owner = ct.fqn();
                    seeds.add(owner);
                }
            }
        }
        return closure(ctx, seeds);
    }

    // ---- a graph-fetch service: its tree as the selection ----

    /** The service's graph-fetch tree, or null for a service that fetches none. */
    static @com.legend.base.Nullable GraphFetchLiteral graphFetchTree(ValueSpecification v) {
        if (v instanceof GraphFetchLiteral g) {
            return g;
        }
        for (ValueSpecification c : v.children()) {
            GraphFetchLiteral g = graphFetchTree(c);
            if (g != null) {
                return g;
            }
        }
        return null;
    }

    /** The graph-fetch twin: the tree as a GraphQL document over the root class's list root (a subtype entry as an
     *  inline fragment), with its scope. Only {@code Class.all()->graphFetch(tree)->serialize(tree)}. */
    record GraphTwin(String document, Set<String> scope) {
    }

    static GraphTwin graphTwin(ModelContext ctx, ValueSpecification body) {
        ValueSpecification v = body instanceof LambdaFunction l && l.body().size() == 1 ? l.body().get(0) : body;
        GraphFetchLiteral tree = graphFetchTree(v);
        if (tree == null) {
            throw new Unsupported("no graph fetch");
        }
        // serialize(graphFetch(getAll(C), tree), tree): nothing else on the extent
        if (!(v instanceof AppliedFunction ser && simple(ser.function()).equals("serialize")
                && ser.parameters().get(0) instanceof AppliedFunction gf && simple(gf.function()).equals("graphFetch")
                && gf.parameters().get(0) instanceof AppliedFunction all && simple(all.function()).equals("getAll")
                && all.parameters().size() == 1)) {
            throw new Unsupported("a graph fetch over more than Class.all()");
        }
        Set<String> seeds = new TreeSet<>();
        touched(ctx, tree.className(), tree.subTrees(), seeds);
        for (GraphFetchLiteral.SubTypeNode st : tree.subTypeTrees()) {
            touched(ctx, st.subTypeClass(), st.subTrees(), seeds);
        }
        StringBuilder sb = new StringBuilder("{ ").append(simple(tree.className())).append(' ');
        selection(tree.subTrees(), tree.subTypeTrees(), sb);
        return new GraphTwin(sb.append(" }").toString(), closure(ctx, seeds));
    }

    private static void touched(ModelContext ctx, String owner, List<GraphFetchLiteral.Node> nodes, Set<String> into) {
        into.add(owner);
        for (GraphFetchLiteral.Node n : nodes) {
            if (n.subTrees().isEmpty() && n.subType() == null) {
                continue;
            }
            Property p = ctx.findProperty(owner, n.property()).orElseThrow(
                    () -> new Unsupported(owner + " has no property " + n.property()));
            String target = n.subType() != null ? n.subType() : ((Type.ClassType) p.type()).fqn();
            if (n.subType() != null) {
                into.add(((Type.ClassType) p.type()).fqn());
            }
            touched(ctx, target, n.subTrees(), into);
        }
    }

    private static void selection(List<GraphFetchLiteral.Node> nodes, List<GraphFetchLiteral.SubTypeNode> subs,
            StringBuilder sb) {
        sb.append("{ ");
        for (GraphFetchLiteral.Node n : nodes) {
            if (!n.parameters().isEmpty()) {
                throw new Unsupported("a tree node with arguments: " + n.property());
            }
            if (n.alias() != null) {
                sb.append(n.alias()).append(": ");
            }
            sb.append(n.property()).append(' ');
            if (n.subType() != null) {
                sb.append("{ ... on ").append(simple(n.subType())).append(' ');
                selection(n.subTrees(), List.of(), sb);
                sb.append(" } ");
            } else if (!n.subTrees().isEmpty()) {
                selection(n.subTrees(), n.subTypeTrees(), sb);
            }
        }
        for (GraphFetchLiteral.SubTypeNode st : subs) {
            sb.append("... on ").append(simple(st.subTypeClass())).append(' ');
            selection(st.subTrees(), List.of(), sb);
        }
        sb.append("} ");
    }

    // ---- a TDS service: its query as a selection with clauses ----

    enum Kind { TREE, GROUPS }

    /** One step of a column's path: a property and, for a qualified property, its arguments. */
    record Step(String name, List<ValueSpecification> args) {
    }

    /** What a column reads at its path's end: the value, or a probe of what is there ({@code ->count()},
     *  {@code ->isEmpty()}, {@code ->isNotEmpty()}). */
    enum Probe { VALUE, COUNT, EMPTY, NOT_EMPTY }

    /** What a path ends on: a scalar, a to-one object, a to-many collection of objects. */
    enum Leaf { SCALAR, ONE, MANY }

    /** A projected column: its name, its path from the root object, what it reads there and what the path ends
     *  on. A probe asks GraphQL for the leaf's presence ({@code { __typename }} on an object, the value of a scalar)
     *  or for a collection's {@code pAggregate { count }}, and the row's value is read from that answer. */
    record Column(String name, List<Step> path, Probe probe, Leaf leaf) {
        Column(String name, List<Step> path) {
            this(name, path, Probe.VALUE, Leaf.SCALAR);
        }

        /** Whether the leaf is aliased by the column's name (a call, or a probe). */
        boolean aliased() {
            return probe != Probe.VALUE || !path.get(path.size() - 1).args().isEmpty();
        }
    }

    /** A groupBy aggregate column: its name, the fold ({@code count}, {@code sum}, {@code max}, {@code min},
     *  {@code avg}) and the column it folds. */
    record Agg(String name, String fold, String column) {
    }

    /** The twin: the document, how its answer maps back to the service's columns, and the columns the rows are
     *  sorted by, first key first (none when unsorted). */
    record Twin(String document, Kind kind, String rootClass, List<Column> columns, List<String> keys,
            List<Agg> aggs, List<String> sortColumns) {
    }

    static final class Unsupported extends RuntimeException {
        Unsupported(String why) {
            super(why);
        }
    }

    private record Op(String fn, AppliedFunction call) {
    }

    static Twin of(ModelContext ctx, ValueSpecification body) {
        ValueSpecification v = body;
        if (v instanceof LambdaFunction l) {
            if (l.body().size() != 1) {
                throw new Unsupported("a lambda of " + l.body().size() + " statements");
            }
            v = l.body().get(0);
        }
        List<Op> ops = new ArrayList<>();
        while (v instanceof AppliedFunction f && !simple(f.function()).equals("getAll")) {
            ops.add(0, new Op(simple(f.function()), f));
            v = f.parameters().get(0);
        }
        if (!(v instanceof AppliedFunction g && !g.parameters().isEmpty()
                && g.parameters().get(0) instanceof PackageableElementPtr ptr)) {
            throw new Unsupported("the root is not Class.all()");
        }
        String root = ptr.fullPath();
        Map<String, Gv> dates = dates(ctx, root, g.parameters().subList(1, g.parameters().size()));
        List<Gv> where = new ArrayList<>();
        Map<String, Column> columns = new LinkedHashMap<>();
        List<String> keys = new ArrayList<>();
        List<Agg> aggs = new ArrayList<>();
        Map<String, Boolean> sorts = new LinkedHashMap<>();
        Integer limit = null;
        boolean projected = false;
        boolean grouped = false;
        for (Op op : ops) {
            AppliedFunction f = op.call();
            switch (op.fn()) {
                case "filter" -> {
                    if (grouped || !sorts.isEmpty() || limit != null) {
                        throw new Unsupported("a filter after a groupBy, sort or limit");
                    }
                    LambdaFunction l = lambda(f.parameters().get(1));
                    String var = l.parameters().get(0).name();
                    where.add(predicate(ctx, root, l.body().get(0), var, projected ? columns : null));
                }
                case "project" -> {
                    if (projected) {
                        throw new Unsupported("a second project");
                    }
                    projected = true;
                    for (ColSpec cs : colSpecs(f.parameters().get(1))) {
                        if (cs.function1() == null || cs.function2() != null) {
                            throw new Unsupported("a project column that is not one lambda");
                        }
                        String var = cs.function1().parameters().get(0).name();
                        columns.put(cs.name(), column(ctx, root, cs.name(), cs.function1().body().get(0), var));
                    }
                }
                case "groupBy" -> {
                    if (!projected || grouped) {
                        throw new Unsupported("a groupBy not over one project");
                    }
                    grouped = true;
                    for (ColSpec k : colSpecs(f.parameters().get(1))) {
                        Column kc = column(columns, k.name());
                        if (kc.probe() != Probe.VALUE) {
                            throw new Unsupported("a group key over a probed column");
                        }
                        keys.add(kc.name());
                    }
                    for (ColSpec a : colSpecs(f.parameters().get(2))) {
                        if (a.function1() == null || a.function2() == null) {
                            throw new Unsupported("an aggregate that is not map and fold");
                        }
                        String var = a.function1().parameters().get(0).name();
                        List<Step> p = path(a.function1().body().get(0), var);
                        if (p.size() != 1) {
                            throw new Unsupported("an aggregate over a computed column");
                        }
                        Column c = column(columns, p.get(0).name());
                        if (c.probe() != Probe.VALUE) {
                            throw new Unsupported("a group over a probed column");
                        }
                        ValueSpecification fold = a.function2().body().get(0);
                        if (!(fold instanceof AppliedFunction ff) || ff.parameters().size() != 1) {
                            throw new Unsupported("an aggregate fold that is not a call");
                        }
                        String fn = switch (simple(ff.function())) {
                            case "count" -> "count";
                            case "sum" -> "sum";
                            case "max" -> "max";
                            case "min" -> "min";
                            case "average" -> "avg";
                            default -> throw new Unsupported("the fold " + ff.function());
                        };
                        if (!fn.equals("count") && (c.path().size() != 1 || !c.path().get(0).args().isEmpty())) {
                            throw new Unsupported("a " + fn + " over a navigated column");
                        }
                        aggs.add(new Agg(a.name(), fn, c.name()));
                    }
                }
                case "sort" -> {
                    if (!sorts.isEmpty()) {
                        throw new Unsupported("a second sort");
                    }
                    ValueSpecification spec = f.parameters().get(1);
                    List<ValueSpecification> specs = spec instanceof PureCollection pc ? pc.values() : List.of(spec);
                    for (ValueSpecification one : specs) {
                        if (!(one instanceof AppliedFunction dir && dir.parameters().size() == 1
                                && dir.parameters().get(0) instanceof ColSpec cs)) {
                            throw new Unsupported("a sort that is not ~col->ascending()");
                        }
                        if (!columns.containsKey(cs.name()) && aggs.stream().noneMatch(x -> x.name().equals(cs.name()))) {
                            throw new Unsupported("a sort on an unknown column " + cs.name());
                        }
                        sorts.put(cs.name(), simple(dir.function()).equals("descending"));
                    }
                }
                case "limit", "take" -> {
                    if (!(f.parameters().get(1) instanceof CInteger n)) {
                        throw new Unsupported("a limit that is not a literal");
                    }
                    limit = n.value().intValue();
                }
                default -> throw new Unsupported("the operation " + op.fn());
            }
        }
        if (!projected) {
            throw new Unsupported("no project");
        }
        String cls = simple(root);
        Gv args = new Gv.Obj(new LinkedHashMap<>(dates));
        Map<String, Gv> a = ((Gv.Obj) args).fields();
        if (!where.isEmpty()) {
            a.put("where", where.size() == 1 ? where.get(0) : new Gv.Obj(Map.of("AND", new Gv.Lst(where))));
        }
        StringBuilder doc = new StringBuilder("{ ");
        if (grouped) {
            List<Gv> orders = new ArrayList<>();
            for (Map.Entry<String, Boolean> sort : sorts.entrySet()) {
                String sortColumn = sort.getKey();
                Gv dirv = new Gv.Enum(sort.getValue() ? "DESC" : "ASC");
                Agg sortAgg = null;
                for (Agg ag : aggs) {
                    if (ag.name().equals(sortColumn)) {
                        sortAgg = ag;
                    }
                }
                if (sortAgg != null) {
                    orders.add(sortAgg.fold().equals("count") ? obj("count", dirv)
                            : obj(sortAgg.fold(), nest(columns.get(sortAgg.column()).path(), dirv)));
                } else {
                    if (!keys.contains(sortColumn)) {
                        throw new Unsupported("a groups sort on a column that is no key");
                    }
                    orders.add(obj("key", nest(columns.get(sortColumn).path(), dirv)));
                }
            }
            if (!orders.isEmpty()) {
                a.put("orderBy", new Gv.Lst(orders));
            }
            if (limit != null) {
                a.put("limit", new Gv.Num(String.valueOf(limit)));
            }
            doc.append(cls).append("Groups").append(arguments(args)).append(" { key ");
            Tree keyTree = new Tree();
            for (String k : keys) {
                keyTree.add(columns.get(k));
            }
            keyTree.render(doc);
            boolean counted = false;
            Map<String, Tree> folds = new LinkedHashMap<>();
            for (Agg ag : aggs) {
                if (ag.fold().equals("count")) {
                    if (!counted) {
                        doc.append(" count");
                        counted = true;
                    }
                } else {
                    folds.computeIfAbsent(ag.fold(), x -> new Tree()).add(columns.get(ag.column()));
                }
            }
            folds.forEach((fn, t) -> {
                doc.append(' ').append(fn).append(' ');
                t.render(doc);
            });
            doc.append(" } }");
        } else {
            List<Gv> orders = new ArrayList<>();
            for (Map.Entry<String, Boolean> sort : sorts.entrySet()) {
                Column sc = columns.get(sort.getKey());
                if (sc.probe() != Probe.VALUE) {
                    throw new Unsupported("a sort on a probed column");
                }
                for (Step st : sc.path()) {
                    if (!st.args().isEmpty()) {
                        throw new Unsupported("a sort on a qualified property");
                    }
                }
                orders.add(nest(sc.path(), new Gv.Enum(sort.getValue() ? "DESC" : "ASC")));
            }
            if (!orders.isEmpty()) {
                a.put("orderBy", new Gv.Lst(orders));
            }
            if (limit != null) {
                a.put("limit", new Gv.Num(String.valueOf(limit)));
            }
            doc.append(cls).append(arguments(args)).append(' ');
            Tree t = new Tree();
            for (Column c : columns.values()) {
                t.add(c);
            }
            t.render(doc);
            doc.append(" }");
        }
        return new Twin(doc.toString(), grouped ? Kind.GROUPS : Kind.TREE, root, List.copyOf(columns.values()), keys,
                aggs, List.copyOf(sorts.keySet()));
    }

    /** A dated extent's dates ({@code C.all(%pd, %bd)}, {@code C.all(%d)}) as the root's arguments, by the class's
     *  dimensions in Pure's order (the processing date first); none for a class that is not temporal. */
    private static Map<String, Gv> dates(ModelContext ctx, String root, List<ValueSpecification> args) {
        var strategy = com.legend.compiler.element.Temporal.strategyOf(ctx, root);
        Map<String, Gv> out = new LinkedHashMap<>();
        if (strategy == null) {
            if (!args.isEmpty()) {
                throw new Unsupported("dates on a class that is not temporal");
            }
            return out;
        }
        List<String> names = new ArrayList<>();
        if (strategy.has(com.legend.compiler.element.MilestoningStrategy.Dimension.PROCESSING)) {
            names.add("processingDate");
        }
        if (strategy.has(com.legend.compiler.element.MilestoningStrategy.Dimension.BUSINESS)) {
            names.add("businessDate");
        }
        if (args.size() != names.size()) {
            throw new Unsupported("a temporal extent read with " + args.size() + " dates");
        }
        for (int i = 0; i < args.size(); i++) {
            if (!(args.get(i) instanceof CDate)) {
                throw new Unsupported("a date that is not a literal");
            }
            out.put(names.get(i), literal(args.get(i)));
        }
        return out;
    }

    /** A project column: a path, or a probe of one ({@code ->count()}, {@code ->isEmpty()}, {@code ->isNotEmpty()}). */
    private static Column column(ModelContext ctx, String root, String name, ValueSpecification v, String var) {
        Probe probe = Probe.VALUE;
        if (v instanceof AppliedFunction f && !f.propertyCall() && f.parameters().size() == 1) {
            probe = switch (simple(f.function())) {
                case "count" -> Probe.COUNT;
                case "isEmpty" -> Probe.EMPTY;
                case "isNotEmpty" -> Probe.NOT_EMPTY;
                default -> throw new Unsupported("a column that is not a property path: " + simple(f.function()));
            };
            v = f.parameters().get(0);
        }
        List<Step> path = path(v, var);
        Leaf leaf = checkPath(ctx, root, path, probe != Probe.VALUE);
        if (probe != Probe.VALUE && !path.get(path.size() - 1).args().isEmpty()) {
            throw new Unsupported("a probe of a qualified property");
        }
        return new Column(name, path, probe, leaf);
    }

    // ---- the answer, flattened back to the service's rows ----

    private static final java.util.regex.Pattern ZONED_DATE_TIME =
            java.util.regex.Pattern.compile("\\d{4}-\\d{2}-\\d{2}T.*\\+0000");

    /** The oracle's rows as the GRAPH channel spells them: a DateTime without the {@code +0000} the service-test
     *  (TDS) channel appends -- the engine's execute-to-JSON spelling, which lite's graph answers (and so GraphQL's)
     *  carry ({@code Fold}'s graph DateTime leaf, 2026-09-16); every other value as it is. */
    static Object graphChannel(Object expected) {
        if (expected instanceof String s && ZONED_DATE_TIME.matcher(s).matches()) {
            return s.substring(0, s.length() - "+0000".length());
        }
        if (expected instanceof List<?> l) {
            List<Object> out = new ArrayList<>();
            for (Object o : l) {
                out.add(graphChannel(o));
            }
            return out;
        }
        if (expected instanceof Map<?, ?> m) {
            Map<Object, Object> out = new LinkedHashMap<>();
            m.forEach((k, v) -> out.put(k, graphChannel(v)));
            return out;
        }
        return expected;
    }

    /** The GraphQL answer's rows as the service's: one map per object (or group), keyed by the service's columns. */
    static List<Map<String, Object>> rows(Twin t, Object answer) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (!(answer instanceof List<?> items)) {
            throw new IllegalStateException("the answer is not a list: " + answer);
        }
        for (Object item : items) {
            Map<?, ?> o = (Map<?, ?>) item;
            Map<String, Object> row = new LinkedHashMap<>();
            if (t.kind() == Kind.TREE) {
                for (Column c : t.columns()) {
                    row.put(c.name(), probed(c, walk(o, c)));
                }
            } else {
                Map<?, ?> key = (Map<?, ?>) o.get("key");
                for (String k : t.keys()) {
                    row.put(k, walk(key, columnOf(t, k)));
                }
                for (Agg ag : t.aggs()) {
                    if (ag.fold().equals("count")) {
                        row.put(ag.name(), o.get("count"));
                    } else {
                        row.put(ag.name(), walk((Map<?, ?>) o.get(ag.fold()), columnOf(t, ag.column())));
                    }
                }
            }
            out.add(row);
        }
        return out;
    }

    /** A groups root's rows (columns named by their dotted paths) arranged into objects, as the route arranges them. */
    @SuppressWarnings("unchecked")
    static List<Object> arranged(List<?> rows) {
        List<Object> out = new ArrayList<>();
        for (Object r : rows) {
            Map<String, Object> o = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : ((Map<?, ?>) r).entrySet()) {
                String[] path = ((String) e.getKey()).split(java.util.regex.Pattern.quote(
                        com.legend.protocol.graphql.GqlToPure.PATH_SEPARATOR));
                Map<String, Object> at = o;
                for (int i = 0; i < path.length - 1; i++) {
                    at = (Map<String, Object>) at.computeIfAbsent(path[i], x -> new LinkedHashMap<String, Object>());
                }
                at.put(path[path.length - 1], e.getValue());
            }
            out.add(o);
        }
        return out;
    }

    private static Column columnOf(Twin t, String name) {
        for (Column c : t.columns()) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        throw new IllegalStateException("no column " + name);
    }

    private static @com.legend.base.Nullable Object walk(@com.legend.base.Nullable Map<?, ?> o, Column c) {
        Object at = o;
        for (int i = 0; i < c.path().size(); i++) {
            if (at == null) {
                return null;
            }
            Step s = c.path().get(i);
            String key = i == c.path().size() - 1 && c.aliased() ? c.name() : s.name();
            at = ((Map<?, ?>) at).get(key);
        }
        return at;
    }

    /** A probed column's value from what GraphQL answered at its leaf: a collection's {@code count}, an object's or
     *  a scalar's presence. An absent object on the way is an empty collection and an absent leaf. */
    private static @com.legend.base.Nullable Object probed(Column c, @com.legend.base.Nullable Object at) {
        if (c.probe() == Probe.VALUE) {
            return at;
        }
        long n;
        if (c.leaf() == Leaf.MANY) {
            n = at == null ? 0 : ((Number) ((Map<?, ?>) at).get("count")).longValue();
        } else {
            n = at == null ? 0 : 1;
        }
        return switch (c.probe()) {
            case COUNT -> n;
            case EMPTY -> n == 0;
            case NOT_EMPTY -> n > 0;
            case VALUE -> throw new IllegalStateException("a value is no probe");
        };
    }

    // ---- reading the query ----

    private static Column column(Map<String, Column> columns, String name) {
        Column c = columns.get(name);
        if (c == null) {
            throw new Unsupported("no projected column " + name);
        }
        return c;
    }

    private static List<ColSpec> colSpecs(ValueSpecification v) {
        if (v instanceof ColSpec cs) {
            return List.of(cs);
        }
        if (v instanceof ColSpecArray arr) {
            return arr.colSpecs();
        }
        throw new Unsupported("column specs of " + v.getClass().getSimpleName());
    }

    private static LambdaFunction lambda(ValueSpecification v) {
        if (v instanceof LambdaFunction l && l.parameters().size() == 1 && l.body().size() == 1) {
            return l;
        }
        throw new Unsupported("a filter that is not a one-parameter lambda");
    }

    /** {@code $x.a.b.c(args)} as its steps. */
    static List<Step> path(ValueSpecification v, String var) {
        if (v instanceof Variable x && x.name().equals(var)) {
            return new ArrayList<>();
        }
        if (v instanceof AppliedProperty p) {
            List<Step> out = path(p.receiver(), var);
            out.add(new Step(p.property(), List.of()));
            return out;
        }
        if (v instanceof AppliedFunction f && f.propertyCall() && !f.parameters().isEmpty()) {
            List<Step> out = path(f.parameters().get(0), var);
            for (ValueSpecification arg : f.parameters().subList(1, f.parameters().size())) {
                literal(arg);
            }
            out.add(new Step(f.function(), f.parameters().subList(1, f.parameters().size())));
            return out;
        }
        throw new Unsupported("a column that is not a property path: " + v.getClass().getSimpleName()
                + (v instanceof AppliedFunction f ? " " + f.function() : ""));
    }

    /** Every step to-one, every intermediate a class, no temporal class on the way, arguments on the leaf only; a
     *  PROBED path may end on an object or a collection of objects. What the path ends on. */
    private static Leaf checkPath(ModelContext ctx, String root, List<Step> path, boolean probed) {
        String owner = root;
        for (int i = 0; i < path.size(); i++) {
            Step s = path.get(i);
            String o = owner;
            Property p = ctx.findProperty(owner, s.name()).orElseThrow(
                    () -> new Unsupported(o + " has no property " + s.name()));
            boolean last = i == path.size() - 1;
            if (last && probed && p.type() instanceof Type.ClassType ct) {
                if (com.legend.compiler.element.Temporal.strategyOf(ctx, ct.fqn()) != null) {
                    throw new Unsupported("a probe of a temporal navigation");
                }
                return p.multiplicity().isMany() ? Leaf.MANY : Leaf.ONE;
            }
            if (p.multiplicity().isMany()) {
                throw new Unsupported("a to-many step " + s.name());
            }
            if (p.type() instanceof Type.ClassType ct) {
                if (last) {
                    throw new Unsupported("a column of class type " + s.name());
                }
                if (!s.args().isEmpty()) {
                    throw new Unsupported("a qualified navigation " + s.name());
                }
                if (com.legend.compiler.element.Temporal.strategyOf(ctx, ct.fqn()) != null) {
                    throw new Unsupported("a temporal navigation " + s.name());
                }
                owner = ct.fqn();
            } else if (!last) {
                throw new Unsupported("a step past a scalar " + s.name());
            }
        }
        return Leaf.SCALAR;
    }

    private static final Map<String, String> COMPARISONS = Map.ofEntries(
            Map.entry("equal", "equals"), Map.entry("==", "equals"),
            Map.entry("greaterThan", "gt"), Map.entry(">", "gt"),
            Map.entry("greaterThanEqual", "gte"), Map.entry(">=", "gte"),
            Map.entry("lessThan", "lt"), Map.entry("<", "lt"),
            Map.entry("lessThanEqual", "lte"), Map.entry("<=", "lte"));

    private static final Map<String, String> FLIPPED = Map.of("gt", "lt", "lt", "gt", "gte", "lte", "lte", "gte",
            "equals", "equals");

    /** A predicate over the row as a {@code where} value; {@code columns} non-null when it reads projected columns. */
    private static Gv predicate(ModelContext ctx, String root, ValueSpecification v, String var,
            @com.legend.base.Nullable Map<String, Column> columns) {
        if (v instanceof AppliedProperty) {
            // a Boolean property as the predicate: its value is true
            return nest(target(ctx, root, v, var, columns), obj("equals", new Gv.Bool(true)));
        }
        if (!(v instanceof AppliedFunction f)) {
            throw new Unsupported("a predicate that is not a call: " + v.getClass().getSimpleName());
        }
        String fn = simple(f.function());
        List<ValueSpecification> ps = operands(f);
        switch (fn) {
            case "and", "&&", "or", "||" -> {
                List<Gv> parts = new ArrayList<>();
                for (ValueSpecification p : ps) {
                    parts.add(predicate(ctx, root, p, var, columns));
                }
                return obj(fn.equals("and") || fn.equals("&&") ? "AND" : "OR", new Gv.Lst(parts));
            }
            case "not", "!" -> {
                // the engine's SQL NOT of a comparison drops an absent value; GraphQL's NOT keeps it (two-valued,
                // register S112): the twin asks for the value present as well
                List<Step> leaf = comparedPath(ctx, root, ps.get(0), var, columns);
                Gv negated = obj("NOT", predicate(ctx, root, ps.get(0), var, columns));
                return leaf == null ? negated : present(leaf, negated);
            }
            case "isEmpty", "isNotEmpty" -> {
                List<Step> path = target(ctx, root, ps.get(0), var, columns);
                Gv isNull = obj("equals", Gv.NULL);
                return nest(path, fn.equals("isEmpty") ? isNull : obj("not", isNull));
            }
            case "startsWith", "endsWith", "contains" -> {
                List<Step> path = target(ctx, root, ps.get(0), var, columns);
                return nest(path, obj(fn, literal(ps.get(1))));
            }
            case "notEqual", "!=" -> {
                // SQL's <> (the engine's): false of an absent value; GraphQL's not keeps it (register S112)
                boolean pathLeft = !isLiteral(ps.get(0));
                List<Step> path = target(ctx, root, pathLeft ? ps.get(0) : ps.get(1), var, columns);
                return present(path, nest(path, obj("not", obj("equals", literal(pathLeft ? ps.get(1) : ps.get(0))))));
            }
            case "in" -> {
                List<Step> path = target(ctx, root, ps.get(0), var, columns);
                if (!(ps.get(1) instanceof PureCollection pc)) {
                    throw new Unsupported("in over a non-literal list");
                }
                List<Gv> vs = new ArrayList<>();
                for (ValueSpecification x : pc.values()) {
                    vs.add(literal(x));
                }
                return nest(path, obj("in", new Gv.Lst(vs)));
            }
            default -> {
                String op = COMPARISONS.get(fn);
                if (op == null || ps.size() != 2) {
                    throw new Unsupported("the predicate " + fn);
                }
                boolean pathLeft = !isLiteral(ps.get(0));
                ValueSpecification lhs = pathLeft ? ps.get(0) : ps.get(1);
                ValueSpecification rhs = pathLeft ? ps.get(1) : ps.get(0);
                List<Step> path = target(ctx, root, lhs, var, columns);
                return nest(path, obj(pathLeft ? op : FLIPPED.get(op), literal(rhs)));
            }
        }
    }

    /** {@code cond} with the path's value present: {@code AND: [{p: {not: {equals: null}}}, cond]}. */
    private static Gv present(List<Step> path, Gv cond) {
        return obj("AND", new Gv.Lst(List.of(nest(path, obj("not", obj("equals", Gv.NULL))), cond)));
    }

    /** The path a single comparison reads ({@code $x.p > 1}, {@code $x.p == 'a'}), or null for any other predicate. */
    private static @com.legend.base.Nullable List<Step> comparedPath(ModelContext ctx, String root,
            ValueSpecification v, String var, @com.legend.base.Nullable Map<String, Column> columns) {
        if (v instanceof AppliedProperty) {
            return target(ctx, root, v, var, columns);
        }
        if (!(v instanceof AppliedFunction f)) {
            return null;
        }
        String fn = simple(f.function());
        List<ValueSpecification> ps = operands(f);
        if ((COMPARISONS.containsKey(fn) || fn.equals("startsWith") || fn.equals("endsWith")
                || fn.equals("contains")) && ps.size() == 2) {
            return target(ctx, root, isLiteral(ps.get(0)) ? ps.get(1) : ps.get(0), var, columns);
        }
        return null;
    }

    private static List<ValueSpecification> operands(AppliedFunction f) {
        if (f.infix() && f.parameters().size() == 1 && f.parameters().get(0) instanceof PureCollection pc) {
            return pc.values();
        }
        return f.parameters();
    }

    /** The path a predicate's operand reads: a property path over the root, or a projected column's. */
    private static List<Step> target(ModelContext ctx, String root, ValueSpecification v, String var,
            @com.legend.base.Nullable Map<String, Column> columns) {
        List<Step> p = path(v, var);
        if (columns != null) {
            if (p.size() != 1) {
                throw new Unsupported("a post-project predicate that navigates");
            }
            Column c = column(columns, p.get(0).name());
            if (c.probe() != Probe.VALUE) {
                throw new Unsupported("a predicate on a probed column");
            }
            p = c.path();
        } else {
            checkPath(ctx, root, p, false);
        }
        for (Step s : p) {
            if (!s.args().isEmpty()) {
                throw new Unsupported("a predicate on a qualified property");
            }
        }
        return p;
    }

    private static boolean isLiteral(ValueSpecification v) {
        return v instanceof CString || v instanceof CInteger || v instanceof CFloat || v instanceof CDecimal
                || v instanceof CBoolean || v instanceof EnumValue || v instanceof CDate;
    }

    static Gv literal(ValueSpecification v) {
        return switch (v) {
            case CString s -> new Gv.Str(s.value());
            case CInteger i -> new Gv.Num(i.value().toString());
            case CFloat d -> new Gv.Num(d.exact() != null ? d.exact().toPlainString() : Double.toString(d.value()));
            case CDecimal d -> new Gv.Num(d.value().toPlainString());
            case CBoolean b -> new Gv.Bool(b.value());
            case EnumValue e -> new Gv.Enum(e.value());
            case CDate d -> new Gv.Str(d.written() != null ? d.written().replaceFirst("^%", "") : d.value().toString());
            default -> throw new Unsupported("a literal " + v.getClass().getSimpleName());
        };
    }

    // ---- GraphQL values ----

    sealed interface Gv {
        Gv NULL = new Null();

        record Obj(Map<String, Gv> fields) implements Gv {
        }

        record Lst(List<Gv> items) implements Gv {
        }

        record Str(String value) implements Gv {
        }

        record Num(String text) implements Gv {
        }

        record Bool(boolean value) implements Gv {
        }

        record Enum(String name) implements Gv {
        }

        record Null() implements Gv {
        }
    }

    static Gv obj(String k, Gv v) {
        Map<String, Gv> m = new LinkedHashMap<>();
        m.put(k, v);
        return new Gv.Obj(m);
    }

    /** {@code leaf} under the path's steps: {@code {a: {b: leaf}}}. */
    static Gv nest(List<Step> path, Gv leaf) {
        Gv at = leaf;
        for (int i = path.size() - 1; i >= 0; i--) {
            at = obj(path.get(i).name(), at);
        }
        return at;
    }

    static String arguments(Gv args) {
        Map<String, Gv> m = ((Gv.Obj) args).fields();
        if (m.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder("(");
        boolean first = true;
        for (Map.Entry<String, Gv> e : m.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(e.getKey()).append(": ");
            render(e.getValue(), sb);
        }
        return sb.append(')').toString();
    }

    static void render(Gv v, StringBuilder sb) {
        switch (v) {
            case Gv.Obj o -> {
                sb.append("{ ");
                boolean first = true;
                for (Map.Entry<String, Gv> e : o.fields().entrySet()) {
                    if (!first) {
                        sb.append(", ");
                    }
                    first = false;
                    sb.append(e.getKey()).append(": ");
                    render(e.getValue(), sb);
                }
                sb.append(" }");
            }
            case Gv.Lst l -> {
                sb.append('[');
                for (int i = 0; i < l.items().size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    render(l.items().get(i), sb);
                }
                sb.append(']');
            }
            case Gv.Str s -> sb.append(com.legend.json.Json.toCompact(s.value()));
            case Gv.Num n -> sb.append(n.text());
            case Gv.Bool b -> sb.append(b.value());
            case Gv.Enum e -> sb.append(e.name());
            case Gv.Null x -> sb.append("null");
        }
    }

    /** A selection tree merged from the columns' paths; a qualified leaf is aliased by its column. */
    private static final class Tree {
        private final Map<String, Object> children = new LinkedHashMap<>();

        @SuppressWarnings("unchecked")
        void add(Column c) {
            Map<String, Object> at = children;
            for (int i = 0; i < c.path().size(); i++) {
                Step s = c.path().get(i);
                boolean leaf = i == c.path().size() - 1;
                if (leaf) {
                    if (c.probe() != Probe.VALUE) {
                        // a collection's count, an object's presence, a scalar's value: aliased by the column
                        at.put(switch (c.leaf()) {
                            case MANY -> c.name() + ": " + s.name() + "Aggregate { count }";
                            case ONE -> c.name() + ": " + s.name() + " { __typename }";
                            case SCALAR -> c.name() + ": " + s.name();
                        }, null);
                    } else if (s.args().isEmpty()) {
                        at.putIfAbsent(s.name(), null);
                    } else {
                        at.put(c.name() + ": " + s.name() + qualified(s), null);
                    }
                } else {
                    at = (Map<String, Object>) at.computeIfAbsent(s.name(), x -> new LinkedHashMap<String, Object>());
                }
            }
        }

        void render(StringBuilder sb) {
            render(children, sb);
        }

        @SuppressWarnings("unchecked")
        private static void render(Map<String, Object> m, StringBuilder sb) {
            sb.append("{ ");
            for (Map.Entry<String, Object> e : m.entrySet()) {
                sb.append(e.getKey()).append(' ');
                if (e.getValue() != null) {
                    render((Map<String, Object>) e.getValue(), sb);
                    sb.append(' ');
                }
            }
            sb.append('}');
        }
    }

    /** A qualified property's arguments, named by the property's parameters: filled in by {@link #named}. */
    private static String qualified(Step s) {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < s.args().size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("$").append(i).append(": ");
            render(literal(s.args().get(i)), sb);
        }
        return sb.append(')').toString();
    }

    /** The document with each qualified property's positional {@code $i} names replaced by its parameters'. */
    static String named(ModelContext ctx, Twin t) {
        String doc = t.document();
        for (Column c : t.columns()) {
            Step leaf = c.path().get(c.path().size() - 1);
            if (leaf.args().isEmpty()) {
                continue;
            }
            String owner = t.rootClass();
            for (Step s : c.path().subList(0, c.path().size() - 1)) {
                owner = ((Type.ClassType) ctx.findProperty(owner, s.name()).orElseThrow().type()).fqn();
            }
            Property p = ctx.findProperty(owner, leaf.name()).orElseThrow();
            if (!(p instanceof Property.Derived d)) {
                throw new Unsupported("arguments on a stored property " + leaf.name());
            }
            String marker = c.name() + ": " + leaf.name() + "(";
            int at = doc.indexOf(marker);
            if (at < 0) {
                continue;
            }
            int end = doc.indexOf(')', at);
            String call = doc.substring(at, end);
            for (int i = 0; i < leaf.args().size(); i++) {
                call = call.replace("$" + i + ":", d.parameters().get(i).name() + ":");
            }
            doc = doc.substring(0, at) + call + doc.substring(end);
        }
        return doc;
    }

    private static String simple(String fn) {
        int at = fn.lastIndexOf("::");
        return at < 0 ? fn : fn.substring(at + 2);
    }
}
