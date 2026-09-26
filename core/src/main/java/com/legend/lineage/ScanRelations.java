// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lineage;

import com.legend.compiler.element.ModelContext;
import com.legend.error.NotImplementedException;
import com.legend.model.AssociationMapping;
import com.legend.model.ClassMapping;
import com.legend.model.DatabaseDefinition;
import com.legend.model.JoinChainElement;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.PropertyMapping;
import com.legend.model.RelationalOperation;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The engine's STATIC {@code scanRelations} tree (#44, feature map §14.1:
 * "static form off the mapping" — no pipeline dependency): the tables a
 * query's property demand touches, arranged by JOIN traversal with the
 * MAPPING's join names, each node listing the SORTED columns that table
 * contributes (leaf reads plus its side of every join condition):
 * <pre>
 *   root
 *     ------&gt; (t) Person [ID, name]
 *       ------&gt; (t) Bicycle(PersonBicycle) [b_PersonID]
 * </pre>
 * Union-mapped properties branch per property-mapping entry (the
 * {@code prop[setId]} routing rides {@code Join.targetSetId});
 * {@code subType(@X)} keeps only branches whose target set maps X.
 * Anything outside the built vocabulary is a LOUD wall, never a silently
 * wrong tree.
 */
public final class ScanRelations {

    private ScanRelations() {
    }

    /** One path segment: a property hop or a subType restriction. */
    private sealed interface Seg {
        record Prop(String name) implements Seg { }

        record SubType(String classFqn) implements Seg { }
    }

    private static final class Node {
        final String db;          // defining database (view detection)
        final @com.legend.base.Nullable String schema;   // qualifying schema, null = unqualified
        final @com.legend.base.Nullable String table;
        final @com.legend.base.Nullable String joinName; // null on the root table node
        final Set<String> cols = new TreeSet<>();
        final TreeMap<String, Node> children = new TreeMap<>();
        // SYNTHETIC edge condition (tableToTDS ->join lambdas — no named
        // store join exists); null on model-join edges
        @com.legend.base.Nullable RelationalOperation cond;
        // UNION-navigation label: non-null wins over joinName at print —
        // "" prints NO label (per-member forks), a merged-key label
        // prints verbatim (the unionAlias grammar)
        @com.legend.base.Nullable String labelOverride;
        // a BARE tableToTDS side (no project): the whole table is the
        // demand — string retention must not narrow it
        boolean keepAll;

        Node(String db, @com.legend.base.Nullable String table, @com.legend.base.Nullable String joinName) {
            this(db, null, table, joinName);
        }

        Node(String db, @com.legend.base.Nullable String schema, @com.legend.base.Nullable String table,
                @com.legend.base.Nullable String joinName) {
            this.db = db;
            this.schema = schema;
            this.table = table;
            this.joinName = joinName;
        }
    }

    public static String treeString(ModelContext ctx, LambdaFunction query,
            String mappingFqn) {
        return treeString(ctx, query, mappingFqn, false, true);
    }

    /** {@code runtimeVariant}: the RUNTIME (4-arg) scan — an undemanded
     * root prints its EXTENT (`(t) Person []`), union-mapped navigation
     * forks per MEMBER SET (union keys + member PK demand), and
     * milestoned tables read their window columns; the STATIC variant
     * scans per pair route and prints nothing for a constant-only
     * projection (testConstant pins both). */
    /** {@code showLabels}: relationTreeAsString's boolean — {@code
     * (false)} HIDES the edge labels; no-arg and {@code (true)} show
     * them. The label CONTENT is the variant's: the STATIC tree carries
     * JOIN NAMES, the RUNTIME tree the CONDITION MANGLE (testUnion pins
     * static+no-arg=names and runtime+(false)=hidden; WithDiffKeys pins
     * runtime+(true)=mangle). */
    public static String treeString(ModelContext ctx, LambdaFunction query,
            String mappingFqn, boolean runtimeVariant, boolean showLabels) {
        StringBuilder sb = new StringBuilder();
        for (Line l : lines(ctx, query, mappingFqn, runtimeVariant)) {
            sb.append("  ".repeat(l.depth()));
            if (l.kind().equals("root")) {
                sb.append("root\n");
                continue;
            }
            sb.append("------> (").append(l.kind()).append(") ").append(l.name());
            if (showLabels && l.label() != null) {
                sb.append('(').append(l.label()).append(')');
            }
            sb.append(" [").append(String.join(", ", l.cols())).append("]\n");
        }
        return sb.toString();
    }

    /** One printed line of the tree, as DATA (LineageRows carries these as
     * rows; the database prints them): {@code kind} is {@code root},
     * {@code t} or {@code v}; {@code label} the join label the labelled
     * print spells (null = none); {@code cols} sorted, deduplicated. */
    public record Line(int depth, String kind, @com.legend.base.Nullable String name,
            @com.legend.base.Nullable String label, List<String> cols) {
    }

    /** The tree in PREORDER, one line per node. */
    public static List<Line> lines(ModelContext ctx, LambdaFunction query,
            String mappingFqn, boolean runtimeVariant) {
        List<Line> out = new ArrayList<>();
        out.add(new Line(0, "root", null, null, List.of()));
        for (Node r : scanRoots(ctx, query, mappingFqn, runtimeVariant)) {
            flatten(out, r, 1, ctx, r.table, runtimeVariant);
        }
        return out;
    }

    /** BRANCH-AWARE roots: concatenate splits (engine: each branch scans
     * independently, in order — demand never bleeds across branches; a
     * mixed tds/class concatenate scans each branch by its own kind). */
    private static List<Node> scanRoots(ModelContext ctx,
            LambdaFunction query, String mappingFqn, boolean extentRoots) {
        ValueSpecification body = query.body().isEmpty() ? query
                : query.body().get(query.body().size() - 1);
        List<ValueSpecification> branches = new ArrayList<>();
        splitConcatenate(body, branches);
        List<Node> out = new ArrayList<>();
        for (ValueSpecification b : branches) {
            LambdaFunction bl = new LambdaFunction(List.of(), List.of(b));
            List<Node> tds = tableToTdsRoots(ctx, bl, mappingFqn);
            out.addAll(tds.isEmpty()
                    ? buildRoots(ctx, bl, mappingFqn, false, extentRoots)
                    : tds);
        }
        if (branches.size() > 1) {
            // engine root order across concatenate branches: by TABLE name,
            // stable for ties (both concat goldens pin exactly this)
            out.sort(java.util.Comparator.comparing(nd -> nd.table));
        }
        return out;
    }

    /** Flatten {@code concatenate(a, b)} spines in document order. */
    private static void splitConcatenate(ValueSpecification v,
            List<ValueSpecification> out) {
        if (v instanceof AppliedFunction af
                && "concatenate".equals(af.function()
                        .substring(af.function().lastIndexOf(':') + 1))
                && af.parameters().size() == 2) {
            splitConcatenate(af.parameters().get(0), out);
            splitConcatenate(af.parameters().get(1), out);
            return;
        }
        // DISTRIBUTE a join over a concatenate on its LEFT operand — the
        // AST mirror of the engine's one-base-tree-per-UnionAll-query rule,
        // each branch getting a copy of the join child (ledger cluster 53).
        // join ONLY: the engine splits only where the union alias is a
        // join operand, never generically over any wrapper.
        if (v instanceof AppliedFunction jf
                && "join".equals(jf.function()
                        .substring(jf.function().lastIndexOf(':') + 1))
                && jf.parameters().size() >= 3) {
            List<ValueSpecification> lefts = new ArrayList<>();
            splitConcatenate(jf.parameters().get(0), lefts);
            if (lefts.size() > 1) {
                for (ValueSpecification l : lefts) {
                    List<ValueSpecification> ps =
                            new ArrayList<>(jf.parameters());
                    ps.set(0, l);
                    splitConcatenate(jf.withParameters(ps), out);
                }
                return;
            }
        }
        out.add(v);
    }

    /**
     * The scanned relation tree as DATA (the #46 test-data-generation
     * consumer): the same walk {@link #treeString} renders, one node per
     * reached table with the join edge that reached it and the scanned
     * columns. Views are NOT expanded here (treeString expands them at
     * print time); the consumer sees the view node itself.
     */
    public record Rel(String db, @com.legend.base.Nullable String table, @com.legend.base.Nullable String joinName,
            @com.legend.base.Nullable RelationalOperation cond, List<String> cols,
            List<Rel> children) {

        /** Model-join edge (no synthetic condition). */
        public Rel(String db, @com.legend.base.Nullable String table, @com.legend.base.Nullable String joinName,
                List<String> cols, List<Rel> children) {
            this(db, table, joinName, null, cols, children);
        }
    }

    public static List<Rel> relTree(ModelContext ctx, LambdaFunction query,
            String mappingFqn) {
        // tableToTDS(tableReference(db,s,t)) roots: the relation IS the
        // table, every column in play (engine scans TDS queries through
        // the plan; the direct-table shape needs no mapping walk). Gated
        // to the DATA consumer — treeString's lineage goldens keep their
        // current vocabulary.
        List<Node> tdsRoots = tableToTdsRoots(ctx, query, mappingFqn);
        List<Rel> out = new ArrayList<>();
        if (!tdsRoots.isEmpty()) {
            for (Node r : tdsRoots) {
                out.add(toRel(r));
            }
            return out;
        }
        List<Node> roots = buildRoots(ctx, query, mappingFqn, true);
        if (roots.isEmpty()) {
            // constant-only projection: no scanned path, but the root
            // class's table is still the tree (engine testConstant)
            LegacyMappingDefinition md = mapping(ctx, mappingFqn);
            for (ClassMapping.Relational cm : rootClassMappings(ctx, md,
                    rootClassFqn(query))) {
                Node r = new Node(java.util.Objects.requireNonNull(
                        mainDbOf(cm), "root set without a main db"),
                        mainTableOf(cm), null);
                foldClassFilter(ctx, r, cm);
                roots.add(r);
            }
        }
        for (Node r : roots) {
            out.add(toRel(r));
        }
        return out;
    }

    /** Every {@code tableToTDS(tableReference(db, 'schema', 'table'))}
     * root in the query, document order; join steps are a named wall.
     * Column demand: TDS ops name columns as STRINGS (getString('COL'),
     * groupBy(['COL'])) — literals matching a table column restrict its
     * fetch set (PK rides via the consumer); no matching literal = the
     * bare-tableToTDS whole-table shape. */
    private static List<Node> tableToTdsRoots(ModelContext ctx,
            ValueSpecification n, String mappingFqn) {
        List<Node> out = new ArrayList<>();
        if (containsCall(n, "join")) {
            // ->join(tableToTDS(...), TYPE, {a,b|...}) chains: each right
            // side becomes a CHILD of the node owning its left-side
            // column, with the lambda as a SYNTHETIC edge condition
            // (aliases resolve through the project cols)
            ValueSpecification top = n instanceof LambdaFunction lf
                    && !lf.body().isEmpty()
                    ? lf.body().get(lf.body().size() - 1) : n;
            // peel post-join wrapper ops (olapGroupBy, sort, ...) down to
            // the join spine
            while (top instanceof AppliedFunction w
                    && !w.function()
                            .substring(w.function().lastIndexOf(':') + 1)
                            .equals("join")
                    && !w.parameters().isEmpty()
                    && containsCall(w.parameters().get(0), "join")) {
                top = w.parameters().get(0);
            }
            // a concatenate under the join distributes first (ledger
            // clusters 53/54: one base tree per UnionAll query) — each
            // branch parses with its OWN alias/byTable maps, engine root
            // order across branches is by table name
            List<ValueSpecification> spines = new ArrayList<>();
            splitConcatenate(top, spines);
            for (ValueSpecification spine : spines) {
                Map<String, String[]> aliases = new LinkedHashMap<>();
                Map<String, Node> byTable = new LinkedHashMap<>();
                parseTdsJoinChain(ctx, spine, out, aliases, byTable, mappingFqn);
            }
            if (spines.size() > 1) {
                out.sort(java.util.Comparator.comparing(nd -> nd.table));
            }
            // chain nodes narrowed per-source in parseTdsSource (the
            // global string pool cross-matches other sources' aliases)
            return out;
        }
        collectTableToTds(ctx, n, out);
        if (out.isEmpty()) {
            return out;
        }
        // RESULT-PRESERVING ops (filter/take) keep the whole-table column
        // set — the engine narrows demand only when the query SHAPES its
        // result (project/restrict/groupBy/distinct/olap). A sort ANYWHERE
        // keeps the whole table too (engine: sort materializes the full
        // row stream — testTableToTdsWithSort pins all columns despite a
        // later project).
        if (out.size() > 1) {
            // engine reOrderAndMergeRelationTree sorts EVERY top-level
            // child (join-less key = relation name) — the join arms above
            // already sort; a no-join concatenate's roots must too
            // (testTableToTdsWithConcatenate pins firmTable < personTable)
            out.sort(java.util.Comparator.comparing(nd -> nd.table));
        }
        if (containsCall(n, "sort")
                || (!containsCall(n, "project") && !containsCall(n, "restrict")
                        && !containsCall(n, "groupBy")
                        && !containsCall(n, "olapGroupBy")
                        && !containsCall(n, "distinct"))) {
            return out;
        }
        Set<String> strings = new LinkedHashSet<>();
        collectStrings(n, strings);
        for (Node node : walkAll(out)) {
            Set<String> demanded = new TreeSet<>();
            for (String s : strings) {
                for (String c : node.cols) {
                    if (c.equalsIgnoreCase(s)) {
                        demanded.add(c);
                    }
                }
            }
            if (!demanded.isEmpty()) {
                node.cols.retainAll(demanded);
            }
        }
        return out;
    }

    private static List<Node> walkAll(List<Node> roots) {
        List<Node> all = new ArrayList<>();
        java.util.ArrayDeque<Node> work = new java.util.ArrayDeque<>(roots);
        while (!work.isEmpty()) {
            Node x = work.poll();
            all.add(x);
            work.addAll(x.children.values());
        }
        return all;
    }

    /** Recursive descent over {@code join(left, right, TYPE, lambda)}
     * spines; the base tds source becomes the root. Alias map entries are
     * {@code alias -> [table, physicalColumn]}. */
    private static void parseTdsJoinChain(ModelContext ctx,
            ValueSpecification v, List<Node> roots,
            Map<String, String[]> aliases, Map<String, Node> byTable,
            String mappingFqn) {
        // a WRAPPER op over the join spine (extend/restrict/sort/…
        // between joins — testTdsJoinConcatenateAndJoin's extend) reads
        // no table of its own: the spine continues underneath (batch 84)
        while (v instanceof AppliedFunction w
                && !w.function().substring(w.function().lastIndexOf(':') + 1)
                        .equals("join")
                && !w.function().substring(w.function().lastIndexOf(':') + 1)
                        .equals("project")
                && !w.parameters().isEmpty()
                && containsCall(w.parameters().get(0), "join")) {
            v = w.parameters().get(0);
        }
        if (v instanceof AppliedFunction af && af.function()
                .substring(af.function().lastIndexOf(':') + 1)
                .equals("join") && af.parameters().size() >= 3) {
            parseTdsJoinChain(ctx, af.parameters().get(0), roots, aliases,
                    byTable, mappingFqn);
            TdsSrc right = parseTdsSource(ctx, af.parameters().get(1),
                    aliases, byTable, mappingFqn);
            // the NAMED form join(left, right, TYPE, 'L', 'R') (tds.pure)
            // — the engine's TDS join by column names (batch 84)
            if (af.parameters().size() == 5
                    && af.parameters().get(3) instanceof com.legend.protocol.spec.CString ln
                    && af.parameters().get(4) instanceof com.legend.protocol.spec.CString rn) {
                attachTdsJoinNamed(ln.value(), rn.value(), right, aliases, byTable);
                return;
            }
            if (!(lastParam(af) instanceof LambdaFunction cl)
                    || cl.parameters().size() != 2 || cl.body().isEmpty()) {
                throw new NotImplementedException("scanRelations:"
                        + " tableToTDS join without a 2-param condition"
                        + " lambda");
            }
            attachTdsJoin(cl, right, aliases, byTable, roots);
            return;
        }
        TdsSrc base = parseTdsSource(ctx, v, aliases, byTable, mappingFqn);
        roots.add(base.node());
    }

    /** The named TDS join {@code join(l, r, TYPE, 'L', 'R')}: the owner
     * of the left alias gains the right node as a child. Siblings print
     * in the engine's decorated-alias order — the OUTERMOST join first
     * (its alias breadcrumb is the shortest), so the key descends with
     * the attach index (testTdsJoinConcatenateAndJoin's arms). */
    private static void attachTdsJoinNamed(String lAlias, String rAlias,
            TdsSrc rightSrc, Map<String, String[]> aliases,
            Map<String, Node> byTable) {
        String[] l = aliases.get(lAlias);
        if (l == null) {
            throw new NotImplementedException("scanRelations: tds join alias '"
                    + lAlias + "' is not a projected column");
        }
        String[] own = rightSrc.own().get(rAlias);
        String r = own != null ? own[1] : rAlias;
        Node right = rightSrc.node();
        Node parent = java.util.Objects.requireNonNull(byTable.get(l[0]),
                "tds join condition references unseeded table " + l[0]);
        parent.cols.add(l[1]);
        right.cols.add(r);
        right.cond = new RelationalOperation.Comparison(
                new RelationalOperation.ColumnRef(null, l[0], l[1]),
                com.legend.model.ComparisonOp.EQ,
                new RelationalOperation.TargetColumnRef(r));
        right.labelOverride = "equal_\"joinleft_\"\"" + lAlias
                + "\"_\"joinright_\"\"" + rAlias + "\"";
        parent.children.put(String.format("%03d", 999 - parent.children.size())
                + right.table + "(tds_join)", right);
    }

    /** A parsed tds source with ITS OWN alias map (alias ->
     * [table, physicalColumn]). */
    private record TdsSrc(Node node, Map<String, String[]> own) {
    }

    private static ValueSpecification lastParam(AppliedFunction af) {
        return af.parameters().get(af.parameters().size() - 1);
    }

    /** {@code $a.getX('L') == $b.getX('R')}: resolve L through the
     * accumulated alias map to (ownerTable, physCol); the owner's node
     * gains the child. The RIGHT side rides as {@code {target}} so the
     * renderer aliases it even on self-joins. */
    private static void attachTdsJoin(LambdaFunction cl, TdsSrc rightSrc,
            Map<String, String[]> aliases, Map<String, Node> byTable,
            List<Node> roots) {
        Node right = rightSrc.node();
        ValueSpecification body = cl.body().get(cl.body().size() - 1);
        String leftVar = cl.parameters().get(0).name();
        // CROSS JOIN ({a, b | true}): no key columns, no synthetic edge
        // condition — the right table hangs under the LEFT TDS's root (the
        // root this chain's left arm parsed, the last one added) with the
        // bare tdsJoin label; siblings key as the named form does
        // (scanRelations golden testTableToTdsWithCrossJoin:
        // `firmTable(tdsJoin) [CEOID, ID]`, the projected columns only)
        if (body instanceof com.legend.protocol.spec.CBoolean cb && cb.value()) {
            Node parent = roots.get(roots.size() - 1);
            right.labelOverride = "tdsJoin";
            parent.children.put(String.format("%03d", 999 - parent.children.size())
                    + right.table + "(tds_join)", right);
            return;
        }
        if (!(body instanceof AppliedFunction eq
                && eq.function().substring(eq.function().lastIndexOf(':') + 1)
                        .equals("equal")
                && eq.parameters().size() == 2)) {
            throw new NotImplementedException("scanRelations: tableToTDS"
                    + " join condition beyond a single equality pending");
        }
        String[] l = null;
        String r = null;
        String lAlias = null;
        String rAlias = null;
        for (ValueSpecification side : eq.parameters()) {
            String[] read = tdsColRead(side);
            if (read == null) {
                throw new NotImplementedException("scanRelations:"
                        + " tableToTDS join side is not a column read");
            }
            if (read[0].equals(leftVar)) {
                l = aliases.get(read[1]);
                lAlias = read[1];
                if (l == null) {
                    throw new NotImplementedException("scanRelations:"
                            + " tableToTDS join alias '" + read[1]
                            + "' is not a projected column");
                }
            } else {
                // the right side reads ITS OWN projected alias
                String[] own = rightSrc.own().get(read[1]);
                rAlias = read[1];
                r = own != null ? own[1] : read[1];
            }
        }
        if (l == null || r == null) {
            throw new NotImplementedException("scanRelations: tableToTDS"
                    + " join condition must compare left and right sides");
        }
        Node parent = java.util.Objects.requireNonNull(byTable.get(l[0]),
                "tds join condition references unseeded table " + l[0]);
        parent.cols.add(l[1]);
        right.cols.add(r);
        right.cond = new RelationalOperation.Comparison(
                new RelationalOperation.ColumnRef(null, l[0], l[1]),
                com.legend.model.ComparisonOp.EQ,
                new RelationalOperation.TargetColumnRef(r));
        // the engine's tds-join label: the SYNTHETIC side aliases quoted
        // (joinleft_/joinright_ + breadcrumb) with the PROJECTED tds
        // column names quoted (testTableToTdsWithJoin's golden)
        right.labelOverride = "equal_\"joinleft_\"\"" + lAlias
                + "\"_\"joinright_\"\"" + rAlias + "\"";
        parent.children.put(right.table + "(tds_join_"
                + parent.children.size() + ")", right);
    }

    /** {@code $v.getX('COL')} -> [varName, COL]. */
    private static String @com.legend.base.Nullable [] tdsColRead(ValueSpecification v) {
        if (v instanceof AppliedFunction af
                && af.function().substring(af.function().lastIndexOf(':') + 1)
                        .startsWith("get")
                && af.parameters().size() == 2
                && af.parameters().get(0)
                        instanceof com.legend.protocol.spec.Variable var
                && af.parameters().get(1)
                        instanceof com.legend.protocol.spec.CString c) {
            return new String[]{var.name(), c.value()};
        }
        return null;
    }

    /** A tds source: {@code tableToTDS(tableReference(...))} optionally
     * under {@code ->project([col(r|$r.getX('PHYS'), 'alias'), ...])};
     * merges its alias map (or the identity map) into {@code aliases}. */
    private static TdsSrc parseTdsSource(ModelContext ctx,
            ValueSpecification v, Map<String, String[]> aliases,
            Map<String, Node> byTable, String mappingFqn) {
        List<AppliedFunction> projects = new ArrayList<>();
        ValueSpecification cur = v;
        while (cur instanceof AppliedFunction af && af.function()
                .substring(af.function().lastIndexOf(':') + 1)
                .equals("project")) {
            projects.add(af);
            cur = af.parameters().get(0);
        }
        List<Node> found = new ArrayList<>();
        collectTableToTds(ctx, cur, found);
        if (found.isEmpty() && rootClassFqn(cur) != null) {
            return classProjectionSource(ctx, cur, projects, aliases, byTable,
                    mappingFqn);
        }
        if (found.size() != 1) {
            throw new NotImplementedException("scanRelations: tableToTDS"
                    + " join side is not a single table source");
        }
        Node node = found.get(0);
        // LEFT-owner registry: a same-table RIGHT side must not shadow
        // the accumulated left (the self-join test's parent lookup)
        byTable.putIfAbsent(node.table, node);
        Map<String, String[]> own = new LinkedHashMap<>();
        if (projects.isEmpty()) {
            node.keepAll = true;
            for (String c : node.cols) {
                own.put(c, new String[]{node.table, c});
            }
        } else {
            for (AppliedFunction p : projects) {
                for (ValueSpecification e
                        : flatValues(p.parameters().get(1))) {
                    if (e instanceof AppliedFunction colFn
                            && colFn.function().substring(
                                    colFn.function().lastIndexOf(':') + 1)
                                    .equals("col")
                            && colFn.parameters().size() >= 2
                            && colFn.parameters().get(1)
                                    instanceof com.legend.protocol.spec.CString a
                            && colFn.parameters().get(0)
                                    instanceof LambdaFunction cl
                            && !cl.body().isEmpty()) {
                        String[] read = tdsColRead(
                                cl.body().get(cl.body().size() - 1));
                        if (read != null) {
                            own.put(a.value(),
                                    new String[]{node.table, read[1]});
                        }
                    }
                }
            }
        }
        if (!node.keepAll) {
            // demand = this source's OWN projected physical columns
            Set<String> phys = new TreeSet<>();
            for (String[] t : own.values()) {
                if (t[0].equals(node.table)) {
                    phys.add(t[1]);
                }
            }
            node.cols.retainAll(phys);
        }
        // accumulated view: LEFT-most binding wins duplicate names
        own.forEach(aliases::putIfAbsent);
        return new TdsSrc(node, own);
    }

    /** A TDS-join side rooted at a CLASS extent (batch 84:
     * {@code X.all()[->filter(…)]->project([col(p|$p.prop, 'alias')…])}
     * or the (lambdas, names) form): the class's root table node under
     * the mapping (rootClassMappings), the projected and filtered
     * properties' COLUMN mappings as its scanned columns, and the alias
     * map alias → [table, column] for the joins above. A projected
     * property that is not a plain column mapping is loud. */
    private static TdsSrc classProjectionSource(ModelContext ctx,
            ValueSpecification extent, List<AppliedFunction> projects,
            Map<String, String[]> aliases, Map<String, Node> byTable,
            String mappingFqn) {
        String classFqn = java.util.Objects.requireNonNull(rootClassFqn(extent));
        LegacyMappingDefinition md = mapping(ctx, mappingFqn);
        ClassMapping.Relational cm = rootClassMappings(ctx, md, classFqn).get(0);
        Node node = new Node(java.util.Objects.requireNonNull(
                mainDbOf(cm), "root set without a main db"),
                mainTableOf(cm), null);
        byTable.putIfAbsent(node.table, node);
        Map<String, String[]> own = new LinkedHashMap<>();
        // filter reads under the extent demand their columns
        for (String prop : propertyReads(extent)) {
            node.cols.add(classColumn(ctx, md, cm, prop));
        }
        for (AppliedFunction pr : projects) {
            List<ValueSpecification> specs = pr.parameters().size() > 1
                    ? flatValues(pr.parameters().get(1)) : List.of();
            List<ValueSpecification> names = pr.parameters().size() > 2
                    ? flatValues(pr.parameters().get(2)) : List.of();
            for (int i = 0; i < specs.size(); i++) {
                ValueSpecification sp = specs.get(i);
                LambdaFunction fn;
                String alias;
                if (sp instanceof AppliedFunction col
                        && com.legend.builtin.TdsLegacy.COL.matches(col)
                        && col.parameters().size() >= 2
                        && col.parameters().get(0) instanceof LambdaFunction cf
                        && col.parameters().get(1) instanceof com.legend.protocol.spec.CString cn) {
                    fn = cf;
                    alias = cn.value();
                } else if (sp instanceof LambdaFunction lf && i < names.size()
                        && names.get(i) instanceof com.legend.protocol.spec.CString nn) {
                    fn = lf;
                    alias = nn.value();
                } else {
                    throw new NotImplementedException("scanRelations: class"
                            + " projection column is not col(fn, 'name') or"
                            + " (lambda, name)");
                }
                for (String prop : propertyReads(fn)) {
                    String column = classColumn(ctx, md, cm, prop);
                    node.cols.add(column);
                    own.put(alias, new String[]{node.table, column});
                }
            }
        }
        aliases.putAll(own);
        return new TdsSrc(node, own);
    }

    /** The single-hop property reads {@code $x.prop} under {@code n}. */
    private static List<String> propertyReads(ValueSpecification n) {
        List<String> out = new ArrayList<>();
        if (n instanceof AppliedProperty ap
                && ap.receiver() instanceof com.legend.protocol.spec.Variable) {
            out.add(ap.property());
        } else if (n instanceof AppliedFunction af) {
            for (ValueSpecification p : af.parameters()) {
                out.addAll(propertyReads(p));
            }
        } else if (n instanceof LambdaFunction lf) {
            for (ValueSpecification b : lf.body()) {
                out.addAll(propertyReads(b));
            }
        } else if (n instanceof com.legend.protocol.spec.PureCollection pc) {
            for (ValueSpecification e : pc.values()) {
                out.addAll(propertyReads(e));
            }
        } else if (n instanceof AppliedProperty ap2) {
            out.addAll(propertyReads(ap2.receiver()));
        }
        return out;
    }

    private static String classColumn(ModelContext ctx, LegacyMappingDefinition md,
            ClassMapping.Relational cm, String prop) {
        for (PropertyMapping pm : pmsFor(ctx, md, cm, prop)) {
            if (pm instanceof PropertyMapping.Column c) {
                return c.column();
            }
        }
        throw new NotImplementedException("scanRelations: property '" + prop
                + "' of " + cm.className() + " is not a plain column mapping");
    }

    private static List<ValueSpecification> flatValues(ValueSpecification v) {
        if (v instanceof com.legend.protocol.spec.PureCollection pc) {
            List<ValueSpecification> out = new ArrayList<>();
            for (ValueSpecification e : pc.values()) {
                out.addAll(flatValues(e));
            }
            return out;
        }
        return List.of(v);
    }

    private static void collectStrings(ValueSpecification n,
            Set<String> out) {
        if (n instanceof com.legend.protocol.spec.CString cs) {
            out.add(cs.value());
        } else if (n instanceof AppliedFunction af) {
            for (ValueSpecification p : af.parameters()) {
                collectStrings(p, out);
            }
        } else if (n instanceof AppliedProperty ap) {
            collectStrings(ap.receiver(), out);
        } else if (n instanceof LambdaFunction lf) {
            for (ValueSpecification b : lf.body()) {
                collectStrings(b, out);
            }
        } else if (n instanceof com.legend.protocol.spec.PureCollection pc) {
            for (ValueSpecification e : pc.values()) {
                collectStrings(e, out);
            }
        }
    }

    private static void collectTableToTds(ModelContext ctx,
            ValueSpecification n, List<Node> out) {
        if (n instanceof AppliedFunction af) {
            if (com.legend.platform.CoreFn.of(af.function()).orElse(null)
                            == com.legend.platform.CoreFn.TABLE_TO_TDS
                    && !af.parameters().isEmpty()
                    && af.parameters().get(0) instanceof AppliedFunction tr
                    && com.legend.platform.CoreFn.of(tr.function()).orElse(null)
                            == com.legend.platform.CoreFn.TABLE_REFERENCE
                    && tr.parameters().size() >= 3
                    && tr.parameters().get(0)
                            instanceof PackageableElementPtr db
                    && tr.parameters().get(2)
                            instanceof com.legend.protocol.spec.CString t) {
                Node node = new Node(db.fullPath(), t.value(), null);
                var td = ctx.findTableDefinition(db.fullPath(), t.value());
                if (td.isPresent()) {
                    for (var c : td.get().columns()) {
                        node.cols.add(c.name());
                    }
                }
                out.add(node);
                return;
            }
            for (ValueSpecification p : af.parameters()) {
                collectTableToTds(ctx, p, out);
            }
        } else if (n instanceof AppliedProperty ap) {
            collectTableToTds(ctx, ap.receiver(), out);
        } else if (n instanceof LambdaFunction lf) {
            for (ValueSpecification b : lf.body()) {
                collectTableToTds(ctx, b, out);
            }
        } else if (n instanceof com.legend.protocol.spec.PureCollection pc) {
            for (ValueSpecification e : pc.values()) {
                collectTableToTds(ctx, e, out);
            }
        }
    }

    private static boolean containsCall(ValueSpecification n, String name) {
        if (n instanceof AppliedFunction af) {
            if (name.equals(af.function()
                    .substring(af.function().lastIndexOf(':') + 1))) {
                return true;
            }
            for (ValueSpecification p : af.parameters()) {
                if (containsCall(p, name)) {
                    return true;
                }
            }
        } else if (n instanceof AppliedProperty ap) {
            return containsCall(ap.receiver(), name);
        } else if (n instanceof LambdaFunction lf) {
            for (ValueSpecification b : lf.body()) {
                if (containsCall(b, name)) {
                    return true;
                }
            }
        } else if (n instanceof com.legend.protocol.spec.PureCollection pc) {
            for (ValueSpecification e : pc.values()) {
                if (containsCall(e, name)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** The engine's set-implementation identity for a class under a
     * mapping (#47 plan printer): {@code [definingMappingName, setId,
     * dbFqn, mainTable]} — the DEFINING mapping is the include-child
     * declaring the set; the default setId is the class FQN with
     * underscores. Loud when the class maps ambiguously. */
    public static String[] rootImpl(ModelContext ctx, String mappingFqn,
            String classFqn) {
        return rootImpl(ctx, mappingFqn, classFqn, java.util.List.of());
    }

    /** {@code chainMappings}: ModelChainConnection mappings — the M2M2R
     * surface; a ~src class unmapped in {@code mappingFqn} chases its
     * relational set through them (engine chained-mapping resolution). */
    public static String[] rootImpl(ModelContext ctx, String mappingFqn,
            String classFqn, java.util.List<String> chainMappings) {
        String[] r = rootImplOrNull(ctx, mappingFqn, classFqn, 0,
                chainMappings);
        if (r == null) {
            throw new NotImplementedException("plan: no class mapping for '"
                    + classFqn + "' under '" + mappingFqn + "'"
                    + (chainMappings.isEmpty() ? ""
                            : " or chain " + chainMappings));
        }
        return r;
    }

    private static String @com.legend.base.Nullable [] rootImplOrNull(
            ModelContext ctx, String mappingFqn,
            String classFqn, int depth,
            java.util.List<String> chainMappings) {
        LegacyMappingDefinition md = mapping(ctx, mappingFqn);
        for (LegacyMappingDefinition m : withIncludes(ctx, md)) {
            for (ClassMapping.Relational r : allClassMappings(m)) {
                if (typeMatches(r.className(), classFqn)) {
                    String name = m.qualifiedName().substring(
                            m.qualifiedName().lastIndexOf(':') + 1);
                    String setId = com.legend.model.SetId.of(r);
                    return new String[]{name, setId, mainDbOf(r),
                            mainTableOf(r)};
                }
            }
        }
        // an M2M (Pure) set: the physical identity follows ~src to the
        // UPSTREAM class's relational set (the H5 collapse — the plan's
        // db/table identity is the composed source's; max 4 hops, the
        // corpus chains twice); the upstream class may live in THIS
        // mapping or a CHAIN mapping (ModelChainConnection)
        if (depth < 4) {
            for (LegacyMappingDefinition m : withIncludes(ctx, md)) {
                for (com.legend.model.ClassMapping cm : m.classMappings()) {
                    if (cm instanceof ClassMapping.Pure pm
                            && typeMatches(pm.className(), classFqn)
                            // no ~src = no upstream to chase; the chain
                            // simply ends here rather than being unknown
                            && pm.sourceClass() != null) {
                        String[] up = rootImplOrNull(ctx, mappingFqn,
                                pm.sourceClass(), depth + 1, chainMappings);
                        for (int i = 0; up == null
                                && i < chainMappings.size(); i++) {
                            up = rootImplOrNull(ctx, chainMappings.get(i),
                                    pm.sourceClass(), depth + 1,
                                    java.util.List.of());
                        }
                        if (up == null) {
                            return null;
                        }
                        // 5th element marks the M2M chase: the plan's TDS
                        // tuple types spell PURE defaults, not the
                        // physical columns (the M2M layer erases them —
                        // m2m2rShowcase golden name VARCHAR(8192))
                        return new String[]{up[0], up[1], up[2], up[3],
                                "m2m"};
                    }
                }
            }
        }
        return null;
    }

    /** Whether {@code name} is a VIEW of {@code db} (include closure). */
    public static boolean isView(ModelContext ctx, String db,
            @com.legend.base.Nullable String name) {
        return db != null && name != null && ctx.findView(db, name).isPresent();
    }

    /** The view definition, or a loud wall — the tdg view-fetch builder's
     * accessor (the private lookup keeps its null-tolerant contract). */
    public static DatabaseDefinition.ViewDefinition viewDef(ModelContext ctx,
            String db, String name) {
        DatabaseDefinition.ViewDefinition vd = ctx.findView(db, name).orElse(null);
        if (vd == null) {
            throw new NotImplementedException("scanRelations: view '" + name
                    + "' not found in '" + db + "'");
        }
        return vd;
    }

    /** The view's INTERNAL relation tree as a {@link Rel} (the tdg view
     * fetch): root = the view's seed table carrying every plain
     * column-mapped base column, with the view's own join web (column
     * expressions + ~filter) as children — the engine's nestedViewTree.
     * Nested views (a view whose seed is itself a view) stay a named
     * wall. */
    public static Rel viewTree(ModelContext ctx, String db,
            String viewName) {
        return viewExpansion(ctx, db, viewName).tree();
    }

    /** A view's tdg expansion: the internal tree plus the seed (main)
     * table identity, the PLAIN column-mapped view-column &rarr;
     * base-column map (join-navigated columns are absent), and the
     * view-layer CHAIN outer-first (view-on-view stacks — the engine
     * emits one view fetch PER LAYER, inner-first). */
    public record ViewExpansion(Rel tree, String db,
            @com.legend.base.Nullable String mainTable,
            java.util.Map<String, String> colToBase,
            java.util.List<String> viewChain) {
    }

    public static ViewExpansion viewExpansion(ModelContext ctx, String db,
            @com.legend.base.Nullable String viewName) {
        DatabaseDefinition.ViewDefinition vd = viewName == null ? null
                : ctx.findView(db, viewName).orElse(null);
        if (vd == null) {
            throw new NotImplementedException("scanRelations: view '"
                    + viewName + "' not found in '" + db + "'");
        }
        Node root = java.util.Objects.requireNonNull(
                expandView(ctx, db, vd),
                "view '" + viewName + "' has no column-mapped seed table");
        java.util.Map<String, String> m = new java.util.LinkedHashMap<>();
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping cm
                : vd.columnMappings()) {
            if (cm.expression()
                    instanceof RelationalOperation.JoinNavigation) {
                continue;
            }
            List<RelationalOperation.ColumnRef> refs = new ArrayList<>();
            columnRefs(cm.expression(), refs);
            if (refs.size() == 1) {
                m.put(cm.name(), refs.get(0).column());
            }
        }
        if (isView(ctx, root.db, root.table)) {
            // NESTED VIEW: the seed is itself a view — compose through
            // the inner expansion (plain-column composition; a nested
            // view with its own join webs stays a named wall)
            if (!root.children.isEmpty()) {
                throw new NotImplementedException("scanRelations: nested"
                        + " view '" + root.table + "' with join webs"
                        + " under '" + viewName + "' pending");
            }
            ViewExpansion inner = viewExpansion(ctx, root.db, root.table);
            java.util.Map<String, String> composed =
                    new java.util.LinkedHashMap<>();
            for (java.util.Map.Entry<String, String> e : m.entrySet()) {
                String base = inner.colToBase().get(e.getValue());
                if (base == null) {
                    throw new NotImplementedException("scanRelations:"
                            + " nested view column '" + e.getValue()
                            + "' of '" + root.table + "' is not plain"
                            + " column-mapped");
                }
                composed.put(e.getKey(), base);
            }
            java.util.LinkedHashSet<String> cols =
                    new java.util.LinkedHashSet<>(inner.tree().cols());
            root.cols.forEach(c -> cols.add(
                    inner.colToBase().getOrDefault(c, c)));
            Rel merged = new Rel(inner.tree().db(), inner.tree().table(),
                    inner.tree().joinName(), inner.tree().cond(),
                    List.copyOf(cols), inner.tree().children());
            List<String> chain = new ArrayList<>();
            chain.add(java.util.Objects.requireNonNull(viewName, "viewName"));
            chain.addAll(inner.viewChain());
            return new ViewExpansion(merged, inner.db(),
                    inner.mainTable(), composed, List.copyOf(chain));
        }
        return new ViewExpansion(toRel(root), root.db, root.table, m,
                List.of(java.util.Objects.requireNonNull(viewName, "viewName")));
    }

    private static Rel toRel(Node n) {
        List<Rel> kids = new ArrayList<>();
        for (Node c : n.children.values()) {
            kids.add(toRel(c));
        }
        return new Rel(n.db, n.table, n.joinName, n.cond,
                List.copyOf(n.cols), kids);
    }

    private static List<Node> buildRoots(ModelContext ctx,
            LambdaFunction query, String mappingFqn, boolean tdgMode) {
        return buildRoots(ctx, query, mappingFqn, tdgMode, true);
    }

    /** {@code tdgMode}: the #46 data consumer — union-mapped JOIN targets
     * branch a child PER TARGET SET (the engine's testDataGeneration
     * relation tree fetches every set, its own goldens carry the
     * duplicate SQLs); treeString's lineage vocabulary keeps the
     * single-target shape. {@code extentRoots}: an undemanded root still
     * prints its extent — the RUNTIME scan and tdg; the STATIC scan
     * prints nothing for a constant-only projection (testConstant pins
     * both variants). */
    private static List<Node> buildRoots(ModelContext ctx,
            LambdaFunction query, String mappingFqn, boolean tdgMode,
            boolean extentRoots) {
        LegacyMappingDefinition md = mapping(ctx, mappingFqn);
        String rootClass = rootClassFqn(query);
        // a UNION-mapped root walks ONCE PER SET, one root table node
        // each, in mapping declaration order (testUnion golden)
        List<ClassMapping.Relational> rootCms =
                rootClassMappings(ctx, md, rootClass);
        List<List<Seg>> paths = new ArrayList<>();
        collectChains(query, paths);
        if (System.getenv("LL_LINEAGE_DEBUG") != null) {
            System.err.println("[scanRelations] paths=" + paths);
        }
        List<Node> roots = new ArrayList<>();
        if (paths.isEmpty() && !extentRoots) {
            return roots;
        }
        for (ClassMapping.Relational cm : rootCms) {
            Node r = new Node(java.util.Objects.requireNonNull(
                    mainDbOf(cm), "root set without a main db"),
                    mainTableOf(cm), null);
            roots.add(r);
            foldClassFilter(ctx, r, cm);
            // a UNION-mapped ROOT's arms project their PK for instance
            // identity under the runtime scan (same rule as union
            // navigation targets — testUnionToUnionMultiple)
            if (extentRoots && rootCms.size() > 1) {
                r.cols.addAll(pkCols(ctx, r.db, r.table));
            }
        }
        for (List<Seg> p : paths) {
            for (int i = 0; i < rootCms.size(); i++) {
                walk(ctx, md, rootCms.get(i), roots.get(i), p, 0, tdgMode,
                        extentRoots && !tdgMode, paths.indexOf(p));
            }
        }
        return roots;
    }

    private static List<ClassMapping.Relational> rootClassMappings(
            ModelContext ctx, LegacyMappingDefinition md, @com.legend.base.Nullable String classFqn) {
        List<ClassMapping.Relational> hits = new ArrayList<>();
        for (LegacyMappingDefinition m : withIncludes(ctx, md)) {
            for (ClassMapping.Relational r : allClassMappings(m)) {
                if (typeMatches(r.className(), classFqn)) {
                    hits.add(r);
                }
            }
        }
        if (hits.isEmpty()) {
            throw new NotImplementedException("scanRelations: no class"
                    + " mapping for '" + classFqn + "'");
        }
        // union sets print in SET-ID order (testUnion FirmSet1<FirmSet2 AND
        // testUnionViewOnView neg<nonNeg — declaration order fits only the
        // first golden)
        hits.sort(java.util.Comparator.comparing(
                r -> r.setId() == null ? "" : r.setId()));
        return hits;
    }

    private static void flatten(List<Line> out, Node n, int depth,
            ModelContext ctx, @com.legend.base.Nullable String rootTable,
            boolean runtimeVariant) {
        DatabaseDefinition.ViewDefinition vd = n.db == null || n.table == null ? null
                : ctx.findView(n.db, n.schema, n.table).orElse(null);
        String label = null;
        if (n.labelOverride != null) {
            label = n.labelOverride.isEmpty() ? null : n.labelOverride;
        } else if (n.joinName != null) {
            label = runtimeVariant ? joinLabel(ctx, n, rootTable) : n.joinName;
        }
        Set<String> cols = n.cols;
        if (runtimeVariant) {
            // the RUNTIME scan reads a MILESTONED table's window columns
            // (pureToSqlQuery's temporal predicates demand them —
            // testTableTreeMilestoning pins from_z/thru_z/in_z/out_z)
            Set<String> ms = milestoningCols(ctx, n);
            if (!ms.isEmpty()) {
                cols = new TreeSet<>(cols);
                cols.addAll(ms);
            }
        }
        out.add(new Line(depth, vd != null ? "v" : "t", n.table, label,
                List.copyOf(cols)));
        for (Node c : n.children.values()) {
            // the 'root' spelling binds the ROOT ROW — only the root
            // node's direct children label against it; deeper hops spell
            // the source table by NAME (Inheritance_2's nested Person:
            // equal_PersonID_Bicycleb_PersonID, not rootID)
            flatten(out, c, depth + 1, ctx,
                    n.joinName == null && n.labelOverride == null
                            ? rootTable : null,
                    runtimeVariant);
        }
        if (vd != null) {
            // a VIEW EXPANDS: a nested 'root' subtree of its underlying
            // tables — every column mapping expression plus the view
            // filter's join web (the engine's view internals)
            out.add(new Line(depth + 1, "root", null, null, List.of()));
            Node inner = expandView(ctx, n.db, vd);
            if (inner != null) {
                flatten(out, inner, depth + 2, ctx, inner.table, runtimeVariant);
            }
        }
    }

    /** A milestoned table's window columns (business from/thru,
     * processing in/out, snapshot) — empty for plain tables. */
    private static Set<String> milestoningCols(ModelContext ctx, Node n) {
        if (n.db == null || n.table == null) {
            return Set.of();
        }
        DatabaseDefinition.TableDefinition td =
                ctx.findTableDefinition(n.db, n.table).orElse(null);
        if (td != null && td.milestoning() != null) {
            Set<String> out = new TreeSet<>();
            var ms = td.milestoning();
            if (ms.business() != null) {
                var b = ms.business();
                if (b.snapshotDate() != null) {
                    out.add(b.snapshotDate());
                } else {
                    out.add(java.util.Objects.requireNonNull(b.from()));
                    out.add(java.util.Objects.requireNonNull(b.thru()));
                }
            }
            if (ms.processing() != null) {
                var p = ms.processing();
                if (p.snapshotDate() != null) {
                    out.add(p.snapshotDate());
                } else {
                    out.add(java.util.Objects.requireNonNull(p.in()));
                    out.add(java.util.Objects.requireNonNull(p.out()));
                }
            }
            return out;
        }
        return Set.of();
    }

    /** The engine's node label is the join CONDITION rendered
     * function-prefix style (`equal_<alias><col>_<alias><col>`), with
     * the subtree's ROOT relation spelled `root` — its alias BREADCRUMBS
     * (`_d#2_m1`…) are pureToSqlQuery's internal counters and are
     * stripped on BOTH sides at compare time (handoff: §12 settled).
     * A join we cannot resolve, or a condition shape outside the
     * rendered grammar, keeps the JOIN NAME — a visible diff, never an
     * error. */
    private static String joinLabel(ModelContext ctx, Node n,
            @com.legend.base.Nullable String rootTable) {
        String jn = java.util.Objects.requireNonNull(n.joinName, "joinName");
        if (n.db == null) {
            return jn;
        }
        DatabaseDefinition db = ctx.findDatabase(n.db).orElse(null);
        if (db == null) {
            return jn;
        }
        DatabaseDefinition.JoinDefinition jd = db.joins().stream()
                .filter(j -> j.name().equals(jn)).findFirst().orElse(null);
        if (jd == null) {
            return jn;
        }
        try {
            return mangleCond(jd.operation(), rootTable, n.table);
        } catch (NotImplementedException outsideGrammar) {
            return jn;
        }
    }

    private static String mangleCond(RelationalOperation op,
            @com.legend.base.Nullable String rootTable, @com.legend.base.Nullable String selfTable) {
        return switch (op) {
            case RelationalOperation.ColumnRef cr ->
                    (java.util.Objects.equals(bare(cr.table()), rootTable)
                            ? "root" : bare(cr.table())) + cr.column();
            case RelationalOperation.TargetColumnRef tr ->
                    (java.util.Objects.equals(selfTable, rootTable)
                            ? "root" : String.valueOf(selfTable)) + tr.column();
            case RelationalOperation.Literal l -> String.valueOf(l.value());
            case RelationalOperation.Comparison c ->
                    comparisonName(c.op()) + "_"
                    + mangleCond(c.left(), rootTable, selfTable) + "_"
                    + mangleCond(c.right(), rootTable, selfTable);
            case RelationalOperation.BooleanOp b ->
                    (b.op() == com.legend.model.LogicalOp.AND ? "and_" : "or_")
                    + mangleCond(b.left(), rootTable, selfTable) + "_"
                    + mangleCond(b.right(), rootTable, selfTable);
            case RelationalOperation.Group g ->
                    mangleCond(g.inner(), rootTable, selfTable);
            default -> throw new NotImplementedException(
                    "label grammar: " + op.getClass().getSimpleName());
        };
    }

    private static String comparisonName(com.legend.model.ComparisonOp op) {
        return switch (op) {
            case EQ -> "equal";
            case NEQ -> "notEqual";
            case GT -> "greaterThan";
            case LT -> "lessThan";
            case GTE -> "greaterThanEqual";
            case LTE -> "lessThanEqual";
        };
    }

    /** The view's INTERNAL tree: the model's main-table rule seeds the
     * root ({@link ModelContext#viewMainTable}), the plain column
     * expressions its columns; JoinNavigation expressions and the view
     * ~filter fold their join chains off it. */
    private static @com.legend.base.Nullable Node expandView(ModelContext ctx, String dbName,
            DatabaseDefinition.ViewDefinition vd) {
        String main = ctx.viewMainTable(dbName, vd);
        List<RelationalOperation.ColumnRef> plainRefs = new ArrayList<>();
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping cm
                : vd.columnMappings()) {
            if (!(cm.expression()
                    instanceof RelationalOperation.JoinNavigation)) {
                columnRefs(cm.expression(), plainRefs);
            }
        }
        String rootDb = dbName;
        for (RelationalOperation.ColumnRef r : plainRefs) {
            if (r.databaseName() != null) {
                rootDb = r.databaseName();
                break;
            }
        }
        Node root = new Node(rootDb, bare(main), null);
        for (RelationalOperation.ColumnRef r : plainRefs) {
            root.cols.add(r.column());
        }
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping cm
                : vd.columnMappings()) {
            if (cm.expression()
                    instanceof RelationalOperation.JoinNavigation jn) {
                // webs MERGE by chain prefix (§8.2: the per-web fork
                // compensated for the missing VIEW FETCH — retired in the
                // same commit that emits it; the engine issues one merged
                // base fetch plus the view's own SQL)
                foldJoinNavigation(ctx, root, dbName, jn, "");
            }
        }
        com.legend.model.FilterMapping fm = vd.filter();
        if (fm instanceof com.legend.model.FilterMapping.JoinMediated jm) {
            Node at = joinChain(ctx, null, root,
                    jm.sourceDb() != null ? jm.sourceDb() : dbName,
                    jm.joins());
            assignFilter(ctx, root, at, jm.sourceDb() != null ? jm.sourceDb()
                    : dbName, jm.filter());
        } else if (fm instanceof com.legend.model.FilterMapping.Direct d) {
            assignFilter(ctx, root, root, dbName, d.filter());
        }
        for (RelationalOperation g : vd.groupByColumns()) {
            List<RelationalOperation.ColumnRef> refs = new ArrayList<>();
            columnRefs(g, refs);
            assignByTable(root, refs);
        }
        return root;
    }

    /** A set's ~filter joins the tree like the view filter: its join
     * web's tables and condition columns are part of the set's rows. */
    private static void foldClassFilter(ModelContext ctx, Node node,
            ClassMapping.Relational cm) {
        com.legend.model.FilterMapping fm = cm.filter();
        if (fm instanceof com.legend.model.FilterMapping.JoinMediated jm) {
            Node at = joinChain(ctx, null, node, jm.sourceDb(), jm.joins());
            assignFilter(ctx, node, at, jm.sourceDb(), jm.filter());
        } else if (fm instanceof com.legend.model.FilterMapping.Direct d) {
            String db = node.db;
            if (d.filter() instanceof com.legend.model.FilterPointer.Cross c) {
                db = c.db();
            }
            assignFilter(ctx, node, node, db, d.filter());
        }
    }

    private static void foldJoinNavigation(ModelContext ctx, Node root,
            String dbName, RelationalOperation.JoinNavigation jn,
            String keySuffix) {
        Node at = joinChain(ctx, null, root,
                jn.databaseName() != null ? jn.databaseName() : dbName,
                jn.chain(), keySuffix);
        if (jn.terminal() != null) {
            List<RelationalOperation.ColumnRef> refs = new ArrayList<>();
            columnRefs(jn.terminal(), refs);
            for (RelationalOperation.ColumnRef r : refs) {
                if (java.util.Objects.equals(bare(r.table()), at.table)) {
                    at.cols.add(r.column());
                } else {
                    assignByTable(root, List.of(r));
                }
            }
        }
    }

    private static void assignFilter(ModelContext ctx, Node root, Node at,
            @com.legend.base.Nullable String dbName, com.legend.model.FilterPointer ptr) {
        String fdb = ptr instanceof com.legend.model.FilterPointer.Cross c
                ? c.db() : dbName;
        DatabaseDefinition db = ctx.findDatabase(fdb).orElseThrow(() ->
                new NotImplementedException("scanRelations: unknown filter"
                        + " database '" + fdb + "'"));
        DatabaseDefinition.FilterDefinition fd = db.filters().stream()
                .filter(f -> f.name().equals(ptr.name())).findFirst()
                .orElseThrow(() -> new NotImplementedException(
                        "scanRelations: unknown filter '" + ptr.name() + "'"));
        List<RelationalOperation.ColumnRef> refs = new ArrayList<>();
        columnRefs(fd.condition(), refs);
        assignByTable(root, refs);
    }

    /** Assign each ref's column to the tree node whose table matches —
     * a ref landing nowhere is loud (the tree would silently lie). */
    private static void assignByTable(Node root,
            List<RelationalOperation.ColumnRef> refs) {
        for (RelationalOperation.ColumnRef r : refs) {
            if (!assignOne(root, r)) {
                throw new NotImplementedException("scanRelations: column '"
                        + r.table() + "." + r.column()
                        + "' matches no tree node");
            }
        }
    }

    private static boolean assignOne(Node n,
            RelationalOperation.ColumnRef r) {
        if (java.util.Objects.equals(bare(r.table()), n.table)) {
            n.cols.add(r.column());
            return true;
        }
        for (Node c : n.children.values()) {
            if (assignOne(c, r)) {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------
    // The mapping walk
    // ------------------------------------------------------------------

    private static void walk(ModelContext ctx, LegacyMappingDefinition md,
            ClassMapping.Relational cm, Node node, List<Seg> path, int i,
            boolean tdgMode, boolean runtimeScan, int pathIdx) {
        if (i >= path.size()) {
            return;
        }
        if (!(path.get(i) instanceof Seg.Prop prop)) {
            throw new NotImplementedException(
                    "scanRelations: subType not following a property hop");
        }
        Seg.SubType st = i + 1 < path.size()
                && path.get(i + 1) instanceof Seg.SubType s ? s : null;
        int next = st == null ? i + 1 : i + 2;
        List<PropertyMapping> pms = pmsFor(ctx, md, cm, prop.name());
        if (pms.isEmpty()) {
            // a CLASS-DERIVED property (name = firstName + lastName /
            // qualifiers): expand its BODY's $this chains in place — the
            // engine's scanProperties walks derived definitions the same
            // way
            Derived expanded = derivedChains(ctx, cm, prop.name());
            if (expanded != null) {
                // evaluating a DERIVED property on a UNION arm
                // materializes the instance — the arm projects its PK for
                // identity (engine <pk>_N; plain-column leaves do NOT —
                // testUnionToUnion vs testUnionToUnionMultipleLevels)
                if (runtimeScan
                        && unionSets(ctx, md, cm.className()).size() > 1) {
                    node.cols.addAll(pkCols(ctx, node.db, node.table));
                }
                // only the RESULT chain continues the outer path; the
                // body's predicate reads are self-contained leaf demands
                // (splicing them mid-chained 'type' into the tail was the
                // graphFetch qualifier bug)
                for (List<Seg> sub : expanded.results()) {
                    List<Seg> spliced = new ArrayList<>(sub);
                    spliced.addAll(path.subList(next, path.size()));
                    walk(ctx, md, cm, node, spliced, 0, tdgMode, runtimeScan, pathIdx);
                }
                for (List<Seg> side : expanded.sides()) {
                    walk(ctx, md, cm, node, side, 0, tdgMode, runtimeScan, pathIdx);
                }
                return;
            }
            // GENERATED milestoning members (businessDate/processingDate
            // — real pure generates them, no mapping exists): their value
            // is the query's temporal context, and the table's milestone
            // COLUMNS already ride the tdg demand — nothing to scan
            if (com.legend.compiler.element.Temporal.isGeneratedDateName(prop.name())) {
                return;
            }
            // engine scanRelations.pure: an empty propMappings collection
            // is a NORMAL outcome — $propMappings->map(...) contributes
            // nothing and the set prints childless (union arms that do
            // not map the navigated property; adjudication ledger
            // cluster 4). The old loud throw walled every such arm.
            return;
        }
        // the RUNTIME scan forks a UNION-mapped navigation target per
        // MEMBER SET (union keys + member PK demand; a merged-local-key
        // union labels with the engine's unionAlias grammar); the STATIC
        // scan and tdg walk per pair route
        if (runtimeScan && st == null
                && pms.stream().allMatch(p2 -> p2 instanceof PropertyMapping.Join)) {
            String tgtCls = propertyTargetClass(ctx, cm, prop.name());
            List<ClassMapping.Relational> tsets = tgtCls == null
                    ? List.<ClassMapping.Relational>of() : unionSets(ctx, md, tgtCls);
            if (tsets.size() > 1) {
                unionNavigate(ctx, md, cm, node, prop, tsets, path, next, pathIdx);
                return;
            }
        }
        dispatchPms(ctx, md, cm, node, prop, st, pms, path, next, tdgMode,
                runtimeScan, pathIdx);
    }

    private static List<ClassMapping.Relational> unionSets(ModelContext ctx,
            LegacyMappingDefinition md, String classFqn) {
        try {
            return rootClassMappings(ctx, md, classFqn);
        } catch (NotImplementedException e) {
            return List.of();
        }
    }

    private record URoute(@com.legend.base.Nullable String srcCol, String tgtCol) {
    }

    /** Union-target navigation (engine: the property joins the UNION
     * SUBSELECT, scanned as one child PER MEMBER SET). Key demand per
     * child: the routes' target-side key columns present on the member's
     * table — unless every member maps its route key through ONE
     * same-named LOCAL (+) property (the merged-key union), where each
     * member demands only ITS mapped column and the child labels with
     * the engine's {@code equal_unionAlias<name>_root<srcCol>} grammar
     * (testUnionToSameTableWithDiffKeys); unmerged unions print NO label
     * (testUnion/testUnionToUnion pin both). */
    private static void unionNavigate(ModelContext ctx,
            LegacyMappingDefinition md, ClassMapping.Relational cm, Node node,
            Seg.Prop prop, List<ClassMapping.Relational> targetSets,
            List<Seg> path, int next, int pathIdx) {
        List<ClassMapping.Relational> srcSets = unionSets(ctx, md, cm.className());
        if (srcSets.isEmpty()) {
            srcSets = List.of(cm);
        }
        List<URoute> routes = new ArrayList<>();
        for (ClassMapping.Relational s : srcSets) {
            String srcMain = mainTableOf(s);
            for (PropertyMapping pm : pmsFor(ctx, md, s, prop.name())) {
                if (!(pm instanceof PropertyMapping.Join j)
                        || j.joins().isEmpty()) {
                    continue;
                }
                JoinChainElement last = j.joins().get(j.joins().size() - 1);
                DatabaseDefinition.JoinDefinition jd = joinDef(ctx,
                        last.databaseName() != null ? last.databaseName()
                                : j.database(), last.joinName());
                List<RelationalOperation.ColumnRef> refs = new ArrayList<>();
                columnRefs(jd.operation(), refs);
                String sCol = null;
                String tCol = null;
                for (RelationalOperation.ColumnRef r : refs) {
                    if (java.util.Objects.equals(bare(r.table()), srcMain)) {
                        sCol = r.column();
                    } else {
                        tCol = r.column();
                    }
                }
                if (tCol != null) {
                    URoute r = new URoute(sCol, tCol);
                    if (!routes.contains(r)) {
                        routes.add(r);
                    }
                    // the source set's OWN route key demands on the
                    // source node (root [ID, name] — engine union join)
                    if (sCol != null
                            && java.util.Objects.equals(s.setId(), cm.setId())) {
                        node.cols.add(sCol);
                    }
                }
            }
        }
        // merged-key detection: one local (+) property name maps EVERY
        // member's route key, and all routes read one source column
        String mergedName = null;
        boolean merged = !routes.isEmpty()
                && routes.stream().map(URoute::srcCol).distinct().count() == 1
                && routes.get(0).srcCol() != null;
        for (ClassMapping.Relational ts : merged ? targetSets
                : List.<ClassMapping.Relational>of()) {
            String name = null;
            for (URoute r : routes) {
                String n2 = localPropNameFor(ts, r.tgtCol());
                if (n2 != null) {
                    name = n2;
                    break;
                }
            }
            if (name == null || (mergedName != null && !mergedName.equals(name))) {
                merged = false;
                break;
            }
            mergedName = name;
        }
        for (ClassMapping.Relational ts : targetSets) {
            // repeated paths REUSE the member child (put would overwrite
            // the earlier path's demand — the MultipleLevels name loss)
            Node child = node.children.computeIfAbsent(
                    "union#" + ts.setId(),
                    x -> new Node(java.util.Objects.requireNonNull(
                            mainDbOf(ts), "union member set without a main db"),
                            mainTableOf(ts), null));
            child.labelOverride = merged
                    ? "equal_unionAlias" + mergedName + "_root"
                            + routes.get(0).srcCol()
                    : "";
            if (merged) {
                for (URoute r : routes) {
                    if (localPropNameFor(ts, r.tgtCol()) != null) {
                        child.cols.add(r.tgtCol());
                    }
                }
            } else {
                for (URoute r : routes) {
                    if (tableHasCol(ctx, child.db, child.table, r.tgtCol())) {
                        child.cols.add(r.tgtCol());
                    }
                }
            }
            walk(ctx, md, ts, child, path, next, false, true, pathIdx);
        }
    }

    /** The suffixed-OR union label for the arm {@code self} of a
     * UNION-OPERATION target: {@code or_} over arms j of
     * {@code equal_<src><srcCol_j>_} + (own arm ?
     * {@code unionAlias<tgtCol_j>_<j>} : {@code SQLNull}) — the arm's
     * slice of the union join condition, other arms' key columns reading
     * SQLNull (testTableTreeMultiJoin pins both arms). Null when the
     * target class carries no Union operation mapping or fewer than two
     * Join arms exist. */
    private static @com.legend.base.Nullable String orUnionLabel(ModelContext ctx,
            LegacyMappingDefinition md, ClassMapping.Relational cm, Node node,
            @com.legend.base.Nullable String targetClassFqn, List<PropertyMapping> pms,
            PropertyMapping.Join self) {
        if (targetClassFqn == null
                || !hasUnionOperation(ctx, md, targetClassFqn)) {
            return null;
        }
        List<PropertyMapping.Join> arms = new ArrayList<>();
        for (PropertyMapping pm : pms) {
            if (pm instanceof PropertyMapping.Join a && !a.joins().isEmpty()) {
                arms.add(a);
            }
        }
        if (arms.size() < 2) {
            return null;
        }
        String srcMain = mainTableOf(cm);
        String srcName = node.joinName == null && node.labelOverride == null
                ? "root" : String.valueOf(node.table);
        record Arm(PropertyMapping.Join pm, String tgtTable, String sCol,
                String tCol) {
        }
        List<Arm> resolved = new ArrayList<>();
        for (PropertyMapping.Join a : arms) {
            JoinChainElement last = a.joins().get(a.joins().size() - 1);
            DatabaseDefinition.JoinDefinition jd = joinDef(ctx,
                    last.databaseName() != null ? last.databaseName()
                            : a.database(), last.joinName());
            List<RelationalOperation.ColumnRef> refs = new ArrayList<>();
            columnRefs(jd.operation(), refs);
            String sCol = null;
            String tCol = null;
            String tTab = null;
            for (RelationalOperation.ColumnRef r : refs) {
                if (java.util.Objects.equals(bare(r.table()), srcMain)) {
                    sCol = r.column();
                } else {
                    tCol = r.column();
                    tTab = bare(r.table());
                }
            }
            if (sCol == null || tCol == null || tTab == null) {
                return null;   // outside the grammar — keep the mangle
            }
            resolved.add(new Arm(a, tTab, sCol, tCol));
        }
        // the engine's arm ordinal follows the OPERATION's member order —
        // target-name order in the inheritance router (Bicycle < Car,
        // testTableTreeMultiJoin), not PM declaration order
        resolved.sort(java.util.Comparator
                .comparing(Arm::tgtTable).thenComparing(Arm::tCol));
        StringBuilder sb = new StringBuilder("or");
        for (int jx = 0; jx < resolved.size(); jx++) {
            Arm a = resolved.get(jx);
            sb.append("_equal_").append(srcName).append(a.sCol()).append('_');
            sb.append(a.pm() == self
                    ? "unionAlias" + a.tCol() + "_" + jx : "SQLNull");
        }
        return sb.toString();
    }

    /** The SOURCE-side union label — the mirror of {@link #orUnionLabel}
     * for a hop departing FROM a union member (the union subselect is the
     * join's LEFT side): arm k's label is {@code or_} over member arms j
     * of {@code equal_} + (j==k ? {@code unionBase<srcCol_j>_<j>} :
     * {@code SQLNull}) + {@code _<tgtTable><tgtCol_j>}; arm ordinals from
     * the Union operation's memberSetIds (testUnionViewOnView pins it).
     * Null when the source class carries no Union operation with declared
     * members, or a member lacks a Join PM for the property. */
    private static @com.legend.base.Nullable String srcUnionLabel(ModelContext ctx,
            LegacyMappingDefinition md, ClassMapping.Relational cm,
            String propName) {
        List<String> members = null;
        for (LegacyMappingDefinition m : withIncludes(ctx, md)) {
            for (ClassMapping c : m.classMappings()) {
                if (c instanceof ClassMapping.Union u
                        && typeMatches(u.className(), cm.className())
                        && !u.memberSetIds().isEmpty()) {
                    members = u.memberSetIds();
                    break;
                }
            }
        }
        if (members == null || members.size() < 2
                || !members.contains(cm.setId())) {
            return null;
        }
        int k = members.indexOf(cm.setId());
        StringBuilder sb = new StringBuilder("or");
        for (int jx = 0; jx < members.size(); jx++) {
            ClassMapping.Relational ms;
            try {
                ms = classMappingFor(ctx, md, null, members.get(jx));
            } catch (NotImplementedException e) {
                return null;
            }
            PropertyMapping.Join mj = null;
            for (PropertyMapping pm : pmsFor(ctx, md, ms, propName)) {
                if (pm instanceof PropertyMapping.Join pj
                        && !pj.joins().isEmpty()) {
                    mj = pj;
                    break;
                }
            }
            if (mj == null) {
                return null;
            }
            JoinChainElement last = mj.joins().get(mj.joins().size() - 1);
            DatabaseDefinition.JoinDefinition jd = joinDef(ctx,
                    last.databaseName() != null ? last.databaseName()
                            : mj.database(), last.joinName());
            String srcMain = mainTableOf(ms);
            List<RelationalOperation.ColumnRef> refs = new ArrayList<>();
            columnRefs(jd.operation(), refs);
            String sCol = null;
            String tCol = null;
            String tTab = null;
            for (RelationalOperation.ColumnRef r : refs) {
                if (java.util.Objects.equals(bare(r.table()), srcMain)) {
                    sCol = r.column();
                } else {
                    tCol = r.column();
                    tTab = bare(r.table());
                }
            }
            if (sCol == null || tCol == null || tTab == null) {
                return null;
            }
            sb.append("_equal_");
            sb.append(jx == k ? "unionBase" + sCol + "_" + jx : "SQLNull");
            sb.append('_').append(tTab).append(tCol);
        }
        return sb.toString();
    }

    /** Whether the mapping closure carries an OPERATION mapping (union
     * or inheritance) for {@code classFqn} — an UN-NARROWED hop to such a
     * target rides the union subselect and labels with the suffixed-OR
     * grammar; a ->subType()-pinned hop joins its arm independently and
     * keeps the plain mangle (testTableTreeMultiJoin vs
     * testTableTree_Inheritance_2 pin the split). */
    private static boolean hasUnionOperation(ModelContext ctx,
            LegacyMappingDefinition md, String classFqn) {
        for (LegacyMappingDefinition m : withIncludes(ctx, md)) {
            for (ClassMapping c : m.classMappings()) {
                if ((c instanceof ClassMapping.Union
                        || c instanceof ClassMapping.Inheritance)
                        && typeMatches(c.className(), classFqn)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** The LOCAL (+) property name whose Column PM maps {@code col} in
     * {@code cm}, or null. */
    private static @com.legend.base.Nullable String localPropNameFor(
            ClassMapping.Relational cm, String col) {
        for (PropertyMapping pm : cm.propertyMappings()) {
            if (pm instanceof PropertyMapping.LocalProperty lp
                    && lp.body() instanceof PropertyMapping.Column c
                    && c.column().equals(col)) {
                return lp.propertyName();
            }
        }
        return null;
    }

    /** PRIMARY KEY columns of {@code table} in {@code dbFqn}. */
    private static Set<String> pkCols(ModelContext ctx, String dbFqn,
            @com.legend.base.Nullable String table) {
        if (table == null) {
            return Set.of();
        }
        Set<String> out = new TreeSet<>();
        ctx.findTableDefinition(dbFqn, table).ifPresent(t -> t.columns().stream()
                .filter(DatabaseDefinition.ColumnDefinition::primaryKey)
                .forEach(c -> out.add(c.name())));
        return out;
    }

    private static boolean tableHasCol(ModelContext ctx, String dbFqn,
            @com.legend.base.Nullable String table, String col) {
        if (table == null) {
            return false;
        }
        return ctx.findTableDefinition(dbFqn, table)
                .map(t -> t.columns().stream().anyMatch(c -> c.name().equals(col)))
                .orElse(false);
    }

    /** One property hop's mappings dispatch — shared by class-mapping hops
     * and EMBEDDED sub-hops (which continue on the SAME node). */
    private static void dispatchPms(ModelContext ctx,
            LegacyMappingDefinition md, ClassMapping.Relational cm, Node node,
            Seg.Prop prop, Seg.@com.legend.base.Nullable SubType st, List<PropertyMapping> pms,
            List<Seg> path, int next, boolean tdgMode, boolean runtimeScan,
            int pathIdx) {
        for (PropertyMapping pm : pms) {
            switch (pm) {
                case PropertyMapping.Column c -> {
                    if (next < path.size()) {
                        throw new NotImplementedException("scanRelations:"
                                + " scalar '" + prop.name()
                                + "' in MID position");
                    }
                    node.cols.add(c.column());
                }
                case PropertyMapping.Expression ex -> {
                    // derived scalar (concat(firstName, lastName)): its
                    // source columns are the demand
                    List<RelationalOperation.ColumnRef> refs =
                            new ArrayList<>();
                    columnRefs(ex.expression(), refs);
                    assignByTable(node, refs);
                }
                case PropertyMapping.EnumeratedColumn ec -> {
                    // the enum decode is value-level — the SOURCE COLUMN
                    // is the lineage, same as a plain Column leaf
                    if (next < path.size()) {
                        throw new NotImplementedException("scanRelations:"
                                + " scalar '" + prop.name()
                                + "' in MID position");
                    }
                    node.cols.add(ec.column());
                }
                case PropertyMapping.Join j -> {
                    ClassMapping.Relational target = targetCm(ctx, md, j,
                            node.table, propertyTargetClass(ctx, cm,
                                    prop.name()));
                    // STATIC: EVERY subtype set's join edge emits (the
                    // engine fetches all sets' keys — testSelectOnLeftSide
                    // pins Bicycle(PersonBicycle) [b_PersonID] though only
                    // Car is navigated) and subType narrows the
                    // CONTINUATION. RUNTIME: a narrowed hop PRUNES the
                    // non-matching arm entirely (the same test's runtime
                    // assert carries no Bicycle node).
                    if (runtimeScan && st != null
                            && !typeMatches(target.className(),
                                    st.classFqn())) {
                        continue;
                    }
                    // the RUNTIME scan keys a chain by its FULL identity:
                    // the SAME chain read from many paths SHARES one
                    // thread (Qualifier/TwoFilters), but chains differing
                    // ANYWHERE never share a prefix hop (SameRelations
                    // AtSameLevel: three @AB-prefixed chains fork three
                    // bTable1 instances). The STATIC scan merges per hop.
                    Node child = joinChain(ctx, md, node, j.database(),
                            j.joins(), runtimeScan
                                    ? "#c" + chainKey(j.joins()) : "");
                    // a UNION-OPERATION target's runtime label is the
                    // arm's SLICE of the union join condition (suffixed-OR
                    // grammar; other arms read SQLNull) — plain multi-set
                    // inheritance keeps join-name mangles
                    // (testTableTreeMultiJoin vs Inheritance_2)
                    if (runtimeScan && st == null) {
                        String lbl = orUnionLabel(ctx, md, cm, node,
                                propertyTargetClass(ctx, cm, prop.name()),
                                pms, j);
                        if (lbl == null) {
                            lbl = srcUnionLabel(ctx, md, cm, prop.name());
                        }
                        if (lbl != null) {
                            child.labelOverride = lbl;
                        }
                    }
                    if (st != null && !typeMatches(target.className(),
                            st.classFqn())) {
                        continue;
                    }
                    walk(ctx, md, target, child, path, next, tdgMode, runtimeScan, pathIdx);
                    if (tdgMode) {
                        unionSiblings(ctx, md, node, child, target, st,
                                path, next);
                    }
                }
                case PropertyMapping.JoinTerminalColumn jt -> {
                    Node child = joinChain(ctx, md, node, jt.database(),
                            jt.joins(), runtimeScan
                                    ? "#c" + chainKey(jt.joins()) : "");
                    if (jt.terminalColumn()
                            instanceof RelationalOperation.ColumnRef cr) {
                        child.cols.add(cr.column());
                    } else {
                        throw new NotImplementedException("scanRelations:"
                                + " non-column join terminal");
                    }
                }
                case PropertyMapping.Embedded emb -> {
                    // EMBEDDED = same-table sub-columns: no join, the
                    // nested class's property mappings resolve against
                    // the SAME node (engine embedded semantics)
                    if (next >= path.size()) {
                        throw new NotImplementedException("scanRelations:"
                                + " embedded '" + prop.name()
                                + "' as a LEAF is not supported yet");
                    }
                    if (!(path.get(next) instanceof Seg.Prop nprop)) {
                        throw new NotImplementedException("scanRelations:"
                                + " subType directly on embedded '"
                                + prop.name() + "'");
                    }
                    Seg.SubType nst = next + 1 < path.size()
                            && path.get(next + 1) instanceof Seg.SubType s2
                            ? s2 : null;
                    int nnext = nst == null ? next + 1 : next + 2;
                    List<PropertyMapping> sub = new ArrayList<>();
                    for (PropertyMapping p2 : emb.propertyMappings()) {
                        if (p2.propertyName().equals(nprop.name())) {
                            sub.add(p2);
                        }
                    }
                    if (sub.isEmpty()) {
                        throw new NotImplementedException("scanRelations:"
                                + " property '" + nprop.name()
                                + "' has no mapping in embedded '"
                                + prop.name() + "'");
                    }
                    dispatchPms(ctx, md, cm, node, nprop, nst, sub, path,
                            nnext, tdgMode, runtimeScan, pathIdx);
                }
                default -> throw new NotImplementedException("scanRelations: "
                        + pm.getClass().getSimpleName()
                        + " property mapping is not supported yet");
            }
        }
    }

    /** #46 union-target expansion: the engine's testDataGeneration tree
     * fetches a union-mapped JOIN target once PER SET — its own goldens
     * carry the duplicate per-set SQLs. Each extra set gets a SIBLING of
     * the walked child (same table/edge) and its own sub-walk. */
    private static void unionSiblings(ModelContext ctx,
            LegacyMappingDefinition md, Node parent, Node child,
            ClassMapping.Relational walked, Seg.@com.legend.base.Nullable SubType st, List<Seg> path,
            int next) {
        List<ClassMapping.Relational> sets;
        try {
            sets = rootClassMappings(ctx, md, walked.className());
        } catch (NotImplementedException e) {
            return;
        }
        if (sets.size() < 2) {
            return;
        }
        int k = 0;
        for (ClassMapping.Relational cm2 : sets) {
            k++;
            if (Objects.equals(cm2.setId(), walked.setId())
                    || st != null
                       && !typeMatches(cm2.className(), st.classFqn())) {
                continue;
            }
            // only SAME-TABLE sibling sets fetch through the cloned edge
            // (a different-table set has no rows reachable via this join
            // — testUnionToUnionMultipleLevels' per-set tables)
            String mt2;
            try {
                mt2 = mainTableOf(cm2);
            } catch (NotImplementedException e) {
                continue;
            }
            if (!java.util.Objects.equals(mt2, child.table)) {
                continue;
            }
            String key = child.table + "(" + child.joinName + ")[set" + k
                    + "]";
            Node dup = parent.children.computeIfAbsent(key,
                    x -> new Node(child.db, child.table, child.joinName));
            dup.cols.addAll(child.cols);
            dup.cond = child.cond;
            walk(ctx, md, cm2, dup, path, next, true, false, 0);
        }
    }

    /** The $this chains of a class-derived property's body, or null when
     * the class declares no such derived property (or its body is a
     * function-ref binding). */
    private record Derived(List<List<Seg>> results, List<List<Seg>> sides) {}

    private static @com.legend.base.Nullable Derived derivedChains(ModelContext ctx,
            ClassMapping.Relational cm, String prop) {
        com.legend.model.ClassDefinition cd = classDef(ctx, cm.className());
        if (cd == null) {
            return null;
        }
        for (com.legend.protocol.DerivedPropertyDefinition dp
                : cd.derivedProperties()) {
            if (!dp.name().equals(prop)) {
                continue;
            }
            if (!(dp.realization()
                    instanceof com.legend.protocol.Realization.Inline inl)) {
                return null;
            }
            // VAR-SCOPED collection: $this chains splice verbatim; an
            // inner lambda var over a scoped source ($this.synonyms
            // ->filter(s|$s.type == $type)) splices its reads UNDER the
            // source chain ([synonyms, type]); qualifier PARAMETERS are
            // out of scope and contribute nothing (the '$type' collision
            // walled the graphFetch qualifier trees)
            List<List<Seg>> sides = new ArrayList<>();
            List<List<Seg>> results = new ArrayList<>();
            java.util.Map<String, List<Seg>> scope = new java.util.HashMap<>();
            scope.put("this", List.of());
            for (ValueSpecification b : inl.body()) {
                scopedChains(b, scope, sides);
            }
            // the RESULT chain: the (last) body expression's pipe seen
            // through multiplicity wrappers and filters
            ValueSpecification last = inl.body().get(inl.body().size() - 1);
            List<Seg> res = qualifierResultChain(last, scope);
            if (res != null && !res.isEmpty()) {
                results.add(res);
                sides.removeIf(s -> s.equals(res));
            }
            return new Derived(results, sides);
        }
        return null;
    }

    private static @com.legend.base.Nullable List<Seg> qualifierResultChain(ValueSpecification b,
            java.util.Map<String, List<Seg>> scope) {
        ValueSpecification cur = b;
        while (cur instanceof AppliedFunction af && !af.parameters().isEmpty()
                && java.util.Set.of("toOne", "first", "head", "filter",
                        "toOneMany").contains(simple(af.function()))) {
            cur = af.parameters().get(0);
        }
        return scopedChainOf(cur, scope);
    }

    /** Boolean/comparison/arithmetic natives recurse into their operands —
     * chainOf's qualified-property arm must not hop on them ('equal' and
     * 'times' tails: averageEmployeesAge = ...->average() * 2.0). */
    private static final java.util.Set<String> PRED_OPS = java.util.Set.of(
            "equal", "notEqual", "and", "or", "not", "lessThan",
            "lessThanEqual", "greaterThan", "greaterThanEqual", "in",
            "isEmpty", "isNotEmpty", "contains", "startsWith", "endsWith",
            "times", "multiply", "plus", "minus", "divide", "rem", "mod");

    private static void scopedChains(ValueSpecification n,
            java.util.Map<String, List<Seg>> scope, List<List<Seg>> out) {
        if (n instanceof AppliedFunction bf
                && PRED_OPS.contains(simple(bf.function()))) {
            bf.parameters().forEach(p -> scopedChains(p, scope, out));
            return;
        }
        if (n instanceof com.legend.protocol.spec.PureCollection run) {
            // the arithmetic carrier's operands (a + b is plus([a, b]))
            run.values().forEach(p -> scopedChains(p, scope, out));
            return;
        }
        List<Seg> c = scopedChainOf(n, scope);
        if (c != null) {
            if (!c.isEmpty()) {
                out.add(c);
            }
            return;
        }
        switch (n) {
            case AppliedFunction af -> {
                if (af.parameters().size() == 2
                        && af.parameters().get(1) instanceof LambdaFunction lam
                        && lam.parameters().size() == 1) {
                    List<Seg> src = scopedChainOf(af.parameters().get(0), scope);
                    if (src != null) {
                        if (!src.isEmpty()) {
                            out.add(src);
                        }
                        java.util.Map<String, List<Seg>> inner =
                                new java.util.HashMap<>(scope);
                        inner.put(lam.parameters().get(0).name(), src);
                        lam.body().forEach(x -> scopedChains(x, inner, out));
                        return;
                    }
                }
                af.parameters().forEach(p -> scopedChains(p, scope, out));
            }
            case AppliedProperty ap -> scopedChains(ap.receiver(), scope, out);
            case LambdaFunction lf -> lf.body()
                    .forEach(x -> scopedChains(x, scope, out));
            case PureCollection pc -> pc.values()
                    .forEach(v -> scopedChains(v, scope, out));
            default -> n.children().forEach(x -> scopedChains(x, scope, out));
        }
    }

    /** {@link #chainOf} restricted to roots IN SCOPE, prefixed by the
     * root's own chain; null when the root var is unscoped (a qualifier
     * parameter) or the node is not a chain. */
    private static @com.legend.base.Nullable List<Seg> scopedChainOf(ValueSpecification n,
            java.util.Map<String, List<Seg>> scope) {
        String root = rootVarOf(n);
        if (root == null || !scope.containsKey(root)) {
            return null;
        }
        List<Seg> tail = chainOf(n);
        if (tail == null) {
            return null;
        }
        List<Seg> full = new ArrayList<>(scope.get(root));
        full.addAll(tail);
        return full;
    }

    private static @com.legend.base.Nullable String rootVarOf(ValueSpecification n) {
        return switch (n) {
            case Variable v -> v.name();
            case AppliedProperty ap -> rootVarOf(ap.receiver());
            case AppliedFunction af -> af.parameters().isEmpty() ? null
                    : rootVarOf(af.parameters().get(0));
            default -> null;
        };
    }

    /** The parsed class definition for an as-written class spelling. */
    private static com.legend.model.@com.legend.base.Nullable ClassDefinition classDef(ModelContext ctx,
            String written) {
        var direct = ctx.findClassDefinition(written);
        if (direct.isPresent()) {
            return direct.get();
        }
        // as-written short names: resolve by unambiguous tail over the
        // element index
        com.legend.model.ClassDefinition hit = null;
        for (String fqn : ctx.elementFqns()) {
            if (typeMatches(written, fqn)) {
                var cd = ctx.findClassDefinition(fqn);
                if (cd.isPresent()) {
                    if (hit != null) {
                        return null;   // ambiguous: stay loud upstream
                    }
                    hit = cd.get();
                }
            }
        }
        return hit;
    }

    /** All same-named PMs: class-mapping entries first, association-
     * mapping ends second (matched by source set). */
    private static List<PropertyMapping> pmsFor(ModelContext ctx,
            LegacyMappingDefinition md,
            ClassMapping.Relational cm, String prop) {
        List<PropertyMapping> out = new ArrayList<>();
        for (PropertyMapping pm : cm.propertyMappings()) {
            if (pm.propertyName().equals(prop)) {
                out.add(pm);
            }
        }
        if (!out.isEmpty()) {
            return out;
        }
        List<AssociationMapping> ams = new ArrayList<>();
        for (LegacyMappingDefinition m : withIncludes(ctx, md)) {
            ams.addAll(allAssociationMappings(m));
        }
        for (AssociationMapping am : ams) {
            for (com.legend.model.AssociationPropertyMapping apm
                    : am.propertyMappings()) {
                if (!apm.propertyName().equals(prop)) {
                    continue;
                }
                if (apm.sourceSetId() != null && cm.setId() != null
                        && !apm.sourceSetId().equals(cm.setId())) {
                    continue;
                }
                out.add(apm.body());
            }
        }
        return out;
    }

    /** The chain's identity for runtime-scan thread keying — the full
     * ordered join-name list. */
    private static String chainKey(List<JoinChainElement> joins) {
        StringBuilder sb = new StringBuilder();
        for (JoinChainElement el : joins) {
            sb.append(el.joinName()).append('>');
        }
        return sb.toString();
    }

    /** Fold a join chain under {@code parent}, assigning each side's
     * condition columns to its node; returns the DEEPEST node. */
    private static Node joinChain(ModelContext ctx,
            @com.legend.base.Nullable LegacyMappingDefinition md,
            Node parent, @com.legend.base.Nullable String db, List<JoinChainElement> joins) {
        return joinChain(ctx, md, parent, db, joins, "");
    }

    private static Node joinChain(ModelContext ctx,
            @com.legend.base.Nullable LegacyMappingDefinition md,
            Node parent, @com.legend.base.Nullable String db, List<JoinChainElement> joins,
            String keySuffix) {
        Node cur = parent;
        for (JoinChainElement el : joins) {
            final Node at = cur;
            String edb = el.databaseName();
            String dbName = edb != null ? edb : db;
            DatabaseDefinition.JoinDefinition jd = joinDef(ctx, dbName,
                    el.joinName());
            Set<String> tables = new LinkedHashSet<>();
            List<RelationalOperation.ColumnRef> refs = new ArrayList<>();
            List<String> targetCols = new ArrayList<>();
            columnRefs(jd.operation(), refs, targetCols);
            for (RelationalOperation.ColumnRef r : refs) {
                tables.add(bare(r.table()));
            }
            // SELF-JOIN: every plain ref spells the parent side, {target}
            // refs the child — same table, distinct node
            String other = tables.stream()
                    .filter(t -> !t.equals(at.table))
                    .findFirst().orElseGet(() -> {
                        if (!targetCols.isEmpty()) {
                            return at.table;
                        }
                        throw new NotImplementedException("scanRelations:"
                                + " self-join '" + el.joinName()
                                + "' carries no {target} side");
                    });
            String otherDb = refs.stream()
                    .filter(r -> other.equals(bare(r.table())))
                    .map(RelationalOperation.ColumnRef::databaseName)
                    .filter(Objects::nonNull).findFirst().orElse(dbName);
            String otherSchema = refs.stream()
                    .filter(r -> other.equals(bare(r.table())))
                    .map(r -> schemaOf(r.table()))
                    .filter(Objects::nonNull).findFirst().orElse(null);
            Node child = at.children.computeIfAbsent(
                    other + "(" + el.joinName() + ")" + keySuffix,
                    k -> new Node(java.util.Objects.requireNonNull(otherDb,
                            "join edge without a database"),
                            otherSchema, other, el.joinName()));
            boolean selfJoin = java.util.Objects.equals(at.table,
                    child.table);
            for (RelationalOperation.ColumnRef r : refs) {
                if (selfJoin) {
                    // SELF-join: both operand columns exist on both sides
                    // — the engine assigns them to both nodes
                    // (testMultipleTablesInQualifiedPropertiesInGraphFetch
                    // Tree pins parent ID + child MANAGERID)
                    at.cols.add(r.column());
                    child.cols.add(r.column());
                } else if (java.util.Objects.equals(bare(r.table()), at.table)) {
                    at.cols.add(r.column());
                } else if (java.util.Objects.equals(bare(r.table()), child.table)) {
                    child.cols.add(r.column());
                } else {
                    throw new NotImplementedException("scanRelations: join '"
                            + el.joinName() + "' touches a third table '"
                            + r.table() + "'");
                }
            }
            child.cols.addAll(targetCols);
            if (selfJoin) {
                // {target}-side columns of a SELF-join exist on the parent
                // too (same table — the engine assigns both sides both)
                at.cols.addAll(targetCols);
            }
            cur = child;
        }
        return cur;
    }

    private static void columnRefs(RelationalOperation op,
            List<RelationalOperation.ColumnRef> out) {
        columnRefs(op, out, null);
    }

    /** {@code targetCols} non-null accepts {@code {target}.COL} refs
     * (self-join vocabulary); null keeps them a loud wall (view
     * expressions never carry a target side). */
    private static void columnRefs(RelationalOperation op,
            List<RelationalOperation.ColumnRef> out,
            @com.legend.base.Nullable List<String> targetCols) {
        switch (op) {
            case RelationalOperation.ColumnRef cr -> out.add(cr);
            case RelationalOperation.TargetColumnRef tr -> {
                if (targetCols == null) {
                    throw new NotImplementedException("scanRelations:"
                            + " {target} reference outside a join"
                            + " condition");
                }
                targetCols.add(tr.column());
            }
            case RelationalOperation.Comparison c -> {
                columnRefs(c.left(), out, targetCols);
                columnRefs(c.right(), out, targetCols);
            }
            case RelationalOperation.BooleanOp b -> {
                columnRefs(b.left(), out, targetCols);
                columnRefs(b.right(), out, targetCols);
            }
            case RelationalOperation.Group g ->
                    columnRefs(g.inner(), out, targetCols);
            case RelationalOperation.IsNull n ->
                    columnRefs(n.operand(), out, targetCols);
            case RelationalOperation.IsNotNull n ->
                    columnRefs(n.operand(), out, targetCols);
            case RelationalOperation.FunctionCall f -> {
                for (RelationalOperation a : f.args()) {
                    columnRefs(a, out, targetCols);
                }
            }
            case RelationalOperation.Literal ignored -> { }
            default -> throw new NotImplementedException("scanRelations:"
                    + " join condition node "
                    + op.getClass().getSimpleName());
        }
    }

    // ------------------------------------------------------------------
    // Lookups
    // ------------------------------------------------------------------

    private static LegacyMappingDefinition mapping(ModelContext ctx,
            String fqn) {
        return ctx.findLegacyMapping(fqn).orElseThrow(() ->
                new NotImplementedException("scanRelations: unknown legacy"
                        + " mapping '" + fqn + "'"));
    }

    private static List<ClassMapping.Relational> allClassMappings(
            LegacyMappingDefinition md) {
        List<ClassMapping.Relational> out = new ArrayList<>();
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.Relational r) {
                out.add(r);
            }
        }
        return out;
    }

    private static List<AssociationMapping> allAssociationMappings(
            LegacyMappingDefinition md) {
        return md.associationMappings();
    }

    /** {@code md} plus its include closure, own-first (own definitions
     * shadow included ones, the mapping-include rule). */
    private static List<LegacyMappingDefinition> withIncludes(ModelContext ctx,
            LegacyMappingDefinition md) {
        List<LegacyMappingDefinition> out = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        collectIncludes(ctx, md, out, seen);
        return out;
    }

    private static void collectIncludes(ModelContext ctx,
            LegacyMappingDefinition md, List<LegacyMappingDefinition> out,
            Set<String> seen) {
        if (!seen.add(md.qualifiedName())) {
            return;
        }
        out.add(md);
        for (com.legend.model.MappingInclude inc : md.includes()) {
            ctx.findLegacyMapping(inc.mappingPath()).ifPresent(in ->
                    collectIncludes(ctx, in, out, seen));
        }
    }

    private static ClassMapping.Relational classMappingFor(ModelContext ctx,
            LegacyMappingDefinition md, @com.legend.base.Nullable String classFqn,
            @com.legend.base.Nullable String setId) {
        List<ClassMapping.Relational> hits = new ArrayList<>();
        for (LegacyMappingDefinition m : withIncludes(ctx, md)) {
            for (ClassMapping.Relational r : allClassMappings(m)) {
                if (setId != null ? setId.equals(r.setId())
                        : typeMatches(r.className(), classFqn)) {
                    hits.add(r);
                }
            }
        }
        if (hits.size() != 1) {
            throw new NotImplementedException("scanRelations: "
                    + hits.size() + " class mappings for '"
                    + (setId != null ? "[" + setId + "]" : classFqn) + "'");
        }
        return hits.get(0);
    }

    /** The join PM's target class mapping: explicit {@code targetSetId}
     * wins; else the single set whose main table is the join's landing
     * table. */
    /** The declared class of a property (deep through supers), null when
     * unresolvable — the join-target disambiguator. */
    private static @com.legend.base.Nullable String propertyTargetClass(ModelContext ctx,
            ClassMapping.Relational cm, String prop) {
        java.util.ArrayDeque<String> q = new java.util.ArrayDeque<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        q.add(cm.className());
        while (!q.isEmpty()) {
            com.legend.model.ClassDefinition cd = classDef(ctx, q.poll());
            if (cd == null) {
                continue;
            }
            for (var p : cd.properties()) {
                if (p.name().equals(prop)
                        && p.type() instanceof com.legend.protocol.TypeExpression.NameRef nr) {
                    return nr.name();
                }
            }
            for (com.legend.protocol.TypeExpression s : cd.superClasses()) {
                if (s instanceof com.legend.protocol.TypeExpression.NameRef snr
                        && seen.add(snr.name())) {
                    q.add(snr.name());
                }
            }
            // association ends are class properties semantically
            var end = ctx.findAssociationEnd(cd.qualifiedName(), prop);
            if (end.isPresent() && end.get().targetClass()
                    instanceof com.legend.protocol.TypeExpression.NameRef anr) {
                return anr.name();
            }
        }
        return null;
    }

    private static ClassMapping.Relational targetCm(ModelContext ctx,
            LegacyMappingDefinition md, PropertyMapping.Join j,
            @com.legend.base.Nullable String fromTable, @com.legend.base.Nullable String targetClassHint) {
        if (j.targetSetId() != null) {
            return classMappingFor(ctx, md, null, j.targetSetId());
        }
        JoinChainElement last = j.joins().get(j.joins().size() - 1);
        DatabaseDefinition.JoinDefinition jd = joinDef(ctx,
                last.databaseName() != null ? last.databaseName()
                        : j.database(), last.joinName());
        List<RelationalOperation.ColumnRef> refs = new ArrayList<>();
        // {target} refs tolerated: table inference rides the plain side
        // (a self-join's target table IS the plain side's table)
        columnRefs(jd.operation(), refs, new ArrayList<>());
        List<ClassMapping.Relational> hits = new ArrayList<>();
        List<ClassMapping.Relational> all = new ArrayList<>();
        for (LegacyMappingDefinition m : withIncludes(ctx, md)) {
            all.addAll(allClassMappings(m));
        }
        for (RelationalOperation.ColumnRef r : refs) {
            for (ClassMapping.Relational cm : all) {
                String mt;
                try {
                    mt = mainTableOf(cm);
                } catch (NotImplementedException e) {
                    continue;
                }
                if (mt != null && mt.equals(bare(r.table()))
                        && !hits.contains(cm)) {
                    hits.add(cm);
                }
            }
        }
        // prefer sets on OTHER tables — but a SELF-JOIN's target IS the
        // from-table, so never empty the candidate list
        List<ClassMapping.Relational> nonSelf = hits.stream()
                .filter(cm -> !java.util.Objects.equals(mainTableOf(cm),
                        bare(fromTable)))
                .toList();
        if (!nonSelf.isEmpty()) {
            hits = new ArrayList<>(nonSelf);
        }
        if (hits.size() > 1 && targetClassHint != null) {
            // several classes share the join's table: the PROPERTY's
            // declared type picks the set (engine findPropertyMapping
            // resolves per receiver class)
            List<ClassMapping.Relational> byClass = hits.stream()
                    .filter(cm -> typeMatches(cm.className(), targetClassHint)
                            || typeMatches(targetClassHint, cm.className()))
                    .toList();
            if (byClass.size() == 1) {
                return byClass.get(0);
            }
        }
        if (hits.size() != 1) {
            throw new NotImplementedException("scanRelations: join target of"
                    + " '" + j.propertyName() + "' is ambiguous ("
                    + hits.size() + " sets: " + hits.stream()
                            .map(h -> h.className() + "[" + h.setId() + "]")
                            .toList() + "; hint=" + targetClassHint + ")");
        }
        return hits.get(0);
    }

    private static DatabaseDefinition.JoinDefinition joinDef(ModelContext ctx,
            @com.legend.base.Nullable String dbName, String joinName) {
        // include-closure aware (Database DB2 ( include DB1 ) resolves
        // DB1's joins — the quoted-columns-for-views tdg family)
        return ctx.findJoinDefinition(dbName, joinName)
                .orElseThrow(() -> new NotImplementedException(
                        "scanRelations: unknown join '" + joinName
                        + "' in database '" + dbName + "'"));
    }

    /** As-written vs resolved class spellings: exact first, then an
     * unambiguous tail match (mapping models keep source spellings). */
    private static boolean typeMatches(String written, @com.legend.base.Nullable String fqn) {
        if (Objects.equals(written, fqn)) {
            return true;
        }
        if (fqn == null) {
            return false;
        }
        String wt = written.contains("::")
                ? written.substring(written.lastIndexOf("::") + 2) : written;
        String ft = fqn.contains("::")
                ? fqn.substring(fqn.lastIndexOf("::") + 2) : fqn;
        return wt.equals(ft)
                && (!written.contains("::") || !fqn.contains("::"));
    }

    /** The set's main table — explicit {@code ~mainTable}, else IMPLIED
     * by the first column property mapping (engine grammar: mainTable is
     * 0..1). */
    private static @com.legend.base.Nullable String mainTableOf(ClassMapping.Relational cm) {
        if (cm.mainTable() != null) {
            return bare(cm.mainTable().table());
        }
        for (PropertyMapping pm : cm.propertyMappings()) {
            if (pm instanceof PropertyMapping.Column c) {
                return bare(c.table());
            }
        }
        throw new NotImplementedException("scanRelations: set '"
                + cm.className() + "' has no main table (explicit or"
                + " implied by a column mapping)");
    }

    private static @com.legend.base.Nullable String mainDbOf(ClassMapping.Relational cm) {
        if (cm.mainTable() != null) {
            return cm.mainTable().database();
        }
        for (PropertyMapping pm : cm.propertyMappings()) {
            if (pm instanceof PropertyMapping.Column c) {
                return c.database();
            }
        }
        return null;
    }

    private static @com.legend.base.Nullable String bare(@com.legend.base.Nullable String table) {
        return table != null && table.contains(".")
                ? table.substring(table.lastIndexOf('.') + 1) : table;
    }

    /** The qualifying schema of {@code Schema.Table}, null when bare. */
    private static @com.legend.base.Nullable String schemaOf(@com.legend.base.Nullable String table) {
        return table != null && table.contains(".")
                ? table.substring(0, table.lastIndexOf('.')) : null;
    }

    // ------------------------------------------------------------------
    // Query-side extraction
    // ------------------------------------------------------------------

    private static @com.legend.base.Nullable String rootClassFqn(ValueSpecification n) {
        if (n instanceof AppliedFunction af) {
            if (com.legend.compiler.ResolvedNames.names(af, com.legend.builtin.Pure.GET_ALL__CLASS_1.qualifiedName())
                    && !af.parameters().isEmpty()
                    && af.parameters().get(0)
                            instanceof PackageableElementPtr p) {
                return p.fullPath();
            }
            for (ValueSpecification c : af.parameters()) {
                try {
                    return rootClassFqn(c);
                } catch (IllegalStateException ignore) {
                    // keep scanning
                }
            }
        }
        if (n instanceof LambdaFunction lf) {
            for (ValueSpecification b : lf.body()) {
                try {
                    return rootClassFqn(b);
                } catch (IllegalStateException ignore) {
                    // keep scanning
                }
            }
        }
        if (n instanceof AppliedProperty ap) {
            return rootClassFqn(ap.receiver());
        }
        throw new IllegalStateException("no getAll root");
    }

    /** Every var-rooted property chain in the query (any lambda), with
     * subType restrictions kept as segments. */
    private static void collectChains(ValueSpecification n,
            List<List<Seg>> out) {
        List<Seg> chain = chainOf(n);
        if (chain != null) {
            if (!chain.isEmpty()) {
                out.add(chain);
            }
            return;
        }
        switch (n) {
            case AppliedFunction af -> af.parameters()
                    .forEach(p -> collectChains(p, out));
            case AppliedProperty ap -> collectChains(ap.receiver(), out);
            case LambdaFunction lf -> lf.body()
                    .forEach(b -> collectChains(b, out));
            case PureCollection pc -> pc.values()
                    .forEach(v -> collectChains(v, out));
            // graphFetch trees desugar to ColSpecs (function1 = x|$x.prop,
            // function2 = the lambda-wrapped nested sub-tree): chains
            // compose parent-first — product{name} contributes [product]
            // and [product, name]
            case com.legend.protocol.spec.ColSpecArray ca ->
                    collectTreeChains(ca, List.of(), out);
            case com.legend.protocol.spec.ColSpec cs -> collectTreeChains(
                    new com.legend.protocol.spec.ColSpecArray(List.of(cs)),
                    List.of(), out);
            default -> n.children().forEach(x -> collectChains(x, out));
        }
    }

    private static void collectTreeChains(com.legend.protocol.spec.ColSpecArray tree,
            List<Seg> parent, List<List<Seg>> out) {
        for (com.legend.protocol.spec.ColSpec cs : tree.colSpecs()) {
            List<Seg> hop = null;
            if (cs.function1() != null && cs.function1().body().size() == 1) {
                List<Seg> c = chainOf(cs.function1().body().get(0));
                if (c != null && !c.isEmpty()) {
                    hop = c;
                }
            }
            if (System.getenv("LL_LINEAGE_DEBUG") != null) {
                System.err.println("[treeChains] name=" + cs.name()
                        + " f1=" + (cs.function1() == null ? "null"
                                : cs.function1().body())
                        + " hop=" + hop);
            }
            if (hop == null) {
                continue;
            }
            List<Seg> full = new ArrayList<>(parent);
            full.addAll(hop);
            out.add(full);
            if (cs.function2() != null) {
                for (var b : cs.function2().body()) {
                    if (b instanceof com.legend.protocol.spec.ColSpecArray sub) {
                        collectTreeChains(sub, full, out);
                    }
                }
            }
        }
    }

    /** The segment list when {@code n} IS a var-rooted chain (possibly
     * with subType/toOne links); null when it is not a chain. */
    /** Comparison/boolean/arithmetic OPERATORS terminate a chain — they
     * are never qualified-property hops (equal($p.name, 'ok') was read as
     * a 'equal' hop, putting the scalar leaf in MID position); their
     * operand chains collect separately. */
    private static final Set<String> OPERATORS = Set.of(
            "equal", "lessThan", "lessThanEqual", "greaterThan",
            "greaterThanEqual", "plus", "minus", "times", "divide",
            "and", "or", "in", "startsWith", "endsWith");

    private static @com.legend.base.Nullable List<Seg> chainOf(ValueSpecification n) {
        if (n instanceof Variable) {
            return new ArrayList<>();
        }
        if (n instanceof AppliedProperty ap) {
            List<Seg> base = chainOf(ap.receiver());
            if (base == null) {
                return null;
            }
            base.add(new Seg.Prop(ap.property()));
            return base;
        }
        if (n instanceof AppliedFunction af
                && "subType".equals(simple(af.function()))
                && af.parameters().size() == 2) {
            List<Seg> base = chainOf(af.parameters().get(0));
            if (base == null) {
                return null;
            }
            base.add(new Seg.SubType(typeName(af.parameters().get(1))));
            return base;
        }
        if (n instanceof AppliedFunction af && af.parameters().size() == 1) {
            // single-arg calls over a chain (toOne/isEmpty/count/...):
            // transparent CONSUMERS — the chain itself is the demand
            return chainOf(af.parameters().get(0));
        }
        if (n instanceof AppliedFunction af && af.parameters().size() >= 2
                && !OPERATORS.contains(simple(af.function()))
                && af.parameters().stream().skip(1)
                        .noneMatch(a -> a instanceof LambdaFunction)
                && af.parameters().stream().skip(1)
                        .allMatch(a -> !carriesChain(a)
                                || isTemporalContextArg(a))) {
            // a call whose extras are chain-free non-lambdas over a chain
            // is a QUALIFIED PROPERTY hop (milestoned dates:
            // product($businessDate)) — an OPERATOR over chains
            // (firstName + lastName) is NOT: its operand chains collect
            // separately. A non-property still lands on the walk's loud
            // unmapped wall, never a silently wrong tree.
            List<Seg> base = chainOf(af.parameters().get(0));
            if (base == null) {
                return null;
            }
            base.add(new Seg.Prop(simple(af.function())));
            return base;
        }
        return null;
    }

    /** A qualified-hop argument that only reads GENERATED temporal
     * context ({@code $this.businessDate}) — the date is the query's
     * milestoning context, not data demand; the hop still parses. */
    private static boolean isTemporalContextArg(ValueSpecification n) {
        List<List<Seg>> probe = new ArrayList<>();
        collectChains(n, probe);
        return !probe.isEmpty() && probe.stream().allMatch(c ->
                !c.isEmpty()
                        && c.get(c.size() - 1) instanceof Seg.Prop pr
                        && com.legend.compiler.element.Temporal.isGeneratedDateName(pr.name()));
    }

    /** Whether the expression contains a NON-EMPTY var-rooted chain. */
    private static boolean carriesChain(ValueSpecification n) {
        List<List<Seg>> probe = new ArrayList<>();
        collectChains(n, probe);
        return !probe.isEmpty();
    }

    private static String typeName(ValueSpecification v) {
        if (v instanceof TypeAnnotation.Named named) {
            return switch (named.type()) {
                case com.legend.protocol.TypeExpression.NameRef nr -> nr.name();
                case com.legend.protocol.TypeExpression.Generic g -> g.name();
                default -> throw new NotImplementedException(
                        "scanRelations: structural subType annotation");
            };
        }
        if (v instanceof PackageableElementPtr p) {
            return p.fullPath();
        }
        throw new NotImplementedException(
                "scanRelations: subType argument "
                + v.getClass().getSimpleName());
    }

    private static String simple(String f) {
        return f.substring(f.lastIndexOf(':') + 1);
    }

    static Optional<ClassMapping.Relational> rootFor(ModelContext ctx,
            LegacyMappingDefinition md, String classFqn) {
        try {
            return Optional.of(classMappingFor(ctx, md, classFqn, null));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }
}
