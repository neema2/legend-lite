// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import com.legend.graphql.hardening.ReferenceModel.Cls;
import com.legend.graphql.hardening.ReferenceModel.DatedToOne;
import com.legend.graphql.hardening.ReferenceModel.Fld;
import com.legend.graphql.hardening.ReferenceModel.Kind;
import com.legend.graphql.hardening.ReferenceModel.Obj;
import com.legend.graphql.hardening.ReferenceModel.Scalar;
import com.legend.graphql.hardening.ReferenceModel.ToMany;
import com.legend.graphql.hardening.ReferenceModel.ToOne;
import com.legend.json.Json;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

/**
 * THE FUZZER (docs/GRAPHQL_STUDY_2026_10_18.md §13.7): seeded random documents over a fixture -- {@code where} trees
 * of every operator family and combinator, sort keys over scalars, derived properties and to-one paths, paging,
 * nested lists with their own clauses, nested aggregates -- each answered by lite through the product path and by the
 * {@link Reference}, a disagreement shrunk to a minimal document. The documents are a small tree, so shrinking is
 * structural: remove an entry, an item, a field, while the disagreement holds.
 */
public final class Fuzz {

    // ---- the document as a tree ----

    sealed interface GV permits GObj, GList, GLit {
        String render();
    }

    record GLit(String text) implements GV {
        public String render() {
            return text;
        }
    }

    record GList(List<GV> items) implements GV {
        public String render() {
            StringBuilder b = new StringBuilder("[");
            for (int i = 0; i < items.size(); i++) {
                b.append(i == 0 ? "" : ", ").append(items.get(i).render());
            }
            return b.append(']').toString();
        }
    }

    record GObj(List<Map.Entry<String, GV>> fields) implements GV {
        public String render() {
            StringBuilder b = new StringBuilder("{ ");
            for (int i = 0; i < fields.size(); i++) {
                b.append(i == 0 ? "" : ", ").append(fields.get(i).getKey()).append(": ")
                        .append(fields.get(i).getValue().render());
            }
            return b.append(" }").toString();
        }
    }

    /** A selection: {@code name} may carry an alias ({@code "a1: name"}) or be a fragment spread or an inline
     *  fragment ({@code "...F1"}, {@code "... on Customer"}); {@code suffix} is a directive ({@code @include(...)}). */
    record GSel(String name, GObj args, List<GSel> sub, String suffix) {
        GSel(String name, GObj args, List<GSel> sub) {
            this(name, args, sub, "");
        }

        String render() {
            StringBuilder b = new StringBuilder(name);
            if (args != null && !args.fields().isEmpty()) {
                String a = args.render();
                b.append('(').append(a, 2, a.length() - 2).append(')');
            }
            b.append(suffix);
            if (!sub.isEmpty()) {
                b.append(" {");
                for (GSel s : sub) {
                    b.append(' ').append(s.render());
                }
                b.append(" }");
            }
            return b.toString();
        }
    }

    /** A variable: its declared GraphQL type and its value as JSON. */
    record Var(String type, String json) {
    }

    /** A document: its root fields, the named fragments they spread (each rendered whole), and the variables its
     *  values may name ({@code $v0}); only the variables the text still uses are declared and sent (an unused
     *  variable is a validation error, which would make a shrunk document disagree for the wrong reason). */
    record GDoc(List<GSel> roots, List<String> fragments, Map<String, Var> vars) {
        GDoc(GSel root) {
            this(List.of(root), List.of(), Map.of());
        }

        GDoc(List<GSel> roots, List<String> fragments) {
            this(roots, fragments, Map.of());
        }

        private String body() {
            StringBuilder b = new StringBuilder("{");
            for (GSel r : roots) {
                b.append(' ').append(r.render());
            }
            return b.append(" }").toString();
        }

        private List<String> used() {
            StringBuilder text = new StringBuilder(body());
            for (String f : fragments) {
                text.append(' ').append(f);
            }
            List<String> out = new ArrayList<>();
            for (String name : vars.keySet()) {
                if (java.util.regex.Pattern.compile("\\$" + name + "\\b").matcher(text).find()) {
                    out.add(name);
                }
            }
            return out;
        }

        String render() {
            StringBuilder b = new StringBuilder();
            for (String f : fragments) {
                b.append(f).append(' ');
            }
            List<String> used = used();
            if (!used.isEmpty()) {
                b.append("query Q(");
                for (int i = 0; i < used.size(); i++) {
                    b.append(i == 0 ? "" : ", ").append('$').append(used.get(i)).append(": ")
                            .append(vars.get(used.get(i)).type());
                }
                b.append(") ");
            }
            return b.append(body()).toString();
        }

        /** The used variables' values as a JSON object, null when the document names none. */
        @com.legend.base.Nullable String variables() {
            List<String> used = used();
            if (used.isEmpty()) {
                return null;
            }
            StringBuilder b = new StringBuilder("{");
            for (int i = 0; i < used.size(); i++) {
                b.append(i == 0 ? "" : ",").append(Json.toCompact(used.get(i))).append(':')
                        .append(vars.get(used.get(i)).json());
            }
            return b.append('}').toString();
        }
    }

    // ---- generation ----

    final ReferenceModel model;
    final Random r;
    final Map<String, String> unique;           // class -> its unique sort key
    final Map<String, List<String>> enums;      // "Class.field" -> the enum's values
    final Map<String, String> enumTypes;        // "Class.field" -> the enum's GraphQL type name
    final Map<String, List<Object>> pool = new LinkedHashMap<>();

    public Fuzz(ReferenceModel model, long seed, Map<String, String> unique, Map<String, List<String>> enums,
            Map<String, String> enumTypes) {
        this.model = model;
        this.r = new Random(seed);
        this.unique = unique;
        this.enums = enums;
        this.enumTypes = enumTypes;
    }

    <T> T pick(List<T> xs) {
        return xs.get(r.nextInt(xs.size()));
    }

    boolean chance(double p) {
        return r.nextDouble() < p;
    }

    static Map.Entry<String, GV> e(String k, GV v) {
        return Map.entry(k, v);
    }

    static GObj obj(List<Map.Entry<String, GV>> fs) {
        return new GObj(new ArrayList<>(fs));
    }

    /** The scalar fields a where/orderBy may name (no parameters). */
    List<String> filterable(String cls) {
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, Fld> f : model.get(cls).fields.entrySet()) {
            // a temporal object's businessDate is its read's date, an output only (register S119)
            if (f.getValue() instanceof Scalar s && s.params().isEmpty()
                    && !(model.get(cls).temporal() && f.getKey().equals("businessDate"))) {
                out.add(f.getKey());
            }
        }
        return out;
    }

    /** The business dates a temporal read is asked at: version boundaries (a version holds from its first day, not
     *  on its last), the leap day's one-day version, gaps, before every version and after. */
    static final List<String> DATES = List.of("2019-06-01", "2020-06-01", "2023-06-15", "2023-12-31", "2024-01-01",
            "2024-02-15", "2024-02-29", "2024-03-01", "2025-01-01");

    /** A business date: a literal, now and then through a variable. */
    GV businessDate() {
        return maybeVar(new GLit("\"" + pick(DATES) + "\""), "Date!");
    }

    /** A root over a temporal class takes its business date (register S119). */
    GSel dated(String cls, GSel root) {
        if (!model.get(cls).temporal()) {
            return root;
        }
        List<Map.Entry<String, GV>> args = new ArrayList<>();
        args.add(e("businessDate", businessDate()));
        if (root.args() != null) {
            args.addAll(root.args().fields());
        }
        return new GSel(root.name(), obj(args), root.sub(), root.suffix());
    }

    /** The GraphQL type a position of {@code cls} has: {@code XInterface} where the class has subclasses (G8), so a
     *  fragment on the position names the interface (a fragment on {@code X} itself is the exact-type trap lite
     *  refuses by name). */
    String positionType(String cls) {
        return model.below(cls).isEmpty() ? model.get(cls).name() : model.get(cls).name() + "Interface";
    }

    List<String> toOnes(String cls) {
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, Fld> f : model.get(cls).fields.entrySet()) {
            if (f.getValue() instanceof ToOne) {
                out.add(f.getKey());
            }
        }
        return out;
    }

    List<String> toManys(String cls) {
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, Fld> f : model.get(cls).fields.entrySet()) {
            if (f.getValue() instanceof ToMany) {
                out.add(f.getKey());
            }
        }
        return out;
    }

    /** The values a field takes over the class's rows (the literals a condition draws from). */
    List<Object> values(String cls, String field) {
        return pool.computeIfAbsent(cls + "." + field, k -> {
            Scalar s = (Scalar) model.get(cls).fields.get(field);
            List<Object> out = new ArrayList<>();
            for (Obj o : model.get(cls).roots.get()) {
                Object v = s.get().apply(o, Map.of());
                if (v != null) {
                    out.add(v);
                }
            }
            return out;
        });
    }

    /** Named fragments the document being generated defines. */
    private final List<String> fragments = new ArrayList<>();
    /** The variables the document being generated declares. */
    private final Map<String, Var> vars = new LinkedHashMap<>();
    private int aliasCount;

    GDoc document() {
        fragments.clear();
        vars.clear();
        aliasCount = 0;
        List<GSel> roots = new ArrayList<>();
        int n = chance(0.15) ? 2 : 1;
        for (int i = 0; i < n; i++) {
            String cls = pick(new ArrayList<>(model.classes.keySet()));
            double x = r.nextDouble();
            GSel root = dated(cls, x < 0.12 ? rootAggregate(cls) : x < 0.24 ? groupsField(cls + "Groups", cls)
                    : listField(cls, cls, 0));
            // a second root field is aliased: two roots of one name would merge or conflict
            roots.add(i == 0 ? root : renamed(root, "r" + i + ": " + root.name()));
        }
        return new GDoc(roots, List.copyOf(fragments), new LinkedHashMap<>(vars));
    }

    /** A literal moved into a variable now and then ({@code $v3}, its type the position's): the same value through
     *  the variables' coercion instead of the literal's. */
    GV maybeVar(GV literal, String type) {
        if (!chance(0.12) || literal instanceof GLit l && l.text().equals("null")) {
            return literal;
        }
        String name = "v" + vars.size();
        vars.put(name, new Var(type, json(literal)));
        return new GLit("$" + name);
    }

    /** A value literal as JSON: an enum's name quoted, everything else as GraphQL spells it. */
    static String json(GV v) {
        return switch (v) {
            case GLit l -> l.text().matches("[A-Z_][A-Z_0-9]*") ? Json.toCompact(l.text()) : l.text();
            case GList l -> {
                StringBuilder b = new StringBuilder("[");
                for (int i = 0; i < l.items().size(); i++) {
                    b.append(i == 0 ? "" : ",").append(json(l.items().get(i)));
                }
                yield b.append(']').toString();
            }
            case GObj o -> throw new IllegalArgumentException("an input object is never a variable here");
        };
    }

    /** The GraphQL type a scalar field's filter values take. */
    String typeOf(String cls, String field, Kind kind) {
        return switch (kind) {
            case STRING -> "String";
            case BIGINT -> "BigInt";
            case FLOAT -> "Float";
            case DECIMAL -> "Decimal";
            case BOOLEAN -> "Boolean";
            case DATE -> "Date";
            case DATETIME -> "DateTime";
            case ENUM -> java.util.Objects.requireNonNull(enumTypes.get(model.get(cls).name() + "." + field),
                    "enum type of " + cls + "." + field);
        };
    }

    static GSel renamed(GSel s, String name) {
        return new GSel(name, s.args(), s.sub(), s.suffix());
    }

    /** {@code CustomerAggregate(where: W) { count sum { ... } ... }}. */
    GSel rootAggregate(String cls) {
        GSel nested = aggregateField(cls, cls);
        return new GSel(cls + "Aggregate", nested.args(), nested.sub());
    }

    /** {@code CustomerGroups(where, having, orderBy, limit, offset) { key { ... } count sum { ... } }} at the root,
     *  or {@code ordersGroups(...)} under a parent (the relation node, register S118): keys over the class's
     *  scalars and one to-one hop, a date key truncated now and then. */
    GSel groupsField(String name, String cls) {
        List<GSel> keys = new ArrayList<>();
        List<List<String>> keyPaths = new ArrayList<>();
        List<String> scalars = new ArrayList<>();
        for (String f : filterable(cls)) {
            Kind k = ((Scalar) model.get(cls).fields.get(f)).kind();
            if (k != Kind.FLOAT && k != Kind.DECIMAL) {
                scalars.add(f);
            }
        }
        if (scalars.isEmpty()) {
            scalars.addAll(filterable(cls));
        }
        int nk = 1 + r.nextInt(2);
        Set<String> used = new LinkedHashSet<>();
        for (int i = 0; i < nk; i++) {
            List<String> ones = toOnes(cls);
            if (!ones.isEmpty() && chance(0.3)) {
                String f = pick(ones);
                String target = ((ToOne) model.get(cls).fields.get(f)).target();
                List<String> ts = new ArrayList<>();
                for (String g : filterable(target)) {
                    Kind k = ((Scalar) model.get(target).fields.get(g)).kind();
                    if (k == Kind.ENUM || k == Kind.STRING || k == Kind.BOOLEAN) {
                        ts.add(g);
                    }
                }
                if (!ts.isEmpty() && used.add(f)) {
                    String g = pick(ts);
                    keys.add(new GSel(f, null, List.of(new GSel(g, null, List.of()))));
                    keyPaths.add(List.of(f, g));
                }
                continue;
            }
            String f = pick(scalars);
            if (!used.add(f)) {
                continue;
            }
            Kind k = ((Scalar) model.get(cls).fields.get(f)).kind();
            GObj args = (k == Kind.DATE || k == Kind.DATETIME) && chance(0.5)
                    ? obj(List.of(e("truncate", new GLit(pick(List.of("YEAR", "QUARTER", "MONTH", "DAY"))))))
                    : null;
            keys.add(new GSel(f, args, List.of()));
            keyPaths.add(List.of(f));
        }
        if (chance(0.1)) {
            keys.add(r.nextInt(keys.size() + 1), new GSel("__typename", null, List.of()));
        }
        List<GSel> sel = new ArrayList<>();
        GSel agg = aggregateField(cls, cls);
        sel.addAll(agg.sub());
        // the key anywhere among the members: a group answers in the order asked
        sel.add(r.nextInt(sel.size() + 1), new GSel("key", null, keys));
        List<Map.Entry<String, GV>> args = new ArrayList<>();
        if (chance(0.5)) {
            args.add(e("where", where(cls, 2)));
        }
        List<Map.Entry<String, GV>> having = new ArrayList<>();
        if (chance(0.3)) {
            GObj bound = obj(List.of(e(pick(List.of("gte", "lt", "equals")), new GLit(Integer.toString(r.nextInt(4))))));
            having.add(e("count", bound));
        }
        List<String[]> folds = foldable(cls);
        if (!folds.isEmpty() && chance(0.2)) {
            // a fold's bound: sum, min or max of a property (the fold need not be selected: S106)
            String[] fp = pick(folds);
            Kind k = ((Scalar) model.get(cls).fields.get(fp[1])).kind();
            GObj bound = obj(List.of(e(pick(List.of("gt", "lt", "gte", "lte", "equals")), literal(cls, fp[1], k))));
            having.add(e(fp[0], obj(List.of(e(fp[1], bound)))));
        }
        if (!having.isEmpty()) {
            args.add(e("having", obj(having)));
        }
        // a total order over the groups: every selected key, the count or a fold first now and then
        List<GV> order = new ArrayList<>();
        if (chance(0.3)) {
            order.add(obj(List.of(e("count", new GLit(chance(0.5) ? "ASC" : "DESC")))));
        }
        if (!folds.isEmpty() && chance(0.2)) {
            String[] fp = pick(folds);
            order.add(obj(List.of(e(fp[0], obj(List.of(e(fp[1], new GLit(chance(0.5) ? "ASC" : "DESC"))))))));
        }
        for (List<String> kp : keyPaths) {
            GV dir = new GLit(chance(0.5) ? "ASC" : "DESC");
            for (int i = kp.size() - 1; i >= 0; i--) {
                dir = obj(List.of(e(kp.get(i), dir)));
            }
            order.add(obj(List.of(e("key", dir))));
        }
        args.add(e("orderBy", new GList(order)));
        if (chance(0.25)) {
            args.add(e("limit", new GLit(Integer.toString(r.nextInt(4)))));
        }
        return new GSel(name, obj(args), sel);
    }

    /** The (fold, property) pairs a group's {@code having} and {@code orderBy} may name with a literal of the
     *  property's own kind: sum over numbers, min and max over numbers, dates and strings (avg answers a Float). */
    List<String[]> foldable(String cls) {
        List<String[]> out = new ArrayList<>();
        for (String f : filterable(cls)) {
            Kind k = ((Scalar) model.get(cls).fields.get(f)).kind();
            if (k == Kind.BIGINT || k == Kind.FLOAT || k == Kind.DECIMAL) {
                out.add(new String[] {"sum", f});
                out.add(new String[] {"min", f});
                out.add(new String[] {"max", f});
            } else if (k == Kind.DATE || k == Kind.DATETIME || k == Kind.STRING) {
                out.add(new String[] {"min", f});
                out.add(new String[] {"max", f});
            }
        }
        return out;
    }

    /** A list field ({@code Customer(...)} at the root, {@code orders(...)} below) with clauses and a selection. */
    GSel listField(String name, String cls, int depth) {
        List<Map.Entry<String, GV>> args = new ArrayList<>();
        if (chance(0.75)) {
            args.add(e("where", where(cls, depth == 0 ? 3 : 2)));
        }
        boolean paged = chance(0.3);
        List<GV> keys = new ArrayList<>();
        if (paged || chance(0.6)) {
            int n = r.nextInt(3);
            for (int i = 0; i < n; i++) {
                keys.add(sortKey(cls, 0));
            }
            if (paged || chance(0.5)) {
                keys.add(obj(List.of(e(unique.get(cls), new GLit(chance(0.5) ? "ASC" : "DESC")))));
            }
        }
        if (!keys.isEmpty()) {
            args.add(e("orderBy", new GList(keys)));
        }
        if (paged) {
            if (chance(0.8)) {
                args.add(e("limit", maybeVar(new GLit(Integer.toString(r.nextInt(5))), "Int")));
            }
            if (chance(0.5)) {
                args.add(e("offset", maybeVar(new GLit(Integer.toString(r.nextInt(4))), "Int")));
            }
        }
        return new GSel(name, obj(args), selection(cls, depth));
    }

    List<GSel> selection(String cls, int depth) {
        List<GSel> out = new ArrayList<>();
        out.add(new GSel(unique.get(cls), null, List.of()));
        Cls c = model.get(cls);
        List<String> names = new ArrayList<>(c.fields.keySet());
        int n = 1 + r.nextInt(4);
        Set<String> used = new LinkedHashSet<>();
        used.add(unique.get(cls));
        for (int i = 0; i < n; i++) {
            String f = pick(names);
            if (!used.add(f)) {
                continue;
            }
            switch (c.fields.get(f)) {
                case Scalar s -> {
                    if (s.params().isEmpty()) {
                        out.add(new GSel(f, null, List.of()));
                    } else {
                        List<Map.Entry<String, GV>> a = new ArrayList<>();
                        for (Map.Entry<String, Kind> p : s.params().entrySet()) {
                            a.add(e(p.getKey(), maybeVar(new GLit(Long.toString(r.nextInt(7) - 2)), "BigInt!")));
                        }
                        out.add(new GSel(f, obj(a), List.of()));
                    }
                }
                case ToOne t -> {
                    if (depth < 2) {
                        out.add(new GSel(f, null, selection(t.target(), depth + 1)));
                    }
                }
                case DatedToOne t -> {
                    if (depth < 2) {
                        // a temporal target's version as of a date; now and then the same field twice, at two dates
                        out.add(new GSel(f, obj(List.of(e("businessDate", businessDate()))),
                                selection(t.target(), depth + 1)));
                        if (chance(0.2)) {
                            out.add(new GSel("d" + aliasCount++ + ": " + f, obj(List.of(e("businessDate",
                                    businessDate()))), selection(t.target(), depth + 1)));
                        }
                    }
                }
                case ToMany t -> {
                    if (depth < 2) {
                        double y = r.nextDouble();
                        if (y < 0.25) {
                            out.add(aggregateField(f, t.target()));
                        } else if (y < 0.4) {
                            out.add(groupsField(f + "Groups", t.target()));
                        } else {
                            out.add(listField(f, t.target(), depth + 1));
                        }
                    }
                }
            }
        }
        List<String> subs = model.below(cls);
        if (chance(subs.isEmpty() ? 0.1 : 0.4)) {
            out.add(new GSel("__typename", null, List.of()));
        }
        // SUBTYPE FRAGMENTS: a subclass's own fields under `... on Sub` (a hierarchy's position)
        if (!subs.isEmpty() && chance(0.5)) {
            String sub = pick(subs);
            List<String> own = new ArrayList<>(filterable(sub));
            own.removeAll(filterable(cls));
            if (!own.isEmpty()) {
                List<GSel> fs = new ArrayList<>();
                fs.add(new GSel(pick(own), null, List.of()));
                if (own.size() > 1 && chance(0.5)) {
                    String g = pick(own);
                    if (!g.equals(fs.get(0).name())) {
                        fs.add(new GSel(g, null, List.of()));
                    }
                }
                out.add(new GSel("... on " + positionType(sub), null, fs));
            }
        }
        // ALIASES: a scalar under another key, and a to-many list twice under two keys with its own clauses
        if (chance(0.15)) {
            List<String> fs = filterable(cls);
            out.add(new GSel("a" + aliasCount++ + ": " + pick(fs), null, List.of()));
        }
        if (depth < 2 && chance(0.1) && !toManys(cls).isEmpty()) {
            String f = pick(toManys(cls));
            GSel twin = listField(f, ((ToMany) c.fields.get(f)).target(), depth + 1);
            out.add(renamed(twin, "b" + aliasCount++ + ": " + f));
        }
        // DIRECTIVES: a field kept or dropped by @include/@skip
        if (chance(0.1)) {
            boolean v = chance(0.5);
            String d = chance(0.5) ? " @include(if: " + v + ")" : " @skip(if: " + v + ")";
            out.add(new GSel("d" + aliasCount++ + ": " + pick(filterable(cls)), null, List.of(), d));
        }
        // FRAGMENTS: part of the selection moved into an inline fragment or a named one (same type)
        if (out.size() > 2 && chance(0.15)) {
            int cut = 1 + r.nextInt(out.size() - 1);
            List<GSel> moved = new ArrayList<>(out.subList(cut, out.size()));
            List<GSel> kept = new ArrayList<>(out.subList(0, cut));
            String type = positionType(cls);
            if (chance(0.5)) {
                kept.add(new GSel("... on " + type, null, moved));
            } else {
                String name = "F" + aliasCount++;
                StringBuilder f = new StringBuilder("fragment ").append(name).append(" on ").append(type).append(" {");
                for (GSel m : moved) {
                    f.append(' ').append(m.render());
                }
                fragments.add(f.append(" }").toString());
                kept.add(new GSel("..." + name, null, List.of()));
            }
            return kept;
        }
        return out;
    }

    GSel aggregateField(String toMany, String target) {
        List<GSel> members = new ArrayList<>();
        members.add(new GSel(chance(0.1) ? "n" + aliasCount++ + ": count" : "count", null, List.of()));
        List<String> numeric = new ArrayList<>();
        List<String> ordered = new ArrayList<>();
        for (String f : filterable(target)) {
            Kind k = ((Scalar) model.get(target).fields.get(f)).kind();
            if (k == Kind.BIGINT || k == Kind.FLOAT || k == Kind.DECIMAL) {
                numeric.add(f);
                ordered.add(f);
            } else if (k == Kind.DATE || k == Kind.DATETIME || k == Kind.STRING) {
                ordered.add(f);
            }
        }
        for (String fold : List.of("sum", "avg", "min", "max")) {
            List<String> props = fold.equals("sum") || fold.equals("avg") ? numeric : ordered;
            if (!props.isEmpty() && chance(0.35)) {
                List<GSel> fs = new ArrayList<>();
                int leaves = 1 + (chance(0.3) ? 1 : 0) + (chance(0.1) ? 1 : 0);
                Set<String> seen = new LinkedHashSet<>();
                for (int i = 0; i < leaves; i++) {
                    String leaf = pick(props);
                    if (seen.add(leaf)) {
                        fs.add(new GSel(chance(0.1) ? "l" + aliasCount++ + ": " + leaf : leaf, null, List.of()));
                    } else if (chance(0.5)) {
                        // one leaf under a second key
                        fs.add(new GSel("l" + aliasCount++ + ": " + leaf, null, List.of()));
                    }
                }
                if (chance(0.1)) {
                    fs.add(r.nextInt(fs.size() + 1), new GSel("__typename", null, List.of()));
                }
                members.add(new GSel(chance(0.1) ? "f" + aliasCount++ + ": " + fold : fold, null, fs));
                if (chance(0.05)) {
                    // the same fold twice, under two keys, over different leaves
                    members.add(new GSel("g" + aliasCount++ + ": " + fold, null,
                            List.of(new GSel(pick(props), null, List.of()))));
                }
            }
        }
        if (chance(0.1)) {
            members.add(r.nextInt(members.size() + 1), new GSel("__typename", null, List.of()));
        }
        GObj args = chance(0.4) ? obj(List.of(e("where", where(target, 1)))) : null;
        return new GSel(toMany + "Aggregate", args, members);
    }

    GObj where(String cls, int depth) {
        List<Map.Entry<String, GV>> fs = new ArrayList<>();
        int n = 1 + (chance(0.3) ? 1 : 0);
        Set<String> used = new LinkedHashSet<>();
        for (int i = 0; i < n; i++) {
            Map.Entry<String, GV> c = condition(cls, depth);
            if (used.add(c.getKey())) {     // an input object names each field once
                fs.add(c);
            }
        }
        return obj(fs);
    }

    Map.Entry<String, GV> condition(String cls, int depth) {
        double x = r.nextDouble();
        if (depth > 0 && x < 0.08) {
            List<GV> items = new ArrayList<>();
            int n = r.nextInt(3);
            for (int i = 0; i < n; i++) {
                items.add(where(cls, depth - 1));
            }
            return e("AND", new GList(items));
        }
        if (depth > 0 && x < 0.18) {
            List<GV> items = new ArrayList<>();
            int n = 1 + r.nextInt(2);
            for (int i = 0; i < n; i++) {
                items.add(where(cls, depth - 1));
            }
            return e("OR", new GList(items));
        }
        if (depth > 0 && x < 0.26) {
            return e("NOT", where(cls, depth - 1));
        }
        List<String> ones = toOnes(cls);
        List<String> manys = toManys(cls);
        List<String> subs = model.below(cls);
        if (!subs.isEmpty() && chance(0.25)) {
            // a subclass's condition (`asCardPayment: { last4: ... }`), or membership (`asX: { isNot: null }`)
            String sub = pick(subs);
            String f = "as" + model.get(sub).name();
            if (chance(0.3)) {
                return e(f, obj(List.of(e(chance(0.5) ? "is" : "isNot", new GLit("null")))));
            }
            return e(f, where(sub, Math.max(0, depth - 1)));
        }
        if (depth > 0 && !ones.isEmpty() && x < 0.38) {
            String f = pick(ones);
            String target = ((ToOne) model.get(cls).fields.get(f)).target();
            double y = r.nextDouble();
            if (y < 0.2) {
                return e(f, obj(List.of(e(chance(0.5) ? "is" : "isNot", new GLit("null")))));
            }
            if (y < 0.35) {
                return e(f, obj(List.of(e(chance(0.5) ? "is" : "isNot", where(target, depth - 1)))));
            }
            return e(f, where(target, depth - 1));
        }
        if (depth > 0 && !manys.isEmpty() && x < 0.48) {
            String f = pick(manys);
            String target = ((ToMany) model.get(cls).fields.get(f)).target();
            return e(f, obj(List.of(e(pick(List.of("some", "every", "none")), where(target, depth - 1)))));
        }
        String f = pick(filterable(cls));
        return e(f, scalarFilter(cls, f, true));
    }

    GObj scalarFilter(String cls, String field, boolean allowNot) {
        Kind kind = ((Scalar) model.get(cls).fields.get(field)).kind();
        List<String> ops = switch (kind) {
            case STRING -> List.of("equals", "not", "in", "notIn", "lt", "lte", "gt", "gte", "contains", "startsWith",
                    "endsWith");
            case BIGINT, FLOAT, DECIMAL, DATE, DATETIME -> List.of("equals", "not", "in", "notIn", "lt", "lte", "gt",
                    "gte");
            case BOOLEAN -> List.of("equals", "not");
            case ENUM -> List.of("equals", "not", "in", "notIn");
        };
        List<Map.Entry<String, GV>> fs = new ArrayList<>();
        int n = 1 + (chance(0.25) ? 1 : 0);
        for (int i = 0; i < n; i++) {
            String op = pick(ops);
            if (op.equals("not") && !allowNot) {
                op = "equals";
            }
            String t = typeOf(cls, field, kind);
            GV v = switch (op) {
                case "equals" -> chance(0.1) ? new GLit("null") : maybeVar(literal(cls, field, kind), t);
                case "not" -> chance(0.15) ? new GLit("null") : scalarFilter(cls, field, false);
                case "in", "notIn" -> {
                    List<GV> items = new ArrayList<>();
                    int m = r.nextInt(4);
                    for (int k = 0; k < m; k++) {
                        items.add(literal(cls, field, kind));
                    }
                    yield maybeVar(new GList(items), "[" + t + "!]");
                }
                case "contains", "startsWith", "endsWith" -> maybeVar(fragmentLiteral(cls, field, op), "String");
                default -> maybeVar(literal(cls, field, kind), t);
            };
            String key = op;
            if (fs.stream().anyMatch(en -> en.getKey().equals(key))) {
                continue;
            }
            fs.add(e(op, v));
        }
        if (kind == Kind.STRING && chance(0.3)) {
            fs.add(e("mode", new GLit(chance(0.8) ? "insensitive" : "default")));
        }
        return obj(fs);
    }

    static final List<String> SPECIAL = List.of("%", "_", "'", "é", "É", "a", "A", " ", "@", "0");

    GV fragmentLiteral(String cls, String field, String op) {
        List<Object> vs = values(cls, field);
        if (vs.isEmpty() || chance(0.2)) {
            return str(pick(SPECIAL));
        }
        String s = (String) pick(vs);
        if (s.isEmpty()) {
            return str("");
        }
        int a = r.nextInt(s.length());
        int b = a + 1 + r.nextInt(s.length() - a);
        String sub = switch (op) {
            case "startsWith" -> s.substring(0, b);
            case "endsWith" -> s.substring(a);
            default -> s.substring(a, b);
        };
        if (chance(0.3)) {
            sub = chance(0.5) ? sub.toUpperCase(Locale.ROOT) : sub.toLowerCase(Locale.ROOT);
        }
        return str(sub);
    }

    GV literal(String cls, String field, Kind kind) {
        if (kind == Kind.ENUM) {
            return new GLit(pick(enums.get(model.get(cls).name() + "." + field)));
        }
        if (kind == Kind.BOOLEAN) {
            return new GLit(chance(0.5) ? "true" : "false");
        }
        List<Object> vs = values(cls, field);
        Object v = vs.isEmpty() ? null : pick(vs);
        switch (kind) {
            case STRING -> {
                String s = v == null ? pick(SPECIAL) : (String) v;
                if (chance(0.2)) {
                    s = chance(0.5) ? s.toUpperCase(Locale.ROOT) : s.toLowerCase(Locale.ROOT);
                } else if (chance(0.1)) {
                    s = pick(SPECIAL);
                }
                return str(s);
            }
            case BIGINT -> {
                long x = v == null ? 0 : ((Number) v).longValue();
                if (chance(0.3)) {
                    x += r.nextInt(3) - 1;
                }
                return new GLit(Long.toString(x));
            }
            case FLOAT -> {
                double x = v == null ? 0.0 : ((Number) v).doubleValue();
                if (chance(0.3)) {
                    x += chance(0.5) ? 0.5 : -0.5;
                }
                String t = BigDecimal.valueOf(x).stripTrailingZeros().toPlainString();
                return new GLit(t.contains(".") ? t : t + ".0");
            }
            case DECIMAL -> {
                BigDecimal x = v == null ? BigDecimal.ZERO : (BigDecimal) v;
                if (chance(0.3)) {
                    x = x.add(new BigDecimal(chance(0.5) ? "0.01" : "-1"));
                }
                String t = x.stripTrailingZeros().toPlainString();
                return new GLit(t.contains(".") ? t : t + ".0");
            }
            case DATE -> {
                LocalDate d = v == null ? LocalDate.parse("2024-01-01") : (LocalDate) v;
                if (chance(0.3)) {
                    d = d.plusDays(r.nextInt(3) - 1);
                }
                return str(d.toString());
            }
            case DATETIME -> {
                LocalDateTime d = v == null ? LocalDateTime.parse("2024-01-01T00:00:00") : (LocalDateTime) v;
                if (chance(0.3)) {
                    d = d.plusSeconds(r.nextInt(3) - 1);
                }
                return str(d.toString().length() == 16 ? d + ":00" : d.toString());
            }
            default -> throw new IllegalStateException(kind.toString());
        }
    }

    static GLit str(String s) {
        return new GLit(Json.toCompact(s));
    }

    GV sortKey(String cls, int depth) {
        List<String> manys = toManys(cls);
        if (!manys.isEmpty() && chance(0.12)) {
            // a to-many property's aggregate: its count, or a fold of one of its properties
            String f = pick(manys);
            String target = ((ToMany) model.get(cls).fields.get(f)).target();
            GLit dir = new GLit(chance(0.5) ? "ASC" : "DESC");
            List<String> numeric = new ArrayList<>();
            List<String> ordered = new ArrayList<>();
            for (String p : filterable(target)) {
                Kind k = ((Scalar) model.get(target).fields.get(p)).kind();
                if (k == Kind.BIGINT || k == Kind.FLOAT || k == Kind.DECIMAL) {
                    numeric.add(p);
                    ordered.add(p);
                } else if (k == Kind.DATE || k == Kind.DATETIME || k == Kind.STRING) {
                    ordered.add(p);
                }
            }
            String fold = pick(List.of("count", "sum", "avg", "min", "max"));
            List<String> props = fold.equals("sum") || fold.equals("avg") ? numeric : ordered;
            if (fold.equals("count") || props.isEmpty()) {
                return obj(List.of(e(f + "Aggregate", obj(List.of(e("count", dir))))));
            }
            return obj(List.of(e(f + "Aggregate", obj(List.of(e(fold, obj(List.of(e(pick(props), dir)))))))));
        }
        List<String> subs = model.below(cls);
        if (!subs.isEmpty() && chance(0.2)) {
            // a subclass's key (`{ asCardPayment: { last4: ASC } }`): absent on the other classes' rows
            String sub = pick(subs);
            List<String> own = new ArrayList<>(filterable(sub));
            own.removeAll(filterable(cls));
            if (!own.isEmpty()) {
                return obj(List.of(e("as" + model.get(sub).name(),
                        obj(List.of(e(pick(own), new GLit(chance(0.5) ? "ASC" : "DESC")))))));
            }
        }
        List<String> ones = toOnes(cls);
        if ((depth < 2 || depth < 3 && chance(0.3)) && !ones.isEmpty() && chance(0.25)) {
            String f = pick(ones);
            String target = ((ToOne) model.get(cls).fields.get(f)).target();
            return obj(List.of(e(f, sortKey(target, depth + 1))));
        }
        return obj(List.of(e(pick(filterable(cls)), new GLit(chance(0.5) ? "ASC" : "DESC"))));
    }

    // ---- shrinking ----

    /** Every document one structural step smaller than {@code d}. */
    static List<GDoc> smaller(GDoc d) {
        List<GDoc> out = new ArrayList<>();
        for (int i = 0; i < d.roots().size(); i++) {
            if (d.roots().size() > 1) {
                List<GSel> rs = new ArrayList<>(d.roots());
                rs.remove(i);
                out.add(new GDoc(rs, used(rs, d.fragments()), d.vars()));
            }
            for (GSel s : smaller(d.roots().get(i), true)) {
                List<GSel> rs = new ArrayList<>(d.roots());
                rs.set(i, s);
                out.add(new GDoc(rs, used(rs, d.fragments()), d.vars()));
            }
        }
        return out;
    }

    /** The fragments the roots still spread, directly or through another kept fragment (an unused fragment is
     *  a validation error, which would make a shrunk document disagree for the wrong reason). */
    static List<String> used(List<GSel> roots, List<String> fragments) {
        StringBuilder text = new StringBuilder();
        for (GSel r : roots) {
            text.append(r.render()).append(' ');
        }
        List<String> kept = new ArrayList<>();
        boolean grew = true;
        while (grew) {
            grew = false;
            for (String f : fragments) {
                String name = f.substring("fragment ".length(), f.indexOf(" on "));
                if (!kept.contains(f)
                        && java.util.regex.Pattern.compile("\\.\\.\\." + name + "\\b").matcher(text).find()) {
                    kept.add(f);
                    text.append(f).append(' ');
                    grew = true;
                }
            }
        }
        List<String> ordered = new ArrayList<>();
        for (String f : fragments) {
            if (kept.contains(f)) {
                ordered.add(f);
            }
        }
        return ordered;
    }

    static List<GSel> smaller(GSel s, boolean root) {
        List<GSel> out = new ArrayList<>();
        if (!s.suffix().isEmpty()) {
            out.add(new GSel(s.name(), s.args(), s.sub()));
        }
        if (s.args() != null) {
            for (GV a : smallerV(s.args())) {
                out.add(new GSel(s.name(), (GObj) a, s.sub(), s.suffix()));
            }
        }
        for (int i = 0; i < s.sub().size(); i++) {
            if (s.sub().size() > 1) {
                List<GSel> sub = new ArrayList<>(s.sub());
                sub.remove(i);
                out.add(new GSel(s.name(), s.args(), sub, s.suffix()));
            }
            for (GSel c : smaller(s.sub().get(i), false)) {
                List<GSel> sub = new ArrayList<>(s.sub());
                sub.set(i, c);
                out.add(new GSel(s.name(), s.args(), sub, s.suffix()));
            }
        }
        return out;
    }

    static List<GV> smallerV(GV v) {
        List<GV> out = new ArrayList<>();
        switch (v) {
            case GObj o -> {
                for (int i = 0; i < o.fields().size(); i++) {
                    List<Map.Entry<String, GV>> fs = new ArrayList<>(o.fields());
                    fs.remove(i);
                    out.add(new GObj(fs));
                    for (GV c : smallerV(o.fields().get(i).getValue())) {
                        List<Map.Entry<String, GV>> gs = new ArrayList<>(o.fields());
                        gs.set(i, Map.entry(o.fields().get(i).getKey(), c));
                        out.add(new GObj(gs));
                    }
                }
            }
            case GList l -> {
                for (int i = 0; i < l.items().size(); i++) {
                    List<GV> is = new ArrayList<>(l.items());
                    is.remove(i);
                    out.add(new GList(is));
                    for (GV c : smallerV(l.items().get(i))) {
                        List<GV> js = new ArrayList<>(l.items());
                        js.set(i, c);
                        out.add(new GList(js));
                    }
                }
            }
            case GLit x -> { }
        }
        return out;
    }

    /** The verdict of one document: null when lite and the reference agree. */
    public static String verdict(Harness h, ReferenceModel model, String doc) {
        return verdict(h, model, doc, null);
    }

    public static String verdict(Harness h, ReferenceModel model, GDoc d) {
        return verdict(h, model, d.render(), d.variables());
    }

    static String verdict(Harness h, ReferenceModel model, String doc, @com.legend.base.Nullable String vars) {
        Json.Obj variables = vars == null ? null : (Json.Obj) Json.parse(vars, Harness.JSON);
        OVal expected;
        boolean refused = false;
        try {
            expected = Reference.answer(model, doc, variables, null);
        } catch (Reference.Undetermined u) {
            return null;    // the contract fixes no answer: nothing to judge
        } catch (Reference.Refused u) {
            expected = null;
            refused = true;
        } catch (RuntimeException e) {
            return "REFERENCE: " + e;
        }
        Json.Obj a;
        try {
            a = h.query(doc, vars, null);
        } catch (RuntimeException e) {
            return "THROWN: " + e;
        }
        if (refused) {
            return a.fields().get("errors") instanceof Json.Arr errs && !errs.items().isEmpty()
                    && !(a.fields().get("data") instanceof Json.Obj) ? null
                    : "ACCEPTED what the contract refuses: " + Json.toCompact(a);
        }
        if (!(a.fields().get("data") instanceof Json.Obj data)) {
            Json.Node errors = a.fields().get("errors");
            return "ERROR: " + (errors == null ? Json.toCompact(a) : Json.toCompact(errors));
        }
        return OVal.diff(expected, data, "data");
    }

    /** {@code d} shrunk while it still disagrees the same way (its failure's signature: a step that turns one bug
     *  into another, a validation error say, is not taken); at most {@code budget} runs. */
    static GDoc shrink(Harness h, ReferenceModel model, GDoc d, int budget) {
        GDoc best = d;
        String first = verdict(h, model, d);
        String sig = first == null ? null : signature(first);
        boolean progress = true;
        while (progress && budget > 0) {
            progress = false;
            for (GDoc c : smaller(best)) {
                if (--budget <= 0) {
                    break;
                }
                String v = verdict(h, model, c);
                if (v != null && (sig == null || sig.equals(signature(v)))) {
                    best = c;
                    progress = true;
                    break;
                }
            }
        }
        return best;
    }

    /** A failure's cluster: its message with the numbers and quoted text replaced, so one bug counts once. */
    static String signature(String verdict) {
        String s = verdict;
        if (s.startsWith("ERROR: ")) {
            // the messages, their quoted names and numbers blurred: one bug, one cluster
            StringBuilder m = new StringBuilder("ERROR: ");
            try {
                for (Json.Node e : ((Json.Arr) Json.parse(s.substring(7))).items()) {
                    m.append(((Json.Obj) e).getString("message")).append(" | ");
                }
                s = m.toString();
            } catch (RuntimeException ignored) {
                // not the envelope's errors: keep the text
            }
            return s.replaceAll("'[^']*'", "'…'").replaceAll("-?[0-9]+(\\.[0-9]+)?", "N");
        }
        s = s.replaceAll("\"[^\"]*\"", "\"…\"").replaceAll("-?[0-9]+(\\.[0-9]+)?", "N");
        int cut = s.indexOf(", got");
        return (cut > 0 ? s.substring(0, cut) : s).replaceAll("data(\\.[A-Za-z_]+|\\[N\\.\\.N\\))*", "data…");
    }

    static Function<String, String> renderer() {
        return s -> s;
    }
}
