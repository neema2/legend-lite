// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import com.legend.graphql.hardening.ReferenceModel.Cls;
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

    record GSel(String name, GObj args, List<GSel> sub) {
        String render() {
            StringBuilder b = new StringBuilder(name);
            if (args != null && !args.fields().isEmpty()) {
                String a = args.render();
                b.append('(').append(a, 2, a.length() - 2).append(')');
            }
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

    record GDoc(GSel root) {
        String render() {
            return "{ " + root.render() + " }";
        }
    }

    // ---- generation ----

    final ReferenceModel model;
    final Random r;
    final Map<String, String> unique;           // class -> its unique sort key
    final Map<String, List<String>> enums;      // "Class.field" -> the enum's values
    final Map<String, List<Object>> pool = new LinkedHashMap<>();

    public Fuzz(ReferenceModel model, long seed, Map<String, String> unique, Map<String, List<String>> enums) {
        this.model = model;
        this.r = new Random(seed);
        this.unique = unique;
        this.enums = enums;
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
            if (f.getValue() instanceof Scalar s && s.params().isEmpty()) {
                out.add(f.getKey());
            }
        }
        return out;
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

    GDoc document() {
        String cls = pick(new ArrayList<>(model.classes.keySet()));
        return new GDoc(listField(cls, cls, 0));
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
                args.add(e("limit", new GLit(Integer.toString(r.nextInt(5)))));
            }
            if (chance(0.5)) {
                args.add(e("offset", new GLit(Integer.toString(r.nextInt(4)))));
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
                            a.add(e(p.getKey(), new GLit(Long.toString(r.nextInt(7) - 2))));
                        }
                        out.add(new GSel(f, obj(a), List.of()));
                    }
                }
                case ToOne t -> {
                    if (depth < 2) {
                        out.add(new GSel(f, null, selection(t.target(), depth + 1)));
                    }
                }
                case ToMany t -> {
                    if (depth < 2) {
                        if (chance(0.3)) {
                            out.add(aggregateField(f, t.target()));
                        } else {
                            out.add(listField(f, t.target(), depth + 1));
                        }
                    }
                }
            }
        }
        if (chance(0.1)) {
            out.add(new GSel("__typename", null, List.of()));
        }
        return out;
    }

    GSel aggregateField(String toMany, String target) {
        List<GSel> members = new ArrayList<>();
        members.add(new GSel("count", null, List.of()));
        List<String> numeric = new ArrayList<>();
        List<String> ordered = new ArrayList<>();
        for (String f : filterable(target)) {
            Kind k = ((Scalar) model.get(target).fields.get(f)).kind();
            if (k == Kind.BIGINT || k == Kind.FLOAT || k == Kind.DECIMAL) {
                numeric.add(f);
                ordered.add(f);
            } else if (k == Kind.DATE || k == Kind.DATETIME) {
                ordered.add(f);
            }
        }
        for (String fold : List.of("sum", "avg", "min", "max")) {
            List<String> props = fold.equals("sum") || fold.equals("avg") ? numeric : ordered;
            if (!props.isEmpty() && chance(0.35)) {
                members.add(new GSel(fold, null, List.of(new GSel(pick(props), null, List.of()))));
            }
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
            GV v = switch (op) {
                case "equals" -> chance(0.1) ? new GLit("null") : literal(cls, field, kind);
                case "not" -> chance(0.15) ? new GLit("null") : scalarFilter(cls, field, false);
                case "in", "notIn" -> {
                    List<GV> items = new ArrayList<>();
                    int m = r.nextInt(4);
                    for (int k = 0; k < m; k++) {
                        items.add(literal(cls, field, kind));
                    }
                    yield new GList(items);
                }
                case "contains", "startsWith", "endsWith" -> fragmentLiteral(cls, field, op);
                default -> literal(cls, field, kind);
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
        List<String> ones = toOnes(cls);
        if (depth < 2 && !ones.isEmpty() && chance(0.25)) {
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
        for (GSel s : smaller(d.root(), true)) {
            out.add(new GDoc(s));
        }
        return out;
    }

    static List<GSel> smaller(GSel s, boolean root) {
        List<GSel> out = new ArrayList<>();
        if (s.args() != null) {
            for (GV a : smallerV(s.args())) {
                out.add(new GSel(s.name(), (GObj) a, s.sub()));
            }
        }
        for (int i = 0; i < s.sub().size(); i++) {
            if (s.sub().size() > 1) {
                List<GSel> sub = new ArrayList<>(s.sub());
                sub.remove(i);
                out.add(new GSel(s.name(), s.args(), sub));
            }
            for (GSel c : smaller(s.sub().get(i), false)) {
                List<GSel> sub = new ArrayList<>(s.sub());
                sub.set(i, c);
                out.add(new GSel(s.name(), s.args(), sub));
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
        OVal expected;
        boolean refused = false;
        try {
            expected = Reference.answer(model, doc, null, null);
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
            a = h.query(doc);
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

    /** {@code d} shrunk while it still disagrees; at most {@code budget} runs. */
    static GDoc shrink(Harness h, ReferenceModel model, GDoc d, int budget) {
        GDoc best = d;
        boolean progress = true;
        while (progress && budget > 0) {
            progress = false;
            for (GDoc c : smaller(best)) {
                if (--budget <= 0) {
                    break;
                }
                if (verdict(h, model, c.render()) != null) {
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
