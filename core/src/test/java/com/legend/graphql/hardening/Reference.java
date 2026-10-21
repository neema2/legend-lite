// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import com.legend.graphql.hardening.ReferenceModel.Cls;
import com.legend.graphql.hardening.ReferenceModel.DatedToMany;
import com.legend.graphql.hardening.ReferenceModel.DatedToOne;
import com.legend.graphql.hardening.ReferenceModel.Fld;
import com.legend.graphql.hardening.ReferenceModel.Kind;
import com.legend.graphql.hardening.ReferenceModel.Obj;
import com.legend.graphql.hardening.ReferenceModel.Scalar;
import com.legend.graphql.hardening.ReferenceModel.ToMany;
import com.legend.graphql.hardening.ReferenceModel.ToOne;
import com.legend.json.Json;
import com.legend.parser.GqlParser;
import com.legend.protocol.spec.Gql;
import com.legend.protocol.spec.Gql.Argument;
import com.legend.protocol.spec.Gql.Field;
import com.legend.protocol.spec.Gql.Selection;
import com.legend.protocol.spec.Gql.Value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * THE REFERENCE (docs/GRAPHQL_STUDY_2026_10_18.md §13.7): lite's GraphQL dialect answered over a fixture's rows held in
 * memory, by a reading of the contract written independently of lite's compiler. It shares only the parser (graphql-js
 * judges that, PR1b). The semantics it encodes are the contract's, each a register row:
 *
 * <ul>
 *   <li>Two-valued logic: a comparison with an absent value is false, and {@code not}, {@code notIn}, {@code NOT} are
 *       exact complements, so an absent value is "not equal" to anything and {@code NOT} of a comparison holds of it.</li>
 *   <li>A condition on a to-one object other than {@code is: null}/{@code isNot: null} requires the object to exist
 *       ({@code firm: W} is {@code firm: { is: W }}, Prisma's reading); {@code isNot: W} is its complement.</li>
 *   <li>{@code some} / {@code every} / {@code none}: Prisma's truth table, {@code every} true of an empty list.</li>
 *   <li>{@code mode: insensitive} lower-cases both sides of every string comparison in its object, nested
 *       {@code not} included.</li>
 *   <li>Order: the first key primary; an absent key sorts as the largest value (last ascending, first descending);
 *       strings by code point, enums by name, false before true.</li>
 *   <li>Folds skip absent values; a fold over no values is null; {@code count} counts the rows; {@code avg} is a
 *       float.</li>
 *   <li>Groups: an absent key value is a group of its own; {@code having} is two-valued over the group's folds.</li>
 * </ul>
 */
public final class Reference {

    final ReferenceModel model;
    final Map<String, Gql.Fragment> fragments = new LinkedHashMap<>();
    final Map<String, Value> variables = new LinkedHashMap<>();

    Reference(ReferenceModel model) {
        this.model = model;
    }

    /** The {@code data} the document's operation answers. */
    public static OVal answer(ReferenceModel model, String document, Json.Obj variables, String operationName) {
        Reference o = new Reference(model);
        Gql.Document doc = GqlParser.parseDocument(document, 1, 1);
        Gql.Operation op = null;
        for (Gql.Definition d : doc.definitions()) {
            switch (d) {
                case Gql.Fragment f -> o.fragments.put(f.name(), f);
                case Gql.Operation p -> {
                    if (operationName == null || operationName.equals(p.name())) {
                        op = p;
                    }
                }
                default -> throw new IllegalArgumentException("not a query document");
            }
        }
        Objects.requireNonNull(op, "no operation");
        for (Gql.VariableDef v : op.variables()) {
            String name = v.dollarName().startsWith("$") ? v.dollarName().substring(1) : v.dollarName();
            if (variables != null && variables.fields().containsKey(name)) {
                o.variables.put(name, fromJson(variables.fields().get(name)));
            } else if (v.defaultValue() != null) {
                o.variables.put(name, v.defaultValue());
            } else {
                o.variables.put(name, new Gql.NullValue());
            }
        }
        LinkedHashMap<String, OVal> data = new LinkedHashMap<>();
        for (Map.Entry<String, List<Field>> e : o.collect(op.selectionSet(), "Query").entrySet()) {
            Field f = e.getValue().get(0);
            if (f.name().startsWith("__") && !f.name().equals("__typename")) {
                continue;   // introspection: the generator's schema answers it, judged elsewhere
            }
            data.put(e.getKey(), o.root(f, merged(e.getValue())));
        }
        return new OVal.Obj(data);
    }

    static Value fromJson(Json.Node n) {
        return switch (n) {
            case Json.Null x -> new Gql.NullValue();
            case Json.Bool b -> new Gql.BooleanValue(b.value());
            case Json.Str s -> new Gql.StringValue(s.value());
            case Json.Num num -> num.isInteger() ? new Gql.IntValue(num.longValue()) : new Gql.FloatValue(num.doubleValue());
            case Json.Arr a -> {
                List<Value> items = new ArrayList<>();
                for (Json.Node i : a.items()) {
                    items.add(fromJson(i));
                }
                yield new Gql.ListValue(items);
            }
            case Json.Obj ob -> {
                List<Argument> fs = new ArrayList<>();
                for (Map.Entry<String, Json.Node> e : ob.fields().entrySet()) {
                    fs.add(new Argument(e.getKey(), fromJson(e.getValue())));
                }
                yield new Gql.ObjectValue(fs);
            }
        };
    }

    // ---- selections ----

    static String key(Field f) {
        String a = f.alias();
        if (a == null) {
            return f.name();
        }
        return a.endsWith(":") ? a.substring(0, a.length() - 1) : a;
    }

    /** The fields of a selection set on an object of the runtime type {@code type}, grouped by response key in order
     *  (GraphQL's CollectFields: directives, spreads, inline fragments whose type condition the object meets). */
    LinkedHashMap<String, List<Field>> collect(List<Selection> sels, String type) {
        LinkedHashMap<String, List<Field>> out = new LinkedHashMap<>();
        collect(sels, type, out);
        return out;
    }

    private void collect(List<Selection> sels, String type, LinkedHashMap<String, List<Field>> out) {
        for (Selection s : sels) {
            switch (s) {
                case Field f -> {
                    if (included(f.directives())) {
                        out.computeIfAbsent(key(f), k -> new ArrayList<>()).add(f);
                    }
                }
                case Gql.FragmentSpread sp -> {
                    Gql.Fragment fr = fragments.get(sp.name());
                    if (included(sp.directives()) && applies(fr.typeCondition(), type)) {
                        collect(fr.selectionSet(), type, out);
                    }
                }
                case Gql.InlineFragment in -> {
                    if (included(in.directives()) && (in.typeCondition() == null || applies(in.typeCondition(), type))) {
                        collect(in.selectionSet(), type, out);
                    }
                }
            }
        }
    }

    boolean included(List<Gql.Directive> ds) {
        for (Gql.Directive d : ds) {
            boolean v = false;
            for (Argument a : d.arguments()) {
                if (a.name().equals("if")) {
                    v = resolve(a.value()) instanceof Gql.BooleanValue b && b.value();
                }
            }
            if (d.name().equals("skip") && v) {
                return false;
            }
            if (d.name().equals("include") && !v) {
                return false;
            }
        }
        return true;
    }

    /** Does a type condition hold of an object of exact class {@code type}? {@code XInterface} holds of X and every
     *  class below it; {@code X} of exact X instances only. */
    boolean applies(String condition, String type) {
        if (condition.equals("Query") || type.equals("Query")) {
            return condition.equals(type);
        }
        if (condition.endsWith("Interface") && model.classes.containsKey(
                condition.substring(0, condition.length() - "Interface".length()))) {
            return model.isA(type, condition.substring(0, condition.length() - "Interface".length()));
        }
        return condition.equals(type);
    }

    static List<Selection> merged(List<Field> fields) {
        List<Selection> sels = new ArrayList<>();
        for (Field f : fields) {
            sels.addAll(f.selectionSet());
        }
        return sels;
    }

    Value resolve(Value v) {
        if (v instanceof Gql.VariableRef r) {
            Value given = variables.get(r.name());
            return given == null ? new Gql.NullValue() : given;
        }
        return v;
    }

    Value arg(List<Argument> args, String name) {
        for (Argument a : args) {
            if (a.name().equals(name)) {
                return resolve(a.value());
            }
        }
        return null;
    }

    // ---- roots ----

    OVal root(Field f, List<Selection> sels) {
        String name = f.name();
        if (name.equals("__typename")) {
            return new OVal.Str("Query");
        }
        if (model.classes.containsKey(name)) {
            return list(rootsOf(name, f), name, f.arguments(), sels);
        }
        if (name.endsWith("Aggregate") && model.classes.containsKey(base(name, "Aggregate"))) {
            String cls = base(name, "Aggregate");
            return aggregate(where(rootsOf(cls, f), cls, arg(f.arguments(), "where")), cls, sels);
        }
        if (name.endsWith("Groups") && model.classes.containsKey(base(name, "Groups"))) {
            String cls = base(name, "Groups");
            return groups(rootsOf(cls, f), cls, f.arguments(), sels);
        }
        throw new IllegalArgumentException("no root field " + name);
    }

    /** A root's rows: a business-temporal class's as of the field's {@code businessDate}, which it requires. */
    List<Obj> rootsOf(String cls, Field f) {
        Cls c = model.get(cls);
        if (!c.temporal()) {
            return c.roots.get();
        }
        Value d = arg(f.arguments(), "businessDate");
        if (d == null || d instanceof Gql.NullValue) {
            throw new IllegalArgumentException(cls + " is business-temporal: its root takes a businessDate");
        }
        return c.datedRoots.apply((LocalDate) literal(d, Kind.DATE));
    }

    static String base(String name, String suffix) {
        return name.substring(0, name.length() - suffix.length());
    }

    // ---- objects ----

    OVal object(Obj o, List<Selection> sels) {
        LinkedHashMap<String, OVal> out = new LinkedHashMap<>();
        for (Map.Entry<String, List<Field>> e : collect(sels, o.type()).entrySet()) {
            Field f = e.getValue().get(0);
            out.put(e.getKey(), field(o, f, merged(e.getValue())));
        }
        return new OVal.Obj(out);
    }

    OVal field(Obj o, Field f, List<Selection> sels) {
        String name = f.name();
        if (name.equals("__typename")) {
            return new OVal.Str(o.type());
        }
        Cls c = model.get(o.type());
        Fld fld = c.fields.get(name);
        if (fld == null && name.endsWith("Aggregate") && c.fields.get(base(name, "Aggregate")) instanceof ToMany tm) {
            return aggregate(where(tm.get().apply(o), tm.target(), arg(f.arguments(), "where")), tm.target(), sels);
        }
        if (fld == null && name.endsWith("Groups") && c.fields.get(base(name, "Groups")) instanceof ToMany tm) {
            return groups(tm.get().apply(o), tm.target(), f.arguments(), sels);
        }
        if (fld == null && name.endsWith("Aggregate") && c.fields.get(base(name, "Aggregate")) instanceof DatedToMany tm) {
            return aggregate(where(tm.get().apply(o, dateOf(o, f, name)), tm.target(), arg(f.arguments(), "where")),
                    tm.target(), sels);
        }
        if (fld == null && name.endsWith("Groups") && c.fields.get(base(name, "Groups")) instanceof DatedToMany tm) {
            return groups(tm.get().apply(o, dateOf(o, f, name)), tm.target(), f.arguments(), sels);
        }
        if (fld == null) {
            throw new IllegalArgumentException(o.type() + " has no field " + name);
        }
        return switch (fld) {
            case Scalar s -> value(s.kind(), s.get().apply(o, args(s, f.arguments())));
            case ToOne t -> {
                Obj child = t.get().apply(o);
                yield child == null ? OVal.NULL : object(child, sels);
            }
            case ToMany t -> list(t.get().apply(o), t.target(), f.arguments(), sels);
            case DatedToOne t -> {
                Obj child = t.get().apply(o, dateOf(o, f, name));
                yield child == null ? OVal.NULL : object(child, sels);
            }
            case DatedToMany t -> list(t.get().apply(o, dateOf(o, f, name)), t.target(), f.arguments(), sels);
        };
    }

    /** A dated field's business date: its argument, else the owner's own (a temporal owner's date propagates). */
    LocalDate dateOf(Obj o, Field f, String name) {
        Value d = arg(f.arguments(), "businessDate");
        LocalDate date = d != null && !(d instanceof Gql.NullValue) ? (LocalDate) literal(d, Kind.DATE)
                : (LocalDate) o.get("businessDate");
        if (date == null) {
            throw new IllegalArgumentException(name + " reaches a business-temporal class: it takes a date");
        }
        return date;
    }

    Map<String, Object> args(Scalar s, List<Argument> given) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Kind> p : s.params().entrySet()) {
            Value v = arg(given, p.getKey());
            out.put(p.getKey(), v == null ? null : literal(v, p.getValue()));
        }
        return out;
    }

    /** A list of objects under {@code where}, {@code orderBy}, {@code offset}, {@code limit}. */
    OVal list(List<Obj> source, String cls, List<Argument> args, List<Selection> sels) {
        List<Obj> rows = where(source, cls, arg(args, "where"));
        List<SortKey> keys = sortKeys(arg(args, "orderBy"), cls);
        List<Integer> ranks = new ArrayList<>();
        if (!keys.isEmpty()) {
            Comparator<Obj> cmp = comparator(keys);
            rows = new ArrayList<>(rows);
            rows.sort(cmp);
            int rank = 0;
            for (int i = 0; i < rows.size(); i++) {
                if (i > 0 && cmp.compare(rows.get(i - 1), rows.get(i)) != 0) {
                    rank++;
                }
                ranks.add(rank);
            }
        } else {
            for (int i = 0; i < rows.size(); i++) {
                ranks.add(0);
            }
        }
        int from = count(arg(args, "offset"));
        int limit = count(arg(args, "limit"));
        int to = limit < 0 ? rows.size() : Math.min(rows.size(), Math.max(from, 0) + limit);
        from = Math.min(Math.max(from, 0), rows.size());
        if (from > 0 || to < rows.size()) {
            // the cut must not split a run of ties: the document would not fix which rows it keeps
            if ((from > 0 && from < rows.size() && ranks.get(from).equals(ranks.get(from - 1)))
                    || (to > 0 && to < rows.size() && ranks.get(to).equals(ranks.get(to - 1)))) {
                throw new Undetermined("limit/offset cuts through rows the orderBy does not order");
            }
        }
        List<OVal> items = new ArrayList<>();
        List<Integer> kept = new ArrayList<>();
        for (int i = from; i < to; i++) {
            items.add(object(rows.get(i), sels));
            kept.add(ranks.get(i));
        }
        return new OVal.Lst(items, kept);
    }

    /** A document whose answer the contract does not fix (a cut through ties): the generator avoids it. */
    public static final class Undetermined extends RuntimeException {
        Undetermined(String m) {
            super(m);
        }
    }

    /** A document the contract refuses (an empty sort key, two keys in one): lite must answer with errors. */
    public static final class Refused extends RuntimeException {
        Refused(String m) {
            super(m);
        }
    }

    static int count(Value v) {
        if (v == null || v instanceof Gql.NullValue) {
            return -1;
        }
        return (int) ((Gql.IntValue) v).value();
    }

    // ---- where ----

    List<Obj> where(List<Obj> rows, String cls, Value w) {
        if (w == null || w instanceof Gql.NullValue) {
            return rows;
        }
        List<Obj> out = new ArrayList<>();
        for (Obj o : rows) {
            if (holds(o, cls, (Gql.ObjectValue) w)) {
                out.add(o);
            }
        }
        return out;
    }

    /** {@code w} over the object {@code o}, of the static class {@code cls}. */
    boolean holds(Obj o, String cls, Gql.ObjectValue w) {
        for (Argument a : w.fields()) {
            Value v = resolve(a.value());
            boolean ok = switch (a.name()) {
                case "AND" -> {
                    boolean all = true;
                    for (Gql.ObjectValue x : objects(v)) {
                        all &= holds(o, cls, x);
                    }
                    yield all;
                }
                case "OR" -> {
                    boolean any = false;
                    for (Gql.ObjectValue x : objects(v)) {
                        any |= holds(o, cls, x);
                    }
                    yield any;
                }
                case "NOT" -> v instanceof Gql.NullValue || !holds(o, cls, (Gql.ObjectValue) v);
                case "is" -> v instanceof Gql.NullValue ? false : holds(o, cls, (Gql.ObjectValue) v);
                case "isNot" -> v instanceof Gql.NullValue ? true : !holds(o, cls, (Gql.ObjectValue) v);
                default -> v instanceof Gql.NullValue || condition(o, cls, a.name(), v);
            };
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    List<Gql.ObjectValue> objects(Value v) {
        List<Gql.ObjectValue> out = new ArrayList<>();
        switch (v) {
            case Gql.NullValue n -> { }
            case Gql.ObjectValue o -> out.add(o);
            case Gql.ListValue l -> {
                for (Value i : l.values()) {
                    Value r = resolve(i);
                    if (r instanceof Gql.ObjectValue o) {
                        out.add(o);
                    }
                }
            }
            default -> throw new IllegalArgumentException("not a condition object: " + v);
        }
        return out;
    }

    boolean condition(Obj o, String cls, String name, Value v) {
        if (name.startsWith("as") && model.classes.containsKey(name.substring(2))) {
            String sub = name.substring(2);
            return related(model.isA(o.type(), sub) ? o : null, sub, (Gql.ObjectValue) v);
        }
        Fld fld = model.get(o.type()).fields.get(name);
        if (fld == null) {
            fld = model.get(cls).fields.get(name);
        }
        return switch (fld) {
            case Scalar s -> scalar(s.get().apply(o, Map.of()), s.kind(), (Gql.ObjectValue) v, false);
            case ToOne t -> related(t.get().apply(o), t.target(), (Gql.ObjectValue) v);
            case ToMany t -> quantified(t.get().apply(o), t.target(), (Gql.ObjectValue) v);
            case DatedToOne t -> throw new IllegalArgumentException(cls + "." + name
                    + " reaches a business-temporal class: no condition names it (it takes a date)");
            case DatedToMany t -> throw new IllegalArgumentException(cls + "." + name
                    + " reaches a business-temporal class: no condition names it (it takes a date)");
            case null -> throw new IllegalArgumentException(cls + " has no field " + name);
        };
    }

    /** A condition on a to-one object (or the row as a subclass): {@code is: null} / {@code isNot: null} test its
     *  absence or presence; any other condition requires it to exist, {@code isNot: W} being the complement. */
    boolean related(Obj child, String target, Gql.ObjectValue w) {
        boolean ok = true;
        List<Argument> rest = new ArrayList<>();
        for (Argument a : w.fields()) {
            Value v = resolve(a.value());
            switch (a.name()) {
                case "is" -> ok &= v instanceof Gql.NullValue ? child == null
                        : child != null && holds(child, target, (Gql.ObjectValue) v);
                case "isNot" -> ok &= v instanceof Gql.NullValue ? child != null
                        : child == null || !holds(child, target, (Gql.ObjectValue) v);
                default -> rest.add(a);
            }
        }
        if (!rest.isEmpty()) {
            ok &= child != null && holds(child, target, new Gql.ObjectValue(rest));
        }
        return ok;
    }

    boolean quantified(List<Obj> items, String target, Gql.ObjectValue w) {
        boolean ok = true;
        for (Argument a : w.fields()) {
            Value v = resolve(a.value());
            if (v instanceof Gql.NullValue) {
                continue;
            }
            Gql.ObjectValue c = (Gql.ObjectValue) v;
            long n = items.stream().filter(i -> holds(i, target, c)).count();
            ok &= switch (a.name()) {
                case "some" -> n > 0;
                case "none" -> n == 0;
                case "every" -> n == items.size();
                default -> throw new IllegalArgumentException("no list quantifier " + a.name());
            };
        }
        return ok;
    }

    /** A scalar filter over the value {@code x} (null: absent): two-valued, {@code mode} inherited by nested
     *  {@code not} objects. */
    boolean scalar(Object x, Kind kind, Gql.ObjectValue ops, boolean insensitiveOuter) {
        boolean ins = insensitiveOuter;
        for (Argument a : ops.fields()) {
            if (a.name().equals("mode")) {
                Value m = resolve(a.value());
                ins = (m instanceof Gql.EnumValue e && e.value().equals("insensitive"))
                        || (m instanceof Gql.StringValue s && s.value().equals("insensitive"));
            }
        }
        for (Argument a : ops.fields()) {
            Value v = resolve(a.value());
            boolean ok = switch (a.name()) {
                case "mode" -> true;
                case "equals" -> v instanceof Gql.NullValue ? x == null : x != null && eq(x, literal(v, kind), ins);
                case "not" -> {
                    if (v instanceof Gql.NullValue) {
                        yield x != null;
                    }
                    if (v instanceof Gql.ObjectValue nested) {
                        yield !scalar(x, kind, nested, ins);
                    }
                    yield !(x != null && eq(x, literal(v, kind), ins));
                }
                case "in", "notIn" -> {
                    if (v instanceof Gql.NullValue) {
                        yield true;
                    }
                    List<Value> items = v instanceof Gql.ListValue l ? l.values() : List.of(v);
                    boolean member = false;
                    for (Value i : items) {
                        member |= x != null && eq(x, literal(resolve(i), kind), ins);
                    }
                    yield a.name().equals("in") == member;
                }
                case "lt", "lte", "gt", "gte" -> {
                    if (v instanceof Gql.NullValue) {
                        yield true;
                    }
                    if (x == null) {
                        yield false;
                    }
                    int c = compare(caseFold(x, ins), caseFold(literal(v, kind), ins));
                    yield switch (a.name()) {
                        case "lt" -> c < 0;
                        case "lte" -> c <= 0;
                        case "gt" -> c > 0;
                        default -> c >= 0;
                    };
                }
                case "contains", "startsWith", "endsWith" -> {
                    if (v instanceof Gql.NullValue) {
                        yield true;
                    }
                    if (x == null) {
                        yield false;
                    }
                    String s = (String) caseFold(x, ins);
                    String p = (String) caseFold(literal(v, kind), ins);
                    yield switch (a.name()) {
                        case "contains" -> s.contains(p);
                        case "startsWith" -> s.startsWith(p);
                        default -> s.endsWith(p);
                    };
                }
                default -> throw new IllegalArgumentException("no filter operation " + a.name());
            };
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    /** A string lower-cased under {@code mode: insensitive}, by the model's case folding (the database's). */
    Object caseFold(Object x, boolean insensitive) {
        return insensitive && x instanceof String s ? model.lower(s) : x;
    }

    boolean eq(Object x, Object lit, boolean insensitive) {
        return compare(caseFold(x, insensitive), caseFold(lit, insensitive)) == 0;
    }

    /** A GraphQL literal as the Java value of the kind it is compared with. */
    static Object literal(Value v, Kind kind) {
        return switch (kind) {
            case STRING, ENUM -> switch (v) {
                case Gql.StringValue s -> s.value();
                case Gql.EnumValue e -> e.value();
                default -> throw new IllegalArgumentException("not a " + kind + ": " + v);
            };
            case BIGINT -> ((Gql.IntValue) v).value();
            case FLOAT -> v instanceof Gql.IntValue i ? (double) i.value() : ((Gql.FloatValue) v).value();
            case DECIMAL -> switch (v) {
                case Gql.IntValue i -> BigDecimal.valueOf(i.value());
                case Gql.FloatValue f -> new BigDecimal(Double.toString(f.value()));
                case Gql.StringValue s -> new BigDecimal(s.value());
                default -> throw new IllegalArgumentException("not a Decimal: " + v);
            };
            case BOOLEAN -> ((Gql.BooleanValue) v).value();
            case DATE -> LocalDate.parse(((Gql.StringValue) v).value());
            case DATETIME -> {
                String s = ((Gql.StringValue) v).value();
                yield s.length() == 10 ? LocalDate.parse(s).atStartOfDay() : LocalDateTime.parse(s);
            }
        };
    }

    /** Values of one kind compared: numbers by value, strings by code point, booleans false first, dates by time. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static int compare(Object a, Object b) {
        if (a instanceof Number x && b instanceof Number y) {
            return toBig(x).compareTo(toBig(y));
        }
        if (a instanceof String x && b instanceof String y) {
            return codePoints(x, y);
        }
        if (a instanceof LocalDate x && b instanceof LocalDateTime y) {
            return x.atStartOfDay().compareTo(y);
        }
        if (a instanceof LocalDateTime x && b instanceof LocalDate y) {
            return x.compareTo(y.atStartOfDay());
        }
        return ((Comparable) a).compareTo(b);
    }

    static BigDecimal toBig(Number n) {
        return switch (n) {
            case BigDecimal b -> b;
            case Long l -> BigDecimal.valueOf(l);
            case Integer i -> BigDecimal.valueOf(i);
            case BigInteger b -> new BigDecimal(b);
            default -> new BigDecimal(Double.toString(n.doubleValue()));
        };
    }

    static int codePoints(String a, String b) {
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int x = a.codePointAt(i);
            int y = b.codePointAt(j);
            if (x != y) {
                return Integer.compare(x, y);
            }
            i += Character.charCount(x);
            j += Character.charCount(y);
        }
        return Integer.compare(a.length() - i, b.length() - j);
    }

    // ---- order ----

    /** One sort key: how to read it off an object, and its direction. */
    record SortKey(java.util.function.Function<Obj, Object> read, boolean ascending) {
    }

    List<SortKey> sortKeys(Value orderBy, String cls) {
        List<SortKey> out = new ArrayList<>();
        if (orderBy == null || orderBy instanceof Gql.NullValue) {
            return out;
        }
        for (Gql.ObjectValue o : objects(orderBy)) {
            out.add(sortKey(o, cls));
        }
        return out;
    }

    SortKey sortKey(Gql.ObjectValue o, String cls) {
        Argument a = null;
        int given = 0;
        for (Argument x : o.fields()) {
            if (!(resolve(x.value()) instanceof Gql.NullValue)) {
                a = x;
                given++;
            }
        }
        if (given != 1) {
            throw new Refused("a sort key names exactly one property (G16): " + given);
        }
        Value v = resolve(a.value());
        String name = a.name();
        if (v instanceof Gql.EnumValue dir) {
            Fld f = model.get(cls).fields.get(name);
            if (!(f instanceof Scalar s)) {
                throw new IllegalArgumentException("not a scalar sort key: " + name);
            }
            return new SortKey(x -> s.get().apply(x, Map.of()), dir.value().equals("ASC"));
        }
        Gql.ObjectValue inner = (Gql.ObjectValue) v;
        if (name.startsWith("as") && model.classes.containsKey(name.substring(2))) {
            String sub = name.substring(2);
            SortKey k = sortKey(inner, sub);
            return new SortKey(x -> model.isA(x.type(), sub) ? k.read().apply(x) : null, k.ascending());
        }
        if (name.endsWith("Aggregate") && model.get(cls).fields.get(base(name, "Aggregate")) instanceof ToMany tm) {
            // a to-many property's aggregate: the count of its objects, or a fold of one of their properties
            // (absent values skipped, null over none -- the same fold the aggregate fields answer)
            Argument m = single(inner);
            Value mv = resolve(m.value());
            if (m.name().equals("count")) {
                return new SortKey(x -> (long) tm.get().apply(x).size(), ((Gql.EnumValue) mv).value().equals("ASC"));
            }
            Argument leaf = single((Gql.ObjectValue) mv);
            Scalar s = (Scalar) model.get(tm.target()).fields.get(leaf.name());
            String fn = m.name();
            return new SortKey(x -> raw(fold(fn, s.kind(), tm.get().apply(x), it -> s.get().apply(it, Map.of()))),
                    ((Gql.EnumValue) resolve(leaf.value())).value().equals("ASC"));
        }
        Fld f = model.get(cls).fields.get(name);
        if (!(f instanceof ToOne t)) {
            throw new IllegalArgumentException("not a to-one sort path: " + name);
        }
        SortKey k = sortKey(inner, t.target());
        return new SortKey(x -> {
            Obj c = t.get().apply(x);
            return c == null ? null : k.read().apply(c);
        }, k.ascending());
    }

    /** The one non-null field of a key object. */
    Argument single(Gql.ObjectValue o) {
        Argument a = null;
        int given = 0;
        for (Argument x : o.fields()) {
            if (!(resolve(x.value()) instanceof Gql.NullValue)) {
                a = x;
                given++;
            }
        }
        if (given != 1) {
            throw new Refused("a sort key names exactly one member (G16): " + given);
        }
        return a;
    }

    /** A folded value back as the Java value the comparator reads (null for none). */
    static Object raw(OVal v) {
        return switch (v) {
            case OVal.Null n -> null;
            case OVal.Num n -> n.value();
            case OVal.Str x -> x.value();
            case OVal.Bool b -> b.value();
            case OVal.DateV d -> d.value();
            case OVal.DateTimeV d -> d.value();
            default -> throw new IllegalArgumentException("not a folded value: " + v);
        };
    }

    static Comparator<Obj> comparator(List<SortKey> keys) {
        return (x, y) -> {
            for (SortKey k : keys) {
                int c = nullsLargest(k.read().apply(x), k.read().apply(y));
                if (c != 0) {
                    return k.ascending() ? c : -c;
                }
            }
            return 0;
        };
    }

    static int nullsLargest(Object a, Object b) {
        if (a == null || b == null) {
            return a == null ? (b == null ? 0 : 1) : -1;
        }
        return compare(a, b);
    }

    // ---- values ----

    static OVal value(Kind kind, Object x) {
        if (x == null) {
            return OVal.NULL;
        }
        return switch (kind) {
            case STRING, ENUM -> new OVal.Str((String) x);
            case BIGINT -> new OVal.Num(toBig((Number) x), true);
            case FLOAT, DECIMAL -> new OVal.Num(toBig((Number) x), false);
            case BOOLEAN -> new OVal.Bool((Boolean) x);
            case DATE -> x instanceof LocalDateTime t ? new OVal.DateTimeV(t) : new OVal.DateV((LocalDate) x);
            case DATETIME -> new OVal.DateTimeV(x instanceof LocalDate d ? d.atStartOfDay() : (LocalDateTime) x);
        };
    }

    // ---- aggregates ----

    static final List<String> FOLDS = List.of("sum", "avg", "min", "max");

    OVal aggregate(List<Obj> items, String cls, List<Selection> sels) {
        LinkedHashMap<String, OVal> out = new LinkedHashMap<>();
        for (Map.Entry<String, List<Field>> e : collect(sels, "Aggregate").entrySet()) {
            Field f = e.getValue().get(0);
            out.put(e.getKey(), member(items, cls, f, merged(e.getValue())));
        }
        return new OVal.Obj(out);
    }

    /** {@code count}, or a fold's object of its properties. */
    OVal member(List<Obj> items, String cls, Field f, List<Selection> sels) {
        if (f.name().equals("count")) {
            return new OVal.Num(BigDecimal.valueOf(items.size()), true);
        }
        if (f.name().equals("__typename")) {
            return new OVal.Str(cls + "Aggregate");
        }
        String foldType = cls + Character.toUpperCase(f.name().charAt(0)) + f.name().substring(1);
        if (!FOLDS.contains(f.name())) {
            throw new IllegalArgumentException("no aggregate member " + f.name());
        }
        LinkedHashMap<String, OVal> out = new LinkedHashMap<>();
        for (Map.Entry<String, List<Field>> e : collect(sels, "Fold").entrySet()) {
            String p = e.getValue().get(0).name();
            if (p.equals("__typename")) {
                out.put(e.getKey(), new OVal.Str(foldType));   // the fold's own type: CustomerSum
                continue;
            }
            Scalar s = (Scalar) model.get(cls).fields.get(p);
            out.put(e.getKey(), fold(f.name(), s.kind(), items, x -> s.get().apply(x, Map.of())));
        }
        return new OVal.Obj(out);
    }

    static OVal fold(String fn, Kind kind, List<Obj> items, java.util.function.Function<Obj, Object> read) {
        List<Object> vals = new ArrayList<>();
        for (Obj o : items) {
            Object v = read.apply(o);
            if (v != null) {
                vals.add(v);
            }
        }
        if (vals.isEmpty()) {
            return OVal.NULL;
        }
        return switch (fn) {
            case "sum" -> {
                BigDecimal s = BigDecimal.ZERO;
                for (Object v : vals) {
                    s = s.add(toBig((Number) v));
                }
                yield new OVal.Num(s, kind == Kind.BIGINT);
            }
            case "avg" -> {
                BigDecimal s = BigDecimal.ZERO;
                for (Object v : vals) {
                    s = s.add(toBig((Number) v));
                }
                yield new OVal.Num(s.divide(BigDecimal.valueOf(vals.size()), MathContext.DECIMAL64), false);
            }
            case "min", "max" -> {
                Object best = vals.get(0);
                for (Object v : vals) {
                    int c = compare(v, best);
                    if (fn.equals("min") ? c < 0 : c > 0) {
                        best = v;
                    }
                }
                yield value(kind, best);
            }
            default -> throw new IllegalArgumentException(fn);
        };
    }

    // ---- groups ----

    /** One key the selection under {@code key} names: its response path, how to read it, its kind. */
    /** {@code path}: the response keys (aliases) from the group's {@code key} field down; {@code names}: the
     *  property names, which {@code orderBy} spells. */
    record GroupKey(List<String> path, List<String> names, java.util.function.Function<Obj, Object> read, Kind kind) {
    }

    OVal groups(List<Obj> source, String cls, List<Argument> args, List<Selection> sels) {
        List<Obj> rows = where(source, cls, arg(args, "where"));
        LinkedHashMap<String, List<Field>> top = collect(sels, "Group");
        List<GroupKey> keys = new ArrayList<>();
        for (Map.Entry<String, List<Field>> e : top.entrySet()) {
            if (e.getValue().get(0).name().equals("key")) {
                groupKeys(merged(e.getValue()), cls, List.of(e.getKey()), List.of("key"), x -> x, keys);
            }
        }
        // the groups, in first-seen order
        LinkedHashMap<List<Object>, List<Obj>> groups = new LinkedHashMap<>();
        for (Obj o : rows) {
            List<Object> k = new ArrayList<>();
            for (GroupKey g : keys) {
                k.add(g.read().apply(o));
            }
            groups.computeIfAbsent(k, x -> new ArrayList<>()).add(o);
        }
        List<Map.Entry<List<Object>, List<Obj>>> list = new ArrayList<>(groups.entrySet());
        Value having = arg(args, "having");
        if (having != null && !(having instanceof Gql.NullValue)) {
            list.removeIf(g -> !having(g.getValue(), cls, (Gql.ObjectValue) having));
        }
        // order
        List<java.util.function.Function<Map.Entry<List<Object>, List<Obj>>, Object>> reads = new ArrayList<>();
        List<Boolean> asc = new ArrayList<>();
        Value orderBy = arg(args, "orderBy");
        if (orderBy != null && !(orderBy instanceof Gql.NullValue)) {
            for (Gql.ObjectValue o : objects(orderBy)) {
                groupSortKey(o, cls, keys, reads, asc);
            }
        }
        Comparator<Map.Entry<List<Object>, List<Obj>>> cmp = (x, y) -> {
            for (int i = 0; i < reads.size(); i++) {
                int c = nullsLargest(reads.get(i).apply(x), reads.get(i).apply(y));
                if (c != 0) {
                    return asc.get(i) ? c : -c;
                }
            }
            return 0;
        };
        List<Integer> ranks = new ArrayList<>();
        if (!reads.isEmpty()) {
            list.sort(cmp);
        }
        int rank = 0;
        for (int i = 0; i < list.size(); i++) {
            if (i > 0 && (reads.isEmpty() ? false : cmp.compare(list.get(i - 1), list.get(i)) != 0)) {
                rank++;
            }
            ranks.add(reads.isEmpty() ? 0 : rank);
        }
        int from = Math.max(count(arg(args, "offset")), 0);
        int limit = count(arg(args, "limit"));
        int to = limit < 0 ? list.size() : Math.min(list.size(), from + limit);
        from = Math.min(from, list.size());
        if ((from > 0 && from < list.size() && ranks.get(from).equals(ranks.get(from - 1)))
                || (to > 0 && to < list.size() && ranks.get(to).equals(ranks.get(to - 1)))) {
            throw new Undetermined("limit/offset cuts through groups the orderBy does not order");
        }
        List<OVal> items = new ArrayList<>();
        List<Integer> kept = new ArrayList<>();
        for (int i = from; i < to; i++) {
            Map.Entry<List<Object>, List<Obj>> g = list.get(i);
            LinkedHashMap<String, OVal> out = new LinkedHashMap<>();
            for (Map.Entry<String, List<Field>> e : top.entrySet()) {
                Field f = e.getValue().get(0);
                if (f.name().equals("key")) {
                    out.put(e.getKey(), keyObject(merged(e.getValue()), cls, List.of(e.getKey()), keys, g.getKey()));
                } else if (f.name().equals("__typename")) {
                    out.put(e.getKey(), new OVal.Str(cls + "Group"));
                } else {
                    out.put(e.getKey(), member(g.getValue(), cls, f, merged(e.getValue())));
                }
            }
            items.add(new OVal.Obj(out));
            kept.add(ranks.get(i));
        }
        return new OVal.Lst(items, kept);
    }

    void groupKeys(List<Selection> sels, String cls, List<String> path, List<String> names,
            java.util.function.Function<Obj, Obj> hop, List<GroupKey> into) {
        for (Map.Entry<String, List<Field>> e : collect(sels, "GroupKey").entrySet()) {
            Field f = e.getValue().get(0);
            if (f.name().startsWith("__")) {
                continue;   // a meta-field is never a key (G19)
            }
            List<String> p = new ArrayList<>(path);
            p.add(e.getKey());
            List<String> n = new ArrayList<>(names);
            n.add(f.name());
            Fld fld = model.get(cls).fields.get(f.name());
            switch (fld) {
                case Scalar s -> {
                    Value unit = arg(f.arguments(), "truncate");
                    String u = unit instanceof Gql.EnumValue ev ? ev.value() : null;
                    into.add(new GroupKey(p, n, x -> {
                        Obj o = hop.apply(x);
                        Object v = o == null ? null : s.get().apply(o, Map.of());
                        return u == null || v == null ? v : truncate(v, u);
                    }, s.kind()));
                }
                case ToOne t -> groupKeys(merged(e.getValue()), t.target(), p, n, x -> {
                    Obj o = hop.apply(x);
                    return o == null ? null : t.get().apply(o);
                }, into);
                default -> throw new IllegalArgumentException("not a group key: " + f.name());
            }
        }
    }

    static Object truncate(Object v, String unit) {
        LocalDateTime t = v instanceof LocalDate d ? d.atStartOfDay() : (LocalDateTime) v;
        LocalDateTime out = switch (unit) {
            case "YEAR" -> t.toLocalDate().withDayOfYear(1).atStartOfDay();
            case "QUARTER" -> t.toLocalDate().withMonth(((t.getMonthValue() - 1) / 3) * 3 + 1).withDayOfMonth(1)
                    .atStartOfDay();
            case "MONTH" -> t.toLocalDate().withDayOfMonth(1).atStartOfDay();
            case "WEEK" -> t.toLocalDate().with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY)).atStartOfDay();
            case "DAY" -> t.toLocalDate().atStartOfDay();
            case "HOUR" -> t.withMinute(0).withSecond(0).withNano(0);
            default -> throw new IllegalArgumentException(unit);
        };
        return v instanceof LocalDate ? out.toLocalDate() : out;
    }

    OVal keyObject(List<Selection> sels, String cls, List<String> path, List<GroupKey> keys, List<Object> values) {
        LinkedHashMap<String, OVal> out = new LinkedHashMap<>();
        for (Map.Entry<String, List<Field>> e : collect(sels, "GroupKey").entrySet()) {
            Field f = e.getValue().get(0);
            if (f.name().equals("__typename")) {
                out.put(e.getKey(), new OVal.Str(cls + "GroupKey"));   // the key object's own type
                continue;
            }
            if (f.name().startsWith("__")) {
                continue;
            }
            List<String> p = new ArrayList<>(path);
            p.add(e.getKey());
            int i = indexOf(keys, p);
            if (i >= 0) {
                out.put(e.getKey(), value(keys.get(i).kind(), values.get(i)));
            } else {
                Fld kf = model.get(cls).fields.get(f.name());
                String target = kf instanceof ToOne t ? t.target() : cls;
                out.put(e.getKey(), keyObject(merged(e.getValue()), target, p, keys, values));
            }
        }
        return new OVal.Obj(out);
    }

    static int indexOf(List<GroupKey> keys, List<String> path) {
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).path().equals(path)) {
                return i;
            }
        }
        return -1;
    }

    boolean having(List<Obj> group, String cls, Gql.ObjectValue h) {
        for (Argument a : h.fields()) {
            Value v = resolve(a.value());
            if (v instanceof Gql.NullValue) {
                continue;
            }
            boolean ok = switch (a.name()) {
                case "AND" -> objects(v).stream().allMatch(x -> having(group, cls, x));
                case "OR" -> objects(v).stream().anyMatch(x -> having(group, cls, x));
                case "NOT" -> !having(group, cls, (Gql.ObjectValue) v);
                case "count" -> scalar((long) group.size(), Kind.BIGINT, (Gql.ObjectValue) v, false);
                default -> {
                    boolean all = true;
                    for (Argument p : ((Gql.ObjectValue) v).fields()) {
                        Value pv = resolve(p.value());
                        if (pv instanceof Gql.NullValue) {
                            continue;
                        }
                        Scalar s = (Scalar) model.get(cls).fields.get(p.name());
                        OVal folded = fold(a.name(), s.kind(), group, x -> s.get().apply(x, Map.of()));
                        Object x = folded instanceof OVal.Num n ? n.value()
                                : folded instanceof OVal.DateV d ? d.value()
                                : folded instanceof OVal.DateTimeV d ? d.value()
                                : folded instanceof OVal.Str st ? st.value() : null;
                        Kind k = a.name().equals("avg") ? Kind.FLOAT : s.kind();
                        all &= scalar(x, k, (Gql.ObjectValue) pv, false);
                    }
                    yield all;
                }
            };
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    void groupSortKey(Gql.ObjectValue o, String cls, List<GroupKey> keys,
            List<java.util.function.Function<Map.Entry<List<Object>, List<Obj>>, Object>> reads, List<Boolean> asc) {
        for (Argument a : o.fields()) {
            Value v = resolve(a.value());
            if (v instanceof Gql.NullValue) {
                continue;
            }
            switch (a.name()) {
                case "count" -> {
                    reads.add(g -> (long) g.getValue().size());
                    asc.add(((Gql.EnumValue) v).value().equals("ASC"));
                }
                case "key" -> {
                    List<String> path = new ArrayList<>();
                    path.add("key");
                    Value cur = v;
                    String dir;
                    while (true) {
                        Argument x = ((Gql.ObjectValue) cur).fields().get(0);
                        path.add(x.name());
                        Value nv = resolve(x.value());
                        if (nv instanceof Gql.EnumValue e) {
                            dir = e.value();
                            break;
                        }
                        cur = nv;
                    }
                    int i = -1;
                    for (int k = 0; k < keys.size(); k++) {
                        // the key's response path matched by its property names (no aliases inside orderBy)
                        if (keys.get(k).names().subList(1, keys.get(k).names().size())
                                .equals(path.subList(1, path.size()))) {
                            i = k;
                        }
                    }
                    if (i < 0) {
                        throw new IllegalArgumentException("orderBy names a key not selected: " + path);
                    }
                    int idx = i;
                    reads.add(g -> g.getKey().get(idx));
                    asc.add(dir.equals("ASC"));
                }
                default -> {
                    Argument p = ((Gql.ObjectValue) v).fields().get(0);
                    Scalar s = (Scalar) model.get(cls).fields.get(p.name());
                    String fn = a.name();
                    reads.add(g -> {
                        OVal f = fold(fn, s.kind(), g.getValue(), x -> s.get().apply(x, Map.of()));
                        return f instanceof OVal.Num n ? n.value() : f instanceof OVal.DateV d ? d.value()
                                : f instanceof OVal.DateTimeV d ? d.value() : f instanceof OVal.Str st ? st.value() : null;
                    });
                    asc.add(((Gql.EnumValue) resolve(p.value())).value().equals("ASC"));
                }
            }
        }
    }
}
