// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.graphql;

import com.legend.protocol.graphql.GqlSchema.Composite;
import com.legend.protocol.graphql.GqlSchema.EnumType;
import com.legend.protocol.graphql.GqlSchema.FieldDef;
import com.legend.protocol.graphql.GqlSchema.InputObjectType;
import com.legend.protocol.graphql.GqlSchema.InputValueDef;
import com.legend.protocol.graphql.GqlSchema.Meaning;
import com.legend.protocol.graphql.GqlSchema.Schema;
import com.legend.protocol.graphql.GqlSchema.TypeDef;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.CTime;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.Gql;
import com.legend.protocol.spec.Gql.Argument;
import com.legend.protocol.spec.Gql.Field;
import com.legend.protocol.spec.Gql.Selection;
import com.legend.protocol.spec.Gql.Value;
import com.legend.protocol.spec.GraphFetchLiteral;
import com.legend.protocol.spec.GraphFetchLiteral.Node;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import com.legend.protocol.TypeExpression;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * GraphQL to Pure (docs/GRAPHQL_STUDY_2026_10_18.md §6): a validated document over the schema generated from the
 * class model, as the Pure expression a person would write, one per root field, typed once by the Typer through
 * {@code compiler/graphql/GqlIslands} or run by the endpoint. Every construct is a Pure spelling the database runs
 * (§6.1): a root is {@code Class.all()} with its clauses chained ({@code filter}, one {@code sortBy} or
 * {@code sortByReversed} per key, the last-applied sort the primary key, {@code drop}, {@code limit}), fetched and
 * serialized by the tree the selection spells; a condition is the Pure comparison its Prisma name means; a
 * {@code some} is {@code exists}; {@code every} and {@code none} are {@code exists} negated; {@code mode:
 * insensitive} lowers both sides. Java computes no value.
 *
 * <p>What this landing (PR2) spells: selections with aliases, arguments of derived properties, to-one and to-many
 * nodes, named and inline fragments on the selection's own type, {@code @include} and {@code @skip}, variables
 * with a value (substituted) or without one (the enclosing Pure scope's {@code $name}), the root clauses. Named by
 * the landing that builds them, and refused until then: a clause on a nested collection (the composed body,
 * PR2c), subtype fragments and {@code __typename} (PR3), aggregates (PR4), groups (PR5).
 */
public final class GqlToPure {

    /** One root field translated: its response key (the alias, else the name) and its Pure expression. */
    public record Root(String key, ValueSpecification expression) {

        /** The expression as the query lambda a runner executes ({@code |expression}). */
        public LambdaFunction asLambda() {
            return new LambdaFunction(List.of(), List.of(expression));
        }
    }

    /** The operation translated: its roots in document order. */
    public record Translation(List<Root> roots, Gql.Operation operation) {
        public Translation {
            roots = List.copyOf(roots);
        }
    }

    /**
     * A request's {@code variables} object as GraphQL values: a JSON string is a string (an enumeration's or a
     * date's value arrives so and is read by the position's type), a number an Int when integral else a Float,
     * an object an input object, an array a list.
     */
    public static Map<String, Value> variables(com.legend.json.Json.@com.legend.base.Nullable Obj variables) {
        Map<String, Value> out = new TreeMap<>();
        if (variables == null) {
            return out;
        }
        for (Map.Entry<String, com.legend.json.Json.Node> e : variables.fields().entrySet()) {
            out.put(e.getKey(), valueOf(e.getValue()));
        }
        return out;
    }

    /** A JSON value as the GraphQL value it is. */
    public static Value valueOf(com.legend.json.Json.Node node) {
        return switch (node) {
            case com.legend.json.Json.Null n -> new Gql.NullValue();
            case com.legend.json.Json.Bool b -> new Gql.BooleanValue(b.value());
            case com.legend.json.Json.Str s -> new Gql.StringValue(s.value());
            case com.legend.json.Json.Num n -> n.isInteger() ? new Gql.IntValue(n.longValue())
                    : new Gql.FloatValue(n.doubleValue());
            case com.legend.json.Json.Arr a -> {
                List<Value> items = new ArrayList<>();
                for (com.legend.json.Json.Node item : a.items()) {
                    items.add(valueOf(item));
                }
                yield new Gql.ListValue(items);
            }
            case com.legend.json.Json.Obj o -> {
                List<Argument> fields = new ArrayList<>();
                for (Map.Entry<String, com.legend.json.Json.Node> e : o.fields().entrySet()) {
                    fields.add(new Argument(e.getKey(), valueOf(e.getValue())));
                }
                yield new Gql.ObjectValue(fields);
            }
        };
    }

    private static final String WHERE = "where";
    private static final String ORDER_BY = "orderBy";
    private static final String LIMIT = "limit";
    private static final String OFFSET = "offset";
    private static final java.util.Set<String> CLAUSES = java.util.Set.of(WHERE, ORDER_BY, LIMIT, OFFSET);

    private final Schema schema;
    private final Map<String, Gql.Fragment> fragments = new TreeMap<>();
    /** A declared variable's value: the one given, else its default; absent when it is the enclosing scope's. */
    private final Map<String, Value> values = new TreeMap<>();
    private final java.util.Set<String> declared = new TreeSet<>();
    private int fresh;

    private GqlToPure(Schema schema) {
        this.schema = schema;
    }

    /**
     * Translates the operation {@code operationName} names (the document's only one when null) over
     * {@code schema}, with {@code variableValues} the values given for its variables (a value is the GraphQL value
     * the caller read; an enumeration or a date may arrive as a string). A declared variable without a value and
     * without a default is the enclosing Pure scope's variable of that name, read where a Pure value can stand.
     */
    public static Translation translate(Gql.Document document, Schema schema,
            @com.legend.base.Nullable String operationName, Map<String, Value> variableValues) {
        GqlToPure t = new GqlToPure(schema);
        Gql.Operation op = t.operation(document, operationName);
        for (Gql.Definition d : document.definitions()) {
            if (d instanceof Gql.Fragment f) {
                t.fragments.put(f.name(), f);
            }
        }
        for (Gql.VariableDef v : op.variables()) {
            String name = v.dollarName().startsWith("$") ? v.dollarName().substring(1) : v.dollarName();
            Value given = variableValues.get(name);
            if (given != null) {
                t.values.put(name, given);
            } else if (v.defaultValue() != null) {
                t.values.put(name, v.defaultValue());
            } else {
                t.declared.add(name);
            }
        }
        List<Root> roots = new ArrayList<>();
        boolean introspection = false;
        for (Field f : t.flatten(op.selectionSet(), schema.query(), "")) {
            if (Introspection.isRoot(f.name())) {
                introspection = true;   // the endpoint answers it from the schema
                continue;
            }
            String key = key(f);
            roots.add(new Root(key, t.root(f, key)));
        }
        if (roots.isEmpty() && !introspection) {
            throw GqlTranslationException.invalid("", "the operation selects nothing");
        }
        return new Translation(roots, op);
    }

    private Gql.Operation operation(Gql.Document document, @com.legend.base.Nullable String operationName) {
        List<Gql.Operation> ops = new ArrayList<>();
        for (Gql.Definition d : document.definitions()) {
            if (d instanceof Gql.Operation o) {
                ops.add(o);
            }
        }
        Gql.Operation op = null;
        if (operationName == null) {
            if (ops.size() != 1) {
                throw GqlTranslationException.invalid("", ops.isEmpty() ? "the document has no operation"
                        : "the document has " + ops.size() + " operations: name the one to run (operationName)");
            }
            op = ops.get(0);
        } else {
            for (Gql.Operation o : ops) {
                if (operationName.equals(o.name())) {
                    op = o;
                }
            }
            if (op == null) {
                throw GqlTranslationException.invalid("", "the document has no operation named " + operationName);
            }
        }
        if (op.type() != null && !op.type().equals("query")) {
            throw GqlTranslationException.unsupported("", "a " + op.type() + " is not a query: this line reads"
                    + " (docs/GRAPHQL_STUDY_2026_10_18.md §2.3)");
        }
        return op;
    }

    // ---- selections ----

    /**
     * The fields of a selection set on {@code type}, in document order, fragments inlined, {@code @include} and
     * {@code @skip} applied, fields with one response key merged (their selections concatenated, as the
     * specification's field merging has it; the validator checked they agree).
     */
    private List<Field> flatten(List<Selection> selections, Composite type, String path) {
        return level(selections, type, path).fields();
    }

    /** A selection set on {@code type}: its fields merged by response key, and the selections of each fragment on
     *  a subtype (an object type implementing {@code type}), by that type's name, in document order. */
    private record Level(List<Field> fields, Map<String, List<Selection>> subtypes) {
    }

    private Level level(List<Selection> selections, Composite type, String path) {
        Map<String, Field> byKey = new LinkedHashMap<>();
        Map<String, List<Selection>> subtypes = new LinkedHashMap<>();
        collect(selections, type, path, byKey, subtypes);
        return new Level(new ArrayList<>(byKey.values()), subtypes);
    }

    private void collect(List<Selection> selections, Composite type, String path, Map<String, Field> byKey,
            Map<String, List<Selection>> subtypes) {
        for (Selection s : selections) {
            switch (s) {
                case Field f -> {
                    if (included(f.directives(), path)) {
                        merge(byKey, f);
                    }
                }
                case Gql.FragmentSpread fs -> {
                    if (!included(fs.directives(), path)) {
                        continue;
                    }
                    Gql.Fragment fr = fragments.get(fs.name());
                    if (fr == null) {
                        throw GqlTranslationException.invalid(path, "no fragment named " + fs.name());
                    }
                    fragment(fr.typeCondition(), fr.selectionSet(), type, path, byKey, subtypes);
                }
                case Gql.InlineFragment inl -> {
                    if (!included(inl.directives(), path)) {
                        continue;
                    }
                    if (inl.typeCondition() == null) {
                        collect(inl.selectionSet(), type, path, byKey, subtypes);
                    } else {
                        fragment(inl.typeCondition(), inl.selectionSet(), type, path, byKey, subtypes);
                    }
                }
            }
        }
    }

    /** A fragment on the selection's own type, or on an interface it implements, merges in place; one on a
     *  subtype is kept under the subtype's name: the tree's {@code ->subType(@Sub) { ... }} entry (G8). */
    private void fragment(String condition, List<Selection> selections, Composite type, String path,
            Map<String, Field> byKey, Map<String, List<Selection>> subtypes) {
        if (condition.equals(type.name()) || type.implementsList().contains(condition)) {
            collect(selections, type, path, byKey, subtypes);
            return;
        }
        Composite sub = compositeNamed(condition, path);
        if (!sub.implementsList().contains(type.name())) {
            throw GqlTranslationException.invalid(path, "a fragment on " + condition + " inside a selection on "
                    + type.name() + ": " + condition + " does not implement it");
        }
        // an object type (a leaf class, or a class the validator let stand: G10) or a subclass's interface, which
        // is the subclass with its own subclasses: the tree's ->subType(@Sub) entry is the class either way
        subtypes.computeIfAbsent(condition, k -> new ArrayList<>()).addAll(selections);
    }

    /**
     * The tree's {@code ->subType(@Sub) { ... }} entries from a level's subtype fragments. The emitter's patches
     * are a first-match CASE over disjoint members, where GraphQL's fragments accumulate down the hierarchy (a
     * Manager is an Employee, so {@code ... on EmployeeInterface} applies to it too): each entry carries the
     * selections of every fragment its class satisfies, and the entries stand deepest first, so the first match
     * is the exact class with everything it inherits. A structural rearrangement; nothing is computed.
     */
    private List<GraphFetchLiteral.SubTypeNode> subTypeEntries(Level level, String path) {
        // by class, the object type's name: a fragment on Employee and one on EmployeeInterface are one class's
        Map<String, Composite> objectTypes = new LinkedHashMap<>();
        Map<String, List<Selection>> byClass = new LinkedHashMap<>();
        for (Map.Entry<String, List<Selection>> e : level.subtypes().entrySet()) {
            Composite c = compositeNamed(e.getKey(), path);
            String fqn = classFqnOf(c, path);
            objectTypes.putIfAbsent(fqn, objectTypeOf(fqn, path));
            byClass.computeIfAbsent(fqn, k -> new ArrayList<>()).addAll(e.getValue());
        }
        List<String> order = new ArrayList<>(byClass.keySet());
        // deepest first: a class below more of the entries' classes sits lower in the hierarchy
        Map<String, Integer> depth = new LinkedHashMap<>();
        for (String fqn : order) {
            int d = 0;
            for (String other : order) {
                if (!other.equals(fqn) && isA(objectTypes, fqn, other)) {
                    d++;
                }
            }
            depth.put(fqn, d);
        }
        order.sort((a, b) -> Integer.compare(depth.getOrDefault(b, 0), depth.getOrDefault(a, 0)));
        List<GraphFetchLiteral.SubTypeNode> out = new ArrayList<>();
        for (String fqn : order) {
            Composite own = Objects.requireNonNull(objectTypes.get(fqn));
            List<Selection> selections = new ArrayList<>();
            for (Map.Entry<String, List<Selection>> e : byClass.entrySet()) {
                if (e.getKey().equals(fqn) || isA(objectTypes, fqn, e.getKey())) {
                    selections.addAll(e.getValue());
                }
            }
            out.add(new GraphFetchLiteral.SubTypeNode(fqn, null, nodes(selections, own, path)));
        }
        return out;
    }

    /** Whether the class {@code sub} is the class {@code sup} or below it: its type implements {@code sup}'s
     *  interface (a class with subclasses has one), or {@code sup}'s own type name. */
    private static boolean isA(Map<String, Composite> objectTypes, String sub, String sup) {
        Composite s = Objects.requireNonNull(objectTypes.get(sub));
        Composite p = Objects.requireNonNull(objectTypes.get(sup));
        return s.implementsList().contains(p.name() + "Interface") || s.implementsList().contains(p.name());
    }

    /** The object type generated for the class {@code fqn} (its exact type, which every fragment on it or on its
     *  interface selects). */
    private Composite objectTypeOf(String fqn, String path) {
        for (TypeDef t : schema.types()) {
            if (t instanceof GqlSchema.ObjectType o && fqn.equals(o.classFqn())) {
                return o;
            }
        }
        // an abstract class has the interface only: its entry reads the interface's fields
        for (TypeDef t : schema.types()) {
            if (t instanceof GqlSchema.InterfaceType i && fqn.equals(i.classFqn())) {
                return i;
            }
        }
        throw GqlTranslationException.invalid(path, "no type is generated for " + fqn);
    }

    private Composite compositeNamed(String name, String path) {
        TypeDef td = schema.find(name);
        if (!(td instanceof Composite c)) {
            throw GqlTranslationException.invalid(path, "no object or interface type named " + name);
        }
        return c;
    }

    /** The class an object or interface type is generated for. */
    private static String classFqnOf(Composite c, String path) {
        String fqn = c instanceof GqlSchema.ObjectType o ? o.classFqn() : ((GqlSchema.InterfaceType) c).classFqn();
        if (fqn == null) {
            throw GqlTranslationException.invalid(path, c.name() + " is not a class's type");
        }
        return fqn;
    }

    /** The response key: the alias (the parser keeps the colon inside it, as the engine's wire has it), else
     *  the name. */
    public static String key(Field f) {
        String alias = f.alias();
        if (alias == null) {
            return f.name();
        }
        return alias.endsWith(":") ? alias.substring(0, alias.length() - 1) : alias;
    }

    private static void merge(Map<String, Field> byKey, Field f) {
        String key = key(f);
        Field seen = byKey.get(key);
        if (seen == null) {
            byKey.put(key, f);
            return;
        }
        List<Selection> merged = new ArrayList<>(seen.selectionSet());
        merged.addAll(f.selectionSet());
        byKey.put(key, new Field(seen.alias(), seen.name(), seen.arguments(), List.of(), merged));
    }

    /** {@code @include(if:)} and {@code @skip(if:)}, decided before typing (§6.1): the shape is per value. */
    private boolean included(List<Gql.Directive> directives, String path) {
        boolean in = true;
        for (Gql.Directive d : directives) {
            boolean include = d.name().equals("include");
            boolean skip = d.name().equals("skip");
            if (!include && !skip) {
                continue;
            }
            Value v = null;
            for (Argument a : d.arguments()) {
                if (a.name().equals("if")) {
                    v = resolve(a.value(), path + "@" + d.name());
                }
            }
            if (!(v instanceof Gql.BooleanValue b)) {
                throw GqlTranslationException.invalid(path, "@" + d.name() + " takes if: Boolean");
            }
            in &= include == b.value();
        }
        return in;
    }

    // ---- values ----

    /** A value with its variable substituted: the variable's value, or a refusal where a value is needed to shape
     *  the query (a variable the enclosing scope provides can stand only where a Pure value can). */
    private Value resolve(Value v, String path) {
        if (!(v instanceof Gql.VariableRef ref)) {
            return v;
        }
        Value given = values.get(ref.name());
        if (given != null) {
            return given;
        }
        if (declared.contains(ref.name())) {
            throw GqlTranslationException.invalid(path, "the shape of the query depends on $" + ref.name()
                    + ", which has no value: give it one, or put it where a value stands (a comparison, a limit)");
        }
        throw GqlTranslationException.invalid(path, "$" + ref.name() + " is not declared by the operation");
    }

    /** A value with a provided variable's value substituted; a variable the enclosing scope provides stays the
     *  reference it is, for a position where a Pure value stands ({@link #literal}). */
    private Value peek(Value v, String path) {
        if (v instanceof Gql.VariableRef ref && !values.containsKey(ref.name())) {
            if (declared.contains(ref.name())) {
                return v;
            }
            throw GqlTranslationException.invalid(path, "$" + ref.name() + " is not declared by the operation");
        }
        return resolve(v, path);
    }

    /** A value standing for a Pure value of {@code typeName} (a scalar or an enumeration of the schema). */
    private ValueSpecification literal(Value v, String typeName, String path) {
        if (v instanceof Gql.VariableRef ref) {
            Value given = values.get(ref.name());
            if (given != null) {
                return literal(given, typeName, path);
            }
            if (declared.contains(ref.name())) {
                return new Variable(ref.name());
            }
            throw GqlTranslationException.invalid(path, "$" + ref.name() + " is not declared by the operation");
        }
        TypeDef td = schema.find(typeName);
        if (td instanceof EnumType e) {
            String name = switch (v) {
                case Gql.EnumValue ev -> ev.value();
                case Gql.StringValue sv -> sv.value();   // a variable's value arrives as JSON text
                default -> throw GqlTranslationException.invalid(path, typeName + " takes one of " + e.values());
            };
            if (!e.values().contains(name)) {
                throw GqlTranslationException.invalid(path, typeName + " has no value " + name);
            }
            if (e.pureFqn() == null) {
                throw GqlTranslationException.invalid(path, typeName + " is not a value a query compares");
            }
            return new EnumValue(e.pureFqn(), name, null, null);
        }
        return switch (v) {
            case Gql.NullValue n -> new PureCollection(List.of());
            case Gql.ListValue l -> {
                List<ValueSpecification> items = new ArrayList<>();
                for (Value item : l.values()) {
                    items.add(literal(item, typeName, path));
                }
                yield new PureCollection(items);
            }
            case Gql.IntValue i -> switch (typeName) {
                case "Float" -> new CFloat((double) i.value());
                case "Decimal" -> new CDecimal(BigDecimal.valueOf(i.value()));
                default -> new CInteger(i.value(), null);
            };
            case Gql.FloatValue f -> "Decimal".equals(typeName) ? new CDecimal(BigDecimal.valueOf(f.value()))
                    : new CFloat(f.value());
            case Gql.BooleanValue b -> new CBoolean(b.value(), null);
            case Gql.StringValue s -> switch (typeName) {
                case "Date", "DateTime" -> {
                    try {
                        yield new CDate(PureDateLiteral.parse(s.value()));
                    } catch (IllegalArgumentException e) {
                        throw GqlTranslationException.invalid(path, "not a " + typeName + ": " + s.value());
                    }
                }
                case "Time" -> {
                    try {
                        yield new CTime(PureTimeLiteral.parse(s.value()));
                    } catch (IllegalArgumentException e) {
                        throw GqlTranslationException.invalid(path, "not a Time: " + s.value());
                    }
                }
                case "Decimal" -> {
                    try {
                        yield new CDecimal(new BigDecimal(s.value()));
                    } catch (NumberFormatException e) {
                        throw GqlTranslationException.invalid(path, "not a Decimal: " + s.value());
                    }
                }
                case "BigInt", "Int" -> {
                    try {
                        yield new CInteger(Long.parseLong(s.value()), null);
                    } catch (NumberFormatException e) {
                        throw GqlTranslationException.invalid(path, "not a " + typeName + ": " + s.value());
                    }
                }
                default -> new CString(s.value());
            };
            case Gql.EnumValue e -> throw GqlTranslationException.invalid(path, typeName + " is not an enumeration");
            case Gql.ObjectValue o -> throw GqlTranslationException.invalid(path, typeName + " is not an object");
            case Gql.VariableRef r -> throw new IllegalStateException("resolved above");
        };
    }

    private Gql.@com.legend.base.Nullable ObjectValue object(Value v, String path) {
        Value r = resolve(v, path);
        return switch (r) {
            case Gql.NullValue n -> null;
            case Gql.ObjectValue o -> o;
            default -> throw GqlTranslationException.invalid(path, "an object is expected here");
        };
    }

    private static @com.legend.base.Nullable Value argument(List<Argument> args, String name) {
        for (Argument a : args) {
            if (a.name().equals(name)) {
                return a.value();
            }
        }
        return null;
    }

    // ---- roots ----

    private ValueSpecification root(Field f, String path) {
        FieldDef fd = schema.query().field(f.name());
        if (fd == null) {
            throw GqlTranslationException.invalid(path, "the root field " + f.name() + " is not a query over the model");
        }
        return switch (fd.meaning()) {
            case Meaning.Root r -> {
                Composite type = composite(fd);
                ValueSpecification vs = call("getAll", new PackageableElementPtr(r.classFqn()));
                vs = clauses(vs, f.arguments(), type, path);
                Level level = level(f.selectionSet(), type, path);
                GraphFetchLiteral tree = new GraphFetchLiteral(r.classFqn(), nodesOf(level.fields(), type, path),
                        subTypeEntries(level, path), null);
                yield call("serialize", call("graphFetch", vs, tree), tree);
            }
            case Meaning.AggregateRoot a -> throw GqlTranslationException.unsupported(path, f.name()
                    + " aggregates: it lands with the aggregates landing (PR4, docs/GRAPHQL_STUDY_2026_10_18.md §10)");
            case Meaning.GroupsRoot g -> throw GqlTranslationException.unsupported(path, f.name()
                    + " groups: it lands with the groups landing (PR5, docs/GRAPHQL_STUDY_2026_10_18.md §10)");
            default -> throw GqlTranslationException.invalid(path, "not a root field: " + f.name());
        };
    }

    /** The object or interface type a field's type names. */
    private Composite composite(FieldDef fd) {
        TypeDef td = schema.find(GqlSchema.baseName(fd.type()));
        if (!(td instanceof Composite c)) {
            throw new IllegalStateException("field " + fd.name() + " of a non-composite type " + fd.type());
        }
        return c;
    }

    /** {@code where}, {@code orderBy}, {@code offset}, {@code limit} over a collection of {@code type}'s class,
     *  chained in the order Pure evaluates them (§6.1). */
    private ValueSpecification clauses(ValueSpecification coll, List<Argument> args, Composite type, String path) {
        ValueSpecification vs = coll;
        Value where = argument(args, WHERE);
        if (where != null) {
            Gql.ObjectValue w = object(where, path + "(where)");
            if (w != null) {
                String v = fresh("x");
                vs = call("filter", vs, lambda(v, where(w, whereType(type), new Variable(v), type,
                        path + "(where)")));
            }
        }
        Value orderBy = argument(args, ORDER_BY);
        if (orderBy != null) {
            Value r = resolve(orderBy, path + "(orderBy)");
            List<Value> keys = switch (r) {
                case Gql.NullValue n -> List.of();
                case Gql.ListValue l -> l.values();
                case Gql.ObjectValue o -> List.of(o);
                default -> throw GqlTranslationException.invalid(path, "orderBy takes a list of keys");
            };
            // the last-applied sort is the primary key (G16): the first key is applied last
            for (int i = keys.size() - 1; i >= 0; i--) {
                Gql.ObjectValue k = object(keys.get(i), path + "(orderBy)");
                if (k == null) {
                    continue;
                }
                SortKey sk = sortKey(k, orderByType(type), type, path + "(orderBy)");
                String v = fresh("x");
                vs = call(sk.ascending() ? "sortBy" : "sortByReversed", vs, lambda(v, sortRead(new Variable(v), sk)));
            }
        }
        Value offset = argument(args, OFFSET);
        if (offset != null && !(peek(offset, path) instanceof Gql.NullValue)) {
            vs = call("drop", vs, count(offset, path + "(offset)"));
        }
        Value limit = argument(args, LIMIT);
        if (limit != null && !(peek(limit, path) instanceof Gql.NullValue)) {
            vs = call("limit", vs, count(limit, path + "(limit)"));
        }
        return vs;
    }

    private ValueSpecification count(Value v, String path) {
        ValueSpecification n = literal(v, "Int", path);
        if (n instanceof CInteger c && c.value().longValue() < 0) {
            throw GqlTranslationException.invalid(path, "a count is not negative: " + c.value());
        }
        return n;
    }

    private InputObjectType whereType(Composite type) {
        return inputType(className(type) + "Where");
    }

    private InputObjectType orderByType(Composite type) {
        return inputType(className(type) + "OrderBy");
    }

    /** The class's simple name a composite is generated for: the object type's name, or the interface's without
     *  its suffix. */
    private static String className(Composite type) {
        return type instanceof GqlSchema.InterfaceType i && i.name().endsWith("Interface")
                ? i.name().substring(0, i.name().length() - "Interface".length()) : type.name();
    }

    private InputObjectType inputType(String name) {
        TypeDef td = schema.find(name);
        if (!(td instanceof InputObjectType in)) {
            throw new IllegalStateException("no input type " + name);
        }
        return in;
    }

    // ---- where ----

    /** The condition {@code where} spells over {@code subject}, a value of {@code type}'s class. */
    private ValueSpecification where(Gql.ObjectValue where, InputObjectType whereType, ValueSpecification subject,
            Composite type, String path) {
        List<ValueSpecification> conds = new ArrayList<>();
        for (Argument a : where.fields()) {
            String at = path + "." + a.name();
            Value v = resolve(a.value(), at);
            switch (a.name()) {
                case "AND" -> {
                    for (Gql.ObjectValue o : objects(v, at)) {
                        conds.add(where(o, whereType, subject, type, at));
                    }
                }
                case "OR" -> {
                    ValueSpecification or = null;
                    for (Gql.ObjectValue o : objects(v, at)) {
                        ValueSpecification c = where(o, whereType, subject, type, at);
                        or = or == null ? c : call("or", or, c);
                    }
                    conds.add(or == null ? new CBoolean(false, null) : or);
                }
                case "NOT" -> {
                    Gql.ObjectValue o = object(v, at);
                    if (o != null) {
                        conds.add(call("not", where(o, whereType, subject, type, at)));
                    }
                }
                case "is" -> {
                    if (v instanceof Gql.NullValue) {
                        conds.add(call("isEmpty", subject));
                    } else {
                        conds.add(where(Objects.requireNonNull(object(v, at)), whereType, subject, type, at));
                    }
                }
                case "isNot" -> {
                    if (v instanceof Gql.NullValue) {
                        conds.add(call("isNotEmpty", subject));
                    } else {
                        conds.add(call("not", where(Objects.requireNonNull(object(v, at)), whereType, subject,
                                type, at)));
                    }
                }
                default -> {
                    if (v instanceof Gql.NullValue) {
                        continue;   // a key given no condition (Prisma: a null filter filters nothing)
                    }
                    InputValueDef def = whereType.field(a.name());
                    if (def == null) {
                        throw GqlTranslationException.invalid(at, whereType.name() + " has no " + a.name());
                    }
                    FieldDef prop = type.field(a.name());
                    if (prop == null) {
                        // as<Sub>: the row as a subclass (G9): its conditions hold where the row is a Sub and
                        // the conditions hold over the row cast; `isNot: null` alone is the membership test
                        conds.add(asSubtype(a.name(), v, def, subject, at));
                        continue;
                    }
                    conds.add(condition(a.name(), v, def, prop, subject, at));
                }
            }
        }
        if (conds.isEmpty()) {
            return new CBoolean(true, null);
        }
        ValueSpecification and = conds.get(0);
        for (int i = 1; i < conds.size(); i++) {
            and = call("and", and, conds.get(i));
        }
        return and;
    }

    /** {@code asSub: W}: {@code $x->instanceOf(Sub) && W($x->cast(@Sub))}; {@code asSub: { isNot: null }} is the
     *  membership test alone, {@code asSub: { is: null }} its negation. The cast's reads are evaluated only where
     *  the membership holds (the conjunction's first operand; the database's CASE guards the rest). */
    private ValueSpecification asSubtype(String key, Value v, InputValueDef def, ValueSpecification subject,
            String path) {
        Gql.ObjectValue o = object(v, path);
        if (o == null) {
            return new CBoolean(true, null);
        }
        InputObjectType subWhere = inputType(GqlSchema.baseName(def.type()));
        String subName = subWhere.name().substring(0, subWhere.name().length() - "Where".length());
        Composite sub = compositeNamed(subName, path);
        String subFqn = classFqnOf(sub, path);
        ValueSpecification member = call("instanceOf", subject, new PackageableElementPtr(subFqn));
        ValueSpecification cast = call("cast", subject, new TypeAnnotation.Named(new TypeExpression.NameRef(subFqn)));
        List<Argument> rest = new ArrayList<>();
        ValueSpecification out = member;
        for (Argument a : o.fields()) {
            Value av = resolve(a.value(), path + "." + a.name());
            if (a.name().equals("isNot") && av instanceof Gql.NullValue) {
                continue;   // membership: the test itself
            }
            if (a.name().equals("is") && av instanceof Gql.NullValue) {
                return call("not", member);
            }
            rest.add(a);
        }
        if (!rest.isEmpty()) {
            ValueSpecification inner = where(new Gql.ObjectValue(rest), subWhere, cast, sub, path);
            out = call("and", member, call("if", member, thunk(inner), thunk(new CBoolean(false, null))));
        }
        return out;
    }

    private List<Gql.ObjectValue> objects(Value v, String path) {
        List<Gql.ObjectValue> out = new ArrayList<>();
        switch (v) {
            case Gql.NullValue n -> { }
            case Gql.ObjectValue o -> out.add(o);
            case Gql.ListValue l -> {
                for (Value item : l.values()) {
                    Gql.ObjectValue o = object(item, path);
                    if (o != null) {
                        out.add(o);
                    }
                }
            }
            default -> throw GqlTranslationException.invalid(path, "a condition or a list of them is expected");
        }
        return out;
    }

    /** One property's condition: a scalar or enumeration filter, a to-one class's nested conditions, or a
     *  to-many class's {@code some}/{@code every}/{@code none}. */
    private ValueSpecification condition(String name, Value v, InputValueDef def, FieldDef prop,
            ValueSpecification subject, String path) {
        Gql.ObjectValue o = object(v, path);
        if (o == null) {
            return new CBoolean(true, null);
        }
        InputObjectType in = inputType(GqlSchema.baseName(def.type()));
        ValueSpecification read = new AppliedProperty(subject, name);
        if (!(prop.meaning() instanceof Meaning.Property p)) {
            throw GqlTranslationException.invalid(path, name + " is not a property");
        }
        Composite target = schema.find(GqlSchema.baseName(prop.type())) instanceof Composite c ? c : null;
        if (target == null) {
            // a scalar's or an enumeration's filter: its equals field names the value type
            InputValueDef eq = in.field("equals");
            if (eq == null) {
                throw new IllegalStateException("filter " + in.name() + " without equals");
            }
            return scalarFilter(o, in, read, GqlSchema.baseName(eq.type()), path);
        }
        if (p.toMany()) {
            return listFilter(o, in, read, target, path);
        }
        return where(o, in, read, target, path);
    }

    private ValueSpecification scalarFilter(Gql.ObjectValue ops, InputObjectType in, ValueSpecification read,
            String scalar, String path) {
        boolean insensitive = false;
        for (Argument a : ops.fields()) {
            if (a.name().equals("mode")) {
                Value m = resolve(a.value(), path + ".mode");
                String mode = switch (m) {
                    case Gql.EnumValue e -> e.value();
                    case Gql.StringValue s -> s.value();
                    case Gql.NullValue n -> "default";
                    default -> throw GqlTranslationException.invalid(path + ".mode", "mode takes a QueryMode");
                };
                insensitive = mode.equals("insensitive");
            }
        }
        List<ValueSpecification> conds = new ArrayList<>();
        for (Argument a : ops.fields()) {
            String at = path + "." + a.name();
            Value v = peek(a.value(), at);
            if (in.field(a.name()) == null) {
                throw GqlTranslationException.invalid(at, in.name() + " has no " + a.name());
            }
            switch (a.name()) {
                case "mode" -> { }
                case "equals" -> conds.add(v instanceof Gql.NullValue ? call("isEmpty", read)
                        : call("equal", side(read, insensitive), side(literal(v, scalar, at), insensitive)));
                case "not" -> {
                    if (v instanceof Gql.NullValue) {
                        conds.add(call("isNotEmpty", read));
                    } else if (v instanceof Gql.ObjectValue nested) {
                        conds.add(call("not", scalarFilter(nested, in, read, scalar, at)));
                    } else {
                        conds.add(call("not", call("equal", side(read, insensitive),
                                side(literal(v, scalar, at), insensitive))));
                    }
                }
                case "in", "notIn" -> {
                    if (v instanceof Gql.NullValue) {
                        continue;
                    }
                    ValueSpecification set = literal(v instanceof Gql.ListValue ? v : new Gql.ListValue(List.of(v)),
                            scalar, at);
                    if (insensitive && set instanceof PureCollection pc) {
                        List<ValueSpecification> lowered = new ArrayList<>();
                        for (ValueSpecification item : pc.values()) {
                            lowered.add(call("toLower", item));
                        }
                        set = new PureCollection(lowered);
                    }
                    ValueSpecification isIn = call("in", side(read, insensitive), set);
                    conds.add(a.name().equals("in") ? isIn : call("not", isIn));
                }
                case "lt", "lte", "gt", "gte" -> {
                    if (v instanceof Gql.NullValue) {
                        continue;
                    }
                    String fn = switch (a.name()) {
                        case "lt" -> "lessThan";
                        case "lte" -> "lessThanEqual";
                        case "gt" -> "greaterThan";
                        default -> "greaterThanEqual";
                    };
                    conds.add(call(fn, call("toOne", read), literal(v, scalar, at)));
                }
                case "contains", "startsWith", "endsWith" -> {
                    if (v instanceof Gql.NullValue) {
                        continue;
                    }
                    conds.add(call(a.name(), side(call("toOne", read), insensitive),
                            side(literal(v, scalar, at), insensitive)));
                }
                default -> throw GqlTranslationException.invalid(at, "no such filter operation: " + a.name());
            }
        }
        if (conds.isEmpty()) {
            return new CBoolean(true, null);
        }
        ValueSpecification and = conds.get(0);
        for (int i = 1; i < conds.size(); i++) {
            and = call("and", and, conds.get(i));
        }
        return and;
    }

    private static ValueSpecification side(ValueSpecification v, boolean insensitive) {
        return insensitive ? call("toLower", v) : v;
    }

    private ValueSpecification listFilter(Gql.ObjectValue ops, InputObjectType in, ValueSpecification read,
            Composite target, String path) {
        List<ValueSpecification> conds = new ArrayList<>();
        InputObjectType elementWhere = whereType(target);
        for (Argument a : ops.fields()) {
            String at = path + "." + a.name();
            Gql.ObjectValue w = object(a.value(), at);
            if (w == null) {
                continue;
            }
            String e = fresh("e");
            ValueSpecification inner = where(w, elementWhere, new Variable(e), target, at);
            conds.add(switch (a.name()) {
                case "some" -> call("exists", read, lambda(e, inner));
                case "none" -> call("not", call("exists", read, lambda(e, inner)));
                case "every" -> call("not", call("exists", read, lambda(e, call("not", inner))));
                default -> throw GqlTranslationException.invalid(at, in.name() + " has no " + a.name());
            });
        }
        if (conds.isEmpty()) {
            return new CBoolean(true, null);
        }
        ValueSpecification and = conds.get(0);
        for (int i = 1; i < conds.size(); i++) {
            and = call("and", and, conds.get(i));
        }
        return and;
    }

    // ---- orderBy ----

    /** A key: the property chain, the direction, and the subtype the chain reads through ({@code asSub}; null
     *  when none): the key is {@code if($x->instanceOf(Sub), |$x->cast(@Sub).chain, |[])}. */
    private record SortKey(List<String> path, boolean ascending, @com.legend.base.Nullable String subtypeFqn) {
        SortKey(List<String> path, boolean ascending) {
            this(path, ascending, null);
        }
    }

    /** One key object: a single property with a direction, or a to-one property's own key object (a chain). */
    private SortKey sortKey(Gql.ObjectValue key, InputObjectType orderByType, Composite type, String path) {
        List<Argument> given = new ArrayList<>();
        for (Argument a : key.fields()) {
            if (!(resolve(a.value(), path) instanceof Gql.NullValue)) {
                given.add(a);
            }
        }
        if (given.size() != 1) {
            throw GqlTranslationException.invalid(path, "a sort key names exactly one property (G16): "
                    + orderByType.name() + " got " + given.size());
        }
        Argument a = given.get(0);
        String at = path + "." + a.name();
        InputValueDef def = orderByType.field(a.name());
        if (def == null) {
            throw GqlTranslationException.invalid(at, orderByType.name() + " has no " + a.name());
        }
        Value v = resolve(a.value(), at);
        String typeName = GqlSchema.baseName(def.type());
        if (typeName.equals("SortDirection")) {
            String dir = switch (v) {
                case Gql.EnumValue e -> e.value();
                case Gql.StringValue s -> s.value();
                default -> throw GqlTranslationException.invalid(at, "a direction, ASC or DESC");
            };
            if (!dir.equals("ASC") && !dir.equals("DESC")) {
                throw GqlTranslationException.invalid(at, "a direction, ASC or DESC: " + dir);
            }
            return new SortKey(List.of(a.name()), dir.equals("ASC"));
        }
        FieldDef prop = type.field(a.name());
        if (prop == null) {
            // as<Sub>: the key is the subtype's, read where the row is a member, empty elsewhere
            InputObjectType subOrder = inputType(typeName);
            String subName = subOrder.name().substring(0, subOrder.name().length() - "OrderBy".length());
            Composite sub = compositeNamed(subName, at);
            if (!(v instanceof Gql.ObjectValue o)) {
                throw GqlTranslationException.invalid(at, "a key object is expected under " + a.name());
            }
            SortKey inner = sortKey(o, subOrder, sub, at);
            return new SortKey(inner.path(), inner.ascending(), classFqnOf(sub, at));
        }
        if (typeName.endsWith("AggregateOrderBy")) {
            throw GqlTranslationException.unsupported(at, a.name() + " sorts by an aggregate: it lands with the"
                    + " aggregates landing (PR4)");
        }
        Composite target = schema.find(GqlSchema.baseName(prop.type())) instanceof Composite c ? c : null;
        if (target == null || !(v instanceof Gql.ObjectValue o)) {
            throw GqlTranslationException.invalid(at, "a key object is expected under " + a.name());
        }
        SortKey inner = sortKey(o, inputType(typeName), target, at);
        List<String> chain = new ArrayList<>();
        chain.add(a.name());
        chain.addAll(inner.path());
        return new SortKey(chain, inner.ascending());
    }

    private static ValueSpecification sortRead(ValueSpecification row, SortKey sk) {
        if (sk.subtypeFqn() == null) {
            return navigate(row, sk.path());
        }
        ValueSpecification cast = call("cast", row, new TypeAnnotation.Named(new TypeExpression.NameRef(sk.subtypeFqn())));
        // toOne: the key lambda is {T[1]->U[1]}; the empty arm is the null the database sorts
        return call("toOne", call("if", call("instanceOf", row, new PackageableElementPtr(sk.subtypeFqn())),
                thunk(navigate(cast, sk.path())), thunk(new PureCollection(List.of()))));
    }

    private static LambdaFunction thunk(ValueSpecification body) {
        return new LambdaFunction(List.of(), List.of(body));
    }

    private static ValueSpecification navigate(ValueSpecification from, List<String> path) {
        ValueSpecification v = from;
        for (String p : path) {
            v = new AppliedProperty(v, p);
        }
        return v;
    }

    // ---- the tree ----

    private List<Node> nodes(List<Selection> selections, Composite type, String path) {
        return nodesOf(flatten(selections, type, path), type, path);
    }

    private List<Node> nodesOf(List<Field> fields, Composite type, String path) {
        List<Node> out = new ArrayList<>();
        for (Field f : fields) {
            out.add(node(f, type, path));
        }
        return out;
    }

    private Node node(Field f, Composite type, String parentPath) {
        String key = key(f);
        String path = parentPath.isEmpty() ? key : parentPath + "." + key;
        return switch (f.name()) {
            case "__typename" -> new Node("__typename", null, List.of(), key, null, List.of());
            default -> node0(f, type, key, path);
        };
    }

    private Node node0(Field f, Composite type, String key, String path) {
        FieldDef fd = type.field(f.name());
        if (fd == null) {
            throw GqlTranslationException.invalid(path, type.name() + " has no field " + f.name());
        }
        return switch (fd.meaning()) {
            case Meaning.Property p -> {
                List<ValueSpecification> args = new ArrayList<>();
                boolean clause = false;
                for (InputValueDef a : fd.args()) {
                    boolean collection = p.toMany() && CLAUSES.contains(a.name());
                    Value given = argument(f.arguments(), a.name());
                    if (collection) {
                        clause |= given != null && !(peek(given, path) instanceof Gql.NullValue);
                        continue;
                    }
                    if (given == null) {
                        if (!a.type().nullable()) {
                            throw GqlTranslationException.invalid(path, f.name() + " takes " + a.name());
                        }
                        args.add(new PureCollection(List.of()));
                    } else {
                        args.add(literal(given, GqlSchema.baseName(a.type()), path + "(" + a.name() + ")"));
                    }
                }
                List<Node> children = List.of();
                String view = null;
                if (!f.selectionSet().isEmpty()) {
                    Composite target = composite(fd);
                    Level level = level(f.selectionSet(), target, path);
                    if (!level.subtypes().isEmpty()) {
                        // the engine's tree has a subtype VIEW under a node (prop->subType(@Sub) { ... }), every
                        // child read as the subtype's; a subtype's fields beside the type's own, or two
                        // subtypes, wait on a nested subtype entry (PR3's not-done list)
                        if (!level.fields().isEmpty() || level.subtypes().size() != 1) {
                            throw GqlTranslationException.unsupported(path, "a fragment on a subtype under "
                                    + f.name() + " beside other fields: a nested selection reads one subtype,"
                                    + " alone (... on " + level.subtypes().keySet() + ")");
                        }
                        Map.Entry<String, List<Selection>> only = level.subtypes().entrySet().iterator().next();
                        Composite sub = compositeNamed(only.getKey(), path);
                        view = classFqnOf(sub, path);
                        children = nodes(only.getValue(), sub, path);
                    } else {
                        children = nodesOf(level.fields(), target, path);
                    }
                }
                if (clause) {
                    // the COMPOSED BODY (§6.2): the clauses over $this.<property>, keyed by the response key
                    if (!args.isEmpty()) {
                        throw GqlTranslationException.unsupported(path, "a clause on the derived collection "
                                + f.name() + ", which takes arguments: a composed body navigates a stored"
                                + " property (docs/GRAPHQL_STUDY_2026_10_18.md §6.2)");
                    }
                    ValueSpecification body = clauses(new AppliedProperty(new Variable("this"), p.name()),
                            f.arguments(), composite(fd), path);
                    yield new Node(p.name(), null, List.of(), key, null, children, body);
                }
                // a derived leaf is keyed by its call spelling ("fullName()") unless aliased: the response
                // key is the field's, so a derived property always carries it
                yield new Node(p.name(), null, args, f.alias() == null && !p.derived() ? null : key, view,
                        children);
            }
            case Meaning.AggregateOf a -> throw GqlTranslationException.unsupported(path, f.name()
                    + " aggregates: it lands with the aggregates landing (PR4)");
            case Meaning.GroupsOf g -> throw GqlTranslationException.unsupported(path, f.name()
                    + " groups: it lands with the groups landing (PR5)");
            case Meaning.Typename t -> new Node("__typename", null, List.of(), key, null, List.of());
            default -> throw GqlTranslationException.invalid(path, f.name() + " is not selectable here");
        };
    }

    // ---- spelling helpers ----

    private String fresh(String stem) {
        return stem + (fresh++ == 0 ? "" : Integer.toString(fresh - 1));
    }

    private static AppliedFunction call(String fn, ValueSpecification... args) {
        return new AppliedFunction(fn, List.of(args));
    }

    private static LambdaFunction lambda(String parameter, ValueSpecification body) {
        return new LambdaFunction(List.of(new Variable(parameter)), List.of(body));
    }
}
