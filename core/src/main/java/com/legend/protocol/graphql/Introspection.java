// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.graphql;

import com.legend.json.Json;
import com.legend.protocol.graphql.GqlSchema.Composite;
import com.legend.protocol.graphql.GqlSchema.EnumType;
import com.legend.protocol.graphql.GqlSchema.FieldDef;
import com.legend.protocol.graphql.GqlSchema.InputObjectType;
import com.legend.protocol.graphql.GqlSchema.InputValueDef;
import com.legend.protocol.graphql.GqlSchema.InterfaceType;
import com.legend.protocol.graphql.GqlSchema.ObjectType;
import com.legend.protocol.graphql.GqlSchema.ScalarType;
import com.legend.protocol.graphql.GqlSchema.Schema;
import com.legend.protocol.graphql.GqlSchema.TypeDef;
import com.legend.protocol.graphql.GqlSchema.TypeRef;
import com.legend.protocol.spec.Gql;
import com.legend.protocol.spec.Gql.Field;
import com.legend.protocol.spec.Gql.Selection;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Introspection (GraphQL September 2025 specification §4; docs/GRAPHQL_STUDY_2026_10_18.md §8.1): {@code __schema}
 * and {@code __type(name:)} answered from the generated schema. The whole introspection graph is built as JSON
 * ({@code __Schema}, {@code __Type}, {@code __Field}, {@code __InputValue}, {@code __EnumValue},
 * {@code __Directive}) and the client's selection is projected over it -- aliases kept, fragments inlined, every
 * deprecated member included whatever {@code includeDeprecated} says -- so GraphiQL's and every agent's
 * introspection query reads exactly the fields it asked for. Nothing here is computed from data: it is the
 * schema, spelled.
 */
public final class Introspection {

    private Introspection() {
    }

    /** Whether {@code field} is an introspection root ({@code __schema}, {@code __type}, {@code __typename}). */
    public static boolean isRoot(String fieldName) {
        return fieldName.equals("__schema") || fieldName.equals("__type") || fieldName.equals("__typename");
    }

    /**
     * The answer to one introspection root field: {@code __schema { ... }}, {@code __type(name: "X") { ... }}
     * or the root's {@code __typename} ({@code "Query"}); {@code fragments} are the document's, by name.
     */
    public static Json.Node answer(Field root, Schema schema, Map<String, Gql.Fragment> fragments) {
        Graph g = new Graph(schema);
        return switch (root.name()) {
            case "__typename" -> Json.str("Query");
            case "__schema" -> project(g.schemaNode(), root.selectionSet(), fragments);
            case "__type" -> {
                String name = null;
                for (Gql.Argument a : root.arguments()) {
                    if (a.name().equals("name") && a.value() instanceof Gql.StringValue s) {
                        name = s.value();
                    }
                }
                if (name == null) {
                    throw GqlTranslationException.invalid("__type", "__type takes name: String!");
                }
                Json.Node type = g.typeNode(name);
                yield type == null ? Json.nil() : project(type, root.selectionSet(), fragments);
            }
            default -> throw new IllegalArgumentException("not an introspection root: " + root.name());
        };
    }

    // ---- the projection ----

    private static Json.Node project(Json.Node value, List<Selection> selections, Map<String, Gql.Fragment> fragments) {
        if (selections.isEmpty() || value instanceof Json.Null) {
            return value;
        }
        if (value instanceof Json.Arr arr) {
            List<Json.Node> items = new ArrayList<>();
            for (Json.Node item : arr.items()) {
                items.add(project(item, selections, fragments));
            }
            return new Json.Arr(items);
        }
        if (!(value instanceof Json.Obj obj)) {
            return value;
        }
        Json.Obj out = Json.obj();
        for (Field f : fields(selections, fragments)) {
            String key = GqlToPure.key(f);
            Json.Node v = f.name().equals("__typename") ? obj.fields().get("__typename") : obj.fields().get(f.name());
            if (v == null) {
                throw GqlTranslationException.invalid(key, "no introspection field " + f.name() + " on "
                        + obj.fields().getOrDefault("__typename", Json.str("?")));
            }
            out.fields().put(key, project(v, f.selectionSet(), fragments));
        }
        return out;
    }

    private static List<Field> fields(List<Selection> selections, Map<String, Gql.Fragment> fragments) {
        List<Field> out = new ArrayList<>();
        for (Selection s : selections) {
            switch (s) {
                case Field f -> out.add(f);
                case Gql.InlineFragment inl -> out.addAll(fields(inl.selectionSet(), fragments));
                case Gql.FragmentSpread fs -> {
                    Gql.Fragment fr = fragments.get(fs.name());
                    if (fr == null) {
                        throw GqlTranslationException.invalid("", "no fragment named " + fs.name());
                    }
                    out.addAll(fields(fr.selectionSet(), fragments));
                }
            }
        }
        return out;
    }

    // ---- the graph ----

    private static final class Graph {

        private final Schema schema;
        private final Map<String, Json.Node> types = new TreeMap<>();

        Graph(Schema schema) {
            this.schema = schema;
        }

        Json.Node schemaNode() {
            Json.Obj out = obj("__Schema");
            out.fields().put("description", Json.nil());
            List<Json.Node> all = new ArrayList<>();
            for (TypeDef t : schema.types()) {
                all.add(typeNode(t.name()));
            }
            for (String builtin : List.of("Int", "Float", "String", "Boolean", "ID")) {
                if (schema.find(builtin) == null) {
                    all.add(scalarNode(builtin, null));
                }
            }
            all.addAll(introspectionTypes());
            out.fields().put("types", new Json.Arr(all));
            out.fields().put("queryType", ref("Query"));
            out.fields().put("mutationType", Json.nil());
            out.fields().put("subscriptionType", Json.nil());
            out.fields().put("directives", new Json.Arr(List.of(
                    directive("include", "Directs the executor to include this field or fragment only when the"
                            + " `if` argument is true.", List.of("FIELD", "FRAGMENT_SPREAD", "INLINE_FRAGMENT"),
                            List.of(inputValue("if", "Included when true.", nonNull("Boolean"), null))),
                    directive("skip", "Directs the executor to skip this field or fragment when the `if` argument"
                            + " is true.", List.of("FIELD", "FRAGMENT_SPREAD", "INLINE_FRAGMENT"),
                            List.of(inputValue("if", "Skipped when true.", nonNull("Boolean"), null))),
                    directive("deprecated", "Marks an element of a GraphQL schema as no longer supported.",
                            List.of("FIELD_DEFINITION", "ARGUMENT_DEFINITION", "INPUT_FIELD_DEFINITION",
                                    "ENUM_VALUE"),
                            List.of(inputValue("reason", "Explains why this element was deprecated.",
                                    named("String"), "\"No longer supported\""))))));
            return out;
        }

        /** The full {@code __Type} of {@code name}, or null when the schema has no such type. */
        Json.@com.legend.base.Nullable Node typeNode(String name) {
            Json.Node cached = types.get(name);
            if (cached != null) {
                return cached;
            }
            TypeDef t = schema.find(name);
            Json.Obj out;
            if (t == null) {
                if (List.of("Int", "Float", "String", "Boolean", "ID").contains(name)) {
                    out = scalarNode(name, null);
                } else {
                    return null;
                }
            } else {
                out = switch (t) {
                    case ScalarType s -> scalarNode(s.name(), s.description());
                    case EnumType e -> enumNode(e);
                    case InputObjectType in -> inputObjectNode(in);
                    case ObjectType o -> compositeNode(o, "OBJECT");
                    case InterfaceType i -> compositeNode(i, "INTERFACE");
                };
            }
            return out;
        }

        private Json.Obj compositeNode(Composite c, String kind) {
            Json.Obj out = typeShell(kind, c.name(), c instanceof ObjectType o ? o.description()
                    : ((InterfaceType) c).description());
            List<Json.Node> fields = new ArrayList<>();
            for (FieldDef f : c.fields()) {
                Json.Obj fo = obj("__Field");
                fo.fields().put("name", Json.str(f.name()));
                fo.fields().put("description", f.description() == null ? Json.nil() : Json.str(f.description()));
                List<Json.Node> args = new ArrayList<>();
                for (InputValueDef a : f.args()) {
                    args.add(inputValue(a.name(), a.description(), typeRef(a.type()), null));
                }
                fo.fields().put("args", new Json.Arr(args));
                fo.fields().put("type", typeRef(f.type()));
                fo.fields().put("isDeprecated", Json.bool(f.deprecationReason() != null));
                fo.fields().put("deprecationReason", f.deprecationReason() == null ? Json.nil()
                        : Json.str(f.deprecationReason()));
                fields.add(fo);
            }
            out.fields().put("fields", new Json.Arr(fields));
            List<Json.Node> interfaces = new ArrayList<>();
            for (String i : c.implementsList()) {
                interfaces.add(ref(i));
            }
            out.fields().put("interfaces", new Json.Arr(interfaces));
            if (c instanceof InterfaceType) {
                List<Json.Node> possible = new ArrayList<>();
                for (TypeDef t : schema.types()) {
                    if (t instanceof Composite other && other.implementsList().contains(c.name())) {
                        possible.add(ref(other.name()));
                    }
                }
                out.fields().put("possibleTypes", new Json.Arr(possible));
            }
            return out;
        }

        private Json.Obj inputObjectNode(InputObjectType in) {
            Json.Obj out = typeShell("INPUT_OBJECT", in.name(), null);
            List<Json.Node> fields = new ArrayList<>();
            for (InputValueDef f : in.fields()) {
                fields.add(inputValue(f.name(), f.description(), typeRef(f.type()), null));
            }
            out.fields().put("inputFields", new Json.Arr(fields));
            out.fields().put("isOneOf", Json.bool(false));
            return out;
        }

        private Json.Obj enumNode(EnumType e) {
            Json.Obj out = typeShell("ENUM", e.name(), e.description());
            List<Json.Node> values = new ArrayList<>();
            for (String v : e.values()) {
                Json.Obj vo = obj("__EnumValue");
                vo.fields().put("name", Json.str(v));
                vo.fields().put("description", Json.nil());
                vo.fields().put("isDeprecated", Json.bool(false));
                vo.fields().put("deprecationReason", Json.nil());
                values.add(vo);
            }
            out.fields().put("enumValues", new Json.Arr(values));
            return out;
        }

        private Json.Obj scalarNode(String name, @com.legend.base.Nullable String description) {
            Json.Obj out = typeShell("SCALAR", name, description);
            out.fields().put("specifiedByURL", Json.nil());
            return out;
        }

        /** Every {@code __Type} member, null where the kind has none: a client may select any of them. The shell
         *  is cached before its members are filled, so a cycle of references (Person.firm, Firm.employees) reads
         *  the one node; the projection builds a fresh, finite answer from it. */
        private Json.Obj typeShell(String kind, String name, @com.legend.base.Nullable String description) {
            Json.Obj out = obj("__Type");
            types.put(name, out);
            out.fields().put("kind", Json.str(kind));
            out.fields().put("name", Json.str(name));
            out.fields().put("description", description == null ? Json.nil() : Json.str(description));
            out.fields().put("specifiedByURL", Json.nil());
            out.fields().put("fields", Json.nil());
            out.fields().put("interfaces", Json.nil());
            out.fields().put("possibleTypes", Json.nil());
            out.fields().put("enumValues", Json.nil());
            out.fields().put("inputFields", Json.nil());
            out.fields().put("ofType", Json.nil());
            out.fields().put("isOneOf", Json.nil());
            return out;
        }

        /** A type reference: {@code NON_NULL} and {@code LIST} wrappers around a named reference. */
        private Json.Node typeRef(TypeRef ref) {
            Json.Node inner = switch (ref) {
                case GqlSchema.Named n -> ref(n.name());
                case GqlSchema.ListOf l -> wrap("LIST", typeRef(l.item()));
            };
            return ref.nullable() ? inner : wrap("NON_NULL", inner);
        }

        private Json.Node named(String name) {
            return ref(name);
        }

        private Json.Node nonNull(String name) {
            return wrap("NON_NULL", ref(name));
        }

        private Json.Obj wrap(String kind, Json.Node of) {
            Json.Obj out = obj("__Type");
            out.fields().put("kind", Json.str(kind));
            out.fields().put("name", Json.nil());
            out.fields().put("description", Json.nil());
            out.fields().put("specifiedByURL", Json.nil());
            out.fields().put("fields", Json.nil());
            out.fields().put("interfaces", Json.nil());
            out.fields().put("possibleTypes", Json.nil());
            out.fields().put("enumValues", Json.nil());
            out.fields().put("inputFields", Json.nil());
            out.fields().put("ofType", of);
            out.fields().put("isOneOf", Json.nil());
            return out;
        }

        /** A named reference: the type's kind and name, its members, {@code ofType} null. A reference carries the
         *  full node so that a deep selection ({@code type { fields { ... } }}) can read through it. */
        private Json.Node ref(String name) {
            Json.Node full = typeNode(name);
            if (full == null) {
                throw new IllegalStateException("introspection: no type " + name);
            }
            return full;
        }

        private Json.Node inputValue(String name, @com.legend.base.Nullable String description, Json.Node type,
                @com.legend.base.Nullable String defaultValue) {
            Json.Obj out = obj("__InputValue");
            out.fields().put("name", Json.str(name));
            out.fields().put("description", description == null ? Json.nil() : Json.str(description));
            out.fields().put("type", type);
            out.fields().put("defaultValue", defaultValue == null ? Json.nil() : Json.str(defaultValue));
            out.fields().put("isDeprecated", Json.bool(false));
            out.fields().put("deprecationReason", Json.nil());
            return out;
        }

        private Json.Node directive(String name, String description, List<String> locations,
                List<Json.Node> args) {
            Json.Obj out = obj("__Directive");
            out.fields().put("name", Json.str(name));
            out.fields().put("description", Json.str(description));
            List<Json.Node> locs = new ArrayList<>();
            for (String l : locations) {
                locs.add(Json.str(l));
            }
            out.fields().put("locations", new Json.Arr(locs));
            out.fields().put("args", new Json.Arr(args));
            out.fields().put("isRepeatable", Json.bool(false));
            return out;
        }

        /** The introspection types themselves, as every introspection answer lists them. */
        private List<Json.Node> introspectionTypes() {
            List<Json.Node> out = new ArrayList<>();
            for (String n : List.of("__Schema", "__Type", "__Field", "__InputValue", "__EnumValue", "__Directive")) {
                Json.Obj t = typeShell("OBJECT", n, null);
                t.fields().put("fields", new Json.Arr(List.of()));
                t.fields().put("interfaces", new Json.Arr(List.of()));
                out.add(t);
            }
            for (String n : List.of("__TypeKind", "__DirectiveLocation")) {
                Json.Obj t = typeShell("ENUM", n, null);
                t.fields().put("enumValues", new Json.Arr(List.of()));
                out.add(t);
            }
            return out;
        }

        private static Json.Obj obj(String typename) {
            Json.Obj out = new Json.Obj(new LinkedHashMap<>());
            out.fields().put("__typename", Json.str(typename));
            return out;
        }
    }
}
