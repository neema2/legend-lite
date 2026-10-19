// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.graphql;

import com.legend.protocol.spec.Gql;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The generated GraphQL schema as data (docs/GRAPHQL_STUDY_2026_10_18.md §5): what {@link SchemaGenerator} builds
 * from a Pure model, what {@link SdlPrinter} prints, what {@link GqlValidator} validates a document against. Every
 * field carries its {@link Meaning}, the Pure it stands for, so that the translator (PR2) walks the validated
 * document without a second lookup. Type references are the schema's own ({@link TypeRef}): the protocol's nodes
 * are the parser's to build; a variable declaration's reference is read into one ({@link TypeRef#of}).
 */
public final class GqlSchema {

    private GqlSchema() {
    }

    /** The schema: its types in print order ({@code Query} first, then by name), indexed by name. */
    public record Schema(List<TypeDef> types, Map<String, TypeDef> byName) {
        public Schema {
            types = List.copyOf(types);
            byName = Collections.unmodifiableMap(new LinkedHashMap<>(byName));
        }

        public static Schema of(List<TypeDef> types) {
            Map<String, TypeDef> m = new LinkedHashMap<>();
            for (TypeDef t : types) {
                TypeDef dup = m.put(t.name(), t);
                if (dup != null) {
                    throw new IllegalStateException("two generated types named " + t.name());
                }
            }
            return new Schema(types, m);
        }

        public ObjectType query() {
            TypeDef q = byName.get("Query");
            if (!(q instanceof ObjectType o)) {
                throw new IllegalStateException("a schema without its Query type");
            }
            return o;
        }

        public @com.legend.base.Nullable TypeDef find(String name) {
            return byName.get(name);
        }
    }

    public sealed interface TypeDef permits Composite, InputObjectType, EnumType, ScalarType {
        String name();
    }

    /** A reference to a type: named, or a list of references; each nullable or not. */
    public sealed interface TypeRef permits Named, ListOf {
        boolean nullable();

        /** The schema's reference for a document's (a variable declaration's). */
        static TypeRef of(Gql.TypeRef ref) {
            return switch (ref) {
                case Gql.NamedType n -> new Named(n.name(), n.nullable());
                case Gql.ListType l -> new ListOf(of(l.itemType()), l.nullable());
            };
        }
    }

    public record Named(String name, boolean nullable) implements TypeRef {
        public Named {
            Objects.requireNonNull(name, "name");
        }
    }

    public record ListOf(TypeRef item, boolean nullable) implements TypeRef {
        public ListOf {
            Objects.requireNonNull(item, "item");
        }
    }

    /** A composite output type: an object or an interface. */
    public sealed interface Composite extends TypeDef permits ObjectType, InterfaceType {
        List<String> implementsList();

        List<FieldDef> fields();

        default @com.legend.base.Nullable FieldDef field(String name) {
            for (FieldDef f : fields()) {
                if (f.name().equals(name)) {
                    return f;
                }
            }
            return null;
        }
    }

    /** An object type; {@code classFqn} names the Pure class it stands for, null for a generated shape. */
    public record ObjectType(String name, @com.legend.base.Nullable String description, List<String> implementsList,
            List<FieldDef> fields, @com.legend.base.Nullable String classFqn) implements Composite {
        public ObjectType {
            Objects.requireNonNull(name, "name");
            implementsList = List.copyOf(implementsList);
            fields = List.copyOf(fields);
        }
    }

    /** {@code XInterface} (G8): the polymorphic type of a class with subclasses, or of an abstract class. */
    public record InterfaceType(String name, @com.legend.base.Nullable String description,
            List<String> implementsList, List<FieldDef> fields, String classFqn) implements Composite {
        public InterfaceType {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(classFqn, "classFqn");
            implementsList = List.copyOf(implementsList);
            fields = List.copyOf(fields);
        }
    }

    public record InputObjectType(String name, List<InputValueDef> fields) implements TypeDef {
        public InputObjectType {
            Objects.requireNonNull(name, "name");
            fields = List.copyOf(fields);
        }

        public @com.legend.base.Nullable InputValueDef field(String fieldName) {
            for (InputValueDef f : fields) {
                if (f.name().equals(fieldName)) {
                    return f;
                }
            }
            return null;
        }
    }

    /** An enum; {@code pureFqn} names the Pure enumeration, null for a generated one ({@code SortDirection}). */
    public record EnumType(String name, @com.legend.base.Nullable String description, List<String> values,
            @com.legend.base.Nullable String pureFqn) implements TypeDef {
        public EnumType {
            Objects.requireNonNull(name, "name");
            values = List.copyOf(values);
        }
    }

    public record ScalarType(String name, String description) implements TypeDef {
        public ScalarType {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(description, "description");
        }
    }

    public record FieldDef(String name, @com.legend.base.Nullable String description, TypeRef type,
            List<InputValueDef> args, @com.legend.base.Nullable String deprecationReason, Meaning meaning) {
        public FieldDef {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(meaning, "meaning");
            args = List.copyOf(args);
        }

        public @com.legend.base.Nullable InputValueDef arg(String argName) {
            for (InputValueDef a : args) {
                if (a.name().equals(argName)) {
                    return a;
                }
            }
            return null;
        }
    }

    public record InputValueDef(String name, TypeRef type, @com.legend.base.Nullable String description) {
        public InputValueDef {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
        }

        public InputValueDef(String name, TypeRef type) {
            this(name, type, null);
        }
    }

    /** What a field stands for in Pure (§6.1). */
    public sealed interface Meaning permits Meaning.Root, Meaning.AggregateRoot, Meaning.GroupsRoot,
            Meaning.Property, Meaning.AggregateOf, Meaning.GroupsOf, Meaning.Typename, Meaning.Count,
            Meaning.Fold, Meaning.FoldOf, Meaning.GroupKey, Meaning.GroupKeyProperty {

        /** {@code Person(...)}: {@code Person.all()} with the collection clauses. */
        record Root(String classFqn) implements Meaning {
        }

        /** {@code PersonAggregate(where)}: one aggregate row over the class. */
        record AggregateRoot(String classFqn) implements Meaning {
        }

        /** {@code PersonGroups(...)}: the groups of the class (G7). */
        record GroupsRoot(String classFqn) implements Meaning {
        }

        /** A property of the class, stored, derived or association-injected; {@code derived} with its
         *  parameters as the field's arguments; {@code toMany} with the collection clauses. */
        record Property(String classFqn, String name, boolean derived, boolean toMany) implements Meaning {
        }

        /** {@code employeesAggregate(where)} beside the to-many property {@code employees}. */
        record AggregateOf(String classFqn, String property) implements Meaning {
        }

        /** {@code employeesGroups(...)} beside the to-many property {@code employees}. */
        record GroupsOf(String classFqn, String property) implements Meaning {
        }

        /** {@code __typename}: the exact class of the row. */
        record Typename() implements Meaning {
        }

        /** {@code count} of an aggregate or a group. */
        record Count() implements Meaning {
        }

        /** {@code sum}, {@code avg}, {@code min} or {@code max}: the object holding one fold per property. */
        record Fold(Companion.Fold fn) implements Meaning {
        }

        /** One folded property, {@code sum { salary }}. */
        record FoldOf(Companion.Fold fn, String classFqn, String property) implements Meaning {
        }

        /** {@code key} of a group: the selection under it is the grouping (G7). */
        record GroupKey() implements Meaning {
        }

        /** A groupable property under {@code key}; a to-one one groups through its own key type. */
        record GroupKeyProperty(String classFqn, String property, boolean toOne) implements Meaning {
        }
    }

    // ---- type-reference helpers ----

    public static TypeRef named(String name, boolean nullable) {
        return new Named(name, nullable);
    }

    /** {@code [X!]!}: how every to-many position is typed (always an array, items never null). */
    public static TypeRef listOf(String name) {
        return new ListOf(new Named(name, false), false);
    }

    /** {@code [X!]}: a nullable list of non-null items, the shape of a list argument. */
    public static TypeRef nullableListOf(String name) {
        return new ListOf(new Named(name, false), true);
    }

    /** The named type at the bottom of a reference. */
    public static String baseName(TypeRef ref) {
        return switch (ref) {
            case Named n -> n.name();
            case ListOf l -> baseName(l.item());
        };
    }

    /** The reference as SDL spells it: {@code [Person!]!}, {@code String}. */
    public static String spell(TypeRef ref) {
        return switch (ref) {
            case Named n -> n.name() + (n.nullable() ? "" : "!");
            case ListOf l -> "[" + spell(l.item()) + "]" + (l.nullable() ? "" : "!");
        };
    }
}
