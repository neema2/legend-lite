// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.schema;

import com.legend.json.Json;

import java.math.BigDecimal;
import java.util.List;

/**
 * A MODEL READ FROM A SCHEMA LANGUAGE (docs/MODEL_IMPORT_STUDY_2026_10_20.md §3): what every schema reader produces
 * ({@link JsonSchemaReader} the first) and the model's writer turns into Pure elements ({@code server.ImportSchema}).
 * Format-neutral: a class, its properties, its superclasses and the rules its values keep; an enumeration and its
 * values; a property typed by another class is a reference, which the writer makes an association. Names are already
 * Pure names; how each was made from the schema's is said in {@link #notes}.
 *
 * @param classes      the classes, in the order the schema defines them (a class made of an inline object follows its
 *                     owner)
 * @param enumerations the enumerations, in order
 * @param notes        what the reader did not carry or renamed, each naming where in the schema
 */
public record ImportedModel(List<ImportedClass> classes, List<ImportedEnum> enumerations, List<String> notes) {

    public ImportedModel {
        classes = List.copyOf(classes);
        enumerations = List.copyOf(enumerations);
        notes = List.copyOf(notes);
    }

    /** What a property's type is: a primitive (or {@code Variant}), an enumeration of the model, or a class of it. */
    public enum Kind { PRIMITIVE, ENUMERATION, CLASS }

    /**
     * A class.
     *
     * @param name       its Pure name
     * @param doc        its description, or null
     * @param supers     its superclasses' names (classes of the model)
     * @param properties its own properties, in the schema's order
     * @param owned      whether it was an inline object of another class's property (a value it owns), not a schema
     *                   of its own
     * @param deprecated whether the schema says it is deprecated
     * @param rules      the rules its properties' values keep (a JSON Schema's validations), each a constraint
     * @param logic      the conditions its values keep as a whole (a JSON Schema's if/then/else, not, ...), each a
     *                   constraint
     */
    public record ImportedClass(String name, @com.legend.base.Nullable String doc, List<String> supers,
            List<ImportedProperty> properties, boolean owned, boolean deprecated, List<Rule> rules,
            List<Logic> logic) {
        public ImportedClass {
            supers = List.copyOf(supers);
            properties = List.copyOf(properties);
            rules = List.copyOf(rules);
            logic = List.copyOf(logic);
        }

        /** A class with no rule and not deprecated. */
        public ImportedClass(String name, @com.legend.base.Nullable String doc, List<String> supers,
                List<ImportedProperty> properties, boolean owned) {
            this(name, doc, supers, properties, owned, false, List.of(), List.of());
        }
    }

    /**
     * A property.
     *
     * @param name         its Pure name
     * @param doc          its description, or null
     * @param kind         what its type is
     * @param type         a primitive's Pure name ({@code String}, {@code meta::pure::metamodel::variant::Variant}),
     *                     or the name of an enumeration or class of the model
     * @param lower        its multiplicity's lower bound
     * @param upper        its upper bound, null for many
     * @param deprecated   whether the schema says it is deprecated
     * @param defaultValue its default: a JSON string, number or boolean (an enumeration's value by its Pure name), or
     *                     null
     */
    public record ImportedProperty(String name, @com.legend.base.Nullable String doc, Kind kind, String type, int lower,
            @com.legend.base.Nullable Integer upper, boolean deprecated,
            com.legend.json.Json.@com.legend.base.Nullable Node defaultValue) {

        /** A property not deprecated and with no default. */
        public ImportedProperty(String name, @com.legend.base.Nullable String doc, Kind kind, String type, int lower,
                @com.legend.base.Nullable Integer upper) {
            this(name, doc, kind, type, lower, upper, false, null);
        }

        /** Its multiplicity as Pure writes it: {@code [1]}, {@code [0..1]}, {@code [*]}, {@code [1..*]}. */
        public String multiplicity() {
            if (upper == null) {
                return lower == 0 ? "[*]" : "[" + lower + "..*]";
            }
            return lower == upper ? "[" + lower + "]" : "[" + lower + ".." + upper + "]";
        }
    }

    /**
     * An enumeration.
     *
     * @param name      its Pure name
     * @param doc       its description, or null
     * @param values    its values' Pure names, in order
     * @param texts     each value as the schema spells it (what a column holds), in the same order
     * @param valueDocs each value's description, or null, in the same order
     */
    public record ImportedEnum(String name, @com.legend.base.Nullable String doc, List<String> values,
            List<String> texts, List<@com.legend.base.Nullable String> valueDocs) {
        public ImportedEnum {
            values = List.copyOf(values);
            texts = List.copyOf(texts);
            valueDocs = java.util.Collections.unmodifiableList(new java.util.ArrayList<>(valueDocs));
        }

        /** An enumeration whose values are spelled as they are named, with no value documented. */
        public ImportedEnum(String name, @com.legend.base.Nullable String doc, List<String> values) {
            this(name, doc, values, values, java.util.Collections.nCopies(values.size(), null));
        }
    }

    /**
     * A RULE a property's values keep: one of a JSON Schema's validations, which the writer makes a class constraint.
     * {@code each}: the rule is each value's of a property holding many.
     */
    public sealed interface Rule permits Bound, Length, Pattern, MultipleOf, OneOf, Unique, Requires {
        /** The property the rule is of. */
        String property();

        /** Whether the rule is each value's, of a property holding many. */
        boolean each();
    }

    /** {@code minimum}, {@code maximum}, and their exclusive forms. */
    public record Bound(String property, boolean each, BigDecimal value, boolean lower, boolean exclusive)
            implements Rule {
    }

    /** {@code minLength}, {@code maxLength}. */
    public record Length(String property, boolean each, int value, boolean lower) implements Rule {
    }

    /** {@code pattern}: a regular expression the text contains a match of. */
    public record Pattern(String property, boolean each, String regex) implements Rule {
    }

    /** {@code multipleOf}, of an integer. */
    public record MultipleOf(String property, boolean each, BigDecimal value) implements Rule {
    }

    /** {@code const}, or an {@code enum} not made an enumeration: the values a property may hold. */
    public record OneOf(String property, boolean each, List<Json.Node> values) implements Rule {
        public OneOf {
            values = List.copyOf(values);
        }
    }

    /**
     * A CONDITION a class's values keep as a whole: JSON Schema's logic over a value (if/then/else, not, a union of
     * requirements, dependentSchemas), which the writer makes a constraint.
     *
     * @param name      what its constraint is named after ({@code condition}, {@code exclusion}, {@code oneOf},
     *                  {@code <property>Dependency}, {@code <property>Not})
     * @param condition the condition
     */
    public record Logic(String name, Condition condition) {
    }

    /** A condition on a class's value: a property has a value, its value keeps a rule, and their combinations. */
    public sealed interface Condition permits Has, Holds, Not, All, Any {
    }

    /** The property has a value. */
    public record Has(String property) implements Condition {
    }

    /** The property's value (each of them, of a property holding many) keeps the rule; said of a value present. */
    public record Holds(Rule rule) implements Condition {
    }

    /** The condition does not hold. */
    public record Not(Condition condition) implements Condition {
    }

    /** Every condition holds; none is true. */
    public record All(List<Condition> conditions) implements Condition {
        public All {
            conditions = List.copyOf(conditions);
        }
    }

    /** One condition at least holds; none is false. */
    public record Any(List<Condition> conditions) implements Condition {
        public Any {
            conditions = List.copyOf(conditions);
        }
    }

    /** {@code uniqueItems}: a list of primitives or enumeration values holds no value twice. */
    public record Unique(String property, boolean each) implements Rule {
    }

    /** {@code dependentRequired} (draft 7's {@code dependencies} naming properties): when a value has the property, it
     *  has {@code others} too, by their Pure names. */
    public record Requires(String property, boolean each, List<String> others) implements Rule {
        public Requires {
            others = List.copyOf(others);
        }
    }
}
