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
 * @param notes        what the reader did not carry, or carried otherwise, each naming where in the schema
 */
public record ImportedModel(List<ImportedClass> classes, List<ImportedEnum> enumerations, List<Note> notes) {

    public ImportedModel {
        classes = List.copyOf(classes);
        enumerations = List.copyOf(enumerations);
        notes = List.copyOf(notes);
    }

    /**
     * A NOTE: what a reader did not carry, or carried otherwise, said once however many times it is met.
     *
     * @param code    what kind of note it is, for a reader of notes to group and filter by: {@code variant.union},
     *                {@code rule.not-carried}, {@code logic.not-carried}, {@code reference.unresolved}, ...
     * @param where   where it was first met (a JSON pointer: {@code #/$defs/T/properties/a}; or a class's name)
     * @param message what it says
     * @param count   how many times it was met
     */
    public record Note(String code, String where, String message, int count) {
        @Override
        public String toString() {
            return where + ": " + message + (count > 1 ? " (" + count + " times)" : "");
        }
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
     * @param logic       the conditions its values keep as a whole (a JSON Schema's if/then/else, not, ...), each a
     *                    constraint
     * @param annotations what else the schema says of it, as the format's profiles say it
     */
    public record ImportedClass(String name, @com.legend.base.Nullable String doc, List<String> supers,
            List<ImportedProperty> properties, boolean owned, boolean deprecated, List<Rule> rules,
            List<Logic> logic, List<Annotation> annotations) {
        public ImportedClass {
            supers = List.copyOf(supers);
            properties = List.copyOf(properties);
            rules = List.copyOf(rules);
            logic = List.copyOf(logic);
            annotations = List.copyOf(annotations);
        }

        /** A class with no rule, not deprecated, and annotated by nothing. */
        public ImportedClass(String name, @com.legend.base.Nullable String doc, List<String> supers,
                List<ImportedProperty> properties, boolean owned) {
            this(name, doc, supers, properties, owned, false, List.of(), List.of(), List.of());
        }

        /** This class with other superclasses, or held otherwise. */
        public ImportedClass with(List<String> otherSupers, boolean nowOwned) {
            return new ImportedClass(name, doc, otherSupers, properties, nowOwned, deprecated, rules, logic,
                    annotations);
        }

        /** This class with other properties, and more annotations. */
        public ImportedClass with(List<ImportedProperty> otherProperties, List<Annotation> more) {
            List<Annotation> all = new java.util.ArrayList<>(annotations);
            all.addAll(more);
            return new ImportedClass(name, doc, supers, otherProperties, owned, deprecated, rules, logic, all);
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
     * @param annotations  what else the schema says of it, as the format's profiles say it
     */
    public record ImportedProperty(String name, @com.legend.base.Nullable String doc, Kind kind, String type, int lower,
            @com.legend.base.Nullable Integer upper, boolean deprecated,
            com.legend.json.Json.@com.legend.base.Nullable Node defaultValue, List<Annotation> annotations) {
        public ImportedProperty {
            annotations = List.copyOf(annotations);
        }

        /** A property not deprecated, with no default, and annotated by nothing. */
        public ImportedProperty(String name, @com.legend.base.Nullable String doc, Kind kind, String type, int lower,
                @com.legend.base.Nullable Integer upper) {
            this(name, doc, kind, type, lower, upper, false, null, List.of());
        }

        /** This property with one more annotation. */
        public ImportedProperty annotated(Annotation more) {
            List<Annotation> all = new java.util.ArrayList<>(annotations);
            all.add(more);
            return new ImportedProperty(name, doc, kind, type, lower, upper, deprecated, defaultValue, all);
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
     * @param valueDocs        each value's description, or null, in the same order
     * @param annotations      what else the schema says of the enumeration, as the format's profiles say it
     * @param valueAnnotations what it says of each value (its text, where its name differs), in the same order
     */
    public record ImportedEnum(String name, @com.legend.base.Nullable String doc, List<String> values,
            List<String> texts, List<@com.legend.base.Nullable String> valueDocs, List<Annotation> annotations,
            List<List<Annotation>> valueAnnotations) {
        public ImportedEnum {
            values = List.copyOf(values);
            texts = List.copyOf(texts);
            valueDocs = java.util.Collections.unmodifiableList(new java.util.ArrayList<>(valueDocs));
            annotations = List.copyOf(annotations);
            List<List<Annotation>> each = new java.util.ArrayList<>();
            for (List<Annotation> a : valueAnnotations) {
                each.add(List.copyOf(a));
            }
            valueAnnotations = List.copyOf(each);
        }

        /** An enumeration whose values are spelled as they are named, with no value documented. */
        public ImportedEnum(String name, @com.legend.base.Nullable String doc, List<String> values) {
            this(name, doc, values, values, java.util.Collections.nCopies(values.size(), null), List.of(),
                    java.util.Collections.nCopies(values.size(), List.of()));
        }
    }

    /**
     * What a schema says of an element beyond its shape, as a profile says it (upstream's: a JSON Schema's are
     * legend-engine's {@code meta::json::schema::JSONSchemaGeneration}, {@code ...JSONSchemaTypeExtension} and
     * {@code ...JSONSchemaOpenAPIExtension}): a stereotype ({@code value} null), or a tag and its value.
     *
     * @param profile the profile's path
     * @param name    the stereotype's or the tag's name
     * @param value   the tag's value; null for a stereotype
     */
    public record Annotation(String profile, String name, @com.legend.base.Nullable String value) {

        public static Annotation stereotype(String profile, String name) {
            return new Annotation(profile, name, null);
        }

        public static Annotation tag(String profile, String name, String value) {
            return new Annotation(profile, name, value);
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
