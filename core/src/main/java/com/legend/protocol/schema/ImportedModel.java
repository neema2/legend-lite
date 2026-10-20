// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.schema;

import java.util.List;

/**
 * A MODEL READ FROM A SCHEMA LANGUAGE (docs/MODEL_IMPORT_STUDY_2026_10_20.md §3): what every schema reader produces
 * ({@link JsonSchemaReader} the first) and the model's writer turns into Pure elements ({@code server.ImportSchema}).
 * Format-neutral: a class, its properties, its superclasses; an enumeration and its values; a property typed by
 * another class is a reference, which the writer makes an association. Names are already Pure names; how each was
 * made from the schema's is said in {@link #notes}.
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
     */
    public record ImportedClass(String name, @com.legend.base.Nullable String doc, List<String> supers,
            List<ImportedProperty> properties, boolean owned) {
        public ImportedClass {
            supers = List.copyOf(supers);
            properties = List.copyOf(properties);
        }
    }

    /**
     * A property.
     *
     * @param name  its Pure name
     * @param doc   its description, or null
     * @param kind  what its type is
     * @param type  a primitive's Pure name ({@code String}, {@code meta::pure::metamodel::variant::Variant}), or the
     *              name of an enumeration or class of the model
     * @param lower its multiplicity's lower bound
     * @param upper its upper bound, null for many
     */
    public record ImportedProperty(String name, @com.legend.base.Nullable String doc, Kind kind, String type, int lower,
            @com.legend.base.Nullable Integer upper) {

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
     * @param name   its Pure name
     * @param doc    its description, or null
     * @param values its values' Pure names, in order
     */
    public record ImportedEnum(String name, @com.legend.base.Nullable String doc, List<String> values) {
        public ImportedEnum {
            values = List.copyOf(values);
        }
    }
}
