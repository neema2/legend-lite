// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static com.legend.server.ModelChanges.element;
import static com.legend.server.ModelChanges.literal;
import static com.legend.server.ModelChanges.lowerFirst;
import static com.legend.server.ModelChanges.refused;
import static com.legend.server.ModelChanges.upperFirst;

import com.legend.json.Json;
import com.legend.protocol.schema.ImportedModel;
import com.legend.protocol.schema.JsonSchemaReader;
import com.legend.protocol.schema.SchemaImportException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * IMPORT A SCHEMA (docs/MODEL_IMPORT_STUDY_2026_10_20.md §4; an action of {@link ModelChanges}): the classes and
 * enumerations a schema language defines, written into the project as Pure, in {@code <pkg>::domain} where Create a
 * Class writes its classes -- so the classes are mapped next ({@link MapClass}) and their associations joined
 * ({@link JoinAssociation}) as any class of the model is. Written ONCE: the Pure is the model from then on, owned and
 * edited as Pure; importing again writes the same elements over the ones it wrote.
 *
 * <p>A property typed by another class becomes an ASSOCIATION, {@code <Owner>_<Property>}: the property, as the schema
 * names it, an end of it, and the way back the other end -- named after the owner, plural ({@code trades: Trade[*]}),
 * or, for a class made of an inline object (a value its owner holds), singular and to one ({@code order:
 * Order[0..1]}); {@code <plural>As<Property>} when the plural is taken ({@code tradesAsBuyer}, {@code tradesAsSeller}).
 * An association, not a property of the class, because a Relation-mapped class reaches another through a ModelJoin,
 * and a ModelJoin joins an association.
 */
final class ImportSchema {

    private ImportSchema() {
    }

    private static final String DOC = "meta::pure::profiles::doc.doc";

    /** An import made: the elements written, by kind. */
    record Made(List<String> classes, List<String> enumerations, List<String> associations) {
        Map<String, Object> json() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("classes", classes);
            out.put("enumerations", enumerations);
            out.put("associations", associations);
            return out;
        }
    }

    /**
     * One schema, into {@code pkg}: {@code spec} is {@code {"format": "jsonSchema"|"openApi", "content", "name"?,
     * "schemas"?}} -- {@code content} the document's text, {@code name} its root object's class name when it has no
     * title, {@code schemas} the schemas to import (each with the schemas it reaches), every one when absent.
     */
    static Made apply(ModelChanges m, String pkg, Json.Obj spec) {
        String format = spec.getString("format");
        ImportedModel model;
        try {
            // each schema language its reader; an OpenAPI document's schemas are JSON Schema's
            model = switch (format) {
                case "jsonSchema", "openApi" -> JsonSchemaReader.read(spec.getString("content"),
                        spec.getStringOr("name", null), spec.getStringArrayOr("schemas", List.of()));
                default -> throw refused("a schema is read as jsonSchema or openApi, not '" + format + "'");
            };
        } catch (SchemaImportException e) {
            throw refused("the schema cannot be imported:\n- " + String.join("\n- ", e.reasons()));
        }
        m.notes.addAll(model.notes());
        String domain = pkg + "::domain";

        List<String> enumerations = new ArrayList<>();
        for (ImportedModel.ImportedEnum e : model.enumerations()) {
            String path = domain + "::" + e.name();
            m.put(element("###Pure\nEnum " + docOf(e.doc()) + path + "\n{\n  " + String.join(",\n  ", e.values())
                    + "\n}\n"));
            enumerations.add(path);
        }

        List<String> classes = new ArrayList<>();
        List<Reference> references = new ArrayList<>();
        Set<String> owned = new HashSet<>();
        for (ImportedModel.ImportedClass c : model.classes()) {
            if (c.owned()) {
                owned.add(c.name());
            }
        }
        for (ImportedModel.ImportedClass c : model.classes()) {
            String path = domain + "::" + c.name();
            StringBuilder text = new StringBuilder("###Pure\nClass ").append(docOf(c.doc())).append(path);
            if (!c.supers().isEmpty()) {
                List<String> supers = new ArrayList<>();
                for (String s : c.supers()) {
                    supers.add(domain + "::" + s);
                }
                text.append(" extends ").append(String.join(", ", supers));
            }
            text.append("\n{\n");
            for (ImportedModel.ImportedProperty p : c.properties()) {
                if (p.kind() == ImportedModel.Kind.CLASS) {
                    references.add(new Reference(c.name(), p, owned.contains(p.type())));
                    continue;
                }
                String type = p.kind() == ImportedModel.Kind.ENUMERATION ? domain + "::" + p.type() : p.type();
                text.append("  ").append(docOf(p.doc())).append(p.name()).append(": ").append(type)
                        .append(p.multiplicity()).append(";\n");
            }
            m.put(element(text.append("}\n").toString()));
            classes.add(path);
        }

        List<String> associations = new ArrayList<>();
        for (Reference r : references) {
            associations.add(associate(m, domain, r, references));
        }
        return new Made(classes, enumerations, associations);
    }

    /** A property of {@code owner} typed by a class; {@code owned} when that class is a value the owner holds. */
    private record Reference(String owner, ImportedModel.ImportedProperty property, boolean owned) {
    }

    /** The association a reference is made: its path. */
    private static String associate(ModelChanges m, String domain, Reference r, List<Reference> all) {
        ImportedModel.ImportedProperty p = r.property();
        String path = domain + "::" + r.owner() + "_" + upperFirst(p.name());
        String back;
        String backMultiplicity;
        if (r.owned()) {
            back = lowerFirst(r.owner());
            backMultiplicity = "[0..1]";
        } else {
            back = lowerFirst(CreateAssociation.plural(r.owner()));
            backMultiplicity = "[*]";
            // the plural is the way back of ONE reference from the owner to the class, not its own: taken by the
            // class itself, or by another of the owner's references to the class, each is named after its property
            boolean shared = r.owner().equals(p.type());
            for (Reference other : all) {
                shared |= other != r && other.owner().equals(r.owner()) && other.property().type().equals(p.type());
            }
            if (shared || propertyNames(m, domain + "::" + p.type()).contains(back)) {
                back = back + "As" + upperFirst(p.name());
            }
        }
        m.put(element("###Pure\nAssociation " + path + "\n{\n  " + docOf(p.doc()) + p.name() + ": " + domain + "::"
                + p.type() + p.multiplicity() + ";\n  " + back + ": " + domain + "::" + r.owner() + backMultiplicity
                + ";\n}\n"));
        return path;
    }

    /** The names of the properties a class of the project declares. */
    private static Set<String> propertyNames(ModelChanges m, String cls) {
        Set<String> out = new HashSet<>();
        Json.Obj c = m.get(cls);
        if (c != null) {
            for (Json.Node n : ModelChanges.arrOf(c, "properties").items()) {
                out.add(((Json.Obj) n).getString("name"));
            }
        }
        return out;
    }

    private static String docOf(@com.legend.base.Nullable String doc) {
        return doc == null || doc.strip().isEmpty() ? "" : "{" + DOC + " = " + literal(doc.strip()) + "} ";
    }
}
