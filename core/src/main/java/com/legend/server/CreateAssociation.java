// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static com.legend.server.ModelChanges.arrOf;
import static com.legend.server.ModelChanges.element;
import static com.legend.server.ModelChanges.isIdentifier;
import static com.legend.server.ModelChanges.lowerFirst;
import static com.legend.server.ModelChanges.refused;
import static com.legend.server.ModelChanges.with;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CREATE AN ASSOCIATION (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.1, §15.6a; an action of {@link ModelChanges}):
 * two classes of the project related by a property or key of each, equal -- an Association, in Pure's own shape (each
 * end the name the OTHER class reaches it by, its class, how many of it one of the other has:
 * {@code customer: Customer[0..1]; trades: Trade[*];}), and its ModelJoin in the model's mapping
 * ({@code {customer: Customer[1], trades: Trade[1] | $customer.id == $trades.customerId}}), where the runtime and a data
 * space find it. A key only -- a column the class does not show, its mapping's local property -- may be joined on: the
 * ModelJoin reads it as a property, as legend-engine's does.
 *
 * <p>How many: an end is {@code [0..1]} when its {@code on} is unique over its class's rows (the facts DataCube reads in
 * the tab, {@code "unique"}), {@code [*]} when not, or as the caller says ({@code "multiplicity"}); neither is refused,
 * never guessed. Named by default after the other class ({@code customer}, plural when many: {@code trades}).
 */
final class CreateAssociation {

    private CreateAssociation() {
    }

    private static final Set<String> MULTIPLICITIES = Set.of("[0..1]", "[1]", "[*]", "[1..*]");

    /** One end: its class, what it is joined on, the name the other class reaches it by, how many of it per other. */
    record End(String cls, String on, String name, String multiplicity) {
        Map<String, Object> json() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("class", cls);
            out.put("on", on);
            out.put("name", name);
            out.put("multiplicity", multiplicity);
            return out;
        }
    }

    /** An association made: its path, its two ends, the runtime it runs on, and its navigations both ways, to plan. */
    record Made(String path, List<End> ends, String runtime, List<String> walks) {
        Map<String, Object> json() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("association", path);
            out.put("ends", ends.stream().map(End::json).toList());
            return out;
        }
    }

    /**
     * One association, of {@code pkg}: {@code spec} is {@code {"name"?, "ends": [{"class", "on", "name"?,
     * "multiplicity"?, "unique"?}, {...}]}} -- each class one of the project's (or made in the same request).
     */
    static Made apply(ModelChanges m, String pkg, Json.Obj spec) {
        ModelChanges.only(spec, "an association to make", "name", "ends");
        List<Json.Node> given = spec.getArr("ends").items();
        if (given.size() != 2) {
            throw new IllegalArgumentException("an association has two ends, not " + given.size());
        }
        Json.Obj a = (Json.Obj) given.get(0);
        Json.Obj b = (Json.Obj) given.get(1);
        ModelChanges.only(a, "an association's end", "class", "on", "name", "multiplicity", "unique");
        ModelChanges.only(b, "an association's end", "class", "on", "name", "multiplicity", "unique");
        String aType = keyType(m, pkg, a);
        String bType = keyType(m, pkg, b);
        if (!aType.equals(bType)) {
            throw refused("an association joins equal values: " + a.getString("class") + "." + a.getString("on") + " is "
                    + aType + ", " + b.getString("class") + "." + b.getString("on") + " " + bType);
        }
        End ea = end(m, a);
        End eb = end(m, b);
        if (ea.name().equals(eb.name())) {
            throw refused("an association's two ends are named apart: both are '" + ea.name() + "'");
        }
        String name = spec.getStringOr("name", null);
        if (name == null) {
            // made again with its ends the other way round: the same association, its ends in its own order
            Json.Obj other = m.get(pkg + "::domain::" + simple(m, eb.cls()) + "_" + simple(m, ea.cls()));
            if (relates(other, eb.cls(), ea.cls())) {
                End first = eb;
                eb = ea;
                ea = first;
            }
            name = simple(m, ea.cls()) + "_" + simple(m, eb.cls());
        }
        if (!isIdentifier(name)) {
            throw refused("an association's name is a Pure name: '" + name + "'");
        }
        String path = pkg + "::domain::" + name;
        String modelMapping = pkg + "::mapping::Model";
        Json.Obj model = m.get(modelMapping);
        if (model == null) {
            throw refused("the model has no mapping yet (" + modelMapping + "): make its classes first");
        }
        // one there already that the model's mapping does not join -- written in Studio, say -- is not written over
        Json.Obj existing = m.get(path);
        if (existing != null && "association".equals(ModelChanges.textOf(existing, "_type")) && !joins(model, path)) {
            throw refused("'" + path + "' is already in the project, not made here: name this association otherwise");
        }
        // each end's name is a property of the OTHER class from now: one it has already is not taken again (this
        // association's own, made again, aside)
        named(m, eb.cls(), ea, path);
        named(m, ea.cls(), eb, path);
        m.put(element("###Pure\nAssociation " + path + "\n{\n  " + ea.name() + ": " + ea.cls() + ea.multiplicity()
                + ";\n  " + eb.name() + ": " + eb.cls() + eb.multiplicity() + ";\n}\n"));

        // its ModelJoin, in the model's mapping: the join's lambda read by the grammar, from a mapping written for it
        Json.Obj join = (Json.Obj) element("###Mapping\nMapping " + pkg + "::mapping::Join\n(\n  " + path
                + ": ModelJoin\n  {\n    {" + ea.name() + ": " + ea.cls() + "[1], " + eb.name() + ": " + eb.cls() + "[1]|$"
                + ea.name() + "." + ea.on() + " == $" + eb.name() + "." + eb.on() + "}\n  }\n)\n")
                .getArr("associationMappings").items().get(0);
        List<Json.Node> joins = new ArrayList<>();
        for (Json.Node n : arrOf(model, "associationMappings").items()) {
            // made again: in place of the one it was
            if (!path.equals(joined(n))) {
                joins.add(n);
            }
        }
        joins.add(join);
        m.put(with(model, "associationMappings", new Json.Arr(joins)));

        String runtime = m.runtimeNaming(modelMapping);
        // walked both ways, each to a property of the class it reaches
        List<String> walks = List.of(
                eb.cls() + ".all()->project(~[reached: x|$x." + ea.name() + "." + firstProperty(m, ea.cls()) + "])",
                ea.cls() + ".all()->project(~[reached: x|$x." + eb.name() + "." + firstProperty(m, eb.cls()) + "])");
        return new Made(path, List.of(ea, eb), runtime, walks);
    }

    /** An end's {@code on}: a property or key of its class, of {@code pkg}; its type's path. */
    private static String keyType(ModelChanges m, String pkg, Json.Obj end) {
        String cls = end.getString("class");
        Json.Obj c = m.get(cls);
        if (c == null || !"class".equals(c.getStringOr("_type", ""))) {
            throw refused("the project has no class " + cls);
        }
        if (!(pkg + "::domain").equals(c.getStringOr("package", ""))) {
            throw refused("an association relates classes of one package, " + pkg + "'s: " + cls + " is not one");
        }
        String on = end.getString("on");
        String type = m.keysOf(cls).get(on);
        if (type == null) {
            throw refused(cls + " has no property or key '" + on + "'");
        }
        return type;
    }

    /** The end as written: its name (by default the class's, plural when many) and how many. */
    private static End end(ModelChanges m, Json.Obj end) {
        String cls = end.getString("class");
        String multiplicity = end.getStringOr("multiplicity", null);
        if (multiplicity == null) {
            if (!end.has("unique")) {
                throw refused("how many of " + cls + " one of the other has is said, or the facts of its '"
                        + end.getString("on") + "' (unique or not)");
            }
            multiplicity = end.getBool("unique") ? "[0..1]" : "[*]";
        }
        if (!MULTIPLICITIES.contains(multiplicity)) {
            throw refused("an end is [0..1], [1], [*] or [1..*], not " + multiplicity);
        }
        String name = end.getStringOr("name", null);
        if (name == null) {
            String base = lowerFirst(simple(m, cls));
            name = multiplicity.endsWith("*]") ? plural(base) : base;
        }
        if (!isIdentifier(name)) {
            throw refused("an end's name is a Pure name: '" + name + "'");
        }
        return new End(cls, end.getString("on"), name, multiplicity);
    }

    /** {@code end}'s name, a property {@code owner} reaches it by: refused when {@code owner} has one of that name. */
    private static void named(ModelChanges m, String owner, End end, String association) {
        if (m.keysOf(owner).containsKey(end.name())) {
            throw refused(owner + " has a property '" + end.name() + "' already: name the association's end otherwise");
        }
        for (Json.Obj e : m.all()) {
            if (!"association".equals(e.getStringOr("_type", "")) || association.equals(ModelChanges.pathOf(e))) {
                continue;
            }
            List<Json.Node> props = arrOf(e, "properties").items();
            for (int i = 0; i < props.size(); i++) {
                Json.Obj p = (Json.Obj) props.get(i);
                // the property reached FROM the other end's class
                Json.Obj other = (Json.Obj) props.get(1 - i);
                if (end.name().equals(p.getString("name")) && owner.equals(ModelChanges.typeOf(other))) {
                    throw refused(owner + " reaches '" + end.name() + "' by " + ModelChanges.pathOf(e)
                            + " already: name the association's end otherwise");
                }
            }
        }
    }

    /** Whether {@code e} is an association whose ends are {@code first}, then {@code second}. */
    private static boolean relates(@com.legend.base.Nullable Json.Obj e, String first, String second) {
        if (e == null || !"association".equals(e.getStringOr("_type", ""))) {
            return false;
        }
        List<Json.Node> props = arrOf(e, "properties").items();
        return props.size() == 2 && first.equals(ModelChanges.typeOf((Json.Obj) props.get(0)))
                && second.equals(ModelChanges.typeOf((Json.Obj) props.get(1)));
    }

    /** Whether the model's mapping joins the association at {@code path}. */
    private static boolean joins(Json.Obj model, String path) {
        for (Json.Node n : arrOf(model, "associationMappings").items()) {
            if (path.equals(joined(n))) {
                return true;
            }
        }
        return false;
    }

    /** The association an association mapping joins. */
    private static String joined(Json.Node mapping) {
        return ModelChanges.textOf(ModelChanges.objOf((Json.Obj) mapping, "association"), "path");
    }

    /** A class's own name, as its element names it. */
    private static String simple(ModelChanges m, String cls) {
        Json.Obj c = m.get(cls);
        return c == null ? cls : c.getString("name");
    }

    /** A class's first property, which a navigation to it reads. */
    private static String firstProperty(ModelChanges m, String cls) {
        Json.Obj c = m.get(cls);
        List<Json.Node> props = c == null ? List.of() : arrOf(c, "properties").items();
        if (props.isEmpty()) {
            throw refused(cls + " has no property to reach");
        }
        return ((Json.Obj) props.get(0)).getString("name");
    }

    /** An English plural, for the common endings: {@code trade} trades, {@code company} companies, {@code box} boxes. */
    static String plural(String word) {
        if (word.endsWith("y") && word.length() > 1 && "aeiou".indexOf(word.charAt(word.length() - 2)) < 0) {
            return word.substring(0, word.length() - 1) + "ies";
        }
        if (word.endsWith("s") || word.endsWith("x") || word.endsWith("ch") || word.endsWith("sh")) {
            return word + "es";
        }
        return word + "s";
    }
}
