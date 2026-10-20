// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static com.legend.server.ModelChanges.arrOf;
import static com.legend.server.ModelChanges.element;
import static com.legend.server.ModelChanges.objOf;
import static com.legend.server.ModelChanges.refused;
import static com.legend.server.ModelChanges.textOf;
import static com.legend.server.ModelChanges.typeOf;
import static com.legend.server.ModelChanges.with;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JOIN AN ASSOCIATION (docs/MODEL_IMPORT_STUDY_2026_10_20.md §5; an action of {@link ModelChanges}): an association
 * the project has already -- imported ({@link ImportSchema}), written in Studio -- joined in the model's mapping by a
 * ModelJoin, a property or key of each end's class equal ({@code {customer: Customer[1], trades: Trade[1] |
 * $customer.id == $trades.customerId}}), as Create an Association joins the one it makes. A key -- a column the class's
 * SQL keeps that no property reads ({@link MapClass}) -- may be joined on, as Create an Association's may.
 */
final class JoinAssociation {

    private JoinAssociation() {
    }

    /** An association joined: its path, each end's name and what it is joined on, and the walks to plan. */
    record Made(String path, List<Map<String, Object>> ends, String runtime, List<String> walks) {
        Map<String, Object> json() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("association", path);
            out.put("ends", ends);
            return out;
        }
    }

    /** One end: its name, its class, what it is joined on. */
    private record End(String name, String cls, String on) {
    }

    /**
     * One association, of {@code pkg}: {@code spec} is {@code {"association", "ends": [{"class"?, "end"?, "on"},
     * {...}]}} -- each end named by its class, or by its name ({@code "end"}) when both ends are one class.
     */
    static Made apply(ModelChanges m, String pkg, Json.Obj spec) {
        String path = spec.getString("association");
        Json.Obj association = m.get(path);
        if (association == null || !"association".equals(association.getStringOr("_type", ""))) {
            throw refused("the project has no association " + path);
        }
        List<Json.Node> props = arrOf(association, "properties").items();
        List<Json.Node> given = spec.getArr("ends").items();
        if (given.size() != 2) {
            throw new IllegalArgumentException("an association is joined on its two ends, not " + given.size());
        }
        End a = end(path, props, (Json.Obj) given.get(0));
        End b = end(path, props, (Json.Obj) given.get(1));
        if (a.name().equals(b.name())) {
            throw refused("an association is joined on its two ends, and both name '" + a.name() + "'");
        }
        String aType = keyType(m, a);
        String bType = keyType(m, b);
        if (!aType.equals(bType)) {
            throw refused("an association joins equal values: " + a.cls() + "." + a.on() + " is " + aType + ", "
                    + b.cls() + "." + b.on() + " " + bType);
        }
        String modelMapping = pkg + "::mapping::Model";
        Json.Obj model = m.get(modelMapping);
        if (model == null) {
            throw refused("the model has no mapping yet (" + modelMapping + "): map its classes first");
        }
        Json.Obj join = (Json.Obj) element("###Mapping\nMapping " + pkg + "::mapping::Join\n(\n  " + path
                + ": ModelJoin\n  {\n    {" + a.name() + ": " + a.cls() + "[1], " + b.name() + ": " + b.cls() + "[1]|$"
                + a.name() + "." + a.on() + " == $" + b.name() + "." + b.on() + "}\n  }\n)\n")
                .getArr("associationMappings").items().get(0);
        List<Json.Node> joins = new ArrayList<>();
        for (Json.Node n : arrOf(model, "associationMappings").items()) {
            // joined again: in place of the join it had
            if (!path.equals(textOf(objOf((Json.Obj) n, "association"), "path"))) {
                joins.add(n);
            }
        }
        joins.add(join);
        m.put(with(model, "associationMappings", new Json.Arr(joins)));

        String runtime = m.runtimeNaming(modelMapping);
        // walked both ways: from each end's class to what the other end is joined on
        List<String> walks = List.of(
                b.cls() + ".all()->project(~[reached: x|$x." + a.name() + "." + reached(m, a) + "])",
                a.cls() + ".all()->project(~[reached: x|$x." + b.name() + "." + reached(m, b) + "])");
        List<Map<String, Object>> ends = new ArrayList<>();
        for (End e : List.of(a, b)) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("end", e.name());
            out.put("class", e.cls());
            out.put("on", e.on());
            ends.add(out);
        }
        return new Made(path, ends, runtime, walks);
    }

    /** The association's end {@code given} names: by its name, or as the only end of its class. */
    private static End end(String path, List<Json.Node> props, Json.Obj given) {
        String name = given.getStringOr("end", null);
        String cls = given.getStringOr("class", null);
        List<Json.Obj> found = new ArrayList<>();
        for (Json.Node n : props) {
            Json.Obj p = (Json.Obj) n;
            if (name != null ? name.equals(p.getString("name")) : typeOf(p).equals(cls)) {
                found.add(p);
            }
        }
        if (found.size() != 1) {
            throw refused(found.isEmpty() ? path + " has no end " + (name != null ? "'" + name + "'" : "of " + cls)
                    : "both ends of " + path + " are " + cls + ": name the end ('end')");
        }
        return new End(found.get(0).getString("name"), typeOf(found.get(0)), given.getString("on"));
    }

    /** An end's {@code on}: a property or key of its class; its type's path. */
    private static String keyType(ModelChanges m, End end) {
        String type = m.keysOf(end.cls()).get(end.on());
        if (type == null) {
            throw refused(end.cls() + " has no property or key '" + end.on() + "' (a key is a column its SQL keeps)");
        }
        return type;
    }

    /** What a walk reaches on an end: the property it is joined on, when the class shows it; else its first. */
    private static String reached(ModelChanges m, End end) {
        Json.Obj c = m.get(end.cls());
        List<Json.Node> props = c == null ? List.of() : arrOf(c, "properties").items();
        for (Json.Node n : props) {
            if (end.on().equals(((Json.Obj) n).getString("name"))) {
                return end.on();
            }
        }
        if (props.isEmpty()) {
            throw refused(end.cls() + " has no property to reach");
        }
        return ((Json.Obj) props.get(0)).getString("name");
    }
}
