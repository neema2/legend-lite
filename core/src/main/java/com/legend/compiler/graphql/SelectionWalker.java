// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

import com.legend.protocol.graphql.GqlSchema;
import com.legend.protocol.graphql.GqlSchema.Composite;
import com.legend.protocol.graphql.GqlSchema.EnumType;
import com.legend.protocol.graphql.GqlSchema.FieldDef;
import com.legend.protocol.graphql.GqlSchema.InputValueDef;
import com.legend.protocol.graphql.GqlSchema.InterfaceType;
import com.legend.protocol.graphql.GqlSchema.Meaning;
import com.legend.protocol.graphql.GqlSchema.ObjectType;
import com.legend.protocol.graphql.GqlSchema.ScalarType;
import com.legend.protocol.graphql.GqlSchema.TypeDef;
import com.legend.compiler.graphql.GqlValidator.Frame;
import com.legend.protocol.spec.Gql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The selection-set walk of the validator (§6.3): fields on their types, arguments, directives, spreads, leaves
 * and composites, merging response keys, the limits (G17), the exact-type fragment trap (G10), the group rules
 * (G7), the warnings, and the typed result shape of what was selected.
 */
final class SelectionWalker {

    private final Frame f;
    private final List<String> spreadStack = new ArrayList<>();

    SelectionWalker(Frame f) {
        this.f = f;
    }

    /** One response key's contribution to a selection set: for merging and for the shape. */
    private record Entry(String fieldName, String args, String shape) {
    }

    String walkOperation(Gql.Operation op) {
        Map<String, Entry> entries = new LinkedHashMap<>();
        walk(f.schema.query(), op.selectionSet(), "", 0, null, entries);
        return shapeOf(entries);
    }

    /** Walks {@code selections} typed at {@code parent}; entries by response key accumulate for the merge rule
     *  and the shape; {@code condition} names the inline fragment's type the fields are reached through. */
    private void walk(Composite parent, List<Gql.Selection> selections, String path, int depth,
            @com.legend.base.Nullable String condition, Map<String, Entry> entries) {
        int listFields = 0;
        Set<String> conditionsHere = new LinkedHashSet<>();
        for (Gql.Selection s : selections) {
            String cond = switch (s) {
                case Gql.InlineFragment inl -> inl.typeCondition();
                case Gql.FragmentSpread sp -> {
                    Gql.Fragment fr = f.fragments.get(sp.name());
                    yield fr == null ? null : fr.typeCondition();
                }
                case Gql.Field fl -> null;
            };
            if (cond != null) {
                conditionsHere.add(cond);
            }
        }
        for (Gql.Selection s : selections) {
            switch (s) {
                case Gql.Field field -> listFields += field(parent, field, path, depth, condition, entries) ? 1 : 0;
                case Gql.InlineFragment inl -> {
                    directives(inl.directives(), path + "...");
                    Composite target = inl.typeCondition() == null ? parent
                            : spreadTarget(parent, inl.typeCondition(), path, conditionsHere);
                    if (target != null) {
                        walk(target, inl.selectionSet(), path, depth,
                                inl.typeCondition() == null ? condition : inl.typeCondition(), entries);
                    }
                }
                case Gql.FragmentSpread sp -> spread(parent, sp, path, depth, conditionsHere, entries);
            }
        }
        if (listFields > f.limits.maxListFieldsPerLevel()) {
            f.error(path, listFields + " list fields in one selection, over the limit of "
                    + f.limits.maxListFieldsPerLevel() + " (G17)");
        }
    }

    private void spread(Composite parent, Gql.FragmentSpread sp, String path, int depth, Set<String> conditionsHere,
            Map<String, Entry> entries) {
        directives(sp.directives(), path + "..." + sp.name());
        Gql.Fragment fr = f.fragments.get(sp.name());
        if (fr == null) {
            f.error(path, "no fragment named " + sp.name());
            return;
        }
        f.fragmentsUsed.add(sp.name());
        if (spreadStack.contains(sp.name())) {
            f.error(path, "fragment " + sp.name() + " spreads itself: " + String.join(" > ", spreadStack));
            return;
        }
        Composite target = spreadTarget(parent, fr.typeCondition(), path, conditionsHere);
        if (target == null) {
            return;
        }
        spreadStack.add(sp.name());
        walk(target, fr.selectionSet(), path, depth, fr.typeCondition(), entries);
        spreadStack.remove(spreadStack.size() - 1);
    }

    /** The composite a spread on {@code conditionName} reads at {@code parent}, or null with the error. */
    private @com.legend.base.Nullable Composite spreadTarget(Composite parent, String conditionName, String path,
            Set<String> conditionsHere) {
        TypeDef t = f.schema.find(conditionName);
        if (!(t instanceof Composite target)) {
            f.error(path, "... on " + conditionName + ": " + (t == null ? "no such type" : "not an object or"
                    + " interface type"));
            return null;
        }
        Set<String> possible = possibleTypes(parent);
        possible.retainAll(possibleTypes(target));
        if (possible.isEmpty()) {
            f.error(path, "... on " + conditionName + " can never match a " + parent.name());
            return null;
        }
        if (target instanceof ObjectType o && parent instanceof InterfaceType) {
            List<String> subclasses = subclassesOf(o);
            if (!subclasses.isEmpty() && !conditionsHere.containsAll(subclasses)) {
                f.error(path, "... on " + o.name() + " matches exact " + o.name() + " instances only, and "
                        + o.name() + " has the subclasses " + String.join(", ", subclasses) + ": spread on "
                        + o.name() + "Interface to include them, or on " + o.name() + " beside each of them if the"
                        + " exact split is wanted (G10)");
                return null;
            }
        }
        return target;
    }

    /** The object types a value typed {@code c} can be. */
    private Set<String> possibleTypes(Composite c) {
        Set<String> out = new LinkedHashSet<>();
        if (c instanceof ObjectType) {
            out.add(c.name());
            return out;
        }
        for (TypeDef t : f.schema.types()) {
            if (t instanceof ObjectType o && o.implementsList().contains(c.name())) {
                out.add(o.name());
            }
        }
        return out;
    }

    /** The concrete subclasses of a concrete class with subclasses: the other objects implementing its own
     *  interface (G8 names the interface after the class). */
    private List<String> subclassesOf(ObjectType o) {
        List<String> out = new ArrayList<>();
        TypeDef own = f.schema.find(o.name() + "Interface");
        if (!(own instanceof InterfaceType i) || o.classFqn() == null || !o.classFqn().equals(i.classFqn())) {
            return out;
        }
        for (String name : possibleTypes(i)) {
            if (!name.equals(o.name())) {
                out.add(name);
            }
        }
        return out;
    }

    /** One field; true when it is a list field (counted against the per-level limit). */
    private boolean field(Composite parent, Gql.Field field, String path, int depth,
            @com.legend.base.Nullable String condition, Map<String, Entry> entries) {
        String key = responseKey(field);
        String here = path.isEmpty() ? key : path + "." + key;
        directives(field.directives(), here);
        if (field.name().startsWith("__")) {
            return meta(parent, field, here, key, condition, entries);
        }
        FieldDef def = parent.field(field.name());
        if (def == null) {
            List<String> names = parent.fields().stream().map(FieldDef::name).toList();
            String near = Names.nearest(names, field.name());
            f.error(here, parent.name() + " has no field " + field.name() + (near != null ? "; did you mean " + near
                    : "; its fields are " + String.join(", ", names)));
            return false;
        }
        if (def.deprecationReason() != null) {
            f.warn(here, field.name() + " is deprecated: " + def.deprecationReason());
        }
        arguments(def, field, here);
        TypeDef type = f.schema.find(GqlSchema.baseName(def.type()));
        boolean list = def.type() instanceof GqlSchema.ListOf;
        String shape;
        if (type instanceof Composite child) {
            if (field.selectionSet().isEmpty()) {
                f.error(here, field.name() + " is a " + child.name() + ": select its fields, { ... }");
                return list;
            }
            if (depth + 1 > f.limits.maxDepth()) {
                f.error(here, "the selection is " + (depth + 1) + " levels deep, over the limit of "
                        + f.limits.maxDepth() + " (G17)");
                return list;
            }
            Map<String, Entry> childEntries = new LinkedHashMap<>();
            walk(child, field.selectionSet(), here, depth + 1, null, childEntries);
            groupRules(def, field, here, childEntries);
            shape = wrap(def.type(), shapeOf(childEntries));
        } else {
            if (!field.selectionSet().isEmpty()) {
                f.error(here, field.name() + " is a " + GqlSchema.spell(def.type()) + ", a leaf: it takes no"
                        + " selection");
            }
            if (!(type instanceof ScalarType || type instanceof EnumType || type == null)) {
                f.error(here, field.name() + " is typed " + GqlSchema.spell(def.type()) + ", not an output type");
            }
            shape = GqlSchema.spell(def.type());
        }
        if (condition != null) {
            shape += " @" + condition;
        }
        merge(entries, key, new Entry(field.name(), argsText(field), shape), here);
        return list;
    }

    private boolean meta(Composite parent, Gql.Field field, String here, String key,
            @com.legend.base.Nullable String condition, Map<String, Entry> entries) {
        if ("__typename".equals(field.name())) {
            if (!field.selectionSet().isEmpty() || !field.arguments().isEmpty()) {
                f.error(here, "__typename is a leaf with no arguments");
            }
            merge(entries, key, new Entry("__typename", "", "String!" + (condition != null ? " @" + condition : "")),
                    here);
            return false;
        }
        if (("__schema".equals(field.name()) || "__type".equals(field.name())) && "Query".equals(parent.name())) {
            f.error(here, "introspection (" + field.name() + ") is not served by this landing: ask describe for the"
                    + " SDL (docs/GRAPHQL_STUDY_2026_10_18.md §10, PR2)");
            return false;
        }
        f.error(here, parent.name() + " has no field " + field.name());
        return false;
    }

    private void arguments(FieldDef def, Gql.Field field, String here) {
        Set<String> seen = new LinkedHashSet<>();
        for (Gql.Argument a : field.arguments()) {
            if (!seen.add(a.name())) {
                f.error(here, "the argument " + a.name() + " is given twice");
                continue;
            }
            InputValueDef arg = def.arg(a.name());
            if (arg == null) {
                List<String> names = def.args().stream().map(InputValueDef::name).toList();
                String near = Names.nearest(names, a.name());
                f.error(here, field.name() + " has no argument " + a.name() + (near != null ? "; did you mean " + near
                        : names.isEmpty() ? "; it takes none" : "; it takes " + String.join(", ", names)));
                continue;
            }
            ValueCoercion.coerce(f, a.value(), arg.type(), here + "(" + a.name() + ")");
        }
        for (InputValueDef arg : def.args()) {
            if (!ValueCoercion.nullable(arg.type()) && !seen.contains(arg.name())) {
                f.error(here, field.name() + " requires the argument " + arg.name() + ": " + GqlSchema.spell(arg.type()));
            }
        }
        if (seen.contains("limit") && !seen.contains("orderBy") && def.arg("orderBy") != null) {
            f.warn(here, "limit without orderBy: the page is not stable between runs");
        }
    }

    /** {@code @include(if:)} and {@code @skip(if:)} only, each with its Boolean. */
    private void directives(List<Gql.Directive> directives, String here) {
        for (Gql.Directive d : directives) {
            if (!"include".equals(d.name()) && !"skip".equals(d.name())) {
                f.error(here, "@" + d.name() + " is not a directive of this schema; @include and @skip are");
                continue;
            }
            if (d.arguments().size() != 1 || !"if".equals(d.arguments().get(0).name())) {
                f.error(here, "@" + d.name() + " takes exactly if: Boolean!");
                continue;
            }
            ValueCoercion.coerce(f, d.arguments().get(0).value(), new GqlSchema.Named("Boolean", false),
                    here + "@" + d.name() + "(if)");
        }
    }

    /** G7: a group's key is what is selected under {@code key}, never empty, never a meta-field; an orderBy on
     *  the key names a selected key only. */
    private void groupRules(FieldDef def, Gql.Field field, String here, Map<String, Entry> childEntries) {
        if (!(def.meaning() instanceof Meaning.GroupsRoot || def.meaning() instanceof Meaning.GroupsOf)) {
            return;
        }
        Set<String> keys = new LinkedHashSet<>();
        boolean keySelected = false;
        for (Gql.Selection s : field.selectionSet()) {
            if (s instanceof Gql.Field kf && "key".equals(kf.name())) {
                keySelected = true;
                for (Gql.Selection ks : kf.selectionSet()) {
                    if (ks instanceof Gql.Field k && !k.name().startsWith("__")) {
                        keys.add(k.name());
                    }
                }
            }
        }
        String aggregate = GqlSchema.baseName(def.type()).replace("Group", "Aggregate");
        if (!keySelected || keys.isEmpty()) {
            f.error(here, "a group selects its keys under key { ... }; for one group over everything ask "
                    + aggregate + " (G7)");
            return;
        }
        for (Gql.Argument a : field.arguments()) {
            if (!"orderBy".equals(a.name())) {
                continue;
            }
            List<Gql.Value> items = a.value() instanceof Gql.ListValue lv ? lv.values() : List.of(a.value());
            for (Gql.Value item : items) {
                if (!(item instanceof Gql.ObjectValue ov)) {
                    continue;
                }
                for (Gql.Argument g : ov.fields()) {
                    if ("key".equals(g.name()) && g.value() instanceof Gql.ObjectValue kv) {
                        for (Gql.Argument k : kv.fields()) {
                            if (!keys.contains(k.name())) {
                                f.error(here + "(orderBy)", "orderBy names key." + k.name() + ", which is not selected"
                                        + " under key (G7)");
                            }
                        }
                    }
                }
            }
        }
    }

    private void merge(Map<String, Entry> entries, String key, Entry entry, String here) {
        Entry prior = entries.get(key);
        if (prior == null) {
            entries.put(key, entry);
            return;
        }
        if (!prior.fieldName().equals(entry.fieldName()) || !prior.args().equals(entry.args())) {
            f.error(here, "the response key " + key + " is selected twice with different fields or arguments: alias"
                    + " one of them");
        } else if (!prior.shape().equals(entry.shape())) {
            entries.put(key, new Entry(entry.fieldName(), entry.args(), prior.shape()));
        }
    }

    // ---- the shape (§8.4) ----

    private static String shapeOf(Map<String, Entry> entries) {
        StringBuilder b = new StringBuilder("{ ");
        boolean first = true;
        for (Map.Entry<String, Entry> e : entries.entrySet()) {
            if (!first) {
                b.append(", ");
            }
            first = false;
            b.append(e.getKey()).append(": ").append(e.getValue().shape());
        }
        return b.append(" }").toString();
    }

    private static String wrap(GqlSchema.TypeRef type, String inner) {
        return switch (type) {
            case GqlSchema.Named n -> inner + (n.nullable() ? "?" : "");
            case GqlSchema.ListOf l -> "[" + wrap(l.item(), inner) + "]" + (l.nullable() ? "?" : "");
        };
    }

    /** The engine keeps the colon inside the alias ({@code "h:"}); the response key is the name before it. */
    static String responseKey(Gql.Field field) {
        String alias = field.alias();
        if (alias == null) {
            return field.name();
        }
        return alias.endsWith(":") ? alias.substring(0, alias.length() - 1) : alias;
    }

    private static String argsText(Gql.Field field) {
        StringBuilder b = new StringBuilder();
        for (Gql.Argument a : field.arguments()) {
            b.append(a.name()).append('=').append(a.value()).append(';');
        }
        return b.toString();
    }
}
