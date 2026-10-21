// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The reference's view of a fixture: its classes by their GraphQL type names, each with its fields as plain Java
 * functions over in-memory rows. Nothing here reads lite's compiler: a derived property is the fixture's own Java
 * spelling of what the Pure body means, so a disagreement between lite and the reference is a finding, not an echo.
 */
public final class ReferenceModel {

    /** A row of a class: its exact class (a GraphQL object type's name), its values by property name. */
    public record Obj(String type, Map<String, Object> v) {
        public Object get(String p) {
            return v.get(p);
        }

        @Override
        public String toString() {
            return type + v;
        }
    }

    /** A scalar's kind, as the generator maps it. */
    public enum Kind { STRING, BIGINT, FLOAT, DECIMAL, BOOLEAN, DATE, DATETIME, ENUM }

    public sealed interface Fld permits Scalar, ToOne, ToMany, DatedToOne, DatedToMany {
    }

    /** A scalar property, stored or derived; {@code params} the derived property's arguments, by name and kind. */
    public record Scalar(Kind kind, Map<String, Kind> params, BiFunction<Obj, Map<String, Object>, Object> get)
            implements Fld {
        public static Scalar of(Kind kind, Function<Obj, Object> get) {
            return new Scalar(kind, Map.of(), (o, a) -> get.apply(o));
        }

        public static Scalar stored(Kind kind, String property) {
            return of(kind, o -> o.get(property));
        }
    }

    public record ToOne(String target, Function<Obj, Obj> get) implements Fld {
    }

    public record ToMany(String target, Function<Obj, List<Obj>> get) implements Fld {
    }

    /** A to-one property reaching a BUSINESS-TEMPORAL class: the version holding on a business date (the field's
     *  {@code businessDate} argument, else the owner's own when the owner is temporal: the engine's propagation). */
    public record DatedToOne(String target, BiFunction<Obj, java.time.LocalDate, Obj> get) implements Fld {
    }

    /** A to-many property reaching a BUSINESS-TEMPORAL class: the versions holding on the field's business date. */
    public record DatedToMany(String target, BiFunction<Obj, java.time.LocalDate, List<Obj>> get) implements Fld {
    }

    /** A class: its GraphQL name, its superclass's, its fields (inherited ones included), its root set (the rows its
     *  mapping maps: a subclass mapped on its own table answers its own rows only, as legend-engine's do). */
    public static final class Cls {
        final String name;
        final String parent;
        final Map<String, Fld> fields = new LinkedHashMap<>();
        Supplier<List<Obj>> roots = List::of;
        /** A business-temporal class's rows as of a date (each carrying it as {@code businessDate}); null otherwise. */
        Function<java.time.LocalDate, List<Obj>> datedRoots;

        Cls(String name, String parent) {
            this.name = name;
            this.parent = parent;
        }

        public Cls scalar(String name, Kind kind) {
            fields.put(name, Scalar.stored(kind, name));
            return this;
        }

        public Cls derived(String name, Kind kind, Function<Obj, Object> get) {
            fields.put(name, Scalar.of(kind, get));
            return this;
        }

        public Cls derived(String name, Kind kind, Map<String, Kind> params,
                BiFunction<Obj, Map<String, Object>, Object> get) {
            fields.put(name, new Scalar(kind, params, get));
            return this;
        }

        public Cls toOne(String name, String target, Function<Obj, Obj> get) {
            fields.put(name, new ToOne(target, get));
            return this;
        }

        public Cls toMany(String name, String target, Function<Obj, List<Obj>> get) {
            fields.put(name, new ToMany(target, get));
            return this;
        }

        public Cls roots(Supplier<List<Obj>> roots) {
            this.roots = roots;
            return this;
        }

        /** A business-temporal class: its rows as of a date; {@link #roots} stays every version (the pool literals
         *  are drawn from). */
        public Cls datedRoots(Function<java.time.LocalDate, List<Obj>> asOf) {
            this.datedRoots = asOf;
            return this;
        }

        public boolean temporal() {
            return datedRoots != null;
        }

        public Cls datedToOne(String name, String target, BiFunction<Obj, java.time.LocalDate, Obj> get) {
            fields.put(name, new DatedToOne(target, get));
            return this;
        }

        public Cls datedToMany(String name, String target, BiFunction<Obj, java.time.LocalDate, List<Obj>> get) {
            fields.put(name, new DatedToMany(target, get));
            return this;
        }

        public String name() {
            return name;
        }
    }

    final Map<String, Cls> classes = new LinkedHashMap<>();

    /** How the database folds a letter's case: all of Unicode (Java's, DuckDB's), or ASCII only (a Postgres in the
     *  C locale: the embedded test server's, docs/CYPHER_STUDY_2026_10_15.md's case-folding row). */
    public enum CaseFolding { UNICODE, ASCII }

    CaseFolding folding = CaseFolding.UNICODE;

    String lower(String s) {
        if (folding == CaseFolding.UNICODE) {
            return s.toLowerCase(java.util.Locale.ROOT);
        }
        StringBuilder b = new StringBuilder(s.length());
        for (char c : s.toCharArray()) {
            b.append(c >= 'A' && c <= 'Z' ? (char) (c + 32) : c);
        }
        return b.toString();
    }

    String upper(String s) {
        if (folding == CaseFolding.UNICODE) {
            return s.toUpperCase(java.util.Locale.ROOT);
        }
        StringBuilder b = new StringBuilder(s.length());
        for (char c : s.toCharArray()) {
            b.append(c >= 'a' && c <= 'z' ? (char) (c - 32) : c);
        }
        return b.toString();
    }

    public Cls cls(String name) {
        return cls(name, null);
    }

    public Cls cls(String name, String parent) {
        Cls c = new Cls(name, parent);
        if (parent != null) {
            c.fields.putAll(classes.get(parent).fields);
        }
        classes.put(name, c);
        return c;
    }

    public Cls get(String name) {
        Cls c = classes.get(name);
        if (c == null) {
            throw new IllegalArgumentException("the reference knows no class " + name);
        }
        return c;
    }

    /** Is {@code type} the class {@code cls} or below it? */
    public boolean isA(String type, String cls) {
        for (String t = type; t != null; t = classes.get(t).parent) {
            if (t.equals(cls)) {
                return true;
            }
        }
        return false;
    }

    /** The classes strictly below {@code cls}. */
    public List<String> below(String cls) {
        List<String> out = new ArrayList<>();
        for (Cls c : classes.values()) {
            if (!c.name.equals(cls) && isA(c.name, cls)) {
                out.add(c.name);
            }
        }
        return out;
    }
}
