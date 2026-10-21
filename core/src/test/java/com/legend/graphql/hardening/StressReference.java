// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.type.Type;
import com.legend.graphql.hardening.ReferenceModel.Kind;
import com.legend.graphql.hardening.ReferenceModel.Obj;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE REFERENCE OVER A REAL MODEL: the stress corpus's core domains read as the hardening reference reads the shop
 * (docs/GRAPHQL_STUDY_2026_10_18.md §13.11). Built from the corpus's TEXT -- its mappings' column and enumeration
 * mappings, its store's joins and keys, its seed's rows -- never from lite's compiled mapping: a class mapped by a
 * main table and plain columns is its table's rows, a property mapped to a column is the cell as the database casts
 * it, an enumeration mapping is its source-to-value table, an association mapped by one equality join is the rows
 * whose columns are equal (NULL equal to nothing). What does not fit that algebra (a filter, a join in a property
 * mapping, an embedded or otherwise set, a derived property, a class in a hierarchy, a temporal class, a composite or
 * unmapped key) is left out of the reference, never guessed: the fuzzer then asks nothing of it. The compiled model
 * is read for the properties' declared types only (the schema's own source of truth).
 */
public final class StressReference {

    private StressReference() {
    }

    /** The reference, and what the fuzzer needs to ask it: each class's unique key, the enumerations' values and
     *  GraphQL types by {@code Class.field}, and the classes the schema is scoped to (the reference's, closed). */
    public record Built(ReferenceModel model, Map<String, String> unique, Map<String, List<String>> enums,
            Map<String, String> enumTypes, List<String> scope, List<String> skipped) {
    }

    /** A class mapping as its text spells it: the class, its main table, its properties' columns. */
    private record ClassMap(String classFqn, String table, Map<String, String> columns, Map<String, String> enumIds) {
    }

    private record Join(String leftTable, String leftColumn, String rightTable, String rightColumn) {
    }

    private record AssocEnd(String associationFqn, String property, String join) {
    }

    /**
     * @param files the corpus's files (name, text)
     * @param mappings the mappings whose class mappings the reference reads, and whose association mappings it reads
     *     with those of {@code extraAssociations}
     */
    public static Built build(ModelContext ctx, List<Map.Entry<String, String>> files, Set<String> mappings,
            Set<String> extraAssociations, Map<String, String> tables) {
        StringBuilder all = new StringBuilder();
        for (Map.Entry<String, String> f : files) {
            all.append(f.getValue()).append('\n');
        }
        String text = all.toString();
        Map<String, Join> joins = joins(text);
        Map<String, List<String>> primaryKeys = primaryKeys(text);
        Map<String, Map<String, String>> enumMappings = new LinkedHashMap<>();
        List<ClassMap> classMaps = new ArrayList<>();
        List<AssocEnd> ends = new ArrayList<>();
        List<String> skipped = new ArrayList<>();
        for (String mapping : union(mappings, extraAssociations)) {
            String body = mappingBody(text, mapping);
            if (body == null) {
                throw new IllegalArgumentException("no mapping " + mapping + " in the corpus");
            }
            readMapping(body, mappings.contains(mapping), classMaps, ends, enumMappings, skipped);
        }
        ReferenceModel m = new ReferenceModel();
        Map<String, String> unique = new LinkedHashMap<>();
        Map<String, List<String>> enums = new LinkedHashMap<>();
        Map<String, String> enumTypes = new LinkedHashMap<>();
        Map<String, ClassMap> byClass = new LinkedHashMap<>();
        Map<String, List<Obj>> rowsByClass = new LinkedHashMap<>();
        Map<String, String> simpleToFqn = new TreeMap<>();
        for (ClassMap cm : classMaps) {
            String why = unusable(ctx, cm, primaryKeys);
            if (why == null && keyProperty(ctx, cm, primaryKeys) == null) {
                why = "its key column is not a property it maps";
            }
            String simple = simple(cm.classFqn());
            if (why == null && simpleToFqn.containsKey(simple)) {
                why = "a second class named " + simple;
            }
            if (why != null) {
                skipped.add(cm.classFqn() + ": " + why);
                continue;
            }
            simpleToFqn.put(simple, cm.classFqn());
            byClass.put(cm.classFqn(), cm);
        }
        for (ClassMap cm : byClass.values()) {
            String simple = simple(cm.classFqn());
            ReferenceModel.Cls cls = m.cls(simple);
            String csv = tables.get(cm.table());
            List<Map<String, String>> raw = csv == null ? List.of() : rows(csv);
            List<Obj> objs = new ArrayList<>();
            Map<String, Kind> kinds = new LinkedHashMap<>();
            for (Map.Entry<String, String> col : cm.columns().entrySet()) {
                Property p = ctx.findProperty(cm.classFqn(), col.getKey()).orElseThrow();
                Kind k = kindOf(p.type());
                if (k == null || p.multiplicity().isMany()) {
                    skipped.add(cm.classFqn() + "." + col.getKey() + ": a type the reference does not read");
                    continue;
                }
                kinds.put(col.getKey(), k);
            }
            for (Map<String, String> r : raw) {
                Map<String, Object> v = new LinkedHashMap<>();
                for (Map.Entry<String, Kind> e : kinds.entrySet()) {
                    String cell = r.get(cm.columns().get(e.getKey()));
                    String enumId = cm.enumIds().get(e.getKey());
                    v.put(e.getKey(), value(e.getValue(), cell, enumId == null ? null : enumMappings.get(enumId)));
                }
                for (Map.Entry<String, String> c : r.entrySet()) {
                    v.put("#" + c.getKey(), c.getValue());
                }
                objs.add(new Obj(simple, v));
            }
            for (Map.Entry<String, Kind> e : kinds.entrySet()) {
                cls.scalar(e.getKey(), e.getValue());
                if (e.getValue() == Kind.ENUM) {
                    var et = (Type.EnumType) ctx.findProperty(cm.classFqn(), e.getKey()).orElseThrow().type();
                    var te = ctx.findEnum(et.fqn()).orElseThrow();
                    enums.put(simple + "." + e.getKey(), List.copyOf(te.values()));
                    enumTypes.put(simple + "." + e.getKey(), simple(et.fqn()));
                }
            }
            unique.put(simple, java.util.Objects.requireNonNull(keyProperty(ctx, cm, primaryKeys)));
            List<Obj> fixed = List.copyOf(objs);
            cls.roots(() -> fixed);
            rowsByClass.put(cm.classFqn(), fixed);
        }
        for (AssocEnd end : ends) {
            relation(ctx, end, joins, byClass, rowsByClass, m, skipped);
        }
        Set<String> scope = closure(ctx, byClass.keySet());
        return new Built(m, unique, enums, enumTypes, List.copyOf(scope), skipped);
    }

    private static Set<String> union(Set<String> a, Set<String> b) {
        Set<String> out = new LinkedHashSet<>(a);
        out.addAll(b);
        return out;
    }

    /** Why a class mapping is outside the reference's algebra, or null. */
    private static @com.legend.base.Nullable String unusable(ModelContext ctx, ClassMap cm,
            Map<String, List<String>> primaryKeys) {
        var c = ctx.findClass(cm.classFqn()).orElse(null);
        if (c == null) {
            return "not a class of the model";
        }
        if (!c.superClassFqns().isEmpty()) {
            return "a subclass";
        }
        for (String fqn : ctx.elementFqns()) {
            var other = ctx.findClass(fqn).orElse(null);
            if (other != null && other.superClassFqns().contains(cm.classFqn())) {
                return "a superclass";
            }
        }
        if (com.legend.compiler.element.Temporal.strategyOf(ctx, cm.classFqn()) != null) {
            return "temporal";
        }
        List<String> pk = primaryKeys.get(cm.table());
        if (pk == null || pk.size() != 1) {
            return "its table has no single-column key";
        }
        return null;
    }

    /** The property mapped to the main table's one key column, of a kind the reference reads; null when none. */
    private static @com.legend.base.Nullable String keyProperty(ModelContext ctx, ClassMap cm,
            Map<String, List<String>> primaryKeys) {
        String pk = primaryKeys.get(cm.table()).get(0);
        for (Map.Entry<String, String> col : cm.columns().entrySet()) {
            Property p = ctx.findProperty(cm.classFqn(), col.getKey()).orElse(null);
            if (col.getValue().equals(pk) && !cm.enumIds().containsKey(col.getKey()) && p != null
                    && !p.multiplicity().isMany() && kindOf(p.type()) != null) {
                return col.getKey();
            }
        }
        return null;
    }

    private static void relation(ModelContext ctx, AssocEnd end, Map<String, Join> joins,
            Map<String, ClassMap> byClass, Map<String, List<Obj>> rowsByClass, ReferenceModel m, List<String> skipped) {
        var ad = ctx.findAssociation(end.associationFqn()).orElse(null);
        if (ad == null) {
            skipped.add(end.associationFqn() + ": no association");
            return;
        }
        // the property `end.property` is injected on the class the OTHER end names, and reaches its own end's class
        var mine = ad.property1().propertyName().equals(end.property()) ? ad.property1() : ad.property2();
        var other = mine == ad.property1() ? ad.property2() : ad.property1();
        String target = className(mine.targetClass());
        String owner = className(other.targetClass());
        if (target == null || owner == null || !byClass.containsKey(target) || !byClass.containsKey(owner)) {
            return;
        }
        Join j = joins.get(end.join());
        if (j == null) {
            skipped.add(end.associationFqn() + "." + end.property() + ": its join is not one equality");
            return;
        }
        String ownerTable = byClass.get(owner).table();
        String targetTable = byClass.get(target).table();
        String ownerCol;
        String targetCol;
        if (ownerTable.equals(targetTable)) {
            skipped.add(end.associationFqn() + "." + end.property() + ": a self join");
            return;
        } else if (j.leftTable().equals(ownerTable) && j.rightTable().equals(targetTable)) {
            ownerCol = j.leftColumn();
            targetCol = j.rightColumn();
        } else if (j.rightTable().equals(ownerTable) && j.leftTable().equals(targetTable)) {
            ownerCol = j.rightColumn();
            targetCol = j.leftColumn();
        } else {
            skipped.add(end.associationFqn() + "." + end.property() + ": its join is not between the two tables");
            return;
        }
        List<Obj> targets = rowsByClass.get(target);
        String oc = "#" + ownerCol;
        String tc = "#" + targetCol;
        ReferenceModel.Cls cls = m.get(simple(owner));
        if (mine.isToOne()) {
            cls.toOne(end.property(), simple(target), o -> {
                Object key = o.get(oc);
                if (key == null) {
                    return null;
                }
                for (Obj t : targets) {
                    if (key.equals(t.get(tc))) {
                        return t;
                    }
                }
                return null;
            });
        } else {
            cls.toMany(end.property(), simple(target), o -> {
                Object key = o.get(oc);
                List<Obj> out = new ArrayList<>();
                if (key != null) {
                    for (Obj t : targets) {
                        if (key.equals(t.get(tc))) {
                            out.add(t);
                        }
                    }
                }
                return out;
            });
        }
    }

    private static @com.legend.base.Nullable String className(com.legend.protocol.TypeExpression t) {
        return t instanceof com.legend.protocol.TypeExpression.NameRef nr ? nr.name() : null;
    }

    /** The reference's kind for a declared type, or null for one it does not read. */
    private static @com.legend.base.Nullable Kind kindOf(Type t) {
        return switch (t) {
            case Type.Primitive p -> switch (p) {
                case STRING -> Kind.STRING;
                case INTEGER -> Kind.BIGINT;
                case FLOAT -> Kind.FLOAT;
                case DECIMAL -> Kind.DECIMAL;
                case BOOLEAN -> Kind.BOOLEAN;
                case STRICT_DATE -> Kind.DATE;
                case DATE_TIME -> Kind.DATETIME;
                default -> null;
            };
            case Type.PrecisionDecimal d -> Kind.DECIMAL;
            case Type.EnumType e -> Kind.ENUM;
            default -> null;
        };
    }

    /** A seed cell as the database holds it under the property's kind: NULL for an empty cell. */
    private static @com.legend.base.Nullable Object value(Kind k, @com.legend.base.Nullable String cell,
            @com.legend.base.Nullable Map<String, String> enumMapping) {
        if (cell == null) {
            return null;
        }
        return switch (k) {
            case STRING -> cell;
            case BIGINT -> new BigDecimal(cell).longValueExact();
            case FLOAT -> Double.parseDouble(cell);
            case DECIMAL -> new BigDecimal(cell);
            case BOOLEAN -> cell.equalsIgnoreCase("true") || cell.equals("1");
            case DATE -> LocalDate.parse(cell.length() > 10 ? cell.substring(0, 10) : cell);
            case DATETIME -> cell.length() == 10 ? LocalDate.parse(cell).atStartOfDay()
                    : LocalDateTime.parse(cell.replace(' ', 'T'));
            case ENUM -> enumMapping == null ? cell : enumMapping.get(cell);
        };
    }

    // ---- reading the text ----

    private static final Pattern JOIN = Pattern.compile(
            "Join\\s+(\\w+)\\s*\\(\\s*(\\w+)\\.(\\w+)\\s*=\\s*(\\w+)\\.(\\w+)\\s*\\)");

    /** The store's joins that are one equality between two default-schema tables, by name. */
    private static Map<String, Join> joins(String text) {
        Map<String, Join> out = new LinkedHashMap<>();
        Matcher mt = JOIN.matcher(text);
        while (mt.find()) {
            out.put(mt.group(1), new Join(mt.group(2), mt.group(3), mt.group(4), mt.group(5)));
        }
        return out;
    }

    private static final Pattern TABLE = Pattern.compile("\\bTable\\s+(\\w+)\\s*\\(");

    /** Each table's PRIMARY KEY columns, by table name. */
    private static Map<String, List<String>> primaryKeys(String text) {
        Map<String, List<String>> out = new LinkedHashMap<>();
        Matcher mt = TABLE.matcher(text);
        while (mt.find()) {
            int end = closing(text, mt.end() - 1, '(', ')');
            String body = text.substring(mt.end(), end);
            List<String> pk = new ArrayList<>();
            for (String col : splitTop(body)) {
                String c = col.strip();
                if (c.contains("PRIMARY KEY")) {
                    pk.add(c.split("\\s+")[0].replace("\"", ""));
                }
            }
            out.putIfAbsent(mt.group(1), pk);
        }
        return out;
    }

    /** The text between a mapping's own parentheses. */
    private static @com.legend.base.Nullable String mappingBody(String text, String fqn) {
        Matcher mt = Pattern.compile("\\bMapping\\s+" + Pattern.quote(fqn) + "\\s*\\(").matcher(text);
        if (!mt.find()) {
            return null;
        }
        return text.substring(mt.end(), closing(text, mt.end() - 1, '(', ')'));
    }

    private static final Pattern ENTRY = Pattern.compile("\\*?([\\w:]+)(\\[(\\w+)\\])?(\\s+extends\\s+\\[\\w+\\])?"
            + "\\s*:\\s*(Relational|EnumerationMapping)\\s*([\\w:]*)\\s*\\{");
    private static final Pattern MAIN_TABLE = Pattern.compile("~mainTable\\s+\\[store::DB\\]\\s*(\\w+)\\s*");
    private static final Pattern COLUMN = Pattern.compile("\\[store::DB\\]\\s*(\\w+)\\.(\\w+)");
    private static final Pattern ENUM_COLUMN = Pattern.compile(
            "EnumerationMapping\\s+(\\w+)\\s*:\\s*\\[store::DB\\]\\s*(\\w+)\\.(\\w+)");
    private static final Pattern ASSOC_END = Pattern.compile("(\\w+)\\s*:\\s*\\[store::DB\\]\\s*@(\\w+)");
    private static final Pattern ENUM_ENTRY = Pattern.compile(
            "(\\w+)\\s*:\\s*(\\[[^\\]]*\\]|'(?:[^'\\\\]|\\\\.)*'|-?\\d+)");
    private static final Pattern ENUM_SOURCE = Pattern.compile("'((?:[^'\\\\]|\\\\.)*)'|(-?\\d+)");

    private static void readMapping(String body, boolean classes, List<ClassMap> classMaps, List<AssocEnd> ends,
            Map<String, Map<String, String>> enumMappings, List<String> skipped) {
        Matcher mt = ENTRY.matcher(body);
        int at = 0;
        while (mt.find(at)) {
            int open = mt.end() - 1;
            int close = closing(body, open, '{', '}');
            String block = body.substring(open + 1, close);
            at = close + 1;
            String head = mt.group(1);
            if (mt.group(5).equals("EnumerationMapping")) {
                Map<String, String> sourceToValue = new LinkedHashMap<>();
                Matcher em = ENUM_ENTRY.matcher(block);
                while (em.find()) {
                    Matcher sm = ENUM_SOURCE.matcher(em.group(2));
                    while (sm.find()) {
                        sourceToValue.put(sm.group(1) != null ? sm.group(1).replace("\\'", "'") : sm.group(2),
                                em.group(1));
                    }
                }
                // `trading::Side: EnumerationMapping SideMapping { ... }`: the enumeration, then the mapping's id
                enumMappings.put(mt.group(6).isEmpty() ? head : mt.group(6), sourceToValue);
                continue;
            }
            if (block.contains("AssociationMapping")) {
                for (String e : splitTop(block.substring(block.indexOf('(') + 1, block.lastIndexOf(')')))) {
                    Matcher am = ASSOC_END.matcher(e.strip());
                    if (am.matches()) {
                        ends.add(new AssocEnd(head, am.group(1), am.group(2)));
                    }
                }
                continue;
            }
            if (!classes) {
                continue;
            }
            if (mt.group(4) != null || block.contains("~filter") || block.contains("~distinct")
                    || block.contains("~groupBy") || block.contains("Otherwise") || mt.group(2) != null) {
                skipped.add(head + ": a set the reference does not read (a filter, an extension, a set id)");
                continue;
            }
            Matcher tm = MAIN_TABLE.matcher(block);
            if (!tm.find()) {
                skipped.add(head + ": no main table");
                continue;
            }
            String table = tm.group(1);
            String props = block.substring(0, tm.start()) + block.substring(tm.end());
            props = props.replaceAll("~primaryKey\\s*\\([^)]*\\)", "");
            Map<String, String> columns = new LinkedHashMap<>();
            Map<String, String> enumIds = new LinkedHashMap<>();
            for (String p : splitTop(props)) {
                String e = p.strip();
                int colon = e.indexOf(':');
                if (colon < 0) {
                    continue;
                }
                String name = e.substring(0, colon).strip();
                String rhs = e.substring(colon + 1).strip();
                Matcher em = ENUM_COLUMN.matcher(rhs);
                Matcher cm = COLUMN.matcher(rhs);
                if (em.matches() && em.group(2).equals(table)) {
                    columns.put(name, em.group(3));
                    enumIds.put(name, em.group(1));
                } else if (cm.matches() && cm.group(1).equals(table) && name.matches("\\w+")) {
                    columns.put(name, cm.group(2));
                }
            }
            classMaps.add(new ClassMap(head, table, columns, enumIds));
        }
    }

    /** The index of the bracket closing the one at {@code open}, past quoted text. */
    private static int closing(String s, int open, char o, char c) {
        int depth = 0;
        boolean quoted = false;
        for (int i = open; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (quoted) {
                if (ch == '\\') {
                    i++;
                } else if (ch == '\'') {
                    quoted = false;
                }
            } else if (ch == '\'') {
                quoted = true;
            } else if (ch == o) {
                depth++;
            } else if (ch == c && --depth == 0) {
                return i;
            }
        }
        throw new IllegalArgumentException("unbalanced " + o + " at " + open);
    }

    /** {@code s} split at the commas outside brackets and quotes. */
    private static List<String> splitTop(String s) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        boolean quoted = false;
        int from = 0;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (quoted) {
                if (ch == '\\') {
                    i++;
                } else if (ch == '\'') {
                    quoted = false;
                }
            } else if (ch == '\'') {
                quoted = true;
            } else if (ch == '(' || ch == '[' || ch == '{') {
                depth++;
            } else if (ch == ')' || ch == ']' || ch == '}') {
                depth--;
            } else if (ch == ',' && depth == 0) {
                out.add(s.substring(from, i));
                from = i + 1;
            }
        }
        out.add(s.substring(from));
        return out;
    }

    /** A seed table's CSV as rows by column: a cell empty or {@code ---null---} is NULL (CsvSeed's reading). */
    static List<Map<String, String>> rows(String csv) {
        List<String> lines = csv.lines().filter(l -> !l.isBlank()).toList();
        if (lines.isEmpty()) {
            return List.of();
        }
        String[] header = cells(lines.get(0));
        List<Map<String, String>> out = new ArrayList<>();
        for (String line : lines.subList(1, lines.size())) {
            String[] vals = cells(line);
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < header.length; i++) {
                String tok = i < vals.length ? vals[i].strip() : "";
                row.put(header[i].strip(), tok.isEmpty() || tok.equals("---null---") ? null : tok);
            }
            out.add(row);
        }
        return out;
    }

    /** One CSV line's cells, a double-quoted cell keeping its commas and reading {@code ""} as a quote. */
    private static String[] cells(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cell = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (quoted) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cell.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    cell.append(c);
                }
            } else if (c == '"') {
                quoted = true;
            } else if (c == ',') {
                out.add(cell.toString());
                cell.setLength(0);
            } else {
                cell.append(c);
            }
        }
        out.add(cell.toString());
        return out.toArray(String[]::new);
    }

    /** {@code seeds} closed under superclasses and the classes their properties reach: the schema's scope. */
    static Set<String> closure(ModelContext ctx, Set<String> seeds) {
        Set<String> out = new TreeSet<>();
        ArrayDeque<String> todo = new ArrayDeque<>(seeds);
        while (!todo.isEmpty()) {
            String fqn = todo.pop();
            if (!out.add(fqn)) {
                continue;
            }
            var c = ctx.findClass(fqn).orElseThrow(() -> new IllegalStateException("no class " + fqn));
            todo.addAll(c.superClassFqns());
            for (Property p : c.properties()) {
                if (p.type() instanceof Type.ClassType ct) {
                    todo.add(ct.fqn());
                }
            }
        }
        return out;
    }

    private static String simple(String fqn) {
        return fqn.substring(fqn.lastIndexOf(':') + 1);
    }
}
