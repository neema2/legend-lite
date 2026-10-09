// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.Protocol.PRelOp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The relational store read back -- the mirror of {@link ProtocolEmitter}'s {@code relational} (the
 * {@code ###Relational} Database), its schemas, tables, views, joins, filters and milestoning, the
 * relational OPERATION trees ({@code relOp}) mappings share, and the {@code relationalMapper}
 * ({@code ###QueryPostProcessor}). Keys the emitter writes only when non-empty read back empty when absent.
 */
final class StoreReader {

    private StoreReader() {
    }

    static Protocol.Element database(Wire w) {
        // a list older JSON leaves out is empty, as the engine's Database and Store start it
        List<Protocol.PDbFilter> filters = w.listOrEmpty("filters", StoreReader::filter);
        List<Protocol.PIncludedStoreSpec> specs = w.listOrEmpty("includedStoreSpecifications",
                StoreReader::includedStoreSpec);
        if (w.has("includedStoreSpecifications") && specs.isEmpty()) {
            throw Wire.refuse("an empty includedStoreSpecifications (the wire omits it)");
        }
        List<Protocol.PTaggedValue> tvs = nonEmpty(w, "taggedValues", DomainReader::taggedValue);
        return new Protocol.PDatabase(w.str("package"), w.str("name"),
                w.listOrEmpty("includedStores", n -> DomainReader.pointer(n, "STORE")), specs,
                w.listOrEmpty("schemas", StoreReader::schema), w.listOrEmpty("joins", StoreReader::join), filters,
                DomainReader.stereotypes(w), tvs, w.span());
    }

    /** A list the emitter writes only when non-empty: absent is empty, present-and-empty is refused. */
    static <T> List<T> nonEmpty(Wire w, String key, Function<Json.Node, T> read) {
        List<T> out = w.listOrEmpty(key, read);
        if (w.has(key) && out.isEmpty()) {
            throw Wire.refuse(w.where() + "." + key + " is written empty (the wire omits an empty one)");
        }
        return out;
    }

    private static Protocol.PDbFilter filter(Json.Node node) {
        Wire f = Wire.of(node, "filter");
        String type = f.type();
        if (type == null) {
            throw Wire.refuse("a filter without its _type");
        }
        return f.done(new Protocol.PDbFilter(type, f.str("name"), relOp(f.take("operation")), f.span()));
    }

    /** {@code include <storeType> <path>}: the pointer and the entry share one span. */
    private static Protocol.PIncludedStoreSpec includedStoreSpec(Json.Node node) {
        Wire s = Wire.of(node, "included store specification");
        Wire p = s.obj("packageableElementPointer");
        String path = p.str("path");
        SourceInfo inner = p.done(p.span());
        SourceInfo outer = s.span();
        sameSpan(inner, outer, "included store specification");
        return s.done(new Protocol.PIncludedStoreSpec(path, s.str("storeType"), outer));
    }

    static void sameSpan(@com.legend.base.Nullable SourceInfo a, @com.legend.base.Nullable SourceInfo b,
            String what) {
        if (!java.util.Objects.equals(a, b)) {
            throw Wire.refuse(what + ": two spans the emitter writes from one (" + a + ", " + b + ")");
        }
    }

    private static Protocol.PDbJoin join(Json.Node node) {
        Wire j = Wire.of(node, "join");
        return j.done(new Protocol.PDbJoin(j.str("name"), relOp(j.take("operation")), j.span()));
    }

    /** A list older JSON leaves out is empty where the engine's {@code Schema} and {@code Table} start it empty. */
    private static Protocol.PDbSchema schema(Json.Node node) {
        Wire s = Wire.of(node, "schema");
        return s.done(new Protocol.PDbSchema(s.str("name"), s.listOrEmpty("tables", StoreReader::table),
                s.listOrEmpty("views", StoreReader::view), s.listOrEmpty("tabularFunctions", StoreReader::tabularFunction),
                nonEmpty(s, "stereotypes", DomainReader::stereotype),
                nonEmpty(s, "taggedValues", DomainReader::taggedValue), s.span()));
    }

    private static Protocol.PDbTable table(Json.Node node) {
        Wire t = Wire.of(node, "table");
        List<String> primaryKey = t.optStrings("primaryKey");
        return t.done(new Protocol.PDbTable(t.str("name"), t.listOrEmpty("columns", StoreReader::column),
                t.listOrEmpty("milestoning", StoreReader::milestoning), primaryKey == null ? List.of() : primaryKey,
                nonEmpty(t, "stereotypes", DomainReader::stereotype),
                nonEmpty(t, "taggedValues", DomainReader::taggedValue), t.span()));
    }

    /** A tabular function's slim wire: columns, name and span only. */
    private static Protocol.PDbTable tabularFunction(Json.Node node) {
        Wire t = Wire.of(node, "tabular function");
        return t.done(new Protocol.PDbTable(t.str("name"), t.list("columns", StoreReader::column), List.of(),
                List.of(), List.of(), List.of(), t.span()));
    }

    private static Protocol.PDbColumn column(Json.Node node) {
        Wire c = Wire.of(node, "column");
        Wire t = c.obj("type");
        String kind = t.type();
        if (kind == null) {
            throw Wire.refuse("a column type without its _type");
        }
        Protocol.PDbType type = t.done(new Protocol.PDbType(kind, t.optLong("size"), t.optLong("precision"),
                t.optLong("scale")));
        // nullable left out is false: the engine's Column holds a primitive boolean
        Boolean nullable = c.optBool("nullable");
        return c.done(new Protocol.PDbColumn(c.str("name"), nullable != null && nullable, type,
                nonEmpty(c, "stereotypes", DomainReader::stereotype),
                nonEmpty(c, "taggedValues", DomainReader::taggedValue), c.span()));
    }

    private static Protocol.PMilestoning milestoning(Json.Node node) {
        Wire m = Wire.of(node, "milestoning");
        String type = m.type();
        Protocol.PMilestoning out;
        if ("businessMilestoning".equals(type)) {
            out = new Protocol.PBusinessMilestoning(m.str("from"), m.str("thru"), m.bool("thruIsInclusive"),
                    infinity(m), m.span());
        } else if ("processingMilestoning".equals(type)) {
            out = new Protocol.PProcessingMilestoning(m.str("in"), m.str("out"), m.bool("outIsInclusive"),
                    infinity(m), m.span());
        } else if ("businessSnapshotMilestoning".equals(type)) {
            out = new Protocol.PBusinessSnapshotMilestoning(m.str("snapshotDate"), m.span());
        } else if ("processingSnapshotMilestoning".equals(type)) {
            out = new Protocol.PProcessingSnapshotMilestoning(m.str("snapshotDate"), m.span());
        } else {
            throw Wire.refuse("no reader rule for milestoning _type '" + type + "'");
        }
        return m.done(out);
    }

    /** The optional infinity date: {@code dateTime} when the value has a time part, else {@code strictDate}. */
    private static Protocol.@com.legend.base.Nullable PDateTimeLit infinity(Wire m) {
        Wire d = m.optObj("infinityDate");
        if (d == null) {
            return null;
        }
        String type = d.type();
        Protocol.PDateTimeLit lit = new Protocol.PDateTimeLit(d.str("value"), d.span());
        if (!lit.wireType().equals(type)) {
            throw Wire.refuse("an infinity date '" + lit.value() + "' tagged " + type);
        }
        return d.done(lit);
    }

    private static Protocol.PDbView view(Json.Node node) {
        Wire v = Wire.of(node, "view");
        Protocol.PViewFilter filter = null;
        Wire f = v.optObj("filter");
        if (f != null) {
            Wire ptr = f.obj("filter");
            String db = ptr.optStr("db");
            String name = ptr.done(ptr.str("name"));
            filter = f.done(new Protocol.PViewFilter(db, name, f.listOrEmpty("joins", StoreReader::joinPtr), f.span()));
        }
        // a list older JSON leaves out is empty, as the engine's View starts it
        List<String> primaryKey = v.optStrings("primaryKey");
        return v.done(new Protocol.PDbView(v.str("name"), v.listOrEmpty("columnMappings", StoreReader::viewColumn),
                nonEmpty(v, "stereotypes", DomainReader::stereotype),
                nonEmpty(v, "taggedValues", DomainReader::taggedValue), v.bool("distinct"), filter,
                v.listOrEmpty("groupBy", StoreReader::relOp), primaryKey == null ? List.of() : primaryKey, v.span()));
    }

    private static Protocol.PViewColumnMapping viewColumn(Json.Node node) {
        Wire c = Wire.of(node, "view column mapping");
        return c.done(new Protocol.PViewColumnMapping(c.str("name"), relOp(c.take("operation")), c.span()));
    }

    // ---------------------------------------------------------------------
    // Relational operations
    // ---------------------------------------------------------------------

    /** The reader rule for each relational-operation {@code _type}. */
    private static final Map<String, Function<Wire, PRelOp>> REL_OPS = Map.of(
            "relationalLambda", w -> new Protocol.PRelLambda(w.strings("parameterNames"), relOp(w.take("body")),
                    w.span()),
            "lambdaParameter", w -> new Protocol.PLambdaParam(w.str("name"), w.span()),
            "dynaFunc", w -> new Protocol.PDynaFunc(w.str("funcName"), w.list("parameters", StoreReader::relOp),
                    w.span()),
            "column", w -> new Protocol.PColumnRef(w.str("column"), tablePtr(w.take("table")), w.str("tableAlias"),
                    w.span()),
            "elemtWithJoins", w -> {
                Json.Node e = w.opt("relationalElement");
                return new Protocol.PElemtWithJoins(w.list("joins", StoreReader::joinPtr),
                        e == null ? null : relOp(e), w.span());
            },
            "literalList", w -> new Protocol.PRelLiteralList(w.list("values", StoreReader::listItem), w.span()),
            "literal", w -> new Protocol.PRelLiteral(literalValue(w.take("value")), w.span()));

    static PRelOp relOp(Json.Node node) {
        Wire w = Wire.of(node, "relational operation");
        return w.done(Wire.rule(REL_OPS, w.type(), "relational operation").apply(w));
    }

    /**
     * One element of a {@code literalList}: a {@code literal} wrapper whose {@code value} is the element
     * itself, written WITHOUT a {@code _type} (the engine's {@code Object}-typed field): its kind is told
     * by the field only that kind has.
     */
    private static PRelOp listItem(Json.Node node) {
        Wire wrapper = Wire.of(node, "literal list item");
        wrapper.constant("_type", "literal");
        Wire v = Wire.of(wrapper.take("value"), "literal list value");
        wrapper.done(v);
        String kind = v.has("funcName") ? "dynaFunc" : v.has("column") ? "column" : v.has("joins") ? "elemtWithJoins"
                : v.has("values") ? "literalList" : v.has("body") ? "relationalLambda"
                : v.has("name") ? "lambdaParameter" : v.has("value") ? "literal" : null;
        return v.done(Wire.rule(REL_OPS, kind, "untyped literal list value").apply(v));
    }

    /** A literal's value: a string, or a number kept as the parser holds it (a long, else a double). */
    private static Object literalValue(Json.Node v) {
        if (v instanceof Json.Str s) {
            return s.value();
        }
        if (v instanceof Json.Num n) {
            if (n.isInteger()) {
                return n.longValue();
            }
            // any spelling of the number (1.50, 1e3): the engine reads the Double and writes and prints it back as
            // Java spells it (1.5, 1000.0), as lite does
            return Wire.asDouble(n, "relational literal");
        }
        throw Wire.refuse("a relational literal that is neither a string nor a number: " + Wire.abbreviate(v));
    }

    /**
     * {@code {"_type":"Table", [database, mainTableDb,] schema, sourceInformation, table}}. Older JSON (the engine's Pure-side serializer) spells its {@code _type} {@code "table"} and may
     * give the {@code database} alone; the engine keeps both as written ({@code TablePtr} is a plain object) and its
     * mapping compile reads neither the spelling nor a missing {@code mainTableDb}. A {@code mainTableDb} alone does
     * not compile there ("Can't resolve from 'null' path"): refused.
     */
    static Protocol.PTablePtr tablePtr(Json.Node node) {
        Wire t = Wire.of(node, "table pointer");
        String type = t.str("_type");
        if (!type.equals("Table") && !type.equals("table")) {
            throw Wire.refuse("table pointer._type is '" + type + "': 'Table', or the older 'table'");
        }
        String db = t.optStr("database");
        String mainDb = t.optStr("mainTableDb");
        if (db == null && mainDb != null) {
            throw Wire.refuse("a table pointer with a mainTableDb and no database: the engine cannot resolve it");
        }
        return t.done(new Protocol.PTablePtr(db, mainDb, t.str("schema"), t.str("table"), t.span(),
                type.equals("Table") ? null : type));
    }

    static Protocol.PJoinPtr joinPtr(Json.Node node) {
        Wire j = Wire.of(node, "join pointer");
        return j.done(new Protocol.PJoinPtr(j.optStr("db"), j.optStr("joinType"), j.str("name"), j.span()));
    }

    // ---------------------------------------------------------------------
    // ###QueryPostProcessor
    // ---------------------------------------------------------------------

    static Protocol.Element relationalMapper(Wire w) {
        return new Protocol.PRelationalMapper(w.str("package"), w.str("name"),
                w.list("databaseMappers", StoreReader::databaseMapper),
                w.list("schemaMappers", StoreReader::schemaMapper), w.list("tableMappers", StoreReader::tableMapper),
                w.span());
    }

    private static Protocol.PDatabaseMapper databaseMapper(Json.Node node) {
        Wire d = Wire.of(node, "database mapper");
        return d.done(new Protocol.PDatabaseMapper(d.str("databaseName"),
                d.list("schemas", StoreReader::schemaPointer)));
    }

    private static Protocol.PSchemaMapper2 schemaMapper(Json.Node node) {
        Wire s = Wire.of(node, "schema mapper");
        return s.done(new Protocol.PSchemaMapper2(schemaPointer(s.take("from")), s.str("to")));
    }

    private static Protocol.PTableMapper2 tableMapper(Json.Node node) {
        Wire t = Wire.of(node, "table mapper");
        Wire from = t.obj("from");
        from.constant("_type", "Table");
        Protocol.PTablePointer2 ptr = from.done(new Protocol.PTablePointer2(from.str("database"),
                from.str("schema"), from.str("table"), from.span()));
        return t.done(new Protocol.PTableMapper2(ptr, t.str("to")));
    }

    private static Protocol.PSchemaPointer schemaPointer(Json.Node node) {
        Wire s = Wire.of(node, "schema pointer");
        s.constant("_type", "Schema");
        return s.done(new Protocol.PSchemaPointer(s.str("database"), s.str("schema"), s.span()));
    }

    /** A list of nodes read by {@code read}, for callers outside a {@link Wire}. */
    static <T> List<T> each(List<Json.Node> nodes, Function<Json.Node, T> read) {
        List<T> out = new ArrayList<>(nodes.size());
        for (Json.Node n : nodes) {
            out.add(read.apply(n));
        }
        return out;
    }
}
