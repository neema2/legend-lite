// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.spec.ValueSpecification;

import java.util.List;

/**
 * A {@code _type:"relation"} class mapping read back -- the mirror of {@link MappingEmitter}'s relation
 * arm, {@code relationFnEmbedded} and {@code relationFnPm}: the {@code ~func} descriptor pointer or the
 * {@code ~src} source lambda, and the column bindings (plain, nested-embedded, inline-embedded).
 */
final class RelationMappingReader {

    private RelationMappingReader() {
    }

    static Protocol.PClassMapping relation(Wire w) {
        String fn = null;
        SourceInfo fnSpan = null;
        Wire rf = w.optObj("relationFunction");
        if (rf != null) {
            rf.constant("type", "FUNCTION");
            fn = rf.str("path");
            fnSpan = rf.done(rf.span());
        }
        Json.Node src = w.opt("sourceLambda");
        return new Protocol.PClassMappingRelation(w.str("class"), w.optStr("id"), w.optStr("extendsClassMappingId"),
                w.strings("primaryKey"), w.list("propertyMappings", RelationMappingReader::propertyMapping), fn, fnSpan,
                src == null ? null : sourceLambda(src), w.bool("root"), w.span());
    }

    /**
     * {@code ~src}: a parameterless lambda over one statement -- the bare {@code fn()} spelled as a
     * parameterless {@code func} (the function form), or any other expression.
     */
    private static Protocol.PRelationSrcLambda sourceLambda(Json.Node node) {
        Wire l = Wire.of(node, "relation source lambda");
        l.constant("_type", "lambda");
        l.emptyArray("parameters");
        List<Json.Node> body = l.arr("body");
        SourceInfo span = l.span();
        l.done(body);
        if (body.size() != 1) {
            throw Wire.refuse("a relation source lambda with " + body.size() + " statements");
        }
        Json.Node stmt = body.get(0);
        if (stmt instanceof Json.Obj o && "func".equals(o.getStringOr("_type", null))) {
            Wire f = Wire.of(stmt, "relation source function");
            f.type();
            String name = f.str("function");
            if (f.arr("parameters").isEmpty()) {
                return f.done(new Protocol.PRelationSrcLambda(name, f.span(), null, span));
            }
        }
        return new Protocol.PRelationSrcLambda(null, null, ProtocolReader.valueSpec(stmt), span);
    }

    /** A binding: inline-embedded ({@code prop () Inline[set]}, an {@code id}), nested-embedded, or plain. */
    private static Protocol.PRelationFnPropertyMapping propertyMapping(Json.Node node) {
        Wire w = Wire.of(node, "relation property mapping");
        String type = w.type();
        if ("relationFunctionPropertyMapping".equals(type)) {
            return w.done(plain(w));
        }
        if (!"relationFunctionEmbeddedPropertyMapping".equals(type)) {
            throw Wire.refuse("no reader rule for relation property mapping _type '" + type + "'");
        }
        Wire p = w.obj("property");
        String owner = p.str("class");
        String property = p.str("property");
        SourceInfo propSpan = p.done(p.span());
        String inline = w.optStr("id");
        List<Protocol.PRelationFnPropertyMapping> nested;
        if (inline != null) {
            w.emptyArray("propertyMappings");
            nested = null;
        } else {
            nested = w.list("propertyMappings", n -> {
                Wire pm = Wire.of(n, "nested relation property mapping");
                pm.constant("_type", "relationFunctionPropertyMapping");
                return pm.done(plain(pm));
            });
        }
        return w.done(new Protocol.PRelationFnPropertyMapping(owner, null, property, propSpan, null, inline, nested,
                null, w.optStr("source"), null, null, null, w.span()));
    }

    /** {@code relationFunctionPropertyMapping}: a column, or a value lambda ({@code valueFn}) with its span. */
    private static Protocol.PRelationFnPropertyMapping plain(Wire w) {
        Wire p = w.obj("property");
        String owner = p.optStr("class");
        String property = p.str("property");
        SourceInfo propSpan = p.done(p.span());
        ValueSpecification expr = null;
        SourceInfo exprSpan = null;
        Wire v = w.optObj("valueFn");
        if (v != null) {
            v.constant("_type", "lambda");
            v.emptyArray("parameters");
            List<ValueSpecification> body = v.list("body", ProtocolReader::valueSpec);
            if (body.size() != 1) {
                throw Wire.refuse("a relation valueFn with " + body.size() + " statements");
            }
            expr = body.get(0);
            exprSpan = v.done(v.span());
        }
        return new Protocol.PRelationFnPropertyMapping(owner, PropertyMappingReader.bindingTransformer(w), property,
                propSpan, w.optStr("column"), null, null, PropertyMappingReader.localProperty(w), w.optStr("source"),
                w.optStr("enumMappingId"), expr, exprSpan, w.span());
    }
}
