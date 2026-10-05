// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.ValueSpecification;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A mapping's CLASS mappings read back -- the mirror of {@link MappingEmitter}'s class-mapping arms:
 * relational, pureInstance, operation, mergeOperation, relation, aggregationAware, functionInstance,
 * serviceStore and MongoDB, with their property mappings ({@link PropertyMappingReader}). What the wire
 * does not carry comes back as the record's absent value: an operation's or an aggregation-aware
 * mapping's {@code extends} id, an aggregation-aware property mapping's transform.
 */
final class ClassMappingReader {

    private ClassMappingReader() {
    }

    /** The reader rule for each class-mapping {@code _type}. */
    private static final Map<String, Function<Wire, Protocol.PClassMapping>> CLASS_MAPPINGS = Map.of(
            "relational", ClassMappingReader::relational,
            "pureInstance", ClassMappingReader::pure,
            "operation", ClassMappingReader::operation,
            "mergeOperation", ClassMappingReader::merge,
            "relation", RelationMappingReader::relation,
            "aggregationAware", ClassMappingReader::aggregationAware,
            "functionInstance", ClassMappingReader::function,
            "serviceStore", ServiceStoreMappingReader::serviceStore,
            "MongoDB", ClassMappingReader::mongo);

    static Protocol.PClassMapping classMapping(Json.Node node) {
        Wire w = Wire.of(node, "class mapping");
        return w.done(Wire.rule(CLASS_MAPPINGS, w.type(), "class mapping").apply(w));
    }

    /** An aggregation-aware mapping's set implementations: relational, pure or function only. */
    private static Protocol.PClassMapping nested(Json.Node node) {
        Wire w = Wire.of(node, "nested class mapping");
        String type = w.type();
        Protocol.PClassMapping out;
        if ("relational".equals(type)) {
            out = relational(w);
        } else if ("pureInstance".equals(type)) {
            out = pure(w);
        } else if ("functionInstance".equals(type)) {
            out = function(w);
        } else {
            throw Wire.refuse("no reader rule for a nested class mapping of _type '" + type + "'");
        }
        return w.done(out);
    }

    private static Protocol.PClassMapping relational(Wire w) {
        Wire f = w.optObj("filter");
        Protocol.PFilterMapping filter = null;
        if (f != null) {
            Wire ptr = f.obj("filter");
            String db = ptr.str("db");
            String name = ptr.done(ptr.str("name"));
            filter = f.done(new Protocol.PFilterMapping(db, name, f.list("joins", StoreReader::joinPtr), f.span()));
        }
        Json.Node main = w.opt("mainTable");
        return new Protocol.PClassMappingRel(w.str("class"), w.span("classSourceInformation"), w.optStr("id"),
                w.bool("root"), w.bool("distinct"), w.optStr("extendsClassMappingId"), filter,
                w.list("groupBy", StoreReader::relOp), main == null ? null : StoreReader.tablePtr(main),
                w.list("primaryKey", StoreReader::relOp),
                w.list("propertyMappings", PropertyMappingReader::relational), w.span());
    }

    private static Protocol.PClassMapping pure(Wire w) {
        List<ValueSpecification> filter = null;
        Json.Node f = w.opt("filter");
        if (f != null) {
            filter = bareLambda(f, "pure filter");
        }
        String srcClass = w.optStr("srcClass");
        SourceInfo srcSpan = w.span("sourceClassSourceInformation");
        if (srcClass == null && srcSpan != null) {
            throw Wire.refuse("a sourceClassSourceInformation without its srcClass");
        }
        return new Protocol.PClassMappingPure(w.str("class"), w.span("classSourceInformation"),
                w.optStr("extendsClassMappingId"), w.optStr("id"), w.bool("root"), srcClass, srcSpan, filter,
                w.list("propertyMappings", PropertyMappingReader::pure), w.span());
    }

    /** {@code {"_type":"lambda","body":[..],"parameters":[]}} with NO span: its body. */
    static List<ValueSpecification> bareLambda(Json.Node node, String what) {
        Wire l = Wire.of(node, what + " lambda");
        l.constant("_type", "lambda");
        l.emptyArray("parameters");
        return l.done(l.list("body", ProtocolReader::valueSpec));
    }

    /** A span-less lambda wrapping exactly one statement. */
    static ValueSpecification bareLambdaOne(Json.Node node, String what) {
        List<ValueSpecification> body = bareLambda(node, what);
        if (body.size() != 1) {
            throw Wire.refuse(what + " lambda with " + body.size() + " statements (the record holds one)");
        }
        return body.get(0);
    }

    private static Protocol.PClassMapping operation(Wire w) {
        return new Protocol.PClassMappingOperation(w.str("class"), w.span("classSourceInformation"), w.optStr("id"),
                null, w.bool("root"), w.optStr("operation"), w.strings("parameters"), w.span());
    }

    /** {@code merge_...([p1,p2], {lambda})}: operation MERGE and the typed lambda wrapped in a bare one. */
    private static Protocol.PClassMapping merge(Wire w) {
        w.constant("operation", "MERGE");
        return new Protocol.PClassMappingMergeOperation(w.str("class"), w.span("classSourceInformation"),
                w.optStr("id"), w.bool("root"), w.strings("parameters"),
                bareLambdaOne(w.take("validationFunction"), "merge validation"), w.span());
    }

    private static Protocol.PClassMapping function(Wire w) {
        Json.Node body = w.opt("bodyLambda");
        Json.Node fn = w.opt("function");
        LambdaFunction lambda = body == null ? null : ProtocolReader.lambdaNode(body);
        PackageableElementPtr ptr = null;
        if (fn != null) {
            if (!(ProtocolReader.valueSpec(fn) instanceof PackageableElementPtr p)) {
                throw Wire.refuse("a functionInstance class mapping whose function is not an element pointer");
            }
            ptr = p;
        }
        if ((ptr == null) == (lambda == null)) {
            throw Wire.refuse("a functionInstance class mapping is EITHER a function or a body lambda");
        }
        return new Protocol.PClassMappingFunction(w.str("class"), w.span("classSourceInformation"), w.optStr("id"),
                w.optStr("extendsClassMappingId"), w.bool("root"), w.str("kind"), ptr, lambda, w.span());
    }

    private static Protocol.PClassMapping mongo(Wire w) {
        return new Protocol.PClassMappingMongoDb(w.str("class"), w.optStr("id"), w.bool("root"), w.str("storePath"),
                w.str("mainCollectionName"), w.optStr("bindingPath"));
    }

    // ---------------------------------------------------------------------
    // Aggregation aware
    // ---------------------------------------------------------------------

    private static Protocol.PClassMapping aggregationAware(Wire w) {
        List<Protocol.PPurePropertyMapping> aggPms = StoreReader.nonEmpty(w, "propertyMappings",
                PropertyMappingReader::aggregationAware);
        return new Protocol.PClassMappingAggregationAware(w.str("class"), w.str("id"),
                w.list("aggregateSetImplementations", ClassMappingReader::aggregateSet),
                nested(w.take("mainSetImplementation")), aggPms, w.bool("root"), w.span());
    }

    private static Protocol.PAggregateSetImplementation aggregateSet(Json.Node node) {
        Wire a = Wire.of(node, "aggregate set implementation");
        Wire spec = a.obj("aggregateSpecification");
        List<Protocol.PAggregateValue> values = spec.list("aggregateValues", n -> {
            Wire v = Wire.of(n, "aggregate value");
            return v.done(new Protocol.PAggregateValue(bareLambdaOne(v.take("mapFn"), "aggregate map"),
                    bareLambdaOne(v.take("aggregateFn"), "aggregate function")));
        });
        boolean canAggregate = spec.bool("canAggregate");
        List<ValueSpecification> groupBy = spec.list("groupByFunctions", n -> {
            Wire g = Wire.of(n, "group by function");
            return g.done(bareLambdaOne(g.take("groupByFn"), "group by"));
        });
        spec.done(values);
        return a.done(new Protocol.PAggregateSetImplementation(canAggregate, groupBy, values, a.integer("index"),
                nested(a.take("setImplementation"))));
    }
}
