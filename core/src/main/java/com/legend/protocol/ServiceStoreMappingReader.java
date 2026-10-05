// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@code _type:"serviceStore"} class mapping read back -- the mirror of {@link MappingEmitter}'s
 * {@code serviceStoreClassMapping}, {@code servicePtr}, {@code groupPtr} and {@code transformLambda}:
 * local mapping properties, and per service its path offset, request build info and the dotted service
 * pointer (parent groups nested outermost-last on the wire).
 */
final class ServiceStoreMappingReader {

    private ServiceStoreMappingReader() {
    }

    static Protocol.PClassMapping serviceStore(Wire w) {
        return new Protocol.PServiceStoreClassMapping(w.str("class"), w.span("classSourceInformation"),
                w.optStr("id"), w.bool("root"),
                w.list("localMappingProperties", ServiceStoreMappingReader::localProp),
                w.list("servicesMapping", ServiceStoreMappingReader::serviceMapping), w.span());
    }

    private static Protocol.PServiceStoreLocalProp localProp(Json.Node node) {
        Wire p = Wire.of(node, "service store local property");
        Wire m = p.obj("multiplicity");
        int lower = m.integer("lowerBound");
        Integer upper = m.done(m.optInt("upperBound"));
        return p.done(new Protocol.PServiceStoreLocalProp(p.str("name"), p.str("type"), lower, upper, p.span()));
    }

    private static Protocol.PServiceMapping serviceMapping(Json.Node node) {
        Wire s = Wire.of(node, "service mapping");
        Protocol.PPathOffset offset = null;
        Wire po = s.optObj("pathOffset");
        if (po != null) {
            List<String> path = po.list("path", n -> {
                Wire seg = Wire.of(n, "path offset segment");
                seg.constant("_type", "propertyPath");
                seg.emptyArray("parameters");
                return seg.done(seg.str("property"));
            });
            offset = po.done(new Protocol.PPathOffset(po.str("startType"), path));
        }
        Json.Node req = s.opt("requestBuildInfo");
        return s.done(new Protocol.PServiceMapping(servicePtr(s.take("service")), offset,
                req == null ? null : request(req), s.span()));
    }

    private static Protocol.PRequestBuildInfo request(Json.Node node) {
        Wire r = Wire.of(node, "request build info");
        Protocol.PBodyBuildInfo body = null;
        Wire b = r.optObj("requestBodyBuildInfo");
        if (b != null) {
            Transform t = transform(b.take("transform"));
            body = b.done(new Protocol.PBodyBuildInfo(t.expr(), t.span(), b.span()));
        }
        Protocol.PParametersBuildInfo params = null;
        Wire p = r.optObj("requestParametersBuildInfo");
        if (p != null) {
            List<Protocol.PParameterBuildInfo> entries = p.list("parameterBuildInfoList", n -> {
                Wire e = Wire.of(n, "parameter build info");
                Transform t = transform(e.take("transform"));
                return e.done(new Protocol.PParameterBuildInfo(e.str("serviceParameter"), t.expr(), t.span(),
                        e.span()));
            });
            params = p.done(new Protocol.PParametersBuildInfo(entries, p.span()));
        }
        if (body == null && params == null) {
            throw Wire.refuse("a request build info with neither body nor parameters");
        }
        return r.done(new Protocol.PRequestBuildInfo(params, body, r.span()));
    }

    /** A transform: a parameterless lambda over one expression, spanning that expression. */
    private record Transform(ValueSpecification expr, @com.legend.base.Nullable SourceInfo span) {
    }

    private static Transform transform(Json.Node node) {
        Wire l = Wire.of(node, "transform lambda");
        l.constant("_type", "lambda");
        l.emptyArray("parameters");
        List<ValueSpecification> body = l.list("body", ProtocolReader::valueSpec);
        if (body.size() != 1) {
            throw Wire.refuse("a transform lambda with " + body.size() + " statements");
        }
        return l.done(new Transform(body.get(0), l.span()));
    }

    /** The dotted service pointer: groups nest as {@code parent}s, the service store named at every level. */
    private static Protocol.PServicePtr servicePtr(Json.Node node) {
        Wire s = Wire.of(node, "service pointer");
        String store = s.str("serviceStore");
        List<Protocol.PServiceSegment> segments = new ArrayList<>();
        segments.add(new Protocol.PServiceSegment(s.str("service"), s.span()));
        Json.Node parent = s.opt("parent");
        s.done(store);
        while (parent != null) {
            Wire g = Wire.of(parent, "service group pointer");
            if (!store.equals(g.str("serviceStore"))) {
                throw Wire.refuse("a service group pointer naming another service store");
            }
            segments.add(new Protocol.PServiceSegment(g.str("serviceGroup"), g.span()));
            parent = g.opt("parent");
            g.done(store);
        }
        Collections.reverse(segments);
        return new Protocol.PServicePtr(store, segments);
    }
}
