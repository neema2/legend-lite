// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

/**
 * Data spaces read back -- the mirror of {@link TailEmitter}'s {@code dataSpace},
 * {@code dataSpaceContext}, {@code dataSpaceExecutable}, {@code dataSpaceLink} and
 * {@code dataSpaceSupport} rules. Lists the wire omits when unspelled read back {@code null}, as the
 * parser leaves them.
 */
final class DataSpaceReader {

    private DataSpaceReader() {
    }

    static Protocol.Element dataSpace(Wire w) {
        Protocol.PDataSpaceOperationalMetadata om = null;
        Wire o = w.optObj("operationalMetadata");
        if (o != null) {
            om = o.done(new Protocol.PDataSpaceOperationalMetadata(
                    StoreReader.nonEmpty(o, "coverageRegions", n -> o.asStr(n, "coverageRegions[]")),
                    o.optStr("updateFrequency"), o.span()));
        }
        Json.Node support = w.opt("supportInfo");
        return new Protocol.PDataSpace(w.str("package"), w.str("name"),
                DomainReader.stereotypes(w), DomainReader.taggedValues(w),
                w.optList("executionContexts", DataSpaceReader::context), w.optStr("defaultExecutionContext"),
                w.optStr("title"), w.optStr("description"), w.optList("executables", DataSpaceReader::executable),
                w.optList("diagrams", DataSpaceReader::diagram), support == null ? null : support(support), om,
                w.optList("elements", DataSpaceReader::elementRef), w.span());
    }

    /** A typed pointer whose {@code type} the emitter writes as a constant for its slot. */
    private static Protocol.PPointer typedPointer(Json.Node node, String type) {
        Protocol.PPointer p = DomainReader.pointer(node);
        if (!type.equals(p.type())) {
            throw Wire.refuse("a data space pointer typed '" + p.type() + "' where the wire writes " + type);
        }
        return p;
    }

    private static Protocol.PDataSpaceContext context(Json.Node node) {
        Wire c = Wire.of(node, "data space execution context");
        Json.Node rt = c.opt("defaultRuntime");
        Protocol.PPointer runtime = rt == null ? null : typedPointer(rt, "RUNTIME");
        Json.Node mp = c.opt("mapping");
        Protocol.PPointer mapping = mp == null ? null : typedPointer(mp, "MAPPING");
        Protocol.PDataSpaceMappingProvider provider = null;
        Wire p = c.optObj("mappingProvider");
        if (p != null) {
            Protocol.PElementRef element = EmbeddedDataReader.elementRef(p.take("element"));
            provider = p.done(new Protocol.PDataSpaceMappingProvider(element.path(), element.sourceInformation(),
                    p.strings("keys"), p.span()));
        }
        Protocol.PDataSpaceTestData testData = null;
        Wire t = c.optObj("testData");
        if (t != null) {
            t.constant("_type", "reference");
            Protocol.PPointer de = DomainReader.pointer(t.take("dataElement"));
            String kind;
            if ("DATA".equals(de.type())) {
                kind = "Reference";
            } else if ("DATASPACE".equals(de.type())) {
                kind = "DataspaceTestData";
            } else {
                throw Wire.refuse("no reader rule for a data space test data pointer typed '" + de.type() + "'");
            }
            SourceInfo span = t.span();
            StoreReader.sameSpan(de.sourceInformation(), span, "data space test data");
            testData = t.done(new Protocol.PDataSpaceTestData(kind, de.path(), span));
        }
        return c.done(new Protocol.PDataSpaceContext(c.str("name"), c.optStr("title"), c.optStr("description"),
                mapping == null ? null : mapping.path(), mapping == null ? null : mapping.sourceInformation(),
                provider, runtime == null ? null : runtime.path(), runtime == null ? null : runtime.sourceInformation(),
                testData, c.span()));
    }

    /**
     * An executable: the template form carries its query, the element form its pointer -- typed
     * {@code FUNCTION} exactly when the path is a signature.
     */
    private static Protocol.PDataSpaceExecutable executable(Json.Node node) {
        Wire e = Wire.of(node, "data space executable");
        String type = e.type();
        String path = null;
        SourceInfo pathSpan = null;
        com.legend.protocol.spec.ValueSpecification query = null;
        if ("dataSpaceTemplateExecutable".equals(type)) {
            query = ProtocolReader.valueSpec(e.take("query"));
        } else if ("dataSpacePackageableElementExecutable".equals(type)) {
            Wire x = e.obj("executable");
            path = x.str("path");
            pathSpan = x.span();
            String ptrType = x.optStr("type");
            boolean signature = path.indexOf('(') >= 0;
            if (signature != "FUNCTION".equals(ptrType) || (ptrType != null && !"FUNCTION".equals(ptrType))) {
                throw Wire.refuse("an executable pointer typed '" + ptrType + "' for path " + path);
            }
            x.done(path);
        } else {
            throw Wire.refuse("no reader rule for data space executable _type '" + type + "'");
        }
        Json.Node sample = e.opt("sampleValues");
        return e.done(new Protocol.PDataSpaceExecutable(e.optStr("id"), e.str("title"), e.optStr("description"),
                path, pathSpan, query, e.optStr("executionContextKey"),
                sample == null ? null : EmbeddedDataReader.relationElement(sample), e.span()));
    }

    private static Protocol.PDataSpaceDiagram diagram(Json.Node node) {
        Wire d = Wire.of(node, "data space diagram");
        Protocol.PElementRef ref = EmbeddedDataReader.elementRef(d.take("diagram"));
        return d.done(new Protocol.PDataSpaceDiagram(d.str("title"), d.optStr("description"), ref.path(),
                ref.sourceInformation(), d.span()));
    }

    /** An element reference: {@code exclude} is written only when true. */
    private static Protocol.PDataSpaceElementRef elementRef(Json.Node node) {
        Wire r = Wire.of(node, "data space element");
        Boolean exclude = r.optBool("exclude");
        if (Boolean.FALSE.equals(exclude)) {
            throw Wire.refuse("a data space element with exclude:false (the wire omits it)");
        }
        return r.done(new Protocol.PDataSpaceElementRef(r.str("path"), exclude != null, r.span()));
    }

    // ---------------------------------------------------------------------
    // Support information
    // ---------------------------------------------------------------------

    private static Protocol.PDataSpaceSupport support(Json.Node node) {
        Wire s = Wire.of(node, "data space support");
        String type = s.type();
        Protocol.PDataSpaceSupport out;
        if ("email".equals(type)) {
            out = new Protocol.PDataSpaceSupport.PSupportEmail(s.str("address"), s.optStr("documentationUrl"),
                    s.span());
        } else if ("combined".equals(type)) {
            out = new Protocol.PDataSpaceSupport.PSupportCombined(s.optStr("documentationUrl"), s.optStr("website"),
                    s.optStr("faqUrl"), s.optStr("supportUrl"), s.optStrings("emails"), s.span());
        } else if ("full".equals(type)) {
            out = new Protocol.PDataSpaceSupport.PSupportFull(link(s, "documentation"), link(s, "website"),
                    link(s, "faqUrl"), link(s, "supportUrl"), s.optList("emails", DataSpaceReader::email),
                    s.optList("expertise", DataSpaceReader::expertise), s.span());
        } else {
            throw Wire.refuse("no reader rule for data space support _type '" + type + "'");
        }
        return s.done(out);
    }

    private static Protocol.@com.legend.base.Nullable PDataSpaceLink link(Wire s, String key) {
        Wire l = s.optObj(key);
        if (l == null) {
            return null;
        }
        return l.done(new Protocol.PDataSpaceLink(l.optStr("label"), l.str("url"), l.span()));
    }

    private static Protocol.PDataSpaceEmail email(Json.Node node) {
        Wire e = Wire.of(node, "data space support email");
        return e.done(new Protocol.PDataSpaceEmail(e.str("title"), e.str("address"), e.span()));
    }

    private static Protocol.PDataSpaceExpertise expertise(Json.Node node) {
        Wire e = Wire.of(node, "data space expertise");
        return e.done(new Protocol.PDataSpaceExpertise(e.optStr("description"), e.optStrings("expertIds"),
                e.span()));
    }
}
