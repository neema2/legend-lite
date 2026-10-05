// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.List;

import static com.legend.protocol.ProtocolEmitter.joinPtr;
import static com.legend.protocol.ProtocolEmitter.pointer;
import static com.legend.protocol.ProtocolEmitter.relOp;
import static com.legend.protocol.ProtocolEmitter.srcInfo;
import static com.legend.protocol.ProtocolEmitter.str;
import static com.legend.protocol.ProtocolEmitter.tablePtr;
import static com.legend.protocol.ProtocolEmitter.valueSpec;

/** {@code ###Mapping} wire emission — split from {@link ProtocolEmitter}
 *  when the Mapping leg outgrew the file guardrail; byte behavior
 *  identical, every arm probe-derived (ZMappingProbe). */
final class MappingEmitter {

    private MappingEmitter() {
    }


    /** {@code _type:"mapping"} envelope (ZMappingProbe). */
    static void mapping(StringBuilder b, Protocol.PMapping m) {
        b.append("{\"_type\":\"mapping\",\"associationMappings\":[");
        for (int i = 0; i < m.associationMappings().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            associationMapping(b, m.associationMappings().get(i));
        }
        b.append("],\"classMappings\":[");
        for (int i = 0; i < m.classMappings().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            switch (m.classMappings().get(i)) {
                case Protocol.PServiceStoreClassMapping ss ->
                        serviceStoreClassMapping(b, ss);
                case Protocol.PClassMappingMongoDb mg ->
                        mongoDbClassMapping(b, mg);
                case Protocol.PClassMappingRel r -> relClassMapping(b, r);
                case Protocol.PClassMappingPure pu -> pureClassMapping(b, pu);
                case Protocol.PClassMappingFunction fi -> functionClassMapping(b, fi);
                case Protocol.PClassMappingAggregationAware aa ->
                        aggregationAware(b, aa);
                case Protocol.PClassMappingMergeOperation mo -> {
                    b.append("{\"_type\":\"mergeOperation\",\"class\":");
                    str(b, mo.className());
                    b.append(",\"classSourceInformation\":");
                    srcInfo(b, mo.classSourceInformation());
                    if (mo.id() != null) {
                        b.append(",\"id\":");
                        str(b, mo.id());
                    }
                    b.append(",\"operation\":\"MERGE\",\"parameters\":[");
                    for (int j = 0; j < mo.parameters().size(); j++) {
                        if (j > 0) {
                            b.append(',');
                        }
                        str(b, mo.parameters().get(j));
                    }
                    b.append("],\"root\":").append(mo.root());
                    b.append(",\"sourceInformation\":");
                    srcInfo(b, mo.sourceInformation());
                    b.append(",\"validationFunction\":"
                            + "{\"_type\":\"lambda\",\"body\":[");
                    valueSpec(b, mo.validationLambda());
                    b.append("],\"parameters\":[]}}");
                }
                case Protocol.PClassMappingRelation rf -> {
                    b.append("{\"_type\":\"relation\",\"class\":");
                    str(b, rf.className());
                    if (rf.extendsClassMappingId() != null) {
                        b.append(",\"extendsClassMappingId\":");
                        str(b, rf.extendsClassMappingId());
                    }
                    if (rf.id() != null) {
                        b.append(",\"id\":");
                        str(b, rf.id());
                    }
                    b.append(",\"primaryKey\":[");
                    for (int j = 0; j < rf.primaryKey().size(); j++) {
                        if (j > 0) {
                            b.append(',');
                        }
                        str(b, rf.primaryKey().get(j));
                    }
                    b.append("],\"propertyMappings\":[");
                    for (int j = 0; j < rf.propertyMappings().size(); j++) {
                        if (j > 0) {
                            b.append(',');
                        }
                        Protocol.PRelationFnPropertyMapping pm =
                                rf.propertyMappings().get(j);
                        if (pm.nested() != null) {
                            relationFnEmbedded(b, pm);
                            continue;
                        }
                        if (pm.inlineSetId() != null) {
                            // prop () Inline [set] (probe relation-inline)
                            b.append("{\"_type\":\"relationFunction"
                                    + "EmbeddedPropertyMapping\",\"id\":");
                            str(b, pm.inlineSetId());
                            b.append(",\"property\":{\"class\":");
                            str(b, java.util.Objects.requireNonNull(
                                    pm.ownerClass()));
                            b.append(",\"property\":");
                            str(b, pm.property());
                            b.append(",\"sourceInformation\":");
                            srcInfo(b, pm.propertySourceInformation());
                            b.append("},\"propertyMappings\":[]");
                            if (pm.source() != null) {
                                b.append(",\"source\":");
                                str(b, pm.source());
                            }
                            b.append(",\"sourceInformation\":");
                            srcInfo(b, pm.sourceInformation());
                            b.append('}');
                            continue;
                        }
                        relationFnPm(b, pm);
                    }
                    b.append(']');
                    if (rf.relationFunction() != null) {
                        b.append(",\"relationFunction\":{\"path\":");
                        str(b, rf.relationFunction());
                        b.append(",\"sourceInformation\":");
                        srcInfo(b, java.util.Objects.requireNonNull(
                                rf.relationFunctionSourceInformation()));
                        b.append(",\"type\":\"FUNCTION\"}");
                    }
                    b.append(",\"root\":").append(rf.root());
                    b.append(",\"sourceInformation\":");
                    srcInfo(b, rf.sourceInformation());
                    if (rf.sourceLambda() != null) {
                        // ~src row-source form (probe ZRelationMappingProbe):
                        // one func node over the BARE name; wrapper span is
                        // the parser's shifted descriptor span
                        Protocol.PRelationSrcLambda sl = rf.sourceLambda();
                        b.append(",\"sourceLambda\":{\"_type\":\"lambda\","
                                + "\"body\":[");
                        if (sl.expr() != null) {
                            // ~src <expression> — the raw expression IS the
                            // body (harvest '~src 1')
                            valueSpec(b, sl.expr());
                        } else {
                            b.append("{\"_type\":\"func\","
                                    + "\"function\":");
                            str(b, java.util.Objects.requireNonNull(
                                    sl.function()));
                            b.append(",\"parameters\":[],"
                                    + "\"sourceInformation\":");
                            srcInfo(b, java.util.Objects.requireNonNull(
                                    sl.functionSourceInformation()));
                            b.append('}');
                        }
                        b.append("],\"parameters\":[],"
                                + "\"sourceInformation\":");
                        srcInfo(b, sl.sourceInformation());
                        b.append('}');
                    }
                    b.append('}');
                }
                case Protocol.PClassMappingOperation om -> {
                    b.append("{\"_type\":\"operation\",\"class\":");
                    str(b, om.className());
                    b.append(",\"classSourceInformation\":");
                    srcInfo(b, om.classSourceInformation());
                    if (om.id() != null) {
                        b.append(",\"id\":");
                        str(b, om.id());
                    }
                    if (om.operation() != null) {
                        // MERGE emits NO discriminator (probe merge-op)
                        b.append(",\"operation\":");
                        str(b, om.operation());
                    }
                    b.append(",\"parameters\":[");
                    for (int j = 0; j < om.parameters().size(); j++) {
                        if (j > 0) {
                            b.append(',');
                        }
                        str(b, om.parameters().get(j));
                    }
                    b.append("],\"root\":").append(om.root());
                    b.append(",\"sourceInformation\":");
                    srcInfo(b, om.sourceInformation());
                    b.append('}');
                }
            }
        }
        b.append("],\"enumerationMappings\":[");
        for (int i = 0; i < m.enumerationMappings().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            enumerationMapping(b, m.enumerationMappings().get(i));
        }
        b.append("],\"includedMappings\":[");
        for (int i = 0; i < m.includedMappings().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            mappingInclude(b, m.includedMappings().get(i));
        }
        b.append("],\"name\":");
        str(b, m.name());
        b.append(",\"package\":");
        str(b, m.pkg());
        b.append(",\"sourceInformation\":");
        srcInfo(b, m.sourceInformation());
        if (!m.testSuites().isEmpty()) {
            b.append(",\"testSuites\":[");
            for (int i = 0; i < m.testSuites().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                testSuite(b, m.testSuites().get(i));
            }
            b.append(']');
        }
        b.append(",\"tests\":[");
        for (int i = 0; i < m.tests().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            Protocol.PLegacyMappingTest t = m.tests().get(i);
            b.append("{\"assert\":{\"_type\":"
                    + "\"expectedOutputMappingTestAssert\","
                    + "\"expectedOutput\":");
            str(b, t.expectedOutput());
            b.append(",\"sourceInformation\":");
            srcInfo(b, t.assertSourceInformation());
            b.append("},\"inputData\":[");
            for (int j = 0; j < t.inputData().size(); j++) {
                if (j > 0) {
                    b.append(',');
                }
                Protocol.PLegacyInputData in = t.inputData().get(j);
                if (in.relational()) {
                    b.append("{\"_type\":\"relational\",\"data\":");
                    str(b, in.data());
                    b.append(",\"database\":");
                    str(b, in.targetPath());
                } else {
                    b.append("{\"_type\":\"object\",\"data\":");
                    str(b, in.data());
                }
                b.append(",\"inputType\":");
                str(b, in.inputType());
                if (!in.relational()) {
                    b.append(",\"sourceClass\":");
                    str(b, in.targetPath());
                }
                b.append(",\"sourceInformation\":");
                srcInfo(b, in.sourceInformation());
                b.append('}');
            }
            b.append("],\"name\":");
            str(b, t.name());
            b.append(",\"query\":");
            valueSpec(b, t.query());
            b.append(",\"sourceInformation\":");
            srcInfo(b, t.sourceInformation());
            b.append('}');
        }
        b.append("]}");
    }


    static void associationMapping(StringBuilder b,
            Protocol.PAssociationMapping am) {
        switch (am) {
                // clean-sheet association binding — the same
                // pointer-XOR-lambda fork as functionInstance
                case Protocol.PFunctionAssociationMapping fa -> {
                    b.append("{\"_type\":\"functionAssociation\",\"association\":");
                    pointer(b, fa.association());
                    if (fa.bodyLambda() != null) {
                        b.append(",\"bodyLambda\":");
                        valueSpec(b, fa.bodyLambda());
                    }
                    if (fa.function() != null) {
                        b.append(",\"function\":");
                        valueSpec(b, fa.function());
                    }
                    b.append(",\"sourceInformation\":");
                    srcInfo(b, fa.sourceInformation());
                    b.append('}');
                }
                case Protocol.PRelAssociationMapping ra -> {
                    b.append("{\"_type\":\"relational\",\"association\":");
                    pointer(b, ra.association());
                    if (ra.id() != null) {
                        b.append(",\"id\":");
                        str(b, ra.id());
                    }
                    b.append(",\"propertyMappings\":[");
                    for (int j = 0; j < ra.propertyMappings().size(); j++) {
                        if (j > 0) {
                            b.append(',');
                        }
                        Protocol.PRelAssocPropertyMapping pm =
                                ra.propertyMappings().get(j);
                        b.append("{\"_type\":\"relationalPropertyMapping\","
                                + "\"property\":{\"property\":");
                        str(b, pm.property());
                        b.append(",\"sourceInformation\":");
                        srcInfo(b, pm.propertySourceInformation());
                        b.append("},\"relationalOperation\":");
                        relOp(b, pm.relationalOperation());
                        if (pm.source() != null) {
                            b.append(",\"source\":");
                            str(b, pm.source());
                        }
                        b.append(",\"sourceInformation\":");
                        srcInfo(b, pm.sourceInformation());
                        if (pm.target() != null) {
                            b.append(",\"target\":");
                            str(b, pm.target());
                        }
                        b.append('}');
                    }
                    b.append("],\"sourceInformation\":");
                    srcInfo(b, ra.sourceInformation());
                    b.append(",\"stores\":[");
                    for (int j = 0; j < ra.stores().size(); j++) {
                        if (j > 0) {
                            b.append(',');
                        }
                        str(b, ra.stores().get(j));
                    }
                    b.append("]}");
                }
                case Protocol.PXStoreAssociationMapping xa -> {
                    b.append("{\"_type\":\"xStore\",\"association\":");
                    pointer(b, xa.association());
                    if (xa.id() != null) {
                        b.append(",\"id\":");
                        str(b, xa.id());
                    }
                    b.append(",\"propertyMappings\":[");
                    for (int j = 0; j < xa.propertyMappings().size(); j++) {
                        if (j > 0) {
                            b.append(',');
                        }
                        Protocol.PXStorePropertyMapping pm =
                                xa.propertyMappings().get(j);
                        b.append("{\"_type\":\"xStorePropertyMapping\","
                                + "\"crossExpression\":{\"_type\":\"lambda\","
                                + "\"body\":[");
                        for (int k = 0; k < pm.crossExpression().size(); k++) {
                            if (k > 0) {
                                b.append(',');
                            }
                            valueSpec(b, pm.crossExpression().get(k));
                        }
                        b.append("],\"parameters\":[]},\"property\":{\"class\":");
                        str(b, pm.ownerClass());
                        b.append(",\"property\":");
                        str(b, pm.property());
                        b.append(",\"sourceInformation\":");
                        srcInfo(b, pm.propertySourceInformation());
                        b.append("},\"source\":");
                        str(b, pm.source());
                        b.append(",\"sourceInformation\":");
                        srcInfo(b, pm.sourceInformation());
                        b.append(",\"target\":");
                        str(b, pm.target());
                        b.append('}');
                    }
                    b.append("],\"sourceInformation\":");
                    srcInfo(b, xa.sourceInformation());
                    b.append(",\"stores\":[]}");
                }
                case Protocol.PModelJoinAssociationMapping mj -> {
                    b.append("{\"_type\":\"modelJoin\",\"association\":");
                    pointer(b, mj.association());
                    if (mj.id() != null) {
                        b.append(",\"id\":");
                        str(b, mj.id());
                    }
                    b.append(",\"joinCondition\":");
                    valueSpec(b, mj.joinCondition());
                    b.append(",\"sourceInformation\":");
                    srcInfo(b, mj.sourceInformation());
                    b.append(",\"stores\":[]}");
                }
            }
            }


    /** {@code _type:"MongoDB"} class mapping — NO spans on this wire
     *  (ZTailProbe "mongodb-mapping"). */
    private static void mongoDbClassMapping(StringBuilder b,
            Protocol.PClassMappingMongoDb mg) {
        b.append("{\"_type\":\"MongoDB\"");
        if (mg.bindingPath() != null) {
            b.append(",\"bindingPath\":");
            str(b, mg.bindingPath());
        }
        b.append(",\"class\":");
        str(b, mg.className());
        if (mg.id() != null) {
            b.append(",\"id\":");
            str(b, mg.id());
        }
        b.append(",\"mainCollectionName\":");
        str(b, mg.mainCollectionName());
        b.append(",\"root\":").append(mg.root());
        b.append(",\"storePath\":");
        str(b, mg.storePath());
        b.append('}');
    }

    /** {@code _type:"serviceStore"} class mapping (ZTailProbe
     *  "servicestore-mapping" / "-rich" / "-body"): dotted service pointers
     *  nest parent group objects; transforms wrap as parameterless lambdas
     *  spanning their expression. */
    private static void serviceStoreClassMapping(StringBuilder b,
            Protocol.PServiceStoreClassMapping ss) {
        b.append("{\"_type\":\"serviceStore\",\"class\":");
        str(b, ss.className());
        b.append(",\"classSourceInformation\":");
        srcInfo(b, ss.classSpan());
        if (ss.id() != null) {
            b.append(",\"id\":");
            str(b, ss.id());
        }
        b.append(",\"localMappingProperties\":[");
        for (int i = 0; i < ss.localProps().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            Protocol.PServiceStoreLocalProp lp = ss.localProps().get(i);
            b.append("{\"multiplicity\":{\"lowerBound\":")
                    .append(lp.lowerBound());
            if (lp.upperBound() != null) {
                b.append(",\"upperBound\":").append(lp.upperBound());
            }
            b.append("},\"name\":");
            str(b, lp.name());
            b.append(",\"sourceInformation\":");
            srcInfo(b, lp.sourceInformation());
            b.append(",\"type\":");
            str(b, lp.type());
            b.append('}');
        }
        b.append("],\"root\":").append(ss.root());
        b.append(",\"servicesMapping\":[");
        for (int i = 0; i < ss.services().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            Protocol.PServiceMapping sm = ss.services().get(i);
            b.append('{');
            if (sm.pathOffset() != null) {
                b.append("\"pathOffset\":{\"path\":[");
                var segs = sm.pathOffset().propertyPath();
                for (int k = 0; k < segs.size(); k++) {
                    if (k > 0) {
                        b.append(',');
                    }
                    b.append("{\"_type\":\"propertyPath\","
                            + "\"parameters\":[],\"property\":");
                    str(b, segs.get(k));
                    b.append('}');
                }
                b.append("],\"startType\":");
                str(b, sm.pathOffset().startType());
                b.append("},");
            }
            if (sm.request() != null) {
                Protocol.PRequestBuildInfo req = sm.request();
                b.append("\"requestBuildInfo\":{");
                boolean first = true;
                if (req.body() != null) {
                    b.append("\"requestBodyBuildInfo\":{\"sourceInformation\":");
                    srcInfo(b, req.body().sourceInformation());
                    b.append(",\"transform\":");
                    transformLambda(b, req.body().transform(),
                            req.body().transformSpan());
                    b.append('}');
                    first = false;
                }
                if (req.parameters() != null) {
                    if (!first) {
                        b.append(',');
                    }
                    b.append("\"requestParametersBuildInfo\":{"
                            + "\"parameterBuildInfoList\":[");
                    var entries = req.parameters().entries();
                    for (int k = 0; k < entries.size(); k++) {
                        if (k > 0) {
                            b.append(',');
                        }
                        b.append("{\"serviceParameter\":");
                        str(b, entries.get(k).serviceParameter());
                        b.append(",\"sourceInformation\":");
                        srcInfo(b, entries.get(k).sourceInformation());
                        b.append(",\"transform\":");
                        transformLambda(b, entries.get(k).transform(),
                                entries.get(k).transformSpan());
                        b.append('}');
                    }
                    b.append(']');
                    if (req.parameters().sourceInformation() != null) {
                        b.append(",\"sourceInformation\":");
                        srcInfo(b, req.parameters().sourceInformation());
                    }
                    b.append('}');
                }
                if (req.sourceInformation() != null) {
                    b.append(",\"sourceInformation\":");
                    srcInfo(b, req.sourceInformation());
                }
                b.append("},");
            }
            b.append("\"service\":");
            servicePtr(b, sm.service());
            b.append(",\"sourceInformation\":");
            srcInfo(b, sm.sourceInformation());
            b.append('}');
        }
        b.append("],\"sourceInformation\":");
        srcInfo(b, ss.sourceInformation());
        b.append('}');
    }

    /** The nested group/service pointer chain: leading segments are
     *  GROUPS (outermost first), the last is the service. */
    private static void servicePtr(StringBuilder b, Protocol.PServicePtr ptr) {
        var segs = ptr.segments();
        int last = segs.size() - 1;
        b.append('{');
        if (last > 0) {
            b.append("\"parent\":");
            groupPtr(b, ptr, last - 1);
            b.append(',');
        }
        b.append("\"service\":");
        str(b, segs.get(last).name());
        b.append(",\"serviceStore\":");
        str(b, ptr.serviceStore());
        b.append(",\"sourceInformation\":");
        srcInfo(b, segs.get(last).sourceInformation());
        b.append('}');
    }

    private static void groupPtr(StringBuilder b, Protocol.PServicePtr ptr,
            int idx) {
        b.append('{');
        if (idx > 0) {
            b.append("\"parent\":");
            groupPtr(b, ptr, idx - 1);
            b.append(',');
        }
        b.append("\"serviceGroup\":");
        str(b, ptr.segments().get(idx).name());
        b.append(",\"serviceStore\":");
        str(b, ptr.serviceStore());
        b.append(",\"sourceInformation\":");
        srcInfo(b, ptr.segments().get(idx).sourceInformation());
        b.append('}');
    }

    /** A transform expression wraps as a parameterless lambda whose span is
     *  the EXPRESSION's own token range, threaded from the parse. */
    private static void transformLambda(StringBuilder b,
            com.legend.protocol.spec.ValueSpecification expr,
            com.legend.protocol.@com.legend.base.Nullable SourceInfo span) {
        b.append("{\"_type\":\"lambda\",\"body\":[");
        valueSpec(b, expr);
        b.append("],\"parameters\":[],\"sourceInformation\":");
        srcInfo(b, span);
        b.append('}');
    }

    static void mappingInclude(StringBuilder b,
            Protocol.PMappingInclude inc) {

            if (inc.includedDataSpace() != null) {
                // include dataspace (ZTailProbe "include-dataspace")
                b.append("{\"_type\":\"mappingIncludeDataSpace\","
                        + "\"includedDataSpace\":");
                str(b, inc.includedDataSpace());
                b.append(",\"sourceInformation\":");
                srcInfo(b, inc.sourceInformation());
                b.append('}');
                return;
            }
            b.append("{\"_type\":\"mappingIncludeMapping\","
                    + "\"includedMapping\":");
            str(b, java.util.Objects.requireNonNull(inc.includedMapping()));
            if (inc.sourceDatabasePath() != null) {
                b.append(",\"sourceDatabasePath\":");
                str(b, inc.sourceDatabasePath());
            }
            b.append(",\"sourceInformation\":");
            srcInfo(b, inc.sourceInformation());
            if (inc.targetDatabasePath() != null) {
                b.append(",\"targetDatabasePath\":");
                str(b, inc.targetDatabasePath());
            }
            b.append('}');
            }


    static void enumerationMapping(StringBuilder b,
            Protocol.PEnumerationMapping em) {

            b.append("{\"enumValueMappings\":[");
            for (int j = 0; j < em.enumValueMappings().size(); j++) {
                if (j > 0) {
                    b.append(',');
                }
                Protocol.PEnumValueMapping vm = em.enumValueMappings().get(j);
                b.append("{\"enumValue\":");
                str(b, vm.enumValue());
                b.append(",\"sourceValues\":[");
                for (int k = 0; k < vm.sourceValues().size(); k++) {
                    if (k > 0) {
                        b.append(',');
                    }
                    Protocol.PEnumSourceValue esv = vm.sourceValues().get(k);
                    Object v = esv.value();
                    if (esv.enumeration() != null) {
                        // [my::Other.bla] (probe enum-source-enumref)
                        b.append("{\"_type\":\"enumSourceValue\","
                                + "\"enumeration\":");
                        str(b, esv.enumeration());
                        b.append(",\"value\":");
                        str(b, (String) v);
                    } else if (v instanceof String sv) {
                        b.append("{\"_type\":\"stringSourceValue\",\"value\":");
                        str(b, sv);
                    } else {
                        b.append("{\"_type\":\"integerSourceValue\",\"value\":")
                                .append(v);
                    }
                    b.append('}');
                }
                b.append("]}");
            }
            b.append("],\"enumeration\":");
            pointer(b, em.enumeration());
            if (em.id() != null) {
                b.append(",\"id\":");
                str(b, em.id());
            }
            b.append(",\"sourceInformation\":");
            srcInfo(b, em.sourceInformation());
            b.append('}');
            }


    static void relClassMapping(StringBuilder b,
            Protocol.PClassMappingRel cm) {
        b.append("{\"_type\":\"relational\",\"class\":");
        str(b, cm.className());
        b.append(",\"classSourceInformation\":");
        srcInfo(b, cm.classSourceInformation());
        b.append(",\"distinct\":").append(cm.distinct());
        if (cm.extendsClassMappingId() != null) {
            b.append(",\"extendsClassMappingId\":");
            str(b, cm.extendsClassMappingId());
        }
        if (cm.filter() != null) {
            b.append(",\"filter\":");
            filterMapping(b, cm.filter());
        }
        b.append(",\"groupBy\":[");
        for (int i = 0; i < cm.groupBy().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            relOp(b, cm.groupBy().get(i));
        }
        b.append(']');
        if (cm.id() != null) {
            b.append(",\"id\":");
            str(b, cm.id());
        }
        if (cm.mainTable() != null) {
            b.append(",\"mainTable\":");
            tablePtr(b, cm.mainTable());
        }
        b.append(",\"primaryKey\":[");
        for (int i = 0; i < cm.primaryKey().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            relOp(b, cm.primaryKey().get(i));
        }
        b.append("],\"propertyMappings\":[");
        for (int i = 0; i < cm.propertyMappings().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            relPropertyMapping(b, cm.propertyMappings().get(i));
        }
        b.append("],\"root\":").append(cm.root());
        b.append(",\"sourceInformation\":");
        srcInfo(b, cm.sourceInformation());
        b.append('}');
    }


    /** {@code _type:"functionInstance"} — the clean-sheet function form.
     *  Keys alphabetical after the discriminator, as the sibling arms are;
     *  `function` and `bodyLambda` are mutually exclusive by construction,
     *  so exactly one appears. */
    static void functionClassMapping(StringBuilder b,
            Protocol.PClassMappingFunction cm) {
        b.append("{\"_type\":\"functionInstance\"");
        if (cm.bodyLambda() != null) {
            b.append(",\"bodyLambda\":");
            valueSpec(b, cm.bodyLambda());
        }
        b.append(",\"class\":");
        str(b, cm.className());
        b.append(",\"classSourceInformation\":");
        srcInfo(b, cm.classSourceInformation());
        if (cm.extendsClassMappingId() != null) {
            b.append(",\"extendsClassMappingId\":");
            str(b, cm.extendsClassMappingId());
        }
        if (cm.function() != null) {
            b.append(",\"function\":");
            valueSpec(b, cm.function());
        }
        if (cm.id() != null) {
            b.append(",\"id\":");
            str(b, cm.id());
        }
        b.append(",\"kind\":");
        str(b, cm.kind());
        b.append(",\"root\":").append(cm.root());
        b.append(",\"sourceInformation\":");
        srcInfo(b, cm.sourceInformation());
        b.append('}');
    }

    static void pureClassMapping(StringBuilder b,
            Protocol.PClassMappingPure cm) {
        b.append("{\"_type\":\"pureInstance\",\"class\":");
        str(b, cm.className());
        b.append(",\"classSourceInformation\":");
        srcInfo(b, cm.classSourceInformation());
        if (cm.extendsClassMappingId() != null) {
            b.append(",\"extendsClassMappingId\":");
            str(b, cm.extendsClassMappingId());
        }
        if (cm.filter() != null) {
            // ~filter lambda rides between the class span and id
            // (probe pure-filter)
            b.append(",\"filter\":{\"_type\":\"lambda\",\"body\":[");
            List<com.legend.protocol.spec.ValueSpecification> fb = cm.filter();
            for (int i = 0; i < fb.size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                valueSpec(b, fb.get(i));
            }
            b.append("],\"parameters\":[]}");
        }
        if (cm.id() != null) {
            b.append(",\"id\":");
            str(b, cm.id());
        }
        b.append(",\"propertyMappings\":[");
        for (int i = 0; i < cm.propertyMappings().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            Protocol.PPurePropertyMapping pm = cm.propertyMappings().get(i);
            b.append("{\"_type\":\"purePropertyMapping\",");
            if (pm.enumMappingId() != null) {
                b.append("\"enumMappingId\":");
                str(b, pm.enumMappingId());
                b.append(',');
            }
            b.append("\"explodeProperty\":").append(pm.explodeProperty());
            if (pm.localMappingProperty() != null) {
                Protocol.PLocalProp lp = pm.localMappingProperty();
                b.append(",\"localMappingProperty\":{\"multiplicity\":"
                        + "{\"lowerBound\":");
                b.append(lp.lowerBound());
                if (lp.upperBound() != null) {
                    b.append(",\"upperBound\":").append(lp.upperBound());
                }
                b.append("},\"sourceInformation\":");
                srcInfo(b, lp.sourceInformation());
                b.append(",\"type\":");
                str(b, lp.type());
                b.append('}');
            }
            b.append(",\"property\":{");
            if (pm.ownerClass() != null) {
                b.append("\"class\":");
                str(b, pm.ownerClass());
                b.append(',');
            }
            b.append("\"property\":");
            str(b, pm.property());
            b.append(",\"sourceInformation\":");
            srcInfo(b, pm.propertySourceInformation());
            b.append('}');
            if (pm.source() != null) {
                b.append(",\"source\":");
                str(b, pm.source());
            }
            b.append(",\"sourceInformation\":");
            srcInfo(b, pm.sourceInformation());
            if (pm.target() != null) {
                b.append(",\"target\":");
                str(b, pm.target());
            }
            b.append(",\"transform\":{\"_type\":\"lambda\",\"body\":[");
            for (int j = 0; j < pm.transform().size(); j++) {
                if (j > 0) {
                    b.append(',');
                }
                valueSpec(b, pm.transform().get(j));
            }
            b.append("],\"parameters\":[]}}");
        }
        b.append("],\"root\":").append(cm.root());
        if (cm.srcClass() != null && cm.sourceClassSourceInformation() != null) {
            b.append(",\"sourceClassSourceInformation\":");
            srcInfo(b, cm.sourceClassSourceInformation());
        }
        b.append(",\"sourceInformation\":");
        srcInfo(b, cm.sourceInformation());
        if (cm.srcClass() != null) {
            b.append(",\"srcClass\":");
            str(b, cm.srcClass());
        }
        b.append('}');
    }


    static void aggregationAware(StringBuilder b,
            Protocol.PClassMappingAggregationAware aa) {
        b.append("{\"_type\":\"aggregationAware\","
                + "\"aggregateSetImplementations\":[");
        for (int i = 0; i < aa.aggregateSetImplementations().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            Protocol.PAggregateSetImplementation asi =
                    aa.aggregateSetImplementations().get(i);
            b.append("{\"aggregateSpecification\":{\"aggregateValues\":[");
            for (int j = 0; j < asi.aggregateValues().size(); j++) {
                if (j > 0) {
                    b.append(',');
                }
                Protocol.PAggregateValue av = asi.aggregateValues().get(j);
                b.append("{\"aggregateFn\":");
                lambdaEnvelope(b, av.aggregateFn());
                b.append(",\"mapFn\":");
                lambdaEnvelope(b, av.mapFn());
                b.append('}');
            }
            b.append("],\"canAggregate\":").append(asi.canAggregate());
            b.append(",\"groupByFunctions\":[");
            for (int j = 0; j < asi.groupByFunctions().size(); j++) {
                if (j > 0) {
                    b.append(',');
                }
                b.append("{\"groupByFn\":");
                lambdaEnvelope(b, asi.groupByFunctions().get(j));
                b.append('}');
            }
            b.append("]},\"index\":").append(asi.index());
            b.append(",\"setImplementation\":");
            nestedClassMapping(b, asi.setImplementation());
            b.append('}');
        }
        b.append("],\"class\":");
        str(b, aa.className());
        b.append(",\"id\":");
        str(b, aa.id());
        b.append(",\"mainSetImplementation\":");
        nestedClassMapping(b, aa.mainSetImplementation());
        if (!aa.aggPropertyMappings().isEmpty()) {
            // Pure main mappings COPY their pms onto the agg node as
            // AggregationAwarePropertyMapping (walker:92-95, probe
            // agg-pure-nested)
            b.append(",\"propertyMappings\":[");
            for (int i = 0; i < aa.aggPropertyMappings().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                Protocol.PPurePropertyMapping pm =
                        aa.aggPropertyMappings().get(i);
                b.append("{\"_type\":\"AggregationAwarePropertyMapping\","
                        + "\"property\":{\"class\":");
                str(b, java.util.Objects.requireNonNull(pm.ownerClass()));
                b.append(",\"property\":");
                str(b, pm.property());
                b.append(",\"sourceInformation\":");
                srcInfo(b, pm.propertySourceInformation());
                b.append('}');
                if (pm.source() != null) {
                    b.append(",\"source\":");
                    str(b, pm.source());
                }
                b.append(",\"sourceInformation\":");
                srcInfo(b, pm.sourceInformation());
                if (pm.target() != null) {
                    b.append(",\"target\":");
                    str(b, pm.target());
                }
                b.append('}');
            }
            b.append(']');
        }
        b.append(",\"root\":").append(aa.root());
        b.append(",\"sourceInformation\":");
        srcInfo(b, aa.sourceInformation());
        b.append('}');
    }


    /** {@code {_type:lambda, body:[expr], parameters:[]}} with NO span —
     *  the aggregate-lambda envelope (probe agg-off). */
    static void lambdaEnvelope(StringBuilder b,
            com.legend.protocol.spec.ValueSpecification expr) {
        b.append("{\"_type\":\"lambda\",\"body\":[");
        valueSpec(b, expr);
        b.append("],\"parameters\":[]}");
    }


    static void testSuite(StringBuilder b,
            Protocol.PMappingTestSuite ts) {
        b.append("{\"_type\":\"mappingTestSuite\"");
        if (ts.doc() != null) {
            b.append(",\"doc\":");
            str(b, ts.doc());
        }
        b.append(",\"func\":");
        valueSpec(b, ts.func());
        b.append(",\"id\":");
        str(b, ts.id());
        b.append(",\"sourceInformation\":");
        srcInfo(b, ts.sourceInformation());
        b.append(",\"tests\":[");
        for (int i = 0; i < ts.tests().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            Protocol.PMappingTest t = ts.tests().get(i);
            b.append("{\"_type\":\"mappingTest\",\"assertions\":[");
            for (int j = 0; j < t.assertions().size(); j++) {
                if (j > 0) {
                    b.append(',');
                }
                Protocol.PTestAssertion a = t.assertions().get(j);
                switch (a.expected()) {
                    case Protocol.PExternalFormatData ef -> {
                        b.append("{\"_type\":\"equalToJson\","
                                + "\"expected\":");
                        externalFormatData(b, ef);
                    }
                    case Protocol.PRelationElement re -> {
                        b.append("{\"_type\":\"equalToRelation\","
                                + "\"expected\":");
                        relationElement(b, re);
                    }
                    case Protocol.PEqualToValue ev -> {
                        b.append("{\"_type\":\"equalTo\","
                                + "\"expected\":");
                        valueSpec(b, ev.value());
                    }
                }
                b.append(",\"id\":");
                str(b, a.id());
                b.append(",\"sourceInformation\":");
                srcInfo(b, a.sourceInformation());
                b.append('}');
            }
            b.append(']');
            if (t.doc() != null) {
                b.append(",\"doc\":");
                str(b, t.doc());
            }
            b.append(",\"id\":");
            str(b, t.id());
            b.append(",\"sourceInformation\":");
            srcInfo(b, t.sourceInformation());
            b.append(",\"storeTestData\":[");
            for (int j = 0; j < t.storeTestData().size(); j++) {
                if (j > 0) {
                    b.append(',');
                }
                Protocol.PStoreTestData sd = t.storeTestData().get(j);
                if (sd.relationElements() != null) {
                    // Relation #{ schema.table: CSV...; }# (probe
                    // relation-data-exact)
                    b.append("{\"data\":{\"_type\":\"relationAccessor\","
                            + "\"relationElements\":[");
                    List<Protocol.PRelationElement> rels =
                            sd.relationElements();
                    for (int k = 0; k < rels.size(); k++) {
                        if (k > 0) {
                            b.append(',');
                        }
                        relationElement(b, rels.get(k));
                    }
                    b.append("],\"sourceInformation\":");
                    srcInfo(b, java.util.Objects.requireNonNull(
                            sd.relationAccessorSourceInformation()));
                    b.append("},\"sourceInformation\":");
                    srcInfo(b, sd.sourceInformation());
                    b.append(",\"store\":");
                    pointer(b, sd.store());
                    b.append('}');
                    continue;
                }
                if (sd.embedded() != null) {
                    // a plain embedded-data value under the store pointer
                    b.append("{\"data\":");
                    embeddedDataValue(b, sd.embedded());
                    b.append(",\"sourceInformation\":");
                    srcInfo(b, sd.sourceInformation());
                    b.append(",\"store\":");
                    pointer(b, sd.store());
                    b.append('}');
                    continue;
                }
                if (sd.dataElement() != null) {
                    // Reference #{ path }# (probe reference-data)
                    b.append("{\"data\":{\"_type\":\"reference\","
                            + "\"dataElement\":");
                    pointer(b, sd.dataElement());
                    b.append(",\"sourceInformation\":");
                    srcInfo(b, sd.dataElement().sourceInformation());
                    b.append("},\"sourceInformation\":");
                    srcInfo(b, sd.sourceInformation());
                    b.append(",\"store\":");
                    pointer(b, sd.store());
                    b.append('}');
                    continue;
                }
                List<Protocol.PModelData> mdl =
                        java.util.Objects.requireNonNull(sd.modelData());
                SourceInfo msSi = java.util.Objects.requireNonNull(
                        sd.modelStoreSourceInformation());
                b.append("{\"data\":{\"_type\":\"modelStore\","
                        + "\"modelData\":[");
                for (int k = 0; k < mdl.size(); k++) {
                    if (k > 0) {
                        b.append(',');
                    }
                    modelDataEntry(b, mdl.get(k));
                }
                b.append("],\"sourceInformation\":");
                srcInfo(b, msSi);
                b.append("},\"sourceInformation\":");
                srcInfo(b, sd.sourceInformation());
                b.append(",\"store\":");
                pointer(b, sd.store());
                b.append('}');
            }
            b.append("]}");
        }
        b.append("]}");
    }


    static void nestedClassMapping(StringBuilder b,
            Protocol.PClassMapping cm) {
        switch (cm) {
            case Protocol.PClassMappingRel r -> relClassMapping(b, r);
            case Protocol.PClassMappingPure pu -> pureClassMapping(b, pu);
            case Protocol.PClassMappingFunction fi -> functionClassMapping(b, fi);
            default -> throw new IllegalStateException(
                    "nested class-mapping kind unbuilt: " + cm.getClass());
        }
    }

    /** {@code prop ( street: COL, ... )} in a Relation mapping —
     *  embedded with NESTED classless pms (probe relation-embedded). */
    static void relationFnEmbedded(StringBuilder b,
            Protocol.PRelationFnPropertyMapping pm) {
        b.append("{\"_type\":\"relationFunctionEmbeddedPropertyMapping\","
                + "\"property\":{\"class\":");
        str(b, java.util.Objects.requireNonNull(pm.ownerClass()));
        b.append(",\"property\":");
        str(b, pm.property());
        b.append(",\"sourceInformation\":");
        srcInfo(b, pm.propertySourceInformation());
        b.append("},\"propertyMappings\":[");
        List<Protocol.PRelationFnPropertyMapping> nested =
                java.util.Objects.requireNonNull(pm.nested());
        for (int i = 0; i < nested.size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            relationFnPm(b, nested.get(i));
        }
        b.append(']');
        if (pm.source() != null) {
            b.append(",\"source\":");
            str(b, pm.source());
        }
        b.append(",\"sourceInformation\":");
        srcInfo(b, pm.sourceInformation());
        b.append('}');
    }

    /** ONE {@code relationFunctionPropertyMapping} — key order: _type,
     *  [column], [enumMappingId], [localMappingProperty],
     *  property{[class],property,si}, [source], sourceInformation,
     *  [valueFn] (probe ZRelationMappingProbe; nested pms carry NO class,
     *  signalled by a null ownerClass). Shared by the outer arm and the
     *  embedded helper so the pm wire cannot drift between them. */
    static void relationFnPm(StringBuilder b,
            Protocol.PRelationFnPropertyMapping pm) {
        b.append("{\"_type\":\"relationFunctionPropertyMapping\"");
        if (pm.bindingTransformer() != null) {
            // probed wire: {"binding": "<fqn>"} right after _type
            b.append(",\"bindingTransformer\":{\"binding\":");
            str(b, pm.bindingTransformer());
            b.append('}');
        }
        if (pm.column() != null) {
            b.append(",\"column\":");
            str(b, pm.column());
        }
        if (pm.enumMappingId() != null) {
            b.append(",\"enumMappingId\":");
            str(b, pm.enumMappingId());
        }
        if (pm.localMappingProperty() != null) {
            Protocol.PLocalProp lp = pm.localMappingProperty();
            b.append(",\"localMappingProperty\":"
                    + "{\"multiplicity\":{\"lowerBound\":");
            b.append(lp.lowerBound());
            if (lp.upperBound() != null) {
                b.append(",\"upperBound\":").append(lp.upperBound());
            }
            b.append("},\"sourceInformation\":");
            srcInfo(b, lp.sourceInformation());
            b.append(",\"type\":");
            str(b, lp.type());
            b.append('}');
        }
        b.append(",\"property\":{");
        if (pm.ownerClass() != null) {
            b.append("\"class\":");
            str(b, pm.ownerClass());
            b.append(',');
        }
        b.append("\"property\":");
        str(b, pm.property());
        b.append(",\"sourceInformation\":");
        srcInfo(b, pm.propertySourceInformation());
        b.append('}');
        if (pm.source() != null) {
            b.append(",\"source\":");
            str(b, pm.source());
        }
        b.append(",\"sourceInformation\":");
        srcInfo(b, pm.sourceInformation());
        if (pm.expr() != null) {
            b.append(",\"valueFn\":{\"_type\":\"lambda\",\"body\":[");
            valueSpec(b, pm.expr());
            b.append("],\"parameters\":[],\"sourceInformation\":");
            srcInfo(b, java.util.Objects.requireNonNull(
                    pm.exprLambdaSourceInformation()));
            b.append('}');
        }
        b.append('}');
    }

    static void relationElement(StringBuilder b,
            Protocol.PRelationElement re) {
        b.append("{\"columns\":[");
        for (int i = 0; i < re.columns().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            str(b, re.columns().get(i));
        }
        b.append("],\"paths\":[");
        for (int i = 0; i < re.paths().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            str(b, re.paths().get(i));
        }
        b.append("],\"rows\":[");
        for (int i = 0; i < re.rows().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"values\":[");
            List<String> row = re.rows().get(i);
            for (int j = 0; j < row.size(); j++) {
                if (j > 0) {
                    b.append(',');
                }
                str(b, row.get(j));
            }
            b.append("]}");
        }
        b.append("],\"sourceInformation\":");
        srcInfo(b, re.sourceInformation());
        b.append('}');
    }

    /** ONE {@code path: <payload>} ModelStore entry — an embedded value or a
     *  {@code [ ^X(...) ]} instance collection (probe model-instances). */
    private static void modelDataEntry(StringBuilder b, Protocol.PModelData md) {
        switch (md) {
            case Protocol.PModelEmbeddedData e -> {
                b.append("{\"_type\":\"modelEmbeddedData\",\"data\":");
                embeddedDataValue(b, e.data());
            }
            case Protocol.PModelInstanceData i -> {
                b.append("{\"_type\":\"modelInstanceData\",\"instances\":");
                ProtocolEmitter.instancesCollection(b, i.instances());
            }
        }
        b.append(",\"model\":");
        str(b, md.model());
        b.append(",\"sourceInformation\":");
        srcInfo(b, md.sourceInformation());
        b.append('}');
    }

    private static void stubParams(StringBuilder b, String field,
            java.util.List<Protocol.PStubParam> paramsIn) {
        // the engine's mapper sorts MAP entries by key (C12 byte pins
        // TestServiceTestSuite#65/#70)
        var params = new java.util.ArrayList<>(paramsIn);
        params.sort(java.util.Comparator.comparing(Protocol.PStubParam::name));
        b.append('"').append(field).append("\":{");
        for (int i = 0; i < params.size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            str(b, params.get(i).name());
            b.append(':');
            stringValuePattern(b, params.get(i).pattern());
        }
        b.append("},");
    }

    private static void stringValuePattern(StringBuilder b,
            Protocol.PStringValuePattern v) {
        b.append("{\"_type\":");
        str(b, v.type());
        b.append(",\"expectedValue\":");
        str(b, v.expectedValue());
        b.append('}');
    }

    static void embeddedDataValue(StringBuilder b,
            Protocol.PEmbeddedDataValue v) {
        switch (v) {
            case Protocol.PServiceStoreData ss -> {
                // ZTailProbe "servicestore-data"
                b.append("{\"_type\":\"serviceStore\","
                        + "\"serviceStubMappings\":[");
                for (int i = 0; i < ss.stubs().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    Protocol.PServiceStub st = ss.stubs().get(i);
                    b.append("{\"requestPattern\":{");
                    if (st.bodyPatterns() != null) {
                        b.append("\"bodyPatterns\":[");
                        for (int k = 0; k < st.bodyPatterns().size(); k++) {
                            if (k > 0) {
                                b.append(',');
                            }
                            stringValuePattern(b, st.bodyPatterns().get(k));
                        }
                        b.append("],");
                    }
                    if (st.headerParams() != null) {
                        stubParams(b, "headerParams", st.headerParams());
                    }
                    b.append("\"method\":");
                    str(b, st.method());
                    if (st.queryParams() != null) {
                        b.append(',');
                        stubParams(b, "queryParams", st.queryParams());
                        b.setLength(b.length() - 1);   // drop trailing comma
                    }
                    b.append(",\"sourceInformation\":");
                    srcInfo(b, st.requestSpan());
                    if (st.url() != null) {
                        b.append(",\"url\":");
                        str(b, st.url());
                    }
                    if (st.urlPath() != null) {
                        b.append(",\"urlPath\":");
                        str(b, st.urlPath());
                    }
                    b.append("},\"responseDefinition\":{\"body\":");
                    externalFormatData(b, st.body());
                    b.append(",\"sourceInformation\":");
                    srcInfo(b, st.responseSpan());
                    b.append("},\"sourceInformation\":");
                    srcInfo(b, st.sourceInformation());
                    b.append('}');
                }
                b.append("],\"sourceInformation\":");
                srcInfo(b, ss.sourceInformation());
                b.append('}');
            }
            case Protocol.PExternalFormatData ef -> externalFormatData(b, ef);
            case Protocol.PModelStoreData ms -> {
                b.append("{\"_type\":\"modelStore\",\"modelData\":[");
                for (int i = 0; i < ms.modelData().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    modelDataEntry(b, ms.modelData().get(i));
                }
                b.append("],\"sourceInformation\":");
                srcInfo(b, ms.sourceInformation());
                b.append('}');
            }
            case Protocol.PRelationData rd -> {
                b.append("{\"_type\":\"relationAccessor\","
                        + "\"relationElements\":[");
                for (int i = 0; i < rd.relationElements().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    relationElement(b, rd.relationElements().get(i));
                }
                b.append("],\"sourceInformation\":");
                srcInfo(b, rd.sourceInformation());
                b.append('}');
            }
            case Protocol.PRelationalCsvData rc -> {
                b.append("{\"_type\":\"relationalCSVData\","
                        + "\"sourceInformation\":");
                srcInfo(b, rc.sourceInformation());
                b.append(",\"tables\":[");
                for (int i = 0; i < rc.tables().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    Protocol.PRelationalCsvTable t = rc.tables().get(i);
                    b.append("{\"schema\":");
                    str(b, t.schema());
                    b.append(",\"sourceInformation\":");
                    srcInfo(b, t.sourceInformation());
                    b.append(",\"table\":");
                    str(b, t.table());
                    b.append(",\"values\":");
                    str(b, t.values());
                    b.append('}');
                }
                b.append("]}");
            }
            case Protocol.PDataReference dr -> {
                b.append("{\"_type\":\"reference\",\"dataElement\":");
                pointer(b, dr.dataElement());
                b.append(",\"sourceInformation\":");
                srcInfo(b, dr.sourceInformation());
                b.append('}');
            }
        }
    }

    static void externalFormatData(StringBuilder b,
            Protocol.PExternalFormatData ef) {
        b.append("{\"_type\":\"externalFormat\",\"contentType\":");
        str(b, ef.contentType());
        b.append(",\"data\":");
        str(b, ef.data());
        b.append(",\"sourceInformation\":");
        srcInfo(b, ef.sourceInformation());
        b.append('}');
    }


    static void relPropertyMapping(StringBuilder b,
            Protocol.PPropertyMapping pmi) {
        switch (pmi) {
            case Protocol.PRelPropertyMapping pm -> {
                b.append("{\"_type\":\"relationalPropertyMapping\",");
                if (pm.bindingTransformer() != null) {
                    // probed wire: {"binding": fqn} right after _type
                    b.append("\"bindingTransformer\":{\"binding\":");
                    str(b, pm.bindingTransformer());
                    b.append("},");
                }
                if (pm.enumMappingId() != null) {
                    b.append("\"enumMappingId\":");
                    str(b, pm.enumMappingId());
                    b.append(',');
                }
                if (pm.localMappingProperty() != null) {
                    Protocol.PLocalProp lp = pm.localMappingProperty();
                    b.append("\"localMappingProperty\":{\"multiplicity\":"
                            + "{\"lowerBound\":");
                    b.append(lp.lowerBound());
                    if (lp.upperBound() != null) {
                        b.append(",\"upperBound\":").append(lp.upperBound());
                    }
                    b.append("},\"sourceInformation\":");
                    srcInfo(b, lp.sourceInformation());
                    b.append(",\"type\":");
                    str(b, lp.type());
                    b.append("},");
                }
                b.append("\"property\":{");
                if (pm.ownerClass() != null) {
                    b.append("\"class\":");
                    str(b, pm.ownerClass());
                    b.append(',');
                }
                b.append("\"property\":");
                str(b, pm.property());
                b.append(",\"sourceInformation\":");
                srcInfo(b, pm.propertySourceInformation());
                b.append("},\"relationalOperation\":");
                relOp(b, pm.relationalOperation());
                if (pm.source() != null) {
                    b.append(",\"source\":");
                    str(b, pm.source());
                }
                b.append(",\"sourceInformation\":");
                srcInfo(b, pm.sourceInformation());
                if (pm.target() != null) {
                    b.append(",\"target\":");
                    str(b, pm.target());
                }
                b.append('}');
            }
            case Protocol.PEmbeddedPropertyMapping em -> {
                b.append("{\"_type\":\"embeddedPropertyMapping\","
                        + "\"classMapping\":{\"_type\":\"embedded\"");
                if (em.id() != null) {
                    b.append(",\"id\":");
                    str(b, em.id());
                }
                b.append(",\"primaryKey\":[");
                for (int i = 0; i < em.primaryKey().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    relOp(b, em.primaryKey().get(i));
                }
                b.append("],\"propertyMappings\":[");
                for (int i = 0; i < em.propertyMappings().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    relPropertyMapping(b, em.propertyMappings().get(i));
                }
                b.append("],\"root\":false,\"sourceInformation\":");
                srcInfo(b, em.sourceInformation());
                b.append('}');
                if (em.id() != null) {
                    b.append(",\"id\":");
                    str(b, em.id());
                }
                b.append(",\"property\":{");
                if (em.ownerClass() != null) {
                    b.append("\"class\":");
                    str(b, em.ownerClass());
                    b.append(',');
                }
                b.append("\"property\":");
                str(b, em.property());
                b.append(",\"sourceInformation\":");
                srcInfo(b, em.propertySourceInformation());
                b.append("},\"sourceInformation\":");
                srcInfo(b, em.sourceInformation());
                if (em.id() != null) {
                    b.append(",\"target\":");
                    str(b, em.id());
                }
                b.append('}');
            }
            case Protocol.PInlineEmbeddedPropertyMapping ie -> {
                b.append("{\"_type\":\"inlineEmbeddedPropertyMapping\"");
                if (ie.id() != null) {
                    b.append(",\"id\":");
                    str(b, ie.id());
                }
                b.append(",\"property\":{");
                if (ie.ownerClass() != null) {
                    b.append("\"class\":");
                    str(b, ie.ownerClass());
                    b.append(',');
                }
                b.append("\"property\":");
                str(b, ie.property());
                b.append(",\"sourceInformation\":");
                srcInfo(b, ie.propertySourceInformation());
                b.append("},\"setImplementationId\":");
                str(b, ie.setImplementationId());
                b.append(",\"sourceInformation\":");
                srcInfo(b, ie.sourceInformation());
                b.append('}');
            }
            case Protocol.POtherwiseEmbeddedPropertyMapping oe -> {
                b.append("{\"_type\":\"otherwiseEmbeddedPropertyMapping\","
                        + "\"classMapping\":{\"_type\":\"embedded\"");
                if (oe.id() != null) {
                    b.append(",\"id\":");
                    str(b, oe.id());
                }
                b.append(",\"primaryKey\":[");
                for (int i = 0; i < oe.primaryKey().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    relOp(b, oe.primaryKey().get(i));
                }
                b.append("],\"propertyMappings\":[");
                for (int i = 0; i < oe.propertyMappings().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    relPropertyMapping(b, oe.propertyMappings().get(i));
                }
                b.append("],\"root\":false,\"sourceInformation\":");
                srcInfo(b, oe.classMappingSourceInformation());
                b.append("},\"otherwisePropertyMapping\":{"
                        + "\"_type\":\"relationalPropertyMapping\","
                        + "\"property\":{");
                if (oe.ownerClass() != null) {
                    b.append("\"class\":");
                    str(b, oe.ownerClass());
                    b.append(',');
                }
                b.append("\"property\":");
                str(b, oe.property());
                b.append(",\"sourceInformation\":");
                srcInfo(b, oe.propertySourceInformation());
                b.append("},\"relationalOperation\":");
                relOp(b, oe.otherwiseOp());
                b.append(",\"sourceInformation\":");
                srcInfo(b, oe.otherwiseOp().sourceInformation());
                b.append(",\"target\":");
                str(b, oe.otherwiseTarget());
                b.append("},\"property\":{");
                if (oe.ownerClass() != null) {
                    b.append("\"class\":");
                    str(b, oe.ownerClass());
                    b.append(',');
                }
                b.append("\"property\":");
                str(b, oe.property());
                b.append(",\"sourceInformation\":");
                srcInfo(b, oe.propertySourceInformation());
                b.append("},\"sourceInformation\":");
                srcInfo(b, oe.sourceInformation());
                b.append('}');
            }
        }
    }


    static void filterMapping(StringBuilder b,
            Protocol.PFilterMapping f) {
        b.append("{\"filter\":{\"db\":");
        str(b, f.db());
        b.append(",\"name\":");
        str(b, f.name());
        b.append("},\"joins\":[");
        for (int i = 0; i < f.joins().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            joinPtr(b, f.joins().get(i));
        }
        b.append("],\"sourceInformation\":");
        srcInfo(b, f.sourceInformation());
        b.append('}');
    }
}
