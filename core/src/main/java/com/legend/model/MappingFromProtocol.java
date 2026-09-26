// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code PMapping} &rarr; {@link LegacyMappingDefinition}: the Mapping half of
 * the protocol-first migration (PARSER_COMPLETENESS_PLAN.md §1, phase M).
 *
 * <p>The target is the LEGACY SURFACE TREE, not the clean-sheet function form.
 * The model is mid-migration on two independent axes — protocol&rarr;model
 * (this class) and surface&rarr;function ({@code MappingNormalizer}) — and all
 * 1,503 corpus mapping elements are legacy-DSL, so this transform feeds the
 * normalizer exactly what {@code MappingGrammarParser} fed it. Conflating the
 * axes would turn a completion into a rewrite (§M0).
 *
 * <p><b>The interesting half is the property-mapping dispatch.</b> The wire
 * carries ONE {@code relationalOperation} per property line; the model splits
 * it into distinct variants by the SHAPE of that operation, because they
 * resolve down different paths (a {@code Join} yields an instance, a
 * {@code JoinTerminalColumn} a primitive, an {@code EnumeratedColumn} routes
 * through a value table). {@code MappingGrammarParser:1160-1277} is the spec
 * for that split and this mirrors it arm for arm.
 *
 * <p><b>Databases are threaded, never nulled.</b> Unlike a Database body —
 * where {@code RelOpFromProtocol} nulls a self-reference to match what the
 * source wrote — {@link PropertyMapping.Column#database()} and friends are
 * documented as ALWAYS populated, from the explicit {@code [DB]} bracket or
 * the enclosing class mapping's main table. So the operation transform runs
 * with a {@code null} enclosing database (nothing is elided) and the db is
 * read back off the node, with the main table as the fallback.
 *
 * <p>Everything not yet ported refuses LOUDLY through
 * {@link UnsupportedMappingShape}. A silent gap here would be invisible until
 * it generated wrong SQL, which is the failure mode this whole programme
 * exists to remove.
 */
public final class MappingFromProtocol {

    private MappingFromProtocol() {
    }

    /** A protocol shape this transform has not yet learned. Carries the
     *  wire's own vocabulary so the census and the caller agree on names. */
    public static class UnsupportedMappingShape extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private final String reason;

        public UnsupportedMappingShape(String message) {
            super(message);
            this.reason = message;
        }

        /** The refusal text. {@link #getMessage()} is {@code @Nullable} by
         *  its inherited contract; this one is not — the constructor is the
         *  only way in and it always carries a reason. */
        public String reason() {
            return reason;
        }
    }

    /**
     * The element split, mirroring {@code MappingGrammarParser:105-120}: a
     * CLEAN-SHEET body yields {@link MappingDefinition} (the function form),
     * a legacy DSL body yields {@link LegacyMappingDefinition} (the surface
     * tree {@code MappingNormalizer} rewrites). They are two independent
     * axes and a mapping is all one or all the other.
     */
    public static com.legend.model.PackageableElement toMappingElement(
            Protocol.PMapping m) {
        boolean hasCleanSheet0 = m.classMappings().stream()
                .anyMatch(cm -> cm instanceof Protocol.PClassMappingFunction);
        boolean hasCleanSheet = hasCleanSheet0;
        boolean hasCleanSheetAssoc = m.associationMappings().stream()
                .anyMatch(am -> am instanceof Protocol.PFunctionAssociationMapping);
        hasCleanSheet = hasCleanSheet || hasCleanSheetAssoc;
        boolean hasLegacy = m.classMappings().stream()
                .anyMatch(cm -> !(cm instanceof Protocol.PClassMappingFunction))
                || m.associationMappings().stream()
                        .anyMatch(am -> !(am instanceof Protocol.PFunctionAssociationMapping));
        if (hasLegacy && hasCleanSheet) {
            throw new UnsupportedMappingShape("Mapping '" + m.qualifiedName()
                    + "' mixes legacy DSL bodies with function-form bindings;"
                    + " a mapping must be all-legacy or all-clean-sheet"
                    + " (convert the whole mapping)");
        }
        return hasCleanSheet ? toCleanSheetDefinition(m) : toMappingDefinition(m);
    }

    private static CleanSheetMappingDefinition toCleanSheetDefinition(Protocol.PMapping m) {
        List<CleanSheetMappingDefinition.ClassBinding> bindings = new ArrayList<>();
        for (Protocol.PClassMapping cm : m.classMappings()) {
            Protocol.PClassMappingFunction fn = (Protocol.PClassMappingFunction) cm;
            // Ref vs Inline is decided by the WIRE's mutually-exclusive
            // pair, exactly as ElementParser.realizationOf decides it from
            // the parsed body — a lone packageable-element pointer is a
            // reference to an ordinary user function. Sources are NOT
            // stamped here: the stamp is born at the Phase-E lift, after
            // name resolution (CleanSheetMappingDefinition is the pre-E
            // surface — no placeholder needed).
            bindings.add(new CleanSheetMappingDefinition.ClassBinding(
                    fn.className(),
                    "PURE".equals(fn.kind())
                            ? CleanSheetMappingDefinition.Kind.PURE
                            : CleanSheetMappingDefinition.Kind.RELATIONAL,
                    fn.id(), fn.extendsClassMappingId(), fn.root(),
                    realizationOf(fn.function(), fn.bodyLambda()),
                    java.util.List.of()));
        }
        List<EnumerationMapping> enums = new ArrayList<>();
        for (Protocol.PEnumerationMapping em : m.enumerationMappings()) {
            enums.add(enumerationMapping(em));
        }
        List<CleanSheetMappingDefinition.AssociationBinding> assocBindings = new ArrayList<>();
        for (Protocol.PAssociationMapping am : m.associationMappings()) {
            Protocol.PFunctionAssociationMapping fa =
                    (Protocol.PFunctionAssociationMapping) am;
            assocBindings.add(new CleanSheetMappingDefinition.AssociationBinding(
                    fa.association().path(), realizationOf(fa.function(),
                            fa.bodyLambda())));
        }
        List<MappingInclude> includes = includesOf(m);
        return new CleanSheetMappingDefinition(m.qualifiedName(), includes, bindings,
                assocBindings, enums, m.testSuitesSource());
    }

    /** The wire's mutually-exclusive pair becomes the model's sealed
     *  Realization — the same Ref/Inline decision
     *  {@code ElementParser.realizationOf} makes from a parsed body. */
    private static com.legend.protocol.Realization realizationOf(
            com.legend.protocol.spec.@com.legend.base.Nullable PackageableElementPtr fn,
            com.legend.protocol.spec.@com.legend.base.Nullable LambdaFunction lambda) {
        return fn != null
                ? new com.legend.protocol.Realization.Ref(fn.fullPath(), fn)
                : new com.legend.protocol.Realization.Inline(
                        java.util.Objects.requireNonNull(lambda).body());
    }

    private static List<MappingInclude> includesOf(Protocol.PMapping m) {
        List<MappingInclude> includes = new ArrayList<>();
        for (Protocol.PMappingInclude inc : m.includedMappings()) {
            if (inc.includedDataSpace() != null) {
                // include dataspace rides the PROTOCOL record only: its
                // compile semantic (splice the dataspace's default-context
                // mapping) needs dataspace resolution the compiler does not
                // do yet, and a dataspace path in the model include list
                // would mis-resolve as a mapping name
                continue;
            }
            List<MappingInclude.StoreSubstitution> subs = new ArrayList<>();
            for (Protocol.PStoreSubstitution s : inc.substitutions()) {
                subs.add(new MappingInclude.StoreSubstitution(
                        s.sourceDatabasePath(), s.targetDatabasePath()));
            }
            includes.add(new MappingInclude(java.util.Objects.requireNonNull(
                    inc.includedMapping()), subs));
        }
        return includes;
    }

    public static LegacyMappingDefinition toMappingDefinition(Protocol.PMapping m) {
        List<MappingInclude> includes = new ArrayList<>();
        for (Protocol.PMappingInclude inc : m.includedMappings()) {
            if (inc.includedDataSpace() != null) {
                continue;   // protocol-carried only — see toCanonical above
            }
            // Read the FULL list, not the emitted pair: engine keeps only a
            // lone substitution and nulls both paths when there are several
            // (CorePureGrammarParser:504-516). legend-lite has always
            // honoured every pair, so taking the emitted fields here would
            // have silently changed which store those mappings resolve
            // against. Protocol.PStoreSubstitution carries the superset
            // without putting it on the wire.
            List<MappingInclude.StoreSubstitution> subs = new ArrayList<>();
            for (Protocol.PStoreSubstitution s : inc.substitutions()) {
                subs.add(new MappingInclude.StoreSubstitution(
                        s.sourceDatabasePath(), s.targetDatabasePath()));
            }
            includes.add(new MappingInclude(java.util.Objects.requireNonNull(
                    inc.includedMapping()), subs));
        }

        List<ClassMapping> classMappings = new ArrayList<>();
        for (Protocol.PClassMapping cm : m.classMappings()) {
            ClassMapping built = classMapping(cm);
            if (built != null) {
                classMappings.add(built);
            }
        }

        List<AssociationMapping> associations = new ArrayList<>();
        for (Protocol.PAssociationMapping am : m.associationMappings()) {
            associations.add(associationMapping(am));
        }

        List<EnumerationMapping> enums = new ArrayList<>();
        for (Protocol.PEnumerationMapping em : m.enumerationMappings()) {
            enums.add(enumerationMapping(em));
        }
        // The model carries testSuites as RAW TEXT — Phase C parses them
        // lazily — so the protocol record keeps the verbatim slice alongside
        // the parsed suites. `tests` (the LEGACY test block) has no model
        // field at all and never had one; the legacy parser drops it too.
        return new LegacyMappingDefinition(m.qualifiedName(), includes,
                classMappings, associations, enums, m.testSuitesSource());
    }

    // The WIRE discriminators, not the router FQNs: MappingProtocolParser
    // has already classified union_/special_union_/inheritance_ into these
    // (:780-793) and emits NO discriminator for merge_ or any unknown
    // function. Matching FQNs here instead would have silently dropped
    // every Operation class mapping — it did, for 173 mappings, until the
    // differential said so.
    private static final String OP_INHERITANCE = "INHERITANCE";
    private static final String OP_STORE_UNION = "STORE_UNION";
    private static final String OP_ROUTER_UNION = "ROUTER_UNION";

    /** {@code null} when the element is deliberately NOT modelled — see the
     *  Operation and AggregationAware arms. */
    private static @com.legend.base.Nullable ClassMapping classMapping(Protocol.PClassMapping cm) {
        if (cm instanceof Protocol.PServiceStoreClassMapping
                || cm instanceof Protocol.PClassMappingMongoDb) {
            // a FOREIGN store class mapping rides the protocol only — the
            // class is simply not mapped in a store lite executes, so the
            // model skips it (a query against it fails loudly at
            // resolution, engine-consistent)
            return null;
        }
        if (cm instanceof Protocol.PClassMappingRel rel) {
            return relational(rel);
        }
        if (cm instanceof Protocol.PClassMappingFunction) {
            throw new UnsupportedMappingShape("a function-form binding has no"
                    + " legacy surface-tree shape; it belongs to"
                    + " MappingDefinition (use toMappingElement)");
        }
        if (cm instanceof Protocol.PClassMappingPure pure) {
            return pureInstance(pure, pure.id());
        }
        if (cm instanceof Protocol.PClassMappingOperation op) {
            return operation(op);
        }
        if (cm instanceof Protocol.PClassMappingRelation rel) {
            return relationFunction(rel);
        }
        if (cm instanceof Protocol.PClassMappingAggregationAware agg) {
            // The AggregationAware element is ONE node: the ~mainMapping
            // (the class's root set, flagged by its non-null aggregation
            // component) carrying the aggregate Views with their
            // specifications (batch 25, 2026-09-03 — was: flattened, views
            // dropped, so no rewrite could ever happen).
            // PClassMappingAggregationAware.id is NON-null on the wire: when
            // the source writes no [id], engine DERIVES one from the class
            // FQN (`demo::agg::Sale` -> `demo_agg_Sale`). The model records
            // only a written id, so an id that is exactly that derivation is
            // an absence, not a name.
            String aggId = SetId.isDefault(agg.id(), agg.className()) ? null : agg.id();
            ClassMapping main = classMapping(agg.mainSetImplementation());
            if (main instanceof ClassMapping.Relational r) {
                // the flattened main keeps the AGGREGATION-AWARE element's own
                // set id and root flag: the legacy parser parses the
                // ~mainMapping body with the OUTER id threaded in
                // (MappingGrammarParser:487-495), while the wire mints a
                // derived `<id>_Main` for it. The aggregate views ride the
                // NODE (engine aggregateSetImplementations): each a non-root
                // Relational set of the class, id <outer>_Aggregate_<i>, with
                // its specification kept as syntax facts.
                List<ClassMapping.AggregateView> views = new ArrayList<>();
                for (Protocol.PAggregateSetImplementation asi : agg.aggregateSetImplementations()) {
                    ClassMapping vcm = classMapping(asi.setImplementation());
                    if (!(vcm instanceof ClassMapping.Relational v)) {
                        throw new UnsupportedMappingShape("AggregationAware view #"
                                + asi.index() + " of " + agg.className()
                                + " is not a Relational mapping");
                    }
                    List<ClassMapping.AggregateValue> avs = new ArrayList<>();
                    for (Protocol.PAggregateValue av : asi.aggregateValues()) {
                        avs.add(new ClassMapping.AggregateValue(av.mapFn(), av.aggregateFn()));
                    }
                    views.add(new ClassMapping.AggregateView(asi.index(), asi.canAggregate(),
                            asi.groupByFunctions(), avs,
                            new ClassMapping.Relational(v.className(), v.setId(),
                                    v.extendsSetId(), false, v.mainTable(), v.filter(),
                                    v.distinct(), v.groupBy(), v.primaryKey(),
                                    v.propertyMappings(), v.sourceUrl(),
                                    v.propertyTargetSets(), null)));
                }
                return new ClassMapping.Relational(r.className(), aggId,
                        r.extendsSetId(), agg.root(), r.mainTable(), r.filter(),
                        r.distinct(), r.groupBy(), r.primaryKey(),
                        r.propertyMappings(), r.sourceUrl(),
                        r.propertyTargetSets(), new ClassMapping.AggregationAware(views));
            }
            if (agg.mainSetImplementation() instanceof Protocol.PClassMappingPure pm) {
                // a Pure main serves as-is (no rewrite machinery to flag),
                // but the set id — and every property route measured against
                // it — is the OUTER element's
                return pureInstance(pm, aggId);
            }
            if (main == null) {
                throw new UnsupportedMappingShape(
                        "AggregationAware mapping has no ~mainMapping");
            }
            return main;
        }
        // A MERGE operation is parse-and-SKIP in the legacy parser: it is
        // consumed so the surrounding mapping still loads, and no class
        // mapping is recorded, so a query against the class stays loud at
        // resolution ("no mapping for class"). Reproduce that exactly —
        // inventing a variant here would turn a loud wall into a silent one.
        if (cm instanceof Protocol.PClassMappingMergeOperation) {
            return null;
        }
        throw new UnsupportedMappingShape("class mapping kind "
                + cm.getClass().getSimpleName() + " is not ported yet");
    }

    private static @com.legend.base.Nullable ClassMapping operation(
            Protocol.PClassMappingOperation op) {
        String fn = op.operation() == null ? "" : op.operation();
        if (OP_INHERITANCE.equals(fn)) {
            // members are IMPLICIT — the router resolves them from the
            // class hierarchy
            return new ClassMapping.Inheritance(op.className(), op.id(),
                    op.extendsClassMappingId(), op.root());
        }
        if (!OP_STORE_UNION.equals(fn) && !OP_ROUTER_UNION.equals(fn)) {
            return null;    // parse-and-skip, as above
        }
        if (op.parameters().isEmpty()) {
            throw new UnsupportedMappingShape("Operation union for '"
                    + op.className() + "' names no member sets");
        }
        // STORE_UNION = one SQL; ROUTER_UNION = per-member execution,
        // results concatenated. Row CONTENT is identical, so both build the
        // same member union here.
        return new ClassMapping.Union(op.className(), op.id(),
                op.extendsClassMappingId(), op.root(), op.parameters());
    }

    /** {@code ownerId} is the set id the MODEL will carry, which differs
     *  from {@code pure.id()} when this is an AggregationAware ~mainMapping
     *  (the wire mints a derived {@code <id>_Main}). The source-route
     *  elision below has to compare against the owner, not the wire's id. */
    private static ClassMapping pureInstance(Protocol.PClassMappingPure pure,
            @com.legend.base.Nullable String ownerId) {
        List<ClassMapping.Pure.PropertyBinding> bindings = new ArrayList<>();
        for (Protocol.PPurePropertyMapping pm : pure.propertyMappings()) {
            bindings.add(new ClassMapping.Pure.PropertyBinding(pm.property(),
                    single(pm.transform(), "pure property transform"),
                    // the wire RESOLVES sourceSetId to the class mapping's
                    // own id; the model records only an explicitly-written
                    // `prop[src, tgt]:` route, so an echo of the owning id
                    // is not a route
                    // the wire RESOLVES sourceSetId to the class mapping's
                    // own id, and spells "absent" as the empty string; the
                    // model records only an explicitly-written
                    // `prop[src, tgt]:` route
                    blankToNull(java.util.Objects.equals(pm.source(), ownerId)
                            ? null : pm.source()),
                    blankToNull(pm.target()), pm.explodeProperty(),
                    pm.localMappingProperty() != null, pm.enumMappingId()));
        }
        return new ClassMapping.Pure(pure.className(), ownerId,
                pure.extendsClassMappingId(), pure.root(),
                // ~src is OPTIONAL (see ClassMapping.Pure#sourceClass)
                pure.srcClass(),
                pure.filter() == null ? null
                        : single(pure.filter(), "pure class-mapping filter"),
                bindings);
    }

    private static ClassMapping relationFunction(Protocol.PClassMappingRelation rel) {
        List<ClassMapping.RelationFunction.Col> cols = new ArrayList<>();
        for (Protocol.PRelationFnPropertyMapping pm : rel.propertyMappings()) {
            cols.add(relationCol(pm));
        }
        // the FQN identifies the function; a signature spelling
        // `f():Relation<Any>[1]` carries redundant tokens the legacy parser
        // skips (MappingGrammarParser:546-554). Both wire forms (~func
        // pointer / ~src sourceLambda) fold into the SAME model channel —
        // a class binding realized by a function (MAPPING_CLEAN_SHEET §1).
        String funcRef = rel.relationFunction() != null
                ? rel.relationFunction()
                : java.util.Objects.requireNonNull(rel.sourceLambda())
                        .function();
        if (funcRef == null) {
            // ~src <expression>: the engine compiles the expression itself
            // into _relationFunction (engine #4941) — carry it inline
            var expr = java.util.Objects.requireNonNull(
                    java.util.Objects.requireNonNull(rel.sourceLambda()).expr(),
                    "a Relation mapping source needs a function or an"
                    + " expression");
            return new ClassMapping.RelationFunction(rel.className(),
                    rel.id(), null, rel.root(), null, expr, cols,
                    rel.primaryKey());
        }
        int sig = funcRef.indexOf('(');
        return new ClassMapping.RelationFunction(rel.className(), rel.id(), null,
                rel.root(), sig < 0 ? funcRef : funcRef.substring(0, sig), null,
                cols, rel.primaryKey());
    }

    private static ClassMapping.RelationFunction.Col relationCol(
            Protocol.PRelationFnPropertyMapping pm) {
        List<ClassMapping.RelationFunction.Col> nested = new ArrayList<>();
        if (pm.nested() != null) {
            for (Protocol.PRelationFnPropertyMapping sub : pm.nested()) {
                nested.add(relationCol(sub));
            }
        }
        return new ClassMapping.RelationFunction.Col(pm.property(), pm.column(),
                pm.localMappingProperty() != null, pm.enumMappingId(), nested,
                pm.inlineSetId(), pm.expr());
    }

    private static AssociationMapping associationMapping(Protocol.PAssociationMapping am) {
        if (am instanceof Protocol.PRelAssociationMapping rel) {
            List<AssociationPropertyMapping> pms = new ArrayList<>();
            for (Protocol.PRelAssocPropertyMapping pm : rel.propertyMappings()) {
                // an association end names BOTH sets or neither
                String src = pm.source();
                String tgt = pm.target();
                pms.add(new AssociationPropertyMapping(
                        src == null || tgt == null ? null : src,
                        src == null || tgt == null ? null : tgt,
                        bodyOf(pm.property(), pm.relationalOperation(), null,
                                null, null)));
            }
            return new AssociationMapping.Relational(rel.association().path(), pms);
        }
        if (am instanceof Protocol.PXStoreAssociationMapping xs) {
            List<AssociationMapping.Cross.XStoreProperty> pms = new ArrayList<>();
            for (Protocol.PXStorePropertyMapping pm : xs.propertyMappings()) {
                pms.add(new AssociationMapping.Cross.XStoreProperty(pm.property(),
                        blankToNull(pm.source()), blankToNull(pm.target()),
                        single(pm.crossExpression(), "xstore cross expression")));
            }
            return new AssociationMapping.Cross(xs.association().path(), pms);
        }
        if (am instanceof Protocol.PModelJoinAssociationMapping mj) {
            if (!(mj.joinCondition() instanceof com.legend.protocol.spec.LambdaFunction lf)) {
                throw new UnsupportedMappingShape(
                        "ModelJoin condition is not a lambda");
            }
            return new AssociationMapping.ModelJoin(mj.association().path(), lf);
        }
        throw new UnsupportedMappingShape("association mapping kind "
                + am.getClass().getSimpleName() + " is not ported yet");
    }

    private static EnumerationMapping enumerationMapping(Protocol.PEnumerationMapping em) {
        List<EnumerationMapping.EnumValueMapping> values = new ArrayList<>();
        for (Protocol.PEnumValueMapping v : em.enumValueMappings()) {
            List<EnumerationMapping.SourceValue> sources = new ArrayList<>();
            for (Protocol.PEnumSourceValue sv : v.sourceValues()) {
                sources.add(sourceValue(sv));
            }
            values.add(new EnumerationMapping.EnumValueMapping(v.enumValue(), sources));
        }
        return new EnumerationMapping(em.enumeration().path(), em.id(), values);
    }

    private static EnumerationMapping.SourceValue sourceValue(Protocol.PEnumSourceValue sv) {
        if (sv.enumeration() != null) {
            return new EnumerationMapping.SourceValue.EnumRef(sv.enumeration(),
                    String.valueOf(sv.value()));
        }
        if (sv.value() instanceof Number n) {
            return new EnumerationMapping.SourceValue.IntegerValue(n.longValue());
        }
        return new EnumerationMapping.SourceValue.StringValue(String.valueOf(sv.value()));
    }



    /** The wire wraps single expressions in a list; the model holds one. */
    private static com.legend.protocol.spec.ValueSpecification single(
            List<com.legend.protocol.spec.ValueSpecification> vs, String what) {
        if (vs == null || vs.size() != 1) {
            throw new UnsupportedMappingShape("a " + what
                    + " must have exactly one expression, got an empty body");
        }
        return vs.get(0);
    }

    private static @com.legend.base.Nullable ClassMapping relational(
            Protocol.PClassMappingRel rel) {
        try {
            return relationalInner(rel);
        } catch (MissingDatabase md) {
            // database inference (legend-pure) is unbuilt — skip, carried
            return null;
        }
    }

    private static ClassMapping relationalInner(Protocol.PClassMappingRel rel) {
        LegacyMappingDefinition.TableReference mainTable = rel.mainTable() == null
                ? null
                : new LegacyMappingDefinition.TableReference(
                        require(rel.mainTable().database(), "main table database"),
                        tableName(rel.mainTable()));
        String mainDb = mainTable == null ? null : mainTable.database();

        FilterMapping filter = rel.filter() == null ? null : filterMapping(rel.filter());

        List<RelationalOperation> groupBy = new ArrayList<>();
        for (Protocol.PRelOp op : rel.groupBy()) {
            groupBy.add(RelOpFromProtocol.op(op, null));
        }
        List<RelationalOperation> primaryKey = new ArrayList<>();
        for (Protocol.PRelOp op : rel.primaryKey()) {
            primaryKey.add(RelOpFromProtocol.op(op, null));
        }

        List<PropertyMapping> pms = new ArrayList<>();
        Map<String, String> targetSets = new LinkedHashMap<>();
        for (Protocol.PPropertyMapping pm : rel.propertyMappings()) {
            pms.add(propertyMapping(pm, mainDb, targetSets));
        }

        return new ClassMapping.Relational(rel.className(), rel.id(),
                rel.extendsClassMappingId(), rel.root(), mainTable,
                filter, rel.distinct(), groupBy, primaryKey, pms,
                null, targetSets, null);
    }

    /**
     * {@code ~filter [DB] F} and {@code ~filter [DB] @J | [DB] F}. The wire
     * always carries the filter's own database, so an unmediated reference
     * is Cross-or-Local by whether that database differs from the joins'
     * — mirroring {@code MappingGrammarParser}'s split.
     */
    private static FilterMapping filterMapping(Protocol.PFilterMapping f) {
        if (f.joins().isEmpty()) {
            return new FilterMapping.Direct(new FilterPointer.Cross(f.db(), f.name()));
        }
        // `~filter [DB] (INNER) @J | [DB] F` — engine's joinSequence puts the
        // LEADING join type on the first pointer; the model hangs it on the
        // FilterMapping itself (RelationalGrammarParser:341-357), and only a
        // `> (INNER) @Next` type rides its own hop. Leaving it on hop 0 made
        // the filter and the chain BOTH wrong at once.
        List<JoinChainElement> chain = new ArrayList<>();
        List<Protocol.PJoinPtr> hops = f.joins();
        String filterJoinType = hops.get(0).joinType();
        for (int i = 0; i < hops.size(); i++) {
            Protocol.PJoinPtr p = hops.get(i);
            String hopType = i == 0 ? null : p.joinType();
            chain.add(new JoinChainElement(p.name(),
                    hopType == null ? null : JoinType.fromIdentifier(hopType),
                    p.db(), false));
        }
        return new FilterMapping.JoinMediated(
                require(hops.get(0).db(), "join-mediated filter source database"),
                chain, new FilterPointer.Cross(f.db(), f.name()), filterJoinType);
    }

    /** The wire splits schema from table; the model carries the name as
     *  written, which for a NAMED schema is {@code "schema.table"}. A
     *  double-quoted relational identifier reaches the wire WITH its quotes
     *  and the model stores it bare. */
    private static String tableName(Protocol.PTablePtr t) {
        String table = unquote(t.table());
        return t.schema() == null || "default".equals(t.schema())
                ? table : unquote(t.schema()) + "." + table;
    }

    private static String unquote(String s) {
        return s.length() >= 2 && s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"'
                ? s.substring(1, s.length() - 1) : s;
    }

    /** The database a property line resolves in, taken from the PROTOCOL
     *  node. It cannot be read back off the model node: the model elides a
     *  self-reference (that is the as-written rule {@code RelOpFromProtocol}
     *  applies), and here the elision is exactly what we need to undo. */
    private static @com.legend.base.Nullable String protocolDb(Protocol.PRelOp op) {
        if (op instanceof Protocol.PColumnRef c) {
            return c.table().database();
        }
        if (op instanceof Protocol.PElemtWithJoins j && !j.joins().isEmpty()) {
            return j.joins().get(0).db();
        }
        return null;
    }

    private static PropertyMapping propertyMapping(Protocol.PPropertyMapping pm,
            @com.legend.base.Nullable String mainDb, Map<String, String> targetSets) {
        if (pm instanceof Protocol.PInlineEmbeddedPropertyMapping inl) {
            return new PropertyMapping.InlineEmbedded(inl.property(),
                    inl.setImplementationId());
        }
        if (pm instanceof Protocol.PEmbeddedPropertyMapping emb) {
            List<PropertyMapping> subs = new ArrayList<>();
            for (Protocol.PPropertyMapping sub : emb.propertyMappings()) {
                subs.add(propertyMapping(sub, mainDb, targetSets));
            }
            List<RelationalOperation> pk = new ArrayList<>();
            for (Protocol.PRelOp op : emb.primaryKey()) {
                pk.add(RelOpFromProtocol.op(op, null));
            }
            return new PropertyMapping.Embedded(emb.property(), subs, pk);
        }
        if (pm instanceof Protocol.POtherwiseEmbeddedPropertyMapping oth) {
            List<PropertyMapping> subs = new ArrayList<>();
            for (Protocol.PPropertyMapping sub : oth.propertyMappings()) {
                subs.add(propertyMapping(sub, mainDb, targetSets));
            }
            // the Otherwise arm is a property-mapping BODY (typically a
            // join) whose own name is the outer property's
            // targetSetId stays NULL on the fallback body: `otherwiseTarget`
            // is the OtherwiseEmbedded's own fallbackSetId, not a per-property
            // set route, and the legacy parser leaves the inner Join unrouted.
            PropertyMapping fallback = bodyOf(oth.property(), oth.otherwiseOp(),
                    mainDb, null, null);
            return new PropertyMapping.OtherwiseEmbedded(oth.property(), subs,
                    oth.otherwiseTarget(), fallback);
        }
        if (!(pm instanceof Protocol.PRelPropertyMapping rel)) {
            throw new UnsupportedMappingShape("property mapping kind "
                    + pm.getClass().getSimpleName() + " is not ported yet");
        }
        if (rel.localMappingProperty() != null) {
            Protocol.PLocalProp lp = rel.localMappingProperty();
            return new PropertyMapping.LocalProperty(rel.property(),
                    new com.legend.protocol.TypeExpression.NameRef(lp.type(), null),
                    new com.legend.protocol.Multiplicity.Concrete(
                            (int) lp.lowerBound(),
                            lp.upperBound() == null ? null
                                    : Integer.valueOf(lp.upperBound().intValue())),
                    bodyOf(rel.property(), rel.relationalOperation(), mainDb,
                            rel.enumMappingId(), rel.target()));
        }
        if (rel.target() != null) {
            targetSets.put(rel.property(), rel.target());
        }

        return bodyOf(rel.property(), rel.relationalOperation(), mainDb,
                rel.enumMappingId(), rel.target());
    }

    /**
     * The dispatch itself: ONE wire operation becomes one of five model
     * variants by its SHAPE, because they resolve down different paths — a
     * {@code Join} yields an instance, a {@code JoinTerminalColumn} a
     * primitive, an {@code Enumerated*} routes through a value table.
     * Mirrors {@code MappingGrammarParser:1160-1277} arm for arm.
     *
     * <p>Shared by plain lines, {@code +local} bodies and the
     * {@code Otherwise} arm, all of which the legacy parser routes through
     * its own {@code parsePropertyMappingBody}.
     */
    private static PropertyMapping bodyOf(String property, Protocol.PRelOp rawOp,
            @com.legend.base.Nullable String mainDb,
            @com.legend.base.Nullable String enumMappingId,
            @com.legend.base.Nullable String targetSetId) {
        // The PM's own database comes off the PROTOCOL node (always
        // resolved); the OPERATION is transformed under that database so
        // inner self-references elide exactly as the legacy parser left
        // them — `concat(personTable.FIRSTNAME, ...)` keeps a null
        // databaseName on the inner ColumnRef even though the wire
        // resolved it.
        String db = firstNonNull(protocolDb(rawOp), mainDb);
        RelationalOperation op = RelOpFromProtocol.op(rawOp, db);

        if (op instanceof RelationalOperation.JoinNavigation jn) {
            String navDb = require(firstNonNull(db, jn.databaseName(), chainDb(jn)),
                    "a join navigation requires a database — either a"
                            + " ~mainTable directive or an explicit [DB]"
                            + " qualifier");
            if (jn.terminal() == null) {
                if (enumMappingId != null) {
                    throw new UnsupportedMappingShape("EnumerationMapping on a"
                            + " join property mapping requires a terminal column");
                }
                return new PropertyMapping.Join(property, navDb, jn.chain(),
                        targetSetId);
            }
            return new PropertyMapping.JoinTerminalColumn(property, navDb,
                    jn.chain(), jn.terminal(), enumMappingId, enumMappingId != null);
        }
        if (op instanceof RelationalOperation.ColumnRef cr) {
            String colDb = require(db, "a column reference requires a database"
                    + " — either a ~mainTable directive or an explicit [DB]"
                    + " qualifier");
            return enumMappingId == null
                    ? new PropertyMapping.Column(property, colDb, cr.table(), cr.column())
                    : new PropertyMapping.EnumeratedColumn(property, enumMappingId,
                            colDb, cr.table(), cr.column());
        }
        return enumMappingId == null
                ? new PropertyMapping.Expression(property, op)
                : new PropertyMapping.EnumeratedExpression(property, enumMappingId, op);
    }

    /** A nav's own db may be absent while its FIRST hop carries one. */
    private static @com.legend.base.Nullable String chainDb(
            RelationalOperation.JoinNavigation jn) {
        return jn.chain().isEmpty() ? null : jn.chain().get(0).databaseName();
    }

    @SafeVarargs
    private static @com.legend.base.Nullable String firstNonNull(
            @com.legend.base.Nullable String... candidates) {
        for (String c : candidates) {
            if (c != null) {
                return c;
            }
        }
        return null;
    }

    private static @com.legend.base.Nullable String blankToNull(
            @com.legend.base.Nullable String s) {
        return s == null || s.isEmpty() ? null : s;
    }

    /** Refusals name the missing SOURCE DIRECTIVE, not the missing wire
     *  field: "the wire carries no column database" tells a user nothing,
     *  "requires a ~mainTable directive or an explicit [DB] qualifier" tells
     *  them what to type. The legacy parser's messages set that bar. */
    private static String require(@com.legend.base.Nullable String v, String what) {
        if (v == null) {
            throw what.contains("requires a database")
                    ? new MissingDatabase(what)
                    : new UnsupportedMappingShape(what);
        }
        return v;
    }

    /** The PRE-{@code ~mainTable} legend-pure shape: dotted column refs
     *  with NO database anywhere — legend-pure infers the store at compile;
     *  lite's database-inference leg is unbuilt, so the class mapping is
     *  SKIPPED (carried on protocol) rather than refusing the file. */
    static final class MissingDatabase extends UnsupportedMappingShape {
        private static final long serialVersionUID = 1L;

        MissingDatabase(String message) {
            super(message);
        }
    }
}
