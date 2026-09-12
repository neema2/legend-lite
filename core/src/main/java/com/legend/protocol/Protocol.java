package com.legend.protocol;

import java.util.List;

/**
 * The Legend protocol shape — <em>our</em> types, <em>their</em> bytes.
 *
 * <p>These records mirror the shape legend-engine serialises as
 * {@code PureModelContextData}, so {@link ProtocolEmitter} can reproduce its JSON
 * byte-for-byte. They are a <strong>clean-room reimplementation</strong>: legend-lite takes no
 * dependency on {@code legend-engine-protocol-pure}, and nothing here imports
 * {@code org.finos.legend.engine}.
 *
 * <p><b>The protocol is a serialization contract, not a design constraint.</b> Upstream's protocol
 * is mutable public-field POJOs dispatched by Jackson type-ids. Ours is:
 * <ul>
 *   <li><b>sealed</b> hierarchies with explicit {@code permits} — so {@link ProtocolEmitter}'s
 *       switch is exhaustive and adding a variant without an emit rule is a <em>compile error</em>,
 *       the same discipline {@code AGENTS.md} invariant 3 imposes on MIR &rarr; dialect;</li>
 *   <li><b>100% immutable</b> records;</li>
 *   <li><b>loud</b> — no defaulting, no silent absence (invariant 4).</li>
 * </ul>
 *
 * <p>Positions are captured at construction because that is the only point where token offsets are
 * in hand. {@link com.legend.protocol.SourceInfo} follows the engine's convention exactly: 1-based lines, 1-based start
 * column, and an <em>inclusive</em> end column.
 */
public final class Protocol {

    private Protocol() {
    }

    /** Root: {@code {"_type":"data","elements":[...]}}. Null {@code serializer}/{@code origin} are omitted. */
    public record PureModelContextData(List<Element> elements) {
        public PureModelContextData {
            elements = List.copyOf(elements);
        }
    }

    /** A packageable element. Sealed so the emitter's switch is exhaustive. */
    public sealed interface Element permits PClass, PAssociation, PEnumeration, PFunction,
            PProfile, PSectionIndex, PMeasure, PRuntime, PConnection, PDatabase,
            PService, PExecutionEnvironment, PDataSpace,
            PPersistence, PPersistenceContext, PFunctionActivator,
            PDiagram,
            PText, PGenerationSpecification, PFileGeneration,
            PDeephavenDatabase, PElasticsearch7Cluster, PMongoDatabase,
            PDataQualityValidation, PDataQualityRelationValidation,
            PDataQualityRelationComparison, PSchemaSet, PBinding,
            PServiceStoreDefinition,
            PMapping, PDataElement, PRelationalMapper {
    }

    /** {@code ###Data Data [decorations] qn { <body> }} — {@code _type:
     *  "dataElement"} (probe data-section). */
    public record PDataElement(String pkg, String name,
                               PDataBody body,
                               List<PStereotype> stereotypes,
                               List<PTaggedValue> taggedValues,
                               com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** A ###Data element's body — engine wire (probes data-section,
     *  store-keyed; harvest fixtures testDataElementWith*Resolvers): an
     *  optional {@code data} value AND optional {@code dataResolvers},
     *  either or both present. */
    public record PDataBody(@com.legend.Nullable PEmbeddedDataValue value,
                            List<PDataResolver> resolvers) {
        public PDataBody {
            resolvers = List.copyOf(resolvers);
        }
    }

    /** One resolver entry. {@code store::S: <value>;} is
     *  {@code _type:"baseDataResolver"} (span store path through island
     *  close, excluding the ';'); a bare {@code fqn;} is
     *  {@code _type:"referenceDataResolver"} — {@code data} is null and
     *  only the {@code elementPointer} rides the wire. */
    public record PDataResolver(PElementRef elementPointer,
                                @com.legend.Nullable PEmbeddedDataValue data,
                                com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** A typeless element pointer — {@code path} + span only, unlike
     *  {@link PPointer}, which carries the {@code type} discriminator. */
    public record PElementRef(String path,
                              com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code _type:"mapping"} — a ###Mapping element (ZMappingProbe):
     *  envelope {associationMappings, classMappings, enumerationMappings,
     *  includedMappings, name, package, sourceInformation, tests}. Class
     *  mapping families land probe-by-probe; unbuilt ones WALL. */
    /** Association mappings: relational sides are join navs; xStore sides
     *  are Pure cross-expression lambdas (probes include-and-assoc,
     *  xstore). */
    public sealed interface PAssociationMapping
            permits PRelAssociationMapping, PXStoreAssociationMapping,
            PModelJoinAssociationMapping, PFunctionAssociationMapping {
    }

    /** {@code assoc: AssociationMapping { acme::funcs::personFirmMatch }} —
     *  the CLEAN-SHEET association binding. Same mutually-exclusive
     *  pointer/lambda fork as {@link PClassMappingFunction}; the body is
     *  typed {@code (Source[1], Target[1]) -> Boolean[1]}. */
    public record PFunctionAssociationMapping(PPointer association,
                                              com.legend.protocol.spec.@com.legend.Nullable PackageableElementPtr function,
                                              com.legend.protocol.spec.@com.legend.Nullable LambdaFunction bodyLambda,
                                              com.legend.protocol.SourceInfo sourceInformation)
            implements PAssociationMapping {
        public PFunctionAssociationMapping {
            if ((function == null) == (bodyLambda == null)) {
                throw new IllegalArgumentException(
                        "a function-form association binding is EITHER a"
                                + " function reference or an inline body");
            }
        }
    }

    /** {@code assoc: ModelJoin { {x:T[1], y:U[1]|expr} }} — the join
     *  condition is a TYPED ###Pure lambda via SpecParser; member span
     *  target..outer brace (probe modeljoin). */
    public record PModelJoinAssociationMapping(PPointer association,
                                               @com.legend.Nullable String id,
                                               com.legend.protocol.spec.ValueSpecification joinCondition,
                                               com.legend.protocol.SourceInfo sourceInformation)
            implements PAssociationMapping {
    }

    public record PRelAssociationMapping(PPointer association,
                                         @com.legend.Nullable String id,
                                         List<PRelAssocPropertyMapping> propertyMappings,
                                         List<String> stores,
                                         com.legend.protocol.SourceInfo sourceInformation)
            implements PAssociationMapping {
    }

    public record PRelAssocPropertyMapping(String property,
                                           com.legend.protocol.SourceInfo propertySourceInformation,
                                           PRelOp relationalOperation,
                                           @com.legend.Nullable String source,
                                           @com.legend.Nullable String target,
                                           com.legend.protocol.SourceInfo sourceInformation) {
    }

    public record PXStoreAssociationMapping(PPointer association,
                                            @com.legend.Nullable String id,
                                            List<PXStorePropertyMapping> propertyMappings,
                                            com.legend.protocol.SourceInfo sourceInformation)
            implements PAssociationMapping {
        /** The id-less shape earlier callers built. */
        public PXStoreAssociationMapping(PPointer association,
                List<PXStorePropertyMapping> propertyMappings,
                com.legend.protocol.SourceInfo sourceInformation) {
            this(association, null, propertyMappings, sourceInformation);
        }
    }

    public record PXStorePropertyMapping(String ownerClass, String property,
                                         com.legend.protocol.SourceInfo propertySourceInformation,
                                         List<com.legend.protocol.spec.ValueSpecification> crossExpression,
                                         String source, String target,
                                         com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** Class mappings emit in SOURCE ORDER — one sealed list. */
    public sealed interface PClassMapping
            permits PClassMappingRel, PClassMappingPure,
            PClassMappingOperation, PClassMappingRelation,
            PClassMappingMergeOperation, PClassMappingAggregationAware,
            PClassMappingFunction, PServiceStoreClassMapping,
            PClassMappingMongoDb {
    }

    /** {@code *Class[id]: ServiceStore { ~service [store] G.S (~request
     *  (...))* }} — {@code _type:"serviceStore"} (ZTailProbe
     *  "servicestore-mapping" / "-rich" / "-body"). The model transform
     *  SKIPS it (the class is not mapped in a store lite executes). */
    public record PServiceStoreClassMapping(String className,
                                            com.legend.protocol.SourceInfo classSpan,
                                            @com.legend.Nullable String id,
                                            boolean root,
                                            List<PServiceStoreLocalProp> localProps,
                                            List<PServiceMapping> services,
                                            com.legend.protocol.SourceInfo sourceInformation)
            implements PClassMapping {
    }

    /** One {@code +name: Type[mult];} local mapping property; span runs
     *  the {@code +} through the multiplicity close. */
    public record PServiceStoreLocalProp(String name, String type,
                                         int lowerBound,
                                         @com.legend.Nullable Integer upperBound,
                                         com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One {@code ~service [store] Seg.….Svc (~path ... ~request(...))?}
     *  entry. */
    public record PServiceMapping(PServicePtr service,
                                  @com.legend.Nullable PPathOffset pathOffset,
                                  @com.legend.Nullable PRequestBuildInfo request,
                                  com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code ~path $service.response.a.b} — startType is the fixed
     *  {@code $service.response} head, the rest are propertyPath entries;
     *  NO spans on this wire (ZTailProbe "servicestore-mapping-rich2"). */
    public record PPathOffset(String startType, List<String> propertyPath) {
        public PPathOffset {
            propertyPath = List.copyOf(propertyPath);
        }
    }

    /** A dotted service pointer: leading segments are GROUPS, the last is
     *  the service; each segment keeps its own span. */
    public record PServicePtr(String serviceStore, List<PServiceSegment> segments) {
        public PServicePtr {
            segments = List.copyOf(segments);
        }
    }

    /** One dotted-path segment with its span. */
    public record PServiceSegment(String name,
                                  com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code ~request ( parameters(...)? body = ...? )}. */
    public record PRequestBuildInfo(
            @com.legend.Nullable PParametersBuildInfo parameters,
            @com.legend.Nullable PBodyBuildInfo body,
            @com.legend.Nullable com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code parameters ( name = expr, ... )}. */
    public record PParametersBuildInfo(List<PParameterBuildInfo> entries,
                                       @com.legend.Nullable com.legend.protocol.SourceInfo sourceInformation) {
        public PParametersBuildInfo {
            entries = List.copyOf(entries);
        }
    }

    /** One {@code name = expr}; the transform lambda's span is the EXPR's
     *  own token range ({@code transformSpan}). */
    public record PParameterBuildInfo(String serviceParameter,
                                      com.legend.protocol.spec.ValueSpecification transform,
                                      com.legend.protocol.SourceInfo transformSpan,
                                      com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code body = expr}. */
    public record PBodyBuildInfo(com.legend.protocol.spec.ValueSpecification transform,
                                 com.legend.protocol.SourceInfo transformSpan,
                                 com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code *Class[id]: MongoDB { ~mainCollection [db] Coll }} —
     *  {@code _type:"MongoDB"}, NO spans on the wire (ZTailProbe
     *  "mongodb-mapping"). Model transform skips it. */
    public record PClassMappingMongoDb(String className,
                                       @com.legend.Nullable String id,
                                       boolean root, String storePath,
                                       String mainCollectionName,
                                       @com.legend.Nullable String bindingPath)
            implements PClassMapping {
    }

    /**
     * {@code _type:"functionInstance"} — the CLEAN-SHEET function form,
     * {@code *acme::Person: Relational { acme::funcs::personMapping }}.
     *
     * <p>Modelled on engine's {@code RelationFunctionClassMapping}, which
     * solved the same problem for {@code ~func}: a
     * {@code PackageableElementPtr} for the reference spelling and a
     * {@code LambdaFunction} for the inline one, MUTUALLY EXCLUSIVE. The
     * difference is where row&rarr;instance promotion lives. Engine's
     * {@code relation} points at a function returning a {@code Relation}
     * and binds properties to its COLUMNS through {@code propertyMappings};
     * here the function is typed {@code (): Class[*]} and does the
     * promotion itself with {@code ^Class(...)}, so there are no property
     * mappings and no primary key — the construction is opaque to the
     * mapping element.
     *
     * <p><b>{@code kind} is load-bearing and cannot be derived.</b> A
     * {@code Relational} body and a {@code Pure} body are BOTH
     * {@code (): Class[*]} (MAPPING_CLEAN_SHEET.md §1), so the return type
     * cannot disambiguate them — the kind is a property of the mapping
     * relationship, not of the function. One {@code _type} with a
     * discriminating field rather than two types, following
     * {@code OperationClassMapping.operation} rather than the
     * relational/pureInstance split, because the SHAPES are identical.
     *
     * <p>This is a legend-lite surface: engine has no function-form mapping,
     * so no corpus file exercises it and the byte-parity gate has no opinion
     * on it. It rides the wire anyway, the way any non-core class mapping
     * does through {@code PureProtocolExtension}.
     */
    public record PClassMappingFunction(String className,
                                        com.legend.protocol.SourceInfo classSourceInformation,
                                        @com.legend.Nullable String id,
                                        @com.legend.Nullable String extendsClassMappingId,
                                        boolean root,
                                        String kind,
                                        com.legend.protocol.spec.@com.legend.Nullable PackageableElementPtr function,
                                        com.legend.protocol.spec.@com.legend.Nullable LambdaFunction bodyLambda,
                                        com.legend.protocol.SourceInfo sourceInformation)
            implements PClassMapping {
        public PClassMappingFunction {
            if ((function == null) == (bodyLambda == null)) {
                throw new IllegalArgumentException(
                        "a function-instance class mapping is EITHER a function"
                                + " reference or an inline body, never both or"
                                + " neither");
            }
        }
    }

    /** {@code cls[id]: AggregationAware { Views: [...], ~mainMapping:
     *  Relational {...} }} — nested CM spans ride the ENGINE's sub-parse
     *  shift (member-anchored spans +DELTA lines); aggregate lambdas ride
     *  a CONSTANT -1 line shift (probes agg-off-A/B). */
    public record PClassMappingAggregationAware(String className,
                                                String id,
                                                List<PAggregateSetImplementation> aggregateSetImplementations,
                                                PClassMapping mainSetImplementation,
                                                List<PPurePropertyMapping> aggPropertyMappings,
                                                boolean root,
                                                com.legend.protocol.SourceInfo sourceInformation)
            implements PClassMapping {
    }

    public record PAggregateSetImplementation(boolean canAggregate,
                                              List<com.legend.protocol.spec.ValueSpecification> groupByFunctions,
                                              List<PAggregateValue> aggregateValues,
                                              int index,
                                              PClassMapping setImplementation) {
    }

    public record PAggregateValue(com.legend.protocol.spec.ValueSpecification mapFn,
                                  com.legend.protocol.spec.ValueSpecification aggregateFn) {
    }

    /** {@code merge_...([p1,p2], {typed lambda})} — {@code _type:
     *  "mergeOperation"} WITH operation:"MERGE" and a validationFunction
     *  lambda WRAPPING the typed inner lambda; the bare-paren merge form
     *  stays a plain operation with NO discriminator (probes
     *  merge-params-lambda vs merge-op). */
    public record PClassMappingMergeOperation(String className,
                                              com.legend.protocol.SourceInfo classSourceInformation,
                                              @com.legend.Nullable String id,
                                              boolean root,
                                              List<String> parameters,
                                              com.legend.protocol.spec.ValueSpecification validationLambda,
                                              com.legend.protocol.SourceInfo sourceInformation)
            implements PClassMapping {
    }

    /** {@code _type:"relation"} class mapping (probe relation-fn):
     *  NO classSourceInformation; the {@code ~func} descriptor is a FUNCTION
     *  pointer whose path is the CANONICAL descriptor text. The {@code ~src}
     *  row-source form (4.138 wire, probe ZRelationMappingProbe) rides
     *  {@code sourceLambda} INSTEAD — exactly one of
     *  {@code relationFunction}/{@code sourceLambda} is set. */
    public record PClassMappingRelation(String className,
                                        @com.legend.Nullable String id,
                                        @com.legend.Nullable String extendsClassMappingId,
                                        List<String> primaryKey,
                                        List<PRelationFnPropertyMapping> propertyMappings,
                                        @com.legend.Nullable String relationFunction,
                                        @com.legend.Nullable com.legend.protocol.SourceInfo relationFunctionSourceInformation,
                                        @com.legend.Nullable PRelationSrcLambda sourceLambda,
                                        boolean root,
                                        com.legend.protocol.SourceInfo sourceInformation)
            implements PClassMapping {
    }

    /** The {@code ~src fn()} wire: a {@code sourceLambda} whose body is one
     *  {@code func} node spanning the BARE function name, while the WRAPPER
     *  lambda's span is the full descriptor's columns with both lines
     *  shifted DOWN by (descriptor line - cm brace line) — the engine
     *  walker's re-parse anchor quirk (probe ZRelationMappingProbe:
     *  srcMapping 41→42 / 92→93). */
    public record PRelationSrcLambda(@com.legend.Nullable String function,
                                     @com.legend.Nullable com.legend.protocol.SourceInfo functionSourceInformation,
                                     @com.legend.Nullable com.legend.protocol.spec.ValueSpecification expr,
                                     com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code prop: col} inside a Relation mapping — span prop..col;
     *  {@code prop () Inline [set]} rides inlineSetId (probe
     *  relation-inline). */
    /**
     * A column binding in a {@code : Relation} class mapping.
     *
     * <p>{@code enumMappingId} and {@code expr} were carried-not-emitted
     * while the 4.133 oracle had no production for them; the 4.138 grammar
     * DOES (probe ZRelationMappingProbe), so both are real wire now:
     * {@code enumMappingId} emits after {@code column}, and {@code expr}
     * emits as {@code valueFn} — a parameterless lambda whose body keeps
     * TRUE spans while the wrapper's span is the expression's columns with
     * both lines shifted DOWN by (expr line - cm brace line), the same
     * anchor quirk as {@link PRelationSrcLambda}. The shifted wrapper span
     * rides {@code exprLambdaSourceInformation}.
     */
    public record PRelationFnPropertyMapping(@com.legend.Nullable String ownerClass,
                                             @com.legend.Nullable String bindingTransformer,
                                             String property,
                                             com.legend.protocol.SourceInfo propertySourceInformation,
                                             @com.legend.Nullable String column,
                                             @com.legend.Nullable String inlineSetId,
                                             @com.legend.Nullable List<PRelationFnPropertyMapping> nested,
                                             @com.legend.Nullable PLocalProp localMappingProperty,
                                             @com.legend.Nullable String source,
                                             @com.legend.Nullable String enumMappingId,
                                             @com.legend.Nullable com.legend.protocol.spec.ValueSpecification expr,
                                             @com.legend.Nullable com.legend.protocol.SourceInfo exprLambdaSourceInformation,
                                             com.legend.protocol.SourceInfo sourceInformation) {
    }

    public record PMapping(String pkg, String name,
                           List<PAssociationMapping> associationMappings,
                           List<PClassMapping> classMappings,
                           List<PEnumerationMapping> enumerationMappings,
                           List<PMappingInclude> includedMappings,
                           List<PMappingTestSuite> testSuites,
                           List<PLegacyMappingTest> tests,
                           @com.legend.Nullable String testSuitesSource,
                           com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** {@code testSuites: [ id: { function: |...; tests: [...] } ]} —
     *  {@code _type:"mappingTestSuite"}; the query lambda's spec spans
     *  carry the MAPPING's path as sourceId (probe test-suites). */
    public record PMappingTestSuite(String id,
                                    @com.legend.Nullable String doc,
                                    com.legend.protocol.spec.ValueSpecification func,
                                    List<PMappingTest> tests,
                                    com.legend.protocol.SourceInfo sourceInformation) {
    }

    public record PMappingTest(String id,
                               @com.legend.Nullable String doc,
                               List<PStoreTestData> storeTestData,
                               List<PTestAssertion> assertions,
                               com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code Store: ModelStore #{...}#} or {@code Store: Reference
     *  #{ my::DataElement }#} (probe reference-data). */
    public record PStoreTestData(PPointer store,
                                 @com.legend.Nullable List<PModelData> modelData,
                                 @com.legend.Nullable com.legend.protocol.SourceInfo modelStoreSourceInformation,
                                 @com.legend.Nullable PPointer dataElement,
                                 @com.legend.Nullable List<PRelationElement> relationElements,
                                 @com.legend.Nullable com.legend.protocol.SourceInfo relationAccessorSourceInformation,
                                 @com.legend.Nullable PEmbeddedDataValue embedded,
                                 com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** Model-entry payload: ExternalFormat or a Reference (probe
     *  embedded-reference). */
    public sealed interface PEmbeddedDataValue
            permits PExternalFormatData, PDataReference, PModelStoreData,
            PRelationData, PRelationalCsvData, PServiceStoreData {
    }

    /** {@code ServiceStore #{ [ {request; response;} ] }#} —
     *  {@code _type:"serviceStore"} with serviceStubMappings (ZTailProbe
     *  "servicestore-data"). */
    public record PServiceStoreData(List<PServiceStub> stubs,
                                    com.legend.protocol.SourceInfo sourceInformation)
            implements PEmbeddedDataValue {
        public PServiceStoreData {
            stubs = List.copyOf(stubs);
        }
    }

    /** One stub: requestPattern (method + optional url/urlPath/params/
     *  bodyPatterns) and a responseDefinition whose body is an
     *  externalFormat blob (the engine parses ANY embedded data there and
     *  refuses non-ExternalFormat AFTER the parse). */
    public record PServiceStub(String method, @com.legend.Nullable String url,
                               @com.legend.Nullable String urlPath,
                               @com.legend.Nullable List<PStubParam> queryParams,
                               @com.legend.Nullable List<PStubParam> headerParams,
                               @com.legend.Nullable List<PStringValuePattern> bodyPatterns,
                               com.legend.protocol.SourceInfo requestSpan,
                               PExternalFormatData body,
                               com.legend.protocol.SourceInfo responseSpan,
                               com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** A named request-parameter pattern ({@code name: EqualTo #{...}#}). */
    public record PStubParam(String name, PStringValuePattern pattern) {
    }

    /** {@code EqualTo #{ expected: '...'; }#} — wire
     *  {@code {_type:"equalTo"|"equalToJson", expectedValue}} (the engine's
     *  two registered contentPattern extensions). */
    public record PStringValuePattern(String type, String expectedValue) {
    }

    /** {@code Relational #{ schema.table: 'csv' + 'csv'; }#} —
     *  {@code _type:"relationalCSVData"}. The path is exactly TWO segments
     *  (the engine rejects one and three) and the value is the escape-decoded
     *  concatenation of its string literals (probe relational-csv). */
    public record PRelationalCsvData(List<PRelationalCsvTable> tables,
                                     com.legend.protocol.SourceInfo sourceInformation)
            implements PEmbeddedDataValue {
    }

    /** One {@code schema.table: 'csv';} row — span runs the schema token
     *  through the terminating {@code ';'} (probe relational-multi). */
    public record PRelationalCsvTable(String schema, String table,
                                      String values,
                                      com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code ModelStore #{ path: <value>, ... }#} as a first-class data
     *  body (probe data-section). */
    public record PModelStoreData(List<PModelData> modelData,
                                  com.legend.protocol.SourceInfo sourceInformation)
            implements PEmbeddedDataValue {
    }

    /** {@code Relation #{ schema.table: CSV; }#} as a data body. */
    public record PRelationData(List<PRelationElement> relationElements,
                                com.legend.protocol.SourceInfo sourceInformation)
            implements PEmbeddedDataValue {
    }

    public record PDataReference(PPointer dataElement,
                                 com.legend.protocol.SourceInfo sourceInformation)
            implements PEmbeddedDataValue {
    }

    /** One {@code path: <payload>} entry of a ModelStore island — the engine
     *  has TWO node types for it, so this is sealed rather than a record with
     *  a null arm (probe model-instances). */
    public sealed interface PModelData permits PModelEmbeddedData, PModelInstanceData {
        String model();

        com.legend.protocol.SourceInfo sourceInformation();
    }

    public record PModelEmbeddedData(String model, PEmbeddedDataValue data,
                                     com.legend.protocol.SourceInfo sourceInformation)
            implements PModelData {
    }

    /** {@code my::P: [ ^my::P(...) ]} — {@code _type:"modelInstanceData"},
     *  whose {@code instances} is the ###Pure value-expression collection
     *  (probe model-instances). */
    public record PModelInstanceData(String model,
                                     com.legend.protocol.spec.ValueSpecification instances,
                                     com.legend.protocol.SourceInfo sourceInformation)
            implements PModelData {
    }

    /** {@code ExternalFormat #{ contentType: '...'; data: '...'; }#} —
     *  span ExternalFormat..}# (probe test-suites). */
    public record PExternalFormatData(String contentType, String data,
                                      com.legend.protocol.SourceInfo sourceInformation)
            implements PEmbeddedDataValue, PAssertionValue {
    }

    /** {@code id: EqualToJson #{...}#} or {@code id: Relation #{...}#}
     *  (probe relation-data-exact). */
    public sealed interface PAssertionValue
            permits PExternalFormatData, PRelationElement, PEqualToValue {
    }

    /** {@code EqualTo #{ expected: <value>; }#} — wire
     *  {@code _type:"equalTo"} with a SPEC value (harvest
     *  testServiceTestSuite ak2_9). */
    public record PEqualToValue(com.legend.protocol.spec.ValueSpecification value)
            implements PAssertionValue {
    }

    public record PTestAssertion(String id, PAssertionValue expected,
                                 com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One {@code schema.table: CSV...;} group inside a Relation island —
     *  cells TRIMMED both sides; assertion islands have NO path line. */
    public record PRelationElement(List<String> columns, List<String> paths,
                                   List<List<String>> rows,
                                   com.legend.protocol.SourceInfo sourceInformation)
            implements PAssertionValue {
    }

    /** {@code MappingTests [ name ( query: |...; data: [<Object, JSON,
     *  cls, 'json'>]; assert: '...'; ) ]} — the LEGACY tests array on the
     *  mapping envelope (probe legacy-mapping-tests). */
    public record PLegacyMappingTest(String name,
                                     com.legend.protocol.spec.ValueSpecification query,
                                     List<PLegacyInputData> inputData,
                                     String expectedOutput,
                                     com.legend.protocol.SourceInfo assertSourceInformation,
                                     com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code <Object, JSON, cls, 'data'>} or {@code <Relational, CSV,
     *  db, 'data'>} (probe legacy-rel-input). */
    public record PLegacyInputData(boolean relational, String targetPath,
                                   String inputType, String data,
                                   com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One enumeration mapping: id + typed enumeration pointer + value rows
     *  (probe enum-mapping). */
    /** {@code _type:"relational"} class mapping (probe
     *  relational-class-mapping): id omitted when unset; root from the
     *  {@code *} marker; propertyMapping spans run COLON..operation end. */
    public record PClassMappingRel(String className,
                                   com.legend.protocol.SourceInfo classSourceInformation,
                                   @com.legend.Nullable String id,
                                   boolean root,
                                   boolean distinct,
                                   @com.legend.Nullable String extendsClassMappingId,
                                   @com.legend.Nullable PFilterMapping filter,
                                   List<PRelOp> groupBy,
                                   @com.legend.Nullable PTablePtr mainTable,
                                   List<PRelOp> primaryKey,
                                   List<PPropertyMapping> propertyMappings,
                                   com.legend.protocol.SourceInfo sourceInformation)
            implements PClassMapping {
    }

    /** {@code _type:"operation"} class mapping: the called FQN maps to the
     *  operation discriminator by EXACT FQN (union_*=STORE_UNION etc.);
     *  parameters are bare set ids (probe operation). */
    public record PClassMappingOperation(String className,
                                         com.legend.protocol.SourceInfo classSourceInformation,
                                         @com.legend.Nullable String id,
                                         @com.legend.Nullable String extendsClassMappingId,
                                         boolean root,
                                         @com.legend.Nullable String operation,
                                         List<String> parameters,
                                         com.legend.protocol.SourceInfo sourceInformation)
            implements PClassMapping {
    }

    /** {@code _type:"pureInstance"} class mapping (probe pure-m2m):
     *  transforms are PURE lambda trees — the ###Pure ValueSpecification
     *  wire, parsed by SpecParser and emitted by the SAME spec arms. */
    public record PClassMappingPure(String className,
                                    com.legend.protocol.SourceInfo classSourceInformation,
                                    @com.legend.Nullable String extendsClassMappingId,
                                    @com.legend.Nullable String id,
                                    boolean root,
                                    @com.legend.Nullable String srcClass,
                                    @com.legend.Nullable com.legend.protocol.SourceInfo sourceClassSourceInformation,
                                    @com.legend.Nullable List<com.legend.protocol.spec.ValueSpecification> filter,
                                    List<PPurePropertyMapping> propertyMappings,
                                    com.legend.protocol.SourceInfo sourceInformation)
            implements PClassMapping {
    }

    public record PPurePropertyMapping(@com.legend.Nullable String ownerClass,
                                       String property,
                                       com.legend.protocol.SourceInfo propertySourceInformation,
                                       @com.legend.Nullable String enumMappingId,
                                       boolean explodeProperty,
                                       @com.legend.Nullable PLocalProp localMappingProperty,
                                       List<com.legend.protocol.spec.ValueSpecification> transform,
                                       @com.legend.Nullable String source,
                                       @com.legend.Nullable String target,
                                       com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** A relational class-mapping property line — plain column/nav lines
     *  and embedded blocks ({@code emb[k] ( ... )}) share the list. */
    public sealed interface PPropertyMapping
            permits PRelPropertyMapping, PEmbeddedPropertyMapping,
            PInlineEmbeddedPropertyMapping,
            POtherwiseEmbeddedPropertyMapping {
    }

    public record PRelPropertyMapping(@com.legend.Nullable String ownerClass,
                                      @com.legend.Nullable String bindingTransformer,
                                      String property,
                                      com.legend.protocol.SourceInfo propertySourceInformation,
                                      @com.legend.Nullable String enumMappingId,
                                      @com.legend.Nullable PLocalProp localMappingProperty,
                                      PRelOp relationalOperation,
                                      @com.legend.Nullable String source,
                                      @com.legend.Nullable String target,
                                      com.legend.protocol.SourceInfo sourceInformation)
            implements PPropertyMapping {
    }

    /** {@code +prop: Type[m]: <op>} — a mapping-local property; the span
     *  runs the FIRST colon through the multiplicity bracket (probe
     *  local-prop). */
    public record PLocalProp(String type, long lowerBound,
                             @com.legend.Nullable Long upperBound,
                             com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code prop[k] ( lines... )} — {@code _type:
     *  "embeddedPropertyMapping"}; the OUTER span and the nested
     *  {@code _type:"embedded"} class mapping's span are BOTH the paren
     *  region; the single bracket id rides id AND target AND the nested
     *  mapping's id (probe embedded-plain/embedded-id-and-milestoning). */
    public record PEmbeddedPropertyMapping(@com.legend.Nullable String ownerClass,
                                           String property,
                                           com.legend.protocol.SourceInfo propertySourceInformation,
                                           @com.legend.Nullable String id,
                                           List<PRelOp> primaryKey,
                                           List<PPropertyMapping> propertyMappings,
                                           com.legend.protocol.SourceInfo sourceInformation)
            implements PPropertyMapping {
    }

    /** {@code prop() Inline[setId]} — span paren-open..bracket-close
     *  (probe inline-embedded). */
    public record PInlineEmbeddedPropertyMapping(@com.legend.Nullable String ownerClass,
                                                 String property,
                                                 com.legend.protocol.SourceInfo propertySourceInformation,
                                                 @com.legend.Nullable String id,
                                                 String setImplementationId,
                                                 com.legend.protocol.SourceInfo sourceInformation)
            implements PPropertyMapping {
    }

    /** {@code prop ( ... ) Otherwise ( [tgt]:<op> )} — the embedded
     *  classMapping span runs paren-open..OTHERWISE-close; the otherwise
     *  op's span STRETCHES back to the {@code [tgt]} bracket (probe
     *  otherwise-embedded). */
    public record POtherwiseEmbeddedPropertyMapping(@com.legend.Nullable String ownerClass,
                                                    String property,
                                                    com.legend.protocol.SourceInfo propertySourceInformation,
                                                    @com.legend.Nullable String id,
                                                    List<PRelOp> primaryKey,
                                                    List<PPropertyMapping> propertyMappings,
                                                    PRelOp otherwiseOp,
                                                    String otherwiseTarget,
                                                    com.legend.protocol.SourceInfo classMappingSourceInformation,
                                                    com.legend.protocol.SourceInfo sourceInformation)
            implements PPropertyMapping {
    }

    /** {@code ~filter [db] NAME} / {@code ~filter [db]@J | [db2]NAME} —
     *  span '~' through the name (probe rel-filter/rel-filter-joined). */
    public record PFilterMapping(String db, String name,
                                 List<PJoinPtr> joins,
                                 com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code include [mapping] my::Other} — {@code _type:
     *  "mappingIncludeMapping"}, span keyword..path (probe include-plain).
     *  {@code include dataspace my::DS} — {@code _type:
     *  "mappingIncludeDataSpace"} with {@code includedDataSpace} instead
     *  (ZTailProbe "include-dataspace"); exactly one of the two paths is
     *  set. */
    public record PMappingInclude(@com.legend.Nullable String includedMapping,
                                  @com.legend.Nullable String includedDataSpace,
                                  @com.legend.Nullable String sourceDatabasePath,
                                  @com.legend.Nullable String targetDatabasePath,
                                  List<PStoreSubstitution> substitutions,
                                  com.legend.protocol.SourceInfo sourceInformation) {
        public PMappingInclude {
            substitutions = substitutions == null ? List.of()
                    : List.copyOf(substitutions);
        }

        /** The mapping-include shape every pre-dataspace caller builds. */
        public PMappingInclude(String includedMapping,
                @com.legend.Nullable String sourceDatabasePath,
                @com.legend.Nullable String targetDatabasePath,
                List<PStoreSubstitution> substitutions,
                com.legend.protocol.SourceInfo sourceInformation) {
            this(includedMapping, null, sourceDatabasePath, targetDatabasePath,
                    substitutions, sourceInformation);
        }
    }

    /**
     * ALL {@code src->tgt} pairs of an {@code include m[a->b, c->d]}.
     *
     * <p>Deliberately NOT emitted, and that is the point. Engine's own walker
     * keeps a substitution only when there is exactly ONE pair and sets both
     * paths to null otherwise (`CorePureGrammarParser:504-516`), so
     * {@code sourceDatabasePath}/{@code targetDatabasePath} above reproduce
     * the wire byte for byte. legend-lite's model has always honoured every
     * pair, which is a real superset — dropping it on the way through
     * protocol would have silently changed which store a mapping resolves
     * against. This field carries the superset THROUGH the protocol without
     * putting it ON the wire.
     */
    public record PStoreSubstitution(String sourceDatabasePath,
                                     String targetDatabasePath) {
    }

    public record PEnumerationMapping(@com.legend.Nullable String id,
                                      PPointer enumeration,
                                      List<PEnumValueMapping> enumValueMappings,
                                      com.legend.protocol.SourceInfo sourceInformation) {
    }

    public record PEnumValueMapping(String enumValue,
                                    List<PEnumSourceValue> sourceValues) {
    }

    /** {@code _type:"stringSourceValue"|"integerSourceValue"}. */
    /** A source value: string/integer literal, or an ENUM VALUE reference
     *  ({@code [my::Other.bla]} — probe enum-source-enumref). */
    public record PEnumSourceValue(@com.legend.Nullable String enumeration,
                                   Object value) {
    }

    /** {@code _type:"relational"} — a ###Relational Database element
     *  (ZRelationalProbe shapes). Implicit default schema takes the WHOLE
     *  database's span. */
    public record PDatabase(String pkg, String name,
                            List<PPointer> includedStores,
                            List<PIncludedStoreSpec> includedStoreSpecifications,
                            List<PDbSchema> schemas,
                            List<PDbJoin> joins,
                            List<PDbFilter> filters,
                            List<PStereotype> stereotypes,
                            List<PTaggedValue> taggedValues,
                            com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    public record PDbSchema(String name, List<PDbTable> tables,
                            List<PDbView> views,
                            List<PDbTable> tabularFunctions,
                            List<PStereotype> stereotypes,
                            List<PTaggedValue> taggedValues,
                            com.legend.protocol.SourceInfo sourceInformation) {
    }

    public record PDbTable(String name, List<PDbColumn> columns,
                           List<PMilestoning> milestoning,
                           List<String> primaryKey,
                           List<PStereotype> stereotypes,
                           List<PTaggedValue> taggedValues,
                           com.legend.protocol.SourceInfo sourceInformation) {
    }

    public record PDbColumn(String name, boolean nullable, PDbType type,
                            List<PStereotype> stereotypes,
                            List<PTaggedValue> taggedValues,
                            com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** Column datatype: {@code _type} spelling + optional size/precision
     *  fields (probe TYPES). */
    public record PDbType(String kind, @com.legend.Nullable Long size,
                          @com.legend.Nullable Long precision,
                          @com.legend.Nullable Long scale) {
    }

    /** {@code businessMilestoning} / {@code processingMilestoning}; the
     *  optional infinityDate nests a dateTime literal node. */
    public sealed interface PMilestoning
            permits PBusinessMilestoning, PProcessingMilestoning,
            PBusinessSnapshotMilestoning, PProcessingSnapshotMilestoning {
    }

    /** {@code processing(PROCESSING_SNAPSHOT_DATE = col)}. */
    public record PProcessingSnapshotMilestoning(String snapshotDate,
            com.legend.protocol.SourceInfo sourceInformation)
            implements PMilestoning {
    }

    /** {@code business(BUS_SNAPSHOT_DATE = col)}. */
    public record PBusinessSnapshotMilestoning(String snapshotDate,
            com.legend.protocol.SourceInfo sourceInformation)
            implements PMilestoning {
    }

    public record PBusinessMilestoning(String from, String thru,
                                       boolean thruIsInclusive,
                                       @com.legend.Nullable PDateTimeLit infinityDate,
                                       com.legend.protocol.SourceInfo sourceInformation)
            implements PMilestoning {
    }

    public record PProcessingMilestoning(String in, String out,
                                         boolean outIsInclusive,
                                         @com.legend.Nullable PDateTimeLit infinityDate,
                                         com.legend.protocol.SourceInfo sourceInformation)
            implements PMilestoning {
    }

    /** {@code _type} discriminates on the spelling: a time part makes it
     *  dateTime, date-only is strictDate. */
    public record PDateTimeLit(String value,
                               com.legend.protocol.SourceInfo sourceInformation) {
        public String wireType() {
            return value.contains("T") ? "dateTime" : "strictDate";
        }
    }

    public record PDbJoin(String name, PRelOp operation,
                          com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code _type:"filter"} (or multigrain) named filter. */
    public record PDbFilter(String filterType, String name, PRelOp operation,
                            com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** A relational OPERATION node (join/filter/view expressions). */
    public sealed interface PRelOp
            permits PDynaFunc, PColumnRef, PRelLiteral, PRelLiteralList,
            PElemtWithJoins, PRelLambda, PLambdaParam {
        com.legend.protocol.SourceInfo sourceInformation();
    }

    /** A lambda as a function-operation argument (4.145.0,
     *  {@code _type:"relationalLambda"}): a filter/map/fold over an array
     *  take — {@code x | body} or {@code (a, b | body)}. */
    public record PRelLambda(List<String> parameterNames, PRelOp body,
                             com.legend.protocol.SourceInfo sourceInformation)
            implements PRelOp {
    }

    /** {@code $x} inside a relational lambda's body (4.145.0,
     *  {@code _type:"lambdaParameter"}). */
    public record PLambdaParam(String name,
                               com.legend.protocol.SourceInfo sourceInformation)
            implements PRelOp {
    }

    /** {@code [1,2,3]} — items emit the NESTED literal form
     *  ({@code {"_type":"literal","value":{span,value}}}), unlike the flat
     *  scalar literal (probe proc-snapshot-array-json). */
    public record PRelLiteralList(List<PRelLiteral> values,
                                  com.legend.protocol.SourceInfo sourceInformation)
            implements PRelOp {
    }

    /** {@code @Join | expr} navigation — the wire _type is the engine's own
     *  TYPO {@code "elemtWithJoins"}, preserved deliberately. A join-only
     *  nav ({@code [db]@J}, association sides) has NO element. */
    public record PElemtWithJoins(List<PJoinPtr> joins,
                                  @com.legend.Nullable PRelOp relationalElement,
                                  com.legend.protocol.SourceInfo sourceInformation)
            implements PRelOp {
    }

    public record PJoinPtr(@com.legend.Nullable String db,
                           @com.legend.Nullable String joinType,
                           String name,
                           com.legend.protocol.SourceInfo sourceInformation) {
    }

    public record PDynaFunc(String funcName, List<PRelOp> parameters,
                            com.legend.protocol.SourceInfo sourceInformation)
            implements PRelOp {
    }

    /** {@code _type:"column"} — a table-qualified column read; the self-join
     *  target spells {@code {target}} for BOTH table and alias. */
    public record PColumnRef(String column, PTablePtr table, String tableAlias,
                             com.legend.protocol.SourceInfo sourceInformation)
            implements PRelOp {
    }

    public record PTablePtr(@com.legend.Nullable String database,
                            @com.legend.Nullable String mainTableDb, String schema,
                            String table,
                            com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code _type:"literal"} — value is a string or a number. */
    public record PRelLiteral(Object value,
                              com.legend.protocol.SourceInfo sourceInformation)
            implements PRelOp {
    }

    public record PDbView(String name,
                          List<PViewColumnMapping> columnMappings,
                          List<PStereotype> stereotypes,
                          List<PTaggedValue> taggedValues,
                          boolean distinct,
                          @com.legend.Nullable PViewFilter filter,
                          @com.legend.Nullable List<PRelOp> groupBy,
                          List<String> primaryKey,
                          com.legend.protocol.SourceInfo sourceInformation) {
    }

    public record PViewColumnMapping(String name, PRelOp operation,
                                     com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code ~filter F} on a view: {filter:{name}, joins:[], srcInfo}. */
    /**
     * A view's {@code ~filter} clause. The engine's FilterMapping is a filter
     * POINTER (db + name) plus a join chain — grammar rule
     * {@code viewFilterMapping: FILTER_CMD (viewFilterMappingJoin |
     * databasePointer)? identifier}, where the join form REQUIRES a database
     * pointer on both sides of the pipe.
     *
     * <p>This record carried only {@code name} until 2026-08-08, and the
     * emitter hardcoded {@code "joins":[]}. That was right for every element
     * the corpus compares and wrong for the grammar — a latent parity bug, and
     * a blocker for the protocol-first migration, since the model the compiler
     * runs on DOES carry the join chain and would have quietly lost it.
     */
    public record PViewFilter(@com.legend.Nullable String db, String name,
                              List<PJoinPtr> joins,
                              com.legend.protocol.SourceInfo sourceInformation) {
        public PViewFilter {
            joins = joins == null ? List.of() : List.copyOf(joins);
        }
    }

    /** {@code _type:"connection"} — a ###Connection element: envelope
     *  {name, package, connectionValue, sourceInformation}; element and
     *  value share ONE span (ZConnectionProbe). */
    public record PConnection(String pkg, String name, PConnectionValue value,
                              com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /**
     * A {@code ###Service} Service element, corpus-censused scope: envelope
     * decorations, pattern/owners/documentation/autoActivateUpdates, a
     * Single or Multi execution, and RAW-captured legacy-test / testSuites
     * payloads. NO engine wire shape is claimed yet — the emitter walls —
     * so the parity harness keeps Service files OUT_OF_SCOPE; the record
     * exists for the parse/transform seam.
     */
    public record PService(String pkg, String name,
                           List<PStereotype> stereotypes,
                           List<PTaggedValue> taggedValues,
                           @com.legend.Nullable String pattern,
                           @com.legend.Nullable String title,
                           List<String> owners,
                           @com.legend.Nullable String ownershipKind,
                           @com.legend.Nullable String ownershipId,
                           @com.legend.Nullable List<String> ownershipUsers,
                           @com.legend.Nullable String mcpServer,
                           @com.legend.Nullable String documentation,
                           @com.legend.Nullable Boolean autoActivateUpdates,
                           PServiceExecution execution,
                           @com.legend.Nullable PLegacyServiceTest test,
                           @com.legend.Nullable List<PServiceTestSuite> testSuites,
                           @com.legend.Nullable List<PPostValidation> postValidations,
                           com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** {@code execution: Single | Multi}. */
    public sealed interface PServiceExecution
            permits PSingleExecution, PMultiExecution {
    }

    /** An embedded anonymous runtime island body — the {@code PRuntime}
     *  part records; wire {@code _type:"engineRuntime"} with a
     *  CONTENT-anchored span (ZTailProbe "service-single"). */
    public record PEmbeddedRuntime(List<PPointer> mappings,
                                   List<PStoreConnections> connections,
                                   List<PConnectionStores> connectionStores,
                                   com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code runtime:} is a POINTER (with its path span) or an embedded
     *  anonymous runtime island; exactly one of {@code runtime} /
     *  {@code embeddedRuntime} is set when the source spells one. The
     *  execution span covers {@code Kind..'}'}. */
    public record PSingleExecution(
            com.legend.protocol.spec.ValueSpecification query,
            @com.legend.Nullable String mapping,
            @com.legend.Nullable com.legend.protocol.SourceInfo mappingSpan,
            @com.legend.Nullable String runtime,
            @com.legend.Nullable com.legend.protocol.SourceInfo runtimeSpan,
            @com.legend.Nullable PEmbeddedRuntime embeddedRuntime,
            com.legend.protocol.SourceInfo sourceInformation)
            implements PServiceExecution {
    }

    public record PMultiExecution(
            com.legend.protocol.spec.ValueSpecification query,
            @com.legend.Nullable String executionKey,
            @com.legend.Nullable List<PKeyedExecution> executions,
            com.legend.protocol.SourceInfo sourceInformation)
            implements PServiceExecution {
    }

    /** {@code executions['QA']: { mapping; runtime; }} — also the entry
     *  shape of an {@code ExecutionEnvironment}; span = braces. */
    public record PKeyedExecution(String keyValue,
                                  @com.legend.Nullable String mapping,
                                  @com.legend.Nullable com.legend.protocol.SourceInfo mappingSpan,
                                  @com.legend.Nullable String runtime,
                                  @com.legend.Nullable com.legend.protocol.SourceInfo runtimeSpan,
                                  @com.legend.Nullable PEmbeddedRuntime embeddedRuntime,
                                  @com.legend.Nullable PRuntimeComponents runtimeComponents,
                                  com.legend.protocol.SourceInfo sourceInformation)
            implements PExecutionParameters {
    }

    /** {@code runtimeComponents: { class; binding; runtime; }} — wire
     *  binding/clazz pointers + runtimePointer, alphabetical (harvest
     *  testExecutionEnvironment; probe ee3). */
    public record PRuntimeComponents(PPointer binding, PPointer clazz,
                                     String runtime,
                                     com.legend.protocol.SourceInfo runtimeSpan) {
    }

    /** One {@code postValidations:} entry (harvest
     *  testServiceWithPostValidation): description + parameters (spec
     *  values; source key `params`) + assertions ({@code id: lambda;}). */
    public record PPostValidation(String description,
                                  List<com.legend.protocol.spec.ValueSpecification> parameters,
                                  List<PPostValidationAssertion> assertions,
                                  com.legend.protocol.SourceInfo sourceInformation) {
    }

    public record PPostValidationAssertion(String id,
                                           com.legend.protocol.spec.ValueSpecification assertion,
                                           com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One {@code testSuites:} entry — wire {@code serviceTestSuite}
     *  (ZTailProbe "service-suites2"): suite/test spans anchor at their
     *  ID tokens; connection data rides the embedded-data wire. The 4.138
     *  COMPACT form {@code id 'doc'? ( resolvers... tests... )} adds
     *  {@code doc} and resolver-style test data (ZServiceV2Probe). */
    public record PServiceTestSuite(String id,
                                    @com.legend.Nullable String doc,
                                    @com.legend.Nullable PSuiteData testData,
                                    List<PSuiteTest> tests,
                                    com.legend.protocol.SourceInfo sourceInformation) {
        /** {@code data: [ connections: [...] ]} — span key..']'; OR the
         *  compact form's resolver entries (exactly one list is used). */
        public record PSuiteData(List<PSuiteConnData> connectionsTestData,
                                 @com.legend.Nullable List<PResolverData> serviceTestData,
                                 com.legend.protocol.SourceInfo sourceInformation) {
        }

        /** {@code id: Kind #{...}#} — span id..'}#'. */
        public record PSuiteConnData(String id, PEmbeddedDataValue data,
                                     com.legend.protocol.SourceInfo sourceInformation) {
        }

        /** One compact-form data entry (ZServiceV2Probe): {@code path;}
         *  is a {@code referenceDataResolver}, {@code path: Kind #{...}#;}
         *  a {@code baseDataResolver}; the elementPointer wire carries NO
         *  "type" key. */
        public record PResolverData(@com.legend.Nullable PEmbeddedDataValue data,
                                    String elementPath,
                                    com.legend.protocol.SourceInfo elementSourceInformation,
                                    com.legend.protocol.SourceInfo sourceInformation) {
        }

        /** {@code id: { serializationFormat?; asserts: [...] }} — wire
         *  {@code serviceTest}; keys always []. */
        /** {@code name = value} — the value rides the spec wire. */
        public record PSuiteParam(String name,
                                  com.legend.protocol.spec.ValueSpecification value) {
        }

        public record PSuiteTest(String id,
                                 @com.legend.Nullable String doc,
                                 @com.legend.Nullable String serializationFormat,
                                 List<String> keys,
                                 @com.legend.Nullable List<PSuiteParam> parameters,
                                 List<PTestAssertion> assertions,
                                 com.legend.protocol.SourceInfo sourceInformation) {
        }
    }

    /** Legacy {@code test: Single { data; asserts }} — wire
     *  {@code singleExecutionTest} (ZTailProbe "service-legacy-test"). */
    public record PLegacyServiceTest(String kind,
                                     @com.legend.Nullable String data,
                                     List<PLegacyAssert> asserts,
                                     List<PKeyedLegacyTest> keyedTests,
                                     com.legend.protocol.SourceInfo sourceInformation) {
        /** One {@code tests['KEY']: { data; asserts }} entry of a Multi
         *  test — span starts at the {@code tests} keyword. */
        public record PKeyedLegacyTest(String key, String data,
                                       List<PLegacyAssert> asserts,
                                       com.legend.protocol.SourceInfo sourceInformation) {
        }

        /** One assert: {@code { [params], lambda }} — parametersValues
         *  are spec values (harvest testServiceTestParameters). */
        public record PLegacyAssert(
                List<com.legend.protocol.spec.ValueSpecification> parametersValues,
                com.legend.protocol.spec.ValueSpecification assertion,
                com.legend.protocol.SourceInfo sourceInformation) {
        }
    }

    /** {@code RelationalMapper qn ( DatabaseMappers/SchemaMappers/
     *  TableMappers )} — the ###QueryPostProcessor element (harvest
     *  TestRelationalMapperCompilationFromGrammar; wire probe rm.out):
     *  {@code _type:"relationalMapper"}, fields alphabetical. */
    public record PRelationalMapper(String pkg, String name,
                                    List<PDatabaseMapper> databaseMappers,
                                    List<PSchemaMapper2> schemaMappers,
                                    List<PTableMapper2> tableMappers,
                                    com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** {@code [db.Schema, ...] -> 'name'} — databaseName is the TARGET. */
    public record PDatabaseMapper(String databaseName,
                                  List<PSchemaPointer> schemas) {
    }

    /** {@code db.Schema -> 'name'}. */
    public record PSchemaMapper2(PSchemaPointer from, String to) {
    }

    /** {@code db.Schema.Table -> 'name'}. */
    public record PTableMapper2(PTablePointer2 from, String to) {
    }

    /** {@code _type:"Schema"} pointer: database + schema + span. */
    public record PSchemaPointer(String database, String schema,
                                 com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code _type:"Table"} pointer: database + schema + table + span
     *  (fields alphabetical on the wire: database, schema, si, table). */
    public record PTablePointer2(String database, String schema, String table,
                                 com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One {@code executions:} entry of an {@code ExecutionEnvironment} —
     *  either a keyed single or a keyed LIST of singles (multi). */
    public sealed interface PExecutionParameters
            permits PKeyedExecution, PMultiKeyedExecution {
    }

    /** {@code KEY: [ subKey: {..}, .. ]} — wire
     *  {@code _type:"multiExecutionParameters"} with masterKey +
     *  singleExecutionParameters, no span of its own (harvest
     *  testExecutionEnvironmentInMultiExecService). */
    public record PMultiKeyedExecution(String masterKey,
                                       List<PKeyedExecution> singles)
            implements PExecutionParameters {
    }

    /** An {@code ExecutionEnvironment} element (###Service section). */
    public record PExecutionEnvironment(String pkg, String name,
                                        List<PExecutionParameters> executions,
                                        com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /**
     * A {@code ###DataSpace} DataSpace element — {@code _type:"dataSpace"}
     * (ZTailProbe "dataspace-rich"/"dataspace-email"). Lists are NULL when
     * their key is unspelled: the wire omits the slot entirely.
     */
    public record PDataSpace(String pkg, String name,
                             List<PStereotype> stereotypes,
                             List<PTaggedValue> taggedValues,
                             @com.legend.Nullable List<PDataSpaceContext> executionContexts,
                             @com.legend.Nullable String defaultExecutionContext,
                             @com.legend.Nullable String title,
                             @com.legend.Nullable String description,
                             @com.legend.Nullable List<PDataSpaceExecutable> executables,
                             @com.legend.Nullable List<PDataSpaceDiagram> diagrams,
                             @com.legend.Nullable PDataSpaceSupport supportInfo,
                             @com.legend.Nullable PDataSpaceOperationalMetadata operationalMetadata,
                             @com.legend.Nullable List<PDataSpaceElementRef> elements,
                             com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** One dataspace execution context. Pointer spans cover the WHOLE
     *  {@code key: value;} statement (semicolon included); the testData
     *  span covers the VALUE only ({@code Kind #{...}#}). Since 4.145.0 a
     *  context names EITHER a mapping OR a mapping provider, and its
     *  default runtime is optional. */
    public record PDataSpaceContext(String name,
                                    @com.legend.Nullable String title,
                                    @com.legend.Nullable String description,
                                    @com.legend.Nullable String mapping,
                                    @com.legend.Nullable com.legend.protocol.SourceInfo mappingSpan,
                                    @com.legend.Nullable PDataSpaceMappingProvider mappingProvider,
                                    @com.legend.Nullable String defaultRuntime,
                                    @com.legend.Nullable com.legend.protocol.SourceInfo runtimeSpan,
                                    @com.legend.Nullable PDataSpaceTestData testData,
                                    com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code mappingProvider: fn.k1, k2;} (4.145.0) — an element pointer
     *  (typeless on the wire) plus its keys; the provider's span covers
     *  the whole statement, the element's its qualified name. */
    public record PDataSpaceMappingProvider(String element,
                                            com.legend.protocol.SourceInfo elementSpan,
                                            List<String> keys,
                                            com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code operationalMetadata: { coverageRegions: [..]; updateFrequency: X; };}
     *  (4.145.0) — regions and frequency are the engine's enum names,
     *  validated as written; span covers key through {@code }} . */
    public record PDataSpaceOperationalMetadata(List<String> coverageRegions,
                                                @com.legend.Nullable String updateFrequency,
                                                com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code testData: Kind #{ path }#} — kind Reference /
     *  DataspaceTestData / ...; span covers kind through {@code }#}. */
    public record PDataSpaceTestData(String kind, String path,
                                     com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One dataspace executable: path form OR inline-query template form
     *  (the id stringifies on the wire even when written as an integer). */
    public record PDataSpaceExecutable(@com.legend.Nullable String id,
                                       String title,
                                       @com.legend.Nullable String description,
                                       @com.legend.Nullable String executable,
                                       @com.legend.Nullable com.legend.protocol.SourceInfo executableSpan,
                                       @com.legend.Nullable com.legend.protocol.spec.ValueSpecification query,
                                       @com.legend.Nullable String executionContextKey,
                                       @com.legend.Nullable PRelationElement sampleValues,
                                       com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code { label: '..'; url: '..'; }} inside the full support form
     *  (4.145.0); span covers the braces. */
    public record PDataSpaceLink(@com.legend.Nullable String label, String url,
                                 com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code { title: '..'; address: '..'; }} — one email of the full
     *  support form. */
    public record PDataSpaceEmail(String title, String address,
                                  com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code { description: '..'; expertIds: ['..']; }} — one expertise
     *  entry of the full support form. */
    public record PDataSpaceExpertise(@com.legend.Nullable String description,
                                      @com.legend.Nullable List<String> expertIds,
                                      com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One dataspace diagram reference — the pointer carries NO type
     *  field on the wire. */
    public record PDataSpaceDiagram(String title,
                                    @com.legend.Nullable String description,
                                    String diagram,
                                    com.legend.protocol.SourceInfo diagramSpan,
                                    com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One {@code elements:} entry — {@code exclude} only when the path
     *  is '-'-prefixed; span includes the '-'. */
    public record PDataSpaceElementRef(String path, boolean exclude,
                                       com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code supportInfo:} — Email or Combined; span covers the VALUE
     *  ({@code Kind { ... }}), no key, no semicolon. */
    public sealed interface PDataSpaceSupport {
        record PSupportEmail(String address,
                             @com.legend.Nullable String documentationUrl,
                             com.legend.protocol.SourceInfo sourceInformation)
                implements PDataSpaceSupport {
        }

        record PSupportCombined(@com.legend.Nullable String documentationUrl,
                                @com.legend.Nullable String website,
                                @com.legend.Nullable String faqUrl,
                                @com.legend.Nullable String supportUrl,
                                @com.legend.Nullable List<String> emails,
                                com.legend.protocol.SourceInfo sourceInformation)
                implements PDataSpaceSupport {
        }

        /** The keyword-less FULL form (4.145.0, {@code _type:"full"}):
         *  {@code supportInfo: { documentation: {..}; website: {..};
         *  faqUrl: {..}; supportUrl: {..}; emails: [..]; expertise: [..]; };}
         *  — links carry a label; emails and expertise are structured. */
        record PSupportFull(@com.legend.Nullable PDataSpaceLink documentation,
                            @com.legend.Nullable PDataSpaceLink website,
                            @com.legend.Nullable PDataSpaceLink faqUrl,
                            @com.legend.Nullable PDataSpaceLink supportUrl,
                            @com.legend.Nullable List<PDataSpaceEmail> emails,
                            @com.legend.Nullable List<PDataSpaceExpertise> expertise,
                            com.legend.protocol.SourceInfo sourceInformation)
                implements PDataSpaceSupport {
        }
    }

    /** A {@code ###Persistence} Persistence element — top-level keys
     *  structured, deep sub-DSLs RAW (see PersistenceSectionGrammar). No
     *  wire shape claimed; emission walls. */
    public record PPersistence(String pkg, String name,
                               List<PStereotype> stereotypes,
                               List<PTaggedValue> taggedValues,
                               @com.legend.Nullable String doc,
                               String triggerKind,
                               @com.legend.Nullable String service,
                               @com.legend.Nullable com.legend.protocol.SourceInfo serviceSpan,
                               @com.legend.Nullable PPersistenceNode persister,
                               @com.legend.Nullable PPersistenceNotifier notifier,
                               @com.legend.Nullable List<PServiceOutputTarget> serviceOutputTargets,
                               @com.legend.Nullable List<PPersistenceTest> tests,
                               com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** One persistence sub-DSL node — {@code Kind { entries }} (span
     *  kind..'}'), {@code Kind #{ entries }#} (span kind..'}#'), or a
     *  bare keyword leaf (empty entries, span = the keyword text). Wire
     *  field names ARE the grammar keys; slots sort alphabetically at
     *  emission (ZTailProbe "persist-v1"/"persist-v2"). */
    public record PPersistenceNode(String kind,
                                   @com.legend.Nullable com.legend.protocol.spec.ValueSpecification headPath,
                                   List<PPersistenceEntry> entries,
                                   com.legend.protocol.SourceInfo sourceInformation) {
        /** A keyword-headed node (the usual form). */
        public PPersistenceNode(String kind, List<PPersistenceEntry> entries,
                com.legend.protocol.SourceInfo sourceInformation) {
            this(kind, null, entries, sourceInformation);
        }
    }

    /** One entry of a persistence node. */
    public sealed interface PPersistenceEntry {
        String key();

        /** {@code key: 'string' | identifier | true/false;}. */
        record Scalar(String key, String value, boolean quoted)
                implements PPersistenceEntry {
        }

        /** {@code key: Kind {...} | Kind #{...}# | Keyword;}. */
        record Node(String key, PPersistenceNode node)
                implements PPersistenceEntry {
        }

        /** A store/element pointer ({@code database:}) — span covers the
         *  whole {@code key: path;} statement. */
        record Pointer(String key, String path,
                       com.legend.protocol.SourceInfo sourceInformation)
                implements PPersistenceEntry {
        }

        /** {@code key: [id, id]} — identifiers as wire strings. */
        record Strings(String key, List<String> values)
                implements PPersistenceEntry {
        }

        /** {@code key: #/Class/prop#;} — a spec path literal riding the
         *  shifted-span path-value wire. */
        record PathValue(String key,
                         com.legend.protocol.spec.ValueSpecification spec)
                implements PPersistenceEntry {
        }

        /** {@code key: [ { ... }, ... ]} — KEYLESS braced parts
         *  (MultiFlat parts). */
        record NodeList(String key, List<PPersistenceNode> nodes)
                implements PPersistenceEntry {
        }

        /** {@code key: [ #/a/b#, ... ]} — graphFetch key paths, wired as
         *  bare path values. */
        record PathList(String key,
                        List<com.legend.protocol.spec.ValueSpecification> specs)
                implements PPersistenceEntry {
        }
    }

    /** {@code notifier: { notifyees: [...] }} — span key..'}' (the ONE
     *  key-anchored span in the family); ALWAYS on the wire, empty
     *  notifyees and no span when unspelled. */
    public record PPersistenceNotifier(List<PPersistenceNode> notifyees,
                                       @com.legend.Nullable com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One {@code serviceOutputTargets:} pair —
     *  {@code ServiceOutput -> Target}; span covers the whole pair. */
    public record PServiceOutputTarget(PPersistenceNode serviceOutput,
                                       PPersistenceNode persistenceTarget,
                                       com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One {@code tests:} entry — {@code id: { testBatches; ... }};
     *  graphFetch tests carry a {@code graphFetchPath:} path literal. */
    public record PPersistenceTest(String id,
                                   List<PPersistenceTestBatch> testBatches,
                                   boolean isTestDataFromServiceOutput,
                                   @com.legend.Nullable com.legend.protocol.spec.ValueSpecification graphFetchPath,
                                   com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One test batch — batchId AUTO-NUMBERS in source order. */
    public record PPersistenceTestBatch(String id,
                                        PPersistenceNode connectionData,
                                        com.legend.protocol.SourceInfo connectionSpan,
                                        com.legend.protocol.SourceInfo dataSpan,
                                        List<PPersistenceAssert> asserts,
                                        com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One assertion — {@code id: Kind #{ ... }#}. */
    public record PPersistenceAssert(String id, PPersistenceNode assertion,
                                     com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** A {@code PersistenceContext} element. */
    public record PPersistenceContext(String pkg, String name,
                                      List<PStereotype> stereotypes,
                                      List<PTaggedValue> taggedValues,
                                      String persistence,
                                      com.legend.protocol.SourceInfo persistenceSpan,
                                      @com.legend.Nullable PPersistenceNode platform,
                                      List<PCtxParam> serviceParameters,
                                      @com.legend.Nullable PConnectionValue sinkConnection,
                                      com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** One {@code serviceParameters:} entry — {@code name=value} where the
     *  value is a primitive (spec wire), a connection pointer, or an
     *  embedded connection island; span covers the whole entry. */
    public record PCtxParam(String name, PCtxParamValue value,
                            com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** A context service-parameter value. */
    public sealed interface PCtxParamValue {
        record Primitive(com.legend.protocol.spec.ValueSpecification spec)
                implements PCtxParamValue {
        }

        record ConnectionPtr(String path,
                             com.legend.protocol.SourceInfo sourceInformation)
                implements PCtxParamValue {
        }

        record ConnectionVal(PConnectionValue connection)
                implements PCtxParamValue {
        }
    }

    /** One diagram geometry point. */
    public record PDiagramPoint(double x, double y) {
    }

    /** {@code classView id { class: ...; position/rectangle; hide*; }} —
     *  hide flags only when spelled; class path unquoted on the wire but
     *  its span covers the text AS WRITTEN (ZTailProbe "diagram"). */
    public record PClassView(String id, String classPath,
                             com.legend.protocol.SourceInfo classSpan,
                             @com.legend.Nullable Boolean hideProperties,
                             @com.legend.Nullable Boolean hideStereotypes,
                             @com.legend.Nullable Boolean hideTaggedValues,
                             double x, double y, double width, double height,
                             com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code propertyView { property: Class.prop; source/target; points }}
     *  — the property's span covers the CLASS portion only. */
    public record PPropertyView(String propertyClass, String propertyName,
                                com.legend.protocol.SourceInfo propertySpan,
                                String sourceView,
                                com.legend.protocol.SourceInfo sourceViewSpan,
                                String targetView,
                                com.legend.protocol.SourceInfo targetViewSpan,
                                List<PDiagramPoint> points,
                                com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code generalizationView { source/target; points }}. */
    public record PGeneralizationView(String sourceView,
                                      com.legend.protocol.SourceInfo sourceViewSpan,
                                      String targetView,
                                      com.legend.protocol.SourceInfo targetViewSpan,
                                      List<PDiagramPoint> points,
                                      com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** A {@code ###Diagram} Diagram element — {@code _type:"diagram"}
     *  (ZTailProbe "diagram"): structured views with file-absolute spans
     *  even though the section content never reaches the shared lexer. */
    public record PDiagram(String pkg, String name,
                           List<PClassView> classViews,
                           List<PPropertyView> propertyViews,
                           List<PGeneralizationView> generalizationViews,
                           com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /**
     * A FUNCTION ACTIVATOR element — the uniform family SnowflakeApp /
     * SnowflakeM2MUdf / MemSqlFunction / BigQueryFunction / HostedService /
     * FunctionJar (ZTailProbe "activatorShapes"/"activatorShapes2"): typed
     * scalar/boolean fields, a FUNCTION pointer keeping its full signature
     * text and span, ownership (a DeploymentOwner id OR a UserList — the
     * two are exclusive), and an optional activation-configuration
     * connection pointer.
     */
    public record PFunctionActivator(String pkg, String name, String kind,
                                     List<PStereotype> stereotypes,
                                     List<PTaggedValue> taggedValues,
                                     java.util.Map<String, String> scalars,
                                     java.util.Map<String, Boolean> booleans,
                                     String functionPath,
                                     com.legend.protocol.SourceInfo functionSpan,
                                     @com.legend.Nullable String ownerId,
                                     @com.legend.Nullable List<String> userListUsers,
                                     @com.legend.Nullable String activationConnection,
                                     @com.legend.Nullable com.legend.protocol.SourceInfo activationConnectionSpan,
                                     com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public PFunctionActivator {
            scalars = java.util.Map.copyOf(scalars);
            booleans = java.util.Map.copyOf(booleans);
            if (ownerId != null && userListUsers != null) {
                throw new IllegalArgumentException(
                        "ownership is Deployment OR UserList, not both");
            }
        }

        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** {@code ###Text} — {@code Text pkg::T { type: STRING; content: '...' }};
     *  type is optional (ZTailProbe "text"). */
    public record PText(String pkg, String name,
                        @com.legend.Nullable String type, String content,
                        com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** One {@code generationNodes:} entry — id defaults to the element path
     *  text when not spelled (ZTailProbe "genspec"). */
    public record PGenerationNode(String generationElement, String id,
                                  com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code ###GenerationSpecification} (ZTailProbe "genspec"):
     *  generation-node list + FILE_GENERATION pointers. */
    public record PGenerationSpecification(String pkg, String name,
                                           List<PGenerationNode> generationNodes,
                                           List<PPointer> fileGenerations,
                                           com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** A {@code ###FileGeneration} config value — the typed JSON scalar
     *  forms the composer round-trips (ZTailProbe "filegen"). */
    public sealed interface PConfigValue {
        record PCString(String value) implements PConfigValue { }
        record PCBoolean(boolean value) implements PConfigValue { }
        record PCInteger(long value) implements PConfigValue { }
        record PCStrings(List<String> values) implements PConfigValue { }
        /** {@code {k1: 'v1'; k2: 'v2';}} — insertion-ordered. */
        record PCMap(java.util.LinkedHashMap<String, String> entries)
                implements PConfigValue { }
    }

    /** One config property — name keeps its QUOTES when quoted; span covers
     *  {@code name: value;} including the semicolon. */
    public record PConfigProperty(String name, PConfigValue value,
                                  com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code ###FileGeneration} (ZTailProbe "filegen"): the type keyword is
     *  OPEN (Avro/Java/Protobuf/...); scopeElements + generationOutputPath
     *  hoist out of configurationProperties. */
    public record PFileGeneration(String pkg, String name, String type,
                                  com.legend.protocol.SourceInfo typeSourceInformation,
                                  @com.legend.Nullable String generationOutputPath,
                                  List<String> scopeElements,
                                  List<PConfigProperty> configurationProperties,
                                  com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** A Deephaven table column — kind is the wire {@code _type} stem
     *  (stringType/intType/...); precision/scale ride DECIMAL only. */
    public record PDeephavenColumn(String name, String kind,
                                   @com.legend.Nullable Integer precision,
                                   @com.legend.Nullable Integer scale) {
    }

    /** {@code ###Deephaven} store (ZTailProbe "deephaven-store"/"-cols"):
     *  tables of typed columns; no table/column spans on the wire. */
    public record PDeephavenDatabase(String pkg, String name,
                                  List<PDeephavenTable> tables,
                                  com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public record PDeephavenTable(String name, List<PDeephavenColumn> columns) {
        }

        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** {@code ###Elasticsearch} cluster (ZTailProbe "elastic-cluster"):
     *  indices of named properties; the property type is the wire key
     *  ({@code keyword}) with a {@code _pure_protocol_type} carrier. */
    public record PElasticsearch7Cluster(String pkg, String name,
                                      List<PEsIndex> indices,
                                      com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public record PEsIndex(String indexName, List<PEsProperty> properties) {
        }

        /** {@code typeKey} is the lowercase wire key ({@code keyword}). */
        public record PEsProperty(String propertyName, String wireKey,
                String protocolType, String esType,
                @com.legend.Nullable List<PEsProperty> fields,
                @com.legend.Nullable List<PEsProperty> childProperties) {
        }

        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** A BSON schema node from a MongoDB {@code jsonSchema:} island —
     *  typed by wire {@code _type} (schema/stringType/longType/...);
     *  scalar facets only when spelled, properties in source order
     *  (ZTailProbe "mongo-rich"). */
    public record PBsonSchema(String wireType,
                              @com.legend.Nullable Boolean additionalPropertiesAllowed,
                              List<java.util.Map.Entry<String, PBsonSchema>> properties,
                              List<String> required,
                              @com.legend.Nullable String title,
                              @com.legend.Nullable String description,
                              @com.legend.Nullable Long minLength,
                              @com.legend.Nullable Long maxLength,
                              @com.legend.Nullable List<PBsonSchema> items) {
    }

    /** {@code ###MongoDB} database (ZTailProbe "mongo-db"/"mongo-rich"):
     *  collections with validation levels and a structured BSON schema. */
    public record PMongoDatabase(String pkg, String name,
                                 List<PMongoCollection> collections,
                                 com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public record PMongoCollection(String name, String validationLevel,
                                       String validationAction,
                                       PBsonSchema schema) {
        }

        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** A DataQuality graph-fetch tree node: root carries the class name,
     *  property nodes the property name; both carry constraint names and
     *  subtrees (ZTailProbe "dq-validation"). */
    public record PDqTreeNode(@com.legend.Nullable String className,
                              @com.legend.Nullable String property,
                              List<String> constraints,
                              List<PDqTreeNode> subTrees,
                              @com.legend.Nullable String subType,
                              com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code DataQualityValidation} (tree kind): context pointer(s) +
     *  validation tree + optional filter lambda (spec wire). */
    public record PDataQualityValidation(String pkg, String name,
                                         List<PStereotype> stereotypes,
                                         List<PTaggedValue> taggedValues,
                                         String contextKind,
                                         String contextPath,
                                         com.legend.protocol.SourceInfo contextPathSpan,
                                         @com.legend.Nullable String contextSecond,
                                         @com.legend.Nullable com.legend.protocol.SourceInfo contextSecondSpan,
                                         PDqTreeNode validationTree,
                                         @com.legend.Nullable com.legend.protocol.spec.ValueSpecification filter,
                                         com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** One {@code validations:} entry of a relation validation. */
    public record PDqRelationCheck(String name,
                                   @com.legend.Nullable String description,
                                   com.legend.protocol.spec.ValueSpecification assertion,
                                   @com.legend.Nullable String type) {
    }

    /** {@code DataQualityRelationValidation} (ZTailProbe
     *  "dq-relation-validation"): a relation query lambda + named
     *  assertion lambdas, all on the spec wire. */
    public record PDataQualityRelationValidation(String pkg, String name,
                                                 List<PStereotype> stereotypes,
                                                 List<PTaggedValue> taggedValues,
                                                 com.legend.protocol.spec.ValueSpecification query,
                                                 List<PDqRelationCheck> validations,
                                                 @com.legend.Nullable List<PDqTestSuite> testSuites,
                                                 com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** {@code testSuites: [ id: { data: [store: EmbeddedData]; tests: [id: {
     *  asserts: [id: Assertion] }] } ]} on the two relation-level DataQuality
     *  elements (4.145.0, the engine's Testable): the suite's {@code _type}
     *  is the owner's ({@code dataQualityRelationValidationTestSuite} /
     *  {@code dataQualityRelationComparisonTestSuite}, tests likewise);
     *  the data block is a wrapper object with its own span. */
    public record PDqTestSuite(String id,
                               @com.legend.Nullable PDqTestData testData,
                               List<PDqTest> tests,
                               com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** The {@code data: [...]} block — a wrapper carrying its own span. */
    public record PDqTestData(List<PDqStoreData> testData,
                              com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One {@code store: EmbeddedData} entry (the engine's FunctionTestData:
     *  a STORE pointer plus embedded data; span covers the whole entry). */
    public record PDqStoreData(String store, com.legend.protocol.SourceInfo storeSpan,
                               PEmbeddedDataValue data,
                               com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One {@code id: { asserts: [...] }} test. */
    public record PDqTest(String id, List<PTestAssertion> assertions,
                          com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code DataQualityRelationComparison} (ZTailProbe
     *  "dq-relation-comparison"): source/target lambdas, key names, and a
     *  strategy tag ({@code MD5Hash} &rarr; {@code md5Hash}). */
    public record PDataQualityRelationComparison(String pkg, String name,
                                                 com.legend.protocol.spec.ValueSpecification source,
                                                 com.legend.protocol.spec.ValueSpecification target,
                                                 List<String> keys,
                                                 List<String> columnsToCompare,
                                                 @com.legend.Nullable Double expectedMatch,
                                                 PReconStrategy strategy,
                                                 @com.legend.Nullable List<PDqTestSuite> testSuites,
                                                 com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** {@code strategy: MD5Hash { sourceHashColumn: c; ... }} — wire
     *  {@code {_type:"md5Hash", ...}}; the config block is OPTIONAL but
     *  must be non-empty when present (engine grammar {@code (...)+}). */
    public record PReconStrategy(String kind,
                                 @com.legend.Nullable String sourceHashColumn,
                                 @com.legend.Nullable String targetHashColumn,
                                 @com.legend.Nullable Boolean aggregatedHash) {
    }

    /** One {@code schemas:} entry of a SchemaSet — contentSourceInformation
     *  is the string TOKEN's span, quotes included; sourceInformation the
     *  brace body (ZTailProbe "schemaset"). */
    public record PSchema(@com.legend.Nullable String id,
                          @com.legend.Nullable String location,
                          String content,
                          com.legend.protocol.SourceInfo contentSourceInformation,
                          com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code ###ExternalFormat SchemaSet} (ZTailProbe "schemaset"):
     *  {@code _type:"externalFormatSchemaSet"}. */
    public record PSchemaSet(String pkg, String name, String format,
                             List<PSchema> schemas,
                             com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** {@code ###ExternalFormat Binding} (ZTailProbe "binding"):
     *  {@code _type:"binding"}; schemaSet/schemaId omitted when schemaless. */
    public record PBinding(String pkg, String name,
                           @com.legend.Nullable String schemaSet,
                           @com.legend.Nullable String schemaId,
                           String contentType,
                           List<String> modelIncludes,
                           List<String> modelExcludes,
                           com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** A ServiceStore type reference — primitive ({@code String} /
     *  {@code [Integer]}) or complex ({@code Class <- binding}); exactly
     *  one of {@code primitive}/{@code complexType} is set (ZTailProbe
     *  "svcstore-rich"). */
    public record PSsTypeRef(@com.legend.Nullable String primitive,
                             @com.legend.Nullable String complexType,
                             @com.legend.Nullable String binding,
                             boolean list,
                             com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One ServiceStore service parameter — serializationFormat facets
     *  (style/explode) carry their own {@code x = y} text spans. */
    public record PSsParam(String name, PSsTypeRef type,
                           @com.legend.Nullable Boolean allowReserved,
                           @com.legend.Nullable Boolean required,
                           String location,
                           @com.legend.Nullable String style,
                           @com.legend.Nullable com.legend.protocol.SourceInfo styleSpan,
                           @com.legend.Nullable Boolean explode,
                           @com.legend.Nullable com.legend.protocol.SourceInfo explodeSpan,
                           @com.legend.Nullable String enumeration,
                           com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** A ServiceStore element: a service or a nested group. */
    public sealed interface PServiceStoreElement {
    }

    /** {@code Service id ( ... )} — parameters is null when the block is
     *  not spelled (the wire omits the slot entirely). */
    public record PSsService(String id, String path,
                             @com.legend.Nullable PSsTypeRef requestBody,
                             String method,
                             @com.legend.Nullable List<PSsParam> parameters,
                             PSsTypeRef response,
                             List<String> security,
                             com.legend.protocol.SourceInfo sourceInformation)
            implements PServiceStoreElement {
    }

    /** {@code ServiceGroup id ( path ... nested )}. */
    public record PSsServiceGroup(String id, String path,
                                  List<PServiceStoreElement> elements,
                                  com.legend.protocol.SourceInfo sourceInformation)
            implements PServiceStoreElement {
    }

    /** {@code ###ServiceStore} (ZTailProbe "svcstore-*"): _type
     *  "serviceStore". The {@code description:} field parses but never
     *  reaches the wire (engine walker drops it). */
    public record PServiceStoreDefinition(String pkg, String name,
                                          @com.legend.Nullable String description,
                                          List<PServiceStoreElement> elements,
                                          com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /**
     * {@code _type:"runtime"} — {@code Runtime pkg::R { mappings: [...];
     * connections: [...] }} and {@code SingleConnectionRuntime} (whose
     * runtimeValue discriminates {@code localEngineRuntime}). Probed
     * byte-shapes: ZRuntimeProbe — fields alphabetical, element and
     * runtimeValue share ONE span, pointer types spelled {@code MAPPING} /
     * {@code STORE}, connections keep their IDs and source ORDER.
     */
    public record PRuntime(String pkg, String name, boolean single,
                           List<PPointer> mappings,
                           List<PStoreConnections> connections,
                           List<PConnectionStores> connectionStores,
                           com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** A typed packageable-element pointer ({@code path}/{@code type}/span). */
    public record PPointer(String type, String path,
                           com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** One {@code store: [ id: conn, ... ]} group — order preserved. */
    public record PStoreConnections(PPointer store,
                                    List<PIdentifiedConnection> storeConnections,
                                    com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code id: <connection>} — pointer or embedded connection value. */
    public record PIdentifiedConnection(String id, PConnectionValue connection,
                                        com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code connectionStores: [conn: [stores...]]} group (also
     *  SingleConnectionRuntime's {@code connection:}, storePointers empty).
     *  Store pointers serialize path+span WITHOUT a type field (probe). */
    public record PConnectionStores(PConnectionValue connectionPointer,
                                    List<PStorePointer> storePointers,
                                    com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** A typeless store pointer inside connectionStores. */
    public record PStorePointer(String path,
                                com.legend.protocol.SourceInfo sourceInformation,
                                @com.legend.Nullable String type) {
    }

    /** A connection VALUE: a pointer, or a concrete connection (standalone
     *  ###Connection element or embedded runtime island). */
    public sealed interface PConnectionValue
            permits PConnectionPointer, PJsonModelConnection,
            PXmlModelConnection, PModelChainConnection,
            PRelationalDatabaseConnection, PServiceStoreConnection,
            PDeephavenConnection, PMongoDbConnection,
            PElasticsearchConnection {
    }

    /** {@code _type:"elasticsearch7StoreConnection"} — store element,
     *  {@code clusterDetails: # URL {...}#} (wire {@code sourceSpec.url}
     *  only) and a UserPassword auth island sharing the Mongo secret
     *  wire (C12 ES leg; wire probed live). */
    public record PElasticsearchConnection(String element,
                               com.legend.protocol.SourceInfo elementSourceInformation,
                               String url, PAuthSpecValue auth,
                               com.legend.protocol.SourceInfo sourceInformation)
            implements PConnectionValue {
    }

    /** {@code ServiceStoreConnection { store; baseUrl; }} —
     *  {@code _type:"serviceStore"} (ZTailProbe "servicestore-conn"). */
    public record PServiceStoreConnection(String baseUrl,
                                          @com.legend.Nullable String element,
                                          @com.legend.Nullable com.legend.protocol.SourceInfo elementSourceInformation,
                                          com.legend.protocol.SourceInfo sourceInformation)
            implements PConnectionValue {
    }

    /** {@code DeephavenConnection { store; serverUrl; authentication:
     *  # PSK {...}#; }} — {@code _type:"deephavenConnection"} (ZTailProbe
     *  "deephaven-conn"); the value's span runs the first body key through
     *  the island close. */
    public record PDeephavenConnection(String serverUrl, String psk,
                                       @com.legend.Nullable String element,
                                       @com.legend.Nullable com.legend.protocol.SourceInfo elementSourceInformation,
                                       com.legend.protocol.SourceInfo sourceInformation)
            implements PConnectionValue {
    }

    /** {@code MongoDBConnection { database; store; serverURLs;
     *  authentication: # UserPassword {...}#; }} —
     *  {@code _type:"MongoDBConnection"} with {@code type:"MongoDb"}
     *  (ZTailProbe "mongodb-conn"). */
    public record PMongoDbConnection(String databaseName,
                                     List<PMongoServerUrl> serverUrls,
                                     PAuthSpecValue auth,
                                     @com.legend.Nullable String element,
                                     @com.legend.Nullable com.legend.protocol.SourceInfo elementSourceInformation,
                                     com.legend.protocol.SourceInfo sourceInformation)
            implements PConnectionValue {
        public PMongoDbConnection {
            serverUrls = List.copyOf(serverUrls);
        }
    }

    /** One {@code host:port} server URL. */
    public record PMongoServerUrl(String baseUrl, long port) {
    }

    /** {@code # UserPassword { username; password: <Kind>Secret {...}; }#} —
     *  {@code _type:"userPassword"}; span = the {@code authentication} key
     *  through the island close. */
    /** An authentication-module island value ({@code # <Kind> {...}#}) —
     *  the kinds the ORACLE's registered extensions accept (probed:
     *  UserPassword, ApiKey, Kerberos, EncryptedPrivateKey). */
    public sealed interface PAuthSpecValue
            permits PMongoAuth, PApiKeyAuth, PKerberosAuth, PEpkAuth,
                    PPskAuth, PGcpWifIslandAuth {
    }

    /** {@code _type:"gcpWithAWSIdP"} — the GCPWIFWithAWSIdP auth island
     *  (probe t2-authentication 2026-08-14): idP + workload blocks with
     *  recursive AWS credentials. */
    public record PGcpWifIslandAuth(String serviceAccountEmail,
                                    String accountId, String region,
                                    String role, PAwsCredentials awsCredentials,
                                    String projectNumber, String providerId,
                                    String poolId,
                                    com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthSpecValue {
    }

    /** AWS credentials value — recursive via STSAssumeRole. */
    public sealed interface PAwsCredentials
            permits PAwsDefault, PAwsStatic, PAwsStsRole {
    }

    /** {@code _type:"awsDefault"} — spanless (probed). */
    public record PAwsDefault() implements PAwsCredentials {
    }

    /** {@code _type:"awsStatic"} — span runs the kind keyword through
     *  the closing brace. */
    public record PAwsStatic(PMongoSecret accessKeyId,
                             PMongoSecret secretAccessKey,
                             com.legend.protocol.SourceInfo sourceInformation)
            implements PAwsCredentials {
    }

    /** {@code _type:"awsSTSAssumeRole"} — nests another credentials
     *  value. */
    public record PAwsStsRole(String roleArn, String roleSessionName,
                              PAwsCredentials awsCredentials,
                              com.legend.protocol.SourceInfo sourceInformation)
            implements PAwsCredentials {
    }

    /** {@code _type:"PSK"} — Deephaven pre-shared key; the walker never
     *  sets sourceInformation (spanless on the wire). */
    public record PPskAuth(String psk) implements PAuthSpecValue {
    }

    /** {@code _type:"apiKey"} — location is UPPERCASED on the wire. */
    public record PApiKeyAuth(String keyName, String location,
                              PVaultSecret value,
                              com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthSpecValue {
    }

    /** {@code _type:"kerberos"} — empty body. */
    public record PKerberosAuth(com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthSpecValue {
    }

    /** {@code _type:"encryptedPrivateKey"}. */
    public record PEpkAuth(String userName, PVaultSecret privateKey,
                           PVaultSecret passphrase,
                           com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthSpecValue {
    }

    public record PMongoAuth(String username, PVaultSecret password,
                             com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthSpecValue {
    }

    /** A secret reference: {@code kind} is the wire discriminator
     *  ({@code properties} / {@code systemproperties}), {@code fieldKey}
     *  the wire field name, {@code value} its content; span = the secret
     *  VALUE region. */
    /** A credential-vault secret — the single-field kinds
     *  (properties/systemproperties/environment) or the AWS secrets
     *  manager shape (probed 2026-08-14). */
    public sealed interface PVaultSecret permits PMongoSecret, PAwsSecret {
    }

    /** {@code _type:"awssecretsmanager"} — NO sourceInformation on the
     *  secret itself; awsDefault credentials are spanless, awsStatic
     *  carries its block span (probed asymmetry). */
    public record PAwsSecret(String secretId,
                             @com.legend.Nullable String versionId,
                             @com.legend.Nullable String versionStage,
                             String credsKind,
                             @com.legend.Nullable PMongoSecret accessKeyId,
                             @com.legend.Nullable PMongoSecret secretAccessKey,
                             @com.legend.Nullable com.legend.protocol.SourceInfo credsSpan)
            implements PVaultSecret {
    }

    public record PMongoSecret(String kind, String fieldKey, String value,
                               com.legend.protocol.SourceInfo sourceInformation) 
            implements PVaultSecret {
    }

    /** {@code _type:"connectionPointer"}. */
    public record PConnectionPointer(String connection,
                                     com.legend.protocol.SourceInfo sourceInformation)
            implements PConnectionValue {
    }

    /** {@code _type:"JsonModelConnection"} — class + classSourceInformation
     *  + url; {@code element} is {@code "ModelStore"} for STANDALONE
     *  connection elements and null when embedded in a runtime (probes
     *  embedded-json vs json). */
    public record PJsonModelConnection(String className,
                                       com.legend.protocol.SourceInfo classSourceInformation,
                                       @com.legend.Nullable String element,
                                       String url,
                                       com.legend.protocol.SourceInfo sourceInformation)
            implements PConnectionValue {
    }

    /** {@code _type:"XmlModelConnection"} — same shape as Json (probe xml). */
    public record PXmlModelConnection(String className,
                                      com.legend.protocol.SourceInfo classSourceInformation,
                                      @com.legend.Nullable String element,
                                      String url,
                                      com.legend.protocol.SourceInfo sourceInformation)
            implements PConnectionValue {
    }

    /** {@code _type:"ModelChainConnection"} — mappings as STRINGS + one
     *  mappingsSourceInformation (probe model-chain). */
    public record PModelChainConnection(@com.legend.Nullable String element,
                                        List<String> mappings,
                                        com.legend.protocol.SourceInfo mappingsSourceInformation,
                                        com.legend.protocol.SourceInfo sourceInformation)
            implements PConnectionValue {
    }

    /** {@code _type:"RelationalDatabaseConnection"} — databaseType AND type
     *  both spelled; spec/auth spans run keyword..close (probes
     *  relational-*). Only corpus-censused spec/auth shapes exist; the
     *  parser WALLS the rest loudly. */
    public record PRelationalDatabaseConnection(
            PAuthStrategy authenticationStrategy,
            String databaseType,
            PDatasourceSpec datasourceSpecification,
            @com.legend.Nullable String element,
            @com.legend.Nullable com.legend.protocol.SourceInfo elementSourceInformation,
            @com.legend.Nullable Boolean localMode,
            List<PPostProcessor> postProcessors,
            @com.legend.Nullable List<PGenerationFeaturesConfig> queryGenerationConfigs,
            @com.legend.Nullable Long queryTimeOutInSeconds,
            @com.legend.Nullable Boolean quoteIdentifiers,
            @com.legend.Nullable String timeZone,
            com.legend.protocol.SourceInfo sourceInformation)
            implements PConnectionValue {
    }

    /** {@code GenerationFeaturesConfig { enabled: [...]; disabled: [...] }}
     *  — wire {@code _type:"generationFeaturesConfig"}, disabled/enabled
     *  alphabetical, span keyword..close (harvest f_qgc probe). */
    public record PGenerationFeaturesConfig(List<String> enabled,
                                            List<String> disabled,
                                            com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code include <storeType> <path>} — a TYPED include rides
     *  includedStoreSpecifications (harvest testDatabaseIncludeStoreOrder):
     *  packageableElementPointer + the SAME span + storeType. */
    public record PIncludedStoreSpec(String path, String storeType,
                                     com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** Connection post-processor flavors: the {@code mapper} table/schema
     *  renamer and the {@code relationalMapper} pointer list. */
    public sealed interface PPostProcessor
            permits PMapperPostProcessor, PRelationalMapperPostProcessor {
    }

    /** {@code _type:"mapper"} post-processor: table/schema mappers; a table
     *  mapper's schemaFrom/schemaTo synthesize a NESTED schema object on the
     *  wire; mappers carry no sourceInformation (probe post-processors). */
    public record PMapperPostProcessor(List<PMapper> mappers)
            implements PPostProcessor {
    }

    /** {@code _type:"relationalMapper"} post-processor — pointers typed
     *  {@code QUERYPOSTPROCESSOR}, no span of its own (harvest
     *  testRelationalMapperRoundTrip). */
    public record PRelationalMapperPostProcessor(
            List<PPointer> relationalMappers) implements PPostProcessor {
    }

    public sealed interface PMapper permits PTableMapper, PSchemaMapper {
    }

    public record PTableMapper(String from, String to, String schemaFrom,
                               String schemaTo) implements PMapper {
    }

    public record PSchemaMapper(String from, String to) implements PMapper {
    }

    /** Datasource specifications the corpus actually uses, plus the
     *  legend-lite extension flavors ({@link PInMemory}, {@link PLocalFile},
     *  {@code url}-bearing {@link PH2Local}) that exist for lite's own
     *  DuckDB-first operation. The extension flavors have NO engine wire
     *  shape — {@link ProtocolEmitter} refuses them loudly. */
    public sealed interface PDatasourceSpec
            permits PH2Local, PStaticSpec, PH2EmbeddedSpec, PDuckDBSpec,
            PSQLiteSpec, PSnowflakeSpec, PSpannerSpec, PDatabricksSpec,
            PBigQuerySpec, PRedshiftSpec, PTrinoSpec {
    }

    /** {@code _type:"h2Embedded"} — EmbeddedH2 { name; directory;
     *  autoServerMode; } (probe connection-auth 2026-08-14). */
    public record PH2EmbeddedSpec(String databaseName, String directory,
                                  boolean autoServerMode,
                                  com.legend.protocol.SourceInfo sourceInformation)
            implements PDatasourceSpec {
    }

    /** {@code _type:"duckDB"} — the engine's DuckDB datasource spec
     *  (DuckDBParserGrammar: {@code DuckDB { (path: '...';)* }}; probe
     *  ZMigrationTargetProbe). No path = in-process/in-memory. */
    public record PDuckDBSpec(@com.legend.Nullable String path,
                              com.legend.protocol.SourceInfo sourceInformation)
            implements PDatasourceSpec {
    }

    /** The lite SQLite datasource spec — spelled EXACTLY like the engine's
     *  DuckDB extension pattern ({@code SQLite { (path: '...';)* }}) so a
     *  future engine SQLite extension finds our text conformant
     *  (own-corpus decision review 2026-08-11). No engine wire shape —
     *  the emitter refuses it loudly like the other extension flavors. */
    public record PSQLiteSpec(@com.legend.Nullable String path,
                              com.legend.protocol.SourceInfo sourceInformation)
            implements PDatasourceSpec {
    }

    /** {@code _type:"snowflake"} — source keys rename on the wire:
     *  {@code name}&rarr;databaseName, {@code account}&rarr;accountName,
     *  {@code warehouse}&rarr;warehouseName; optional fields omitted when
     *  absent (probe ZConnWidenProbe). */
    public record PSnowflakeSpec(String accountName,
                                 @com.legend.Nullable String accountType,
                                 @com.legend.Nullable String cloudType,
                                 String databaseName,
                                 @com.legend.Nullable Boolean enableQueryTags,
                                 @com.legend.Nullable String nonProxyHosts,
                                 @com.legend.Nullable String organization,
                                 @com.legend.Nullable String proxyHost,
                                 @com.legend.Nullable String proxyPort,
                                 @com.legend.Nullable Boolean quotedIdentifiersIgnoreCase,
                                 String region,
                                 @com.legend.Nullable String role,
                                 @com.legend.Nullable String tempTableDb,
                                 @com.legend.Nullable String tempTableSchema,
                                 String warehouseName,
                                 @com.legend.Nullable com.legend.protocol.SourceInfo sourceInformation)
            implements PDatasourceSpec {
    }

    /** {@code _type:"spanner"} (probe ZConnWidenProbe). */
    public record PSpannerSpec(String databaseId, String instanceId,
                               String projectId,
                               @com.legend.Nullable String proxyHost,
                               @com.legend.Nullable Long proxyPort,
                               com.legend.protocol.SourceInfo sourceInformation)
            implements PDatasourceSpec {
    }

    /** {@code _type:"redshift"} (census queued-relational-flavors). */
    public record PRedshiftSpec(String clusterID, String databaseName,
                                @com.legend.Nullable String endpointURL,
                                String host, long port, String region,
                                com.legend.protocol.SourceInfo sourceInformation)
            implements PDatasourceSpec {
    }

    /** {@code _type:"Trino"} — capitalized on the wire (census
     *  queued-relational-flavors). */
    public record PTrinoSpec(@com.legend.Nullable String catalog,
                             @com.legend.Nullable String clientTags,
                             String host, long port,
                             @com.legend.Nullable String schema,
                             @com.legend.Nullable PTrinoSsl sslSpecification,
                             com.legend.protocol.SourceInfo sourceInformation)
            implements PDatasourceSpec {
    }

    /** Trino {@code sslSpecification { ssl; trustStore...; }}. */
    public record PTrinoSsl(boolean ssl,
                            @com.legend.Nullable String trustStorePathVaultReference,
                            @com.legend.Nullable String trustStorePasswordVaultReference) {
    }

    /** {@code _type:"TrinoDelegatedKerberosAuth"} (census C12 flavor). */
    public record PTrinoKerberosAuth(
            @com.legend.Nullable String kerberosRemoteServiceName,
            @com.legend.Nullable Boolean kerberosUseCanonicalHostname,
            @com.legend.Nullable String serverPrincipal,
            com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthStrategy {
    }

    /** {@code _type:"gcpWorkloadIdentityFederation"} (census
     *  queued-relational-flavors, C12 sentinel). */
    public record PGcpWifAuth(@com.legend.Nullable List<String> additionalGcpScopes,
                              String serviceAccountEmail,
                              com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthStrategy {
    }

    /** {@code _type:"databricks"} — port stays a STRING on the wire
     *  (probe ZConnWidenProbe). */
    public record PDatabricksSpec(String hostname, String httpPath,
                                  String port, String protocol,
                                  com.legend.protocol.SourceInfo sourceInformation)
            implements PDatasourceSpec {
    }

    /** {@code _type:"bigQuery"} (probe ZConnWidenProbe). */
    public record PBigQuerySpec(String defaultDataset, String projectId,
                                @com.legend.Nullable String proxyHost,
                                @com.legend.Nullable String proxyPort,
                                com.legend.protocol.SourceInfo sourceInformation)
            implements PDatasourceSpec {
    }

    /** {@code _type:"h2Local"}; testDataSetupCsv/Sqls omitted when absent
     *  (source key testDataSetupCSV spells testDataSetupCsv on the wire).
     */
    public record PH2Local(@com.legend.Nullable String testDataSetupCsv,
                           @com.legend.Nullable List<String> testDataSetupSqls,
                           com.legend.protocol.SourceInfo sourceInformation)
            implements PDatasourceSpec {
    }

    /** {@code _type:"static"} — name: spells databaseName on the wire.
     *  legend-lite's extension spelling names the same field {@code
     *  database:} and may omit {@code port} (0). */
    public record PStaticSpec(String databaseName, String host, long port,
                              com.legend.protocol.SourceInfo sourceInformation)
            implements PDatasourceSpec {
    }

    /** Authentication strategies the corpus actually uses, plus the
     *  legend-lite extension flavors ({@link PNoAuth},
     *  {@link PPlainUserPassword}); the extensions refuse to emit. */
    public sealed interface PAuthStrategy
            permits PH2Default, PTestAuth, PDelegatedKerberos,
            PUserNamePassword, POAuth,
            PSnowflakePublic, PGCPApplicationDefaultCredentials, PApiToken,
            PMiddleTierUserNamePassword, PGcpWifAuth, PTrinoKerberosAuth {
    }

    /** {@code _type:"oauth"} — oauthKey + scopeName, both required
     *  (harvest testRelationalDatabaseConnection). */
    public record POAuth(String oauthKey, String scopeName,
                         com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthStrategy {
    }

    /** {@code _type:"snowflakePublic"} (probe ZConnWidenProbe). */
    public record PSnowflakePublic(String passPhraseVaultReference,
                                   String privateKeyVaultReference,
                                   String publicUserName,
                                   @com.legend.Nullable com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthStrategy {
    }

    /** {@code _type:"gcpApplicationDefaultCredentials"} — bodyless
     *  (probe ZConnWidenProbe). */
    public record PGCPApplicationDefaultCredentials(
            com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthStrategy {
    }

    /** {@code _type:"apiToken"} (probe ZConnWidenProbe). */
    public record PApiToken(String apiToken,
                            com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthStrategy {
    }

    /** {@code _type:"middleTierUserNamePassword"} (probe ZConnWidenProbe). */
    public record PMiddleTierUserNamePassword(String vaultReference,
                                              com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthStrategy {
    }

    /** {@code _type:"userNamePassword"} — vault references, base optional. */
    public record PUserNamePassword(@com.legend.Nullable String baseVaultReference,
                                    String userNameVaultReference,
                                    String passwordVaultReference,
                                    com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthStrategy {
    }

    /** {@code _type:"h2Default"}. */
    public record PH2Default(com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthStrategy {
    }

    /** {@code _type:"test"}. */
    public record PTestAuth(com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthStrategy {
    }

    /** {@code _type:"delegatedKerberos"}; serverPrincipal optional. */
    public record PDelegatedKerberos(@com.legend.Nullable String serverPrincipal,
                                     com.legend.protocol.SourceInfo sourceInformation)
            implements PAuthStrategy {
    }

    /**
     * {@code _type:"measure"} — {@code Measure pkg::M { *Canon: x -> $x; Other: x -> ... }}.
     * The {@code *}-marked unit is canonical; each unit's conversion is an arrow-form
     * lambda whose wrapper carries NO span (probe: vanilla engine Measure JSON).
     */
    public record PMeasure(String pkg, String name,
                           @com.legend.Nullable PUnit canonicalUnit,
                           List<PUnit> nonCanonicalUnits,
                           com.legend.protocol.SourceInfo sourceInformation)
            implements Element {
        public PMeasure {
            nonCanonicalUnits = List.copyOf(nonCanonicalUnits);
        }

        /** The UNmangled FQN — legend-lite's key. */
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** One measure unit: name, owning measure FQN, optional conversion (param + body —
     *  {@code *Unit;} has none), span = name..the terminating ';' ({@code *} excluded). */
    public record PUnit(String name, String measureFqn,
                        @com.legend.Nullable String paramName,
                        @com.legend.Nullable com.legend.protocol.spec.ValueSpecification body,
                        com.legend.protocol.SourceInfo sourceInformation) {
    }

    /**
     * One {@code functionTestSuite} (legend-testable trailing block). A {@code null} id is
     * the UNNAMED brace form — the wire spells it {@code "default"} and its span covers
     * the whole block; a named suite ({@code name ( ... )}) spans name..close-paren
     * (ProbeWireShapes "fn tests wire", "fn tests named suite").
     */
    public record PTestSuite(@com.legend.Nullable String id,
                             com.legend.protocol.SourceInfo sourceInformation,
                             List<PTestData> testData,
                             List<PFunctionTest> tests) {
        public PTestSuite {
            testData = List.copyOf(testData);
            tests = List.copyOf(tests);
        }

        /** No-data convenience constructor. */
        public PTestSuite(@com.legend.Nullable String id,
                          com.legend.protocol.SourceInfo sourceInformation,
                          List<PFunctionTest> tests) {
            this(id, sourceInformation, List.of(), tests);
        }
    }

    /** One {@code store: <payload>;} entry in a test block (probe "pf test store data"
     *  and friends). {@code pointerType} is non-null only for the marked
     *  form {@code (dataspace) my::DS: ...} — the wire gains
     *  {@code "type":"DATASPACE"} on the pointer (ZTailProbe
     *  "dataspace-testref"); the plain store form emits no type key. */
    public record PTestData(String storePath,
                            com.legend.protocol.SourceInfo storeSpan,
                            PTestPayload data,
                            @com.legend.Nullable String pointerType,
                            com.legend.protocol.SourceInfo sourceInformation) {
        /** The plain store form every pre-dataspace caller builds. */
        public PTestData(String storePath,
                com.legend.protocol.SourceInfo storeSpan, PTestPayload data,
                com.legend.protocol.SourceInfo sourceInformation) {
            this(storePath, storeSpan, data, null, sourceInformation);
        }
    }

    /** A test-data payload / format-prefixed assertion payload. */
    public sealed interface PTestPayload {
        /** {@code (JSON) '...'} — an externalFormat blob. */
        record ExternalFormat(String contentType, String data,
                              com.legend.protocol.SourceInfo sourceInformation)
                implements PTestPayload {
        }

        /** {@code some::DataElement} — a reference; {@code refType} null
         *  spells {@code DATA} on the wire, {@code DATASPACE} for the
         *  {@code DataspaceTestData #{...}#} island form (ZTailProbe
         *  "dataspace-testref"). */
        record Reference(String path, @com.legend.Nullable String refType,
                         com.legend.protocol.SourceInfo sourceInformation)
                implements PTestPayload {
            public Reference(String path,
                    com.legend.protocol.SourceInfo sourceInformation) {
                this(path, null, sourceInformation);
            }
        }

        /** {@code Relation #{ path: cols rows }#} — a relationAccessor. */
        record RelationElements(List<RelationElement> elements,
                                com.legend.protocol.SourceInfo sourceInformation)
                implements PTestPayload {
            public RelationElements {
                elements = List.copyOf(elements);
            }
        }

        /** One relation block: dotted path, column names, string-valued rows. */
        record RelationElement(List<String> columns, List<String> paths,
                               List<List<String>> rows,
                               com.legend.protocol.SourceInfo sourceInformation) {
            public RelationElement {
                columns = List.copyOf(columns);
                paths = List.copyOf(paths);
                rows = rows.stream().map(List::copyOf).toList();
            }
        }

        /** {@code ModelStore #{ FQN: ExternalFormat #{...}# }#} — modelStore data. */
        record ModelStoreData(List<ModelEmbedded> modelData,
                              com.legend.protocol.SourceInfo sourceInformation)
                implements PTestPayload {
            public ModelStoreData {
                modelData = List.copyOf(modelData);
            }
        }

        /** One {@code FQN: ExternalFormat #{ contentType: '...'; data: '...'; }#}. */
        record ModelEmbedded(String model, ExternalFormat data,
                             com.legend.protocol.SourceInfo sourceInformation) {
        }

        /** {@code Relational #{ schema.table: 'csv'; }#} — relationalCSVData. */
        record RelationalCsv(List<CsvTable> tables,
                             com.legend.protocol.SourceInfo sourceInformation)
                implements PTestPayload {
            public RelationalCsv {
                tables = List.copyOf(tables);
            }
        }

        /** One CSV table: schema, table, concatenated values; span =
         *  {@code schema.table:}..values end (the ';' excluded). */
        record CsvTable(String schema, String table, String values,
                        com.legend.protocol.SourceInfo sourceInformation) {
        }
    }

    /** One {@code functionTest}: {@code id | call(args) => expected;} — span includes the
     *  semicolon. */
    public record PFunctionTest(String id,
                                com.legend.protocol.SourceInfo sourceInformation,
                                List<PTestParam> parameters,
                                PAssertion assertion) {
        public PFunctionTest {
            parameters = List.copyOf(parameters);
        }
    }

    /** The test's single assertion, id always {@code "default"} on the wire. */
    public sealed interface PAssertion {
        /** {@code => expr} — equalTo spanning the expected value. */
        record EqualTo(com.legend.protocol.spec.ValueSpecification expected,
                       com.legend.protocol.SourceInfo span) implements PAssertion {
        }

        /** {@code => (JSON) '...'} — equalToJson with an externalFormat expected. */
        record EqualToJson(PTestPayload.ExternalFormat expected,
                           com.legend.protocol.SourceInfo span) implements PAssertion {
        }

        /** {@code => Relation #{...}#} — equalToRelation; the expected object is the bare
         *  columns/paths/rows shape (no _type), spanning the island CONTENT; the
         *  assertion spans {@code Relation}..{@code }#}. */
        record EqualToRelation(PTestPayload.RelationElement expected,
                               com.legend.protocol.SourceInfo span) implements PAssertion {
        }
    }

    /** One test-call argument, keyed by the SIGNATURE parameter name at its position —
     *  {@code null} when the call passes more arguments than the signature declares
     *  (probe "pf extra test arg": the name key is simply absent). */
    public record PTestParam(@com.legend.Nullable String name,
                             com.legend.protocol.spec.ValueSpecification value,
                             com.legend.protocol.SourceInfo sourceInformation) {
    }

    /**
     * {@code _type:"function"} — the wire NAME is SIGNATURE-MANGLED
     * ({@code f_Integer_1__String_MANY__Integer_1_}, verified via ProbeWireShapes
     * "function mangling"): simple type names (packages stripped, generic arguments
     * dropped), multiplicities as {@code 1}/{@code MANY}/{@code $0_1$}/{@code $1_MANY$},
     * parameters joined by {@code __}, return appended with a trailing underscore.
     */
    public record PFunction(String pkg, String name,
                            List<String> typeParams, List<String> multParams,
                            List<com.legend.protocol.ParameterDefinition> parameters,
                            com.legend.protocol.TypeExpression returnType,
                            com.legend.protocol.Multiplicity returnMultiplicity,
                            List<com.legend.protocol.spec.ValueSpecification> body,
                            List<com.legend.protocol.ConstraintDefinition> preConstraints,
                            List<PTestSuite> testSuites,
                            List<PStereotype> stereotypes,
                            List<PTaggedValue> taggedValues,
                            com.legend.protocol.SourceInfo sourceInformation) implements Element {
        public PFunction {
            typeParams = List.copyOf(typeParams);
            multParams = List.copyOf(multParams);
            parameters = List.copyOf(parameters);
            body = List.copyOf(body);
            preConstraints = List.copyOf(preConstraints);
            testSuites = List.copyOf(testSuites);
            stereotypes = List.copyOf(stereotypes);
            taggedValues = List.copyOf(taggedValues);
        }

        /** The UNmangled FQN — legend-lite's key. */
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }

        /** The wire name. Throws (loudly) on shapes the mangle rules do not cover yet.
         *  The return segment joins with {@code __} after parameters, {@code _} when there
         *  are none: {@code f_Integer_1__String_MANY__Integer_1_}, {@code h__Boolean_1_}. */
        public String mangledName() {
            StringBuilder m = new StringBuilder(name).append('_');
            for (int i = 0; i < parameters.size(); i++) {
                if (i > 0) {
                    m.append("__");
                }
                com.legend.protocol.ParameterDefinition pd = parameters.get(i);
                m.append(mangleType(pd.type())).append('_').append(mangleMult(pd.multiplicity()));
            }
            m.append(parameters.isEmpty() ? "_" : "__")
                    .append(mangleType(returnType)).append('_')
                    .append(mangleMult(returnMultiplicity)).append('_');
            return m.toString();
        }

        private static String mangleType(com.legend.protocol.TypeExpression t) {
            String full = switch (t) {
                case com.legend.protocol.TypeExpression.NameRef n -> n.name();
                case com.legend.protocol.TypeExpression.Generic g -> g.name();
                default -> throw new UnsupportedOperationException(
                        "no mangle rule for parameter type " + t.getClass().getSimpleName());
            };
            int i = full.lastIndexOf("::");
            return i < 0 ? full : full.substring(i + 2);
        }

        private static String mangleMult(com.legend.protocol.Multiplicity mult) {
            if (!(mult instanceof com.legend.protocol.Multiplicity.Concrete c)) {
                throw new UnsupportedOperationException("no mangle rule for a multiplicity parameter");
            }
            String lo = String.valueOf(c.lowerBound());
            if (c.upperBound() == null) {
                return c.lowerBound() == 0 ? "MANY" : "$" + lo + "_MANY$";
            }
            if (c.upperBound().intValue() == c.lowerBound()) {
                return lo;
            }
            return "$" + lo + "_" + c.upperBound() + "$";
        }
    }

    /** {@code _type:"association"} — ends are ordinary wire properties; qualified properties
     *  ride along exactly as on classes. */
    public record PAssociation(String pkg, String name,
                               List<PProperty> properties,
                               List<com.legend.protocol.DerivedPropertyDefinition> derivedProperties,
                               List<PStereotype> stereotypes,
                               List<PTaggedValue> taggedValues,
                               com.legend.protocol.SourceInfo sourceInformation) implements Element {
        public PAssociation {
            properties = List.copyOf(properties);
            derivedProperties = List.copyOf(derivedProperties);
            stereotypes = List.copyOf(stereotypes);
            taggedValues = List.copyOf(taggedValues);
        }

        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** {@code _type:"profile"} — stereotype/tag declarations as bare name+span entries. */
    public record PProfile(String pkg, String name,
                           List<PProfileEntry> stereotypes,
                           List<PProfileEntry> tags,
                           com.legend.protocol.SourceInfo sourceInformation) implements Element {
        public PProfile {
            stereotypes = List.copyOf(stereotypes);
            tags = List.copyOf(tags);
        }

        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** One declared stereotype or tag: {@code {"sourceInformation":…,"value":…}} — the span
     *  covers the name token only. */
    public record PProfileEntry(String value, com.legend.protocol.SourceInfo sourceInformation) {
    }

    /**
     * {@code _type:"Enumeration"} — CAPITALIZED on the wire, unlike class/profile/association
     * (verified via ProbeWireShapes; an engine quirk, reproduced not questioned).
     */
    public record PEnumeration(String pkg, String name,
                               List<PEnumValue> values,
                               List<PStereotype> stereotypes,
                               List<PTaggedValue> taggedValues,
                               com.legend.protocol.SourceInfo sourceInformation) implements Element {
        public PEnumeration {
            values = List.copyOf(values);
            stereotypes = List.copyOf(stereotypes);
            taggedValues = List.copyOf(taggedValues);
        }

        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** One enum value: annotations plus the entry's span (annotations..name, comma excluded). */
    public record PEnumValue(String value,
                             List<PStereotype> stereotypes,
                             List<PTaggedValue> taggedValues,
                             com.legend.protocol.SourceInfo sourceInformation) {
        public PEnumValue {
            stereotypes = List.copyOf(stereotypes);
            taggedValues = List.copyOf(taggedValues);
        }
    }

    /**
     * {@code _type:"class"} — <b>the parser's output for a {@code Class} declaration</b>.
     *
     * <p>Carries everything the parse produced, which is a superset of what goes on the wire.
     * {@code typeParams} and {@code isNative} have no protocol equivalent and
     * {@link ProtocolEmitter} simply does not emit them: <b>these are our records; the wire shape
     * is the emitter's decision, not the record's.</b> That is what lets the parser have exactly
     * one output while still round-tripping losslessly (via {@code com.legend.model.FromProtocol})
     * into the model for our own compiler.
     */
    public record PClass(String pkg, String name,
                         List<String> typeParams,
                         /** {@code Class X(x:Integer[1])} — TYPE VARIABLES (legend-pure new.pure,
                          * cast.pure, precisePrimitives.pure; m3 Class.typeVariables): declared
                          * values a constraint or derived body may name as {@code $x} */
                         List<com.legend.protocol.ParameterDefinition> typeVariables,
                         List<PSuperType> superTypes,
                         List<PProperty> properties,
                         List<com.legend.protocol.DerivedPropertyDefinition> derivedProperties,
                         List<com.legend.protocol.ConstraintDefinition> constraints,
                         List<PStereotype> stereotypes,
                         List<PTaggedValue> taggedValues,
                         boolean isNative,
                         com.legend.protocol.SourceInfo sourceInformation) implements Element {
        /** The pre-type-variable arity: no type variables. */
        public PClass(String pkg, String name, List<String> typeParams,
                      List<PSuperType> superTypes, List<PProperty> properties,
                      List<com.legend.protocol.DerivedPropertyDefinition> derivedProperties,
                      List<com.legend.protocol.ConstraintDefinition> constraints,
                      List<PStereotype> stereotypes, List<PTaggedValue> taggedValues,
                      boolean isNative, com.legend.protocol.SourceInfo sourceInformation) {
            this(pkg, name, typeParams, List.of(), superTypes, properties, derivedProperties,
                    constraints, stereotypes, taggedValues, isNative, sourceInformation);
        }

        public PClass {
            typeParams = List.copyOf(typeParams);
            typeVariables = List.copyOf(typeVariables);
            superTypes = List.copyOf(superTypes);
            properties = List.copyOf(properties);
            derivedProperties = List.copyOf(derivedProperties);
            constraints = List.copyOf(constraints);
            stereotypes = List.copyOf(stereotypes);
            taggedValues = List.copyOf(taggedValues);
        }

        /** The wire's {@code package} + {@code name} recombined — legend-lite keys by FQN, always. */
        public String qualifiedName() {
            return pkg.isEmpty() ? name : pkg + "::" + name;
        }
    }

    /** {@code _type:"sectionIndex"} — synthesised, and always emitted last. */
    public record PSectionIndex(String pkg, String name, List<PSection> sections) implements Element {
        public PSectionIndex {
            sections = List.copyOf(sections);
        }
    }

    /** One sectionIndex entry (ZPmcdProbe): {@code _type:"importAware"}
     *  for import-supporting parsers (imports emitted), {@code "default"}
     *  otherwise (NO imports key). Spans are the engine's BUFFER
     *  coordinates over {@code "\n###Pure\n" + source} with the
     *  newline-inclusive SECTION_START token (PureGrammarParser.parse). */
    public record PSection(boolean importAware, String parserName,
                           List<String> elements, List<String> imports,
                           com.legend.protocol.SourceInfo sourceInformation) {
        public PSection {
            // NOT List.copyOf: the BigQuery deployment-config walker
            // registers a literal null element entry (probe 2026-08-14)
            elements = java.util.Collections.unmodifiableList(
                    new java.util.ArrayList<>(elements));
            imports = List.copyOf(imports);
        }
    }

    /**
     * A simple (non-derived) property, carrying the <b>parse product</b> — the type expression and
     * multiplicity exactly as parsed — plus the property's own span. The type's span lives on the
     * type node itself ({@code TypeExpression.NameRef#pos()} / {@code Generic#pos()}), threaded by
     * {@code parseType}, so nested type arguments carry their own spans uniformly.
     *
     * <p>It deliberately does <em>not</em> pre-flatten the type into the wire's
     * {@code genericType}/{@code rawType} shape. Doing that at parse time forced the parser to
     * reject type expressions it can parse perfectly well, and to invent a multiplicity when the
     * declaration used a parameter. <b>The parser stays total; the emitter owns what the wire can
     * express</b> and walls loudly on the rest.
     */
    public record PProperty(String name,
                            com.legend.protocol.TypeExpression type,
                            com.legend.protocol.Multiplicity multiplicity,
                            List<PStereotype> stereotypes,
                            List<PTaggedValue> taggedValues,
                            com.legend.protocol.SourceInfo sourceInformation,
                            @com.legend.Nullable PDefaultValue defaultValue,
                            @com.legend.Nullable String aggregation) {
        public PProperty {
            stereotypes = List.copyOf(stereotypes);
            taggedValues = List.copyOf(taggedValues);
        }

        /** The common no-aggregation-kind form. */
        public PProperty(String name, com.legend.protocol.TypeExpression type,
                         com.legend.protocol.Multiplicity multiplicity,
                         List<PStereotype> stereotypes, List<PTaggedValue> taggedValues,
                         com.legend.protocol.SourceInfo sourceInformation,
                         @com.legend.Nullable PDefaultValue defaultValue) {
            this(name, type, multiplicity, stereotypes, taggedValues,
                    sourceInformation, defaultValue, null);
        }
    }

    /**
     * A property's default value: {@code {"sourceInformation":…,"value":…}} on the wire, no
     * {@code _type}. The outer span covers the whole default expression.
     *
     * <p>{@code value} is {@code null} when the parser accepted the default expression but
     * could not build a value-spec tree for it — the parser stays total; {@link ProtocolEmitter}
     * walls loudly on the null rather than dropping the property or the build silently.
     */
    public record PDefaultValue(@com.legend.Nullable com.legend.protocol.spec.ValueSpecification value,
                                com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** Note: carries no {@code _type} on the wire. */
    public record PGenericType(PPackageableType rawType) {
    }

    /**
     * The wire's {@code StereotypePtr}:
     * {@code {"profile":…,"profileSourceInformation":…,"sourceInformation":…,"value":…}}.
     *
     * <p>Two spans: {@code profileSourceInformation} covers just the profile FQN,
     * {@code sourceInformation} covers the whole {@code a::P.s1}.
     */
    public record PStereotype(String profile, String value,
                              com.legend.protocol.SourceInfo profileSourceInformation,
                              com.legend.protocol.SourceInfo sourceInformation) {
    }

    /**
     * The wire's {@code TagPtr}. Same four fields as {@link PStereotype} — but note the
     * <b>asymmetry</b>: a tag's {@code sourceInformation} covers only the tag NAME, where a
     * stereotype's covers the whole {@code profile.name}. Verified against legend-engine, not
     * assumed.
     */
    public record PTag(String profile, String value,
                       com.legend.protocol.SourceInfo profileSourceInformation,
                       com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code {"sourceInformation":…,"tag":{…},"value":…}}. */
    /** A tagged value. {@code multiLine} is the engine's own carrier (4.145.0,
     *  {@code TaggedValue.value} became a {@code CString}): the value was
     *  authored as a {@code '''...'''} block — a documentation literal, or an
     *  explicit tagged value written as one. On the wire a flagged value is
     *  an object ({@code _type:string, multiLine:true, value}); an unflagged
     *  one stays the bare string it always was. */
    public record PTaggedValue(PTag tag, String value, boolean multiLine,
            com.legend.protocol.SourceInfo sourceInformation) {
    }

    /**
     * One entry of the wire's {@code superTypes}: {@code {"path":…,"sourceInformation":…,"type":"CLASS"}}.
     *
     * <p>Carries the parsed {@code TypeExpression} rather than a pre-flattened path, for the same
     * reason {@link PProperty} does — the parser stays total and the emitter owns what the wire can
     * express.
     */
    public record PSuperType(com.legend.protocol.TypeExpression type, com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** {@code _type:"packageableType"}. */
    public record PPackageableType(String fullPath, com.legend.protocol.SourceInfo sourceInformation) {
    }

    /** Carries no {@code _type} and no source information. */
    public record PMultiplicity(int lowerBound, @com.legend.Nullable Integer upperBound) {
    }

    /**
     * Splits an FQN into the wire's {@code package} / {@code name} pair.
     *
     * <p><b>This is the only place in legend-lite that splits an FQN</b>, and it exists solely to
     * satisfy the wire. {@code PackageableElement} deliberately has no {@code simpleName()} /
     * {@code packagePath()} because they invite {@code findClass(element.simpleName())} — the
     * simple-name collision documented in {@code docs/NAME_RESOLUTION_BUG.md}. Splitting on the way
     * OUT is safe; nothing reads these back as a lookup key.
     */
    public static String[] splitFqn(String qualifiedName) {
        // QUOTE-AWARE last separator: a::'b::c' is package "a", name "b::c"
        // (deep-audit 1b — the raw lastIndexOf split inside the quotes)
        int i = lastTopLevelSeparator(qualifiedName);
        String pkg = i < 0 ? "" : qualifiedName.substring(0, i);
        String name = i < 0 ? qualifiedName : qualifiedName.substring(i + 2);
        return new String[]{unquoteSegments(pkg), unquoteSegments(name)};
    }

    /** Index of the last {@code ::} OUTSIDE quoted segments, or -1. Quoted
     *  segments honour backslash escapes ({@code 'b\'c'}). */
    private static int lastTopLevelSeparator(String path) {
        boolean inQuote = false;
        int last = -1;
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (inQuote && c == '\\') {
                i++;
            } else if (c == '\'') {
                inQuote = !inQuote;
            } else if (!inQuote && c == ':' && i + 1 < path.length()
                    && path.charAt(i + 1) == ':') {
                last = i;
                i++;
            }
        }
        return last;
    }

    /** Public form for REFERENCE positions that also unquote (stereotype/tag
     *  profiles — corpus DIFF: {@code <<'a profile'.st>>} emits {@code a profile}). */
    public static String unquotePath(String path) {
        return unquoteSegments(path);
    }

    /** DECLARATION names unquote their quoted segments on the wire
     *  ({@code test::'p a c k'::A} &rarr; package {@code test::p a c k}); REFERENCES
     *  keep the quotes — the parser hands the raw spelling to both. */
    private static String unquoteSegments(String path) {
        if (path.indexOf('\'') < 0) {
            return path;
        }
        // segment boundaries are QUOTE-AWARE (a quoted segment may itself
        // contain '::'), and quoted segments UNESCAPE — 'b\'c' is b'c on
        // the wire (deep-audit 1b: the old raw indexOf split mangled both)
        StringBuilder out = new StringBuilder();
        boolean inQuote = false;
        int start = 0;
        for (int i = 0; i <= path.length(); i++) {
            boolean atSep = !inQuote && i + 1 < path.length()
                    && path.charAt(i) == ':' && path.charAt(i + 1) == ':';
            if (i == path.length() || atSep) {
                if (start > 0) {
                    out.append("::");
                }
                String s = path.substring(start, i);
                out.append(s.length() >= 2 && s.startsWith("'") && s.endsWith("'")
                        ? Escapes.unescapeJavaLike(s.substring(1, s.length() - 1)) : s);
                if (i == path.length()) {
                    break;
                }
                i++;                        // past the second ':'
                start = i + 1;
                continue;
            }
            char c = path.charAt(i);
            if (inQuote && c == '\\') {
                i++;
            } else if (c == '\'') {
                inQuote = !inQuote;
            }
        }
        return out.toString();
    }

}
