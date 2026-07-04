package com.legend.compiler;

import com.legend.parser.ImportScope;
import com.legend.parser.Multiplicity;
import com.legend.parser.ParsedModel;
import com.legend.parser.TypeExpression;
import com.legend.parser.element.AssociationDefinition;
import com.legend.parser.element.AssociationDefinition.AssociationEndDefinition;
import com.legend.parser.element.AssociationMapping;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.ClassDefinition.ConstraintDefinition;
import com.legend.parser.element.ClassDefinition.DerivedPropertyDefinition;
import com.legend.parser.element.ClassDefinition.PropertyDefinition;
import com.legend.parser.element.ClassMapping;
import com.legend.parser.element.AuthenticationSpec;
import com.legend.parser.element.ConnectionDefinition;
import com.legend.parser.element.ConnectionSpecification;
import com.legend.parser.element.DatabaseDefinition;
import com.legend.parser.element.DatabaseDefinition.FilterDefinition;
import com.legend.parser.element.DatabaseDefinition.JoinDefinition;
import com.legend.parser.element.FunctionDefinition;
import com.legend.parser.element.JoinChainElement;
import com.legend.parser.element.JoinType;
import com.legend.parser.element.LegacyMappingDefinition;
import com.legend.parser.element.MappingDefinition;
import com.legend.parser.element.MappingInclude;
import com.legend.parser.element.LegacyMappingDefinition.TableReference;
import com.legend.parser.element.NativeFunctionDefinition;
import com.legend.parser.element.PackageableElement;
import com.legend.parser.element.PropertyMapping;
import com.legend.parser.element.RelationalOperation;
import com.legend.parser.element.RuntimeDefinition;
import com.legend.parser.element.ServiceDefinition;
import com.legend.parser.element.StereotypeApplication;
import com.legend.parser.element.TaggedValue;
import com.legend.parser.element.EnumDefinition;
import com.legend.parser.element.EnumerationMapping;
import com.legend.parser.element.EnumerationMapping.EnumValueMapping;
import com.legend.parser.element.EnumerationMapping.SourceValue;
import com.legend.parser.element.ProfileDefinition;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.spec.CInteger;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.ColSpecArray;
import com.legend.parser.spec.EnumValue;
import com.legend.parser.spec.KeyExpression;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.NewInstance;
import com.legend.parser.spec.PackageableElementPtr;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.TypeAnnotation;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;
import com.legend.parser.element.FilterMapping;
import com.legend.parser.element.FilterPointer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link NameResolver}. Covers the core resolution rule
 * (wildcard / specific / FQN / ambiguity), reference-equality
 * preservation, and the structural walkers for every
 * {@link PackageableElement} kind, every
 * {@link com.legend.parser.spec.ValueSpecification} variant, and the
 * shared {@link TypeExpression}, {@link RelationalOperation},
 * {@link PropertyMapping} hierarchies.
 */
class NameResolverTest {

    // =================================================================
    // Fixtures
    // =================================================================

    private static final ImportScope WILDCARD_MODEL = new ImportScope.Builder()
            .add("model::*").build();
    private static final ImportScope SPECIFIC_PERSON = new ImportScope.Builder()
            .add("model::Person").build();
    private static final ImportScope WILDCARD_STORE = new ImportScope.Builder()
            .add("store::*").build();
    private static final ImportScope EMPTY = ImportScope.empty();

    private static final Set<String> FQNS = Set.of(
            "model::Person", "model::Firm", "model::Address", "model::List",
            "store::DB", "store::OtherDB",
            "doc::Documentation",
            "service::PersonService", "mapping::PersonMapping",
            "runtime::DefaultRuntime", "connection::DBConn");

    private static final Multiplicity ONE = Multiplicity.Concrete.PURE_ONE;
    private static final Multiplicity ZERO_ONE = Multiplicity.Concrete.ZERO_ONE;

    private static PackageableElement resolveOne(
            PackageableElement el, ImportScope imp, Set<String> fqns) {
        return NameResolver.resolve(new ParsedModel(List.of(el), imp), fqns)
                .elements().get(0);
    }

    private static PackageableElement resolveOne(PackageableElement el) {
        return resolveOne(el, WILDCARD_MODEL, FQNS);
    }

    private static TypeExpression nameRef(String n) {
        return new TypeExpression.NameRef(n);
    }

    private static ClassDefinition simpleClass(String fqn, List<TypeExpression> supers,
            List<PropertyDefinition> props) {
        return new ClassDefinition(fqn, List.of(), supers, props,
                List.of(), List.of(), List.of(), List.of(), false);
    }

    private static PropertyDefinition prop(String name, TypeExpression type) {
        return new PropertyDefinition(name, type, ONE, List.of(), List.of());
    }

    // =================================================================
    // Core resolution rule
    // =================================================================

    @Test
    void wildcardImportResolvesSimpleNameToFqn() {
        var cd = simpleClass("model::Sub", List.of(nameRef("Person")), List.of());
        var r = (ClassDefinition) resolveOne(cd);
        assertEquals(new TypeExpression.NameRef("model::Person"),
                r.superClasses().get(0));
    }

    @Test
    void specificImportResolvesSimpleNameToFqn() {
        var cd = simpleClass("model::Sub", List.of(nameRef("Person")), List.of());
        var r = (ClassDefinition) resolveOne(cd, SPECIFIC_PERSON, FQNS);
        assertEquals(new TypeExpression.NameRef("model::Person"),
                r.superClasses().get(0));
    }

    @Test
    void alreadyQualifiedNamePassesThrough() {
        var cd = simpleClass("model::Sub",
                List.of(nameRef("model::Firm")), List.of());
        var r = (ClassDefinition) resolveOne(cd);
        assertEquals(new TypeExpression.NameRef("model::Firm"),
                r.superClasses().get(0));
    }

    @Test
    void unknownNamePassesThroughForPrimitiveFallback() {
        // "Integer" is not in FQNS \u2014 stays as-is (next layer will recognise
        // it as a primitive).
        var cd = simpleClass("model::Sub", List.of(),
                List.of(prop("age", nameRef("Integer"))));
        var r = (ClassDefinition) resolveOne(cd);
        assertEquals(new TypeExpression.NameRef("Integer"),
                r.properties().get(0).type());
    }

    @Test
    void ambiguousReferenceThrows() {
        ImportScope two = new ImportScope.Builder()
                .add("model::*").add("other::*").build();
        Set<String> ambig = Set.of("model::Person", "other::Person");
        var cd = simpleClass("x::Sub", List.of(nameRef("Person")), List.of());
        assertThrows(IllegalStateException.class,
                () -> resolveOne(cd, two, ambig));
    }

    @Test
    void noImportNoChangeReturnsSameInstance() {
        var cd = simpleClass("model::Sub",
                List.of(nameRef("model::Firm")), List.of());
        var model = new ParsedModel(List.of(cd), EMPTY);
        ParsedModel r = NameResolver.resolve(model, FQNS);
        assertSame(model, r, "all-FQN model should round-trip unchanged");
        assertSame(cd, r.elements().get(0));
    }

    @Test
    void emptyParsedModelReturnsSameInstance() {
        var m = new ParsedModel(List.of(), WILDCARD_MODEL);
        assertSame(m, NameResolver.resolve(m, FQNS));
    }

    // =================================================================
    // Class — full coverage
    // =================================================================

    @Test
    void classPropertyTypeResolved() {
        var cd = simpleClass("model::Sub", List.of(),
                List.of(prop("boss", nameRef("Person"))));
        var r = (ClassDefinition) resolveOne(cd);
        assertEquals("model::Person",
                ((TypeExpression.NameRef) r.properties().get(0).type()).name());
    }

    @Test
    void classGenericTypeArgsResolved() {
        // List<Person> \u2014 the wrapper name `List` is in FQNS, and the
        // argument `Person` should also be canonicalised.
        var generic = new TypeExpression.Generic("List", List.of(nameRef("Person")));
        var cd = simpleClass("model::Sub", List.of(),
                List.of(prop("friends", generic)));
        var r = (ClassDefinition) resolveOne(cd);
        var g = (TypeExpression.Generic) r.properties().get(0).type();
        assertEquals("model::List", g.name());
        assertEquals("model::Person", ((TypeExpression.NameRef) g.arguments().get(0)).name());
    }

    @Test
    void classDerivedPropertyTypeAndBodyResolved() {
        // derive: |Person.all()->first()   returns Person[0..1]
        var body = new AppliedFunction("first", List.of(
                new AppliedFunction("all",
                        List.of(new PackageableElementPtr("Person")))));
        var dp = new DerivedPropertyDefinition(
                "first",
                List.of(),
                List.of(body),
                nameRef("Person"),
                ZERO_ONE);
        var cd = new ClassDefinition("model::Sub", List.of(), List.of(),
                List.of(), List.of(dp), List.of(), List.of(), List.of(), false);
        var r = (ClassDefinition) resolveOne(cd);
        var rdp = r.derivedProperties().get(0);
        assertEquals("model::Person", ((TypeExpression.NameRef) rdp.type()).name());
        var rbody = (AppliedFunction) rdp.expression().get(0);
        var rinner = (AppliedFunction) rbody.parameters().get(0);
        assertEquals("model::Person",
                ((PackageableElementPtr) rinner.parameters().get(0)).fullPath());
    }

    @Test
    void classConstraintExpressionResolved() {
        // constraint: $this->instanceOf(Person)
        var expr = new AppliedFunction("instanceOf", List.of(
                new Variable("this", null, ONE),
                new PackageableElementPtr("Person")));
        var c = new ConstraintDefinition("ofPerson", expr);
        var cd = new ClassDefinition("model::Sub", List.of(), List.of(),
                List.of(), List.of(), List.of(c), List.of(), List.of(), false);
        var r = (ClassDefinition) resolveOne(cd);
        var raf = (AppliedFunction) r.constraints().get(0).expression();
        assertEquals("model::Person",
                ((PackageableElementPtr) raf.parameters().get(1)).fullPath());
    }

    @Test
    void classStereotypeProfileNameResolved() {
        ImportScope imp = new ImportScope.Builder().add("doc::*").build();
        var cd = new ClassDefinition("model::Sub", List.of(), List.of(),
                List.of(), List.of(), List.of(),
                List.of(new StereotypeApplication("Documentation", "deprecated")),
                List.of(), false);
        var r = (ClassDefinition) resolveOne(cd, imp, FQNS);
        assertEquals("doc::Documentation", r.stereotypes().get(0).profileName());
    }

    @Test
    void classTaggedValueProfileNameResolved() {
        ImportScope imp = new ImportScope.Builder().add("doc::*").build();
        var cd = new ClassDefinition("model::Sub", List.of(), List.of(),
                List.of(), List.of(), List.of(),
                List.of(),
                List.of(new TaggedValue("Documentation", "doc", "see also")),
                false);
        var r = (ClassDefinition) resolveOne(cd, imp, FQNS);
        assertEquals("doc::Documentation", r.taggedValues().get(0).profileName());
    }

    @Test
    void classWithFullyResolvedTypesReturnsSameInstance() {
        var cd = simpleClass("model::Sub",
                List.of(new TypeExpression.NameRef("model::Firm")),
                List.of(prop("p", new TypeExpression.NameRef("model::Person"))));
        var r = resolveOne(cd);
        assertSame(cd, r,
                "fully-FQN class should round-trip through resolveClass unchanged");
    }

    // =================================================================
    // Association
    // =================================================================

    @Test
    void associationBothEndsResolved() {
        var p1 = new AssociationEndDefinition("worksFor", nameRef("Firm"), ONE);
        var p2 = new AssociationEndDefinition("employees", nameRef("Person"),
                Multiplicity.Concrete.PURE_MANY);
        var ad = new AssociationDefinition("model::Employment", p1, p2);
        var r = (AssociationDefinition) resolveOne(ad);
        assertEquals("model::Firm", ((TypeExpression.NameRef) r.property1().targetClass()).name());
        assertEquals("model::Person", ((TypeExpression.NameRef) r.property2().targetClass()).name());
    }

    // =================================================================
    // Function / NativeFunction
    // =================================================================

    @Test
    void functionParamAndReturnTypeAndBodyResolved() {
        var param = new FunctionDefinition.ParameterDefinition(
                "p", nameRef("Person"), ONE);
        var body = new AppliedFunction("all",
                List.of(new PackageableElementPtr("Firm")));
        var fd = new FunctionDefinition("model::myFn", List.of(), List.of(),
                List.of(param), nameRef("Person"), ZERO_ONE,
                List.of(body), List.of(), List.of());
        var r = (FunctionDefinition) resolveOne(fd);
        assertEquals("model::Person",
                ((TypeExpression.NameRef) r.parameters().get(0).type()).name());
        assertEquals("model::Person", ((TypeExpression.NameRef) r.returnType()).name());
        var rbody = (AppliedFunction) r.body().get(0);
        assertEquals("model::Firm",
                ((PackageableElementPtr) rbody.parameters().get(0)).fullPath());
    }

    @Test
    void nativeFunctionParamAndReturnTypeResolved() {
        var param = new FunctionDefinition.ParameterDefinition(
                "p", nameRef("Person"), ONE);
        var nfd = new NativeFunctionDefinition("model::nat", List.of(), List.of(),
                List.of(param), nameRef("Firm"), ONE, List.of(), List.of());
        var r = (NativeFunctionDefinition) resolveOne(nfd);
        assertEquals("model::Person",
                ((TypeExpression.NameRef) r.parameters().get(0).type()).name());
        assertEquals("model::Firm", ((TypeExpression.NameRef) r.returnType()).name());
    }

    // =================================================================
    // ValueSpecification (via the value-spec entry point)
    // =================================================================

    @Test
    void valueSpecPackageableElementPtrResolved() {
        ValueSpecification vs = new PackageableElementPtr("Person");
        var r = (PackageableElementPtr) NameResolver.resolve(vs, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertEquals("model::Person", r.fullPath());
    }

    @Test
    void valueSpecEnumValueClassPathResolved() {
        ValueSpecification vs = new EnumValue("Person", "ACTIVE");
        var r = (EnumValue) NameResolver.resolve(vs, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertEquals("model::Person", r.fullPath());
        assertEquals("ACTIVE", r.value());
    }

    @Test
    void valueSpecNewInstanceClassNameAndTypeArgsResolved() {
        var ni = new NewInstance("Person",
                List.of(nameRef("Firm")),
                Map.of("name", new KeyExpression(new CString("alice"), false)));
        var r = (NewInstance) NameResolver.resolve(ni, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertEquals("model::Person", r.className());
        assertEquals("model::Firm",
                ((TypeExpression.NameRef) r.typeArguments().get(0)).name());
    }

    @Test
    void valueSpecLambdaParameterTypeResolved() {
        var lam = new LambdaFunction(
                List.of(new Variable("x", nameRef("Person"), ONE)),
                List.of(new Variable("x", null, ONE)));
        var r = (LambdaFunction) NameResolver.resolve(lam, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertEquals("model::Person",
                ((TypeExpression.NameRef) r.parameters().get(0).type()).name());
    }

    @Test
    void valueSpecTableReferenceDbArgResolved() {
        // tableReference(PackageableElementPtr('DB'), CString('Schema.Table'))
        // \u2014 the db arg is a typed FQN reference, resolved generically
        // through the PackageableElementPtr arm of resolveVs. No
        // tableReference-specific code path in the resolver.
        var af = new AppliedFunction("tableReference",
                List.of(new PackageableElementPtr("DB"),
                        new CString("Schema.Table")));
        var r = (AppliedFunction) NameResolver.resolve(af, NameResolver.Scope.of(WILDCARD_STORE, FQNS));
        assertEquals("store::DB",
                ((PackageableElementPtr) r.parameters().get(0)).fullPath());
        assertEquals("Schema.Table",
                ((CString) r.parameters().get(1)).value(),
                "table-name CString is opaque, untouched");
    }

    // =================================================================
    // Mapping
    // =================================================================

    @Test
    void mappingClassMappingRelationalClassNameAndDbResolved() {
        var rel = new ClassMapping.Relational(
                "Person", "p_set", null, true,
                new TableReference("DB", "PersonTbl"),
                null, false, List.of(), List.of(), List.of(),
                /* sourceUrl */ null);
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(rel), List.of(), List.of(), null);
        ImportScope both = new ImportScope.Builder()
                .add("model::*").add("store::*").build();
        var r = (LegacyMappingDefinition) resolveOne(md, both, FQNS);
        var rrel = (ClassMapping.Relational) r.classMappings().get(0);
        assertEquals("model::Person", rrel.className());
        assertEquals("store::DB", rrel.mainTable().database());
    }

    @Test
    void mappingClassMappingPureClassNameAndSourceClassResolved() {
        var pure = new ClassMapping.Pure("Person", "p_set", null, true,
                "Firm", null, List.of());
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(pure), List.of(), List.of(), null);
        var r = (LegacyMappingDefinition) resolveOne(md);
        var rp = (ClassMapping.Pure) r.classMappings().get(0);
        assertEquals("model::Person", rp.className());
        assertEquals("model::Firm", rp.sourceClass());
    }

    @Test
    void mappingIncludePathResolved() {
        var inc = new MappingInclude("PersonMapping", List.of());
        ImportScope imp = new ImportScope.Builder().add("mapping::*").build();
        var md = new LegacyMappingDefinition("mapping::M", List.of(inc),
                List.of(), List.of(), List.of(), null);
        var r = (LegacyMappingDefinition) resolveOne(md, imp, FQNS);
        assertEquals("mapping::PersonMapping", r.includes().get(0).mappingPath());
    }

    @Test
    void mappingAssociationMappingNameResolved() {
        ImportScope imp = new ImportScope.Builder().add("model::*").build();
        Set<String> fqns = Set.of("model::Employment");
        var am = new AssociationMapping.Relational("Employment", List.of());
        var md = new LegacyMappingDefinition("mapping::M", List.of(), List.of(),
                List.of(am), List.of(), null);
        var r = (LegacyMappingDefinition) resolveOne(md, imp, fqns);
        var ram = (AssociationMapping.Relational) r.associationMappings().get(0);
        assertEquals("model::Employment", ram.associationName());
    }

    // =================================================================
    // PropertyMapping variants
    // =================================================================

    @Test
    void propertyMappingColumnDbResolved() {
        var col = new PropertyMapping.Column("name", "DB", "PersonTbl", "name_col");
        var rel = new ClassMapping.Relational(
                "model::Person", "p_set", null, true,
                new TableReference("store::DB", "PersonTbl"),
                null, false, List.of(), List.of(), List.of(col),
                /* sourceUrl */ null);
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(rel), List.of(), List.of(), null);
        var r = (LegacyMappingDefinition) resolveOne(md, WILDCARD_STORE, FQNS);
        var rrel = (ClassMapping.Relational) r.classMappings().get(0);
        var rcol = (PropertyMapping.Column) rrel.propertyMappings().get(0);
        assertEquals("store::DB", rcol.database());
    }

    @Test
    void propertyMappingLocalPropertyTypeResolved() {
        var lp = new PropertyMapping.LocalProperty(
                "x", nameRef("Person"), ONE,
                new PropertyMapping.Column("x", "DB", "T", "c"));
        var rel = new ClassMapping.Relational(
                "model::Sub", "s_set", null, true,
                new TableReference("store::DB", "T"),
                null, false, List.of(), List.of(), List.of(lp),
                /* sourceUrl */ null);
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(rel), List.of(), List.of(), null);
        ImportScope both = new ImportScope.Builder()
                .add("model::*").add("store::*").build();
        var r = (LegacyMappingDefinition) resolveOne(md, both, FQNS);
        var rrel = (ClassMapping.Relational) r.classMappings().get(0);
        var rlp = (PropertyMapping.LocalProperty) rrel.propertyMappings().get(0);
        assertEquals("model::Person",
                ((TypeExpression.NameRef) rlp.type()).name());
        assertEquals("store::DB",
                ((PropertyMapping.Column) rlp.body()).database());
    }

    // =================================================================
    // Database
    // =================================================================

    @Test
    void databaseIncludesResolved() {
        var db = new DatabaseDefinition("store::Main",
                List.of("OtherDB"),
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of());
        var r = (DatabaseDefinition) resolveOne(db, WILDCARD_STORE, FQNS);
        assertEquals(List.of("store::OtherDB"), r.includes());
    }

    @Test
    void databaseJoinOperationColumnRefDbResolved() {
        // A simple join: A.id = B.fk
        var cmp = new RelationalOperation.Comparison(
                new RelationalOperation.ColumnRef("DB", "A", "id"),
                com.legend.parser.element.ComparisonOp.EQ,
                new RelationalOperation.ColumnRef("DB", "B", "fk"));
        var join = new JoinDefinition("AB", cmp);
        var db = new DatabaseDefinition("store::Main",
                List.of(), List.of(), List.of(), List.of(), List.of(join),
                List.of(), List.of());
        var r = (DatabaseDefinition) resolveOne(db, WILDCARD_STORE, FQNS);
        var rop = (RelationalOperation.Comparison) r.joins().get(0).operation();
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef) rop.left()).databaseName());
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef) rop.right()).databaseName());
    }

    @Test
    void databaseFilterConditionResolved() {
        var cond = new RelationalOperation.IsNotNull(
                new RelationalOperation.ColumnRef("DB", "A", "id"));
        var filter = new FilterDefinition("ActiveOnly", cond);
        var db = new DatabaseDefinition("store::Main",
                List.of(), List.of(), List.of(), List.of(), List.of(),
                List.of(filter), List.of());
        var r = (DatabaseDefinition) resolveOne(db, WILDCARD_STORE, FQNS);
        var rcond = (RelationalOperation.IsNotNull) r.filters().get(0).condition();
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef) rcond.operand()).databaseName());
    }

    @Test
    void relationalOpJoinNavigationDbResolved() {
        // db.[joinA] | resultColumn
        var chain = List.of(
                new JoinChainElement("joinA", JoinType.INNER, "DB", false));
        var nav = new RelationalOperation.JoinNavigation("DB", chain,
                new RelationalOperation.TargetColumnRef("result"));
        var filter = new FilterDefinition("nav", nav);
        var db = new DatabaseDefinition("store::Main",
                List.of(), List.of(), List.of(), List.of(), List.of(),
                List.of(filter), List.of());
        var r = (DatabaseDefinition) resolveOne(db, WILDCARD_STORE, FQNS);
        var rnav = (RelationalOperation.JoinNavigation) r.filters().get(0).condition();
        assertEquals("store::DB", rnav.databaseName());
        assertEquals("store::DB", rnav.chain().get(0).databaseName());
    }

    // =================================================================
    // Runtime / Service / Connection
    // =================================================================

    @Test
    void runtimeMappingsAndConnectionBindingsResolved() {
        ImportScope imp = new ImportScope.Builder()
                .add("mapping::*").add("store::*").add("connection::*").build();
        var rd = new RuntimeDefinition("runtime::R",
                List.of("PersonMapping"),
                Map.of("DB", "DBConn"),
                List.of());
        var r = (RuntimeDefinition) resolveOne(rd, imp, FQNS);
        assertEquals(List.of("mapping::PersonMapping"), r.mappings());
        assertEquals(Map.of("store::DB", "connection::DBConn"), r.connectionBindings());
    }

    @Test
    void serviceMappingAndRuntimeRefsAndBodyResolved() {
        ImportScope imp = new ImportScope.Builder()
                .add("mapping::*").add("runtime::*").add("model::*").build();
        var body = new AppliedFunction("all",
                List.of(new PackageableElementPtr("Person")));
        var sd = new ServiceDefinition("service::S", "/api/x", body,
                "doc", "PersonMapping", "DefaultRuntime", null);
        var r = (ServiceDefinition) resolveOne(sd, imp, FQNS);
        assertEquals("mapping::PersonMapping", r.mappingRef());
        assertEquals("runtime::DefaultRuntime", r.runtimeRef());
        var rbody = (AppliedFunction) r.functionBody();
        assertEquals("model::Person",
                ((PackageableElementPtr) rbody.parameters().get(0)).fullPath());
    }

    @Test
    void connectionStoreNameResolved() {
        var cd = new ConnectionDefinition("connection::C", "DB",
                ConnectionDefinition.DatabaseType.H2,
                new ConnectionSpecification.InMemory(),
                new AuthenticationSpec.NoAuth());
        var r = (ConnectionDefinition) resolveOne(cd, WILDCARD_STORE, FQNS);
        assertEquals("store::DB", r.storeName());
    }

    // =================================================================
    // TypeExpression structural variants (through Function paramaters)
    // =================================================================

    @Test
    void typeExprFunctionTypeRecursivelyResolved() {
        // FunctionType: (Person[1]) -> Firm[1]
        var ft = new TypeExpression.FunctionType(
                List.of(new TypeExpression.TypedParameter(nameRef("Person"), ONE)),
                new TypeExpression.TypedParameter(nameRef("Firm"), ONE));
        var param = new FunctionDefinition.ParameterDefinition("f", ft, ONE);
        var fd = new FunctionDefinition("model::g", List.of(), List.of(),
                List.of(param), nameRef("Person"), ONE, List.of(), List.of(), List.of());
        var r = (FunctionDefinition) resolveOne(fd);
        var rft = (TypeExpression.FunctionType) r.parameters().get(0).type();
        assertEquals("model::Person",
                ((TypeExpression.NameRef) rft.parameters().get(0).type()).name());
        assertEquals("model::Firm",
                ((TypeExpression.NameRef) rft.result().type()).name());
    }

    @Test
    void typeExprRelationTypeColumnsResolved() {
        // Relation<(name: Person[1])>
        var rt = new TypeExpression.RelationType(List.of(
                new TypeExpression.Column("name", nameRef("Person"), ONE)));
        var param = new FunctionDefinition.ParameterDefinition("r", rt, ONE);
        var fd = new FunctionDefinition("model::g", List.of(), List.of(),
                List.of(param), nameRef("Person"), ONE, List.of(), List.of(), List.of());
        var r = (FunctionDefinition) resolveOne(fd);
        var rrt = (TypeExpression.RelationType) r.parameters().get(0).type();
        assertEquals("model::Person",
                ((TypeExpression.NameRef) rrt.columns().get(0).type()).name());
    }

    @Test
    void typeExprSchemaAlgebraRecursivelyResolved() {
        var sa = new TypeExpression.SchemaAlgebra(
                nameRef("Person"), TypeExpression.Op.UNION, nameRef("Firm"));
        var param = new FunctionDefinition.ParameterDefinition("s", sa, ONE);
        var fd = new FunctionDefinition("model::g", List.of(), List.of(),
                List.of(param), nameRef("Person"), ONE, List.of(), List.of(), List.of());
        var r = (FunctionDefinition) resolveOne(fd);
        var rsa = (TypeExpression.SchemaAlgebra) r.parameters().get(0).type();
        assertEquals("model::Person", ((TypeExpression.NameRef) rsa.left()).name());
        assertEquals("model::Firm", ((TypeExpression.NameRef) rsa.right()).name());
        assertSame(sa.op(), rsa.op(), "Op must be preserved");
    }

    @Test
    void typeExprGenericWithEmptyArgsResolved() {
        var g = new TypeExpression.Generic("List", List.of());
        var cd = simpleClass("model::Sub", List.of(),
                List.of(prop("xs", g)));
        var r = (ClassDefinition) resolveOne(cd);
        var rg = (TypeExpression.Generic) r.properties().get(0).type();
        assertEquals("model::List", rg.name());
        assertEquals(0, rg.arguments().size());
    }

    @Test
    void typeExprDeeplyNestedGenericResolved() {
        // Generic("List", [Generic("Map", [Person, Address])])
        var inner = new TypeExpression.Generic("Map",
                List.of(nameRef("Person"), nameRef("Address")));
        var outer = new TypeExpression.Generic("List", List.of(inner));
        var cd = simpleClass("model::Sub", List.of(), List.of(prop("xs", outer)));
        var r = (ClassDefinition) resolveOne(cd);
        var rOuter = (TypeExpression.Generic) r.properties().get(0).type();
        assertEquals("model::List", rOuter.name());
        var rInner = (TypeExpression.Generic) rOuter.arguments().get(0);
        assertEquals("Map", rInner.name(), "Map not in FQNS, should pass through");
        assertEquals("model::Person",
                ((TypeExpression.NameRef) rInner.arguments().get(0)).name());
        assertEquals("model::Address",
                ((TypeExpression.NameRef) rInner.arguments().get(1)).name());
    }

    // =================================================================
    // PackageableElement pass-through (Enum, Profile)
    // =================================================================

    @Test
    void enumDefinitionPassesThrough() {
        var en = new EnumDefinition("model::Status", List.of("ACTIVE", "INACTIVE"));
        var r = resolveOne(en);
        assertSame(en, r, "EnumDefinition has no cross-element refs; same instance");
    }

    @Test
    void profileDefinitionPassesThrough() {
        var pf = new ProfileDefinition("doc::Documentation",
                List.of("deprecated"), List.of("doc"));
        var r = resolveOne(pf);
        assertSame(pf, r, "ProfileDefinition has no cross-element refs; same instance");
    }

    // =================================================================
    // Stereotype/TaggedValue — at property/function level + already-FQN no-op
    // =================================================================

    @Test
    void propertyStereotypeProfileResolved() {
        ImportScope imp = new ImportScope.Builder().add("doc::*").build();
        var p = new PropertyDefinition("x", nameRef("Integer"), ONE,
                List.of(new StereotypeApplication("Documentation", "deprecated")),
                List.of());
        var cd = simpleClass("model::Sub", List.of(), List.of(p));
        var r = (ClassDefinition) resolveOne(cd, imp, FQNS);
        assertEquals("doc::Documentation",
                r.properties().get(0).stereotypes().get(0).profileName());
    }

    @Test
    void functionStereotypeAndTaggedValueResolved() {
        ImportScope imp = new ImportScope.Builder()
                .add("doc::*").add("model::*").build();
        var fd = new FunctionDefinition("model::myFn", List.of(), List.of(),
                List.of(), nameRef("Person"), ONE, List.of(),
                List.of(new StereotypeApplication("Documentation", "stable")),
                List.of(new TaggedValue("Documentation", "since", "1.0")));
        var r = (FunctionDefinition) resolveOne(fd, imp, FQNS);
        assertEquals("doc::Documentation", r.stereotypes().get(0).profileName());
        assertEquals("doc::Documentation", r.taggedValues().get(0).profileName());
        assertEquals("stable", r.stereotypes().get(0).stereotypeName(),
                "non-resolved fields preserved");
        assertEquals("1.0", r.taggedValues().get(0).value());
    }

    @Test
    void alreadyFqnStereotypeIsNoOp() {
        var st = new StereotypeApplication("doc::Documentation", "deprecated");
        var cd = new ClassDefinition("model::Sub", List.of(), List.of(),
                List.of(), List.of(), List.of(), List.of(st), List.of(), false);
        var r = resolveOne(cd);
        assertSame(cd, r, "already-FQN stereotype should not trigger any allocation");
    }

    // =================================================================
    // ValueSpecification — remaining variants
    // =================================================================

    @Test
    void appliedPropertyReceiverResolved() {
        // Person.all()->first().name  \u2014 outer is AppliedProperty
        ValueSpecification recv = new AppliedFunction("first", List.of(
                new AppliedFunction("all",
                        List.of(new PackageableElementPtr("Person")))));
        ValueSpecification ap = new AppliedProperty(recv, "name");
        var r = (AppliedProperty) NameResolver.resolve(ap, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertEquals("name", r.property());
        var rRecv = (AppliedFunction) r.receiver();
        var rAll = (AppliedFunction) rRecv.parameters().get(0);
        assertEquals("model::Person",
                ((PackageableElementPtr) rAll.parameters().get(0)).fullPath());
    }

    @Test
    void pureCollectionElementsResolved() {
        var coll = new PureCollection(List.of(
                new PackageableElementPtr("Person"),
                new PackageableElementPtr("Firm"),
                new CInteger(42)));
        var r = (PureCollection) NameResolver.resolve(coll, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertEquals("model::Person",
                ((PackageableElementPtr) r.values().get(0)).fullPath());
        assertEquals("model::Firm",
                ((PackageableElementPtr) r.values().get(1)).fullPath());
        assertSame(coll.values().get(2), r.values().get(2),
                "leaf CInteger preserved by reference");
    }

    @Test
    void colSpecLambdaResolved() {
        // ~name: x | Person.all()
        var lam = new LambdaFunction(
                List.of(new Variable("x", null, ONE)),
                List.of(new AppliedFunction("all",
                        List.of(new PackageableElementPtr("Person")))));
        var cs = new ColSpec("name", lam, null);
        var r = (ColSpec) NameResolver.resolve(cs, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertEquals("name", r.name());
        var rbody = (AppliedFunction) r.function1().body().get(0);
        assertEquals("model::Person",
                ((PackageableElementPtr) rbody.parameters().get(0)).fullPath());
    }

    @Test
    void colSpecArrayMultipleResolved() {
        var l1 = new LambdaFunction(List.of(), List.of(new PackageableElementPtr("Person")));
        var l2 = new LambdaFunction(List.of(), List.of(new PackageableElementPtr("Firm")));
        var csa = new ColSpecArray(List.of(
                new ColSpec("a", l1, null),
                new ColSpec("b", l2, null)));
        var r = (ColSpecArray) NameResolver.resolve(csa, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertEquals("model::Person",
                ((PackageableElementPtr) r.colSpecs().get(0).function1().body().get(0))
                        .fullPath());
        assertEquals("model::Firm",
                ((PackageableElementPtr) r.colSpecs().get(1).function1().body().get(0))
                        .fullPath());
    }

    @Test
    void typeAnnotationNamedResolved() {
        var ta = new TypeAnnotation.Named(nameRef("Person"));
        var r = (TypeAnnotation.Named) NameResolver.resolve(ta, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertEquals("model::Person", ((TypeExpression.NameRef) r.type()).name());
    }

    @Test
    void typeAnnotationRelationShapeColumnsResolved() {
        var col = new TypeAnnotation.RelationShape.Column(
                "p", new TypeAnnotation.Named(nameRef("Person")), ONE);
        var shape = new TypeAnnotation.RelationShape(List.of(col));
        var r = (TypeAnnotation.RelationShape) NameResolver.resolve(shape, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        var rcol = r.columns().get(0);
        assertEquals("p", rcol.name(), "column name preserved");
        var named = (TypeAnnotation.Named) rcol.type();
        assertEquals("model::Person", ((TypeExpression.NameRef) named.type()).name());
    }

    @Test
    void typeAnnotationWildcardPassesThrough() {
        var wc = new TypeAnnotation.Wildcard();
        var r = NameResolver.resolve(wc, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertSame(wc, r);
    }

    @Test
    void leafLiteralPassesThrough() {
        ValueSpecification cstr = new CString("hello");
        ValueSpecification cint = new CInteger(42);
        assertSame(cstr, NameResolver.resolve(cstr, NameResolver.Scope.of(WILDCARD_MODEL, FQNS)));
        assertSame(cint, NameResolver.resolve(cint, NameResolver.Scope.of(WILDCARD_MODEL, FQNS)));
    }

    @Test
    void variableTypeAloneResolved() {
        ValueSpecification v = new Variable("x", nameRef("Person"), ONE);
        var r = (Variable) NameResolver.resolve(v, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertEquals("model::Person", ((TypeExpression.NameRef) r.type()).name());
        assertEquals("x", r.name());
        assertSame(ONE, r.multiplicity(), "multiplicity preserved by identity");
    }

    @Test
    void variableWithoutTypePassesThrough() {
        ValueSpecification v = new Variable("x", null, ONE);
        var r = NameResolver.resolve(v, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertSame(v, r, "null-type variable should round-trip unchanged");
    }

    // =================================================================
    // PropertyMapping — remaining 7 variants
    // =================================================================

    /** Helper: wrap a single PropertyMapping in a minimal Mapping element. */
    private static LegacyMappingDefinition mapping(PropertyMapping pm, String className) {
        var rel = new ClassMapping.Relational(
                className, "s_set", null, true,
                new TableReference("DB", "T"),
                null, false, List.of(), List.of(), List.of(pm),
                /* sourceUrl */ null);
        return new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(rel), List.of(), List.of(), null);
    }

    private static PropertyMapping firstPm(LegacyMappingDefinition md) {
        var rel = (ClassMapping.Relational) md.classMappings().get(0);
        return rel.propertyMappings().get(0);
    }

    @Test
    void propertyMappingEnumeratedColumnDbResolved() {
        var ec = new PropertyMapping.EnumeratedColumn("status", "myEnum",
                "DB", "T", "status_col");
        var md = mapping(ec, "model::Person");
        var r = (PropertyMapping.EnumeratedColumn) firstPm(
                (LegacyMappingDefinition) resolveOne(md, WILDCARD_STORE, FQNS));
        assertEquals("store::DB", r.database());
        assertEquals("myEnum", r.enumMappingId(), "enumMappingId is a set ID, not resolved");
        assertEquals("status_col", r.column());
    }

    @Test
    void propertyMappingJoinDbAndChainResolved() {
        var chain = List.of(
                new JoinChainElement("j1", JoinType.INNER, "DB", false),
                new JoinChainElement("j2", JoinType.LEFT_OUTER, "DB", false));
        var j = new PropertyMapping.Join("friend", "DB", chain);
        var md = mapping(j, "model::Person");
        var r = (PropertyMapping.Join) firstPm(
                (LegacyMappingDefinition) resolveOne(md, WILDCARD_STORE, FQNS));
        assertEquals("store::DB", r.database());
        assertEquals("store::DB", r.joins().get(0).databaseName());
        assertEquals("store::DB", r.joins().get(1).databaseName());
        assertSame(JoinType.INNER, r.joins().get(0).joinType(), "joinType preserved");
    }

    @Test
    void propertyMappingJoinTerminalColumnResolved() {
        var chain = List.of(new JoinChainElement("j1", JoinType.INNER, "DB", false));
        var term = new RelationalOperation.ColumnRef("DB", "T2", "id");
        var jtc = new PropertyMapping.JoinTerminalColumn("id", "DB", chain, term);
        var md = mapping(jtc, "model::Person");
        var r = (PropertyMapping.JoinTerminalColumn) firstPm(
                (LegacyMappingDefinition) resolveOne(md, WILDCARD_STORE, FQNS));
        assertEquals("store::DB", r.database());
        assertEquals("store::DB", r.joins().get(0).databaseName());
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef) r.terminalColumn()).databaseName());
    }

    @Test
    void propertyMappingExpressionRelationalOpResolved() {
        var expr = new PropertyMapping.Expression("x",
                new RelationalOperation.ColumnRef("DB", "T", "c"));
        var md = mapping(expr, "model::Person");
        var r = (PropertyMapping.Expression) firstPm(
                (LegacyMappingDefinition) resolveOne(md, WILDCARD_STORE, FQNS));
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef) r.expression()).databaseName());
    }

    @Test
    void propertyMappingEmbeddedRecursesIntoSubMappings() {
        var inner = new PropertyMapping.Column("street", "DB", "T", "street_col");
        var emb = new PropertyMapping.Embedded("addr", List.of(inner));
        var md = mapping(emb, "model::Person");
        var r = (PropertyMapping.Embedded) firstPm(
                (LegacyMappingDefinition) resolveOne(md, WILDCARD_STORE, FQNS));
        var rInner = (PropertyMapping.Column) r.propertyMappings().get(0);
        assertEquals("store::DB", rInner.database());
        assertEquals("addr", r.propertyName(), "parent property preserved");
    }

    @Test
    void propertyMappingInlineEmbeddedPassesThroughIdentity() {
        var ie = new PropertyMapping.InlineEmbedded("addr", "AddressMapping_set");
        var md = mapping(ie, "model::Person");
        var resolved = (LegacyMappingDefinition) resolveOne(md, WILDCARD_STORE, FQNS);
        var rie = (PropertyMapping.InlineEmbedded) firstPm(resolved);
        assertSame(ie, rie, "InlineEmbedded has no FQN refs; identity preserved");
    }

    @Test
    void propertyMappingOtherwiseEmbeddedResolved() {
        var inner = new PropertyMapping.Column("a", "DB", "T", "c");
        var fallback = new PropertyMapping.Column("a", "DB", "T2", "c2");
        var oe = new PropertyMapping.OtherwiseEmbedded("addr",
                List.of(inner), "default_set", fallback);
        var md = mapping(oe, "model::Person");
        var r = (PropertyMapping.OtherwiseEmbedded) firstPm(
                (LegacyMappingDefinition) resolveOne(md, WILDCARD_STORE, FQNS));
        assertEquals("store::DB",
                ((PropertyMapping.Column) r.embedded().get(0)).database());
        assertEquals("store::DB",
                ((PropertyMapping.Column) r.fallback()).database());
        assertEquals("default_set", r.fallbackSetId(), "fallbackSetId is a set ID, not resolved");
    }

    // =================================================================
    // RelationalOperation — remaining 7 variants
    // =================================================================

    /** Helper: wrap a RelationalOperation as a Database filter condition. */
    private static RelationalOperation resolveDbFilter(
            RelationalOperation op, ImportScope imp) {
        var db = new DatabaseDefinition("store::Main",
                List.of(), List.of(), List.of(), List.of(), List.of(),
                List.of(new FilterDefinition("F", op)),
                List.of());
        var r = (DatabaseDefinition) resolveOne(db, imp, FQNS);
        return r.filters().get(0).condition();
    }

    @Test
    void relOpTargetColumnRefPassesThrough() {
        var op = new RelationalOperation.TargetColumnRef("c");
        var r = resolveDbFilter(op, WILDCARD_STORE);
        assertSame(op, r);
    }

    @Test
    void relOpLiteralPassesThrough() {
        var op = new RelationalOperation.Literal(42L);
        var r = resolveDbFilter(op, WILDCARD_STORE);
        assertSame(op, r);
    }

    @Test
    void relOpFunctionCallArgsResolved() {
        // concat(DB.t.c1, DB.t.c2) \u2014 function name NOT a Pure FQN, args recurse
        var op = new RelationalOperation.FunctionCall("concat", List.of(
                new RelationalOperation.ColumnRef("DB", "t", "c1"),
                new RelationalOperation.ColumnRef("DB", "t", "c2")));
        var r = (RelationalOperation.FunctionCall) resolveDbFilter(op, WILDCARD_STORE);
        assertEquals("concat", r.name(), "function name (DB function) unchanged");
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef) r.args().get(0)).databaseName());
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef) r.args().get(1)).databaseName());
    }

    @Test
    void relOpBooleanOpRecursivelyResolved() {
        var op = new RelationalOperation.BooleanOp(
                new RelationalOperation.IsNotNull(
                        new RelationalOperation.ColumnRef("DB", "T", "a")),
                com.legend.parser.element.LogicalOp.AND,
                new RelationalOperation.IsNotNull(
                        new RelationalOperation.ColumnRef("DB", "T", "b")));
        var r = (RelationalOperation.BooleanOp) resolveDbFilter(op, WILDCARD_STORE);
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef)
                        ((RelationalOperation.IsNotNull) r.left()).operand()).databaseName());
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef)
                        ((RelationalOperation.IsNotNull) r.right()).operand()).databaseName());
        assertSame(com.legend.parser.element.LogicalOp.AND, r.op());
    }

    @Test
    void relOpIsNullOperandResolved() {
        var op = new RelationalOperation.IsNull(
                new RelationalOperation.ColumnRef("DB", "T", "c"));
        var r = (RelationalOperation.IsNull) resolveDbFilter(op, WILDCARD_STORE);
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef) r.operand()).databaseName());
    }

    @Test
    void relOpGroupInnerResolved() {
        var op = new RelationalOperation.Group(
                new RelationalOperation.ColumnRef("DB", "T", "c"));
        var r = (RelationalOperation.Group) resolveDbFilter(op, WILDCARD_STORE);
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef) r.inner()).databaseName());
    }

    @Test
    void relOpArrayLiteralElementsResolved() {
        var op = new RelationalOperation.ArrayLiteral(List.of(
                new RelationalOperation.ColumnRef("DB", "T", "a"),
                new RelationalOperation.Literal("lit")));
        var r = (RelationalOperation.ArrayLiteral) resolveDbFilter(op, WILDCARD_STORE);
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef) r.elements().get(0)).databaseName());
        assertSame(op.elements().get(1), r.elements().get(1),
                "literal element preserved by reference");
    }

    @Test
    void relOpColumnRefWithNullDbPassesThrough() {
        // ColumnRef with null db can appear pre-resolution; resolver tolerates.
        var op = new RelationalOperation.ColumnRef(null, "T", "c");
        var r = resolveDbFilter(op, WILDCARD_STORE);
        assertSame(op, r);
    }

    // =================================================================
    // FilterMapping & FilterPointer
    // =================================================================

    @Test
    void filterMappingDirectLocalPassesThrough() {
        var fm = new FilterMapping.Direct(new FilterPointer.Local("ActiveOnly"));
        // Wrap in a ClassMapping.Relational filter slot
        var rel = new ClassMapping.Relational(
                "model::Person", "p_set", null, true,
                new TableReference("store::DB", "T"),
                fm, false, List.of(), List.of(), List.of(),
                /* sourceUrl */ null);
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(rel), List.of(), List.of(), null);
        var r = (LegacyMappingDefinition) resolveOne(md, WILDCARD_STORE, FQNS);
        var rrel = (ClassMapping.Relational) r.classMappings().get(0);
        assertSame(fm, rrel.filter(), "Direct(Local) has no FQN refs; same instance");
    }

    @Test
    void filterMappingDirectCrossDbResolved() {
        var fm = new FilterMapping.Direct(new FilterPointer.Cross("DB", "ActiveOnly"));
        var rel = new ClassMapping.Relational(
                "model::Person", "p_set", null, true,
                new TableReference("store::DB", "T"),
                fm, false, List.of(), List.of(), List.of(),
                /* sourceUrl */ null);
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(rel), List.of(), List.of(), null);
        var r = (LegacyMappingDefinition) resolveOne(md, WILDCARD_STORE, FQNS);
        var rrel = (ClassMapping.Relational) r.classMappings().get(0);
        var rfm = (FilterMapping.Direct) rrel.filter();
        var cross = (FilterPointer.Cross) rfm.filter();
        assertEquals("store::DB", cross.db());
        assertEquals("ActiveOnly", cross.name());
    }

    @Test
    void filterMappingJoinMediatedFullyResolved() {
        var chain = List.of(new JoinChainElement("j1", JoinType.INNER, "DB", false));
        var fm = new FilterMapping.JoinMediated(
                "DB", chain, new FilterPointer.Cross("DB", "F"));
        var rel = new ClassMapping.Relational(
                "model::Person", "p_set", null, true,
                new TableReference("store::DB", "T"),
                fm, false, List.of(), List.of(), List.of(),
                /* sourceUrl */ null);
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(rel), List.of(), List.of(), null);
        var r = (LegacyMappingDefinition) resolveOne(md, WILDCARD_STORE, FQNS);
        var rrel = (ClassMapping.Relational) r.classMappings().get(0);
        var rfm = (FilterMapping.JoinMediated) rrel.filter();
        assertEquals("store::DB", rfm.sourceDb());
        assertEquals("store::DB", rfm.joins().get(0).databaseName());
        assertEquals("store::DB", ((FilterPointer.Cross) rfm.filter()).db());
    }

    // =================================================================
    // Mapping internals — include substitutions, enum mappings
    // =================================================================

    @Test
    void mappingIncludeStoreSubstitutionResolved() {
        var sub = new MappingInclude.StoreSubstitution(
                "DB", "OtherDB");
        var inc = new MappingInclude("PersonMapping", List.of(sub));
        ImportScope imp = new ImportScope.Builder()
                .add("mapping::*").add("store::*").build();
        var md = new LegacyMappingDefinition("mapping::M", List.of(inc),
                List.of(), List.of(), List.of(), null);
        var r = (LegacyMappingDefinition) resolveOne(md, imp, FQNS);
        var rinc = r.includes().get(0);
        assertEquals("mapping::PersonMapping", rinc.mappingPath());
        var rsub = rinc.substitutions().get(0);
        assertEquals("store::DB", rsub.originalStore());
        assertEquals("store::OtherDB", rsub.replacementStore());
    }

    // =================================================================
    // Null safety
    // =================================================================

    @Test
    void serviceWithNullMappingAndRuntimeRefsRoundTrips() {
        var body = new PackageableElementPtr("model::Person");
        var sd = new ServiceDefinition("service::S", "/p", body,
                "doc", null, null, null);
        var r = (ServiceDefinition) resolveOne(sd, WILDCARD_MODEL, FQNS);
        assertEquals(null, r.mappingRef());
        assertEquals(null, r.runtimeRef());
    }

    @Test
    void classMappingPureWithNullSourceClassAndFilter() {
        var pure = new ClassMapping.Pure("Person", "p_set", null, true,
                "Firm", null, List.of()); // sourceClass = Firm
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(pure), List.of(), List.of(), null);
        var r = (LegacyMappingDefinition) resolveOne(md);
        var rp = (ClassMapping.Pure) r.classMappings().get(0);
        assertEquals("model::Person", rp.className());
        assertEquals("model::Firm", rp.sourceClass());
        assertEquals(null, rp.filter(), "null filter preserved");
    }

    @Test
    void emptyImportScopeReturnsSameInstanceForUnresolvableNames() {
        var cd = simpleClass("model::Sub",
                List.of(nameRef("Person")), List.of());
        var model = new ParsedModel(List.of(cd), EMPTY);
        var r = NameResolver.resolve(model, FQNS);
        assertSame(model, r,
                "no imports + simple name with no wildcard match \u2192 no change");
    }

    // =================================================================
    // Complex / deeply nested
    // =================================================================

    @Test
    void ambiguityInsideLambdaBodyThrows() {
        ImportScope two = new ImportScope.Builder()
                .add("a::*").add("b::*").build();
        Set<String> ambig = Set.of("a::Foo", "b::Foo");
        var lam = new LambdaFunction(List.of(),
                List.of(new PackageableElementPtr("Foo")));
        assertThrows(IllegalStateException.class,
                () -> NameResolver.resolve(lam, NameResolver.Scope.of(two, ambig)));
    }

    @Test
    void deeplyNestedFunctionBodyResolved() {
        // myFn(): Person[1] | Person.all()->filter(x | $x.name == 'a')->first()
        var filterLam = new LambdaFunction(
                List.of(new Variable("x", null, ONE)),
                List.of(new AppliedFunction("equal", List.of(
                        new AppliedProperty(new Variable("x", null, ONE), "name"),
                        new CString("a")))));
        var body = new AppliedFunction("first", List.of(
                new AppliedFunction("filter", List.of(
                        new AppliedFunction("all",
                                List.of(new PackageableElementPtr("Person"))),
                        filterLam))));
        var fd = new FunctionDefinition("model::myFn", List.of(), List.of(),
                List.of(), nameRef("Person"), ZERO_ONE,
                List.of(body), List.of(), List.of());
        var r = (FunctionDefinition) resolveOne(fd);
        var rbody = (AppliedFunction) r.body().get(0);
        var rfilter = (AppliedFunction) rbody.parameters().get(0);
        var rall = (AppliedFunction) rfilter.parameters().get(0);
        assertEquals("model::Person",
                ((PackageableElementPtr) rall.parameters().get(0)).fullPath());
        // verify lambda body unchanged structurally (no name refs)
        var rlam = (LambdaFunction) rfilter.parameters().get(1);
        assertEquals("equal",
                ((AppliedFunction) rlam.body().get(0)).function(),
                "non-resolvable function name preserved");
    }

    @Test
    void newInstancePropertyValuesRecursivelyResolved() {
        // ^Person(friend = ^Firm(...))
        var inner = new NewInstance("Firm", List.of(), Map.of());
        var outer = new NewInstance("Person", List.of(),
                Map.of("friend", new KeyExpression(inner, false)));
        var r = (NewInstance) NameResolver.resolve(outer, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertEquals("model::Person", r.className());
        var rInner = (NewInstance) r.properties().get("friend").value();
        assertEquals("model::Firm", rInner.className());
    }

    @Test
    void classDefinitionAllFieldsPreservedAfterPartialResolution() {
        // Class with type params + native flag + everything \u2014 ensure rebuilt
        // record preserves all non-resolved fields.
        var cd = new ClassDefinition(
                "model::Holder",
                List.of("T", "U"),                                 // typeParams
                List.of(nameRef("Person")),                        // superClasses
                List.of(prop("x", nameRef("T"))),                  // properties (T not in FQNS, stays)
                List.of(), List.of(), List.of(), List.of(),
                true);                                             // isNative
        var r = (ClassDefinition) resolveOne(cd);
        assertEquals("model::Person",
                ((TypeExpression.NameRef) r.superClasses().get(0)).name(),
                "Person resolved");
        assertEquals("T",
                ((TypeExpression.NameRef) r.properties().get(0).type()).name(),
                "T pass-through (type param)");
        assertEquals(List.of("T", "U"), r.typeParams(),
                "type params preserved through rebuild");
        assertEquals(true, r.isNative(), "native flag preserved through rebuild");
        assertEquals("model::Holder", r.qualifiedName());
    }

    // =================================================================
    // Negative — fields that must NOT be resolved
    // =================================================================

    @Test
    void propertyNameNotResolved() {
        // Property mapping has a propertyName field that happens to match an
        // FQNS entry. It must NOT be resolved (it's a property name, not a
        // type reference).
        Set<String> trickyFqns = Set.of("store::DB", "model::name");
        ImportScope imp = new ImportScope.Builder()
                .add("model::*").add("store::*").build();
        var col = new PropertyMapping.Column("name", "DB", "T", "name_col");
        var md = mapping(col, "model::Person");
        var r = (PropertyMapping.Column) firstPm(
                (LegacyMappingDefinition) resolveOne(md, imp, trickyFqns));
        assertEquals("name", r.propertyName(),
                "propertyName is a property identifier, never resolved");
    }

    @Test
    void columnAndTableNamesNotResolved() {
        Set<String> trickyFqns = Set.of("store::DB", "model::T", "model::name_col");
        ImportScope imp = new ImportScope.Builder()
                .add("model::*").add("store::*").build();
        var col = new PropertyMapping.Column("x", "DB", "T", "name_col");
        var md = mapping(col, "model::Person");
        var r = (PropertyMapping.Column) firstPm(
                (LegacyMappingDefinition) resolveOne(md, imp, trickyFqns));
        assertEquals("T", r.table(), "table name not a Pure FQN; never resolved");
        assertEquals("name_col", r.column(), "column name not a Pure FQN; never resolved");
    }

    @Test
    void setIdsNotResolved() {
        Set<String> trickyFqns = Set.of("model::p_set", "store::DB");
        ImportScope imp = new ImportScope.Builder()
                .add("model::*").add("store::*").build();
        var rel = new ClassMapping.Relational(
                "Person", "p_set", "parent_set", true,
                new TableReference("DB", "T"),
                null, false, List.of(), List.of(), List.of(),
                /* sourceUrl */ null);
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(rel), List.of(), List.of(), null);
        var r = (LegacyMappingDefinition) resolveOne(md, imp, trickyFqns);
        var rrel = (ClassMapping.Relational) r.classMappings().get(0);
        assertEquals("p_set", rrel.setId(), "setId is a local id; never resolved");
        assertEquals("parent_set", rrel.extendsSetId(),
                "extendsSetId is a local id; never resolved");
    }

    @Test
    void cleanSheetMappingBindingFqnsResolved() {
        // The clean-sheet (canonical) mapping is parsed straight from Door 1, so
        // its binding FQNs reach the resolver as simple names under `import` and
        // MUST resolve — the gap M4.5 closed (NameResolver used to pass the
        // canonical record through untouched).
        Set<String> fqns = Set.of("acme::Person", "acme::personMapping",
                "acme::Person_Firm", "acme::personFirmMatch");
        ImportScope imp = new ImportScope.Builder().add("acme::*").build();
        var classBinding = new MappingDefinition.ClassBinding(
                "Person", MappingDefinition.Kind.RELATIONAL, "emp", "base", true, "personMapping");
        var assocBinding = new MappingDefinition.AssociationBinding(
                "Person_Firm", "personFirmMatch");
        var md = new MappingDefinition("acme::M", List.of(),
                List.of(classBinding), List.of(assocBinding), List.of(), null);

        var r = (MappingDefinition) resolveOne(md, imp, fqns);
        var cb = r.classBindings().get(0);
        assertEquals("acme::Person", cb.classFqn(), "class FQN resolved via wildcard import");
        assertEquals("acme::personMapping", cb.functionFqn(), "function FQN resolved");
        assertEquals("emp", cb.setId(), "setId is a local id; never resolved");
        assertEquals("base", cb.extendsSetId(), "extendsSetId is a local id; never resolved");
        assertEquals(MappingDefinition.Kind.RELATIONAL, cb.kind());
        assertTrue(cb.root());
        var ab = r.associationBindings().get(0);
        assertEquals("acme::Person_Firm", ab.associationFqn());
        assertEquals("acme::personFirmMatch", ab.predicateFunctionFqn());
    }

    @Test
    void cleanSheetMappingWithFqnsRoundTripsUnchanged() {
        // Reference-equality discipline: a mapping whose binding names are
        // already fully qualified resolves to the identical instance.
        Set<String> fqns = Set.of("acme::Person", "acme::personMapping");
        ImportScope imp = new ImportScope.Builder().add("acme::*").build();
        var md = new MappingDefinition("acme::M", List.of(),
                List.of(new MappingDefinition.ClassBinding(
                        "acme::Person", MappingDefinition.Kind.PURE, null, null, true,
                        "acme::personMapping")),
                List.of(), List.of(), null);
        assertSame(md, resolveOne(md, imp, fqns),
                "already-FQN bindings round-trip without allocating a new record");
    }

    @Test
    void stereotypeNameAndTagNameNotResolved() {
        // Only the profileName gets resolved; stereotypeName / tagName are
        // intra-profile identifiers.
        Set<String> trickyFqns = Set.of("doc::Documentation", "doc::deprecated", "doc::since");
        ImportScope imp = new ImportScope.Builder().add("doc::*").build();
        var cd = new ClassDefinition("model::Sub", List.of(), List.of(),
                List.of(), List.of(), List.of(),
                List.of(new StereotypeApplication("Documentation", "deprecated")),
                List.of(new TaggedValue("Documentation", "since", "1.0")),
                false);
        var r = (ClassDefinition) resolveOne(cd, imp, trickyFqns);
        assertEquals("deprecated", r.stereotypes().get(0).stereotypeName(),
                "stereotypeName never resolved even when match exists in FQNS");
        assertEquals("since", r.taggedValues().get(0).tagName(),
                "tagName never resolved even when match exists in FQNS");
    }

    @Test
    void variableNameNotResolved() {
        // Variable `name` field collides with an FQN entry but must not be resolved.
        Set<String> trickyFqns = Set.of("model::x");
        ImportScope imp = new ImportScope.Builder().add("model::*").build();
        ValueSpecification v = new Variable("x", null, ONE);
        var r = (Variable) NameResolver.resolve(v, NameResolver.Scope.of(imp, trickyFqns));
        assertEquals("x", r.name(), "variable name never resolved");
    }

    @Test
    void appliedPropertyNameNotResolved() {
        // AppliedProperty.property() is a property name, never an FQN.
        Set<String> trickyFqns = Set.of("model::Person", "model::name");
        ImportScope imp = new ImportScope.Builder().add("model::*").build();
        var ap = new AppliedProperty(new PackageableElementPtr("Person"), "name");
        var r = (AppliedProperty) NameResolver.resolve(ap, NameResolver.Scope.of(imp, trickyFqns));
        assertEquals("name", r.property(),
                "property name field never resolved even when match exists in FQNS");
    }

    @Test
    void enumValueNotResolved() {
        // EnumValue.value() is the enum-constant name, not an FQN.
        Set<String> trickyFqns = Set.of("model::Status", "model::ACTIVE");
        ImportScope imp = new ImportScope.Builder().add("model::*").build();
        var ev = new EnumValue("Status", "ACTIVE");
        var r = (EnumValue) NameResolver.resolve(ev, NameResolver.Scope.of(imp, trickyFqns));
        assertEquals("model::Status", r.fullPath(), "enum FQN resolved");
        assertEquals("ACTIVE", r.value(), "enum value name never resolved");
    }

    @Test
    void unresolvableNameStaysVerbatimWhenWildcardHasNoMatch() {
        // Wildcard import doesn't contain `Foo` \u2014 stays as `Foo`.
        var cd = simpleClass("model::Sub",
                List.of(nameRef("Foo")), List.of());
        var r = (ClassDefinition) resolveOne(cd, WILDCARD_MODEL, FQNS);
        assertEquals("Foo", ((TypeExpression.NameRef) r.superClasses().get(0)).name(),
                "no wildcard match \u2192 verbatim pass-through (next layer handles primitives or errors)");
    }

    // =================================================================
    // Name conflicts and precedence
    // =================================================================

    @Test
    void specificImportWinsOverWildcardMatch() {
        // Wildcard `wild::*` contains a Person; specific `target::Person`
        // also names a Person. Specific must win.
        ImportScope imp = new ImportScope.Builder()
                .add("wild::*")
                .add("target::Person")
                .build();
        Set<String> fqns = Set.of("wild::Person", "target::Person");
        var cd = simpleClass("model::Sub",
                List.of(nameRef("Person")), List.of());
        var r = (ClassDefinition) resolveOne(cd, imp, fqns);
        assertEquals("target::Person",
                ((TypeExpression.NameRef) r.superClasses().get(0)).name(),
                "specific import has higher precedence than wildcard");
    }

    @Test
    void specificImportOverridesEvenWhenWildcardWouldBeAmbiguous() {
        // Without the specific import, `Person` would be ambiguous across
        // two wildcards. The specific resolves it deterministically before
        // ambiguity check runs.
        ImportScope imp = new ImportScope.Builder()
                .add("a::*").add("b::*")
                .add("target::Person")
                .build();
        Set<String> fqns = Set.of("a::Person", "b::Person", "target::Person");
        var cd = simpleClass("model::Sub",
                List.of(nameRef("Person")), List.of());
        var r = (ClassDefinition) resolveOne(cd, imp, fqns);
        assertEquals("target::Person",
                ((TypeExpression.NameRef) r.superClasses().get(0)).name());
    }

    @Test
    void wildcardThatDoesNotMatchKnownFqnIgnored() {
        // wildcard `phantom::*` is declared but `phantom::Person` is not in
        // FQNS. Only the wildcard with a real match counts.
        ImportScope imp = new ImportScope.Builder()
                .add("phantom::*").add("model::*").build();
        var cd = simpleClass("model::Sub",
                List.of(nameRef("Person")), List.of());
        var r = (ClassDefinition) resolveOne(cd, imp, FQNS);
        assertEquals("model::Person",
                ((TypeExpression.NameRef) r.superClasses().get(0)).name(),
                "non-matching wildcard candidate ignored \u2192 no ambiguity");
    }

    @Test
    void multipleWildcardsResolveIndependentNames() {
        // 3 wildcards, each contributes one name. No conflicts.
        ImportScope imp = new ImportScope.Builder()
                .add("model::*").add("store::*").add("doc::*").build();
        var cd = new ClassDefinition("x::Sub", List.of(),
                List.of(nameRef("Person")),                                 // model::Person
                List.of(prop("db", nameRef("DB"))),                         // store::DB
                List.of(), List.of(),
                List.of(new StereotypeApplication("Documentation", "x")),   // doc::Documentation
                List.of(), false);
        var r = (ClassDefinition) resolveOne(cd, imp, FQNS);
        assertEquals("model::Person",
                ((TypeExpression.NameRef) r.superClasses().get(0)).name());
        assertEquals("store::DB",
                ((TypeExpression.NameRef) r.properties().get(0).type()).name());
        assertEquals("doc::Documentation", r.stereotypes().get(0).profileName());
    }

    @Test
    void manySpecificImportsResolveCorrectly() {
        ImportScope imp = new ImportScope.Builder()
                .add("model::Person")
                .add("model::Firm")
                .add("model::Address")
                .add("store::DB")
                .build();
        var cd = new ClassDefinition("x::Sub", List.of(),
                List.of(nameRef("Person"), nameRef("Firm")),
                List.of(prop("addr", nameRef("Address"))),
                List.of(), List.of(), List.of(), List.of(), false);
        var r = (ClassDefinition) resolveOne(cd, imp, FQNS);
        assertEquals("model::Person",
                ((TypeExpression.NameRef) r.superClasses().get(0)).name());
        assertEquals("model::Firm",
                ((TypeExpression.NameRef) r.superClasses().get(1)).name());
        assertEquals("model::Address",
                ((TypeExpression.NameRef) r.properties().get(0).type()).name());
    }

    // =================================================================
    // Primitives — must pass through (no FQNS entry; not resolver's job)
    // =================================================================

    @Test
    void commonPrimitivesPassThroughUnchanged() {
        // None of String, Integer, Boolean, Date, Float, Decimal, DateTime
        // are in FQNS; resolver returns them verbatim. PureModelBuilder
        // classifies them later.
        for (String prim : List.of("String", "Integer", "Boolean", "Date",
                "Float", "Decimal", "DateTime", "StrictDate")) {
            var cd = simpleClass("model::Sub", List.of(),
                    List.of(prop("x", nameRef(prim))));
            var r = (ClassDefinition) resolveOne(cd);
            assertEquals(prim,
                    ((TypeExpression.NameRef) r.properties().get(0).type()).name(),
                    "primitive " + prim + " must pass through unchanged");
        }
    }

    @Test
    void primitiveInsideGenericArgsPassesThrough() {
        // List<Integer> \u2014 List resolves, Integer stays primitive.
        var generic = new TypeExpression.Generic("List",
                List.of(nameRef("Integer")));
        var cd = simpleClass("model::Sub", List.of(),
                List.of(prop("xs", generic)));
        var r = (ClassDefinition) resolveOne(cd);
        var g = (TypeExpression.Generic) r.properties().get(0).type();
        assertEquals("model::List", g.name());
        assertEquals("Integer", ((TypeExpression.NameRef) g.arguments().get(0)).name(),
                "primitive type arg passes through");
    }

    @Test
    void primitiveAsVariableTypePassesThrough() {
        // Lambda parameter typed as Integer should keep the simple name.
        var lam = new LambdaFunction(
                List.of(new Variable("x", nameRef("Integer"), ONE)),
                List.of(new Variable("x", null, ONE)));
        var r = (LambdaFunction) NameResolver.resolve(lam, NameResolver.Scope.of(WILDCARD_MODEL, FQNS));
        assertEquals("Integer",
                ((TypeExpression.NameRef) r.parameters().get(0).type()).name());
    }

    // =================================================================
    // User functions (FQN-style references)
    // =================================================================

    @Test
    void userFunctionAlreadyFqnPassesThrough() {
        // Body calls my::pkg::myFn(x) \u2014 already FQN, no change.
        var body = new AppliedFunction("my::pkg::myFn",
                List.of(new CInteger(1)));
        var fd = new FunctionDefinition("model::caller", List.of(), List.of(),
                List.of(), nameRef("Person"), ONE,
                List.of(body), List.of(), List.of());
        var r = (FunctionDefinition) resolveOne(fd);
        assertEquals("my::pkg::myFn",
                ((AppliedFunction) r.body().get(0)).function());
    }

    @Test
    void userFunctionResolvedViaWildcardImport() {
        // Body calls myFn(x). wildcard `my::pkg::*` contains my::pkg::myFn.
        ImportScope imp = new ImportScope.Builder()
                .add("my::pkg::*").add("model::*").build();
        Set<String> fqns = Set.of("my::pkg::myFn", "model::Person");
        var body = new AppliedFunction("myFn", List.of(new CInteger(1)));
        var fd = new FunctionDefinition("model::caller", List.of(), List.of(),
                List.of(), nameRef("Person"), ONE,
                List.of(body), List.of(), List.of());
        var r = (FunctionDefinition) resolveOne(fd, imp, fqns);
        assertEquals("my::pkg::myFn",
                ((AppliedFunction) r.body().get(0)).function());
    }

    @Test
    void userFunctionResolvedViaSpecificImport() {
        ImportScope imp = new ImportScope.Builder()
                .add("my::pkg::myFn").add("model::*").build();
        Set<String> fqns = Set.of("my::pkg::myFn", "model::Person");
        var body = new AppliedFunction("myFn", List.of());
        var fd = new FunctionDefinition("model::caller", List.of(), List.of(),
                List.of(), nameRef("Person"), ONE,
                List.of(body), List.of(), List.of());
        var r = (FunctionDefinition) resolveOne(fd, imp, fqns);
        assertEquals("my::pkg::myFn",
                ((AppliedFunction) r.body().get(0)).function());
    }

    @Test
    void unknownFunctionNamePassesThrough() {
        // Calling a builtin like `first` that isn't in FQNS \u2014 stays verbatim
        // (PureModelBuilder + dispatch handle resolution later).
        var body = new AppliedFunction("first", List.of(
                new AppliedFunction("all",
                        List.of(new PackageableElementPtr("Person")))));
        var fd = new FunctionDefinition("model::caller", List.of(), List.of(),
                List.of(), nameRef("Person"), ZERO_ONE,
                List.of(body), List.of(), List.of());
        var r = (FunctionDefinition) resolveOne(fd);
        var rbody = (AppliedFunction) r.body().get(0);
        assertEquals("first", rbody.function(),
                "builtin function name not in FQNS \u2192 passes through verbatim");
        assertEquals("all", ((AppliedFunction) rbody.parameters().get(0)).function());
    }

    // =================================================================
    // Mixed realistic scenario
    // =================================================================

    @Test
    void realisticClassWithMixedPrimitivesUserTypesAndImports() {
        // Class Sub extends Person {
        //   name: String[1];      // primitive
        //   addr: Address[0..1];  // user type via wildcard
        //   firm: Firm[1];        // user type via specific import
        //   db: DB[1];             // store via separate wildcard
        // }
        ImportScope imp = new ImportScope.Builder()
                .add("model::*")
                .add("model::Firm")    // specific to test it works alongside wildcard
                .add("store::*")
                .build();
        var cd = new ClassDefinition("app::Sub", List.of(),
                List.of(nameRef("Person")),
                List.of(
                        prop("name", nameRef("String")),
                        prop("addr", nameRef("Address")),
                        prop("firm", nameRef("Firm")),
                        prop("db", nameRef("DB"))),
                List.of(), List.of(), List.of(), List.of(), false);
        var r = (ClassDefinition) resolveOne(cd, imp, FQNS);
        assertEquals("model::Person",
                ((TypeExpression.NameRef) r.superClasses().get(0)).name());
        assertEquals("String",
                ((TypeExpression.NameRef) r.properties().get(0).type()).name(),
                "String primitive passes through");
        assertEquals("model::Address",
                ((TypeExpression.NameRef) r.properties().get(1).type()).name());
        assertEquals("model::Firm",
                ((TypeExpression.NameRef) r.properties().get(2).type()).name(),
                "Firm resolved (specific import; wildcard also matches \u2192 specific wins)");
        assertEquals("store::DB",
                ((TypeExpression.NameRef) r.properties().get(3).type()).name(),
                "DB resolved via separate wildcard");
    }

    @Test
    void mappingDeeplyNestedRelationalResolution() {
        // Mapping with ClassMapping.Relational with a complex filter +
        // join-mediated filter mapping + property bindings.
        ImportScope both = new ImportScope.Builder()
                .add("model::*").add("store::*").build();
        var chain = List.of(new JoinChainElement("j1", JoinType.INNER, "DB", false));
        var jmFm = new FilterMapping.JoinMediated(
                "DB", chain, new FilterPointer.Cross("DB", "ActiveOnly"));
        var pmCol = new PropertyMapping.Column("name", "DB", "T", "name_col");
        var pmJoin = new PropertyMapping.Join("friend", "DB", chain);
        var rel = new ClassMapping.Relational(
                "Person", "p_set", null, true,
                new TableReference("DB", "PersonTbl"),
                jmFm, false,
                List.of(new RelationalOperation.ColumnRef("DB", "T", "g")),  // groupBy
                List.of(new RelationalOperation.ColumnRef("DB", "T", "pk")), // primaryKey
                List.of(pmCol, pmJoin),
                /* sourceUrl */ null);
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(rel), List.of(), List.of(), null);
        var r = (LegacyMappingDefinition) resolveOne(md, both, FQNS);
        var rrel = (ClassMapping.Relational) r.classMappings().get(0);
        assertEquals("model::Person", rrel.className());
        assertEquals("store::DB", rrel.mainTable().database());
        assertEquals("store::DB",
                ((FilterMapping.JoinMediated) rrel.filter()).sourceDb());
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef) rrel.groupBy().get(0)).databaseName());
        assertEquals("store::DB",
                ((RelationalOperation.ColumnRef) rrel.primaryKey().get(0)).databaseName());
        assertEquals("store::DB",
                ((PropertyMapping.Column) rrel.propertyMappings().get(0)).database());
        assertEquals("store::DB",
                ((PropertyMapping.Join) rrel.propertyMappings().get(1)).database());
    }

    // =================================================================
    // EnumerationMapping resolution (regression for previously silent bug)
    // =================================================================

    @Test
    void enumerationMappingEnumNameResolved() {
        // mapping::M includes an enum mapping for Status with one value.
        ImportScope imp = new ImportScope.Builder().add("model::*").build();
        Set<String> fqns = Set.of("model::Status");
        var em = new EnumerationMapping("Status", "status_set",
                List.of(new EnumValueMapping("ACTIVE",
                        List.of(new SourceValue.StringValue("A")))));
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(), List.of(), List.of(em), null);
        var r = (LegacyMappingDefinition) resolveOne(md, imp, fqns);
        assertEquals("model::Status",
                r.enumerationMappings().get(0).enumName(),
                "enum FQN must be resolved");
        assertEquals("status_set", r.enumerationMappings().get(0).mappingId(),
                "mappingId is a set id; not resolved");
    }

    @Test
    void enumerationMappingSourceValueEnumRefResolved() {
        // EnumRef source value carries its own enum FQN.
        ImportScope imp = new ImportScope.Builder().add("model::*").build();
        Set<String> fqns = Set.of("model::Status", "model::Source");
        var ref = new SourceValue.EnumRef("Source", "ACTIVE");
        var em = new EnumerationMapping("Status", "status_set",
                List.of(new EnumValueMapping("ACTIVE", List.of(ref))));
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(), List.of(), List.of(em), null);
        var r = (LegacyMappingDefinition) resolveOne(md, imp, fqns);
        var rref = (SourceValue.EnumRef) r.enumerationMappings().get(0)
                .valueMappings().get(0).sourceValues().get(0);
        assertEquals("model::Source", rref.enumPath());
        assertEquals("ACTIVE", rref.enumValueName(),
                "enum value name within EnumRef never resolved");
    }

    @Test
    void enumerationMappingStringAndIntegerSourceValuesPassThrough() {
        // Verifies non-EnumRef SourceValue variants are identity-preserved.
        ImportScope imp = new ImportScope.Builder().add("model::*").build();
        Set<String> fqns = Set.of("model::Status");
        var sv1 = new SourceValue.StringValue("A");
        var sv2 = new SourceValue.IntegerValue(1L);
        var em = new EnumerationMapping("model::Status", "status_set",
                List.of(new EnumValueMapping("ACTIVE", List.of(sv1, sv2))));
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(), List.of(), List.of(em), null);
        var r = (LegacyMappingDefinition) resolveOne(md, imp, fqns);
        var values = r.enumerationMappings().get(0).valueMappings().get(0).sourceValues();
        assertSame(sv1, values.get(0));
        assertSame(sv2, values.get(1));
    }

    @Test
    void enumerationMappingFullyResolvedReturnsSameInstance() {
        // All FQNs already qualified \u2014 mapping round-trips by identity.
        var em = new EnumerationMapping("model::Status", "status_set",
                List.of(new EnumValueMapping("ACTIVE",
                        List.of(new SourceValue.EnumRef("model::Source", "X")))));
        var md = new LegacyMappingDefinition("mapping::M", List.of(),
                List.of(), List.of(), List.of(em), null);
        var r = resolveOne(md);
        assertSame(md, r, "fully-FQN enumeration mapping round-trips unchanged");
    }

    // =================================================================
    // Idempotency & change-detection
    // =================================================================

    @Test
    void resolveIsIdempotent() {
        // Running resolve twice must give the same result as running once.
        var cd = simpleClass("model::Sub",
                List.of(nameRef("Person")),
                List.of(prop("addr", nameRef("Address"))));
        var model = new ParsedModel(List.of(cd), WILDCARD_MODEL);
        var once = NameResolver.resolve(model, FQNS);
        var twice = NameResolver.resolve(once, FQNS);
        assertEquals(once, twice, "structural equality after second resolve");
        assertSame(once, twice,
                "all names already FQN after first pass \u2192 second pass is a no-op");
    }

    @Test
    void resolveProducesNewParsedModelWhenAnyChange() {
        // Any name change anywhere in the tree must surface as a NEW
        // ParsedModel instance \u2014 callers rely on identity to detect work.
        var cd = simpleClass("model::Sub",
                List.of(nameRef("Person")), List.of());
        var model = new ParsedModel(List.of(cd), WILDCARD_MODEL);
        var resolved = NameResolver.resolve(model, FQNS);
        assertNotSame(model, resolved,
                "name was rewritten somewhere \u2192 outer ParsedModel must differ");
        assertNotSame(cd, resolved.elements().get(0),
                "the changed element must be a new instance");
    }

    // =================================================================
    // Type-parameter shadowing
    // =================================================================

    @Test
    void classTypeParameterShadowsImportInProperty() {
        // Class Foo<T> { x: T[1] }   with FQNS containing model::T.
        // T inside the class body is the type parameter, NOT model::T.
        Set<String> trickyFqns = Set.of("model::T", "model::Person");
        var cd = new ClassDefinition("model::Foo",
                List.of("T"),                                       // type params
                List.of(),
                List.of(prop("x", nameRef("T"))),
                List.of(), List.of(), List.of(), List.of(), false);
        var r = (ClassDefinition) resolveOne(cd, WILDCARD_MODEL, trickyFqns);
        assertEquals("T",
                ((TypeExpression.NameRef) r.properties().get(0).type()).name(),
                "T must remain a type-parameter reference, not be resolved to model::T");
    }

    @Test
    void classTypeParameterShadowsImportInSuperclass() {
        Set<String> trickyFqns = Set.of("model::T");
        var cd = new ClassDefinition("model::Foo",
                List.of("T"),
                List.of(nameRef("T")),                               // super = T
                List.of(), List.of(), List.of(), List.of(), List.of(), false);
        var r = (ClassDefinition) resolveOne(cd, WILDCARD_MODEL, trickyFqns);
        assertEquals("T",
                ((TypeExpression.NameRef) r.superClasses().get(0)).name(),
                "type-param T in superclass position must shadow model::T");
    }

    @Test
    void multipleClassTypeParametersAllShadow() {
        Set<String> trickyFqns = Set.of("model::T", "model::U", "model::Person");
        var cd = new ClassDefinition("model::Holder",
                List.of("T", "U"),
                List.of(),
                List.of(prop("t", nameRef("T")),
                        prop("u", nameRef("U")),
                        prop("p", nameRef("Person"))),
                List.of(), List.of(), List.of(), List.of(), false);
        var r = (ClassDefinition) resolveOne(cd, WILDCARD_MODEL, trickyFqns);
        assertEquals("T",
                ((TypeExpression.NameRef) r.properties().get(0).type()).name());
        assertEquals("U",
                ((TypeExpression.NameRef) r.properties().get(1).type()).name());
        assertEquals("model::Person",
                ((TypeExpression.NameRef) r.properties().get(2).type()).name(),
                "non-type-param name still resolves normally");
    }

    @Test
    void classTypeParameterShadowsInGenericArg() {
        // List<T> where T is a class type parameter and model::T also exists.
        Set<String> trickyFqns = Set.of("model::List", "model::T");
        var generic = new TypeExpression.Generic("List", List.of(nameRef("T")));
        var cd = new ClassDefinition("model::Holder",
                List.of("T"),
                List.of(),
                List.of(prop("xs", generic)),
                List.of(), List.of(), List.of(), List.of(), false);
        var r = (ClassDefinition) resolveOne(cd, WILDCARD_MODEL, trickyFqns);
        var g = (TypeExpression.Generic) r.properties().get(0).type();
        assertEquals("model::List", g.name(),
                "outer List resolves; it is NOT a type param");
        assertEquals("T", ((TypeExpression.NameRef) g.arguments().get(0)).name(),
                "inner T shadows model::T");
    }

    @Test
    void functionTypeParameterShadowsInReturnAndParam() {
        // function myFn<T>(x:T[1]):T[1] | $x  with model::T present.
        Set<String> trickyFqns = Set.of("model::T", "model::Person");
        var param = new FunctionDefinition.ParameterDefinition(
                "x", nameRef("T"), ONE);
        var fd = new FunctionDefinition("model::myFn",
                List.of("T"),                                        // type params
                List.of(),
                List.of(param),
                nameRef("T"),
                ONE,
                List.of(new Variable("x", null, ONE)),
                List.of(), List.of());
        var r = (FunctionDefinition) resolveOne(fd, WILDCARD_MODEL, trickyFqns);
        assertEquals("T",
                ((TypeExpression.NameRef) r.parameters().get(0).type()).name(),
                "T in parameter type shadows model::T");
        assertEquals("T",
                ((TypeExpression.NameRef) r.returnType()).name(),
                "T in return type shadows model::T");
    }

    @Test
    void typeParameterDoesNotLeakIntoSiblingClass() {
        // Two classes in the same model: Foo<T> declares T; Bar does not.
        // model::T exists in FQNS. T inside Foo must shadow; T anywhere
        // inside Bar must resolve to model::T.
        Set<String> trickyFqns = Set.of("model::T");
        var foo = new ClassDefinition("model::Foo",
                List.of("T"), List.of(),
                List.of(prop("x", nameRef("T"))),
                List.of(), List.of(), List.of(), List.of(), false);
        var bar = new ClassDefinition("model::Bar",
                List.of(), List.of(),
                List.of(prop("x", nameRef("T"))),
                List.of(), List.of(), List.of(), List.of(), false);
        var model = new ParsedModel(List.of(foo, bar), WILDCARD_MODEL);
        var resolved = NameResolver.resolve(model, trickyFqns);
        var rFoo = (ClassDefinition) resolved.elements().get(0);
        var rBar = (ClassDefinition) resolved.elements().get(1);
        assertEquals("T",
                ((TypeExpression.NameRef) rFoo.properties().get(0).type()).name(),
                "T inside Foo<T> shadows");
        assertEquals("model::T",
                ((TypeExpression.NameRef) rBar.properties().get(0).type()).name(),
                "T inside Bar (no type params) resolves to model::T \u2014 no leak");
    }

    @Test
    void resolveProducesSameInstanceWhenNothingChanges() {
        // Already-FQN tree round-trips through resolve with reference
        // equality preserved at every level.
        var cd = simpleClass("model::Sub",
                List.of(new TypeExpression.NameRef("model::Firm")),
                List.of(prop("name", new TypeExpression.NameRef("model::Address"))));
        var model = new ParsedModel(List.of(cd), WILDCARD_MODEL);
        var resolved = NameResolver.resolve(model, FQNS);
        assertSame(model, resolved);
        assertSame(cd, resolved.elements().get(0));
        assertSame(cd.superClasses(), ((ClassDefinition) resolved.elements().get(0))
                .superClasses());
        assertSame(cd.properties().get(0).type(),
                ((ClassDefinition) resolved.elements().get(0)).properties().get(0).type());
    }
}
