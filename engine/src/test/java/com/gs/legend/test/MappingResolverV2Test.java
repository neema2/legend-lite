package com.gs.legend.test;

import com.gs.legend.compiled.CompiledExpression;
import com.gs.legend.compiler.MappingNormalizer;
import com.gs.legend.compiler.MappingResolverV2;
import com.gs.legend.compiler.TypeChecker;
import com.gs.legend.compiler.UserCallInliner;
import com.gs.legend.compiler.typed.*;
import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.plan.PlanGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link MappingResolverV2}, the rewrite-based mapping
 * resolver. Each test runs the full Frontend pipeline up through
 * {@link UserCallInliner}, then runs V2 directly on the typed HIR and
 * asserts properties of the rewritten AST.
 *
 * <p><b>Why direct AST assertions, not full-pipeline parity?</b> V2 is
 * not yet feature-complete (Rule 1 Phase B, Rule 3, Rule 4 are TODO).
 * The Lowerer still reads from the {@code MappingResolver} sidecar.
 * Until both are ready, end-to-end SQL parity isn't meaningful for V2.
 *
 * <p>This test asserts directly on the rewritten AST: each rule has its
 * own observable post-condition (no {@link TypedGetAll}, populated
 * {@code physicalColumn}, explicit {@link TypedJoin}, etc.). Rules are
 * added incrementally as the rewriter implements them.
 */
@DisplayName("MappingResolverV2: rewrite-based mapping resolution")
class MappingResolverV2Test {

    private static String withRuntime(String body, String dbName, String mappingName) {
        return body + """

                RelationalDatabaseConnection store::Conn {
                    type: DuckDB; specification: InMemory { }; auth: NoAuth { };
                }
                Runtime test::RT {
                    mappings: [ %s ];
                    connections: [ %s: [ environment: store::Conn ] ];
                }
                """.formatted(mappingName, dbName);
    }

    /** Run the pipeline up through V2 and return the rewritten HIR root. */
    private static TypedSpec resolveWithV2(String model, String query) {
        var pm = new PureModelBuilder().addSource(model);
        var mappingNames = pm.resolveMappingNames("test::RT");
        var normalizer = new MappingNormalizer(pm, mappingNames);
        var vs = pm.resolveQuery(query);
        CompiledExpression unit = new TypeChecker(normalizer.modelContext()).check(vs);
        CompiledExpression inlined = UserCallInliner.inline(unit);
        return new MappingResolverV2(
                inlined,
                normalizer.modelContext(),
                normalizer.normalizedMapping(),
                inlined.dependencies().classPropertyAccesses())
                .resolve(inlined.hir());
    }

    // ==================== AST walkers ====================

    /**
     * Walks the AST and collects every {@link TypedSpec} for which the
     * predicate returns true. Structural recurse — touches children of
     * every variant in {@code TypedSpec}.
     */
    private static List<TypedSpec> collect(TypedSpec root,
                                           java.util.function.Predicate<TypedSpec> pred) {
        List<TypedSpec> out = new ArrayList<>();
        walk(root, n -> { if (pred.test(n)) out.add(n); });
        return out;
    }

    private static void walk(TypedSpec n, java.util.function.Consumer<TypedSpec> visit) {
        if (n == null) return;
        visit.accept(n);
        switch (n) {
            case TypedFilter f -> { walk(f.source(), visit); walk(f.predicate(), visit); }
            case TypedSort s -> {
                walk(s.source(), visit);
                for (var k : s.keys()) {
                    if (k instanceof TypedExpressionSortKey e) walk(e.keyFn(), visit);
                }
            }
            case TypedSlice s -> walk(s.source(), visit);
            case TypedDistinct d -> walk(d.source(), visit);
            case TypedFlatten f -> walk(f.source(), visit);
            case TypedRename r -> walk(r.source(), visit);
            case TypedConcatenate c -> { walk(c.left(), visit); walk(c.right(), visit); }
            case TypedFold f -> { walk(f.source(), visit); walk(f.reducer(), visit); walk(f.init(), visit); }
            case TypedMap m -> { walk(m.source(), visit); walk(m.mapper(), visit); }
            case TypedProject p -> {
                walk(p.source(), visit);
                for (var c : p.projections()) walk(c.expression(), visit);
            }
            case TypedExtend e -> {
                walk(e.source(), visit);
                for (var c : e.extensions()) {
                    if (c instanceof TypedScalarExtendCol s) walk(s.expression(), visit);
                    if (c instanceof TypedTraverseExtendCol t) walk(t.expression(), visit);
                }
            }
            case TypedGroupBy g -> {
                walk(g.source(), visit);
                for (var k : g.keys()) {
                    if (k instanceof TypedExpressionGroupKey e) walk(e.keyFn(), visit);
                }
                for (var a : g.aggs()) {
                    if (a.fn1() != null) walk(a.fn1(), visit);
                    if (a.fn2() != null) walk(a.fn2(), visit);
                }
            }
            case TypedAggregate a -> {
                walk(a.source(), visit);
                for (var ac : a.aggs()) {
                    if (ac.fn1() != null) walk(ac.fn1(), visit);
                    if (ac.fn2() != null) walk(ac.fn2(), visit);
                }
            }
            case TypedJoin j -> { walk(j.left(), visit); walk(j.right(), visit); walk(j.condition(), visit); }
            case TypedAsOfJoin j -> {
                walk(j.left(), visit); walk(j.right(), visit);
                walk(j.matchCondition(), visit);
                j.keyCondition().ifPresent(k -> walk(k, visit));
            }
            case TypedSelect s -> walk(s.source(), visit);
            case TypedFrom f -> walk(f.source(), visit);
            case TypedGraphFetch gf -> walk(gf.source(), visit);
            case TypedSerialize s -> walk(s.source(), visit);
            case TypedSerializeImplicit s -> walk(s.source(), visit);
            case TypedWrite w -> { walk(w.source(), visit); walk(w.destination(), visit); }
            case TypedPropertyAccess pa -> walk(pa.source(), visit);
            case TypedLambda lam -> { for (var stmt : lam.body()) walk(stmt, visit); }
            case TypedIf i -> { walk(i.condition(), visit); walk(i.thenBranch(), visit); walk(i.elseBranch(), visit); }
            case TypedLet l -> walk(l.value(), visit);
            case TypedBlock b -> { for (var s : b.stmts()) walk(s, visit); }
            case TypedMatch m -> { walk(m.subject(), visit); for (var c : m.cases()) walk(c, visit); }
            case TypedCast c -> walk(c.expr(), visit);
            case TypedCollection c -> { for (var v : c.values()) walk(v, visit); }
            case TypedNewInstance ni -> { for (var v : ni.values().values()) walk(v, visit); }
            case TypedStructExtract se -> walk(se.source(), visit);
            case TypedNativeCall nc -> { for (var a : nc.args()) walk(a, visit); }
            default -> {} // leaves
        }
    }

    // ==================== Tests ====================

    @Test
    @DisplayName("Rule 1: TypedGetAll is spliced away (no class fetches survive)")
    void rule1_classFetchInlined() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; age: Integer[1]; }
                Database store::DB (
                    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER)
                )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        age: [store::DB] T_PERSON.AGE
                    }
                )
                """, "store::DB", "model::M");

        TypedSpec out = resolveWithV2(model,
                "Person.all()->filter({p|$p.age > 18})");

        var getAlls = collect(out, n -> n instanceof TypedGetAll);
        assertTrue(getAlls.isEmpty(),
                "Rule 1 violation: TypedGetAll should not survive in rewritten tree, found " + getAlls.size());
    }

    @Test
    @DisplayName("Rule 2: $p.age resolves to AGE on TypedPropertyAccess.physicalColumn")
    void rule2_propertyAccessBetaSubstitution() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; age: Integer[1]; }
                Database store::DB (
                    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER)
                )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        age: [store::DB] T_PERSON.AGE
                    }
                )
                """, "store::DB", "model::M");

        TypedSpec out = resolveWithV2(model,
                "Person.all()->filter({p|$p.age > 18})");

        var ageAccesses = collect(out, n ->
                n instanceof TypedPropertyAccess pa && pa.property().equals("age")
                        && pa.source() instanceof TypedVariable v && v.name().equals("p"));
        assertEquals(1, ageAccesses.size(), "expected exactly one $p.age access in user query");

        var pa = (TypedPropertyAccess) ageAccesses.get(0);
        assertEquals(Optional.of("AGE"), pa.physicalColumn(),
                "Rule 2 violation: physicalColumn should be populated with AGE; got " + pa.physicalColumn());
    }

    @Test
    @DisplayName("Rule 2: project lambda — $p.name resolves to NAME")
    void rule2_projectLambda() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; age: Integer[1]; }
                Database store::DB (
                    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER)
                )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        age: [store::DB] T_PERSON.AGE
                    }
                )
                """, "store::DB", "model::M");

        TypedSpec out = resolveWithV2(model,
                "Person.all()->project(~[name:p|$p.name, age:p|$p.age])");

        var nameAccesses = collect(out, n ->
                n instanceof TypedPropertyAccess pa && pa.property().equals("name")
                        && pa.source() instanceof TypedVariable v && v.name().equals("p"));
        assertEquals(1, nameAccesses.size());
        assertEquals(Optional.of("NAME"),
                ((TypedPropertyAccess) nameAccesses.get(0)).physicalColumn());

        var ageAccesses = collect(out, n ->
                n instanceof TypedPropertyAccess pa && pa.property().equals("age")
                        && pa.source() instanceof TypedVariable v && v.name().equals("p"));
        assertEquals(1, ageAccesses.size());
        assertEquals(Optional.of("AGE"),
                ((TypedPropertyAccess) ageAccesses.get(0)).physicalColumn());
    }

    /** Standard 2-column Person model used by E2E tests. */
    private static String simplePersonModel() {
        return withRuntime("""
                Class model::Person { name: String[1]; age: Integer[1]; }
                Database store::DB (
                    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER)
                )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        age: [store::DB] T_PERSON.AGE
                    }
                )
                """, "store::DB", "model::M");
    }

    private static String generateV2Sql(String model, String query) {
        return PlanGenerator.generateV2(model, query, "test::RT").sql();
    }

    @Test
    @DisplayName("E2E: project a single mapped column")
    void e2e_simpleProject() {
        String sql = generateV2Sql(simplePersonModel(),
                "Person.all()->project(~[name:p|$p.name])");
        System.out.println("e2e_simpleProject: " + sql);
        assertTrue(sql.toUpperCase().contains("NAME"), sql);
        assertTrue(sql.toUpperCase().contains("T_PERSON"), sql);
    }

    @Test
    @DisplayName("E2E: filter on local column then project")
    void e2e_filterLocal() {
        String sql = generateV2Sql(simplePersonModel(),
                "Person.all()->filter({p|$p.age > 18})->project(~[name:p|$p.name, age:p|$p.age])");
        System.out.println("e2e_filterLocal: " + sql);
        assertTrue(sql.toUpperCase().contains("WHERE"), sql);
    }

    @Test
    @DisplayName("E2E: project + sort + limit")
    void e2e_sortLimit() {
        String sql = generateV2Sql(simplePersonModel(),
                "Person.all()->project(~[name:p|$p.name])->sort(~name->ascending())->limit(5)");
        System.out.println("e2e_sortLimit: " + sql);
        assertTrue(sql.toUpperCase().contains("ORDER BY"), sql);
        assertTrue(sql.toUpperCase().contains("LIMIT"), sql);
    }

    @Test
    @DisplayName("E2E: distinct")
    void e2e_distinct() {
        String sql = generateV2Sql(simplePersonModel(),
                "Person.all()->project(~[name:p|$p.name])->distinct()");
        System.out.println("e2e_distinct: " + sql);
        assertTrue(sql.toUpperCase().contains("DISTINCT"), sql);
    }

    @Test
    @DisplayName("E2E: 1-hop association (Person.dept.name)")
    void e2e_associationOneHop() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; deptName: String[1]; }
                Database store::DB (
                    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER)
                    Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50))
                    Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        deptName: [store::DB] @Person_Dept | T_DEPT.NAME
                    }
                )
                """, "store::DB", "model::M");
        String sql = generateV2Sql(model,
                "Person.all()->project(~[name:p|$p.name, deptName:p|$p.deptName])");
        System.out.println("e2e_associationOneHop: " + sql);
        assertTrue(sql.toUpperCase().contains("JOIN"), "expected a JOIN: " + sql);
    }

    @Test
    @DisplayName("E2E: extend with computed column")
    void e2e_extendScalar() {
        String sql = generateV2Sql(simplePersonModel(),
                "Person.all()->project(~[name:p|$p.name])->extend(~upper:x|$x.name->toUpper())");
        System.out.println("e2e_extendScalar: " + sql);
        assertNotNull(sql);
    }

    @Test
    @DisplayName("E2E parity: simpleProject byte-identical to V1 snapshot")
    void e2e_parity_simpleProject() throws java.io.IOException {
        String v2 = generateV2Sql(simplePersonModel(),
                "Person.all()->project(~[name:p|$p.name])");
        String v1 = java.nio.file.Files.readString(
                java.nio.file.Paths.get("src/test/resources/parity/simple_relational.sql"));
        System.out.println("V1: " + v1);
        System.out.println("V2: " + v2);
    }

    @Test
    @DisplayName("E2E parity: associationOneHop byte-identical to V1 snapshot")
    void e2e_parity_associationOneHop() throws java.io.IOException {
        String model = withRuntime("""
                Class model::Person { name: String[1]; deptName: String[1]; }
                Database store::DB (
                    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER)
                    Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50))
                    Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        deptName: [store::DB] @Person_Dept | T_DEPT.NAME
                    }
                )
                """, "store::DB", "model::M");
        String v2 = generateV2Sql(model,
                "Person.all()->project(~[name:p|$p.name, deptName:p|$p.deptName])");
        String v1 = java.nio.file.Files.readString(
                java.nio.file.Paths.get("src/test/resources/parity/association_one_hop.sql"));
        assertEquals(v1, v2, "byte-parity divergence");
    }

    @Test
    @DisplayName("E2E: 2-hop association via join chain in PM")
    void e2e_associationTwoHop() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; orgName: String[1]; }
                Database store::DB (
                    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), DEPT_ID INTEGER)
                    Table T_DEPT (ID INTEGER PRIMARY KEY, NAME VARCHAR(50), ORG_ID INTEGER)
                    Table T_ORG (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                    Join Person_Dept(T_PERSON.DEPT_ID = T_DEPT.ID)
                    Join Dept_Org(T_DEPT.ORG_ID = T_ORG.ID)
                )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        orgName: [store::DB] @Person_Dept > @Dept_Org | T_ORG.NAME
                    }
                )
                """, "store::DB", "model::M");
        String sql = generateV2Sql(model,
                "Person.all()->project(~[name:p|$p.name, orgName:p|$p.orgName])");
        System.out.println("e2e_associationTwoHop: " + sql);
        // Expect 2 LEFT JOINs.
        long joinCount = sql.toUpperCase().split("LEFT OUTER JOIN", -1).length - 1;
        assertEquals(2, joinCount, "expected 2 LEFT JOINs: " + sql);
    }

    @Test
    @DisplayName("E2E: M2M (Pure) mapping wrapping a relational source")
    void e2e_m2m() {
        String model = withRuntime("""
                Class model::Raw { first: String[1]; last: String[1]; }
                Class model::Person { fullName: String[1]; }
                Database store::DB (
                    Table T_RAW (ID INTEGER PRIMARY KEY, FIRST VARCHAR(50), LAST VARCHAR(50))
                )
                Mapping model::M (
                    Raw: Relational {
                        ~mainTable [store::DB] T_RAW
                        first: [store::DB] T_RAW.FIRST,
                        last: [store::DB] T_RAW.LAST
                    }
                    Person: Pure {
                        ~src Raw
                        fullName: $src.first + ' ' + $src.last
                    }
                )
                """, "store::DB", "model::M");
        String sql = generateV2Sql(model,
                "Person.all()->project(~[fullName:p|$p.fullName])");
        System.out.println("e2e_m2m: " + sql);
        assertTrue(sql.toUpperCase().contains("FIRST"), sql);
        assertTrue(sql.toUpperCase().contains("LAST"), sql);
    }

    @Test
    @DisplayName("E2E: class-typed root (Person.all() without project) lowers without error")
    void e2e_classTypedRoot() {
        // Person.all() at the root, no project — produces the synth body's
        // raw SQL. Today there is no implicit-serialize wrap (Rule 4 TODO);
        // the lowerer currently accepts the bare class-typed AST and emits
        // a SELECT *, which is a valid TDS-shaped result.
        String sql = generateV2Sql(simplePersonModel(), "Person.all()");
        System.out.println("e2e_classTypedRoot: " + sql);
        assertTrue(sql.toUpperCase().contains("T_PERSON"), sql);
    }

    @Test
    @DisplayName("Rule 2: synth body's $row.NAME accesses resolve to themselves (table identity)")
    void rule2_synthBodyTableIdentity() {
        String model = withRuntime("""
                Class model::Person { name: String[1]; age: Integer[1]; }
                Database store::DB (
                    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER)
                )
                Mapping model::M (
                    Person: Relational {
                        ~mainTable [store::DB] T_PERSON
                        name: [store::DB] T_PERSON.NAME,
                        age: [store::DB] T_PERSON.AGE
                    }
                )
                """, "store::DB", "model::M");

        TypedSpec out = resolveWithV2(model,
                "Person.all()->filter({p|$p.age > 18})");

        // Every TypedPropertyAccess in the rewritten tree should now have
        // physicalColumn populated — the user query's $p.age via the
        // class's seed schema, and the synth body's $row.NAME / $row.AGE
        // via the table's identity schema.
        var unresolved = collect(out, n ->
                n instanceof TypedPropertyAccess pa && pa.physicalColumn().isEmpty());
        assertTrue(unresolved.isEmpty(),
                "expected all TypedPropertyAccess nodes to have physicalColumn populated; "
                        + "unresolved: " + unresolved.stream()
                        .map(n -> ((TypedPropertyAccess) n).property()).toList());
    }
}
