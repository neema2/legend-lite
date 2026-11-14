// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.model.ClassDefinition;
import com.legend.model.ParsedModel;
import com.legend.protocol.TypeExpression;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T4.1 step 1 witness: association QUALIFIED properties adopt into their
 * owning class in the knowledge layer (F1), before Phase E. Modeled on the
 * corpus's {@code meta::relational::tests::model::simple::ProdSynonym}
 * (distinct ends, a parameterized qualifier, both return multiplicities),
 * plus the self-association and the no-unique-end error.
 */
class KnowledgeLayerTest {

    private static final String PROD_SYNONYM = """
            Class w::Product { name: String[1]; }
            Class w::Synonym { name: String[1]; type: String[1]; }
            Association w::ProdSynonym
            {
              synonyms: w::Synonym[*];
              product: w::Product[1];
              synonymByType(type: String[1]) { $this.synonyms->filter(s | $s.type == $type)->toOne() }: w::Synonym[1];
              synonymsByTypes(types: String[*]) { $this.synonyms->filter(s | $s.type->in($types)) }: w::Synonym[*];
            }
            """;

    private static Property.Derived derived(ModelContext ctx, String cls, String name) {
        return ctx.findClass(cls).orElseThrow().properties().stream()
                .filter(p -> p instanceof Property.Derived d && d.name().equals(name))
                .map(p -> (Property.Derived) p)
                .findFirst().orElseThrow(() -> new AssertionError(
                        cls + " does not carry qualified property " + name));
    }

    private static boolean carries(ModelContext ctx, String cls, String name) {
        return ctx.findClass(cls).orElseThrow().properties().stream()
                .anyMatch(p -> p instanceof Property.Derived d && d.name().equals(name));
    }

    @Test
    @DisplayName("distinct ends: the OTHER end's class owns the qualified property (both multiplicities)")
    void distinctEndsOtherEndOwns() {
        ModelContext ctx = Compiler.compileModel(PROD_SYNONYM);
        // both return Synonym, so Product (the other end) owns both
        derived(ctx, "w::Product", "synonymByType");
        derived(ctx, "w::Product", "synonymsByTypes");
        assertFalse(carries(ctx, "w::Synonym", "synonymByType"));
        assertFalse(carries(ctx, "w::Synonym", "synonymsByTypes"));
        // the adopted property lifts through the ONE class-derived funnel
        assertEquals(SynthFqn.prop("w::Product", "synonymByType"),
                derived(ctx, "w::Product", "synonymByType").bodyFunctionFqn());
    }

    @Test
    @DisplayName("parameterized qualifier: the parameter rides the adopted property")
    void parameterizedQualifierKeepsItsParameter() {
        ModelContext ctx = Compiler.compileModel(PROD_SYNONYM);
        Property.Derived byType = derived(ctx, "w::Product", "synonymByType");
        assertEquals(1, byType.parameters().size(), byType.parameters().toString());
        assertEquals("type", byType.parameters().get(0).name());
        Property.Derived byTypes = derived(ctx, "w::Product", "synonymsByTypes");
        assertEquals("types", byTypes.parameters().get(0).name());
    }

    @Test
    @DisplayName("self-association: the one class owns its qualified property")
    void selfAssociationOwnsItself() {
        ModelContext ctx = Compiler.compileModel("""
                Class w::Person { name: String[1]; }
                Association w::Friends
                {
                  a: w::Person[*];
                  b: w::Person[*];
                  friendsNamed(n: String[1]) { $this.a->filter(p | $p.name == $n) }: w::Person[*];
                }
                """);
        derived(ctx, "w::Person", "friendsNamed");
    }

    private static final String NO_UNIQUE_END = """
            Class w::Product { name: String[1]; }
            Class w::Synonym { name: String[1]; }
            Class w::Other { name: String[1]; }
            Association w::ProdSynonym
            {
              synonyms: w::Synonym[*];
              product: w::Product[1];
              stray() { $this.synonyms->map(s | ^w::Other(name = $s.name)) }: w::Other[*];
            }
            """;

    @Test
    @DisplayName("no unique owning end: a MODEL error — strict throws, a module build walls the association")
    void noUniqueOwningEnd() {
        ModelException strict = assertThrows(ModelException.class,
                () -> Compiler.compileModel(NO_UNIQUE_END));
        assertEquals(LegendCompileException.Phase.MODEL, strict.phase());
        assertTrue(strict.getMessage().contains(
                "qualified property 'stray' returns 'w::Other', which does not identify"
                        + " a unique owning end"), strict.getMessage());
        Compiler.BuiltModule module = Compiler.buildModule(Compiler.parseSources(List.of(
                new Compiler.ModelSource("m.pure", NO_UNIQUE_END))).model());
        // same message; the strict path additionally carries the element's
        // [line:col] because the error now names the association
        String wall = module.walls().get("w::ProdSynonym");
        assertTrue(wall != null && strict.getMessage().endsWith(wall),
                strict.getMessage() + " vs " + module.walls());
        // the rest of the module stands; nothing adopted the stray property
        assertFalse(carries(module.context(), "w::Product", "stray"));
        assertFalse(carries(module.context(), "w::Synonym", "stray"));
    }

    /** The names fixes, item 7 (2026-11-13, docs/rust-port/PHASE3_SHIM_DESIGN.md §10 ruling 29): a qualified property
     * returning a type that names no class is refused with the type's kind in words -- it was spelled by the record's
     * Java {@code toString()} ({@code FunctionType[parameters=[...], ...]}). The function type is set on the records, as a
     * model given as protocol records can carry it: the engine's grammar refuses a function type there ("not supported
     * yet"), and the own-corpus guards keep that grammar to the declared hosts (2026-11-13, the names fixes). */
    @Test
    @DisplayName("no unique owning end: a type that names no class is refused by its kind, never a record's toString")
    void noClassReturnTypeIsRefusedByItsKind() {
        ParsedModel parsed = NameResolver.resolve(com.legend.testing.Own.model("""
                Class a::P {}
                Class a::F {}
                Association a::PF { p: a::P[*]; f: a::F[1]; q() { [] }: a::P[1]; }
                """));
        TypeExpression.TypedParameter one = new TypeExpression.TypedParameter(new TypeExpression.NameRef("String"),
                com.legend.protocol.Multiplicity.Concrete.PURE_ONE);
        TypeExpression function = new TypeExpression.FunctionType(List.of(one), one);
        List<com.legend.model.PackageableElement> elements = new java.util.ArrayList<>();
        for (com.legend.model.PackageableElement el : parsed.elements()) {
            if (el instanceof com.legend.model.AssociationDefinition a) {
                List<com.legend.protocol.DerivedPropertyDefinition> qps = new java.util.ArrayList<>();
                for (com.legend.protocol.DerivedPropertyDefinition q : a.derivedProperties()) {
                    qps.add(new com.legend.protocol.DerivedPropertyDefinition(q.name(), q.parameters(), q.realization(),
                            function, q.multiplicity(), q.stereotypes(), q.taggedValues(), q.pos()));
                }
                el = new com.legend.model.AssociationDefinition(a.qualifiedName(), a.property1(), a.property2(), qps,
                        a.stereotypes(), a.taggedValues());
            }
            elements.add(el);
        }
        ModelException e = assertThrows(ModelException.class, () -> KnowledgeLayer.adoptAssociationQualifiedProperties(
                new ParsedModel(elements, parsed.imports()), null));
        assertEquals("association 'a::PF' qualified property 'q' returns a function type, which does not identify"
                + " a unique owning end", e.getMessage());
    }

    @Test
    @DisplayName("the normalizer refuses a model the knowledge layer has not adopted")
    void normalizerRequiresAdoption() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> com.legend.testing.Phases.normalize(
                        NameResolver.resolve(com.legend.testing.Own.model(PROD_SYNONYM))));
        assertTrue(ex.getMessage().contains("w::ProdSynonym"), ex.getMessage());
        assertTrue(ex.getMessage().contains("synonymByType"), ex.getMessage());
    }

    // ==================================================================
    // Step 3a — the knowledge kernel (subtype family)
    // ==================================================================

    private static KnowledgeLayer kernel(String src) {
        return ModelBuilder.from(NameResolver.resolve(com.legend.testing.Own.model(src))).knowledge();
    }

    @Test
    @DisplayName("kernel: a bare superclass name under an import resolves — no bare-name gap")
    void bareSuperclassNameUnderImportIsResolved() {
        KnowledgeLayer k = kernel("""
                import w::base::*;
                Class w::base::Person { name: String[1]; }
                Class w::Employee extends Person { grade: Integer[1]; }
                Class w::Manager extends w::Employee { reports: Integer[1]; }
                """);
        assertTrue(k.isSubtype("w::Employee", "w::base::Person"));
        assertTrue(k.isSubtype("w::Manager", "w::base::Person"));
        assertFalse(k.isSubtype("w::base::Person", "w::Employee"));
        assertFalse(k.isSubtype("w::Nobody", "w::base::Person"));
        // a primitive is not a class for the hierarchy (the catalog's lattice
        // node is not walked), while classDef still answers the catalog
        assertFalse(k.isSubtype("Integer", "Number"));
        assertTrue(k.hierarchyClass("Integer").isEmpty());
        assertTrue(k.hierarchyClass(null).isEmpty());
        assertEquals(List.of("w::Manager", "w::Employee", "w::base::Person"),
                List.copyOf(k.ancestorsAndSelf("w::Manager")));
        assertEquals(java.util.Set.of("w::Employee", "w::Manager"), k.subtree("w::base::Person"));
        // lineage: definitions nearest first; LOUD for an unknown class
        assertEquals(List.of("w::Manager", "w::Employee", "w::base::Person"),
                k.lineage("w::Manager").stream().map(ClassDefinition::qualifiedName).toList());
        assertThrows(IllegalStateException.class, () -> k.lineage("w::Nobody"));
        assertEquals(List.of("w::Employee"), k.directSubtypes("w::base::Person"));
    }

    private static ClassDefinition cls(String fqn, TypeExpression... supers) {
        return new ClassDefinition(fqn, List.of(), List.of(), List.of(supers), List.of(),
                List.of(), List.of(), List.of(), List.of(), false, List.of());
    }

    @Test
    @DisplayName("kernel: a generic superclass head IS a superclass; ancestorsBelow stops at the root")
    void genericSuperAndAncestorsBelow() {
        // records, not Pure text: generics are platform-dialect grammar the
        // product surface refuses, and the kernel's rule is what is pinned
        ParsedModel parsed = new ParsedModel(List.of(
                cls("w::Box"),
                cls("w::Named"),
                cls("w::Root", new TypeExpression.NameRef("w::Named")),
                cls("w::Mid", new TypeExpression.NameRef("w::Root"),
                        new TypeExpression.Generic("w::Box",
                                List.of(new TypeExpression.NameRef("String")))),
                cls("w::Leaf", new TypeExpression.NameRef("w::Mid"))),
                com.legend.model.ImportScope.empty());
        KnowledgeLayer k = ModelBuilder.from(parsed).knowledge();
        assertTrue(k.isSubtype("w::Mid", "w::Box"), "extends Box<String> is a subtype of Box");
        assertTrue(k.isSubtype("w::Leaf", "w::Named"));
        // below Root: Leaf, Mid and Mid's OTHER parent Box — never Root or Named
        assertEquals(List.of("w::Leaf", "w::Mid", "w::Box"),
                List.copyOf(k.ancestorsBelow("w::Leaf", "w::Root")));
        // memoized answers survive repeated asks
        assertTrue(k.isSubtype("w::Leaf", "w::Named"));
    }

    // ==================================================================
    // Step 3d — the store family
    // ==================================================================

    @Test
    @DisplayName("kernel: tables and columns resolve through database includes, schemas and views")
    void storeLookupsWalkIncludesSchemasAndViews() {
        KnowledgeLayer k = kernel("""
                ###Relational
                Database w::Base (
                  Schema hr ( Table EMP (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), SALARY DECIMAL(10,2)) )
                  Table DEPT (ID INTEGER PRIMARY KEY, TITLE VARCHAR(50))
                  View DeptView ( id: DEPT.ID, title: DEPT.TITLE )
                )
                Database w::Top ( include w::Base
                  Table LOCAL (ID INTEGER PRIMARY KEY, WHEN_ TIMESTAMP) )
                """);
        // include walk; table and column names compare exactly, as the engine's do
        assertTrue(k.table("w::Top", "DEPT").isPresent());
        assertTrue(k.table("w::Top", "dept").isEmpty());
        assertEquals("TITLE", k.column("w::Top", "DEPT", "TITLE").orElseThrow().name());
        assertTrue(k.column("w::Top", "DEPT", "title").isEmpty());
        // a dotted spelling names the schema's table only; a bare name reaches every schema
        assertTrue(k.table("w::Top", "hr.EMP").isPresent());
        assertTrue(k.table("w::Top", "EMP").isPresent());
        // a WRONG schema qualifier resolves nothing (the engine refuses it)
        assertTrue(k.table("w::Top", "sales.EMP").isEmpty());
        assertTrue(k.table("w::Top", "default.DEPT").isPresent());
        assertTrue(k.table("w::Top", null).isEmpty());
        assertTrue(k.table("w::Nowhere", "DEPT").isEmpty());
        // kinds: a physical column's, a view column's through its ColumnRef, a local table's
        assertEquals("Decimal", k.columnKind("w::Base", "hr.EMP", "SALARY"));
        assertEquals("String", k.columnKind("w::Base", "DeptView", "title"));
        assertEquals("DateTime", k.columnKind("w::Top", "LOCAL", "WHEN_"));
        assertEquals(null, k.columnKind("w::Base", "DeptView", "nope"));
    }

    /** The names fixes, item 13 (2026-11-13, docs/rust-port/PHASE3_SHIM_DESIGN.md §10 ruling 28): {@code columnKind}'s
     * walk keys each step on its (database, table, column) triple. A schema view {@code S.V}'s column {@code c} reading
     * a top-level view {@code S}'s column {@code V.c} is two steps; the joined text {@code a::Db@S.V.c} made them one
     * and the walk answered nothing. A text cannot name that column (a dotted name is quoted, and a definition keeps
     * its quotes where a reference drops them); a model given as protocol records can, so the column is renamed as such
     * a model carries it. */
    @Test
    @DisplayName("kernel: the column-kind walk keys each step on its triple")
    void columnKindWalkKeysEachStepOnItsTriple() {
        ParsedModel parsed = NameResolver.resolve(com.legend.testing.Own.model("""
                ###Relational
                Database a::Db (
                  Table T (ID INTEGER PRIMARY KEY, X VARCHAR(10))
                  View S (Vc: T.X)
                  Schema S ( View V (c: default.S."V.c") )
                )
                """));
        List<com.legend.model.PackageableElement> elements = new java.util.ArrayList<>();
        for (com.legend.model.PackageableElement el : parsed.elements()) {
            if (el instanceof com.legend.model.DatabaseDefinition db) {
                List<com.legend.model.DatabaseDefinition.ViewDefinition> views = new java.util.ArrayList<>();
                for (com.legend.model.DatabaseDefinition.ViewDefinition v : db.views()) {
                    if (!v.name().equals("S")) {
                        views.add(v);
                        continue;
                    }
                    List<com.legend.model.DatabaseDefinition.ViewDefinition.ViewColumnMapping> cms =
                            new java.util.ArrayList<>();
                    for (var cm : v.columnMappings()) {
                        cms.add(new com.legend.model.DatabaseDefinition.ViewDefinition.ViewColumnMapping(
                                "V.c", cm.targetSetId(), cm.expression(), cm.primaryKey()));
                    }
                    views.add(new com.legend.model.DatabaseDefinition.ViewDefinition(v.name(), v.filter(),
                            v.groupByColumns(), v.distinct(), cms));
                }
                el = new com.legend.model.DatabaseDefinition(db.qualifiedName(), db.includes(), db.schemas(),
                        db.tables(), views, db.tabularFunctions(), db.joins(), db.filters(), db.multiGrainFilters());
            }
            elements.add(el);
        }
        KnowledgeLayer k = ModelBuilder.from(new ParsedModel(elements, parsed.imports())).knowledge();
        assertEquals("String", k.columnKind("a::Db", "S", "V.c"));
        assertEquals("String", k.columnKind("a::Db", "S.V", "c"), "two triples that join alike are two steps");
    }

    /** The names fixes, item 8 (2026-11-13, docs/rust-port/PHASE3_SHIM_DESIGN.md §10 ruling 29): the type inference
     * refuses a view-on-view cycle by name, worded as {@code ViewSignatures}' own refusal -- a computed column whose
     * expression reaches its own view through a column reference recursed until {@code StackOverflowError}. One column
     * read twice side by side is no cycle. */
    @Test
    @DisplayName("kernel: the type inference refuses a view-on-view cycle by name")
    void typeInferenceRefusesAViewCycleByName() {
        ModelBuilder ix = ModelBuilder.from(NameResolver.resolve(com.legend.testing.Own.model("""
                ###Relational
                Database a::Db (
                  Table T (ID INTEGER PRIMARY KEY, S VARCHAR(10))
                  View P (A: concat(Q.B, 'x'))
                  View Q (B: concat(P.A, 'y'))
                  View R (C: concat(T.S, T.S), D: concat(R2.E, R2.E))
                  View R2 (E: concat(T.S, 'z'))
                )
                """)));
        KnowledgeLayer k = ix.knowledge();
        ModelException e = assertThrows(ModelException.class, () -> k.columnKind("a::Db", "P", "A"));
        assertEquals(LegendCompileException.Phase.MODEL, e.phase());
        assertEquals("view 'Q' of 'a::Db' reads its own columns (cyclic view-on-view chain)", e.getMessage());
        com.legend.model.DatabaseDefinition db = ix.findDatabase("a::Db").orElseThrow();
        com.legend.model.RelationalOperation q = ix.findView("a::Db", "Q").orElseThrow().columnMappings().get(0)
                .expression();
        ModelException e2 = assertThrows(ModelException.class,
                () -> com.legend.compiler.element.RelationalTypeInference.infer(q, db, ix));
        assertEquals("view 'P' of 'a::Db' reads its own columns (cyclic view-on-view chain)", e2.getMessage());
        assertEquals("String", k.columnKind("a::Db", "R", "C"));
        assertEquals("String", k.columnKind("a::Db", "R", "D"), "one column read twice side by side is no cycle");
    }

    /** The names fixes, items 9 to 11 (2026-11-13, docs/rust-port/PHASE3_SHIM_DESIGN.md §10 ruling 28): the type
     * inference's engine rules (relationalExtension.pure). A decimal is safe with the integer and float kinds alone (with
     * a string there is no safe type: it beat any type); {@code sub} with no typed operand has no type (it defaulted to
     * INTEGER); {@code max} and {@code min} over several arguments are the safe type over all of them when one exists,
     * else the first's (the first's alone). */
    @Test
    @DisplayName("kernel: the type inference keeps the engine's safe-type rules")
    void typeInferenceKeepsTheEnginesSafeTypeRules() {
        ModelBuilder ix = ModelBuilder.from(NameResolver.resolve(com.legend.testing.Own.model("""
                ###Relational
                Database a::Db (
                  Table T (ID INTEGER PRIMARY KEY, D DECIMAL(10,2), S VARCHAR(10), I INTEGER)
                  View V (CD: case(equal(T.ID, 1), T.D, T.S), CI: case(equal(T.ID, 1), T.D, T.I), \
                SB: sub(nope(T.ID), nope(T.ID)), MX: max(T.I, T.D), MS: max(T.I, T.S), MN: min(T.D))
                )
                """)));
        com.legend.model.DatabaseDefinition db = ix.findDatabase("a::Db").orElseThrow();
        var cms = ix.findView("a::Db", "V").orElseThrow().columnMappings();
        java.util.function.IntFunction<com.legend.model.RelationalDataType> infer =
                i -> com.legend.compiler.element.RelationalTypeInference.infer(cms.get(i).expression(), db, ix);
        com.legend.model.RelationalDataType dec = new com.legend.model.RelationalDataType.Decimal(10, 2);
        assertEquals(null, infer.apply(0), "a decimal against a string has no safe type");
        assertEquals(dec, infer.apply(1), "a decimal beats an integer");
        assertEquals(null, infer.apply(2), "sub with no typed operand has no type");
        assertEquals(dec, infer.apply(3), "max over two: their safe type");
        assertEquals(new com.legend.model.RelationalDataType.Integer_(), infer.apply(4),
                "max over two with no safe type: the first's");
        assertEquals(dec, infer.apply(5), "min over one: its own");
    }

    /** S378 (2026-11-13, the names landings, docs/rust-port/PHASE3_SHIM_DESIGN.md §10 ruling 32): the type inference's
     * safe type is the engine's safeTypeMap, isSafeTypePossible and getSafeType exactly (relationalExtension.pure
     * :1926-2033): a FLOAT with an INTEGER is FLOAT (the integer lists it), a BIGINT with a FLOAT has none (neither lists
     * the other), two CHARs or a CHAR and a VARCHAR are a VARCHAR of the larger size, two equal NUMERICs a DECIMAL;
     * count is BIGINT; substring and its family a VARCHAR sized by their first argument's CHAR or VARCHAR, no type for
     * another; sub its first argument's type when its first two share a kind, else none; group_concat no rule. */
    @Test
    @DisplayName("kernel: the type inference's safe type and rules are the engine's")
    void typeInferenceSafeTypeAndRulesAreTheEngines() {
        ModelBuilder ix = ModelBuilder.from(NameResolver.resolve(com.legend.testing.Own.model("""
                ###Relational
                Database a::Db (
                  Table T (ID INTEGER PRIMARY KEY, F FLOAT, B BIGINT, C CHAR(3), K CHAR(5), V VARCHAR(4), \
                N NUMERIC(6,2), I INTEGER)
                  View V (FI: case(equal(T.ID, 1), T.F, T.I), BF: case(equal(T.ID, 1), T.B, T.F), \
                CC: case(equal(T.ID, 1), T.C, T.K), CV: case(equal(T.ID, 1), T.C, T.V), \
                NN: case(equal(T.ID, 1), T.N, T.N), CT: count(T.ID), SC: substring(T.C, 1, 2), \
                SI: substring(T.I, 1, 2), SV: sub(T.V, T.V), SM: sub(T.V, T.I), GC: group_concat(T.V))
                )
                """)));
        com.legend.model.DatabaseDefinition db = ix.findDatabase("a::Db").orElseThrow();
        var cms = ix.findView("a::Db", "V").orElseThrow().columnMappings();
        java.util.function.IntFunction<com.legend.model.RelationalDataType> infer =
                i -> com.legend.compiler.element.RelationalTypeInference.infer(cms.get(i).expression(), db, ix);
        assertEquals(new com.legend.model.RelationalDataType.Float_(), infer.apply(0), "FLOAT with INTEGER: FLOAT");
        assertEquals(null, infer.apply(1), "BIGINT with FLOAT: no safe type");
        assertEquals(new com.legend.model.RelationalDataType.Varchar(5), infer.apply(2), "two CHARs: VARCHAR(5)");
        assertEquals(new com.legend.model.RelationalDataType.Varchar(4), infer.apply(3), "CHAR(3), VARCHAR(4): VARCHAR(4)");
        assertEquals(new com.legend.model.RelationalDataType.Decimal(6, 2), infer.apply(4), "two NUMERICs: DECIMAL");
        assertEquals(new com.legend.model.RelationalDataType.BigInt(), infer.apply(5), "count: BIGINT");
        assertEquals(new com.legend.model.RelationalDataType.Varchar(3), infer.apply(6), "substring of a CHAR(3)");
        assertEquals(null, infer.apply(7), "substring of an INTEGER: no type");
        assertEquals(new com.legend.model.RelationalDataType.Varchar(4), infer.apply(8), "sub of two VARCHARs: the first's");
        assertEquals(null, infer.apply(9), "sub of a VARCHAR and an INTEGER: no rule");
        assertEquals(null, infer.apply(10), "group_concat: no rule");
    }

    /** S378's remainder (2026-11-13, the names follow-ups, docs/rust-port/PHASE3_SHIM_DESIGN.md §10 ruling 34): the
     * operators, {@code sum}, {@code average}, a float literal and {@code upper}/{@code lower} typed by the engine's own
     * rules (relationalExtension.pure): {@code plus} over one argument widens it (:1408-1436), {@code plus} and
     * {@code minus} over several are their first argument's type when a safe type is possible (:1408-1436, :1209-1218),
     * {@code times} the safe type of its first two (:1773-1782), {@code divide} and {@code average} DOUBLE (:693-702,
     * :291-300), {@code sum} by its first argument's kind, a DECIMAL or NUMERIC capped at precision 31, a FLOAT a DOUBLE
     * and no rule for a REAL (:1717-1741), a float literal FLOAT (:148), {@code upper} and {@code lower} no rule (a view column
     * {@code Any[0..1]}). Each was the safe type over every argument, its first argument's type, or DOUBLE. */
    @Test
    @DisplayName("kernel: the operators, sum, average, a float literal and upper/lower are typed by the engine's rules")
    void theOperatorsAndAggregatesAreTypedByTheEnginesRules() {
        ModelBuilder ix = ModelBuilder.from(NameResolver.resolve(com.legend.testing.Own.model("""
                ###Relational
                Database a::Db (
                  Table T (ID INTEGER PRIMARY KEY, S SMALLINT, B BIGINT, F FLOAT, D DECIMAL(40,2), N NUMERIC(6,2), \
                V VARCHAR(4), R REAL)
                  View V (P1: plus(T.S), P2: plus(T.ID), P3: plus(T.D), P4: plus(T.F), P5: plus(T.V), \
                P6: plus(T.ID, T.F), P7: plus(T.B, T.F), M1: minus(T.N, T.ID), M2: minus(T.ID), \
                T1: times(T.ID, T.F, T.V), T2: times(T.ID), D1: divide(T.ID, T.ID), A1: average(T.V), S1: sum(T.S), \
                S2: sum(T.B), S3: sum(T.D), S4: sum(T.F), L1: case(equal(T.ID, 1), 1.5, 2.5), U1: upper(T.V), \
                U2: lower(T.V), S5: sum(T.R))
                )
                """)));
        com.legend.model.DatabaseDefinition db = ix.findDatabase("a::Db").orElseThrow();
        var cms = ix.findView("a::Db", "V").orElseThrow().columnMappings();
        java.util.function.IntFunction<com.legend.model.RelationalDataType> infer =
                i -> com.legend.compiler.element.RelationalTypeInference.infer(cms.get(i).expression(), db, ix);
        assertEquals(new com.legend.model.RelationalDataType.Integer_(), infer.apply(0), "plus of a SMALLINT: INTEGER");
        assertEquals(new com.legend.model.RelationalDataType.BigInt(), infer.apply(1), "plus of an INTEGER: BIGINT");
        assertEquals(new com.legend.model.RelationalDataType.Decimal(31, 2), infer.apply(2),
                "plus of a DECIMAL(40,2): capped at 31");
        assertEquals(new com.legend.model.RelationalDataType.Double_(), infer.apply(3), "plus of a FLOAT: DOUBLE");
        assertEquals(null, infer.apply(4), "plus of a VARCHAR: no rule");
        assertEquals(new com.legend.model.RelationalDataType.Integer_(), infer.apply(5),
                "plus of an INTEGER and a FLOAT: the first's");
        assertEquals(null, infer.apply(6), "plus of a BIGINT and a FLOAT: no safe type, no rule");
        assertEquals(new com.legend.model.RelationalDataType.Numeric(6, 2), infer.apply(7),
                "minus of a NUMERIC and an INTEGER: the first's");
        assertEquals(null, infer.apply(8), "minus of one argument: no rule");
        assertEquals(new com.legend.model.RelationalDataType.Float_(), infer.apply(9),
                "times: the safe type of the first two, the third unread");
        assertEquals(null, infer.apply(10), "times of one argument: no type");
        assertEquals(new com.legend.model.RelationalDataType.Double_(), infer.apply(11), "divide: DOUBLE");
        assertEquals(new com.legend.model.RelationalDataType.Double_(), infer.apply(12), "average: DOUBLE");
        assertEquals(new com.legend.model.RelationalDataType.Integer_(), infer.apply(13), "sum of a SMALLINT: INTEGER");
        assertEquals(new com.legend.model.RelationalDataType.BigInt(), infer.apply(14), "sum of a BIGINT: BIGINT");
        assertEquals(new com.legend.model.RelationalDataType.Decimal(31, 2), infer.apply(15),
                "sum of a DECIMAL(40,2): capped at 31");
        assertEquals(new com.legend.model.RelationalDataType.Double_(), infer.apply(16), "sum of a FLOAT: DOUBLE");
        assertEquals(new com.legend.model.RelationalDataType.Float_(), infer.apply(17), "float literals: FLOAT");
        assertEquals(null, infer.apply(18), "upper: no rule");
        assertEquals(null, infer.apply(19), "lower: no rule");
        assertEquals(null, infer.apply(20), "sum of a REAL: no rule");
        var signature = com.legend.compiler.element.ViewSignatures.of(ix, "a::Db", ix.findView("a::Db", "V")
                .orElseThrow()).columns();
        assertEquals("U1:meta::pure::metamodel::type::Any[0..1]", signature.get(18).text(), "a column no rule types: Any[0..1]");
        assertEquals("L1:Float[0..1]", signature.get(17).text());
    }

    /** S376's join terminal (2026-11-13, the names follow-ups, docs/rust-port/PHASE3_SHIM_DESIGN.md §10 ruling 34): a join
     * navigation's terminal is compiled as any other operation once the navigation's joins resolve
     * (HelperRelationalBuilder.java:1181-1186), in the database of its last join (RelationalParseTreeWalker.java
     * :1024-1044), so a terminal reading a column or table its database lacks refuses the view in the engine's words,
     * plain, nested in the terminal, or with the navigation inside a function; a navigation whose join is missing is left
     * to the lift, which refuses the join first, as the engine resolves the joins first. The terminal was not checked. */
    @Test
    @DisplayName("kernel: a view's join terminal reading a column its database has not is refused once its joins resolve")
    void aViewsJoinTerminalReadingAMissingColumnIsRefused() {
        ModelBuilder ix = ModelBuilder.from(NameResolver.resolve(com.legend.testing.Own.model("""
                ###Relational
                Database b::Far (
                  Table T3 (ID INTEGER PRIMARY KEY, C VARCHAR(10))
                  Table T4 (ID INTEGER PRIMARY KEY)
                  Join K(T4.ID = T3.ID)
                )
                Database a::Main (
                  Table T (ID INTEGER PRIMARY KEY, S VARCHAR(10))
                  Table T2 (ID INTEGER PRIMARY KEY, C VARCHAR(10))
                  Join J(T.ID = T2.ID)
                  View V1 (X: T.ID PRIMARY KEY, Y: @J | T2.NOPE)
                  View V2 (X: T.ID PRIMARY KEY, Y: @J | U.C)
                  View V3 (X: T.ID PRIMARY KEY, Y: @J | concat(T2.C, T2.NOPE))
                  View V4 (X: T.ID PRIMARY KEY, Y: concat(T.S, @J | T2.NOPE))
                  View V5 (X: T.ID PRIMARY KEY, Y: @Nope | T2.NOPE)
                  View V6 (X: T.ID PRIMARY KEY, Y: [a::Main]@J > [b::Far]@K | T3.C)
                  View V7 (X: T.ID PRIMARY KEY, Y: [a::Main]@J > [b::Far]@K | T3.NOPE)
                  View V8 (X: T.ID PRIMARY KEY, Y: @J | T2.C)
                )
                """)));
        java.util.function.Function<String, String> signature = name -> {
            try {
                return com.legend.compiler.element.ViewSignatures.of(ix, "a::Main",
                        ix.findView("a::Main", name).orElseThrow()).columns().stream()
                        .map(com.legend.compiler.element.type.Type.Column::text)
                        .collect(java.util.stream.Collectors.joining(", "));
            } catch (com.legend.error.ModelException e) {
                return "REFUSED " + e.getMessage();
            }
        };
        assertTrue(signature.apply("V1").endsWith("Can't find column 'NOPE'"), signature.apply("V1"));
        assertTrue(signature.apply("V2").endsWith("Can't find table 'U' in schema 'default' and database 'Main'"),
                signature.apply("V2"));
        assertTrue(signature.apply("V3").endsWith("Can't find column 'NOPE'"), signature.apply("V3"));
        assertTrue(signature.apply("V4").endsWith("Can't find column 'NOPE'"), signature.apply("V4"));
        assertEquals("X:Integer[1], Y:meta::pure::metamodel::type::Any[0..1]", signature.apply("V5"),
                "a missing join is the lift's to refuse, first");
        assertEquals("X:Integer[1], Y:String[0..1]", signature.apply("V6"), "the terminal reads its last join's database");
        assertTrue(signature.apply("V7").endsWith("Can't find column 'NOPE'"), signature.apply("V7"));
        assertEquals("X:Integer[1], Y:String[0..1]", signature.apply("V8"));
    }

    /** S376 (2026-11-13, the names landings, docs/rust-port/PHASE3_SHIM_DESIGN.md §10 ruling 32): a view column whose
     * operation reads a column or a table its database does not have is refused in the engine's words, as the engine
     * refuses it when the database compiles (HelperRelationalBuilder.getColumn :160-165, getRelation :284-293), the
     * plain column mapping's and a nested reference alike (a column found nowhere was {@code Any[0..1]}); an aggregate
     * is typed at any arity as the engine's inferRelationalType types it (it was recognized at one argument only). */
    @Test
    @DisplayName("kernel: a view reading a column its database has not is refused; an aggregate at any arity")
    void aViewReadingAMissingColumnIsRefusedAndAnAggregateIsTypedAtAnyArity() {
        ModelBuilder ix = ModelBuilder.from(NameResolver.resolve(com.legend.testing.Own.model("""
                ###Relational
                Database a::Db (
                  Table T (ID INTEGER PRIMARY KEY, S VARCHAR(10))
                  View V1 (X: T.NOPE)
                  View V2 (X: concat(T.S, T.NOPE))
                  View V3 (X: U.ID)
                  View V4 (C0: count(), C2: count(T.ID, T.S), M2: max(T.ID, T.ID), A2: average(T.ID, T.ID))
                )
                """)));
        java.util.function.Function<String, String> signature = name -> {
            try {
                return com.legend.compiler.element.ViewSignatures.of(ix, "a::Db",
                        ix.findView("a::Db", name).orElseThrow()).columns().stream()
                        .map(com.legend.compiler.element.type.Type.Column::text)
                        .collect(java.util.stream.Collectors.joining(", "));
            } catch (com.legend.error.ModelException e) {
                return "REFUSED " + e.getMessage();
            }
        };
        assertTrue(signature.apply("V1").endsWith("Can't find column 'NOPE'"), signature.apply("V1"));
        assertTrue(signature.apply("V2").endsWith("Can't find column 'NOPE'"), signature.apply("V2"));
        assertTrue(signature.apply("V3").endsWith("Can't find table 'U' in schema 'default' and database 'Db'"),
                signature.apply("V3"));
        assertEquals("C0:Integer[1], C2:Integer[1], M2:Integer[0..1], A2:Float[1]", signature.apply("V4"));
    }

    /** The names fixes, item 10 (2026-11-13, ruling 28): a concatenation with an operand typed other than CHAR or
     * VARCHAR has no type -- the engine's one concat rule asks every typed operand to be one -- and an operand with no
     * type drops out. It summed the string operands' sizes and skipped the rest ({@code concat(INT, 'ab')} was
     * VARCHAR(2)). */
    @Test
    @DisplayName("kernel: a concatenation of a non-string has no type")
    void aConcatenationOfANonStringHasNoType() {
        ModelBuilder ix = ModelBuilder.from(NameResolver.resolve(com.legend.testing.Own.model("""
                ###Relational
                Database a::Db (
                  Table T (ID INTEGER PRIMARY KEY, S VARCHAR(10), I INTEGER)
                  View V (CI: concat(T.S, T.I), CN: concat(T.S, nope(T.ID)), CS: concat(T.S, 'ab'))
                )
                """)));
        com.legend.model.DatabaseDefinition db = ix.findDatabase("a::Db").orElseThrow();
        var cms = ix.findView("a::Db", "V").orElseThrow().columnMappings();
        java.util.function.IntFunction<com.legend.model.RelationalDataType> infer =
                i -> com.legend.compiler.element.RelationalTypeInference.infer(cms.get(i).expression(), db, ix);
        assertEquals(null, infer.apply(0), "an INTEGER operand leaves no type");
        assertEquals(new com.legend.model.RelationalDataType.Varchar(10), infer.apply(1),
                "an operand with no type drops out");
        assertEquals(new com.legend.model.RelationalDataType.Varchar(12), infer.apply(2));
    }
}
