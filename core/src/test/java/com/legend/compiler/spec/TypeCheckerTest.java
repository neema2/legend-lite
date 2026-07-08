package com.legend.compiler.spec;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedAggregate;
import com.legend.compiler.spec.typed.TypedAsOfJoin;
import com.legend.compiler.spec.typed.TypedMatch;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedEval;
import com.legend.compiler.spec.typed.FoldStrategy;
import com.legend.compiler.spec.typed.TypedFlatten;
import com.legend.compiler.spec.typed.TypedFold;
import com.legend.compiler.spec.typed.TypedExtendAgg;
import com.legend.compiler.spec.typed.TypedExtendWindow;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedWrite;
import com.legend.compiler.spec.typed.TypedPivot;
import com.legend.compiler.spec.typed.TypedSourceUrl;
import com.legend.compiler.spec.typed.TypedTds;
import com.legend.compiler.spec.typed.TypedMatch;
import com.legend.compiler.spec.typed.TypedExtend;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.SpecParser;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <strong>Ported from engine's {@code compiler/TypeCheckerTest.java}</strong> &mdash; the
 * type-only behavioral spec for the relational ({@code #>{db.TABLE}#}) source and the
 * relation transformers that consume it (filter / sort / rename / limit), per
 * PHASE_G_SPEC_COMPILER.md &sect;12.
 *
 * <p><strong>Adaptations to core's idiom.</strong> Engine hand-builds a {@code Table}
 * and an inline {@code ModelContext.findTable}; core compiles from source, so the table
 * is declared as a {@code Database} grammar element and resolved by <em>FQN</em>
 * ({@code #>{test::PersonDatabase.T_PERSON}#}). Engine asserts on {@code unit.hir().schema()}
 * (a {@code Type.Schema} keyed by column name); core's relation value carries a bare
 * {@link Type.RelationType} (doc &sect;G-&alpha;), so we assert on its ordered columns.
 *
 * <p><strong>TDD ledger.</strong> The assertions whose features are implemented run live
 * ({@code tableReference} + relation {@code filter}). The rest are {@link Disabled} stubs
 * naming the &sect;12 row that unblocks them &mdash; remove {@code @Disabled} as each
 * transformer lands. Engine's two SQL-generation tests ({@code filterGeneratesValidSql},
 * {@code limitGeneratesSql}) are Phase I/J, not this type-only phase, and are intentionally
 * omitted here.
 */
class TypeCheckerTest {

    /** Engine's PERSON_TABLE, declared in core's {@code Database} grammar (FQN-resolved). */
    private static final String DB_MODEL = """
            Database test::PersonDatabase
            (
              Table T_PERSON
              (
                FIRST_NAME VARCHAR(200) NOT NULL,
                LAST_NAME VARCHAR(200) NOT NULL,
                AGE INTEGER NOT NULL,
                SALARY DOUBLE,
                HIRE_DATE DATE,
                ACTIVE BOOLEAN
              )
              Table T_FIRM
              (
                ID INTEGER NOT NULL,
                LEGAL_NAME VARCHAR(200) NOT NULL
              )
              Table T_EVENTS
              (
                ID INTEGER NOT NULL,
                PAYLOAD SEMISTRUCTURED
              )
            )

            Class test::Person
            {
              name: String[1];
              age: Integer[1];
              nicknames: String[*];
              firm: test::Firm[1];
            }

            Class test::Firm
            {
              legalName: String[1];
            }

            function test::inc(x: Integer[1]): Integer[1]
            {
              $x + 1
            }
            """;

    private static final String T_PERSON = "#>{test::PersonDatabase.T_PERSON}#";

    private static TypedSpec typeQuery(String query) {
        ModelContext ctx = Compiler.compileModel(DB_MODEL);
        return new SpecCompiler(ctx).typeBody(SpecParser.parse(query), Env.empty(), Expected.infer());
    }

    private static Type.RelationType schemaOf(TypedSpec node) {
        assertInstanceOf(Type.RelationType.class, node.info().type(),
                "a relation value must carry a bare RelationType (doc §G-α)");
        return (Type.RelationType) node.info().type();
    }

    private static Type columnType(Type.RelationType rt, String name) {
        return rt.columns().stream()
                .filter(c -> c.name().equals(name))
                .map(Type.Column::type)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no column '" + name + "' in " + rt.typeName()));
    }

    private static boolean hasColumn(Type.RelationType rt, String name) {
        return rt.columns().stream().anyMatch(c -> c.name().equals(name));
    }

    // ========== Type Resolution ==========

    @Test
    void tableAccessResolvesTableSchema() {
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON));
        assertEquals(6, rt.columns().size(), "Should have 6 columns");
        assertEquals(Type.Primitive.STRING, columnType(rt, "FIRST_NAME"));
        assertEquals(Type.Primitive.STRING, columnType(rt, "LAST_NAME"));
        assertEquals(Type.Primitive.INTEGER, columnType(rt, "AGE"));
        assertEquals(Type.Primitive.FLOAT, columnType(rt, "SALARY"));
        assertEquals(Type.Primitive.STRICT_DATE, columnType(rt, "HIRE_DATE"));
        assertEquals(Type.Primitive.BOOLEAN, columnType(rt, "ACTIVE"));
    }

    @Test
    void tableAccessColumnMultiplicityReflectsNullability() {
        // Engine's Schema drops per-column multiplicity; core keeps it (pure SQL transpiler,
        // doc §8). NOT NULL ⇒ [1], nullable ⇒ [0..1].
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON));
        assertEquals(Multiplicity.Bounded.ONE,
                rt.columns().stream().filter(c -> c.name().equals("AGE")).findFirst().orElseThrow().multiplicity());
        assertEquals(Multiplicity.Bounded.ZERO_ONE,
                rt.columns().stream().filter(c -> c.name().equals("SALARY")).findFirst().orElseThrow().multiplicity());
    }

    @Test
    void filterPreservesSourceType() {
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON + "->filter(x|$x.AGE > 25)"));
        assertEquals(6, rt.columns().size(), "Filter doesn't change column set");
        assertEquals(Type.Primitive.INTEGER, columnType(rt, "AGE"));
        assertEquals(Type.Primitive.STRING, columnType(rt, "FIRST_NAME"));
    }

    @Test
    void filterRejectsInvalidColumn() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery(T_PERSON + "->filter(x|$x.NONEXISTENT > 25)"),
                "Should reject reference to non-existent column");
    }

    @Test
    void unknownDatabaseFailsLoudly() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery("#>{test::NoSuchDb.T_PERSON}#"));
    }

    @Test
    void unknownTableFailsLoudly() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery("#>{test::PersonDatabase.NO_SUCH_TABLE}#"));
    }

    // ========== Deferred: §12 transformers not yet implemented ==========

    @Test
    void sortValidatesColumns() {
        assertEquals(6, schemaOf(typeQuery(T_PERSON + "->sort(asc(~AGE))")).columns().size());
    }

    @Test
    void sortRejectsInvalidColumn() {
        assertThrows(TypeInferenceException.class, () -> typeQuery(T_PERSON + "->sort(asc(~NONEXISTENT))"));
    }

    @Test
    void renameChangesColumnName() {
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON + "->rename(~FIRST_NAME, ~NAME)"));
        assertFalse(hasColumn(rt, "FIRST_NAME"), "Old column name should be gone");
        assertTrue(hasColumn(rt, "NAME"), "New column name should exist");
        assertEquals(Type.Primitive.STRING, columnType(rt, "NAME"));
    }

    @Test
    void sortMultipleKeysAccumulateAndPreserveSchema() {
        // Both keys flow through ONE generic check: the collection's LUB is
        // SortInfo<(FIRST_NAME:?, AGE:?)>, whose X⊆T accumulates both columns.
        TypedSpec n = typeQuery(T_PERSON + "->sort([asc(~FIRST_NAME), desc(~AGE)])");
        TypedSort sort = assertInstanceOf(TypedSort.class, n);
        assertEquals(java.util.List.of(
                        new TypedSort.TypedSortKey("FIRST_NAME", true),
                        new TypedSort.TypedSortKey("AGE", false)),
                sort.keys());
        assertEquals(6, schemaOf(n).columns().size(), "sort preserves the schema");
    }

    @Test
    void sortBareColSpecDefaultsAscending() {
        // `~col` desugars to asc(~col) (engine SortChecker's default direction).
        TypedSort sort = assertInstanceOf(TypedSort.class, typeQuery(T_PERSON + "->sort(~AGE)"));
        assertEquals(java.util.List.of(new TypedSort.TypedSortKey("AGE", true)), sort.keys());
    }

    @Test
    void sortMixedBareAndDirectedKeys() {
        TypedSort sort = assertInstanceOf(TypedSort.class,
                typeQuery(T_PERSON + "->sort([~FIRST_NAME, desc(~AGE)])"));
        assertEquals(java.util.List.of(
                        new TypedSort.TypedSortKey("FIRST_NAME", true),
                        new TypedSort.TypedSortKey("AGE", false)),
                sort.keys());
    }

    @Test
    void sortRejectsInvalidColumnAmongValidOnes() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery(T_PERSON + "->sort([asc(~FIRST_NAME), desc(~NONEXISTENT)])"));
    }

    @Test
    void renameArrayFormRenamesEachPair() {
        // ~[old…],~[new…] desugars to a chain of scalar renames (the array signature
        // carries no K, so pairing is positional — engine RenameChecker semantics).
        Type.RelationType rt = schemaOf(typeQuery(
                T_PERSON + "->rename(~[FIRST_NAME, LAST_NAME], ~[FN, LN])"));
        assertFalse(hasColumn(rt, "FIRST_NAME"));
        assertFalse(hasColumn(rt, "LAST_NAME"));
        assertEquals(Type.Primitive.STRING, columnType(rt, "FN"));
        assertEquals(Type.Primitive.STRING, columnType(rt, "LN"));
        assertEquals(6, rt.columns().size(), "rename neither adds nor drops columns");
    }

    @Test
    void renamePreservesColumnMultiplicity() {
        // SALARY is nullable ([0..1]); the shared K carries its type AND (via the
        // shadow-mult binding) its multiplicity onto the new column.
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON + "->rename(~SALARY, ~PAY)"));
        Type.Column pay = rt.columns().stream()
                .filter(c -> c.name().equals("PAY")).findFirst().orElseThrow();
        assertEquals(Type.Primitive.FLOAT, pay.type());
        assertEquals(Multiplicity.Bounded.ZERO_ONE, pay.multiplicity());
    }

    @Test
    void renameRejectsUnknownOldColumn() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery(T_PERSON + "->rename(~NONEXISTENT, ~X)"));
    }

    @Test
    void limitPreservesType() {
        assertEquals(6, schemaOf(typeQuery(T_PERSON + "->limit(10)")).columns().size());
    }

    @Test
    void selectSingleColumnNarrowsSchema() {
        // select<T,Z>(r, ColSpec<Z⊆T>):Relation<Z> — Z is the concrete looked-up column.
        TypedSpec n = typeQuery(T_PERSON + "->select(~FIRST_NAME)");
        TypedSelect sel = assertInstanceOf(TypedSelect.class, n);
        assertEquals(java.util.List.of("FIRST_NAME"), sel.columns());
        Type.RelationType rt = schemaOf(n);
        assertEquals(1, rt.columns().size());
        assertEquals(Type.Primitive.STRING, columnType(rt, "FIRST_NAME"));
    }

    @Test
    void selectColumnArrayNarrowsSchemaInOrder() {
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON + "->select(~[AGE, FIRST_NAME])"));
        assertEquals(java.util.List.of("AGE", "FIRST_NAME"),
                rt.columns().stream().map(Type.Column::name).toList());
        assertEquals(Type.Primitive.INTEGER, columnType(rt, "AGE"));
    }

    @Test
    void selectAllKeepsSchema() {
        assertEquals(6, schemaOf(typeQuery(T_PERSON + "->select()")).columns().size());
    }

    @Test
    void selectRejectsUnknownColumn() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery(T_PERSON + "->select(~NONEXISTENT)"));
    }

    @Test
    void distinctWholeRowKeepsSchema() {
        TypedSpec n = typeQuery(T_PERSON + "->distinct()");
        assertInstanceOf(TypedDistinct.class, n);
        assertEquals(6, schemaOf(n).columns().size());
    }

    @Test
    void distinctOnColumnsNarrowsToThem() {
        // distinct<X,T>(r, ColSpecArray<X⊆T>):Relation<X> — dedup keys become the schema.
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON + "->distinct(~[FIRST_NAME, LAST_NAME])"));
        assertEquals(java.util.List.of("FIRST_NAME", "LAST_NAME"),
                rt.columns().stream().map(Type.Column::name).toList());
    }

    @Test
    void concatenateSameSchemaPreservesIt() {
        TypedSpec n = typeQuery(T_PERSON + "->concatenate(" + T_PERSON + ")");
        assertInstanceOf(TypedConcatenate.class, n);
        assertEquals(6, schemaOf(n).columns().size());
    }

    @Test
    void concatenateMismatchedSchemasThrows() {
        // The shared T cannot bind two different row-structs.
        assertThrows(TypeInferenceException.class, () -> typeQuery(
                T_PERSON + "->select(~FIRST_NAME)->concatenate(" + T_PERSON + ")"));
    }

    @Test
    void takeDropSlicePreserveSchemaAndEmitNodes() {
        assertInstanceOf(TypedLimit.class, typeQuery(T_PERSON + "->take(3)"));
        assertInstanceOf(TypedLimit.class, typeQuery(T_PERSON + "->limit(3)"));
        assertInstanceOf(TypedDrop.class, typeQuery(T_PERSON + "->drop(2)"));
        TypedSpec sliced = typeQuery(T_PERSON + "->slice(1, 4)");
        assertInstanceOf(TypedSlice.class, sliced);
        assertEquals(6, schemaOf(sliced).columns().size());
    }

    @Test
    void extendAppendsComputedColumn() {
        // extend<T,Z>(r, FuncColSpec<{T[1]->Any[0..1]},Z>):Relation<T+Z> — Z's type comes
        // from type-checking the lambda against the source row; output is resolve(T+Z).
        TypedSpec n = typeQuery(T_PERSON + "->extend(~AGE_NEXT:x|$x.AGE + 1)");
        TypedExtend ext = assertInstanceOf(TypedExtend.class, n);
        assertEquals(1, ext.columns().size());
        assertEquals("AGE_NEXT", ext.columns().get(0).name());
        Type.RelationType rt = schemaOf(n);
        assertEquals(7, rt.columns().size(), "T+Z appends the new column");
        assertEquals("AGE_NEXT", rt.columns().get(6).name());
        assertEquals(Type.Primitive.INTEGER, columnType(rt, "AGE_NEXT"));
    }

    @Test
    void extendArrayAppendsEachColumn() {
        Type.RelationType rt = schemaOf(typeQuery(
                T_PERSON + "->extend(~[A:x|$x.AGE + 1, B:x|$x.FIRST_NAME])"));
        assertEquals(8, rt.columns().size());
        assertEquals(Type.Primitive.INTEGER, columnType(rt, "A"));
        assertEquals(Type.Primitive.STRING, columnType(rt, "B"));
    }

    @Test
    void extendRejectsUnknownColumnInLambda() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery(T_PERSON + "->extend(~BAD:x|$x.NONEXISTENT)"));
    }

    @Test
    void extendAggregateFormIsWholeRelationWindow() {
        // extend<T,K,V,R>(r, AggColSpec<…,R>):Relation<T+R> — every row gets the
        // whole-relation aggregate; the schema appends the aggregate column.
        TypedSpec n = typeQuery(T_PERSON + "->extend(~total : x|$x.AGE : y|$y->sum())");
        TypedExtendAgg ext = assertInstanceOf(TypedExtendAgg.class, n);
        assertEquals("total", ext.aggs().get(0).name());
        Type.RelationType rt = schemaOf(n);
        assertEquals(7, rt.columns().size());
        assertEquals(Type.Primitive.NUMBER, columnType(rt, "total"));
    }

    @Test
    void extendWithWindowFunctionColumn() {
        // extend(over(~partition), ~alias:{p,w,r|…}) — the window natives (rank/denseRank)
        // type on the generic path; the partition column validates via the fragment rebind.
        TypedSpec n = typeQuery(T_PERSON
                + "->extend(over(~LAST_NAME), ~rnk : {p, w, r | $p->rank($w, $r)})");
        TypedExtendWindow ext = assertInstanceOf(TypedExtendWindow.class, n);
        assertEquals(java.util.List.of("LAST_NAME"), ext.window().partitions());
        assertEquals("rnk", ext.columns().get(0).name());
        Type.RelationType rt = schemaOf(n);
        assertEquals(7, rt.columns().size());
        assertEquals(Type.Primitive.INTEGER, columnType(rt, "rnk"));
    }

    @Test
    void extendWithWindowedAggregate() {
        TypedSpec n = typeQuery(T_PERSON
                + "->extend(over(~LAST_NAME), ~avgAge : {p, w, r | $r.AGE} : y | $y->avg())");
        TypedExtendWindow ext = assertInstanceOf(TypedExtendWindow.class, n);
        assertEquals(1, ext.aggs().size());
        assertEquals(Type.Primitive.FLOAT, columnType(schemaOf(n), "avgAge"));
    }

    @Test
    void extendWindowRejectsUnknownPartitionColumn() {
        assertThrows(TypeInferenceException.class, () -> typeQuery(T_PERSON
                + "->extend(over(~NONEXISTENT), ~rnk : {p, w, r | $p->rank($w, $r)})"));
    }

    @Test
    void legacyGroupByDesugarsToModernForm() {
        // groupBy(src, [keys], [agg(map,agg)], ['aliases']) — relation source keys
        // become bare alias-named colspecs (engine rewriteLegacyGroupBy).
        TypedSpec n = typeQuery(T_PERSON + "->groupBy([x|$x.LAST_NAME],"
                + " [agg(x|$x.AGE, y|$y->sum())], ['LAST_NAME', 'total'])");
        assertInstanceOf(TypedGroupBy.class, n);
        Type.RelationType rt = schemaOf(n);
        assertEquals(java.util.List.of("LAST_NAME", "total"),
                rt.columns().stream().map(Type.Column::name).toList());
        assertEquals(Type.Primitive.NUMBER, columnType(rt, "total"));
    }

    @Test
    void graphFetchValidatesTreeAndPreservesSourceType() {
        TypedSpec n = typeQuery(
                "test::Person.all()->graphFetch(#{test::Person{name, firm{legalName}}}#)");
        TypedGraphFetch gf = assertInstanceOf(TypedGraphFetch.class, n);
        assertEquals(2, gf.tree().size());
        assertEquals("firm", gf.tree().get(1).property());
        assertEquals("legalName", gf.tree().get(1).children().get(0).property());
        assertEquals("test::Person", ((Type.ClassType) n.info().type()).fqn());
        assertEquals(Multiplicity.Bounded.ZERO_MANY, n.info().multiplicity());
    }

    @Test
    void graphFetchRejectsUnknownProperty() {
        assertThrows(TypeInferenceException.class, () -> typeQuery(
                "test::Person.all()->graphFetch(#{test::Person{nonexistent}}#)"));
    }

    @Test
    void serializeReturnsString() {
        TypedSpec n = typeQuery(
                "test::Person.all()->serialize(#{test::Person{name}}#)");
        assertInstanceOf(TypedSerialize.class, n);
        assertEquals(Type.Primitive.STRING, n.info().type());
        assertEquals(Multiplicity.Bounded.ONE, n.info().multiplicity());
    }

    @Test
    void projectModernColSpecFormBuildsSchema() {
        // project<T,Z>(r, FuncColSpecArray<{T[1]->Any[*]},Z>):Relation<Z>.
        TypedSpec n = typeQuery(T_PERSON + "->project(~[NAME:x|$x.FIRST_NAME, AGE2:x|$x.AGE])");
        TypedProject pj = assertInstanceOf(TypedProject.class, n);
        assertEquals(2, pj.columns().size());
        Type.RelationType rt = schemaOf(n);
        assertEquals(java.util.List.of("NAME", "AGE2"),
                rt.columns().stream().map(Type.Column::name).toList());
        assertEquals(Type.Primitive.STRING, columnType(rt, "NAME"));
        assertEquals(Type.Primitive.INTEGER, columnType(rt, "AGE2"));
    }

    @Test
    void projectBareColSpecIsIdentityColumn() {
        // ~prop desugars to prop:x|$x.prop (engine ProjectChecker's bare-reference rule).
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON + "->project(~[FIRST_NAME, AGE])"));
        assertEquals(java.util.List.of("FIRST_NAME", "AGE"),
                rt.columns().stream().map(Type.Column::name).toList());
        assertEquals(Type.Primitive.STRING, columnType(rt, "FIRST_NAME"));
    }

    @Test
    void groupBySingleKeySingleAgg() {
        // groupBy<T,Z,K,V,R>(r, ColSpec<Z⊆T>, AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>):Relation<Z+R>
        TypedSpec n = typeQuery(T_PERSON
                + "->groupBy(~LAST_NAME, ~TOTAL_AGE : x|$x.AGE : y|$y->sum())");
        TypedGroupBy gb = assertInstanceOf(TypedGroupBy.class, n);
        assertEquals(java.util.List.of("LAST_NAME"),
                gb.keys().stream().map(TypedGroupBy.GroupKey::column).toList());
        assertEquals(java.util.List.of("TOTAL_AGE"),
                gb.aggs().stream().map(a -> a.name()).toList());
        Type.RelationType rt = schemaOf(n);
        assertEquals(java.util.List.of("LAST_NAME", "TOTAL_AGE"),
                rt.columns().stream().map(Type.Column::name).toList());
        assertEquals(Type.Primitive.STRING, columnType(rt, "LAST_NAME"));
        assertEquals(Type.Primitive.NUMBER, columnType(rt, "TOTAL_AGE"));   // sum(Number[*]):Number[1]
    }

    @Test
    void groupByMultiKeyMultiAggWithIndependentValueTypes() {
        // The two aggregates' K solve independently (Integer vs Float) — the array
        // signature's K is shared only syntactically; per-column bindings isolate it.
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON
                + "->groupBy(~[FIRST_NAME, LAST_NAME],"
                + " ~[TOTAL : x|$x.AGE : y|$y->sum(), AVG_SAL : x|$x.SALARY : y|$y->avg()])"));
        assertEquals(java.util.List.of("FIRST_NAME", "LAST_NAME", "TOTAL", "AVG_SAL"),
                rt.columns().stream().map(Type.Column::name).toList());
        assertEquals(Type.Primitive.NUMBER, columnType(rt, "TOTAL"));
        assertEquals(Type.Primitive.FLOAT, columnType(rt, "AVG_SAL"));   // avg(Number[*]):Float[1]
    }

    @Test
    void groupByRejectsUnknownKeyColumn() {
        assertThrows(TypeInferenceException.class, () -> typeQuery(T_PERSON
                + "->groupBy(~NONEXISTENT, ~T : x|$x.AGE : y|$y->sum())"));
    }

    @Test
    void groupByRejectsUnknownColumnInAggMap() {
        assertThrows(TypeInferenceException.class, () -> typeQuery(T_PERSON
                + "->groupBy(~LAST_NAME, ~T : x|$x.NONEXISTENT : y|$y->sum())"));
    }

    @Test
    void aggregateCollapsesToAggColumnsOnly() {
        TypedSpec n = typeQuery(T_PERSON + "->aggregate(~TOTAL : x|$x.AGE : y|$y->sum())");
        assertInstanceOf(TypedAggregate.class, n);
        Type.RelationType rt = schemaOf(n);
        assertEquals(java.util.List.of("TOTAL"),
                rt.columns().stream().map(Type.Column::name).toList());
    }

    @Test
    void joinUnionsBothSchemas() {
        // join<T,V>(rel1, rel2, JoinKind[1], {T[1],V[1]->Boolean[1]}):Relation<T+V>
        TypedSpec n = typeQuery(T_PERSON
                + "->join(#>{test::PersonDatabase.T_FIRM}#,"
                + " meta::pure::functions::relation::JoinKind.INNER,"
                + " {p, f | $p.AGE == $f.ID})");
        TypedJoin join = assertInstanceOf(TypedJoin.class, n);
        assertEquals("INNER", join.kind().value());
        Type.RelationType rt = schemaOf(n);
        assertEquals(8, rt.columns().size(), "T+V unions both schemas");
        assertEquals(Type.Primitive.STRING, columnType(rt, "LEGAL_NAME"));
        assertEquals(Type.Primitive.INTEGER, columnType(rt, "AGE"));
    }

    @Test
    void joinRejectsUnknownEnumValue() {
        assertThrows(TypeInferenceException.class, () -> typeQuery(T_PERSON
                + "->join(#>{test::PersonDatabase.T_FIRM}#,"
                + " meta::pure::functions::relation::JoinKind.SIDEWAYS,"
                + " {p, f | $p.AGE == $f.ID})"));
    }

    @Test
    void joinRejectsUnknownColumnInCondition() {
        assertThrows(TypeInferenceException.class, () -> typeQuery(T_PERSON
                + "->join(#>{test::PersonDatabase.T_FIRM}#,"
                + " meta::pure::functions::relation::JoinKind.INNER,"
                + " {p, f | $p.AGE == $f.NONEXISTENT})"));
    }

    @Test
    void asOfJoinUnionsBothSchemas() {
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON
                + "->asOfJoin(#>{test::PersonDatabase.T_FIRM}#, {p, f | $p.AGE > $f.ID})"));
        assertEquals(8, rt.columns().size(), "asOfJoin T+V unions both schemas");
    }

    @Test
    void dateLiteralsTypeByPrecision() {
        // Year/year-month -> Date; full day -> StrictDate; time part -> DateTime; %latest -> LatestDate.
        assertEquals(Type.Primitive.DATE, typeQuery("%2020").info().type());
        assertEquals(Type.Primitive.STRICT_DATE, typeQuery("%2020-01-01").info().type());
        assertEquals(Type.Primitive.DATE_TIME, typeQuery("%2020-01-01T10:30:00").info().type());
        assertEquals(Type.Primitive.LATEST_DATE, typeQuery("%latest").info().type());
        assertEquals(Multiplicity.Bounded.ONE, typeQuery("%2020-01-01").info().multiplicity());
    }

    @Test
    void castBindsTargetFromTypeAnnotation() {
        // cast<T|m>(Any[m], type:T[1]):T[m] — T binds from the @Type prototype value,
        // m from the source, all on the generic path.
        TypedSpec n = typeQuery("'x'->cast(@Integer)");
        TypedCast cast = assertInstanceOf(TypedCast.class, n);
        assertEquals(Type.Primitive.INTEGER, cast.target());
        assertEquals(Type.Primitive.INTEGER, n.info().type());
        assertEquals(Multiplicity.Bounded.ONE, n.info().multiplicity());
    }

    @Test
    void castPreservesSourceMultiplicity() {
        TypedSpec n = typeQuery("[1, 2, 3]->cast(@Number)");
        assertEquals(Type.Primitive.NUMBER, n.info().type());
        assertEquals(new Multiplicity.Bounded(3, 3), n.info().multiplicity());
    }

    @Test
    void castToRelationShapeYieldsBareRowSchema() {
        // The pivot idiom: cast(@Relation<(…)>) — the target resolves to the bare
        // row-struct (G-α), same representation every relation op emits.
        Type.RelationType rt = schemaOf(typeQuery(
                T_PERSON + "->cast(@Relation<(FIRST_NAME:String, AGE:Integer)>)"));
        assertEquals(java.util.List.of("FIRST_NAME", "AGE"),
                rt.columns().stream().map(Type.Column::name).toList());
        assertEquals(Type.Primitive.STRING, columnType(rt, "FIRST_NAME"));
    }

    @Test
    void toManyWidensToTargetAtStar() {
        TypedSpec n = typeQuery("1->toMany(@Number)");
        assertInstanceOf(TypedCast.class, n);
        assertEquals(Type.Primitive.NUMBER, n.info().type());
        assertEquals(Multiplicity.Bounded.ZERO_MANY, n.info().multiplicity());
    }

    @Test
    void toConvertsNullableToTarget() {
        TypedSpec n = typeQuery("1->to(@String)");
        assertEquals(Type.Primitive.STRING, n.info().type());
        assertEquals(Multiplicity.Bounded.ZERO_ONE, n.info().multiplicity());
    }

    @Test
    void castRejectsUnknownTargetType() {
        assertThrows(TypeInferenceException.class, () -> typeQuery("'x'->cast(@Nonexistent)"));
    }

    @Test
    void matchStaticallyDispatchesToTypedBranch() {
        // 5 : Integer matches the Integer branch; result = THAT body's type, not Any[*].
        TypedSpec n = typeQuery("5->match([i:Integer[1]|$i + 1, s:String[1]|'x'])");
        TypedMatch m = assertInstanceOf(TypedMatch.class, n);
        assertEquals("i", m.param());
        assertEquals(Type.Primitive.INTEGER, n.info().type());
        assertEquals(Multiplicity.Bounded.ONE, n.info().multiplicity());
    }

    @Test
    void matchAcceptsSubtypeBranch() {
        // StrictDate <: Date — the Date branch accepts a strict-date input.
        TypedSpec n = typeQuery("%2020-01-01->match([d:Date[1]|'matched'])");
        assertEquals(Type.Primitive.STRING, n.info().type());
    }

    @Test
    void matchWithNoMatchingBranchFailsLoudly() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery("5->match([s:String[1]|$s])"));
    }

    @Test
    void evalBetaReducesLambdaToBodyType() {
        TypedSpec n = typeQuery("{x | $x + 1}->eval(5)");
        assertInstanceOf(TypedEval.class, n);
        assertEquals(Type.Primitive.INTEGER, n.info().type());
        assertEquals(Multiplicity.Bounded.ONE, n.info().multiplicity());
    }

    @Test
    void evalOnColSpecDesugarsToColumnAccess() {
        // ~AGE->eval($x) ==> $x.AGE — the MappingNormalizer chain-join idiom.
        Type.RelationType rt = schemaOf(typeQuery(
                T_PERSON + "->filter(x | ~AGE->eval($x) > 20)"));
        assertEquals(6, rt.columns().size(), "filter preserves the schema");
    }

    @Test
    void tdsLiteralParsesHeaderIntoRealSchema() {
        // Explicit col:Type annotations + inference from the first data row —
        // never the signature's Relation<Any> placeholder.
        TypedSpec n = typeQuery("""
                #TDS
                  id:Integer, name, score:Float
                  1, joe, 1.5
                  2, bob, 2.5
                #""");
        TypedTds tds = assertInstanceOf(TypedTds.class, n);
        assertEquals(2, tds.rows().size());
        Type.RelationType rt = schemaOf(n);
        assertEquals(Type.Primitive.INTEGER, columnType(rt, "id"));
        assertEquals(Type.Primitive.STRING, columnType(rt, "name"), "inferred from 'joe'");
        assertEquals(Type.Primitive.FLOAT, columnType(rt, "score"));
    }

    @Test
    void tdsFeedsRelationOperators() {
        Type.RelationType rt = schemaOf(typeQuery("""
                #TDS
                  id, name
                  1, joe
                #->filter(x|$x.id > 0)->select(~name)"""));
        assertEquals(java.util.List.of("name"),
                rt.columns().stream().map(Type.Column::name).toList());
    }

    @Test
    void tdsRejectsUnknownColumnType() {
        assertThrows(TypeInferenceException.class, () -> typeQuery("""
                #TDS
                  id:Whatever
                  1
                #"""));
    }

    @Test
    void sourceUrlIsVariantDataRelation() {
        TypedSpec n = typeQuery("sourceUrl('file:/data.json')");
        assertInstanceOf(TypedSourceUrl.class, n);
        Type.RelationType rt = schemaOf(n);
        assertEquals(1, rt.columns().size());
        assertEquals("data", rt.columns().get(0).name());
        assertEquals("meta::pure::metamodel::variant::Variant",
                ((Type.ClassType) rt.columns().get(0).type()).fqn());
    }

    @Test
    void flattenWidensColumnToVariantKeepingSchema() {
        TypedSpec n = typeQuery(T_PERSON + "->flatten(~FIRST_NAME)");
        TypedFlatten fl = assertInstanceOf(TypedFlatten.class, n);
        assertEquals("FIRST_NAME", fl.column());
        Type.RelationType rt = schemaOf(n);
        assertEquals(6, rt.columns().size(), "flatten keeps every source column");
        assertInstanceOf(Type.ClassType.class, columnType(rt, "FIRST_NAME"));
        assertEquals(Type.Primitive.INTEGER, columnType(rt, "AGE"), "other columns untouched");
    }

    @Test
    void flattenRejectsUnknownColumn() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery(T_PERSON + "->flatten(~NONEXISTENT)"));
    }

    @Test
    void pivotKeepsStaticGroupColumnsAndCastConcretizes() {
        // Static half: source − pivot(FIRST_NAME) − value(AGE) = 4 group columns;
        // the pivoted columns are data-dependent and concretized by cast(@Relation<…>).
        TypedSpec n = typeQuery(T_PERSON
                + "->pivot(~FIRST_NAME, ~total : x|$x.AGE : y|$y->sum())");
        TypedPivot pivot = assertInstanceOf(TypedPivot.class, n);
        assertEquals(java.util.List.of("FIRST_NAME"), pivot.pivotColumns());
        Type.RelationType rt = schemaOf(n);
        assertEquals(4, rt.columns().size(), "group columns = source − pivot − value");
        assertFalse(hasColumn(rt, "FIRST_NAME"));
        assertFalse(hasColumn(rt, "AGE"));

        Type.RelationType cast = schemaOf(typeQuery(T_PERSON
                + "->pivot(~FIRST_NAME, ~total : x|$x.AGE : y|$y->sum())"
                + "->cast(@Relation<(LAST_NAME:String, joe_total:Integer)>)"));
        assertEquals(Type.Primitive.INTEGER, columnType(cast, "joe_total"));
    }

    @Test
    void sortByIsFixedDirectionKeyedSort() {
        TypedSpec n = typeQuery("[3, 1, 2]->sortBy(x|$x)");
        TypedSortBy sb = assertInstanceOf(TypedSortBy.class, n);
        assertTrue(sb.ascending());
        assertEquals(Type.Primitive.INTEGER, n.info().type());
        assertFalse(assertInstanceOf(TypedSortBy.class,
                typeQuery("[3, 1, 2]->sortByReversed(x|$x)")).ascending());
    }

    @Test
    void newTdsRelationAccessorIsSelectAlias() {
        // Engine routes newTDSRelationAccessor through SelectChecker — same here.
        TypedSpec n = typeQuery(T_PERSON + "->newTDSRelationAccessor()");
        assertInstanceOf(TypedSelect.class, n);
        assertEquals(6, schemaOf(n).columns().size());
    }

    @Test
    void fromSlotsRuntimeAndPassesTypeThrough() {
        TypedSpec n = typeQuery(T_PERSON + "->from(test::PersonDatabase)");
        TypedFrom from = assertInstanceOf(TypedFrom.class, n);
        assertTrue(from.runtime().isPresent());
        assertEquals("test::PersonDatabase", from.runtime().get().fullPath());
        assertTrue(from.mapping().isEmpty());
        assertEquals(6, schemaOf(n).columns().size(), "from is a type passthrough");
    }

    @Test
    void writeReturnsRowCount() {
        TypedSpec n = typeQuery(T_PERSON + "->write(test::PersonDatabase)");
        TypedWrite w = assertInstanceOf(TypedWrite.class, n);
        assertTrue(w.destination().isPresent());
        assertEquals(Type.Primitive.INTEGER, n.info().type());
        assertEquals(Multiplicity.Bounded.ONE, n.info().multiplicity());
    }

    @Test
    void foldSameTypeStrategy() {
        TypedSpec n = typeQuery("[1, 2, 3]->fold({e, a | $a + $e}, 0)");
        TypedFold fold = assertInstanceOf(TypedFold.class, n);
        assertInstanceOf(FoldStrategy.SameType.class, fold.strategy());
        assertEquals(Type.Primitive.INTEGER, n.info().type());
    }

    @Test
    void foldMapReduceStrategyDecomposesBody() {
        // T=String ≠ V=Integer; plus(acc, length(e)) strips to transform=length(e).
        TypedFold fold = assertInstanceOf(TypedFold.class,
                typeQuery("['a', 'bb']->fold({e, a | $a + length($e)}, 0)"));
        FoldStrategy.MapReduce mr = assertInstanceOf(FoldStrategy.MapReduce.class, fold.strategy());
        assertEquals(Type.Primitive.INTEGER, mr.transform().info().type());
        assertEquals(Type.Primitive.INTEGER, fold.info().type());
    }

    @Test
    void legacyStringSortDesugarsToColspecKeys() {
        // sort(rel, 'COL', SortDirection.DESC) and sort(rel, ['A','B']) — engine's
        // legacy TDS string-key forms — desugar to the colspec keys.
        TypedSort desc = assertInstanceOf(TypedSort.class, typeQuery(T_PERSON
                + "->sort('AGE', meta::relational::metamodel::SortDirection.DESC)"));
        assertEquals(java.util.List.of(new TypedSort.TypedSortKey("AGE", false)), desc.keys());

        TypedSort multi = assertInstanceOf(TypedSort.class,
                typeQuery(T_PERSON + "->sort(['FIRST_NAME', 'AGE'])"));
        assertEquals(java.util.List.of(
                        new TypedSort.TypedSortKey("FIRST_NAME", true),
                        new TypedSort.TypedSortKey("AGE", true)),
                multi.keys());
    }

    @Test
    void overCarriesRowsFrame() {
        TypedExtendWindow ext = assertInstanceOf(TypedExtendWindow.class, typeQuery(T_PERSON
                + "->extend(over(~LAST_NAME, [asc(~AGE)], rows(-1, 0)),"
                + " ~rnk : {p, w, r | $p->rank($w, $r)})"));
        assertTrue(ext.window().frame().isPresent());
        assertEquals(java.util.List.of(new TypedSort.TypedSortKey("AGE", true)),
                ext.window().sortKeys());
    }

    @Test
    void asOfJoinPrefixOverload() {
        TypedAsOfJoin aj = assertInstanceOf(TypedAsOfJoin.class, typeQuery(T_PERSON
                + "->asOfJoin(#>{test::PersonDatabase.T_FIRM}#,"
                + " {p, f | $p.AGE > $f.ID}, {p, f | $p.AGE == $f.ID}, 'r_')"));
        assertEquals("r_", aj.prefix().orElseThrow());
        assertTrue(aj.condition().isPresent());
    }

    @Test
    void matchExtraArgumentBindsSecondBranchParameter() {
        TypedSpec n = typeQuery("5->match([{i:Integer[1], s:String[1] | $s}], 'ctx')");
        TypedMatch m = assertInstanceOf(TypedMatch.class, n);
        assertEquals("s", m.extraParam().orElseThrow());
        assertEquals(Type.Primitive.STRING, n.info().type());
    }

    @Test
    void windowBodyPropertyChainPattern() {
        // Engine's Pattern 2: a property access chained off a window call —
        // lag(p, r) : row[0..1], then .SALARY composes to Float[0..1].
        TypedExtendWindow ext = assertInstanceOf(TypedExtendWindow.class, typeQuery(T_PERSON
                + "->extend(over(~LAST_NAME, [asc(~AGE)]),"
                + " ~prevSal : {p, w, r | $p->lag($r).SALARY})"));
        Type.RelationType rt = schemaOf(ext);
        assertEquals(Type.Primitive.FLOAT, columnType(rt, "prevSal"));
        Type.Column prevSal = rt.columns().stream()
                .filter(c -> c.name().equals("prevSal")).findFirst().orElseThrow();
        assertEquals(Multiplicity.Bounded.ZERO_ONE, prevSal.multiplicity(),
                "lag is [0..1] and SALARY is nullable — the chain stays optional");
    }

    @Test
    void windowBodyScalarWrapperPattern() {
        // Engine's Pattern 3: a scalar wrapping the window call — the whole body
        // types on the generic path (minus over Integers).
        TypedExtendWindow ext = assertInstanceOf(TypedExtendWindow.class, typeQuery(T_PERSON
                + "->extend(over(~LAST_NAME, [asc(~AGE)]),"
                + " ~ageDelta : {p, w, r | $r.AGE - $p->lag($r).AGE->toOne()})"));
        assertEquals(Type.Primitive.INTEGER, columnType(schemaOf(ext), "ageDelta"));
    }

    @Test
    void overCarriesRangeFrame() {
        TypedExtendWindow ext = assertInstanceOf(TypedExtendWindow.class, typeQuery(T_PERSON
                + "->extend(over(~LAST_NAME, [asc(~AGE)], _range(-2, 0)),"
                + " ~rnk : {p, w, r | $p->rank($w, $r)})"));
        assertTrue(ext.window().frame().isPresent());
    }

    @Test
    void ntileTypesOnGenericPath() {
        TypedExtendWindow ext = assertInstanceOf(TypedExtendWindow.class, typeQuery(T_PERSON
                + "->extend(over(~LAST_NAME), ~bucket : {p, w, r | $p->ntile($r, 4)})"));
        assertEquals(Type.Primitive.INTEGER, columnType(schemaOf(ext), "bucket"));
    }

    // ---- duplicate-column invariants (real legend-pure errors; never silent) ----

    @Test
    void extendRejectsExistingColumnName() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery(T_PERSON + "->extend(~AGE : x|$x.AGE + 1)"));
    }

    @Test
    void renameRejectsCollisionWithExistingColumn() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery(T_PERSON + "->rename(~FIRST_NAME, ~AGE)"));
    }

    @Test
    void selectRejectsDuplicateAndEmptyColumns() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery(T_PERSON + "->select(~[AGE, AGE])"));
        assertThrows(TypeInferenceException.class,
                () -> typeQuery(T_PERSON + "->select(~[])"));
    }

    @Test
    void selfJoinWithOverlappingColumnsFailsLoudly() {
        assertThrows(TypeInferenceException.class, () -> typeQuery(T_PERSON
                + "->join(" + T_PERSON + ", meta::pure::functions::relation::JoinKind.INNER,"
                + " {a, b | $a.AGE == $b.AGE})"));
    }

    @Test
    void groupByRejectsAggAliasShadowingKey() {
        assertThrows(TypeInferenceException.class, () -> typeQuery(T_PERSON
                + "->groupBy(~LAST_NAME, ~LAST_NAME : x|$x.AGE : y|$y->sum())"));
    }

    @Test
    void tdsRejectsDuplicateHeaderColumn() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery("#TDS\n id, id\n 1, 2\n#"));
    }

    @Test
    void tdsQuotedCellsKeepCommas() {
        TypedTds tds = assertInstanceOf(TypedTds.class, typeQuery("""
                #TDS
                  id, name
                  1, 'a, b'
                #"""));
        assertEquals(java.util.List.of("1", "a, b"), tds.rows().get(0),
                "a comma inside a quoted cell must not split");
        assertEquals(Type.Primitive.STRING, columnType(schemaOf(tds), "name"));
    }

    // ---- dispatch/strategy edges ----

    @Test
    void matchFirstAcceptingBranchWins() {
        // 5 : Integer is accepted by BOTH branches (Integer <: Number); first wins.
        TypedMatch m = assertInstanceOf(TypedMatch.class,
                typeQuery("5->match([n:Number[1]|'num', i:Integer[1]|$i])"));
        assertEquals("n", m.param());
        assertEquals(Type.Primitive.STRING, m.info().type());
    }

    @Test
    void matchManyInputSkipsToOneBranches() {
        // [1,2] is many-valued: the Integer[1] branch cannot hold it; the [*] branch can.
        TypedMatch m = assertInstanceOf(TypedMatch.class,
                typeQuery("[1, 2]->match([one:Integer[1]|'one', many:Integer[*]|'many'])"));
        assertEquals("many", m.param());
    }

    @Test
    void foldConcatenationStrategy() {
        TypedFold fold = assertInstanceOf(TypedFold.class,
                typeQuery("[1, 2]->fold({e, a | $a->add($e)}, [0])"));
        assertInstanceOf(FoldStrategy.Concatenation.class, fold.strategy());
    }

    @Test
    void foldCommutativeAccOnRightDecomposesToMapReduce() {
        // plus(length(e), acc) has the accumulator on the RIGHT — the left
        // spine can't strip it, but numeric plus is COMMUTATIVE, so the
        // checker decomposes anyway (engine's left-spine-only rule left this
        // an un-lowerable scalar-acc CollectionBuild; found by an executed
        // fold test hitting DuckDB's list_reduce type binder).
        TypedFold fold = assertInstanceOf(TypedFold.class,
                typeQuery("['a', 'bb']->fold({e, a | length($e) + $a}, 0)"));
        assertInstanceOf(FoldStrategy.MapReduce.class, fold.strategy());
    }

    @Test
    void foldCollectionBuildStrategy() {
        // minus is NOT commutative: acc on the right cannot decompose —
        // genuinely element-by-element.
        TypedFold fold = assertInstanceOf(TypedFold.class,
                typeQuery("['a', 'bb']->fold({e, a | $e->length() - $a}, 0)"));
        assertInstanceOf(FoldStrategy.CollectionBuild.class, fold.strategy());
    }

    // ---- remaining construct edges ----

    @Test
    void fromThreeArgumentM2MSlotsMappingAndRuntime() {
        TypedFrom from = assertInstanceOf(TypedFrom.class, typeQuery(T_PERSON
                + "->from(test::PersonDatabase, test::PersonDatabase)"));
        assertTrue(from.mapping().isPresent());
        assertTrue(from.runtime().isPresent());
    }

    @Test
    void writeWithoutDestination() {
        TypedWrite w = assertInstanceOf(TypedWrite.class, typeQuery(T_PERSON + "->write()"));
        assertTrue(w.destination().isEmpty());
        assertEquals(Type.Primitive.INTEGER, w.info().type());
    }

    @Test
    void overWithSortKeysOnlyAndMultiplePartitions() {
        TypedExtendWindow sortsOnly = assertInstanceOf(TypedExtendWindow.class, typeQuery(T_PERSON
                + "->extend(over([asc(~AGE)]), ~rn : {p, w, r | $p->rank($w, $r)})"));
        assertTrue(sortsOnly.window().partitions().isEmpty());
        assertEquals(1, sortsOnly.window().sortKeys().size());

        TypedExtendWindow multi = assertInstanceOf(TypedExtendWindow.class, typeQuery(T_PERSON
                + "->extend(over(~[LAST_NAME, FIRST_NAME]), ~rn : {p, w, r | $p->rank($w, $r)})"));
        assertEquals(java.util.List.of("LAST_NAME", "FIRST_NAME"), multi.window().partitions());
    }

    @Test
    void graphFetchBareClassTypedLeaf() {
        // A class-typed property WITHOUT a sub-tree is a legal leaf.
        TypedGraphFetch gf = assertInstanceOf(TypedGraphFetch.class,
                typeQuery("test::Person.all()->graphFetch(#{test::Person{name, firm}}#)"));
        assertEquals("firm", gf.tree().get(1).property());
        assertTrue(gf.tree().get(1).children().isEmpty());
    }

    @Test
    void legacyGroupByClassSourceKeysCarryExtractionLambdas() {
        TypedGroupBy gb = assertInstanceOf(TypedGroupBy.class, typeQuery(
                "test::Person.all()->groupBy([p|$p.name], [agg(p|$p.age, y|$y->sum())],"
                        + " ['nm', 'total'])"));
        assertTrue(gb.keys().get(0).fn().isPresent(), "class-source keys keep their lambdas");
        Type.RelationType rt = schemaOf(gb);
        assertEquals(java.util.List.of("nm", "total"),
                rt.columns().stream().map(Type.Column::name).toList());
        assertEquals(Type.Primitive.STRING, columnType(rt, "nm"));
    }

    @Test
    void evalFunctionReferenceDesugarsToDirectCall() {
        TypedSpec n = typeQuery("test::inc->eval(5)");
        assertEquals(Type.Primitive.INTEGER, n.info().type());
        assertEquals(Multiplicity.Bounded.ONE, n.info().multiplicity());
    }

    // ---- engine *CheckerTest corpus pins (type-level intent of the integration suites) ----

    @Test
    void joinPrefixRenamesEveryRightColumn() {
        // Engine JoinCheckerTest: the 5-arg prefix renames ALL right-side columns,
        // which is what makes a self-join with overlapping names legal.
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON
                + "->join(" + T_PERSON + ", meta::pure::functions::relation::JoinKind.INNER,"
                + " {a, b | $a.AGE == $b.AGE}, 'r_')"));
        assertEquals(12, rt.columns().size());
        assertTrue(hasColumn(rt, "AGE"));
        assertTrue(hasColumn(rt, "r_AGE"));
        assertTrue(hasColumn(rt, "r_LAST_NAME"), "non-overlapping right columns are prefixed too");
    }

    @Test
    void asOfJoinPrefixRenamesEveryRightColumnLikeJoin() {
        // DELIBERATE divergence from engine-lite (which prefixes only overlapping
        // columns here while join prefixes all): one prefix rule for both joins.
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON
                + "->asOfJoin(" + T_PERSON + ", {a, b | $a.AGE > $b.AGE},"
                + " {a, b | $a.LAST_NAME == $b.LAST_NAME}, 'r_')"));
        assertEquals(12, rt.columns().size());
        assertTrue(hasColumn(rt, "r_AGE"));
        assertTrue(hasColumn(rt, "r_LAST_NAME"), "non-overlapping right columns are prefixed too");

        Type.RelationType disjoint = schemaOf(typeQuery(T_PERSON
                + "->asOfJoin(#>{test::PersonDatabase.T_FIRM}#, {a, b | $a.AGE > $b.ID},"
                + " {a, b | $a.AGE == $b.ID}, 'r_')"));
        assertTrue(hasColumn(disjoint, "r_ID"), "same rule regardless of overlap");
        assertFalse(hasColumn(disjoint, "ID"));
    }

    @Test
    void unboundedWindowFrames() {
        TypedExtendWindow ext = assertInstanceOf(TypedExtendWindow.class, typeQuery(T_PERSON
                + "->extend(over(~LAST_NAME, [asc(~AGE)], unbounded()->rows(unbounded())),"
                + " ~rn : {p, w, r | $p->rank($w, $r)})"));
        assertTrue(ext.window().frame().isPresent());
        assertInstanceOf(TypedExtendWindow.class, typeQuery(T_PERSON
                + "->extend(over(~LAST_NAME, [asc(~AGE)], unbounded()->rows(0)),"
                + " ~rn : {p, w, r | $p->rank($w, $r)})"));
    }

    @Test
    void semiStructuredColumnNavigatesAsVariant() {
        // Engine GetCheckerTest: SEMISTRUCTURED -> Variant; get()/get(i) stay Variant
        // until ->to(@Type); ->toMany(@Variant)->map->fold aggregates.
        String events = "#>{test::PersonDatabase.T_EVENTS}#";
        Type.RelationType rt = schemaOf(typeQuery(events
                + "->extend(~sku : x | $x.PAYLOAD->get('items')->get(0)->get('sku')->to(@String))"));
        assertEquals(Type.Primitive.STRING, columnType(rt, "sku"));

        Type.RelationType agg = schemaOf(typeQuery(events
                + "->extend(~t : x | $x.PAYLOAD->get('i')->toMany(@Variant)"
                + "->map(i | $i->to(@Integer)->toOne())->fold({a, v | $a + $v}, 0))"));
        assertEquals(Type.Primitive.INTEGER, columnType(agg, "t"));
    }

    @Test
    void extendRejectsDuplicateWithinColspecArray() {
        assertThrows(TypeInferenceException.class,
                () -> typeQuery(T_PERSON + "->extend(~[d : x|1, d : x|2])"));
    }

    @Test
    void rowMapperAggregatesAndStatisticalReducers() {
        // Engine GroupByCheckerTest: bivariant rowMapper fn1 + wavg/corr reducers,
        // and the statistical single-arg family — all on the generic path.
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON
                + "->groupBy(~LAST_NAME, ~w : x|rowMapper($x.AGE, $x.AGE) : y|$y->wavg())"));
        assertEquals(Type.Primitive.FLOAT, columnType(rt, "w"));
        assertEquals(Type.Primitive.NUMBER, columnType(schemaOf(typeQuery(T_PERSON
                + "->groupBy(~LAST_NAME, ~s : x|$x.AGE : y|$y->stdDev())")), "s"));
    }

    @Test
    void windowAggregateWithReducerArgsAndOffsetPropertyChain() {
        // joinStrings reducer carries an extra separator arg; nth(w,r,2).col chains
        // a property off the [0..1] row value — both engine window-test idioms.
        assertEquals(Type.Primitive.STRING, columnType(schemaOf(typeQuery(T_PERSON
                + "->extend(over(~LAST_NAME, [asc(~AGE)]),"
                + " ~ns : {p, w, r | $r.FIRST_NAME} : y|$y->joinStrings('_'))")), "ns"));
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON
                + "->extend(over(~LAST_NAME, [asc(~AGE)]), ~n2 : {p, w, r | $p->nth($w, $r, 2).AGE})"));
        Type.Column n2 = rt.columns().stream().filter(c -> c.name().equals("n2")).findFirst().orElseThrow();
        assertEquals(Multiplicity.Bounded.ZERO_ONE, n2.multiplicity(), "nth is [0..1]; the chain stays optional");
    }

    @Test
    void quotedColumnNamesFlowThroughCastFilterAndSort() {
        // The pivot idiom's quoted '..__|__..' names: cast target, property access, colspec.
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON
                + "->cast(@Relation<('2011__|__total':Integer, city:String)>)"
                + "->filter(x | $x.'2011__|__total' > 100)"
                + "->sort(~'2011__|__total'->ascending())"));
        assertEquals(Type.Primitive.INTEGER, columnType(rt, "2011__|__total"));
    }

    @Test
    void sortByRejectsToManyKey() {
        // Engine SortCheckerTest negative: the key lambda must be {T[1]->U[1]};
        // a to-many navigation ([*] body) cannot serve the to-one key slot.
        assertThrows(TypeInferenceException.class,
                () -> typeQuery("test::Person.all()->sortBy(p|$p.nicknames)"));
        // The to-one key stays legal.
        assertEquals("test::Person", ((Type.ClassType) typeQuery(
                "test::Person.all()->sortBy(p|$p.name)").info().type()).fqn());
    }

    // ---- navigate: the clean-sheet graph-traversal primitive (MAPPING_CLEAN_SHEET.md §3) ----

    @Test
    void navigatePreMapWidensRelationWithClassSubRow() {
        // §3.1: rel->navigate(~firm: Firm.all(), {r,f|…}) : Relation<S + (firm:Firm)>.
        // Rows multiply like join; the sub-row COLUMN is [1] per output row (§3.4).
        TypedSpec n = typeQuery(T_PERSON
                + "->navigate(~firm: test::Firm.all(), {r, f | $r.FIRST_NAME == $f.legalName})");
        TypedNavigate nav = assertInstanceOf(TypedNavigate.class, n);
        assertEquals(TypedNavigate.Form.PRE_MAP, nav.form());
        assertEquals("firm", nav.alias().orElseThrow());
        Type.RelationType rt = schemaOf(n);
        assertEquals(7, rt.columns().size());
        Type.Column firm = rt.columns().stream()
                .filter(c -> c.name().equals("firm")).findFirst().orElseThrow();
        assertEquals("test::Firm", ((Type.ClassType) firm.type()).fqn());
        assertEquals(Multiplicity.Bounded.ONE, firm.multiplicity());
    }

    @Test
    void navigatedSubRowComposesWithPropertyAccess() {
        // The design's core promise: $r.firm.legalName — relation-column access
        // chaining into class-property access — works with NO new machinery.
        Type.RelationType rt = schemaOf(typeQuery(T_PERSON
                + "->navigate(~firm: test::Firm.all(), {r, f | $r.FIRST_NAME == $f.legalName})"
                + "->filter(x | $x.firm.legalName == 'ACME')"
                + "->extend(~firmName : x | $x.firm.legalName)"));
        assertEquals(Type.Primitive.STRING, columnType(rt, "firmName"));
    }

    @Test
    void navigatePostMapFillsDeclaredPropertyAndPassesThrough() {
        // §3.3: Class[*] widen — the slot must be a DECLARED property of the source
        // class with a matching target type; no instance multiplication.
        TypedSpec n = typeQuery("test::Person.all()"
                + "->navigate(~firm: test::Firm.all(), {p, f | $p.name == $f.legalName})");
        TypedNavigate nav = assertInstanceOf(TypedNavigate.class, n);
        assertEquals(TypedNavigate.Form.POST_MAP, nav.form());
        assertEquals("test::Person", ((Type.ClassType) n.info().type()).fqn());
        assertEquals(Multiplicity.Bounded.ZERO_MANY, n.info().multiplicity());
    }

    @Test
    void evalOfFunctionValueTypesThroughTheVerbatimSignature() {
        // eval<T,V|m,n>(Function<{T[n]->V[m]}>[1], param:T[n]):V[m] — a
        // function-typed PARAMETER's own type solves V[m] via the kernel's
        // FunctionType unification arm (was: a hand-rolled per-arg loop
        // against a weakened Any[*] signature). Let-bound bare lambdas do
        // not synthesize FunctionType values yet — that boundary is loud.
        String model = "function test::apply(f: Function<{Integer[1]->Boolean[1]}>[1],"
                + " v: Integer[1]): Boolean[1] { eval($f, $v) }";
        ModelContext ctx = Compiler.compileModel(model);
        var fn = ctx.findFunction("test::apply").get(0);
        var compiled = new SpecCompiler(ctx).compile(fn);
        TypedSpec body = compiled.body().get(compiled.body().size() - 1);
        assertEquals(Type.Primitive.BOOLEAN, body.info().type());
        assertEquals(Multiplicity.Bounded.ONE, body.info().multiplicity());
    }

    @Test
    void matchTypesTheStaticallySelectedBranch() {
        // DELIBERATE REFINEMENT over real pure's T[m] (= LUB of branches):
        // when the input's STATIC type selects a branch at compile time, the
        // result is that branch's narrowed type — sound (a subtype of the
        // LUB) and more precise. The verbatim signature governs the SHAPE
        // (Any[*] value + Function[1..*] branches).
        TypedSpec t = typeQuery("5->match([i: Integer[1] | $i + 1,"
                + " f: Float[1] | $f * 2.0])");
        assertEquals(Type.Primitive.INTEGER, t.info().type());
    }

    @Test
    void associationNavigationTypesFromTheAssociationIndex() {
        // findProperty's contract THIRD LEG (Property doc §5 discipline 3):
        // association-injected navigation properties resolve at LOOKUP time,
        // never stored on the class. $p.employer walks the Association.
        String model = """
                Class test::P { name: String[1]; }
                Class test::F { legal: String[1]; }
                Association test::Emp
                {
                    employer: test::F[1];
                    staff: test::P[*];
                }
                """;
        ModelContext ctx = Compiler.compileModel(model);
        TypedSpec t = new SpecCompiler(ctx).typeBody(
                SpecParser.parse("test::P.all()->map(p | $p.employer.legal)"),
                Env.empty(), Expected.infer());
        assertEquals(Type.Primitive.STRING, t.info().type());
        // And the reverse end, with the association's declared multiplicity:
        TypedSpec back = new SpecCompiler(ctx).typeBody(
                SpecParser.parse("test::F.all()->map(f | $f.staff)"),
                Env.empty(), Expected.infer());
        assertEquals("test::P", ((Type.ClassType) back.info().type()).fqn());
    }

    @Test
    void distinctBareColSpecDesugarsToArray() {
        // Real Pure registers ONLY distinct(rel, ColSpecArray) — the bare
        // ~col form is sugar (engine-accepted); DistinctChecker desugars.
        TypedSpec t = typeQuery(T_PERSON + "->distinct(~FIRST_NAME)");
        Type.RelationType rt = schemaOf(t);
        assertEquals(1, rt.columns().size());
        assertEquals("FIRST_NAME", rt.columns().get(0).name());
    }

    @Test
    void navigateInlineSlotForm() {
        // §3.2: navigate(T.all(), {t|pred}) : T[*] — the constructor-slot form,
        // standalone and inside ^Class(…) with the predicate capturing the outer row.
        TypedSpec n = typeQuery("navigate(test::Firm.all(), {f | $f.legalName == 'ACME'})");
        assertEquals(TypedNavigate.Form.INLINE,
                assertInstanceOf(TypedNavigate.class, n).form());
        assertEquals("test::Firm", ((Type.ClassType) n.info().type()).fqn());

        TypedSpec mapped = typeQuery("test::Person.all()->map(p | ^test::Person("
                + "name = $p.name,"
                + " firm = navigate(test::Firm.all(), {f | $f.legalName == $p.name})))");
        assertEquals("test::Person", ((Type.ClassType) mapped.info().type()).fqn());
    }

    @Test
    void navigatePostMapRejectsUnknownOrMistypedSlot() {
        assertThrows(TypeInferenceException.class, () -> typeQuery("test::Person.all()"
                + "->navigate(~boss: test::Firm.all(), {p, f | $p.name == $f.legalName})"));
        // 'name' is a declared property but String, not Firm.
        assertThrows(TypeInferenceException.class, () -> typeQuery("test::Person.all()"
                + "->navigate(~name: test::Firm.all(), {p, f | $p.name == $f.legalName})"));
    }

    @Test
    void navigateRejectsNonClassTarget() {
        assertThrows(TypeInferenceException.class, () -> typeQuery(T_PERSON
                + "->navigate(~x: [1, 2, 3], {r, f | $r.AGE == $f})"));
    }

    @Test
    void chainedOperationsPreserveType() {
        Type.RelationType rt = schemaOf(typeQuery(
                T_PERSON + "->filter(x|$x.AGE > 18)->sort(asc(~FIRST_NAME))->limit(5)"));
        assertEquals(6, rt.columns().size(), "Chained ops preserve columns");
        assertEquals(Type.Primitive.INTEGER, columnType(rt, "AGE"));
    }
}
