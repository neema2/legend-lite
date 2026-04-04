package com.gs.legend.compiler;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.model.store.Column;
import com.gs.legend.model.store.SqlDataType;
import com.gs.legend.model.store.Table;
import com.gs.legend.plan.GenericType;
import com.gs.legend.plan.PlanGenerator;

import com.gs.legend.sqlgen.DuckDBDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for TypeChecker type resolution and PlanGenerator SQL output.
 *
 * <p>
 * Sets up a real MappingRegistry with a PERSON table so the compiler
 * exercises actual type resolution, validation, and propagation.
 */
class TypeCheckerTest {

    // --- Test schema: PersonDatabase.T_PERSON ---
    private static final Table PERSON_TABLE = new Table("T_PERSON", List.of(
            Column.required("FIRST_NAME", SqlDataType.VARCHAR),
            Column.required("LAST_NAME", SqlDataType.VARCHAR),
            Column.required("AGE", SqlDataType.INTEGER),
            Column.nullable("SALARY", SqlDataType.DOUBLE),
            Column.nullable("HIRE_DATE", SqlDataType.DATE),
            Column.nullable("ACTIVE", SqlDataType.BOOLEAN)));


    /** Minimal ModelContext wrapping the test registry. */
    private static final com.gs.legend.model.ModelContext testModel = new com.gs.legend.model.ModelContext() {
        public java.util.Optional<com.gs.legend.model.m3.PureClass> findClass(String n) {
            return java.util.Optional.empty();
        }

        public java.util.Optional<com.gs.legend.model.ModelContext.AssociationNavigation> findAssociationByProperty(
                String c, String p) {
            return java.util.Optional.empty();
        }

        public java.util.Optional<com.gs.legend.model.store.Table> findTable(String n) {
            if (n.contains("T_PERSON")) return java.util.Optional.of(PERSON_TABLE);
            return java.util.Optional.empty();
        }
    };

    private final TypeChecker compiler = new TypeChecker(testModel);

    // ========== Type Resolution ==========

    @Test
    void tableAccessResolvesTableSchema() {
        var vs = parse("#>{PersonDatabase.T_PERSON}#");
        var unit = compiler.check(vs);

        GenericType.Relation.Schema rt = unit.typeInfoFor(unit.root()).schema();
        assertEquals(6, rt.size(), "Should have 6 columns");
        assertEquals(GenericType.Primitive.STRING, rt.requireColumn("FIRST_NAME"));
        assertEquals(GenericType.Primitive.STRING, rt.requireColumn("LAST_NAME"));
        assertEquals(GenericType.Primitive.INTEGER, rt.requireColumn("AGE"));
        assertEquals(GenericType.Primitive.FLOAT, rt.requireColumn("SALARY"));
        assertEquals(GenericType.Primitive.STRICT_DATE, rt.requireColumn("HIRE_DATE"));
        assertEquals(GenericType.Primitive.BOOLEAN, rt.requireColumn("ACTIVE"));
    }

    @Test
    void filterPreservesSourceType() {
        var vs = parse("#>{PersonDatabase.T_PERSON}->filter(x|$x.AGE > 25)");
        var unit = compiler.check(vs);

        // Filter preserves ALL columns from source
        GenericType.Relation.Schema rt = unit.typeInfoFor(unit.root()).schema();
        assertEquals(6, rt.size(), "Filter doesn't change column set");
        assertEquals(GenericType.Primitive.INTEGER, rt.requireColumn("AGE"));
        assertEquals(GenericType.Primitive.STRING, rt.requireColumn("FIRST_NAME"));
    }

    @Test
    void filterRejectsInvalidColumn() {
        var vs = parse("#>{PersonDatabase.T_PERSON}->filter(x|$x.NONEXISTENT > 25)");
        assertThrows(PureCompileException.class,
                () -> compiler.check(vs),
                "Should reject reference to non-existent column");
    }

    @Test
    void sortValidatesColumns() {
        var vs = parse("#>{PersonDatabase.T_PERSON}->sort(asc(~AGE))");
        var unit = compiler.check(vs);

        assertEquals(6, unit.typeInfoFor(unit.root()).schema().size(), "Sort preserves columns");
    }

    @Test
    void sortRejectsInvalidColumn() {
        var vs = parse("#>{PersonDatabase.T_PERSON}->sort(asc(~NONEXISTENT))");
        assertThrows(PureCompileException.class,
                () -> compiler.check(vs),
                "Should reject sort on non-existent column");
    }

    @Test
    void renameChangesColumnName() {
        var vs = parse("#>{PersonDatabase.T_PERSON}->rename(~FIRST_NAME, ~NAME)");
        var unit = compiler.check(vs);

        GenericType.Relation.Schema rt = unit.typeInfoFor(unit.root()).schema();
        assertFalse(rt.hasColumn("FIRST_NAME"), "Old column name should be gone");
        assertTrue(rt.hasColumn("NAME"), "New column name should exist");
        assertEquals(GenericType.Primitive.STRING, rt.requireColumn("NAME"));
    }

    @Test
    void limitPreservesType() {
        var vs = parse("#>{PersonDatabase.T_PERSON}->limit(10)");
        var unit = compiler.check(vs);
        assertEquals(6, unit.typeInfoFor(unit.root()).schema().size());
    }

    @Test
    void chainedOperationsPreserveType() {
        var vs = parse("#>{PersonDatabase.T_PERSON}->filter(x|$x.AGE > 18)->sort(asc(~FIRST_NAME))->limit(5)");
        var unit = compiler.check(vs);

        GenericType.Relation.Schema rt = unit.typeInfoFor(unit.root()).schema();
        assertEquals(6, rt.size(), "Chained ops preserve columns");
        assertEquals(GenericType.Primitive.INTEGER, rt.requireColumn("AGE"));
    }

    // ========== SQL Generation ==========

    @Test
    void filterGeneratesValidSql() {
        var vs = parse("#>{PersonDatabase.T_PERSON}->filter(x|$x.AGE > 25)");
        var unit = compiler.check(vs);
        String sql = compileSql(unit);

        assertTrue(sql.contains("WHERE"), "Should have WHERE: " + sql);
        assertTrue(sql.contains("\"AGE\""), "Should reference AGE column: " + sql);
        assertTrue(sql.contains(">"), "Should have > operator: " + sql);
        assertTrue(sql.contains("25"), "Should have literal 25: " + sql);
    }

    @Test
    void limitGeneratesSql() {
        var vs = parse("#>{PersonDatabase.T_PERSON}->limit(10)");
        var unit = compiler.check(vs);
        String sql = compileSql(unit);

        assertTrue(sql.contains("LIMIT 10"), "Should have LIMIT 10: " + sql);
    }

    // ========== Helpers ==========

    private ValueSpecification parse(String pureQuery) {
        return com.gs.legend.parser.PureParser.parseQuery(pureQuery);
    }

    private String compileSql(TypeCheckResult unit) {
        var planGenerator = new PlanGenerator(unit, DuckDBDialect.INSTANCE);
        var plan = planGenerator.generate();
        return plan.sql();
    }
}
