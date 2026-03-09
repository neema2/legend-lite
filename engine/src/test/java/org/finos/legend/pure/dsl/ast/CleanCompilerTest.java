package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.plan.GenericType;
import org.finos.legend.engine.plan.RelationType;
import org.finos.legend.engine.store.Column;
import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.engine.store.PropertyMapping;
import org.finos.legend.engine.store.RelationalMapping;
import org.finos.legend.engine.store.SqlDataType;
import org.finos.legend.engine.store.Table;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.pure.dsl.PureCompileException;
import org.finos.legend.pure.m3.PrimitiveType;
import org.finos.legend.pure.m3.Property;
import org.finos.legend.pure.m3.PureClass;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CleanCompiler type resolution and PlanGenerator SQL output.
 *
 * <p>
 * Sets up a real MappingRegistry with a PERSON table so the compiler
 * exercises actual type resolution, validation, and propagation.
 */
class CleanCompilerTest {

    // --- Test schema: PersonDatabase.T_PERSON ---
    private static final Table PERSON_TABLE = new Table("T_PERSON", List.of(
            Column.required("FIRST_NAME", SqlDataType.VARCHAR),
            Column.required("LAST_NAME", SqlDataType.VARCHAR),
            Column.required("AGE", SqlDataType.INTEGER),
            Column.nullable("SALARY", SqlDataType.DOUBLE),
            Column.nullable("HIRE_DATE", SqlDataType.DATE),
            Column.nullable("ACTIVE", SqlDataType.BOOLEAN)));

    private static final PureClass PERSON_CLASS = new PureClass("model", "Person", List.of(
            Property.required("firstName", PrimitiveType.STRING),
            Property.required("lastName", PrimitiveType.STRING),
            Property.required("age", PrimitiveType.INTEGER)));

    private static final MappingRegistry registry = new MappingRegistry()
            .register(new RelationalMapping(PERSON_CLASS, PERSON_TABLE, List.of(
                    PropertyMapping.column("firstName", "FIRST_NAME"),
                    PropertyMapping.column("lastName", "LAST_NAME"),
                    PropertyMapping.column("age", "AGE"))));

    private final CleanCompiler compiler = new CleanCompiler(registry, null);

    // ========== Type Resolution ==========

    @Test
    void tableAccessResolvesTableSchema() {
        var vs = parse("table('PersonDatabase.T_PERSON')");
        var unit = compiler.compile(vs);

        RelationType rt = unit.typeInfoFor(unit.root()).relationType();
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
        var vs = parse("table('PersonDatabase.T_PERSON')->filter(x|$x.AGE > 25)");
        var unit = compiler.compile(vs);

        // Filter preserves ALL columns from source
        RelationType rt = unit.typeInfoFor(unit.root()).relationType();
        assertEquals(6, rt.size(), "Filter doesn't change column set");
        assertEquals(GenericType.Primitive.INTEGER, rt.requireColumn("AGE"));
        assertEquals(GenericType.Primitive.STRING, rt.requireColumn("FIRST_NAME"));
    }

    @Test
    void filterRejectsInvalidColumn() {
        var vs = parse("table('PersonDatabase.T_PERSON')->filter(x|$x.NONEXISTENT > 25)");
        assertThrows(PureCompileException.class,
                () -> compiler.compile(vs),
                "Should reject reference to non-existent column");
    }

    @Test
    void sortValidatesColumns() {
        var vs = parse("table('PersonDatabase.T_PERSON')->sort(asc(~AGE))");
        var unit = compiler.compile(vs);

        assertEquals(6, unit.typeInfoFor(unit.root()).relationType().size(), "Sort preserves columns");
    }

    @Test
    void sortRejectsInvalidColumn() {
        var vs = parse("table('PersonDatabase.T_PERSON')->sort(asc(~NONEXISTENT))");
        assertThrows(PureCompileException.class,
                () -> compiler.compile(vs),
                "Should reject sort on non-existent column");
    }

    @Test
    void renameChangesColumnName() {
        var vs = parse("table('PersonDatabase.T_PERSON')->rename(~FIRST_NAME, ~NAME)");
        var unit = compiler.compile(vs);

        RelationType rt = unit.typeInfoFor(unit.root()).relationType();
        assertFalse(rt.hasColumn("FIRST_NAME"), "Old column name should be gone");
        assertTrue(rt.hasColumn("NAME"), "New column name should exist");
        assertEquals(GenericType.Primitive.STRING, rt.requireColumn("NAME"));
    }

    @Test
    void limitPreservesType() {
        var vs = parse("table('PersonDatabase.T_PERSON')->limit(10)");
        var unit = compiler.compile(vs);
        assertEquals(6, unit.typeInfoFor(unit.root()).relationType().size());
    }

    @Test
    void chainedOperationsPreserveType() {
        var vs = parse("table('PersonDatabase.T_PERSON')->filter(x|$x.AGE > 18)->sort(asc(~FIRST_NAME))->limit(5)");
        var unit = compiler.compile(vs);

        RelationType rt = unit.typeInfoFor(unit.root()).relationType();
        assertEquals(6, rt.size(), "Chained ops preserve columns");
        assertEquals(GenericType.Primitive.INTEGER, rt.requireColumn("AGE"));
    }

    // ========== SQL Generation ==========

    @Test
    void filterGeneratesValidSql() {
        var vs = parse("table('PersonDatabase.T_PERSON')->filter(x|$x.AGE > 25)");
        var unit = compiler.compile(vs);
        String sql = compileSql(unit);

        assertTrue(sql.contains("WHERE"), "Should have WHERE: " + sql);
        assertTrue(sql.contains("\"AGE\""), "Should reference AGE column: " + sql);
        assertTrue(sql.contains(">"), "Should have > operator: " + sql);
        assertTrue(sql.contains("25"), "Should have literal 25: " + sql);
    }

    @Test
    void limitGeneratesSql() {
        var vs = parse("table('PersonDatabase.T_PERSON')->limit(10)");
        var unit = compiler.compile(vs);
        String sql = compileSql(unit);

        assertTrue(sql.contains("LIMIT 10"), "Should have LIMIT 10: " + sql);
    }

    // ========== Helpers ==========

    private ValueSpecification parse(String pureQuery) {
        return org.finos.legend.pure.dsl.PureParser.parseClean(pureQuery);
    }

    private String compileSql(CompilationUnit unit) {
        var planGenerator = new PlanGenerator(unit, DuckDBDialect.INSTANCE);
        SqlBuilder builder = planGenerator.generate();
        return builder.toSql(DuckDBDialect.INSTANCE);
    }
}
