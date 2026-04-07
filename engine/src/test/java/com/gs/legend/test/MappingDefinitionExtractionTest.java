package com.gs.legend.test;

import com.gs.legend.model.def.DatabaseDefinition;
import com.gs.legend.model.def.MappingDefinition;
import com.gs.legend.parser.PureParser;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tier 1 Tests: Parse → Definition Record extraction.
 *
 * Each test parses a Pure source string through the parser and builder,
 * then asserts that the resulting definition record captures ALL grammar features.
 *
 * These tests catch builder bugs BEFORE they propagate to model/resolver/plan.
 *
 * Organized by feature area, mirroring legend-pure's test files:
 * - TestMappingGrammar.java → set IDs, root markers, includes, store substitution
 * - TestEmbeddedGrammar.java → embedded, inline, otherwise
 * - TestDatabaseInclude.java → database includes
 * - TestExtendGrammar.java → mapping inheritance
 * - TestAssociationMappingValidation.java → association mappings
 */
class MappingDefinitionExtractionTest {

    // ==================== Helper ====================

    private MappingDefinition parseMapping(String pureSource) {
        return PureParser.parseMappingDefinition(pureSource);
    }

    private DatabaseDefinition parseDatabase(String pureSource) {
        return PureParser.parseDatabaseDefinition(pureSource);
    }

    // ==================== Database Definition Extraction ====================

    @Nested
    @DisplayName("Database Definition Extraction")
    class DatabaseTests {

        @Test
        @DisplayName("Simple table with columns")
        void testSimpleTable() {
            var db = parseDatabase("""
                    import store::*;

                    ###Relational
                    Database store::MyDB
                    (
                        Table PersonTable (id INTEGER PRIMARY KEY, name VARCHAR(200), age INTEGER NOT NULL)
                    )
                    """);

            assertEquals("store::MyDB", db.qualifiedName());
            assertEquals(1, db.tables().size());

            var table = db.tables().get(0);
            assertEquals("PersonTable", table.name());
            assertEquals(3, table.columns().size());

            var idCol = table.columns().get(0);
            assertEquals("id", idCol.name());
            assertEquals("INTEGER", idCol.dataType());
            assertTrue(idCol.primaryKey());
            assertTrue(idCol.notNull());

            var nameCol = table.columns().get(1);
            assertEquals("name", nameCol.name());
            assertEquals("VARCHAR(200)", nameCol.dataType());
            assertFalse(nameCol.primaryKey());
            assertFalse(nameCol.notNull());

            var ageCol = table.columns().get(2);
            assertEquals("age", ageCol.name());
            assertTrue(ageCol.notNull());
        }

        @Test
        @DisplayName("Simple equi-join")
        void testSimpleEquiJoin() {
            var db = parseDatabase("""
                    import store::*;

                    ###Relational
                    Database store::MyDB
                    (
                        Table T_PERSON (id INTEGER PRIMARY KEY, address_id INTEGER)
                        Table T_ADDRESS (id INTEGER PRIMARY KEY, street VARCHAR(200))
                        Join Person_Address(T_PERSON.address_id = T_ADDRESS.id)
                    )
                    """);

            assertEquals(1, db.joins().size());
            var join = db.joins().get(0);
            assertEquals("Person_Address", join.name());

            // Inspect condition tree: Comparison(ColumnRef(T_PERSON, address_id), "=", ColumnRef(T_ADDRESS, id))
            var cmp = (com.gs.legend.model.def.RelationalOperation.Comparison) join.operation();
            assertEquals("=", cmp.op());
            var left = (com.gs.legend.model.def.RelationalOperation.ColumnRef) cmp.left();
            assertEquals("T_PERSON", left.table());
            assertEquals("address_id", left.column());
            var right = (com.gs.legend.model.def.RelationalOperation.ColumnRef) cmp.right();
            assertEquals("T_ADDRESS", right.table());
            assertEquals("id", right.column());
        }

        // ======================== FAILING TESTS — FEATURES NOT YET EXTRACTED ========================
        // These document the gaps. They will fail until we implement proper extraction.

        @Test
        @DisplayName("Multi-column join condition (and) - full expression tree")
        void testMultiColumnJoin() {
            var db = parseDatabase("""
                    import store::*;

                    ###Relational
                    Database store::MyDB
                    (
                        Table T1 (a INTEGER, b INTEGER)
                        Table T2 (a INTEGER, b INTEGER)
                        Join MultiColJoin(T1.a = T2.a and T1.b = T2.b)
                    )
                    """);

            var join = db.joins().get(0);
            assertEquals("MultiColJoin", join.name());

            // Full expression tree: BooleanOp(Comparison, "and", Comparison)
            var op = join.operation();
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.BooleanOp.class, op);
            var boolOp = (com.gs.legend.model.def.RelationalOperation.BooleanOp) op;
            assertEquals("and", boolOp.op());

            // Left: T1.a = T2.a
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.Comparison.class, boolOp.left());
            var leftCmp = (com.gs.legend.model.def.RelationalOperation.Comparison) boolOp.left();
            assertEquals("=", leftCmp.op());
            var leftCol = (com.gs.legend.model.def.RelationalOperation.ColumnRef) leftCmp.left();
            assertEquals("T1", leftCol.table());
            assertEquals("a", leftCol.column());

            // Right: T1.b = T2.b
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.Comparison.class, boolOp.right());
            var rightCmp = (com.gs.legend.model.def.RelationalOperation.Comparison) boolOp.right();
            assertEquals("=", rightCmp.op());

        }

        @Test
        @DisplayName("Function-based join condition - full expression tree")
        void testFunctionJoin() {
            var db = parseDatabase("""
                    import store::*;

                    ###Relational
                    Database store::MyDB
                    (
                        Table T1 (name VARCHAR(200))
                        Table T2 (prefixed_name VARCHAR(200))
                        Join FuncJoin(concat('prefix_', T1.name) = T2.prefixed_name)
                    )
                    """);

            var join = db.joins().get(0);
            assertEquals("FuncJoin", join.name());

            // Comparison(FunctionCall("concat", [Literal, ColumnRef]), "=", ColumnRef)
            var op = join.operation();
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.Comparison.class, op);
            var cmp = (com.gs.legend.model.def.RelationalOperation.Comparison) op;
            assertEquals("=", cmp.op());

            // Left: concat('prefix_', T1.name)
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.FunctionCall.class, cmp.left());
            var func = (com.gs.legend.model.def.RelationalOperation.FunctionCall) cmp.left();
            assertEquals("concat", func.name());
            assertEquals(2, func.args().size());

            // Right: T2.prefixed_name
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.ColumnRef.class, cmp.right());
            var rightCol = (com.gs.legend.model.def.RelationalOperation.ColumnRef) cmp.right();
            assertEquals("T2", rightCol.table());
            assertEquals("prefixed_name", rightCol.column());
        }

        @Test
        @DisplayName("Self-join with {target} - full expression tree")
        void testSelfJoin() {
            var db = parseDatabase("""
                    import store::*;

                    ###Relational
                    Database store::MyDB
                    (
                        Table T_ORG (id INTEGER PRIMARY KEY, parent_id INTEGER, name VARCHAR(200))
                        Join OrgHierarchy(T_ORG.parent_id = {target}.id)
                    )
                    """);

            var join = db.joins().get(0);
            assertEquals("OrgHierarchy", join.name());

            // Comparison(ColumnRef(T_ORG, parent_id), "=", TargetColumnRef(id))
            var op = join.operation();
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.Comparison.class, op);
            var cmp = (com.gs.legend.model.def.RelationalOperation.Comparison) op;
            assertEquals("=", cmp.op());

            // Left: T_ORG.parent_id
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.ColumnRef.class, cmp.left());
            var leftCol = (com.gs.legend.model.def.RelationalOperation.ColumnRef) cmp.left();
            assertEquals("T_ORG", leftCol.table());
            assertEquals("parent_id", leftCol.column());

            // Right: {target}.id
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.TargetColumnRef.class, cmp.right());
            var targetCol = (com.gs.legend.model.def.RelationalOperation.TargetColumnRef) cmp.right();
            assertEquals("id", targetCol.column());
        }

        @Test
        @DisplayName("View extraction with column mappings")
        void testViewExtraction() {
            var db = parseDatabase("""
                    import store::*;

                    ###Relational
                    Database store::MyDB
                    (
                        Table TradeTable (trade_id INTEGER PRIMARY KEY, status INTEGER, amount FLOAT)
                        View ActiveTradeView
                        (
                            trade_id : TradeTable.trade_id PRIMARY KEY,
                            amount   : TradeTable.amount
                        )
                    )
                    """);

            assertEquals(1, db.tables().size());
            assertEquals(1, db.views().size());

            var view = db.views().get(0);
            assertEquals("ActiveTradeView", view.name());
            assertFalse(view.distinct());
            assertNull(view.filterMapping());
            assertEquals(0, view.groupByColumns().size());
            assertEquals(2, view.columnMappings().size());

            var col0 = view.columnMappings().get(0);
            assertEquals("trade_id", col0.name());
            assertTrue(col0.primaryKey());
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.ColumnRef.class, col0.expression());

            var col1 = view.columnMappings().get(1);
            assertEquals("amount", col1.name());
            assertFalse(col1.primaryKey());
        }

        @Test
        @DisplayName("Filter extraction with expression tree")
        void testFilterExtraction() {
            var db = parseDatabase("""
                    import store::*;

                    ###Relational
                    Database store::MyDB
                    (
                        Table TradeTable (trade_id INTEGER PRIMARY KEY, status INTEGER)
                        Filter ActiveFilter(TradeTable.status = 1)
                    )
                    """);

            assertEquals(1, db.filters().size());
            var filter = db.filters().get(0);
            assertEquals("ActiveFilter", filter.name());

            // Comparison(ColumnRef(TradeTable, status), "=", Literal(1))
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.Comparison.class, filter.condition());
            var cmp = (com.gs.legend.model.def.RelationalOperation.Comparison) filter.condition();
            assertEquals("=", cmp.op());
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.ColumnRef.class, cmp.left());
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.Literal.class, cmp.right());
        }

        @Test
        @DisplayName("Database includes extraction")
        void testDatabaseIncludes() {
            // parseDatabase returns first DB, so parse just the one with includes
            var db = parseDatabase("""
                    import store::*;

                    ###Relational
                    Database store::ExtendedDB
                    (
                        include store::BaseDB
                        Table ExtTable (id INTEGER PRIMARY KEY)
                    )
                    """);

            assertNotNull(db);
            assertEquals(1, db.includes().size());
            assertEquals("store::BaseDB", db.includes().get(0));
        }

        @Test
        @DisplayName("Schema-qualified tables preserve schema info")
        void testSchemaPreservation() {
            var db = parseDatabase("""
                    import store::*;

                    ###Relational
                    Database store::MyDB
                    (
                        Schema mySchema
                        (
                            Table PersonTable (id INTEGER PRIMARY KEY, name VARCHAR(200))
                        )
                    )
                    """);

            // Schema tables also appear in top-level list for backward compat
            assertEquals(1, db.tables().size());
            assertEquals("PersonTable", db.tables().get(0).name());

            // Schema is now extracted
            assertEquals(1, db.schemas().size());
            assertEquals("mySchema", db.schemas().get(0).name());
            assertEquals(1, db.schemas().get(0).tables().size());
            assertEquals("PersonTable", db.schemas().get(0).tables().get(0).name());
        }
    }

    // ==================== Mapping Definition Extraction ====================

    @Nested
    @DisplayName("Mapping Definition Extraction — Class Mappings")
    class ClassMappingTests {

        @Test
        @DisplayName("Simple relational class mapping")
        void testSimpleRelationalMapping() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::PersonMapping
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            firstName: [store::DB] T_PERSON.FIRST_NAME,
                            lastName: [store::DB] T_PERSON.LAST_NAME
                        }
                    )
                    """);

            assertEquals("model::PersonMapping", mapping.qualifiedName());
            assertEquals(1, mapping.classMappings().size());

            var cm = mapping.classMappings().get(0);
            assertEquals("Person", cm.className());
            assertEquals("Relational", cm.mappingType());
            assertNotNull(cm.mainTable());
            assertEquals("store::DB", cm.mainTable().databaseName());
            assertEquals(2, cm.propertyMappings().size());
        }

        @Test
        @DisplayName("Set ID extraction")
        void testSetIdExtraction() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::PersonMapping
                    (
                        Person[person_set1]: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            id: [store::DB] T_PERSON.ID
                        }
                    )
                    """);

            var cm = mapping.classMappings().get(0);
            assertEquals("person_set1", cm.setId());
        }

        @Test
        @DisplayName("Root marker extraction")
        void testRootMarkerExtraction() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::PersonMapping
                    (
                        *Person[person_set1]: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            id: [store::DB] T_PERSON.ID
                        }
                    )
                    """);

            var cm = mapping.classMappings().get(0);
            assertTrue(cm.isRoot());
            assertEquals("person_set1", cm.setId());
        }

        @Test
        @DisplayName("Extends extraction")
        void testExtendsExtraction() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::PersonMapping
                    (
                        Employee[emp] extends [person_base]: Relational
                        {
                            ~mainTable [store::DB] T_EMPLOYEE
                            salary: [store::DB] T_EMPLOYEE.SALARY
                        }
                    )
                    """);

            var cm = mapping.classMappings().get(0);
            assertEquals("emp", cm.setId());
            assertEquals("person_base", cm.extendsSetId());
        }

        @Test
        @DisplayName("Mapping filter extraction")
        void testMappingFilterExtraction() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::PersonMapping
                    (
                        Person: Relational
                        {
                            ~filter [store::DB] ActiveFilter
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME
                        }
                    )
                    """);

            var cm = mapping.classMappings().get(0);
            assertNotNull(cm.filter());
            assertEquals("ActiveFilter", cm.filter().filterName());
            assertEquals("store::DB", cm.filter().databaseName());
            assertTrue(cm.filter().joinPath().isEmpty());
        }

        @Test
        @DisplayName("Distinct flag extraction")
        void testDistinctExtraction() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::PersonMapping
                    (
                        Person: Relational
                        {
                            ~distinct
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME
                        }
                    )
                    """);

            var cm = mapping.classMappings().get(0);
            assertTrue(cm.distinct());
        }

        @Test
        @DisplayName("GroupBy extraction")
        void testGroupByExtraction() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::PersonMapping
                    (
                        Person: Relational
                        {
                            ~groupBy([store::DB] T_PERSON.DEPT_ID)
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME
                        }
                    )
                    """);

            var cm = mapping.classMappings().get(0);
            assertEquals(1, cm.groupBy().size());
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.ColumnRef.class, cm.groupBy().get(0));
        }

        @Test
        @DisplayName("PrimaryKey extraction")
        void testPrimaryKeyExtraction() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::PersonMapping
                    (
                        Person: Relational
                        {
                            ~primaryKey([store::DB] T_PERSON.ID)
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME
                        }
                    )
                    """);

            var cm = mapping.classMappings().get(0);
            assertEquals(1, cm.primaryKey().size());
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.ColumnRef.class, cm.primaryKey().get(0));
        }

        @Test
        @DisplayName("GAP: Qualified class name preserved")
        void testQualifiedClassNamePreserved() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::PersonMapping
                    (
                        model::domain::Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME
                        }
                    )
                    """);

            var cm = mapping.classMappings().get(0);
            assertEquals("model::domain::Person", cm.className()); // FQN preserved by parser
        }
    }

    // ==================== Property Mapping Extraction ====================

    @Nested
    @DisplayName("Property Mapping Extraction")
    class PropertyMappingTests {

        @Test
        @DisplayName("Simple column mapping")
        void testSimpleColumnMapping() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME
                        }
                    )
                    """);

            var props = mapping.classMappings().get(0).propertyMappings();
            assertEquals(1, props.size());
            assertEquals("name", props.get(0).propertyName());
            assertNotNull(props.get(0).columnReference());
            assertEquals("NAME", props.get(0).columnReference().columnName());
        }

        @Test
        @DisplayName("Single join reference")
        void testSingleJoinReference() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            address: [store::DB] @Person_Address
                        }
                    )
                    """);

            var props = mapping.classMappings().get(0).propertyMappings();
            assertEquals(2, props.size());

            var addressProp = props.get(1);
            assertEquals("address", addressProp.propertyName());
            assertNotNull(addressProp.joinReference());
            assertEquals("Person_Address", addressProp.joinReference().joinName());
        }

        @Test
        @DisplayName("Enum transformer on column")
        void testEnumTransformerColumn() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            status: EnumerationMapping myEnum: [store::DB] T_PERSON.STATUS_CODE
                        }
                    )
                    """);

            var prop = mapping.classMappings().get(0).propertyMappings().get(0);
            assertEquals("status", prop.propertyName());
            assertNotNull(prop.columnReference());
            assertEquals("myEnum", prop.enumMappingId());
        }

        @Test
        @DisplayName("Multi-hop join chain")
        void testMultiHopJoinChain() {
            // Legend-pure: prop : [DB]@J1 > @J2 | T.COL
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            city: [store::DB] @Person_Address > @Address_City | T_CITY.NAME
                        }
                    )
                    """);

            var prop = mapping.classMappings().get(0).propertyMappings().get(0);
            assertEquals("city", prop.propertyName());
            assertNotNull(prop.joinReference());
            assertTrue(prop.joinReference().isMultiHop());
            assertEquals(2, prop.joinReference().joinChain().size());
            assertEquals("Person_Address", prop.joinReference().joinChain().get(0).joinName());
            assertEquals("Address_City", prop.joinReference().joinChain().get(1).joinName());
            assertNotNull(prop.joinReference().terminalColumn());
        }

        @Test
        @DisplayName("Join with join type (INNER)")
        void testJoinWithType() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            address: [store::DB] (INNER) @Person_Address
                        }
                    )
                    """);

            var prop = mapping.classMappings().get(0).propertyMappings().get(0);
            assertNotNull(prop.joinReference());
            assertEquals("Person_Address", prop.joinReference().joinName());
            assertEquals("INNER", prop.joinReference().joinChain().get(0).joinType());
        }

        @Test
        @DisplayName("GAP: Embedded property mapping")
        void testEmbeddedPropertyMapping() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            firm
                            (
                                legalName: [store::DB] T_PERSON.FIRM_NAME,
                                employeeCount: [store::DB] T_PERSON.EMP_COUNT
                            )
                        }
                    )
                    """);

            var props = mapping.classMappings().get(0).propertyMappings();
            assertEquals(2, props.size());

            var firmProp = props.get(1);
            assertEquals("firm", firmProp.propertyName());
            assertNotNull(firmProp.structuredValue(), "Should have structured embedded value");
            assertInstanceOf(com.gs.legend.model.def.PropertyMappingValue.EmbeddedMapping.class,
                    firmProp.structuredValue());
            var embedded = (com.gs.legend.model.def.PropertyMappingValue.EmbeddedMapping) firmProp.structuredValue();
            assertEquals(2, embedded.properties().size());
            assertEquals("legalName", embedded.properties().get(0).propertyName());
            assertEquals("employeeCount", embedded.properties().get(1).propertyName());
        }

        @Test
        @DisplayName("GAP: Inline property mapping")
        void testInlinePropertyMapping() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            firm() Inline[firm_set1]
                        }
                    )
                    """);

            var props = mapping.classMappings().get(0).propertyMappings();
            var firmProp = props.get(1);
            assertEquals("firm", firmProp.propertyName());
            assertNotNull(firmProp.structuredValue(), "Should have structured inline value");
            assertInstanceOf(com.gs.legend.model.def.PropertyMappingValue.InlineMapping.class,
                    firmProp.structuredValue());
            var inline = (com.gs.legend.model.def.PropertyMappingValue.InlineMapping) firmProp.structuredValue();
            assertEquals("firm_set1", inline.targetSetId());
        }

        @Test
        @DisplayName("Otherwise property mapping")
        void testOtherwisePropertyMapping() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            firm
                            (
                                legalName: [store::DB] T_PERSON.FIRM_NAME
                            ) Otherwise ([firm_set1]: [store::DB] @Person_Firm)
                        }
                    )
                    """);

            var props = mapping.classMappings().get(0).propertyMappings();
            var firmProp = props.get(1);
            assertEquals("firm", firmProp.propertyName());
            assertNotNull(firmProp.structuredValue(), "Should have structured otherwise value");
            assertInstanceOf(com.gs.legend.model.def.PropertyMappingValue.OtherwiseMapping.class,
                    firmProp.structuredValue());
            var ow = (com.gs.legend.model.def.PropertyMappingValue.OtherwiseMapping) firmProp.structuredValue();
            // Embedded sub-properties
            assertEquals(1, ow.embedded().properties().size());
            assertEquals("legalName", ow.embedded().properties().get(0).propertyName());
            // Fallback
            assertEquals("firm_set1", ow.fallbackSetId());
            assertNotNull(ow.fallbackJoin());
            assertEquals("Person_Firm", ow.fallbackJoin().joinChain().get(0).joinName());
        }

        @Test
        @DisplayName("GAP: DynaFunction expression in property mapping")
        void testDynaFunctionExpression() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            fullName: concat([store::DB] T_PERSON.FIRST, ' ', [store::DB] T_PERSON.LAST)
                        }
                    )
                    """);

            var prop = mapping.classMappings().get(0).propertyMappings().get(0);
            assertEquals("fullName", prop.propertyName());
            // Verify structured FunctionCall is produced (not raw expression string)
            assertTrue(prop.hasMappingExpression(), "Should have structured mappingExpression");
            var expr = prop.mappingExpression();
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.FunctionCall.class, expr);
            var fc = (com.gs.legend.model.def.RelationalOperation.FunctionCall) expr;
            assertEquals("concat", fc.name());
            assertEquals(3, fc.args().size());
            // arg[0]: column ref FIRST
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.ColumnRef.class, fc.args().get(0));
            // arg[1]: string literal ' '
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.Literal.class, fc.args().get(1));
            var lit = (com.gs.legend.model.def.RelationalOperation.Literal) fc.args().get(1);
            assertEquals(" ", lit.value());
            // arg[2]: column ref LAST
            assertInstanceOf(com.gs.legend.model.def.RelationalOperation.ColumnRef.class, fc.args().get(2));
        }

        @Test
        @DisplayName("GAP: Local mapping property (+)")
        void testLocalMappingProperty() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            +localProp: String[1]: [store::DB] T_PERSON.EXTRA
                        }
                    )
                    """);

            var props = mapping.classMappings().get(0).propertyMappings();
            assertEquals(2, props.size());
            var localProp = props.get(1);
            assertEquals("localProp", localProp.propertyName());
            // TODO: localProp.isLocal() should return true
            // TODO: localProp.localType() should return "String"
            // TODO: localProp.localMultiplicity() should return "[1]"
        }

        @Test
        @DisplayName("GAP: Source/target mapping IDs on property")
        void testSourceTargetMappingIds() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME,
                            firm[person_src, firm_tgt]: [store::DB] @Person_Firm
                        }
                    )
                    """);

            var firmProp = mapping.classMappings().get(0).propertyMappings().get(1);
            assertEquals("firm", firmProp.propertyName());
            // TODO: firmProp.sourceSetId() should return "person_src"
            // TODO: firmProp.targetSetId() should return "firm_tgt"
        }
    }

    // ==================== Association Mapping Extraction ====================

    @Nested
    @DisplayName("Association Mapping Extraction")
    class AssociationMappingTests {

        @Test
        @DisplayName("Association mapping extraction")
        void testAssociationMappingExtraction() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME
                        }

                        PersonFirmAssociation: AssociationMapping
                        (
                            persons: [store::DB] @Firm_Person,
                            firm: [store::DB] @Person_Firm
                        )
                    )
                    """);

            assertEquals(1, mapping.classMappings().size());
            assertEquals(1, mapping.associationMappings().size());

            var assoc = mapping.associationMappings().get(0);
            assertEquals("PersonFirmAssociation", assoc.associationName());
            assertEquals("Relational", assoc.mappingType());
            assertEquals(2, assoc.properties().size());
            assertEquals("persons", assoc.properties().get(0).propertyName());
            assertEquals("Firm_Person", assoc.properties().get(0).joinChain().get(0).joinName());
            assertEquals("firm", assoc.properties().get(1).propertyName());
            assertEquals("Person_Firm", assoc.properties().get(1).joinChain().get(0).joinName());
        }
    }

    // ==================== Mapping Include Extraction ====================

    @Nested
    @DisplayName("Mapping Include Extraction")
    class IncludeTests {

        @Test
        @DisplayName("Simple include extraction")
        void testSimpleInclude() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::ExtendedMapping
                    (
                        include model::BaseMapping

                        Person: Relational
                        {
                            ~mainTable [store::DB] T_PERSON
                            name: [store::DB] T_PERSON.NAME
                        }
                    )
                    """);

            assertEquals(1, mapping.classMappings().size());
            assertEquals(1, mapping.includes().size());
            assertEquals("model::BaseMapping", mapping.includes().get(0).includedMappingPath());
            assertTrue(mapping.includes().get(0).storeSubstitutions().isEmpty());
        }

        @Test
        @DisplayName("Include with store substitution")
        void testIncludeWithStoreSubstitution() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::ExtendedMapping
                    (
                        include model::BaseMapping[store::DevDB -> store::ProdDB]

                        Person: Relational
                        {
                            ~mainTable [store::ProdDB] T_PERSON
                            name: [store::ProdDB] T_PERSON.NAME
                        }
                    )
                    """);

            assertEquals(1, mapping.includes().size());
            var inc = mapping.includes().get(0);
            assertEquals("model::BaseMapping", inc.includedMappingPath());
            assertEquals(1, inc.storeSubstitutions().size());
            assertEquals("store::DevDB", inc.storeSubstitutions().get(0).originalStore());
            assertEquals("store::ProdDB", inc.storeSubstitutions().get(0).substituteStore());
        }
    }

    // ==================== Enumeration Mapping Extraction ====================

    @Nested
    @DisplayName("Enumeration Mapping Extraction")
    class EnumerationMappingTests {

        @Test
        @DisplayName("Enumeration mapping with single values")
        void testEnumMappingSingleValues() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        model::Status: EnumerationMapping StatusMap
                        {
                            ACTIVE: 'A',
                            INACTIVE: 'I'
                        }
                    )
                    """);

            assertEquals(1, mapping.enumerationMappings().size());
            var em = mapping.enumerationMappings().get(0);
            assertEquals("model::Status", em.enumType());
            assertNotNull(em.valueMappings());
        }

        @Test
        @DisplayName("Enumeration mapping with array values")
        void testEnumMappingArrayValues() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Status: EnumerationMapping
                        {
                            ACTIVE: ['A', 'ACT', 'ACTIVE'],
                            INACTIVE: 'I'
                        }
                    )
                    """);

            var em = mapping.enumerationMappings().get(0);
            var activeValues = em.valueMappings().get("ACTIVE");
            assertNotNull(activeValues);
            assertEquals(3, activeValues.size());
        }

        @Test
        @DisplayName("Enumeration mapping with integer values")
        void testEnumMappingIntegerValues() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Status: EnumerationMapping
                        {
                            ACTIVE: 1,
                            INACTIVE: 0
                        }
                    )
                    """);

            var em = mapping.enumerationMappings().get(0);
            assertNotNull(em.valueMappings());
            assertEquals(2, em.valueMappings().size());
        }
    }

    // ==================== Pure M2M Mapping Extraction ====================

    @Nested
    @DisplayName("Pure M2M Mapping Extraction")
    class PureM2MTests {

        @Test
        @DisplayName("Simple M2M mapping")
        void testSimpleM2M() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        TargetPerson: Pure
                        {
                            ~src SourcePerson
                            fullName: $src.firstName + ' ' + $src.lastName,
                            age: $src.age
                        }
                    )
                    """);

            var cm = mapping.classMappings().get(0);
            assertEquals("TargetPerson", cm.className());
            assertEquals("Pure", cm.mappingType());
            assertEquals("SourcePerson", cm.sourceClassName());
            assertEquals(2, cm.m2mPropertyExpressions().size());
            assertNotNull(cm.m2mPropertyExpressions().get("fullName"));
            assertNotNull(cm.m2mPropertyExpressions().get("age"));
        }

        @Test
        @DisplayName("M2M mapping with filter")
        void testM2MWithFilter() {
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        ActivePerson: Pure
                        {
                            ~src Person
                            ~filter $src.isActive == true
                            name: $src.name
                        }
                    )
                    """);

            var cm = mapping.classMappings().get(0);
            assertNotNull(cm.filterExpression());
        }
    }

    // ==================== Scope Block Extraction ====================

    @Nested
    @DisplayName("Scope Block Extraction")
    class ScopeBlockTests {

        @Test
        @Disabled("GRAMMAR GAP: scope keyword exists in lexer but no grammar rule in relationalClassMappingBody")
        @DisplayName("GAP: Scope block desugaring")
        void testScopeBlock() {
            // scope([DB]T) (p1 : C1, p2 : C2) should desugar to
            // p1 : [DB] T.C1, p2 : [DB] T.C2
            var mapping = parseMapping("""
                    import model::*;

                    ###Mapping
                    Mapping model::M
                    (
                        Person: Relational
                        {
                            scope([store::DB] T_PERSON)
                            (
                                firstName: FIRST_NAME,
                                lastName: LAST_NAME
                            )
                        }
                    )
                    """);

            var props = mapping.classMappings().get(0).propertyMappings();
            // TODO: scope should desugar into 2 column mappings with
            // databaseName="store::DB", tableName="T_PERSON"
            // Currently falls back to expression strings
        }

        @Test
        @Disabled("GRAMMAR GAP: scope keyword exists in lexer but no grammar rule in relationalClassMappingBody")
        @DisplayName("GAP: Scope block with embedded mapping (legend-pure TestMappingGrammar)")
        void testScopeBlockWithEmbedded() {
            // From TestMappingGrammar.testCombinationOfDistinctWithEmbeddedPropertyMappings
            var mapping = parseMapping("""
                    ###Mapping
                    Mapping FirmMapping
                    (
                        Firm: Relational
                        {
                            ~distinct
                            scope([FirmDb] FirmTable)
                            (
                                legalName: legal_name,
                                details(taxLocation: tax_location, extraDetails(employeeCount: employee_count, taxLocation: tax_location))
                            )
                        }
                    )
                    """);

            // TODO: This should parse into:
            // - distinct = true
            // - 2 properties: legalName (column), details (embedded with nested embedded)
        }
    }

    // ==================== Multiple Mapping Sets ====================

    @Nested
    @DisplayName("Multiple Mapping Sets")
    class MultipleMappingSetsTests {

        @Test
        @DisplayName("GAP: Two set implementations for same class (legend-pure TestMappingGrammar)")
        void testMultipleSetsForSameClass() {
            var mapping = parseMapping("""
                    ###Mapping
                    Mapping myMap
                    (
                        *Firm[m1]: Relational
                        {
                            ~mainTable [db] firmTb
                            name: [db] firmTb.name
                        }
                        Firm[m2]: Relational
                        {
                            ~mainTable [db] firmTb
                            name: [db] firmTb.name
                        }
                    )
                    """);

            assertEquals(2, mapping.classMappings().size());
            // TODO: First should have setId="m1", isRoot=true
            // TODO: Second should have setId="m2", isRoot=false
        }
    }
}
