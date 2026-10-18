// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql;

import com.legend.compiler.graphql.GqlSchema;
import com.legend.compiler.graphql.GqlSchemaException;
import com.legend.compiler.graphql.GqlValidator;
import com.legend.compiler.graphql.SchemaGenerator;
import com.legend.compiler.graphql.Scope;
import com.legend.compiler.graphql.SdlPrinter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.graphql.GqlSchema.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * The schema generator against the worked model (docs/GRAPHQL_STUDY_2026_10_18.md §7), pinned by the golden
 * {@code graphql-parity/schema/demo.graphql}: a change to a generator rule is a reviewed diff of the SDL. Set
 * {@code -Dgraphql.schema.write=<path>} to rewrite a golden from the generator.
 */
class SchemaGeneratorTest {

    static String resource(String path) {
        try (InputStream in = SchemaGeneratorTest.class.getResourceAsStream("/graphql-parity/" + path)) {
            assertNotNull(in, "resource graphql-parity/" + path);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static ModelContext demo() {
        return Compiler.compileModel(resource("models/demo.pure"));
    }

    static Schema demoSchema() {
        ModelContext ctx = demo();
        return SchemaGenerator.generate(ctx, Scope.all(ctx));
    }

    @Test
    void theDemoSchemaMatchesItsGolden() throws IOException {
        String sdl = SdlPrinter.print(demoSchema());
        String write = System.getProperty("graphql.schema.write");
        if (write != null) {
            Files.writeString(Path.of(write), sdl);
        }
        assertEquals(resource("schema/demo.graphql"), sdl, "the generated SDL of models/demo.pure");
    }

    @Test
    void aClassWithSubclassesIsAnInterfaceAndAnObject() {
        Schema s = demoSchema();
        assertTrue(s.find("PersonInterface") instanceof GqlSchema.InterfaceType, "PersonInterface");
        assertTrue(s.find("Person") instanceof GqlSchema.ObjectType, "Person");
        assertTrue(s.find("EmployeeInterface") instanceof GqlSchema.InterfaceType, "EmployeeInterface");
        GqlSchema.ObjectType manager = (GqlSchema.ObjectType) s.find("Manager");
        assertNotNull(manager);
        assertEquals(java.util.List.of("EmployeeInterface", "PersonInterface"), manager.implementsList(),
                "a leaf class implements every ancestor's interface, transitively");
        GqlSchema.FieldDef root = s.query().field("Person");
        assertNotNull(root);
        assertEquals("[PersonInterface!]!", GqlSchema.spell(root.type()), "a root is typed by the interface");
    }

    @Test
    void theInputSideCarriesTheDowncasts() {
        Schema s = demoSchema();
        GqlSchema.InputObjectType where = (GqlSchema.InputObjectType) s.find("PersonWhere");
        assertNotNull(where);
        assertNotNull(where.field("asEmployee"), "asEmployee in PersonWhere (G9)");
        assertNotNull(where.field("asManager"), "asManager in PersonWhere, transitively");
        assertNotNull(where.field("firm"), "the association-injected to-one as a nested where");
        GqlSchema.InputObjectType firmWhere = (GqlSchema.InputObjectType) s.find("FirmWhere");
        assertNotNull(firmWhere);
        GqlSchema.InputValueDef employees = firmWhere.field("employees");
        assertNotNull(employees, "the association-injected to-many as a list filter");
        assertEquals("PersonListFilter", GqlSchema.spell(employees.type()));
        GqlSchema.InputObjectType order = (GqlSchema.InputObjectType) s.find("FirmOrderBy");
        assertNotNull(order);
        assertNotNull(order.field("employeesAggregate"), "ordering a firm by an aggregate of its employees");
    }

    @Test
    void aCollidingPropertyIsRefusedByName() {
        ModelContext ctx = Compiler.compileModel("""
                ###Pure
                Class demo::Firm { legalName: String[1]; employeesAggregate: Integer[1]; }
                Class demo::Person { name: String[1]; }
                Association demo::Employment { firm: demo::Firm[0..1]; employees: demo::Person[*]; }
                """);
        GqlSchemaException e = assertThrows(GqlSchemaException.class,
                () -> SchemaGenerator.generate(ctx, Scope.all(ctx)));
        assertTrue(e.getMessage().contains("demo::Firm.employeesAggregate"), e.getMessage());
    }

    @Test
    void anAbstractClassIsAnInterfaceOnly() {
        ModelContext ctx = Compiler.compileModel("""
                ###Pure
                Class <<typemodifiers.abstract>> demo::Shape { name: String[1]; }
                Class demo::Circle extends demo::Shape { radius: Float[1]; }
                """);
        Schema s = SchemaGenerator.generate(ctx, Scope.all(ctx));
        assertTrue(s.find("ShapeInterface") instanceof GqlSchema.InterfaceType, "ShapeInterface");
        assertEquals(null, s.find("Shape"), "no concrete Shape");
        GqlSchema.FieldDef root = s.query().field("Shape");
        assertNotNull(root);
        assertEquals("[ShapeInterface!]!", GqlSchema.spell(root.type()));
    }

    static com.legend.model.DataSpaceDefinition dataSpace(String model, String name) {
        return (com.legend.model.DataSpaceDefinition) Compiler.parseModel(model).elements().stream()
                .filter(e -> e instanceof com.legend.model.DataSpaceDefinition ds && ds.qualifiedName().equals(name))
                .findFirst().orElseThrow();
    }

    @Test
    void aDataSpaceScopesTheClassesAndTheirAssociations() {
        String model = resource("models/demo.pure") + """
                ###DataSpace
                DataSpace demo::Firms
                {
                  executionContexts: [];
                  elements: [ demo::Firm ];
                }
                DataSpace demo::Staff
                {
                  executionContexts: [];
                  elements: [ demo::Employee ];
                }
                """;
        ModelContext ctx = Compiler.compileModel(model);
        Schema firms = SchemaGenerator.generate(ctx, Scope.of(ctx, dataSpace(model, "demo::Firms")));
        GqlSchema.ObjectType firm = (GqlSchema.ObjectType) firms.find("Firm");
        assertNotNull(firm);
        assertEquals(null, firm.field("employees"), "an association to a class out of scope injects nothing");
        assertEquals(null, firms.find("Person"), "Person is out of scope");
        GqlSchemaException e = assertThrows(GqlSchemaException.class,
                () -> SchemaGenerator.generate(ctx, Scope.of(ctx, dataSpace(model, "demo::Staff"))));
        assertTrue(e.getMessage().contains("demo::Employee extends demo::Person"), "a superclass out of scope is"
                + " refused: " + e.getMessage());
    }
}
