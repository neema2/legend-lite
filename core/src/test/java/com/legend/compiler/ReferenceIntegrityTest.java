// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.Compiler;
import com.legend.parser.PmcdParser;
import com.legend.protocol.ModelReader;
import com.legend.protocol.ProtocolEmitter;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every reference a model element makes names something that exists and is of the right kind, or the model is
 * refused -- whichever door it comes through (its text, or its JSON read into the same records). Each case is one a
 * probe of 2026-10-21 found lite accepting while legend-engine (or legend-pure, where the engine's grammar has no form
 * for it) refuses; the message is the engine's (or legend-pure's) words where lite had none of its own.
 */
class ReferenceIntegrityTest {

    private static final String BASE = """
            ###Pure
            Profile a::P
            {
              stereotypes: [s];
              tags: [t];
            }
            Class a::C
            {
              x: Integer[1];
              name: String[1];
            }
            Enum a::E
            {
              A, B
            }
            """;

    private static final String GENERIC = "Class a::G<T>\n{\n  v: T[1];\n}\n";

    /** {@code model} (after the base) compiled from its text; and, when legend-engine's grammar reads it, from its
     *  JSON too: one error, naming {@code expected}. */
    private static void refused(String model, String expected) {
        String text = BASE + model;
        assertOneError(Compiler.compileErrors(text), expected, "the text");
        if (engineReads(text)) {
            assertOneError(Compiler.compileErrors(ModelReader.read(ProtocolEmitter.emit(PmcdParser.parseModel(text)))),
                    expected, "the JSON");
        }
    }

    /** As {@link #refused}, for a construct legend-engine's grammar reads otherwise (rule A, S89: it drops a
     *  supertype's arguments), so only the text at lite's dialect carries it. */
    private static void refusedAsText(String model, String expected) {
        assertOneError(Compiler.compileErrors(BASE + model), expected, "the text");
    }

    private static void accepted(String model) {
        String text = BASE + model;
        assertEquals(List.of(), Compiler.compileErrors(text), "the text");
        if (engineReads(text)) {
            assertEquals(List.of(),
                    Compiler.compileErrors(ModelReader.read(ProtocolEmitter.emit(PmcdParser.parseModel(text)))),
                    "the JSON");
        }
    }

    private static boolean engineReads(String text) {
        try {
            PmcdParser.parseModel(text);
            return true;
        } catch (com.legend.parser.ParseException notTheEnginesGrammar) {
            return false;
        }
    }

    private static void assertOneError(List<String> errors, String expected, String door) {
        assertEquals(1, errors.size(), door + ": " + errors);
        assertTrue(errors.get(0).contains(expected), door + ": " + errors.get(0));
    }

    // ---- supertypes

    @Test
    void aSupertypeThatDoesNotExistIsRefused() {
        refused("Class a::B extends a::Missing\n{\n}\n", "Unknown type: 'a::Missing'");
    }

    @Test
    void aSupertypeThatIsNotAClassIsRefused() {
        refused("Class a::B extends a::E\n{\n}\n", "Invalid supertype: 'a::B' cannot extend 'a::E' as it is not a class.");
        refused("Class a::B extends String\n{\n}\n", "Invalid supertype: 'a::B' cannot extend 'meta::pure::metamodel::type::String' as it is not a class.");
    }

    @Test
    void aGenericSupertypesArgumentsAreItsParameters() {
        refused(GENERIC + "Class a::B extends a::G<String, Integer>\n{\n}\n",
                "Type argument mismatch for the class a::G<T> (expected 1, got 2)");
        refused(GENERIC + "Class a::B extends a::G<a::Missing>\n{\n}\n", "Unknown type: 'a::Missing'");
        refusedAsText("Class a::B extends a::C<String>\n{\n}\n",
                "Type argument mismatch for the class a::C<> (expected 0, got 1)");
        accepted(GENERIC + "Class a::B extends a::G<String>\n{\n}\n");
    }

    @Test
    void aGenericClassNamedWithoutItsArgumentsIsRefused() {
        refused(GENERIC + "Class a::B\n{\n  g: a::G[1];\n}\n", "Type argument mismatch for the class a::G<T> (expected 1, got 0)");
        refused("Class a::B\n{\n  g: a::Missing<String>[1];\n}\n", "Unknown type: 'a::Missing'");
        accepted(GENERIC + "Class a::B\n{\n  g: a::G<String>[1];\n}\n");
    }

    @Test
    void aClassMayExtendAnotherClassAndAny() {
        accepted("Class a::B extends a::C\n{\n}\nClass a::D extends Any\n{\n}\n");
    }

    // ---- the elements that only name others

    private static final String DB = "###Relational\nDatabase a::Db\n(\n  Table T (X INTEGER)\n)\n";

    @Test
    void aRuntimesMappingIsAMapping() {
        refused("###Runtime\nRuntime a::R\n{\n  mappings:\n  [\n    a::NoMapping\n  ];\n}\n",
                "Can't find mapping 'a::NoMapping'");
    }

    @Test
    void aConnectionsStoreIsAStore() {
        refused("###Connection\nRelationalDatabaseConnection a::Cn\n{\n  store: a::NoDb;\n  type: H2;\n"
                + "  specification: LocalH2\n  {\n  };\n  auth: DefaultH2;\n}\n", "The store 'a::NoDb' can't be found.");
    }

    @Test
    void aServicesMappingAndRuntimeExist() {
        refused("###Service\nService a::S\n{\n  pattern: '/x';\n  documentation: '';\n  autoActivateUpdates: true;\n"
                + "  execution: Single\n  {\n    query: |a::C.all()->project([x|$x.x], ['x']);\n"
                + "    mapping: a::NoMapping;\n    runtime: a::NoRuntime;\n  }\n}\n", "Can't find mapping 'a::NoMapping'");
    }

    @Test
    void aDataSpacesMappingAndRuntimeExist() {
        refused("###DataSpace\nDataSpace a::Ds\n{\n  executionContexts:\n  [\n    {\n      name: 'c';\n"
                + "      mapping: a::NoMapping;\n      defaultRuntime: a::NoRuntime;\n    }\n  ];\n"
                + "  defaultExecutionContext: 'c';\n}\n", "Can't find mapping 'a::NoMapping'");
    }

    @Test
    void aDatabasesIncludeIsADatabase() {
        refused("###Relational\nDatabase a::Db\n(\n  include a::NoDb\n  Table T (X INTEGER)\n)\n",
                "Can't find database 'a::NoDb'");
    }

    @Test
    void aMappingsIncludeIsAMappingAndItsSubstitutionsDatabases() {
        // a missing included mapping was an internal IllegalStateException (MappingClosures' never-fired probe)
        refused("###Mapping\nMapping a::M\n(\n  include a::NoMapping\n)\n", "Can't find mapping 'a::NoMapping'");
        refused(DB + "###Mapping\nMapping a::Base\n(\n)\nMapping a::M\n(\n  include a::Base[a::NoDb -> a::Db]\n)\n",
                "Can't find database 'a::NoDb'");
        accepted(DB + "###Mapping\nMapping a::Base\n(\n)\nMapping a::M\n(\n  include a::Base\n)\n");
    }

    @Test
    void anOperationsMemberSetsAreTheMappingsClassMappings() {
        refused("###Mapping\nMapping a::M\n(\n  *a::C : Operation\n  {\n"
                + "    meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(p1, p2)\n"
                + "  }\n)\n", "Can't find class mapping 'p1' in mapping 'a::M'");
    }

    @Test
    void theElementsThatNameOthersCompileWhenTheyExist() {
        accepted(DB + "###Mapping\nMapping a::M\n(\n)\n###Connection\nRelationalDatabaseConnection a::Cn\n{\n"
                + "  store: a::Db;\n  type: H2;\n  specification: LocalH2\n  {\n  };\n  auth: DefaultH2;\n}\n"
                + "###Runtime\nRuntime a::R\n{\n  mappings:\n  [\n    a::M\n  ];\n  connections:\n  [\n"
                + "    a::Db:\n    [\n      c1: a::Cn\n    ]\n  ];\n}\n");
    }

    // ---- profiles, stereotypes and tags

    @Test
    void aStereotypesProfileExistsAndDeclaresIt() {
        refused("Class <<a::NoProfile.s>> a::B\n{\n}\n", "Can't find the profile 'a::NoProfile'");
        refused("Class <<a::P.nope>> a::B\n{\n}\n", "Can't find stereotype 'nope' in profile 'a::P'");
        refused("Class <<a::P.t>> a::B\n{\n}\n", "Can't find stereotype 't' in profile 'a::P'");   // a tag, not a stereotype
        refused("Class a::B\n{\n  <<a::NoProfile.s>> p: String[1];\n}\n", "Can't find the profile 'a::NoProfile'");
        refused("function <<a::NoProfile.s>> a::f(): String[1]\n{\n  'x'\n}\n", "Can't find the profile 'a::NoProfile'");
        refused("Enum <<a::NoProfile.s>> a::F\n{\n  X\n}\n", "Can't find the profile 'a::NoProfile'");
        refused("Enum a::F\n{\n  <<a::P.nope>> X\n}\n", "Can't find stereotype 'nope' in profile 'a::P'");
    }

    @Test
    void aTagsProfileExistsAndDeclaresIt() {
        refused("Class {a::NoProfile.t = 'v'} a::B\n{\n}\n", "Can't find the profile 'a::NoProfile'");
        refused("Class {a::P.nope = 'v'} a::B\n{\n}\n", "Can't find tag 'nope' in profile 'a::P'");
        refused("Class a::B\n{\n  {a::P.nope = 'v'} p: String[1];\n}\n", "Can't find tag 'nope' in profile 'a::P'");
        refused("function {a::P.nope = 'v'} a::f(): String[1]\n{\n  'x'\n}\n", "Can't find tag 'nope' in profile 'a::P'");
    }

    @Test
    void thePlatformsProfilesAreTheModelsToo() {
        // legend-pure's own (the prelude carries them), reached through the auto-imports as the engine reaches them
        accepted("Class <<temporal.businesstemporal>> a::B\n{\n  <<equality.Key>> k: String[1];\n}\n"
                + "Class {doc.doc = 'a person'} a::D\n{\n}\n"
                + "Class <<a::P.s>> {a::P.t = 'v'} a::F\n{\n}\n");
        refused("Class <<temporal.nope>> a::B\n{\n}\n",
                "Can't find stereotype 'nope' in profile 'meta::pure::profiles::temporal'");
    }
}
