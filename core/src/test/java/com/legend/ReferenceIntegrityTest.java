// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

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
    void anAssociationsAndADerivedPropertysAnnotationsAreChecked() {
        // an association's own annotations were dropped at the door, a derived property's never resolved (2026-10-21)
        refused("Association <<a::NoProfile.s>> a::AB\n{\n  c: a::C[1];\n  d: a::C[*];\n}\n",
                "Can't find the profile 'a::NoProfile'");
        refused("Association {a::P.nope = 'v'} a::AB\n{\n  c: a::C[1];\n  d: a::C[*];\n}\n",
                "Can't find tag 'nope' in profile 'a::P'");
        refused("Association a::AB\n{\n  {a::P.nope = 'v'} c: a::C[1];\n  d: a::C[*];\n}\n",
                "Can't find tag 'nope' in profile 'a::P'");
        refused("Class a::B\n{\n  <<a::NoProfile.s>> d() {1}: Integer[1];\n}\n", "Can't find the profile 'a::NoProfile'");
        accepted("Class a::B\n{\n  <<a::P.s>> {a::P.t = 'v'} d() {1}: Integer[1];\n}\n"
                + "Association <<a::P.s>> a::AB\n{\n  c: a::C[1];\n  d: a::C[*];\n}\n");
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

    // ---- default values

    @Test
    void aDefaultValueIsTypedAsItsProperty() {
        // in a form legend-engine's grammar reads (defaultValueExpression: an enum value, a new instance, a literal)
        refused("Class a::B\n{\n  e: a::E[1] = a::Missing.A;\n}\n", "unknown enumeration 'a::Missing'");
        refused("Class a::B\n{\n  c: a::C[1] = ^a::Missing();\n}\n", "unknown class 'a::Missing'");
        refused("Class a::B\n{\n  x: Integer[1] = 'a';\n}\n", "in function 'a::B$default$x': declares return type Integer but body returns String");
        accepted("Class a::B\n{\n  x: Integer[1] = 5;\n  b: Boolean[1] = false;\n  e: a::E[1] = a::E.A;\n}\n");
    }

    @Test
    void aDefaultIsCheckedAsABodyIs() {
        // legend-pure checks a default's type and multiplicity only (PropertyValidator), so the engine's own
        // State.debug = ^DebugContext() passes there; lite checks it as any body, legend-pure's NewValidator rule (S90)
        refused("Class a::B\n{\n  d: meta::pure::tools::DebugContext[1] = ^meta::pure::tools::DebugContext();\n}\n",
                "Missing value(s) for required property 'debug' which has a multiplicity of [1]");
    }

    @Test
    void anOptionalPropertyHasNoDefault() {
        refused("Class a::B\n{\n  x: Integer[0..1] = 5;\n}\n",
                "Default values are supported only for mandatory fields, and property 'x' is optional.");
    }

    // ---- a function's constraints (lite's dialect: legend-engine's grammar drops the block, rule A)

    @Test
    void aFunctionsConditionsAreTypedPredicates() {
        refusedAsText("function a::g(s: String[1]): String[1]\n[\n  1\n]\n{\n  $s\n}\n",
                "declares return type Boolean but body returns Integer");
        refusedAsText("function a::g(s: String[1]): String[1]\n[\n  $nope->startsWith('A')\n]\n{\n  $s\n}\n",
                "nope");
        // a post-condition reads $return, typed as the function's result
        refusedAsText("function a::g(s: String[1]): String[1]\n[\n  $return > 2\n]\n{\n  $s\n}\n", "a::g$post$0");
        accepted("function a::g(s: String[1]): String[1]\n[\n  pre: $s->startsWith('A'),\n"
                + "  post: $return->endsWith('A')\n]\n{\n  $s\n}\n");
    }

    // ---- what a refusal says

    @Test
    void aMissingDatabaseIsSaidToBeMissing() {
        // was "unknown table 'T' in database 'a::NoDb'"
        refused("###Mapping\nMapping a::M\n(\n  a::C: Relational\n  {\n    x: [a::NoDb]T.X\n  }\n)\n",
                "Can't find database 'a::NoDb'");
    }

    @Test
    void anUnsupportedExtendsNamesItsMappingsKind() {
        // was "... on a non-Relational (Pure) mapping" for an Operation mapping too
        refusedAsText("###Mapping\nMapping a::M\n(\n  *a::C extends [base] : Operation\n  {\n"
                + "    meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(p1)\n"
                + "  }\n)\n", "on an Operation (union) class mapping");
    }
}
