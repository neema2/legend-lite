// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.parser.Dialect;
import com.legend.parser.PmcdParser;
import com.legend.protocol.Protocol.PureModelContextData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE USER'S RULE A (docs/SEMANTICS_REGISTER.md S89, 2026-10-21): text legend-engine's grammar accepts and its parser
 * drops is read at the engine's dialect exactly as the engine reads it -- so the records and their JSON are the
 * engine's, and a JSON hop gives the same records -- and kept at lite's dialect, whose JSON carries it where the
 * engine's JSON has a field for it as written and refuses it by name where it has none (never a silent drop).
 * {@code JsonModelDoorTest} counts every case of the corpora; these are one of each kind.
 */
class EngineDropsTest {

    private static final String CLASSES = """
            ###Pure
            Class a::P
            {
              s: String[1];
              n: Integer[0..1];
            }
            Class a::Q
            {
              p: a::P[1];
            }
            """;

    /** The text's records at {@code dialect}, after asserting that a JSON hop gives the same records. */
    private static PureModelContextData hopped(String text, Dialect dialect) {
        PureModelContextData records = PmcdParser.parse(text, dialect).model();
        assertEquals(records.elements(), ModelReader.read(ProtocolEmitter.emit(records)).elements(),
                "the records written as JSON and read back");
        return records;
    }

    private static String engineJson(String text) {
        return ProtocolEmitter.emit(hopped(text, Dialect.LEGEND_ENGINE));
    }

    private static void refusedAtLitesDialect(String text, String what) {
        PureModelContextData lite = PmcdParser.parse(text, Dialect.LEGEND_LITE).model();
        UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class,
                () -> ProtocolEmitter.emit(lite));
        assertTrue(e.getMessage().startsWith(what + " is not written as legend-engine's JSON"), e.getMessage());
    }

    @Test
    void aBindingsChainIsItsFirstExpressionAtTheEnginesDialect_andWholeAtLites() {
        String text = CLASSES + "function a::f(v: String[1]): a::P[1]\n{\n  ^a::P(s = 'a' + $v)\n}\n";
        assertFalse(engineJson(text).contains("\"function\":\"plus\""), "the engine keeps 'a' alone");
        // lite's dialect writes the whole chain (the engine's JSON has room for it) and reads it back exactly
        assertTrue(ProtocolEmitter.emit(hopped(text, Dialect.LEGEND_LITE)).contains("\"function\":\"plus\""));
    }

    @Test
    void anAddingBindingIsAnAssignmentAtTheEnginesDialect_andAddsAtLites() {
        String text = CLASSES + "function a::f(): a::P[1]\n{\n  ^a::P(s += 'x')\n}\n";
        assertTrue(engineJson(text).contains("\"add\":false"));
        assertTrue(ProtocolEmitter.emit(hopped(text, Dialect.LEGEND_LITE)).contains("\"add\":true"));
    }

    @Test
    void aDottedKeyIsItsFirstNameAtTheEnginesDialect_andRefusedFromLites() {
        String text = CLASSES + "function a::f(): a::Q[1]\n{\n  ^a::Q(p.s = 'x')\n}\n";
        String engine = engineJson(text);
        assertTrue(engine.contains("\"key\":{\"_type\":\"string\",\"value\":\"p\"}"), engine);
        refusedAtLitesDialect(text, "a dotted key (the engine keys addr.city = .. by addr)");
    }

    @Test
    void aFunctionsConstraintsAreDroppedAtTheEnginesDialect_andWrittenFromLitesAsPreAndPost() {
        String text = CLASSES + "function a::g(s: String[1]): String[1]\n[\n  $s->startsWith('A'),\n"
                + "  $return->endsWith('A')\n]\n{\n  $s\n}\n";
        assertTrue(engineJson(text).contains("\"postConstraints\":[],\"preConstraints\":[]"));
        // legend-pure's split: the condition reading $return is a post-condition; written as the engine's compiler
        // reads them, and read back to the same records
        String lite = ProtocolEmitter.emit(hopped(text, Dialect.LEGEND_LITE));
        assertTrue(lite.contains("\"postConstraints\":[{"), lite);
        assertTrue(lite.contains("\"preConstraints\":[{"), lite);
    }

    @Test
    void anIncludesSeveralSubstitutionsAreDroppedAtTheEnginesDialect_andRefusedFromLites() {
        String text = "###Mapping\nMapping a::M\n(\n  include a::Base[a::db1 -> a::db3, a::db2 -> a::db3]\n)\n";
        assertFalse(engineJson(text).contains("DatabasePath"));
        refusedAtLitesDialect(text, "an include's several store substitutions");
        // one substitution is the engine's own (sourceDatabasePath, targetDatabasePath), at either dialect
        String one = "###Mapping\nMapping a::M\n(\n  include a::Base[a::db1 -> a::db3]\n)\n";
        assertTrue(engineJson(one).contains("\"sourceDatabasePath\":\"a::db1\""));
        hopped(one, Dialect.LEGEND_LITE);
    }

    @Test
    void anOperationMappingsExtendsIsDroppedAtTheEnginesDialect_andWrittenFromLites() {
        String text = "###Mapping\nMapping a::M\n(\n  *a::P extends [base] : Operation\n  {\n"
                + "    meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(p1, p2)\n"
                + "  }\n)\n";
        assertFalse(engineJson(text).contains("extendsClassMappingId"));
        assertTrue(ProtocolEmitter.emit(hopped(text, Dialect.LEGEND_LITE)).contains("\"extendsClassMappingId\":\"base\""));
    }

    @Test
    void aSupertypesTypeArgumentsAreDroppedAtTheEnginesDialect_andRefusedFromLites() {
        String text = "###Pure\nClass a::R extends a::G<String>\n{\n}\n";
        assertTrue(engineJson(text).contains("\"path\":\"a::G\""));
        refusedAtLitesDialect(text, "a supertype's type arguments");
    }

    @Test
    void aServiceStoresDescriptionIsDroppedAtTheEnginesDialect_andWrittenFromLites() {
        String text = "###ServiceStore\nServiceStore a::S\n(\n  description : 'my store';\n)\n";
        assertFalse(engineJson(text).contains("description"));
        assertTrue(ProtocolEmitter.emit(hopped(text, Dialect.LEGEND_LITE)).contains("\"description\":\"my store\""));
    }

    @Test
    void aModelStoresNegativeNumberIsOneLiteral_asTheEnginesDataWalkerReadsIt() {
        String text = CLASSES + "###Data\nData a::D\n{\n  ModelStore #{\n    a::P: [\n"
                + "      ^a::P(s = 'x', n = -1)\n    ]\n  }#\n}\n";
        String engine = engineJson(text);
        assertTrue(engine.contains("\"value\":-1"), engine);
        hopped(text, Dialect.LEGEND_LITE);
    }
}
