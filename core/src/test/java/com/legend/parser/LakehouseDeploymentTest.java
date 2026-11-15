// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.protocol.ModelComposer;
import com.legend.protocol.ModelReader;
import com.legend.protocol.Protocol;
import com.legend.protocol.ProtocolEmitter;
import com.legend.protocol.SourceInformation;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code ###Lakehouse}'s Deployment, read and written (docs/DEPLOYMENT_DESIGN_2026_11_14.md §8; lite's element,
 * {@code _type:"lakehouseDeployment"}): text to the record and back, the record to JSON and back, and the grammar's
 * refusals by name. Its checks and its plan: {@code planner.DeploymentPlannerTest}.
 */
class LakehouseDeploymentTest {

    private static List<Protocol.Element> parse(String text) {
        return PmcdParser.parse(text, Dialect.LEGEND_LITE).model().elements().stream()
                .filter(e -> e instanceof Protocol.PDeployment).toList();
    }

    /** text -> record -> JSON -> record: the same JSON, with spans and without; text -> record -> text -> record: the
     *  same record, spans aside. */
    @Test
    void theElementGoesToJsonAndTextAndBack() {
        String full = """
                ###Lakehouse
                Deployment t::Everywhere
                {
                  deploys: [ t::Feed, t::News ];
                  sources: [ t::Brokers: [ t::Topics ], t::Drop: [ t::RefLoad ] ];
                  live: t::Server keep [ appended: LastRows(500), milestoned: current, NEWS: Last('PT8H') ];
                  history: t::Lake keep [ appended: all, milestoned: versions ];
                }
                Deployment t::Least
                {
                  deploys: [ t::Feed ];
                  history: t::Lake;
                }
                """;
        List<Protocol.Element> elements = parse(full);
        assertEquals(2, elements.size());
        for (Protocol.Element e : elements) {
            String json = ProtocolEmitter.emitElement(e);
            assertEquals(json, ProtocolEmitter.emitElement(ModelReader.readElement(json)));
            String spanless = SourceInformation.stripAll(json);
            assertEquals(spanless, SourceInformation.stripAll(ProtocolEmitter.emitElement(ModelReader.readElement(spanless))));
            String printed = ModelComposer.element(e);
            List<Protocol.Element> again = parse("###Lakehouse\n" + printed + "\n");
            assertEquals(spanless, SourceInformation.stripAll(ProtocolEmitter.emitElement(again.get(0))), printed);
        }
        Protocol.PDeployment d = (Protocol.PDeployment) elements.get(0);
        assertEquals(List.of(new Protocol.PKeep("appended", new Protocol.PKeepRows(500)),
                new Protocol.PKeep("milestoned", new Protocol.PKeepCurrent()),
                new Protocol.PKeep("NEWS", new Protocol.PKeepFor("PT8H"))), d.live().keep());
        assertTrue(ProtocolEmitter.emitElement(d).startsWith("{\"_type\":\"lakehouseDeployment\""));
    }

    private static void grammarRefused(String body, String message) {
        ParseException e = assertThrows(ParseException.class, () -> PmcdParser.parse(
                "###Lakehouse\nDeployment t::D\n{\n" + body + "\n}\n", Dialect.LEGEND_LITE));
        assertTrue(String.valueOf(e.getMessage()).contains(message), body + " -> " + e.getMessage());
    }

    @Test
    void theGrammarRefusesByName() {
        grammarRefused("live: t::Server;", "deploys");
        grammarRefused("deploys: [ t::Feed ];", "target");
        grammarRefused("deploys: [ t::Feed ]; deploys: [ t::News ]; live: t::Server;", "deploys");
        grammarRefused("deploys: [ t::Feed ]; live: t::Server keep [ appended: all, appended: LastRows(5) ];",
                "appended");
        grammarRefused("deploys: [ t::Feed ]; live: t::Server keep [ appended: Forever ];", "Forever");
        grammarRefused("deploys: [ t::Feed ]; elsewhere: t::Server;", "elsewhere");
    }
}
