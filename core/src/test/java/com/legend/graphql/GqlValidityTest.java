// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql;

import com.legend.compiler.graphql.GqlValidator;
import com.legend.compiler.graphql.GqlValidator.Limits;
import com.legend.compiler.graphql.GqlValidator.Validation;
import com.legend.json.Json;
import com.legend.parser.GqlParser;
import com.legend.protocol.graphql.GqlSchema.Schema;
import com.legend.protocol.spec.Gql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * THE GRAPHQL-JS DIFFERENTIAL, lite's half (docs/GRAPHQL_STUDY_2026_10_18.md §9.3, G1; PR1b):
 * {@code validity/validity.jsonl} holds graphql-js's verdict on each document {@code tools/graphql-js/differential.mjs}
 * wrote over the golden {@code demo.graphql} (the base documents as written, and their seeded mutations). This
 * test runs lite's validator over the same schema and asserts the rule of G1: lite rejects everything graphql-js
 * rejects (lite's acceptance a subset of graphql-js's, never more). Where lite is STRICTER (graphql-js accepts,
 * lite refuses) the refusal is one of §6.3's named rules, listed here by the document's id with its rule, so a
 * new strictness is a reviewed change.
 */
class GqlValidityTest {

    record Verdict(String id, String base, String mutation, String document, boolean valid, List<String> errors) {
    }

    static List<Verdict> verdicts() {
        List<Verdict> out = new ArrayList<>();
        for (String line : SchemaGeneratorTest.resource("validity/validity.jsonl").split("\n")) {
            if (line.isBlank()) {
                continue;
            }
            Json.Obj o = (Json.Obj) Json.parse(line);
            List<String> errors = new ArrayList<>();
            for (Json.Node e : o.getArr("errors").items()) {
                errors.add(((Json.Obj) e).getString("message"));
            }
            out.add(new Verdict(o.getString("id"), o.getString("base"), o.getString("mutation"),
                    o.getString("document"), o.getBool("valid"), errors));
        }
        return out;
    }

    /** lite's verdict: the parser's refusal or the validator's errors; empty where lite accepts. */
    static List<String> lite(Schema schema, String document) {
        Gql.Document doc;
        try {
            doc = GqlParser.parseDocument(document, 1, 1);
        } catch (RuntimeException e) {
            return List.of("SYNTAX: " + e.getMessage());
        }
        Validation v = GqlValidator.validate(doc, schema, Limits.DEFAULT, null);
        List<String> out = new ArrayList<>();
        for (var p : v.errors()) {
            out.add(p.path() + ": " + p.message());
        }
        return out;
    }

    /** The documents graphql-js accepts and lite refuses, each by the §6.3 rule that refuses it (G16: one key per
     *  sort object). A document here is stricter by design; one not here fails the test. */
    static final Map<String, String> STRICTER = Map.of(
            "twoKeysInOneSortObject", "G16: a sort key names exactly one property");

    @Test
    void liteRejectsEverythingGraphqlJsRejects() {
        Schema schema = SchemaGeneratorTest.demoSchema();
        List<Verdict> all = verdicts();
        assertTrue(all.size() >= 200, "the differential wrote " + all.size() + " documents");
        List<String> looser = new ArrayList<>();
        Map<String, Integer> stricter = new LinkedHashMap<>();
        List<String> unexplained = new ArrayList<>();
        int agreed = 0;
        for (Verdict v : all) {
            List<String> mine = lite(schema, v.document());
            boolean liteValid = mine.isEmpty();
            if (!v.valid() && liteValid) {
                looser.add(v.id() + " (" + v.mutation() + "): graphql-js says " + v.errors() + " but lite accepts: "
                        + v.document());
            } else if (v.valid() && !liteValid) {
                stricter.merge(v.mutation(), 1, Integer::sum);
                if (!STRICTER.containsKey(v.mutation())) {
                    unexplained.add(v.id() + " (" + v.mutation() + "): lite says " + mine + " on " + v.document());
                }
            } else {
                agreed++;
            }
        }
        assertEquals(List.of(), looser, "G1: lite accepts what graphql-js rejects");
        assertEquals(List.of(), unexplained, "lite is stricter without a named rule");
        assertTrue(agreed > all.size() * 0.9, "agreed on " + agreed + " of " + all.size() + "; stricter: " + stricter);
    }

    @Test
    void everyBaseDocumentIsValidForBoth() {
        Schema schema = SchemaGeneratorTest.demoSchema();
        List<String> problems = new ArrayList<>();
        for (Verdict v : verdicts()) {
            if (!v.mutation().equals("none")) {
                continue;
            }
            if (!v.valid()) {
                problems.add(v.base() + ": graphql-js: " + v.errors());
            }
            List<String> mine = lite(schema, v.document());
            if (!mine.isEmpty()) {
                problems.add(v.base() + ": lite: " + mine + " on " + v.document());
            }
        }
        assertEquals(List.of(), problems);
    }
}
