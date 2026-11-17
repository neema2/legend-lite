// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.ModelContext;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.testdatagen.TestDataGenerationNatives;
import com.legend.testdatagen.TestDataGenerator;

import java.util.ArrayList;
import java.util.List;

/**
 * legend-engine's planTestDataGeneration plan TEXT (Phase 6's B13, 2026-11-17, moved from testdatagen's printer): a
 * MultiResultSequence of per-fetch Allocations ({@code res_cN} names in tree order), each a Relational node whose SQL
 * is the engine text dialect's spelling of the fetch the generator built ({@link TestDataGenerator#planFetches}),
 * parents referenced as {@code ${res_cN}} placeholders; a root with no row identifiers an Error node over its sample.
 * Pure text over the generator's data, no execution.
 */
final class TestDataPlanText {

    private TestDataPlanText() {
    }

    /** planToString over a test-data carrier: the {@code plan} flavor prints the engine's MultiResultSequence text;
     *  any other flavor is not a plan (loud). */
    static String planTextResult(com.legend.compiler.spec.typed.TypedTestDataGen g, ModelContext ctx,
            EngineTextDialect text, String connection) {
        if (!"plan".equals(g.flavor())) {
            throw new com.legend.error.NotImplementedException(
                    "planToString over a non-plan test-data carrier ("
                            + g.flavor() + ")");
        }
        List<ValueSpecification> ps = g.params();
        LambdaFunction query = (LambdaFunction) ps.get(0);
        String mappingFqn = ((com.legend.protocol.spec.PackageableElementPtr) ps.get(1)).fullPath();
        List<TestDataGenerator.TableRowIds> rowIds = new ArrayList<>();
        TestDataGenerator.MilestoningDates[] dates = new TestDataGenerator.MilestoningDates[1];
        boolean[] hash = new boolean[1];
        for (int i = 4; i < ps.size(); i++) {
            TestDataGenerationNatives.classifyArg(ps.get(i), rowIds, dates, hash);
        }
        return planText(TestDataGenerator.planFetches(ctx, query, mappingFqn, rowIds, dates[0]), text, connection);
    }

    /** The plan text of the generator's {@code roots}. */
    static String planText(List<TestDataGenerator.FetchNode> roots, EngineTextDialect text, String connection) {
        StringBuilder kids = new StringBuilder();
        for (TestDataGenerator.FetchNode r : roots) {
            node(r, kids, text, connection);
        }
        return "MultiResultSequence\n(\n"
                + "  type = meta::pure::metamodel::type::Any\n"
                + "  (\n"
                + PlanText.indent(kids.toString(), "    ")
                + "  )\n)\n";
    }

    private static void node(TestDataGenerator.FetchNode n, StringBuilder out, EngineTextDialect text,
            String connection) {
        switch (n) {
            case TestDataGenerator.Unidentified u -> out.append(unidentified(u, text, connection));
            case TestDataGenerator.Fetch f -> {
                String relType = relationType(f.db(), f.schema(), f.table(), f.columns());
                // the fetch as the engine prints the query it built (its own aliases), from IR
                String inner = relational(relType, f.columns(), text.renderAsAliased(f.query()), connection);
                out.append(PlanText.allocation(f.res(), "  type = " + relType + "\n  resultSizeRange = *\n", inner));
                for (TestDataGenerator.FetchNode child : f.children()) {
                    node(child, out, text, connection);
                }
            }
        }
    }

    private static String relational(String relType, List<TestDataGenerator.FetchColumn> cols, String sql,
            String connection) {
        return "Relational\n(\n"
                + "  type = " + relType + "\n"
                + "  resultSizeRange = *\n"
                + "  resultColumns = [" + cols.stream()
                        .map(c -> "(\"" + c.name() + "\", " + PlanText.spell(c.type()) + ")")
                        .collect(java.util.stream.Collectors.joining(", ")) + "]\n"
                + "  sql = " + sql + "\n"
                + "  connection = " + connection + "\n"
                + ")\n";
    }

    private static String relationType(String db, String schema, String table,
            List<TestDataGenerator.FetchColumn> cols) {
        return "Relation[name=" + table + ", type=TABLE, schema="
                + schema + ", database=" + db + ", columns=["
                + cols.stream().map(c -> "(\"" + c.name() + "\"," + PlanText.spell(c.type()) + ")")
                        .collect(java.util.stream.Collectors.joining(", "))
                + "]]";
    }

    /** The engine's Error node for a root without row identifiers: the assert message (primary key columns as
     *  name:TYPE) over the sampled {@code select top 5} of the root table. */
    private static String unidentified(TestDataGenerator.Unidentified u, EngineTextDialect text, String connection) {
        String relType = relationType(u.db(), u.schema(), u.table(), u.keys());
        String pk = u.keys().stream()
                .map(c -> c.name() + ":" + PlanText.spell(c.type()))
                .collect(java.util.stream.Collectors.joining(","));
        String qualified = "default".equals(u.schema()) ? u.table() : u.schema() + "." + u.table();
        String inner = relational(relType, u.keys(), text.renderAsAliased(u.sample()), connection);
        return "Error\n(\n"
                + "  type = meta::pure::metamodel::type::Any\n"
                + "  message = Row Identifers should be provided for the root"
                + " table: " + u.db() + "." + qualified + " [" + pk
                + "]\\n\n"
                + "  (\n" + PlanText.indent(inner, "    ")
                + "  )\n)\n";
    }
}
