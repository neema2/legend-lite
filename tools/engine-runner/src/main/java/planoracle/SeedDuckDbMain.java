// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planoracle;

import com.fasterxml.jackson.databind.JsonNode;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposer;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposerContext;
import org.finos.legend.engine.protocol.pure.m3.PackageableElement;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.section.SectionIndex;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE SHARED DUCKDB (slice R, 2026-11-18): one DuckDB file seeded once, from the seed the pure/v1 goldens' H2 model
 * carries, and the same model with its connection pointed at that file. Test tooling.
 *
 * <pre>
 *   --model=&lt;file&gt;        the goldens' model (its LocalH2 connection's testDataSetupSqls are the seed)
 *   --connection=&lt;file&gt;   the DuckDB connection, in grammar, under the H2 connection's path: it replaces it
 *   --db=&lt;file&gt;           the DuckDB file to seed (must not exist)
 *   --model-out=&lt;file&gt;    the model with the connection replaced, composed by legend-engine's grammar composer
 * </pre>
 *
 * <p>The seed is read by legend-engine's own parser (the connection's {@code testDataSetupSqls}, through the
 * protocol's JSON), so the H2 goldens and the DuckDB file hold the same rows from one source. The file is written by
 * the runner's DuckDB, 1.3.0.0, legend-engine's own ({@code pom.xml:165}) and the oldest of its readers: lite's DuckDB
 * (1.5.x) reads a file of an older storage version, an older DuckDB does not read a newer one. A reader works on a
 * copy of it (DuckDB opens a database file to write, and a build input is read-only).
 */
public final class SeedDuckDbMain
{
    private SeedDuckDbMain()
    {
    }

    public static void main(String[] args) throws Exception
    {
        Map<String, String> a = new LinkedHashMap<>();
        for (String arg : args)
        {
            int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0)
            {
                throw new IllegalArgumentException("unknown argument " + arg);
            }
            a.put(arg.substring(2, eq), arg.substring(eq + 1));
        }
        PureGrammarParser parser = PureGrammarParser.newInstance();
        PureModelContextData model = parser.parseModel(Files.readString(Path.of(a.get("model")), StandardCharsets.UTF_8));
        List<String> seed = seedOf(model);
        if (a.containsKey("statements-out"))
        {
            // the statements as legend-engine's parser reads them (H7: the batteries' per-push half seeds lite's own
            // file from them, so neither half parses the model differently)
            Files.writeString(Path.of(a.get("statements-out")),
                    EngineRoutes.SERVER_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(seed) + "\n",
                    StandardCharsets.UTF_8);
        }
        Path db = Path.of(a.get("db"));
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:" + db.toAbsolutePath());
             Statement s = c.createStatement())
        {
            for (String sql : seed)
            {
                s.execute(sql);
            }
            try (ResultSet rs = s.executeQuery("SELECT version()"))
            {
                rs.next();
                System.out.println("seeded " + db + " with " + seed.size() + " statements, DuckDB " + rs.getString(1));
            }
        }
        PureModelContextData connection = parser.parseModel(
                Files.readString(Path.of(a.get("connection")), StandardCharsets.UTF_8));
        PureModelContextData duck = replace(model, connection);
        String text = PureGrammarComposer.newInstance(PureGrammarComposerContext.Builder.newInstance().build())
                .renderPureModelContextData(duck);
        Files.writeString(Path.of(a.get("model-out")), text, StandardCharsets.UTF_8);
        System.exit(0);
    }

    /** The one LocalH2 connection's testDataSetupSqls, read from the protocol's JSON. */
    static List<String> seedOf(PureModelContextData model)
    {
        JsonNode tree = EngineRoutes.SERVER_MAPPER.valueToTree(model);
        List<JsonNode> specs = new ArrayList<>();
        collect(tree, specs);
        if (specs.size() != 1)
        {
            throw new IllegalStateException("the model holds " + specs.size() + " seeded LocalH2 connections, not one");
        }
        List<String> out = new ArrayList<>();
        specs.get(0).get("testDataSetupSqls").forEach(n -> out.add(n.asText()));
        return out;
    }

    private static void collect(JsonNode node, List<JsonNode> out)
    {
        if (node.isObject() && "h2Local".equals(node.path("_type").asText()) && node.has("testDataSetupSqls"))
        {
            out.add(node);
        }
        node.forEach(child -> collect(child, out));
    }

    /** {@code base} with each element of {@code over} in place of the element of the same path (the section indexes
     *  of {@code over} left out), as {@code TestableMain.override} does. */
    static PureModelContextData replace(PureModelContextData base, PureModelContextData over)
    {
        Map<String, PackageableElement> byPath = new LinkedHashMap<>();
        for (PackageableElement e : base.getElements())
        {
            byPath.put(e.getPath(), e);
        }
        for (PackageableElement e : over.getElements())
        {
            if (e instanceof SectionIndex)
            {
                continue;
            }
            if (!byPath.containsKey(e.getPath()))
            {
                throw new IllegalArgumentException(e.getPath() + " replaces no element of the model");
            }
            byPath.put(e.getPath(), e);
        }
        return PureModelContextData.newBuilder()
                .withOrigin(base.getOrigin())
                .withSerializer(base.getSerializer())
                .withElements(new ArrayList<>(byPath.values()))
                .build();
    }
}
