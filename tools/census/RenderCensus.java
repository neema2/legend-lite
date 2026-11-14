// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.SqlQuery;
import com.legend.edge.EngineStyleH2;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testbackend.StorelessRuntime;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * THE RENDER CENSUS (docs/STORE_TYPES_HOMEWORK_2026_10_02.md, step 5): every recorded case
 * (model, expression) lowered once, the way {@code Compiler.execute} does -- parse, resolve,
 * {@code lowerResolved} with the storeless runtime on DuckDB declared beside the model ({@code StorelessRuntime.with};
 * since the dialect program's decision D2 the planner refuses a query with no runtime, the language of any raw text
 * being the runtime's database, and until 2026-10-15 this tool passed none, so every case failed to lower on both
 * sides of a comparison) -- and rendered by each dialect: DuckDb, H2, EngineStyleH2,
 * Postgres. One line per case and dialect: the SQL (newlines escaped), or the failure's class and
 * first message line. Run at two commits against each one's core test deploy jar ({@code //core:core_tests_root_deploy.jar})
 * and diff the outputs: a change to rendering shows as a changed line.
 *
 * <p>usage: bazel run //tools/census:render_census -- out.tsv cases.tsv... (paths from where it is run), or
 * java -cp core_tests_root_deploy.jar:. RenderCensus out.tsv cases.tsv... (render.sh, at a commit older than the target)
 * (cases: Base64 model TAB Base64 expression, one a line; the PCT lane writes them with
 * -Dlegend.diagnostics=pct-cases).
 */
public final class RenderCensus {

    public static void main(String[] args) throws Exception {
        Set<String> cases = new LinkedHashSet<>();
        for (int i = 1; i < args.length; i++) {
            cases.addAll(Files.readAllLines(arg(args[i])));
        }
        // every database the platform executes on, by its declared type (the dialect program's Phase 6, 2026-10-16:
        // until then nine dialects by hand, three missing), and legend-engine's text, a render target of its own
        Map<String, Supplier<SqlDialect>> dialects = new LinkedHashMap<>();
        for (DatabaseType type : Databases.implemented()) {
            dialects.put(type.name(), () -> Databases.dialect(type));
        }
        dialects.put("EngineStyleH2", EngineStyleH2::new);
        Map<String, Object> models = new HashMap<>();
        StringBuilder out = new StringBuilder();
        int n = 0;
        for (String line : cases) {
            String[] f = line.split("\t", -1);
            String model = decode(f[0]);
            String expression = decode(f[1]);
            String id = String.format(java.util.Locale.ROOT, "%05d", n++);
            SqlQuery q;
            try {
                Object built = models.computeIfAbsent(model, m -> {
                    try {
                        // a model that declares the storeless runtime itself keeps its own
                        return Compiler.compileModel(m.contains("Runtime " + StorelessRuntime.RUNTIME) ? m
                                : StorelessRuntime.with(m, Databases.PLATFORM));
                    } catch (RuntimeException e) {
                        return e;
                    }
                });
                if (built instanceof RuntimeException e) {
                    throw e;
                }
                ModelContext ctx = (ModelContext) built;
                var parsed = com.legend.parser.SpecParser.parse(expression, com.legend.parser.GrammarLevel.LEGEND_LITE);
                List<com.legend.protocol.spec.ValueSpecification> statements =
                        parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                                ? lf.body() : List.of(parsed);
                var resolved = Compiler.resolveQuery(statements,
                        new com.legend.model.ImportScope(List.of()), ctx);
                q = Compiler.lowerResolved(resolved, ctx, StorelessRuntime.RUNTIME, false);
            } catch (RuntimeException | StackOverflowError e) {
                out.append(id).append("\tLOWER\t").append(failure(e)).append('\n');
                continue;
            }
            for (var d : dialects.entrySet()) {
                String sql;
                try {
                    sql = d.getValue().get().render(q).replace("\\", "\\\\").replace("\n", "\\n");
                } catch (RuntimeException | StackOverflowError e) {
                    sql = failure(e);
                }
                out.append(id).append('\t').append(d.getKey()).append('\t').append(sql).append('\n');
            }
        }
        Files.writeString(arg(args[0]), out.toString());
        System.out.println(n + " cases rendered to " + args[0]);
    }

    private static String decode(String b64) {
        return new String(Base64.getDecoder().decode(b64), StandardCharsets.UTF_8);
    }

    private static String failure(Throwable e) {
        String m = String.valueOf(e.getMessage());
        int nl = m.indexOf('\n');
        return "FAIL " + e.getClass().getSimpleName() + ": " + (nl < 0 ? m : m.substring(0, nl));
    }

    /** A path argument, from the directory {@code bazel run} was started in (it runs the program in its runfiles). */
    private static Path arg(String path) {
        String cwd = System.getenv("BUILD_WORKING_DIRECTORY");
        return cwd == null ? Path.of(path) : Path.of(cwd).resolve(path);
    }
}
