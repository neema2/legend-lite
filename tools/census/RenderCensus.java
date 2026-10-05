// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.DuckDb;
import com.legend.sql.dialect.EngineStyleH2;
import com.legend.sql.dialect.H2;
import com.legend.sql.dialect.Postgres;
import com.legend.sql.dialect.SqlDialect;

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
 * {@code lowerResolved} with no runtime -- and rendered by each dialect: DuckDb, H2, EngineStyleH2,
 * Postgres. One line per case and dialect: the SQL (newlines escaped), or the failure's class and
 * first message line. Run at two commits against each one's {@code //core:core_tests_deploy.jar}
 * and diff the outputs: a change to rendering shows as a changed line.
 *
 * <p>usage: bazel run //tools/census:render_census -- out.tsv cases.tsv... (paths from where it is run), or
 * java -cp core_tests_deploy.jar:. RenderCensus out.tsv cases.tsv... (render.sh, at a commit older than the target)
 * (cases: Base64 model TAB Base64 expression, one a line; the PCT lane writes them with
 * -Dlegend.diagnostics=pct-cases).
 */
public final class RenderCensus {

    public static void main(String[] args) throws Exception {
        Set<String> cases = new LinkedHashSet<>();
        for (int i = 1; i < args.length; i++) {
            cases.addAll(Files.readAllLines(arg(args[i])));
        }
        Map<String, Supplier<SqlDialect>> dialects = new LinkedHashMap<>();
        dialects.put("DuckDb", DuckDb::new);
        dialects.put("H2", H2::new);
        dialects.put("EngineStyleH2", EngineStyleH2::new);
        dialects.put("Postgres", Postgres::new);
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
                        return Compiler.compileModel(m);
                    } catch (RuntimeException e) {
                        return e;
                    }
                });
                if (built instanceof RuntimeException e) {
                    throw e;
                }
                ModelContext ctx = (ModelContext) built;
                var parsed = com.legend.parser.SpecParser.parse(expression, com.legend.parser.Dialect.LEGEND_LITE);
                List<com.legend.protocol.spec.ValueSpecification> statements =
                        parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                                ? lf.body() : List.of(parsed);
                var resolved = Compiler.resolveQuery(statements,
                        new com.legend.model.ImportScope(List.of()), ctx);
                q = Compiler.lowerResolved(resolved, ctx, null, false);
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
