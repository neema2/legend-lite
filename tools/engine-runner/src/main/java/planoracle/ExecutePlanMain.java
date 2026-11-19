// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planoracle;

import com.fasterxml.jackson.core.type.TypeReference;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain.ParameterValue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * {@code bazel run //tools/engine-runner:execute_plan -- <plan.json> <out> [<parameterValues.json>]}: a GIVEN
 * {@code SingleExecutionPlan} JSON -- legend-engine's own, or one lite exported -- run by legend-engine 4.145.0's
 * {@code PlanExecutor}, its answer's bytes written to {@code <out>} as the pure/v1 execute route sends them and its
 * status printed (slice R, 2026-11-18; the oracle S8's export is compared against). Test tooling.
 *
 * <p>{@code <parameterValues.json>} is a request's {@code parameterValues}, a JSON array of {@code ParameterValue}s
 * ({"name": .., "value": a value specification}), made constants as the execute route makes them. A plan's
 * connection is its own: a LocalH2 connection seeds a fresh in-memory H2 from the plan's {@code testDataSetupSqls}, a
 * DuckDB connection opens the file its path names (give it a copy: the database is opened to write).
 * Exit status 0 on a 200 answer, 1 otherwise.
 */
public final class ExecutePlanMain
{
    private ExecutePlanMain()
    {
    }

    public static void main(String[] args) throws Exception
    {
        if (args.length < 2 || args.length > 3)
        {
            System.err.println("usage: execute_plan <plan.json> <out> [<parameterValues.json>]");
            System.exit(2);
        }
        String plan = Files.readString(Cwd.resolve(args[0]), StandardCharsets.UTF_8);
        Map<String, Object> parameters = Map.of();
        if (args.length == 3)
        {
            List<ParameterValue> values = EngineRoutes.SERVER_MAPPER.readValue(
                    Files.readString(Cwd.resolve(args[2]), StandardCharsets.UTF_8),
                    new TypeReference<List<ParameterValue>>()
                    {
                    });
            parameters = EngineRoutes.parameterValues(values);
        }
        EngineAnswer answer = new EngineRoutes().execute(plan, parameters);
        Path out = Cwd.resolve(args[1]);
        Files.write(out, answer.body());
        System.out.println(answer.status() + " " + out);
        System.exit(answer.status() == 200 ? 0 : 1);
    }
}
