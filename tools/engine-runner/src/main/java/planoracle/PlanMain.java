// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planoracle;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@code bazel run //tools/engine-runner:plan -- <request.json> <out.json>}: legend-engine's answer to
 * {@code pure/v1/execution/generatePlan} for a pure/v1 request body (an {@code ExecuteInput}), written to
 * {@code <out.json>} -- the plan's JSON, or the error -- and its status printed (slice R, 2026-11-18; the input S9's
 * import census reads). Test tooling. Exit status 0 when legend-engine planned it, 1 when it answered an error.
 */
public final class PlanMain
{
    private PlanMain()
    {
    }

    public static void main(String[] args) throws Exception
    {
        if (args.length != 2)
        {
            System.err.println("usage: plan <request.json> <out.json>");
            System.exit(2);
        }
        String request = Files.readString(Cwd.resolve(args[0]), StandardCharsets.UTF_8);
        EngineAnswer answer = new EngineRoutes().plan(request);
        Path out = Cwd.resolve(args[1]);
        Files.write(out, answer.body());
        System.out.println(answer.status() + " " + out);
        System.exit(answer.status() == 200 ? 0 : 1);
    }
}
