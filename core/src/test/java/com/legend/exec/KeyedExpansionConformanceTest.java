package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * THE KEYED EXPANSION'S ONE FIXTURE (the total wall's C final audit, H8, 2026-11-12; engine-client/test/fixtures/
 * keyed-expansion.json): the one text a runner writes at run (the charter's A3(a)) -- Java's {@link PlanRunner#expand},
 * the tab's expandKeyed and Python's _keyed_text, each reading these cases: the text, and each key's cells bound in its
 * region's slots' order.
 */
class KeyedExpansionConformanceTest {

    private static final ExecutionPlan.Target TARGET = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(
            com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null), List.of(), List.of(), null, null, null);

    @Test
    void eachCaseIsWrittenAsTheOtherRunnersWriteIt() throws IOException {
        Json.Obj fixture = (Json.Obj) Json.parse(Files.readString(Runfile.property("keyed.fixture"), StandardCharsets.UTF_8));
        List<Json.Node> cases = fixture.getArr("cases").items();
        assertTrue(cases.size() > 3, "the fixture holds too few cases to prove anything: " + cases.size());
        for (Json.Node node : cases) {
            Json.Obj c = (Json.Obj) node;
            String name = c.getString("name");
            ExecutionPlan.Sql body = body(c.getObj("body"));
            List<List<Object>> keys = new ArrayList<>();
            for (Json.Node key : c.getArr("keys").items()) {
                keys.add(new ArrayList<>(strings((Json.Arr) key)));
            }
            if (c.has("refused")) {
                RuntimeException refused = assertThrows(RuntimeException.class, () -> PlanRunner.expand(body, keys, Map.of()), name);
                assertEquals(c.getString("refused"), refused.getMessage(), name);
                continue;
            }
            PlanRunner.Expanded written = PlanRunner.expand(body, keys, Map.of());
            assertEquals(c.getString("statement"), written.statement().text(), name);
            List<Object> bound = new ArrayList<>();
            for (PlanParameters.Checked value : written.values()) {
                bound.add(((PlanParameters.One) value).value());
            }
            assertEquals(new ArrayList<Object>(strings(c.getArr("params"))), bound, name);
        }
    }

    private static ExecutionPlan.Sql body(Json.Obj o) {
        List<ExecutionPlan.Slot> slots = new ArrayList<>();
        for (Json.Node s : o.getArr("slots").items()) {
            Json.Obj slot = (Json.Obj) s;
            slots.add(new ExecutionPlan.Slot(slot.getString("parameter"),
                    new ExecutionPlan.Binding.Key(slot.getObj("binding").getInt("cell"))));
        }
        List<ExecutionPlan.Region> regions = new ArrayList<>();
        for (Json.Node r : o.getArr("regions").items()) {
            Json.Obj region = (Json.Obj) r;
            List<Integer> counters = new ArrayList<>();
            for (Json.Node at : region.getArr("counters").items()) {
                counters.add((int) ((Json.Num) at).longValue());
            }
            Json.Node empty = region.getOr("empty", null);
            regions.add(new ExecutionPlan.Region(region.getInt("start"), region.getInt("end"), region.getString("lead"),
                    region.getString("separator"), empty instanceof Json.Str str ? str.value() : null,
                    region.getInt("firstSlot"), region.getInt("slotCount"), counters));
        }
        return new ExecutionPlan.Sql(o.getString("statement"), slots, TARGET, null, regions);
    }

    private static List<String> strings(Json.Arr a) {
        return a.items().stream().map(n -> ((Json.Str) n).value()).toList();
    }
}
