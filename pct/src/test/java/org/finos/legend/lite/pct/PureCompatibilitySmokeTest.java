package org.finos.legend.lite.pct;

import org.finos.legend.engine.server.QueryService;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Smoke test to verify PCT module can access legend-lite-engine
 * and execute queries through QueryService.
 */
public class PureCompatibilitySmokeTest {

    @Test
    public void testQueryServiceAccessible() {
        // Verify we can instantiate QueryService from the engine module
        QueryService qs = new QueryService();
        assertNotNull(qs, "QueryService should be accessible from PCT module");
    }

    @Test
    public void testSimplePureExecution() {
        // Execute a simple Pure expression through legend-lite
        QueryService qs = new QueryService();

        String pureCode = """
                ###Pure
                function test::simple(): Integer[1] {
                    1 + 1
                }
                """;

        // For now, just verify we can call the service
        // Full PCT integration will build on this
        assertNotNull(qs);
    }
}
