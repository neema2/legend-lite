// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.lite.pct.extension.RouteAudience;
import org.finos.legend.pure.code.core.CoreStandardFunctionsCodeRepositoryProvider;
import org.finos.legend.pure.code.core.CoreUnclassifiedFunctionsCodeRepositoryProvider;
import org.finos.legend.pure.code.core.RelationCodeRepositoryProvider;
import org.finos.legend.pure.m3.PlatformCodeRepositoryProvider;
import org.finos.legend.pure.m3.pct.reports.config.PCTReportConfiguration;
import org.finos.legend.pure.m3.pct.reports.config.exclusion.ExclusionSpecification;
import org.finos.legend.pure.m3.pct.reports.model.Adapter;
import org.finos.legend.pure.m3.pct.shared.model.ReportScope;
import org.finos.legend.pure.runtime.java.interpreted.testHelper.PureTestBuilderInterpreted;

import static org.finos.legend.engine.test.shared.framework.PureTestHelperFramework.wrapSuite;

/**
 * THE ENGINE LANE (H7, slice 0.3 of docs/records/compat-program-plan-2026-11-19.md; report-only): one PCT suite, named
 * by {@code -Dpct.engine.suite} (Essential, Grammar, Relation, Standard or Unclassified), run on channel A as
 * legend-engine's audience ({@code -Dpct.audience=engine}: {@code PctPlanRoute} plans under the pure/v1 route's style,
 * with no feature flag, as legend-engine's own PCT adapters run a case) with NO expected failures, so each test's
 * outcome is the run's own. The lane records each outcome by the test's full name ({@code JUnitMain}'s
 * {@code -Dlegend.junit.outcomes.full}); the classification against legend-engine's manifests is made outside this
 * module ({@code tools/enginelane}), which holds no comparison machinery ({@link PctDisciplineTest}).
 */
public class Test_LegendLite_Engine_PCT extends PCTReportConfiguration {

    private static final Adapter adapter = LegendLitePCTReportProvider.LegendLiteAdapter;
    private static final String platform = "interpreted";

    /** No expected failures: the lane reports, it does not judge. */
    private static final MutableList<ExclusionSpecification> expectedFailures = Lists.mutable.empty();

    /** The suite {@code -Dpct.engine.suite} names. */
    private static ReportScope scope() {
        String suite = System.getProperty("pct.engine.suite");
        if (suite == null) {
            throw new IllegalStateException("the ENGINE lane runs one suite: -Dpct.engine.suite=<Essential, Grammar,"
                    + " Relation, Standard or Unclassified>");
        }
        return switch (suite) {
            case "Essential" -> PlatformCodeRepositoryProvider.essentialFunctions;
            case "Grammar" -> PlatformCodeRepositoryProvider.grammarFunctions;
            case "Relation" -> RelationCodeRepositoryProvider.relationFunctions;
            case "Standard" -> CoreStandardFunctionsCodeRepositoryProvider.standardFunctions;
            case "Unclassified" -> CoreUnclassifiedFunctionsCodeRepositoryProvider.unclassifiedFunctions;
            default -> throw new IllegalStateException("no PCT suite '" + suite + "': Essential, Grammar, Relation,"
                    + " Standard or Unclassified");
        };
    }

    public static Test suite() {
        if (!"engine".equals(System.getProperty("pct.audience"))) {
            throw new IllegalStateException("the ENGINE lane runs as legend-engine's audience: -Dpct.audience=engine");
        }
        ReportScope reportScope = scope();
        TestSuite lane = new TestSuite(LANE);
        lane.addTest(wrapSuite(
                () -> true,
                () -> PureTestBuilderInterpreted.buildPCTTestSuite(reportScope, expectedFailures, adapter),
                () -> false,
                Lists.mutable.empty()));
        // after every case
        lane.addTest(new AudienceCheck());
        return lane;
    }

    /** The suite's name: with {@link AudienceCheck#NAME}, the check's full name in the lane's outcomes. */
    static final String LANE = "engine_lane";

    /**
     * THE LANE'S PLANT (H7's independent review, 2026-11-19): after its last case, the lane asserts that channel A
     * planned every case as legend-engine's audience, as the route itself chose it ({@link RouteAudience}), and at least
     * one. The property this class reads is not enough: a route that stopped reading it would plan as the PCT's own
     * audience, and every case lite passes there would pass here too. Its outcome is a line of the lane's outcomes like
     * any case's; the table ({@code com.legend.tools.enginelane.EngineLaneTable}) refuses a lane whose check did not
     * pass, and keeps it out of the rows.
     */
    public static final class AudienceCheck extends TestCase {

        /** The check's name, which the table looks for. */
        public static final String NAME = "everyCaseWasPlannedAsLegendEnginesAudience";

        public AudienceCheck() {
            super(NAME);
        }

        @Override
        protected void runTest() {
            long asEngine = RouteAudience.plannedAsLegendEngine();
            long otherwise = RouteAudience.plannedOtherwise();
            assertTrue("channel A planned " + asEngine + " cases as legend-engine's audience and " + otherwise
                    + " otherwise", asEngine > 0 && otherwise == 0);
        }
    }

    @Override
    public MutableList<ExclusionSpecification> expectedFailures() {
        return expectedFailures;
    }

    @Override
    public ReportScope getReportScope() {
        return scope();
    }

    @Override
    public Adapter getAdapter() {
        return adapter;
    }

    @Override
    public String getPlatform() {
        return platform;
    }
}
