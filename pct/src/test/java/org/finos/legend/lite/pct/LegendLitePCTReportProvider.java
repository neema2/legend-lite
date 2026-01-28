// Copyright 2026 Legend Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.lite.pct;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.pure.m3.pct.functions.model.Functions;
import org.finos.legend.pure.m3.pct.reports.model.Adapter;
import org.finos.legend.pure.m3.pct.reports.model.AdapterReport;
import org.finos.legend.pure.m3.pct.shared.provider.PCTReportProvider;

/**
 * Registers Legend-Lite as a PCT adapter target.
 * 
 * This enables legend-engine's PCT framework to dispatch test execution
 * to legend-lite's Pure execution engine. Unlike Python/SQL reverse adapters,
 * legend-lite executes Pure directly (like DuckDB) via QueryService.
 */
public class LegendLitePCTReportProvider implements PCTReportProvider {

    /**
     * The Legend-Lite adapter for PCT testing.
     * 
     * Constructor: Adapter(name, group, function)
     * - name: "LegendLite" - identifier for this adapter
     * - group: "interpreted" - execution mode (uses interpreted runtime)
     * - function: Pure function path that handles test execution
     */
    public static final Adapter LegendLiteAdapter = new Adapter(
            "LegendLite",
            "interpreted",
            "meta::legend::lite::pct::testAdapterForLegendLiteExecution_Function_1__X_o_");

    @Override
    public MutableList<Functions> getFunctions() {
        return Lists.mutable.empty();
    }

    @Override
    public MutableList<AdapterReport> getAdapterReports() {
        return Lists.mutable.empty();
    }
}
