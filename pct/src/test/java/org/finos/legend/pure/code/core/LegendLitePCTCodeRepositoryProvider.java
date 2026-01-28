// Copyright 2026 Legend Lite Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.pure.code.core;

import org.finos.legend.pure.m3.pct.reports.model.Adapter;
import org.finos.legend.pure.m3.serialization.filesystem.repository.CodeRepository;
import org.finos.legend.pure.m3.serialization.filesystem.repository.CodeRepositoryProvider;
import org.finos.legend.pure.m3.serialization.filesystem.repository.GenericCodeRepository;

/**
 * Provides the Legend-Lite PCT adapter to the Pure runtime.
 * This registers our adapter function so the PCT framework can discover it.
 */
public class LegendLitePCTCodeRepositoryProvider implements CodeRepositoryProvider {

    /**
     * The Legend-Lite adapter for PCT tests.
     * - Name: "LegendLite" - shown in PCT reports
     * - Category: "Store_Relational" - groups with other relational tests
     * - Function: fully qualified name of the adapter function
     */
    public static final Adapter legendLiteAdapter = new Adapter(
            "LegendLite",
            "Store_Relational",
            "meta::legend::lite::pct::testAdapterForLegendLiteExecution_Function_1__X_o_");

    @Override
    public CodeRepository repository() {
        return GenericCodeRepository.build("core_legend_lite_pct.definition.json");
    }
}
