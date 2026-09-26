// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import java.util.List;

/**
 * The plan node model's CONNECTION handle (cluster 60): the engine's
 * generated plan carries the runtime connection on every
 * {@code SQLExecutionNode}, with {@code processRuntimeTestConnections}'
 * DDL expansion populated onto {@code testDataSetupSqls}. Kinds are the
 * engine's class SIMPLE names (cast/instanceOf match on them, same as
 * {@link PlanNode#kind()}).
 *
 * @param kind e.g. {@code TestDatabaseConnection},
 *             {@code RelationalDatabaseConnection}
 * @param type the DatabaseType name ({@code H2})
 */
public record PlanConn(String kind, String type,
        @com.legend.base.Nullable String testDataSetupCsv,
        List<String> testDataSetupSqls,
        @com.legend.base.Nullable DsSpec datasourceSpecification) {

    public PlanConn {
        testDataSetupSqls = testDataSetupSqls == null ? List.of()
                : List.copyOf(testDataSetupSqls);
    }

    /** The datasource-specification handle
     * ({@code LocalH2DatasourceSpecification}). */
    public record DsSpec(String kind,
            @com.legend.base.Nullable String testDataSetupCsv,
            List<String> testDataSetupSqls) {
        public DsSpec {
            testDataSetupSqls = testDataSetupSqls == null ? List.of()
                    : List.copyOf(testDataSetupSqls);
        }
    }
}
