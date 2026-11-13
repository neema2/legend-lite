// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.ExecutionPlan;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSource;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Classes PLANTED in the planner's package, each naming a guarded record's constructor BY REFERENCE ({@code X::new})
 * and never calling it, for one test alone (2026-11-13, the orchestrator's completion B: ArchUnit's
 * {@code callConstructorWhere} reads a constructor's calls, not its references, so a maker writing
 * {@code SqlExpr.DateLit::new} passed every constructor rule): {@code ArchitectureTest.theConstructorRulesBiteAReference}
 * shows each rule fails on one. Test classes: the rules' real checks read the product's classes, never these.
 */
public final class PlantedConstructorReference {

    private PlantedConstructorReference() {
    }

    /** A date literal's maker, of whatever text: the temporal literals' rule. */
    public static final class TemporalLiteral {
        private TemporalLiteral() {
        }

        public static Function<String, SqlExpr> maker() {
            return SqlExpr.DateLit::new;
        }
    }

    /** A load's maker outside the load planner: the loads' rule. */
    public static final class Load {
        private Load() {
        }

        /** The load's constructor, as a function of its five components. */
        public interface Maker {
            ExecutionPlan.Load make(String identity, ExecutionPlan.Load.Cadence cadence,
                    List<ExecutionPlan.LoadStep> steps, ExecutionPlan.Target target, Map<String, String> content);
        }

        public static Maker maker() {
            return ExecutionPlan.Load::new;
        }
    }

    /** A load step's maker outside the step makers: the load steps' rule. */
    public static final class LoadStep {
        private LoadStep() {
        }

        public static Function<String, ExecutionPlan.LoadStep> maker() {
            return ExecutionPlan.LoadStep.Statement::new;
        }
    }

    /** A raw SQL source's maker outside the chartered seam: the RawSql quarantine. */
    public static final class RawSql {
        private RawSql() {
        }

        /** The source's constructor, as a function of its four components. */
        public interface Maker {
            SqlSource make(String sql, String alias, List<OutputCol> outputs, String writtenFor);
        }

        public static Maker maker() {
            return SqlSource.RawSql::new;
        }
    }

    /** A call's maker outside the builder: the calls' rule. */
    public static final class Call {
        private Call() {
        }

        public static BiFunction<String, List<ValueSpecification>, AppliedFunction> maker() {
            return AppliedFunction::new;
        }
    }
}
