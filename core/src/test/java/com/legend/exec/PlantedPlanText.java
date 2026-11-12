// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlannedSql;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Classes PLANTED in exec's package to write statement text of their own, for two tests alone (the C final audit's J2,
 * 2026-11-12): {@code ArchitectureTest.theMintRuleBitesAPlantedMint} shows
 * {@code ArchitectureTest.onlyThePlansReadersMintPlannedSql} fails on {@link Mints} and {@link MintsByReference}, and
 * {@code ArchitectureTest.theRunSideRuleBitesAPlantedPlanText} shows
 * {@code ArchitectureTest.theRunSideMakesNoPlanRecordThatCarriesText} fails on {@link Factory}, {@link Constructs},
 * {@link ByReference}, and (the re-audit's note 7, 2026-11-12) {@link Derives}, {@link DerivesByReference},
 * {@link ReadsAPlan} and {@link ReadsATestPlan}, and passes {@link ReadsOnly}. Test classes: the rules' real checks read
 * the product's classes, never these.
 */
public final class PlantedPlanText {

    private PlantedPlanText() {
    }

    /** Mints the brand by a call: a statement exec wrote. */
    public static final class Mints {
        private Mints() {
        }

        public static PlannedSql minted() {
            return PlannedSql.fromPlan("SELECT 'exec wrote this'");
        }
    }

    /** Mints the brand by a method reference. */
    public static final class MintsByReference {
        private MintsByReference() {
        }

        public static Function<String, PlannedSql> minter() {
            return PlannedSql::fromPlan;
        }
    }

    /** Builds a statement by the record's static factory: the brand minted through the record. */
    public static final class Factory {
        private Factory() {
        }

        public static PlannedSql written(ExecutionPlan.Target target) {
            return ExecutionPlan.Sql.of("SELECT 'exec wrote this'", target).planned();
        }
    }

    /** Constructs a target whose session statement exec wrote. */
    public static final class Constructs {
        private Constructs() {
        }

        public static List<PlannedSql> written(ExecutionPlan.Target target) {
            return new ExecutionPlan.Target(target.database(), List.of("SET exec_wrote_this = 1"), List.of(), null,
                    target.settings()).plannedSession();
        }
    }

    /** Derives a statement from the plan's by an instance method (the re-audit's note 7, 2026-11-12). */
    public static final class Derives {
        private Derives() {
        }

        public static PlannedSql derived(ExecutionPlan.Sql sql) {
            return sql.stamped("#stamp.exec").planned();
        }
    }

    /** Derives a target by an instance method named by reference. */
    public static final class DerivesByReference {
        private DerivesByReference() {
        }

        public static Function<ExecutionPlan.Target, ExecutionPlan.Target> bare() {
            return ExecutionPlan.Target::bare;
        }
    }

    /** Reads a plan from its JSON on the run side. */
    public static final class ReadsAPlan {
        private ReadsAPlan() {
        }

        public static ExecutionPlan read(String json) {
            return com.legend.executionplan.PlanJson.read(json);
        }
    }

    /** Reads a test plan from its JSON on the run side. */
    public static final class ReadsATestPlan {
        private ReadsATestPlan() {
        }

        public static com.legend.testexecutionplan.TestPlan read(String json) {
            return com.legend.testexecutionplan.TestPlanJson.read(json);
        }
    }

    /** Reads the plan's records by their accessors alone: no violation. */
    public static final class ReadsOnly {
        private ReadsOnly() {
        }

        public static List<PlannedSql> read(ExecutionPlan.Keyed keyed) {
            ExecutionPlan.Sql keys = keyed.keys();
            ExecutionPlan.Target target = keys.target();
            return List.of(keys.planned(), keyed.body().planned(), target.plannedSession().get(0));
        }
    }

    /** Names a read-back's constructor by reference. */
    public static final class ByReference {
        private ByReference() {
        }

        public static BiFunction<String, String, ExecutionPlan.RaiseReadBack> readBack() {
            return ExecutionPlan.RaiseReadBack::new;
        }
    }
}
