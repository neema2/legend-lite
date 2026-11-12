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
 * {@code ArchitectureTest.theRunSideMakesNoPlanRecordThatCarriesText} fails on {@link Factory}, {@link Constructs} and
 * {@link ByReference}. Test classes: the rules' real checks read the product's classes, never these.
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
                    target.settings(), null).plannedSession();
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
