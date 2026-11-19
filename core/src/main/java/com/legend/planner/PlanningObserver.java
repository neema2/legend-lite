// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;

/**
 * A TYPED OBSERVER of the planner's text doors (E-1 of the total-wall program, the user's ruling of 2026-11-12: the
 * lean-SQL sets' capture OUT of the product; the planner keeps at most a typed observer hook, no file I/O, no
 * diagnostics switch). The planner tells an installed observer what it made from text -- a model context from a
 * model's text, a typed query from a query's text -- and each query it planned, with the runtime's NAME; it reads
 * nothing back, so an observer changes no plan. None is installed in the product: a TEST HARNESS installs one (core's
 * suites: {@code com.legend.leansql.QueryCaptureListener}), and what it records, and where, is the harness's.
 */
public interface PlanningObserver {

    /** {@code model} was compiled from {@code text}. */
    void modelCompiled(ModelContext model, String text);

    /** {@code query} was typed from {@code text}. */
    void queryTyped(TypedQuery query, String text);

    /** {@code query} was planned for the runtime named {@code runtime}. */
    void planned(TypedQuery query, @com.legend.base.Nullable String runtime);

    /** Installs {@code observer} (a test harness's); null uninstalls. */
    static void install(@com.legend.base.Nullable PlanningObserver observer) {
        Installed.set(observer);
    }

    /** The installed observer, or null. */
    static @com.legend.base.Nullable PlanningObserver installed() {
        return Installed.get();
    }

    /** The observer the planner tells; none (null) in the product. */
    final class Installed {

        private static final java.util.concurrent.atomic.AtomicReference<@com.legend.base.Nullable PlanningObserver>
                OBSERVER = new java.util.concurrent.atomic.AtomicReference<>();

        private Installed() {
        }

        static void set(@com.legend.base.Nullable PlanningObserver observer) {
            OBSERVER.set(observer);
        }

        static @com.legend.base.Nullable PlanningObserver get() {
            return OBSERVER.get();
        }
    }
}
