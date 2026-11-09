// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

/** The tests' session on the server's database for a runtime of a compiled model: the same session a query on that
 *  runtime executes on (a test that seeds tables before querying shares the database) -- the runtime's target planned,
 *  then opened by the server's source. {@code ConnectionResolver.session} until slice 5 (2026-11-09): the server reads
 *  no planner. The caller closes it. */
final class ServerSessions {

    private ServerSessions() {
    }

    static com.legend.exec.Sessions.Session session(com.legend.compiler.element.ModelContext ctx, String runtimeFqn) {
        return com.legend.exec.PlanSessions.PROJECT_DATABASES.open(com.legend.planner.PlanMaker.runtimeTarget(ctx, runtimeFqn));
    }
}
