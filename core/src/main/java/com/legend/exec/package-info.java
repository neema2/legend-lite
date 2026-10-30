/**
 * THE RUNTIME (step K): a plan and a session to typed rows. The plan runner sends a plan's statements on a session's
 * statements ({@link com.legend.exec.PlanRunner}, the one caller of {@link com.legend.exec.Statements}), the session
 * seam opens, checks and closes connections ({@link com.legend.exec.Sessions}, the one maker of sessions since D70,
 * 2026-10-30), and a row is decoded by the plan's column ({@link com.legend.exec.JdbcDecode}); every value leaves as
 * text at the result boundary ({@code com.legend.result}), never here. The judges (the host lattice, the grid compare,
 * the canonical instruments, the referee, the judges' collected side {@code com.legend.judge.TypedSide}) and the
 * schema probes lived here until D71 (2026-10-30, the seam-and-wall program's B1): they are planning-side work
 * ({@code com.legend.judge}, the root); since D72 (the same day, B2) exec reaches no planning library at all -- the
 * judges' column and row are judge's, the driver's decode facts the connector's ({@link DatabaseConnector#decoding}),
 * the session source and the system database the driver's, the wire table's type a NAME in the plan's vocabulary
 * ({@link WireTypes#pureTypeNameOf}) -- and {@code ArchitectureTest.execReachesNoPlanning} is the wall, the build's
 * dependency list (tools/deps/core-layers.txt) its twin.
 */
package com.legend.exec;
