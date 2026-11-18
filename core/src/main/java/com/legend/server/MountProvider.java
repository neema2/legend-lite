// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

/**
 * A MODULE OF ROUTES THE SERVER MOUNTS WHEN CONFIGURED (K2's server half, the user's ruling of 2026-11-18: "Product
 * root, edge opt-in"). The shipped server starts at lite's own entry ({@link LegendHttpServer#main}) and mounts nothing
 * beside lite's routes; a provider is looked up only when the configuration names an engine ({@code --engine NAME}),
 * through {@link java.util.ServiceLoader}, and the one whose {@link #engine} is that
 * name is mounted. An engine no provider answers is refused by name, never served without it. A hook: signatures
 * only, implemented at the edge alone ({@code com.legend.edge}'s legend-engine module), the product naming none
 * (HookRulingTest).
 */
public interface MountProvider {

    /** The engine name the configuration selects it by ({@code legend-engine}). */
    String engine();

    /** The routes it mounts beside lite's own. */
    LegendHttpServer.Mount mount();
}
