package com.gs.legend.compiler.typed;

/** Window frame: row or range type plus start/end bounds. */
public record TypedFrame(
        FrameType type,
        TypedFrameBound start,
        TypedFrameBound end
) {}
