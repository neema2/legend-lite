package com.gs.legend.compiler.typed;

/** Single rename entry: {@code from → to}. Used by {@link TypedRename} and {@link TypedJoin}. */
public record ColRename(String from, String to) {}
