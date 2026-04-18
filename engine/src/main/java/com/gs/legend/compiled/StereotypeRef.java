package com.gs.legend.compiled;

/**
 * Compiled-state stereotype reference: the FQN of the profile that declares
 * the stereotype plus the stereotype's simple name.
 *
 * <p>Mirrors Pure's {@code Stereotype} as a pure-data fingerprint — no live
 * profile ref, no cycles — so it can be serialized and reloaded verbatim.
 */
public record StereotypeRef(String profileFqn, String name) {
}
