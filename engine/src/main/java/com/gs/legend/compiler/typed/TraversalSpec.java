package com.gs.legend.compiler.typed;

import java.util.List;

/**
 * One traverse-chain spec: an ordered sequence of {@link TraversalHop}s that
 * together form a single linear association chain (e.g.
 * {@code @Person_Dept > @Dept_Org > @Org_Country}).
 *
 * <p>A {@link TypedExtend} carries a {@link List} of these specs. Within a
 * single spec, hop {@code i+1}'s join condition references hop {@code i}'s
 * alias as its {@code prev} parameter — so multi-hop chains build joins
 * {@code source -> hop0 -> hop1 -> ...}. Across specs, each spec resets
 * to the source alias because two parallel chains
 * (e.g. {@code summary = concat(@OrdCust|.NAME, ' ', @OrdProd|.NAME)})
 * are independent navigations off the same source row.
 *
 * <p>This boundary used to be implicit in the legacy plangen
 * ({@code for (var spec : specs) { prevAlias = sourceAlias; ... }}); the
 * earlier typed IR flattened all hops into a single list, which forced the
 * lowerer to either chain everything (wrong for parallel) or chain
 * nothing (wrong for multi-hop). Restoring the spec boundary on the IR is
 * what closes that gap.
 */
public record TraversalSpec(List<TraversalHop> hops) {
    public TraversalSpec {
        hops = List.copyOf(hops);
    }
}
