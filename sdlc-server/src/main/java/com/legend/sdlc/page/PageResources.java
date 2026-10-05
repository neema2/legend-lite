package com.legend.sdlc.page;

import org.teavm.classlib.ResourceSupplier;
import org.teavm.classlib.ResourceSupplierContext;

/**
 * Tells the TeaVM compiler to EMBED the compiler's data files in the page's SDLC module, as the
 * planner's module does ({@code wasm/.../PreludeResources}): the SDLC's gates compile models, and the
 * compiler reads its prelude with {@code getResourceAsStream}, which a WebAssembly module can only
 * answer from what was baked in. Packaging, not product.
 */
public final class PageResources implements ResourceSupplier {
    @Override
    public String[] supplyResources(ResourceSupplierContext context) {
        return new String[] {"com/legend/builtin/prelude.pure", "com/legend/builtin/engine-handlers.tsv"};
    }
}
