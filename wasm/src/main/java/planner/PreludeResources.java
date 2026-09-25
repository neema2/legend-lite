package planner;

import org.teavm.classlib.ResourceSupplier;
import org.teavm.classlib.ResourceSupplierContext;

/**
 * Tells the TeaVM compiler to EMBED prelude.pure in the module.
 *
 * <p>{@code Prelude} reads its source with {@code getResourceAsStream},
 * which on a JVM finds the file on the classpath. A WASM module has no
 * classpath, so the resource has to be baked in at build time — and
 * this SPI, read by the TeaVM compiler, is where that is declared.
 *
 * <p>It lives in the WASM module rather than in the product because it
 * is a PACKAGING concern: nothing about the planner changes, only how
 * its one data file is delivered.
 */
public final class PreludeResources implements ResourceSupplier {

    @Override
    public String[] supplyResources(ResourceSupplierContext context) {
        return new String[] {"com/legend/builtin/prelude.pure", "com/legend/builtin/engine-handlers.tsv"};
    }
}
