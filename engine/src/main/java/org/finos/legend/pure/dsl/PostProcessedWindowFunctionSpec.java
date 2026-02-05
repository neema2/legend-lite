package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * A window function spec wrapped with a post-processor scalar function.
 * E.g., cumulativeDistribution($w,$r)->round(2) where round is the
 * post-processor.
 */
public record PostProcessedWindowFunctionSpec(
        WindowFunctionSpec inner,
        String postProcessorFunction,
        List<Object> postProcessorArgs) implements WindowFunctionSpec {

    @Override
    public List<String> partitionBy() {
        return inner.partitionBy();
    }

    @Override
    public List<WindowSortSpec> orderBy() {
        return inner.orderBy();
    }

    @Override
    public WindowFrameSpec frame() {
        return inner.frame();
    }
}
