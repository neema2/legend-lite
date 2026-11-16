// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.stack.MutableStack;
import org.finos.legend.pure.m3.compiler.Context;
import org.finos.legend.pure.m3.exception.PureExecutionException;
import org.finos.legend.pure.m3.navigation.Instance;
import org.finos.legend.pure.m3.navigation.M3Properties;
import org.finos.legend.pure.m3.navigation.PrimitiveUtilities;
import org.finos.legend.pure.m3.navigation.ProcessorSupport;
import org.finos.legend.pure.m4.ModelRepository;
import org.finos.legend.pure.m4.coreinstance.CoreInstance;
import org.finos.legend.pure.runtime.java.interpreted.ExecutionSupport;
import org.finos.legend.pure.runtime.java.interpreted.FunctionExecutionInterpreted;
import org.finos.legend.pure.runtime.java.interpreted.VariableContext;
import org.finos.legend.pure.runtime.java.interpreted.natives.InstantiationContext;
import org.finos.legend.pure.runtime.java.interpreted.profiler.Profiler;

import java.util.Stack;

/**
 * THE EDGE GOLDEN NET's error tap (2026-11-16): {@link PctExecuteNative} wrapped from the harness, so the net records
 * an error's message EXACTLY as the adapter hands it to legend-pure -- the very exception's text, never recomputed --
 * and PctExecuteNative itself carries no recording call (its JavaEvalLedger pin stays). The extension registers this in
 * its place; it adds nothing to a run but the record.
 */
public class EdgeRecordingNative extends PctExecuteNative {

    public EdgeRecordingNative(FunctionExecutionInterpreted functionExecution, ModelRepository modelRepository) {
        super(functionExecution, modelRepository);
    }

    @Override
    public CoreInstance execute(
            ListIterable<? extends CoreInstance> params,
            Stack<MutableMap<String, CoreInstance>> resolvedTypeParameters,
            Stack<MutableMap<String, CoreInstance>> resolvedMultiplicityParameters,
            VariableContext variableContext,
            MutableStack<CoreInstance> functionExpressionCallStack,
            Profiler profiler,
            InstantiationContext instantiationContext,
            ExecutionSupport executionSupport,
            Context context,
            ProcessorSupport processorSupport) throws PureExecutionException {
        try {
            return super.execute(params, resolvedTypeParameters, resolvedMultiplicityParameters, variableContext,
                    functionExpressionCallStack, profiler, instantiationContext, executionSupport, context,
                    processorSupport);
        } catch (PureExecutionException handed) {
            PctCaseRecorder.edgeError(PrimitiveUtilities.getStringValue(
                    Instance.getValueForMetaPropertyToOneResolved(params.get(0), M3Properties.values, processorSupport)),
                    handed.getInfo());
            throw handed;
        }
    }
}
