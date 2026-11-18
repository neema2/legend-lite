// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planoracle;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.HelperRuntimeBuilder;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.HelperValueSpecificationBuilder;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.language.pure.modelManager.ModelManager;
import org.finos.legend.engine.plan.execution.PlanExecutor;
import org.finos.legend.engine.plan.execution.planHelper.PrimitiveValueSpecificationToObjectVisitor;
import org.finos.legend.engine.plan.execution.result.ConstantResult;
import org.finos.legend.engine.plan.execution.result.ErrorResult;
import org.finos.legend.engine.plan.execution.result.Result;
import org.finos.legend.engine.plan.execution.result.StreamingResult;
import org.finos.legend.engine.plan.execution.result.serialization.SerializationFormat;
import org.finos.legend.engine.plan.generation.PlanGenerator;
import org.finos.legend.engine.plan.generation.extension.PlanGeneratorExtension;
import org.finos.legend.engine.plan.generation.transformers.PlanTransformer;
import org.finos.legend.engine.plan.platform.PlanPlatform;
import org.finos.legend.engine.protocol.pure.PureClientVersions;
import org.finos.legend.engine.protocol.pure.v1.PureProtocolObjectMapperFactory;
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.ExecutionPlan;
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.SingleExecutionPlan;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain.ParameterValue;
import org.finos.legend.engine.pure.code.core.PureCoreExtensionLoader;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.finos.legend.engine.shared.core.api.model.ExecuteInput;
import org.finos.legend.engine.shared.core.api.request.RequestContext;
import org.finos.legend.engine.shared.core.deployment.DeploymentMode;
import org.finos.legend.engine.shared.core.identity.Identity;
import org.finos.legend.engine.shared.core.operational.errorManagement.ExceptionError;
import org.finos.legend.pure.generated.Root_meta_core_runtime_Runtime;
import org.finos.legend.pure.generated.Root_meta_pure_extension_Extension;
import org.finos.legend.pure.generated.Root_meta_pure_runtime_ExecutionContext;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.Mapping;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;

/**
 * legend-engine 4.145.0's two pure/v1 routes, {@code generatePlan} and the run of a plan, as its server answers them,
 * composed from the calls its route classes make (slice R of the presentation program, 2026-11-18). TEST TOOLING: the
 * oracle lite's plan export (S8) and import (S9) are measured against; never a product component (AGENTS.md, the
 * reference-checkout tenet: upstream feeds the thing doing the judging, never the thing under test).
 *
 * <p>Read at the pin (legend-engine 230c1591): the server registers {@code new Execute(modelManager, planExecutor,
 * routerExtensions, transformers)} with no plan execution authorizer ({@code Server.java:395-397}), so
 * {@code pure/v1/execution/execute} runs {@code Execute.execLegacy} ({@code Execute.java:309-342}): the plan built
 * by {@code Execute.buildPlan} ({@code :268-279}, the same calls {@code generatePlan} makes, {@code :215-240}), then
 * {@code PlanExecutor.execute}, the result written by {@code ResultManager.manageResult}
 * ({@code ResultManager.java:59-80}) and an exception by {@code ExceptionTool.exceptionManager}
 * ({@code ExceptionTool.java:35,60}). Neither {@code Execute} nor {@code ResultManager} is in the runner's pool, so
 * this composes their calls, each cited where it is made. What it does NOT reproduce, by construction: the HTTP layer
 * (Dropwizard's own mapper modules, Jersey's response buffering), and the server's configured store executors
 * ({@code Server.java:316-325}; here {@code withAvailableStoreExecutors()}, each store's default configuration).
 *
 * <p>The split is the slice's: {@link #plan} answers a request with legend-engine's {@code SingleExecutionPlan} JSON,
 * and {@link #execute} runs a GIVEN plan JSON -- legend-engine's own, or one lite exported -- so a plan always goes
 * through its JSON between the two.
 */
public final class EngineRoutes
{
    /** The server's mapper: Dropwizard's, given the Pure protocol's extensions and then the standard configurations
     *  ({@code Server.java:216-219}): properties sorted, nulls left out ({@code ObjectMapperFactory.java:32-45}). */
    public static final ObjectMapper SERVER_MAPPER = ObjectMapperFactory.withStandardConfigurations(
            PureProtocolObjectMapperFactory.withPureProtocolExtensions(new ObjectMapper()));

    private static final JsonStringEncoder QUOTE = JsonStringEncoder.getInstance();

    /** The server's extensions ({@code Server.java:396}) and transformers ({@code :395, :397}). */
    private static final Function<PureModel, RichIterable<? extends Root_meta_pure_extension_Extension>> EXTENSIONS =
            pureModel -> PureCoreExtensionLoader.extensions()
                    .flatCollect(e -> e.extraPureCoreExtensions(pureModel.getExecutionSupport()));
    private static final Iterable<? extends PlanTransformer> TRANSFORMERS =
            Lists.mutable.withAll(ServiceLoader.load(PlanGeneratorExtension.class))
                    .flatCollect(PlanGeneratorExtension::getExtraPlanTransformers);

    private final ModelManager modelManager = new ModelManager(DeploymentMode.TEST);
    private final PlanExecutor planExecutor = PlanExecutor.newPlanExecutorBuilder().withAvailableStoreExecutors().build();
    /** No profiles, as a server without authentication sees a request ({@code Execute.java:183-184}). */
    private final Identity identity = Identity.makeIdentity(Lists.fixedSize.empty());

    /** {@code pure/v1/execution/generatePlan} ({@code Execute.java:215-240}): 200 and the plan's JSON, or 500 and the
     *  error {@code ExceptionTool} writes. */
    public EngineAnswer plan(String requestJson)
    {
        try
        {
            ExecuteInput input = SERVER_MAPPER.readValue(requestJson, ExecuteInput.class);
            return new EngineAnswer(200, SERVER_MAPPER.writeValueAsBytes(buildPlan(input)));
        }
        catch (Exception ex)
        {
            return exceptionAnswer(ex);
        }
    }

    /** The plan of a request, {@code Execute.buildPlan} ({@code Execute.java:268-279}). */
    public SingleExecutionPlan buildPlan(ExecuteInput input)
    {
        String clientVersion = input.clientVersion == null ? PureClientVersions.production : input.clientVersion;
        PureModel pureModel = this.modelManager.loadModel(input.model, clientVersion, this.identity, null);
        LambdaFunction<?> lambda = HelperValueSpecificationBuilder.buildLambda(input.function.body,
                input.function.parameters, pureModel.getContext());
        Mapping mapping = input.mapping == null ? null : pureModel.getMapping(input.mapping);
        Root_meta_core_runtime_Runtime runtime = HelperRuntimeBuilder.buildPureRuntime(input.runtime,
                pureModel.getContext());
        Root_meta_pure_runtime_ExecutionContext context = HelperValueSpecificationBuilder.processExecutionContext(
                input.context, pureModel.getContext());
        return PlanGenerator.generateExecutionPlan(lambda, mapping, runtime, context, pureModel, clientVersion,
                PlanPlatform.JAVA, null, EXTENSIONS.apply(pureModel), TRANSFORMERS);
    }

    /** A request's parameter values as {@code Execute.LambdaWithParameters} reads them ({@code Execute.java:156}). */
    public static Map<String, Object> parameterValues(List<ParameterValue> values)
    {
        MutableMap<String, Object> map = Maps.mutable.empty();
        if (values != null)
        {
            for (ParameterValue p : values)
            {
                map.put(p.name, p.value.accept(new PrimitiveValueSpecificationToObjectVisitor()));
            }
        }
        return map;
    }

    /** The run of a GIVEN plan JSON, as {@code Execute.execLegacy} runs the plan it built ({@code Execute.java:325-333}):
     *  read by the executor's own reader ({@code PlanExecutor.readExecutionPlan}, {@code PlanExecutor.java:401}),
     *  its parameters made constants, executed, and the result written as {@code ResultManager} writes it. */
    public EngineAnswer execute(String planJson, Map<String, ?> parameters)
    {
        try
        {
            ExecutionPlan read = PlanExecutor.readExecutionPlan(planJson);
            if (!(read instanceof SingleExecutionPlan))
            {
                throw new IllegalArgumentException("the runner executes a SingleExecutionPlan, not a "
                        + read.getClass().getSimpleName());
            }
            SingleExecutionPlan plan = (SingleExecutionPlan) read;
            MutableMap<String, Result> constants = Maps.mutable.empty();
            org.finos.legend.engine.plan.execution.nodes.helpers.ExecuteNodeParameterTransformationHelper
                    .buildParameterToConstantResult(plan, parameters, constants);
            Result result = this.planExecutor.execute(plan, constants, null, this.identity, null, new RequestContext());
            return resultAnswer(result);
        }
        catch (Exception ex)
        {
            return exceptionAnswer(ex);
        }
    }

    /** {@code ResultManager.manageResultWithCustomErrorCodeImpl} with {@code propagateErrorCode} false, the DEFAULT
     *  format ({@code ResultManager.java:59-80}; {@code Execute.java:332}). */
    private static EngineAnswer resultAnswer(Result result) throws Exception
    {
        if (result instanceof ErrorResult)
        {
            String message = ((ErrorResult) result).getMessage();
            ErrorMessage entity = new ErrorMessage(20,
                    "{\"message\":\"" + new String(QUOTE.quoteAsString(message)) + "\"}");
            return new EngineAnswer(500, SERVER_MAPPER.writeValueAsBytes(entity));
        }
        if (result instanceof StreamingResult)
        {
            // StreamingResultHandler.write (ResultManager.java:103-118): the serializer streams, and a failure while
            // streaming is printed into the stream after what was written (the status already sent). The server
            // prints the exception's whole stack trace there; its frames are the server's own call path, which no
            // in-process run reproduces, so this prints the exception's line alone
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try
            {
                ((StreamingResult) result).stream(out, SerializationFormat.DEFAULT);
            }
            catch (Exception e)
            {
                PrintStream print = new PrintStream(out, true, "UTF-8");
                print.println();
                print.println();
                print.println("*************************************ERROR*************************************");
                print.println('*');
                print.println(e);
                print.println('*');
                print.println("*******************************************************************************");
            }
            return new EngineAnswer(200, out.toByteArray());
        }
        if (result instanceof ConstantResult)
        {
            // ManageConstantResult.manageResult: the standard mapper's text of the value
            return EngineAnswer.of(200,
                    ObjectMapperFactory.getNewStandardObjectMapper().writeValueAsString(((ConstantResult) result).getValue()));
        }
        throw new RuntimeException("Unknown Error " + result.getClass().getName());
    }

    /** {@code ExceptionTool.exceptionManager(ex, ...)}: 500 and {@code new ExceptionError(-1, ex)} as the server's
     *  mapper writes it ({@code ExceptionTool.java:35, 60-80}; {@code ExceptionError.java:50}). */
    public static EngineAnswer exceptionAnswer(Exception ex)
    {
        try
        {
            return new EngineAnswer(500, SERVER_MAPPER.writeValueAsBytes(new ExceptionError(-1, ex)));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /** {@code ResultManager.ErrorMessage} ({@code ResultManager.java:82-96}), which is not in the runner's pool. */
    public static final class ErrorMessage
    {
        public int code;
        public String message;

        public ErrorMessage(int code, String message)
        {
            this.code = code;
            this.message = message;
        }
    }
}
