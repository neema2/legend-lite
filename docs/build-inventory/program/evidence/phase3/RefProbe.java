import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.finos.legend.pure.m3.compiler.postprocessing.functionmatch.FunctionExpressionMatcher;
import org.finos.legend.pure.m3.coreinstance.Package;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.ConcreteFunctionDefinition;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.Function;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.FunctionExpression;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.InstanceValue;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.ValueSpecification;
import org.finos.legend.pure.m3.navigation.ProcessorSupport;
import org.finos.legend.pure.m3.serialization.filesystem.repository.CodeRepositoryProviderHelper;
import org.finos.legend.pure.m3.serialization.filesystem.usercodestorage.classpath.ClassLoaderCodeStorage;
import org.finos.legend.pure.m3.serialization.filesystem.usercodestorage.composite.CompositeCodeStorage;
import org.finos.legend.pure.m3.serialization.runtime.PureRuntime;
import org.finos.legend.pure.m3.serialization.runtime.PureRuntimeBuilder;
import org.finos.legend.pure.m3.tools.PackageTreeIterable;
import org.finos.legend.pure.m4.coreinstance.CoreInstance;
import org.finos.legend.pure.m4.coreinstance.SourceInformation;

/** Experiment (Phase 3): what legend-pure resolves a call to, the argument types it sees, and its matcher's order. */
public final class RefProbe {

    public static void main(String[] args) throws Exception {
        String fileFilter = args[0];
        String name = args[1];
        PureRuntime runtime = new PureRuntimeBuilder(new CompositeCodeStorage(
                new ClassLoaderCodeStorage(CodeRepositoryProviderHelper.findCodeRepositories()))).build();
        runtime.loadAndCompileCore();
        runtime.loadAndCompileSystem();
        ProcessorSupport ps = runtime.getProcessorSupport();
        MutableList<Function<?>> candidates = Lists.mutable.empty();
        for (CoreInstance f : ps.function_getFunctionsForName(name)) {
            candidates.add((Function<?>) f);
        }
        System.out.println("candidates: " + candidates.collect(f -> path(f)));
        for (Package pkg : PackageTreeIterable.newRootPackageTreeIterable(ps)) {
            for (CoreInstance child : pkg._children()) {
                if (child instanceof ConcreteFunctionDefinition) {
                    SourceInformation si = child.getSourceInformation();
                    if (si != null && si.getSourceId().contains(fileFilter)) {
                        for (ValueSpecification vs : ((ConcreteFunctionDefinition<?>) child)._expressionSequence()) {
                            walk(vs, name, candidates, ps);
                        }
                    }
                }
            }
        }
    }

    private static String path(CoreInstance f) {
        return org.finos.legend.pure.m3.navigation.PackageableElement.PackageableElement.getUserPathForPackageableElement(f);
    }

    private static void walk(CoreInstance vs, String name, MutableList<Function<?>> candidates, ProcessorSupport ps) {
        if (vs instanceof FunctionExpression) {
            FunctionExpression fe = (FunctionExpression) vs;
            if (name.equals(fe._functionName())) {
                SourceInformation si = fe.getSourceInformation();
                StringBuilder sb = new StringBuilder();
                sb.append(si == null ? "?" : si.getLine() + ":" + si.getColumn()).append(" -> ")
                        .append(fe._func() == null ? "null" : path(fe._func())).append("\n   args:");
                ListIterable<? extends ValueSpecification> params = Lists.mutable.withAll(fe._parametersValues());
                for (ValueSpecification p : params) {
                    CoreInstance gt = p.getValueForMetaPropertyToOne("genericType");
                    CoreInstance m = p.getValueForMetaPropertyToOne("multiplicity");
                    sb.append(" [").append(gt == null ? "null" : org.finos.legend.pure.m3.navigation.generictype.GenericType.print(gt, ps))
                            .append(" ").append(m == null ? "null" : org.finos.legend.pure.m3.navigation.multiplicity.Multiplicity.print(m))
                            .append(" rawType=").append(gt == null || gt.getValueForMetaPropertyToOne("rawType") == null ? "null(non-concrete)" : "concrete")
                            .append("]");
                }
                try {
                    Function<?> strict = FunctionExpressionMatcher.getBestFunctionMatch(candidates, params, name, si, false, ps);
                    sb.append("\n   strict best: ").append(strict == null ? "none" : path(strict));
                } catch (RuntimeException e) {
                    sb.append("\n   strict best: ERROR ").append(e.getMessage());
                }
                try {
                    ListIterable<Function<?>> lenient = FunctionExpressionMatcher.getFunctionMatches(candidates, params, name, si, true, ps);
                    sb.append("\n   lenient order: ").append(lenient.collect(RefProbe::path));
                } catch (RuntimeException e) {
                    sb.append("\n   lenient order: ERROR ").append(e.getMessage());
                }
                System.out.println(sb);
            }
            for (ValueSpecification p : fe._parametersValues()) {
                walk(p, name, candidates, ps);
            }
        } else if (vs instanceof InstanceValue) {
            for (Object v : ((InstanceValue) vs)._values()) {
                if (v instanceof LambdaFunction) {
                    for (ValueSpecification body : ((LambdaFunction<?>) v)._expressionSequence()) {
                        walk(body, name, candidates, ps);
                    }
                } else if (v instanceof ValueSpecification) {
                    walk((CoreInstance) v, name, candidates, ps);
                }
            }
        }
    }
}
