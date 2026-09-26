import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.ConcreteFunctionDefinition;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.FunctionExpression;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.ValueSpecification;
import org.finos.legend.pure.m3.coreinstance.Package;
import org.finos.legend.pure.m3.navigation.imports.Imports;
import org.finos.legend.pure.m3.serialization.filesystem.repository.CodeRepositoryProviderHelper;
import org.finos.legend.pure.m3.serialization.filesystem.usercodestorage.classpath.ClassLoaderCodeStorage;
import org.finos.legend.pure.m3.serialization.filesystem.usercodestorage.composite.CompositeCodeStorage;
import org.finos.legend.pure.m3.serialization.runtime.PureRuntime;
import org.finos.legend.pure.m3.serialization.runtime.PureRuntimeBuilder;
import org.finos.legend.pure.m3.tools.PackageTreeIterable;
import org.finos.legend.pure.m4.coreinstance.CoreInstance;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/** For every function body's FIRST call expression, the packages its import group makes visible:
 *  what a bare name can mean in that source section, as the reference sees it — implicit imports included. */
public final class RefImports {
    public static void main(String[] args) throws Exception {
        PureRuntime runtime = new PureRuntimeBuilder(new CompositeCodeStorage(
                new ClassLoaderCodeStorage(CodeRepositoryProviderHelper.findCodeRepositories()))).build();
        runtime.loadAndCompileCore();
        runtime.loadAndCompileSystem();
        Map<String, Set<String>> bySource = new TreeMap<>();
        for (Package pkg : PackageTreeIterable.newRootPackageTreeIterable(runtime.getProcessorSupport())) {
            for (CoreInstance child : pkg._children()) {
                if (!(child instanceof ConcreteFunctionDefinition)) continue;
                ConcreteFunctionDefinition<?> fn = (ConcreteFunctionDefinition<?>) child;
                if (fn.getSourceInformation() == null) continue;
                FunctionExpression fe = firstCall(fn._expressionSequence());
                if (fe == null || fe._importGroup() == null) continue;
                String src = fn.getSourceInformation().getSourceId();
                Set<String> pkgs = bySource.computeIfAbsent(src, k -> new TreeSet<>());
                for (CoreInstance p : Imports.getImportGroupPackages(fe._importGroup(), runtime.getProcessorSupport())) {
                    pkgs.add(org.finos.legend.pure.m3.navigation.PackageableElement.PackageableElement.getUserPathForPackageableElement(p));
                }
            }
        }
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(Path.of(args[0]), StandardCharsets.UTF_8))) {
            for (var e : bySource.entrySet()) w.println(e.getKey() + "\t" + String.join(",", e.getValue()));
        }
        System.err.println("[refimports] sources " + bySource.size());
    }
    private static FunctionExpression firstCall(Iterable<? extends ValueSpecification> body) {
        for (ValueSpecification vs : body) if (vs instanceof FunctionExpression) return (FunctionExpression) vs;
        return null;
    }
}
