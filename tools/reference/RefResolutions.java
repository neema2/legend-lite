import org.eclipse.collections.api.RichIterable;
import org.finos.legend.pure.m3.coreinstance.Package;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.PackageableElement;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.ConcreteFunctionDefinition;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.FunctionExpression;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.InstanceValue;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.ValueSpecification;
import org.finos.legend.pure.m3.tools.PackageTreeIterable;
import org.finos.legend.pure.m3.serialization.filesystem.repository.CodeRepositoryProviderHelper;
import org.finos.legend.pure.m3.serialization.filesystem.usercodestorage.classpath.ClassLoaderCodeStorage;
import org.finos.legend.pure.m3.serialization.filesystem.usercodestorage.composite.CompositeCodeStorage;
import org.finos.legend.pure.m3.serialization.runtime.PureRuntime;
import org.finos.legend.pure.m3.serialization.runtime.PureRuntimeBuilder;
import org.finos.legend.pure.m4.coreinstance.CoreInstance;
import org.finos.legend.pure.m4.coreinstance.SourceInformation;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * THE REFERENCE RESOLUTIONS: compile every repository on the classpath with the real Pure
 * compiler (interpreted runtime, the jar's own sources), then for every function-call
 * expression in every function body whose source id starts with a given prefix, print
 * what the reference resolved it to: source id, line, column, the spelling written, the
 * resolved function's signature id. One row per call; nested calls and lambda bodies walked.
 *
 * usage: RefResolutions <out.tsv> <sourceIdPrefix> [<sourceIdPrefix> ...]
 */
public final class RefResolutions {

    public static void main(String[] args) throws Exception {
        Path out = Path.of(args[0]);
        String[] prefixes = java.util.Arrays.copyOfRange(args, 1, args.length);
        long t0 = System.nanoTime();
        PureRuntime runtime = new PureRuntimeBuilder(new CompositeCodeStorage(
                new ClassLoaderCodeStorage(CodeRepositoryProviderHelper.findCodeRepositories()))).build();
        runtime.loadAndCompileCore();
        runtime.loadAndCompileSystem();
        System.err.printf("[refres] loaded and compiled the system in %.1fs%n", (System.nanoTime() - t0) / 1e9);
        long functions = 0;
        long calls = 0;
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8))) {
            w.println("sourceId\tline\tcolumn\tspelling\tresolvedFqn\tresolvedId\tenclosingFqn\tenclosingId");
            for (Package pkg : PackageTreeIterable.newRootPackageTreeIterable(runtime.getProcessorSupport())) {
                for (CoreInstance child : pkg._children()) {
                    if (!(child instanceof ConcreteFunctionDefinition)) {
                        continue;
                    }
                    ConcreteFunctionDefinition<?> fn = (ConcreteFunctionDefinition<?>) child;
                    SourceInformation si = fn.getSourceInformation();
                    if (si == null || !startsWithAny(si.getSourceId(), prefixes)) {
                        continue;
                    }
                    functions++;
                    String enclosing = fqnOf(fn) + "\t" + org.finos.legend.pure.m3.navigation.PackageableElement.PackageableElement.getUserPathForPackageableElement(fn);
                    for (ValueSpecification vs : fn._expressionSequence()) {
                        calls += walk(vs, enclosing, w);
                    }
                }
            }
        }
        System.err.printf("[refres] %d functions, %d calls written to %s%n", functions, calls, out);
    }

    /** package path + the function's DECLARED name (its functionName property) — never a cut of the id. */
    private static String fqnOf(CoreInstance fn) {
        CoreInstance pkg = fn.getValueForMetaPropertyToOne("package");
        CoreInstance name = fn.getValueForMetaPropertyToOne("functionName");
        String path = pkg == null ? "" : org.finos.legend.pure.m3.navigation.PackageableElement.PackageableElement.getUserPathForPackageableElement(pkg);
        return (path.isEmpty() || path.equals("::") ? "" : path + "::") + (name == null ? fn.getName() : name.getName());
    }

    private static boolean startsWithAny(String s, String[] prefixes) {
        for (String p : prefixes) {
            if (s.startsWith(p)) {
                return true;
            }
        }
        return false;
    }

    /** Every function-call expression under {@code vs}, depth first, one row each. */
    private static long walk(CoreInstance vs, String enclosing, PrintWriter w) {
        long n = 0;
        if (vs instanceof FunctionExpression) {
            FunctionExpression fe = (FunctionExpression) vs;
            SourceInformation si = fe.getSourceInformation();
            CoreInstance func = fe._func();
            w.println((si == null ? "" : si.getSourceId()) + "\t"
                    + (si == null ? "" : si.getLine()) + "\t"
                    + (si == null ? "" : si.getColumn()) + "\t"
                    + fe._functionName() + "\t"
                    + (func == null ? "" : fqnOf(func) + "\t" + org.finos.legend.pure.m3.navigation.PackageableElement.PackageableElement.getUserPathForPackageableElement(func)) + "\t"
                    + enclosing);
            n++;
            for (ValueSpecification p : fe._parametersValues()) {
                n += walk(p, enclosing, w);
            }
        } else if (vs instanceof InstanceValue) {
            for (Object v : ((InstanceValue) vs)._values()) {
                if (v instanceof LambdaFunction) {
                    for (ValueSpecification body : ((LambdaFunction<?>) v)._expressionSequence()) {
                        n += walk(body, enclosing, w);
                    }
                } else if (v instanceof ValueSpecification) {
                    n += walk((CoreInstance) v, enclosing, w);
                }
            }
        }
        return n;
    }
}
