import org.eclipse.collections.api.factory.Lists;
import org.finos.legend.engine.plan.execution.nodes.state.ExecutionState;
import org.finos.legend.engine.plan.execution.result.ConstantResult;
import org.finos.legend.engine.plan.execution.result.Result;
import org.finos.legend.engine.plan.execution.validation.FunctionParametersParametersValidation;
import org.finos.legend.engine.protocol.pure.m3.multiplicity.Multiplicity;
import org.finos.legend.engine.protocol.pure.m3.valuespecification.Variable;

import java.util.*;

/**
 * legend-engine's own parameter validation (4.145.0, the released jars: engine-dist-4.145.0/lib), run on the cases the
 * runner's checks were questioned on (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3; the second audit's S-A,
 * S-B, S-C, N1): what the engine says, word for word, or what it passes on. Run:
 * java -cp "<engine-dist-4.145.0>/lib/*" EngineValidationProbe.java
 */
public class EngineValidationProbe {
    static void run(String type, int lower, Integer upper, Object value, boolean present) {
        Variable p = new Variable("p", type, new Multiplicity(lower, upper));
        Map<String, Result> results = new HashMap<>();
        if (present) {
            results.put("p", new ConstantResult(value));
        }
        ExecutionState state = new ExecutionState(results, Collections.emptyList(), Collections.emptyList());
        String shown = type + "[" + lower + ".." + (upper == null ? "*" : upper) + "] "
                + (present ? show(value) : "(absent)");
        try {
            FunctionParametersParametersValidation.validate(Lists.immutable.with(p), Collections.emptyList(), state, null);
            Result r = state.getResult("p");
            Object v = r instanceof ConstantResult c ? c.getValue() : r;
            System.out.println(shown + "\n    passes, as " + show(v));
        } catch (RuntimeException e) {
            System.out.println(shown + "\n    " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    static String show(Object v) {
        if (v == null) {
            return "null";
        }
        if (v instanceof List<?> l) {
            List<String> out = new ArrayList<>();
            for (Object o : l) {
                out.add(show(o));
            }
            return "List" + out;
        }
        return v.getClass().getSimpleName() + " " + (v instanceof String ? "'" + v + "'" : String.valueOf(v));
    }

    public static void main(String[] args) {
        System.out.println("== an unknown type: the types the message names, in its order");
        run("Number", 1, 1, 1L, true);
        System.out.println("== a list for an upper bound of 1, its elements valid and not");
        run("Integer", 1, 1, List.of(1L, 2L), true);
        run("Integer", 1, 1, List.of(1L, "x"), true);
        run("Integer", 1, 1, List.of("x", 1L), true);
        System.out.println("== an empty list, a null value, an absent one");
        run("Integer", 1, 1, List.of(), true);
        run("Integer", 1, 1, null, true);
        run("Integer", 1, 1, null, false);
        run("Integer", 0, 1, List.of(), true);
        run("Integer", 1, null, List.of(), true);
        System.out.println("== a null element in a list");
        run("Integer", 0, null, Arrays.asList(1L, null), true);
        run("Integer", 0, null, Arrays.asList(null, "x"), true);
        run("Float", 0, null, Arrays.asList(1.5, null), true);
        run("String", 0, null, Arrays.asList("a", null), true);
        System.out.println("== a Float that is not finite");
        run("Float", 1, 1, "NaN", true);
        run("Float", 1, 1, Double.NaN, true);
        run("Float", 1, 1, "Infinity", true);
        System.out.println("== a Byte and a Variant");
        run("Byte", 1, 1, 1L, true);
        run("Byte", 1, 1, "x", true);
        run("Byte", 1, 1, new java.io.ByteArrayInputStream(new byte[] {1}), true);
        run("meta::pure::metamodel::variant::Variant", 1, 1, "x", true);
        run("meta::pure::metamodel::variant::Variant", 1, 1, 5L, true);
    }
}
