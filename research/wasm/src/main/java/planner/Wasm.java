package planner;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.ClassLayouts;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.UserCallInliner;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
import com.legend.parser.Dialect;
import com.legend.parser.ElementParser;
import com.legend.parser.SpecParser;
import com.legend.resolver.StoreResolver;
import com.legend.sql.dialect.DuckDb;

import java.util.List;

/**
 * legend-lite's PLANNER, compiled to WebAssembly.
 *
 * <p>Pure text in, SQL text out. No executor, no server, no JDBC — the
 * planner packages depend on java.base alone (jdeps), and the whole
 * pipeline was already proved to run under {@code --limit-modules
 * java.base}. This asks the next question: does it survive an AOT
 * compile to WASM, and does it then answer IDENTICALLY.
 *
 * <p>This one class is the entry point for BOTH targets — TeaVM
 * compiles it to WASM, and {@code JvmMain} calls it on the JVM — so the
 * differential test compares two builds of the same source rather than
 * two hand-kept copies that could drift apart silently.
 */
public final class Wasm {

    private Wasm() {
    }

    private static final String MODEL = """
            ###Relational
            Database demo::DB
            (
              Table TRADES
              (
                region VARCHAR(64), desk VARCHAR(64),
                notional DOUBLE, qty INTEGER
              )
            )
            """;

    private static final String QUERY = "#>{demo::DB.TRADES}#"
            + "->filter(x|$x.region == 'EMEA')"
            + "->select(~[region, desk, notional])"
            + "->groupBy(~[region, desk], ~[total:x|$x.notional:y|$y->sum()])"
            + "->sort([~region->ascending()])->limit(10)";

    /** The planner, end to end: Pure text in, dialect SQL out. */
    @org.teavm.jso.JSExport
    public static String plan(String model, String query) {
        var parsed = ElementParser.parse(model, Dialect.LEGEND_LITE);
        ModelContext ctx = com.legend.Compiler.buildModel(parsed);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(NameResolver.resolveQuery(
                SpecParser.parse(query, Dialect.LEGEND_LITE)));
        body = new UserCallInliner(specs).inlineBody(body);
        body = new StoreResolver(ctx, specs).resolve(body, null);
        Lowerer lw = new Lowerer(
                t -> ClassLayouts.layoutOf(ctx, t),
                f -> ctx.findClass(f).isPresent())
                .withEngineExistsJoinForm();
        return new DuckDb().render(lw.lower(body));
    }

    /**
     * {@link #plan} with the failure path folded into the RETURN VALUE.
     *
     * <p>A refusal is an answer the planner is expected to give, so the
     * differential has to compare refusals too — and comparing them
     * through the return value rather than through a thrown exception
     * keeps the two targets honest without depending on how TeaVM
     * bridges Java throwables into JS.
     */
    @org.teavm.jso.JSExport
    public static String planOrError(String model, String query) {
        try {
            return "OK\n" + plan(model, query);
        } catch (RuntimeException | StackOverflowError e) {
            String name = e.getClass().getName();
            return "ERR\n" + name + "\n" + (e.getMessage() == null ? "" : e.getMessage());
        }
    }

    /**
     * The one {@code ZoneId.of} on the planner's path
     * ({@code LiteralSpelling.inZone}, the engine's dbTimeZone literal
     * rule). A timezone database is a RESOURCE, not code, so whether it
     * survives the WASM build is a question the census cannot answer by
     * reading — only by asking the built module.
     */
    @org.teavm.jso.JSExport
    public static String zoneProbe(String utcIso, String zone) {
        try {
            return com.legend.lowering.LiteralSpelling.inZone(utcIso, zone);
        } catch (RuntimeException e) {
            return "ERR " + e.getClass().getName() + ": " + e.getMessage();
        }
    }

    public static void main(String[] args) {
        System.out.println(plan(MODEL, QUERY));
    }
}
