package planner;

import org.graalvm.webimage.api.JS;
import org.graalvm.webimage.api.JSString;
import org.graalvm.webimage.api.JSValue;

/**
 * SPIKE (2026-10-10): the tab's adapter for GraalVM Web Image, beside TeaVM's. It runs the SAME TabExports methods the
 * TeaVM module exports, so the differentials compare the same Java code compiled by a different compiler. Web Image
 * has no @JS.Export yet; its documented pattern is a functional object installed on globalThis from main().
 */
public final class WebImageExports {

    private WebImageExports() {
    }

    @FunctionalInterface
    public interface Call {
        JSValue call(JSValue name, JSValue a, JSValue b, JSValue c);
    }

    @JS(args = {"call"}, value = "globalThis.legendPlanner = (name, a, b, c) => call(name, a, b, c);")
    private static native void export(Call call);

    private static String text(JSValue v) {
        return v instanceof JSString s ? s.asString() : null;
    }

    static String dispatch(String name, String a, String b, String c) {
        return switch (name) {
            case "pureV1OrError" -> TabExports.pureV1OrError(a, b, c);
            case "plan" -> TabExports.plan(a, b, c);
            case "planOrError" -> TabExports.planOrError(a, b, c);
            case "relationTypeOrError" -> TabExports.relationTypeOrError(a, b);
            case "planJsonOrError" -> TabExports.planJsonOrError(a, b, c);
            case "relationTypeJsonOrError" -> TabExports.relationTypeJsonOrError(a, b);
            case "compileOrError" -> TabExports.compileOrError(a);
            case "zoneProbe" -> TabExports.zoneProbe(a, b);
            case "warmModel" -> String.valueOf(TabExports.warmModel(a));
            case "touchPrelude" -> String.valueOf(TabExports.touchPrelude());
            case "touchSystemMetamodel" -> String.valueOf(TabExports.touchSystemMetamodel());
            case "hashBootSource" -> String.valueOf(TabExports.hashBootSource());
            case "resolveBootLayer" -> String.valueOf(TabExports.resolveBootLayer());
            default -> throw new IllegalArgumentException("no export " + name);
        };
    }

    public static void main(String[] args) {
        export((name, a, b, c) -> JSString.of(dispatch(text(name), text(a), text(b), text(c))));
    }
}
