package planner;

/**
 * The text encoding both strings-only hosts use for the boundary's answers -- the tab through TeaVM
 * ({@link TabExports}) and Python through the C library (native/'s {@code nativelib.Compiler}) -- written once
 * (docs/PROTOCOL_PROGRAM_2026_10_05.md, invariant 5): {@code "OK\n<answer>"}, a refusal as
 * {@code "ERR\n<class>\n<message>"}, and a {@code pure/v1} answer as {@code "OK\n<status>\n<media type>\n<body>"}.
 *
 * <p>A refusal is an answer the planner is expected to give, so it travels in the return value: the differential
 * compares refusals too, without depending on how a host bridges a Java throwable.
 */
public final class Folded {

    private Folded() {
    }

    /** {@code answer}, or the refusal it threw. */
    public static String of(java.util.function.Supplier<String> answer) {
        try {
            return "OK\n" + answer.get();
        } catch (RuntimeException | StackOverflowError e) {
            return failure(e);
        }
    }

    /** A {@code pure/v1} answer -- its status, media type and body -- or the refusal it threw. */
    public static String http(java.util.function.Supplier<com.legend.server.PureV1Api.Answer> answer) {
        try {
            com.legend.server.PureV1Api.Answer a = answer.get();
            return "OK\n" + a.status() + "\n" + a.contentType() + "\n" + a.json();
        } catch (RuntimeException | StackOverflowError e) {
            return failure(e);
        }
    }

    /** A failure as a refusal: its class and its message. */
    public static String failure(Throwable e) {
        return "ERR\n" + e.getClass().getName() + "\n" + (e.getMessage() == null ? "" : e.getMessage());
    }
}
