package planner;

/**
 * The text encoding both strings-only hosts use for the boundary's answers -- the tab through TeaVM
 * ({@code TabExports}) and Python through the C library (native/'s {@code nativelib.Compiler}) -- written once
 * (docs/PROTOCOL_PROGRAM_2026_10_05.md, invariant 5): {@code "OK\n<answer>"}, a refusal as {@code "ERR\n<refusal>"},
 * and a {@code pure/v1} answer as {@code "OK\n<status>\n<media type>\n<body>"}.
 *
 * <p>A refusal is lite API v1's one error shape (docs/records/lite-api-v1-2026-11-17.md §3, the user's go of
 * 2026-11-17): its JSON {@code {kind, message, where?, raise?}}, classified by the one classifier the server's routes
 * use ({@code LiteV1Api.refusal}), so every binding -- the tab's exports, Python's native entries, the server's routes --
 * says the same refusal. Until 2026-11-17 it was {@code "ERR\n<kind>\n<message>"}.
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

    /** The media type's suffix of a binary body folded as base64: {@code application/json;base64}. */
    public static final String BASE64 = ";base64";

    /** A {@code pure/v1} answer -- its status, media type and body -- or the refusal it threw. A BINARY body (the
     *  ARROW_IPC execute's bytes, track A, 2026-10-29) travels as base64 under its media type with the
     *  {@link #BASE64} suffix: both hosts speak strings. */
    public static String http(java.util.function.Supplier<com.legend.server.LiteV1Api.Answer> answer) {
        try {
            com.legend.server.LiteV1Api.Answer a = answer.get();
            if (a.bytes() != null) {
                return "OK\n" + a.status() + "\n" + a.contentType() + BASE64 + "\n"
                        + java.util.Base64.getEncoder().encodeToString(a.bytes());
            }
            return "OK\n" + a.status() + "\n" + a.contentType() + "\n" + a.json();
        } catch (RuntimeException | StackOverflowError e) {
            return failure(e);
        }
    }

    /** A failure as a refusal: lite API v1's error shape ({@code {kind, message, where?, raise?}}), as the server's
     *  routes classify it ({@link com.legend.server.LiteV1Api#refusal}). */
    public static String failure(Throwable e) {
        return "ERR\n" + com.legend.server.LiteV1Api.refusal(e).json();
    }
}
