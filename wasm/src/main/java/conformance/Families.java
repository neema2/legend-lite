package conformance;

import java.util.List;

/**
 * THE CONFORMANCE FAMILIES (docs/WEB_IMAGE_SPIKE_2026_10_10.md, step 1): TeaVM's class library held to the JDK's over
 * what lite's browser code calls, the riskiest first -- a number's text both ways, Character and String, the formats
 * and regular expressions lite writes, time and zones, the order hashed collections iterate in, the codecs. Each
 * family is a function of nothing but its own fixed seed, so the JVM and the module answer the same questions.
 */
final class Families {

    private Families() {
    }

    static final List<String> NAMES = List.of("double.toString.edges", "double.toString.random", "double.append",
            "double.shortDecimals", "double.parse.forms", "double.parse.random", "double.parse.midpoints",
            "float.toString", "bigDecimal", "bigInteger", "integers", "character", "string", "format", "regex",
            "codecs", "time.text", "time.zones", "ordering.hash", "ordering.codes");

    static String answers(String family) {
        return switch (family) {
            case "double.toString.edges" -> Numbers.doubleToStringEdges();
            case "double.toString.random" -> Numbers.doubleToStringRandom();
            case "double.append" -> Numbers.doubleAppend();
            case "double.shortDecimals" -> Numbers.doubleShortDecimals();
            case "double.parse.forms" -> Numbers.doubleParseForms();
            case "double.parse.random" -> Numbers.doubleParseRandom();
            case "double.parse.midpoints" -> Numbers.doubleParseMidpoints();
            case "float.toString" -> Numbers.floatToString();
            case "bigDecimal" -> Numbers.bigDecimal();
            case "bigInteger" -> Numbers.bigInteger();
            case "integers" -> Numbers.integers();
            case "character" -> Text.character();
            case "string" -> Text.string();
            case "format" -> Text.format();
            case "regex" -> Text.regex();
            case "codecs" -> Text.codecs();
            case "time.text" -> Time.parseAndPrint();
            case "time.zones" -> Time.zones();
            case "ordering.hash" -> Ordering.hashOrders();
            case "ordering.codes" -> Ordering.hashCodes();
            default -> throw new IllegalArgumentException("no conformance family " + family);
        };
    }
}
