package com.gs.legend.util;

import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive test suite for {@link Json} — the canonical JSON utility.
 *
 * <p>Covers: RFC 8259 conformance, all escape sequences, unicode/surrogate
 * handling, pretty vs compact output, streaming Writer over Appendable,
 * parser positive & negative cases, error position accuracy, depth limit,
 * polymorphic serialization, round-trip fidelity, and Node accessor shape.
 *
 * <p>This test is the authoritative conformance contract for
 * {@link com.gs.legend.util.Json}. Do not weaken assertions; add tests rather
 * than modify existing ones when behavior is extended.
 */
class JsonTest {

    // =========================================================================
    //  ESCAPE PRIMITIVES
    // =========================================================================

    @Nested
    @DisplayName("Json.escape / Json.unescape")
    class EscapePrimitives {

        @Test void escapesQuote() { assertEquals("\\\"", Json.escape("\"")); }
        @Test void escapesBackslash() { assertEquals("\\\\", Json.escape("\\")); }
        @Test void escapesNewline() { assertEquals("\\n", Json.escape("\n")); }
        @Test void escapesCarriageReturn() { assertEquals("\\r", Json.escape("\r")); }
        @Test void escapesTab() { assertEquals("\\t", Json.escape("\t")); }
        @Test void escapesBackspace() { assertEquals("\\b", Json.escape("\b")); }
        @Test void escapesFormfeed() { assertEquals("\\f", Json.escape("\f")); }

        @Test void escapesControlChar_00() { assertEquals("\\u0000", Json.escape("\0")); }
        @Test void escapesControlChar_01() { assertEquals("\\u0001", Json.escape("\u0001")); }
        @Test void escapesControlChar_1F() { assertEquals("\\u001f", Json.escape("\u001f")); }
        @Test void doesNotEscapeSpace() { assertEquals(" ", Json.escape(" ")); }
        @Test void doesNotEscapeAsciiPrintable() {
            String ascii = "!#$%&'()*+,-./0123456789:;<=>?@ABCXYZ[]^_`abcxyz{|}~";
            assertEquals(ascii, Json.escape(ascii));
        }
        @Test void escapesMixedContent() {
            assertEquals("hello\\nworld\\t!\\\"", Json.escape("hello\nworld\t!\""));
        }
        @Test void emptyStringEscapesToEmpty() { assertEquals("", Json.escape("")); }
        @Test void nullEscapesToEmpty() { assertEquals("", Json.escape(null)); }

        @Test void doesNotEscapeHighAscii() {
            // High-ASCII and Unicode chars are emitted literally; UTF-8 handles them natively.
            assertEquals("é", Json.escape("é"));
            assertEquals("中", Json.escape("中"));
        }

        @Test void escapeToStreamingProducesSameOutput() {
            StringBuilder sb = new StringBuilder();
            Json.escapeTo(sb, "hello\nworld");
            assertEquals(Json.escape("hello\nworld"), sb.toString());
        }

        @Test void unescapeInverseOfEscape_quote() { assertEquals("\"", Json.unescape("\\\"")); }
        @Test void unescapeInverseOfEscape_backslash() { assertEquals("\\", Json.unescape("\\\\")); }
        @Test void unescapeInverseOfEscape_newline() { assertEquals("\n", Json.unescape("\\n")); }
        @Test void unescapeInverseOfEscape_cr() { assertEquals("\r", Json.unescape("\\r")); }
        @Test void unescapeInverseOfEscape_tab() { assertEquals("\t", Json.unescape("\\t")); }
        @Test void unescapeInverseOfEscape_backspace() { assertEquals("\b", Json.unescape("\\b")); }
        @Test void unescapeInverseOfEscape_formfeed() { assertEquals("\f", Json.unescape("\\f")); }
        @Test void unescapeHandlesSlash() { assertEquals("/", Json.unescape("\\/")); }
        @Test void unescapeUnicodeEscape() { assertEquals("é", Json.unescape("\\u00e9")); }
        @Test void unescapeUnicodeEscape_zero() { assertEquals("\0", Json.unescape("\\u0000")); }
        @Test void unescapeNullReturnsNull() { assertEquals(null, Json.unescape(null)); }
        @Test void unescapeEmpty() { assertEquals("", Json.unescape("")); }
        @Test void unescapeNoEscapes() { assertEquals("plain", Json.unescape("plain")); }

        @Test void unescapeThrowsOnBadEscape() {
            assertThrows(IllegalArgumentException.class, () -> Json.unescape("\\x"));
        }
        @Test void unescapeThrowsOnTrailingBackslash() {
            assertThrows(IllegalArgumentException.class, () -> Json.unescape("foo\\"));
        }
        @Test void unescapeThrowsOnTruncatedUnicode() {
            assertThrows(IllegalArgumentException.class, () -> Json.unescape("\\u00"));
        }

        @Test void roundTripProperty() {
            String[] samples = {
                "", "hello", "\"quote\"", "\\backslash", "tab\there",
                "newline\nmid", "\0null", "\u001fboundary",
                "emoji\uD83D\uDE00here", "中文test"
            };
            for (String s : samples) {
                assertEquals(s, Json.unescape(Json.escape(s)), "round-trip failed for: " + s);
            }
        }
    }

    // =========================================================================
    //  WRITER — PRETTY
    // =========================================================================

    @Nested
    @DisplayName("Json.writer() — pretty")
    class WriterPretty {

        @Test void emptyObject() {
            assertEquals("{}", Json.writer().beginObject().endObject().toString());
        }

        @Test void emptyArray() {
            assertEquals("[]", Json.writer().beginArray().endArray().toString());
        }

        @Test void singleFieldObject() {
            String out = Json.writer().beginObject().field("x", "hi").endObject().toString();
            assertEquals("{\n  \"x\": \"hi\"\n}", out);
        }

        @Test void multiFieldObject() {
            String out = Json.writer().beginObject()
                    .field("a", "one").field("b", 2L).field("c", true)
                    .endObject().toString();
            assertEquals("{\n  \"a\": \"one\",\n  \"b\": 2,\n  \"c\": true\n}", out);
        }

        @Test void nestedObject() {
            String out = Json.writer().beginObject()
                    .name("inner").beginObject().field("x", 1L).endObject()
                    .endObject().toString();
            assertEquals("{\n  \"inner\": {\n    \"x\": 1\n  }\n}", out);
        }

        @Test void arrayOfStrings() {
            String out = Json.writer().beginArray()
                    .writeString("a").writeString("b").writeString("c")
                    .endArray().toString();
            assertEquals("[\n  \"a\",\n  \"b\",\n  \"c\"\n]", out);
        }

        @Test void fieldStringArray() {
            String out = Json.writer().beginObject()
                    .fieldStringArray("values", List.of("x", "y", "z"))
                    .endObject().toString();
            assertEquals("{\n  \"values\": [\n    \"x\",\n    \"y\",\n    \"z\"\n  ]\n}", out);
        }

        @Test void emptyArrayFieldNoInnerNewlines() {
            String out = Json.writer().beginObject()
                    .fieldStringArray("values", List.of())
                    .endObject().toString();
            assertEquals("{\n  \"values\": []\n}", out);
        }

        @Test void nullStringFieldEmitsJsonNull() {
            String out = Json.writer().beginObject().field("x", (String) null).endObject().toString();
            assertEquals("{\n  \"x\": null\n}", out);
        }

        @Test void explicitFieldNull() {
            String out = Json.writer().beginObject().fieldNull("x").endObject().toString();
            assertEquals("{\n  \"x\": null\n}", out);
        }

        @Test void escapesSpecialCharsInFieldValue() {
            String out = Json.writer().beginObject()
                    .field("x", "a\"b\\c\nd")
                    .endObject().toString();
            assertEquals("{\n  \"x\": \"a\\\"b\\\\c\\nd\"\n}", out);
        }

        @Test void escapesSpecialCharsInFieldName() {
            String out = Json.writer().beginObject()
                    .name("a\"b").writeString("v")
                    .endObject().toString();
            assertEquals("{\n  \"a\\\"b\": \"v\"\n}", out);
        }

        @Test void longValue() {
            String out = Json.writer().beginArray().writeLong(42L).endArray().toString();
            assertEquals("[\n  42\n]", out);
        }

        @Test void negativeLongValue() {
            String out = Json.writer().beginArray().writeLong(-100L).endArray().toString();
            assertEquals("[\n  -100\n]", out);
        }

        @Test void doubleValueIntegral() {
            String out = Json.writer().beginArray().writeDouble(3.0).endArray().toString();
            assertEquals("[\n  3\n]", out);
        }

        @Test void doubleValueFractional() {
            String out = Json.writer().beginArray().writeDouble(3.14).endArray().toString();
            assertEquals("[\n  3.14\n]", out);
        }

        @Test void boolValues() {
            String out = Json.writer().beginArray().writeBool(true).writeBool(false).endArray().toString();
            assertEquals("[\n  true,\n  false\n]", out);
        }

        @Test void nullValue() {
            String out = Json.writer().beginArray().writeNull().endArray().toString();
            assertEquals("[\n  null\n]", out);
        }

        @Test void writeDoubleRejectsNaN() {
            var w = Json.writer().beginArray();
            assertThrows(IllegalArgumentException.class, () -> w.writeDouble(Double.NaN));
        }

        @Test void writeDoubleRejectsInfinity() {
            var w = Json.writer().beginArray();
            assertThrows(IllegalArgumentException.class, () -> w.writeDouble(Double.POSITIVE_INFINITY));
            var w2 = Json.writer().beginArray();
            assertThrows(IllegalArgumentException.class, () -> w2.writeDouble(Double.NEGATIVE_INFINITY));
        }

        @Test void deeplyNestedStructure() {
            String out = Json.writer().beginObject()
                    .name("a").beginObject()
                        .name("b").beginObject()
                            .name("c").beginArray().writeLong(1L).endArray()
                        .endObject()
                    .endObject()
                .endObject().toString();
            assertEquals("""
                    {
                      "a": {
                        "b": {
                          "c": [
                            1
                          ]
                        }
                      }
                    }""", out);
        }

        @Test void throwsWhenCallingToStringWithUnclosedContainer() {
            var w = Json.writer().beginObject();
            assertThrows(IllegalStateException.class, w::toString);
        }

        @Test void throwsOnMismatchedEnd() {
            var w = Json.writer().beginObject();
            assertThrows(IllegalStateException.class, w::endArray);
        }

        @Test void throwsOnNameOutsideObject() {
            var w = Json.writer().beginArray();
            assertThrows(IllegalStateException.class, () -> w.name("x"));
        }

        @Test void throwsOnNameAtTopLevel() {
            var w = Json.writer();
            assertThrows(IllegalStateException.class, () -> w.name("x"));
        }
    }

    // =========================================================================
    //  WRITER — COMPACT
    // =========================================================================

    @Nested
    @DisplayName("Json.compactWriter() — compact, no whitespace")
    class WriterCompact {

        @Test void emptyObject() {
            assertEquals("{}", Json.compactWriter().beginObject().endObject().toString());
        }

        @Test void emptyArray() {
            assertEquals("[]", Json.compactWriter().beginArray().endArray().toString());
        }

        @Test void singleFieldObject() {
            String out = Json.compactWriter().beginObject().field("x", "hi").endObject().toString();
            assertEquals("{\"x\":\"hi\"}", out);
        }

        @Test void multiFieldObject() {
            String out = Json.compactWriter().beginObject()
                    .field("a", 1L).field("b", 2L).field("c", 3L)
                    .endObject().toString();
            assertEquals("{\"a\":1,\"b\":2,\"c\":3}", out);
        }

        @Test void nestedObject() {
            String out = Json.compactWriter().beginObject()
                    .name("inner").beginObject().field("x", 1L).endObject()
                    .endObject().toString();
            assertEquals("{\"inner\":{\"x\":1}}", out);
        }

        @Test void arrayMixedTypes() {
            String out = Json.compactWriter().beginArray()
                    .writeString("a").writeLong(42L).writeBool(true).writeNull()
                    .endArray().toString();
            assertEquals("[\"a\",42,true,null]", out);
        }

        @Test void noWhitespaceBetweenTokens() {
            String out = Json.compactWriter().beginObject()
                    .field("x", "a").field("y", "b")
                    .endObject().toString();
            assertFalse(out.contains(" "));
            assertFalse(out.contains("\n"));
        }

        @Test void escapesSpecialChars() {
            String out = Json.compactWriter().beginObject()
                    .field("x", "a\"b\\c\nd")
                    .endObject().toString();
            assertEquals("{\"x\":\"a\\\"b\\\\c\\nd\"}", out);
        }

        @Test void prettyAndCompactProduceSameTreeWhenParsed() {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("a", 1);
            data.put("b", List.of("x", "y"));
            data.put("c", true);
            String pretty = Json.toPretty(data);
            String compact = Json.toCompact(data);
            assertEquals(Json.parse(pretty), Json.parse(compact));
        }
    }

    // =========================================================================
    //  WRITER — STREAMING (Appendable-backed)
    // =========================================================================

    @Nested
    @DisplayName("Json.writer(Appendable) — streaming output")
    class WriterStreaming {

        @Test void streamsToStringWriter() {
            StringWriter sw = new StringWriter();
            Json.writer(sw).beginObject().field("x", "hi").endObject();
            assertEquals("{\n  \"x\": \"hi\"\n}", sw.toString());
        }

        @Test void compactStreamsToStringWriter() {
            StringWriter sw = new StringWriter();
            Json.compactWriter(sw).beginObject().field("x", 42L).endObject();
            assertEquals("{\"x\":42}", sw.toString());
        }

        @Test void streamingOutputByteEqualsStringBuilderOutput() {
            StringWriter sw = new StringWriter();
            Json.writer(sw).beginArray().writeLong(1L).writeLong(2L).writeLong(3L).endArray();
            String sbOut = Json.writer().beginArray().writeLong(1L).writeLong(2L).writeLong(3L).endArray().toString();
            assertEquals(sbOut, sw.toString());
        }

        @Test void streamingAppendableBackedWriterThrowsOnToString() {
            StringWriter sw = new StringWriter();
            var w = Json.writer(sw).beginObject().field("x", 1L).endObject();
            assertThrows(IllegalStateException.class, w::toString);
        }

        @Test void stringBuilderBackedWriterSupportsToString() {
            String s = Json.writer().beginObject().field("x", 1L).endObject().toString();
            assertEquals("{\n  \"x\": 1\n}", s);
        }

        @Test void pipedWriterRoundTrip() throws Exception {
            PipedWriter pw = new PipedWriter();
            PipedReader pr = new PipedReader(pw, 64 * 1024);

            Thread producer = new Thread(() -> {
                try (pw) {
                    Json.compactWriter(pw).beginObject()
                            .field("id", 42L)
                            .field("name", "piped")
                            .endObject();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            producer.start();

            Json.Obj parsed = Json.parseObject(pr);
            producer.join(5_000);

            assertEquals(42, parsed.getInt("id"));
            assertEquals("piped", parsed.getString("name"));
        }

        @Test void largeStreamingOutput() {
            StringWriter sw = new StringWriter();
            var w = Json.compactWriter(sw).beginArray();
            for (int i = 0; i < 10_000; i++) {
                w.beginObject().field("i", i).field("s", "row" + i).endObject();
            }
            w.endArray();
            // Parse back to verify well-formed
            Json.Node parsed = Json.parse(sw.toString());
            assertInstanceOf(Json.Arr.class, parsed);
            assertEquals(10_000, ((Json.Arr) parsed).items().size());
        }
    }

    // =========================================================================
    //  PARSER — POSITIVE CASES
    // =========================================================================

    @Nested
    @DisplayName("Json.parse — positive cases")
    class ParserPositive {

        @Test void parseEmptyObject() {
            assertInstanceOf(Json.Obj.class, Json.parse("{}"));
            assertTrue(((Json.Obj) Json.parse("{}")).fields().isEmpty());
        }

        @Test void parseEmptyArray() {
            assertInstanceOf(Json.Arr.class, Json.parse("[]"));
            assertTrue(((Json.Arr) Json.parse("[]")).items().isEmpty());
        }

        @Test void parseString() {
            assertEquals("hello", ((Json.Str) Json.parse("\"hello\"")).value());
        }

        @Test void parseEmptyString() {
            assertEquals("", ((Json.Str) Json.parse("\"\"")).value());
        }

        @Test void parseIntegerNumber() {
            Json.Num n = (Json.Num) Json.parse("42");
            assertEquals(42L, n.longValue());
            assertTrue(n.isInteger());
        }

        @Test void parseNegativeInteger() {
            Json.Num n = (Json.Num) Json.parse("-17");
            assertEquals(-17L, n.longValue());
        }

        @Test void parseZero() {
            Json.Num n = (Json.Num) Json.parse("0");
            assertEquals(0L, n.longValue());
        }

        @Test void parseFloat() {
            Json.Num n = (Json.Num) Json.parse("3.14");
            assertEquals(3.14, n.doubleValue(), 0.0);
            assertFalse(n.isInteger());
        }

        @Test void parseNegativeFloat() {
            Json.Num n = (Json.Num) Json.parse("-2.5");
            assertEquals(-2.5, n.doubleValue(), 0.0);
        }

        @Test void parseExponent() {
            Json.Num n = (Json.Num) Json.parse("1e5");
            assertEquals(100000.0, n.doubleValue(), 0.0);
            assertFalse(n.isInteger());
        }

        @Test void parseNegativeExponent() {
            Json.Num n = (Json.Num) Json.parse("1e-2");
            assertEquals(0.01, n.doubleValue(), 0.0);
        }

        @Test void parseUppercaseExponent() {
            Json.Num n = (Json.Num) Json.parse("1E3");
            assertEquals(1000.0, n.doubleValue(), 0.0);
        }

        @Test void parseExponentWithPlus() {
            Json.Num n = (Json.Num) Json.parse("1e+3");
            assertEquals(1000.0, n.doubleValue(), 0.0);
        }

        @Test void parseTrue() {
            assertTrue(((Json.Bool) Json.parse("true")).value());
        }

        @Test void parseFalse() {
            assertFalse(((Json.Bool) Json.parse("false")).value());
        }

        @Test void parseNull() {
            assertSame(Json.Null.INSTANCE, Json.parse("null"));
        }

        @Test void parseSingleFieldObject() {
            Json.Obj o = (Json.Obj) Json.parse("{\"x\":1}");
            assertEquals(1, o.getInt("x"));
        }

        @Test void parseMultiFieldObject() {
            Json.Obj o = (Json.Obj) Json.parse("{\"a\":1,\"b\":\"two\",\"c\":true}");
            assertEquals(1, o.getInt("a"));
            assertEquals("two", o.getString("b"));
            assertTrue(o.getBool("c"));
        }

        @Test void parseNestedObject() {
            Json.Obj o = (Json.Obj) Json.parse("{\"inner\":{\"x\":42}}");
            assertEquals(42, o.getObj("inner").getInt("x"));
        }

        @Test void parseArrayMixed() {
            Json.Arr a = (Json.Arr) Json.parse("[1,\"two\",true,null]");
            assertEquals(4, a.items().size());
            assertEquals(1, ((Json.Num) a.items().get(0)).longValue());
            assertEquals("two", ((Json.Str) a.items().get(1)).value());
            assertTrue(((Json.Bool) a.items().get(2)).value());
            assertSame(Json.Null.INSTANCE, a.items().get(3));
        }

        @Test void parseNestedArray() {
            Json.Arr a = (Json.Arr) Json.parse("[[1,2],[3,4]]");
            assertEquals(2, a.items().size());
            assertEquals(1, ((Json.Num) ((Json.Arr) a.items().get(0)).items().get(0)).longValue());
        }

        @Test void parseLeadingWhitespace() {
            assertInstanceOf(Json.Obj.class, Json.parse("  \n\t{}"));
        }

        @Test void parseTrailingWhitespace() {
            assertInstanceOf(Json.Obj.class, Json.parse("{}  \n"));
        }

        @Test void parseWhitespaceBetweenTokens() {
            Json.Obj o = (Json.Obj) Json.parse("{ \"a\" : 1 , \"b\" : 2 }");
            assertEquals(1, o.getInt("a"));
            assertEquals(2, o.getInt("b"));
        }

        @Test void parsePreservesInsertionOrder() {
            Json.Obj o = (Json.Obj) Json.parse("{\"z\":1,\"a\":2,\"m\":3}");
            assertEquals(List.of("z", "a", "m"), new ArrayList<>(o.fields().keySet()));
        }

        @Test void parseHandlesStringWithEscapes() {
            Json.Str s = (Json.Str) Json.parse("\"hello\\nworld\\t!\"");
            assertEquals("hello\nworld\t!", s.value());
        }

        @Test void parseHandlesUnicodeEscape() {
            Json.Str s = (Json.Str) Json.parse("\"\\u00e9\"");
            assertEquals("é", s.value());
        }

        @Test void parseHandlesSlashEscape() {
            Json.Str s = (Json.Str) Json.parse("\"a\\/b\"");
            assertEquals("a/b", s.value());
        }

        @Test void parseHandlesEscapeOfBackspace() {
            Json.Str s = (Json.Str) Json.parse("\"a\\bb\"");
            assertEquals("a\bb", s.value());
        }

        @Test void parseHandlesEscapeOfFormfeed() {
            Json.Str s = (Json.Str) Json.parse("\"a\\fb\"");
            assertEquals("a\fb", s.value());
        }

        @Test void parseLargeLong() {
            Json.Num n = (Json.Num) Json.parse("9223372036854775807");
            assertEquals(Long.MAX_VALUE, n.longValue());
        }

        @Test void parseMostNegativeLong() {
            Json.Num n = (Json.Num) Json.parse("-9223372036854775808");
            assertEquals(Long.MIN_VALUE, n.longValue());
        }

        @Test void parseFromReader() {
            Json.Obj o = (Json.Obj) Json.parse(new StringReader("{\"x\":1}"));
            assertEquals(1, o.getInt("x"));
        }

        @Test void parseObjectFromString() {
            Json.Obj o = Json.parseObject("{\"x\":1}");
            assertEquals(1, o.getInt("x"));
        }

        @Test void parseObjectFromReader() {
            Json.Obj o = Json.parseObject(new StringReader("{\"x\":1}"));
            assertEquals(1, o.getInt("x"));
        }
    }

    // =========================================================================
    //  PARSER — NEGATIVE CASES (strict error handling)
    // =========================================================================

    @Nested
    @DisplayName("Json.parse — strict error handling")
    class ParserNegative {

        @ParameterizedTest
        @ValueSource(strings = {
                "{",                      // unclosed object
                "}",                      // stray close
                "[",                      // unclosed array
                "]",                      // stray close
                "{\"x\":1,}",             // trailing comma in object
                "[1,2,]",                 // trailing comma in array
                "{\"x\"1}",               // missing colon
                "{\"x\":1",               // missing closing brace
                "{x:1}",                  // unquoted key
                "'single'",               // single-quoted string
                "\"unterminated",         // unterminated string
                "",                       // empty
                "   ",                    // whitespace only
                "-",                      // lone minus
                "--5",                    // double minus
                "tru",                    // truncated boolean
                "fals",                   // truncated boolean
                "nul",                    // truncated null
                "{} extra",               // trailing content
                "1 2",                    // two values
                "\"a\" \"b\"",            // two strings
        })
        void rejectsMalformedInput(String input) {
            assertThrows(IllegalArgumentException.class, () -> Json.parse(input),
                    "Should reject: " + input);
        }

        @Test void rejectsUnescapedNewlineInString() {
            // RFC 8259 §7: control chars 0x00-0x1F must be escaped.
            assertThrows(IllegalArgumentException.class, () -> Json.parse("\"line1\nline2\""));
        }

        @Test void rejectsUnescapedTabInString() {
            assertThrows(IllegalArgumentException.class, () -> Json.parse("\"a\tb\""));
        }

        @Test void rejectsUnescapedControlCharInString() {
            assertThrows(IllegalArgumentException.class, () -> Json.parse("\"a\u0001b\""));
        }

        @Test void rejectsInvalidEscape() {
            assertThrows(IllegalArgumentException.class, () -> Json.parse("\"\\x\""));
        }

        @Test void rejectsTruncatedUnicodeEscape() {
            assertThrows(IllegalArgumentException.class, () -> Json.parse("\"\\u12\""));
        }

        @Test void parseObjectThrowsIfRootIsArray() {
            assertThrows(IllegalArgumentException.class, () -> Json.parseObject("[1,2]"));
        }

        @Test void parseObjectThrowsIfRootIsScalar() {
            assertThrows(IllegalArgumentException.class, () -> Json.parseObject("42"));
        }
    }

    // =========================================================================
    //  PARSER — ERROR POSITION ACCURACY
    // =========================================================================

    @Nested
    @DisplayName("Parser error positions")
    class ErrorPositions {

        @Test void reportsLineNumber() {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> Json.parse("{\n\"x\":@\n}"));
            assertTrue(ex.getMessage().contains("line 2"), "Expected 'line 2' in: " + ex.getMessage());
        }

        @Test void reportsColumn() {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> Json.parse("{\"x\":@}"));
            assertTrue(ex.getMessage().contains("col"), "Expected 'col' in: " + ex.getMessage());
        }

        @Test void reportsLineOneForFirstLineError() {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> Json.parse("@"));
            assertTrue(ex.getMessage().contains("line 1"), "Expected 'line 1' in: " + ex.getMessage());
        }

        @Test void includesUsefulMessage() {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> Json.parse("{\"x\"1}"));
            assertNotNull(ex.getMessage());
            assertFalse(ex.getMessage().isBlank());
        }
    }

    // =========================================================================
    //  DEPTH LIMIT (security)
    // =========================================================================

    @Nested
    @DisplayName("Parser depth limit")
    class DepthLimit {

        @Test void defaultDepthAllows64DeepObjects() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 64; i++) sb.append("{\"x\":");
            sb.append("1");
            for (int i = 0; i < 64; i++) sb.append("}");
            assertDoesNotThrow(() -> Json.parse(sb.toString()));
        }

        @Test void defaultDepthRejects65DeepObjects() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 65; i++) sb.append("{\"x\":");
            sb.append("1");
            for (int i = 0; i < 65; i++) sb.append("}");
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> Json.parse(sb.toString()));
            assertTrue(ex.getMessage().contains("depth"),
                    "Expected 'depth' in error: " + ex.getMessage());
        }

        @Test void defaultDepthRejectsDeepArrays() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 65; i++) sb.append("[");
            sb.append("1");
            for (int i = 0; i < 65; i++) sb.append("]");
            assertThrows(IllegalArgumentException.class, () -> Json.parse(sb.toString()));
        }

        @Test void customDepthLimitRespected() {
            String threeDeep = "{\"a\":{\"b\":{\"c\":1}}}";
            Json.Config limit2 = new Json.Config(2);
            assertThrows(IllegalArgumentException.class, () -> Json.parse(threeDeep, limit2));
            Json.Config limit3 = new Json.Config(3);
            assertDoesNotThrow(() -> Json.parse(threeDeep, limit3));
        }

        @Test void mixedArrayObjectDepthCounted() {
            String s = "[{\"a\":[1]}]"; // depth 3: arr > obj > arr
            Json.Config limit2 = new Json.Config(2);
            assertThrows(IllegalArgumentException.class, () -> Json.parse(s, limit2));
            Json.Config limit3 = new Json.Config(3);
            assertDoesNotThrow(() -> Json.parse(s, limit3));
        }

        @Test void zeroDepthIsRejected() {
            assertThrows(IllegalArgumentException.class, () -> new Json.Config(0));
        }

        @Test void negativeDepthIsRejected() {
            assertThrows(IllegalArgumentException.class, () -> new Json.Config(-1));
        }
    }

    // =========================================================================
    //  NODE ACCESSORS
    // =========================================================================

    @Nested
    @DisplayName("Obj accessors")
    class NodeAccessors {

        private Json.Obj sample() {
            return Json.parseObject("""
                    {"s":"hello","i":42,"l":9999999999,"d":3.14,"b":true,"n":null,"arr":["a","b","c"],"obj":{"x":1}}""");
        }

        @Test void getString() { assertEquals("hello", sample().getString("s")); }
        @Test void getInt() { assertEquals(42, sample().getInt("i")); }
        @Test void getLong() { assertEquals(9999999999L, sample().getLong("l")); }
        @Test void getDouble() { assertEquals(3.14, sample().getDouble("d"), 0.0); }
        @Test void getBool() { assertTrue(sample().getBool("b")); }
        @Test void getArr() { assertEquals(3, sample().getArr("arr").items().size()); }
        @Test void getObj() { assertEquals(1, sample().getObj("obj").getInt("x")); }
        @Test void getStringArray() { assertEquals(List.of("a","b","c"), sample().getStringArray("arr")); }
        @Test void hasReturnsTrue() { assertTrue(sample().has("s")); }
        @Test void hasReturnsFalse() { assertFalse(sample().has("missing")); }

        @Test void getThrowsOnMissing() {
            assertThrows(IllegalArgumentException.class, () -> sample().get("missing"));
        }
        @Test void getStringThrowsOnMissing() {
            assertThrows(IllegalArgumentException.class, () -> sample().getString("missing"));
        }

        @Test void getStringOrReturnsDefaultForMissing() {
            assertEquals("DEFAULT", sample().getStringOr("missing", "DEFAULT"));
        }
        @Test void getStringOrReturnsDefaultForNull() {
            assertEquals("DEFAULT", sample().getStringOr("n", "DEFAULT"));
        }
        @Test void getIntOrReturnsDefaultForMissing() {
            assertEquals(-1, sample().getIntOr("missing", -1));
        }
        @Test void getLongOrReturnsDefaultForNull() {
            assertEquals(-1L, sample().getLongOr("n", -1L));
        }
        @Test void getBoolOrReturnsDefaultForMissing() {
            assertFalse(sample().getBoolOr("missing", false));
        }
        @Test void getObjOrReturnsDefaultForMissing() {
            Json.Obj def = Json.obj();
            assertSame(def, sample().getObjOr("missing", def));
        }
        @Test void getArrOrReturnsDefaultForMissing() {
            Json.Arr def = Json.arr();
            assertSame(def, sample().getArrOr("missing", def));
        }
        @Test void getStringArrayOrReturnsDefaultForMissing() {
            List<String> def = List.of("d");
            assertSame(def, sample().getStringArrayOr("missing", def));
        }
    }

    // =========================================================================
    //  NODE CONSTRUCTION (Json.of, obj, arr, etc.)
    // =========================================================================

    @Nested
    @DisplayName("Json.of coercion + construction")
    class Construction {

        @Test void ofNull() { assertSame(Json.Null.INSTANCE, Json.of(null)); }
        @Test void ofString() { assertEquals(new Json.Str("hi"), Json.of("hi")); }
        @Test void ofLong() { assertTrue(((Json.Num) Json.of(42L)).isInteger()); }
        @Test void ofInt() { assertTrue(((Json.Num) Json.of(42)).isInteger()); }
        @Test void ofShort() { assertTrue(((Json.Num) Json.of((short) 42)).isInteger()); }
        @Test void ofByte() { assertTrue(((Json.Num) Json.of((byte) 42)).isInteger()); }
        @Test void ofDouble() { assertFalse(((Json.Num) Json.of(3.14)).isInteger()); }
        @Test void ofFloat() { assertFalse(((Json.Num) Json.of(3.14f)).isInteger()); }
        @Test void ofBoolean() { assertEquals(new Json.Bool(true), Json.of(true)); }

        @Test void ofMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("a", 1);
            m.put("b", "two");
            Json.Obj o = (Json.Obj) Json.of(m);
            assertEquals(1, o.getInt("a"));
            assertEquals("two", o.getString("b"));
        }

        @Test void ofList() {
            Json.Arr a = (Json.Arr) Json.of(List.of(1, 2, 3));
            assertEquals(3, a.items().size());
        }

        @Test void ofArray() {
            Json.Arr a = (Json.Arr) Json.of(new int[]{1, 2, 3});
            assertEquals(3, a.items().size());
        }

        @Test void ofStringArray() {
            Json.Arr a = (Json.Arr) Json.of(new String[]{"x", "y"});
            assertEquals("x", ((Json.Str) a.items().get(0)).value());
        }

        @Test void ofRejectsUnsupportedType() {
            assertThrows(IllegalArgumentException.class, () -> Json.of(new Object()));
        }

        @Test void ofRejectsNonStringKeys() {
            Map<Object, Object> m = new LinkedHashMap<>();
            m.put(1, "bad");
            assertThrows(IllegalArgumentException.class, () -> Json.of(m));
        }

        @Test void ofReturnsNodeAsIs() {
            Json.Str s = new Json.Str("hello");
            assertSame(s, Json.of(s));
        }

        @Test void objFactoryReturnsEmpty() {
            assertTrue(Json.obj().fields().isEmpty());
        }

        @Test void arrFactoryReturnsEmpty() {
            assertTrue(Json.arr().items().isEmpty());
        }

        @Test void strFactory() { assertEquals("hi", Json.str("hi").value()); }
        @Test void numFactoryLong() { assertEquals(42L, Json.num(42L).longValue()); }
        @Test void numFactoryDouble() { assertEquals(3.14, Json.num(3.14).doubleValue(), 0.0); }
        @Test void boolFactory() { assertTrue(Json.bool(true).value()); }
        @Test void nilFactory() { assertSame(Json.Null.INSTANCE, Json.nil()); }

        @Test void strRejectsNull() {
            assertThrows(NullPointerException.class, () -> new Json.Str(null));
        }

        @Test void numRejectsNaN() {
            assertThrows(IllegalArgumentException.class, () -> Json.num(Double.NaN));
        }

        @Test void numRejectsInfinity() {
            assertThrows(IllegalArgumentException.class, () -> Json.num(Double.POSITIVE_INFINITY));
        }
    }

    // =========================================================================
    //  POLYMORPHIC SERIALIZATION (toCompact / toPretty)
    // =========================================================================

    @Nested
    @DisplayName("Json.toCompact / Json.toPretty")
    class PolymorphicSerialization {

        @Test void serializeNull() { assertEquals("null", Json.toCompact(null)); }
        @Test void serializeString() { assertEquals("\"hi\"", Json.toCompact("hi")); }
        @Test void serializeStringWithEscape() {
            assertEquals("\"a\\nb\"", Json.toCompact("a\nb"));
        }
        @Test void serializeLong() { assertEquals("42", Json.toCompact(42L)); }
        @Test void serializeInteger() { assertEquals("42", Json.toCompact(42)); }
        @Test void serializeDouble() { assertEquals("3.14", Json.toCompact(3.14)); }
        @Test void serializeBoolean() { assertEquals("true", Json.toCompact(true)); }

        @Test void serializeMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("a", 1);
            m.put("b", "two");
            assertEquals("{\"a\":1,\"b\":\"two\"}", Json.toCompact(m));
        }

        @Test void serializeList() {
            assertEquals("[1,2,3]", Json.toCompact(List.of(1, 2, 3)));
        }

        @Test void serializeNode() {
            assertEquals("42", Json.toCompact(Json.num(42L)));
        }

        @Test void serializeNestedMap() {
            Map<String, Object> inner = new LinkedHashMap<>();
            inner.put("x", 1);
            Map<String, Object> outer = new LinkedHashMap<>();
            outer.put("inner", inner);
            assertEquals("{\"inner\":{\"x\":1}}", Json.toCompact(outer));
        }

        @Test void toPrettyEmitsWhitespace() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("x", 1);
            assertTrue(Json.toPretty(m).contains("\n"));
            assertTrue(Json.toPretty(m).contains("  "));
        }

        @Test void toCompactEmitsNoWhitespace() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("x", 1);
            assertFalse(Json.toCompact(m).contains(" "));
            assertFalse(Json.toCompact(m).contains("\n"));
        }

        @Test void toCompactRejectsUnsupported() {
            assertThrows(IllegalArgumentException.class, () -> Json.toCompact(new Object()));
        }

        @Test void toCompactRejectsNonStringKeys() {
            Map<Object, Object> m = new LinkedHashMap<>();
            m.put(1, "v");
            assertThrows(IllegalArgumentException.class, () -> Json.toCompact(m));
        }
    }

    // =========================================================================
    //  ROUND-TRIP (parse → serialize → parse)
    // =========================================================================

    @Nested
    @DisplayName("Parse → Serialize → Parse round-trip")
    class RoundTrip {

        @Test void emptyObject() { assertRoundTrip("{}"); }
        @Test void emptyArray() { assertRoundTrip("[]"); }
        @Test void flatObject() { assertRoundTrip("{\"a\":1,\"b\":\"two\",\"c\":true,\"d\":null}"); }
        @Test void nestedObject() { assertRoundTrip("{\"a\":{\"b\":{\"c\":1}}}"); }
        @Test void arrayOfObjects() { assertRoundTrip("[{\"x\":1},{\"y\":2},{\"z\":3}]"); }
        @Test void escapeSequences() { assertRoundTrip("{\"s\":\"a\\nb\\tc\\\"d\\\\e\"}"); }
        @Test void unicodeChars() { assertRoundTrip("{\"s\":\"中文éñ\"}"); }
        @Test void surrogatePair() {
            // Emoji is stored as a surrogate pair in Java String; round-trip preserves it.
            Json.Node parsed = Json.parse("{\"s\":\"😀\"}");
            String serialized = Json.toCompact(parsed);
            assertEquals(parsed, Json.parse(serialized));
        }
        @Test void numbers() { assertRoundTrip("[0,1,-1,42,-42,3.14,-2.5]"); }

        @Test void integerValuedExponentLosesFloatFlavorButPreservesValue() {
            // 1e5 parses as Num(100000.0, isInteger=false).  Compact serialization
            // emits "100000" (writeDouble short-circuits for integer-valued doubles).
            // Re-parse yields Num(100000, isInteger=true).  Value is preserved; the
            // "this came from a float expression" flavor is not. This is intentional
            // — writeDouble chooses the more compact representation. Document the
            // expectation here rather than in RoundTrip so the distinction is visible.
            Json.Arr first = (Json.Arr) Json.parse("[1e5]");
            Json.Num firstNum = (Json.Num) first.items().get(0);
            assertEquals(100000.0, firstNum.doubleValue(), 0.0);
            assertFalse(firstNum.isInteger());

            String compact = Json.toCompact(first);
            assertEquals("[100000]", compact);

            Json.Arr second = (Json.Arr) Json.parse(compact);
            Json.Num secondNum = (Json.Num) second.items().get(0);
            assertEquals(100000L, secondNum.longValue());
            assertTrue(secondNum.isInteger());
        }
        @Test void deepNesting() {
            assertRoundTrip("{\"a\":{\"b\":{\"c\":{\"d\":{\"e\":1}}}}}");
        }
        @Test void manyFields() {
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < 100; i++) {
                if (i > 0) sb.append(",");
                sb.append("\"k").append(i).append("\":").append(i);
            }
            sb.append("}");
            assertRoundTrip(sb.toString());
        }

        private void assertRoundTrip(String compact) {
            Json.Node first = Json.parse(compact);
            String serialized = Json.toCompact(first);
            Json.Node second = Json.parse(serialized);
            assertEquals(first, second, "Round-trip failed; serialized=" + serialized);
        }
    }

    // =========================================================================
    //  GOLDEN FIXTURE COMPATIBILITY (Phase B)
    // =========================================================================

    @Nested
    @DisplayName("Compatibility with Phase B golden fixtures")
    class GoldenFixtureCompat {

        @Test void prettyOutputByteEqualsExpectedForSimpleObject() {
            // This pins the exact pretty-print format — changes here will surface in
            // Phase B golden byte-equality tests (refdata__Rating.json etc.).
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("schemaVersion", 1);
            m.put("kind", "enum");
            m.put("fqn", "a::b::C");
            m.put("values", List.of("X", "Y", "Z"));
            String expected = """
                    {
                      "schemaVersion": 1,
                      "kind": "enum",
                      "fqn": "a::b::C",
                      "values": [
                        "X",
                        "Y",
                        "Z"
                      ]
                    }""";
            assertEquals(expected, Json.toPretty(m));
        }

        @Test void emptyArrayFieldFormatsInline() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("values", List.of());
            assertEquals("{\n  \"values\": []\n}", Json.toPretty(m));
        }
    }

    // =========================================================================
    //  RFC 8259 CONFORMANCE SUBSET
    //  (representative cases from nst/JSONTestSuite)
    // =========================================================================

    @Nested
    @DisplayName("RFC 8259 conformance (JSONTestSuite representative corpus)")
    class JsonTestSuiteConformance {

        // y_* cases — MUST accept

        @ParameterizedTest
        @ValueSource(strings = {
                "[]",
                "[0]",
                "[-0]",
                "[0.1]",
                "[-0.1]",
                "[123]",
                "[-123]",
                "[1e0]",
                "[1E0]",
                "[1e+0]",
                "[1e-0]",
                "[1.5e10]",
                "[\"\"]",
                "[\"a\"]",
                "[\"ab\"]",
                "[\"\\\"\"]",
                "[\"\\\\\"]",
                "[\"\\/\"]",
                "[\"\\b\"]",
                "[\"\\f\"]",
                "[\"\\n\"]",
                "[\"\\r\"]",
                "[\"\\t\"]",
                "[\"\\u0000\"]",
                "[\"\\u00ff\"]",
                "[\"a/b\"]",
                "[\"\\u0060\"]",
                "[true]",
                "[false]",
                "[null]",
                "[[]]",
                "[[1,2,3]]",
                "[{}]",
                "[{\"a\":1}]",
                "{}",
                "{\"a\":\"b\"}",
                "{\"a\":null}",
                "{\"\":0}",
                "{\"a\":[]}",
                " [] ",
                "\t{}\n",
        })
        void accepts(String input) {
            assertDoesNotThrow(() -> Json.parse(input), "Should accept: " + input);
        }

        // n_* cases — MUST reject

        @ParameterizedTest
        @ValueSource(strings = {
                "",
                "[",
                "]",
                "{",
                "}",
                "[1,",
                "[1 2]",
                "{\"a\"}",
                "{\"a\":1,}",
                "[1,]",
                "{,}",
                "[,]",
                "{\"a\":1,,\"b\":2}",
                "\"unterminated",
                "'single'",
                "[01]",                    // leading zero — Java accepts but RFC rejects
                "[1.]",                    // trailing dot — loose, but most parsers accept
                "[.5]",                    // missing leading digit
                "[1e]",                    // incomplete exponent
                "[1e+]",                   // incomplete exponent
                "True",                    // case-sensitive keywords
                "False",
                "Null",
                "NaN",
                "Infinity",
                "undefined",
                "[1 2 3]",
                "[1,2,,3]",
                "{\"a\"=1}",               // wrong separator
                "{\"a\";1}",               // wrong separator
                "[\"\\z\"]",               // bad escape
                "[\"\\u00\"]",             // truncated unicode
                "[\"\\u00g0\"]",           // non-hex in unicode
                "null trailing",
        })
        void rejects(String input) {
            assertThrows(IllegalArgumentException.class, () -> Json.parse(input),
                    "Should reject: " + input);
        }
    }

    // =========================================================================
    //  EDGE CASES
    // =========================================================================

    @Nested
    @DisplayName("Edge cases")
    class EdgeCases {

        @Test void objectWithEmptyStringKey() {
            Json.Obj o = Json.parseObject("{\"\":42}");
            assertEquals(42, o.getInt(""));
        }

        @Test void objectWithAllAsciiPrintableCharsInKey() {
            String key = "!#$%&'()*+,-./0123456789:;<=>?@ABC[]^_`abc{|}~";
            String json = "{\"" + key + "\":1}";
            Json.Obj o = Json.parseObject(json);
            assertEquals(1, o.getInt(key));
        }

        @Test void fieldOrderingPreservedForLargeObject() {
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < 200; i++) {
                if (i > 0) sb.append(",");
                sb.append("\"k").append(i).append("\":").append(i);
            }
            sb.append("}");
            Json.Obj o = Json.parseObject(sb.toString());
            int idx = 0;
            for (String key : o.fields().keySet()) {
                assertEquals("k" + idx, key);
                idx++;
            }
        }

        @Test void longStringRoundTrip() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 10_000; i++) sb.append("x");
            String s = sb.toString();
            Json.Str parsed = (Json.Str) Json.parse("\"" + s + "\"");
            assertEquals(s, parsed.value());
        }

        @Test void duplicateKeyLastWins() {
            Json.Obj o = Json.parseObject("{\"x\":1,\"x\":2}");
            assertEquals(2, o.getInt("x"));
        }

        @Test void deeplyNestedMixedArraysAndObjects() {
            Json.Node parsed = Json.parse("[{\"a\":[{\"b\":[1,2,3]}]}]");
            Json.Arr outer = (Json.Arr) parsed;
            Json.Obj lvl1 = (Json.Obj) outer.items().get(0);
            Json.Arr lvl2 = lvl1.getArr("a");
            Json.Obj lvl3 = (Json.Obj) lvl2.items().get(0);
            Json.Arr lvl4 = lvl3.getArr("b");
            assertEquals(3, lvl4.items().size());
        }

        @Test void arrayOfEmptyObjects() {
            Json.Arr a = (Json.Arr) Json.parse("[{},{},{}]");
            assertEquals(3, a.items().size());
            for (Json.Node n : a.items()) {
                assertInstanceOf(Json.Obj.class, n);
                assertTrue(((Json.Obj) n).fields().isEmpty());
            }
        }

        @Test void escapedQuotesInNestedContext() {
            Json.Obj o = Json.parseObject("{\"x\":\"say \\\"hi\\\"\"}");
            assertEquals("say \"hi\"", o.getString("x"));
        }
    }
}
