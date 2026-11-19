package com.legend.equivalence;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.protocol.pure.m3.PackageableElement;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * LEGEND-ENGINE'S PARSE OF A TEXT, ONCE PER JVM (2026-11-19, //parser-equivalence:parser_parity's time): the referee's
 * answer for a model text -- the JSON of what it parsed, or what it threw -- kept for the run and handed to every parity
 * class that asks for the same text with the same options.
 *
 * <p>{@link CorpusSweepTest} was built as one referee parse per source (it replaced six classes and the first
 * {@code RefereeParses} cache, whose eviction tuning existed only because each ran its own corpus loop). The protocol
 * program's three parity classes since -- {@link ModelComposerParityTest}, {@link ModelReaderParityTest} and
 * {@link ComposerParityTest} -- each loop over {@link Corpus#all()} again with a parser of their own. Measured on main
 * (b75d6c73a, Linux): 71.6% of parser_parity's CPU samples were inside the engine's grammar parser, and those four
 * classes were 93% of its 284.7 s. On macOS, which runs one test at a time on its 7 GB runner, parser_parity took 361 to
 * 582 s, and a slow runner crossed its 900 s limit (#127's run 44587411059).
 *
 * <p>ONLY THE REFEREE'S ANSWER IS SHARED, and only as TEXT. Every class reads the engine's parse in serialized form
 * alone: the whole model's JSON (all four), and each element's own JSON ({@link ModelReaderParityTest}, which also
 * counts the elements, as {@link ModelComposerParityTest}'s round-trip texts do). So the answer is written once, as
 * those classes wrote it -- {@code mapper.writeValueAsString(parsed)} and {@code mapper.writeValueAsString(element)} for
 * each of {@code parsed.getElements()}, with
 * {@code ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports()}, the mapper all four made --
 * and the engine's object is let go. No class sees an object another could change: what is held is immutable strings. A
 * refusal is the same {@link Throwable} the referee threw, thrown again to each caller, which catches it as before; a
 * model the mapper could not write keeps that failure, thrown where the class asked for the text. Lite's own parser
 * still parses every source in every class: nothing of lite's is shared.
 *
 * <p>The key is the whole text and the options: {@link #withSourceInformation} is {@code parseModel(text)},
 * {@link #withoutSourceInformation} is {@code parseModel(text, "", 0, 0, false)}. Each answer is held softly: the heap's
 * peak was already 2,355 MB of 2.8 GB before this memo, so the JVM lets an answer go when it needs the room, and the
 * next caller parses the text again.
 */
final class RefereeParses {

    /** The referee, as each class made its own: {@code PureGrammarParser.newInstance()}, the default extensions. */
    private static final PureGrammarParser REFEREE = PureGrammarParser.newInstance();

    /** The mapper each of the four classes made for the referee's JSON. */
    private static final ObjectMapper MAPPER =
            ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports();

    private static final Map<Key, SoftReference<Answer>> HELD = new HashMap<>();

    private static int parsed;
    private static int reused;
    private static int keptWhole;

    private RefereeParses() {
    }

    private record Key(String text, boolean sourceInformation) {
    }

    /** What the referee answered: its parse as text, or what it threw (never both). */
    private record Answer(Parse parse, Throwable refused) {
    }

    /**
     * The engine's parse of a text, as JSON: the whole model, and each of its elements on its own, in order. A model
     * the mapper could not write keeps the mapper's failure, thrown by both accessors.
     *
     * <p>The whole model's JSON is held as the text around its elements ({@code before}, {@code after}) when the
     * elements' own JSON, joined by commas, occurs in it -- the mapper writes the model's {@code elements} array with
     * the same bytes -- so each element's text is held once, not twice (measured: holding both took the live peak from
     * 817 MB, the engine's objects held, to 955 MB). {@link #json()} gives back the mapper's text exactly: before,
     * the joined elements and after are its three consecutive parts, cut where the joined elements occur. A model whose
     * elements do not occur in its text that way keeps its whole text.
     */
    static final class Parse {
        private final String whole;
        private final String before;
        private final String after;
        private final List<String> elements;
        private final IOException unwritable;

        private Parse(String whole, String before, String after, List<String> elements, IOException unwritable) {
            this.whole = whole;
            this.before = before;
            this.after = after;
            this.elements = elements;
            this.unwritable = unwritable;
        }

        /** {@code mapper.writeValueAsString(parsed)}. */
        String json() throws IOException {
            if (unwritable != null) {
                throw unwritable;
            }
            return whole != null ? whole : before + String.join(",", elements) + after;
        }

        /** {@code mapper.writeValueAsString(e)} for each {@code e} of {@code parsed.getElements()}, in order. */
        List<String> elements() throws IOException {
            if (unwritable != null) {
                throw unwritable;
            }
            return elements;
        }
    }

    /** The engine's {@code parseModel(text)}, once per text in a run; its refusal thrown again as it was thrown. */
    static Parse withSourceInformation(String text) {
        return answer(new Key(text, true));
    }

    /** The engine's {@code parseModel(text, "", 0, 0, false)}, once per text in a run; its refusal thrown again. */
    static Parse withoutSourceInformation(String text) {
        return answer(new Key(text, false));
    }

    /** How many texts were parsed and how many answers were handed out again, for the run's log. */
    static synchronized String counts() {
        return "referee parses: " + parsed + " parsed, " + reused + " reused, " + keptWhole + " kept whole";
    }

    private static synchronized Parse answer(Key key) {
        SoftReference<Answer> held = HELD.get(key);
        Answer answer = held == null ? null : held.get();
        if (answer == null) {
            answer = parse(key);
            HELD.put(key, new SoftReference<>(answer));
            parsed++;
        } else {
            reused++;
        }
        if (answer.refused() instanceof RuntimeException e) {
            throw e;
        }
        if (answer.refused() instanceof Error e) {
            throw e;
        }
        return answer.parse();
    }

    private static Answer parse(Key key) {
        PureModelContextData model;
        try {
            model = key.sourceInformation()
                    ? REFEREE.parseModel(key.text())
                    : REFEREE.parseModel(key.text(), "", 0, 0, false);
        } catch (RuntimeException | Error t) {
            // parseModel declares no checked exception: what it throws is one of these two, kept as thrown
            return new Answer(null, t);
        }
        try {
            List<String> elements = new ArrayList<>();
            for (PackageableElement e : model.getElements()) {
                elements.add(MAPPER.writeValueAsString(e));
            }
            String whole = MAPPER.writeValueAsString(model);
            String joined = String.join(",", elements);
            int at = elements.isEmpty() ? -1 : whole.indexOf(joined);
            if (at < 0) {
                if (!elements.isEmpty()) {
                    keptWhole++;
                }
                return new Answer(new Parse(whole, null, null, List.copyOf(elements), null), null);
            }
            return new Answer(new Parse(null, whole.substring(0, at), whole.substring(at + joined.length()),
                    List.copyOf(elements), null), null);
        } catch (IOException unwritable) {
            // the mapper's JsonProcessingException, an IOException (jackson-core is not this library's own dependency)
            return new Answer(new Parse(null, null, null, null, unwritable), null);
        }
    }
}
