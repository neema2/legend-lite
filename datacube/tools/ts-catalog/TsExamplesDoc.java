// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package tscatalog;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE DOCUMENTS' EXAMPLES, WRITTEN FROM THEIR ONE SOURCE (docs/TS_V1_PROGRAM_2026_11_01.md A9): each {@code sql} block
 * of a #TS document marked {@code <!-- example: <id> -->} is rewritten as the example of that id in
 * docs/timeseries/examples.sql -- the file //core:ts_documents_test runs -- so a document shows what runs. Arguments:
 * the examples file, then each document and the file to write it to. An unknown id, or an example no document shows,
 * stops it.
 */
public final class TsExamplesDoc {

    private TsExamplesDoc() {
    }

    /** The examples of {@code file}, by id, in order: each the text after its {@code -- @example <id>} line. */
    static Map<String, String> examples(Path file) throws IOException {
        Map<String, String> out = new LinkedHashMap<>();
        String id = null;
        StringBuilder text = new StringBuilder();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.startsWith("-- @example ")) {
                if (id != null) {
                    out.put(id, text.toString().strip());
                }
                id = line.substring("-- @example ".length()).strip();
                text.setLength(0);
            } else if (id != null) {
                text.append(line).append('\n');
            }
        }
        if (id != null) {
            out.put(id, text.toString().strip());
        }
        return out;
    }

    public static void main(String[] args) throws IOException {
        Map<String, String> examples = examples(Path.of(args[0]));
        Map<String, Boolean> shown = new LinkedHashMap<>();
        examples.keySet().forEach(k -> shown.put(k, false));
        Pattern block = Pattern.compile("<!-- example: ([A-Za-z0-9_-]+) -->\\n```sql\\n.*?```", Pattern.DOTALL);
        for (int i = 1; i + 1 < args.length; i += 2) {
            String doc = Files.readString(Path.of(args[i]), StandardCharsets.UTF_8);
            Matcher m = block.matcher(doc);
            StringBuilder out = new StringBuilder();
            while (m.find()) {
                String id = m.group(1);
                String text = examples.get(id);
                if (text == null) {
                    throw new IllegalStateException(args[i] + " shows example " + id + ", which "
                            + args[0] + " has not");
                }
                shown.put(id, true);
                m.appendReplacement(out, Matcher.quoteReplacement("<!-- example: " + id + " -->\n```sql\n" + text
                        + "\n```"));
            }
            m.appendTail(out);
            Files.writeString(Path.of(args[i + 1]), out.toString(), StandardCharsets.UTF_8);
        }
        for (Map.Entry<String, Boolean> e : shown.entrySet()) {
            if (!e.getValue()) {
                throw new IllegalStateException("example " + e.getKey() + " of " + args[0] + " is in no document");
            }
        }
    }
}
