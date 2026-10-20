// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link Yaml}: each YAML construct read as the JSON it is (each expectation written as JSON and read by
 * {@link Json#parse}), YAML 1.2's core schema where it differs from 1.1's, and what is refused, with its line and
 * column. The cases were checked against PyYAML 6.0.3 (2026-10-20), and agree with it but where YAML 1.1 and 1.2
 * differ (`on`, `1e3`, a timestamp).
 */
@DisplayName("Yaml -- a YAML document read as JSON")
class YamlTest {

    private static void reads(String yaml, String json) {
        assertEquals(Json.parse(json.replace('`', '"')), Yaml.parse(yaml));
    }

    private static void refused(String yaml, String message) {
        assertEquals(message, assertThrows(IllegalArgumentException.class, () -> Yaml.parse(yaml)).getMessage());
    }

    @Test
    @DisplayName("block mappings and sequences, a sequence under a key at the key's indentation, nested sequences")
    void blockCollections() {
        reads("a: 1\nb:\n  c: x\n  d: [1, 2]\n", "{`a`: 1, `b`: {`c`: `x`, `d`: [1, 2]}}");
        reads("required:\n- id\n- name\nnext: 1\n", "{`required`: [`id`, `name`], `next`: 1}");
        reads("- - a\n  - b\n- - c\n-\n  - d\n", "[[`a`, `b`], [`c`], [`d`]]");
        reads("- name: x\n  in: query\n  schema:\n    type: string\n- name: y\n",
                "[{`name`: `x`, `in`: `query`, `schema`: {`type`: `string`}}, {`name`: `y`}]");
        reads("a:\nb: ~\nc: null\nd: ''\ne:\n  -\n  -\n", "{`a`: null, `b`: null, `c`: null, `d`: ``, `e`: [null, null]}");
    }

    @Test
    @DisplayName("plain, single- and double-quoted scalars, each folded across lines as YAML folds it")
    void scalars() {
        reads("description: This is a long\n  description that wraps\n\n  with a paragraph.\nnext: x\n",
                "{`description`: `This is a long description that wraps\\nwith a paragraph.`, `next`: `x`}");
        reads("a: \"first\n  second\n\n  third\"\nb: 'it''s\n  folded'\n",
                "{`a`: `first second\\nthird`, `b`: `it's folded`}");
        reads("a: \"tab\\there\\nnew \\u00e9 \\x41 \\\"q\\\" \\\\ \\/\"\nb: \"joined\\\n   line\"\n",
                "{`a`: `tab\\there\\nnew \u00e9 A \\`q\\` \\\\ /`, `b`: `joinedline`}");
        reads("url: https://example.com/a#frag\ncolon: a:b\nhash: a#b\ndash: -x\nq: what?\nc: a # comment\n",
                "{`url`: `https://example.com/a#frag`, `colon`: `a:b`, `hash`: `a#b`, `dash`: `-x`, `q`: `what?`, "
                        + "`c`: `a`}");
        reads("name: R\u00e9sum\u00e9\nemoji: \"\\U0001F600\"\n", "{`name`: `R\u00e9sum\u00e9`, `emoji`: `\uD83D\uDE00`}");
        // only a plain scalar's first line cannot start with an indicator (OpenAPI descriptions, 2026-10-20)
        reads("a: one\n  `owner` role\n  [RFC](x) and\n  \"quoted\" - and @, %, !, ? and\n  - a dash\nb: 1\n",
                "{`a`: `one \\u0060owner\\u0060 role [RFC](x) and \\`quoted\\` - and @, %, !, ? and - a dash`, `b`: 1}");
        // an escaped space before a line break is the scalar's; the spaces written before the break are not
        reads("a: \"x:\\ \n\n  - y\\t  \n  z\"\n", "{`a`: `x: \\n- y\\t z`}");
    }

    @Test
    @DisplayName("literal and folded block scalars, with their chomping and indentation indicators")
    void blockScalars() {
        reads("a: |\n  x\n  y\n\n", "{`a`: `x\\ny\\n`}");
        reads("a: |-\n  line1\n\n  line3\n\nb: 1\n", "{`a`: `line1\\n\\nline3`, `b`: 1}");
        reads("a: |+\n  line1\n  line2\n\n\nb: 1\n", "{`a`: `line1\\nline2\\n\\n\\n`, `b`: 1}");
        reads("a: >\n  one\n  two\n\n  three\n    indented\n  four\nb: 2\n",
                "{`a`: `one two\\nthree\\n  indented\\nfour\\n`, `b`: 2}");
        reads("d: >-\n  The pet's\n  status.\n", "{`d`: `The pet's status.`}");
        reads("a: |2\n    two extra\n  base\n", "{`a`: `  two extra\\nbase\\n`}");
        reads("--- |\n  text\n", "`text\\n`");
        // a line of more spaces than the indentation is text: the spaces beyond it
        reads("a: |-\n  one\n   \n  two\nb: 1\n", "{`a`: `one\\n \\ntwo`, `b`: 1}");
    }

    @Test
    @DisplayName("flow mappings and sequences, across lines, JSON's own spelling, a single pair")
    void flowCollections() {
        reads("enum: [placed,\n  approved,   # a comment\n  delivered]\nobj: {a: 1,\n  b: [x, y], 'c d': \"e\"}\n",
                "{`enum`: [`placed`, `approved`, `delivered`], `obj`: {`a`: 1, `b`: [`x`, `y`], `c d`: `e`}}");
        reads("{\"a\":1, \"b\": [true, null, 1.5, \"x\"], \"c\": {\"d\": -2}}\n",
                "{`a`: 1, `b`: [true, null, 1.5, `x`], `c`: {`d`: -2}}");
        reads("- [a, b]\n- {k: v}\n- [ ]\n- { }\n", "[[`a`, `b`], {`k`: `v`}, [], {}]");
        reads("[a: 1, b]\n", "[{`a`: 1}, `b`]");
    }

    @Test
    @DisplayName("anchors, aliases and merge keys: a mapping's own keys over the merged, the first merged over the later")
    void anchorsAndMerges() {
        reads("base: &b\n  x: 1\n  y: 2\nderived:\n  <<: *b\n  y: 3\n  z: 4\nlist:\n  - *b\n",
                "{`base`: {`x`: 1, `y`: 2}, `derived`: {`y`: 3, `z`: 4, `x`: 1}, `list`: [{`x`: 1, `y`: 2}]}");
        reads("a: &a {p: 1}\nb: &b {p: 2, q: 2}\nc:\n  <<: [*a, *b]\n  r: 3\n",
                "{`a`: {`p`: 1}, `b`: {`p`: 2, `q`: 2}, `c`: {`r`: 3, `p`: 1, `q`: 2}}");
    }

    @Test
    @DisplayName("YAML 1.2's core schema: null, booleans, integers, floats, else text -- 1.1's yes, on, 012 are not 1.2's")
    void theCoreSchema() {
        reads("i: 42\nn: -17\nh: 0x1F\no: 0o17\nf: 3.25\ne: 1e3\nd: .5\nbig: 123456789012345678901234567890\n",
                "{`i`: 42, `n`: -17, `h`: 31, `o`: 15, `f`: 3.25, `e`: 1e3, `d`: 0.5, "
                        + "`big`: 123456789012345678901234567890}");
        reads("v: 1.0.27\nt: 'true'\nu: '123'\nw: \"null\"\non: yes\nx: 2001-12-14\nz: 012\n",
                "{`v`: `1.0.27`, `t`: `true`, `u`: `123`, `w`: `null`, `on`: `yes`, `x`: `2001-12-14`, `z`: 12}");
        reads("'200':\n  description: ok\n\"404\": {description: missing}\n/pets/{id}:\n  get: {}\n$ref: '#/a'\n201: x\n",
                "{`200`: {`description`: `ok`}, `404`: {`description`: `missing`}, `/pets/{id}`: {`get`: {}}, "
                        + "`$ref`: `#/a`, `201`: `x`}");
        reads("a: !!str 123\nb: !!int '7'\nc: !!float 1\n", "{`a`: `123`, `b`: 7, `c`: 1}");
    }

    @Test
    @DisplayName("comments, document markers, a directive, Windows line ends")
    void around() {
        reads("# top\na: 1 # trailing\n# middle\nb: # before nested\n  c: 2\n", "{`a`: 1, `b`: {`c`: 2}}");
        reads("%YAML 1.2\n---\na: 1\n...\n", "{`a`: 1}");
        reads("a: 1\r\nb:\r\n  - x\r\n", "{`a`: 1, `b`: [`x`]}");
        reads("", "null");
    }

    @Test
    @DisplayName("what is not read is refused, with its line and column")
    void refusals() {
        refused("a: 1\n---\nb: 2\n", "YAML line 2, column 1: a second document: one document is read");
        refused("? a\n: 1\n", "YAML line 1, column 1: a complex key ('?') is not read");
        refused("a: 1\na: 2\n", "YAML line 2, column 2: the key 'a' twice in one mapping");
        refused("a: *nope\n", "YAML line 1, column 9: an alias to no anchor: *nope");
        refused("a: !custom x\n", "YAML line 1, column 13: the tag !custom is not the core schema's");
        refused("a:\n\tb: 1\n", "YAML line 2, column 1: a tab in indentation");
        refused("a: .inf\n", "YAML line 1, column 8: the number .inf, which JSON cannot hold");
        refused("a: 1\n  b: 2\n", "YAML line 2, column 4: bad indentation: a key more indented than its mapping, "
                + "under a scalar value");
        refused("a: \"x\" y\n", "YAML line 1, column 8: more after a value on its line");
        refused("a: \"open\n", "YAML line 1, column 4: a double-quoted scalar is not closed");
        refused("a: [1, 2\n", "YAML line 2, column 1: ',' or ']' in a flow sequence");
        refused("[a, b]: 1\n", "YAML line 1, column 7: a collection as a key is not read");
        assertEquals("YAML line 1, column 512: nesting deeper than 512", assertThrows(IllegalArgumentException.class,
                () -> Yaml.parse("[".repeat(600))).getMessage());
    }
}
