// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * THE SEED of the live protocol differential (upstream boundary batch 6): the
 * sources whose wire JSON core used to pin as hand-copied strings — captured
 * 2026-08-04 from engine 4.133.0 through {@code ProbeWireShapes} and
 * re-derived by nothing — compared LIVE against the oracle, byte for byte,
 * element by element, on every run. Exact: no ledger, no floor — a DIFF here
 * is a bug in the emitter or a real upstream change, either way red.
 *
 * <p>Each source carries the wire fact its golden documented (verified, not
 * assumed): the conventions are now held by the comparison itself.
 */
class ProtocolSeedParityTest {

    record Seed(String name, String facts, String source) {
    }

    static final List<Seed> SEEDS = List.of(
            new Seed("simple class", "a property's genericType/multiplicity/sourceInformation shape; the sectionIndex "
                    + "element with its importAware section", """
                    Class model::Person
                    {
                      name: String[1];
                    }
                    """),
            new Seed("supertype", "superTypes entries carry no _type, fields alphabetical, the span covers the name", """
                    Class a::B extends a::C
                    {
                    }
                    """),
            new Seed("generic property", "a generic's rawType span covers the whole application including the closing >; "
                    + "each type argument is a full nested genericType with its own span", """
                    Class a::C
                    {
                      p: a::D<String>[1];
                    }
                    """),
            new Seed("nested generic", "nested generics with multiple arguments", """
                    Class b::C
                    {
                      p: b::E<String, b::D<Integer>>[0..1];
                    }
                    """),
            new Seed("generic supertype", "a GENERIC supertype emits only the base path; the span covers the whole expression", """
                    Class c::C extends c::D<String>
                    {
                    }
                    """),
            new Seed("default values", "defaultValue is alphabetically first among a property's fields; its span equals the "
                    + "literal's; a string literal's span includes the quotes", """
                    Class d::C
                    {
                      flag: Boolean[1] = false;
                      n: Integer[1] = 42;
                      s: String[1] = 'x';
                    }
                    """),
            new Seed("simple and extended constraints", "the constraint lambda's synthesised $this with no span; "
                    + "~function/~message", """
                    Class f::C
                    [
                      c1: $this.n > 1,
                      c2
                      (
                        ~function: $this.n < 10
                        ~message: 'n is ' + $this.n->toString()
                      )
                    ]
                    {
                      n: Integer[1];
                    }
                    """),
            new Seed("operator zoo", "span families: infix arithmetic and comparisons op..RHS-end; equal/and/or the operator "
                    + "token only; ! bang..operand-end; plus/minus/times N-ARY over one collection; != is not(equal())", """
                    Class h::C
                    [
                      cMinus: $this.n - 1 > 0,
                      cTimes: $this.n * 2 > 0,
                      cDiv: $this.n / 2 > 0,
                      cEq: $this.n == 1,
                      cNeq: $this.n != 1,
                      cAnd: ($this.n > 1) && ($this.n < 9),
                      cOr: ($this.n > 1) || ($this.n < 9),
                      cNot: !($this.n > 1),
                      cChain: $this.s + 'a' + 'b',
                      cCall: $this.s->startsWith('a'),
                      cColl: $this.n->in([1, 2, 3])
                    ]
                    {
                      n: Integer[1];
                      s: String[1];
                    }
                    """),
            new Seed("mixed chain", "== after an operand binds to THAT operand: s + 'a' + 'b' == 'x' is "
                    + "plus([s,'a',equal('b','x')]) on both sides", """
                    Class g::C
                    [
                      cChain: $this.s + 'a' + 'b' == 'x'
                    ]
                    {
                      s: String[1];
                    }
                    """),
            new Seed("parenthesised chain comparison", "equal over a parenthesised arithmetic chain: the inner plus keeps "
                    + "its operator-run span, the paren is only a flatten boundary", """
                    function q::parenEq(n: Integer[1]): Boolean[1]
                    {
                      ($n + 1) == 2;
                    }
                    """),
            new Seed("class refs, enum values, lambdas, floats, unary minus, enforcement level", ".all() desugars to "
                    + "getAll spanning DOT..close-paren; an enum value is a property on a packageableElementPtr; an "
                    + "untyped lambda parameter is the bare var; unary minus is a one-parameter func; ~enforcementLevel "
                    + "sorts first", """
                    Class j::C
                    [
                      cPtr: j::C.all()->size() > 0,
                      cEnum: $this.st == j::St.UP,
                      cLambda: $this.xs->exists(x|$x > 1),
                      cLambda2: $this.xs->forAll(x: Integer[1]|$x > 1),
                      cFloat: $this.f > 1.5,
                      cNeg: $this.n > -2,
                      cLevel
                      (
                        ~function: $this.n > 3
                        ~enforcementLevel: Warn
                      )
                    ]
                    {
                      n: Integer[1];
                      f: Float[1];
                      st: j::St[1];
                      xs: Integer[*];
                    }
                    Enum j::St
                    {
                      UP, DOWN
                    }
                    """),
            new Seed("braced lambda, property call, date, qualified property", "braced lambdas span open-brace..body-end; "
                    + "a dot-spelled property CALL spans the name token only; %2020-01-01 is strictDate; qualified "
                    + "properties emit bare bodies with typed-var parameters", """
                    Class k::C
                    [
                      cBrace: $this.xs->map({y | $y + 1})->size() > 0,
                      cPcall: $this.tag('x') == 'y',
                      cDate: $this.d > %2020-01-01
                    ]
                    {
                      n: Integer[1];
                      d: Date[1];
                      xs: Integer[*];
                      tag(s: String[1]) {$s + $this.n->toString()}: String[1];
                    }
                    """),
            new Seed("constraint owner", "~owner is a single identifier on the wire, between name and sourceInformation", """
                    Class k2::C
                    [
                      cOwn
                      (
                        ~owner: Finance
                        ~function: $this.n > 0
                      )
                    ]
                    {
                      n: Integer[1];
                    }
                    """),
            new Seed("enumeration with annotations", "_type is 'Enumeration' (capitalised); per-value annotations ride each "
                    + "entry, whose span covers annotations..value name", """
                    Profile k::P
                    {
                      stereotypes: [s1, s2];
                      tags: [doc, todo];
                    }
                    Enum <<k::P.s1>> {k::P.doc = 'an enum'} k::E
                    {
                      <<k::P.s1>> {k::P.doc = 'up'} UP,
                      DOWN
                    }
                    """),
            new Seed("association with annotations", "association ends are ordinary wire properties", """
                    Profile k::P
                    {
                      stereotypes: [s1, s2];
                      tags: [doc, todo];
                    }
                    Association <<k::P.s1>> k::A
                    {
                      x: k::X[1];
                      ys: k::Y[*];
                    }
                    """),
            new Seed("function name mangling", "wire names are signature-mangled: simple type names, multiplicities as "
                    + "1/MANY/$0_1$/$1_MANY$/n, params joined by __, return appended with a trailing _; zero params "
                    + "collapse to name__Return", """
                    function k::g(a: Integer[0..1], b: String[1..*], c: k::X[2], d: Date[*]): String[0..1]
                    {
                      'x';
                    }
                    function k::h(): Boolean[1]
                    {
                      true;
                    }
                    function k::i(xs: k::List<k::X>[1]): Integer[1]
                    {
                      1;
                    }
                    """));

    @Test
    @DisplayName("every seed source emits the oracle's bytes, element by element — exact, no ledger")
    void seedsMatchTheOracleExactly() {
        ParserEquivalence eq = new ParserEquivalence();
        List<String> bad = new ArrayList<>();
        int matched = 0;
        for (Seed s : SEEDS) {
            for (ParserEquivalence.Verdict v : eq.compare(new Corpus.Source("seed:" + s.name(), s.source(), "seed"))) {
                if (v.kind() == ParserEquivalence.Kind.MATCH) {
                    matched++;
                } else {
                    bad.add(s.name() + " " + v.element() + " " + v.kind() + ": " + v.detail() + " [" + s.facts() + "]");
                }
            }
        }
        assertEquals(List.of(), bad, "protocol seeds diverged from the oracle");
        assertTrue(matched >= SEEDS.size(), "fewer matched elements than seeds: " + matched);
    }
}
