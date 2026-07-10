package com.legend.compiler.spec;

import com.legend.compiler.ModelBuilder;
import com.legend.compiler.NameResolver;
import com.legend.compiler.element.PureModelContext;
import com.legend.normalizer.ModelNormalizer;
import com.legend.parser.ElementParser;
import com.legend.parser.NormalizedModel;
import com.legend.parser.ParsedModel;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * PHASE H, MILESTONE H0 — the census harness (docs/PHASE_H_PLAN.md).
 *
 * <p>Eagerly compiles EVERY synthesized function ({@code $}-sigil FQNs) of a
 * fixture battery covering the mapping feature checklist, and buckets the
 * Phase-G failures. The census state is PINNED (the golden-catalog
 * pattern): every H1 fix deliberately updates the pin — a ratchet that
 * only moves toward green. At H1's exit the pin collapses to
 * {@code green == total} and becomes the standing invariant "every
 * synthesized mapping body type-checks".
 */
public class PhaseHCensusTest {

    /** SHARED battery: the resolver's ClassSourceTest extracts over the same fixtures. */
    public static final Map<String, String> FIXTURES = new LinkedHashMap<>();

    static {
        FIXTURES.put("A1 simple columns", """
            Class m::Person { name: String[1]; age: Integer[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50), AGE INTEGER) )
            Mapping m::M ( *m::Person: Relational { ~mainTable [s::DB] T
              name: T.NAME, age: T.AGE } )
            """);
        FIXTURES.put("A2 join chain property", """
            Class m::Person { name: String[1]; firmName: String[1]; }
            Database s::DB (
              Table P (NAME VARCHAR(50), FID INTEGER)
              Table F (ID INTEGER, LEGAL VARCHAR(50))
              Join PF (P.FID = F.ID) )
            Mapping m::M ( *m::Person: Relational { ~mainTable [s::DB] P
              name: P.NAME, firmName: @PF | F.LEGAL } )
            """);
        FIXTURES.put("A3 dynafunction", """
            Class m::P { full: String[1]; up: String[1]; digest: String[1]; }
            Database s::DB ( Table T (A VARCHAR(10), B VARCHAR(10)) )
            Mapping m::M ( *m::P: Relational { ~mainTable [s::DB] T
              full: concat(T.A, ' ', T.B), up: toUpper(T.A), digest: md5(T.B) } )
            """);
        FIXTURES.put("A5 enum mapping", """
            Enum m::Status { ACTIVE, INACTIVE }
            Class m::P { status: m::Status[1]; }
            Database s::DB ( Table T (CODE VARCHAR(10)) )
            Mapping m::M (
              m::Status: EnumerationMapping StatusM { ACTIVE: 'A', INACTIVE: 'I' }
              *m::P: Relational { ~mainTable [s::DB] T
                status: EnumerationMapping StatusM: T.CODE } )
            """);
        FIXTURES.put("A6 embedded", """
            Class m::P { name: String[1]; addr: m::Addr[1]; }
            Class m::Addr { city: String[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50), CITY VARCHAR(50)) )
            Mapping m::M ( *m::P: Relational { ~mainTable [s::DB] T
              name: T.NAME, addr( city: T.CITY ) } )
            """);
        FIXTURES.put("B2 mapping filter", """
            Class m::P { name: String[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50), ACTIVE INTEGER)
              Filter ActiveF ( T.ACTIVE = 1 ) )
            Mapping m::M ( *m::P: Relational { ~filter [s::DB] ActiveF
              ~mainTable [s::DB] T name: T.NAME } )
            """);
        FIXTURES.put("B4+B5 distinct groupBy", """
            Class m::P { name: String[1]; total: Integer[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50), AMT INTEGER) )
            Mapping m::M ( *m::P: Relational { ~distinct
              ~mainTable [s::DB] T name: T.NAME, total: T.AMT } )
            """);
        FIXTURES.put("B5 groupBy agg", """
            Class m::P { name: String[1]; total: Integer[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50), AMT INTEGER) )
            Mapping m::M ( *m::P: Relational { ~groupBy([s::DB] T.NAME)
              ~mainTable [s::DB] T name: T.NAME, total: sum(T.AMT) } )
            """);
        FIXTURES.put("C association", """
            Class m::Person { name: String[1]; }
            Class m::Firm { legal: String[1]; }
            Association m::Emp { employer: m::Firm[1]; staff: m::Person[*]; }
            Database s::DB (
              Table P (NAME VARCHAR(50), FID INTEGER)
              Table F (ID INTEGER, LEGAL VARCHAR(50))
              Join PF (P.FID = F.ID) )
            Mapping m::M (
              *m::Person: Relational { ~mainTable [s::DB] P name: P.NAME }
              *m::Firm: Relational { ~mainTable [s::DB] F legal: F.LEGAL }
              m::Emp: Relational { AssociationMapping (
                employer: [s::DB] @PF, staff: [s::DB] @PF ) } )
            """);
        FIXTURES.put("view-backed", """
            Class m::P { name: String[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50), ACTIVE INTEGER)
              Filter ActiveF ( T.ACTIVE = 1 )
              View V ( ~filter ActiveF vname: T.NAME ) )
            Mapping m::M ( *m::P: Relational { ~mainTable [s::DB] V
              name: V.vname } )
            """);
        FIXTURES.put("derived property", """
            Class m::P { first: String[1]; last: String[1];
              full() { $this.first + ' ' + $this.last }: String[1]; }
            Database s::DB ( Table T (F VARCHAR(20), L VARCHAR(20)) )
            Mapping m::M ( *m::P: Relational { ~mainTable [s::DB] T
              first: T.F, last: T.L } )
            """);
    }

    @Test
    void censusOfSynthesizedBodies() {
        Map<String, Integer> buckets = new TreeMap<>();
        Map<String, String> sample = new TreeMap<>();
        int total = 0;
        int green = 0;
        for (var fx : FIXTURES.entrySet()) {
            ParsedModel parsed;
            NormalizedModel normalized;
            PureModelContext ctx;
            try {
                parsed = ElementParser.parse(fx.getValue());
                normalized = ModelNormalizer.normalize(NameResolver.resolve(parsed));
                ctx = PureModelContext.from(normalized);
            } catch (Exception e) {
                bucket(buckets, sample, "[MODEL] " + shape(e), fx.getKey() + ": " + e.getMessage());
                continue;
            }
            ModelBuilder mb = ModelBuilder.from(new ParsedModel(
                    normalized.elements(), normalized.imports()));
            SpecCompiler spec = new SpecCompiler(ctx);
            var synth = mb.functions()
                    .filter(f -> f.qualifiedName().contains("$"))
                    .toList();
            for (var fn : synth) {
                total++;
                try {
                    var typed = ctx.findFunction(fn.qualifiedName());
                    spec.compile(typed.get(0));
                    green++;
                } catch (Exception e) {
                    bucket(buckets, sample, shape(e),
                            fx.getKey() + " / " + fn.qualifiedName() + ": " + e.getMessage());
                }
            }
        }
        System.out.println("=== PHASE H CENSUS: " + green + "/" + total
                + " synthesized bodies type-check ===");
        buckets.forEach((k, v) -> {
            System.out.println(String.format("%4d  %s", v, k));
            System.out.println("      e.g. " + sample.get(k));
        });

        // H1 EXIT (2026-07-09): the ratchet reached 14/14 and collapses to
        // the permanent invariant — EVERY synthesized mapping body
        // type-checks. New fixtures are welcome (total may grow); a red body
        // is a Phase-G regression, full stop.
        org.junit.jupiter.api.Assertions.assertEquals(total, green,
                "a synthesized mapping body no longer type-checks — Phase-G"
                        + " regression: " + sample.values());
    }

    private static void bucket(Map<String, Integer> b, Map<String, String> s,
                               String key, String example) {
        b.merge(key, 1, Integer::sum);
        s.putIfAbsent(key, example);
    }

    /** Failure shape: exception type + the message's leading words, numbers stripped. */
    private static String shape(Exception e) {
        String m = String.valueOf(e.getMessage()).replaceAll("'[^']*'", "'_'");
        return e.getClass().getSimpleName() + ": "
                + m.substring(0, Math.min(80, m.length()));
    }
}
