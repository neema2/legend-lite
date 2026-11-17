package com.legend.protocol;

import com.legend.Compiler;
import com.legend.json.Json;
import com.legend.parser.PmcdParser;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A column spec's stereotypes and tagged values print where they were written, leading the spec
 * ({@code ~[<<p.s>> {p.t = 'v'} name:x|...]}), as legend-engine 4.150.0 prints them (#5245, {@code d455688a9f}:
 * {@code HelperValueSpecificationGrammarComposer.printColSpec} writes {@code renderAnnotations} first). The pinned 4.145.0
 * parses them and carries them in its protocol, and prints none; lite prints them, built ahead of the pin on the
 * user's ruling of 2026-11-16 ("Build ahead, ledgered"; docs/SEMANTICS_REGISTER.md S421). Until then the Studio
 * roundtrip's CORE-relation-expression-syntax printed back without them.
 */
@DisplayName("a column spec's annotations print")
class ColSpecAnnotationsPrintTest {

    /** The Studio case's two annotated functions and a single annotated spec, as upstream 4.150.0 prints them. */
    private static final String PRINTED = """
            ###Pure
            Profile model::ColumnProfile
            {
              stereotypes: [important, deprecated];
              tags: [doc];
            }

            Class model::Person
            {
              name: String[1];
              age: Integer[1];
            }

            function model::annotatedColSpecs(): meta::pure::metamodel::relation::Relation<(grp:String, total:Integer)>[1]
            {
              #>{store::PersonDB.PersonTable}#->extend(~[<<model::ColumnProfile.important>> {model::ColumnProfile.doc = 'derived column'} 'Display Name':x|$x.'first name'->toOne()])->select(~[<<model::ColumnProfile.important>> {model::ColumnProfile.doc = 'kept column'} 'Display Name', <<model::ColumnProfile.deprecated>> grp, amount])->groupBy(~[<<model::ColumnProfile.important>> grp], ~[{model::ColumnProfile.doc = 'sum aggregate'} total:x|$x.amount:y|$y->plus()])
            }

            function model::projectAnnotated(): meta::pure::metamodel::relation::Relation<('Person Name':String, 'Person Age':Integer)>[1]
            {
              model::Person.all()->project(~[<<model::ColumnProfile.important>> {model::ColumnProfile.doc = 'name documentation'} 'Person Name':x|$x.name, 'Person Age':x|$x.age])
            }

            function model::singleAnnotated(): meta::pure::metamodel::relation::Relation<meta::pure::metamodel::type::Any>[1]
            {
              #>{store::PersonDB.PersonTable}#->extend(~<<model::ColumnProfile.important, model::ColumnProfile.deprecated>> {model::ColumnProfile.doc = 'one'} upper:x|$x.grp->toOne()->toUpper())
            }
            """;

    @Test
    @DisplayName("parsed, written as JSON, read back and printed: the annotations where they were written")
    void printsBackAsWritten() {
        String json = PmcdParser.parseDocument(PRINTED);
        assertEquals(PRINTED.stripTrailing(), ModelComposer.model(json, PureComposer.Style.STANDARD).stripTrailing());
    }

    @Test
    @DisplayName("in the pretty style too, and a doc tag stays an inline tagged value, never a documentation block")
    void prettyAndNoDocumentationBlock() {
        String pretty = ModelComposer.model(PmcdParser.parseDocument(PRINTED), PureComposer.Style.PRETTY);
        assertTrue(pretty.contains("<<model::ColumnProfile.important>> {model::ColumnProfile.doc = 'derived column'}"
                + " 'Display Name':"), pretty);
        assertTrue(pretty.contains("{model::ColumnProfile.doc = 'sum aggregate'} total:"), pretty);
        assertTrue(!pretty.contains("'''"), pretty);
    }

    @Test
    @DisplayName("an annotated bare spec spans its annotations and its name, as the engine's oneColSpec does")
    void anAnnotatedBareSpecSpansToItsName() {
        // the select's first spec, <<...>> {...} 'Display Name': columns 181 to 272 of the function's body line (the
        // referee's span; until 2026-11-16 lite's ended at 181, its first '<')
        Json.Obj pmcd = (Json.Obj) Json.parse(PmcdParser.parseDocument(PRINTED), new Json.Config(4096));
        Json.Obj function = (Json.Obj) ((Json.Arr) pmcd.fields().get("elements")).items().stream()
                .filter(e -> ((Json.Obj) e).getStringOr("name", "").startsWith("annotatedColSpecs")).findFirst()
                .orElseThrow();
        Json.Obj groupBy = (Json.Obj) ((Json.Arr) function.fields().get("body")).items().get(0);
        Json.Obj select = (Json.Obj) ((Json.Arr) groupBy.fields().get("parameters")).items().get(0);
        Json.Obj array = (Json.Obj) ((Json.Arr) select.fields().get("parameters")).items().get(1);
        Json.Obj spec = (Json.Obj) ((Json.Arr) ((Json.Obj) array.fields().get("value")).fields().get("colSpecs"))
                .items().get(0);
        Json.Obj span = (Json.Obj) spec.fields().get("sourceInformation");
        assertEquals("Display Name", spec.getStringOr("name", ""));
        assertEquals(181L, ((Json.Num) span.fields().get("startColumn")).longValue());
        assertEquals(272L, ((Json.Num) span.fields().get("endColumn")).longValue());
    }

    @Test
    @DisplayName("a rewrite of a spec's children keeps its annotations")
    void aRewriteKeepsThem() {
        LambdaFunction lambda = (LambdaFunction) Compiler.parseQuery("|$r->extend(~[<<p::P.s>> {p::P.t = 'v'} a:x|$x.b])");
        ColSpecArray array = (ColSpecArray) ((AppliedFunction) lambda.body().get(0)).parameters().get(1);
        ColSpec spec = array.colSpecs().get(0);
        assertEquals(1, spec.stereotypes().size());
        List<ValueSpecification> children = spec.children();
        ColSpec rebuilt = (ColSpec) spec.withChildren(children);
        assertEquals(spec.stereotypes(), rebuilt.stereotypes(), "the stereotypes kept");
        assertEquals(spec.taggedValues(), rebuilt.taggedValues(), "the tagged values kept");
    }
}
