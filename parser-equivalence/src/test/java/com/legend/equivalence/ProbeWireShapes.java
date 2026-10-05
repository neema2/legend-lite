package com.legend.equivalence;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.protocol.pure.m3.PackageableElement;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;

/**
 * NOT a gate — an instrument. Prints the reference parser's exact bytes for constructs the
 * emitter does not cover yet, so emit rules are written from observation, never inference
 * (the same discipline as ProtocolEmitterTest's pinned constants).
 *
 * Run on demand (Bazel workplan P3-17): bazel run //parser-equivalence:probe_wire_shapes [-- --out FILE]
 */
final class ProbeWireShapes {

    public static void main(String[] args) throws Exception {
        com.legend.testing.Programs.stdoutToOut(args);
        new ProbeWireShapes().probe();
    }


    private final ObjectMapper mapper =
            ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports();
    private final PureGrammarParser parser = PureGrammarParser.newInstance();

    private void dump(String label, String code) throws Exception {
        System.out.println("======== " + label + " ========");
        System.out.println(code);
        System.out.println("--------");
        try {
            PureModelContextData pmcd = parser.parseModel(code);
            for (PackageableElement e : pmcd.getElements()) {
                if (!"SectionIndex".equals(e.name)) {
                    System.out.println(mapper.writeValueAsString(e));
                }
            }
        } catch (Exception e) {
            // a rejected probe input is itself data — record it and keep dumping
            System.out.println("ENGINE-REJECTED: " + String.valueOf(e.getMessage()).replaceAll("\\s+", " "));
        }
        System.out.println();
    }

    void probe() throws Exception {
        dump("generic property type", """
                Class a::C
                {
                  p: a::D<String>[1];
                }
                """);
        dump("nested generic + multiple args", """
                Class b::C
                {
                  p: b::E<String, b::D<Integer>>[0..1];
                }
                """);
        dump("generic supertype", """
                Class c::C extends c::D<String>
                {
                }
                """);
        dump("default value", """
                Class d::C
                {
                  flag: Boolean[1] = false;
                  n: Integer[1] = 42;
                  s: String[1] = 'x';
                }
                """);
        dump("qualified property", """
                Class e::C
                {
                  first: String[1];
                  greet() {'hi ' + $this.first}: String[1];
                  greetN(n: Integer[1]) {$this.first + $n->toString()}: String[1];
                }
                """);
        dump("operator zoo", """
                Class g::C
                [
                  cMinus: $this.n - 1 > 0,
                  cTimes: $this.n * 2 > 0,
                  cDiv: $this.n / 2 > 0,
                  cEq: $this.n == 1,
                  cNeq: $this.n != 1,
                  cAnd: ($this.n > 1) && ($this.n < 9),
                  cOr: ($this.n > 1) || ($this.n < 9),
                  cNot: !($this.n > 1),
                  cChain: $this.s + 'a' + 'b' == 'x',
                  cCall: $this.s->startsWith('a'),
                  cColl: $this.n->in([1, 2, 3])
                ]
                {
                  n: Integer[1];
                  s: String[1];
                }
                """);
        dump("operator zoo emittable", """
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
                """);
        dump("ptr enum lambda float minus level", """
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
                """);
        dump("brace pcall date", """
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
                """);
        dump("owner", """
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
                """);
        dump("let zoo", """
                Class m::C
                {
                  q()
                  {
                    let a = 5;
                    let s = 'x';
                    let v = $a;
                    let col = [1, 2];
                    let p = $this.n;
                    let arith = $a + 1;
                    let cmp = $a > 1;
                    let eq = $a == 1;
                    let call = $this.xs->filter(e|$e > 1);
                    let neg = !($a > 1);
                    $a;
                  }: Integer[1];
                  n: Integer[1];
                  xs: Integer[*];
                }
                """);
        dump("enumeration", """
                Enum <<k::P.s1>> {k::P.doc = 'an enum'} k::E
                {
                  <<k::P.s1>> {k::P.doc = 'up'} UP,
                  DOWN
                }
                """);
        dump("profile", """
                Profile k::P
                {
                  stereotypes: [s1, s2];
                  tags: [doc, todo];
                }
                """);
        dump("association", """
                Association <<k::P.s1>> k::A
                {
                  x: k::X[1];
                  ys: k::Y[*];
                }
                """);
        dump("function", """
                function <<k::P.s1>> {k::P.doc = 'fn'} k::f(a: Integer[1], b: String[*]): Integer[1]
                {
                  let c = $a + 1;
                  $c * 2;
                }
                """);
        dump("native function", """
                native function k::n(a: Integer[1]): Integer[1];
                """);
        dump("function mangling", """
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
                """);
        dump("precedence zoo", """
                function p::z(): Any[*]
                {
                  2 * 4 + 2;
                  2 + 2 * 4;
                  [1, 2]->map(x|abs($x));
                }
                """);
        dump("burn zoo", """
                function q::newInst(): Any[*]
                {
                  ^q::X(a=1, b='x');
                }
                function q::casts(v: Any[1]): Any[*]
                {
                  $v->cast(@Integer);
                  $v->cast(@q::X);
                }
                function q::dates(): Any[*]
                {
                  %2020;
                  %2020-01;
                  %2020-01-01T10;
                  %2020-01-01T10:20:30;
                  %2020-01-01T10:20:30.123;
                }
                function q::latest(): Any[*]
                {
                  q::X.all(%latest);
                }
                function q::dec(): Any[*]
                {
                  3.14d;
                }
                function q::parenEq(n: Integer[1]): Boolean[1]
                {
                  ($n + 1) == 2;
                }
                Class q::R
                {
                  p: q::Res<Integer|1>[1];
                }
                """);
        dump("path and cols", """
                function q2::path(): Any[*]
                {
                  #/q2::X/prop#;
                }
                function q2::cols(): Any[*]
                {
                  [1, 2]->cast(@meta::pure::metamodel::relation::Relation<(a:Integer)>)->select(~a);
                  [1, 2]->cast(@meta::pure::metamodel::relation::Relation<(a:Integer, b:Integer)>)->select(~[a, b]);
                }
                """);
        dump("burn zoo 2", """
                function r::keyChain(v: String[1]): Any[*]
                {
                  ^r::Y(s='a' + 'b' + $v, n=1 + 2);
                }
                function r::extendCols(): Any[*]
                {
                  [1]->cast(@meta::pure::metamodel::relation::Relation<(a:Integer)>)->extend(~b: x|$x.a);
                  [1]->cast(@meta::pure::metamodel::relation::Relation<(a:Integer)>)->extend(~[c: x|$x.a, d: x|$x.a]);
                }
                function r::tref(): Any[*]
                {
                  #>{r::db.tbl}#->select();
                }
                """);
        dump("path offsets", """
                function s::p1(): Any[*]
                {
                  #/s::X/prop#;
                      #/s::LongerName/ab/cd#;
                  [1]->map(x|#/s::X/prop#);
                }
                """);
        dump("typed new and gft", """
                function s::tn(): Any[*]
                {
                  ^s::Res<Integer>(v=1);
                }
                function s::gft(): Any[*]
                {
                  #{s::X {a, b}}#;
                }
                """);
        dump("colspec fn spans", """
                function s::cf(): Any[*]
                {
                  [1]->cast(@meta::pure::metamodel::relation::Relation<(a:Integer)>)->extend(~b: y|$y.a);
                  [1]->cast(@meta::pure::metamodel::relation::Relation<(a:Integer)>)->extend(~[longName: yy|$yy.a + 1]);
                }
                """);
        dump("fn tests wire", """
                function s::t1(): String[1]
                {
                  'x';
                }
                {
                  testPass | t1() => 'x';
                }
                """);
        dump("colspec lambda span variants", """
                function u::v(): Any[*]
                {
                  [1]->cast(@meta::pure::metamodel::relation::Relation<(a:Integer)>)->extend(~b: y|$y.a);
                  [1]->cast(@meta::pure::metamodel::relation::Relation<(a:Integer)>)->extend(~c: y | $y.a);
                  [1]->cast(@meta::pure::metamodel::relation::Relation<(a:Integer)>)->extend(~[d: z|$z.a]);
                  [1]->cast(@meta::pure::metamodel::relation::Relation<(a:Integer)>)->extend(~[e: z | $z.a]);
                }
                """);
        dump("caret specials", """
                function w::a(): Any[*]
                {
                  ^meta::pure::functions::collection::Pair<String, String>(first = 'a', second = 'b');
                  ^meta::pure::tds::BasicColumnSpecification<meta::pure::tds::TDSRow>(func = {r|1}, name = 'n');
                }
                """);
        dump("alias dated tref2 gft2", """
                function x1::a(): Any[*]
                {
                  #/x1::X/prop!nick#;
                }
                function x1::b(): Any[*]
                {
                  #/x1::X/prop(%latest)#;
                }
                function x1::c(): Any[*]
                {
                  #>{x1::db.schema1.TBL}#->select();
                }
                function x1::d(): Any[*]
                {
                  #{x1::X {a, k {b}}}#;
                }
                """);
        dump("gft in let arg", """
                function y1::a(): Any[*]
                {
                  let t = z()->add(#{I {s, f}}#);
                }
                function y1::b(): Any[*]
                {
                  #{I {s}}#;
                }
                """);
        dump("gft as let value", """
                function y2::a(): Any[*]
                {
                  let t = #{I {s, f}}#;
                }
                """);
        dump("gft rich forms", """
                function y3::a(): Any[*]
                {
                  #{I {s, byName('x'){t}, pair(1,2), dated(%2015-10-16){u}, byVars($a, $b), noArg()}}#;
                }
                function y3::b(): Any[*]
                {
                  #{I {s, shapes->subType(@Circle){radius}}}#;
                }
                function y3::c(): Any[*]
                {
                  #{I {'nick' : s, 'other' : byName('x'){t}}}#;
                }
                """);
        dump("gft enum param", """
                function y4::a(): Any[*]
                {
                  #{I {s, id(BookIdentifierType.ISBN_10)}}#;
                }
                """);
        dump("gft bare date param", """
                function y4::b(): Any[*]
                {
                  #{I {product(2015-10-16){name}}}#;
                }
                """);
        dump("gft bare datetime param", """
                function y4::c(): Any[*]
                {
                  #{I {dt(2015-10-16T10:20:30){name}}}#;
                }
                """);
        dump("gft pct date param", """
                function y4::d(): Any[*]
                {
                  #{I {product(%2015-10-16T10:20:30){name}}}#;
                }
                """);
        dump("gft bool arg and comment", """
                function y5::a(): Any[*]
                {
                  #{I {
                     s,
                     // skipped,
                     f(false),
                     g(true)
                  }}#;
                }
                """);
        dump("gft collection args", """
                function y6::a(): Any[*]
                {
                  #{I {'x' : f([]), 'y' : g([En.A, En.B]), h([1, 2])}}#;
                }
                """);
        dump("time literal", """
                function y7::a(): Any[*]
                {
                  %10:10:10;
                  %10:10:10.283;
                }
                """);
        dump("path dated and enum args", """
                function y8::a(): Any[*]
                {
                  #/Order/biTemporalProduct(%2017-6-10, %2017-6-9)/id#;
                }
                function y8::b(): Any[*]
                {
                  #/Product/synonymsByType(ProductSynonymType.CUSIP)/value!cusip#;
                }
                """);
        dump("relation type sigs", """
                function y9::a(r: meta::pure::metamodel::relation::Relation<(col:String, col2:Integer)>[1]): Any[*]
                {
                  []
                }
                function y9::b(): meta::pure::metamodel::relation::Relation<(ID:Integer, 'FIRST NAME':String, SALARY:Float)>[1]
                {
                  []->cast(@meta::pure::metamodel::relation::Relation<(ID:Integer, 'FIRST NAME':String, SALARY:Float)>)->toOne()
                }
                """);
        dump("fn tests named suite", """
                function z1::a(name: String[1]): String[1]
                {
                  'x' + $name;
                }
                {
                  suite_1
                  (
                    testPass | a('John') => 'xJohn';
                    testFail | a('Jo') => 'xJo';
                  )
                }
                function z1::b(): String[1]
                {
                  'y';
                }
                {
                    t1 | b() => 'y';
                    t2 | b() => 'yy';
                }
                """);
        dump("agg kind and varchar", """
                Class z2::B { s: String[1]; }
                Association z2::A
                {
                  a: z2::B[1];
                  (shared) b: z2::B[1..*];
                }
                Class z2::C
                {
                  (composite) x: z2::B[1];
                }
                function z2::f(v: meta::pure::precisePrimitives::Varchar(200)[1]): String[*]
                {
                  'x';
                }
                """);
        dump("unit type refs", """
                function z3::a(): Any[*]
                {
                  newUnit(Mass~Kilogram, 5.5)->unitType();
                }
                """);
        dump("unary plus chain", """
                function z4::a(): Any[*]
                {
                  let s = +'a\\n'+'b\\n'+'c';
                  let n = +5;
                }
                """);
        dump("declared col mult and relation shape", """
                function z5::a(): Any[*]
                {
                  []->cast(@meta::pure::metamodel::relation::Relation<(ID:String[1], H:String[0..1])>);
                }
                function z5::b(): Any[*]
                {
                  []->cast(@meta::pure::metamodel::relation::Relation<(A:Integer[1])>)->extend(~n:x: (A:Integer[1])[1]|$x.A);
                }
                """);
        dump("simple relation cast", """
                function z6::a(): Any[*]
                {
                  []->cast(@Relation<(ID:String[1], H:String[0..1], K:Integer)>);
                }
                """);
        dump("unit sig and root package", """
                Measure z7::Mass
                {
                  *Gram: x -> $x;
                  Pound: x -> 453 * $x;
                }
                function z7::a(m: z7::Mass~Gram[1]): z7::Mass~Pound[1]
                {
                  $m->convert(z7::Mass~Pound)->toOne()
                }
                function z7::b(): Any[*]
                {
                  [1, 2]->fold({a, b|$a + $b}, ::);
                }
                """);
        dump("mixed bool arith", """
                Class z8::C
                [
                  $this.id > 0 && $this.id < 30
                ]
                {
                  id: Integer[1];
                }
                """);
        dump("unit literal", """
                Measure pkg::Mass
                {
                  *Gram: x -> $x;
                  Pound: x -> 453 * $x;
                }
                function testFunc():Mass~Pound[0..1]
                {
                   1 Mass~Pound;
                }
                """);
        dump("sig multarg", """
                function z9::b(res: Result<meta::pure::metamodel::type::Any|1..*>[1]): Any[1]
                {
                  1
                }
                """);
        dump("bare result type", """
                function z10::a(res: Result[1], other: List[1]): Any[1]
                {
                  1
                }
                """);
        dump("fn constraints", """
                function z11::a(x: Integer[1]): Integer[1]
                [
                  pre1: $x > 0,
                  pre2: $x < 100
                ]
                {
                  $x + 1;
                }
                """);
        dump("path in let", """
                function w1::a(): Any[*]
                {
                  let path = #/w1::Person/firstName#;
                }
                """);
        dump("gft root subtype", """
                function w2::a(): Any[*]
                {
                  #{
                    test::Address {
                      zipCode,
                      ->subType(@test::Street) {
                        street
                      },
                      ->subType(@test::City) {
                        'cityName' : name
                      }
                    }
                  }#
                }
                """);
        dump("path scalar args", """
                function w3::a(): Any[*]
                {
                  print(#/Person/nameWithTitle(1)#, 2);
                  print(#/Person/nameWithTitle('1')#, 2);
                  print(#/Person/nameWithPrefixAndSuffix('a', 'b')#, 2);
                }
                """);
        dump("typed colspec stmt", """
                function w4::a(): meta::pure::metamodel::relation::ColSpec<(name:String)>[1]
                {
                  ~name:String;
                }
                function w4::b(): meta::pure::metamodel::relation::ColSpecArray<(name:String, id:Integer)>[1]
                {
                  ~[name:String, id:Integer];
                }
                function w4::c(): meta::pure::metamodel::relation::ColSpec<(name:String)>[1]
                {
                  ~name:String[1];
                }
                """);
        dump("pf empty enum assoc", """
                Enum v1::A
                {
                }
                Association v1::OneEnd
                {
                   product : v1::P[1];
                }
                Class v1::P { name: String[1]; }
                """);
        dump("pf fmt expected and data", """
                function v2::MyFunc(firstName: String[1]): String[1]
                {
                  ''
                }
                {
                  testDuplicate | MyFunc('x') => (JSON) '[]';
                }
                """);
        dump("pf test store data", """
                function v3::Hello(name: String[1]): String[1]
                {
                  'Hello ' + $name
                }
                {
                  ModelStore: (JSON) '{}';
                  myTest | Hello('John') => 'Hello John!';
                }
                """);
        dump("pf relation expected", """
                function v4::MyFunc(): meta::pure::metamodel::relation::Relation<(id:Integer, name:String)>[1]
                {
                  []->cast(@meta::pure::metamodel::relation::Relation<(id:Integer, name:String)>)
                }
                {
                  testPass | MyFunc() => Relation
                  #{
                    id, name
                    1 , John;
                  }#;
                }
                """);
        dump("pf string tvv", """
                function v5::f(): meta::pure::precisePrimitives::Varchar(200)[0..1]
                {
                  []
                }
                function v5::g(x: Res<String>(1)[1]): Any[*]
                {
                  []
                }
                """);
        dump("pf path exotic", """
                function v6::a(): Any[*]
                {
                  print(#/Person/nameWithPrefixAndSuffix('a', [1, 2])#, 2);
                  print(#/Person/nameWithTitle()#, 2);
                  #/Person#;
                }
                """);
        dump("pf named new and store tref", """
                Class v7::Person { lastName: String[1]; }
                function v7::a(): Any[1]
                {
                  let a = ^v7::Person klp (lastName = 'hello');
                }
                function v7::b(): Any[*]
                {
                  #>{my::Store}#->filter(c|$c.val);
                }
                """);
        dump("pf quoted names", """
                Class test::'p a c k a g e'::A
                {
                  's t r i n g': String[1];
                }
                Enum v8::'my Enum'
                {
                  'Anything e',
                  DOWN
                }
                function v8::f(): Any[*]
                {
                  ^A('firstname' = 'ok');
                }
                Class v8::B { 'first name': String[1]; }
                """);
        dump("pf digit prop lenient time emptyargs", """
                Class v9::C
                {
                  4prop : Date[*];
                  time : StrictTime[1];
                  t(){ $this.time == %200:12:22.88; }: Boolean[1];
                }
                function v9::g(): v9::TestClass<|1>[1]
                {
                  ^v9::TestClass<|1>(names='one name');
                }
                """);
        dump("pf extra test arg", """
                function v10::MyFunc(): String[1]
                {
                  ''
                }
                {
                  testDuplicate | MyFunc('John') => 'x';
                }
                """);
        dump("pf colspec annotations", """
                function v11::t(): meta::pure::metamodel::relation::ColSpec<(name:String)>[1]
                {
                   ~<<meta::pure::profiles::doc.deprecated>> {meta::pure::profiles::doc.doc='test tagged value'} name:String[1];
                }
                """);
        dump("pf reference test data", """
                function v12::Hello(name: String[1]): String[1]
                {
                  'Hello ' + $name
                }
                {
                  store::MyStore: testing::MyReference;
                  myTest | Hello('John') => 'Hello John!';
                }
                """);
        dump("pf relation island data", """
                function v13::SimpleFunction(): String[1]
                {
                  'Hello World!'
                }
                {
                  my::Database:
                      Relation
                      #{
                        Schema.table:
                            id, other
                            1, a;
                      }#;
                  myTest | SimpleFunction() => 'Hello World!';
                }
                """);
        dump("pf mixed suites xml", """
                function v14::MyFunc(): String[1]
                {
                  ''
                }
                {
                  testSuite1
                  (
                      testFail | MyFunc() => (JSON) '[]';
                  )
                  testPass | MyFunc() => (XML) 'x';
                }
                """);
        dump("pf modelstore island", """
                Class model::Firm { name: String[1]; }
                function v15::f(): String[1]
                {
                  ''
                }
                {
                  testSuite_1
                  (
                    ModelStore:
                        ModelStore
                        #{
                          model::Firm:
                            ExternalFormat
                            #{
                              contentType: 'application/json';
                              data: '{}';
                            }#
                        }#;
                    t1 | f() => 'x';
                  )
                }
                """);
        dump("pf relational island", """
                function v16::f(): String[1]
                {
                  ''
                }
                {
                  testSuite_1
                  (
                    store::TestDB:
                        Relational
                        #{
                          default.PersonTable:
                            'id,firstName\\n'+
                            '1,I\\'m John\\n';
                        }#;
                    t1 | f() => 'x';
                  )
                }
                """);
        dump("pf csv cells", """
                function v17::f(): String[1]
                {
                  ''
                }
                {
                  my::Database:
                      Relation
                      #{
                        Schema.table:
                            id, firstName
                            1 , "I'm,John\"\"Doe\"\"";
                        Schema.table2:
                            id, lastName
                            2 , Jr;
                      }#;
                  t1 | f() => 'x';
                }
                """);
        dump("relation span fit", """
                function v18::a(): String[1]
                {
                  ''
                }
                {
                  t1 | a() => Relation
                  #{
                    id, name;
                  }#;
                  t2 | a() => Relation
                  #{
                    ab;
                  }#;
                  t3 | a() => Relation
                  #{ cd, ef
                     1, 2;
                  }#;
                }
                """);
        dump("measure", """
                Measure k::M
                {
                  *Gram: x -> $x;
                  Kilogram: x -> 1000 * $x;
                }
                """);
        dump("constraint", """
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
                """);
    }
}
