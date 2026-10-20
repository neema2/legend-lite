// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.cypher;

import com.legend.protocol.PureComposer;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.SqlTranslationException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The translator ({@link CypherToPure}) on its own: the Pure each query becomes, printed as a person would write it,
 * Cypher's null rules, and each refusal's kind. The model is a stand-in for the compiled one, so nothing is compiled
 * here; {@code CypherIslandIntegrationTest} runs the same pipeline end to end.
 */
class CypherToPureTest {

    /** A property of the stand-in model: name, type, bounds; a type written as a path is a class. */
    private record Prop(String name, String type, int lower, int upper) {
        void reportTo(CypherToPure.PropertySink sink) {
            sink.property(name, type, type.contains("::"), false, lower, upper);
        }
    }

    private static Prop prop(String name, String type, int lower, int upper) {
        return new Prop(name, type, lower, upper);
    }

    /** demo::Clock's marks (the study's §12.64). */
    private static final Map<String, List<String>> MARKS = Map.of("tick", List.of("timeOfDay", "nanoseconds"),
            "at", List.of("nanoseconds"));

    /** demo::Person {name String[1], age Integer[0..1], active Boolean[0..1], firm Firm[0..1]} and demo::Firm
     *  {legalName String[1], hq City[0..1], employees Person[*]}; demo::City {name String[1]}; firm and employees
     *  are one association's ends. */
    /** The classes below plus the stored-temporal ones (the study's §12.59): demo::Stored {t StrictTime[0..1],
     *  dur demo::values::Duration[0..1]} and the Duration value class (the four slots). */
    private static Map<String, Map<String, Prop>> classes() {
        Map<String, Map<String, Prop>> all = new java.util.HashMap<>(BASE);
        all.put("demo::Stored", Map.of(
                "name", prop("name", "String", 1, 1),
                "t", prop("t", "StrictTime", 0, 1),
                "dur", prop("dur", "demo::values::Duration", 0, 1)));
        all.put("demo::values::Duration", Map.of(
                "months", prop("months", "Integer", 1, 1),
                "days", prop("days", "Integer", 1, 1),
                "seconds", prop("seconds", "Integer", 1, 1),
                "nanoseconds", prop("nanoseconds", "Integer", 1, 1)));
        return Map.copyOf(all);
    }

    private static final Map<String, Map<String, Prop>> BASE = Map.of(
            "demo::Person", Map.of(
                    "name", prop("name", "String", 1, 1),
                    "age", prop("age", "Integer", 0, 1),
                    "active", prop("active", "Boolean", 0, 1),
                    "firm", prop("firm", "demo::Firm", 0, 1),
                    "tags", prop("tags", "String", 0, -1)),
            "demo::Firm", Map.of(
                    "legalName", prop("legalName", "String", 1, 1),
                    "hq", prop("hq", "demo::City", 0, 1),
                    "employees", prop("employees", "demo::Person", 0, -1)),
            "demo::City", Map.of("name", prop("name", "String", 1, 1)),
            // a class with the profile's temporal marks (the study's §12.64): tick a nanosecond time of day, at a
            // nanosecond date-time (MARKS)
            "demo::Clock", Map.of("name", prop("name", "String", 1, 1), "tick", prop("tick", "DateTime", 0, 1),
                    "at", prop("at", "DateTime", 0, 1), "zt", prop("zt", "demo::ZonedTime", 0, 1)),
            // the zoned value class (the study's §12.65): an embedded instant (nanosecond) and its offset
            "demo::ZonedTime", Map.of("instant", prop("instant", "DateTime", 1, 1),
                    "offsetSeconds", prop("offsetSeconds", "Integer", 1, 1)),
            // the edge classes (the study's §5.6): Acting's ends actor and firm, its properties role and since;
            // Directing's ends director and firm (never nodes: the models' classes() leave them out)
            "demo::Acting", Map.of(
                    "actor", prop("actor", "demo::Person", 1, 1),
                    "firm", prop("firm", "demo::Firm", 1, 1),
                    "role", prop("role", "String", 1, 1),
                    "since", prop("since", "Integer", 0, 1)),
            "demo::Directing", Map.of(
                    "director", prop("director", "demo::Person", 1, 1),
                    "firm", prop("firm", "demo::Firm", 1, 1)),
            // LINK, a type over two pairs (Person to City, Person to Firm): two edge classes of one type, their
            // properties differing (w on one, v on the other)
            "demo::LinkCity", Map.of(
                    "person", prop("person", "demo::Person", 1, 1),
                    "city", prop("city", "demo::City", 1, 1),
                    "w", prop("w", "Integer", 0, 1)),
            "demo::LinkFirm", Map.of(
                    "person", prop("person", "demo::Person", 1, 1),
                    "firm", prop("firm", "demo::Firm", 1, 1),
                    "v", prop("v", "Integer", 0, 1)));

    private static final Map<String, Map<String, Prop>> CLASSES = classes();   // (after BASE: static order)

    private static final java.util.Set<String> EDGE_CLASSES = java.util.Set.of("demo::Acting", "demo::Directing",
            "demo::LinkCity", "demo::LinkFirm");

    private static final CypherToPure.Model MODEL = new CypherToPure.Model() {
        @Override
        public List<String> classesOf(String label) {
            if (label.contains("::")) {
                return CLASSES.containsKey(label) ? List.of(label) : List.of();
            }
            return CLASSES.keySet().stream().filter(c -> c.endsWith("::" + label)).sorted().toList();
        }

        @Override
        public void property(String classPath, String name, CypherToPure.PropertySink sink) {
            Map<String, Prop> props = CLASSES.get(classPath);
            Prop p = props == null ? null : props.get(name);
            if (p != null) {
                p.reportTo(sink);
            }
        }

        @Override
        public void inverse(String classPath, String property, CypherToPure.PropertySink sink) {
            if (classPath.equals("demo::Person") && property.equals("firm")) {
                prop("employees", "demo::Person", 0, -1).reportTo(sink);
            } else if (classPath.equals("demo::Firm") && property.equals("employees")) {
                prop("firm", "demo::Firm", 0, 1).reportTo(sink);
            }
        }

        @Override
        public boolean isSubclass(String classPath, String superPath) {
            return classPath.equals(superPath);
        }

        @Override
        public List<String> marks(String classPath, String property) {
            return classPath.equals("demo::Clock") ? MARKS.getOrDefault(property, List.of())
                    : classPath.equals("demo::ZonedTime") && property.equals("instant") ? List.of("nanoseconds")
                    : List.of();
        }

        @Override
        public List<String> classes() {
            // (an edge class is never a node)
            return CLASSES.keySet().stream().filter(c -> !EDGE_CLASSES.contains(c)).sorted().toList();
        }

        @Override
        public void properties(String classPath, CypherToPure.PropertySink sink) {
            Map<String, Prop> props = CLASSES.get(classPath);
            if (props != null) {
                props.keySet().stream().sorted().forEach(n -> props.get(n).reportTo(sink));
            }
        }

        @Override
        public List<String> labels(String classPath) {
            return List.of(classPath.substring(classPath.lastIndexOf("::") + 2));
        }

        /** test.cities(name :: STRING?) :: (city :: STRING?, code :: INTEGER?) at test::cities; test.doNothing() a
         *  VOID one (the study's §12.60). */
        @Override
        public void procedure(String name, CypherToPure.ProcedureSink sink) {
            if (name.equals("test.cities")) {
                sink.procedure("test::cities");
                sink.input("name", "String");
                sink.output("city", "String");
                sink.output("code", "Integer");
            } else if (name.equals("test.doNothing")) {
                sink.procedure("test::doNothing");
            }
        }

        @Override
        public void ends(String classPath, CypherToPure.PropertySink sink) {
            // firm is the forward end of Person-Firm (employees its inverse); hq a plain property holding a class
            if (classPath.equals("demo::Person")) {
                prop("firm", "demo::Firm", 0, 1).reportTo(sink);
            } else if (classPath.equals("demo::Firm")) {
                prop("hq", "demo::City", 0, 1).reportTo(sink);
            }
        }

        @Override
        public void edges(CypherToPure.EdgeSink sink) {
        }

        @Override
        public boolean open() {
            return false;
        }
    };

    /** {@code inner} as an OPEN graph (the study's §12.45): every reading its own, the openness true. */
    /** The model without one of its classes (a model declaring no Duration value class). */
    private record Hiding(CypherToPure.Model inner, String hidden) implements CypherToPure.Model {
        @Override
        public List<String> classesOf(String label) {
            return inner.classesOf(label).stream().filter(c -> !c.equals(hidden)).toList();
        }

        @Override
        public void property(String classPath, String name, CypherToPure.PropertySink sink) {
            inner.property(classPath, name, sink);
        }

        @Override
        public void inverse(String classPath, String property, CypherToPure.PropertySink sink) {
            inner.inverse(classPath, property, sink);
        }

        @Override
        public boolean isSubclass(String classPath, String superPath) {
            return inner.isSubclass(classPath, superPath);
        }

        @Override
        public List<String> classes() {
            return inner.classes().stream().filter(c -> !c.equals(hidden)).toList();
        }

        @Override
        public void properties(String classPath, CypherToPure.PropertySink sink) {
            inner.properties(classPath, sink);
        }

        @Override
        public List<String> labels(String classPath) {
            return inner.labels(classPath);
        }

        @Override
        public void ends(String classPath, CypherToPure.PropertySink sink) {
            inner.ends(classPath, sink);
        }

        @Override
        public void edges(CypherToPure.EdgeSink sink) {
            inner.edges(sink);
        }

        @Override
        public boolean open() {
            return inner.open();
        }
    }

    private record Open(CypherToPure.Model inner) implements CypherToPure.Model {
        @Override
        public List<String> classesOf(String label) {
            return inner.classesOf(label);
        }

        @Override
        public void property(String classPath, String name, CypherToPure.PropertySink sink) {
            inner.property(classPath, name, sink);
        }

        @Override
        public void inverse(String classPath, String property, CypherToPure.PropertySink sink) {
            inner.inverse(classPath, property, sink);
        }

        @Override
        public boolean isSubclass(String classPath, String superPath) {
            return inner.isSubclass(classPath, superPath);
        }

        @Override
        public List<String> classes() {
            return inner.classes();
        }

        @Override
        public void properties(String classPath, CypherToPure.PropertySink sink) {
            inner.properties(classPath, sink);
        }

        @Override
        public List<String> labels(String classPath) {
            return inner.labels(classPath);
        }

        @Override
        public void ends(String classPath, CypherToPure.PropertySink sink) {
            inner.ends(classPath, sink);
        }

        @Override
        public void edges(CypherToPure.EdgeSink sink) {
            inner.edges(sink);
        }

        @Override
        public boolean open() {
            return true;
        }
    }

    /** The model with {@code demo::Manager extends demo::Person} ({@code grade Integer[0..1]}) and the anonymous
     *  {@code demo::N_Firm_Person extends demo::Firm, demo::Person} (the study's §12.61: a node of the labels
     *  Firm and Person; its name is no label). */
    private record Subclassing(CypherToPure.Model inner) implements CypherToPure.Model {
        private static final String MANAGER = "demo::Manager";
        private static final String BOTH = "demo::N_Firm_Person";

        @Override
        public List<String> classesOf(String label) {
            if (label.equals("Manager") || label.equals(MANAGER)) {
                return List.of(MANAGER);
            }
            if (label.equals(BOTH)) {
                return List.of(BOTH);   // (by its path alone: anonymous)
            }
            return inner.classesOf(label);
        }

        @Override
        public void property(String classPath, String name, CypherToPure.PropertySink sink) {
            if (classPath.equals(MANAGER)) {
                if (name.equals("grade")) {
                    prop("grade", "Integer", 0, 1).reportTo(sink);
                } else {
                    inner.property("demo::Person", name, sink);
                }
                return;
            }
            if (classPath.equals(BOTH)) {
                boolean[] found = {false};
                inner.property("demo::Firm", name, (n, type, classTyped, enumTyped, lower, upper) -> {
                    found[0] = true;
                    sink.property(n, type, classTyped, enumTyped, lower, upper);
                });
                if (!found[0]) {
                    inner.property("demo::Person", name, sink);
                }
                return;
            }
            inner.property(classPath, name, sink);
        }

        @Override
        public void inverse(String classPath, String property, CypherToPure.PropertySink sink) {
            inner.inverse(classPath.equals(MANAGER) ? "demo::Person" : classPath, property, sink);
        }

        @Override
        public boolean isSubclass(String classPath, String superPath) {
            if (classPath.equals(MANAGER)) {
                return superPath.equals(MANAGER) || superPath.equals("demo::Person");
            }
            if (classPath.equals(BOTH)) {
                return superPath.equals(BOTH) || superPath.equals("demo::Firm") || superPath.equals("demo::Person");
            }
            return inner.isSubclass(classPath, superPath);
        }

        @Override
        public List<String> classes() {
            List<String> out = new java.util.ArrayList<>(inner.classes());
            out.add(MANAGER);
            out.add(BOTH);
            return out.stream().sorted().toList();
        }

        @Override
        public void properties(String classPath, CypherToPure.PropertySink sink) {
            if (classPath.equals(MANAGER)) {
                prop("grade", "Integer", 0, 1).reportTo(sink);
                inner.properties("demo::Person", sink);
            } else if (classPath.equals(BOTH)) {
                inner.properties("demo::Firm", sink);
                inner.properties("demo::Person", sink);
            } else {
                inner.properties(classPath, sink);
            }
        }

        @Override
        public List<String> labels(String classPath) {
            if (classPath.equals(MANAGER)) {
                return List.of("Manager", "Person");
            }
            if (classPath.equals(BOTH)) {
                return List.of("Firm", "Person");
            }
            return inner.labels(classPath);
        }

        @Override
        public void ends(String classPath, CypherToPure.PropertySink sink) {
            if (classPath.equals(MANAGER)) {
                inner.ends("demo::Person", sink);
            } else if (classPath.equals(BOTH)) {
                inner.ends("demo::Firm", sink);
                inner.ends("demo::Person", sink);
            } else {
                inner.ends(classPath, sink);
            }
        }

        @Override
        public void edges(CypherToPure.EdgeSink sink) {
            inner.edges(sink);
        }

        @Override
        public boolean open() {
            return inner.open();
        }
    }

    /** The model with its edge class (the study's §5.6): Acting, of type ACTED_IN from Person to Firm in the
     *  association form (each end's inverse on the node class holds many edges), its properties role and since;
     *  Directing, the compact form (its ends its own properties, no inverse), of its simple name's type. */
    private static final CypherToPure.Model EDGES = new CypherToPure.Model() {
        @Override
        public List<String> classesOf(String label) {
            return MODEL.classesOf(label);
        }

        @Override
        public void property(String classPath, String name, CypherToPure.PropertySink sink) {
            MODEL.property(classPath, name, sink);
        }

        @Override
        public void inverse(String classPath, String property, CypherToPure.PropertySink sink) {
            MODEL.inverse(classPath, property, sink);
        }

        @Override
        public boolean isSubclass(String classPath, String superPath) {
            return MODEL.isSubclass(classPath, superPath);
        }

        @Override
        public List<String> classes() {
            return MODEL.classes();
        }

        @Override
        public void properties(String classPath, CypherToPure.PropertySink sink) {
            MODEL.properties(classPath, sink);
        }

        @Override
        public List<String> labels(String classPath) {
            return MODEL.labels(classPath);
        }

        @Override
        public void ends(String classPath, CypherToPure.PropertySink sink) {
            MODEL.ends(classPath, sink);
        }

        @Override
        public void edges(CypherToPure.EdgeSink sink) {
            sink.edge("demo::Acting", "ACTED_IN", "actor", "demo::Person", "actings", true, "firm", "demo::Firm",
                    "cast", true);
            sink.edge("demo::Directing", "Directing", "director", "demo::Person", null, false, "firm", "demo::Firm",
                    null, false);
            sink.edge("demo::LinkCity", "LINK", "person", "demo::Person", "cityLinks", true, "city", "demo::City",
                    "linkedPeople", true);
            sink.edge("demo::LinkFirm", "LINK", "person", "demo::Person", "firmLinks", true, "firm", "demo::Firm",
                    "linkedPeople", true);
        }

        @Override
        public boolean open() {
            return false;
        }
    };

    private static String edges(String cypher) {
        return PureComposer.valueSpecification(CypherToPure.translate(cypher, EDGES), PureComposer.Style.STANDARD);
    }

    private static SqlTranslationException edgeRefusal(String cypher) {
        return assertThrows(SqlTranslationException.class, () -> edges(cypher));
    }

    @Test
    void aProcedureCallIsTheModelsFunctionOverItsArgumentsJoinedToTheRows() {
        // standalone: the function's relation, every output; after rows: the rows laterally joined to the relation
        // of each row's arguments, the yielded outputs under their aliases (the study's §12.60)
        assertEquals("test::cities('Stefan')", pure("CALL test.cities('Stefan')"));
        assertEquals("test::cities('Stefan')->select(~[city])", pure("CALL test.cities('Stefan') YIELD city"));
        // (the composer spells a call over a variable as its receiver: $r.x->test::cities())
        String rows = pure("MATCH (p:Person) CALL test.cities(p.name) YIELD city AS c, code RETURN p.name AS name, c,"
                + " code");
        assertTrue(rows.contains("->lateral(r|$r._cypher_4->test::cities()->rename(~city, ~_cypher_vl_y0)->rename("
                + "~_cypher_vl_y0, ~c))"), rows);
        // a VOID procedure keeps the rows; standalone it answers no row
        assertEquals("demo::Person.all()->project(~[name:p|$p.name])",
                pure("MATCH (p:Person) CALL test.doNothing() RETURN p.name AS name"));
        assertTrue(pure("CALL test.doNothing()").contains("_cypher_void"), pure("CALL test.doNothing()"));
        // Neo4j's static errors
        invalid("CALL test.other()", "There is no procedure with the name `test.other`");
        invalid("CALL test.cities('a', 1)", "too many arguments");
        invalid("CALL test.cities(1)", "Type mismatch: expected String but was Integer");
        invalid("MATCH (p:Person) CALL test.cities(count(p)) YIELD city RETURN city", "aggregating function");
        invalid("CALL test.cities YIELD city RETURN city", "passing arguments implicitly");
        invalid("CALL test.cities('a') RETURN city", "naming results implicitly");
        invalid("WITH 'x' AS city CALL test.cities('a') YIELD city RETURN *", "Variable `city` already declared");
        invalid("CALL test.cities('a') YIELD town RETURN town", "Unknown procedure output: `town`");
    }

    @Test
    void aStoredTimeOfDayAndAStoredDurationReadAsTheIslandsOwnValues() {
        // a TIME column's StrictTime read as the time the island carries (ordered, a field, beside a duration); a
        // stored duration (an embedded instance over four columns) as the island's own instance of the four slot
        // navigations, null where the row holds none (the study's §12.59)
        String pure = pure("MATCH (s:Stored) RETURN s.t AS t, s.t.hour AS h, s.dur AS dur, s.dur.days AS days,"
                + " s.dur IS NULL AS none ORDER BY s.t");
        assertTrue(pure.contains("t:s|$s.t"), pure);
        assertTrue(pure.contains("->hour()"), pure);
        assertTrue(pure.contains("if($s.dur.months->isEmpty(), |[]->cast(@demo::values::Duration), |^demo::values::Duration("
                + "months=$s.dur.months->toOne()->cast(@Integer)"), pure);
        assertTrue(pure.contains("->sort([~t->ascending("), pure);
        // a node holding a time of day, as a value: the oracle's Time('HH:mm:ss.f') spelling, its text Neo4j's
        String node = pure("MATCH (s:Stored) RETURN s");
        assertTrue(node.contains("'\"Time(\\'' + "), node);
        // a date moved by a duration takes the whole days of the duration's seconds (Neo4j's, cut toward zero)
        String moved = pure("WITH date({year: 1984, month: 10, day: 11}) AS x RETURN x + duration({days: 29, hours: 33}) AS d");
        assertTrue(moved.contains("DurationUnit.DAYS"), moved);
        assertTrue(moved.contains("->rem(86400)"), moved);
    }

    @Test
    void aLabelReadsItsClassAndTheSubclassesAndANodeOfSeveralLabelsIsAnAnonymousClass() {
        // a label's class and its subclasses (the study's §12.61): one arm per class, concatenated
        String label = sub("MATCH (p:Person) RETURN p.name AS name");
        assertTrue(label.contains("demo::Person.all()") && label.contains("demo::Manager.all()")
                && label.contains("demo::N_Firm_Person.all()") && label.contains("->concatenate("), label);
        // the class's own path: that class alone
        String own = sub("MATCH (p:`demo::Person`) RETURN p.name AS name");
        assertTrue(own.contains("demo::Person.all()") && !own.contains("->concatenate("), own);
        // two labels: the classes carrying both, here the anonymous one alone
        String both = sub("MATCH (x:Person:Firm) RETURN x.name AS name, x.legalName AS legalName");
        assertTrue(both.contains("demo::N_Firm_Person.all()") && !both.contains("->concatenate(")
                && !both.contains("demo::Person.all()"), both);
        // labels(n) is the class's label set: a subclass carries its superclass's label, an anonymous class's name
        // is no label (and names no class as a label)
        String labels = sub("MATCH (m:Manager) RETURN labels(m) AS l");
        assertTrue(labels.contains("'[\"Manager\",\"Person\"]'"), labels);
        String anonymous = sub("MATCH (x:Person:Firm) RETURN labels(x) AS l, x:Firm AS isFirm");
        assertTrue(anonymous.contains("'[\"Firm\",\"Person\"]'"), anonymous);
        SqlTranslationException e = assertThrows(SqlTranslationException.class,
                () -> sub("MATCH (x:N_Firm_Person) RETURN x.name AS name"));
        assertTrue(e.getMessage().contains("names no class"), e.getMessage());
        // a label set no class carries reads no rows (the base model: no class is both)
        String none = pure("MATCH (p:Person:Firm) RETURN p.name AS name");
        assertTrue(!none.contains(".all()"), none);
    }

    @Test
    void aTypedNullItemReadsBackAsTheNullItIs() {
        // (the study's §12.82) a projected item that is a typed null (here no way of the model reads the pattern) is
        // read back as that null where the clauses after read it, so its column is never read to one (a statically
        // empty to-one, which the lowering spells as a raise); a sort key null on every row orders nothing
        String p = pure("MATCH (me:City)--(you:City) RETURN me.age + you.age, count(*) AS cnt"
                + " ORDER BY me.age + you.age + count(*)");
        assertTrue(p.contains("groupBy(~['me.age + you.age'], ~[cnt:x|$x._cypher_2:y|$y->count()])"), p);
        assertFalse(p.contains("toOne"), p);
        assertFalse(p.contains("->sort("), p);
    }

    @Test
    void theErrorsRaisedWhereTheTranslatorRefused() {
        // (the study's §12.84) a conversion of a value it does not take is Neo4j's TypeError, the database's raise on
        // the rows that hold one; a literal argument is Neo4j's type mismatch at compile time
        String raised = pure("MATCH (p:Person) RETURN [x IN [1, p] | toInteger(x)] AS l");
        assertTrue(raised.contains("fail('Invalid input for function \\'toInteger()\\': Expected a String, Float,"
                + " Integer or Boolean, got: Node')"), raised);
        invalid("RETURN toFloat(true) AS f", "Type mismatch: expected Float, Integer or String but was Boolean");
        // toBoolean of an integer and toInteger of a boolean, which Neo4j takes
        assertTrue(pure("RETURN toBoolean(7) AS b").contains("b:r|7 != 0"), pure("RETURN toBoolean(7) AS b"));
        assertTrue(pure("RETURN toInteger(true) AS i").contains("if(true, |1, |0)"), pure("RETURN toInteger(true) AS i"));
        // Neo4j's static errors, raised before any clause is read
        invalid("MATCH (a:Person) RETURN size((a)-->()) AS s", "It can no longer be used inside the function size()");
        invalid("MATCH p = (a:Person)-->(b) RETURN p.name AS n", "but was Path");
        invalid("MATCH p = (a:Person)-->(b) RETURN size(p) AS s", "expected String, Vector or List<T> but was Path");
        SqlTranslationException overflow = refusal("RETURN 1.5e400 AS f");
        assertEquals(SqlTranslationException.Kind.SYNTAX, overflow.kind(), overflow.getMessage());
        assertTrue(overflow.getMessage().contains("floating point number is too large"), overflow.getMessage());
        String shadowing = pure("MATCH p = (a:Person)-->(b) RETURN [p IN [{name: 'x'}] | p.name] AS l");
        // (the comprehension's p an element of its list, not the path: it translates)
        assertTrue(shadowing.contains("x"), shadowing);
        invalid("MATCH (n:Person) WHERE EXISTS { MATCH (n)-->(m) SET m.prop = 1 } RETURN n",
                "An Exists Expression cannot contain any updates");
        // a name Neo4j defines no function of is its error; one it defines that the translator does not read is
        // refused by name
        invalid("RETURN foo(1) AS x", "Unknown function 'foo'");
        invalid("RETURN date.yesterday() AS d", "Unknown function 'date.yesterday'");
        unsupported("RETURN vector_norm([1.0], 'EUCLIDEAN') AS n", "the function vector_norm");
    }

    /** The most characters {@link #aValueReadMoreThanOnceIsWrittenOnce}'s translation may print as. MEASURED
     *  2026-10-19: 13,787 with the translator's let, 18,645,908 without it (each read written out); the cap leaves the
     *  let room to grow and fails long before a doubling step comes back. */
    private static final int SHARED_CAP = 50_000;

    private static String pure(String cypher) {
        ValueSpecification vs = CypherToPure.translate(cypher, MODEL);
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD);
    }

    /** {@code cypher} over the model read as an open graph (the study's §12.45). */
    private static String open(String cypher) {
        ValueSpecification vs = CypherToPure.translate(cypher, new Open(MODEL));
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD);
    }

    /** {@code cypher} over the model with a subclass and an anonymous label-set class (the study's §12.61). */
    private static String sub(String cypher) {
        ValueSpecification vs = CypherToPure.translate(cypher, new Subclassing(MODEL));
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD);
    }

    private static SqlTranslationException openRefusal(String cypher) {
        return assertThrows(SqlTranslationException.class, () -> open(cypher));
    }

    private static SqlTranslationException refusal(String cypher) {
        return assertThrows(SqlTranslationException.class, () -> pure(cypher));
    }

    private static void unsupported(String cypher, String message) {
        SqlTranslationException e = refusal(cypher);
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, e.kind(), e.getMessage());
        assertTrue(e.getMessage().contains(message), e.getMessage());
    }

    private static void invalid(String cypher, String message) {
        SqlTranslationException e = refusal(cypher);
        assertEquals(SqlTranslationException.Kind.INVALID, e.kind(), e.getMessage());
        assertTrue(e.getMessage().contains(message), e.getMessage());
    }

    // ------------------------------------------------------------------ the static variable errors (the study's §12.63)

    @Test
    void theStaticVariableErrorsAreRaisedBeforeAnyClauseIsRead() {
        // Neo4j's VariableAlreadyBound and VariableTypeConflict over the whole statement, before any clause is
        // read: each over an unbounded variable-length pattern the clauses would refuse first (W10)
        invalid("MATCH ()-[r*]->() MATCH (r) RETURN r", "Type mismatch: r defined with conflicting type");
        invalid("MATCH r = ()-[*]->(), (r) RETURN r", "Type mismatch: r defined with conflicting type");
        invalid("MATCH (r)-[]-()-[r*]-() RETURN r", "Type mismatch: r defined with conflicting type");
        invalid("MATCH ()-[r*]->() MATCH ()-[r]->() RETURN r", "Type mismatch: r defined with conflicting type");
        invalid("WITH 123 AS n MATCH (n)-[*]->() RETURN n", "Type mismatch: n defined with conflicting type");
        invalid("UNWIND [1, 2] AS x MATCH (x)-[*]->() RETURN x", "Type mismatch: x defined with conflicting type");
        invalid("MATCH ()-[p*]->() MATCH p = ()-[]-() RETURN p", "Variable `p` already declared");
        invalid("MATCH p = ()-[p*]-() RETURN p", "Variable `p` already declared");
        invalid("WITH 'foo' AS p MATCH p = ()-[*]->() RETURN p", "Variable `p` already declared");
        invalid("MATCH (x:Person)-[*]->() UNWIND [1] AS x RETURN x", "Variable `x` already declared");
        // a value never conflicts statically (an expression may well be a node), and a node named again as a node
        // is a bound one: the clauses read on, to their own checks
        // (a node read back from a Variant re-anchors on the identity its JSON carries, the study's §12.71)
        String held = pure("MATCH (a:Person) WITH head(collect(a)) AS n MATCH (n)-[*]->() RETURN n");
        assertTrue(held.contains("recurse(") && held.contains("->get('@id')->to(@String)"), held);
        assertTrue(pure("MATCH (a:Person)-[:firm]->(f) MATCH (a)-[*]->() RETURN f.legalName AS f").contains("recurse("));
        // a list of variables is a value too (it may list relationships: Match4/[8]'s), a list of literals a literal
        // (a variable-length relationship bound to a list matches its relationships' identities in order, §12.71)
        String bound = pure("MATCH ()-[r1]->()-[r2]->() WITH [r1, r2] AS rs LIMIT 1 MATCH (first)-[rs*]->(second) RETURN first");
        assertTrue(bound.contains("->map(e|$e->get('@id')->to(@String))->joinStrings('|')"), bound);
        invalid("WITH [[1], 'x'] AS rs MATCH ()-[rs*]->() RETURN rs", "Type mismatch: rs defined with conflicting type");
        // a list of variables, or a collect, is a list: named as a node it conflicts (Match3/[30]), as a
        // variable-length relationship's list it reads on (the study's §12.70)
        invalid("MATCH (n:Person) WITH [n] AS users MATCH (users)-->(m) RETURN m",
                "Type mismatch: users defined with conflicting type");
        invalid("MATCH (n:Person) WITH collect(n) AS users MATCH (users)-->(m) RETURN m",
                "Type mismatch: users defined with conflicting type");
    }

    // ------------------------------------------------------------------ stored nanosecond values (the study's §12.64)

    @Test
    void aMarkedDateTimeIsANanosecondTimeOfDayOrDateTime() {
        // the profile's timeOfDay and nanoseconds marks on a DateTime property: a nanosecond time of day is the
        // date-time at it on the epoch day, a nanosecond date-time keeps its digits past the microsecond; each read
        // into the parts (W13 leg 6: its date and the database's nanoseconds since midnight of it), so its fields,
        // comparisons and text are the parts'; the node's value spells a Time('...')
        assertEquals("demo::Clock.all()->project(~[_cypher_let_0:c|$c.tick, _cypher_let_1:c|$c.at])->extend(~[_cypher_let_2:c|if($c._cypher_let_1->isEmpty(), |[], |$c._cypher_let_1->toOne()->datePart())])->drop(0)->extend(~[ns:c|if($c._cypher_let_0->isEmpty(), |[], |$c._cypher_let_0->toOne()->datePart()->dateDiff($c._cypher_let_0->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS)->rem(1000000000)->cast(@Integer)), pm:c|if(!$c._cypher_let_0->isEmpty() && ($c._cypher_let_0->toOne()->datePart()->dateDiff($c._cypher_let_0->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS) > 1000 * %1970-01-01->meta::legend::lite::atTime(%12:00)->datePart()->dateDiff(%1970-01-01->meta::legend::lite::atTime(%12:00), meta::pure::functions::date::DurationUnit.MICROSECONDS)), |true, |if(!$c._cypher_let_0->isEmpty() && !($c._cypher_let_0->toOne()->datePart()->dateDiff($c._cypher_let_0->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS) > 1000 * %1970-01-01->meta::legend::lite::atTime(%12:00)->datePart()->dateDiff(%1970-01-01->meta::legend::lite::atTime(%12:00), meta::pure::functions::date::DurationUnit.MICROSECONDS)), |false, |[])), at:c|if($c._cypher_let_2->isEmpty() || if($c._cypher_let_1->isEmpty(), |[], |$c._cypher_let_1->toOne()->datePart()->dateDiff($c._cypher_let_1->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |(if($c._cypher_let_2->toOne()->year() < 1, {|\nfail('a temporal value of a year before 1 is not supported');\nif($c._cypher_let_2->toOne()->year() > 9999, |'+' + $c._cypher_let_2->toOne()->year()->toString(), |toString($c._cypher_let_2->toOne()->year() + 10000)->substring(2));\n}, |if($c._cypher_let_2->toOne()->year() > 9999, |'+' + $c._cypher_let_2->toOne()->year()->toString(), |toString($c._cypher_let_2->toOne()->year() + 10000)->substring(2))) + '-' + toString($c._cypher_let_2->toOne()->monthNumber() + 100)->substring(2) + '-' + toString($c._cypher_let_2->toOne()->dayOfMonth() + 100)->substring(2)) + 'T' + (toString(floor(if($c._cypher_let_1->isEmpty(), |[], |$c._cypher_let_1->toOne()->datePart()->dateDiff($c._cypher_let_1->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne() / 3600000000000) + 100)->substring(2) + ':' + toString(floor(if($c._cypher_let_1->isEmpty(), |[], |$c._cypher_let_1->toOne()->datePart()->dateDiff($c._cypher_let_1->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne()->rem(3600000000000)->cast(@Integer) / 60000000000) + 100)->substring(2) + ':' + toString(floor(if($c._cypher_let_1->isEmpty(), |[], |$c._cypher_let_1->toOne()->datePart()->dateDiff($c._cypher_let_1->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne()->rem(60000000000)->cast(@Integer) / 1000000000) + 100)->substring(2) + if(if($c._cypher_let_1->isEmpty(), |[], |$c._cypher_let_1->toOne()->datePart()->dateDiff($c._cypher_let_1->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne()->rem(1000000000)->cast(@Integer) == 0, |'', |'.' + toString(if($c._cypher_let_1->isEmpty(), |[], |$c._cypher_let_1->toOne()->datePart()->dateDiff($c._cypher_let_1->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne()->rem(1000000000)->cast(@Integer) + 1000000000)->substring(2)->regexpReplace('0+$', '', true)) + ''))])->select(~[ns, pm, at])", pure("MATCH (c:Clock) RETURN c.tick.nanosecond AS ns, c.tick > localtime('12:00') AS pm,"
                + " toString(c.at) AS at"));
        assertEquals("demo::Clock.all()->project(~[c:c|fromJson((('{\"@labels\":' + '[\"Clock\"]') + if($c->meta::legend::lite::elementId()->isEmpty(), |'', |',\"@id\":' + ('\"' + $c->meta::legend::lite::elementId()->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + (((if(if(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->isEmpty() || if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart()->dateDiff($c.at->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |'DateTime(\\'' + ((if(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year() < 1, {|\nfail('a temporal value of a year before 1 is not supported');\nif(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year() > 9999, |'+' + if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year()->toString(), |toString(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year() + 10000)->substring(2));\n}, |if(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year() > 9999, |'+' + if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year()->toString(), |toString(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year() + 10000)->substring(2))) + '-' + toString(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->monthNumber() + 100)->substring(2) + '-' + toString(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->dayOfMonth() + 100)->substring(2)) + 'T' + (toString(floor(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart()->dateDiff($c.at->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne() / 3600000000000) + 100)->substring(2) + ':' + toString(floor(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart()->dateDiff($c.at->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne()->rem(3600000000000)->cast(@Integer) / 60000000000) + 100)->substring(2) + ':' + toString(floor(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart()->dateDiff($c.at->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne()->rem(60000000000)->cast(@Integer) / 1000000000) + 100)->substring(2) + if(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart()->dateDiff($c.at->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne()->rem(1000000000)->cast(@Integer) == 0, |'', |'.' + toString(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart()->dateDiff($c.at->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne()->rem(1000000000)->cast(@Integer) + 1000000000)->substring(2)->regexpReplace('0+$', '', true)) + '')) + '\\')')->isEmpty(), |'', |',\"at\":' + '\"' + if(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->isEmpty() || if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart()->dateDiff($c.at->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |'DateTime(\\'' + ((if(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year() < 1, {|\nfail('a temporal value of a year before 1 is not supported');\nif(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year() > 9999, |'+' + if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year()->toString(), |toString(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year() + 10000)->substring(2));\n}, |if(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year() > 9999, |'+' + if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year()->toString(), |toString(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->year() + 10000)->substring(2))) + '-' + toString(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->monthNumber() + 100)->substring(2) + '-' + toString(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart())->toOne()->dayOfMonth() + 100)->substring(2)) + 'T' + (toString(floor(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart()->dateDiff($c.at->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne() / 3600000000000) + 100)->substring(2) + ':' + toString(floor(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart()->dateDiff($c.at->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne()->rem(3600000000000)->cast(@Integer) / 60000000000) + 100)->substring(2) + ':' + toString(floor(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart()->dateDiff($c.at->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne()->rem(60000000000)->cast(@Integer) / 1000000000) + 100)->substring(2) + if(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart()->dateDiff($c.at->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne()->rem(1000000000)->cast(@Integer) == 0, |'', |'.' + toString(if($c.at->isEmpty(), |[], |$c.at->toOne()->datePart()->dateDiff($c.at->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne()->rem(1000000000)->cast(@Integer) + 1000000000)->substring(2)->regexpReplace('0+$', '', true)) + '')) + '\\')')->toOne() + '\"') + (',\"name\":' + ('\"' + $c.name->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + if(if($c.tick->isEmpty(), |[], |'Time(\\'' + (toString(floor($c.tick->toOne()->datePart()->dateDiff($c.tick->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS) / 3600000000000) + 100)->substring(2) + ':' + toString(floor($c.tick->toOne()->datePart()->dateDiff($c.tick->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS)->rem(3600000000000)->cast(@Integer) / 60000000000) + 100)->substring(2) + ':' + toString(floor($c.tick->toOne()->datePart()->dateDiff($c.tick->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS)->rem(60000000000)->cast(@Integer) / 1000000000) + 100)->substring(2) + if($c.tick->toOne()->datePart()->dateDiff($c.tick->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS)->rem(1000000000)->cast(@Integer) == 0, |'', |'.' + toString($c.tick->toOne()->datePart()->dateDiff($c.tick->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS)->rem(1000000000)->cast(@Integer) + 1000000000)->substring(2)->regexpReplace('0+$', '', true)) + '') + '\\')')->isEmpty(), |'', |',\"tick\":' + '\"' + if($c.tick->isEmpty(), |[], |'Time(\\'' + (toString(floor($c.tick->toOne()->datePart()->dateDiff($c.tick->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS) / 3600000000000) + 100)->substring(2) + ':' + toString(floor($c.tick->toOne()->datePart()->dateDiff($c.tick->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS)->rem(3600000000000)->cast(@Integer) / 60000000000) + 100)->substring(2) + ':' + toString(floor($c.tick->toOne()->datePart()->dateDiff($c.tick->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS)->rem(60000000000)->cast(@Integer) / 1000000000) + 100)->substring(2) + if($c.tick->toOne()->datePart()->dateDiff($c.tick->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS)->rem(1000000000)->cast(@Integer) == 0, |'', |'.' + toString($c.tick->toOne()->datePart()->dateDiff($c.tick->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS)->rem(1000000000)->cast(@Integer) + 1000000000)->substring(2)->regexpReplace('0+$', '', true)) + '') + '\\')')->toOne() + '\"')) + if(if(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->isEmpty() || if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer)->mod(86400000000000)->cast(@Integer))->isEmpty(), |[], |'DateTime(\\'' + ((if(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year() < 1, {|\nfail('a temporal value of a year before 1 is not supported');\nif(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year() > 9999, |'+' + if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year()->toString(), |toString(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year() + 10000)->substring(2));\n}, |if(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year() > 9999, |'+' + if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year()->toString(), |toString(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year() + 10000)->substring(2))) + '-' + toString(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->monthNumber() + 100)->substring(2) + '-' + toString(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->dayOfMonth() + 100)->substring(2)) + 'T' + (toString(floor(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer)->mod(86400000000000)->cast(@Integer))->toOne() / 3600000000000) + 100)->substring(2) + ':' + toString(floor(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer)->mod(86400000000000)->cast(@Integer))->toOne()->rem(3600000000000)->cast(@Integer) / 60000000000) + 100)->substring(2) + ':' + toString(floor(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer)->mod(86400000000000)->cast(@Integer))->toOne()->rem(60000000000)->cast(@Integer) / 1000000000) + 100)->substring(2) + if(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer)->mod(86400000000000)->cast(@Integer))->toOne()->rem(1000000000)->cast(@Integer) == 0, |'', |'.' + toString(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer)->mod(86400000000000)->cast(@Integer))->toOne()->rem(1000000000)->cast(@Integer) + 1000000000)->substring(2)->regexpReplace('0+$', '', true)) + '')) + '\\')')->isEmpty(), |'', |',\"zt\":' + '\"' + if(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->isEmpty() || if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer)->mod(86400000000000)->cast(@Integer))->isEmpty(), |[], |'DateTime(\\'' + ((if(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year() < 1, {|\nfail('a temporal value of a year before 1 is not supported');\nif(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year() > 9999, |'+' + if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year()->toString(), |toString(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year() + 10000)->substring(2));\n}, |if(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year() > 9999, |'+' + if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year()->toString(), |toString(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->year() + 10000)->substring(2))) + '-' + toString(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->monthNumber() + 100)->substring(2) + '-' + toString(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |%1970-01-01->adjust(floor(((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer) / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart())->toOne()->dayOfMonth() + 100)->substring(2)) + 'T' + (toString(floor(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer)->mod(86400000000000)->cast(@Integer))->toOne() / 3600000000000) + 100)->substring(2) + ':' + toString(floor(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer)->mod(86400000000000)->cast(@Integer))->toOne()->rem(3600000000000)->cast(@Integer) / 60000000000) + 100)->substring(2) + ':' + toString(floor(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer)->mod(86400000000000)->cast(@Integer))->toOne()->rem(60000000000)->cast(@Integer) / 1000000000) + 100)->substring(2) + if(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer)->mod(86400000000000)->cast(@Integer))->toOne()->rem(1000000000)->cast(@Integer) == 0, |'', |'.' + toString(if(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->isEmpty() || if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->isEmpty(), |[], |((86400000000000 * %1970-01-01->dateDiff(if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart())->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + if($c.zt.instant->isEmpty(), |[], |$c.zt.instant->toOne()->datePart()->dateDiff($c.zt.instant->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS))->toOne())->cast(@Integer)->mod(86400000000000)->cast(@Integer))->toOne()->rem(1000000000)->cast(@Integer) + 1000000000)->substring(2)->regexpReplace('0+$', '', true)) + '')) + '\\')')->toOne() + '\"')) + '}')])", pure("MATCH (c:Clock) RETURN c"));
    }

    @Test
    void aValueReadMoreThanOnceIsWrittenOnce() {
        // the translator's let (2026-10-19, Shares): each step of nanosecond arithmetic reads its operands more than
        // once (a strict operation's NULL guard and value; a date and a clock from one total; a region's instants), so
        // written out the translation doubled at each step -- lite/temporal/nanos_arithmetic was one 33.7 MB statement,
        // 130 s in DuckDB's prepare. Each operand read more than once on every row is a hidden column, computed once
        String pure = pure("WITH localdatetime('1984-10-11T12:31:14.645876123') AS dt, datetime("
                + "'1984-10-11T12:31:14.645876123+01:00') AS zdt RETURN dt + duration({months: 1, days: 2, hours: 3})"
                + " AS c, zdt + duration({days: 1, nanoseconds: 1}) AS e, datetime("
                + "'2017-10-29T01:30:00.123456789[Europe/Stockholm]') + duration({days: 1}) AS k");
        assertTrue(pure.length() < SHARED_CAP, pure.length() + " characters");
        assertTrue(pure.contains("_cypher_let_"), pure);
    }

    @Test
    void aStoredZonedValueIsItsInstantWithItsOffsetSlotAsItsZone() {
        // a zoned value class's instance (the study's §12.65): its instant slot read as a zoned time of day (on the
        // epoch day; a slot marked nanoseconds read into the parts, its UTC nanoseconds from the epoch day's
        // midnight, W13 leg 6), compared and ordered by the instant; its zone is its offset slot, which the database
        // reads (§12.75): the zone's text and the value's spelled from it (the wall time the UTC nanoseconds plus the
        // offset's)
        assertEquals("demo::Clock.all()->project(~[_cypher_let_0:c|$c.zt.instant])->extend(~[_cypher_let_2:c|$c._cypher_let_0->toOne()->datePart()->dateDiff($c._cypher_let_0->toOne(), meta::pure::functions::date::DurationUnit.NANOSECONDS), _cypher_let_4:c|$c._cypher_let_0->toOne()->datePart()])->drop(0)->extend(~[_cypher_let_1:c|if($c._cypher_let_0->isEmpty(), |[], |$c._cypher_let_2), _cypher_let_3:c|if($c._cypher_let_0->isEmpty(), |[], |$c._cypher_let_4)])->drop(0)->extend(~[_cypher_let_10:c|((86400000000000 * %1970-01-01->dateDiff($c._cypher_let_3->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + $c._cypher_let_1->toOne())->cast(@Integer)])->drop(0)->extend(~[_cypher_let_7:c|$c._cypher_let_10->mod(86400000000000)->cast(@Integer), _cypher_let_9:c|%1970-01-01->adjust(floor($c._cypher_let_10 / 86400000000000)->cast(@Integer), meta::pure::functions::date::DurationUnit.DAYS)->datePart()])->drop(0)->extend(~[_cypher_let_6:c|if($c._cypher_let_3->isEmpty() || $c._cypher_let_1->isEmpty(), |[], |$c._cypher_let_7), _cypher_let_8:c|if($c._cypher_let_3->isEmpty() || $c._cypher_let_1->isEmpty(), |[], |$c._cypher_let_9)])->drop(0)->extend(~[_cypher_let_5:c|$c._cypher_let_8->toOne()->year()])->drop(0)->extend(~[zt:c|if($c._cypher_let_8->isEmpty() || $c._cypher_let_6->isEmpty(), |[], |(if(($c._cypher_let_5 < 1) || ($c._cypher_let_5 > 9999), {|\nfail('a returned temporal value of a year outside 1 to 9999 is not supported');\ntoString($c._cypher_let_5 + 10000)->substring(2);\n}, |toString($c._cypher_let_5 + 10000)->substring(2)) + '-' + toString($c._cypher_let_8->toOne()->monthNumber() + 100)->substring(2) + '-' + toString($c._cypher_let_8->toOne()->dayOfMonth() + 100)->substring(2)) + 'T' + (toString(floor($c._cypher_let_6->toOne() / 3600000000000) + 100)->substring(2) + ':' + toString(floor($c._cypher_let_6->toOne()->rem(3600000000000)->cast(@Integer) / 60000000000) + 100)->substring(2) + ':' + toString(floor($c._cypher_let_6->toOne()->rem(60000000000)->cast(@Integer) / 1000000000) + 100)->substring(2) + '.' + toString($c._cypher_let_6->toOne()->rem(1000000000)->cast(@Integer) + 1000000000)->substring(2))), later:c|if(!($c._cypher_let_3->isEmpty() || $c._cypher_let_1->isEmpty()) && (((86400000000000 * %1970-01-01->dateDiff($c._cypher_let_3->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + $c._cypher_let_1->toOne())->cast(@Integer) > ((86400000000000 * %1970-01-01->dateDiff(%1970-01-01->meta::legend::lite::atTime(%11:00:00)->datePart(), meta::pure::functions::date::DurationUnit.DAYS)) + (1000 * %1970-01-01->meta::legend::lite::atTime(%11:00:00)->datePart()->dateDiff(%1970-01-01->meta::legend::lite::atTime(%11:00:00), meta::pure::functions::date::DurationUnit.MICROSECONDS)))->cast(@Integer)), |true, |if(!($c._cypher_let_3->isEmpty() || $c._cypher_let_1->isEmpty()) && !(((86400000000000 * %1970-01-01->dateDiff($c._cypher_let_3->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) + $c._cypher_let_1->toOne())->cast(@Integer) > ((86400000000000 * %1970-01-01->dateDiff(%1970-01-01->meta::legend::lite::atTime(%11:00:00)->datePart(), meta::pure::functions::date::DurationUnit.DAYS)) + (1000 * %1970-01-01->meta::legend::lite::atTime(%11:00:00)->datePart()->dateDiff(%1970-01-01->meta::legend::lite::atTime(%11:00:00), meta::pure::functions::date::DurationUnit.MICROSECONDS)))->cast(@Integer)), |false, |[])), _cypher_1:c|if($c._cypher_let_3->isEmpty() || $c._cypher_let_1->isEmpty(), |[], |$c._cypher_let_10)])->select(~[zt, later, _cypher_1])->sort([~_cypher_1->ascending(meta::pure::functions::relation::NullOrder.LAST)])->select(~[zt, later])->extend(~[_cypher_2:e|$e.zt->meta::legend::lite::dateTimeOfText()])->select(~[_cypher_2, later])->rename(~_cypher_2, ~zt)", pure("MATCH (c:Clock) RETURN c.zt AS zt, c.zt > time('11:00Z') AS later ORDER BY zt"));
        String zone = pure("MATCH (c:Clock) RETURN c.zt.timezone");
        assertTrue(zone.contains("$c.zt.offsetSeconds->toOne()->abs()"), zone);
        String text = pure("MATCH (c:Clock) RETURN toString(c.zt)");
        // (the offset slot read once, into a column the text's arithmetic reads: the translator's let, Shares)
        assertTrue(text.contains("_cypher_let_1:c|$c.zt.offsetSeconds"), text);
        assertTrue(text.contains("+ (1.0E9 * $c._cypher_let_1->toOne())->cast(@Integer)"), text);
    }

    // ------------------------------------------------------------------ a time of day (the study's §12.7)

    @Test
    void aLocalTimeIsPuresStrictTimeReadThroughTheEpochDay() {
        // a written time is Pure's time literal; one from components is the date-time at it on 1970-01-01, its time
        // taken back (lite's atTime and timeOf: the date is a token the database never sees as a value)
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[t:r|%12:31:14.645,"
                        + " u:r|1970->date(1, 1, 12, 0, 0)->meta::legend::lite::timeOf(),"
                        + " h:r|%1970-01-01->meta::legend::lite::atTime(%12:31:14.645)->hour()])->select(~[t, u, h])",
                pure("RETURN localtime('12:31:14.645') AS t, localtime({hour: 12}) AS u,"
                        + " localtime('12:31:14.645').hour AS h"));
        // Neo4j's errors, spelled as Neo4j spells them
        invalid("RETURN localtime({minute: 5}) AS t", "hour must be specified");
        invalid("RETURN localtime('12:00').year AS y", "Unsupported field: Year");
        invalid("RETURN localtime.truncate('month', localtime('12:00')) AS t",
                "Unit is too large to be used for truncation");
        invalid("RETURN localtime(date('1984-10-11')) AS t", "Cannot get the time of: 1984-10-11");
        invalid("RETURN localtime('25:00') AS t", "Text cannot be parsed to a LocalTime: \"25:00\"");
        // a zoned time of day is its instant on the epoch day, the offset beside: the wall time at the epoch day,
        // less the offset (the adjust steps the database computes); a field reads the wall time back
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[t:r|%1970-01-01->meta::legend::lite::atTime(%12:00:00)"
                        + "->adjust(-1, meta::pure::functions::date::DurationUnit.HOURS),"
                        + " h:r|%1970-01-01->meta::legend::lite::atTime(%12:00:00)"
                        + "->adjust(-1, meta::pure::functions::date::DurationUnit.HOURS)"
                        + "->adjust(1, meta::pure::functions::date::DurationUnit.HOURS)->hour()])->select(~[t, h])",
                pure("RETURN time('12:00+01:00') AS t, time('12:00+01:00').hour AS h"));
        invalid("RETURN time('25:00+01:00') AS t", "Text cannot be parsed to a Time: \"25:00+01:00\"");
        invalid("RETURN time('12:00[Europe/Stockholm]') AS t", "Using a named time zone e.g. [UTC] is not valid");
        // refused by name: a named zone for a time (its offset is the clock's)
        unsupported("RETURN time({hour: 12, timezone: 'Europe/Stockholm'}) AS t", "a named zone for a time of day");
        unsupported("RETURN time({timezone: '+01:00'}) AS t", "the current time in a zone");
        // a list of times: the Variant of the oracle's spellings, which the database parses back (the study's §12.76)
        String times = pure("RETURN [localtime('12:31'), localtime('13:00')] AS l");
        assertTrue(times.contains("fromJson(") && times.contains("Time("), times);
    }

    // ------------------------------------------------------------------ a zoned date-time (the study's §12.8)

    @Test
    void aLabelLessNodeReadsEveryClassOfTheModelAsConcatenatedArms() {
        // MATCH (n): one arm per class, each projected to the same columns, the arms concatenated; a property a
        // class lacks reads as null where another declares it (the study's §12.13)
        String p = pure("MATCH (n) WHERE n.name IS NOT NULL RETURN n.name AS name ORDER BY name");
        assertTrue(p.contains("demo::City.all()"), p);
        assertTrue(p.contains("demo::Firm.all()"), p);
        assertTrue(p.contains("demo::Person.all()"), p);
        assertTrue(p.contains("->concatenate("), p);
        assertTrue(p.indexOf("->sort(") > p.lastIndexOf("->concatenate("), "the sort reads the whole: " + p);
        String q = pure("MATCH (n) RETURN count(*) AS c");
        assertTrue(q.contains("->concatenate(") && q.indexOf("->aggregate(") > q.lastIndexOf("->concatenate("),
                "the aggregate reads the whole: " + q);
        String r = pure("MATCH (n) RETURN n.age AS age");
        assertTrue(r.contains("[]->cast(@Integer)"), "a class without the property reads null: " + r);
        // refused by name: a typed hop after a label-less one (its arms continue only through an untyped hop, the
        // study's §12.32; a pattern sharing no variable is a product, §12.33), a property declared nowhere
        unsupported("MATCH (n) MATCH (n)-[:firm]->(f:Firm) RETURN f.legalName", "a MATCH after a label-less pattern");
        invalid("MATCH (n) RETURN n.nope AS x", "the graph is the model");
    }

    @Test
    void aNanosecondValueIsItsDateAndItsNanosecondsSinceMidnight() {
        // W13 (docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md): a value finer than a microsecond is its parts, the date
        // as written and the nanoseconds since midnight the database adds up from the written clock (a float
        // product over a literal: two 32-bit literals' product overflows); returned, the ISO text the database
        // writes of them, which the result reads as the date-time (lite's dateTimeOfText); a field the database's
        // remainder or quotient of the nanoseconds. No nanosecond epoch: every database computes it.
        String p = pure("RETURN localdatetime('1984-10-11T12:31:14.645876123') AS d,"
                + " localdatetime('1984-10-11T12:31:14.645876123').nanosecond AS n");
        // (the text built in the projection, read as the value at the final one: the returned split columns' egress)
        assertTrue(p.contains("$e.d->meta::legend::lite::dateTimeOfText()"), p);
        assertTrue(p.contains("year(%1984-10-11)"), p);
        assertTrue(p.contains("(1.0E9 * 14)->cast(@Integer)) + 645876123"), p);
        assertTrue(p.contains("->rem(1000000000)->cast(@Integer)])"), p);
        assertFalse(p.contains("NANOSECONDS"), p);
        // a time of day is its nanoseconds since midnight alone, returned through lite's timeOfText
        assertTrue(pure("RETURN localtime('12:31:14.645876123') AS t").contains("meta::legend::lite::timeOfText("));
        // a year beyond the databases' nanosecond timestamps is a date like any other
        assertTrue(pure("RETURN localdatetime({year: 1, month: 1, day: 1, hour: 1, minute: 1, second: 1,"
                + " nanosecond: 1}) AS t").contains("dateTimeOfText"));
        // a nanosecond component of the row: the parts carry whatever the row computes (W13 leg 4)
        String row = pure("UNWIND [1] AS i RETURN localtime({hour: 12, minute: 0, second: 0, nanosecond: i}) AS t");
        assertTrue(row.contains("meta::legend::lite::timeOfText("), row);
        assertFalse(row.contains("NANOSECONDS"), row);
    }

    @Test
    void aZonedNanosecondValueIsItsInstantsParts() {
        // W13 leg 3: a zoned value finer than a microsecond is its instant's parts, the UTC date and the UTC
        // nanoseconds since midnight: the wall time's moved back by the offset's (a sum the database adds), the
        // clock's whole days carried into the date; returned, the instant's ISO text. No nanosecond epoch.
        String p = pure("RETURN datetime('1984-10-11T12:31:14.645876123+01:00') AS d,"
                + " time('12:31:14.645876123+01:00') AS t");
        // (the text built in the projection, read as the value at the final one: the returned split columns' egress)
        assertTrue(p.contains("$e.d->meta::legend::lite::dateTimeOfText()"), p);
        assertTrue(p.contains("- (1.0E9 * (3600 * 1))->cast(@Integer)"), p);
        assertTrue(p.contains("->mod(86400000000000)"), p);
        assertFalse(p.contains("NANOSECONDS"), p);
        // a region's offset is the database's reading of the instant's microseconds in it
        String r = pure("RETURN datetime('2017-07-01T12:00:00.000000001[Europe/Stockholm]') AS d");
        assertTrue(r.contains("meta::legend::lite::instantIn("), r);
        assertFalse(r.contains("NANOSECONDS"), r);
        // datetime.fromepoch's nanoseconds are a second's (Neo4j's)
        invalid("RETURN datetime.fromepoch(0, 1000000000) AS d", "Invalid nanosecond: 1000000000");
    }

    @Test
    void aZonedDateTimeIsItsInstantWithTheZoneCarriedBeside() {
        // a local date-time at the default zone (UTC) is the instant itself; a written offset is spelled as the
        // adjust steps the database computes (the instant is the wall time less the offset); the zone rides beside
        // the value, so a field reads the wall time back
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[d:r|%1984-10-11T12:31:14,"
                        + " z:r|%1984-10-11T12:31:14->adjust(-1, meta::pure::functions::date::DurationUnit.HOURS),"
                        + " h:r|%1984-10-11T12:31:14->adjust(-1, meta::pure::functions::date::DurationUnit.HOURS)"
                        + "->adjust(1, meta::pure::functions::date::DurationUnit.HOURS)->hour()])->select(~[d, z, h])",
                pure("RETURN datetime(localdatetime('1984-10-11T12:31:14')) AS d,"
                        + " datetime('1984-10-11T12:31:14+01:00') AS z, datetime('1984-10-11T12:31:14+01:00').hour AS h"));
        // Neo4j's errors, spelled as Neo4j spells them
        invalid("RETURN datetime(date('1984-10-11')) AS d", "Cannot select datetime from: 1984-10-11");
        invalid("RETURN datetime({year: 1984, month: 10, day: 11, timezone: '+0100'}) AS d",
                "Invalid value for TimeZone: Text '+0100' could not be parsed at index 0");
        invalid("RETURN localdatetime('1984-10-11T12:00').timezone AS z", "Cannot get the offset of: 1984-10-11T12:00:00");
        invalid("RETURN localdatetime({year: 1984, month: 1, day: 1, timezone: '+01:00'}) AS d",
                "Cannot assign time zone if also assigning other fields.");
        invalid("RETURN datetime('1984-10-11T12:31:14+2') AS d", "Invalid value for TimeZone: Text '+2'");
        // a named zone's text: its offset at the instant (the database's wall time against it), then its name in
        // brackets (the study's §12.75)
        String named = pure("RETURN toString(datetime({year: 2017, month: 8, day: 8, hour: 12, timezone: 'Europe/Stockholm'})) AS s");
        assertTrue(named.contains("localIn('Europe/Stockholm')") && named.contains("'[Europe/Stockholm]'"), named);
        // refused by name: a signed year, a zoned value of a list read in its zone
        unsupported("RETURN datetime('+999999999-12-31T12:31:14Z') AS d", "a signed year");
        unsupported("UNWIND [datetime('1984-10-11T12:31:14+01:00'), datetime('1984-10-11T12:31:14Z')] AS d RETURN d.hour",
                "whose zone is not carried here");
    }

    // ------------------------------------------------------------------ a duration (the study's §12.9)

    @Test
    void aDurationIsAnInstanceOfTheModelsDurationClass() {
        // the demo model declares demo::values::Duration (the four slots): a duration is an instance of it
        assertTrue(pure("RETURN duration({days: 1}) AS d").contains("^demo::values::Duration(months="),
                pure("RETURN duration({days: 1}) AS d"));
        // a model declaring none: refused by name, with the declaration to add
        CypherToPure.Model without = new Hiding(MODEL, "demo::values::Duration");
        SqlTranslationException e = assertThrows(SqlTranslationException.class,
                () -> CypherToPure.translate("RETURN duration({days: 1}) AS d", without));
        assertTrue(e.getMessage().contains("a duration needs one class named Duration in the model"), e.getMessage());
        e = assertThrows(SqlTranslationException.class, () -> CypherToPure.translate(
                "RETURN duration.between(date('1984-10-11'), date('2015-06-24')) AS d", without));
        assertTrue(e.getMessage().contains("a duration needs one class named Duration in the model"), e.getMessage());
    }

    // ------------------------------------------------------------------ shapes

    @Test
    void aMatchReturnIsGetAllFilterProject() {
        assertEquals("demo::Person.all()->filter(p|$p.name == 'x')->project(~['p.age':p|$p.age])",
                pure("MATCH (p:Person) WHERE p.name = 'x' RETURN p.age"));
    }

    @Test
    void anAliasNamesTheColumnAndSortSliceDistinctFollow() {
        assertEquals("demo::Person.all()->project(~[who:p|$p.name, age:p|$p.age])->distinct()"
                        + "->sort([~age->descending(meta::pure::functions::relation::NullOrder.FIRST),"
                        + " ~who->ascending(meta::pure::functions::relation::NullOrder.LAST)])->drop(1)->limit(2)",
                pure("MATCH (p:Person) RETURN DISTINCT p.name AS who, p.age AS age ORDER BY age DESC, who SKIP 1"
                        + " LIMIT 2"));
    }

    @Test
    void aSortByAnExpressionNotProjectedIsAHiddenColumnUnderASliceAndRefusedWithout() {
        assertEquals("demo::Person.all()->project(~[who:p|$p.name, _cypher_1:p|$p.age])"
                        + "->sort([~_cypher_1->ascending(meta::pure::functions::relation::NullOrder.LAST)])"
                        + "->select(~[who])->limit(5)",
                pure("MATCH (p:Person) RETURN p.name AS who ORDER BY p.age LIMIT 5"));
        // without a SKIP or LIMIT the same hidden key: the result render carries it as a hidden output of the
        // ordered relation (Render.withSortOutputs, the study's §12.53; the review's finding 6 had it refused)
        assertEquals("demo::Person.all()->project(~[who:p|$p.name, _cypher_1:p|$p.age])"
                        + "->sort([~_cypher_1->ascending(meta::pure::functions::relation::NullOrder.LAST)])"
                        + "->select(~[who])",
                pure("MATCH (p:Person) RETURN p.name AS who ORDER BY p.age"));
    }

    @Test
    void aSortOverTheProjectedColumnsAfterAGroupingIsComputedAfterIt() {
        // the review's finding 7: Neo4j reads ORDER BY a + 'x' over the items a and c
        assertEquals("demo::Person.all()->project(~[a:p|$p.name, _cypher_1:p|1])"
                        + "->groupBy(~[a], ~[c:x|$x._cypher_1:y|$y->count()])->select(~[a, c])"
                        + "->extend(~[_cypher_2:r|$r.a + 'x'])"
                        + "->sort([~_cypher_2->ascending(meta::pure::functions::relation::NullOrder.LAST)])"
                        + "->select(~[a, c])",
                pure("MATCH (p:Person) RETURN p.name AS a, count(*) AS c ORDER BY a + 'x'"));
    }

    @Test
    void twoRelationshipsOfOneTypeInOneMatchAreRefusedUntilIdentity() {
        // the review's finding 1: Neo4j binds the two slots to different relationships (relationship uniqueness),
        // spelled since the study's §12.48 as the relationships' identities pairwise distinct (over a property or
        // association, the ordered pair of the ends' identities, S74); an unnamed one of a repeated type is named
        // (the chain fans out through employees: the pair's columns ride the materialized rows, the identities'
        // texts compared over them)
        String chain = pure("MATCH (p:Person)-[:firm]->(f:Firm)<-[:firm]-(q:Person) RETURN q.name AS n");
        assertTrue(chain.contains("$r._cypher_8->toOne() + ':' + $r._cypher_9->toOne() + '>' + $r._cypher_10->toOne()")
                && chain.contains("->toOne() != if($r._cypher_15->isEmpty()"), chain);
        // (over to-one hops the pair is spelled in the graph: the identities navigated from the root)
        String toOne = pure("MATCH (p:Person)-[r:firm]->(f:Firm)-[s:hq]->(c:City) WHERE r = s RETURN c.name AS n");
        assertTrue(toOne.contains("'firm' + ':' + $p->meta::legend::lite::elementId()->toOne() + '>' + "
                + "$p.firm->meta::legend::lite::elementId()->toOne()"), toOne);
        // (two firm relationships from one person, in one MATCH: distinct by identity, so a to-one firm finds none)
        String fromOne = pure("MATCH (p:Person)-[:firm]->(f:Firm), (p)-[:firm]->(g:Firm) RETURN f.legalName AS l");
        assertTrue(fromOne.contains("->toOne() != "), fromOne);
        // in two MATCH clauses they may be the same relationship, as in Neo4j
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty() && $p.firm->isNotEmpty())"
                        + "->project(~['g.legalName':p|$p.firm.legalName])",
                pure("MATCH (p:Person)-[:firm]->(f:Firm) MATCH (p)-[:firm]->(g:Firm) RETURN g.legalName"));
    }

    @Test
    void aFloatLiteralIsADouble() {
        // the review's finding 3: a bare Float literal lowers to a decimal (S9); Cypher's float is IEEE 754
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[s:r|toFloat(0.1) + toFloat(0.2),"
                        + " eq:r|toFloat(0.1) + toFloat(0.2) == toFloat(0.3)])->select(~[s, eq])",
                pure("RETURN 0.1 + 0.2 AS s, 0.1 + 0.2 = 0.3 AS eq"));
        // the negation of a float is IEEE's (-0.0 is a value of its own); of an integer, Pure's minus
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[z:r|toFloat(0.0) * -1.0, n:r|-3])->select(~[z, n])",
                pure("RETURN -0.0 AS z, -(3) AS n"));
    }

    @Test
    void aToOneHopIsANavigationAndMustFindANode() {
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty())"
                        + "->project(~['p.name':p|$p.name, 'f.legalName':p|$p.firm.legalName])",
                pure("MATCH (p:Person)-[:firm]->(f:Firm) RETURN p.name, f.legalName"));
    }

    @Test
    void aTwoHopChainNavigatesTwice() {
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty() && $p.firm.hq->isNotEmpty())"
                        + "->project(~['c.name':p|$p.firm.hq.name])",
                pure("MATCH (p:Person)-[:firm]->(:Firm)-[:hq]->(c:City) RETURN c.name"));
    }

    @Test
    void anIncomingHopOrAToManyOneIsReadFromTheNodeThatHoldsTheToOneEnd() {
        String expected = "demo::Person.all()->filter(p|$p.firm->isNotEmpty())"
                + "->project(~['f.legalName':p|$p.firm.legalName, 'p.name':p|$p.name])";
        assertEquals(expected, pure("MATCH (f:Firm)<-[:firm]-(p:Person) RETURN f.legalName, p.name"));
        assertEquals(expected, pure("MATCH (f:Firm)-[:employees]->(p:Person) RETURN f.legalName, p.name"));
        assertEquals(expected, pure("MATCH (p:Person)<-[:employees]-(f:Firm) RETURN f.legalName, p.name"));
    }

    @Test
    void aLabelLessNodeTakesThePropertysType() {
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty())"
                        + "->project(~['f.legalName':p|$p.firm.legalName])",
                pure("MATCH (p:Person)-[:firm]->(f) RETURN f.legalName"));
    }

    @Test
    void aPropertyMapIsAnEqualityFilter() {
        assertEquals("demo::Person.all()->filter(p|$p.name == 'Bob')->project(~['p.age':p|$p.age])",
                pure("MATCH (p:Person {name: 'Bob'}) RETURN p.age"));
    }

    @Test
    void aSecondPatternExtendsTheFirstFromABoundNode() {
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty() && $p.firm.hq->isNotEmpty())"
                        + "->project(~['c.name':p|$p.firm.hq.name])",
                pure("MATCH (p:Person)-[:firm]->(f:Firm) MATCH (f)-[:hq]->(c:City) RETURN c.name"));
    }

    @Test
    void aWithOfPatternVariablesKeepsTheGraph() {
        assertEquals("demo::Person.all()->filter(p|$p.age->isNotEmpty() && ($p.age->toOne() > 1))"
                        + "->project(~['q.name':p|$p.name])",
                pure("MATCH (p:Person) WITH p AS q WHERE q.age > 1 RETURN q.name"));
    }

    @Test
    void aWithOfValuesProjectsAndTheRestReadsItsColumns() {
        // the WITH's WHERE is a hidden column of its projection (it may read the scope before the projection too),
        // filtered on and dropped before the clauses after
        assertEquals("demo::Person.all()->project(~[name:p|$p.name, age:p|$p.age,"
                        + " _cypher_1:p|$p.age->isNotEmpty() && ($p.age->toOne() > 1)])"
                        + "->filter(r|$r._cypher_1)->select(~[name, age])"
                        + "->extend(~[twice:r|if($r.age->isEmpty(), |[], |$r.age->toOne() * 2)])"
                        + "->select(~[name, twice])",
                pure("MATCH (p:Person) WITH p.name AS name, p.age AS age WHERE age > 1 RETURN name, age * 2 AS twice"));
    }

    @Test
    void aNodeReturnedIsItsLabelAndItsProperties() {
        // C8 (the study's §12.14): a node as a value is the JSON the judge reads as a node, built as text by the
        // database -- its label, then each property that holds a value (Neo4j stores no null)
        assertEquals("demo::Person.all()->project(~[p:p|fromJson((('{\"@labels\":' + '[\"Person\"]') + if($p->meta::legend::lite::elementId()->isEmpty(), |'', |',\"@id\":' + ('\"' + $p->meta::legend::lite::elementId()->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + ((if(if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[]))->isEmpty(), |'', |',\"active\":' + if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[]))->toOne()->toString()) + if($p.age->isEmpty(), |'', |',\"age\":' + $p.age->toOne()->toString())) + (',\"name\":' + ('\"' + $p.name->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + '}')])",
                pure("MATCH (p:Person) RETURN p"));
    }

    @Test
    void aNodeThroughWithIsCarriedAsItsColumns() {
        // the node's label and properties as columns of the WITH, so ORDER BY p.age sorts a typed column and the
        // RETURN reads p.name as one; the node's own name is a virtual column
        assertEquals("demo::Person.all()->project(~[_cypher_1:p|'[\"Person\"]', _cypher_2:p|if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[])), _cypher_3:p|$p.age, _cypher_4:p|$p.name, _cypher_5:p|$p->meta::legend::lite::elementId()])->sort([~_cypher_3->ascending(meta::pure::functions::relation::NullOrder.LAST)])->limit(1)->extend(~['p.name':r|$r._cypher_4, p:r|fromJson((('{\"@labels\":' + $r._cypher_1->toOne()) + if($r._cypher_5->isEmpty(), |'', |',\"@id\":' + ('\"' + $r._cypher_5->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + ((if(if($r._cypher_2->isNotEmpty() && $r._cypher_2->toOne(), |true, |if($r._cypher_2->isNotEmpty() && !$r._cypher_2->toOne(), |false, |[]))->isEmpty(), |'', |',\"active\":' + if($r._cypher_2->isNotEmpty() && $r._cypher_2->toOne(), |true, |if($r._cypher_2->isNotEmpty() && !$r._cypher_2->toOne(), |false, |[]))->toOne()->toString()) + if($r._cypher_3->isEmpty(), |'', |',\"age\":' + $r._cypher_3->toOne()->toString())) + (',\"name\":' + ('\"' + $r._cypher_4->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + '}')])->select(~['p.name', p])",
                pure("MATCH (p:Person) WITH p ORDER BY p.age LIMIT 1 RETURN p.name, p"));
    }

    @Test
    void aPatternThatFansOutReadsOneRowPerPair() {
        // the study's §12.16: no to-one root, so the first node is the root and the to-many hop (employees) fans
        // out; the graph is materialized to rows at the end of the MATCH (every node carried as its columns, the
        // fanning node's labels navigating its path so a node without a property still counts), the WHERE read over
        // those rows, and the RETURN projects them
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty() && $p.firm.employees->isNotEmpty())->project(~[_cypher_1:p|'[\"Person\"]', _cypher_2:p|if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[])), _cypher_3:p|$p.age, _cypher_4:p|$p.name, _cypher_5:p|$p->meta::legend::lite::elementId(), _cypher_6:p|'[\"Firm\"]', _cypher_7:p|$p.firm.legalName, _cypher_8:p|$p.firm->meta::legend::lite::elementId(), _cypher_9:p|'[\"Person\"]', _cypher_10:p|if($p.firm.employees.active->isNotEmpty() && $p.firm.employees.active->toOne(), |true, |if($p.firm.employees.active->isNotEmpty() && !$p.firm.employees.active->toOne(), |false, |[])), _cypher_11:p|$p.firm.employees.age, _cypher_12:p|$p.firm.employees.name, _cypher_13:p|$p.firm.employees->meta::legend::lite::elementId()])->filter(r|$r._cypher_11->isNotEmpty() && ($r._cypher_11->toOne() > 30))->extend(~['q.name':r|$r._cypher_12])->select(~['q.name'])",
                pure("MATCH (p:Person)-[:firm]->(f:Firm)-[:employees]->(q:Person) WHERE q.age > 30 RETURN q.name"));
    }

    @Test
    void percentilesArePuresPercentileAggregateAndAMapIsIndexedByARowValue() {
        // the study's §12.30
        assertEquals("demo::Person.all()->project(~[_cypher_1:p|$p.age])->aggregate(~[_cypher_2:x|$x._cypher_1:y|$y"
                + "->percentile(0.5, true, false)])->extend(~[d:r|if($r._cypher_2->isEmpty(), |[]->cast(@Integer),"
                + " |$r._cypher_2->toOne()->cast(@Integer))])->select(~[d])",
                pure("MATCH (p:Person) RETURN percentileDisc(p.age, 0.5) AS d"));
        // a map indexed by a value of the row: the database's choice among its entries, strict over the key
        assertEquals("['a', 'b']->flatten(~k)->extend(~[v:r|if($r.k->isEmpty(), |[], |if($r.k->toOne() == 'a', |1,"
                + " |if($r.k->toOne() == 'b', |2, |[]->cast(@Integer))))])->select(~[v])",
                pure("UNWIND ['a', 'b'] AS k RETURN {a: 1, b: 2}[k] AS v"));
        unsupported("MATCH (p:Person) RETURN percentileCont(p.age, 0.5) AS c", "percentileCont over values that are not"
                + " floats");
        invalid("MATCH (p:Person) RETURN percentileDisc(p.age, 1.5) AS d", "must be a number in the range 0.0 to 1.0");
    }

    @Test
    void aLabelPredicateReadsTheNodesLabelsAndSeveralTypesAreArms() {
        // the study's §12.35: n:Label tests the node's labels text (a constant for a known class: the database
        // folds it), null for an absent node; a relationship of several types is one arm per distinct type
        assertEquals("demo::Person.all()->project(~[a:p|'[\"Person\"]'->contains('\"Person\"'), b:p|'[\"Person\"]'->contains('\"Firm\"')])",
                pure("MATCH (p:Person) RETURN p:Person AS a, p:Firm AS b"));
        assertEquals("demo::Person.all()->project(~[a:p|if(!$p.firm->isEmpty() && '[\"Firm\"]'->contains('\"Firm\"'), |true, |if(!$p.firm->isEmpty() && !'[\"Firm\"]'->contains('\"Firm\"'), |false, |[]))])",
                pure("MATCH (p:Person) OPTIONAL MATCH (p)-[:firm]->(f:Firm) RETURN f:Firm AS a"));
        invalid("RETURN 1:Person AS a", "expected Node or Relationship but was Integer");
        // a relationship's label expression (the study's §12.79): its type compared with each label written
        String typed = pure("MATCH (p:Person)-[r:firm]->(f) RETURN r:firm AS a, r:firm:employees AS b");
        assertTrue(typed.contains("'firm' == 'firm'")
                && typed.contains("('firm' == 'firm') && ('firm' == 'employees')"), typed);
        // (a type spelled twice is one type: one arm, read as rows)
        String twice = pure("MATCH (p:Person)-[:firm|:firm]->(f) RETURN f.legalName AS l");
        assertTrue(twice.contains("$p.firm.legalName") && !twice.contains("->concatenate("), twice);
        assertTrue(pure("MATCH (n)-[r:firm|employees]->(x) RETURN type(r) AS t").contains("->concatenate("));
        // (two nodes compare by identity: each carried as its elementId, the mapped instance's key)
        String byIdentity = pure("MATCH (a:Person), (b:Person) WHERE a = b RETURN a.name AS n");
        assertTrue(byIdentity.contains("_cypher_5:a|$a->meta::legend::lite::elementId()")
                && byIdentity.contains("->filter(r|$r._cypher_5 == $r._cypher_10)"), byIdentity);
    }

    @Test
    void listsOfKnownShapeOrderElementByElement() {
        // the study's §12.34: the first pair that is not equal decides (null when either is null or the two do
        // not order), the lengths when every pair is equal; a list and a value of another type do not order
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[a:r|((1 == 3) && (((2 == 4) && false) || ((2 != 4) && (2 < 4)))) || ((1 != 3) && (1 < 3)), b:r|if(false, |true, |if(false, |false, |[])), c:r|((1 == 3) && (((2 == 4) && false) || ((2 != 4) && (2 < 4)))) || ((1 != 3) && (1 < 3)) == false])->select(~[a, b, c])",
                pure("RETURN [1, 2] < [3, 4] AS a, [1, 2] < true AS b, ([1, 2] < [3, 4]) IN [[3, 4], false] AS c"));
        assertEquals("demo::Person.all()->project(~[a:p|if((($p.age->isNotEmpty() && ($p.age->toOne() == 2)) && (((1 == 1) && false) || ((1 != 1) && (1 < 1)))) || (($p.age->isNotEmpty() && ($p.age->toOne() != 2)) && ($p.age->isNotEmpty() && ($p.age->toOne() < 2))), |true, |if((($p.age->isNotEmpty() && ($p.age->toOne() == 2)) && (((1 == 1) && true) || ((1 != 1) && !(1 < 1)))) || (($p.age->isNotEmpty() && ($p.age->toOne() != 2)) && ($p.age->isNotEmpty() && !($p.age->toOne() < 2))), |false, |[]))])",
                pure("MATCH (p:Person) RETURN [p.age, 1] < [2, 1] AS a"));
        unsupported("RETURN [[1]] < [[2]] AS a", "ordering lists holding lists or maps");
        unsupported("RETURN {a: 1} < {a: 2} AS a", "ordering maps");
    }

    @Test
    void aCartesianProductWithALabelLessPatternAndWithStar() {
        // the study's §12.33: WITH * keeps the graph; a pattern sharing no variable with it is a join of the two
        // patterns' rows (§12.28), a label-less one the union of its arms' rows
        assertEquals("demo::Person.all()->project(~[_cypher_1:p|'[\"Person\"]', _cypher_2:p|if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[])), _cypher_3:p|$p.age, _cypher_4:p|$p.name, _cypher_5:p|$p->meta::legend::lite::elementId()])->join(demo::Firm.all()->project(~[_cypher_6:f|'[\"Firm\"]', _cypher_7:f|$f.legalName, _cypher_8:f|$f->meta::legend::lite::elementId()]), meta::pure::functions::relation::JoinKind.INNER, {l, r|true})->extend(~[a:r|$r._cypher_4, b:r|$r._cypher_7])->select(~[a, b])",
                pure("MATCH (p:Person) WITH * MATCH (f:Firm) RETURN p.name AS a, f.legalName AS b"));
        assertTrue(pure("MATCH (n), (m) RETURN n.name AS a, m.name AS b").contains("->join("));
        assertTrue(pure("MATCH (a) WITH a MATCH (b) RETURN a, b").contains("->join("));
        // WITH * keeps a path with the nodes and relationships it reads (one named synthetically)
        assertTrue(pure("MATCH q = (p:Person)-[:firm]->(f) WITH * MATCH (c:City) RETURN q, c.name AS n")
                .contains("@path"));
        // (re-anchoring after rows reads from the carried node's class, the study's §12.49: a label-less node carried
        // as one of several classes has none to read from)
        // (a node carried as one of several classes re-anchors from every class, label-less at the pattern's first
        // node, joined on its identity: the study's §12.52)
        assertTrue(pure("MATCH (n), (m) WITH n, m MATCH (n)-->(x) RETURN x").contains("->join("));
        // a list comprehension's WHERE keeps an element when its condition is true: null, like false, drops it
        assertEquals("[1, 2, 3]->flatten(~value)->extend(~[x:r|[1, 2, 3]->filter(x|$r.value->isNotEmpty() && ($x < $r.value->toOne()))->size()])->select(~[x])",
                pure("UNWIND [1, 2, 3] AS value RETURN size([x IN [1, 2, 3] WHERE x < value]) AS x"));
    }

    @Test
    void anUntypedHopFromABoundNodeIsOneArmPerWayTheModelReadsIt() {
        // the study's §12.32: after a WITH that carried the node (the graph kept), or a MATCH before; the arms
        // materialized to rows and concatenated (one way here: Person.firm)
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty())->project(~[_cypher_1:p|'[\"Person\"]', _cypher_2:p|if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[])), _cypher_3:p|$p.age, _cypher_4:p|$p.name, _cypher_5:p|$p->meta::legend::lite::elementId(), _cypher_6:p|'[\"Firm\"]', _cypher_7:p|$p.firm.legalName, _cypher_8:p|$p.firm->meta::legend::lite::elementId()])->extend(~[n:r|$r._cypher_4, l:r|$r._cypher_7])->select(~[n, l])",
                pure("MATCH (p:Person) WITH p MATCH (p)-->(x) RETURN p.name AS n, x.legalName AS l"));
        // the relationship's type is the way's (hq, the one end leaving Firm)
        assertEquals("demo::Firm.all()->filter(f|$f.hq->isNotEmpty())->project(~[_cypher_1:f|'[\"Firm\"]', _cypher_2:f|$f.legalName, _cypher_3:f|$f->meta::legend::lite::elementId(), _cypher_4:f|'[\"City\"]', _cypher_5:f|$f.hq.name, _cypher_6:f|$f.hq->meta::legend::lite::elementId(), _cypher_7:f|'hq', _cypher_8:f|$f->meta::legend::lite::elementId(), _cypher_9:f|$f.hq->meta::legend::lite::elementId()])->extend(~[l:r|$r._cypher_2, t:r|$r._cypher_7])->select(~[l, t])",
                pure("MATCH (f:Firm) MATCH (f)-[r]->(x) RETURN f.legalName AS l, type(r) AS t"));
        // a MATCH after rows from a node the rows carry: the pattern read from the node's class and joined back on
        // its identity (re-anchoring, the study's §12.49)
        String anchored = pure("MATCH (f:Firm) WITH f.legalName AS l, f MATCH (f)-->(x) RETURN l");
        assertTrue(anchored.contains("->join(demo::Firm.all()") && anchored.contains("JoinKind.INNER, {l, r|$l._cypher_3"
                + " == $r._cypher_"), anchored);
        unsupported("MATCH (f:Firm)-[:employees]->(p:Person) MATCH (f)-->(p) RETURN p.name",
                "two ends are both bound already");
        // no way of the model leaves Firm to a Person (the study's §12.36): the rows in hand limited to none, x
        // an absent node (its labels column null)
        assertEquals("demo::Firm.all()->project(~[_cypher_1:f|'[\"Firm\"]', _cypher_2:f|$f.legalName, _cypher_3:f|$f->meta::legend::lite::elementId()])->limit(0)->extend(~[_cypher_4:r|[]->cast(@String)])->extend(~[n:r|[]])->select(~[n])",
                pure("MATCH (f:Firm) MATCH (f)-->(x:Person) RETURN x.name AS n"));
        unsupported("MATCH (f:Firm) MATCH q = (f)-->(x) RETURN q", "a named path over an untyped relationship");
    }

    @Test
    void aHopFromAnAbsentNodeFindsNothing() {
        // the study's §12.36: x, a node the OPTIONAL MATCH never finds (no `nothing` end leaves Person), carried
        // through a WITH as null everywhere: a MATCH from it reads no row (the rows in hand limited to none), b
        // absent; an OPTIONAL MATCH keeps every row, b and r absent (type(r) null)
        String none = pure("MATCH (p:Person) OPTIONAL MATCH (p)-[:nothing]->(x) WITH x, 1 AS one MATCH (x)-->(b)"
                + " RETURN b");
        assertTrue(none.contains("->limit(0)"), none);
        String kept = pure("MATCH (p:Person) OPTIONAL MATCH (p)-[:nothing]->(x) WITH x, 1 AS one OPTIONAL MATCH"
                + " (x)-[r]->(b) RETURN b, r, type(r) AS t");
        assertFalse(kept.contains("->limit(0)"), kept);
        // the graph kept by the WITH (x a bare item): the same
        assertTrue(pure("MATCH (p:Person) OPTIONAL MATCH (p)-[:nothing]->(x) WITH x MATCH (x)-->(b) RETURN b")
                .contains("->limit(0)"));
        // a null the WITH named: the path null, nodes(p) and length(p) of it null
        String path = pure("WITH null AS a OPTIONAL MATCH p = (a)-[r]->() RETURN nodes(p) AS n, length(p) AS l");
        assertTrue(path.contains("n:r|[]->cast(@meta::pure::metamodel::variant::Variant)")
                && path.contains("l:r|[]->cast(@Integer)"), path);
        // an OPTIONAL MATCH as the first clause whose pattern no way reads (no way leaves City): one null row, the
        // root too (Neo4j's OPTIONAL MATCH is the whole pattern's: nothing matched, null everywhere), over the
        // one-row TDS
        String first = pure("OPTIONAL MATCH (c:City)-[r]->(x) RETURN c, r, type(r) AS t");
        assertTrue(first.startsWith("#TDS{") && !first.contains("->limit(0)") && !first.contains("City.all()"),
                first);
        // the WHERE is still read: a variable it names must exist
        invalid("MATCH (p:Person) OPTIONAL MATCH (p)-[:nothing]->(x) WITH x, 1 AS one MATCH (x)-->(b) WHERE q.x = 1"
                + " RETURN b", "not defined");
        unsupported("MATCH (p:Person) OPTIONAL MATCH (p)-[:nothing]->(x) WITH x, 1 AS one MATCH (x)-->(x) RETURN x",
                "binding x again");
    }

    @Test
    void aPatternComprehensionIsANavigationFromTheBoundNode() {
        // the study's §12.31: size counts the hop's matches; a scalar map over one typed way is a collection the
        // database computes; an untyped hop reads every association end, each way's values joined as JSON text
        assertEquals("demo::Firm.all()->project(~[n:f|$f.legalName, c:f|$f.employees->size()])",
                pure("MATCH (f:Firm) RETURN f.legalName AS n, size([(f)-[:employees]->() | 1]) AS c"));
        assertEquals("demo::Firm.all()->project(~[names:f|$f.employees->filter(_cypher_1|$_cypher_1.age->isNotEmpty() && ($_cypher_1.age->toOne() > 30))->map(_cypher_1|$_cypher_1.name)->toVariant()])",
                pure("MATCH (f:Firm) RETURN [(f)-[:employees]->(e) WHERE e.age > 30 | e.name] AS names"));
        // a to-one way (firm) is read through its path under a presence guard, never a map the database folds
        assertEquals("demo::Person.all()->project(~[l:p|fromJson('[' + if($p.firm->isNotEmpty(), |',' + ('\"' + $p.firm.legalName->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'), |'')->substring(2) + ']')])",
                pure("MATCH (p:Person) RETURN [(p)-->(x) | x.legalName] AS l"));
        assertEquals("demo::Firm.all()->project(~[c:f|if($f.hq->isNotEmpty(), |1, |0) + $f.employees->size()])",
                pure("MATCH (f:Firm) RETURN size([(f)--() | 1]) AS c"));
        unsupported("MATCH (f:Firm) WITH f.legalName AS n RETURN [(f)-[:employees]->() | 1] AS l",
                "a pattern comprehension over rows");
        // what the navigations do not read (two bound ends, more than one hop, a variable-length relationship; the
        // lowering maps the navigated instances alone, so a node outside the pattern or a named path, whose start
        // is the graph's node) reads through the graph's rows (the study's §12.72): the pattern read as its own
        // rows, each match's value's text grouped by the identities it shares with the rows, joined back
        for (String q : List.of("MATCH (f:Firm)-[:employees]->(p:Person) RETURN [(f)-[:employees]->(p) | 1] AS l",
                "MATCH (f:Firm) RETURN [(f)-[:employees]->()-[:firm]->() | 1] AS l",
                "MATCH (f:Firm) RETURN [(f)-[:employees*1..2]->() | 1] AS l",
                "MATCH (f:Firm) RETURN [(f)-[:employees]->(e) | f.legalName + e.name] AS l",
                "MATCH (f:Firm) RETURN [p = (f)-[:employees]->(e) | p] AS l")) {
            String translated = pure(q);
            assertTrue(translated.contains("->groupBy(") && translated.contains("->joinStrings(',')")
                    && translated.contains("JoinKind.LEFT"), translated);
        }
    }

    @Test
    void anOptionalMatchsWhereGuardsWhatThePatternBound() {
        // the study's §12.29: the optional node absent where its hop finds none or the WHERE does not hold; its
        // property null there, its value null there
        assertEquals("demo::Person.all()->project(~[n:p|$p.name, l:p|if($p.firm->isEmpty() || !($p.firm.legalName->isNotEmpty() && ($p.firm.legalName->toOne() == 'x')), |[]->cast(@String), |$p.firm.legalName), f:p|if($p.firm->isEmpty() || !($p.firm.legalName->isNotEmpty() && ($p.firm.legalName->toOne() == 'x')), |[]->cast(@meta::pure::metamodel::variant::Variant), |fromJson((('{\"@labels\":' + '[\"Firm\"]') + if(if($p.firm->isEmpty() || !($p.firm.legalName->isNotEmpty() && ($p.firm.legalName->toOne() == 'x')), |[]->cast(@String), |$p.firm->meta::legend::lite::elementId())->isEmpty(), |'', |',\"@id\":' + ('\"' + if($p.firm->isEmpty() || !($p.firm.legalName->isNotEmpty() && ($p.firm.legalName->toOne() == 'x')), |[]->cast(@String), |$p.firm->meta::legend::lite::elementId())->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + if(if($p.firm->isEmpty() || !($p.firm.legalName->isNotEmpty() && ($p.firm.legalName->toOne() == 'x')), |[]->cast(@String), |$p.firm.legalName)->isEmpty(), |'', |',\"legalName\":' + ('\"' + if($p.firm->isEmpty() || !($p.firm.legalName->isNotEmpty() && ($p.firm.legalName->toOne() == 'x')), |[]->cast(@String), |$p.firm.legalName)->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"')) + '}'))])",
                pure("MATCH (p:Person) OPTIONAL MATCH (p)-[:firm]->(f:Firm) WHERE f.legalName = 'x'"
                        + " RETURN p.name AS n, f.legalName AS l, f"));
        // a WHERE over a hop that fans out (the study's §12.78): the pattern re-anchored over the rows, its WHERE over
        // the pattern's rows before the left join, so a start no match of which holds keeps its row
        String fanned = pure("MATCH (f:Firm) OPTIONAL MATCH (f)-[:employees]->(p:Person) WHERE p.age > 1"
                + " RETURN p.name");
        assertTrue(fanned.contains("JoinKind.LEFT") && fanned.contains("> 1"), fanned);
        // one reading a value of the rows beside the pattern: the left join's condition, the pattern's variables
        // read from the match's side (r), the row's from the row's (l)
        String joined = pure("MATCH (f:Firm) OPTIONAL MATCH (f)-[:employees]->(p:Person) WHERE p.name = f.legalName"
                + " RETURN p.name");
        assertTrue(joined.contains("JoinKind.LEFT, {l, r|") && joined.contains("$l.") && joined.contains("$r."),
                joined);
    }

    @Test
    void aPatternSharingNoVariableIsEveryPairOfRows() {
        // the study's §12.28: each pattern read as rows, the rows joined on a condition that always holds, the WHERE
        // over the pairs
        assertEquals("demo::Person.all()->project(~[_cypher_1:p|'[\"Person\"]', _cypher_2:p|if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[])), _cypher_3:p|$p.age, _cypher_4:p|$p.name, _cypher_5:p|$p->meta::legend::lite::elementId()])->join(demo::Firm.all()->project(~[_cypher_6:f|'[\"Firm\"]', _cypher_7:f|$f.legalName, _cypher_8:f|$f->meta::legend::lite::elementId()]), meta::pure::functions::relation::JoinKind.INNER, {l, r|true})->filter(r|$r._cypher_4 == $r._cypher_7)->extend(~[n:r|$r._cypher_4, l:r|$r._cypher_7])->select(~[n, l])",
                pure("MATCH (p:Person), (f:Firm) WHERE p.name = f.legalName RETURN p.name AS n, f.legalName AS l"));
    }

    @Test
    void aNamedPathIsItsNodesAndRelationshipsWithTheirStoredDirections() {
        // the study's §12.27: the path's JSON (nodes and relationships alternating, each relationship's stored
        // direction beside them) built as text by the database; length(p) spelled from the pattern, nodes(p) and
        // relationships(p) lists of the values; a path written against the stored direction prints it backward
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty())->project(~[pa:p|fromJson(((('{\"@path\":[' + ((('{\"@labels\":' + '[\"Person\"]') + if($p->meta::legend::lite::elementId()->isEmpty(), |'', |',\"@id\":' + ('\"' + $p->meta::legend::lite::elementId()->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + ((if(if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[]))->isEmpty(), |'', |',\"active\":' + if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[]))->toOne()->toString()) + if($p.age->isEmpty(), |'', |',\"age\":' + $p.age->toOne()->toString())) + (',\"name\":' + ('\"' + $p.name->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + '}')) + ',' + ((('{\"@type\":' + '\"firm\"') + if(if($p->meta::legend::lite::elementId()->isEmpty() || $p.firm->meta::legend::lite::elementId()->isEmpty(), |[]->cast(@String), |'firm' + ':' + $p->meta::legend::lite::elementId()->toOne() + '>' + $p.firm->meta::legend::lite::elementId()->toOne())->isEmpty(), |'', |',\"@id\":' + ('\"' + if($p->meta::legend::lite::elementId()->isEmpty() || $p.firm->meta::legend::lite::elementId()->isEmpty(), |[]->cast(@String), |'firm' + ':' + $p->meta::legend::lite::elementId()->toOne() + '>' + $p.firm->meta::legend::lite::elementId()->toOne())->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + '' + '}') + ',') + ((('{\"@labels\":' + '[\"Firm\"]') + if($p.firm->meta::legend::lite::elementId()->isEmpty(), |'', |',\"@id\":' + ('\"' + $p.firm->meta::legend::lite::elementId()->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + if($p.firm.legalName->isEmpty(), |'', |',\"legalName\":' + ('\"' + $p.firm.legalName->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"')) + '}')) + '],\"@dirs\":[true]}')])",
                pure("MATCH pa = (p:Person)-[:firm]->(f:Firm) RETURN pa"));
        // the nodes in the written order (the root is the Person, the pattern starts at the Firm)
        assertEquals("demo::Person.all()->filter(q|$q.firm->isNotEmpty())->project(~[len:q|1, ns:q|fromJson((('[' + fromJson((('{\"@labels\":' + '[\"Firm\"]') + if($q.firm->meta::legend::lite::elementId()->isEmpty(), |'', |',\"@id\":' + ('\"' + $q.firm->meta::legend::lite::elementId()->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + if($q.firm.legalName->isEmpty(), |'', |',\"legalName\":' + ('\"' + $q.firm.legalName->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"')) + '}')->toString()) + (',' + fromJson((('{\"@labels\":' + '[\"Person\"]') + if($q->meta::legend::lite::elementId()->isEmpty(), |'', |',\"@id\":' + ('\"' + $q->meta::legend::lite::elementId()->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + ((if(if($q.active->isNotEmpty() && $q.active->toOne(), |true, |if($q.active->isNotEmpty() && !$q.active->toOne(), |false, |[]))->isEmpty(), |'', |',\"active\":' + if($q.active->isNotEmpty() && $q.active->toOne(), |true, |if($q.active->isNotEmpty() && !$q.active->toOne(), |false, |[]))->toOne()->toString()) + if($q.age->isEmpty(), |'', |',\"age\":' + $q.age->toOne()->toString())) + (',\"name\":' + ('\"' + $q.name->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + '}')->toString())) + ']'), rs:q|fromJson(('[' + fromJson((('{\"@type\":' + '\"firm\"') + if(if($q->meta::legend::lite::elementId()->isEmpty() || $q.firm->meta::legend::lite::elementId()->isEmpty(), |[]->cast(@String), |'firm' + ':' + $q->meta::legend::lite::elementId()->toOne() + '>' + $q.firm->meta::legend::lite::elementId()->toOne())->isEmpty(), |'', |',\"@id\":' + ('\"' + if($q->meta::legend::lite::elementId()->isEmpty() || $q.firm->meta::legend::lite::elementId()->isEmpty(), |[]->cast(@String), |'firm' + ':' + $q->meta::legend::lite::elementId()->toOne() + '>' + $q.firm->meta::legend::lite::elementId()->toOne())->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + '' + '}')->toString()) + ']')])",
                pure("MATCH pa = (f:Firm)<-[:firm]-(q:Person) RETURN length(pa) AS len, nodes(pa) AS ns,"
                        + " relationships(pa) AS rs"));
        assertEquals("demo::Person.all()->project(~[pa:q|fromJson(('{\"@path\":[' + ((('{\"@labels\":' + '[\"Person\"]') + if($q->meta::legend::lite::elementId()->isEmpty(), |'', |',\"@id\":' + ('\"' + $q->meta::legend::lite::elementId()->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + ((if(if($q.active->isNotEmpty() && $q.active->toOne(), |true, |if($q.active->isNotEmpty() && !$q.active->toOne(), |false, |[]))->isEmpty(), |'', |',\"active\":' + if($q.active->isNotEmpty() && $q.active->toOne(), |true, |if($q.active->isNotEmpty() && !$q.active->toOne(), |false, |[]))->toOne()->toString()) + if($q.age->isEmpty(), |'', |',\"age\":' + $q.age->toOne()->toString())) + (',\"name\":' + ('\"' + $q.name->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + '}')) + '],\"@dirs\":[]}')])",
                pure("MATCH pa = (q:Person) RETURN pa"));
        invalid("MATCH p = (p:Person)-[:firm]->(f:Firm) RETURN p", "Variable `p` already declared");
    }

    @Test
    void keysOfANodeAreTheNamesOfItsPropertiesThatHoldAValue() {
        // the study's §12.26: the names' JSON array text (each where its value is present), read back as strings;
        // a relationship holds none; RETURN p ORDER BY p.name with a LIMIT reads the pattern's property
        assertEquals("demo::Person.all()->project(~[n:p|fromJson('[' + substring((if(if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[]))->isEmpty(), |'', |',\"active\"') + if($p.age->isEmpty(), |'', |',\"age\"')) + ',\"name\"', 2) + ']')->toMany(@String)->size(), a:p|'age'->in(fromJson('[' + substring((if(if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[]))->isEmpty(), |'', |',\"active\"') + if($p.age->isEmpty(), |'', |',\"age\"')) + ',\"name\"', 2) + ']')->toMany(@String))])",
                pure("MATCH (p:Person) RETURN size(keys(p)) AS n, 'age' IN keys(p) AS a"));
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty())->project(~[n:p|fromJson('[]')->toMany(@String)->size()])",
                pure("MATCH (p:Person)-[r:firm]->(f:Firm) RETURN size(keys(r)) AS n"));
        assertEquals("demo::Person.all()->project(~[p:p|fromJson((('{\"@labels\":' + '[\"Person\"]') + if($p->meta::legend::lite::elementId()->isEmpty(), |'', |',\"@id\":' + ('\"' + $p->meta::legend::lite::elementId()->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + ((if(if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[]))->isEmpty(), |'', |',\"active\":' + if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[]))->toOne()->toString()) + if($p.age->isEmpty(), |'', |',\"age\":' + $p.age->toOne()->toString())) + (',\"name\":' + ('\"' + $p.name->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + '}'), _cypher_1:p|$p.name])->sort([~_cypher_1->ascending(meta::pure::functions::relation::NullOrder.LAST)])->select(~[p])->limit(1)",
                pure("MATCH (p:Person) RETURN p ORDER BY p.name LIMIT 1"));
    }

    @Test
    void aShapeOfRowValuesIsItsJsonTextBuiltByTheDatabase() {
        // the study's §12.25: each element spelled by its kind (null for an empty one, a string escaped, a whole
        // float with its .0), a map's entries under their keys, a nested shape within; read as a Variant
        assertEquals("demo::Person.all()->project(~[l:p|fromJson(('[' + if($p.age->isEmpty(), |'null', |$p.age->toOne()->toString())) + ']')])",
                pure("MATCH (p:Person) RETURN [p.age] AS l"));
        assertEquals("demo::Person.all()->project(~[l:p|fromJson((((('[' + if($p.age->isEmpty(), |'null', |$p.age->toOne()->toString())) + (',' + ('\"' + $p.name->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + (',' + if(if($p.age->isNotEmpty() && ($p.age->toOne() > 1), |true, |if($p.age->isNotEmpty() && !($p.age->toOne() > 1), |false, |[]))->isEmpty(), |'null', |if($p.age->isNotEmpty() && ($p.age->toOne() > 1), |true, |if($p.age->isNotEmpty() && !($p.age->toOne() > 1), |false, |[]))->toOne()->toString()))) + (',' + if(toFloat(2.5)->toString()->contains('.') || toFloat(2.5)->toString()->contains('e'), |toFloat(2.5)->toString(), |toFloat(2.5)->toString() + '.0'))) + ']')])",
                pure("MATCH (p:Person) RETURN [p.age, p.name, p.age > 1, 2.5] AS l"));
        assertEquals("demo::Person.all()->project(~[m:p|fromJson((('{' + '\"age\":' + if($p.age->isEmpty(), |'null', |$p.age->toOne()->toString())) + ',\"inner\":' + (('[' + ('\"' + $p.name->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"')) + ']')) + '}')])",
                pure("MATCH (p:Person) RETURN {age: p.age, inner: [p.name]} AS m"));
    }

    @Test
    void anExpressionAroundAggregatesIsComputedAfterTheGrouping() {
        // the study's §12.24: each aggregate a hidden column of the grouping, the expression extended over the
        // finished aggregates (a count of nothing is 0), the items alone selected; an ORDER BY aggregate beyond the
        // items a hidden aggregate of the grouping too, read by the sort and dropped after it
        assertEquals("demo::Person.all()->project(~[_cypher_2:p|1, _cypher_4:p|1])->aggregate(~[_cypher_1:x|$x._cypher_2:y|$y->count(), _cypher_3:x|$x._cypher_4:y|$y->count()])->extend(~[c:r|$r._cypher_1 * 10, neg:r|1 - $r._cypher_3])->select(~[c, neg])",
                pure("MATCH (p:Person) RETURN count(*) * 10 AS c, 1 - count(p) AS neg"));
        // a sum of nothing is 0: the raw sum finished first, the expression over the finished one
        assertEquals("demo::Person.all()->project(~[name:p|$p.name, _cypher_2:p|$p.age])->groupBy(~[name], ~[_cypher_3:x|$x._cypher_2:y|$y->sum()])->extend(~[_cypher_1:r|if($r._cypher_3->isEmpty(), |0, |$r._cypher_3->toOne())])->extend(~[twice:r|$r._cypher_1 * 2])->select(~[name, twice])->sort([~name->ascending(meta::pure::functions::relation::NullOrder.LAST)])",
                pure("MATCH (p:Person) RETURN p.name AS name, sum(p.age) * 2 AS twice ORDER BY name"));
        // the sort's aggregate beyond the items: hidden, kept past the grouping's select, dropped after the sort
        assertEquals("demo::Person.all()->project(~[name:p|$p.name, _cypher_1:p|$p.age, _cypher_3:p|$p.age])->groupBy(~[name], ~[min:x|$x._cypher_1:y|$y->min(), _cypher_5:x|$x._cypher_3:y|$y->sum()])->extend(~[_cypher_2:r|if($r._cypher_5->isEmpty(), |0, |$r._cypher_5->toOne())])->select(~[name, min, _cypher_2])->extend(~[_cypher_4:r|$r._cypher_2])->sort([~_cypher_4->ascending(meta::pure::functions::relation::NullOrder.LAST)])->select(~[name, min])->select(~[name])",
                pure("MATCH (p:Person) WITH p.name AS name, min(p.age) AS min ORDER BY sum(p.age) RETURN name"));
        // the sort re-spelling an item's aggregate reads its column
        assertEquals("demo::Person.all()->project(~[_cypher_1:p|$p.age])->aggregate(~[a:x|$x._cypher_1:y|$y->average()])->select(~[a])->extend(~[_cypher_2:r|if($r.a->isEmpty(), |[], |1 + $r.a->toOne())])->sort([~_cypher_2->ascending(meta::pure::functions::relation::NullOrder.LAST)])->select(~[a])",
                pure("MATCH (p:Person) RETURN avg(p.age) AS a ORDER BY 1 + avg(p.age)"));
    }

    @Test
    void aRelationshipIsItsType() {
        // the study's §12.19: a relationship as a value is its type's JSON (the model holds no relationship
        // property); type(r) its type; carried through WITH as its type's column
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty())->project(~[_cypher_1:p|'[\"Person\"]', _cypher_2:p|if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[])), _cypher_3:p|$p.age, _cypher_4:p|$p.name, _cypher_5:p|$p->meta::legend::lite::elementId(), _cypher_6:p|'firm', _cypher_7:p|$p->meta::legend::lite::elementId(), _cypher_8:p|$p.firm->meta::legend::lite::elementId()])->sort([~_cypher_4->ascending(meta::pure::functions::relation::NullOrder.LAST)])->limit(1)->extend(~[r:r|fromJson((('{\"@type\":' + ('\"' + $r._cypher_6->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"')) + if(if($r._cypher_7->isEmpty() || $r._cypher_8->isEmpty(), |[]->cast(@String), |$r._cypher_6->toOne() + ':' + $r._cypher_7->toOne() + '>' + $r._cypher_8->toOne())->isEmpty(), |'', |',\"@id\":' + ('\"' + if($r._cypher_7->isEmpty() || $r._cypher_8->isEmpty(), |[]->cast(@String), |$r._cypher_6->toOne() + ':' + $r._cypher_7->toOne() + '>' + $r._cypher_8->toOne())->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + '' + '}'), t:r|$r._cypher_6])->select(~[r, t])",
                pure("MATCH (p:Person)-[r:firm]->(f:Firm) WITH p, r ORDER BY p.name LIMIT 1 RETURN r, type(r) AS t"));
    }

    @Test
    void aWithsWhereReadsTheScopeBeforeTheProjectionToo() {
        // Cypher's `WITH a.x AS x WHERE a.y > 1`: the WHERE sees the pattern's variables as well as the items
        assertEquals("demo::Person.all()->project(~[n:p|$p.name,"
                        + " _cypher_1:p|$p.age->isNotEmpty() && ($p.age->toOne() > 1)])"
                        + "->filter(r|$r._cypher_1)->select(~[n])->select(~[n])",
                pure("MATCH (p:Person) WITH p.name AS n WHERE p.age > 1 RETURN n"));
    }

    @Test
    void aColumnRenamedOntoATakenNameGoesThroughAFreshOne() {
        assertEquals("demo::Person.all()->project(~[age:p|$p.age])"
                        + "->extend(~[_cypher_1:r|if($r.age->isEmpty(), |[], |$r.age->toOne() + 1)])"
                        + "->select(~[_cypher_1])"
                        + "->rename(~_cypher_1, ~age)",
                pure("MATCH (p:Person) WITH p.age AS age RETURN age + 1 AS age"));
    }

    @Test
    void aQueryWithoutAPatternIsOneRow() {
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[three:r|1 + 2])->select(~[three])",
                pure("RETURN 1 + 2 AS three"));
    }

    // ------------------------------------------------------------------ aggregation

    @Test
    void anAggregateGroupsByTheOtherItemsImplicitly() {
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty())"
                        + "->project(~['f.legalName':p|$p.firm.legalName, _cypher_1:p|1])"
                        + "->groupBy(~['f.legalName'], ~[n:x|$x._cypher_1:y|$y->count()])->select(~['f.legalName', n])"
                        + "->sort([~n->descending(meta::pure::functions::relation::NullOrder.FIRST)])",
                pure("MATCH (f:Firm)<-[:firm]-(p:Person) RETURN f.legalName, count(*) AS n ORDER BY n DESC"));
    }

    @Test
    void anAggregateAloneIsOneRowAndASumOfNothingIsZero() {
        assertEquals("demo::Person.all()->project(~[_cypher_1:p|1, _cypher_2:p|$p.age, _cypher_3:p|$p.age,"
                        + " _cypher_4:p|$p.age])"
                        + "->aggregate(~['count(*)':x|$x._cypher_1:y|$y->count(), _cypher_5:x|$x._cypher_2:y|$y->sum(),"
                        + " 'avg(p.age)':x|$x._cypher_3:y|$y->average(), 'max(p.age)':x|$x._cypher_4:y|$y->max()])"
                        + "->extend(~['sum(p.age)':r|if($r._cypher_5->isEmpty(), |0, |$r._cypher_5->toOne())])"
                        + "->select(~['count(*)', 'sum(p.age)', 'avg(p.age)', 'max(p.age)'])",
                pure("MATCH (p:Person) RETURN count(*), sum(p.age), avg(p.age), max(p.age)"));
    }

    @Test
    void countOfANodeVariableCountsRowsAndCountOfAPropertyCountsItsValues() {
        assertEquals("demo::Person.all()->project(~[_cypher_1:p|1])"
                        + "->aggregate(~['count(p)':x|$x._cypher_1:y|$y->count()])->select(~['count(p)'])",
                pure("MATCH (p:Person) RETURN count(p)"));
        assertEquals("demo::Person.all()->project(~[_cypher_1:p|$p.age])"
                        + "->aggregate(~['count(p.age)':x|$x._cypher_1:y|$y->count()])->select(~['count(p.age)'])",
                pure("MATCH (p:Person) RETURN count(p.age)"));
    }

    @Test
    void aSortInAGroupedProjectionReadsTheItemsOnly() {
        invalid("MATCH (p:Person) RETURN p.name, count(*) ORDER BY p.age", "Variable `p` not defined");
        invalid("MATCH (p:Person) RETURN DISTINCT p.name ORDER BY p.age", "Variable `p` not defined");
    }

    // ------------------------------------------------------------------ null rules

    @Test
    void comparisonsAreNullWhenASideIsAndWhereKeepsOnlyTrue() {
        assertEquals("demo::Person.all()->filter(p|$p.age->isNotEmpty() && ($p.age->toOne() > 30))"
                        + "->project(~['p.name':p|$p.name])",
                pure("MATCH (p:Person) WHERE p.age > 30 RETURN p.name"));
        // NOT null is null: the FALSE case of the comparison, not the negation of its TRUE case
        assertEquals("demo::Person.all()->filter(p|$p.age->isNotEmpty() && !($p.age->toOne() > 30))"
                        + "->project(~['p.name':p|$p.name])",
                pure("MATCH (p:Person) WHERE NOT p.age > 30 RETURN p.name"));
        assertEquals("demo::Person.all()->filter(p|false)->project(~['p.name':p|$p.name])",
                pure("MATCH (p:Person) WHERE p.age = null RETURN p.name"));
    }

    @Test
    void aNullableBooleanPropertyIsAConditionOnlyWhereItIsKnown() {
        assertEquals("demo::Person.all()->filter(p|$p.active->isNotEmpty() && $p.active->toOne())"
                        + "->project(~['p.name':p|$p.name])",
                pure("MATCH (p:Person) WHERE p.active RETURN p.name"));
        assertEquals("demo::Person.all()->filter(p|$p.active->isNotEmpty() && !$p.active->toOne())"
                        + "->project(~['p.name':p|$p.name])",
                pure("MATCH (p:Person) WHERE NOT p.active RETURN p.name"));
    }

    @Test
    void isNullIsTwoValued() {
        assertEquals("demo::Person.all()->filter(p|!$p.age->isNotEmpty())->project(~['p.name':p|$p.name])",
                pure("MATCH (p:Person) WHERE p.age IS NULL RETURN p.name"));
        assertEquals("demo::Person.all()->filter(p|true)->project(~['p.name':p|$p.name])",
                pure("MATCH (p:Person) WHERE p.name IS NOT NULL RETURN p.name"));
    }

    @Test
    void andOrXorKeepThreeValuedLogic() {
        // a AND b is FALSE when either is FALSE, even if the other is null
        assertEquals("demo::Person.all()->filter(p|($p.age->isNotEmpty() && ($p.age->toOne() > 30))"
                        + " && ($p.name == 'x'))->project(~['p.name':p|$p.name])",
                pure("MATCH (p:Person) WHERE p.age > 30 AND p.name = 'x' RETURN p.name"));
        assertEquals("demo::Person.all()->filter(p|($p.age->isNotEmpty() && !($p.age->toOne() > 30))"
                        + " || ($p.name != 'x'))->project(~['p.name':p|$p.name])",
                pure("MATCH (p:Person) WHERE NOT (p.age > 30 AND p.name = 'x') RETURN p.name"));
    }

    @Test
    void aComparisonAsAValueIsTrueFalseOrNothing() {
        assertEquals("demo::Person.all()->project(~[old:p|if($p.age->isNotEmpty() && ($p.age->toOne() > 30), |true,"
                        + " |if($p.age->isNotEmpty() && !($p.age->toOne() > 30), |false, |[]))])",
                pure("MATCH (p:Person) RETURN p.age > 30 AS old"));
    }

    @Test
    void caseAndCoalesce() {
        assertEquals("demo::Person.all()->project(~[band:p|if($p.age->isNotEmpty() && ($p.age->toOne() >= 30),"
                        + " |'senior', |if($p.age->isNotEmpty() && ($p.age->toOne() < 30), |'junior', |'unknown'))])",
                pure("MATCH (p:Person) RETURN CASE WHEN p.age >= 30 THEN 'senior' WHEN p.age < 30 THEN 'junior'"
                        + " ELSE 'unknown' END AS band"));
        assertEquals("demo::Person.all()->project(~[age:p|if($p.age->isEmpty(), |0, |$p.age)->toOne()])",
                pure("MATCH (p:Person) RETURN coalesce(p.age, 0) AS age"));
        assertEquals("demo::Person.all()->project(~[one:p|if($p.name == 'a', |1, |[]->cast(@Integer))])",
                pure("MATCH (p:Person) RETURN CASE p.name WHEN 'a' THEN 1 END AS one"));
    }

    @Test
    void arithmeticIsCyphers() {
        // integer division truncates toward zero: Postgres's spelling (SqlToPure.integerQuotient)
        String quotient = pure("RETURN 7 / 2 AS q");
        assertTrue(quotient.startsWith("#TDS{\n_cypher_row\n1\n}#->extend(~[q:r|if(((7 - 7->rem(2)) / 2)"
                + " * 0.9999999999999991 >= 0, |floor(((7 - 7->rem(2)) / 2) * 0.9999999999999991), |ceiling("),
                quotient);
        assertTrue(quotient.endsWith("])->select(~[q])"), quotient);
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[r:r|7->rem(3), p:r|toFloat(2)->pow(3),"
                        + " f:r|7 / toFloat(2.0), s:r|'a' + 'b'])->select(~[r, p, f, s])",
                pure("RETURN 7 % 3 AS r, 2 ^ 3 AS p, 7 / 2.0 AS f, 'a' + 'b' AS s"));
    }

    @Test
    void stringPredicatesAndIn() {
        assertEquals("demo::Person.all()->filter(p|$p.name->startsWith('A') || $p.name->endsWith('e'))"
                        + "->project(~['p.name':p|$p.name])",
                pure("MATCH (p:Person) WHERE p.name STARTS WITH 'A' OR p.name ENDS WITH 'e' RETURN p.name"));
        assertEquals("demo::Person.all()->filter(p|$p.age->isNotEmpty() && $p.age->toOne()->in([25, 40]))"
                        + "->project(~['p.name':p|$p.name])",
                pure("MATCH (p:Person) WHERE p.age IN [25, 40] RETURN p.name"));
    }

    // ------------------------------------------------------------------ the model is the schema

    @Test
    void anOpenGraphReadsAnUnknownNameAsNeo4jDoes() {
        // the study's §12.45: over an OPEN graph (every class <<graph.Open>>) a label no class has reads no rows (the
        // closed graph's C4 error): as the first clause, the no-row TDS, p an absent node, its property null
        String label = open("MATCH (p:Nobody) RETURN p.name AS n");
        assertTrue(label.contains("->cast(@String)->flatten(") && !label.contains(".all()")
                && label.contains("n:r|[]"), label);
        // an OPTIONAL MATCH as the first clause: one null row
        String optional = open("OPTIONAL MATCH (p:Nobody) RETURN p.name AS n");
        assertTrue(optional.startsWith("#TDS{") && optional.contains("n:r|[]"), optional);
        // a property no class declares is null
        String property = open("MATCH (p:Person) RETURN p.nam AS x");
        assertTrue(property.contains("demo::Person.all()") && property.contains("x:p|[]"), property);
        // a relationship type the model has nothing of: no rows
        String type = open("MATCH (p:Person)-[:boss]->(q) RETURN q.name AS n");
        assertTrue(type.contains("->cast(@String)->flatten(") && !type.contains(".all()"), type);
        // a type the model declares, but no way of it from the class (firm is a Person's): no rows too
        String noWay = open("MATCH (f:Firm)-[:firm]->(x) RETURN x.name AS n");
        assertTrue(noWay.contains("->cast(@String)->flatten(") && !noWay.contains(".all()"), noWay);
        // after a graph: the rows in hand limited to none (an OPTIONAL MATCH keeps them), x absent
        String after = open("MATCH (f:Firm) MATCH (f)-[:nothing]->(x:Person) RETURN f.legalName AS l, x.name AS n");
        assertTrue(after.contains("demo::Firm.all()") && after.contains("->limit(0)") && after.contains("n:r|[]"),
                after);
        String afterOptional = open("MATCH (f:Firm) OPTIONAL MATCH (f)-[:nothing]->(x:Person) RETURN f.legalName AS l,"
                + " x.name AS n");
        assertTrue(afterOptional.contains("demo::Firm.all()") && !afterOptional.contains("->limit(0)")
                && afterOptional.contains("n:r|[]"), afterOptional);
        // after rows: likewise, the rows' columns kept
        String afterRows = open("MATCH (p:Person) WITH p.name AS n MATCH (q:Nobody) RETURN n, q.name AS m");
        assertTrue(afterRows.contains("demo::Person.all()") && afterRows.contains("->limit(0)")
                && afterRows.contains("m:r|[]"), afterRows);
        // a pattern comprehension or predicate over a name the model lacks matches nothing
        String comprehension = open("MATCH (p:Person) RETURN [(p)-[:nothing]->(x) | x.name] AS l");
        assertTrue(comprehension.contains("demo::Person.all()"), comprehension);
        String predicate = open("MATCH (p:Person) WHERE (p)-[:nothing]->(:Nobody) RETURN p.name AS n");
        assertTrue(predicate.contains("demo::Person.all()"), predicate);
        // a WHERE over an undeclared property: null, which filters every row
        String where = open("MATCH (p:Person) WHERE p.nam = 'x' RETURN p.name AS n");
        assertTrue(where.contains("demo::Person.all()"), where);
        // the static errors stay: a variable not defined is Neo4j's error too
        assertEquals(SqlTranslationException.Kind.INVALID,
                openRefusal("MATCH (p:Nobody) RETURN q.name AS n").kind());
        // a UNION whose part answers an untyped null takes the other part's type (the parts concatenate)
        String union = open("MATCH (p:Person) RETURN p.name AS n UNION ALL MATCH (p:Nobody) RETURN p.name AS n");
        assertTrue(union.contains("->concatenate(") && union.contains("n:r|[]->cast(@String)"), union);
        // absent nodes carried through a WITH, each its own column
        String carried = open("MATCH (p:Nobody)-[:firm]->(q) WITH p, q RETURN p.name AS n, q.name AS m");
        assertTrue(carried.contains("n:r|[]") && carried.contains("m:r|[]"), carried);
    }

    @Test
    void aLeadingUseIsTheSeamsAndReadsAsDone() {
        // the study's §12.45: the seam resolves the graph a leading USE names (a DataSpace) into the model before
        // translating, so the translator reads the clause as done; anywhere else it is Neo4j's error
        assertEquals(pure("MATCH (p:Person) RETURN p.name AS n"), pure("USE demo::G MATCH (p:Person) RETURN p.name AS n"));
        invalid("MATCH (p:Person) USE demo::G RETURN p.name AS n", "USE clause must be either the first clause");
        // a UNION's first part opens with it
        assertEquals(pure("MATCH (p:Person) RETURN p.name AS n UNION ALL MATCH (f:Firm) RETURN f.legalName AS n"),
                pure("USE demo::G MATCH (p:Person) RETURN p.name AS n UNION ALL MATCH (f:Firm) RETURN f.legalName AS n"));
        invalid("MATCH (p:Person) RETURN p.name AS n UNION ALL USE demo::G MATCH (f:Firm) RETURN f.legalName AS n",
                "USE clause must be either the first clause");
    }

    @Test
    void anIncomingHopToALabelLessNodeReadsTheOneClassDeclaringTheType() {
        // Person declares firm: Firm, so (f:Firm)<-[:firm]-(p) reads p as a Person through Firm's inverse end
        String p = pure("MATCH (f:Firm)<-[:firm]-(p) RETURN f.legalName AS l, p.name AS n");
        assertTrue(p.contains("demo::Firm.all()") && p.contains("employees"), p);
    }

    @Test
    void anExpressionInWithMustBeAliased() {
        invalid("MATCH (p:Person) WITH p.name, count(*) AS c RETURN c", "Expression in WITH must be aliased (use AS)");
        invalid("MATCH (p:Person) WITH p.name + 'x' RETURN 1 AS one", "Expression in WITH must be aliased");
    }

    @Test
    void anExpressionAroundAggregatesMayReadAGroupingKey() {
        // Neo4j's rule (Return6, With6): another item of the projection is a grouping key, read after the grouping
        String keyed = pure("MATCH (p:Person) RETURN p.age, p.age + count(*) AS s");
        assertTrue(keyed.contains("->groupBy(") && keyed.contains("'p.age'"), keyed);
        String projected = pure("MATCH (p:Person) WITH p.age AS age, p WITH age, age + count(p.name) AS agg RETURN *");
        assertTrue(projected.contains("->groupBy(") && projected.contains("agg"), projected);
        // a value of the row that is no item stays Neo4j's implicit-grouping error; a property of a key is no key
        invalid("MATCH (p:Person) RETURN p.name, p.age + count(*)", "implicit grouping expressions");
        invalid("MATCH (p:Person) WITH p.age AS age, p RETURN age, age + p.age + count(*)",
                "implicit grouping expressions (p.age in");
        invalid("MATCH (p:Person) RETURN p.age + 1, p.age + 1 + 2 + count(*)", "implicit grouping expressions");
    }

    @Test
    void aPatternNavigatingMoreThanTwoAssociationStepsIsCutIntoSegmentsJoinedByIdentity() {
        // the resolver's nested navigation reads no association end past a second step in an existence filter
        // (W9): the pattern is cut at two steps, the first segment materialized, the next re-anchored on the cut
        // node by identity (the study's §12.54)
        String deep = pure("MATCH (f:Firm)-[:employees]->(p)-[:firm]->(g)-[:hq]->(c) RETURN c.name AS n");
        assertTrue(deep.contains("->join(") && deep.contains(".hq"), deep);
        // an untyped hop among typed ones: an arm with an edge way runs past two steps, so the pattern is cut
        // around its hops, the untyped one a segment of its own read over the rows (the study's §12.55)
        String untyped = edges("MATCH (p:Person)-[:ACTED_IN]->(f)-->(c) RETURN c.name AS n");
        assertTrue(untyped.contains("->join("), untyped);
        // five hops: two cuts, two joins
        String deeper = pure("MATCH (p:Person)-[:firm]->(f)-[:employees]->(q)-[:firm]->(g)-[:employees]->(r)-[:firm]->(h)"
                + " RETURN h.legalName AS n");
        assertEquals(2, deeper.split("->join\\(", -1).length - 1, deeper);
    }

    @Test
    void aNamedPathOverACutIsAssembledOverTheRowsAndAVariableLengthChainPastTwoStepsReadsAsRows() {
        // a named path deeper than the cut (the study's §12.56): the segments recorded as partial paths, every
        // relationship carrying its stored direction as a column, the path's JSON and its parts built over the rows
        String path = pure("MATCH p = (f:Firm)-[:employees]->(x)-[:firm]->(g)-[:hq]->(c) RETURN p, length(p) AS l,"
                + " nodes(p) AS ns");
        assertTrue(path.contains("->join(") && path.contains("\"@dirs\":[") && path.contains("_cypher_path_nodes_p"),
                path);
        // a hop past an edge hop under a named path: cut, the edge's direction a constant of its arm
        String edge = edges("MATCH q = (p:Person)-[:ACTED_IN]->(f)-[:hq]->(c) RETURN q");
        assertTrue(edge.contains("->join(") && edge.contains("\"@dirs\":["), edge);
        // a variable-length chain past two steps (the study's §12.57): each chain read as rows of its own (the
        // long ones cut), projected to the pattern's variables under the same columns, and concatenated
        String chain = pure("MATCH (f:Firm)-[:employees|firm|hq*3..3]->(x) RETURN x");
        assertTrue(chain.contains("->join("), chain);
        String chains = pure("MATCH (f:Firm)-[r:employees|firm*2..3]->(x) RETURN x, size(r) AS n");
        assertTrue(chains.contains("->join(") && chains.contains("->concatenate("), chains);
        // a property of a node the database holds as a Variant beside a scalar (§12.57): equal where the Variant
        // holds that kind and the values agree, false where it holds another kind, null where either is null
        String variant = pure("MATCH (p:Person) WITH collect(p) AS ps UNWIND ps AS q RETURN q.name = 'a' AS t,"
                + " q.age = 1 AS one");
        assertTrue(variant.contains("meta::legend::lite::isKind(") && variant.contains("->to(@Float)"), variant);
        // a list predicate whose condition may be null, over a list the database computes: the holds, the fails
        // and the unknowns counted, combined by Cypher's three-valued rules
        String any = pure("MATCH (p:Person) WITH collect(p) AS ps RETURN any(q IN ps WHERE q.name = 'a') AS t");
        assertTrue(any.contains("->filter(") && any.contains("->size()"), any);
        // a list comprehension producing values that may be null: the elements' JSON texts joined as the list
        String mapped = pure("MATCH (p:Person) WITH collect(p) AS ps RETURN [q IN ps | q.name = 'a'] AS ts");
        assertTrue(mapped.contains("joinStrings(") && mapped.contains("fromJson("), mapped);
    }

    @Test
    void variantsOrderCompareAndAggregateByTheOrderabilityKey() {
        // ORDER BY a Variant (the study's §12.58): the orderability key -- the rank by lite's isKind, a number as a
        // fixed-width text, a list as its elements' keys -- a hidden key computed over the rows after the projection
        String sorted = pure("MATCH (p:Person) WITH collect(p.age) AS ages RETURN ages ORDER BY ages");
        assertTrue(sorted.contains("meta::legend::lite::isKind(") && sorted.contains("lpad(")
                && sorted.contains("->extend(") && sorted.contains("->sort("), sorted);
        // min and max over Variants: lite's minBy and maxBy over rowMapper(value, key) rows, a text key
        String extremes = pure("MATCH (p:Person) WITH [p.age, p.name] AS l UNWIND l AS a RETURN min(a) AS mn,"
                + " max(a) AS mx");
        assertTrue(extremes.contains("meta::legend::lite::minBy") && extremes.contains("meta::legend::lite::maxBy")
                && extremes.contains("rowMapper("), extremes);
        // < between Variants: by the keys where both are of one rank, null across ranks
        String less = pure("MATCH (p:Person) WITH [p.age, p.name] AS l UNWIND l AS a UNWIND l AS b"
                + " RETURN a < b AS lt");
        assertTrue(less.contains("left(") && less.contains("isKind('number')"), less);
        // a list beside a Variant in +: the list's JSON as text, x's elements within its brackets where x is a list
        String joined = pure("MATCH (p:Person) WITH [p.age, p.name] AS l UNWIND l AS a WITH collect(a) AS vs, a"
                + " RETURN vs + a AS joined");
        assertTrue(joined.contains("fromJson(") && joined.contains("isKind('list')"), joined);
        // a spelled list of several kinds read by the database: the Variant's elements
        String mixed = pure("MATCH (p:Person) RETURN [x IN [1, 'a', [2], null] WHERE x = p.age] AS some");
        assertTrue(mixed.contains("->toMany(@meta::pure::metamodel::variant::Variant)"), mixed);
        // a float divided by a zero literal inside a list (the study's §12.79): the database's own JSON of it
        String nan = pure("MATCH (p:Person) RETURN [1.5, 0.0 / 0.0] AS l");
        assertTrue(nan.contains("toVariant(toFloat(0.0) / toFloat(0.0))->toString()"), nan);
    }

    @Test
    void anUnknownLabelPropertyOrRelationshipIsAnError() {
        invalid("MATCH (p:Nobody) RETURN p.name", "the label Nobody names no class");
        invalid("MATCH (p:Person) RETURN p.nam", "the class demo::Person has no property nam");
        invalid("MATCH (p:Person)-[:boss]->(q) RETURN q.name", "names no property or association end");
        invalid("MATCH (p:Person) RETURN q.name", "Variable `q` not defined");
        invalid("MATCH (p:Person) RETURN p.name, p.name", "Multiple result columns with the same name");
        invalid("MATCH (p:Person)", "Query cannot conclude with MATCH");
        invalid("MATCH (p:Person) RETURN p.name RETURN p.age", "RETURN can only be used at the end");
    }

    @Test
    void aLabelMayBeAFullPath() {
        assertEquals("demo::Person.all()->project(~['p.name':p|$p.name])",
                pure("MATCH (p:`demo::Person`) RETURN p.name"));
    }

    @Test
    void typeMismatchesAreNeo4jsErrors() {
        invalid("MATCH (p:Person) WHERE p.name RETURN p.name", "Type mismatch: expected Boolean but was String");
        invalid("MATCH (p:Person) WHERE p.age AND true RETURN p.name",
                "Type mismatch: expected Boolean but was Integer");
        invalid("MATCH (p:Person) RETURN p.name - 2", "Type mismatch: expected Float or Integer but was String");
        unsupported("MATCH (p:Person) RETURN p.name + 1", "+ of a string and a Integer");
        invalid("MATCH (p:Person) RETURN sum(p.name)",
                "Type mismatch: expected Float, Integer or Duration but was String");
        invalid("MATCH (p:Person) RETURN count(count(*))", "aggregate functions inside of aggregate functions");
        invalid("MATCH (p:Person) RETURN p.name SKIP -1", "not a valid value");
    }

    // ------------------------------------------------------------------ refusals by name

    @Test
    void whatPr1DoesNotReadIsRefusedByName() {
        unsupported("MATCH (p:Person)-[r:firm]->(f) RETURN r.since", "a property of a relationship");
        unsupported("MATCH (p:Person) RETURN p.firm", "a node as a value is not supported yet");
        unsupported("MATCH (p:Person) RETURN p.tags", "holds many values (a list)");
        // (a hop with no inverse end, cut off into a segment of its own, reads from its other end: the study's
        // §12.55; "no inverse" refused it before)
        assertTrue(pure("MATCH (f:Firm)-[:hq]->(c:City)<-[:hq]-(g:Firm) RETURN g.legalName").contains("->join("));
        // a variable-length relationship with an upper bound unrolls into the chains of each length (the study's
        // §12.42): one arm per length, concatenated; without one it is a recursion, refused by name
        String chains = pure("MATCH (p:Person)-[:firm*1..2]->(f) RETURN f.legalName AS firm");
        // (Firm holds no firm: the chain of two has no way, the one of one reads as the plain pattern)
        assertTrue(chains.contains("$p.firm.legalName"), chains);
        String zero = pure("MATCH (p:Person)-[:firm*0..1]->(x) RETURN x.name AS name");
        assertTrue(zero.contains("->concatenate("), zero);
        // without an upper bound, or with one past the unroll's, it is the walk the data bounds (the study's
        // §12.68): lite's recursion over the hop's rows, the start node's rows joined on its identity, the far
        // node's on the node reached, the depth within the bounds; the relationship's list and the path beside
        String walk = pure("MATCH (p:Person)-[:firm*]->(f) RETURN f.legalName AS firm");
        assertTrue(walk.contains("->meta::legend::lite::recurse(w|$w->join("), walk);
        assertTrue(walk.contains("->filter(r|!$r._cypher_walk_rels->contains('|' + $r._cypher_edge_rel + '|'))"), walk);
        assertTrue(walk.contains("->filter(r|$r._cypher_walk_depth >= 1)"), walk);
        String bounded = pure("MATCH (p:Person)-[:firm*2..9]->(f) RETURN f.legalName AS firm");
        assertTrue(bounded.contains("&& ($r._cypher_walk_depth < 9))") && bounded.contains("_cypher_walk_depth >= 2)"),
                bounded);
        String listed = pure("MATCH q = (p:Person)-[r:firm*0..]->(f) RETURN r, length(q) AS n, nodes(q) AS ns");
        assertTrue(listed.contains("r:r|fromJson('[' + $r._cypher_walk_relsj + ']')"), listed);
        assertTrue(listed.contains("_cypher_path_length_q:r|$r._cypher_walk_depth") && !listed.contains(">= 0)"), listed);
        // a bounded one after another MATCH re-anchors (§12.49), and one in a WHERE's pattern predicate is read over
        // the rows as a flag whose pattern is a MATCH of its own (the study's §12.69)
        String afterMatch = pure("MATCH (p:Person) MATCH (p)-[:firm*1..2]->(f) RETURN f.legalName AS firm");
        assertTrue(afterMatch.contains("JoinKind.INNER, {l, r|$l._cypher_"), afterMatch);
        String predicate = pure("MATCH (p:Person) WHERE (p)-[:firm*]->() RETURN p.name AS name");
        assertTrue(predicate.contains("recurse(") && predicate.contains("JoinKind.LEFT"), predicate);
        // a walk beside single hops (the study's §12.83): the pattern cut at the walk, the hop after it the far
        // node's rows (a MATCH of its own), its relationship never one the walk walked
        String beside = pure("MATCH (p:Person)-[:firm*]->(f)-[r:firm]->(g) RETURN g.legalName AS g");
        assertTrue(beside.contains("recurse("), beside);
        // (the step's own uniqueness over the identities walked, and the hop's against them)
        assertEquals(2, beside.split("_cypher_walk_rels->contains\\('\\|' \\+ ", -1).length - 1, beside);
        unsupported("MATCH (p:Person)-[:firm*]->(f)-[:firm*1..2]->(g) RETURN g.legalName",
                "beside another variable-length relationship");
        // a MATCH of several patterns the graph route does not read as one graph: each pattern as rows, joined on the
        // identities of the variables they share, else their product (the study's §12.70)
        String three = pure("MATCH (p:Person), (f:Firm), (c:City) RETURN p.name AS p, f.legalName AS f, c.name AS c");
        assertEquals(2, three.split("->join\\(", -1).length - 1, three);
        String components = pure("MATCH (p:Person) WITH p MATCH (c:City), (p)-[:firm]->(f) RETURN c.name AS c, f.legalName AS f");
        assertTrue(components.contains("JoinKind.INNER, {l, r|true}") && components.contains("JoinKind.INNER, {l, r|$l._cypher_"),
                components);
        unsupported("MATCH (p:Person)-[:firm*]->(p) RETURN p.name", "back to itself");
        // after rows, the walk is the re-anchored pattern's own (§12.49): read from the rows' node, joined back
        String after = pure("MATCH (p:Person) WITH p MATCH (p)-[:firm*]->(f) RETURN f.legalName AS firm");
        assertTrue(after.contains("recurse(") && after.contains("JoinKind.INNER, {l, r|$l._cypher_"), after);
        String optional = pure("MATCH (p:Person) OPTIONAL MATCH (p)-[:firm*]->(f) RETURN f.legalName AS firm");
        assertTrue(optional.contains("recurse(") && optional.contains("JoinKind.LEFT"), optional);
        unsupported("MATCH (p:Person)-[:firm {since: 1}]->(f) RETURN f.legalName", "properties on a relationship");
        // a MATCH after rows whose pattern names no variable of the rows: the product (the study's §12.44); one that
        // binds a projected variable again is still refused by name
        String product = pure("MATCH (p:Person) WITH p.name AS n MATCH (f:Firm) RETURN n, f.legalName AS firm");
        assertTrue(product.contains("->join(") && product.contains("JoinKind.INNER"), product);
        unsupported("MATCH (p:Person) WITH p.name AS n MATCH (n)-[:firm]->(f:Firm) RETURN n", "which the rows hold as a"
                + " value of another kind");
        unsupported("CREATE (p:Person {name: 'x'})", "CREATE is not supported yet");
        // collect: the group's values as the JSON array the database builds (the study's §12.41)
        String collected = pure("MATCH (p:Person) RETURN collect(p.name) AS names");
        assertTrue(collected.contains("->joinStrings(',')"), collected);
        assertTrue(collected.contains("fromJson('[' + "), collected);
        // (collect(DISTINCT ...) stays refused: the database's order of the distinct values is not Neo4j's, nor
        // the same twice; the study's §12.44)
        // count(DISTINCT x): the distinct values counted (the study's §12.47); count(DISTINCT n) the identities
        String countDistinct = pure("MATCH (p:Person) RETURN count(DISTINCT p.name) AS c");
        assertTrue(countDistinct.contains("y|$y->distinct()->count()"), countDistinct);
        String countNodes = pure("MATCH (p:Person) RETURN count(DISTINCT p) AS c");
        assertTrue(countNodes.contains("$p->meta::legend::lite::elementId()")
                && countNodes.contains("y|$y->distinct()->count()"), countNodes);
        // (a parameter the text did not bind: Neo4j's ParameterMissing, the study's §12.66; a lane binds a
        // scenario's parameters into the text, §12.15)
        invalid("MATCH (p:Person) WHERE p.name = $name RETURN p.name", "Expected parameter(s): name");
        unsupported("MATCH (p:Person) WHERE p.name =~ 'A.*' RETURN p.name", "regular expression");
        // a list of row values through WITH is the Variant the database builds, marked with its element kind
        String carriedList = pure("MATCH (p:Person) WITH [p.age, 1] AS l UNWIND l AS x RETURN x");
        assertTrue(carriedList.contains("l:p|fromJson(") && carriedList.contains("+ ']')"), carriedList);
        assertTrue(carriedList.contains("$r.l->toMany(@Integer)->flatten(~x)"), carriedList);
        // a list of scalars among the grouping keys: the Variant the database builds, a key (the study's §12.57); one
        // holding a node is refused by name (equal by identity in Neo4j, which the text lacks)
        String listKey = pure("MATCH (p:Person) WITH [p.age] AS l, count(*) AS n RETURN l, n");
        assertTrue(listKey.contains("l:p|fromJson(") && listKey.contains("groupBy(~[l]"), listKey);
        // (a list of nodes is a key too: each node's JSON carries its identity, the study's §12.71)
        assertTrue(pure("MATCH (p:Person) WITH [p] AS l, count(*) AS n RETURN l, n").contains("groupBy(~[l]"));
        assertTrue(pure("MATCH (p:Person) RETURN [localtime('12:00')] AS l").contains("Time("));
        invalid("MATCH (p:Person) RETURN all(x IN p.name WHERE x > 1)", "expected List<T> but was String");
        // a node under DISTINCT or among the grouping keys: its columns (its identity among them) the distinct
        // columns or the keys, its JSON rebuilt after (the study's §12.47)
        String distinctNode = pure("MATCH (p:Person) RETURN DISTINCT p");
        assertTrue(distinctNode.contains("$p->meta::legend::lite::elementId()])->distinct()->extend(~[p:r|fromJson(")
                && distinctNode.endsWith("->select(~[p])"), distinctNode);
        String groupedNode = pure("MATCH (p:Person) RETURN p, count(*) AS c");
        assertTrue(groupedNode.contains("->groupBy(~[_cypher_1, _cypher_2, _cypher_3, _cypher_4, _cypher_5], ~[c:x|")
                && groupedNode.endsWith("->select(~[p, c])"), groupedNode);
        unsupported("MATCH (p:Person) RETURN p ORDER BY p", "ORDER BY a node (p");
        // a relationship over a property or association told apart by the pair of its ends' identities (S74, the
        // study's §12.48): its source's and its target's columns among the distinct columns and the keys
        String distinctRel = pure("MATCH (p:Person)-[r:firm]->(f:Firm) RETURN DISTINCT r");
        assertTrue(distinctRel.contains("$p->meta::legend::lite::elementId(), _cypher_3:p|$p.firm->meta::legend::lite"
                + "::elementId()])->distinct()"), distinctRel);
        String groupedRel = pure("MATCH (p:Person)-[r:firm]->(f:Firm) WITH r, count(*) AS c RETURN c");
        assertTrue(groupedRel.contains("->groupBy(~[_cypher_1, _cypher_2, _cypher_3], ~[c:x|"), groupedRel);
        invalid("MATCH (p:Person) RETURN p.age + count(*)", "implicit grouping expressions (p.age in p.age + count(*))");
        // (an extreme float was refused by name while Postgres typed its plain digits numeric: the dialects cast
        // toFloat of one to a double now, the study's §12.66)
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[x:r|toFloat(1.0E308), y:r|toFloat(1.0E-20) * -1.0])->select(~[x, y])", pure("RETURN 1e308 AS x, -1e-20 AS y"));
        // a parameter the text did not bind is Neo4j's ParameterMissing (the island has no parameter channel)
        invalid("RETURN $x AS x", "Expected parameter(s): x");
    }

    // ------------------------------------------------------------------ edge classes (the study's §5.6)

    @Test
    void anEdgeHopNavigatesTheInverseEndThenTheFarEnd() {
        String pure = edges("MATCH (p:Person)-[r:ACTED_IN]->(f:Firm) RETURN r.role AS role, f.legalName AS firm");
        assertTrue(pure.contains("$p.actings.role"), pure);
        assertTrue(pure.contains("$p.actings.firm.legalName"), pure);
        assertTrue(pure.contains("$p.actings.firm->isNotEmpty()"), pure);
    }

    @Test
    void anIncomingEdgeHopNavigatesTheToEndsInverse() {
        String pure = edges("MATCH (f:Firm)<-[r:ACTED_IN]-(p:Person) RETURN r.role AS role, p.name AS name");
        assertTrue(pure.contains("$f.cast.role"), pure);
        assertTrue(pure.contains("$f.cast.actor.name"), pure);
    }

    @Test
    void anEdgeAsAValueCarriesItsTypeAndProperties() {
        String pure = edges("MATCH (p:Person)-[r:ACTED_IN]->(:Firm) RETURN r, type(r) AS t, properties(r) AS ps,"
                + " keys(r) AS ks");
        // (the graph fans out: the relationship is carried as its type's and its properties' columns, and its
        // value is rebuilt from them)
        assertTrue(pure.contains("'{\"@type\":' + ('\"' + $r._cypher_7->toOne()"), pure);
        assertTrue(pure.contains(",\"role\":"), pure);
        assertTrue(pure.contains(",\"since\":"), pure);
        assertTrue(pure.contains("'ACTED_IN'"), pure);
    }

    @Test
    void anEdgesPropertyMapFiltersTheEdgeInstance() {
        String pure = edges("MATCH (p:Person)-[:ACTED_IN {role: 'Neo'}]->(f:Firm) RETURN f.legalName AS firm");
        assertTrue(pure.contains("'Neo'"), pure);
        assertTrue(pure.contains("$p.actings.role"), pure);
    }

    @Test
    void aPropertyOneArmsEdgeClassLacksIsTheKnownNullInThatArm() {
        // (the Postgres lane's catch: within the arm whose edge class lacks the property, its column is the null
        // constant, which the comparison folds to null and the property map to false; read as a column, Postgres
        // types the null navigated to one as text and refuses it against an integer)
        String pure = edges("MATCH (p:Person)-[:LINK {w: 1}]->(x) RETURN p.name AS name");
        assertTrue(pure.contains("$p.cityLinks.w"), pure);
        assertTrue(pure.contains("->filter(r|false)"), pure);
        String where = edges("MATCH (p:Person)-[l:LINK]->(x) WHERE l.w IS NULL RETURN p.name AS name");
        assertTrue(where.contains("->filter(r|true)"), where);
        // a far node's property map in the arm whose class lacks it (Firm has no name): false there, not C4's
        // refusal (lenient across every class the arms read, as the WHERE is), whichever root the arm tries
        String node = edges("MATCH (p:Person)-[:LINK]->(x {name: 'Paris'}) RETURN p.name AS name");
        assertTrue(node.contains("->filter(r|false)"), node);
        assertTrue(node.contains("$p.cityLinks.city.name"), node);
        // after the arms' union the column holds the other arm's values: read as the column, never the constant
        String read = edges("MATCH (p:Person)-[l:LINK]->(x) WITH p, l RETURN p.name AS name, l.w AS w, l.v AS v");
        assertTrue(read.matches("(?s).*w:r\\|\\$r\\._cypher_\\d+, v:r\\|\\$r\\._cypher_\\d+.*"), read);
    }

    @Test
    void anUntypedHopRefusesAnEdgeEndWithoutAnInverse() {
        // (the review's second finding: an edge the node cannot navigate to is refused wherever it is a way, never
        // skipped; Directing's from end is its own property)
        SqlTranslationException e = edgeRefusal("MATCH (p:Person)-->(x) RETURN labels(x) AS l");
        assertTrue(e.getMessage().contains("holds its from end director with no inverse end"), e.getMessage());
        SqlTranslationException pred = edgeRefusal("MATCH (p:Person) WHERE (p)-->() RETURN p.name");
        assertTrue(pred.getMessage().contains("holds its from end director with no inverse end"), pred.getMessage());
    }

    @Test
    void aPatternPredicateBindsNoNewVariableAndStandsWhereABooleanIsTested() {
        SqlTranslationException fresh = refusal("MATCH (p:Person) WHERE (p)-[r:firm]->(f) RETURN p.name");
        assertEquals(SqlTranslationException.Kind.INVALID, fresh.kind(), fresh.getMessage());
        assertTrue(fresh.getMessage().contains("PatternExpressions are not allowed to introduce new variables: 'r'"),
                fresh.getMessage());
        SqlTranslationException value = refusal("MATCH (p:Person) WHERE toString((p)-[:firm]->()) = 'true' RETURN p.name");
        assertTrue(value.getMessage().contains("A pattern expression should only be used"), value.getMessage());
        // (a quantifier's predicate is a boolean context too: the pattern reads from the graph beneath the
        // quantifier's binding, the study's §12.52)
        String quantified = pure("MATCH (p:Person) RETURN any(x IN [1] WHERE (p)-[:firm]->()) AS a");
        assertTrue(quantified.contains("$p.firm->isNotEmpty()"), quantified);
        String negated = pure("MATCH (p:Person) WHERE NOT ((p)-[:firm]->() OR p.age > 1) RETURN p.name");
        assertTrue(negated.contains("$p.firm->isNotEmpty()"), negated);
    }

    @Test
    void aLabelledStartInAPredicateIsTheBoundNodesClassOrNothing() {
        // (the review's third finding: the start node's label is read)
        String nothing = pure("MATCH (p:Person) WHERE (p:Firm)-[:firm]->() RETURN p.name");
        assertTrue(nothing.contains("filter(p|false)"), nothing);
        String same = pure("MATCH (p:Person) WHERE (p:Person)-[:firm]->() RETURN p.name");
        assertTrue(same.contains("$p.firm->isNotEmpty()"), same);
    }

    @Test
    void edgeRefusalsAreNamed() {
        SqlTranslationException label = edgeRefusal("MATCH (a:Acting) RETURN a");
        assertEquals(SqlTranslationException.Kind.INVALID, label.kind(), label.getMessage());
        assertTrue(label.getMessage().contains("names the edge class demo::Acting"), label.getMessage());
        SqlTranslationException compact = edgeRefusal("MATCH (p:Person)-[r:Directing]->(f:Firm) RETURN r");
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, compact.kind(), compact.getMessage());
        assertTrue(compact.getMessage().contains("holds its from end director with no inverse end on demo::Person"),
                compact.getMessage());
        // a hop from a node reached through an edge class: the edge hop is two steps, so the next hop opens a
        // segment re-anchored on that node (the study's §12.54; refused by name before)
        String past = edges("MATCH (p:Person)-[:ACTED_IN]->(f:Firm)-[:hq]->(c:City) RETURN c.name AS n");
        assertTrue(past.contains("->join(") && past.contains(".hq"), past);
        SqlTranslationException wrongWay = edgeRefusal("MATCH (f:Firm)-[r:ACTED_IN]->(p:Person) RETURN r");
        assertEquals(SqlTranslationException.Kind.INVALID, wrongWay.kind(), wrongWay.getMessage());
        assertTrue(wrongWay.getMessage().contains("which no pattern from demo::Firm reads outgoing"),
                wrongWay.getMessage());
    }

    @Test
    void aVariantsEntryIsThePlatformsGetTypedWhereTheShapeIsKnown() {
        // an unwound map's entries (the study's §12.43): get, read by the entry's kind where it is known
        String typed = pure("UNWIND [{a: 1, b: 'x'}] AS m RETURN m.a AS a, m.b AS b");
        assertTrue(typed.contains("$r.m->get('a')->to(@Integer)"), typed);
        assertTrue(typed.contains("$r.m->get('b')->to(@String)"), typed);
        // a key the map lacks is null before the database is asked; a scalar entry has no entries
        String lacks = pure("UNWIND [{a: 1}] AS m RETURN m.z AS z");
        assertTrue(lacks.contains("z:r|[]"), lacks);
        SqlTranslationException scalar = refusal("UNWIND [{a: 1}] AS m RETURN m.a.b");
        assertEquals(SqlTranslationException.Kind.INVALID, scalar.kind(), scalar.getMessage());
        // a collected node's property: get, a Variant (the node's class is not carried)
        String node = pure("MATCH (p:Person) WITH collect(p) AS ps UNWIND ps AS n RETURN n.name AS name");
        assertTrue(node.contains("$r.n->get('name')"), node);
        assertFalse(node.contains("get('name')->to("), node);
        // a list of several kinds carried through WITH: each place by its own kind
        String tuple = pure("MATCH (p:Person) WITH [p.name, 1] AS l RETURN l[0] AS s, l[1] AS i, l[2] AS none");
        assertTrue(tuple.contains("$r.l->get(0)->to(@String)"), tuple);
        assertTrue(tuple.contains("$r.l->get(1)->to(@Integer)"), tuple);
        assertTrue(tuple.contains("none:r|[]"), tuple);
        // a node indexed by a key of the row: get by the key
        String key = pure("MATCH (p:Person) UNWIND ['name', 'age'] AS k RETURN p[k] AS v");
        assertTrue(key.contains("->get($r.k->toOne())"), key);
    }

    @Test
    void graphFunctionsOverValuesRaiseNeo4jsTypeErrorsOrRefuseAnUnknownMark() {
        SqlTranslationException node = refusal("MATCH (p:Person) RETURN type(p)");
        assertEquals(SqlTranslationException.Kind.INVALID, node.kind(), node.getMessage());
        assertTrue(node.getMessage().contains("expected Relationship but was Node"), node.getMessage());
        SqlTranslationException length = refusal("MATCH (p:Person) RETURN length(p)");
        assertTrue(length.getMessage().contains("expected Path but was Node"), length.getMessage());
        SqlTranslationException properties = refusal("RETURN properties([true, false])");
        assertTrue(properties.getMessage().contains("expected Map, Node or Relationship but was List<T>"),
                properties.getMessage());
        // a relationship in a list of several kinds: its place is a relationship, its type read from the Variant
        String typed = pure("MATCH (p:Person)-[r:firm]->(f:Firm) WITH [r, 1] AS l RETURN type(l[0]) AS t");
        assertTrue(typed.contains("$r.l->get(0)->get('@type')->to(@String)"), typed);
        // a Variant of no known mark is refused by name, never read as a relationship
        SqlTranslationException unknown = refusal("MATCH (p:Person) WITH [p, 1] AS l UNWIND l AS x RETURN type(x)");
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, unknown.kind(), unknown.getMessage());
        assertTrue(unknown.getMessage().contains("of a kind the island does not know"), unknown.getMessage());
    }

    @Test
    void aCycleInOnePatternIsTheNodeReachedAgainByIdentity() {
        // (the study's §12.50) a node named again in one pattern reads under a fresh name, the same node by
        // identity: a condition over the materialized rows, with the hop's node present where its identity is
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty() && $p.firm.employees->isNotEmpty())->project(~[_cypher_2:p|'[\"Person\"]', _cypher_3:p|if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[])), _cypher_4:p|$p.age, _cypher_5:p|$p.name, _cypher_6:p|$p->meta::legend::lite::elementId(), _cypher_7:p|'[\"Firm\"]', _cypher_8:p|$p.firm.legalName, _cypher_9:p|$p.firm->meta::legend::lite::elementId(), _cypher_10:p|'[\"Person\"]', _cypher_11:p|if($p.firm.employees.active->isNotEmpty() && $p.firm.employees.active->toOne(), |true, |if($p.firm.employees.active->isNotEmpty() && !$p.firm.employees.active->toOne(), |false, |[])), _cypher_12:p|$p.firm.employees.age, _cypher_13:p|$p.firm.employees.name, _cypher_14:p|$p.firm.employees->meta::legend::lite::elementId()])->filter(r|$r._cypher_14->isNotEmpty() && ($r._cypher_14->toOne() == $r._cypher_6))->extend(~[n:r|$r._cypher_5])->select(~[n])", pure("MATCH (p:Person)-[:firm]->(f)-[:employees]->(p) RETURN p.name AS n"));
        // the second hop of a fanning graph: its node's presence is the rows' (a two-step existence in the graph's
        // filter the resolver reads no bridge-table step of), its properties and identity nullable there
        assertEquals("demo::Firm.all()->filter(f|$f.employees->isNotEmpty())->project(~[_cypher_2:f|'[\"Firm\"]', _cypher_3:f|$f.legalName, _cypher_4:f|$f->meta::legend::lite::elementId(), _cypher_5:f|'[\"Person\"]', _cypher_6:f|if($f.employees.active->isNotEmpty() && $f.employees.active->toOne(), |true, |if($f.employees.active->isNotEmpty() && !$f.employees.active->toOne(), |false, |[])), _cypher_7:f|$f.employees.age, _cypher_8:f|$f.employees.name, _cypher_9:f|$f.employees->meta::legend::lite::elementId(), _cypher_10:f|'[\"Firm\"]', _cypher_11:f|$f.employees.firm.legalName, _cypher_12:f|$f.employees.firm->meta::legend::lite::elementId()])->filter(r|$r._cypher_12->isNotEmpty() && ($r._cypher_12->isNotEmpty() && ($r._cypher_12->toOne() == $r._cypher_4)))->extend(~[l:r|$r._cypher_3])->select(~[l])", pure("MATCH (f:Firm)-[:employees]->(p)-[:firm]->(f) RETURN f.legalName AS l"));
        // a self-loop over a to-one hop: the node bound at both ends, the identities compared in the graph
        assertEquals("demo::Person.all()->filter(p|$p.firm->isNotEmpty() && ($p.firm->meta::legend::lite::elementId()->isNotEmpty() && ($p.firm->meta::legend::lite::elementId()->toOne() == $p->meta::legend::lite::elementId())))->project(~[n:p|$p.name])", pure("MATCH (p:Person)-[:firm]->(p) RETURN p.name AS n"));
    }

    @Test
    void headAndLastOfAListTheDatabaseHoldsReadItsEnds() {
        // a variable-length relationship's list differs across the arms (empty at length zero): the union's shape
        // is a list of relationships, and last(r) its last place (null for the empty list), as r[-1] reads it
        assertEquals("demo::Firm.all()->project(~[_cypher_1:f|'[\"Firm\"]', _cypher_2:f|$f.legalName, _cypher_3:f|$f->meta::legend::lite::elementId(), _cypher_4:f|'[\"Firm\"]', _cypher_5:f|$f.legalName, _cypher_6:f|if(false, |true, |if(false, |false, |[])), _cypher_7:f|[]->cast(@Integer), _cypher_8:f|[]->cast(@String), _cypher_9:f|$f->meta::legend::lite::elementId(), r:f|fromJson('[]')])->concatenate(demo::Person.all()->filter(x|$x.firm->isNotEmpty())->project(~[_cypher_1:x|'[\"Firm\"]', _cypher_2:x|$x.firm.legalName, _cypher_3:x|$x.firm->meta::legend::lite::elementId(), _cypher_4:x|'[\"Person\"]', _cypher_5:x|[]->cast(@String), _cypher_6:x|if($x.active->isNotEmpty() && $x.active->toOne(), |true, |if($x.active->isNotEmpty() && !$x.active->toOne(), |false, |[])), _cypher_7:x|$x.age, _cypher_8:x|$x.name, _cypher_9:x|$x->meta::legend::lite::elementId(), r:x|fromJson(('[' + fromJson((('{\"@type\":' + '\"employees\"') + if(if($x.firm->meta::legend::lite::elementId()->isEmpty() || $x->meta::legend::lite::elementId()->isEmpty(), |[]->cast(@String), |'employees' + ':' + $x.firm->meta::legend::lite::elementId()->toOne() + '>' + $x->meta::legend::lite::elementId()->toOne())->isEmpty(), |'', |',\"@id\":' + ('\"' + if($x.firm->meta::legend::lite::elementId()->isEmpty() || $x->meta::legend::lite::elementId()->isEmpty(), |[]->cast(@String), |'employees' + ':' + $x.firm->meta::legend::lite::elementId()->toOne() + '>' + $x->meta::legend::lite::elementId()->toOne())->toOne()->replace('\\\\', '\\\\\\\\')->replace('\"', '\\\\\"') + '\"'))) + '' + '}')->toString()) + ']')]))->extend(~[l:r|$r.r->get(-1)])->select(~[l])", pure("MATCH (f:Firm)-[r:employees*0..1]->(x) RETURN last(r) AS l"));
        String head = pure("MATCH (p:Person) WITH collect(p.name) AS names RETURN head(names) AS h");
        assertTrue(head.contains("->get(0)"), head);
    }

    @Test
    void randAndTheClocksAreTheDatabasesValuesOfTheCall() {
        // (the study's §12.51) rand() is Pure's random, the database's float in [0, 1); randomUUID() its uuid
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[r:r|meta::pure::functions::math::random(), u:r|generateGuid()])->select(~[r, u])",
                pure("RETURN rand() AS r, randomUUID() AS u"));
        // the statement's clock: today and now, the time of day of now, the instant in UTC
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[d:r|today(), ldt:r|now(), lt:r|now()->meta::legend::lite::timeOf()])->select(~[d, ldt, lt])",
                pure("RETURN date() AS d, localdatetime() AS ldt, localtime() AS lt"));
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[z:r|now(), t:r|%1970-01-01->meta::legend::lite::atTime(now()->meta::legend::lite::timeOf())])->select(~[z, t])",
                pure("RETURN datetime() AS z, time() AS t"));
        // a null argument is null
        assertEquals("#TDS{\n_cypher_row\n1\n}#->extend(~[d:r|[]])->select(~[d])",
                pure("RETURN datetime.statement(null) AS d"));
        // the three clocks are the statement's
        assertEquals(pure("RETURN date() AS d"), pure("RETURN date.realtime() AS d"));
        assertEquals(pure("RETURN localdatetime() AS d"), pure("RETURN localdatetime.transaction() AS d"));
        // Neo4j's errors: rand() inside an aggregate, a clock with a time zone refused by name
        invalid("RETURN count(rand())", "Can't use non-deterministic (random) functions inside of aggregate");
        unsupported("RETURN date.statement({timezone: 'Europe/Stockholm'})", "with an argument (a time zone");
    }

    @Test
    void patternPredicatesOverRowsAreFlagsJoinedBackByIdentity() {
        // (the study's §12.52) a pattern predicate in a WHERE over rows: the pattern read as its own graph, its
        // matches narrowed to the shared variables' identities and deduplicated, a constant beside them, left-joined
        // back on the identities; the predicate is the constant's presence, dropped after the filter
        assertEquals("demo::Person.all()->project(~[_cypher_1:p|'[\"Person\"]', _cypher_2:p|if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[])), _cypher_3:p|$p.age, _cypher_4:p|$p.name, _cypher_5:p|$p->meta::legend::lite::elementId()])->join(demo::Firm.all()->project(~[_cypher_6:f|'[\"Firm\"]', _cypher_7:f|$f.legalName, _cypher_8:f|$f->meta::legend::lite::elementId()]), meta::pure::functions::relation::JoinKind.INNER, {l, r|true})->join(demo::Person.all()->filter(_cypher_9|$_cypher_9.firm->isNotEmpty())->project(~[_cypher_11:_cypher_9|'[\"Person\"]', _cypher_12:_cypher_9|if($_cypher_9.active->isNotEmpty() && $_cypher_9.active->toOne(), |true, |if($_cypher_9.active->isNotEmpty() && !$_cypher_9.active->toOne(), |false, |[])), _cypher_13:_cypher_9|$_cypher_9.age, _cypher_14:_cypher_9|$_cypher_9.name, _cypher_15:_cypher_9|$_cypher_9->meta::legend::lite::elementId(), _cypher_16:_cypher_9|'[\"Firm\"]', _cypher_17:_cypher_9|$_cypher_9.firm.legalName, _cypher_18:_cypher_9|$_cypher_9.firm->meta::legend::lite::elementId()])->select(~[_cypher_15, _cypher_18])->distinct()->extend(~[_cypher_19:r|1]), meta::pure::functions::relation::JoinKind.LEFT, {l, r|($l._cypher_5 == $r._cypher_15) && ($l._cypher_8 == $r._cypher_18)})->filter(r|$r._cypher_19->isNotEmpty())->select(~[_cypher_1, _cypher_2, _cypher_3, _cypher_4, _cypher_5, _cypher_6, _cypher_7, _cypher_8])->extend(~[n:r|$r._cypher_4])->select(~[n])", pure("MATCH (p:Person), (f:Firm) WHERE (p)-[:firm]->(f) RETURN p.name AS n"));
        // in a WITH's WHERE, negated and beside another condition
        assertEquals("demo::Person.all()->project(~[_cypher_1:p|'[\"Person\"]', _cypher_2:p|if($p.active->isNotEmpty() && $p.active->toOne(), |true, |if($p.active->isNotEmpty() && !$p.active->toOne(), |false, |[])), _cypher_3:p|$p.age, _cypher_4:p|$p.name, _cypher_5:p|$p->meta::legend::lite::elementId()])->join(demo::Firm.all()->project(~[_cypher_6:f|'[\"Firm\"]', _cypher_7:f|$f.legalName, _cypher_8:f|$f->meta::legend::lite::elementId()]), meta::pure::functions::relation::JoinKind.INNER, {l, r|true})->join(demo::Person.all()->filter(_cypher_9|$_cypher_9.firm->isNotEmpty())->project(~[_cypher_11:_cypher_9|'[\"Person\"]', _cypher_12:_cypher_9|if($_cypher_9.active->isNotEmpty() && $_cypher_9.active->toOne(), |true, |if($_cypher_9.active->isNotEmpty() && !$_cypher_9.active->toOne(), |false, |[])), _cypher_13:_cypher_9|$_cypher_9.age, _cypher_14:_cypher_9|$_cypher_9.name, _cypher_15:_cypher_9|$_cypher_9->meta::legend::lite::elementId(), _cypher_16:_cypher_9|'[\"Firm\"]', _cypher_17:_cypher_9|$_cypher_9.firm.legalName, _cypher_18:_cypher_9|$_cypher_9.firm->meta::legend::lite::elementId()])->select(~[_cypher_15, _cypher_18])->distinct()->extend(~[_cypher_19:r|1]), meta::pure::functions::relation::JoinKind.LEFT, {l, r|($l._cypher_5 == $r._cypher_15) && ($l._cypher_8 == $r._cypher_18)})->extend(~[_cypher_20:r|$r._cypher_19->isEmpty() && ($r._cypher_3->isNotEmpty() && ($r._cypher_3->toOne() > 1))])->select(~[_cypher_1, _cypher_2, _cypher_3, _cypher_4, _cypher_5, _cypher_6, _cypher_7, _cypher_8, _cypher_20])->filter(r|$r._cypher_20)->select(~[_cypher_1, _cypher_2, _cypher_3, _cypher_4, _cypher_5, _cypher_6, _cypher_7, _cypher_8])->extend(~[l:r|$r._cypher_7])->select(~[l])", pure("MATCH (p:Person), (f:Firm) WITH p, f WHERE NOT (p)-[:firm]->(f) AND p.age > 1 RETURN f.legalName AS l"));
        // an EXISTS subquery over rows with its own WHERE
        assertTrue(pure("MATCH (p:Person), (f:Firm) WHERE EXISTS { (p)-[:firm]->(f) WHERE f.legalName = 'x' } RETURN p.name AS n")
                .contains("->distinct()"));
        // a predicate inside a quantifier over a graph reads from the graph beneath the binding
        assertTrue(pure("MATCH (p:Person) RETURN any(x IN [1, 2] WHERE (p)-[:firm]->()) AS has")
                .contains("$p.firm->isNotEmpty()"));
    }

    @Test
    void comprehensionsOverListsTheDatabaseHoldsReadThemAsCollections() {
        // (the study's §12.52) a collected list, a CASE's or coalesce's list, a list plus a value: comprehensions,
        // quantifiers, reverse, tail and concatenation read the Variant back as the collection of its kind
        assertEquals("demo::Person.all()->project(~[_cypher_1:p|if($p.age->isEmpty(), |[]->cast(@String), |$p.age->toOne()->toString())])->aggregate(~[_cypher_2:x|$x._cypher_1:y|$y->joinStrings(',')])->extend(~[ages:r|fromJson('[' + if($r._cypher_2->isEmpty(), |'', |$r._cypher_2->toOne()) + ']')])->select(~[ages])->extend(~[big:r|$r.ages->toMany(@Integer)->filter(x|$x > 1)->map(x|$x)->toVariant(), one:r|$r.ages->toMany(@Integer)->filter(x|$x == 2)->size() == 1])->select(~[big, one])", pure("MATCH (p:Person) WITH collect(p.age) AS ages RETURN [x IN ages WHERE x > 1 | x] AS big, single(x IN ages WHERE x = 2) AS one"));
        assertAll(
                () -> assertEquals("[1, 2]->flatten(~x)->select(~[x])->extend(~[c:r|fromJson(((('[' + toString(10)) + (',' + toString(20))) + (',' + if($r.x->isEmpty(), |'null', |$r.x->toOne()->toString()))) + ']')])->select(~[c])", pure("UNWIND [1, 2] AS x WITH [10, 20] AS l, x RETURN l + x AS c")),
                () -> assertEquals("[fromJson('[1,2]'), fromJson('[3]')]->flatten(~l)->extend(~[r:r|$r.l->toMany(@Integer)->reverse()->toVariant(), t:r|$r.l->toMany(@Integer)->drop(1)->toVariant(), c:r|$r.l->toMany(@Integer)->concatenate(fromJson('[4]')->toMany(@Integer))->toVariant()])->select(~[r, t, c])", pure("UNWIND [[1, 2], [3]] AS l RETURN reverse(l) AS r, tail(l) AS t, l + [4] AS c")),
                () -> assertEquals("1->range(2)->flatten(~_cypher_place)->extend(~[m:p|if($p._cypher_place == 1, |fromJson('{\"l\":[1],\"f\":true}'), |[]->cast(@meta::pure::metamodel::variant::Variant))])->select(~[m])->extend(~[c:r|if($r.m->get('f')->to(@Boolean)->isNotEmpty() && $r.m->get('f')->to(@Boolean)->toOne(), |$r.m->get('l'), |fromJson('[0]'))])->select(~[c])->extend(~[odd:r|$r.c->toMany(@Integer)->exists(x|$x->rem(2) == 1)])->select(~[odd])", pure("UNWIND [{l: [1], f: true}] AS m WITH CASE WHEN m.f THEN m.l ELSE [0] END AS c RETURN any(x IN c WHERE x % 2 = 1) AS odd")));
        // collected relationships read back as Variants, the elements' mark on the bound variable (type(x) reads it)
        String rels = pure("MATCH (p:Person)-[r:firm]->() WITH collect(r) AS rs RETURN [x IN rs | type(x)] AS t");
        assertTrue(rels.contains("->toMany(@meta::pure::metamodel::variant::Variant)->map(x|'firm')"), rels);
    }

    @Test
    void theTailOfSmallShapesReadsAsTheDatabasesValues() {
        // (the study's §12.53) an ORDER BY on an expression the query does not return, without a SKIP or LIMIT:
        // a hidden key the result render carries (Render.withSortOutputs)
        String sorted = pure("MATCH (p:Person) RETURN p.name AS who ORDER BY p.age");
        assertTrue(sorted.contains("->sort(") && sorted.endsWith("->select(~[who])"), sorted);
        // a string predicate on a value that is not a string is null, with no string test spelled
        String number = pure("MATCH (p:Person) RETURN p.age STARTS WITH '1' AS a, true CONTAINS 'r' AS b");
        assertFalse(number.contains("startsWith") || number.contains("contains("), number);
        // a value the rows hold as a Variant: its string when it holds one (the database's kind test), else null
        String variant = pure("UNWIND [1, 'a'] AS x RETURN x STARTS WITH 'a' AS s");
        assertTrue(variant.contains("meta::legend::lite::isKind(") && variant.contains("'string'")
                && variant.contains("startsWith("), variant);
        // collect(DISTINCT x): the distinct texts joined (S78)
        String distinct = pure("MATCH (p:Person) RETURN collect(DISTINCT p.name) AS c");
        assertTrue(distinct.contains("->distinct()->joinStrings("), distinct);
        // (a duration text in the date-time form, P2012-02-02T14:37:21.545, reads as the map form's fields: this
        // model declares no Duration class, so lite/tail.yaml's a_duration_text_in_the_date_time_form, judged by
        // Neo4j on both lanes, is its pin)
    }
}
