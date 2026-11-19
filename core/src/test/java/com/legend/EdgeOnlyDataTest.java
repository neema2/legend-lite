// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaConstructor;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaFieldAccess;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * GUARD L-B: THE PLAN'S AND THE MIR'S DATA, EACH PIECE THE PRODUCT'S OWN (the orchestrator's order of 2026-11-18, after
 * J28 -- a MIR variant the edge alone made -- and J31 -- legend-engine's template fields, set and read by the edge
 * alone).
 * Every record component, enum constant and sealed variant of the MIR ({@code com.legend.sql}), the plans
 * ({@code com.legend.executionplan}, {@code com.legend.testexecutionplan}) and the typed tree
 * ({@code com.legend.compiler.spec.typed}) needs a REAL product use:
 * <ul>
 *   <li>a component is READ by product code;</li>
 *   <li>a constant is NAMED by product code (a switch's arm is a case, not a use: javac's switch tables are left
 *   out);</li>
 *   <li>a variant is MADE by product code.</li>
 * </ul>
 * Copying is no use: the record's own code (its constructor, its {@code withX}), a {@code SqlRewriter} (it rebuilds
 * every node it walks) and the plans' codecs ({@code MirJson}, {@code PlanJson}, {@code RecordJson}: they carry every
 * field both ways). Each piece with no real product use is reported, the edge's use beside it, and pinned with its
 * ruling ({@link #OPEN}); a new one fails. Named limits: a value-level shape (a component the product always sets to
 * null, a constant it never produces but compares against) is not seen; a constant compiled into a switch through an
 * {@code invokedynamic} label is a case, as a switch table is.
 */
@Tag("guardrail")
class EdgeOnlyDataTest {

    /** The packages whose data is checked (each exactly, not its subpackages). */
    static final Set<String> PACKAGES = Set.of("com.legend.sql", "com.legend.executionplan",
            "com.legend.testexecutionplan", "com.legend.compiler.spec.typed");

    /** The plans' codecs: they carry every field both ways, so their reads and makes are copies. */
    static final Set<String> CODECS = Set.of("com.legend.executionplan.MirJson", "com.legend.executionplan.PlanJson",
            "com.legend.testexecutionplan.RecordJson");

    private static final String EDGE = "com.legend.edge.";

    private static final String REWRITER = "com.legend.sql.SqlRewriter";

    /** Each piece with no real product use, "kind owner#piece: who uses it" -> its ruling, measured 2026-11-18 and ruled
     *  by the orchestrator (06:40): a value starting "L-B." is a census line (counted); "READ_API" a fact of lite's own
     *  the edge reads (what it MEANS decides, not who reads it); "J40." a piece already counted on that line;
     *  "DEAD" data no one reads, deleted in train 17 by its package's owner (shrink-only, not a separation line);
     *  "RULED_PENDING" a question before the user. */
    static final Map<String, String> OPEN = Map.ofEntries(
            // (SqlExpr$PlanParam#kind, now #valueType: ruled the product's in meaning 2026-11-18, train 18 --
            // RULED_IN_MEANING; the measure now sees EnumValueTables' read of it, a rewriter's decision; L-B.PlanParam
            // closes)
            // facts of lite's own (the orchestrator's rulings at J40 and 06:40)
            Map.entry("variant com.legend.sql.SqlDdl$DropSchema: by the edge",
                    "READ_API (2026-11-18): generic SQL DDL in lite's sealed MIR vocabulary (ruled READ_API at J40)"),
            Map.entry("component com.legend.compiler.spec.typed.ExecutionContext#connectionInstance: by the edge",
                    "READ_API (2026-11-18): lite's execution frame's own connection fact (ruled at J40)"),
            Map.entry("component com.legend.compiler.spec.typed.ExecutionContext#quoteIdentifiers: by the edge",
                    "READ_API (2026-11-18): lite's execution frame's own connection fact (ruled at J40)"),
            Map.entry("component com.legend.compiler.spec.typed.ExecutionContext#storeFqn: by the edge",
                    "READ_API (2026-11-18): lite's execution frame's own connection fact (ruled at J40)"),
            // train 16 (F's e92c465d) moved planText to the edge's EnginePlans: a plan connection's class name and type
            // are now read by the edge alone -- EdgeOnlyMembers' READ_API rows of the same members (guard-d16)
            Map.entry("component com.legend.compiler.spec.typed.ExecutionContext$PlanConnection#className: by the edge",
                    "READ_API (2026-11-18): a plan connection's own fact (ruled at J40; planText moved to the edge,"
                            + " train 16)"),
            Map.entry("component com.legend.compiler.spec.typed.ExecutionContext$PlanConnection#type: by the edge",
                    "READ_API (2026-11-18): a plan connection's own fact (ruled at J40; planText moved to the edge,"
                            + " train 16)"),
            Map.entry("component com.legend.testexecutionplan.EnumFacts#decodes: by the edge",
                    "READ_API (2026-11-18): lite's test plan's enum facts (plan-as-data slice 4)"),
            Map.entry("component com.legend.testexecutionplan.EnumFacts#enums: by the edge",
                    "READ_API (2026-11-18): lite's test plan's enum facts (plan-as-data slice 4)"),
            Map.entry("component com.legend.testexecutionplan.EnumFacts#properties: by the edge",
                    "READ_API (2026-11-18): lite's test plan's enum facts (plan-as-data slice 4)"),
            // (SqlExpr$DeferredTdsString with its three components and Form.CSV, and SqlExpr$TempTableInSplice: deleted
            // 2026-11-18 by E-SQL's J40, 9822fd89c8, train 17 -- the J40.SqlExpr line closes)
            // dead data (the orchestrator's ruling: a shrink-only DEAD list, each deleted in train 17 by its owner;
            // LoadCsv#types checked first: no host plan reader -- engine-client, datacube, python -- and no plan codec
            // reads LoadCsv, measured 2026-11-18)
            Map.entry("component com.legend.compiler.spec.typed.Evaluation$Site#origin: by none",
                    "DEAD (2026-11-18): read by no one; deleted in train 17 by compiler.spec.typed's owner"),
            Map.entry("component com.legend.compiler.spec.typed.Evaluation$Site#path: by none",
                    "DEAD (2026-11-18): read by no one; deleted in train 17 by compiler.spec.typed's owner"),
            Map.entry("component com.legend.sql.SqlDml$LoadCsv#types: by none",
                    "DEAD (2026-11-18): read by no one, no host or codec either; deleted in train 17 by sql's owner"),
            Map.entry("variant com.legend.sql.SqlDml$InsertFromTable: by none",
                    "DEAD (2026-11-18): made by no one; deleted in train 17 by sql's owner"),
            // the user's pending proposal A
            Map.entry("component com.legend.executionplan.ExecutionPlan$Sql#tree: by none",
                    "RULED_PENDING (2026-11-18): the user's proposal A -- drop the unread MIR tree from run plans (G11)"),
            // found 2026-11-18 on train 19's trial (the host train's v57, the GUARD stream): a precise decimal column's
            // digits, written into every plan with one and read by no one -- no Java reader, no TS or Python host reads the
            // "decimal" key; kept keyed as self-describing plan data by the orchestrator's ruling, its size growth before the
            // user (cd7d1eece0, "PENDING THE USER'S WORD")
            Map.entry("component com.legend.executionplan.ExecutionPlan$DecimalDigits#precision: by none",
                    "RULED_PENDING (2026-11-18): v57's decimal digits, self-describing and unread; the size before the user"),
            Map.entry("component com.legend.executionplan.ExecutionPlan$DecimalDigits#scale: by none",
                    "RULED_PENDING (2026-11-18): v57's decimal digits, self-describing and unread; the size before the user"));

    /** PIECES EDGE-ONLY IN MEANING, NOT IN READERS (the orchestrator's ruling of 2026-11-18, train 18, on the GUARD
     *  stream's L-B check of E-Java's item 2b, 11bb6517d3): the measure above finds a product reader for each --
     *  JsonEmission, routing them into the edge's ResultEnvelopes facet -- yet each exists only for legend-engine's
     *  result envelope: TypedJsonResult's TDS and CLASS kinds (made by the edge's EngineExecutedValues alone) and its sql
     *  component (set by the edge alone). Counted, one line, owner E-Java; each checked present, so the line closes as
     *  they leave the product (the edge carrying its own envelope node, or lowering it itself). The TDS branch's shared
     *  column and cell parts stay: legend-pure's toJSON vocabulary, the product's. Shrink-only. */
    static final Map<String, String> IN_MEANING = Map.of();
            // (TypedJsonResult's TDS and CLASS kinds and its sql component: gone 2026-11-18 by E-Java's ef7ee2b69d, the
            // orchestrator's ruling (A) -- one neutral kind, STYLED, written by the style's ResultEnvelopes facet over the
            // product's parts; L-B.TypedJsonResult closes)
            // (TypedSort#pureNullOrder: gone 2026-11-18 by F's L14 -- every relation sort stamps Pure's null-largest
            // placement, the legacy TDS shapes' provenance flag deleted; L-B.pureNullOrder closes)

    /** PIECES EXAMINED IN MEANING AND RULED THE PRODUCT'S (the orchestrator's rulings, each dated): not counted, each
     *  checked present so a ruling names what is there (a piece gone: delete its row). */
    static final Map<String, String> RULED_IN_MEANING = Map.of(
            // the lens audit's L13, verified by F (the orchestrator's ruling of 2026-11-18, train 18): a product scope --
            // StoreResolver.viewSlots materializes the view body with its own demand, TemporalFrame scans the milestoned
            // table inside it, CollectionLanes classes it a relation, the product's lowering is transparent
            // (ProductLoweringForms.view returns the body); its name read by the edge's view form and by the product's
            // diagnostic label (Pipelines.materialize)
            "variant com.legend.compiler.spec.typed.TypedViewRelation",
            "2026-11-18: a product scope, the view's own slots and milestoning; its name the product's diagnostic label",
            // L-B.PlanParam (the orchestrator's ruling of 2026-11-18, train 18): the parameter's declared Pure type as a
            // value class, a fact about the user's program, read for the product's own decision -- sql.EnumValueTables
            // tests ENUM to choose the value-table IN (applied by PlanMaker and ProgramState); only the template
            // spellings (DATETIME->TIMESTAMP, DB2 BOOLEAN, quoted ENUM, InProtocol's OTHER) are the edge's, in its
            // dialects and EngineParams. E-Java renamed it (df50405375: Kind -> ValueType, the component kind ->
            // valueType; the wire key "kind" kept, the plan version unchanged): this row followed, 2026-11-18
            "component com.legend.sql.SqlExpr$PlanParam#valueType",
            "2026-11-18: the parameter's declared value class, the user's program's; EnumValueTables decides by it");

    /** The census lines this guard holds: one per "L-B." line. */
    static Set<String> lines() {
        Set<String> out = new TreeSet<>();
        OPEN.values().stream().filter(v -> v.startsWith("L-B.")).forEach(v -> out.add(v.split(" ")[0]));
        IN_MEANING.values().forEach(v -> out.add(v.split(" ")[0]));
        return out;
    }

    /** Whether {@code piece} ("constant C.NAME", "component C#name" or "variant C", C a binary class name) is in the
     *  product. */
    static boolean present(String piece) {
        String what = piece.substring(piece.indexOf(' ') + 1);
        try {
            if (piece.startsWith("variant ")) {
                Class.forName(what);
                return true;
            }
            if (piece.startsWith("constant ")) {
                int dot = what.lastIndexOf('.');
                Object[] constants = Class.forName(what.substring(0, dot)).getEnumConstants();
                return constants != null && java.util.Arrays.stream(constants)
                        .anyMatch(c -> ((Enum<?>) c).name().equals(what.substring(dot + 1)));
            }
            if (piece.startsWith("component ")) {
                int hash = what.indexOf('#');
                java.lang.reflect.RecordComponent[] parts = Class.forName(what.substring(0, hash)).getRecordComponents();
                return parts != null && java.util.Arrays.stream(parts)
                        .anyMatch(r -> r.getName().equals(what.substring(hash + 1)));
            }
        } catch (ClassNotFoundException e) {
            return false;
        }
        throw new AssertionError("a piece is a constant or a component: " + piece);
    }

    enum Who { PRODUCT, EDGE, COPY }

    static String top(String name) {
        return name.contains("$") ? name.substring(0, name.indexOf('$')) : name;
    }

    /** Whose use {@code use} of a piece of {@code owner} is. The record's own code copies in its constructors, in
     *  {@code equals}, {@code hashCode} and {@code toString}, and in a method returning its own type (a {@code withX});
     *  its other methods use. */
    /** Whether {@code unit} constructs {@code owner}: the read it makes of the record rebuilds it. */
    static boolean rebuilds(JavaCodeUnit unit, JavaClass owner) {
        return unit.getConstructorCallsFromSelf().stream()
                .anyMatch(c -> c.getTargetOwner().getName().equals(owner.getName()));
    }

    static Who who(JavaAccess<?> use, JavaClass owner) {
        JavaClass origin = use.getOriginOwner();
        String name = origin.getName();
        if (top(name).startsWith(EDGE)) {
            return Who.EDGE;
        }
        if (CODECS.contains(top(name))) {
            return Who.COPY;
        }
        // a rewriter's read copies only where the same method rebuilds the record (the generic rewrite reads every
        // component to construct it again); a rewriter that reads a component to DECIDE -- EnumValueTables testing a plan
        // parameter's kind -- is a product use (the hole the orchestrator's question found, 2026-11-18, train 18: every
        // rewriter read had counted as a copy, so PlanParam#kind read "by the edge")
        if (origin.isAssignableTo(REWRITER)) {
            return rebuilds(use.getOrigin(), owner) ? Who.COPY : Who.PRODUCT;
        }
        if (name.equals(owner.getName()) || name.startsWith(owner.getName() + "$")) {
            JavaCodeUnit unit = use.getOrigin();
            boolean copies = unit instanceof JavaConstructor || unit.getName().equals("<clinit>")
                    || Set.of("equals", "hashCode", "toString").contains(unit.getName())
                    || unit.getRawReturnType().getName().equals(owner.getName());
            return copies ? Who.COPY : Who.PRODUCT;
        }
        return Who.PRODUCT;
    }

    /** The uses of {@code m} and of every supertype's method it implements (a call through the interface, as
     *  {@code TypedSpec.info()} or {@code SqlExpr.type()}, reaches the record's accessor by dispatch). */
    static List<JavaAccess<?>> usesOf(JavaClass owner, JavaMethod m) {
        List<JavaAccess<?>> out = new ArrayList<>(m.getCallsOfSelf());
        out.addAll(m.getReferencesToSelf());
        for (JavaClass sup : owner.getAllRawInterfaces()) {
            sup.tryGetMethod(m.getName()).ifPresent(s -> {
                out.addAll(s.getCallsOfSelf());
                out.addAll(s.getReferencesToSelf());
            });
        }
        for (JavaClass sup : owner.getAllRawSuperclasses()) {
            sup.tryGetMethod(m.getName()).ifPresent(s -> {
                out.addAll(s.getCallsOfSelf());
                out.addAll(s.getReferencesToSelf());
            });
        }
        return out;
    }

    /** The makes of {@code variant}: its constructors' calls and references, a call from a static factory of its own
     *  or of its top-level class followed to that factory's callers (one step: {@code Call.of(...)}). */
    static List<JavaAccess<?>> makesOf(JavaClass variant) {
        List<JavaAccess<?>> out = new ArrayList<>();
        for (JavaConstructor ctor : variant.getConstructors()) {
            List<JavaAccess<?>> direct = new ArrayList<>(ctor.getCallsOfSelf());
            direct.addAll(ctor.getReferencesToSelf());
            for (JavaAccess<?> a : direct) {
                JavaCodeUnit unit = a.getOrigin();
                boolean home = top(a.getOriginOwner().getName()).equals(top(variant.getName()));
                boolean factory = home && unit instanceof JavaMethod fm
                        && fm.getModifiers().contains(JavaModifier.STATIC) && !fm.getName().startsWith("lambda$")
                        && variant.isAssignableTo(fm.getRawReturnType().getName());
                if (factory) {
                    out.addAll(((JavaMethod) unit).getCallsOfSelf());
                    out.addAll(((JavaMethod) unit).getReferencesToSelf());
                } else if (home && unit.getName().equals("<clinit>")) {
                    // a singleton made once (DataRef.Format's DETECTED): its uses are the reads of the field holding it
                    for (JavaField f : a.getOriginOwner().getFields()) {
                        if (f.getModifiers().contains(JavaModifier.STATIC)
                                && variant.isAssignableTo(f.getRawType().getName())) {
                            f.getAccessesToSelf().stream()
                                    .filter(x -> x.getAccessType() == JavaFieldAccess.AccessType.GET).forEach(out::add);
                        }
                    }
                } else {
                    out.add(a);
                }
            }
        }
        return out;
    }

    /** Whether product code enumerates {@code e}'s constants: {@code values()} or {@code valueOf(String)} called
     *  outside the enum and the codecs (a name read from the user's model or a plan's JSON is each constant's use). */
    static boolean enumerated(JavaClass e) {
        for (JavaMethod m : e.getMethods()) {
            if (m.getName().equals("values") || m.getName().equals("valueOf")) {
                for (JavaAccess<?> a : m.getCallsOfSelf()) {
                    if (!a.getOriginOwner().getName().equals(e.getName()) && who(a, e) == Who.PRODUCT) {
                        return true;
                    }
                }
                for (JavaAccess<?> a : m.getReferencesToSelf()) {
                    if (who(a, e) == Who.PRODUCT) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /** "kind owner#piece: by the edge" or "...: by none", for each piece of {@code classes} in {@code packages} with no
     *  real product use. */
    static Set<String> found(JavaClasses classes, Set<String> packages) {
        Set<String> out = new TreeSet<>();
        for (JavaClass c : classes) {
            // the codecs' own tags are their format's words, not the plan's data
            if (!packages.contains(c.getPackageName()) || c.getName().startsWith(EDGE)
                    || CODECS.contains(top(c.getName()))) {
                continue;
            }
            Class<?> k = c.reflect();
            if (k.isRecord()) {
                for (java.lang.reflect.RecordComponent rc : k.getRecordComponents()) {
                    List<JavaAccess<?>> uses = new ArrayList<>();
                    Optional<JavaMethod> accessor = c.tryGetMethod(rc.getName());
                    accessor.ifPresent(m -> uses.addAll(usesOf(c, m)));
                    // the field's reads, but its own accessor's (the accessor is the component)
                    c.tryGetField(rc.getName()).ifPresent(f -> f.getAccessesToSelf().stream()
                            .filter(a -> a.getAccessType() == JavaFieldAccess.AccessType.GET)
                            .filter(a -> !(a.getOriginOwner().equals(c) && a.getOrigin().getName().equals(rc.getName())
                                    && a.getOrigin().getRawParameterTypes().isEmpty()))
                            .forEach(uses::add));
                    report(out, "component " + c.getName() + "#" + rc.getName(), uses, c);
                }
            }
            if (k.isEnum() && !enumerated(c)) {
                for (Object constant : k.getEnumConstants()) {
                    String name = ((Enum<?>) constant).name();
                    JavaField f = c.getField(name);
                    List<JavaAccess<?>> uses = new ArrayList<>();
                    // a switch's table (javac's synthetic $SwitchMap class) names every constant it cases, and the enum
                    // names its own (its initializer, its $values): neither is a use
                    f.getAccessesToSelf().stream().filter(a -> !a.getOriginOwner().getModifiers()
                            .contains(JavaModifier.SYNTHETIC) && !a.getOriginOwner().equals(c)).forEach(uses::add);
                    report(out, "constant " + c.getName() + "." + name, uses, c);
                }
            }
            if (k.isSealed()) {
                for (Class<?> variant : k.getPermittedSubclasses()) {
                    // an enum variant's constants are checked as constants
                    if (variant.isInterface() || variant.isEnum()
                            || java.lang.reflect.Modifier.isAbstract(variant.getModifiers())
                            || !packages.contains(variant.getPackageName())) {
                        continue;
                    }
                    JavaClass v = classes.get(variant);
                    report(out, "variant " + variant.getName(), makesOf(v), v);
                }
            }
        }
        return out;
    }

    private static void report(Set<String> out, String piece, List<JavaAccess<?>> uses, JavaClass owner) {
        boolean product = false;
        boolean edge = false;
        for (JavaAccess<?> use : uses) {
            switch (who(use, owner)) {
                case PRODUCT -> product = true;
                case EDGE -> edge = true;
                case COPY -> { }
            }
        }
        if (!product) {
            out.add(piece + (edge ? ": by the edge" : ": by none"));
        }
    }

    @Test
    void everyPieceOfThePlansDataHasAProductUse() throws IOException {
        JavaClasses classes = ImportedJars.of("legend.product.jars", "legend.authored.jars",
                "legend.edge.jars");
        Set<String> found = found(classes, PACKAGES);
        Map<String, String> byKind = new TreeMap<>();
        found.forEach(f -> byKind.merge(f.substring(f.lastIndexOf(": ") + 2), "1", (a, b) -> String.valueOf(
                Integer.parseInt(a) + 1)));
        assertEquals(new TreeSet<>(OPEN.keySet()), found, "a piece of the plan's or the MIR's data with no real product"
                + " use -- the edge's data in the product (J28's, J31's shape): move it to the edge, delete it, or rule"
                + " it here (" + byKind + ")");
    }

    @Test
    void theEdgeDataInMeaningIsStillThere() {
        assertEquals(List.of(), IN_MEANING.keySet().stream().filter(p -> !present(p)).sorted().toList(), "a piece"
                + " edge-only in meaning has left the product: delete its row (its line closes with its last)");
        assertEquals(List.of(), RULED_IN_MEANING.keySet().stream().filter(p -> !present(p)).sorted().toList(), "a piece"
                + " ruled the product's has left it: delete its row");
    }

    /** The presence check bites: a constant or a component that is not there is found gone. */
    @Test
    void thePresenceCheckBites() {
        String kind = "com.legend.compiler.spec.typed.TypedJsonResult$" + "Kind";
        String record = "com.legend.compiler.spec.typed." + "TypedJsonResult";
        assertEquals(true, present("constant " + kind + ".TDS_JSON"), "a constant there");
        assertEquals(false, present("constant " + kind + ".NO" + "PE"), "a constant not there");
        assertEquals(true, present("component " + record + "#chain"), "a component there");
        assertEquals(false, present("component " + record + "#no" + "pe"), "a component not there");
        assertEquals(false, present("component " + record + "Gone#chain"), "a class not there");
        assertEquals(true, present("variant " + record), "a variant there");
        assertEquals(false, present("variant " + record + "Go" + "ne"), "a variant not there");
    }

    @Test
    void theGuardBites() {
        JavaClasses planted = new ClassFileImporter().importClasses(com.legend.sql.PlantedEdgeOnlyMir.class,
                com.legend.sql.PlantedEdgeOnlyMir.Node.class, com.legend.sql.PlantedEdgeOnlyMir.Made.class,
                com.legend.sql.PlantedEdgeOnlyMir.Shared.class, com.legend.sql.PlantedEdgeOnlyMir.Kind.class,
                com.legend.sql.PlantedEdgeOnlyMir.Ruled.class, com.legend.sql.PlantedDecidingRewriter.class,
                com.legend.edge.PlantedEdgeOnlyMirMaker.class);
        String p = "com.legend.sql.PlantedEdgeOnlyMir$";
        assertEquals(new TreeSet<>(Set.of(
                "variant " + p + "Made: by the edge",
                "component " + p + "Made#text: by the edge",
                "component " + p + "Shared#template: by the edge",
                "constant " + p + "Kind.RAW: by the edge",
                "component " + p + "Ruled#copied: by none")), found(planted, Set.of("com.legend.sql")),
                "J28's shape (a variant the edge alone makes) and J31's (a field and a constant the edge alone reads);"
                        + " a rewriter's read to decide a use (Ruled#flag), its read only to rebuild a copy (Ruled#copied)");
    }
}
