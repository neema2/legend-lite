package com.legend.normalizer;

import com.legend.model.NormalizedModel;
import com.legend.compiler.NameResolver;
import com.legend.parser.ElementParser;
import com.legend.model.FunctionDefinition;
import com.legend.model.MappingDefinition;
import com.legend.model.PackageableElement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * M6 &mdash; the convergence corpus (CLEAN_SHEET_INVERSION §7.2). For a legacy
 * mapping and its hand-written clean-sheet equivalent, the two surfaces must
 * produce the <strong>same canonical model</strong>: the same binding table and
 * (for the cases the legacy desugarer expresses in pure clean-sheet form) the
 * same lifted-function body. This is what makes "the legacy DSL desugars to the
 * clean-sheet form" a tested claim instead of a doc sentence.
 *
 * <h2>What converges, and what doesn't</h2>
 * <ul>
 *   <li><b>Binding table</b> &mdash; always. Both surfaces yield identical
 *       {@code ClassBinding} / {@code AssociationBinding} records (same class
 *       FQN, kind, root, set id, and the same deterministic
 *       {@code <mapping>$class$<classFqn>} realizing-function FQN).</li>
 *   <li><b>Lifted-function body</b> &mdash; for shapes the desugarer expresses
 *       in pure clean-sheet primitives (M2M, relational columns), byte-for-byte,
 *       because the parser normalizes the clean-sheet spelling to the same AST
 *       the desugarer emits ({@code X.all()} &rarr; {@code getAll(X)}, the
 *       {@code ->} pipeline &rarr; nested application). The two bridge helpers
 *       ({@code legacyNavigate} / {@code legacyAssocPredicate}) are the
 *       documented exceptions and are NOT asserted to converge to pure
 *       clean-sheet at the body level (doc §2).</li>
 * </ul>
 */
@DisplayName("M6 — legacy ↔ clean-sheet convergence")
class LegacyCleanSheetConvergenceTest {

    private static NormalizedModel norm(String src) {
        return ModelNormalizer.normalize(NameResolver.resolve(ElementParser.parse(src)));
    }

    private static MappingDefinition mapping(NormalizedModel m, String fqn) {
        for (PackageableElement el : m.elements()) {
            if (el instanceof MappingDefinition md && md.qualifiedName().equals(fqn)) return md;
        }
        throw new AssertionError("no canonical mapping '" + fqn + "'");
    }

    private static FunctionDefinition liftedFn(NormalizedModel m, String fqn) {
        for (PackageableElement el : m.elements()) {
            if (el instanceof FunctionDefinition fd && fd.qualifiedName().equals(fqn)) return fd;
        }
        throw new AssertionError("no lifted function '" + fqn + "'");
    }

    /**
     * Assert two surfaces converge: identical binding tables, and identical
     * lifted-function bodies for the given realizing-function FQNs.
     */
    private static void assertConverges(String legacy, String clean, String mappingFqn,
                                        String... liftedFqns) {
        NormalizedModel lm = norm(legacy);
        NormalizedModel cm = norm(clean);
        assertEquals(mapping(lm, mappingFqn).classBindings(),
                mapping(cm, mappingFqn).classBindings(),
                "class binding tables must converge");
        assertEquals(mapping(lm, mappingFqn).associationBindings(),
                mapping(cm, mappingFqn).associationBindings(),
                "association binding tables must converge");
        for (String fqn : liftedFqns) {
            assertEquals(liftedFn(lm, fqn).body(), liftedFn(cm, fqn).body(),
                    "lifted body must converge for " + fqn);
            assertEquals(liftedFn(lm, fqn).synthesizedFrom(), liftedFn(cm, fqn).synthesizedFrom(),
                    "provenance must converge for " + fqn);
        }
    }

    /** Binding-table-only convergence: the two surfaces yield identical binding
     *  tables, but the lifted bodies are NOT asserted equal (used where the
     *  desugarer emits a bridge helper or an internal representation a human
     *  wouldn't write). */
    private static void assertBindingTablesConverge(String legacy, String clean, String mappingFqn) {
        NormalizedModel lm = norm(legacy);
        NormalizedModel cm = norm(clean);
        assertEquals(mapping(lm, mappingFqn).classBindings(),
                mapping(cm, mappingFqn).classBindings(),
                "class binding tables must converge");
        assertEquals(mapping(lm, mappingFqn).associationBindings(),
                mapping(cm, mappingFqn).associationBindings(),
                "association binding tables must converge");
    }

    @Test
    @DisplayName("M2M primitive: ~src + property binding  ==  Class.all() -> map(^Class(...))")
    void m2mPrimitiveConverges() {
        String classes =
                "Class model::Person { name: String[1]; } "
              + "Class model::RawPerson { name: String[1]; } ";
        String legacy = classes
              + "Mapping my::M ( "
              + "  *model::Person: Pure { ~src model::RawPerson name: $src.name } "
              + ")";
        String clean = classes
              + "Mapping my::M ( "
              + "  *model::Person: Pure { "
              + "    model::RawPerson.all() -> map(src | ^model::Person(name = $src.name)) "
              + "  } "
              + ")";
        // Full convergence: binding table AND byte-equal lifted body.
        assertConverges(legacy, clean, "my::M", "my::M$class$model::Person");
    }

    @Test
    @DisplayName("M2M with ~filter: filter(src | ...) converges byte-for-byte")
    void m2mWithFilterConverges() {
        String classes =
                "Class model::Person { name: String[1]; } "
              + "Class model::RawPerson { name: String[1]; active: Boolean[1]; } ";
        String legacy = classes
              + "Mapping my::M ( "
              + "  *model::Person: Pure { ~src model::RawPerson ~filter $src.active name: $src.name } "
              + ")";
        String clean = classes
              + "Mapping my::M ( "
              + "  *model::Person: Pure { "
              + "    model::RawPerson.all() -> filter(src | $src.active) "
              + "      -> map(src | ^model::Person(name = $src.name)) "
              + "  } "
              + ")";
        assertConverges(legacy, clean, "my::M", "my::M$class$model::Person");
    }

    @Test
    @DisplayName("Relational column: ~mainTable + column  ==  tableReference(...) -> map(^Class(...)) — byte-for-byte")
    void relationalColumnConverges() {
        // Verified empirically: the relational desugarer's lifted body byte-
        // matches the clean-sheet `#>{db.TABLE}#` sugar form — both spell the
        // db as a PackageableElementPtr, and both spell the [1]-property
        // column bind with an explicit ->toOne() (real pure's NewValidator
        // subsumption: hand-written clean-sheet MUST write the coercion, and
        // the desugarer emits the same).
        String shared =
                "Class model::Person { name: String[1]; } "
              + "Database db::DB ( Table T_PERSON (ID INTEGER, NAME VARCHAR(50)) ) ";
        String legacy = shared
              + "Mapping my::M ( "
              + "  *model::Person: Relational { ~mainTable [db::DB] T_PERSON name: T_PERSON.NAME } "
              + ")";
        String clean = shared
              + "Mapping my::M ( "
              + "  *model::Person: Relational { "
              + "    #>{db::DB.T_PERSON}# -> map(row | ^model::Person(name = $row.NAME->toOne())) "
              + "  } "
              + ")";
        assertConverges(legacy, clean, "my::M", "my::M$class$model::Person");
    }

    @Test
    @DisplayName("Association: binding tables converge (bodies diverge — legacy uses the legacyAssocPredicate bridge)")
    void associationBindingTableConverges() {
        // The documented exception (doc §2): the legacy AssociationMapping
        // desugars its physical-column predicate through the `legacyAssocPredicate`
        // bridge helper, whereas the clean-sheet author writes a plain object-level
        // lambda. Both are valid and both lift to the same <mapping>$assoc$<fqn>
        // FQN, so the BINDING TABLES converge — but the predicate BODIES do not,
        // and that is correct, not a defect.
        String model =
                "Class model::Firm   { id: Integer[1]; } "
              + "Class model::Person { firmId: Integer[1]; } "
              + "Association model::Person_Firm { firm: model::Firm[1]; person: model::Person[1]; } "
              + "Database db::DB ( "
              + "  Table T_FIRM   (ID INTEGER) "
              + "  Table T_PERSON (FIRM_ID INTEGER) "
              + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
              + ") ";
        // A mapping is all-legacy or all-clean-sheet (the mix rule), so both
        // class mappings AND the association are written in one surface each.
        String legacy = model
              + "Mapping my::M ( "
              + "  *model::Firm:   Relational { ~mainTable [db::DB] T_FIRM   id: T_FIRM.ID } "
              + "  *model::Person: Relational { ~mainTable [db::DB] T_PERSON firmId: T_PERSON.FIRM_ID } "
              + "  model::Person_Firm: Relational { AssociationMapping ( firm: [db::DB] @Person_Firm ) } "
              + ")";
        String clean = model
              + "Mapping my::M ( "
              + "  *model::Firm:   Relational { #>{db::DB.T_FIRM}# -> map(r | ^model::Firm(id = $r.ID->toOne())) } "
              + "  *model::Person: Relational { #>{db::DB.T_PERSON}# -> map(r | ^model::Person(firmId = $r.FIRM_ID->toOne())) } "
              + "  model::Person_Firm: AssociationMapping { {a, b | $a.id == $b.firmId} } "
              + ")";
        assertBindingTablesConverge(legacy, clean, "my::M");
    }
}
