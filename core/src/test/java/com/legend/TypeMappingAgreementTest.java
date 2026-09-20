package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.Type;
import com.legend.model.RelationalDataType;
import com.legend.plan.PreciseTypes;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The SQL-type to Pure-type correspondence, asserted to be ONE answer.
 *
 * <p>There are eight places in core that know something about this
 * correspondence, in different directions and at different
 * granularities:
 *
 * <pre>
 *   DatabaseProtocolParser   grammar token  -> protocol name
 *   RelationalDataType       string         -> typed Varchar(n)
 *   StoreCompiler.columnType RelationalDataType -> Pure primitive
 *   PreciseTypes.pureType    RelationalDataType -> precise Pure FQN
 *   PreciseTypes.defaultSpelling  Pure      -> SQL DDL spelling
 *   SqlTypeCensus            string         -> SqlType.Scalar
 *   RelOpTranslator          SQL name       -> Pure name
 *   Executor                 JDBC name      -> runtime value
 * </pre>
 *
 * <p>They are not copies -- which is exactly why they drift instead of
 * being deduplicated, and why nobody notices. It has already cost a
 * real bug: {@code RelOpTranslator} carries the note that DECIMAL once
 * "answered 'Float', contradicting our own column-kind table, which
 * spells Decimal/Numeric as Decimal (audit 2026-09-15 P3-5)" -- a
 * financial product disagreeing with itself about DECIMAL.
 *
 * <p>This pins the two paths a column actually travels: the STORE path
 * a query types through, and the PRECISE path the plan protocol emits.
 * Agreement is asserted by FAMILY rather than by exact name, because
 * the two are deliberately different granularities -- Integer against
 * Int/BigInt/SmallInt. A family disagreement is the bug; a granularity
 * difference is the design.
 */
class TypeMappingAgreementTest {

    /** The coarse family both paths must agree on. */
    private enum Family {
        STRING, INTEGER, FLOAT, DECIMAL, BOOLEAN, DATE, DATETIME, VARIANT,
        BYTE, UNMAPPED,
    }

    /** SQL spelling -> the RelationalDataType the model layer builds. */
    private static Map<String, RelationalDataType> cases() {
        Map<String, RelationalDataType> out = new LinkedHashMap<>();
        out.put("VARCHAR(64)", new RelationalDataType.Varchar(64));
        out.put("CHAR(4)", new RelationalDataType.Char_(4));
        out.put("INTEGER", new RelationalDataType.Integer_());
        out.put("BIGINT", new RelationalDataType.BigInt());
        out.put("SMALLINT", new RelationalDataType.SmallInt());
        out.put("TINYINT", new RelationalDataType.TinyInt());
        out.put("FLOAT", new RelationalDataType.Float_());
        out.put("DOUBLE", new RelationalDataType.Double_());
        out.put("REAL", new RelationalDataType.Real());
        out.put("DECIMAL(10,2)", new RelationalDataType.Decimal(10, 2));
        out.put("NUMERIC(10,2)", new RelationalDataType.Numeric(10, 2));
        out.put("DATE", new RelationalDataType.Date_());
        out.put("TIMESTAMP", new RelationalDataType.Timestamp());
        out.put("BIT", new RelationalDataType.Bit());
        return out;
    }

    private static Family storeFamily(Type t) {
        if (t instanceof Type.PrecisionDecimal) return Family.DECIMAL;
        if (t instanceof Type.ClassType c
                && c.fqn().contains("variant")) return Family.VARIANT;
        if (t == Type.Primitive.STRING) return Family.STRING;
        if (t == Type.Primitive.INTEGER) return Family.INTEGER;
        if (t == Type.Primitive.FLOAT) return Family.FLOAT;
        if (t == Type.Primitive.BOOLEAN) return Family.BOOLEAN;
        if (t == Type.Primitive.STRICT_DATE) return Family.DATE;
        if (t == Type.Primitive.DATE_TIME) return Family.DATETIME;
        if (t == Type.Primitive.BYTE) return Family.BYTE;
        return Family.UNMAPPED;
    }

    private static Family preciseFamily(String pureFqn) {
        String n = pureFqn.substring(pureFqn.lastIndexOf(':') + 1);
        return switch (n) {
            case "Varchar", "String" -> Family.STRING;
            case "Int", "BigInt", "SmallInt", "TinyInt", "UInt", "UBigInt",
                 "USmallInt", "UTinyInt", "Integer" -> Family.INTEGER;
            case "Float4", "Double", "Float" -> Family.FLOAT;
            case "Numeric", "Decimal" -> Family.DECIMAL;
            case "StrictDate", "Date" -> Family.DATE;
            case "Timestamp", "DateTime" -> Family.DATETIME;
            case "Boolean" -> Family.BOOLEAN;
            case "Variant" -> Family.VARIANT;
            default -> Family.UNMAPPED;
        };
    }

    /** The Pure type the STORE path gives a column, via a real compile. */
    private static Map<String, Type> storeTypes() {
        StringBuilder db = new StringBuilder("###Relational\nDatabase t::DB\n(\n  Table T\n  (\n");
        List<String> spellings = List.copyOf(cases().keySet());
        for (int i = 0; i < spellings.size(); i++) {
            db.append("    c").append(i).append(' ').append(spellings.get(i));
            if (i < spellings.size() - 1) db.append(',');
            db.append('\n');
        }
        db.append("  )\n)\n");

        ModelContext ctx = Compiler.compileModel(db.toString());
        Type.RelationType rt = ctx.findTable("t::DB", "T").orElseThrow();
        Map<String, Type> out = new LinkedHashMap<>();
        for (int i = 0; i < spellings.size(); i++) {
            final String col = "c" + i;
            out.put(spellings.get(i), rt.columns().stream()
                    .filter(c -> c.name().equals(col))
                    .findFirst().orElseThrow().type());
        }
        return out;
    }

    /**
     * Disagreements that exist today, each with the side that is right.
     *
     * Listed rather than silently tolerated, and a listed one that
     * starts AGREEING fails this test -- so a note cannot outlive the
     * bug it describes.
     */
    private static final Map<String, String> KNOWN_DISAGREEMENTS = Map.of(
            // StoreCompiler says BOOLEAN, PreciseTypes says TinyInt.
            // UPSTREAM SAYS BOOLEAN, in four places:
            //   pureToRelational.pure:56          pair(Boolean, ^Bit())
            //   relationalExtension.pure:146      Boolean[1] | ^Bit()
            //   databaseHelperFunctions.pure:202  'BOOLEAN' -> ^Bit()
            //   relationalToPure.pure:214         Bit columns <-> Boolean
            // and the only Bit->TINYINT mapping upstream is COMMENTED
            // OUT in the H2 extension, annotated as a workaround for
            // comparisons under a newer H2.
            //
            // So PreciseTypes is the wrong one. NOT fixed here because
            // pureType feeds the PLAN PROTOCOL, and the corpus gates
            // that would catch a regression in emitted plan text could
            // not be run when this was found. The fix also needs a
            // Boolean arm in defaultSpelling (-> "BIT"), which today
            // throws NotImplementedException.
            "BIT", "store=BOOLEAN precise=INTEGER");

    @Test
    void theStoreAndPrecisePathsAgreeOnEveryType() {
        Map<String, Type> store = storeTypes();
        List<String> unexpected = new java.util.ArrayList<>();
        List<String> stale = new java.util.ArrayList<>();

        cases().forEach((spelling, dt) -> {
            Family fromStore = storeFamily(store.get(spelling));
            Family fromPrecise;
            try {
                fromPrecise = preciseFamily(PreciseTypes.pureType(dt));
            } catch (RuntimeException e) {
                fromPrecise = Family.UNMAPPED;
            }
            String observed = "store=" + fromStore + " precise=" + fromPrecise;
            String known = KNOWN_DISAGREEMENTS.get(spelling);
            if (fromStore != fromPrecise) {
                if (!observed.equals(known)) {
                    unexpected.add(spelling + ": " + observed
                            + (known == null ? "" : " (known was: " + known + ")"));
                }
            } else if (known != null) {
                stale.add(spelling + " now AGREES -- remove it from"
                        + " KNOWN_DISAGREEMENTS");
            }
        });

        if (!unexpected.isEmpty() || !stale.isEmpty()) {
            fail("type paths drifted:\n  "
                    + String.join("\n  ", unexpected)
                    + (stale.isEmpty() ? "" : "\n  " + String.join("\n  ", stale)));
        }
    }

    @Test
    void decimalIsDecimalEverywhere() {
        // The bug that actually happened: DECIMAL answering Float in one
        // path while the column-kind table said Decimal. In a financial
        // product, silently.
        Map<String, Type> store = storeTypes();
        for (String spelling : List.of("DECIMAL(10,2)", "NUMERIC(10,2)")) {
            assertEquals(Family.DECIMAL, storeFamily(store.get(spelling)),
                    spelling + " must stay Decimal in the store path");
        }
        assertEquals(Family.DECIMAL,
                preciseFamily(PreciseTypes.pureType(
                        new RelationalDataType.Decimal(10, 2))),
                "DECIMAL must stay Decimal in the precise path");
    }

    @Test
    void everyDeclaredTypeSurvivesTheRoundTripToASqlSpelling() {
        // PreciseTypes.defaultSpelling is the reverse direction, and a
        // type that cannot be spelled back is one the plan protocol
        // cannot emit.
        cases().forEach((spelling, dt) -> {
            String pure;
            try {
                pure = PreciseTypes.pureType(dt);
            } catch (RuntimeException e) {
                return; // covered by the agreement test
            }
            String back = PreciseTypes.defaultSpelling(pure);
            assertTrue(back != null && !back.isBlank(),
                    spelling + " has no SQL spelling back from " + pure);
        });
    }
}
