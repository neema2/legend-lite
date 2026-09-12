// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.builtin;

import com.legend.model.NativeFunctionDefinition;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * THE NATIVE FUNCTION FAMILIES — the catalog natives ({@link Pure}) grouped
 * by the code that IMPLEMENTS them, each family a CLOSED TYPE: an enum whose
 * constants carry their catalog overloads and whose owning switch is a
 * switch EXPRESSION with no default, so a new member does not COMPILE until
 * it is handled. The sibling of {@link com.legend.compiler.spec.CoreFn}
 * (language forms with their own checker): {@code CoreFn} = a form, {@code
 * NativeFn} = a native with an implementer. The registry
 * ({@code com.legend.claims.Claims}) reads {@link #families()}: the enum is
 * the set and the dispatch key, so nothing beside the code can drift, and no
 * compiler code dispatches on a function-name STRING (USER 2026-09-10:
 * "we should not dispatch on strings and only on typed things that are
 * registered"). One file, next to the catalog it groups.
 *
 * <p>Batch 4b of the upstream boundary program folds the batch-3 enums
 * ({@code CalendarFn}, {@code AssertFn}, {@code RowGetter}) in here and adds
 * a family per remaining implementer until the ledger's UNCLAIMED count is 0.
 */
public final class NativeFn {

    private NativeFn() {
    }

    /** One member of a family: the FQN it implements and every catalog overload of it. */
    public interface Member {
        String fqn();

        List<NativeFunctionDefinition> overloads();

        /** The bare function name ({@code assertEquals}) — for messages. */
        default String bareName() {
            String f = fqn();
            return f.substring(f.lastIndexOf(':') + 1);
        }

        /** Whether an APPLIED name (pre-resolution: bare, or qualified) spells
         *  this member — the CoreFn parse-name rule, for the front-end forms
         *  the Typer reads before names resolve. */
        default boolean matches(String appliedName) {
            return appliedName.equals(fqn()) || appliedName.equals(bareName())
                    || appliedName.endsWith("::" + bareName());
        }
    }

    /** FQN -> member; immutable (ArchitectureTest invariant 3). */
    static <E extends Enum<E> & Member> Map<String, E> index(E[] values) {
        Map<String, E> m = new HashMap<>();
        for (E e : values) {
            m.put(e.fqn(), e);
        }
        return Map.copyOf(m);
    }

    /** Every family, by name — THE registration the claim registry reads. A
     *  new enum in this file that is not listed here is unclaimed, and the
     *  ledger says so. */
    public static Map<String, List<? extends Member>> families() {
        Map<String, List<? extends Member>> out = new LinkedHashMap<>();
        out.put("Calendar", List.of(Calendar.values()));
        out.put("Verdict", List.of(Verdict.values()));
        out.put("RowGetter", List.of(RowGetter.values()));
        out.put("Frame", List.of(Frame.values()));
        out.put("RowGetter", List.of(RowGetter.values()));
        out.put("LowererForm", List.of(LowererForm.values()));
        out.put("LiteralForm", List.of(LiteralForm.values()));
        out.put("ContextOption", List.of(ContextOption.values()));
        out.put("PlanWrapper", List.of(PlanWrapper.values()));
        out.put("RelationQuantifier", List.of(RelationQuantifier.values()));
        out.put("ObjectReference", List.of(ObjectReference.values()));
        out.put("SubtypeForm", List.of(SubtypeForm.values()));
        out.put("ResolverForm", List.of(ResolverForm.values()));
        out.put("LiteDesugar", List.of(LiteDesugar.values()));
        out.put("TyperForm", List.of(TyperForm.values()));
        out.put("JavaRoutine", List.of(JavaRoutine.values()));
        out.put("Handle", List.of(Handle.values()));
        out.put("Effect", List.of(Effect.values()));
        out.put("Carrier", List.of(Carrier.values()));
        out.put("ContextOwner", List.of(ContextOwner.values()));
        out.put("DdlStatement", List.of(DdlStatement.values()));
        return out;
    }

    /** the 32 calendar natives CalendarAgg lowers to SQL calendar aggregation. */
    public enum Calendar implements Member {
        CY_MINUS2("meta::pure::functions::date::calendar::CYMinus2",
                Pure.CAL_C_Y_MINUS2),
        CY_MINUS3("meta::pure::functions::date::calendar::CYMinus3",
                Pure.CAL_C_Y_MINUS3),
        ANNUALIZED("meta::pure::functions::date::calendar::annualized",
                Pure.CAL_ANNUALIZED),
        CME("meta::pure::functions::date::calendar::cme",
                Pure.CAL_CME),
        CW("meta::pure::functions::date::calendar::cw",
                Pure.CAL_CW),
        CW_FM("meta::pure::functions::date::calendar::cw_fm",
                Pure.CAL_CW_FM),
        MTD("meta::pure::functions::date::calendar::mtd",
                Pure.CAL_MTD),
        P12MTD("meta::pure::functions::date::calendar::p12mtd",
                Pure.CAL_P12MTD),
        P12WA("meta::pure::functions::date::calendar::p12wa",
                Pure.CAL_P12WA),
        P12WTD("meta::pure::functions::date::calendar::p12wtd",
                Pure.CAL_P12WTD),
        P4WA("meta::pure::functions::date::calendar::p4wa",
                Pure.CAL_P4WA),
        P4WTD("meta::pure::functions::date::calendar::p4wtd",
                Pure.CAL_P4WTD),
        P52WA("meta::pure::functions::date::calendar::p52wa",
                Pure.CAL_P52WA),
        P52WTD("meta::pure::functions::date::calendar::p52wtd",
                Pure.CAL_P52WTD),
        PMA("meta::pure::functions::date::calendar::pma",
                Pure.CAL_PMA),
        PMTD("meta::pure::functions::date::calendar::pmtd",
                Pure.CAL_PMTD),
        PQTD("meta::pure::functions::date::calendar::pqtd",
                Pure.CAL_PQTD),
        PRIOR_DAY("meta::pure::functions::date::calendar::priorDay",
                Pure.CAL_PRIOR_DAY),
        PRIOR_YEAR("meta::pure::functions::date::calendar::priorYear",
                Pure.CAL_PRIOR_YEAR),
        PW("meta::pure::functions::date::calendar::pw",
                Pure.CAL_PW),
        PW_FM("meta::pure::functions::date::calendar::pw_fm",
                Pure.CAL_PW_FM),
        PWA("meta::pure::functions::date::calendar::pwa",
                Pure.CAL_PWA),
        PWTD("meta::pure::functions::date::calendar::pwtd",
                Pure.CAL_PWTD),
        PYMTD("meta::pure::functions::date::calendar::pymtd",
                Pure.CAL_PYMTD),
        PYQTD("meta::pure::functions::date::calendar::pyqtd",
                Pure.CAL_PYQTD),
        PYTD("meta::pure::functions::date::calendar::pytd",
                Pure.CAL_PYTD),
        PYWA("meta::pure::functions::date::calendar::pywa",
                Pure.CAL_PYWA),
        PYWTD("meta::pure::functions::date::calendar::pywtd",
                Pure.CAL_PYWTD),
        QTD("meta::pure::functions::date::calendar::qtd",
                Pure.CAL_QTD),
        REPORT_END_DAY("meta::pure::functions::date::calendar::reportEndDay",
                Pure.CAL_REPORT_END_DAY),
        WTD("meta::pure::functions::date::calendar::wtd",
                Pure.CAL_WTD),
        YTD("meta::pure::functions::date::calendar::ytd",
                Pure.CAL_YTD);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        Calendar(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, Calendar> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<Calendar> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** the asserts AssertVerdicts adjudicates as row verdicts, plus the two relation verdict forms it owns. */
    public enum Verdict implements Member {
        ASSERT_EQUALS("meta::pure::functions::asserts::assertEquals",
                Pure.ASSERT_EQUALS__ANY_MANY__ANY_MANY, Pure.ASSERT_EQUALS__ANY_MANY__ANY_MANY__STRING_1, Pure.ASSERT_EQUALS__ANY_MANY__ANY_MANY__STRING_1__ANY_MANY, Pure.ASSERT_EQUALS__ANY_MANY__ANY_MANY__FN_1),
        ASSERT_NOT_EQUALS("meta::pure::functions::asserts::assertNotEquals",
                Pure.ASSERT_NOT_EQUALS__ANY_MANY__ANY_MANY, Pure.ASSERT_NOT_EQUALS__ANY_MANY__ANY_MANY__STRING_1, Pure.ASSERT_NOT_EQUALS__ANY_MANY__ANY_MANY__STRING_1__ANY_MANY, Pure.ASSERT_NOT_EQUALS__ANY_MANY__ANY_MANY__FN_1),
        ASSERT_SAME_ELEMENTS("meta::pure::functions::asserts::assertSameElements",
                Pure.ASSERT_SAME_ELEMENTS__ANY_MANY__ANY_MANY, Pure.ASSERT_SAME_ELEMENTS__ANY_MANY__ANY_MANY__STRING_1, Pure.ASSERT_SAME_ELEMENTS__ANY_MANY__ANY_MANY__STRING_1__ANY_MANY, Pure.ASSERT_SAME_ELEMENTS__ANY_MANY__ANY_MANY__FN_1),
        ASSERT_SIZE("meta::pure::functions::asserts::assertSize",
                Pure.ASSERT_SIZE__ANY_MANY__INTEGER_1, Pure.ASSERT_SIZE__ANY_MANY__INTEGER_1__STRING_1, Pure.ASSERT_SIZE__ANY_MANY__INTEGER_1__STRING_1__ANY_MANY, Pure.ASSERT_SIZE__ANY_MANY__INTEGER_1__FN_1),
        ASSERT_JSON_STRINGS_EQUAL("meta::pure::functions::asserts::assertJsonStringsEqual",
                Pure.ASSERT_JSON_STRINGS_EQUAL__STRING_1__STRING_1),
        ASSERT_CONTAINS("meta::pure::functions::asserts::assertContains",
                Pure.ASSERT_CONTAINS__ANY_MANY__ANY_1, Pure.ASSERT_CONTAINS__ANY_MANY__ANY_1__STRING_1, Pure.ASSERT_CONTAINS__ANY_MANY__ANY_1__STRING_1__ANY_MANY, Pure.ASSERT_CONTAINS__ANY_MANY__ANY_1__FN_1),
        ASSERT_EQ("meta::pure::functions::asserts::assertEq",
                Pure.ASSERT_EQ__ANY_1__ANY_1, Pure.ASSERT_EQ__ANY_1__ANY_1__STRING_1, Pure.ASSERT_EQ__ANY_1__ANY_1__STRING_1__ANY_MANY, Pure.ASSERT_EQ__ANY_1__ANY_1__FN_1),
        ASSERT_EQ_WITHIN_TOLERANCE("meta::pure::functions::asserts::assertEqWithinTolerance",
                Pure.ASSERT_EQ_WITHIN_TOLERANCE__NUMBER_1__NUMBER_1__NUMBER_1, Pure.ASSERT_EQ_WITHIN_TOLERANCE__N_1__N_1__N_1__STRING_1, Pure.ASSERT_EQ_WITHIN_TOLERANCE__N_1__N_1__N_1__STRING_1__ANY_MANY, Pure.ASSERT_EQ_WITHIN_TOLERANCE__N_1__N_1__N_1__FN_1),
        ASSERT("meta::pure::functions::asserts::assert",
                Pure.ASSERT__BOOLEAN_1, Pure.ASSERT__BOOLEAN_1__STRING_1, Pure.ASSERT__BOOLEAN_1__FN_1, Pure.ASSERT__BOOLEAN_1__STRING_1__ANY_MANY),
        ASSERT_FALSE("meta::pure::functions::asserts::assertFalse",
                Pure.ASSERT_FALSE__BOOLEAN_1, Pure.ASSERT_FALSE__BOOLEAN_1__STRING_1, Pure.ASSERT_FALSE__BOOLEAN_1__STRING_1__ANY_MANY, Pure.ASSERT_FALSE__BOOLEAN_1__FN_1),
        ASSERT_INSTANCE_OF("meta::pure::functions::asserts::assertInstanceOf",
                Pure.ASSERT_INSTANCE_OF__ANY_1__TYPE_1, Pure.ASSERT_INSTANCE_OF__ANY_1__TYPE_1__STRING_1, Pure.ASSERT_INSTANCE_OF__ANY_1__TYPE_1__STRING_1__ANY_MANY, Pure.ASSERT_INSTANCE_OF__ANY_1__TYPE_1__FN_1),
        ASSERT_IS("meta::pure::functions::asserts::assertIs",
                Pure.ASSERT_IS__ANY_1__ANY_1, Pure.ASSERT_IS__ANY_1__ANY_1__STRING_1, Pure.ASSERT_IS__ANY_1__ANY_1__STRING_1__ANY_MANY, Pure.ASSERT_IS__ANY_1__ANY_1__FN_1),
        ASSERT_EMPTY("meta::pure::functions::asserts::assertEmpty",
                Pure.ASSERT_EMPTY__ANY_MANY, Pure.ASSERT_EMPTY__ANY_MANY__STRING_1, Pure.ASSERT_EMPTY__ANY_MANY__STRING_1__ANY_MANY, Pure.ASSERT_EMPTY__ANY_MANY__FN_1),
        ASSERT_NOT_EMPTY("meta::pure::functions::asserts::assertNotEmpty",
                Pure.ASSERT_NOT_EMPTY__ANY_MANY, Pure.ASSERT_NOT_EMPTY__ANY_MANY__STRING_1, Pure.ASSERT_NOT_EMPTY__ANY_MANY__STRING_1__ANY_MANY, Pure.ASSERT_NOT_EMPTY__ANY_MANY__FN_1),
        ASSERT_TDS_EQUIVALENT("meta::pure::functions::relation::assertTdsEquivalent",
                Pure.ASSERT_TDS_EQUIVALENT__REL_1__REL_1__NUMBER_1, Pure.ASSERT_TDS_EQUIVALENT__REL_1__REL_1__NUMBER_1__NUMBER_1),
        TO_CSV("meta::relational::tests::csv::toCSV",
                Pure.TO_CSV__TDS, Pure.TO_CSV__TDS_BOOL, Pure.TO_CSV__TDS_FMT);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        Verdict(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, Verdict> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<Verdict> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }


    /** the window-frame keywords Frames classifies and the over() checker consumes by type. */
    public enum Frame implements Member {
        ROWS("meta::pure::functions::relation::rows",
                Pure.ROWS__INTEGER_1__INTEGER_1, Pure.ROWS__UNBOUNDED_1__UNBOUNDED_1, Pure.ROWS__UNBOUNDED_1__INTEGER_1, Pure.ROWS__INTEGER_1__UNBOUNDED_1),
        RANGE("meta::pure::functions::relation::_range",
                Pure._RANGE__NUMBER_1__NUMBER_1, Pure._RANGE__UNBOUNDED_1__NUMBER_1, Pure._RANGE__NUMBER_1__UNBOUNDED_1, Pure._RANGE__INT_1__DU_1__INT_1__DU_1, Pure._RANGE__UNBOUNDED_1__INT_1__DU_1, Pure._RANGE__INT_1__DU_1__UNBOUNDED_1, Pure._RANGE__UNBOUNDED_1__UNBOUNDED_1),
        UNBOUNDED("meta::pure::functions::relation::unbounded",
                Pure.UNBOUNDED);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        Frame(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, Frame> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<Frame> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** relation forms the Lowerer lowers structurally (lateral joins, windowed map+reduce, composed window expressions, bi-variate aggregate maps). */
    public enum LowererForm implements Member {
        LATERAL("meta::pure::functions::relation::lateral",
                Pure.LATERAL__RELATION_1__FUNCTION_1),
        REDUCE("meta::pure::functions::relation::reduce",
                Pure.REDUCE__RELATION_1__WINDOW_1__T_1__FUNCTION_1__FUNCTION_1),
        Z_SCORE("meta::pure::functions::math::zScore",
                Pure.Z_SCORE__WINDOW),
        ROW_MAPPER("meta::pure::functions::math::mathUtility::rowMapper",
                Pure.ROW_MAPPER__T_0_1__U_0_1),
        WAVG_ROW_MAPPER("meta::pure::functions::math::wavgUtility::wavgRowMapper",
                Pure.WAVG_ROW_MAPPER__NUMBER_0_1__NUMBER_0_1);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        LowererForm(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, LowererForm> BY_FQN = index(values());

        /** rowMapper / wavgRowMapper — the bi-variate aggregate map bodies. */
        public static boolean isBivariateMap(String calleeFqn) {
            LowererForm f = BY_FQN.get(calleeFqn);
            return f == ROW_MAPPER || f == WAVG_ROW_MAPPER;
        }

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<LowererForm> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** calls LiteralUnroll rewrites at type-check over spelled operands. */
    public enum LiteralForm implements Member {
        DYNAMIC_NEW("meta::pure::functions::lang::dynamicNew",
                Pure.DYNAMIC_NEW__CLASS_5, Pure.DYNAMIC_NEW__CLASS_6, Pure.DYNAMIC_NEW__GENERIC_5, Pure.DYNAMIC_NEW__GENERIC_6, Pure.DYNAMIC_NEW__CLASS_1__KEYVALUE_MANY, Pure.DYNAMIC_NEW__GENERICTYPE_1__KEYVALUE_MANY),
        ENUM_VALUES("meta::pure::functions::meta::enumValues",
                Pure.ENUM_VALUES),
        KEY_VALUES("meta::pure::functions::collection::keyValues",
                Pure.KEY_VALUES__MAP_1);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        LiteralForm(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, LiteralForm> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<LiteralForm> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** the execute-context post-processors ContextReading reads as options of the plan. */
    public enum ContextOption implements Member {
        EXTRACT_SUBQUERIES_AS_CTES("meta::relational::postProcessor::cteExtraction::extractSubqueriesAsCTEs",
                Pure.EXTRACT_CTES),
        NON_EXECUTABLE("meta::relational::postProcessor::nonExecutable",
                Pure.NON_EXECUTABLE_PP),
        REPLACE_TABLES("meta::relational::postProcessor::replaceTables",
                Pure.REPLACE_TABLES);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        ContextOption(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, ContextOption> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<ContextOption> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** plan-time wrappers ExecuteChainAssembly / StoreResolver read through (identity for row semantics) or fold; relationalExtensions is an ignored test-data-generation argument. */
    /** THE QUANTIFICATION FAMILY (engine core_functions_relation/quantification,
     *  4.145.0): a value tested against a SINGLE-COLUMN relation —
     *  {@code relation::in}, the ten quantified comparisons
     *  ({@code equal/greaterThan/greaterThanEqual/lessThan/lessThanEqual} ×
     *  {@code Any/All}) and {@code relation::exists(rel, predicate)} — lowered by
     *  RelationPredicates as SQL subquery predicates (IN / op ANY|ALL / EXISTS),
     *  two-valued the way the Pure bodies answer (engine
     *  processRelationQuantifiedComparison, processRelationIn, processRelationExists). */
    public enum RelationQuantifier implements Member {
        EXISTS("meta::pure::functions::relation::exists", Pure.EXISTS__RELATION_1__FUNCTION_1),
        IN("meta::pure::functions::relation::in", Pure.IN__U_0_1__RELATION_1),
        EQUAL_ANY("meta::pure::functions::relation::equalAny", Pure.EQUAL_ANY__U_0_1__RELATION_1),
        EQUAL_ALL("meta::pure::functions::relation::equalAll", Pure.EQUAL_ALL__U_0_1__RELATION_1),
        GREATER_THAN_ANY("meta::pure::functions::relation::greaterThanAny",
                Pure.GREATER_THAN_ANY__U_0_1__RELATION_1),
        GREATER_THAN_ALL("meta::pure::functions::relation::greaterThanAll",
                Pure.GREATER_THAN_ALL__U_0_1__RELATION_1),
        GREATER_THAN_EQUAL_ANY("meta::pure::functions::relation::greaterThanEqualAny",
                Pure.GREATER_THAN_EQUAL_ANY__U_0_1__RELATION_1),
        GREATER_THAN_EQUAL_ALL("meta::pure::functions::relation::greaterThanEqualAll",
                Pure.GREATER_THAN_EQUAL_ALL__U_0_1__RELATION_1),
        LESS_THAN_ANY("meta::pure::functions::relation::lessThanAny",
                Pure.LESS_THAN_ANY__U_0_1__RELATION_1),
        LESS_THAN_ALL("meta::pure::functions::relation::lessThanAll",
                Pure.LESS_THAN_ALL__U_0_1__RELATION_1),
        LESS_THAN_EQUAL_ANY("meta::pure::functions::relation::lessThanEqualAny",
                Pure.LESS_THAN_EQUAL_ANY__U_0_1__RELATION_1),
        LESS_THAN_EQUAL_ALL("meta::pure::functions::relation::lessThanEqualAll",
                Pure.LESS_THAN_EQUAL_ALL__U_0_1__RELATION_1);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        RelationQuantifier(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, RelationQuantifier> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  of this family (exact FQN, never a bare name). */
        public static Optional<RelationQuantifier> of(String calleeFqn) {
            return Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    public enum PlanWrapper implements Member {
        WITH_FEATURE_FLAGS("meta::pure::executionPlan::featureFlag::withFeatureFlags",
                Pure.WITH_FEATURE_FLAGS__T_MANY__ENUM_MANY),
        CONCATENATE_TEMPORAL_TDS_QUERIES("meta::relational::milestoning::concatenateTemporalTdsQueries",
                Pure.CONCATENATE_TEMPORAL_TDS_QUERIES);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        PlanWrapper(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, PlanWrapper> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<PlanWrapper> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** the object-reference encode / decode / membership arms of the resolver. */
    public enum ObjectReference implements Member {
        GENERATE("meta::alloy::objectReference::generateObjectReferences",
                Pure.GENERATE_OBJECT_REFERENCES__6),
        GENERATE_FOR_SET("meta::alloy::objectReference::generateObjectReferencesForGivenSetId",
                Pure.GENERATE_OBJECT_REFERENCES_FOR_GIVEN_SET_ID__7),
        DECODE("meta::alloy::objectReference::decodeObjectReferencesAndGetPkMap",
                Pure.DECODE_OBJECT_REFERENCES__3),
        OBJECT_REFERENCE_IN("meta::pure::functions::collection::objectReferenceIn",
                Pure.OBJECT_REFERENCE_IN__ANY_1__STRING_MANY);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        ObjectReference(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, ObjectReference> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<ObjectReference> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** type-narrowing forms the resolver and the verdicts read structurally (subType, whenSubType, instanceOf, genericType). */
    public enum SubtypeForm implements Member {
        SUB_TYPE("meta::pure::functions::lang::subType",
                Pure.SUB_TYPE__ANY_m__T_1),
        WHEN_SUB_TYPE("meta::pure::functions::lang::whenSubType",
                Pure.WHEN_SUB_TYPE__ANY_1__T_1, Pure.WHEN_SUB_TYPE__ANY_01__T_1, Pure.WHEN_SUB_TYPE__ANY_MANY__T_1),
        INSTANCE_OF("meta::pure::functions::meta::instanceOf",
                Pure.INSTANCE_OF__ANY_1__TYPE_1),
        GENERIC_TYPE("meta::pure::functions::meta::genericType",
                Pure.GENERIC_TYPE__ANY_MANY);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        SubtypeForm(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, SubtypeForm> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<SubtypeForm> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** forms single resolver / checker sites rewrite: tdsContains (EXISTS), variant get, alloyConfig, the from() mapping markers, the relational-mapper post-processor. */
    public enum ResolverForm implements Member {
        TDS_CONTAINS("meta::pure::tds::tdsContains",
                Pure.TDS_CONTAINS__T_1__FUNCTION_MANY__TDS_1, Pure.TDS_CONTAINS__T_1__FUNCTION_MANY__STRING_MANY__TDS_1__FUNCTION_1),
        VARIANT_GET("meta::pure::functions::variant::navigation::get",
                Pure.GET__VARIANT_0_1__STRING_1, Pure.GET__VARIANT_0_1__INTEGER_1),
        ALLOY_CONFIG("meta::pure::graphFetch::execution::alloyConfig",
                Pure.ALLOY_CONFIG__4, Pure.ALLOY_CONFIG__5, Pure.ALLOY_CONFIG__6, Pure.ALLOY_CONFIG__7, Pure.ALLOY_CONFIG__8),
        WITH_MAPPING("meta::pure::mapping::withMapping",
                Pure.WITH_MAPPING),
        WITH_CHAINED_MAPPINGS("meta::pure::mapping::withChainedMappings",
                Pure.WITH_CHAINED_MAPPINGS),
        RELATIONAL_MAPPER_POST_PROCESSOR("meta::pure::alloy::connections::relationalMapperPostProcessor",
                Pure.RELATIONAL_MAPPER_PP);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        ResolverForm(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, ResolverForm> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<ResolverForm> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** the platform's own desugar IR (meta::legend::lite) the mapping normalizer emits and the lowering consumes. */
    public enum LiteDesugar implements Member {
        OTHERWISE("meta::legend::lite::otherwise",
                Pure.OTHERWISE__T_1__T_0_1),
        UNION_SCAN("meta::legend::lite::unionScan",
                Pure.UNION_SCAN__RELATION_1),
        LEGACY_ASSOC_PREDICATE("meta::legend::lite::legacyAssocPredicate",
                Pure.LEGACY_ASSOC_PREDICATE__A_1__B_1__RELATION_1__RELATION_1__FUNCTION_1, Pure.LEGACY_ASSOC_PREDICATE__A_1__B_1__STRING_1__STRING_1__FUNCTION_1),
        LEGACY_LOCAL_PROPERTY("meta::legend::lite::legacyLocalProperty",
                Pure.LEGACY_LOCAL_PROPERTY__ANY_1__STRING_1);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        LiteDesugar(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, LiteDesugar> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<LiteDesugar> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** forms the Typer desugars against the registered signature (paginated -> slice; extractEnumValue over a literal name). */
    public enum TyperForm implements Member {
        PAGINATED("meta::pure::functions::collection::paginated",
                Pure.PAGINATED__T_MANY__INTEGER_1__INTEGER_1),
        EXTRACT_ENUM_VALUE("meta::pure::functions::lang::extractEnumValue",
                Pure.EXTRACT_ENUM_VALUE, Pure.EXTRACT_ENUM_VALUE__OPTIONAL),
        UNION("meta::pure::functions::collection::union",
                Pure.UNION__T_MANY__T_MANY);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        TyperForm(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, TyperForm> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<TyperForm> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** natives the platform computes as a VALUE in Java at orchestration time (compiler-output surfaces: plan text, SQL text); staged by NativeDispatch, the result re-enters the statement as a bound literal. */
    public enum JavaRoutine implements Member {
        PLAN_TO_STRING("meta::pure::executionPlan::toString::planToString",
                Pure.PLAN_TO_STRING__EXECUTION_PLAN_1__EXTENSION_MANY),
        PLAN_TO_STRING_WITHOUT_FORMATTING("meta::pure::executionPlan::toString::planToStringWithoutFormatting",
                Pure.PLAN_TO_STRING_WITHOUT_FORMATTING__EXECUTION_PLAN_1__EXTENSION_MANY),
        /** Also IMPLEMENTS {@code SQLResult.toSQLString(databaseType, dbTimeZone,
         *  quoteIdentifiers, format)} — upstream's QUALIFIED PROPERTY of the
         *  toSQL handle (toSQLString.pure), whose body renders engine SQLQuery
         *  objects this platform never builds: the lifted declaration types the
         *  call, the routine computes it (the leg-4 row-accessor pattern). */
        TO_SQL_STRING("meta::relational::functions::sqlstring::toSQLString",
                "meta::relational::functions::sqlstring::SQLResult", "toSQLString",
                Pure.TO_SQL_STRING__FN_1__MAPPING_1__DATABASE_TYPE_1__EXTENSION_MANY),
        TO_SQL_STRING_PRETTY("meta::relational::functions::sqlstring::toSQLStringPretty",
                Pure.TO_SQL_STRING_PRETTY__FN_1__MAPPING_1__DATABASE_TYPE_1__EXTENSION_MANY, Pure.TO_SQL_STRING_PRETTY__FN_1__MAPPING_1__RUNTIME_1__EXTENSION_MANY),
        TO_NON_EXECUTABLE_SQL_STRING("meta::relational::functions::sqlstring::toNonExecutableSQLString",
                Pure.TO_NON_EXECUTABLE_SQL_STRING__FN_1__MAPPING_1__DATABASE_TYPE_1__EXTENSION_MANY);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;
        /** The lifted qualified property this routine implements, or null. */
        private final @com.legend.Nullable String implementedDerived;

        JavaRoutine(String fqn, NativeFunctionDefinition... overloads) {
            this(fqn, null, overloads);
        }

        JavaRoutine(String fqn, @com.legend.Nullable String derivedOwner,
                NativeFunctionDefinition... overloads) {
            this(fqn, derivedOwner, null, overloads);
        }

        JavaRoutine(String fqn, @com.legend.Nullable String derivedOwner,
                @com.legend.Nullable String derivedProperty, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
            this.implementedDerived = derivedOwner == null || derivedProperty == null ? null
                    : com.legend.model.DerivedPropertyNames.lifted(derivedOwner, derivedProperty);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        /** The lifted qualified property this routine implements, if any. */
        public Optional<String> implementedDerived() {
            return Optional.ofNullable(implementedDerived);
        }

        /** The routine implementing a lifted qualified-property callee, or empty. */
        public static Optional<JavaRoutine> ofDerived(@com.legend.Nullable String liftedFqn) {
            for (JavaRoutine r : values()) {
                if (r.implementedDerived != null && r.implementedDerived.equals(liftedFqn)) {
                    return Optional.of(r);
                }
            }
            return Optional.empty();
        }

        private static final Map<String, JavaRoutine> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<JavaRoutine> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** natives producing an OPAQUE orchestration value consumers force (execute's result frame, the plan handle, the lineage scans, preval). */
    public enum Handle implements Member {
        EXECUTE("meta::pure::router::execute",
                Pure.ROUTER_EXECUTE__FN_1__MAPPING_1__RUNTIME_1__EXTENSION_MANY, Pure.ROUTER_EXECUTE__FN_1__MAPPING_1__RUNTIME_1__EXECUTION_CONTEXT_1__EXTENSION_MANY, Pure.ROUTER_EXECUTE__FN_1__MAPPING_1__RUNTIME_1__EXTENSION_MANY__DEBUG_CONTEXT_1),
        EXECUTION_PLAN_EXECUTE("meta::pure::executionPlan::execute",
                Pure.EXECUTION_PLAN_EXECUTE__EXECUTION_PLAN_1__ANY_MANY__EXTENSION_MANY),
        EXECUTE_LEGEND_QUERY("meta::legend::executeLegendQuery",
                Pure.EXECUTE_LEGEND_QUERY__FN_1__PAIR_MANY__EXTENSION_MANY, Pure.EXECUTE_LEGEND_QUERY__FN_1__PAIR_MANY__EXECUTION_CONTEXT_1__EXTENSION_MANY),
        TO_SQL("meta::relational::functions::sqlstring::toSQL",
                Pure.TO_SQL__FN_1__MAPPING_1__RUNTIME_1__EXTENSION_MANY),
        EXECUTION_PLAN("meta::pure::executionPlan::executionPlan",
                Pure.EXECUTION_PLAN__FUNCTION_DEFINITION_1__MAPPING_1__RUNTIME_1__EXTENSION_MANY, Pure.EXECUTION_PLAN__FUNCTION_DEFINITION_1__MAPPING_1__RUNTIME_1__BOOLEAN_1__EXTENSION_MANY, Pure.EXECUTION_PLAN__FUNCTION_DEFINITION_1__MAPPING_1__RUNTIME_1__EXECUTION_CONTEXT_1__EXTENSION_MANY, Pure.EXECUTION_PLAN__FUNCTION_DEFINITION_1__MAPPING_1__RUNTIME_1__EXTENSION_MANY__DEBUG_CONTEXT_1, Pure.EXECUTION_PLAN__FUNCTION_DEFINITION_1__EXTENSION_MANY, Pure.EXECUTION_PLAN__FUNCTION_DEFINITION_1__EXECUTION_CONTEXT_1__EXTENSION_MANY),
        SCAN_RELATIONS("meta::pure::lineage::scanRelations::scanRelations",
                Pure.SCAN_RELATIONS__FUNCTION_DEFINITION_1__MAPPING_1__EXTENSION_MANY, Pure.SCAN_RELATIONS__FUNCTION_DEFINITION_1__MAPPING_1__RUNTIME_1__EXTENSION_MANY),
        SCAN_PROPERTIES("meta::pure::lineage::scanProperties::scanProperties",
                Pure.SCAN_PROPERTIES__4),
        BUILD_PROPERTY_TREE("meta::pure::lineage::scanProperties::propertyTree::buildPropertyTree",
                Pure.BUILD_PROPERTY_TREE__LISTS),
        SCAN_COLUMNS("meta::pure::lineage::scanColumns::scanColumns",
                Pure.SCAN_COLUMNS__2),
        PREVAL("meta::pure::router::preeval::preval",
                Pure.PREVAL__FUNCTION_DEFINITION_1__EXTENSION_MANY, Pure.PREVAL__FUNCTION_DEFINITION_1__EXTENSION_MANY__DEBUG_CONTEXT_1);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        Handle(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, Handle> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<Handle> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }

        /** The two execute spellings (router / executionPlan): the result frame. */
        public static boolean isExecute(@com.legend.Nullable String fqn) {
            Handle h = fqn == null ? null : BY_FQN.get(fqn);
            return h == EXECUTE || h == EXECUTION_PLAN_EXECUTE;
        }

        /** Which handle forces EAGERLY when consumed at a statement's value
         *  position: execute's frame run IS the value; plan handles stay
         *  symbolic (navigated by the plan reader). */
        public static boolean forcesAtValuePosition(@com.legend.Nullable String fqn) {
            Handle h = fqn == null ? null : BY_FQN.get(fqn);
            return h == EXECUTE || h == EXECUTE_LEGEND_QUERY;
        }
    }

    /** EFFECTFUL Java routines at the execution boundary, run via their registered arm when evaluation reaches the call (never staged). */
    public enum Effect implements Member {
        EXECUTE_IN_DB("meta::relational::metamodel::execute::executeInDb",
                Pure.EXECUTE_IN_DB__STRING_1__CONN_1__INTEGER_1__INTEGER_1, Pure.EXECUTE_IN_DB__STRING_1__CONN_1),
        DROP_AND_CREATE_TABLE_IN_DB("meta::relational::functions::toDDL::dropAndCreateTableInDb",
                Pure.DROP_AND_CREATE_TABLE_IN_DB__ANY_1__STRING_1__CONN_1, Pure.DROP_AND_CREATE_TABLE_IN_DB__ANY_1__STRING_1__STRING_1__CONN_1),
        DROP_AND_CREATE_SCHEMA_IN_DB("meta::relational::functions::toDDL::dropAndCreateSchemaInDb",
                Pure.DROP_AND_CREATE_SCHEMA_IN_DB__STRING_1__CONN_1, Pure.DROP_AND_CREATE_SCHEMA_IN_DB__STRING_1__CONN_1__BOOLEAN_1),
        LOAD_CSV_TO_DB_TABLE("meta::relational::metamodel::execute::loadCsvToDbTable",
                Pure.LOAD_CSV_TO_DB_TABLE__STRING_1__TABLE_1__CONN_1),
        SET_UP_DATA_SQLS("meta::alloy::service::execution::setUpDataSQLs",
                Pure.SET_UP_DATA_SQLS__LIST_MANY__ANY_MANY__ANY_1, Pure.SET_UP_DATA_SQLS__STRING_1__DATABASE_MANY, Pure.SET_UP_DATA_SQLS__STRING_1__ANY_MANY__ANY_1),
        SET_UP_DATA_SQLS_V2("meta::alloy::service::execution::setUpDataSQLsV2",
                Pure.SET_UP_DATA_SQLS_V2__STRING_1__ANY_1__ANY_1),
        PRINT("meta::pure::functions::io::print",
                Pure.PRINT__ANY_M__INTEGER_1, Pure.PRINT__ANY_M),
        PRINTLN("meta::pure::functions::io::println",
                Pure.PRINTLN__ANY_M__INTEGER_1, Pure.PRINTLN__ANY_M),
        CONNECTION_BY_ELEMENT("meta::core::runtime::connectionByElement",
                Pure.CONNECTION_BY_ELEMENT__RUNTIME_1__STORE_1);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        Effect(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, Effect> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<Effect> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }

        /** The DATABASE effects (raw SQL, DDL, CSV load): statement-ordered, session-bound. */
        public static boolean isDbEffect(@com.legend.Nullable String fqn) {
            Effect e = fqn == null ? null : BY_FQN.get(fqn);
            return e == EXECUTE_IN_DB || e == DROP_AND_CREATE_TABLE_IN_DB
                    || e == DROP_AND_CREATE_SCHEMA_IN_DB || e == LOAD_CSV_TO_DB_TABLE;
        }

        /** The seed-SQL forms (setUpDataSQLs / V2). */
        public static boolean isSeedSqlForm(@com.legend.Nullable String fqn) {
            Effect e = fqn == null ? null : BY_FQN.get(fqn);
            return e == SET_UP_DATA_SQLS || e == SET_UP_DATA_SQLS_V2;
        }

        /** print / println — inert diagnostics (no value, no rows). */
        public static boolean isInertDiagnostic(@com.legend.Nullable String fqn) {
            Effect e = fqn == null ? null : BY_FQN.get(fqn);
            return e == PRINT || e == PRINTLN;
        }
    }

    /** natives BOUND ONCE at type-check to a carrier node that knows its implementation (the raw-grid relation, the test-data generators, the JDBC metadata grids). */
    public enum Carrier implements Member {
        EXECUTE_IN_DB_TO_TDS("meta::relational::metamodel::execute::executeInDbToTDS",
                Pure.EXECUTE_IN_DB_TO_TDS__STRING_1__FN_1),
        GENERATE_TEST_DATA("meta::relational::testDataGeneration::generateTestData",
                Pure.GENERATE_TEST_DATA__FUNCTION_DEFINITION_1__MAPPING_1__RUNTIME_1__TABLE_ROW_IDENTIFIERS_MANY__EXTENSION_MANY),
        GET_RELATIONAL_CSV_DATA("meta::relational::testDataGeneration::getRelationalCSVDataFromQuery",
                Pure.GET_RELATIONAL_CSV_DATA__FN_1__MAPPING_1),
        FETCH_DB_SCHEMAS_META_DATA("meta::relational::metamodel::execute::fetchDbSchemasMetaData",
                Pure.FETCH_DB_SCHEMAS_META_DATA),
        FETCH_DB_TABLES_META_DATA("meta::relational::metamodel::execute::fetchDbTablesMetaData",
                Pure.FETCH_DB_TABLES_META_DATA),
        FETCH_DB_COLUMNS_META_DATA("meta::relational::metamodel::execute::fetchDbColumnsMetaData",
                Pure.FETCH_DB_COLUMNS_META_DATA),
        FETCH_DB_PRIMARY_KEYS_META_DATA("meta::relational::metamodel::execute::fetchDbPrimaryKeysMetaData",
                Pure.FETCH_DB_PRIMARY_KEYS_META_DATA);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        Carrier(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, Carrier> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<Carrier> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }

        /** The JDBC DatabaseMetaData grid a fetchDb* native reads — host-evaluated
         *  against the H2 second target (engine-parity metadata casing), never lowered. */
        public enum FetchDbGrid { SCHEMAS, TABLES, COLUMNS, PRIMARY_KEYS }

        /** The metadata grid of a fetchDb* callee, or null when the callee is not one. */
        public static @com.legend.Nullable FetchDbGrid fetchDbGrid(@com.legend.Nullable String fqn) {
            Carrier c = fqn == null ? null : BY_FQN.get(fqn);
            if (c == null) {
                return null;
            }
            return switch (c) {
                case FETCH_DB_SCHEMAS_META_DATA -> FetchDbGrid.SCHEMAS;
                case FETCH_DB_TABLES_META_DATA -> FetchDbGrid.TABLES;
                case FETCH_DB_COLUMNS_META_DATA -> FetchDbGrid.COLUMNS;
                case FETCH_DB_PRIMARY_KEYS_META_DATA -> FetchDbGrid.PRIMARY_KEYS;
                case EXECUTE_IN_DB_TO_TDS, GENERATE_TEST_DATA, GET_RELATIONAL_CSV_DATA -> null;
            };
        }
    }

    /** natives that establish their OWN evaluation context for their arguments (assertError's catch): staging never enters them. */
    public enum ContextOwner implements Member {
        ASSERT_ERROR("meta::pure::functions::asserts::assertError",
                Pure.ASSERT_ERROR__MATCHER, Pure.ASSERT_ERROR__FN_1__STRING_1, Pure.ASSERT_ERROR__FN_1__STRING_1__INTEGER_01__INTEGER_01);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        ContextOwner(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, ContextOwner> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<ContextOwner> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** the DDL statement-string natives the executor renders from the model (create / drop schema and table). */
    public enum DdlStatement implements Member {
        CREATE_SCHEMA_STATEMENT("meta::relational::functions::toDDL::createSchemaStatement",
                Pure.DDL_CREATE_SCHEMA_STATEMENT__STRING_1),
        CREATE_TABLE_STATEMENT("meta::relational::functions::toDDL::createTableStatement",
                Pure.DDL_CREATE_TABLE_STATEMENT__DB_1__STRING_1__STRING_1, Pure.DDL_CREATE_TABLE_STATEMENT__DB_1__STRING_1),
        DROP_SCHEMA_STATEMENT("meta::relational::functions::toDDL::dropSchemaStatement",
                Pure.DDL_DROP_SCHEMA_STATEMENT__STRING_1),
        DROP_TABLE_STATEMENT("meta::relational::functions::toDDL::dropTableStatement",
                Pure.DDL_DROP_TABLE_STATEMENT__DB_1__STRING_1, Pure.DDL_DROP_TABLE_STATEMENT__DB_1__STRING_1__STRING_1);

        private final String fqn;
        private final List<NativeFunctionDefinition> overloads;

        DdlStatement(String fqn, NativeFunctionDefinition... overloads) {
            this.fqn = fqn;
            this.overloads = List.of(overloads);
        }

        @Override
        public String fqn() {
            return fqn;
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return overloads;
        }

        private static final Map<String, DdlStatement> BY_FQN = index(values());

        /** The member a callee FQN resolves to — empty when the callee is not
         *  in this family (a normal fall-through, never an error). */
        public static Optional<DdlStatement> of(@com.legend.Nullable String calleeFqn) {
            return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
        }
    }

    /** The ROW ACCESSORS — upstream's QUALIFIED PROPERTIES of {@code TDSRow}
     *  (tds.pure: {@code getString(colName){$this.get($colName)->cast(@String)}})
     *  and of the ResultSet {@code Row} twin ({@code value}), lifted as
     *  {@code <owner>$prop$<name>(this, colName)} and IMPLEMENTED by the
     *  platform: a literal column name FOLDS to the row's column read at type
     *  time; a non-literal name stays a call to the lifted property that
     *  RowGetters lowers by name (docs/TDS_ERASURE_DESIGN_2026_09_11.md §4b).
     *  No Pure.java overload: the prelude's declaration is the signature. */
    /** THE spelling of TDSRow (PlatformTypes.TDS_ROW reads it). */
    public static final String TDS_ROW_OWNER = "meta::pure::tds::TDSRow";
    /** The ResultSet Row (execute.pure). */
    public static final String EXECUTE_ROW_OWNER = "meta::relational::metamodel::execute::Row";

    public enum RowGetter implements Member {
        GET(TDS_ROW_OWNER, "get", false),
        IS_NULL(TDS_ROW_OWNER, "isNull", false),
        IS_NOT_NULL(TDS_ROW_OWNER, "isNotNull", false),
        GET_STRING(TDS_ROW_OWNER, "getString", true),
        GET_NULLABLE_STRING(TDS_ROW_OWNER, "getNullableString", true),
        GET_NUMBER(TDS_ROW_OWNER, "getNumber", true),
        GET_INTEGER(TDS_ROW_OWNER, "getInteger", true),
        GET_FLOAT(TDS_ROW_OWNER, "getFloat", true),
        GET_DECIMAL(TDS_ROW_OWNER, "getDecimal", true),
        GET_DATE(TDS_ROW_OWNER, "getDate", true),
        GET_DATE_TIME(TDS_ROW_OWNER, "getDateTime", true),
        GET_STRICT_DATE(TDS_ROW_OWNER, "getStrictDate", true),
        GET_BOOLEAN(TDS_ROW_OWNER, "getBoolean", true),
        GET_ENUM(TDS_ROW_OWNER, "getEnum", true),
        /** execute::Row's own by-name cell read (functions.pure: value(name) =
         *  at($this.values, indexOf($this.parent.columnNames, $name))) — the
         *  ResultSet twin of the TDSRow getters. */
        VALUE(EXECUTE_ROW_OWNER, "value", true);

        private final String owner;
        private final String property;
        private final boolean typedCell;

        RowGetter(String owner, String property, boolean typedCell) {
            this.owner = owner;
            this.property = property;
            this.typedCell = typedCell;
        }

        /** The declaring class. */
        public String owner() {
            return owner;
        }

        /** The property name. */
        public String property() {
            return property;
        }

        /** A TYPED cell read (getString, value, …) — folds to the column when
         *  the name is literal; get / isNull / isNotNull have their own forms. */
        public boolean typedCell() {
            return typedCell;
        }

        @Override
        public String fqn() {
            return com.legend.model.DerivedPropertyNames.lifted(owner, property);
        }

        @Override
        public List<NativeFunctionDefinition> overloads() {
            return List.of();
        }

        private static final Map<String, RowGetter> BY_PROPERTY;

        static {
            Map<String, RowGetter> m = new java.util.HashMap<>();
            for (RowGetter g : values()) {
                m.putIfAbsent(g.property, g);
            }
            BY_PROPERTY = Map.copyOf(m);
        }

        /** The accessor an APPLIED (bare) name spells, or empty. */
        public static Optional<RowGetter> of(@com.legend.Nullable String appliedName) {
            if (appliedName == null) {
                return Optional.empty();
            }
            int cut = appliedName.lastIndexOf("::");
            return Optional.ofNullable(BY_PROPERTY.get(cut < 0 ? appliedName : appliedName.substring(cut + 2)));
        }

        /** Whether {@code classFqn} is an ERASED ROW class — an owner of this
         *  family (TDSRow, execute::Row): a row whose columns only its parent
         *  result knows; in a type position it IS the late-bound row struct. */
        public static boolean isOwner(String classFqn) {
            for (RowGetter g : values()) {
                if (g.owner.equals(classFqn)) {
                    return true;
                }
            }
            return false;
        }

        /** The accessor a lifted derived-property callee spells, or empty. */
        public static Optional<RowGetter> ofLifted(@com.legend.Nullable String liftedFqn) {
            String[] ref = liftedFqn == null ? null : com.legend.model.DerivedPropertyNames.split(liftedFqn);
            if (ref == null) {
                return Optional.empty();
            }
            for (RowGetter g : values()) {
                if (g.owner.equals(ref[0]) && g.property.equals(ref[1])) {
                    return Optional.of(g);
                }
            }
            return Optional.empty();
        }
    }
}
