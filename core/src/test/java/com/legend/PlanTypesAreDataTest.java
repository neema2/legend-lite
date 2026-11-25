// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.TestPlan;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * GUARD 1: THE PLAN'S TYPES ARE DATA (docs/PLAN_AS_DATA_2026_11_07.md 2.10, the design's step 3, 2026-11-08; the user,
 * 2026-11-08: "the thing I really want to ensure is that there is no code in the plan it's all data"). Every type
 * reachable from the product's plan ({@link ExecutionPlan}) and a test body's ({@link TestPlan}) -- record components,
 * a sealed interface's permitted subtypes, the elements of a list, a set or a map -- is a primitive or its box, a
 * String, a BigDecimal or BigInteger, a date or time value, an enum, or a record or sealed interface of the plans' own
 * libraries or the SQL tree ({@code com.legend.sql}, pure data by AGENTS.md 3a). A functional type, a Throwable,
 * Object, a wildcard, a type variable or a type of any other library is a violation. The violations found when the
 * guard was introduced are pinned below, shrink-only: a pin leaves with a dated note; a new violation fails.
 */
@Tag("guardrail")
class PlanTypesAreDataTest {

    /** The violations at the guard's introduction (2026-11-08), each the offending type and the first path to it. */
    private static final Set<String> PINNED = Set.<String>of(
            // (2026-11-08, the plan-as-data design's step 7: the model's ConnectionDefinition at Declared.connection and
            // its DatabaseType enum at Platform.type left the pins -- the plan holds its own faithful copy of the
            // connection description, executionplan.Connection and the records it names)
            // (2026-11-08, the plan-as-data design's step 9: the window frame bound's abstract Number left the pins --
            // the SQL tree's offset is a closed data type, Frame.Offset: an integer, a float or a decimal. Guard 1 is at
            // zero: a new violation fails, nothing is pinned)
            );

    private static final Set<String> DATA_PACKAGES = Set.of("com.legend.executionplan", "com.legend.testexecutionplan",
            "com.legend.sql",
            // 2026-11-08, the plan-as-data design's step 8 (2.10's guard 1 names them): the edge's extension records,
            // reached only through the test plan's Extension -- every implementation on the classpath, walked as data
            "com.legend.edge",
            // 2026-11-21, stage B's BE (the one error model; for the user's word with BE's moves): an error's record
            // (error.ErrorRecord: its code, one of the package's enums, already data below; its values; its frames),
            // what a planned failure carries in place of a message -- walked as data, so an exception class of the
            // package reached from a plan is still a violation
            "com.legend.error");

    private static final Set<Class<?>> LEAVES = Set.of(String.class, Boolean.class, Byte.class, Short.class,
            Integer.class, Long.class, Float.class, Double.class, Character.class, java.math.BigDecimal.class,
            java.math.BigInteger.class, java.time.LocalDate.class, java.time.LocalDateTime.class,
            java.time.LocalTime.class, java.time.Instant.class, java.time.OffsetDateTime.class,
            java.time.ZonedDateTime.class);

    /** Every class on the classpath that implements the test plan's {@code Extension} (the edge's records). */
    private static List<Class<?>> extensionImplementations() {
        List<Class<?>> out = new java.util.ArrayList<>();
        for (var c : new com.tngtech.archunit.core.importer.ClassFileImporter().importPackages("com.legend")) {
            if (!c.isInterface() && c.isAssignableTo(com.legend.testexecutionplan.Extension.class)) {
                out.add(c.reflect());
            }
        }
        if (out.isEmpty()) {
            throw new IllegalStateException("no implementation of the test plan's Extension on the classpath to check");
        }
        return out;
    }

    @Test
    void everyTypeAPlanHoldsIsData() {
        Set<String> violations = new TreeSet<>();
        Set<Type> seen = new HashSet<>();
        Deque<Object[]> todo = new ArrayDeque<>();
        todo.add(new Object[] {ExecutionPlan.class, "ExecutionPlan"});
        todo.add(new Object[] {TestPlan.class, "TestPlan"});
        while (!todo.isEmpty()) {
            Object[] next = todo.poll();
            Type type = (Type) next[0];
            String path = (String) next[1];
            // every (type, path) is judged; only the walk INTO a type is done once (the slice 2 invariant audit,
            // 2026-11-08: the seen-check stood before the judgment, so a second field of an already-seen bad type --
            // Following.n beside Preceding.n -- was never reported)
            switch (type) {
                case ParameterizedType p -> {
                    Class<?> raw = (Class<?>) p.getRawType();
                    if (raw == List.class || raw == Set.class || raw == Map.class) {
                        if (seen.add(type)) {
                            for (Type arg : p.getActualTypeArguments()) {
                                todo.add(new Object[] {arg, path + "<>"});
                            }
                        }
                    } else {
                        violations.add(p.getTypeName() + " at " + path);
                    }
                }
                case WildcardType w -> violations.add("wildcard " + w.getTypeName() + " at " + path);
                case TypeVariable<?> v -> violations.add("type variable " + v.getName() + " at " + path);
                case Class<?> c -> {
                    if (c.isPrimitive() || LEAVES.contains(c)) {
                        continue;
                    }
                    if (c.isEnum()) {
                        // an enum carries no state of a type the walk would see (fields, constant bodies): one of the
                        // plans' own libraries, the SQL tree's or the error vocabulary's (a failure's phase) alone
                        // (the slice 1 audit, 2026-11-08)
                        if (!DATA_PACKAGES.contains(c.getPackageName()) && !c.getPackageName().equals("com.legend.error")) {
                            violations.add("enum " + c.getName() + " at " + path);
                        }
                        // its own state and its constants walked too (the slice 3 invariant audit, 2026-11-08: an enum
                        // was taken on trust, so a field holding a lambda would have passed): every instance field
                        // judged as a component, and a constant with a body -- a class of its own, code -- refused
                        if (seen.add(c)) {
                            for (java.lang.reflect.Field f : c.getDeclaredFields()) {
                                if (!f.isEnumConstant() && !f.isSynthetic()
                                        && !java.lang.reflect.Modifier.isStatic(f.getModifiers())) {
                                    todo.add(new Object[] {f.getGenericType(), c.getSimpleName() + "." + f.getName()});
                                }
                            }
                            for (Object constant : c.getEnumConstants()) {
                                if (constant.getClass() != c) {
                                    violations.add("enum constant with a body " + c.getName() + "." + constant + " at "
                                            + path);
                                }
                            }
                        }
                        continue;
                    }
                    if (!seen.add(c) && DATA_PACKAGES.contains(c.getPackageName())) {
                        // a data type already walked: its components were judged on its first path
                        continue;
                    }
                    if (c == com.legend.testexecutionplan.Extension.class) {
                        // the edge's records (2.10, guard 1): every implementation on the classpath, each walked as
                        // the plan's data -- an extension point implemented by anything but data fails here
                        for (Class<?> impl : extensionImplementations()) {
                            todo.add(new Object[] {impl, "Extension implemented by"});
                        }
                        continue;
                    }
                    if (!DATA_PACKAGES.contains(c.getPackageName())) {
                        violations.add(c.getName() + " at " + path);
                        continue;
                    }
                    if (c.isRecord()) {
                        for (RecordComponent rc : c.getRecordComponents()) {
                            todo.add(new Object[] {rc.getGenericType(), c.getSimpleName() + "." + rc.getName()});
                        }
                    } else if (c.isSealed()) {
                        for (Class<?> sub : c.getPermittedSubclasses()) {
                            todo.add(new Object[] {sub, c.getSimpleName() + " permits"});
                        }
                    } else {
                        violations.add(c.getName() + " (neither a record nor a sealed interface) at " + path);
                    }
                }
                default -> violations.add(type.getTypeName() + " at " + path);
            }
        }
        assertEquals(new TreeSet<>(PINNED), violations, "the plans' types are data: a new violation is a type a plan"
                + " must not hold; a pin that no longer holds leaves PINNED with a dated note");
    }

    /** The prose a plan still holds, each "Record.component" with what it is and the step that removes it: shrink-only
     *  (a component that leaves is deleted here with a dated note; a new one fails). Measured 2026-11-21, default-deny
     *  since the BE audit 3's M2 (every text component reviewed: a fact in {@link #FACTS}, or prose here). */
    private static final java.util.Map<String, String> PROSE_RESIDUE = java.util.Map.ofEntries(
            // (gone 2026-11-21, stage B's BE, item 2: a raise's where -- Site.where, RaiseText.where -- its facts its
            // operands and its position; a load step's "of", now its table's name; a payload's label, now its
            // characters; a project's refusal sentence, now a record's code and values)
            // (SqlExpr$RaiseText.asData: gone 2026-11-25, B2b's S7a, the user's ruling "go with A" -- a checked graph's
            // unevaluable-constraint sentence is a plain literal of the defect row, a model constant inside a successful
            // answer, as a violated constraint's is since S3b: a StringLit's value, a fact)
            // a failure with no record (a record and a message never ride together, Failure's constructor)
            // (its reason reworded 2026-11-21, item 3's S3: an assert's failure left it; the entry itself unchanged)
            Map.entry("Failure.message", "a failure with no record: an assert's failure is a record since item 3's S3"
                    + " (AssertKind); what remains is a lite error whose site states no record yet (the one error model's"
                    + " families still being converted) and the edge's prose failures (SqlTextVerdicts' NOT_A_CONSTANT and"
                    + " Appeal.Read.failure, B3)"),
            Map.entry("Failure.unjudgedReason", "why an assert is unjudged: none on the product's planned road since D"
                    + " (an unjudged decline refuses the body at planning); the edge's appeal path still records one"
                    + " (SqlTextVerdicts' Failures.of) -- removed with that path (B3)"),
            // (the host judge's assert words decided at planning -- AllRows.message, Decided.message,
            // Messaged.unplanned: records since 2026-11-21, item 3's S3, assert verdicts as records)
            // the compatibility edge's records in a test plan, withdrawn from the run with E1 (B3)
            Map.entry("Appeal$Unlisted.message", "an appeal's error message -- the edge's, B3"),
            Map.entry("Appeal.contract", "an appeal's description (\"(sql-text, H2 -- text is the contract)\") -- B3"),
            Map.entry("EngineTextJudgment.decline", "an engine-text comparison's decline reason -- the edge's, B3"));

    /** THE TEXT A PLAN HOLDS THAT IS A FACT, every component reviewed (the BE audit 3's M2, 2026-11-21: until then
     *  the guard flagged a component by its NAME alone, so prose under another name passed -- now default-deny: a text
     *  component the plans reach is a fact listed here or prose listed in {@link #PROSE_RESIDUE}, nothing else). Each
     *  set's key says what its texts are (secrets never: a connection's are vault references); a new text component
     *  fails until it is reviewed into one of them. */
    private static final java.util.Map<String, Set<String>> FACTS = java.util.Map.ofEntries(
            Map.entry("SQL: a statement the plan sends, a database's pattern or format", Set.of(
                    "Appeal$Rows.goldenPlan", "Appeal$Rows.replaySql", "DateFmt$Text.s",
                    "EffectTexts$Statement.text", "EngineTextJudgment.golden", "EngineTextJudgment.ours",
                    "ExecutionPlan$DatabaseSettings.readBack", "ExecutionPlan$Effect$Script.abort",
                    "ExecutionPlan$Effect$Script.bracketed", "ExecutionPlan$Effect$Script.statements",
                    "ExecutionPlan$LoadStep$Bound.sql", "ExecutionPlan$LoadStep$Delivery$Reads.after",
                    "ExecutionPlan$LoadStep$Delivery$Reads.statements", "ExecutionPlan$LoadStep$Statement.sql",
                    "ExecutionPlan$LoadStep$Stream.count", "ExecutionPlan$LoadStep$Stream.prepare",
                    "ExecutionPlan$LoadStep$Stream.sql", "ExecutionPlan$RaiseReadBack.read",
                    "ExecutionPlan$RaiseReadBack.reset", "ExecutionPlan$Region.empty", "ExecutionPlan$Region.lead",
                    "ExecutionPlan$Region.separator", "ExecutionPlan$Sql.statement",
                    "ExecutionPlan$Transaction.begin", "ExecutionPlan$Transaction.commit",
                    "ExecutionPlan$Transaction.rollback", "SqlSource$Catalog.columnPattern",
                    "SqlSource$Catalog.schemaPattern", "SqlSource$Catalog.tablePattern", "SqlSource$RawSql.sql",
                    "TestPlan$Verdicts$Savepoint.release", "TestPlan$Verdicts$Savepoint.rollbackTo",
                    "TestPlan$Verdicts$Savepoint.set")),
            Map.entry("a value the program or its data wrote, an error record's values", Set.of(
                    "Appeal$InListSeed.values", "Appeal$TempTable.values", "ErrorRecord$Frame.values",
                    "ErrorRecord.position", "ErrorRecord.values", "ExecutionPlan$Constant.cells",
                    "ExecutionPlan$Keyed.admitted", "ExecutionPlan$Load.content",
                    "ExecutionPlan$LoadStep$Bind$Value.text", "ExecutionPlan$Parameter.enumValues",
                    "ExecutionPlan$Sql.content", "HostJudgment$AssertError.expected",
                    "HostJudgment$RowArg$Literal.text", "Operand$Day.iso", "Operand$Instant.iso",
                    "Operand$Text.value", "RefereeLoad.content", "RootLoop.fragments", "SqlExpr$DateLit.iso",
                    "SqlExpr$StringLit.value", "SqlExpr$TimeLit.iso", "SqlExpr$TimestampLit.iso",
                    "TestPlan$Side$Constant.cells")),
            Map.entry("a connection's or session's settings, a data reference's name or digest", Set.of(
                    "AuthenticationSpec$ApiToken.apiToken",
                    "AuthenticationSpec$DelegatedKerberos.kerberosRemoteServiceName",
                    "AuthenticationSpec$DelegatedKerberos.serverPrincipal",
                    "AuthenticationSpec$GcpWorkloadIdentityFederation.additionalGcpScopes",
                    "AuthenticationSpec$GcpWorkloadIdentityFederation.serviceAccountEmail",
                    "AuthenticationSpec$MiddleTierUserNamePassword.vaultReference",
                    "AuthenticationSpec$OAuth.oauthKey", "AuthenticationSpec$OAuth.scopeName",
                    "AuthenticationSpec$SnowflakePublic.passPhraseVaultReference",
                    "AuthenticationSpec$SnowflakePublic.privateKeyVaultReference",
                    "AuthenticationSpec$SnowflakePublic.publicUserName",
                    "AuthenticationSpec$UsernamePassword.passwordVaultRef",
                    "AuthenticationSpec$UsernamePassword.username",
                    "AuthenticationSpec$VaultUserNamePassword.baseVaultReference",
                    "AuthenticationSpec$VaultUserNamePassword.passwordVaultReference",
                    "AuthenticationSpec$VaultUserNamePassword.userNameVaultReference", "Connection.qualifiedName",
                    "Connection.storeName", "ConnectionSpecification$BigQuery.defaultDataset",
                    "ConnectionSpecification$BigQuery.projectId", "ConnectionSpecification$Databricks.hostname",
                    "ConnectionSpecification$Databricks.httpPath", "ConnectionSpecification$Databricks.port",
                    "ConnectionSpecification$Databricks.protocol", "ConnectionSpecification$EmbeddedH2.databaseName",
                    "ConnectionSpecification$EmbeddedH2.directory", "ConnectionSpecification$LocalFile.path",
                    "ConnectionSpecification$LocalH2.url", "ConnectionSpecification$Snowflake.accountName",
                    "ConnectionSpecification$Snowflake.accountType", "ConnectionSpecification$Snowflake.cloudType",
                    "ConnectionSpecification$Snowflake.databaseName",
                    "ConnectionSpecification$Snowflake.organization", "ConnectionSpecification$Snowflake.region",
                    "ConnectionSpecification$Snowflake.role", "ConnectionSpecification$Snowflake.warehouseName",
                    "ConnectionSpecification$Spanner.databaseId", "ConnectionSpecification$Spanner.instanceId",
                    "ConnectionSpecification$Spanner.projectId", "ConnectionSpecification$StaticDatasource.database",
                    "ConnectionSpecification$StaticDatasource.host", "ConnectionSpecification$Trino.catalog",
                    "ConnectionSpecification$Trino.clientTags", "ConnectionSpecification$Trino.host",
                    "ConnectionSpecification$Trino.schema", "DataRef.digest", "DataRef.name",
                    "ExecutionPlan$Binding$Payload.digest", "ExecutionPlan$Binding$Source.location",
                    "ExecutionPlan$Database$System.graphKey", "ExecutionPlan$Load.identity",
                    "ExecutionPlan$Setting.name", "ExecutionPlan$Setting.value", "ExecutionPlan$Target.session")),
            Map.entry("a code: a kind, a form, a type's spelling, a slot, a style's name", Set.of(
                    "Appeal$Enums.decodes", "Appeal$InListSeed.kind", "Appeal$Rows.planBindings",
                    "Appeal$Rows.sortKeys", "Appeal$TempTable.kind", "Assertion.family", "BodyFacts.shape",
                    "ExecutionPlan$Binding$Array.elementSqlType", "ExecutionPlan$Binding$One.nullType",
                    "ExecutionPlan$Column.slot", "ExecutionPlan$Column.type", "ExecutionPlan$LoadStep$Stream.plane",
                    "ExecutionPlan$Parameter.type", "ExecutionPlan$Sql.stamp", "ExecutionPlan$Update.count",
                    "HostJudgment$EdgeJudged.sideOrigin", "HostJudgment$Negated.kind", "HostJudgment$Rendered.aForm",
                    "HostJudgment$Rendered.eForm", "HostJudgment$Rendered.form",
                    "HostJudgment$RowArg$Literal.carrier", "HostJudgment$SideFacts.declared",
                    "SideShape$Column.type", "SideShape.multiplicity", "SideShape.typeName",
                    "SqlDdl$ColumnType$Scaled.kind", "SqlDdl$ColumnType$Sized.kind", "SqlExpr$Tagged$Candidate.kind",
                    "SqlExpr$TaggedList$Channel.kind", "SqlExpr$WindowCall$Frame$Bound$IntervalFollowing.unit",
                    "SqlExpr$WindowCall$Frame$Bound$IntervalPreceding.unit", "SqlSource$RawSql.writtenFor",
                    "SqlSource$Table.storedTypes", "SqlType$Tagged$Slot.kind", "TestPlan$Verdict.arm")),
                    // (TestPlan.words, the plan's audience's words' name: gone 2026-11-21, item 3's S3)
            Map.entry("a name: an element's, a column's, a table's, an alias, a path", Set.of(
                    "Appeal$Enums.enums", "Appeal$Enums.properties", "Appeal$Rows.classFqn",
                    "Appeal$Rows.mappingFqn", "Appeal$TempTable.name", "Appeal$Unlisted.simpleName", "Appeal.name",
                    "Assertion.listenerName", "EngineTextJudgment.name", "ExecutionPlan$Column.name",
                    "ExecutionPlan$Keyed$Fixed.field", "ExecutionPlan$Keyed$PerKey.field",
                    "ExecutionPlan$Keyed.keyFields", "ExecutionPlan$LoadStep$Attach.as",
                    "ExecutionPlan$LoadStep$Attach.held", "ExecutionPlan$LoadStep$Bind$Held.field",
                    "ExecutionPlan$LoadStep$Bind$Held.name", "ExecutionPlan$LoadStep$Stream.schema",
                    "ExecutionPlan$LoadStep$Stream.table", "ExecutionPlan$LoadStep$Statements.table",
                    "ExecutionPlan$Parameter.name",
                    "ExecutionPlan$Project.key", "ExecutionPlan$Slot.parameter", "Failure.element",
                    "HostJudgment$AllRows.name", "HostJudgment$Condition.name", "HostJudgment$Equals.name",
                    "HostJudgment$GridPair.name", "HostJudgment$InstanceOf.acceptedTypes",
                    "HostJudgment$InstanceOf.typeName", "HostJudgment$InstanceOf.valueClass", "HostJudgment$Is.name",
                    "HostJudgment$Keys.byClass",
                    "HostJudgment$Rendered.name", "HostJudgment$RowArg$Field.name", "HostJudgment$Rows.actualClass",
                    "HostJudgment$Rows.expectedClass", "HostJudgment$Rows.name", "HostJudgment$SideFacts.classFqn",
                    "HostJudgment$SideFacts.typeName", "HostJudgment$Subset.name", "KeyTree$Key.declaredClass",
                    "KeyTree$Key.name", "KeyTree.classFqn", "KindClass$Element.fqn", "KindClass$Enum.fqn",
                    "KindClass$Instance.fqn", "OutputCol.name", "RootLoop$Root.listenerName", "RootLoop$Row.name",
                    "SideShape$Column.name", "SqlExpr$CheckedDefects$Hoist.path", "SqlExpr$Column.name",
                    "SqlExpr$Column.table", "SqlExpr$Lambda.params", "SqlExpr$PlanParam.name",
                    "SqlExpr$RowOrder.table", "SqlExpr$Star.table", "SqlExpr$StarExcept.except",
                    "SqlExpr$StarExcept.table", "SqlExpr$StructGet.field", "SqlExpr$StructLit$Field.name",
                    "SqlSelect$Projection.alias", "SqlSelect$SortKey.outputName", "SqlSource$Catalog.alias",
                    "SqlSource$Cte.alias", "SqlSource$Cte.name", "SqlSource$GridReport.alias",
                    "SqlSource$Pivot$Using.alias", "SqlSource$Pivot.alias", "SqlSource$RawSql.alias",
                    "SqlSource$SourceUrl.alias", "SqlSource$SourceUrl.url", "SqlSource$Subselect.alias",
                    "SqlSource$Subselect.frameName", "SqlSource$Table.alias", "SqlSource$Table.name",
                    "SqlSource$Values.alias", "SqlSource$Values.columns", "SqlType$Struct$Field.name",
                    "SqlWith$Cte.name", "TestPlan$Verdict.name")));

    /** NO PROSE IN A PLAN: every text component the plans' types reach (a String, or a list, set or map of them) is
     *  a reviewed fact or a named residue -- default-deny (the BE audit 3's M2, 2026-11-21). */
    @Test
    void noComponentHoldsProse() {
        Set<String> prose = new TreeSet<>();
        Set<Type> seen = new HashSet<>();
        Deque<Type> todo = new ArrayDeque<>(List.of(ExecutionPlan.class, TestPlan.class));
        while (!todo.isEmpty()) {
            Type type = todo.poll();
            if (!seen.add(type)) {
                continue;
            }
            if (type instanceof ParameterizedType p) {
                todo.addAll(List.of(p.getActualTypeArguments()));
                continue;
            }
            if (!(type instanceof Class<?> c) || !DATA_PACKAGES.contains(c.getPackageName())) {
                continue;
            }
            if (c == com.legend.testexecutionplan.Extension.class) {
                todo.addAll(extensionImplementations());
            } else if (c.isRecord()) {
                for (RecordComponent rc : c.getRecordComponents()) {
                    if (holdsText(rc.getGenericType())) {
                        prose.add(c.getName().substring(c.getPackageName().length() + 1) + "." + rc.getName());
                    }
                    todo.add(rc.getGenericType());
                }
            } else if (c.isSealed()) {
                todo.addAll(List.of(c.getPermittedSubclasses()));
            }
        }
        Set<String> listed = new TreeSet<>(PROSE_RESIDUE.keySet());
        for (Set<String> facts : FACTS.values()) {
            for (String f : facts) {
                assertTrue(listed.add(f), f + ": listed twice, a fact and prose or two facts");
            }
        }
        assertEquals(listed, prose, "a plan holds no prose (stage B's BE-P): a text"
                + " component is a fact reviewed into FACTS, or prose -- a sentence in the plan: make it a code and its values,"
                + " or name it in PROSE_RESIDUE with the step that removes it; a component gone leaves its list with a"
                + " dated note");
    }

    /** Whether {@code t} is a text, or a list, set or map holding texts. */
    private static boolean holdsText(Type t) {
        if (t == String.class) {
            return true;
        }
        return t instanceof ParameterizedType p && List.of(p.getActualTypeArguments()).stream()
                .anyMatch(PlanTypesAreDataTest::holdsText);
    }
}
