// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.builtin.NativeFn;
import com.legend.builtin.Pure;
import com.legend.builtin.SystemMetamodel;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Temporal;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.StatementEffects;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.Literals;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCopyInstance;
import com.legend.compiler.spec.typed.TypedCsvCensus;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedRawSqlRelation;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedTestDataGen;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.model.FunctionId;
import com.legend.model.MappingDefinition;
import com.legend.planner.BodyPlanner;
import com.legend.planner.ConnectionLets;
import com.legend.planner.LetPlanner;
import com.legend.planner.FramePlanner;
import com.legend.planner.ProductStatements;
import com.legend.compiler.spec.ResultEnvelopeSplice;
import com.legend.planner.QueryDoor;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The EFFECT PROGRAM's rule ({@link BodyClass#EFFECT_PROGRAM}): whether a body's database effects are planned steps,
 * and which effect shapes are literal. Split out of {@code BodyClass} (v2 item 8, 2026-11-16), the code moved
 * unchanged.
 */
final class EffectPrograms {

    private EffectPrograms() {
    }

    static boolean effectProgram(TypedQuery query, QueryDoor.Scan scan) {
        ModelContext ctx = query.context();
        List<TypedSpec> body = query.body();
        Map<FunctionId, Boolean> storeMemo = new HashMap<>();
        ElementConsumption consumption = new ElementConsumption(body, StoreReads.systemClasses(ctx), ctx, query.specs());
        boolean anEffect = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            TypedSpec value = statement instanceof TypedLet let ? let.value() : statement;
            if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.EFFECT)) && value instanceof TypedNativeCall call
                    && NativeFn.Effect.isDbEffect(call.callee().id())) {
                if (!literalEffect(call, body.subList(0, i))) {
                    return false;
                }
                if (statement instanceof TypedLet let && !ConnectionLets.onlyConnectionReads(body, i + 1, let.name())) {
                    // an effect let a later statement reads: the test door raises the walk's own refusal at this
                    // statement ("reading an executeInDb result binding ... is not supported", TestKinds.effect), as the
                    // walk raises it at the let's turn -- nothing after it is reached on either road (2026-11-04, the core
                    // program: ExecuteInDbDuckDbTest's effectfulLetReadIsLoud)
                    return true;
                }
                anEffect = true;
                continue;
            }
            if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.EFFECT)) && !(statement instanceof TypedLet)
                    && seedFormEffectMap(value, body.subList(0, i))) {
                // a seed form mapped over an effect (M10c item 7c)
                anEffect = true;
                continue;
            }
            // every other statement holds no test kind (no seeds, no assert, no effect inside it) and reads no store
            if (!kinds.isEmpty() || StoreReads.readsAStore(statement, query, storeMemo)) {
                return false;
            }
            if (statement instanceof TypedLet && i < body.size() - 1
                    && (consumption.consumedElementLet(i) || Seeds.seedFormCall(value, body.subList(0, i)) != null)) {
                // the corpus's setup helpers unfolded (M10c item 7c): the store and the runtime bound and consumed (the
                // element-consumption analysis: no read on either road), and the seed form bound (planning-time texts
                // the map after it spells) -- value lets the program splices
                continue;
            }
            if (!(statement instanceof TypedLet) && i < body.size() - 1
                    && statement instanceof com.legend.compiler.spec.typed.TypedRawSqlRelation) {
                // a RAW READ as a statement (executeInDb('select ...', $c): what an unread let of one is, the user's
                // ruling of 2026-11-02, LetPlanner.unreadAsStatements) -- a check at its turn on both roads, its rows
                // dropped: upstream runs it there, and what it raises fails the test
                continue;
            }
            if (statement instanceof TypedLet && i < body.size() - 1
                    && Literals.fold(value, body.subList(0, i)) instanceof String) {
                // a text the planner folds ('insert into ... ' + '(' + ...: a literal concatenation), which a later
                // effect reads as its text -- the effect's own rule folds it the same way (literalEffect); the corpus
                // burn-down, 2026-11-06: the aggregation-aware setUp's calendar insert
                continue;
            }
            boolean inert = value instanceof TypedNativeCall call
                    && NativeFn.Effect.isInertDiagnostic(call.callee().id());
            if (!inert && !literal(value) && BodyPlanner.handleType(value, ctx) == null) {
                // a computed value: the product's lowering would answer it where the walk sends its own statement
                // (the M5 audit's S3), a class of its own
                return false;
            }
        }
        return anEffect;
    }

    /** Whether {@code value} is a SEED FORM mapped over one db effect ({@code $sqls->map(sql | executeInDb($sql,
     *  $connection))} with {@code sqls} a {@code setUpDataSQLs} form whose data folds over {@code lets}: the corpus's
     *  setup helpers, unfolded into the body by the resolver's statement inliner): the test door plans its texts as the
     *  effect script the walk sends (M10c item 7c; {@code Seeds.seedForm}, one reader for both roads). */
    /** Whether {@code statement} is a database effect the test door writes as a statement of its own: not a let, and
     *  either a literal effect ({@link #literalEffect}) or a text form mapped over one ({@link #seedFormEffectMap}). */
    static boolean plannedEffect(TypedSpec statement, List<TypedSpec> lets) {
        if (statement instanceof TypedLet) {
            return false;
        }
        if (statement instanceof TypedNativeCall call && NativeFn.Effect.isDbEffect(call.callee().id())) {
            return literalEffect(call, lets);
        }
        return seedFormEffectMap(statement, lets);
    }

    static boolean seedFormEffectMap(TypedSpec value, List<TypedSpec> lets) {
        // a text form whose texts the planner spells -- a seed form's (item 7c) or a DDL text collection's (the corpus
        // burn-down, 2026-11-06) -- mapped over the one effect the door writes
        return value instanceof com.legend.compiler.spec.typed.TypedMap tm
                && (Seeds.seedForm(tm, lets) != null || Seeds.ddlTextSource(tm, lets) != null)
                && Seeds.oneEffectMapper(tm, lets);
    }

    /** Whether {@code query}'s body, holding no assert, has a statement that is a database effect whose String
     *  arguments are not all literals ({@link #literalEffect}): a text the database would compute before the effect is
     *  written -- a text hole, which lite does not have. */
    static boolean computedEffectText(TypedQuery query, QueryDoor.Scan scan) {
        List<TypedSpec> body = query.body();
        boolean computed = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            if (scan.kinds(statement).contains(QueryDoor.TestOnly.ASSERT)) {
                return false;
            }
            TypedSpec value = statement instanceof TypedLet let ? let.value() : statement;
            computed |= value instanceof TypedNativeCall call && NativeFn.Effect.isDbEffect(call.callee().id())
                    && !literalEffect(call, body.subList(0, i));
        }
        return computed;
    }

    /** Whether {@code value} is a String or Boolean literal: a value both roads answer with no statement sent. */
    static boolean literal(TypedSpec value) {
        return value instanceof TypedCString || value instanceof TypedCBoolean;
    }

    /** Whether {@code call}, a database effect, is one the test door writes: each String argument a literal (folded
     *  over {@code lets}, the statements before it) -- a {@code loadCsvToDbTable}'s, the CSV file's resource path, which
     *  its statement names for the database to read (the user's ruling, 2026-11-06). */
    static boolean literalEffect(TypedNativeCall call, List<TypedSpec> lets) {
        for (TypedSpec arg : EffectPlanner.stringArgs(call)) {
            if (!(Literals.fold(arg, lets) instanceof String)) {
                return false;
            }
        }
        return true;
    }
}
