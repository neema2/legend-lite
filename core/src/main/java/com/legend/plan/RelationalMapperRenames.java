// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;
import com.legend.model.DatabaseDefinition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The connection's {@code queryPostProcessorsWithParameter}
 * relationalMapperPostProcessor channel (real runtime/connection/
 * postprocessor.pure:50 + metamodel.pure:185-208): DatabaseMapper /
 * SchemaMapper / TableMapper rename table spellings at plan-SQL
 * generation time. The config subtree reaches here UNINLINED
 * (UserCallInliner's boundary property) and is read STRUCTURALLY —
 * {@code schema()/getSchema()/getTable()} navigations contribute
 * (database, schema, table) identities without evaluation, so the
 * corpus's recursive helpers never run. Identities normalize to the
 * DEFINING store through includes (engine object identity — TwoDBs
 * pins that same-named schemas in different databases rename
 * differently); the result is an old&rarr;new table-spelling map
 * applied by {@link SqlPostProcessors#apply} over the SQL IR.
 *
 * <p>Spelling rules (testRelationalMapper.pure goldens): a TableMapper
 * replaces the table name; a SchemaMapper the schema prefix; a
 * DatabaseMapper PREPENDS its catalog to every table of its schemas
 * and makes the {@code default} schema EXPLICIT
 * ({@code snDBDefault.default.firmTableNew}); the three compose.
 * Unrecognized postprocessor entries are ignored (they ride their own
 * channels); unrecognized MAPPER config shapes are loud.
 */
public final class RelationalMapperRenames {

    private RelationalMapperRenames() {
    }

    private static final String PP_FQN = com.legend.builtin.NativeFn.ResolverForm.RELATIONAL_MAPPER_POST_PROCESSOR.fqn();
    private static final String SCHEMA_NAV =
            "meta::relational::metamodel::schema";

    public static java.util.function.UnaryOperator<String> extract(
            TypedSpec runtimeArg,
            SpecCompiler specs, Map<String, TypedSpec> queryLets,
            ModelContext ctx) {
        Cfg cfg = new Cfg(specs, queryLets, ctx);
        walk(runtimeArg, cfg);
        if (cfg.catalog.isEmpty() && cfg.schemaTo.isEmpty()
                && cfg.tableTo.isEmpty()) {
            return java.util.function.UnaryOperator.identity();
        }
        return name -> rename(name, cfg);
    }

    /** Parse state: the three mapper tables keyed by DEFINING-db identity. */
    private static final class Cfg {
        final SpecCompiler specs;
        final Map<String, TypedSpec> queryLets;
        final ModelContext ctx;
        final Map<List<String>, String> catalog = new LinkedHashMap<>();
        final Map<List<String>, String> schemaTo = new LinkedHashMap<>();
        final Map<List<String>, String> tableTo = new LinkedHashMap<>();

        Cfg(SpecCompiler specs, Map<String, TypedSpec> queryLets,
                ModelContext ctx) {
            this.specs = specs;
            this.queryLets = queryLets;
            this.ctx = ctx;
        }

        /** Peel wrappers, splice query lets, and step INTO zero-arg
         * config helpers' single-expression bodies (the standing
         * schemaMappers()/tableMappers() calls). */
        @com.legend.base.Nullable TypedSpec resolve(@com.legend.base.Nullable TypedSpec v) {
            if (v == null) {
                return null;
            }
            TypedSpec cur = peel(v);
            for (int hop = 0; hop < 20; hop++) {
                if (cur instanceof TypedVariable var) {
                    TypedSpec b = queryLets.get(var.name());
                    if (b == null) {
                        throw new NotImplementedException(
                                "relationalMapper config variable '$"
                                + var.name() + "' is not a query let");
                    }
                    cur = peel(b);
                } else if (cur instanceof TypedUserCall uc
                        && uc.args().isEmpty()) {
                    List<TypedSpec> body = specs.compile(uc.callee()).body();
                    if (body.size() != 1) {
                        throw new NotImplementedException(
                                "relationalMapper config helper '"
                                + uc.callee().qualifiedName()
                                + "' is not a single-expression body");
                    }
                    cur = peel(body.get(0));
                } else {
                    return cur;
                }
            }
            throw new NotImplementedException(
                    "relationalMapper config resolution did not converge");
        }
    }

    private static void walk(TypedSpec n, Cfg c) {
        if (n instanceof TypedNewInstance ni && ni.properties()
                .containsKey("queryPostProcessorsWithParameter")) {
            for (TypedSpec el : elements(ni.properties()
                    .get("queryPostProcessorsWithParameter"))) {
                readPostProcessor(peel(el), c);
            }
        }
        for (TypedSpec ch : n.children()) {
            walk(ch, c);
        }
    }

    private static void readPostProcessor(@com.legend.base.Nullable TypedSpec p, Cfg c) {
        if (!(p instanceof TypedNativeCall call) || call.args().isEmpty()
                || com.legend.builtin.NativeFn.ResolverForm.of(call.callee().qualifiedName()).orElse(null) != com.legend.builtin.NativeFn.ResolverForm.RELATIONAL_MAPPER_POST_PROCESSOR) {
            return;   // other postprocessors ride their own channels
        }
        TypedSpec cfg = c.resolve(call.args().get(0));
        if (!(cfg instanceof TypedNewInstance cni)) {
            throw new NotImplementedException(
                    "relationalMapperPostProcessor argument is not a"
                    + " RelationalMapperPostProcessor construction");
        }
        for (TypedSpec rm : elements(c.resolve(
                cni.properties().get("relationalMappers")))) {
            TypedSpec r = c.resolve(rm);
            if (!(r instanceof TypedNewInstance rni)) {
                throw new NotImplementedException(
                        "relationalMappers entry is not a RelationalMapper"
                        + " construction");
            }
            for (TypedSpec dm : mapperList(rni, "databaseMappers", c)) {
                readDatabaseMapper(dm, c);
            }
            for (TypedSpec sm : mapperList(rni, "schemaMappers", c)) {
                TypedNewInstance ni = mapperInst(sm, c);
                c.schemaTo.put(schemaKey(c.resolve(
                        ni.properties().get("from")), c),
                        stringProp(ni, "to"));
            }
            for (TypedSpec tm : mapperList(rni, "tableMappers", c)) {
                TypedNewInstance ni = mapperInst(tm, c);
                c.tableTo.put(tableKey(c.resolve(
                        ni.properties().get("from")), c),
                        stringProp(ni, "to"));
            }
        }
    }

    private static List<TypedSpec> mapperList(TypedNewInstance rni,
            String prop, Cfg c) {
        TypedSpec v = rni.properties().get(prop);
        return v == null ? List.of() : elements(c.resolve(v));
    }

    private static TypedNewInstance mapperInst(@com.legend.base.Nullable TypedSpec v, Cfg c) {
        if (c.resolve(v) instanceof TypedNewInstance ni) {
            return ni;
        }
        throw new NotImplementedException(
                "mapper entry is not an instance construction: " + v);
    }

    private static void readDatabaseMapper(TypedSpec v, Cfg c) {
        TypedNewInstance ni = mapperInst(v, c);
        String catalogName = stringProp(ni, "database");
        for (TypedSpec s : elements(c.resolve(
                ni.properties().get("schemas")))) {
            c.catalog.put(schemaKey(c.resolve(s), c), catalogName);
        }
    }

    /** A schema NAVIGATION — the native {@code schema(db, 'name')} or a
     * shape-alike helper call ({@code getSchema}) — normalized to
     * [definingDbFqn, schemaName]. */
    private static List<String> schemaKey(@com.legend.base.Nullable TypedSpec nav, Cfg c) {
        List<TypedSpec> args = navArgs(nav, 2);
        String dbFqn = refFqn(args.get(0));
        String schema = stringOf(args.get(1));
        return List.of(definingDb(c.ctx, dbFqn, schema), schema);
    }

    /** {@code getTable(db, 'schema', 'table')} — normalized to
     * [definingDbFqn, schema, table]. */
    private static List<String> tableKey(@com.legend.base.Nullable TypedSpec nav, Cfg c) {
        List<TypedSpec> args = navArgs(nav, 3);
        String dbFqn = refFqn(args.get(0));
        String schema = stringOf(args.get(1));
        String table = stringOf(args.get(2));
        return List.of(definingDb(c.ctx, dbFqn, schema), schema, table);
    }

    private static List<TypedSpec> navArgs(@com.legend.base.Nullable TypedSpec nav, int arity) {
        if (nav instanceof TypedNativeCall nc && nc.args().size() == arity
                && (arity != 2
                        || SCHEMA_NAV.equals(nc.callee().qualifiedName()))) {
            return nc.args();
        }
        if (nav instanceof TypedUserCall uc && uc.args().size() == arity) {
            return uc.args();
        }
        throw new NotImplementedException("relationalMapper 'from' is not a"
                + " store navigation: " + nav);
    }

    private static String refFqn(@com.legend.base.Nullable TypedSpec v) {
        if (peel(v) instanceof TypedPackageableRef pr) {
            return pr.fullPath();
        }
        throw new NotImplementedException(
                "relationalMapper database reference is not an element ref");
    }

    private static String stringOf(@com.legend.base.Nullable TypedSpec v) {
        if (peel(v) instanceof TypedCString cs) {
            return cs.value();
        }
        throw new NotImplementedException(
                "relationalMapper name is not a string literal: " + v);
    }

    private static String stringProp(TypedNewInstance ni, String prop) {
        return stringOf(ni.properties().get(prop));
    }

    private static List<TypedSpec> elements(@com.legend.base.Nullable TypedSpec v) {
        if (v == null) {
            return List.of();
        }
        return v instanceof TypedCollection tc ? tc.elements() : List.of(v);
    }

    /** toOne()/cast wrappers peel — identity for navigation. */
    private static @com.legend.base.Nullable TypedSpec peel(@com.legend.base.Nullable TypedSpec v) {
        TypedSpec cur = v;
        while (true) {
            if (cur instanceof TypedNativeCall c && c.args().size() == 1
                    && (com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())
                            || c.callee().qualifiedName()
                                    .endsWith("::toOneMany"))) {
                cur = c.args().get(0);
                continue;
            }
            if (cur instanceof com.legend.compiler.spec.typed.TypedCast tc) {
                cur = tc.source();
                continue;
            }
            return cur;
        }
    }

    // ===== model identity =====

    /** The db that DEFINES (schema) — include sharing preserves engine
     * object identity, so a schema reached through an include belongs
     * to the including chain's defining store. */
    private static String definingDb(ModelContext ctx, String dbFqn,
            String schema) {
        DatabaseDefinition db = ctx.findDatabase(dbFqn).orElse(null);
        if (db == null) {
            return dbFqn;
        }
        if (definesSchema(db, schema)) {
            return dbFqn;
        }
        for (String inc : db.includes()) {
            String hit = definingDb(ctx, inc, schema);
            DatabaseDefinition incDb = ctx.findDatabase(hit).orElse(null);
            if (incDb != null && definesSchema(incDb, schema)) {
                return hit;
            }
        }
        return dbFqn;
    }

    private static boolean definesSchema(DatabaseDefinition db,
            String schema) {
        if ("default".equals(schema)) {
            return !db.tables().isEmpty() || !db.views().isEmpty();
        }
        return db.schemas().stream().anyMatch(s -> s.name().equals(schema));
    }

    // ===== the spelling resolver =====

    /** New spelling of one IR table name ({@code schema.table} or the
     * bare default-schema {@code table}). The owning db comes from the
     * CONFIG keys sharing the schema name; when two configured dbs
     * declare the same schema name (TwoDBs), the table's model
     * membership discriminates — the query's tables are always in the
     * module (its mapping references them), so the lookup cannot miss.
     * Residual ambiguity is loud, never a guessed rename. */
    private static String rename(String name, Cfg c) {
        int dot = name.indexOf('.');
        String schema = dot > 0 ? name.substring(0, dot) : "default";
        String table = dot > 0 ? name.substring(dot + 1) : name;
        if (table.indexOf('.') >= 0) {
            return name;   // already catalog-qualified — not ours
        }
        Set<String> dbs = new LinkedHashSet<>();
        for (List<String> k : c.catalog.keySet()) {
            if (k.get(1).equals(schema)) {
                dbs.add(k.get(0));
            }
        }
        for (List<String> k : c.schemaTo.keySet()) {
            if (k.get(1).equals(schema)) {
                dbs.add(k.get(0));
            }
        }
        for (List<String> k : c.tableTo.keySet()) {
            if (k.get(1).equals(schema) && k.get(2).equals(table)) {
                dbs.add(k.get(0));
            }
        }
        if (dbs.isEmpty()) {
            return name;
        }
        String db;
        if (dbs.size() == 1) {
            db = dbs.iterator().next();
        } else {
            List<String> owning = dbs.stream()
                    .filter(d -> hasTable(c.ctx, d, schema, table))
                    .toList();
            if (owning.size() != 1) {
                throw new NotImplementedException(
                        "relationalMapper: table '" + name
                        + "' is ambiguous across databases " + dbs);
            }
            db = owning.get(0);
        }
        String nt = c.tableTo.getOrDefault(
                List.of(db, schema, table), table);
        String ns = c.schemaTo.getOrDefault(List.of(db, schema), schema);
        String cat = c.catalog.get(List.of(db, schema));
        return cat != null ? cat + "." + ns + "." + nt
                : "default".equals(ns) ? nt : ns + "." + nt;
    }

    private static boolean hasTable(ModelContext ctx, String dbFqn,
            String schema, String table) {
        // which candidate database OWNS the table: its own tables only
        return ctx.findOwnTableDefinition(dbFqn,
                "default".equals(schema) ? table : schema + "." + table).isPresent();
    }
}
