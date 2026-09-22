package com.legend;

import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.model.DatabaseDefinition;
import com.legend.model.MappingDefinition;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The system metamodel store's SEED derivations (SystemMetamodel owns the
 * schema; this owns the rows): per table, the active model context's
 * facts as rows of strings, derived ONCE per graph ({@code exec.
 * SystemDatabase} writes them into the graph's own system database the
 * first time a query reads the metamodel; nothing is seeded per execution)
 * — the builtin package declares the store and must not depend on the
 * compiler's context; exec consumes SQL and result shapes only.
 */
public final class MetamodelSeeds {

    private MetamodelSeeds() {
    }

    /**
     * The seed CONTENT of one system table (charter &sect;5): the active
     * context's facts as rows of strings ({@code null} = an absent
     * optional fact). Pure data: the registry lookups and the row
     * derivations live here; the SQL spelling belongs to the one DDL
     * owner ({@code exec/Ddl.metamodelSeed}), called by the one
     * execution-setup owner. Rows keep the registry's sorted-by-FQN
     * order. An unknown table is a loud bug (the store's DDL and this
     * switch grow together).
     */
    public static List<List<String>> rows(String table, ModelContext ctx) {
        return switch (table) {
            case "classes" -> classes(extent(ctx,
                    com.legend.compiler.element.type.PlatformTypes.CLASS_METACLASS));
            case "mappings" -> mappings(extent(ctx,
                    com.legend.compiler.element.type.PlatformTypes.MAPPING));
            case "mapping_includes_closure" -> includesClosure(ctx);
            case "mapping_includes" -> includes(ctx);
            case "mapping_store_resolutions" -> storeResolutions(ctx);
            case "class_mappings" -> classMappings(ctx);
            case "enumeration_mappings" -> enumerationMappings(ctx, 0);
            case "enum_value_mappings" -> enumerationMappings(ctx, 1);
            case "enum_value_sources" -> enumerationMappings(ctx, 2);
            // THE RelationalOperationElement HIERARCHY AS ONE TABLE (user
            // ruling 2026-09-02): tables, views, columns, main-table
            // aliases and expression nodes are rows of relational_elements
            // with a kind discriminator — the extent is an indexed filtered
            // scan, never a UNION ALL over five tables (H2 could not index
            // it: ten typeInference tests at 9–18s each)
            case "relational_elements" -> relationalElements(ctx);
            case "set_ancestry" -> setAncestry(ctx);
            case "group_by_mappings" -> groupByMappings(ctx);
            case "databases" -> databases(ctx);
            case "schemas" -> schemas(ctx);
            case "properties" -> properties(ctx);
            // the relational-operation TREES (mapping expressions, view
            // column expressions) with the compiler's inferred type stamped
            // per node, the property-mapping and view-column-mapping rows
            // that own them, and every data-type row (columns' declared
            // types + inferred types) — ONE walk, four tables
            case "data_types" -> opSeeds(ctx).dataTypes;
            case "view_column_mappings" -> opSeeds(ctx).viewColumnMappings;
            case "property_mappings" -> opSeeds(ctx).propertyMappings;
            // execution plans are QUERY-LIFETIME facts: their rows ride
            // the query as inline relations ({@code plan.PlanRows}), the
            // graph-lifetime store holds none
            case "plans", "plan_nodes", "plan_template_functions",
                    "plan_function_parameters", "plan_node_closure",
                    "plan_connections", "plan_connection_sqls",
                    // function bodies ride the query too (FunctionBodyRows)
                    "functions", "value_specifications", "vs_primary_key_columns",
                    // lineage trees ride the query too (LineageRows)
                    "relation_trees", "relation_tree_nodes", "relation_tree_node_columns",
                    "column_contexts",
                    // execution activities ride the query too (registerActivityRows)
                    "results", "activities" -> List.of();
            default -> throw new IllegalStateException(
                    "system metamodel table '" + table + "' has no seed"
                    + " derivation — SOURCE and seedRows grow together");
        };
    }

    /** The op-tree walk ONCE per graph (a graph-lifetime derived fact —
     * four tables read it). */
    private static OpSeeds opSeeds(ModelContext ctx) {
        return ctx.derived(OpSeeds.class, OpSeeds::of);
    }

    /** Every relational element of every store, one row layout
     * ({@link com.legend.compiler.element.RelationalOpRows}): tables,
     * views, columns, main-table aliases, then the expression nodes. */
    private static List<List<String>> relationalElements(ModelContext ctx) {
        List<List<String>> rows = new ArrayList<>();
        rows.addAll(tables(ctx));
        rows.addAll(views(ctx));
        rows.addAll(columns(ctx));
        rows.addAll(tableAliases(ctx));
        rows.addAll(opSeeds(ctx).ops);
        return rows;
    }

    private static List<String> extent(ModelContext ctx, String classifier) {
        List<String> fqns = ctx.classifierInstances(classifier);
        return fqns == null ? List.of() : fqns;
    }

    private static List<List<String>> classes(List<String> classFqns) {
        List<List<String>> rows = new ArrayList<>(classFqns.size());
        for (String fqn : classFqns) {
            int cut = fqn.lastIndexOf("::");
            String name = cut < 0 ? fqn : fqn.substring(cut + 2);
            String pkg = cut < 0 ? "" : fqn.substring(0, cut);
            rows.add(List.of(fqn, name, pkg));
        }
        return rows;
    }

    private static List<List<String>> mappings(List<String> mappingFqns) {
        List<List<String>> rows = new ArrayList<>(mappingFqns.size());
        for (String fqn : mappingFqns) {
            int cut = fqn.lastIndexOf("::");
            rows.add(List.of(fqn, cut < 0 ? fqn : fqn.substring(cut + 2)));
        }
        return rows;
    }

    /** The REFLEXIVE-transitive include closure: a mapping sees its own
     * sets and every included mapping's (engine
     * {@code _classMappingByIdRecursive}); bare include paths resolve in
     * the includer's package (the {@code classBindingsWithIncludes}
     * rule); cycle-safe. The third cell is the engine's VISIT ORDER
     * ({@code include_rank}): includes first, depth-first, the viewer
     * itself last — {@code _classMappingByClass} concatenates the
     * includes' sets before its own, and {@code rootClassMappingByClass}
     * takes the LAST root, so the rank is the fact that ordering reads. */
    /** The DIRECT includes (Mapping.includes: MappingInclude[*]), in
     * declaration order — the closure above is the visibility relation. */
    private static List<List<String>> includes(ModelContext ctx) {
        List<List<String>> rows = new ArrayList<>();
        for (String fqn : extent(ctx, com.legend.compiler.element.type.PlatformTypes.MAPPING)) {
            MappingDefinition md = ctx.findMapping(fqn).orElse(null);
            if (md == null) {
                continue;
            }
            int i = 0;
            for (var inc : md.includes()) {
                rows.add(List.of(fqn, resolveIncludePath(ctx, fqn, inc.mappingPath()),
                        Integer.toString(i++)));
            }
        }
        return rows;
    }

    /**
     * STORE SUBSTITUTION as a relation: for every (mapping, database) pair the
     * store the engine's {@code Mapping.resolveStore} answers — a PROJECTION
     * of the compiled fact the normalizer's one include walk recorded
     * ({@code MappingDefinition.resolvedStores}, stamped at Phase E by
     * StoreSubstitutionRewrite.resolveAllStores), the store itself when the chain leaves it alone. Every
     * pair has a row, so the system function reads ONE row, no conditional.
     */
    private static List<List<String>> storeResolutions(ModelContext ctx) {
        List<List<String>> rows = new ArrayList<>();
        List<String> dbs = extent(ctx, com.legend.compiler.element.type.PlatformTypes.DATABASE);
        for (String fqn : extent(ctx, com.legend.compiler.element.type.PlatformTypes.MAPPING)) {
            var resolved = ctx.findMapping(fqn).map(MappingDefinition::resolvedStores)
                    .orElse(java.util.Map.of());
            for (String db : dbs) {
                rows.add(List.of(fqn, db, resolved.getOrDefault(db, db)));
            }
        }
        return rows;
    }

    private static List<List<String>> includesClosure(ModelContext ctx) {
        List<List<String>> rows = new ArrayList<>();
        for (String fqn : extent(ctx, com.legend.compiler.element.type.PlatformTypes.MAPPING)) {
            List<String> order = new ArrayList<>();
            visitIncludes(ctx, fqn, order, new LinkedHashSet<>());
            for (int i = 0; i < order.size(); i++) {
                rows.add(List.of(fqn, order.get(i), Integer.toString(i)));
            }
        }
        return rows;
    }

    /** Post-order include walk: a mapping is listed after every mapping
     * it includes (first visit wins on a diamond). */
    private static void visitIncludes(ModelContext ctx, String cur,
            List<String> order, Set<String> seen) {
        if (!seen.add(cur)) {
            return;
        }
        MappingDefinition md = ctx.findMapping(cur).orElse(null);
        if (md != null) {
            for (var inc : md.includes()) {
                visitIncludes(ctx, resolveIncludePath(ctx, cur, inc.mappingPath()),
                        order, seen);
            }
        }
        order.add(cur);
    }

    private static String resolveIncludePath(ModelContext ctx, String includer,
            String path) {
        if (!path.contains("::") && includer.contains("::")) {
            String inPkg = includer.substring(0, includer.lastIndexOf("::"))
                    + "::" + path;
            if (ctx.findMapping(inPkg).isPresent()) {
                return inPkg;
            }
        }
        return path;
    }

    /** One row per RELATIONAL class binding a mapping DECLARES (included
     * sets ride the closure): the compiler's stamped main table (extends
     * chain already resolved at Phase E). Pure (m2m) bindings are not
     * relational sets — not rows here (grow by witness). A set with no
     * declared id takes the engine's default (the class path with
     * {@code ::} as {@code _}). */
    /** The enumeration mappings of every mapping (m3 EnumerationMapping:
     * name = the declared id, else the enumeration's simple name); level 1
     * = one row per enum value, level 2 = one row per (enum value, source
     * value) — a source value spells as its text (an enum-ref source as
     * its value name). */
    private static List<List<String>> enumerationMappings(ModelContext ctx, int level) {
        List<List<String>> rows = new ArrayList<>();
        for (String fqn : extent(ctx, com.legend.compiler.element.type.PlatformTypes.MAPPING)) {
            MappingDefinition md = ctx.findMapping(fqn).orElse(null);
            if (md == null) {
                continue;
            }
            int ordinal = 0;   // declaration order — Mapping.enumerationMappings is an ORDERED collection
            for (com.legend.model.EnumerationMapping em : md.enumerationMappings()) {
                String name = em.mappingId() != null ? em.mappingId()
                        : em.enumName().substring(em.enumName().lastIndexOf(':') + 1);
                if (level == 0) {
                    rows.add(List.of(fqn, name, em.enumName(), String.valueOf(ordinal++)));
                    continue;
                }
                for (com.legend.model.EnumerationMapping.EnumValueMapping vm : em.valueMappings()) {
                    if (level == 1) {
                        rows.add(List.of(fqn, name, vm.enumValue()));
                        continue;
                    }
                    for (com.legend.model.EnumerationMapping.SourceValue sv : vm.sourceValues()) {
                        String text = switch (sv) {
                            case com.legend.model.EnumerationMapping.SourceValue.StringValue s -> s.value();
                            case com.legend.model.EnumerationMapping.SourceValue.IntegerValue i -> Long.toString(i.value());
                            case com.legend.model.EnumerationMapping.SourceValue.EnumRef r -> r.enumValueName();
                        };
                        rows.add(List.of(fqn, name, vm.enumValue(), text));
                    }
                }
            }
        }
        return rows;
    }

    private static List<List<String>> classMappings(ModelContext ctx) {
        List<List<String>> rows = new ArrayList<>();
        for (String fqn : extent(ctx, com.legend.compiler.element.type.PlatformTypes.MAPPING)) {
            MappingDefinition md = ctx.findMapping(fqn).orElse(null);
            if (md == null) {
                continue;
            }
            for (MappingDefinition.ClassBinding cb : md.classBindings()) {
                if (!(cb instanceof MappingDefinition.ClassBinding.Relational rel)
                        || !(rel.source()
                                instanceof MappingDefinition.RelationalSource.Table t)) {
                    continue;
                }
                String id = com.legend.model.SetId.of(cb);
                // the stamp's documented spelling: a schema-qualified
                // table is 'schema.table', a bare one is in the engine's
                // 'default' schema
                int dot = t.table().indexOf('.');
                String schema = dot < 0 ? "default" : t.table().substring(0, dot);
                String name = dot < 0 ? t.table() : t.table().substring(dot + 1);
                // m3 SetImplementation.root: the '*' set, else the class's
                // SOLE set among the mapping's OWN sets (MappingValidator.
                // validateStar — includes do not count)
                long own = md.classBindings().stream()
                        .filter(b -> b.classFqn().equals(cb.classFqn())).count();
                rows.add(java.util.Arrays.asList(fqn, id, cb.classFqn(),
                        cb.extendsSetId(), t.database(), schema, name,
                        rel.declared().distinct() ? "true" : null,
                        rel.declared().primaryKeyColumns().isEmpty() ? "false" : "true",
                        cb.root() || own == 1 ? "true" : "false"));
            }
        }
        return rows;
    }

    /** The relational sets of every mapping, keyed (mapping, id), with
     * their Phase-E binding — the seed derivations below share it. */
    record SetRow(String mappingFqn, String id,
            MappingDefinition.ClassBinding.Relational binding,
            MappingDefinition.RelationalSource.Table table) {
    }

    private static List<SetRow> relationalSets(ModelContext ctx) {
        List<SetRow> out = new ArrayList<>();
        for (String fqn : extent(ctx, com.legend.compiler.element.type.PlatformTypes.MAPPING)) {
            MappingDefinition md = ctx.findMapping(fqn).orElse(null);
            if (md == null) {
                continue;
            }
            for (MappingDefinition.ClassBinding cb : md.classBindings()) {
                if (cb instanceof MappingDefinition.ClassBinding.Relational rel
                        && rel.source()
                                instanceof MappingDefinition.RelationalSource.Table t) {
                    String id = com.legend.model.SetId.of(cb);
                    out.add(new SetRow(fqn, id, rel, t));
                }
            }
        }
        return out;
    }

    /** The set a set EXTENDS: the super id resolved in the declaring
     * mapping's include closure (the engine's classMappingById walk —
     * own sets first, then the includes in order). */
    private static @com.legend.base.Nullable SetRow superOf(SetRow set,
            List<SetRow> all, List<List<String>> closure) {
        String superId = set.binding().extendsSetId();
        if (superId == null) {
            return null;
        }
        for (List<String> inc : closure) {
            if (!inc.get(0).equals(set.mappingFqn())) {
                continue;
            }
            for (SetRow cand : all) {
                if (cand.mappingFqn().equals(inc.get(1))
                        && cand.id().equals(superId)) {
                    return cand;
                }
            }
        }
        return null;
    }

    /** The REFLEXIVE-transitive extends closure of every relational set
     * with its depth (0 = the set itself): superMapping /
     * allSuperSetImplementations / resolvePrimaryKey read the chain as
     * rows instead of recursing; cycle-safe. */
    private static List<List<String>> setAncestry(ModelContext ctx) {
        List<SetRow> all = relationalSets(ctx);
        List<List<String>> closure = includesClosure(ctx);
        List<List<String>> rows = new ArrayList<>();
        for (SetRow set : all) {
            Set<String> seen = new LinkedHashSet<>();
            SetRow cur = set;
            int depth = 0;
            while (cur != null && seen.add(cur.mappingFqn() + "$" + cur.id())) {
                rows.add(List.of(set.mappingFqn(), set.id(), cur.mappingFqn(),
                        cur.id(), Integer.toString(depth)));
                cur = superOf(cur, all, closure);
                depth++;
            }
        }
        return rows;
    }

    /** One row per set that WROTE ~groupBy (m3: its GroupByMapping). */
    private static List<List<String>> groupByMappings(ModelContext ctx) {
        List<List<String>> rows = new ArrayList<>();
        for (SetRow set : relationalSets(ctx)) {
            if (!set.binding().declared().groupByColumns().isEmpty()) {
                rows.add(List.of(set.mappingFqn(), set.id()));
            }
        }
        return rows;
    }

    /** The set's COMPILED {@code primaryKey} columns (the relational mapping
     * compiler's population rule, engine RelationalInstanceSetImplementation
     * processing): the user's ~primaryKey columns, else the ~groupBy
     * columns, else — ~distinct — every mapped column, else the main
     * table's PRIMARY KEY. One TableAliasColumn node per column, seeded by
     * {@code OpSeeds} as relational-operation rows owned by the set
     * ({@code pk_mapping_fqn} / {@code pk_set_id}). */
    static List<String> primaryKeyColumns(ModelContext ctx, SetRow set) {
        MappingDefinition.ClassBinding.DeclaredKeys b = set.binding().declared();
        if (!b.primaryKeyColumns().isEmpty()) {
            return b.primaryKeyColumns();
        }
        if (!b.groupByColumns().isEmpty()) {
            return b.groupByColumns();
        }
        if (b.distinct()) {
            return b.mappedColumns();
        }
        return tablePrimaryKey(ctx, set.table());
    }

    private static List<String> tablePrimaryKey(ModelContext ctx,
            MappingDefinition.RelationalSource.Table t) {
        DatabaseDefinition db = ctx.findDatabase(t.database()).orElse(null);
        if (db == null) {
            return List.of();
        }
        int dot = t.table().indexOf('.');
        String schema = dot < 0 ? null : t.table().substring(0, dot);
        String name = dot < 0 ? t.table() : t.table().substring(dot + 1);
        List<DatabaseDefinition.TableDefinition> cands = new ArrayList<>();
        if (schema == null) {
            cands.addAll(db.tables());
        }
        for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
            if (schema == null || s.name().equals(schema)) {
                cands.addAll(s.tables());
            }
        }
        List<String> out = new ArrayList<>();
        for (DatabaseDefinition.TableDefinition td : cands) {
            if (td.name().equals(name)) {
                for (DatabaseDefinition.ColumnDefinition c : td.columns()) {
                    if (c.primaryKey()) {
                        out.add(c.name());
                    }
                }
                break;
            }
        }
        return out;
    }

    /** Every view of every store (the {@code tables} schema rule). */
    private static List<List<String>> views(ModelContext ctx) {
        List<List<String>> rows = new ArrayList<>();
        for (String dbFqn : extent(ctx, com.legend.compiler.element.type.PlatformTypes.DATABASE)) {
            DatabaseDefinition db = ctx.findDatabase(dbFqn).orElse(null);
            if (db == null) {
                continue;
            }
            for (SchemaView sv : schemaViews(db)) {
                rows.add(com.legend.compiler.element.RelationalOpRows.viewRow(
                        dbFqn, sv.schema(), sv.view().name()));
            }
        }
        return rows;
    }

    /** {schema, table} the view's columns read, through views of views;
     * null when unresolvable (a non-column column mapping). */
    private static String @com.legend.base.Nullable [] viewBaseTable(DatabaseDefinition db,
            DatabaseDefinition.ViewDefinition v, Set<String> seen) {
        if (!seen.add(v.name())) {
            return null;
        }
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping cm : v.columnMappings()) {
            if (!(cm.expression() instanceof com.legend.model.RelationalOperation.ColumnRef cr)) {
                continue;
            }
            int dot = cr.table().indexOf('.');
            String schema = dot < 0 ? "default" : cr.table().substring(0, dot);
            String name = dot < 0 ? cr.table() : cr.table().substring(dot + 1);
            for (SchemaView sv : schemaViews(db)) {
                if (sv.view().name().equals(name) && sv.schema().equals(schema)) {
                    return viewBaseTable(db, sv.view(), seen);
                }
            }
            return new String[] {schema, name};
        }
        return null;
    }

    /** Every column of every table (the {@code tables} rule for schemas). */
    private static List<List<String>> columns(ModelContext ctx) {
        // every (schema, table) pair ONCE: the default schema's tables are
        // the model's derivation (DatabaseDefinition.defaultSchemaTables),
        // never the flat mirror paired with "default"
        List<List<String>> rows = new ArrayList<>();
        for (String dbFqn : extent(ctx, com.legend.compiler.element.type.PlatformTypes.DATABASE)) {
            DatabaseDefinition db = ctx.findDatabase(dbFqn).orElse(null);
            if (db == null) {
                continue;
            }
            for (SchemaTable st : schemaTables(db)) {
                int ordinal = 0;
                for (DatabaseDefinition.ColumnDefinition c : st.table().columns()) {
                    rows.add(com.legend.compiler.element.RelationalOpRows.columnRow(
                            dbFqn, st.schema(), st.table().name(), c.name(),
                            OpSeeds.columnTypeId(dbFqn, st.schema(), st.table().name(), c.name()),
                            ordinal++));
                }
            }
        }
        return rows;
    }

    /** Every main-table ALIAS in the store: one per relational set (owned
     * by the set: mapping_fqn + id) and one per VIEW (owned by the view:
     * its database + {@code view:<schema>.<name>}, the view identity in
     * the view_* columns) — a view's alias names its base TABLE, resolved
     * transitively through views of views at seed time (the extends-
     * closure pattern: the engine's {@code mainTable()} recurses; here one
     * hop reads the seeded base). */
    private static List<List<String>> tableAliases(ModelContext ctx) {
        List<List<String>> rows = new ArrayList<>();
        for (List<String> cm : classMappings(ctx)) {
            // (mapping_fqn, id, class_fqn, super_set_id, main_db, main_schema, main_table, …)
            String[] base = baseTableOf(ctx, cm.get(4), cm.get(5), cm.get(6));
            List<String> alias = com.legend.compiler.element.RelationalOpRows.aliasRow(
                    cm.get(0), cm.get(1), cm.get(6),
                    mainElementId(ctx, cm.get(4), cm.get(5), cm.get(6)), null,
                    base == null ? null : com.legend.compiler.element.RelationalOpRows
                            .tableId(cm.get(4), base[0], base[1]));
            // TableAlias.database (engine metamodel): the database the
            // mapping REFERENCES for its main table — `[DB1]testTable1`
            // names DB1 even when DB1 only includes the declaring store
            alias.set(3, cm.get(4));
            rows.add(alias);
        }
        for (String dbFqn : extent(ctx, com.legend.compiler.element.type.PlatformTypes.DATABASE)) {
            DatabaseDefinition db = ctx.findDatabase(dbFqn).orElse(null);
            if (db == null) {
                continue;
            }
            for (var sv : schemaViews(db)) {
                String[] base = viewBaseTable(db, sv.view(), new LinkedHashSet<>());
                String viewId = com.legend.compiler.element.RelationalOpRows.viewId(
                        dbFqn, sv.schema(), sv.view().name());
                rows.add(com.legend.compiler.element.RelationalOpRows.aliasRow(dbFqn,
                        "view:" + sv.schema() + "." + sv.view().name(),
                        base == null ? sv.view().name() : base[1],
                        viewId, viewId,
                        base == null ? null : com.legend.compiler.element.RelationalOpRows
                                .tableId(dbFqn, base[0], base[1])));
            }
        }
        return rows;
    }

    private record SchemaView(String schema, DatabaseDefinition.ViewDefinition view) {
    }

    /** The element id of a set's main relation: the VIEW's when the name
     * is a view of the store, else the table's. */
    private static @com.legend.base.Nullable String mainElementId(ModelContext ctx,
            @com.legend.base.Nullable String dbFqn, @com.legend.base.Nullable String schema,
            @com.legend.base.Nullable String name) {
        if (dbFqn == null || schema == null || name == null) {
            return null;
        }
        DatabaseDefinition db = ctx.findDatabase(dbFqn).orElse(null);
        if (db != null) {
            for (SchemaView sv : schemaViews(db)) {
                if (sv.schema().equals(schema) && sv.view().name().equals(name)) {
                    return com.legend.compiler.element.RelationalOpRows.viewId(dbFqn, schema, name);
                }
            }
        }
        return com.legend.compiler.element.RelationalOpRows.tableId(dbFqn, schema, name);
    }

    /** Every view with its schema — top-level views sit in {@code default}. */
    private static List<SchemaView> schemaViews(DatabaseDefinition db) {
        List<SchemaView> out = new ArrayList<>();
        for (DatabaseDefinition.ViewDefinition v : db.defaultSchemaViews()) {
            out.add(new SchemaView(DEFAULT_SCHEMA, v));
        }
        for (DatabaseDefinition.SchemaDefinition sd : db.schemas()) {
            for (DatabaseDefinition.ViewDefinition v : sd.views()) {
                out.add(new SchemaView(sd.name(), v));
            }
        }
        return out;
    }

    /** {schema, table}: the base TABLE behind a main-table name — the
     * table itself, or a view's base resolved through views of views. */
    private static String @com.legend.base.Nullable [] baseTableOf(ModelContext ctx,
            @com.legend.base.Nullable String dbFqn, @com.legend.base.Nullable String schema,
            @com.legend.base.Nullable String name) {
        if (dbFqn == null || schema == null || name == null) {
            return null;
        }
        DatabaseDefinition db = ctx.findDatabase(dbFqn).orElse(null);
        if (db == null) {
            return null;
        }
        for (SchemaView sv : schemaViews(db)) {
            if (sv.schema().equals(schema) && sv.view().name().equals(name)) {
                return viewBaseTable(db, sv.view(), new LinkedHashSet<>());
            }
        }
        return new String[] {schema, name};
    }

    /** Every table of every store: schema-less tables sit in the engine's
     * {@code default} schema. Views are not tables (grow by witness). */
    private static List<List<String>> tables(ModelContext ctx) {
        // keyed: the definition lists a schema's tables under the schema
        // (the default schema's tables are the model's derivation)
        List<List<String>> rows = new ArrayList<>();
        for (String dbFqn : extent(ctx, com.legend.compiler.element.type.PlatformTypes.DATABASE)) {
            DatabaseDefinition db = ctx.findDatabase(dbFqn).orElse(null);
            if (db == null) {
                continue;
            }
            for (SchemaTable st : schemaTables(db)) {
                rows.add(com.legend.compiler.element.RelationalOpRows.tableRow(
                        dbFqn, st.schema(), st.table().name()));
            }
        }
        return rows;
    }

    private record SchemaTable(String schema, DatabaseDefinition.TableDefinition table) {
    }

    /** Every (schema, table) of a store ONCE — default schema first. */
    private static List<SchemaTable> schemaTables(DatabaseDefinition db) {
        List<SchemaTable> out = new ArrayList<>();
        for (DatabaseDefinition.TableDefinition t : db.defaultSchemaTables()) {
            out.add(new SchemaTable(DEFAULT_SCHEMA, t));
        }
        for (DatabaseDefinition.SchemaDefinition sd : db.schemas()) {
            for (DatabaseDefinition.TableDefinition t : sd.tables()) {
                out.add(new SchemaTable(sd.name(), t));
            }
        }
        return out;
    }

    /** Every store as a row (the Database metaclass extent). */
    private static List<List<String>> databases(ModelContext ctx) {
        List<List<String>> rows = new ArrayList<>();
        for (String dbFqn : extent(ctx, com.legend.compiler.element.type.PlatformTypes.DATABASE)) {
            int cut = dbFqn.lastIndexOf("::");
            rows.add(List.of(dbFqn, cut < 0 ? dbFqn : dbFqn.substring(cut + 2)));
        }
        return rows;
    }

    /** Every schema of every store; the engine's {@code default} schema
     * exists when a table or view is declared outside any schema (the
     * parser also lists a schema's tables in the flat list — those do
     * not make a default schema). */
    private static List<List<String>> schemas(ModelContext ctx) {
        List<List<String>> rows = new ArrayList<>();
        for (String dbFqn : extent(ctx, com.legend.compiler.element.type.PlatformTypes.DATABASE)) {
            DatabaseDefinition db = ctx.findDatabase(dbFqn).orElse(null);
            if (db == null) {
                continue;
            }
            if (hasDefaultSchema(db)) {
                rows.add(List.of(dbFqn, "default"));
            }
            for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
                if (!DEFAULT_SCHEMA.equals(s.name()) || !hasDefaultSchema(db)) {
                    rows.add(List.of(dbFqn, s.name()));
                }
            }
        }
        return rows;
    }

    private static final String DEFAULT_SCHEMA = "default";

    static boolean hasDefaultSchema(DatabaseDefinition db) {
        for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
            if (DEFAULT_SCHEMA.equals(s.name())) {
                return true;
            }
        }
        return !db.defaultSchemaTables().isEmpty() || !db.defaultSchemaViews().isEmpty();
    }

    /** Every STORED property as a row keyed by its owner: a class's own
     * declared properties (m3 Class.properties — derived ones are
     * QualifiedProperties, not Property rows), and the ends of every
     * association a property mapping binds (owner = the association;
     * {@code OpSeeds} collects them while it walks the mappings). */
    private static List<List<String>> properties(ModelContext ctx) {
        Set<List<String>> rows = new LinkedHashSet<>();
        for (String fqn : extent(ctx,
                com.legend.compiler.element.type.PlatformTypes.CLASS_METACLASS)) {
            var tc = OpSeeds.classOrNull(ctx, fqn);
            if (tc == null) {
                continue;
            }
            for (var p : tc.properties()) {
                if (p instanceof com.legend.compiler.element.Property.Stored) {
                    rows.add(List.of(fqn, p.name()));
                }
            }
        }
        rows.addAll(OpSeeds.of(ctx).associationProperties);
        return new ArrayList<>(rows);
    }

    /** The relational sets' LEGACY class mappings (property mappings
     * live there; the compiled binding is a lifted function), keyed like
     * {@link SetRow}. */
    static com.legend.model.ClassMapping.@com.legend.base.Nullable Relational legacySet(
            ModelContext ctx, String mappingFqn, String id) {
        var lm = ctx.findLegacyMapping(mappingFqn).orElse(null);
        if (lm == null) {
            return null;
        }
        for (var cm : lm.classMappings()) {
            if (cm instanceof com.legend.model.ClassMapping.Relational r) {
                String rid = com.legend.model.SetId.of(r);
                if (rid.equals(id)) {
                    return r;
                }
            }
        }
        return null;
    }

    /** The extends chain of a set, this set first (depth 0), as the
     * seed's own rows would list it. */
    static List<SetRow> ancestry(ModelContext ctx, SetRow set) {
        List<SetRow> all = relationalSets(ctx);
        List<List<String>> closure = includesClosure(ctx);
        List<SetRow> out = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        SetRow cur = set;
        while (cur != null && seen.add(cur.mappingFqn() + "$" + cur.id())) {
            out.add(cur);
            cur = superOf(cur, all, closure);
        }
        return out;
    }

    static List<SetRow> relationalSetsOf(ModelContext ctx) {
        return relationalSets(ctx);
    }
}
