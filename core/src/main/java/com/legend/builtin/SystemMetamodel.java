package com.legend.builtin;

import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;

import java.util.ArrayList;
import java.util.List;

/**
 * THE system metamodel store (METAMODEL_STORE_HANDOFF.md): the metamodel
 * lives IN the database. {@code Class.all()} is an ordinary mapped-class
 * query over {@code metamodel.classes}, seeded from the compiler's own
 * registry ({@code ModelContext.classifierInstances}) &mdash; a real
 * {@code SELECT}, executed by the database, through the EXISTING store
 * lane. The engine's one routing rule ("mapped &rarr; store") then covers
 * metamodel and user classes with ZERO special cases in dispatch.
 *
 * <p><strong>One owner, three surfaces.</strong> (1) The system Database +
 * Mapping (+ the navigation FUNCTIONS over them) are fixed Pure SOURCE,
 * parsed once at class load and injected into every model build
 * ({@code Compiler.buildModel}/{@code buildModule}) &mdash; the resolver
 * sees them exactly as parsed elements, no parallel lane. (2)
 * {@code MetamodelSeeds} renders the graph's facts per table, ONCE per
 * graph, into the graph's own SYSTEM DATABASE ({@code exec.SystemDatabase}
 * — separate from every user connection, read-only after those rows; the
 * executor ROUTES a store-reading body to it). A query's CONSTRUCTED
 * instances ride the query as inline relations (resolver-scoped class
 * sources), never the database. (3) The FQN constants are the exact-FQN identification surface
 * (D1's ambient rule, the executor's table-reference detection).
 *
 * <p><strong>D2 &mdash; one identity for an element value: the FQN.</strong>
 * Every element table keys on the FQN ({@code ~primaryKey}); {@code name}
 * is the print form the mapping binds. A REFERENCE to a tracked element
 * ({@code B1Mapping} in a query) is the row keyed by its FQN (the
 * resolver's element-reference rule, D3).
 *
 * <p><strong>Metamodel-as-relations step 3 (2026-09-02):</strong> the
 * mapping metamodel joins the store &mdash; {@code mappings},
 * {@code class_mappings} (one row per RELATIONAL class mapping, its
 * extends-resolved main table stamped by the compiler),
 * {@code mapping_includes_closure} (the reflexive-transitive include
 * closure as a ROW ENTITY, {@code meta::lite::metamodel::MappingVisibility}: the navigations need "the sets a mapping can
 * see", which is not a recursion the database should re-derive per
 * query) and {@code tables}. The engine's navigation FUNCTIONS
 * ({@code classMappingById}, {@code mainTable}) are Pure bodies over
 * these rows &mdash; the engine bodies are the SPEC, ours read our own
 * compile-time facts; no engine source is carried. Grow BY WITNESS
 * ONLY.
 */
public final class SystemMetamodel {

    private SystemMetamodel() {
    }

    /** The system store element (charter &sect;4). */
    public static final String STORE_FQN =
            "meta::lite::metamodel::MetamodelStore";

    /** The system mapping — D1's ambient execution context supplies it. */
    /** The system store's ORDER column: every ordered child collection
     * (a table's columns, a function's parameters, a key's columns) keeps
     * each row's declaration position here — a positional read
     * ({@code ->at(k)}) over such a navigation is a filter on it. */
    public static final String ORDINAL_COLUMN = "ordinal";

    public static final String MAPPING_FQN =
            "meta::lite::metamodel::MetamodelMapping";

    private static final String S = "[meta::lite::metamodel::MetamodelStore]";
    private static final String INHERITANCE_OP =
            "meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_()";

    /** The SQL data-type kinds the store models — m3 datatype class simple
     * names, one filtered set per kind over {@code data_types} (the engine
     * idiom for a hierarchy over one table); the seed writes the same
     * spelling in {@code dt_kind} ({@code OpSeeds.kindOf}). */
    static final String[] DATA_TYPE_KINDS = {"BigInt", "SmallInt", "TinyInt",
        "Integer", "Float", "Double", "Real", "Bit", "Timestamp", "Date", "Distinct",
        "Other", "SemiStructured", "Varchar", "Char", "Binary", "Varbinary",
        "Decimal", "Numeric", "Array", "Object"};

    /** The relational-operation node kinds the store models: {m3 class
     * simple name, set id, property lines}. */
    static final String[][] OP_KINDS = {
        {"DynaFunction", "opDyna", "name: " + S + " metamodel.relational_elements.dyna_name"},
        {"Literal", "opLit", ""},
        {"LiteralList", "opLitList", ""},
        {"TableAliasColumn", "opTac", "columnName: " + S + " metamodel.relational_elements.col_name,\n"
            + "                    column[col]: " + S + "@OpToColumn"},
        {"RelationalOperationElementWithJoin", "opJoin", ""},
    };

    private static String filters() {
        StringBuilder sb = new StringBuilder();
        for (String k : DATA_TYPE_KINDS) {
            sb.append("                Filter Dt").append(k)
                    .append("(metamodel.data_types.dt_kind = '").append(k).append("')\n");
        }
        // the RelationalOperationElement hierarchy: one filter per kind over
        // the ONE relational_elements table (expression nodes, and the
        // relations — Table, View, Column, TableAlias)
        for (String[] k : OP_KINDS) {
            sb.append("                Filter Op").append(k[0])
                    .append("(metamodel.relational_elements.kind = '").append(k[0]).append("')\n");
        }
        for (String k : ELEMENT_KINDS) {
            sb.append("                Filter El").append(k)
                    .append("(metamodel.relational_elements.kind = '").append(k).append("')\n");
        }
        // the execution-plan node kinds over the ONE plan_nodes table
        for (String[] k : PLAN_NODE_KINDS) {
            sb.append("                Filter Pn").append(k[1])
                    .append("(metamodel.plan_nodes.kind = '").append(k[1]).append("')\n");
        }
        // the plan connection kinds and datasource-specification kinds over
        // the ONE plan_connections table (PlanRows.connectionRows)
        for (String[] k : CONNECTION_KINDS) {
            sb.append("                Filter Pc").append(k[1])
                    .append("(metamodel.plan_connections.kind = '").append(k[1]).append("')\n");
        }
        for (String[] k : DATASOURCE_KINDS) {
            sb.append("                Filter Ds").append(k[1])
                    .append("(metamodel.plan_connections.ds_kind = '").append(k[1]).append("')\n");
        }
        // the expression-tree node kinds over the ONE value_specifications table
        for (String[] k : VS_KINDS) {
            sb.append("                Filter Vs").append(k[1])
                    .append("(metamodel.value_specifications.kind = '").append(k[1]).append("')\n");
        }
        // the execution-activity kinds over the ONE activities table
        for (String[] k : ACTIVITY_KINDS) {
            sb.append("                Filter Act").append(k[1])
                    .append("(metamodel.activities.kind = '").append(k[1]).append("')\n");
        }
        return sb.toString();
    }

    /** The plan node kinds the store models: {class FQN, m3 simple name
     * (= the kind spelling PlanRows writes), set id, own property lines}.
     * A plan's nodes RIDE THE QUERY as inline rows (PlanRows — the
     * executor's plan model turned into facts); the base ExecutionNode is
     * the inheritance operation over these members. */
    static final String[][] PLAN_NODE_KINDS = {
        {"meta::pure::executionPlan::SequenceExecutionNode", "SequenceExecutionNode", "pnSeq", ""},
        {"meta::pure::executionPlan::FunctionParametersValidationNode", "FunctionParametersValidationNode", "pnFpv",
            "functionParameters[planParam]: " + S + "@NodeToFunctionParameters"},
        {"meta::relational::mapping::RelationalInstantiationExecutionNode", "RelationalInstantiationExecutionNode", "pnRel", ""},
        {"meta::relational::mapping::SQLExecutionNode", "SQLExecutionNode", "pnSql",
            "sqlQuery: " + S + " metamodel.plan_nodes.sql_query,\n"
            + "                    sqlComment: " + S + " metamodel.plan_nodes.sql_comment,\n"
            + "                    connection[pcTest]: " + S + "@NodeToConnection,\n"
            + "                    connection[pcRel]: " + S + "@NodeToConnection"},
    };

    /** The plan CONNECTION kinds (the engine's plan carries the runtime
     * connection on every SQLExecutionNode — PlanRows.connectionRows):
     * {class FQN, kind spelling, set id, own property lines}. The base
     * DatabaseConnection is the inheritance operation over them. */
    static final String[][] CONNECTION_KINDS = {
        {"meta::external::store::relational::runtime::TestDatabaseConnection", "TestDatabaseConnection", "pcTest",
            "testDataSetupCsv: " + S + " metamodel.plan_connections.test_data_setup_csv,\n"
            + "                    testDataSetupSqls: " + S + "@ConnToSqls | metamodel.plan_connection_sqls.text"},
        {"meta::external::store::relational::runtime::RelationalDatabaseConnection", "RelationalDatabaseConnection", "pcRel",
            "datasourceSpecification[dsH2]: " + S + "@ConnToDatasource"},
    };

    /** The datasource-specification kinds over the same connection row
     * (ds_kind): {class FQN, kind spelling, set id, own property lines}. */
    static final String[][] DATASOURCE_KINDS = {
        {"meta::pure::alloy::connections::alloy::specification::LocalH2DatasourceSpecification", "LocalH2DatasourceSpecification", "dsH2",
            "testDataSetupCsv: " + S + " metamodel.plan_connections.ds_test_data_setup_csv,\n"
            + "                    testDataSetupSqls: " + S + "@DatasourceToSqls | metamodel.plan_connection_sqls.text"},
    };

    /** The expression-tree node kinds (real m3 ValueSpecification
     * subclasses the rows discriminate on — FunctionBodyRows.kindOf):
     * {class FQN, kind spelling, set id, own property lines}. */
    static final String[][] VS_KINDS = {
        {"meta::pure::metamodel::valuespecification::FunctionExpression", "FunctionExpression", "vsFe",
            "parametersValues[vsFe]: " + S + "@VsToChildren,\n"
            + "                    parametersValues[vsIv]: " + S + "@VsToChildren,\n"
            + "                    parametersValues[vsVe]: " + S + "@VsToChildren"},
        {"meta::pure::metamodel::valuespecification::InstanceValue", "InstanceValue", "vsIv", ""},
        {"meta::pure::metamodel::valuespecification::VariableExpression", "VariableExpression", "vsVe",
            "name: " + S + " metamodel.value_specifications.var_name"},
    };

    /** The execution-activity kinds (real Activity subclasses: the
     * relational activity carries the SQL the platform ran — its own
     * render; the aggregation-aware activity the routed query's print):
     * {class FQN, kind spelling, set id, own property lines}. An
     * execute()'s activities are ROWS under the call's scope
     * (PlanAllocations.registerActivityRows). */
    static final String[][] ACTIVITY_KINDS = {
        {"meta::relational::mapping::RelationalActivity", "RelationalActivity", "actRel",
            "sql: " + S + " metamodel.activities.sql,\n"
            + "                    comment: " + S + " metamodel.activities.comment"},
        {"meta::pure::mapping::aggregationAware::AggregationAwareActivity", "AggregationAwareActivity", "actAgg",
            "rewrittenQuery: " + S + " metamodel.activities.rewritten_query"},
    };

    private static String activityRoutes(String prop, String join) {
        List<String> lines = new ArrayList<>();
        for (String[] k : ACTIVITY_KINDS) {
            lines.add("                    " + prop + "[" + k[2] + "]: " + S + "@" + join);
        }
        return String.join(",\n", lines);
    }

    private static String pkAssocRoutes() {
        List<String> lines = new ArrayList<>();
        for (String[] k : VS_KINDS) {
            lines.add("                        inferredFor[ipk, " + k[2] + "]: " + S + "@NodeToPkColumns");
            lines.add("                        inferredPrimaryKeyColumns[" + k[2] + ", ipk]: " + S + "@NodeToPkColumns");
        }
        return String.join(",\n", lines);
    }

    private static String vsRoutes(String prop, String join) {
        List<String> lines = new ArrayList<>();
        for (String[] k : VS_KINDS) {
            lines.add("                    " + prop + "[" + k[2] + "]: " + S + "@" + join);
        }
        return String.join(",\n", lines);
    }

    private static String planNodeRoutes(String prop, String join) {
        List<String> lines = new ArrayList<>();
        for (String[] k : PLAN_NODE_KINDS) {
            lines.add("                    " + prop + "[" + k[2] + "]: " + S + "@" + join);
        }
        return String.join(",\n", lines);
    }

    private static String planSets() {
        StringBuilder sb = new StringBuilder();
        sb.append("                *meta::pure::executionPlan::ExecutionPlan[plan]: Relational\n")
                .append("                {\n")
                .append("                    ~primaryKey(").append(S).append(" metamodel.plans.id)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.plans\n")
                .append("                    processingTemplateFunctions: ").append(S)
                .append("@PlanToTemplateFunctions | metamodel.plan_template_functions.text,\n")
                .append(planNodeRoutes("rootExecutionNode", "PlanToRoot")).append("\n")
                .append("                }\n")
                .append("                *meta::pure::executionPlan::ExecutionNode: Operation\n")
                .append("                {\n")
                .append("                    ").append(INHERITANCE_OP).append("\n")
                .append("                }\n");
        for (String[] k : PLAN_NODE_KINDS) {
            sb.append("                ").append(k[0]).append("[").append(k[2]).append("]: Relational\n")
                    .append("                {\n")
                    .append("                    ~filter ").append(S).append(" Pn").append(k[1]).append("\n")
                    .append("                    ~primaryKey(").append(S).append(" metamodel.plan_nodes.id)\n")
                    .append("                    ~mainTable ").append(S).append(" metamodel.plan_nodes\n")
                    .append(k[3].isEmpty() ? "" : "                    " + k[3] + ",\n")
                    .append(planNodeRoutes("executionNodes", "NodeToChildren")).append("\n")
                    .append("                }\n");
        }
        // the plan's FunctionParameter rows and the node-tree CLOSURE (the
        // engine's recursive allNodes walk as a row entity: every
        // ancestor/descendant pair, self at depth 0 — a query never
        // recurses over the tree)
        sb.append("                *meta::pure::executionPlan::FunctionParameter[planParam]: Relational\n")
                .append("                {\n")
                .append("                    ~primaryKey(").append(S).append(" metamodel.plan_function_parameters.node_id, ")
                .append(S).append(" metamodel.plan_function_parameters.ordinal)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.plan_function_parameters\n")
                .append("                    name: ").append(S).append(" metamodel.plan_function_parameters.name,\n")
                .append("                    supportsStream: ").append(S).append(" metamodel.plan_function_parameters.supports_stream\n")
                .append("                }\n")
                .append("                *meta::lite::metamodel::PlanNodeClosure[pnc]: Relational\n")
                .append("                {\n")
                .append("                    ~primaryKey(").append(S).append(" metamodel.plan_node_closure.ancestor_id, ")
                .append(S).append(" metamodel.plan_node_closure.node_id)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.plan_node_closure\n")
                .append("                    depth: ").append(S).append(" metamodel.plan_node_closure.depth\n")
                .append("                }\n");
        // the plan connections (SQLExecutionNode.connection) and their
        // datasource specifications: rows of plan_connections under the
        // node's id; the declared bases are inheritance operations
        sb.append("                *meta::external::store::relational::runtime::DatabaseConnection: Operation\n")
                .append("                {\n")
                .append("                    ").append(INHERITANCE_OP).append("\n")
                .append("                }\n");
        for (String[] k : CONNECTION_KINDS) {
            sb.append("                ").append(k[0]).append("[").append(k[2]).append("]: Relational\n")
                    .append("                {\n")
                    .append("                    ~filter ").append(S).append(" Pc").append(k[1]).append("\n")
                    .append("                    ~primaryKey(").append(S).append(" metamodel.plan_connections.node_id)\n")
                    .append("                    ~mainTable ").append(S).append(" metamodel.plan_connections\n")
                    .append("                    ").append(k[3]).append("\n")
                    .append("                }\n");
        }
        sb.append("                *meta::pure::alloy::connections::alloy::specification::DatasourceSpecification: Operation\n")
                .append("                {\n")
                .append("                    ").append(INHERITANCE_OP).append("\n")
                .append("                }\n");
        for (String[] k : DATASOURCE_KINDS) {
            sb.append("                ").append(k[0]).append("[").append(k[2]).append("]: Relational\n")
                    .append("                {\n")
                    .append("                    ~filter ").append(S).append(" Ds").append(k[1]).append("\n")
                    .append("                    ~primaryKey(").append(S).append(" metamodel.plan_connections.node_id)\n")
                    .append("                    ~mainTable ").append(S).append(" metamodel.plan_connections\n")
                    .append("                    ").append(k[3]).append("\n")
                    .append("                }\n");
        }
        List<String> subtrees = new ArrayList<>();
        List<String> closureNodes = new ArrayList<>();
        for (String[] k : PLAN_NODE_KINDS) {
            subtrees.add("                        subtree[" + k[2] + ", pnc]: " + S + "@NodeToSubtree");
            subtrees.add("                        ancestor[pnc, " + k[2] + "]: " + S + "@NodeToSubtree");
            closureNodes.add("                        node[pnc, " + k[2] + "]: " + S + "@SubtreeToNode");
            closureNodes.add("                        closureOf[" + k[2] + ", pnc]: " + S + "@SubtreeToNode");
        }
        // FUNCTION BODIES as rows (group A burn, 2026-09-03): a lambda /
        // function value's expressionSequence is its statements as
        // ValueSpecification rows, each stamped with the compiler's inferred
        // primary key (PkInference — the engine's inferPrimaryKeyColumnNames
        // rules over the typed tree); the rows ride the query (FunctionBodyRows)
        sb.append("                *meta::pure::metamodel::function::FunctionDefinition[fn]: Relational\n")
                .append("                {\n")
                .append("                    ~primaryKey(").append(S).append(" metamodel.functions.id)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.functions\n")
                .append(vsRoutes("expressionSequence", "FunctionToBody")).append("\n")
                .append("                }\n")
                // the expression TREE (group H burn, 2026-09-03): every node
                // is a row discriminated by its m3 kind; parametersValues
                // are the children rows; the node's Multiplicity is the
                // real m3 object shape (Multiplicity -> MultiplicityValue
                // .value) over the same row
                .append("                *meta::pure::metamodel::valuespecification::ValueSpecification: Operation\n")
                .append("                {\n")
                .append("                    ").append(INHERITANCE_OP).append("\n")
                .append("                }\n");
        for (String[] k : VS_KINDS) {
            sb.append("                ").append(k[0]).append("[").append(k[2]).append("]: Relational\n")
                    .append("                {\n")
                    .append("                    ~filter ").append(S).append(" Vs").append(k[1]).append("\n")
                    .append("                    ~primaryKey(").append(S).append(" metamodel.value_specifications.id)\n")
                    .append("                    ~mainTable ").append(S).append(" metamodel.value_specifications\n")
                    .append("                    multiplicity[mult]: ").append(S).append("@VsSelf")
                    .append(k[3].isEmpty() ? "\n" : ",\n                    " + k[3] + "\n")
                    .append("                }\n");
        }
        sb.append("                *meta::pure::metamodel::multiplicity::Multiplicity[mult]: Relational\n")
                .append("                {\n")
                .append("                    ~primaryKey(").append(S).append(" metamodel.value_specifications.id)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.value_specifications\n")
                .append("                    lowerBound[mvLo]: ").append(S).append("@VsSelf,\n")
                .append("                    upperBound[mvHi]: ").append(S).append("@VsSelf\n")
                .append("                }\n")
                .append("                *meta::pure::metamodel::multiplicity::MultiplicityValue[mvLo]: Relational\n")
                .append("                {\n")
                .append("                    ~primaryKey(").append(S).append(" metamodel.value_specifications.id)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.value_specifications\n")
                .append("                    value: ").append(S).append(" metamodel.value_specifications.mult_lower\n")
                .append("                }\n")
                .append("                meta::pure::metamodel::multiplicity::MultiplicityValue[mvHi]: Relational\n")
                .append("                {\n")
                .append("                    ~primaryKey(").append(S).append(" metamodel.value_specifications.id)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.value_specifications\n")
                .append("                    value: ").append(S).append(" metamodel.value_specifications.mult_upper\n")
                .append("                }\n")
                // EXECUTION ACTIVITIES as rows (2026-09-03): an execute()'s
                // Result is a row keyed by the call's scope; its activities
                // are the kinds' rows (Operation set over the one table)
                .append("                *meta::pure::mapping::Result[res]: Relational\n")
                .append("                {\n")
                .append("                    ~primaryKey(").append(S).append(" metamodel.results.id)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.results\n")
                .append(activityRoutes("activities", "ResultToActivities")).append("\n")
                .append("                }\n")
                .append("                *meta::pure::mapping::Activity: Operation\n")
                .append("                {\n")
                .append("                    ").append(INHERITANCE_OP).append("\n")
                .append("                }\n");
        for (String[] k : ACTIVITY_KINDS) {
            sb.append("                ").append(k[0]).append("[").append(k[2]).append("]: Relational\n")
                    .append("                {\n")
                    .append("                    ~filter ").append(S).append(" Act").append(k[1]).append("\n")
                    .append("                    ~primaryKey(").append(S).append(" metamodel.activities.id)\n")
                    .append("                    ~mainTable ").append(S).append(" metamodel.activities\n")
                    .append("                    ").append(k[3]).append("\n")
                    .append("                }\n");
        }
        sb
                .append("                *meta::lite::metamodel::InferredPrimaryKeyColumn[ipk]: Relational\n")
                .append("                {\n")
                .append("                    ~primaryKey(").append(S).append(" metamodel.vs_primary_key_columns.node_id, ")
                .append(S).append(" metamodel.vs_primary_key_columns.ordinal)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.vs_primary_key_columns\n")
                .append("                    ordinal: ").append(S).append(" metamodel.vs_primary_key_columns.ordinal,\n")
                .append("                    name: ").append(S).append(" metamodel.vs_primary_key_columns.name\n")
                .append("                }\n")
                .append("                meta::lite::metamodel::InferredPrimaryKeys: Relational\n")
                .append("                {\n                    AssociationMapping\n                    (\n")
                .append(pkAssocRoutes()).append("\n")
                .append("                    )\n                }\n");
        // LINEAGE TREES as rows (group E burn, 2026-09-03): a scanRelations
        // handle's relation tree is node rows in PREORDER with their depth's
        // indent, kind (root / t / v), name, join label and columns
        // (LineageRows); relationTreeAsString is a Pure body over them
        sb.append("                *meta::pure::lineage::scanRelations::RelationTree[rtree]: Relational\n")
                .append("                {\n")
                .append("                    ~primaryKey(").append(S).append(" metamodel.relation_trees.id)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.relation_trees\n")
                .append("                }\n")
                .append("                *meta::lite::metamodel::RelationTreeNode[rtnode]: Relational\n")
                .append("                {\n")
                // node_id, not id: the aggregated columns hop keys on the NODE
                // row — a key spelled `id` collides with the tree row's `id`
                // in the aggregated-navigation key resolution (a named
                // resolver debt, harness burn-down batch 20)
                .append("                    ~primaryKey(").append(S).append(" metamodel.relation_tree_nodes.node_id)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.relation_tree_nodes\n")
                .append("                    preorder: ").append(S).append(" metamodel.relation_tree_nodes.preorder,\n")
                .append("                    indent: ").append(S).append(" metamodel.relation_tree_nodes.indent,\n")
                .append("                    kind: ").append(S).append(" metamodel.relation_tree_nodes.kind,\n")
                .append("                    name: ").append(S).append(" metamodel.relation_tree_nodes.name,\n")
                .append("                    joinLabel: ").append(S).append(" metamodel.relation_tree_nodes.join_label\n")
                .append("                }\n")
                .append("                *meta::lite::metamodel::RelationTreeColumn[rtcol]: Relational\n")
                .append("                {\n")
                .append("                    ~primaryKey(").append(S).append(" metamodel.relation_tree_node_columns.node_id, ")
                .append(S).append(" metamodel.relation_tree_node_columns.ordinal)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.relation_tree_node_columns\n")
                .append("                    ordinal: ").append(S).append(" metamodel.relation_tree_node_columns.ordinal,\n")
                .append("                    name: ").append(S).append(" metamodel.relation_tree_node_columns.name\n")
                .append("                }\n")
                .append("                meta::lite::metamodel::RelationTreeNodes: Relational\n")
                .append("                {\n                    AssociationMapping\n                    (\n")
                .append("                        tree[rtnode, rtree]: ").append(S).append("@TreeToNodes,\n")
                .append("                        nodes[rtree, rtnode]: ").append(S).append("@TreeToNodes\n")
                .append("                    )\n                }\n")
                .append("                meta::lite::metamodel::RelationTreeNodeColumns: Relational\n")
                .append("                {\n                    AssociationMapping\n                    (\n")
                .append("                        node[rtcol, rtnode]: ").append(S).append("@TreeNodeToColumns,\n")
                .append("                        columns[rtnode, rtcol]: ").append(S).append("@TreeNodeToColumns\n")
                .append("                    )\n                }\n");
        // COLUMN LINEAGE as rows (group I burn, 2026-09-03): a scanColumns
        // handle's ColumnWithContext rows — the columns the lowered plan
        // reads, joined to the store's own Column rows by (db, schema,
        // table, name); the scan id is the handle's key (one handle, many
        // rows — the ~primaryKey names the key the extent filters on)
        sb.append("                *meta::pure::lineage::scanColumns::ColumnWithContext[cwc]: Relational\n")
                .append("                {\n")
                .append("                    ~primaryKey(").append(S).append(" metamodel.column_contexts.scan_id)\n")
                .append("                    ~mainTable ").append(S).append(" metamodel.column_contexts\n")
                .append("                    context: ").append(S).append(" metamodel.column_contexts.context,\n")
                .append("                    column[col]: ").append(S).append("@ColumnContextToColumn\n")
                .append("                }\n");
        sb.append("                meta::lite::metamodel::PlanNodeSubtrees: Relational\n")
                .append("                {\n                    AssociationMapping\n                    (\n")
                .append(String.join(",\n", subtrees)).append("\n")
                .append("                    )\n                }\n")
                .append("                meta::lite::metamodel::PlanNodeClosureNodes: Relational\n")
                .append("                {\n                    AssociationMapping\n                    (\n")
                .append(String.join(",\n", closureNodes)).append("\n")
                .append("                    )\n                }\n");
        return sb.toString();
    }

    /** The relation kinds of the element table (the op-node kinds are
     * {@link #OP_KINDS}); the seed writes the same spelling in
     * {@code kind} ({@code RelationalOpRows}' factories). */
    static final String[] ELEMENT_KINDS = {"Table", "View", "Column", "TableAlias"};

    private static String dataTypeSets() {
        StringBuilder sb = new StringBuilder();
        for (String k : DATA_TYPE_KINDS) {
            boolean sized = k.equals("Varchar") || k.equals("Char") || k.equals("Binary")
                    || k.equals("Varbinary");
            boolean scaled = k.equals("Decimal") || k.equals("Numeric");
            String props = sized
                    ? "\n                    size: " + S + " metamodel.data_types.type_size"
                    : scaled
                    ? "\n                    precision: " + S + " metamodel.data_types.type_precision,"
                        + "\n                    scale: " + S + " metamodel.data_types.type_scale"
                    : "";
            sb.append("                meta::relational::metamodel::datatype::").append(k)
                    .append("[dt").append(k).append("]: Relational\n")
                    .append("                {\n")
                    .append("                    ~filter ").append(S).append(" Dt").append(k).append("\n")
                    .append("                    ~primaryKey(").append(S).append(" metamodel.data_types.id)\n")
                    .append("                    ~mainTable ").append(S).append(" metamodel.data_types")
                    .append(props).append("\n")
                    .append("                }\n");
        }
        return sb.toString();
    }

    private static String opSets() {
        StringBuilder sb = new StringBuilder();
        for (String[] k : OP_KINDS) {
            sb.append("                meta::relational::metamodel::").append(k[0])
                    .append("[").append(k[1]).append("]: Relational\n")
                    .append("                {\n")
                    .append("                    ~filter ").append(S).append(" Op").append(k[0]).append("\n")
                    .append("                    ~primaryKey(").append(S).append(" metamodel.relational_elements.id)\n")
                    .append("                    ~mainTable ").append(S).append(" metamodel.relational_elements")
                    .append(k[2].isEmpty() ? "" : "\n                    " + k[2]).append("\n")
                    .append("                }\n");
        }
        return sb.toString();
    }

    /** {@code prop[opX]: @Join} per operation kind — a property typed
     * RelationalOperationElement routes to every member set. */
    private static String opRoutes(String prop, String join) {
        List<String> lines = new ArrayList<>();
        for (String[] k : OP_KINDS) {
            lines.add("                    " + prop + "[" + k[1] + "]: " + S + "@" + join);
        }
        return String.join(",\n", lines);
    }

    private static String typeRoutes() {
        List<String> lines = new ArrayList<>();
        for (String k : DATA_TYPE_KINDS) {
            lines.add("                    type[dt" + k + "]: " + S + "@ColumnToType");
        }
        return String.join(",\n", lines);
    }

    private static String inferredTypeEnds() {
        List<String> lines = new ArrayList<>();
        for (String[] op : OP_KINDS) {
            for (String k : DATA_TYPE_KINDS) {
                lines.add("                        inferredType[" + op[1] + ", dt" + k + "]: " + S + "@OpToType");
                lines.add("                        inferredTypeOf[dt" + k + ", " + op[1] + "]: " + S + "@OpToType");
            }
        }
        return String.join(",\n", lines);
    }

    /** Schema (charter &sect;4 + step 3); grow BY WITNESS ONLY. */
    private static final String SOURCE = """
            ###Relational
            Database meta::lite::metamodel::MetamodelStore
            (
                Schema metamodel
                (
                    Table classes
                    (
                        fqn VARCHAR(1024) PRIMARY KEY,
                        name VARCHAR(256) NOT NULL,
                        package VARCHAR(1024) NOT NULL
                    )
                    Table mappings
                    (
                        fqn VARCHAR(1024) PRIMARY KEY,
                        name VARCHAR(256) NOT NULL
                    )
                    Table enumeration_mappings
                    (
                        mapping_fqn VARCHAR(1024) PRIMARY KEY,
                        name VARCHAR(256) PRIMARY KEY,
                        enumeration_fqn VARCHAR(1024) NOT NULL,
                        ordinal INTEGER NOT NULL
                    )
                    Table enum_value_mappings
                    (
                        mapping_fqn VARCHAR(1024) PRIMARY KEY,
                        em_name VARCHAR(256) PRIMARY KEY,
                        enum_value VARCHAR(256) PRIMARY KEY
                    )
                    Table enum_value_sources
                    (
                        mapping_fqn VARCHAR(1024) PRIMARY KEY,
                        em_name VARCHAR(256) PRIMARY KEY,
                        enum_value VARCHAR(256) PRIMARY KEY,
                        source_value VARCHAR(1024) PRIMARY KEY
                    )
                    Table mapping_includes_closure
                    (
                        mapping_fqn VARCHAR(1024) PRIMARY KEY,
                        included_fqn VARCHAR(1024) PRIMARY KEY,
                        include_rank INTEGER NOT NULL
                    )
                    Table mapping_includes
                    (
                        mapping_fqn VARCHAR(1024) PRIMARY KEY,
                        included_fqn VARCHAR(1024) PRIMARY KEY,
                        ordinal INTEGER NOT NULL
                    )
                    Table mapping_store_resolutions
                    (
                        mapping_fqn VARCHAR(1024) PRIMARY KEY,
                        original_fqn VARCHAR(1024) PRIMARY KEY,
                        resolved_fqn VARCHAR(1024) NOT NULL
                    )
                    Table class_mappings
                    (
                        mapping_fqn VARCHAR(1024) PRIMARY KEY,
                        id VARCHAR(256) PRIMARY KEY,
                        mapped_class_fqn VARCHAR(1024) NOT NULL,
                        super_set_id VARCHAR(256),
                        main_db VARCHAR(1024),
                        main_schema VARCHAR(256),
                        main_table VARCHAR(256),
                        distinct_set BIT,
                        user_defined_pk BIT NOT NULL,
                        root BIT NOT NULL
                    )
                    Table set_ancestry
                    (
                        mapping_fqn VARCHAR(1024) PRIMARY KEY,
                        id VARCHAR(256) PRIMARY KEY,
                        super_mapping_fqn VARCHAR(1024) PRIMARY KEY,
                        super_id VARCHAR(256) PRIMARY KEY,
                        depth INTEGER NOT NULL
                    )
                    Table group_by_mappings
                    (
                        mapping_fqn VARCHAR(1024) PRIMARY KEY,
                        id VARCHAR(256) PRIMARY KEY
                    )
                    Table databases
                    (
                        fqn VARCHAR(1024) PRIMARY KEY,
                        name VARCHAR(256) NOT NULL
                    )
                    Table schemas
                    (
                        db_fqn VARCHAR(1024) PRIMARY KEY,
                        name VARCHAR(256) PRIMARY KEY
                    )
                    Table properties
                    (
                        owner_fqn VARCHAR(1024) PRIMARY KEY,
                        name VARCHAR(256) PRIMARY KEY
                    )
                    Table data_types
                    (
                        id VARCHAR(2048) PRIMARY KEY,
                        dt_kind VARCHAR(32) NOT NULL,
                        type_size INTEGER,
                        type_precision INTEGER,
                        type_scale INTEGER
                    )
                    Table relational_elements
                    (
                        id VARCHAR(2048) PRIMARY KEY,
                        kind VARCHAR(64) NOT NULL,
                        name VARCHAR(256),
                        db_fqn VARCHAR(1024),
                        schema_name VARCHAR(256),
                        table_name VARCHAR(256),
                        dtype_id VARCHAR(2048),
                        parent_id VARCHAR(2048),
                        ordinal INTEGER,
                        dyna_name VARCHAR(256),
                        literal_value VARCHAR(4000),
                        col_element_id VARCHAR(2048),
                        col_name VARCHAR(256),
                        itype_id VARCHAR(2048),
                        pk_mapping_fqn VARCHAR(1024),
                        pk_set_id VARCHAR(256),
                        mapping_fqn VARCHAR(1024),
                        set_id VARCHAR(256),
                        main_element_id VARCHAR(2048),
                        view_element_id VARCHAR(2048),
                        base_element_id VARCHAR(2048)
                    )
                    Table view_column_mappings
                    (
                        db_fqn VARCHAR(1024) PRIMARY KEY,
                        schema_name VARCHAR(256) PRIMARY KEY,
                        view_name VARCHAR(256) PRIMARY KEY,
                        column_name VARCHAR(256) PRIMARY KEY,
                        op_id VARCHAR(2048) NOT NULL
                    )
                    Table property_mappings
                    (
                        mapping_fqn VARCHAR(1024) PRIMARY KEY,
                        id VARCHAR(256) PRIMARY KEY,
                        ordinal INTEGER PRIMARY KEY,
                        prop_owner_fqn VARCHAR(1024),
                        prop_name VARCHAR(256) NOT NULL,
                        op_id VARCHAR(2048) NOT NULL,
                        declared_depth INTEGER NOT NULL
                    )
                    Table plans
                    (
                        id VARCHAR(256) PRIMARY KEY,
                        root_node_id VARCHAR(512) NOT NULL
                    )
                    Table plan_nodes
                    (
                        id VARCHAR(512) PRIMARY KEY,
                        plan_id VARCHAR(256) NOT NULL,
                        parent_id VARCHAR(512),
                        ordinal INTEGER NOT NULL,
                        kind VARCHAR(64) NOT NULL,
                        sql_query VARCHAR(65535),
                        sql_comment VARCHAR(1024)
                    )
                    Table plan_template_functions
                    (
                        plan_id VARCHAR(256) PRIMARY KEY,
                        ordinal INTEGER PRIMARY KEY,
                        text VARCHAR(65535) NOT NULL
                    )
                    Table plan_function_parameters
                    (
                        node_id VARCHAR(512) PRIMARY KEY,
                        ordinal INTEGER PRIMARY KEY,
                        name VARCHAR(256) NOT NULL,
                        supports_stream BIT
                    )
                    Table plan_node_closure
                    (
                        ancestor_id VARCHAR(512) PRIMARY KEY,
                        node_id VARCHAR(512) PRIMARY KEY,
                        depth INTEGER NOT NULL
                    )
                    Table plan_connections
                    (
                        node_id VARCHAR(512) PRIMARY KEY,
                        kind VARCHAR(64) NOT NULL,
                        db_type VARCHAR(64) NOT NULL,
                        test_data_setup_csv VARCHAR(65535),
                        ds_kind VARCHAR(64),
                        ds_test_data_setup_csv VARCHAR(65535)
                    )
                    Table plan_connection_sqls
                    (
                        node_id VARCHAR(512) PRIMARY KEY,
                        owner VARCHAR(8) PRIMARY KEY,
                        ordinal INTEGER PRIMARY KEY,
                        text VARCHAR(65535) NOT NULL
                    )
                    Table functions
                    (
                        id VARCHAR(512) PRIMARY KEY,
                        name VARCHAR(512) NOT NULL
                    )
                    Table value_specifications
                    (
                        id VARCHAR(512) PRIMARY KEY,
                        function_id VARCHAR(512) NOT NULL,
                        ordinal INTEGER NOT NULL,
                        kind VARCHAR(64) NOT NULL,
                        parent_id VARCHAR(512),
                        depth INTEGER NOT NULL,
                        mult_lower INTEGER NOT NULL,
                        mult_upper INTEGER,
                        var_name VARCHAR(256) NOT NULL
                    )
                    Table vs_primary_key_columns
                    (
                        node_id VARCHAR(512) PRIMARY KEY,
                        ordinal INTEGER PRIMARY KEY,
                        name VARCHAR(256) NOT NULL
                    )
                    Table relation_trees
                    (
                        id VARCHAR(512) PRIMARY KEY
                    )
                    Table relation_tree_nodes
                    (
                        node_id VARCHAR(512) PRIMARY KEY,
                        tree_id VARCHAR(512) NOT NULL,
                        preorder INTEGER NOT NULL,
                        indent VARCHAR(256) NOT NULL,
                        kind VARCHAR(8) NOT NULL,
                        name VARCHAR(512),
                        join_label VARCHAR(512)
                    )
                    Table relation_tree_node_columns
                    (
                        node_id VARCHAR(512) PRIMARY KEY,
                        ordinal INTEGER PRIMARY KEY,
                        name VARCHAR(256) NOT NULL
                    )
                    Table column_contexts
                    (
                        scan_id VARCHAR(512) PRIMARY KEY,
                        ordinal INTEGER PRIMARY KEY,
                        db_fqn VARCHAR(1024) NOT NULL,
                        schema_name VARCHAR(256) NOT NULL,
                        table_name VARCHAR(256) NOT NULL,
                        read_column VARCHAR(256) NOT NULL,
                        context VARCHAR(64) NOT NULL
                    )
                    Table results
                    (
                        id VARCHAR(512) PRIMARY KEY
                    )
                    Table activities
                    (
                        id VARCHAR(512) PRIMARY KEY,
                        result_id VARCHAR(512) NOT NULL,
                        ordinal INTEGER NOT NULL,
                        kind VARCHAR(64) NOT NULL,
                        sql VARCHAR(65535) NOT NULL,
                        comment VARCHAR(1024),
                        rewritten_query VARCHAR(65535) NOT NULL
                    )
                )
                Join ResultToActivities(metamodel.results.id = metamodel.activities.result_id)
                Join PlanToRoot(metamodel.plans.root_node_id = metamodel.plan_nodes.id)
                Join NodeToChildren(metamodel.plan_nodes.id = {target}.parent_id)
                Join PlanToTemplateFunctions(metamodel.plans.id = metamodel.plan_template_functions.plan_id)
                Join NodeToFunctionParameters(metamodel.plan_nodes.id = metamodel.plan_function_parameters.node_id)
                Join NodeToSubtree(metamodel.plan_nodes.id = metamodel.plan_node_closure.ancestor_id)
                Join SubtreeToNode(metamodel.plan_node_closure.node_id = metamodel.plan_nodes.id)
                Join NodeToConnection(metamodel.plan_nodes.id = metamodel.plan_connections.node_id)
                Join ConnToDatasource(metamodel.plan_connections.node_id = {target}.node_id)
                Join ConnToSqls(metamodel.plan_connections.node_id = metamodel.plan_connection_sqls.node_id and metamodel.plan_connection_sqls.owner = 'conn')
                Join DatasourceToSqls(metamodel.plan_connections.node_id = metamodel.plan_connection_sqls.node_id and metamodel.plan_connection_sqls.owner = 'ds')
                Join FunctionToBody(metamodel.functions.id = metamodel.value_specifications.function_id and metamodel.value_specifications.depth = 0)
                Join VsToChildren(metamodel.value_specifications.id = {target}.parent_id)
                Join VsSelf(metamodel.value_specifications.id = {target}.id)
                Join NodeToPkColumns(metamodel.value_specifications.id = metamodel.vs_primary_key_columns.node_id)
                Join TreeToNodes(metamodel.relation_trees.id = metamodel.relation_tree_nodes.tree_id)
                Join TreeNodeToColumns(metamodel.relation_tree_nodes.node_id = metamodel.relation_tree_node_columns.node_id)
                Join ColumnContextToColumn(metamodel.column_contexts.db_fqn = metamodel.relational_elements.db_fqn
                    and metamodel.column_contexts.schema_name = metamodel.relational_elements.schema_name
                    and metamodel.column_contexts.table_name = metamodel.relational_elements.table_name
                    and metamodel.column_contexts.read_column = metamodel.relational_elements.name)
                Join ColumnToOwnerTable(metamodel.relational_elements.db_fqn = {target}.db_fqn
                    and metamodel.relational_elements.schema_name = {target}.schema_name
                    and metamodel.relational_elements.table_name = {target}.name)
                Join MappingsToClosure(metamodel.mappings.fqn = metamodel.mapping_includes_closure.mapping_fqn)
                Join ClosureToVisible(metamodel.mapping_includes_closure.included_fqn = metamodel.mappings.fqn)
                Join MappingsToIncludes(metamodel.mappings.fqn = metamodel.mapping_includes.mapping_fqn)
                Join IncludeToIncluded(metamodel.mapping_includes.included_fqn = metamodel.mappings.fqn)
                Join ClassMappingsToMappings(metamodel.class_mappings.mapping_fqn = metamodel.mappings.fqn)
                Join EnumerationMappingsToMappings(metamodel.enumeration_mappings.mapping_fqn = metamodel.mappings.fqn)
                Join EnumerationMappingToValues(metamodel.enumeration_mappings.mapping_fqn = metamodel.enum_value_mappings.mapping_fqn
                    and metamodel.enumeration_mappings.name = metamodel.enum_value_mappings.em_name)
                Join EnumValueToSources(metamodel.enum_value_mappings.mapping_fqn = metamodel.enum_value_sources.mapping_fqn
                    and metamodel.enum_value_mappings.em_name = metamodel.enum_value_sources.em_name
                    and metamodel.enum_value_mappings.enum_value = metamodel.enum_value_sources.enum_value)
                Join ClosureToClassMappings(metamodel.mapping_includes_closure.included_fqn = metamodel.class_mappings.mapping_fqn)
                Join MappingsToStoreResolutions(metamodel.mappings.fqn = metamodel.mapping_store_resolutions.mapping_fqn)
                Join ResolutionToOriginal(metamodel.mapping_store_resolutions.original_fqn = metamodel.databases.fqn)
                Join ResolutionToResolved(metamodel.mapping_store_resolutions.resolved_fqn = metamodel.databases.fqn)
                Join AliasToDatabase(metamodel.relational_elements.db_fqn = metamodel.databases.fqn)
                Join ClassMappingsToAlias(metamodel.class_mappings.mapping_fqn = metamodel.relational_elements.mapping_fqn and metamodel.class_mappings.id = metamodel.relational_elements.set_id)
                Join AliasToTables(metamodel.relational_elements.main_element_id = {target}.id)
                Join AliasToViews(metamodel.relational_elements.main_element_id = {target}.id)
                Join AliasToBaseTable(metamodel.relational_elements.base_element_id = {target}.id)
                Join ViewToAlias(metamodel.relational_elements.id = {target}.view_element_id)
                Join SetToAncestry(metamodel.class_mappings.mapping_fqn = metamodel.set_ancestry.mapping_fqn
                    and metamodel.class_mappings.id = metamodel.set_ancestry.id)
                Join AncestryToAncestor(metamodel.set_ancestry.super_mapping_fqn = metamodel.class_mappings.mapping_fqn
                    and metamodel.set_ancestry.super_id = metamodel.class_mappings.id)
                Join SetToGroupBy(metamodel.class_mappings.mapping_fqn = metamodel.group_by_mappings.mapping_fqn
                    and metamodel.class_mappings.id = metamodel.group_by_mappings.id)
                Join SetToPrimaryKeyOps(metamodel.class_mappings.mapping_fqn = metamodel.relational_elements.pk_mapping_fqn
                    and metamodel.class_mappings.id = metamodel.relational_elements.pk_set_id)
                Join DbToSchemas(metamodel.databases.fqn = metamodel.schemas.db_fqn)
                Join SchemaToViews(metamodel.schemas.db_fqn = metamodel.relational_elements.db_fqn
                    and metamodel.schemas.name = metamodel.relational_elements.schema_name)
                Join SchemaToTables(metamodel.schemas.db_fqn = metamodel.relational_elements.db_fqn
                    and metamodel.schemas.name = metamodel.relational_elements.schema_name)
                Join TableToColumns(metamodel.relational_elements.db_fqn = {target}.db_fqn
                    and metamodel.relational_elements.schema_name = {target}.schema_name
                    and metamodel.relational_elements.name = {target}.table_name)
                Join ViewToColumnMappings(metamodel.relational_elements.db_fqn = metamodel.view_column_mappings.db_fqn
                    and metamodel.relational_elements.schema_name = metamodel.view_column_mappings.schema_name
                    and metamodel.relational_elements.name = metamodel.view_column_mappings.view_name)
                Join ColumnMappingToOp(metamodel.view_column_mappings.op_id = metamodel.relational_elements.id)
                Join ClassMappingsToClass(metamodel.class_mappings.mapped_class_fqn = metamodel.classes.fqn)
                Join SetToPropertyMappings(metamodel.class_mappings.mapping_fqn = metamodel.property_mappings.mapping_fqn
                    and metamodel.class_mappings.id = metamodel.property_mappings.id)
                Join PropertyMappingToOp(metamodel.property_mappings.op_id = metamodel.relational_elements.id)
                Join PropertyMappingToProperty(metamodel.property_mappings.prop_owner_fqn = metamodel.properties.owner_fqn
                    and metamodel.property_mappings.prop_name = metamodel.properties.name)
                Join ColumnToType(metamodel.relational_elements.dtype_id = metamodel.data_types.id)
                Join OpToType(metamodel.relational_elements.itype_id = metamodel.data_types.id)
                Join OpToColumn(metamodel.relational_elements.col_element_id = {target}.id)
            %3$s
            )

            ###Pure
            Class meta::lite::metamodel::MappingVisibility
            {
                includeRank: Integer[1];
            }

            Association meta::lite::metamodel::MappingVisibilities
            {
                viewer: meta::pure::mapping::Mapping[1];
                visibility: meta::lite::metamodel::MappingVisibility[*];
            }

            Association meta::lite::metamodel::VisibleMappings
            {
                visible: meta::pure::mapping::Mapping[1];
                visibleFrom: meta::lite::metamodel::MappingVisibility[*];
            }

            Association meta::lite::metamodel::VisibleSets
            {
                visibilityOf: meta::lite::metamodel::MappingVisibility[*];
                visibleSets: meta::relational::mapping::RootRelationalInstanceSetImplementation[*];
            }

            Class meta::lite::metamodel::StoreResolution
            {
            }

            Association meta::lite::metamodel::MappingStoreResolutions
            {
                resolver: meta::pure::mapping::Mapping[1];
                storeResolutions: meta::lite::metamodel::StoreResolution[*];
            }

            Association meta::lite::metamodel::StoreResolutionOriginal
            {
                original: meta::pure::store::Store[1];
                originalOf: meta::lite::metamodel::StoreResolution[*];
            }

            Association meta::lite::metamodel::StoreResolutionResolved
            {
                resolved: meta::pure::store::Store[1];
                resolvedOf: meta::lite::metamodel::StoreResolution[*];
            }

            Association meta::lite::metamodel::AliasBaseTables
            {
                base: meta::relational::metamodel::relation::Table[1];
                baseOf: meta::relational::metamodel::TableAlias[*];
            }

            Class meta::lite::metamodel::PlanNodeClosure
            {
                depth: Integer[1];
            }

            Association meta::lite::metamodel::PlanNodeSubtrees
            {
                ancestor: meta::pure::executionPlan::ExecutionNode[1];
                subtree: meta::lite::metamodel::PlanNodeClosure[*];
            }

            Association meta::lite::metamodel::PlanNodeClosureNodes
            {
                node: meta::pure::executionPlan::ExecutionNode[1];
                closureOf: meta::lite::metamodel::PlanNodeClosure[*];
            }

            Class meta::lite::metamodel::InferredPrimaryKeyColumn
            {
                ordinal: Integer[1];
                name: String[1];
            }

            Association meta::lite::metamodel::InferredPrimaryKeys
            {
                inferredFor: meta::pure::metamodel::valuespecification::ValueSpecification[1];
                inferredPrimaryKeyColumns: meta::lite::metamodel::InferredPrimaryKeyColumn[*];
            }

            Class meta::lite::metamodel::RelationTreeNode
            {
                preorder: Integer[1];
                indent: String[1];
                kind: String[1];
                name: String[0..1];
                joinLabel: String[0..1];
            }

            Class meta::lite::metamodel::RelationTreeColumn
            {
                ordinal: Integer[1];
                name: String[1];
            }

            Association meta::lite::metamodel::RelationTreeNodes
            {
                tree: meta::pure::lineage::scanRelations::RelationTree[1];
                nodes: meta::lite::metamodel::RelationTreeNode[*];
            }

            Association meta::lite::metamodel::RelationTreeNodeColumns
            {
                node: meta::lite::metamodel::RelationTreeNode[1];
                columns: meta::lite::metamodel::RelationTreeColumn[*];
            }

            Class meta::lite::metamodel::SetAncestry
            {
                depth: Integer[1];
            }

            Association meta::lite::metamodel::SetAncestries
            {
                descendant: meta::relational::mapping::RootRelationalInstanceSetImplementation[1];
                ancestry: meta::lite::metamodel::SetAncestry[*];
            }

            Class meta::lite::metamodel::EnumSourceValue
            {
                value: String[1];
            }

            Association meta::lite::metamodel::EnumValueSources
            {
                sources: meta::lite::metamodel::EnumSourceValue[*];
                ofValueMapping: meta::pure::mapping::EnumValueMapping[1];
            }

            Association meta::lite::metamodel::SetAncestors
            {
                ancestor: meta::relational::mapping::RootRelationalInstanceSetImplementation[1];
                ancestryOf: meta::lite::metamodel::SetAncestry[*];
            }

            Association meta::lite::metamodel::InferredTypes
            {
                inferredType: meta::relational::metamodel::datatype::DataType[0..1];
                inferredTypeOf: meta::relational::metamodel::RelationalOperationElement[*];
            }

            Association meta::lite::metamodel::EffectivePropertyMappings
            {
                effectivePropertyMappings: meta::relational::mapping::RelationalPropertyMapping[*];
                allPropertyMappingsOf: meta::relational::mapping::RootRelationalInstanceSetImplementation[1];
            }

            function meta::pure::mapping::_classMappingByClass(_this:meta::pure::mapping::Mapping[1], class:meta::pure::metamodel::type::Class<meta::pure::metamodel::type::Any>[1]):meta::pure::mapping::SetImplementation[*]
            {
                $_this.visibility->sortBy(v|$v.includeRank).visible.classMappings->filter(cm|$cm.class == $class)
            }

            function meta::pure::mapping::rootClassMappingByClass(_this:meta::pure::mapping::Mapping[1], class:meta::pure::metamodel::type::Class<meta::pure::metamodel::type::Any>[1]):meta::pure::mapping::SetImplementation[0..1]
            {
                $_this->meta::pure::mapping::_classMappingByClass($class)->filter(s|$s.root == true)->last()
            }

            function meta::pure::mapping::enumerationMappingByName(_this:meta::pure::mapping::Mapping[1], name:String[1]):meta::pure::mapping::EnumerationMapping[0..1]
            {
                $_this.visibility->sortBy(v|$v.includeRank).visible.enumerationMappings->filter(em|$em.name == $name)->first()
            }

            function meta::pure::mapping::toDomainValue(_this:meta::pure::mapping::EnumerationMapping[1], sourceValue:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::Any[1]
            {
                $_this.enumValueMappings->filter(m|$m.sources.value->contains($sourceValue))->toOne().enum
            }

            function meta::relational::metamodel::schema(_this:meta::relational::metamodel::Database[1], name:String[1]):meta::relational::metamodel::Schema[0..1]
            {
                $_this.schemas->filter(s|$s.name == $name)->first()
            }

            function meta::relational::metamodel::table(_this:meta::relational::metamodel::Schema[1], name:String[1]):meta::relational::metamodel::relation::Table[0..1]
            {
                $_this.tables->filter(t|$t.name == $name)->first()
            }

            function meta::relational::metamodel::view(_this:meta::relational::metamodel::Schema[1], name:String[1]):meta::relational::metamodel::relation::View[0..1]
            {
                $_this.views->filter(t|$t.name == $name)->first()
            }

            function meta::relational::metamodel::column(_this:meta::relational::metamodel::relation::Table[1], columnName:String[1]):meta::relational::metamodel::Column[0..1]
            {
                $_this.columns->cast(@meta::relational::metamodel::Column)->filter(c|$c.name == $columnName)->first()
            }

            function meta::relational::metamodel::children(_this:meta::relational::metamodel::join::RelationalTreeNode[1]):meta::relational::metamodel::join::JoinTreeNode[*]
            {
                $_this.childrenData->cast(@meta::relational::metamodel::join::JoinTreeNode)
            }

            function meta::relational::metamodel::childByJoinName(_this:meta::relational::metamodel::join::RelationalTreeNode[1], s:String[1]):meta::relational::metamodel::join::JoinTreeNode[0..1]
            {
                $_this->children()->filter(c|$c.join.name == $s)->first()
            }

            function meta::pure::mapping::allPropertyMappings(_this:meta::pure::mapping::PropertyMappingsImplementation[1]):meta::pure::mapping::PropertyMapping[*]
            {
                $_this->cast(@meta::relational::mapping::RootRelationalInstanceSetImplementation).effectivePropertyMappings
            }

            function meta::pure::mapping::_propertyMappingsByPropertyName(i:meta::pure::mapping::PropertyMappingsImplementation[1], s:String[1]):meta::pure::mapping::PropertyMapping[*]
            {
                $i->meta::pure::mapping::allPropertyMappings()->filter(pm|$pm.property.name == $s)
            }

            function meta::pure::mapping::propertyMappingsByPropertyName(_this:meta::pure::mapping::PropertyMappingsImplementation[1], s:String[1]):meta::pure::mapping::PropertyMapping[*]
            {
                $_this->meta::pure::mapping::allPropertyMappings()->filter(pm|$pm.property.name == $s)
            }

            function meta::pure::mapping::propertyMappingsByPropertyName(i:meta::pure::mapping::InstanceSetImplementation[1], propertyName:String[1]):meta::pure::mapping::PropertyMapping[*]
            {
                $i->meta::pure::mapping::allPropertyMappings()->filter(pm|$pm.property.name == $propertyName)
            }

            function meta::relational::functions::typeInference::inferRelationalType(rop:meta::relational::metamodel::RelationalOperationElement[1]):meta::relational::metamodel::datatype::DataType[0..1]
            {
                $rop.inferredType
            }

            function meta::relational::metamodel::datatype::dataTypeToSqlText(type:meta::relational::metamodel::datatype::DataType[1]):String[1]
            {
                $type->match([
                    i : meta::relational::metamodel::datatype::Integer[1] | 'INT',
                    f : meta::relational::metamodel::datatype::Float[1] | 'FLOAT',
                    v : meta::relational::metamodel::datatype::Varchar[1] | format('VARCHAR(%%d)', $v.size),
                    c : meta::relational::metamodel::datatype::Char[1] | format('CHAR(%%d)', $c.size),
                    d : meta::relational::metamodel::datatype::Decimal[1] | format('DECIMAL(%%d, %%d)', [$d.precision, $d.scale]),
                    t : meta::relational::metamodel::datatype::Timestamp[1] | 'TIMESTAMP',
                    d : meta::relational::metamodel::datatype::Date[1] | 'DATE',
                    b : meta::relational::metamodel::datatype::BigInt[1] | 'BIGINT',
                    s : meta::relational::metamodel::datatype::SmallInt[1] | 'SMALLINT',
                    t : meta::relational::metamodel::datatype::TinyInt[1] | 'TINYINT',
                    d : meta::relational::metamodel::datatype::Double[1] | 'DOUBLE',
                    n : meta::relational::metamodel::datatype::Numeric[1] | format('NUMERIC(%%d, %%d)', [$n.precision, $n.scale]),
                    d : meta::relational::metamodel::datatype::Distinct[1] | 'DISTINCT',
                    o : meta::relational::metamodel::datatype::Other[1] | 'OTHER',
                    b : meta::relational::metamodel::datatype::Bit[1] | 'BIT',
                    b : meta::relational::metamodel::datatype::Binary[1] | format('BINARY(%%d)', $b.size),
                    r : meta::relational::metamodel::datatype::Real[1] | 'REAL',
                    a : meta::relational::metamodel::datatype::Array[1] | 'ARRAY',
                    v : meta::relational::metamodel::datatype::Varbinary[1] | format('VARBINARY(%%d)', $v.size),
                    s : meta::relational::metamodel::datatype::SemiStructured[1] | 'SEMISTRUCTURED',
                    o : meta::relational::metamodel::datatype::Object[1] | 'OBJECT'
                ])
            }

            function meta::pure::executionPlan::allNodes(node:meta::pure::executionPlan::ExecutionNode[1], extensions:meta::pure::extension::Extension[*]):meta::pure::executionPlan::ExecutionNode[*]
            {
                $node.subtree.node
            }
            function meta::pure::functions::meta::getLowerBound(multiplicity:meta::pure::metamodel::multiplicity::Multiplicity[1]):Integer[1]
            {
                $multiplicity.lowerBound->toOne().value->toOne()
            }
            function meta::pure::extension::routerExtensions(_this:meta::pure::extension::Extension[1]):meta::pure::router::extension::RouterExtension[*]
            {
                $_this.availableStores->concatenate($_this.availableFeatures)->cast(@meta::pure::router::extension::RouterExtension)
            }
            function meta::relational::functions::pureToSqlQuery::expressionSequenceReturnsAtLeastToOneDataType(v:meta::pure::metamodel::valuespecification::ValueSpecification[1]):Boolean[1]
            {
                $v.multiplicity->toOne()->meta::pure::functions::meta::getLowerBound() >= 1
            }
            function meta::relational::mapping::inferPrimaryKeyColumnNames(vs:meta::pure::metamodel::valuespecification::ValueSpecification[1]):String[*]
            {
                $vs.inferredPrimaryKeyColumns->sortBy(c|$c.ordinal).name
            }
            function meta::pure::lineage::scanRelations::relationTreeAsString(t:meta::pure::lineage::scanRelations::RelationTree[1]):String[1]
            {
                $t->meta::pure::lineage::scanRelations::relationTreeAsString(true)
            }
            function meta::pure::lineage::scanRelations::relationTreeAsString(t:meta::pure::lineage::scanRelations::RelationTree[1], withJoin:Boolean[1]):String[1]
            {
                $t->meta::lite::lineage::relationTreeLines($withJoin)->joinStrings('', '\n', '\n')
            }
            function meta::lite::lineage::relationTreeLines(t:meta::pure::lineage::scanRelations::RelationTree[1], withJoin:Boolean[1]):String[*]
            {
                $t.nodes->sortBy(n|$n.preorder)->map(n|if($n.kind == 'root', |$n.indent + 'root', |$n.indent + '------> (' + $n.kind + ') ' + $n.name->toOne() + if($withJoin && $n.joinLabel->isNotEmpty(), |'(' + $n.joinLabel->toOne() + ')', |'') + ' [' + $n.columns->sortBy(c|$c.ordinal).name->joinStrings(', ') + ']'))
            }
            function meta::pure::mapping::resolveStore(_this:meta::pure::mapping::Mapping[1], store:meta::pure::store::Store[1]):meta::pure::store::Store[1]
            {
                let substitute = $_this.storeResolutions->filter(r|$r.original == $store).resolved;
                if($substitute->isEmpty(), |$store, |$substitute->toOne());
            }
            function meta::relational::runtime::extractDBs(m:meta::pure::mapping::Mapping[1]):meta::relational::metamodel::Database[*]
            {
                $m.visibility.visible.classMappings->filter(cm|$cm->instanceOf(meta::relational::mapping::RootRelationalInstanceSetImplementation))->cast(@meta::relational::mapping::RootRelationalInstanceSetImplementation).mainTableAlias.database->removeDuplicates()
            }
            function meta::pure::mapping::classMappings(_this:meta::pure::mapping::Mapping[1]):meta::pure::mapping::SetImplementation[*]
            {
                $_this.visibility->sortBy(v|$v.includeRank).visible.classMappings
            }
            function meta::pure::mapping::classMappingById(_this:meta::pure::mapping::Mapping[1], id:String[1]):meta::pure::mapping::SetImplementation[0..1]
            {
                $_this.visibility.visible.classMappings->filter(cm|$cm.id == $id)->first()
            }

            function meta::relational::metamodel::mainTable(_this:meta::relational::metamodel::RelationalMappingSpecification[1]):meta::relational::metamodel::relation::Table[1]
            {
                $_this.mainTableAlias.base
            }

            function meta::pure::mapping::superMapping(_this:meta::pure::mapping::PropertyMappingsImplementation[1]):meta::pure::mapping::SetImplementation[0..1]
            {
                $_this->cast(@meta::relational::mapping::RootRelationalInstanceSetImplementation).ancestry->filter(a|$a.depth == 1).ancestor->first()
            }

            function meta::pure::mapping::allSuperSetImplementations(set:meta::pure::mapping::PropertyMappingsImplementation[1], m:meta::pure::mapping::Mapping[1]):meta::pure::mapping::PropertyMappingsImplementation[*]
            {
                $set->cast(@meta::relational::mapping::RootRelationalInstanceSetImplementation).ancestry->filter(a|$a.depth > 0).ancestor
            }

            function meta::relational::mapping::resolvePrimaryKey(_this:meta::relational::mapping::RootRelationalInstanceSetImplementation[1]):meta::relational::metamodel::RelationalOperationElement[*]
            {
                $_this.ancestry->filter(a|$a.depth == 0 || !$a.ancestor.groupBy->isEmpty() || $a.ancestor.distinct == true || $a.ancestor.userDefinedPrimaryKey == true)->sortBy(a|if(!$a.ancestor.groupBy->isEmpty(), |0, |if($a.ancestor.distinct == true, |1000, |if($a.ancestor.userDefinedPrimaryKey == true, |2000, |3000))) + $a.depth)->first().ancestor.primaryKey
            }

            ###Mapping
            Mapping meta::lite::metamodel::MetamodelMapping
            (
                *meta::pure::metamodel::type::Class[cls]: Relational
                {
                    ~primaryKey(%1$s metamodel.classes.fqn)
                    ~mainTable %1$s metamodel.classes
                    name: %1$s metamodel.classes.name
                }
                *meta::pure::mapping::Mapping[mapping]: Relational
                {
                    ~primaryKey(%1$s metamodel.mappings.fqn)
                    ~mainTable %1$s metamodel.mappings
                    name: %1$s metamodel.mappings.name,
                    classMappings[rootRel]: %1$s@ClassMappingsToMappings,
                    enumerationMappings[enumMap]: %1$s@EnumerationMappingsToMappings,
                    includes[mappingInclude]: %1$s@MappingsToIncludes
                }
                *meta::pure::mapping::MappingInclude[mappingInclude]: Relational
                {
                    ~primaryKey(%1$s metamodel.mapping_includes.mapping_fqn, %1$s metamodel.mapping_includes.included_fqn)
                    ~mainTable %1$s metamodel.mapping_includes
                    owner[mapping]: %1$s@MappingsToIncludes,
                    included[mapping]: %1$s@IncludeToIncluded
                }
                *meta::pure::mapping::EnumerationMapping[enumMap]: Relational
                {
                    ~primaryKey(%1$s metamodel.enumeration_mappings.mapping_fqn, %1$s metamodel.enumeration_mappings.name)
                    ~mainTable %1$s metamodel.enumeration_mappings
                    name: %1$s metamodel.enumeration_mappings.name,
                    parent[mapping]: %1$s@EnumerationMappingsToMappings,
                    enumValueMappings[evm]: %1$s@EnumerationMappingToValues
                }
                *meta::pure::mapping::EnumValueMapping[evm]: Relational
                {
                    ~primaryKey(%1$s metamodel.enum_value_mappings.mapping_fqn, %1$s metamodel.enum_value_mappings.em_name, %1$s metamodel.enum_value_mappings.enum_value)
                    ~mainTable %1$s metamodel.enum_value_mappings
                    enum: %1$s metamodel.enum_value_mappings.enum_value
                }
                *meta::lite::metamodel::EnumSourceValue[esv]: Relational
                {
                    ~primaryKey(%1$s metamodel.enum_value_sources.mapping_fqn, %1$s metamodel.enum_value_sources.em_name, %1$s metamodel.enum_value_sources.enum_value, %1$s metamodel.enum_value_sources.source_value)
                    ~mainTable %1$s metamodel.enum_value_sources
                    value: %1$s metamodel.enum_value_sources.source_value
                }
                *meta::lite::metamodel::MappingVisibility[vis]: Relational
                {
                    ~primaryKey(%1$s metamodel.mapping_includes_closure.mapping_fqn, %1$s metamodel.mapping_includes_closure.included_fqn)
                    ~mainTable %1$s metamodel.mapping_includes_closure
                    includeRank: %1$s metamodel.mapping_includes_closure.include_rank
                }
                *meta::lite::metamodel::StoreResolution[sres]: Relational
                {
                    ~primaryKey(%1$s metamodel.mapping_store_resolutions.mapping_fqn, %1$s metamodel.mapping_store_resolutions.original_fqn)
                    ~mainTable %1$s metamodel.mapping_store_resolutions
                }
                *meta::pure::mapping::SetImplementation: Operation
                {
                    %2$s
                }
                meta::relational::mapping::RootRelationalInstanceSetImplementation[rootRel]: Relational
                {
                    ~primaryKey(%1$s metamodel.class_mappings.mapping_fqn, %1$s metamodel.class_mappings.id)
                    ~mainTable %1$s metamodel.class_mappings
                    id: %1$s metamodel.class_mappings.id,
                    root: %1$s metamodel.class_mappings.root,
                    class[cls]: %1$s@ClassMappingsToClass,
                    superSetImplementationId: %1$s metamodel.class_mappings.super_set_id,
                    distinct: %1$s metamodel.class_mappings.distinct_set,
                    userDefinedPrimaryKey: %1$s metamodel.class_mappings.user_defined_pk,
                    parent: %1$s@ClassMappingsToMappings,
                    mainTableAlias: %1$s@ClassMappingsToAlias,
                    groupBy[gbm]: %1$s@SetToGroupBy,
                    primaryKey[opTac]: %1$s@SetToPrimaryKeyOps
                }
                meta::relational::mapping::GroupByMapping[gbm]: Relational
                {
                    ~primaryKey(%1$s metamodel.group_by_mappings.mapping_fqn, %1$s metamodel.group_by_mappings.id)
                    ~mainTable %1$s metamodel.group_by_mappings
                }
                meta::lite::metamodel::SetAncestry[anc]: Relational
                {
                    ~primaryKey(%1$s metamodel.set_ancestry.mapping_fqn, %1$s metamodel.set_ancestry.id, %1$s metamodel.set_ancestry.super_mapping_fqn, %1$s metamodel.set_ancestry.super_id)
                    ~mainTable %1$s metamodel.set_ancestry
                    depth: %1$s metamodel.set_ancestry.depth
                }
                meta::relational::metamodel::TableAlias[alias]: Relational
                {
                    ~filter %1$s ElTableAlias
                    ~primaryKey(%1$s metamodel.relational_elements.id)
                    ~mainTable %1$s metamodel.relational_elements
                    name: %1$s metamodel.relational_elements.name,
                    database[db]: %1$s@AliasToDatabase,
                    relationalElement[tbl]: %1$s@AliasToTables,
                    relationalElement[vw]: %1$s@AliasToViews
                }
                meta::relational::metamodel::relation::View[vw]: Relational
                {
                    ~filter %1$s ElView
                    ~primaryKey(%1$s metamodel.relational_elements.id)
                    ~mainTable %1$s metamodel.relational_elements
                    name: %1$s metamodel.relational_elements.name,
                    mainTableAlias[alias]: %1$s@ViewToAlias,
                    columnMappings[vcm]: %1$s@ViewToColumnMappings
                }
                *meta::relational::metamodel::Database[db]: Relational
                {
                    ~primaryKey(%1$s metamodel.databases.fqn)
                    ~mainTable %1$s metamodel.databases
                    name: %1$s metamodel.databases.name,
                    schemas[schema]: %1$s@DbToSchemas
                }
                *meta::relational::metamodel::Schema[schema]: Relational
                {
                    ~primaryKey(%1$s metamodel.schemas.db_fqn, %1$s metamodel.schemas.name)
                    ~mainTable %1$s metamodel.schemas
                    name: %1$s metamodel.schemas.name,
                    tables[tbl]: %1$s@SchemaToTables,
                    views[vw]: %1$s@SchemaToViews
                }
                *meta::relational::mapping::ColumnMapping[vcm]: Relational
                {
                    ~primaryKey(%1$s metamodel.view_column_mappings.db_fqn, %1$s metamodel.view_column_mappings.schema_name, %1$s metamodel.view_column_mappings.view_name, %1$s metamodel.view_column_mappings.column_name)
                    ~mainTable %1$s metamodel.view_column_mappings
                    columnName: %1$s metamodel.view_column_mappings.column_name,
            %5$s
                }
                *meta::pure::metamodel::function::property::Property[prop]: Relational
                {
                    ~primaryKey(%1$s metamodel.properties.owner_fqn, %1$s metamodel.properties.name)
                    ~mainTable %1$s metamodel.properties
                    name: %1$s metamodel.properties.name
                }
                *meta::pure::mapping::PropertyMapping: Operation
                {
                    %2$s
                }
                meta::relational::mapping::RelationalPropertyMapping[rpm]: Relational
                {
                    ~primaryKey(%1$s metamodel.property_mappings.mapping_fqn, %1$s metamodel.property_mappings.id, %1$s metamodel.property_mappings.ordinal)
                    ~mainTable %1$s metamodel.property_mappings
                    property[prop]: %1$s@PropertyMappingToProperty,
            %6$s
                }
                *meta::relational::metamodel::datatype::DataType: Operation
                {
                    %2$s
                }
            %4$s
            %10$s
                *meta::relational::metamodel::RelationalOperationElement: Operation
                {
                    %2$s
                }
                meta::relational::metamodel::relation::Table[tbl]: Relational
                {
                    ~filter %1$s ElTable
                    ~primaryKey(%1$s metamodel.relational_elements.id)
                    ~mainTable %1$s metamodel.relational_elements
                    name: %1$s metamodel.relational_elements.name,
                    schema[schema]: %1$s@SchemaToTables,
                    columns[col]: %1$s@TableToColumns
                }
                meta::relational::metamodel::Column[col]: Relational
                {
                    ~filter %1$s ElColumn
                    ~primaryKey(%1$s metamodel.relational_elements.id)
                    ~mainTable %1$s metamodel.relational_elements
                    name: %1$s metamodel.relational_elements.name,
                    owner[tbl]: %1$s@ColumnToOwnerTable,
            %7$s
                }
            %8$s
                meta::lite::metamodel::InferredTypes: Relational
                {
                    AssociationMapping
                    (
            %9$s
                    )
                }
                meta::lite::metamodel::EffectivePropertyMappings: Relational
                {
                    AssociationMapping
                    (
                        effectivePropertyMappings[rootRel, rpm]: %1$s@SetToPropertyMappings,
                        allPropertyMappingsOf[rpm, rootRel]: %1$s@SetToPropertyMappings
                    )
                }
                meta::lite::metamodel::AliasBaseTables: Relational
                {
                    AssociationMapping
                    (
                        base[alias, tbl]: %1$s@AliasToBaseTable,
                        baseOf[tbl, alias]: %1$s@AliasToBaseTable
                    )
                }
                meta::lite::metamodel::SetAncestries: Relational
                {
                    AssociationMapping
                    (
                        descendant[anc, rootRel]: %1$s@SetToAncestry,
                        ancestry[rootRel, anc]: %1$s@SetToAncestry
                    )
                }
                meta::lite::metamodel::EnumValueSources: Relational
                {
                    AssociationMapping
                    (
                        sources[evm, esv]: %1$s@EnumValueToSources,
                        ofValueMapping[esv, evm]: %1$s@EnumValueToSources
                    )
                }
                meta::lite::metamodel::SetAncestors: Relational
                {
                    AssociationMapping
                    (
                        ancestor[anc, rootRel]: %1$s@AncestryToAncestor,
                        ancestryOf[rootRel, anc]: %1$s@AncestryToAncestor
                    )
                }
                meta::lite::metamodel::MappingVisibilities: Relational
                {
                    AssociationMapping
                    (
                        viewer[vis, mapping]: %1$s@MappingsToClosure,
                        visibility[mapping, vis]: %1$s@MappingsToClosure
                    )
                }
                meta::lite::metamodel::VisibleMappings: Relational
                {
                    AssociationMapping
                    (
                        visible[vis, mapping]: %1$s@ClosureToVisible,
                        visibleFrom[mapping, vis]: %1$s@ClosureToVisible
                    )
                }
                meta::lite::metamodel::VisibleSets: Relational
                {
                    AssociationMapping
                    (
                        visibilityOf[rootRel, vis]: %1$s@ClosureToClassMappings,
                        visibleSets[vis, rootRel]: %1$s@ClosureToClassMappings
                    )
                }
                meta::lite::metamodel::MappingStoreResolutions: Relational
                {
                    AssociationMapping
                    (
                        resolver[sres, mapping]: %1$s@MappingsToStoreResolutions,
                        storeResolutions[mapping, sres]: %1$s@MappingsToStoreResolutions
                    )
                }
                meta::lite::metamodel::StoreResolutionOriginal: Relational
                {
                    AssociationMapping
                    (
                        original[sres, db]: %1$s@ResolutionToOriginal,
                        originalOf[db, sres]: %1$s@ResolutionToOriginal
                    )
                }
                meta::lite::metamodel::StoreResolutionResolved: Relational
                {
                    AssociationMapping
                    (
                        resolved[sres, db]: %1$s@ResolutionToResolved,
                        resolvedOf[db, sres]: %1$s@ResolutionToResolved
                    )
                }
            )
            """.formatted(S, INHERITANCE_OP, filters(), dataTypeSets(),
                    opRoutes("relationalOperationElement", "ColumnMappingToOp"),
                    opRoutes("relationalOperationElement", "PropertyMappingToOp"),
                    typeRoutes(), opSets(), inferredTypeEnds(), planSets());

    /** A model element shadows a system element of the same qualified
     * name — for FUNCTIONS only when the parameter types agree too: a
     * same-name function over other parameter types is an OVERLOAD (the
     * engine's own {@code resolvePrimaryKey(rsi:RelationalInstanceSet
     * Implementation)} beside the root-set body), never a shadow. */
    private static boolean shadows(PackageableElement e, PackageableElement sys) {
        if (!e.qualifiedName().equals(sys.qualifiedName())) {
            return false;
        }
        if (e instanceof com.legend.model.FunctionDefinition f
                && sys instanceof com.legend.model.FunctionDefinition g) {
            if (f.parameters().size() != g.parameters().size()) {
                return false;
            }
            for (int i = 0; i < f.parameters().size(); i++) {
                if (!spelling(f.parameters().get(i).type()).equals(
                        spelling(g.parameters().get(i).type()))) {
                    return false;
                }
            }
        }
        return true;
    }

    /** A parameter type's spelling without source positions (a resolved
     * model type carries its parse position; the system source carries
     * none — the record text never compared equal). */
    private static String spelling(com.legend.protocol.TypeExpression t) {
        return switch (t) {
            case com.legend.protocol.TypeExpression.NameRef nr -> nr.name();
            case com.legend.protocol.TypeExpression.Generic g -> g.name() + "<"
                    + g.arguments().stream().map(SystemMetamodel::spelling)
                            .collect(java.util.stream.Collectors.joining(",")) + ">";
            default -> String.valueOf(t);
        };
    }

    /** The system Pure source (tests inspect it; never edited at run time). */
    public static String source() {
        return SOURCE;
    }

    /** Parsed once at class load — fails loudly if the source rots
     * (the {@code Pure} native-catalog discipline). */
    private static final List<PackageableElement> ELEMENTS =
            com.legend.parser.ElementParser.parse(SOURCE,
                    com.legend.parser.Dialect.LEGEND_LITE).elements();

    /** The system elements as parsed — THE BOOT LAYER's input (user
     * ruling 2026-09-02: compiled once per process, content-addressed by
     * the source's hash, entered into every graph's index exactly like
     * the graph's own elements, protected as system). */
    public static List<PackageableElement> elements() {
        return ELEMENTS;
    }

    /** Every system element's FQN (functions included) — the boot layer's
     * contribution to a graph's name-resolution universe. */
    public static java.util.Set<String> elementFqns() {
        return Fqns.ALL;
    }

    private static final class Fqns {
        static final java.util.Set<String> ALL = ELEMENTS.stream()
                .map(PackageableElement::qualifiedName)
                .collect(java.util.stream.Collectors.collectingAndThen(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new), java.util.Collections::unmodifiableSet));
    }

    /** Whether {@code fqn} names a system ELEMENT (class, association,
     * store, mapping — not a function body): reserved, never redefined. */
    public static boolean isSystemElement(String fqn) {
        for (PackageableElement el : ELEMENTS) {
            if (!(el instanceof com.legend.model.FunctionDefinition)
                    && el.qualifiedName().equals(fqn)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The ONE injection seam's parsed-level half: the model with every
     * same-signature system FUNCTION shadow removed (a system function
     * body is the platform's implementation of a real engine function; a
     * same-signature function in the model is the ENGINE'S OWN SOURCE
     * riding in the corpus universe — spec material, never our runtime,
     * user-ratified 2026-08-18). A model element redefining a system
     * ELEMENT is an error: the system layer is protected.
     */
    public static ParsedModel withoutSystemShadows(ParsedModel parsed) {
        List<PackageableElement> kept = new ArrayList<>(parsed.elements());
        for (PackageableElement el : ELEMENTS) {
            if (el instanceof com.legend.model.FunctionDefinition) {
                kept.removeIf(e -> shadows(e, el));
                continue;
            }
            for (PackageableElement e : parsed.elements()) {
                if (shadows(e, el)) {
                    throw new com.legend.error.ModelException(
                            com.legend.error.LegendCompileException.Phase.NORMALIZE,
                            "'" + el.qualifiedName() + "' is a system element of the"
                            + " platform's metamodel layer and cannot be redefined",
                            el.qualifiedName());
                }
            }
        }
        return kept.size() == parsed.elements().size() ? parsed
                : new ParsedModel(kept, parsed.imports(), parsed.source(),
                        parsed.elementOffsets(), parsed.elementImports(),
                        parsed.elementSources(), parsed.unclaimedSections());
    }
}
