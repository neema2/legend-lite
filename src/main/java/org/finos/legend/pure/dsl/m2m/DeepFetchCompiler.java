package org.finos.legend.pure.dsl.m2m;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.Join;
import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.engine.store.RelationalMapping;
import org.finos.legend.pure.dsl.PureCompileException;
import org.finos.legend.pure.dsl.graphfetch.GraphFetchTree;

import java.util.ArrayList;
import java.util.List;

/**
 * Compiler for deep fetch queries with nested M2M associations.
 * 
 * Handles the generation of JOINs and nested projections when
 * an M2M property expression references an association via @JoinName.
 * 
 * Example input:
 * 
 * <pre>
 * PersonWithAddress: Pure
 * {
 *     ~src RawPerson
 *     fullName: $src.firstName + ' ' + $src.lastName,
 *     address: @PersonAddress   // Association reference
 * }
 * </pre>
 * 
 * Example output IR:
 * - LEFT JOIN T_RAW_ADDRESS ON condition
 * - Nested projection for Address properties
 */
public final class DeepFetchCompiler {

    private final MappingRegistry mappingRegistry;
    private int aliasCounter = 0;

    public DeepFetchCompiler(MappingRegistry mappingRegistry) {
        this.mappingRegistry = mappingRegistry;
    }

    /**
     * Compiles a deep fetch M2M mapping with association handling.
     * 
     * @param m2mMapping     The M2M mapping for the root class
     * @param graphFetchTree The graphFetch tree specifying which properties to
     *                       include
     * @return A ProjectNode with JOINs for associations
     */
    public ProjectNode compile(M2MClassMapping m2mMapping, GraphFetchTree graphFetchTree) {
        // Get source relational mapping
        String sourceClassName = m2mMapping.sourceClassName();
        RelationalMapping sourceMapping = mappingRegistry.findByClassName(sourceClassName)
                .orElseThrow(() -> new PureCompileException(
                        "No relational mapping for source class: " + sourceClassName));

        String rootAlias = generateAlias();
        TableNode rootTable = new TableNode(sourceMapping.table(), rootAlias);

        // Track joins needed for associations
        List<JoinInfo> joinInfos = new ArrayList<>();

        // Build projections, handling associations specially
        List<Projection> projections = new ArrayList<>();

        for (M2MPropertyMapping pm : m2mMapping.propertyMappings()) {
            // Check if graphFetch tree wants this property
            if (!wantsProperty(graphFetchTree, pm.propertyName())) {
                continue;
            }

            if (pm.expression() instanceof AssociationRef assocRef) {
                // This is a nested association - need JOIN + nested projection
                JoinInfo joinInfo = compileAssociation(
                        assocRef, pm.propertyName(), graphFetchTree, rootAlias, sourceMapping);
                joinInfos.add(joinInfo);
                projections.add(joinInfo.projection());
            } else {
                // Regular scalar property
                M2MCompiler scalarCompiler = new M2MCompiler(sourceMapping, rootAlias);
                Expression expr = scalarCompiler.compileExpression(pm.expression());
                projections.add(new Projection(expr, pm.propertyName()));
            }
        }

        // Apply M2M filter if present
        RelationNode source = rootTable;
        if (m2mMapping.filter() != null) {
            M2MCompiler filterCompiler = new M2MCompiler(sourceMapping, rootAlias);
            Expression filterExpr = filterCompiler.compileExpression(m2mMapping.filter());
            source = new FilterNode(source, filterExpr);
        }

        // Apply joins for associations
        for (JoinInfo joinInfo : joinInfos) {
            source = new JoinNode(
                    source,
                    joinInfo.joinedTable(),
                    joinInfo.joinCondition(),
                    JoinNode.JoinType.LEFT_OUTER);
        }

        return new ProjectNode(source, projections);
    }

    /**
     * Compiles an association reference into JOIN + nested projection info.
     */
    private JoinInfo compileAssociation(
            AssociationRef assocRef,
            String propertyName,
            GraphFetchTree parentTree,
            String sourceAlias,
            RelationalMapping sourceMapping) {

        // Look up the Join definition
        Join join = mappingRegistry.findJoin(assocRef.joinName())
                .orElseThrow(() -> new PureCompileException(
                        "No join found: " + assocRef.joinName()));

        // Determine which table is the "target" (the other side of the join)
        String sourceTableName = sourceMapping.table().name();
        String targetTableName = join.getOtherTable(sourceTableName);

        // Find the relational mapping for the target table
        // TOOD: For now, we look up by table name pattern
        RelationalMapping targetMapping = findMappingForTable(targetTableName);

        String targetAlias = generateAlias();
        TableNode targetTable = new TableNode(targetMapping.table(), targetAlias);

        // Build JOIN condition
        Expression joinCondition = buildJoinCondition(join, sourceAlias, targetAlias, sourceTableName);

        // Get the nested graphFetch tree for this property
        GraphFetchTree nestedTree = getNestedTree(parentTree, propertyName);

        // Find the M2M mapping for the nested class (e.g., Address)
        String targetClassName = getTargetClassName(propertyName, nestedTree);
        M2MClassMapping nestedM2M = mappingRegistry.findM2MMapping(targetClassName)
                .orElseThrow(() -> new PureCompileException(
                        "No M2M mapping for nested class: " + targetClassName));

        // Build the nested projection as a JsonObjectExpression
        List<Projection> nestedProjections = new ArrayList<>();
        M2MCompiler nestedCompiler = new M2MCompiler(targetMapping, targetAlias);

        for (M2MPropertyMapping npm : nestedM2M.propertyMappings()) {
            if (nestedTree != null && wantsProperty(nestedTree, npm.propertyName())) {
                Expression expr = nestedCompiler.compileExpression(npm.expression());
                nestedProjections.add(new Projection(expr, npm.propertyName()));
            }
        }

        // Create a nested json object expression for this association
        Expression nestedExpr = new JsonObjectExpression(nestedProjections);
        Projection projection = new Projection(nestedExpr, propertyName);

        return new JoinInfo(targetTable, joinCondition, projection);
    }

    private Expression buildJoinCondition(Join join, String leftAlias, String rightAlias, String leftTableName) {
        // Determine which side is left and which is right based on the source table
        String leftCol, rightCol;
        if (join.leftTable().equals(leftTableName)) {
            leftCol = join.leftColumn();
            rightCol = join.rightColumn();
        } else {
            leftCol = join.rightColumn();
            rightCol = join.leftColumn();
        }

        return new ComparisonExpression(
                ColumnReference.of(leftAlias, leftCol),
                ComparisonExpression.ComparisonOperator.EQUALS,
                ColumnReference.of(rightAlias, rightCol));
    }

    private boolean wantsProperty(GraphFetchTree tree, String propertyName) {
        if (tree == null)
            return true;
        return tree.propertyNames().contains(propertyName);
    }

    private GraphFetchTree getNestedTree(GraphFetchTree parentTree, String propertyName) {
        if (parentTree == null)
            return null;
        return parentTree.properties().stream()
                .filter(p -> p.name().equals(propertyName))
                .findFirst()
                .map(GraphFetchTree.PropertyFetch::subTree)
                .orElse(null);
    }

    private String getTargetClassName(String propertyName, GraphFetchTree nestedTree) {
        // For now, derive the class name from the nested tree's rootClass
        // or use a naming convention
        if (nestedTree != null && nestedTree.rootClass() != null) {
            return nestedTree.rootClass();
        }
        // Fallback: capitalize property name (e.g., "address" -> "Address")
        return propertyName.substring(0, 1).toUpperCase() + propertyName.substring(1);
    }

    private RelationalMapping findMappingForTable(String tableName) {
        // Try to find by table name pattern
        return mappingRegistry.findByTableName(tableName)
                .or(() -> mappingRegistry.findByTableName("store::RawDatabase." + tableName))
                .orElseThrow(() -> new PureCompileException(
                        "No relational mapping for table: " + tableName));
    }

    private String generateAlias() {
        return "t" + aliasCounter++;
    }

    /**
     * Internal record to hold JOIN information.
     */
    private record JoinInfo(
            TableNode joinedTable,
            Expression joinCondition,
            Projection projection) {
    }
}
