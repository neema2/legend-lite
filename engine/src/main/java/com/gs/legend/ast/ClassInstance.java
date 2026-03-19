package com.gs.legend.ast;

/**
 * Extensible DSL instance container.
 *
 * <p>
 * In the legend-engine protocol, {@code ClassInstance} is the extension
 * mechanism for DSL-specific constructs that don't warrant their own
 * ValueSpecification subtype. The {@code type} string identifies the construct,
 * and {@code value} carries the DSL-specific data.
 *
 * <p>
 * Examples:
 * <table>
 * <tr>
 * <th>type</th>
 * <th>value class</th>
 * <th>Pure syntax</th>
 * </tr>
 * <tr>
 * <td>"colSpec"</td>
 * <td>ColSpec</td>
 * <td>~name or ~name:fn1:fn2</td>
 * </tr>
 * <tr>
 * <td>"colSpecArray"</td>
 * <td>ColSpecArray</td>
 * <td>~[name1, name2]</td>
 * </tr>
 * <tr>
 * <td>"rootGraphFetchTree"</td>
 * <td>RootGraphFetchTree</td>
 * <td>#{Class{prop1, prop2}}#</td>
 * </tr>
 * <tr>
 * <td>"relation"</td>
 * <td>RelationStoreAccessor</td>
 * <td>#&gt;{store::Db.TABLE}#</td>
 * </tr>
 * </table>
 *
 * @param type  The DSL type identifier
 * @param value The DSL-specific value object
 */
public record ClassInstance(
        String type,
        Object value) implements ValueSpecification {
}
