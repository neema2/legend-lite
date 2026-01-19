package org.finos.legend.engine.execution;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A row of values in a query result.
 * 
 * GraalVM native-image compatible.
 */
public record Row(List<Object> values) {

    /**
     * Gets the value at the specified index.
     */
    public Object get(int index) {
        return values.get(index);
    }

    /**
     * Creates a Row from the current position of a ResultSet.
     */
    public static Row fromResultSet(ResultSet rs, int columnCount) throws SQLException {
        List<Object> values = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            values.add(rs.getObject(i));
        }
        return new Row(values);
    }
}
