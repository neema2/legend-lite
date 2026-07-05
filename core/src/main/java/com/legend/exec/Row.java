package com.legend.exec;

import java.util.List;

/** One result row; cells are raw JDBC objects (the result's Type carries semantics). */
public record Row(List<Object> values) {

    public Object get(int i) {
        return values.get(i);
    }
}
