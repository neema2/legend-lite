package com.legend.exec;

/** One result column: the name, an informational SQL type, and the Pure type name. */
public record Column(String name, String sqlType, String pureType) {
}
