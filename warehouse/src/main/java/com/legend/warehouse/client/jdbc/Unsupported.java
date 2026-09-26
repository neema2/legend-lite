package com.legend.warehouse.client.jdbc;

import java.sql.SQLFeatureNotSupportedException;

/** A JDBC call the warehouse driver does not implement: said loudly, never answered wrongly. */
final class Unsupported {

    private Unsupported() {
    }

    static SQLFeatureNotSupportedException of(String what) {
        return new SQLFeatureNotSupportedException("the warehouse JDBC driver does not support " + what);
    }
}
