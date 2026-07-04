package com.legend.sql.dialect;

import com.legend.sql.SqlQuery;

/** The Phase-J seam: render an IR query to this dialect's SQL text. */
public interface SqlDialect {

    String render(SqlQuery query);
}
