package com.gs.legend.sql;

/**
 * Base sealed interface for all SQL AST nodes.
 * 
 * The AST represents the syntactic structure of SQL queries before
 * compilation to the RelationNode IR.
 */
public sealed interface SQLNode
        permits SelectStatement, Expression, SelectItem, FromItem, OrderSpec {
}
