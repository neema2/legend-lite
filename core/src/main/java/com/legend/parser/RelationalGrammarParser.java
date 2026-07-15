// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationDefinition.AssociationEndDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.AssociationPropertyMapping;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassDefinition.ConstraintDefinition;
import com.legend.model.ClassDefinition.DerivedPropertyDefinition;
import com.legend.model.ClassDefinition.ParameterDefinition;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;
import com.legend.model.DatabaseDefinition;
import com.legend.model.EnumDefinition;
import com.legend.model.EnumerationMapping;
import com.legend.model.ClassMapping;
import com.legend.model.FilterMapping;
import com.legend.model.FilterPointer;
import com.legend.model.FunctionDefinition;
import com.legend.model.NativeFunctionDefinition;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.MappingDefinition;
import com.legend.model.Realization;
import com.legend.model.MappingInclude;
import com.legend.model.PropertyMapping;
import com.legend.model.spec.PackageableElementPtr;
import com.legend.model.JsonModelConnection;
import com.legend.model.PackageableElement;
import com.legend.model.ComparisonOp;
import com.legend.model.RelationalDataType;
import com.legend.model.JoinChainElement;
import com.legend.model.JoinType;
import com.legend.model.LogicalOp;
import com.legend.model.ProfileDefinition;
import com.legend.model.RelationalOperation;
import com.legend.model.RuntimeDefinition;
import com.legend.model.ServiceDefinition;
import com.legend.model.StereotypeApplication;
import com.legend.model.TaggedValue;
import com.legend.model.spec.ValueSpecification;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Database-declaration and relational-expression sub-grammars, split
 * from ElementParser; shares its token cursor and scope state via {@code p}.
 */
final class RelationalGrammarParser {

    private final ElementParser p;

    RelationalGrammarParser(ElementParser p) {
        this.p = p;
    }

    // ============================================================
    // Database declaration
    // ============================================================

    /**
     * {@code Database <<stereos>> {tags} qualifiedName ( includes? (Schema | Table | View | Join | Filter | MultiGrainFilter)* )}.
     */
    DatabaseDefinition parseDatabase() {
        p.expect(TokenType.DATABASE);
        p.parseStereotypes();   // parity: engine consumes and drops
        p.parseTaggedValues();  // parity: engine consumes and drops
        String qualifiedName = p.parseQualifiedName();
        String dbScope = qualifiedName;
        p.expect(TokenType.PAREN_OPEN);

        List<String> includes = new ArrayList<>();
        List<DatabaseDefinition.SchemaDefinition> schemas = new ArrayList<>();
        List<DatabaseDefinition.TableDefinition> tables = new ArrayList<>();
        List<DatabaseDefinition.ViewDefinition> views = new ArrayList<>();
        List<DatabaseDefinition.JoinDefinition> joins = new ArrayList<>();
        List<DatabaseDefinition.FilterDefinition> filters = new ArrayList<>();
        List<DatabaseDefinition.FilterDefinition> multiGrainFilters = new ArrayList<>();

        while (p.peek() != TokenType.PAREN_CLOSE && !p.atEnd()) {
            if (p.peek() == TokenType.INCLUDE) {
                p.advance();
                includes.add(p.parseQualifiedName());
            } else if (p.peek() == TokenType.SCHEMA) {
                schemas.add(parseDbSchema(dbScope, tables, views));
            } else if (p.peek() == TokenType.TABLE) {
                tables.add(parseDbTable());
            } else if (p.peek() == TokenType.VIEW) {
                views.add(parseDbView(dbScope));
            } else if (p.peek() == TokenType.JOIN) {
                joins.add(parseDbJoin(dbScope));
            } else if (p.peek() == TokenType.FILTER) {
                filters.add(parseDbFilter(dbScope));
            } else if (p.peek() == TokenType.MULTIGRAIN_FILTER) {
                multiGrainFilters.add(parseDbMultiGrainFilter(dbScope));
            } else {
                throw p.error("unknown Database element '" + p.safeText()
                        + "' in '" + qualifiedName + "' (expected include / Schema / Table / View / Join / Filter / MultiGrainFilter)");
            }
        }
        p.expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition(qualifiedName, includes, schemas, tables, views,
                joins, filters, multiGrainFilters);
    }

    /**
     * {@code Schema name ( (Table | View)* )}. Schema tables and views are
     * also mirrored into the database-level flat lists for callers that want
     * a flat view of all tables &mdash; matches engine.
     */
    DatabaseDefinition.SchemaDefinition parseDbSchema(
            String dbScope,
            List<DatabaseDefinition.TableDefinition> flatTables,
            List<DatabaseDefinition.ViewDefinition> flatViews) {
        p.advance(); // "Schema"
        String schemaName = p.parseIdentifier();
        p.expect(TokenType.PAREN_OPEN);
        List<DatabaseDefinition.TableDefinition> schemaTables = new ArrayList<>();
        List<DatabaseDefinition.ViewDefinition> schemaViews = new ArrayList<>();
        while (p.peek() != TokenType.PAREN_CLOSE && !p.atEnd()) {
            if (p.peek() == TokenType.TABLE) {
                DatabaseDefinition.TableDefinition t = parseDbTable();
                schemaTables.add(t);
                flatTables.add(t);
            } else if (p.peek() == TokenType.VIEW) {
                DatabaseDefinition.ViewDefinition v = parseDbView(dbScope);
                schemaViews.add(v);
                flatViews.add(v);
            } else {
                throw p.error("unknown Schema element '" + p.safeText()
                        + "' inside '" + schemaName + "' (expected Table or View)");
            }
        }
        p.expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.SchemaDefinition(schemaName, schemaTables, schemaViews);
    }

    DatabaseDefinition.TableDefinition parseDbTable() {
        p.advance(); // "Table"
        String tableName = parseRelationalIdentifier();
        p.expect(TokenType.PAREN_OPEN);
        // milestoning(business(BUS_FROM=..., BUS_THRU=...),
        // processing(PROCESSING_IN=..., PROCESSING_OUT=...)) — the table's
        // temporal columns, captured for milestoned-fetch filters
        DatabaseDefinition.TableDefinition.Milestoning milestoning = null;
        if (p.isIdentifierToken(p.peek()) && "milestoning".equals(p.text())
                && p.peek(1) == TokenType.PAREN_OPEN) {
            p.advance();
            p.expect(TokenType.PAREN_OPEN);
            DatabaseDefinition.TableDefinition.Milestoning.Business business = null;
            DatabaseDefinition.TableDefinition.Milestoning.Processing processing = null;
            while (p.peek() != TokenType.PAREN_CLOSE && !p.atEnd()) {
                String kind = p.parseIdentifier();
                p.expect(TokenType.PAREN_OPEN);
                java.util.Map<String, String> kv = new java.util.LinkedHashMap<>();
                while (p.peek() != TokenType.PAREN_CLOSE && !p.atEnd()) {
                    String k = p.parseIdentifier();
                    if (p.match(TokenType.EQUAL)) {
                        // values are column names, date literals
                        // (INFINITY_DATE=%9999-12-31...) or booleans
                        // (THRU_IS_INCLUSIVE=true) — ALL are load-bearing
                        // (the engine's %latest filter and range-boundary
                        // operators derive from them); capture verbatim.
                        if (p.isIdentifierToken(p.peek())) {
                            kv.put(k, parseRelationalIdentifier());
                        } else {
                            kv.put(k, p.text());
                            p.advance();
                        }
                    }
                    p.match(TokenType.COMMA);
                }
                p.expect(TokenType.PAREN_CLOSE);
                if (kind.equalsIgnoreCase("business")) {
                    business = new DatabaseDefinition.TableDefinition
                            .Milestoning.Business(
                            kv.get("BUS_FROM"), kv.get("BUS_THRU"),
                            "true".equalsIgnoreCase(kv.get("THRU_IS_INCLUSIVE")),
                            kv.get("BUS_SNAPSHOT_DATE"),
                            kv.get("INFINITY_DATE"));
                } else if (kind.equalsIgnoreCase("processing")) {
                    processing = new DatabaseDefinition.TableDefinition
                            .Milestoning.Processing(
                            kv.get("PROCESSING_IN"), kv.get("PROCESSING_OUT"),
                            "true".equalsIgnoreCase(kv.get("OUT_IS_INCLUSIVE")),
                            kv.get("PROCESSING_SNAPSHOT_DATE"),
                            kv.get("INFINITY_DATE"));
                }
                p.match(TokenType.COMMA);
            }
            p.expect(TokenType.PAREN_CLOSE);
            milestoning = new DatabaseDefinition.TableDefinition.Milestoning(
                    business, processing);
        }
        List<DatabaseDefinition.ColumnDefinition> columns = new ArrayList<>();
        if (p.peek() != TokenType.PAREN_CLOSE) {
            columns.add(parseColumnDefinition());
            while (p.match(TokenType.COMMA)) {
                if (p.peek() == TokenType.PAREN_CLOSE) break;
                columns.add(parseColumnDefinition());
            }
        }
        p.expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.TableDefinition(tableName, columns, milestoning);
    }

    DatabaseDefinition.ColumnDefinition parseColumnDefinition() {
        String columnName = parseRelationalIdentifier();
        RelationalDataType dataType = parseColumnDataType();
        boolean primaryKey = p.match(TokenType.PRIMARY_KEY);
        // PRIMARY KEY implies NOT NULL (engine parity).
        boolean notNull = primaryKey || p.match(TokenType.NOT_NULL);
        return new DatabaseDefinition.ColumnDefinition(columnName, dataType, primaryKey, notNull);
    }

    /**
     * Parse a column data type, dispatching the source identifier (plus
     * optional {@code (size)} or {@code (precision, scale)} arguments) to
     * the appropriate {@link RelationalDataType} record.
     *
     * <p>Sized types ({@code VARCHAR}, {@code CHAR}, {@code BINARY},
     * {@code VARBINARY}) consume one int. Precision/scale types
     * ({@code DECIMAL}, {@code NUMERIC}) consume two. Other types must not
     * carry parens. Unknown identifiers throw per AGENTS.md invariant 4.
     */
    RelationalDataType parseColumnDataType() {
        String name = p.parseIdentifier();
        String upper = name.toUpperCase();
        if (p.peek() == TokenType.PAREN_OPEN) {
            p.advance();
            int first = Integer.parseInt(p.consume(TokenType.INTEGER));
            RelationalDataType sized;
            if (p.match(TokenType.COMMA)) {
                int second = Integer.parseInt(p.consume(TokenType.INTEGER));
                sized = switch (upper) {
                    case "DECIMAL" -> new RelationalDataType.Decimal(first, second);
                    case "NUMERIC" -> new RelationalDataType.Numeric(first, second);
                    default -> throw new IllegalArgumentException(
                            "type '" + name + "' does not take (precision, scale)");
                };
            } else {
                sized = switch (upper) {
                    case "VARCHAR"   -> new RelationalDataType.Varchar(first);
                    case "CHAR"      -> new RelationalDataType.Char_(first);
                    case "BINARY"    -> new RelationalDataType.Binary(first);
                    case "VARBINARY" -> new RelationalDataType.Varbinary(first);
                    // Engine grammar permits DECIMAL(p) with implicit scale=0;
                    // Numeric likewise. Mirror that.
                    case "DECIMAL"   -> new RelationalDataType.Decimal(first, 0);
                    case "NUMERIC"   -> new RelationalDataType.Numeric(first, 0);
                    default -> throw new IllegalArgumentException(
                            "type '" + name + "' does not take a (size) argument");
                };
            }
            p.expect(TokenType.PAREN_CLOSE);
            return sized;
        }
        return RelationalDataType.fromName(name);
    }

    DatabaseDefinition.JoinDefinition parseDbJoin(String dbScope) {
        p.advance(); // "Join"
        String joinName = p.parseIdentifier();
        p.expect(TokenType.PAREN_OPEN);
        RelationalOperation operation = parseDbOperation(dbScope);
        p.expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.JoinDefinition(joinName, operation);
    }

    DatabaseDefinition.FilterDefinition parseDbFilter(String dbScope) {
        p.advance(); // "Filter"
        String filterName = p.parseIdentifier();
        p.expect(TokenType.PAREN_OPEN);
        RelationalOperation condition = parseDbOperation(dbScope);
        p.expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.FilterDefinition(filterName, condition);
    }

    DatabaseDefinition.FilterDefinition parseDbMultiGrainFilter(String dbScope) {
        p.advance(); // "MultiGrainFilter"
        String filterName = p.parseIdentifier();
        p.expect(TokenType.PAREN_OPEN);
        RelationalOperation condition = parseDbOperation(dbScope);
        p.expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.FilterDefinition(filterName, condition);
    }

    DatabaseDefinition.ViewDefinition parseDbView(String dbScope) {
        p.advance(); // "View"
        String viewName = parseRelationalIdentifier();
        p.expect(TokenType.PAREN_OPEN);

        FilterMapping filter = null;
        List<RelationalOperation> groupBy = new ArrayList<>();
        boolean distinct = false;

        while (p.peek() == TokenType.FILTER_CMD
                || p.peek() == TokenType.GROUP_BY_CMD
                || p.peek() == TokenType.DISTINCT_CMD) {
            TokenType cmd = p.peek();
            p.advance();
            if (cmd == TokenType.FILTER_CMD) {
                filter = parseViewFilterClause(dbScope);
            } else if (cmd == TokenType.GROUP_BY_CMD) {
                p.expect(TokenType.PAREN_OPEN);
                if (p.peek() != TokenType.PAREN_CLOSE) {
                    groupBy.add(parseDbOperation(dbScope));
                    while (p.match(TokenType.COMMA)) groupBy.add(parseDbOperation(dbScope));
                }
                p.expect(TokenType.PAREN_CLOSE);
            } else { // DISTINCT_CMD
                distinct = true;
            }
        }

        List<DatabaseDefinition.ViewDefinition.ViewColumnMapping> columnMappings = new ArrayList<>();
        if (p.peek() != TokenType.PAREN_CLOSE) {
            columnMappings.add(parseViewColumnMapping(dbScope));
            while (p.match(TokenType.COMMA)) {
                if (p.peek() == TokenType.PAREN_CLOSE) break;
                columnMappings.add(parseViewColumnMapping(dbScope));
            }
        }
        p.expect(TokenType.PAREN_CLOSE);
        return new DatabaseDefinition.ViewDefinition(viewName, filter, groupBy, distinct, columnMappings);
    }

    /**
     * Parse the body of a view's {@code ~filter ...} clause. Caller has
     * already consumed the {@code ~filter} command token. Four forms:
     * <pre>
     *   ~filter F                                  → Direct(Local("F"))
     *   ~filter [DB] F                             → Direct(Cross("DB", "F"))
     *   ~filter [DB1] @J1 > @J2 | F                → JoinMediated("DB1", [..], Local("F"))
     *   ~filter [DB1] @J1 > @J2 | [DB2] F          → JoinMediated("DB1", [..], Cross("DB2", "F"))
     * </pre>
     *
     * <p>The fork between Direct and JoinMediated happens after seeing the
     * first {@code [DB]} (or the absence thereof): if the next token is
     * {@code @}, we're on the join-mediated path; otherwise it's a direct
     * filter reference.
     */
    FilterMapping parseViewFilterClause(String dbScope) {
        String firstDb = null;
        if (p.peek() == TokenType.BRACKET_OPEN) {
            p.advance();
            firstDb = p.parseQualifiedName();
            p.expect(TokenType.BRACKET_CLOSE);
        }
        // optional JOIN TYPE for the join-mediated form:
        // ~filter [DB] (INNER) @Join | F (engine's FilterMapping joinType)
        String joinType = null;
        if (p.peek() == TokenType.PAREN_OPEN) {
            p.advance();
            joinType = p.parseIdentifier();
            p.expect(TokenType.PAREN_CLOSE);
        }
        if (p.peek() == TokenType.AT) {
            // Join-mediated form: firstDb is the source db (required by grammar)
            if (firstDb == null) {
                throw p.error("Join-mediated filter must start with a [DB] qualifier");
            }
            List<JoinChainElement> joins = parseJoinChain(firstDb);
            p.expect(TokenType.PIPE);
            FilterPointer target = parseFilterPointer();
            return new FilterMapping.JoinMediated(firstDb, joins, target,
                    joinType);
        }
        if (joinType != null) {
            throw p.error("a (" + joinType + ") filter join type requires the"
                    + " join-mediated form: ~filter [DB] (" + joinType
                    + ") @Join | F");
        }
        // Direct form: firstDb (possibly null) qualifies the filter name itself.
        String name = p.parseIdentifier();
        FilterPointer pointer = firstDb == null
                ? new FilterPointer.Local(name)
                : new FilterPointer.Cross(firstDb, name);
        return new FilterMapping.Direct(pointer);
    }

    /**
     * Parse a {@code [DB]?} filter-name pair into a {@link FilterPointer}.
     * Used for the target side of a join-mediated filter (post-pipe).
     */
    FilterPointer parseFilterPointer() {
        String db = null;
        if (p.peek() == TokenType.BRACKET_OPEN) {
            p.advance();
            db = p.parseQualifiedName();
            p.expect(TokenType.BRACKET_CLOSE);
        }
        String name = p.parseIdentifier();
        return db == null ? new FilterPointer.Local(name) : new FilterPointer.Cross(db, name);
    }

    /**
     * Parse {@code @J1 > @J2 > ... > @Jn} into a list of {@link JoinChainElement}.
     * Caller has consumed any leading {@code [DB]}. The {@code initialDb}
     * argument seeds the chain's source database when no per-hop {@code [DB]}
     * is specified.
     */
    /**
     * THE join-chain grammar, engine-uniform (audit M1 found it TRIPLICATED
     * with capability drift):
     * <pre>  (joinType)? @Join ( '&gt;' (joinType)? ('[' db ']')? @Join )*  </pre>
     */
    List<JoinChainElement> parseJoinChain(String defaultDb) {
        List<JoinChainElement> chain = new ArrayList<>();
        JoinType firstJoinType = optionalJoinType();
        p.expect(TokenType.AT);
        chain.add(new JoinChainElement(p.parseIdentifier(), firstJoinType, defaultDb, false));
        parseJoinChainHops(chain, defaultDb);
        return chain;
    }

    /** The {@code ('&gt;' (type)? ([db])? @Join)*} hop loop shared by all chain forms. */
    void parseJoinChainHops(List<JoinChainElement> chain, String defaultDb) {
        while (p.match(TokenType.GREATER_THAN)) {
            JoinType joinType = optionalJoinType();
            String hopDb = defaultDb;
            if (p.peek() == TokenType.BRACKET_OPEN) {
                p.advance();
                hopDb = p.parseQualifiedName();
                p.expect(TokenType.BRACKET_CLOSE);
            }
            p.expect(TokenType.AT);
            chain.add(new JoinChainElement(p.parseIdentifier(), joinType, hopDb, false));
        }
    }

    /** {@code (INNER)}-style parenthesised join type, if present. */
    JoinType optionalJoinType() {
        if (p.peek() != TokenType.PAREN_OPEN) {
            return null;
        }
        p.advance();
        JoinType t = JoinType.fromIdentifier(p.parseIdentifier());
        p.expect(TokenType.PAREN_CLOSE);
        return t;
    }

    DatabaseDefinition.ViewDefinition.ViewColumnMapping parseViewColumnMapping(String dbScope) {
        String colName = p.parseIdentifier();
        String targetSetId = null;
        if (p.peek() == TokenType.BRACKET_OPEN) {
            p.advance();
            targetSetId = p.parseIdentifier();
            p.expect(TokenType.BRACKET_CLOSE);
        }
        p.expect(TokenType.COLON);
        RelationalOperation expression = parseDbOperation(dbScope);
        boolean pk = p.match(TokenType.PRIMARY_KEY);
        return new DatabaseDefinition.ViewDefinition.ViewColumnMapping(
                colName, targetSetId, expression, pk);
    }


    // ============================================================
    // Relational expression sub-grammar
    // ============================================================

    /** {@code dbOperation: dbJoinOperation | dbBooleanOperation}. */
    RelationalOperation parseDbOperation(String dbScope) {
        if (p.peek() == TokenType.AT
                || (p.peek() == TokenType.BRACKET_OPEN && lookAheadIsJoin())) {
            return parseDbJoinOperation(dbScope);
        }
        return parseDbBooleanOperation(dbScope);
    }

    /** {@code dbBooleanOperation: dbAtomicOperation (("and" | "or") dbOperation)?}. */
    RelationalOperation parseDbBooleanOperation(String dbScope) {
        RelationalOperation left = parseDbAtomicOperation(dbScope);
        if (!p.atEnd() && p.isIdentifierToken(p.peek())
                && (p.peek() == TokenType.RELATIONAL_AND || p.peek() == TokenType.RELATIONAL_OR)) {
            LogicalOp op = LogicalOp.fromKeyword(p.text());
            p.advance();
            RelationalOperation right = parseDbOperation(dbScope);
            return new RelationalOperation.BooleanOp(left, op, right);
        }
        return left;
    }

    /**
     * {@code dbAtomicOperation: group | functionCall | columnRef | joinNav
     *                         | targetColumn | literal
     *                         (comparison | IS_NULL | IS_NOT_NULL)?}.
     */
    RelationalOperation parseDbAtomicOperation(String dbScope) {
        RelationalOperation expr;

        if (p.peek() == TokenType.PAREN_OPEN) {
            p.advance();
            expr = new RelationalOperation.Group(parseDbOperation(dbScope));
            p.expect(TokenType.PAREN_CLOSE);
        } else if (p.peek() == TokenType.AT
                || (p.peek() == TokenType.BRACKET_OPEN && lookAheadIsJoin())) {
            expr = parseDbJoinOperation(dbScope);
        } else if (p.peek() == TokenType.BRACKET_OPEN) {
            // '[db]TABLE.COL' or '[db]SCHEMA.TABLE.COL' self-qualified
            // column reference. Required in legend-engine's canonical
            // ~groupBy / ~primaryKey clauses because those clauses are
            // parsed BEFORE ~mainTable (per grammar order), so no
            // enclosing scope is available. Real legend-engine .pure
            // corpora use this form heavily -- e.g.
            //   ~primaryKey([testDatabase]ABC.aName)
            // Without this arm, null-scope clauses cannot carry column
            // references at all, which would reject every real
            // legend-engine mapping that has ~primaryKey or ~groupBy.
            p.advance(); // consume '['
            String db = p.parseQualifiedName();
            p.expect(TokenType.BRACKET_CLOSE);
            String firstId = parseRelationalIdentifier();
            p.expect(TokenType.DOT);
            String second = parseRelationalIdentifier();
            if (p.peek() == TokenType.DOT) {
                p.advance();
                String third = parseRelationalIdentifier();
                // [db]SCHEMA.TABLE.COL
                expr = new RelationalOperation.ColumnRef(db, firstId + "." + second, third);
            } else {
                // [db]TABLE.COL
                expr = new RelationalOperation.ColumnRef(db, firstId, second);
            }
        } else if (p.peek() == TokenType.STRING) {
            expr = RelationalOperation.Literal.string(p.unquoteString(p.text()));
            p.advance();
        } else if (p.peek() == TokenType.INTEGER) {
            expr = RelationalOperation.Literal.integer(Long.parseLong(p.text()));
            p.advance();
        } else if (p.peek() == TokenType.FLOAT || p.peek() == TokenType.DECIMAL) {
            expr = RelationalOperation.Literal.decimal(Double.parseDouble(p.text()));
            p.advance();
        } else if (p.peek() == TokenType.MINUS) {
            // negative literals in relational operations (join conditions:
            // personExtensionTable.ID != -99999999)
            p.advance();
            if (p.peek() == TokenType.INTEGER) {
                expr = RelationalOperation.Literal.integer(-Long.parseLong(p.text()));
            } else if (p.peek() == TokenType.FLOAT || p.peek() == TokenType.DECIMAL) {
                expr = RelationalOperation.Literal.decimal(-Double.parseDouble(p.text()));
            } else {
                throw p.error("expected a numeric literal after '-'");
            }
            p.advance();
        } else if (p.peek() == TokenType.TARGET) {
            p.advance();
            p.expect(TokenType.DOT);
            expr = new RelationalOperation.TargetColumnRef(parseRelationalIdentifier());
        } else {
            String firstId = parseRelationalIdentifier();
            if (p.peek() == TokenType.PAREN_OPEN && !firstId.contains(".")) {
                p.advance(); // '('
                List<RelationalOperation> args = new ArrayList<>();
                if (p.peek() != TokenType.PAREN_CLOSE) {
                    args.add(parseDbFunctionArg(dbScope));
                    while (p.match(TokenType.COMMA)) args.add(parseDbFunctionArg(dbScope));
                }
                p.expect(TokenType.PAREN_CLOSE);
                expr = new RelationalOperation.FunctionCall(firstId, args);
            } else if (p.peek() == TokenType.DOT) {
                p.advance();
                String second = parseRelationalIdentifier();
                // Qualified T.COL: database is ambiguous at parse time
                // (T may live in the enclosing scope's database OR in any
                // of its includes). Leave db null; Phase D resolves it
                // using the enclosing element's scope. Matches engine,
                // which fills in TableAlias.database during binding, not
                // parsing.
                if (p.peek() == TokenType.DOT) {
                    p.advance();
                    String third = parseRelationalIdentifier();
                    expr = new RelationalOperation.ColumnRef(null, firstId + "." + second, third);
                } else {
                    expr = new RelationalOperation.ColumnRef(null, firstId, second);
                }
            } else if (p.currentScopeBlock != null && p.currentScopeBlock.path() != null) {
                // Bare identifier inside a scope BLOCK's expression
                // (scope([db]TRADE)( quantity: sum(QTY) )) — the scoped
                // table's column (engine ScopeInfo parity)
                expr = new RelationalOperation.ColumnRef(
                        p.currentScopeBlock.db(), p.currentScopeBlock.path(), firstId);
            } else if (p.currentMappingScope != null) {
                // Bare identifier in mapping context: unambiguously the
                // class mapping's main table's column. Both table AND
                // database are known at parse time (FINOS engine parity
                // via ScopeInfo). Eager resolution applies only here
                // because this is the ONLY case where parse-time info
                // determines the database without further lookup.
                expr = new RelationalOperation.ColumnRef(
                        p.currentMappingScope.database(),
                        p.currentMappingScope.table(),
                        firstId);
            } else {
                // Database-context bare identifier (Filter / Join /
                // MultiGrainFilter / view filter). FINOS engine rejects this
                // at parse time with this exact message
                // (RelationalParseTreeWalker.generateTableAlias). Match that
                // behavior: no implicit-table column refs in the AST.
                throw p.error("Missing table or alias for column '" + firstId + "'");
            }
        }

        expr = parseArrowChain(expr, dbScope);

        // Optional right side: comparison or null-test.
        if (!p.atEnd()) {
            TokenType next = p.peek();
            if (next == TokenType.EQUAL || next == TokenType.TEST_EQUAL
                    || next == TokenType.TEST_NOT_EQUAL || next == TokenType.LESS_THAN
                    || next == TokenType.GREATER_THAN || next == TokenType.LESS_OR_EQUAL
                    || next == TokenType.GREATER_OR_EQUAL || next == TokenType.NOT_EQUAL) {
                ComparisonOp op = comparisonOpOf(next);
                p.advance();
                RelationalOperation right = parseDbAtomicOperation(dbScope);
                expr = new RelationalOperation.Comparison(expr, op, right);
            } else if (next == TokenType.IS_NULL) {
                p.advance();
                expr = new RelationalOperation.IsNull(expr);
            } else if (next == TokenType.IS_NOT_NULL) {
                p.advance();
                expr = new RelationalOperation.IsNotNull(expr);
            }
        }
        return expr;
    }

    /**
     * Postfix ARROW chain: {@code col->get('k', @String)} is the dynafunction
     * with the receiver as its first argument (engine's method-call spelling
     * in mapping operations).
     */
    RelationalOperation parseArrowChain(RelationalOperation receiver, String dbScope) {
        RelationalOperation expr = receiver;
        while (!p.atEnd() && p.peek() == TokenType.ARROW) {
            p.advance();
            String fn = parseRelationalIdentifier();
            p.expect(TokenType.PAREN_OPEN);
            List<RelationalOperation> chainArgs = new ArrayList<>();
            chainArgs.add(expr);
            if (p.peek() != TokenType.PAREN_CLOSE) {
                chainArgs.add(parseDbFunctionArg(dbScope));
                while (p.match(TokenType.COMMA)) {
                    chainArgs.add(parseDbFunctionArg(dbScope));
                }
            }
            p.expect(TokenType.PAREN_CLOSE);
            expr = new RelationalOperation.FunctionCall(fn, chainArgs);
        }
        return expr;
    }

    RelationalOperation parseDbFunctionArg(String dbScope) {
        // '@Type' in ARGUMENT position is a TYPE REFERENCE (get(col,'k',@String))
        // — a bare join navigation never terminates at ',' or ')'.
        if (p.peek() == TokenType.AT) {
            int save = p.pos;
            p.advance();
            String name = p.parseQualifiedName();
            if (!p.atEnd() && (p.peek() == TokenType.COMMA || p.peek() == TokenType.PAREN_CLOSE)) {
                return new RelationalOperation.TypeRef(name);
            }
            p.pos = save;
        }
        return parseDbFunctionArgTail(dbScope);
    }

    RelationalOperation parseDbFunctionArgTail(String dbScope) {
        // R4.4 prerequisite: distinguish [db]@joinName (a self-qualified
        // JoinNavigation) from [a, b, c] (an array literal). Without the
        // lookahead, every '[' was consumed as array-literal opening and
        // a function argument like concat([db::DB] @J | T.X, ...) failed
        // to parse. Defer to parseDbOperation when the bracket region is
        // a database qualifier on a join chain — or on a COLUMN REF: the
        // dynafunction corpus writes isNull([store::DB] TN1.VAL) with a
        // per-argument database bracket (the atomic grammar's
        // self-qualified arm parses it once we defer).
        if (p.peek() == TokenType.BRACKET_OPEN && !lookAheadIsJoin()
                && !lookAheadIsDbQualifiedColumn()) {
            p.advance();
            List<RelationalOperation> elements = new ArrayList<>();
            if (p.peek() != TokenType.BRACKET_CLOSE) {
                elements.add(parseDbFunctionArg(dbScope));
                while (p.match(TokenType.COMMA)) elements.add(parseDbFunctionArg(dbScope));
            }
            p.expect(TokenType.BRACKET_CLOSE);
            return new RelationalOperation.ArrayLiteral(elements);
        }
        return parseDbOperation(dbScope);
    }

    /**
     * {@code dbJoinOperation: ([DB])? @joinName ( '>' ( '(' joinType ')' )? ([DB])? @joinName )* ( '|' dbBooleanOperation )?}.
     */
    RelationalOperation parseDbJoinOperation(String dbScope) {
        String dbName = null;
        if (p.peek() == TokenType.BRACKET_OPEN) {
            p.advance();
            dbName = p.parseQualifiedName();
            p.expect(TokenType.BRACKET_CLOSE);
        }
        String db = dbName != null ? dbName : dbScope;

        List<JoinChainElement> chain = parseJoinChain(db);

        RelationalOperation terminal = null;
        if (p.match(TokenType.PIPE)) {
            terminal = parseDbBooleanOperation(dbScope);
        }
        return new RelationalOperation.JoinNavigation(dbName, chain, terminal);
    }

    /**
     * Look ahead past a {@code [...]} bracket region and report whether the
     * very next token is {@code @} (i.e. this bracket region is a database
     * qualifier on a join chain, not a generic indexing operator).
     */
    boolean lookAheadIsJoin() {
        int saved = p.pos;
        p.advance(); // skip '['
        while (!p.atEnd() && p.peek() != TokenType.BRACKET_CLOSE) p.advance();
        if (!p.atEnd()) p.advance(); // skip ']'
        boolean isJoin = !p.atEnd() && p.peek() == TokenType.AT;
        p.pos = saved;
        return isJoin;
    }

    /**
     * Lookahead: is this bracket a DATABASE QUALIFIER on a column reference
     * ({@code [store::DB] TN1.VAL}) rather than an array literal? A
     * qualifier bracket contains only a qualified name (identifiers joined
     * by {@code ::} — never dots, commas, or literals) and is followed by
     * an identifier; an array literal's contents or successor break one of
     * those. The {@code @}-successor case is {@link #lookAheadIsJoin}.
     */
    boolean lookAheadIsDbQualifiedColumn() {
        int saved = p.pos;
        p.advance(); // skip '['
        boolean qualifiedName = !p.atEnd() && p.isIdentifierToken(p.peek());
        if (qualifiedName) {
            p.advance();
            while (!p.atEnd() && p.peek() == TokenType.PATH_SEPARATOR) {
                p.advance();
                if (p.atEnd() || !p.isIdentifierToken(p.peek())) {
                    qualifiedName = false;
                    break;
                }
                p.advance();
            }
        }
        boolean isQualifier = qualifiedName && !p.atEnd()
                && p.peek() == TokenType.BRACKET_CLOSE;
        if (isQualifier) {
            p.advance(); // skip ']'
            isQualifier = !p.atEnd() && (p.isIdentifierToken(p.peek())
                    || p.peek() == TokenType.QUOTED_STRING);
        }
        p.pos = saved;
        return isQualifier;
    }

    /**
     * Parse a relational identifier &mdash; either a quoted identifier
     * {@code "T_PERSON"} (returned without the surrounding quotes) or a bare
     * {@link #p.parseIdentifier()}. Quoted identifiers carry case-sensitive
     * names in source-dialect-style relational grammars.
     */
    String parseRelationalIdentifier() {
        if (p.peek() == TokenType.QUOTED_STRING) {
            String quoted = p.text();
            p.advance();
            return quoted.length() >= 2 ? quoted.substring(1, quoted.length() - 1) : quoted;
        }
        return p.parseIdentifier();
    }

    /**
     * Map a lexer token to its canonical comparison op — parse-time
     * knowledge, homed with the grammar (audit 15: this switch was the
     * model records' single lexer dependency).
     */
    private static ComparisonOp comparisonOpOf(TokenType t) {
        return switch (t) {
            case EQUAL, TEST_EQUAL              -> ComparisonOp.EQ;
            case TEST_NOT_EQUAL, NOT_EQUAL      -> ComparisonOp.NEQ;
            case LESS_THAN                      -> ComparisonOp.LT;
            case LESS_OR_EQUAL                  -> ComparisonOp.LTE;
            case GREATER_THAN                   -> ComparisonOp.GT;
            case GREATER_OR_EQUAL               -> ComparisonOp.GTE;
            default -> throw new IllegalArgumentException(
                    "not a comparison token: " + t);
        };
    }
}
