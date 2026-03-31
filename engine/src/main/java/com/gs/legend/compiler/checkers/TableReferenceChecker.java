package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.CString;
import com.gs.legend.compiler.*;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.store.Table;
import com.gs.legend.plan.GenericType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Checker for {@code tableReference(CString)}.
 *
 * <p>Resolves a table reference string (e.g., "db.T") to a physical table,
 * builds a Relation schema from the table's columns, and stamps TypeInfo.
 *
 * <p>Parser emits: {@code tableReference(CString("db.T"))}
 */
public class TableReferenceChecker extends AbstractChecker {

    public TableReferenceChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        String tableRef = ((CString) af.parameters().get(0)).value();
        Table table = resolveTable(tableRef);
        GenericType.Relation.Schema schema = tableToSchema(table);
        return TypeInfo.builder()
                .expressionType(ExpressionType.one(new GenericType.Relation(schema)))
                .build();
    }

    private Table resolveTable(String tableRef) {
        int dotIdx = tableRef.lastIndexOf('.');
        String simpleDbName = tableRef;
        String tableName = tableRef;

        if (dotIdx > 0) {
            String dbRef = tableRef.substring(0, dotIdx);
            tableName = tableRef.substring(dotIdx + 1);
            simpleDbName = dbRef.contains("::")
                    ? dbRef.substring(dbRef.lastIndexOf("::") + 2)
                    : dbRef;
        }

        String tableKey = simpleDbName + "." + tableName;
        ModelContext modelCtx = env.modelContext();

        if (modelCtx != null) {
            var tableOpt = modelCtx.findTable(tableKey);
            if (tableOpt.isPresent())
                return tableOpt.get();
            tableOpt = modelCtx.findTable(tableName);
            if (tableOpt.isPresent())
                return tableOpt.get();
        }

        throw new PureCompileException("Table not found: " + tableRef);
    }

    private static GenericType.Relation.Schema tableToSchema(Table table) {
        Map<String, GenericType> columns = new LinkedHashMap<>();
        for (var col : table.columns()) {
            columns.put(col.name(), col.dataType().toGenericType());
        }
        return GenericType.Relation.Schema.withoutPivot(columns);
    }
}
