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
 * Checker for {@code tableReference(CString db, CString name)}.
 *
 * <p>Resolves a table reference to a physical table using structured (db, name) args,
 * builds a Relation schema from the table's columns, and stamps TypeInfo.
 *
 * <p>MappingNormalizer emits: {@code tableReference(CString("store::DB"), CString("T_PERSON"))}
 */
public class TableReferenceChecker extends AbstractChecker {

    public TableReferenceChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        // Validate arity against registered signature: tableReference(String[1], String[1])
        resolveOverload("tableReference", af.parameters(), source);
        String db = ((CString) af.parameters().get(0)).value();
        String name = ((CString) af.parameters().get(1)).value();
        Table table = resolveTable(db, name);
        GenericType.Relation.Schema schema = tableToSchema(table);
        return TypeInfo.builder()
                .resolvedTableName(table.dbName())
                .expressionType(ExpressionType.one(new GenericType.Relation(schema)))
                .build();
    }

    private Table resolveTable(String db, String name) {
        ModelContext modelCtx = env.modelContext();
        if (modelCtx != null) {
            var tableOpt = modelCtx.findTable(db, name);
            if (tableOpt.isPresent())
                return tableOpt.get();
        }
        throw new PureCompileException("Table not found: " + db + "." + name);
    }

    private static GenericType.Relation.Schema tableToSchema(Table table) {
        Map<String, GenericType> columns = new LinkedHashMap<>();
        for (var col : table.columns()) {
            columns.put(col.name(), col.dataType().toGenericType());
        }
        return GenericType.Relation.Schema.withoutPivot(columns);
    }
}
