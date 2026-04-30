package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.CString;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.compiler.typed.TypedTableReference;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.store.Table;
import com.gs.legend.model.m3.Type;

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
    public TypedTableReference check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        // Validate arity + arg types against the signature.
        resolveOverload("tableReference", af.parameters(), source);
        String db = ((CString) af.parameters().get(0)).value();
        String name = ((CString) af.parameters().get(1)).value();
        Table table = resolveTable(db, name);
        Type.Schema schema = tableToSchema(table);
        return new TypedTableReference(
                table.dbName(),
                table.dbName(),
                ExpressionType.one(new Type.Relation(schema)));
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

    private static Type.Schema tableToSchema(Table table) {
        Map<String, Type> columns = new LinkedHashMap<>();
        for (var col : table.columns()) {
            columns.put(col.name(), col.dataType().toGenericType());
        }
        return Type.Schema.withoutPivot(columns);
    }
}
