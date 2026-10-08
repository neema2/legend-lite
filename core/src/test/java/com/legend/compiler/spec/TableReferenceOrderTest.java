package com.legend.compiler.spec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import java.util.ArrayDeque;
import java.util.List;
import org.junit.jupiter.api.Test;

/** A table reference's stored types and quoted columns iterate in the DDL's declaration order (W1.5): the typed node
 *  prints them and an id hashes the print, so a JVM-salted order was a run-dependent id (the render census, 2026-10-08). */
class TableReferenceOrderTest {

    private static final String MODEL = """
            ###Relational
            Database t::DB
            (
                Table T (zip VARCHAR(8), age INTEGER, "FIRST NAME" VARCHAR(32), dept VARCHAR(8), id INTEGER PRIMARY KEY,
                         "LAST NAME" VARCHAR(32), salary DOUBLE, "DOB" DATE)
            )
            """;

    @Test
    void storedTypesAndQuotedColumnsKeepTheDeclarationOrder() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        TypedTableReference ref = find(Compiler.query(ctx, "|#>{t::DB.T}#").body());
        assertEquals(List.of("zip", "age", "FIRST NAME", "dept", "id", "LAST NAME", "salary", "DOB"),
                List.copyOf(ref.storedTypes().keySet()), "the DDL's order, not a hash's");
        assertEquals(List.of("FIRST NAME", "LAST NAME", "DOB"), List.copyOf(ref.quotedColumns()), "the quoted names, in order");
    }

    private static TypedTableReference find(List<TypedSpec> body) {
        ArrayDeque<TypedSpec> work = new ArrayDeque<>(body);
        while (!work.isEmpty()) {
            TypedSpec n = work.poll();
            if (n instanceof TypedTableReference r) {
                return r;
            }
            work.addAll(n.children());
        }
        throw new AssertionError("no table reference in the typed body");
    }
}
