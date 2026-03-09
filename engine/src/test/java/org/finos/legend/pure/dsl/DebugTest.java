package org.finos.legend.pure.dsl;

public class DebugTest {
    public static void main(String[] args) {
        String[] queries = {
            "|meta::pure::functions::math::abs(-123456789123456789.99)",
            "|'echo'->meta::pure::functions::lang::tests::letFn::letWithParam()->meta::pure::functions::multiplicity::toOne()",
            "|meta::pure::functions::lang::tests::letFn::letAsLastStatement()",
            "|meta::pure::functions::math::divide(3.1415D, 0.1D, 2)",
            "|9223372036854775898 - 132",
        };
        for (String q : queries) {
            System.out.println("=== Query: " + q + " ===");
            try {
                PureExpression oldAst = PureParser.parseLegacy(q);
                System.out.println("OLD: " + oldAst);
            } catch (Exception e) {
                System.out.println("OLD ERR: " + e.getMessage());
            }
            try {
                PureExpression newAst = PureParser.parse(q);
                System.out.println("NEW: " + newAst);
            } catch (Exception e) {
                System.out.println("NEW ERR: " + e.getMessage());
            }
            System.out.println();
        }
    }
}
