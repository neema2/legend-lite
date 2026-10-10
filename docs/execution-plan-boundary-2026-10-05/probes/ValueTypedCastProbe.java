import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * H2 types a parameter when it prepares a statement, before its value is known; a Float's, a Decimal's, a Number's, a
 * Date's or a DateTime's literal type depends on the value (formerly PARK-19). Measured here (H2 2.1.214, lite's
 * settings; docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3): a parameter bound in a cast to the VALUE'S OWN type --
 * the type the runner writes into the plan's type hole when the value is known (H2.holeType): a decimal of its own
 * precision and scale, a floating decimal of its own digits, a whole number a BIGINT, a date, a date-time to the
 * nanosecond passed as its text -- against the literal today's path writes, in the positions a plan puts a parameter:
 * alone, in arithmetic with a column, compared with one, and inside the JSON text the database builds for the answer.
 * Same type and same text, or not. The last case is the control: a plain TIMESTAMP rounds what the literal keeps.
 * Run: java -cp h2-2.1.214.jar ValueTypedCastProbe.java
 */
public class ValueTypedCastProbe {
    record Case(String what, String literal, String cast, Object value) {}

    interface Binder { void bind(PreparedStatement st) throws Exception; }

    static String run(Connection c, String sql, Object value) {
        try (PreparedStatement st = c.prepareStatement(sql)) {
            int marks = (int) sql.chars().filter(ch -> ch == '?').count();
            for (int i = 1; i <= marks; i++) {
                // a date-time is passed as its text, as the runner passes it
                st.setObject(i, value instanceof LocalDateTime t ? t.toString().replace('T', ' ') : value);
            }
            try (ResultSet rs = st.executeQuery()) {
                List<String> out = new ArrayList<>();
                ResultSetMetaData md = rs.getMetaData();
                String type = md.getColumnTypeName(1) + "(" + md.getPrecision(1) + "," + md.getScale(1) + ")";
                while (rs.next()) out.add(rs.getString(1));
                return type + " " + out;
            }
        } catch (Exception e) {
            return "FAIL " + String.valueOf(e.getMessage()).split("\n")[0];
        }
    }

    /** {@code bd}'s own decimal type, as H2 types a literal of its digits: its precision and its scale. */
    static String numeric(BigDecimal bd) {
        return "NUMERIC(" + bd.precision() + "," + bd.scale() + ")";
    }

    /** A whole number's type: BIGINT, as an Integer parameter's (H2 types a small literal INTEGER: the same text). */
    static String integer(long v) {
        return "BIGINT";
    }

    public static void main(String[] a) throws Exception {
        String settings = ";NON_KEYWORDS=ANY,ASYMMETRIC,AUTHORIZATION,CAST,CURRENT_PATH,CURRENT_ROLE,DAY,DEFAULT,ELSE,END,"
                + "HOUR,KEY,MINUTE,MONTH,SECOND,SESSION_USER,SET,SOME,SYMMETRIC,SYSTEM_USER,TO,UESCAPE,USER,VALUE,WHEN,"
                + "YEAR,OVER;MODE=LEGACY;DEFAULT_NULL_ORDERING=HIGH";
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:typed" + settings, "sa", "")) {
            System.out.println("== H2 " + c.getMetaData().getDatabaseProductVersion() + ", lite's settings");
            try (Statement s = c.createStatement()) {
                s.execute("create table T(ID INTEGER, PRICE DECIMAL(10,2), N INTEGER, D DATE, TS TIMESTAMP)");
                s.execute("insert into T values (1, 1.50, 3, DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00'),"
                        + " (2, 3.25, 5, DATE '2024-01-02', TIMESTAMP '2024-01-02 10:30:00')");
            }
            List<Case> numbers = new ArrayList<>();
            for (String d : List.of("1.1", "2.50", "0.0", "0.00", "-0.5", "0.0000010", "100000000000000.0",
                    "12345678901234567890.123", "7", "9999999999")) {
                BigDecimal bd = new BigDecimal(d);
                // a Number's integer is a BIGINT in a plan (a Long); every other value its own decimal type
                boolean integer = !d.contains(".");
                numbers.add(new Case(d, d, integer ? integer(bd.longValueExact()) : numeric(bd),
                        integer ? (Object) bd.longValueExact() : bd));
            }
            // an extreme magnitude: the literal is written in exponent form (Rule 1), a DOUBLE
            for (double v : new double[] {1.5e15, 2.5e-7, Double.MIN_VALUE, Double.MAX_VALUE}) {
                numbers.add(new Case(Double.toString(v), Double.toString(v), "DECFLOAT(" + BigDecimal.valueOf(v).precision() + ")", v));
            }
            String[][] numberShapes = {
                    {"alone", "SELECT %s AS X FROM T ORDER BY ID"},
                    {"times a decimal column", "SELECT PRICE * %s FROM T ORDER BY ID"},
                    {"plus an integer column", "SELECT N + %s FROM T ORDER BY ID"},
                    {"compared with a column", "SELECT ID FROM T WHERE PRICE > %s ORDER BY ID"},
                    {"in the answer's JSON", "SELECT CAST(JSON_OBJECT('x': X) AS VARCHAR) FROM (SELECT %s AS X FROM T) s"},
                    {"times a column, in the JSON", "SELECT CAST(JSON_OBJECT('x': X) AS VARCHAR) FROM"
                            + " (SELECT PRICE * %s AS X FROM T ORDER BY ID) s"},
                    {"plus a column, in the JSON", "SELECT CAST(JSON_OBJECT('x': X) AS VARCHAR) FROM"
                            + " (SELECT N + %s AS X FROM T ORDER BY ID) s"},
            };
            List<Case> dates = List.of(
                    new Case("a StrictDate 2024-01-02", "DATE '2024-01-02'", "DATE", LocalDate.of(2024, 1, 2)),
                    new Case("a DateTime 2024-01-02 10:30", "TIMESTAMP '2024-01-02 10:30:00'", "TIMESTAMP(9)",
                            LocalDateTime.of(2024, 1, 2, 10, 30)),
                    new Case("a DateTime with millis", "TIMESTAMP '2024-01-02 10:30:00.123'", "TIMESTAMP(9)",
                            LocalDateTime.of(2024, 1, 2, 10, 30, 0, 123_000_000)),
                    new Case("a DateTime with nanos", "TIMESTAMP '2024-01-02 10:30:00.123456789'", "TIMESTAMP(9)",
                            LocalDateTime.of(2024, 1, 2, 10, 30, 0, 123_456_789)),
                    new Case("a DateTime with nanos, cast to plain TIMESTAMP",
                            "TIMESTAMP '2024-01-02 10:30:00.123456789'", "TIMESTAMP",
                            LocalDateTime.of(2024, 1, 2, 10, 30, 0, 123_456_789)));
            String[][] dateShapes = {
                    {"alone", "SELECT %s AS X FROM T ORDER BY ID"},
                    {"compared with a date column", "SELECT ID FROM T WHERE D = %s ORDER BY ID"},
                    {"compared with a timestamp column", "SELECT ID FROM T WHERE TS >= %s ORDER BY ID"},
                    {"in the answer's JSON", "SELECT CAST(JSON_OBJECT('x': X) AS VARCHAR) FROM (SELECT %s AS X FROM T) s"},
            };
            int same = 0, diff = 0;
            for (Object[] group : new Object[][] {{numbers, numberShapes}, {dates, dateShapes}}) {
                @SuppressWarnings("unchecked") List<Case> cases = (List<Case>) group[0];
                String[][] shapes = (String[][]) group[1];
                for (Case k : cases) {
                    System.out.println("-- " + k.what() + ": literal " + k.literal() + ", cast to " + k.cast());
                    for (String[] shape : shapes) {
                        String lit = run(c, String.format(shape[1], k.literal()), null);
                        String bound = run(c, String.format(shape[1], "CAST(? AS " + k.cast() + ")"), k.value());
                        boolean ok = lit.equals(bound);
                        if (ok) same++; else diff++;
                        System.out.println("  " + (ok ? "SAME " : "DIFF ") + shape[0] + ": literal " + lit
                                + (ok ? "" : " | cast " + bound));
                    }
                }
            }
            System.out.println("== " + same + " same, " + diff + " different");
            // an absent value: a null of the hole's absent kind, its type spelled by name alone -- valid, and null, in
            // every position (the let path writes the query with the value empty, another statement)
            System.out.println("-- absent values, each a null cast to its kind's type by name alone");
            for (String type : List.of("NUMERIC", "DECFLOAT", "BIGINT", "DATE", "TIMESTAMP(9)")) {
                boolean number = !type.startsWith("DATE") && !type.startsWith("TIMESTAMP");
                String[][] shapes = number ? numberShapes : dateShapes;
                for (String[] shape : shapes) {
                    System.out.println("  " + type + " " + shape[0] + ": "
                            + run(c, String.format(shape[1], "CAST(? AS " + type + ")"), null));
                }
            }
        }
    }
}
