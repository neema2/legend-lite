// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * THE SHOP FIXTURE's rows (docs/GRAPHQL_STUDY_2026_10_18.md §13.7): one source for the database the queries run on
 * and for the reference that judges them. Each row is built to break something: nulls in every nullable column, empty
 * lists (customers 6-8 have no orders, order 7 no lines, product XX-9 no lines and no tags, tag 4 no product), ties on
 * every sortable column, an apostrophe, `%` and `_` in names and notes, an empty string, unicode, case variants
 * ("Alice", "ALICE", "bob"), zero and negative numbers, a 64-bit integer past 2^53, a leap day, an orphan order with no
 * customer, a line with no product.
 */
public final class ShopFixture {

    private ShopFixture() {
    }

    public static final String MODEL = Harness.resource("models/shop.pure");
    public static final String RUNTIME = "shop::Runtime";

    public record Customer(int id, String name, String email, String tier, boolean vip, LocalDate joined,
            LocalDateTime lastSeen, BigDecimal credit, Long score, Integer referrer) {
    }

    public record Order(int id, Integer customer, LocalDate placed, String status, String note, Double discount) {
    }

    public record Line(int id, int order, String sku, int qty, double unitPrice) {
    }

    public record Product(String sku, String title, String category, double price, Double weight) {
    }

    public record Tag(int id, String label) {
    }

    public record Tagging(String sku, int tag) {
    }

    static LocalDate d(String s) {
        return LocalDate.parse(s);
    }

    static LocalDateTime t(String s) {
        return LocalDateTime.parse(s);
    }

    static BigDecimal m(String s) {
        return new BigDecimal(s);
    }

    public static final List<Customer> CUSTOMERS = List.of(
            new Customer(1, "Alice", "alice@example.com", "GOLD", true, d("2020-01-15"), t("2024-06-01T10:00:00"),
                    m("1500.50"), 100L, null),
            new Customer(2, "bob", null, "SILVER", false, d("2021-03-01"), null, m("0.00"), 0L, 1),
            new Customer(3, "Carol O'Neil", "carol@example.com", null, true, d("2021-03-01"),
                    t("2024-05-31T23:59:59"), null, -5L, 1),
            new Customer(4, "Dan 100%", "dan_x@example.com", "BRONZE", false, d("2022-07-04"),
                    t("2024-06-01T10:00:00"), m("99999999.99"), 9007199254740993L, 3),
            new Customer(5, "Émile Zola", "emile@example.fr", "GOLD", false, d("2019-12-31"), null, m("250.00"), 42L,
                    null),
            new Customer(6, "ALICE", "ALICE@EXAMPLE.COM", null, false, d("2023-02-28"), t("2023-02-28T00:00:00"),
                    null, null, 5),
            new Customer(7, "Zed_", null, "SILVER", true, d("2024-02-29"), null, m("10.10"), 7L, null),
            new Customer(8, "a%b_c", "x@y.z", "BRONZE", true, d("2020-01-15"), null, m("0.01"), 1L, 2));

    public static final List<Order> ORDERS = List.of(
            new Order(1, 1, d("2024-01-10"), "SHIPPED", "gift", 0.1),
            new Order(2, 1, d("2024-02-11"), "OPEN", null, null),
            new Order(3, 1, d("2024-02-29"), "CANCELLED", "don't ship", 0.0),
            new Order(4, 2, d("2024-03-05"), "SHIPPED", "", null),
            new Order(5, 3, d("2024-03-05"), "OPEN", "rush 50%", 0.25),
            new Order(6, 4, d("2023-12-31"), "SHIPPED", null, null),
            new Order(7, null, d("2024-04-01"), "OPEN", "orphan", null),
            new Order(8, 5, d("2024-01-01"), "CANCELLED", null, 0.5));

    public static final List<Line> LINES = List.of(
            new Line(1, 1, "BK-1", 2, 10.0),
            new Line(2, 1, "TY-1", 1, 25.5),
            new Line(3, 2, "BK-1", 1, 10.0),
            new Line(4, 3, "GD-1", 3, 7.25),
            new Line(5, 4, "BK-2", 10, 0.99),
            new Line(6, 5, null, 1, 100.0),
            new Line(7, 6, "TY-1", 4, 25.5),
            new Line(8, 8, "GD-2", 1, 0.0));

    public static final List<Product> PRODUCTS = List.of(
            new Product("BK-1", "Pure Patterns", "BOOKS", 10.0, 0.5),
            new Product("BK-2", "The 100% Guide", "BOOKS", 0.99, null),
            new Product("TY-1", "Robot_X", "TOYS", 25.5, 1.2),
            new Product("GD-1", "Hose", "GARDEN", 7.25, 2.0),
            new Product("GD-2", "Seeds (free)", null, 0.0, 0.05),
            new Product("XX-9", "Unsold", "TOYS", 99.0, null));

    public static final List<Tag> TAGS = List.of(
            new Tag(1, "new"), new Tag(2, "sale"), new Tag(3, "eco"), new Tag(4, "unused"));

    public static final List<Tagging> TAGGINGS = List.of(
            new Tagging("BK-1", 1), new Tagging("BK-1", 2), new Tagging("TY-1", 2), new Tagging("GD-1", 3),
            new Tagging("GD-2", 3), new Tagging("GD-2", 2));

    /** The tables and the rows, on a fresh connection. */
    public static void seed(Connection c) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute("CREATE TABLE CUSTOMER (ID INTEGER PRIMARY KEY, NAME VARCHAR(200), EMAIL VARCHAR(200),"
                    + " TIER VARCHAR(10), VIP BOOLEAN, JOINED DATE, LAST_SEEN TIMESTAMP, CREDIT DECIMAL(12, 2),"
                    + " SCORE BIGINT, REFERRER_ID INTEGER)");
            s.execute("CREATE TABLE ORDERS (ID INTEGER PRIMARY KEY, CUSTOMER_ID INTEGER, PLACED DATE,"
                    + " STATUS VARCHAR(10), NOTE VARCHAR(200), DISCOUNT DOUBLE)");
            s.execute("CREATE TABLE LINE (ID INTEGER PRIMARY KEY, ORDER_ID INTEGER, SKU VARCHAR(10), QTY INTEGER,"
                    + " UNIT_PRICE DOUBLE)");
            s.execute("CREATE TABLE PRODUCT (SKU VARCHAR(10) PRIMARY KEY, TITLE VARCHAR(200), CATEGORY VARCHAR(10),"
                    + " PRICE DOUBLE, WEIGHT DOUBLE)");
            s.execute("CREATE TABLE TAG (ID INTEGER PRIMARY KEY, LABEL VARCHAR(50))");
            s.execute("CREATE TABLE PRODUCT_TAG (SKU VARCHAR(10), TAG_ID INTEGER)");
        }
        for (Customer r : CUSTOMERS) {
            insert(c, "CUSTOMER", r.id(), r.name(), r.email(), r.tier(), r.vip(), r.joined(), r.lastSeen(), r.credit(),
                    r.score(), r.referrer());
        }
        for (Order r : ORDERS) {
            insert(c, "ORDERS", r.id(), r.customer(), r.placed(), r.status(), r.note(), r.discount());
        }
        for (Line r : LINES) {
            insert(c, "LINE", r.id(), r.order(), r.sku(), r.qty(), r.unitPrice());
        }
        for (Product r : PRODUCTS) {
            insert(c, "PRODUCT", r.sku(), r.title(), r.category(), r.price(), r.weight());
        }
        for (Tag r : TAGS) {
            insert(c, "TAG", r.id(), r.label());
        }
        for (Tagging r : TAGGINGS) {
            insert(c, "PRODUCT_TAG", r.sku(), r.tag());
        }
    }

    /** One row through a prepared statement: every value bound, none spelled (the harness escapes nothing). */
    static void insert(Connection c, String table, Object... values) throws SQLException {
        String marks = String.join(", ", java.util.Collections.nCopies(values.length, "?"));
        try (PreparedStatement p = c.prepareStatement("INSERT INTO " + table + " VALUES (" + marks + ")")) {
            for (int i = 0; i < values.length; i++) {
                Object v = values[i];
                switch (v) {
                    case null -> p.setNull(i + 1, Types.NULL);
                    case LocalDate ld -> p.setObject(i + 1, ld);
                    case LocalDateTime ldt -> p.setTimestamp(i + 1, Timestamp.valueOf(ldt));
                    default -> p.setObject(i + 1, v);
                }
            }
            p.executeUpdate();
        }
    }

    static List<Object> list(Object... xs) {
        return new ArrayList<>(Arrays.asList(xs));
    }
}
