// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import com.legend.graphql.hardening.ReferenceModel.Kind;
import com.legend.graphql.hardening.ReferenceModel.Obj;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/** The shop fixture as the reference reads it: the same rows, each derived property in its Java meaning. */
public final class ShopReference {

    private ShopReference() {
    }

    public static final ReferenceModel MODEL = build(ReferenceModel.CaseFolding.UNICODE);

    /** The model under a database's case folding. */
    static ReferenceModel build(ReferenceModel.CaseFolding folding) {
        ReferenceModel m = new ReferenceModel();
        m.folding = folding;
        Map<Integer, Obj> customers = new LinkedHashMap<>();
        Map<Integer, Obj> orders = new LinkedHashMap<>();
        Map<Integer, Obj> lines = new LinkedHashMap<>();
        Map<String, Obj> products = new LinkedHashMap<>();
        Map<Integer, Obj> tags = new LinkedHashMap<>();
        for (ShopFixture.Customer r : ShopFixture.CUSTOMERS) {
            Map<String, Object> v = new LinkedHashMap<>();
            v.put("id", (long) r.id());
            v.put("name", r.name());
            v.put("email", r.email());
            v.put("tier", r.tier());
            v.put("vip", r.vip());
            v.put("joined", r.joined());
            v.put("lastSeen", r.lastSeen());
            v.put("credit", r.credit());
            v.put("score", r.score());
            v.put("_referrer", r.referrer());
            customers.put(r.id(), new Obj("Customer", v));
        }
        for (ShopFixture.Order r : ShopFixture.ORDERS) {
            Map<String, Object> v = new LinkedHashMap<>();
            v.put("id", (long) r.id());
            v.put("placed", r.placed());
            v.put("status", r.status());
            v.put("note", r.note());
            v.put("discount", r.discount());
            v.put("_customer", r.customer());
            orders.put(r.id(), new Obj("Order", v));
        }
        for (ShopFixture.Line r : ShopFixture.LINES) {
            Map<String, Object> v = new LinkedHashMap<>();
            v.put("id", (long) r.id());
            v.put("qty", (long) r.qty());
            v.put("unitPrice", r.unitPrice());
            v.put("_order", r.order());
            v.put("_sku", r.sku());
            lines.put(r.id(), new Obj("Line", v));
        }
        for (ShopFixture.Product r : ShopFixture.PRODUCTS) {
            Map<String, Object> v = new LinkedHashMap<>();
            v.put("sku", r.sku());
            v.put("title", r.title());
            v.put("category", r.category());
            v.put("price", r.price());
            v.put("weight", r.weight());
            products.put(r.sku(), new Obj("Product", v));
        }
        for (ShopFixture.Tag r : ShopFixture.TAGS) {
            Map<String, Object> v = new LinkedHashMap<>();
            v.put("id", (long) r.id());
            v.put("label", r.label());
            tags.put(r.id(), new Obj("Tag", v));
        }
        List<Obj> cs = List.copyOf(customers.values());
        List<Obj> os = List.copyOf(orders.values());
        List<Obj> ls = List.copyOf(lines.values());
        List<Obj> ps = List.copyOf(products.values());
        List<Obj> ts = List.copyOf(tags.values());

        java.util.function.Function<Obj, List<Obj>> ordersOf = c -> filter(os, o -> c.get("id").equals(
                o.get("_customer") == null ? null : (long) (int) (Integer) o.get("_customer")));
        java.util.function.Function<Obj, List<Obj>> linesOf = o -> filter(ls, l -> ((Integer) l.get("_order")).longValue()
                == (long) o.get("id"));
        java.util.function.Function<Obj, List<Obj>> tagsOf = p -> {
            List<Obj> out = new ArrayList<>();
            for (ShopFixture.Tagging t : ShopFixture.TAGGINGS) {
                if (t.sku().equals(p.get("sku"))) {
                    out.add(tags.get(t.tag()));
                }
            }
            return out;
        };

        m.cls("Customer")
                .scalar("id", Kind.BIGINT).scalar("name", Kind.STRING).scalar("email", Kind.STRING)
                .scalar("tier", Kind.ENUM).scalar("vip", Kind.BOOLEAN).scalar("joined", Kind.DATE)
                .scalar("lastSeen", Kind.DATETIME).scalar("credit", Kind.DECIMAL).scalar("score", Kind.BIGINT)
                .derived("handle", Kind.STRING, c -> m.lower((String) c.get("name")))
                .derived("label", Kind.STRING, c -> c.get("email") == null ? c.get("name")
                        : c.get("name") + " <" + c.get("email") + ">")
                .derived("loud", Kind.STRING, c -> m.upper(m.lower((String) c.get("name"))))
                .derived("idTimes", Kind.BIGINT, Map.of("k", Kind.BIGINT),
                        (c, a) -> (long) c.get("id") * (long) a.get("k"))
                .derived("orderCount", Kind.BIGINT, c -> (long) ordersOf.apply(c).size())
                .toOne("referrer", "Customer", c -> c.get("_referrer") == null ? null
                        : customers.get((Integer) c.get("_referrer")))
                .toMany("referred", "Customer", c -> filter(cs, x -> x.get("_referrer") != null
                        && ((Integer) x.get("_referrer")).longValue() == (long) c.get("id")))
                .toMany("orders", "Order", ordersOf)
                .toMany("liveOrders", "Order", c -> filter(ordersOf.apply(c), o -> !"CANCELLED".equals(o.get("status"))))
                .roots(() -> cs);
        m.cls("Order")
                .scalar("id", Kind.BIGINT).scalar("placed", Kind.DATE).scalar("status", Kind.ENUM)
                .scalar("note", Kind.STRING).scalar("discount", Kind.FLOAT)
                .derived("lineCount", Kind.BIGINT, o -> (long) linesOf.apply(o).size())
                .toOne("customer", "Customer", o -> o.get("_customer") == null ? null
                        : customers.get((Integer) o.get("_customer")))
                .toMany("lines", "Line", linesOf)
                .roots(() -> os);
        m.cls("Line")
                .scalar("id", Kind.BIGINT).scalar("qty", Kind.BIGINT).scalar("unitPrice", Kind.FLOAT)
                .derived("amount", Kind.FLOAT, l -> (long) l.get("qty") * (double) l.get("unitPrice"))
                .toOne("order", "Order", l -> orders.get((Integer) l.get("_order")))
                .toOne("product", "Product", l -> l.get("_sku") == null ? null : products.get((String) l.get("_sku")))
                .roots(() -> ls);
        m.cls("Product")
                .scalar("sku", Kind.STRING).scalar("title", Kind.STRING).scalar("category", Kind.ENUM)
                .scalar("price", Kind.FLOAT).scalar("weight", Kind.FLOAT)
                .derived("tagCount", Kind.BIGINT, p -> (long) tagsOf.apply(p).size())
                .toMany("tags", "Tag", tagsOf)
                .toMany("productLines", "Line", p -> filter(ls, l -> p.get("sku").equals(l.get("_sku"))))
                .roots(() -> ps);
        m.cls("Tag")
                .scalar("id", Kind.BIGINT).scalar("label", Kind.STRING)
                .toMany("products", "Product", t -> {
                    List<Obj> out = new ArrayList<>();
                    for (ShopFixture.Tagging g : ShopFixture.TAGGINGS) {
                        if ((long) g.tag() == (long) t.get("id")) {
                            out.add(products.get(g.sku()));
                        }
                    }
                    return out;
                })
                .roots(() -> ts);
        return m;
    }

    static List<Obj> filter(List<Obj> xs, Predicate<Obj> p) {
        List<Obj> out = new ArrayList<>();
        for (Obj x : xs) {
            if (p.test(x)) {
                out.add(x);
            }
        }
        return out;
    }
}
