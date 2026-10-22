// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The MongoDB line's homework 3 (2026-10-17, docs/MONGODB_PLAN_2026_10_16.md §9): a differential in miniature. Each
 * case is a storeless Pure expression shaped like the PCT suites' (literal relations, filters, sorts, groups, joins,
 * unions, string, date, number and list functions), planned to the CSV wire on DuckDB (the platform's reference) and
 * on MongoDB, run on both, and the two texts compared. The verdicts -- EQUAL, DIFFERENT, REFUSED (a named refusal
 * at render), ERROR (the server's) -- are written to the test's outputs ({@code mongodb-differential.tsv}) and
 * printed; the test fails only when the reference does not run. It measures; it pins nothing.
 */
class MongoDbDifferentialTest {


    private record Case(String name, String expression, TypedQuery.Output output) {
        Case(String name, String expression) {
            this(name, expression, TypedQuery.Output.CSV);
        }
    }

    /** A class and an enumeration for the text carrier's forms (docs/PLAN_PATH_PROGRAM_2026_10_17.md §4, step 3). */
    private static final String CLASSES = "Class diff::P { name: String[1]; age: Integer[1]; when: StrictDate[0..1];"
            + " tags: String[*]; boss: diff::P[0..1]; kind: diff::E[0..1]; }\nEnum diff::E { A, B }\n";

    private static final String PEOPLE = "#TDS\n id, name, age, city\n 1, Ann, 34, Oslo\n 2, Bob, 28, Oslo\n"
            + " 3, Cy, 41, Rome\n#";
    private static final String CITIES = "#TDS\n town, country\n Oslo, NO\n Paris, FR\n#";
    /** Two groups with tied scores, for the ranking and ranging windows. */
    private static final String SCORES = "#TDS\n id, g, s, n\n 1, a, 10, x\n 2, a, 20, y\n 3, a, 20, z\n"
            + " 4, a, 30, w\n 5, b, 5, v\n 6, b, 6, u\n#";

    private static final List<Case> CASES = List.of(
            new Case("plus", "1 + 2"),
            new Case("string_concat", "'a' + 'b'"),
            new Case("float_divide", "7 / 2"),
            new Case("float_text", "2.0->toString()"),
            new Case("float_texts", "#TDS\n w\n 1.5\n -2.0\n 0.0\n 100.0\n 0.25\n#->extend(~t: x | $x.w->toString())"
                    + "->select(~t)"),
            new Case("float_big", "1.0e20->toString()"),
            new Case("float_small", "1.5e-8->toString()"),
            new Case("float_neg", "(0.0 - 2.0)->toString()"),
            new Case("float_sum", "(0.1 + 0.2)->toString()"),
            new Case("float_third", "(1.0 / 3.0)->toString()"),
            new Case("float_sixteen", "(1.0e16 + 2.0)->toString()"),
            new Case("float_column", "#TDS\n id, w\n 1, 1.5\n 2, 2.0\n 3, -3.0\n#->select(~w)"),
            new Case("float_avg", PEOPLE + "->groupBy(~city, ~a: x | $x.age : y | $y->average())"
                    + "->sort(~city->ascending())"),
            new Case("list_sum", "[1, 2, 3]->plus()"),
            new Case("list_sort", "[3, 1, 2]->sort()"),
            new Case("list_map", "[1, 2, 3]->map(x | $x * 2)"),
            new Case("list_filter", "[1, 2, 3]->filter(x | $x > 1)"),
            new Case("list_fold", "[1, 2, 3]->fold({x, a | $a + $x}, 0)"),
            new Case("list_join", "['a', 'b']->joinStrings(',')"),
            new Case("string_functions", "'Hello'->toUpper() + 'Hello'->toLower() + 'Hello'->length()->toString()"
                    + " + 'Hello'->substring(1, 3) + 'Hello'->replace('l', 'L') + 'Hello'->reverseString()"),
            new Case("string_predicates", "['Hello'->startsWith('He'), 'Hello'->endsWith('lo'), 'Hello'->contains('ell'),"
                    + " 'Hello'->indexOf('l') == 2]"),
            new Case("string_split", "'a,b,c'->split(',')"),
            new Case("date_parts", "[%2020-03-15->year(), %2020-03-15->monthNumber(), %2020-03-15->dayOfMonth()]"),
            new Case("date_adjust", "%2020-03-15->adjust(20, DurationUnit.DAYS)"),
            new Case("date_time_hour", "%2020-03-15T10:20:30->hour()"),
            new Case("date_compare", "%2020-01-01 < %2020-02-01"),
            new Case("date_diff", "%2020-01-01->dateDiff(%2020-03-01, DurationUnit.DAYS)"),
            new Case("math_int", "[10->abs(), -3->abs(), 2.5->round(), 2.4->floor(), 2.4->ceiling()]"),
            new Case("math_float", "[16->sqrt(), 2.0->pow(10.0), 2.5->abs()]"),
            new Case("math_rem", "[7->rem(3), 7->mod(3), -7->rem(3), -7->mod(3)]"),
            new Case("rem_int", "12->rem(5)"),
            new Case("format_bool", "format('a %s b %s', ['fox', false])"),
            new Case("format_float", "format('%f|%f', [2.0, 1.5e-8])"),
            new Case("fold_list", "[1, 2, 3]->fold({a, b | $b->add($a)}, [])"),
            new Case("fold_one", "[1]->fold({a, b | $a + $b}, 0)"),
            new Case("tds_rem", "12->rem(5)", TypedQuery.Output.TDS_TEXT),
            new Case("tds_format_bool", "format('a %s b %s', ['fox', false])", TypedQuery.Output.TDS_TEXT),
            new Case("tds_fold_list", "[1, 2, 3]->fold({a, b | $b->add($a)}, [])", TypedQuery.Output.TDS_TEXT),
            new Case("tds_strings", "['a', '345', 'true', ' x ', '']", TypedQuery.Output.TDS_TEXT),
            new Case("if_then", "if(1 > 2, |'big', |'small')"),
            new Case("filter_select", PEOPLE + "->filter(x | $x.age > 30)->select(~[name, age])->sort(~name->ascending())"),
            new Case("sort_desc_limit", PEOPLE + "->sort(~age->descending())->limit(2)->select(~name)"),
            new Case("drop_slice", PEOPLE + "->sort(~id->ascending())->drop(1)->slice(0, 1)->select(~name)"),
            new Case("extend_strings", PEOPLE + "->extend(~u: x | $x.name->toUpper())->extend(~l: x | $x.name->length())"
                    + "->select(~[u, l])->sort(~u->ascending())"),
            new Case("extend_if_null", PEOPLE + "->extend(~n: x | if($x.age > 30, |$x.age, |[]))"
                    + "->select(~[name, n])->sort(~n->ascending())"),
            new Case("filter_empty", PEOPLE + "->extend(~n: x | if($x.age > 30, |$x.age, |[]))"
                    + "->filter(x | $x.n->isEmpty())->select(~name)"),
            new Case("in_list", PEOPLE + "->filter(x | $x.city->in(['Oslo', 'Nice']))->select(~name)->sort(~name->ascending())"),
            new Case("group_by", PEOPLE + "->groupBy(~city, ~[n: x | $x.name : y | $y->count(), s: x | $x.age : y | $y->plus(),"
                    + " a: x | $x.age : y | $y->average(), mx: x | $x.age : y | $y->max()])->sort(~city->ascending())"),
            new Case("group_join_strings", PEOPLE + "->groupBy(~city, ~[names: x | $x.name : y | $y->joinStrings(',')])"
                    + "->sort(~city->ascending())"),
            new Case("aggregate_all", PEOPLE + "->aggregate(~[n: x | $x.name : y | $y->count(), s: x | $x.age : y | $y->plus()])"),
            new Case("aggregate_none", PEOPLE + "->filter(x | $x.age > 100)->aggregate(~[n: x | $x.name : y | $y->count()])"),
            new Case("size_none", PEOPLE + "->filter(x | $x.age > 100)->size()"),
            new Case("size_some", PEOPLE + "->filter(x | $x.age > 30)->size()"),
            new Case("sum_all_null", PEOPLE + "->extend(~v: x | if($x.age > 100, |$x.age, |[]))"
                    + "->groupBy(~city, ~s: x | $x.v : y | $y->plus())->sort(~city->ascending())"),
            new Case("json_numbers", PEOPLE + "->extend(~h: x | $x.age / 2)->select(~[name, age, h])"
                    + "->sort(~name->ascending())", TypedQuery.Output.JSON),
            new Case("json_dates", PEOPLE + "->extend(~d: x | %2020-01-15->adjust($x.id, DurationUnit.DAYS))"
                    + "->extend(~t: x | %2020-01-15T10:20:30->adjust($x.id, DurationUnit.HOURS))->select(~[name, d, t])"
                    + "->sort(~name->ascending())", TypedQuery.Output.JSON),
            // Pure's TDS text (TypedQuery.Output.TDS_TEXT, the PCT harness's form on the plan path, 2026-10-17)
            new Case("tds_relation", PEOPLE + "->filter(x | $x.age > 30)->select(~[name, age, city])"
                    + "->sort(~name->ascending())", TypedQuery.Output.TDS_TEXT),
            new Case("tds_group", PEOPLE + "->groupBy(~city, ~[n: x | $x.name : y | $y->count(), a: x | $x.age : y | $y->average()])"
                    + "->sort(~city->ascending())", TypedQuery.Output.TDS_TEXT),
            new Case("tds_scalar_float", "7 / 2", TypedQuery.Output.TDS_TEXT),
            new Case("tds_collection", "['a,b', 'c\\'d', 'e']", TypedQuery.Output.TDS_TEXT),
            new Case("tds_dates", PEOPLE + "->extend(~d: x | %2020-01-15->adjust($x.id, DurationUnit.DAYS))"
                    + "->extend(~t: x | %2020-01-15T10:20:30->adjust($x.id, DurationUnit.HOURS))->select(~[name, d, t])"
                    + "->sort(~name->ascending())", TypedQuery.Output.TDS_TEXT),
            new Case("tds_date_value", "%2020-01-15T10:20:30.123->adjust(1, DurationUnit.MINUTES)", TypedQuery.Output.TDS_TEXT),
            new Case("tds_empty", PEOPLE + "->filter(x | $x.age > 100)->select(~[name])", TypedQuery.Output.TDS_TEXT),
            new Case("tds_null_cells", PEOPLE + "->extend(~n: x | if($x.age > 30, |$x.age, |[]))->select(~[name, n])"
                    + "->sort(~name->ascending())", TypedQuery.Output.TDS_TEXT),
            // the text carrier (the plan path program's step 3, 2026-10-18): a value root that is no primitive crosses
            // as Pure's own literal grammar, one case per form
            new Case("tds_instance", "^diff::P(name='a\\'b', age=1, when=%2024-01-02, tags=['x', 'y'],"
                    + " boss=^diff::P(name='b', age=2), kind=diff::E.B)", TypedQuery.Output.TDS_TEXT),
            new Case("tds_instances", "[^diff::P(name='a', age=1), ^diff::P(name='b', age=2)]", TypedQuery.Output.TDS_TEXT),
            new Case("tds_pair", "pair(1, 'a')", TypedQuery.Output.TDS_TEXT),
            new Case("tds_list", "list([1, 2])", TypedQuery.Output.TDS_TEXT),
            new Case("tds_map", "newMap([pair('k', 1)])", TypedQuery.Output.TDS_TEXT),
            new Case("tds_type", "diff::P", TypedQuery.Output.TDS_TEXT),
            new Case("tds_any", "[1, 'a', %2015-01-01, 1.5, true]", TypedQuery.Output.TDS_TEXT),
            new Case("tds_enum", "diff::E.A", TypedQuery.Output.TDS_TEXT),
            new Case("distinct", PEOPLE + "->select(~city)->distinct()->sort(~city->ascending())"),
            new Case("inner_join", PEOPLE + "->join(" + CITIES + ", JoinKind.INNER, {a, b | $a.city == $b.town})"
                    + "->select(~[name, country])->sort(~name->ascending())"),
            new Case("left_join", PEOPLE + "->join(" + CITIES + ", JoinKind.LEFT, {a, b | $a.city == $b.town})"
                    + "->select(~[name, country])->sort(~name->ascending())"),
            new Case("concatenate", PEOPLE + "->select(~name)->concatenate(" + PEOPLE + "->select(~name))"
                    + "->sort(~name->ascending())"),
            new Case("rename", PEOPLE + "->rename(~name, ~who)->select(~who)->sort(~who->ascending())"),
            new Case("date_column", PEOPLE + "->extend(~d: x | %2020-01-15->adjust($x.id, DurationUnit.DAYS))"
                    + "->extend(~day: x | $x.d->dayOfMonth())->select(~[id, day])->sort(~id->ascending())"),
            new Case("pivot", PEOPLE + "->pivot(~city, ~[s: x | $x.age : y | $y->plus()])"),
            new Case("window", PEOPLE + "->extend(over(~city), ~r: {p, w, r | $p->rowNumber($r)})"
                    + "->sort(~name->ascending())->select(~[name, r])"),
            new Case("window_rank", SCORES + "->extend(over(~g, ~s->descending()), ~[r: {p, w, r | $p->rank($w, $r)},"
                    + " d: {p, w, r | $p->denseRank($w, $r)}, rn: {p, w, r | $p->rowNumber($r)}])"
                    + "->sort(~id->ascending())->select(~[id, r, d, rn])"),
            new Case("window_two_keys", SCORES + "->extend(over(~g, [~s->ascending(), ~id->ascending()]),"
                    + " ~[r: {p, w, r | $p->rank($w, $r)}, rn: {p, w, r | $p->rowNumber($r)}])"
                    + "->sort(~id->ascending())->select(~[id, r, rn])"),
            new Case("window_running", SCORES + "->extend(over(~g, ~s->ascending()), ~t: {p, w, r | $r.s}: y | $y->plus())"
                    + "->sort(~id->ascending())->select(~[id, t])"),
            new Case("window_rows", SCORES + "->extend(over(~g, ~id->ascending(), rows(-1, 0)),"
                    + " ~t: {p, w, r | $r.s}: y | $y->plus())->sort(~id->ascending())->select(~[id, t])"),
            new Case("window_range_asc", SCORES + "->extend(over(~g, ~s->ascending(), _range(-1, 1)),"
                    + " ~t: {p, w, r | $r.s}: y | $y->plus())->sort(~id->ascending())->select(~[id, t])"),
            new Case("window_range_desc", SCORES + "->extend(over(~g, ~s->descending(), _range(-1, 1)),"
                    + " ~t: {p, w, r | $r.s}: y | $y->plus())->sort(~id->ascending())->select(~[id, t])"),
            new Case("window_range_unbounded_desc", SCORES + "->extend(over(~g, ~s->descending(),"
                    + " _range(unbounded(), 0)), ~t: {p, w, r | $r.s}: y | $y->plus())->sort(~id->ascending())"
                    + "->select(~[id, t])"),
            new Case("window_lag_lead", SCORES + "->extend(over(~g, ~id->ascending()), ~[a: {p, w, r | $p->lag($r).s},"
                    + " b: {p, w, r | $p->lead($r).s}])->sort(~id->ascending())->select(~[id, a, b])"),
            new Case("window_first_last", SCORES + "->extend(over(~g, ~id->ascending(),"
                    + " rows(unbounded(), unbounded())), ~[a: {p, w, r | $p->first($w, $r).s},"
                    + " b: {p, w, r | $p->last($w, $r).s}])->sort(~id->ascending())->select(~[id, a, b])"),
            new Case("window_percent", SCORES + "->extend(over(~g, ~s->ascending()),"
                    + " ~[a: {p, w, r | $p->percentRank($w, $r)}, b: {p, w, r | $p->cumulativeDistribution($w, $r)},"
                    + " c: {p, w, r | $p->ntile($r, 2)}])->sort(~id->ascending())->select(~[id, a, b, c])"),
            new Case("window_join_strings", SCORES + "->extend(over(~g, ~id->ascending()),"
                    + " ~t: {p, w, r | $r.n}: y | $y->joinStrings(','))->sort(~id->ascending())->select(~[id, t])"),
            new Case("window_unordered_count", SCORES + "->extend(over(~g), ~t: {p, w, r | $r.s}: y | $y->count())"
                    + "->sort(~id->ascending())->select(~[id, t])"),
            new Case("window_range_nulls", "#TDS\n id, g, o, i\n 1, a, 1, 10\n 2, a, 2, 20\n 3, a, null, 30\n"
                    + " 4, a, 3, 30\n 5, a, null, 20\n 6, b, 1, 10\n 7, b, null, 10\n#->extend(over(~g, ~o->ascending(),"
                    + " _range(-1, 1)), ~t: {p, w, r | $r.i}: y | $y->plus())->sort(~id->ascending())->select(~[id, t])"),
            new Case("window_range_nulls_unbounded", "#TDS\n id, g, o, i\n 1, a, 1, 10\n 2, a, 2, 20\n 3, a, null, 30\n"
                    + " 4, a, 3, 30\n 5, a, null, 20\n 6, b, 1, 10\n 7, b, null, 10\n#->extend(over(~g, ~o->ascending(),"
                    + " _range(-1, unbounded())), ~t: {p, w, r | $r.i}: y | $y->plus())->sort(~id->ascending())"
                    + "->select(~[id, t])"),
            new Case("window_range_days", "#TDS\n id:Integer, g:String, d:DateTime, i:Integer\n"
                    + " 1, a, 2020-01-01T00:00:00.000+0000, 10\n 2, a, 2020-01-02T00:00:00.000+0000, 20\n"
                    + " 3, a, 2020-01-05T00:00:00.000+0000, 30\n 4, b, 2020-01-01T00:00:00.000+0000, 5\n"
                    + "#->extend(over(~g, ~d->ascending(),"
                    + " _range(1, DurationUnit.SECONDS, 1, DurationUnit.DAYS)), ~t: {p, w, r | $r.i}: y | $y->plus())"
                    + "->sort(~id->ascending())->select(~[id, t])"),
            new Case("window_stats", SCORES + "->extend(over(~g), ~[a: {p, w, r | $r.s}: y | $y->mode(),"
                    + " b: {p, w, r | $r.s}: y | $y->percentile(0.6, false, true)])->sort(~id->ascending())"
                    + "->select(~[id, a, b])"),
            new Case("group_mode", SCORES + "->groupBy(~g, ~m: x | $x.s : y | $y->mode())->sort(~g->ascending())"),
            new Case("list_mode", "[3, 2, 3.0, 7.0, 2, 2, 3, 2.0, 2.0]->mode()"),
            new Case("group_corr", SCORES + "->groupBy(~g, ~[c: x | meta::pure::functions::math::mathUtility"
                    + "::rowMapper($x.s, $x.id) : y | $y->corr(), p: x | meta::pure::functions::math::mathUtility"
                    + "::rowMapper($x.s, $x.id) : y | $y->covarPopulation(), q: x | meta::pure::functions::math"
                    + "::mathUtility::rowMapper($x.s, $x.id) : y | $y->covarSample()])->sort(~g->ascending())"),
            new Case("window_corr", SCORES + "->extend(over(~g), ~c: {p, w, r | meta::pure::functions::math"
                    + "::mathUtility::rowMapper($r.s, $r.id)}: y | $y->corr())->sort(~id->ascending())"
                    + "->select(~[id, c])"),
            new Case("variant_text", "#TDS\n id:Integer, payload:meta::pure::metamodel::variant::Variant\n 1, '[1,2,3]'\n 2, '{\"a\":1,\"b\":\"x\"}'\n"
                    + " 3, '{\"n\":{\"d\":[true,null,1.5,\"q r\"]}}'\n#->extend(~t: x | $x.payload)"
                    + "->sort(~id->ascending())->select(~[id, t])"),
            new Case("variant_key", "#TDS\n id:Integer, payload:meta::pure::metamodel::variant::Variant\n 1, '{\"a\":1,\"b\":\"x\"}'\n"
                    + " 2, '{\"a\":2,\"b\":\"y\"}'\n#->extend(~[k: x | $x.payload->get('a'), s: x | $x.payload->get('b')])"
                    + "->sort(~id->ascending())->select(~[id, k, s])"),
            new Case("variant_map", "#TDS\n id:Integer, payload:meta::pure::metamodel::variant::Variant\n 1, '[1,2,3]'\n 2, '[4,5,6]'\n#->extend(~m: x |"
                    + " $x.payload->toMany(@Integer)->map(t | $t->mod(2))->toVariant())->sort(~id->ascending())"
                    + "->select(~[id, m])"),
            new Case("lateral", "#TDS\n did, dname\n 1, Eng\n 2, Ops\n 3, Art\n#->lateral(x | #TDS\n eid, department\n"
                    + " 10, 1\n 11, 1\n 12, 2\n#->filter(y | $y.department == $x.did))->sort(~eid->ascending())"),
            new Case("asof", "#TDS\n k, t\n a, 5\n a, 12\n b, 7\n#->asOfJoin(#TDS\n k2, t2, v\n a, 3, p\n a, 10, q\n"
                    + " b, 9, r\n#, {x, y | $x.t->toOne() > $y.t2->toOne()}, {x, y | $x.k->toOne() == $y.k2->toOne()})"
                    + "->sort([~k->ascending(), ~t->ascending()])"),
            new Case("asof_after", "#TDS\n k, t\n a, 5\n a, 12\n b, 7\n#->asOfJoin(#TDS\n k2, t2, v\n a, 3, p\n a, 10, q\n"
                    + " b, 9, r\n#, {x, y | $x.t->toOne() < $y.t2->toOne()})->sort([~k->ascending(), ~t->ascending()])"),
            new Case("list_corr", "corr([1, 2], [10, 20])"),
            new Case("list_covar", "covarPopulation([1, 3.0, 6.0], [10, -20, 30])"),
            new Case("list_mode_mixed", "mode([3, 3.0, 3, 2, 2])"),
            new Case("tds_mode_mixed", "mode([3, 3.0, 3, 2, 2])", TypedQuery.Output.TDS_TEXT),
            new Case("join_no_strings", "joinStrings(['a']->tail(), ',')"),
            new Case("join_no_strings_brackets", "joinStrings(['a']->tail(), '[', ',', ']')"),
            new Case("reverse_empty", "''->reverseString()"),
            new Case("left_empty", "''->left(3)"),
            new Case("index_of_one", "'c'->indexOf('c')"),
            new Case("right_digits", "'000123456789'->right(4)"),
            new Case("tds_right_digits", "'000123456789'->right(4)", TypedQuery.Output.TDS_TEXT),
            new Case("tds_join_no_strings", "joinStrings(['a']->tail(), ',')", TypedQuery.Output.TDS_TEXT),
            new Case("map_get", "newMap(pair('key1', 1))->get('key1')"),
            new Case("map_keys", "newMap([pair('a', 1), pair('b', 2)])->keys()->sort()"),
            new Case("map_put", "newMap(pair('a', 1))->put('b', 2)->get('b')"),
            new Case("remove_duplicates_by", "[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicatesBy({x | $x->toString()})"),
            new Case("remove_duplicates_by_prefix", "['hello', 'helloWorld', 'world']->removeDuplicatesBy(x:String[1]"
                    + " | $x->substring(0, 1))"),
            new Case("drop_in_list", "list([1, 2, 3]->drop(1))"),
            new Case("parse_date_zone", "parseDate('2014-02-27T10:01:35.231-0500')"),
            new Case("format_int", "format('the quick brown %s jumps over the lazy %d', ['fox', -3])"),
            new Case("format_float_rounding", "format('the quick brown %s jumps over the lazy %.2f', ['fox', 1.5])"),
            new Case("format_float_truncation", "format('%.1f and %.0f', [1.56, 2.5])"),
            new Case("format_float_zero_padding", "format('%.4f', [1.5])"),
            new Case("parse_date_zone_colon", "parseDate('2014-02-27T10:01:35.231+05:00')"),
            new Case("instant_wall", "%2014-02-27T10:01:35.231+0000->toString()"),
            new Case("find_length", "['smith', 'branche', 'doe']->find(p | $p->length() < 4)"),
            new Case("tds_list_corr", "corr([1, 2], [10, 20])", TypedQuery.Output.TDS_TEXT),
            new Case("tds_list_corr_empty", "corr([1], [])", TypedQuery.Output.TDS_TEXT),
            new Case("tds_list_corr_three", "corr([1, 3.0, 6.0], [10, -20, 30])", TypedQuery.Output.TDS_TEXT),
            new Case("variant_join_null", "#TDS\n id:Integer, payload:meta::pure::metamodel::variant::Variant\n 1, '[\"1\",\"2\"]'\n"
                    + " 4, 'null'\n#->extend(~joined: x | $x.payload->toMany(@String)->joinStrings(','))"
                    + "->sort(~id->ascending())->select(~[id, joined])"),
            new Case("tds_variant_join_null", "#TDS\n id:Integer, payload:meta::pure::metamodel::variant::Variant\n"
                    + " 1, '[\"1\",\"2\"]'\n 4, 'null'\n#->extend(~joined: x | $x.payload->toMany(@String)->joinStrings(','))"
                    + "->sort(~id->ascending())->select(~[id, joined])", TypedQuery.Output.TDS_TEXT),
            new Case("pair_to_string", "'(' + [pair('a', 'b'), pair('a', 1)]->map(p | $p->toString())->joinStrings(',') + ')'"),
            new Case("ascii_char", "[('A'->ascii()), ('z'->ascii()), (' '->ascii())]->map(c | $c->char())->joinStrings('')"),
            new Case("ascii_codes", "['A', 'z', '0', ' ']->map(c | $c->ascii())"),
            new Case("bit_shifts", "[1->bitShiftLeft(4), 256->bitShiftRight(3), -16->bitShiftRight(2)]"),
            new Case("time_bucket_seconds", "%2024-01-31T00:32:34.999+0000->timeBucket(18, DurationUnit.SECONDS)", TypedQuery.Output.TDS_TEXT),
            new Case("time_bucket_seconds_old", "%1951-10-21T08:59:00.676+0000->timeBucket(3, DurationUnit.SECONDS)", TypedQuery.Output.TDS_TEXT),
            new Case("time_bucket_minutes", "%2024-01-31T00:32:34.999+0000->timeBucket(7, DurationUnit.MINUTES)", TypedQuery.Output.TDS_TEXT),
            new Case("time_bucket_hours", "%2024-01-31T00:32:34.999+0000->timeBucket(5, DurationUnit.HOURS)", TypedQuery.Output.TDS_TEXT),
            new Case("time_bucket_days", "%2024-01-31T00:32:34.999+0000->timeBucket(3, DurationUnit.DAYS)", TypedQuery.Output.TDS_TEXT),
            new Case("time_bucket_weeks", "%2024-01-31T00:32:34.999+0000->timeBucket(2, DurationUnit.WEEKS)", TypedQuery.Output.TDS_TEXT),
            new Case("time_bucket_months", "%2024-05-31T00:32:34.999+0000->timeBucket(5, DurationUnit.MONTHS)", TypedQuery.Output.TDS_TEXT),
            new Case("time_bucket_years", "%2024-05-31T00:32:34.999+0000->timeBucket(3, DurationUnit.YEARS)", TypedQuery.Output.TDS_TEXT),
            new Case("time_bucket_years_old", "%1951-10-21T08:59:00.676+0000->timeBucket(2, DurationUnit.YEARS)", TypedQuery.Output.TDS_TEXT),
            new Case("day_of_week", "[%1985-01-12->dayOfWeek(), %2025-09-22->dayOfWeek()]"),
            new Case("format_date_12h", "[format('on %t{yyyy-MM-dd h:mm:ssa}', %2014-03-10T13:07:44.001),"
                    + " format('on %t{yyyy-MM-dd hh:mm:ssa}', %2014-03-10T01:07:44.001)]"),
            new Case("variant_distinct_null", "#TDS\n id:Integer, payload:meta::pure::metamodel::variant::Variant\n"
                    + " 2, '[1,2,2,3]'\n 3, 'null'\n#->extend(~[d: x | $x.payload->toMany(@Integer)->distinct()->toVariant(),"
                    + " r: x | $x.payload->toMany(@Integer)->removeDuplicates()->toVariant()])->sort(~id->ascending())"
                    + "->select(~[id, d, r])"),
            new Case("variant_deep", "#TDS\n id:Integer, payload:meta::pure::metamodel::variant::Variant\n"
                    + " 1, '[{\"key\":[{\"innerKey\":[1,2]}]}]'\n#->extend(~t: x | $x.payload)->select(~[id, t])"),
            new Case("format_negative_zero", "format('%.2f', [-0.001])"),
            new Case("tds_variant_join_pct", "#TDS\n id, payload:meta::pure::metamodel::variant::Variant\n 1, '[1,2,3]'\n"
                    + " 4, 'null'\n#->extend(~joined: x | $x.payload->toMany(@String)->joinStrings(','))",
                    TypedQuery.Output.TDS_TEXT),
            new Case("tds_char_digit", "48->char()", TypedQuery.Output.TDS_TEXT),
            new Case("tds_format_date_plain", "format('on %t{yyyy-MM-dd}', %2014-03-10T12:00:00)", TypedQuery.Output.TDS_TEXT),
            new Case("tds_format_strict_date", "format('on %t{yyyy-MM-dd}', %2014-03-10)", TypedQuery.Output.TDS_TEXT),
            new Case("bit_shift_62", "1->bitShiftLeft(53)->bitShiftRight(0)"),
            new Case("tds_time_bucket_nanos", "%2024-01-31T00:32:34.999123456+0000->timeBucket(2, DurationUnit.DAYS)",
                    TypedQuery.Output.TDS_TEXT),
            new Case("big_float_abs", "123456789123456789.99->abs()"),
            new Case("regex_extract_whole", "'abc'->regexpExtract('([a-z])(b)', false, 0)"),
            new Case("regex_extract_group", "'abc'->regexpExtract('([a-z])(b)', false, 2)"),
            new Case("regex_index", "['It was the best'->regexpIndexOf('was'), 'abc'->regexpIndexOf('zzz'), 'abc123def'->regexpIndexOf('([a-z]+)([0-9]+)', 2)]"),
            new Case("arg_max_min", SCORES + "->groupBy(~g, ~[a: x | meta::pure::functions::math::mathUtility::rowMapper($x.n, $x.s)"
                    + " : y | $y->maxBy(), b: x | meta::pure::functions::math::mathUtility::rowMapper($x.n, $x.s) : y | $y->minBy()])"
                    + "->sort(~g->ascending())"),
            new Case("join_full", "#TDS\n id, name\n 1, George\n 2, Pierre\n#->join(#TDS\n id2, col\n 1, More\n 3, Other\n#,"
                    + " JoinKind.FULL, {x, y | $x.id == $y.id2})->sort([~id->ascending(), ~id2->ascending()])"),
            new Case("join_right", "#TDS\n id, name\n 1, George\n 2, Pierre\n#->join(#TDS\n id2, col\n 1, More\n 3, Other\n#,"
                    + " JoinKind.RIGHT, {x, y | $x.id == $y.id2})->sort(~id2->ascending())"),
            new Case("range_one", "range(4)"),
            new Case("static_pivot", "#TDS\n city, country, year, treePlanted\n NYC, USA, 2011, 5000\n NYC, USA, 2000, 5000\n"
                    + " SAN, USA, 2011, 2600\n LDN, UK, 2011, 3000\n#->pivot(~year, [2000, 2011], ~newCol : x | $x.treePlanted"
                    + " : y | $y->plus())->sort(~city->ascending())"),
            new Case("tds_static_pivot", "#TDS\n city, country, year, treePlanted\n NYC, USA, 2011, 5000\n NYC, USA, 2000, 5000\n"
                    + " SAN, USA, 2011, 2600\n LDN, UK, 2011, 3000\n#->pivot(~year, [2000, 2011], ~newCol : x | $x.treePlanted"
                    + " : y | $y->plus())->sort(~city->ascending())", TypedQuery.Output.TDS_TEXT),
            new Case("tds_static_pivot_cast", "#TDS\n city, country, year, treePlanted\n NYC, USA, 2011, 5000\n"
                    + " NYC, USA, 2000, 5000\n SAN, USA, 2011, 2600\n LDN, UK, 2011, 3000\n#->pivot(~year, [2000, 2011],"
                    + " ~newCol : x | $x.treePlanted : y | $y->plus())->cast(@Relation<(city:String, country:String,"
                    + " '\\'2000__|__newCol\\'':Integer, '\\'2011__|__newCol\\'':Integer)>)->sort(~city->ascending())",
                    TypedQuery.Output.TDS_TEXT),
            new Case("regex_replace_first", "'abc'->regexpReplace('(b|c)', 'X', false)"),
            new Case("regex_replace_all", "'abc'->regexpReplace('(b|c)', 'X', true)"),
            new Case("regex_replace_many", "'abcabc'->regexpReplace('a(b)', '[x]', true)"),
            new Case("guid_length", "meta::pure::functions::string::generation::generateGuid()->length()"),
            new Case("hash_forms", "hashCode([10]) == hashCode([[10]])"),
            // the cross-lane pass (2026-10-17): what other targets compose without a native, composed here too
            new Case("base64_encode", "'Hello, World!'->encodeBase64()"),
            new Case("base64_decode", "'SGVsbG8sIFdvcmxkIQ=='->decodeBase64()"),
            new Case("base64_decode_unpadded", "'SGVsbG8sIFdvcmxkIQ'->decodeBase64()"),
            new Case("base64_round_trip", "'Any Random String'->encodeBase64()->decodeBase64()"),
            new Case("levenshtein_equal", "'John Smith'->levenshteinDistance('John Smith')"),
            new Case("levenshtein_not_equal", "'John Smith'->levenshteinDistance('Jane Smith')"),
            new Case("levenshtein_empty", "''->levenshteinDistance('abc')"),
            new Case("jaro_winkler_equal", "'John Smith'->jaroWinklerSimilarity('John Smith')"),
            new Case("jaro_winkler_not_equal", "'John Smith'->jaroWinklerSimilarity('Jane Smith')"),
            new Case("jaro_winkler_none", "'abc'->jaroWinklerSimilarity('xyz')"),
            new Case("jaro_winkler_transposed", "'martha'->jaroWinklerSimilarity('marhta')"),
            // a value root's date: the precision the value has (a whole second without a fraction)
            new Case("tds_date_whole_second", "%2015-04-15T13:12:11->adjust(1, DurationUnit.HOURS)",
                    TypedQuery.Output.TDS_TEXT),
            new Case("tds_date_millis", "%2015-04-15T13:12:11.250->adjust(1, DurationUnit.HOURS)",
                    TypedQuery.Output.TDS_TEXT),
            new Case("tds_date_part", "%1973-11-05T13:01:25->datePart()", TypedQuery.Output.TDS_TEXT),
            new Case("tds_drop_list", "[1, 2, 3]->drop(1)", TypedQuery.Output.TDS_TEXT),
            new Case("tds_take_list", "[1, 2, 3]->take(2)", TypedQuery.Output.TDS_TEXT),
            new Case("tds_slice_list", "[1, 2, 3, 4]->slice(1, 3)", TypedQuery.Output.TDS_TEXT),
            new Case("greatest_mixed", "greatest([4.23, 7.345, 1.0, 3, 4])"),
            new Case("least_mixed", "least([1.23, 2])"),
            new Case("max_numbers", "max([1.23, 2, 7.0])"),
            new Case("window_qualify", SCORES + "->extend(over(~g, ~s->descending()), ~r: {p, w, r | $p->rank($w, $r)})"
                    + "->filter(x | $x.r == 1)->sort(~id->ascending())->select(~[id, s])"));

    @Test
    void differential() throws Exception {
        // the reference is the platform's engine, the subject this test's database (their test backends' runtimes)
        String duck = com.legend.testbackend.TestBackends.platform().withStorelessRuntime(CLASSES);
        String mongo = com.legend.testbackend.TestBackends.of(DatabaseType.MongoDB).withStorelessRuntime(CLASSES);
        List<String> lines = new ArrayList<>();
        Map<String, Integer> counts = new TreeMap<>();
        List<String> referenceFailures = new ArrayList<>();
        for (Case c : CASES) {
            System.out.println("[mongodb-differential] running " + c.name());
            String reference;
            try {
                reference = run(duck, c.expression(), c.output());
            } catch (RuntimeException e) {
                referenceFailures.add(c.name() + ": " + e.getClass().getSimpleName() + ": " + first(e.getMessage()));
                lines.add(c.name() + "\tREFERENCE_FAILED\t" + first(e.getMessage()) + "\t");
                counts.merge("REFERENCE_FAILED", 1, Integer::sum);
                continue;
            }
            String verdict;
            String detail;
            try {
                String answer = run(mongo, c.expression(), c.output());
                verdict = answer.equals(reference) ? "EQUAL" : "DIFFERENT";
                detail = verdict.equals("EQUAL") ? "" : "reference=" + flat(reference) + " mongodb=" + flat(answer);
            } catch (com.legend.sql.dialect.DialectCapability e) {
                verdict = "REFUSED";
                detail = first(e.getMessage());
            } catch (com.legend.error.DataError e) {
                verdict = "ERROR";
                detail = first(e.getMessage());
            } catch (RuntimeException e) {
                verdict = "ERROR";
                detail = e.getClass().getSimpleName() + ": " + first(e.getMessage());
            }
            counts.merge(verdict, 1, Integer::sum);
            lines.add(c.name() + "\t" + verdict + "\t" + detail + "\t" + flat(reference));
            if (!verdict.equals("EQUAL")) {
                // the pipeline beside a verdict that is not EQUAL, so the log says what MongoDB was asked
                System.out.println("[mongodb-differential] " + c.name() + " pipeline: " + pipelineOf(mongo, c));
            }
        }
        String out = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (out != null) {
            Files.write(Path.of(out, "mongodb-differential.tsv"), lines, StandardCharsets.UTF_8);
        }
        System.out.println("[mongodb-differential] " + CASES.size() + " cases: " + counts);
        for (String line : lines) {
            System.out.println("[mongodb-differential] " + line);
        }
        assertTrue(referenceFailures.isEmpty(), "the reference (the platform's engine) did not run: " + referenceFailures);
    }

    /** The case's pipeline text, or the refusal that stood in its way. */
    private static String pipelineOf(String model, Case c) {
        try {
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), c.expression())
                    .executionPlan(com.legend.test.StorelessRuntime.RUNTIME, c.output());
            StringBuilder sb = new StringBuilder();
            statements(plan.root(), sb);
            return sb.toString();
        } catch (RuntimeException e) {
            return e.getClass().getSimpleName() + ": " + first(e.getMessage());
        }
    }

    private static void statements(ExecutionPlan.Node node, StringBuilder sb) {
        switch (node) {
            case ExecutionPlan.Sequence seq -> seq.steps().forEach(n -> statements(n, sb));
            case ExecutionPlan.TdsResult t -> sb.append(t.sql().statement()).append(' ');
            case ExecutionPlan.TextResult t -> sb.append(t.sql().statement()).append(' ');
            default -> sb.append("<").append(node.getClass().getSimpleName()).append("> ");
        }
    }

    private static String run(String model, String expression, TypedQuery.Output output) throws Exception {
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), expression)
                .executionPlan(com.legend.test.StorelessRuntime.RUNTIME, output);
        // the plan path program's round-trip gate (step 1b): every case's plan, on both databases
        com.legend.testcases.PlanRoundTrip.assertRoundTrips(plan, expression);
        StringWriter out = new StringWriter();
        PlanRunner.run(plan, Map.of(), PlanSessions.shared(), out);
        return out.toString();
    }

    private static String flat(String s) {
        return s.replace("\n", "\\n").replace("\t", " ");
    }

    private static String first(@com.legend.base.Nullable String message) {
        String m = String.valueOf(message);
        int nl = m.indexOf('\n');
        return (nl < 0 ? m : m.substring(0, nl)).replace('\t', ' ');
    }
}
