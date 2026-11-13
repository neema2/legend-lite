"""legend_lite.scalar_parallel, by level: the compute tree (traced, evaluated, differentiated), the interpolation basis (knots,
basis nodes, expansion), and the basis in Pure (the compute-basis tables and queries, planned for every database the
module names, run on DuckDB and held to Python's own evaluation). No domain: the series and measures are made up."""

import math
import unittest

from legend_lite.scalar_parallel import basis as b
from legend_lite.scalar_parallel import expr as e
from legend_lite.scalar_parallel import basis_pure as p


def fd(f, env, name, h=1e-6):
    up, dn = dict(env), dict(env)
    up[name] += h
    dn[name] -= h
    return (e.evaluate(f, up) - e.evaluate(f, dn)) / (2 * h)


class ComputeTree(unittest.TestCase):
    ENV = {'x': 0.5, 'y': 2.0, 'z': 1.3}

    def test_a_value_written_twice_is_one_node(self):
        x, y = e.Var('x'), e.Var('y')
        self.assertIs(3.0 * x * y + 1.0, 3.0 * x * y + 1.0)

    def test_constants_fold_and_identities_vanish(self):
        x = e.Var('x')
        self.assertIs(x * 1.0 + 0.0, x)
        self.assertIs(x * 0.0, e.ZERO)
        self.assertEqual((e.lift(2.0) * 3.0 + 1.0).value, 7.0)

    def test_it_evaluates_as_python_does(self):
        x, y = e.Var('x'), e.Var('y')
        self.assertAlmostEqual(e.evaluate(3.0 * x * y + e.exp(x) / y - e.log(y), self.ENV),
                               3.0 + math.exp(0.5) / 2.0 - math.log(2.0), places=14)

    def test_derivatives_are_exact(self):
        x, y, z = e.Var('x'), e.Var('y'), e.Var('z')
        for f in (
            x * y * z,
            x / (1.0 + y * z),
            e.exp(x * y) - e.log(z),
            (x - y) * (x - y) / z,
            e.sqrt(x * y + z),
            abs(x - y) * z,
            e.erf(x * y - 0.3 * z),
        ):
            for v in (x, y, z):
                want = fd(f, self.ENV, v.name)
                self.assertAlmostEqual(e.evaluate(e.diff(f, v), self.ENV), want, delta=1e-7 * max(1.0, abs(want)))

    def test_the_tree_is_walked_without_recursion(self):
        x = e.Var('x')
        s = e.ZERO
        for i in range(20000):
            s = s + x * float(i + 1)
        self.assertAlmostEqual(e.evaluate(s, {'x': 1.0}), 20000 * 20001 / 2)
        self.assertEqual(e.evaluate(e.diff(s, x), {'x': 1.0}), 20000 * 20001 / 2)

    def test_variables_and_constancy(self):
        x, y = e.Var('x'), e.Var('y')
        self.assertEqual(e.variables(x * y + x), [x, y])
        self.assertTrue(e.is_constant(e.lift(2.0) * 3.0))
        self.assertFalse(e.is_constant(x * 3.0))

    def test_scalar_expr_and_derivatives_lower_to_pure_lambdas_and_trees(self):
        from legend_lite import compiler
        x, y = e.Var('x'), e.Var('y')
        # An expression outside the degree-3 polynomial basis (exp(x * y) / log(y)) and its symbolic derivative
        f = 3.0 * x * y + e.exp(x * y) / e.log(y) - 2.5
        df_dx = e.diff(f, x)
        for expr_node in (f, df_dx):
            pure_lambda = e.to_pure(expr_node)
            self.assertTrue(pure_lambda.startswith('{x, y | '), pure_lambda)
            tree = e.to_tree(expr_node)
            self.assertEqual(tree['_type'], 'lambda')
            roundtrip = compiler.print_tree(tree, 'STANDARD')
            self.assertIn('exp(', roundtrip)
            row_expr = e.to_pure(expr_node, row_param='x', as_lambda=False)
            self.assertIn('$x.x->toOne()', row_expr)
            self.assertIn('$x.y->toOne()', row_expr)
            self.assertEqual(compiler.parse(f'x|{row_expr}')['_type'], 'lambda')


S = 'S'
AXIS = b.PiecewiseLinear(S, [1.0, 2.0, 5.0])
ENV = {b.knot_name(S, 0): 0.03, b.knot_name(S, 1): 0.04, b.knot_name(S, 2): 0.045}


class InterpolationBasis(unittest.TestCase):
    def test_a_coordinate_falls_between_two_knots_or_flat_beyond(self):
        self.assertEqual(AXIS.at(3.5), b.Interp(S, 1, 2, 0.5))
        self.assertEqual(AXIS.at(0.2), b.Interp(S, 0, 0, 0.0))
        self.assertEqual(AXIS.at(9.0), b.Interp(S, 2, 2, 0.0))
        self.assertEqual(AXIS.at(2.0), b.Interp(S, 1, 1, 0.0))

    def test_a_node_evaluates_as_cbsEvalNode(self):
        self.assertAlmostEqual(e.evaluate(AXIS.lerp(3.5), ENV), 0.0425, places=15)
        self.assertAlmostEqual(e.evaluate(AXIS.exp_neg(3.5, 3.5), ENV), math.exp(-0.0425 * 3.5), places=15)
        self.assertAlmostEqual(e.evaluate(AXIS.recip_1pxt(1.5, 2.0), ENV), 1.0 / (1.0 + 0.035 * 2.0), places=15)

    def test_a_node_derivative_is_exact_and_stays_in_the_basis(self):
        for n in (AXIS.exp_neg(3.5, 3.5), AXIS.lerp(1.25), AXIS.recip_1pxt(4.0, 2.0)):
            for k in AXIS.knots:
                d = e.diff(n, k)
                want = fd(n, ENV, k.name)
                self.assertAlmostEqual(e.evaluate(d, ENV), want, delta=1e-7)
                b.expand(d)   # expands: the basis is closed under diff
        self.assertEqual(set(b.expand(e.diff(AXIS.recip_1pxt(4.0, 2.0), AXIS.knot(1)))),
                         {(AXIS.recip_1pxt(4.0, 2.0),) * 2})

    def test_a_polynomial_of_nodes_expands_to_its_monomials(self):
        u, w = AXIS.exp_neg(1.5, 1.5), AXIS.lerp(4.0)
        self.assertEqual(b.expand(2.0 * u * w + 3.0 * u - 1.0 + u), {b._ordered((u, w)): 2.0, (u,): 4.0, (): -1.0})
        self.assertEqual(b.expand(AXIS.knot(2) * 2.0), {(b.node(b.LERP, S, 2, 2, 0.0),): 2.0})

    def test_exponential_nodes_divide_and_combine_at_shared_knots(self):
        # 1 / exp(-x t) = exp(+x t), and exp(-x t1) * exp(+x t2) on the same knot pair folds to exp(-x (t1 - t2))
        u1, u2 = AXIS.exp_neg(2.0, 2.0), AXIS.exp_neg(3.5, 3.5)
        ratio = u1 / u2
        self.assertEqual(set(b.expand(ratio)), {b._ordered((u1, AXIS.exp_neg(3.5, -3.5)))})
        self.assertAlmostEqual(e.evaluate(ratio, ENV), e.evaluate(u1, ENV) / e.evaluate(u2, ENV), places=15)
        # u2 * (1 / u2) cancels to 1.0
        self.assertEqual(b.expand(u2 / u2), {(): 1.0})

    def test_dynamic_expansion_handles_higher_degree_and_nonlinear_formulas(self):
        w = AXIS.lerp(4.0)
        u = AXIS.lerp(1.5)
        # Degree-4 product of basis functions expands cleanly without a static 3-node ceiling
        poly4 = b.expand(w * w * u * u)
        self.assertEqual(poly4, {b._ordered((w, w, u, u)): 1.0})
        # exp of a linear knot expression lowers directly to an EXP_NEG_XT BasisNode
        self.assertEqual(b.expand(e.exp(AXIS.knot(0))), {(b.BasisNode(b.EXP_NEG_XT, S, 0, 0, 0.0, -1.0, 0.0),): 1.0})
        # General non-polynomial expressions (1/w, log, sqrt, exp of products) expand as dynamic formula factors
        for expr_node in (w * w * w * w, 1.0 / w, e.log(w / u) * e.sqrt(w + u) * AXIS.exp_neg(3.5, 3.5)):
            expanded = b.expand(expr_node)
            self.assertGreater(len(expanded), 0)
            self.assertEqual(e.to_tree(expr_node)['_type'], 'lambda')
        # Optional strict mode still enforces static degree/polynomial bounds when explicitly requested
        for bad in (w * w * w * w, 1.0 / w):
            with self.assertRaises(b.BasisError):
                b.expand(bad, max_degree=3, strict=True)


class BasisInPure(unittest.TestCase):
    """A made-up book over one series: each entity a different mix of basis functions, and its derivatives."""

    @classmethod
    def setUpClass(cls):
        cls.book = p.BasisBook()
        cls.expected = {}
        cache = {}
        for i in range(40):
            c = 0.25 * i
            # each derivative stays of degree three at most, including an exponential ratio times a discount factor
            f = ((i + 1.0) * AXIS.exp_neg(c, c) - 0.5 * AXIS.lerp(c) * AXIS.exp_neg(c + 1.0, 2.0)
                 + 0.3 * (AXIS.exp_neg(c, c) / AXIS.exp_neg(c + 0.5, c + 0.5)) * AXIS.exp_neg(c + 1.25, 1.25)
                 + 0.2 * AXIS.recip_1pxt(c + 0.5, 1.0) + float(i % 3))
            group = f'G{i % 4}'
            cls.book.add(f'E{i}', group, 'VALUE', '*', f)
            cls.expected[(f'E{i}', 'VALUE', '*')] = e.evaluate(f, ENV, cache)
            for k in AXIS.knots:
                d = e.diff(f, k)
                if d is not e.ZERO:
                    cls.book.add(f'E{i}', group, 'SLOPE', k.name, d)
                    cls.expected[(f'E{i}', 'SLOPE', k.name)] = e.evaluate(d, ENV, cache)
        knots = [(f'S:{c:g}', c, ENV[b.knot_name(S, i)]) for i, c in enumerate(AXIS.coords)]
        cls.scenario = p.Scenario('BASE', {S: knots})
        cls.jacobian = [p.JacobianEntry('BASE', k.name, f'IN{j}', 1.0 if k.index == j else 0.5) for k in AXIS.knots
                        for j in range(2)]
        cls.cb = p.ComputeBasis()
        cls.con = cls.cb.connect(cls.book.tables([cls.scenario], cls.jacobian))

    @classmethod
    def tearDownClass(cls):
        cls.cb.close()

    def test_each_node_is_written_once(self):
        rows = self.book.rows([self.scenario])
        ids = [r['NODE_ID'] for r in rows['CBS_SHARED_NODE']]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(len(ids), len(self.book.nodes))
        self.assertEqual(len(rows['CBS_BASIS_TERM']), self.book.term_count())

    def test_every_query_plans_for_every_database(self):
        for db in p.DATABASES:
            for query in (p.measures_query(), p.chained_query('SLOPE'), p.stored_measures_query()):
                self.assertIn('SELECT', self.cb.plan(query, db).sql.upper(), db)

    def test_each_node_is_evaluated_in_one_place(self):
        sql = self.cb.plan(p.measures_query()).sql
        # the node subselect is written for node A, node B and node C, nowhere else
        self.assertEqual(sql.count("'EXP_NEG_XT'"), 3)

    def test_measures_valued_in_sql_are_pythons(self):
        got = {(r['entityId'], r['measureCode'], r['factorId']): r['value']
               for r in self.cb.run(self.con, p.measures_query('BASE')).to_pylist()}
        self.assertEqual(set(got), set(self.expected))
        for k, v in self.expected.items():
            self.assertAlmostEqual(got[k], v, delta=1e-12 * max(1.0, abs(v)))

    def test_stored_and_live_values_agree(self):
        stored = {(r['entityId'], r['measureCode'], r['factorId']): r['value']
                  for r in self.cb.run(self.con, p.stored_measures_query()).to_pylist()}
        for k, v in self.expected.items():
            self.assertAlmostEqual(stored[k], v, delta=1e-12 * max(1.0, abs(v)))

    def test_the_chain_rule_in_sql(self):
        by_group: dict = {}
        for (entity, measure, factor), v in self.expected.items():
            if measure == 'SLOPE':
                group = f'G{int(entity[1:]) % 4}'
                for j in self.jacobian:
                    if j.factor_id == factor:
                        key = (group, j.input_id)
                        by_group[key] = by_group.get(key, 0.0) + v * j.weight
        got = {(r['groupId'], r['inputId']): r['chainedValue']
               for r in self.cb.run(self.con, p.chained_query('SLOPE')).to_pylist()}
        self.assertEqual(set(got), set(by_group))
        for k, v in by_group.items():
            self.assertAlmostEqual(got[k], v, delta=1e-12 * max(1.0, abs(v)))

    def test_plan_typed_results_and_tree_queries(self):
        from legend_lite import compiler
        from legend_lite.frames import TYPE_KEY
        q_text = p.measures_query('BASE')
        q_tree = p.query_tree(q_text)
        self.assertIsInstance(self.cb.held(), compiler.HeldModel)
        plan_from_text = self.cb.plan(q_text)
        plan_from_tree = self.cb.plan(q_tree)
        self.assertIsInstance(plan_from_text, compiler.Plan)
        self.assertEqual(plan_from_text.sql, plan_from_tree.sql)
        self.assertEqual(self.cb.relation_type(q_tree), plan_from_text.columns)

        table = self.cb.run(self.con, q_tree)
        for col_spec, field in zip(plan_from_text.columns, table.schema):
            self.assertEqual(field.name, col_spec.name)
            self.assertEqual((field.metadata or {}).get(TYPE_KEY), col_spec.type.encode('utf-8'))

    def test_store_schemas_and_aggregation_aware_exposure_and_frames(self):
        schemas = p.store_schemas()
        self.assertEqual(set(schemas), {'CBS_KNOT', 'CBS_SHARED_NODE', 'CBS_BASIS_TERM', 'CBS_JACOBIAN_ENTRY'})

        # exposure_query over CbsMeasureExposure routes via AggregationAware to the store View CBS_ENTITY_MEASURE (TOTAL_VALUE)
        exp_q = p.exposure_query('BASE', by_entity=True, measure_code='VALUE')
        exp_plan = self.cb.plan(exp_q)
        self.assertIn('TOTAL_VALUE', exp_plan.sql.upper())
        got_exp = {r['entityId']: r['value'] for r in self.cb.run(self.con, exp_q).to_pylist()}
        for (entity, measure, factor), v in self.expected.items():
            if measure == 'VALUE':
                self.assertAlmostEqual(got_exp[entity], v, delta=1e-12 * max(1.0, abs(v)))

        # Registering tables into Frames admits the full multi-table compute_basis model and runs queries over Frames
        frames = self.book.frames([self.scenario], self.jacobian)
        try:
            self.assertIsNotNone(frames.model_for(self.cb.model()))
            via_frames = {r['entityId']: r['value'] for r in self.cb.run(frames, exp_q).to_pylist()}
            self.assertEqual(via_frames, got_exp)
        finally:
            frames.close()

    def test_warehouse_execution_returns_plan_typed_arrow_table(self):
        from unittest import mock
        import pyarrow as pa
        import pyarrow.ipc
        from legend_lite import warehouse
        from legend_lite.frames import TYPE_KEY

        q = p.measures_query('BASE', by_entity=False, measure_code='VALUE')
        expected_table = self.cb.run(self.con, q)
        sink = pa.BufferOutputStream()
        with pyarrow.ipc.new_stream(sink, expected_table.schema) as writer:
            writer.write_table(expected_table)
        chunk_bytes = sink.getvalue().to_pybytes()

        sess = warehouse.Session('https://wh.example', 'tok', 'alice', '2099-01-01T00:00:00Z')
        wh = warehouse.Warehouse(sess)
        with mock.patch.object(wh, 'arrow_chunks', return_value=iter([chunk_bytes])) as mocked:
            got_table = self.cb.run(wh, q)
            mocked.assert_called_once()
        self.assertEqual(got_table.to_pylist(), expected_table.to_pylist())
        for field in got_table.schema:
            self.assertIn(TYPE_KEY, field.metadata or {})

    def test_dynamic_table_sizing_for_higher_degree_products_and_option_formulas(self):
        """Verifies that a BasisBook containing >3-node products and Black-Scholes / CND non-linear formulas
        dynamically synthesizes the Pure model and evaluates both values and exact symbolic derivatives in SQL."""
        dyn_book = p.BasisBook()
        w1 = AXIS.lerp(1.25)
        w2 = AXIS.lerp(2.5)
        w3 = AXIS.lerp(3.5)
        w4 = AXIS.lerp(4.5)
        df = AXIS.exp_neg(3.5, 3.5)

        # 1. Product of two 2-parameter basis expressions requiring 4 node slots (NODE_A_ID .. NODE_D_ID)
        deg4_expr = (2.0 * w1 * w2 + df) * (1.5 * w3 * w4 - w1)
        # 2. Non-polynomial formula with log, sqrt, exp(-0.5 * z^2), and rational reciprocal
        d1 = (e.log(w2 / 0.035) + 0.5 * w3 * w3 * 2.5) / (w3 * e.sqrt(2.5))
        d2 = d1 - w3 * e.sqrt(2.5)
        opt_expr = 1000.0 * df * (w2 * e.exp(-0.5 * d1 * d1) - 0.035 * e.exp(-0.5 * d2 * d2)) + 10.0 / (1.0 + w1 * w4)

        want: dict[tuple[str, str, str], float] = {}
        for eid, expr_node in (('DEG4', deg4_expr), ('OPT', opt_expr)):
            dyn_book.add(eid, 'DYN_GRP', 'VALUE', '*', expr_node)
            want[(eid, 'VALUE', '*')] = e.evaluate(expr_node, ENV)
            for k in AXIS.knots:
                d = e.diff(expr_node, k)
                if d is not e.ZERO:
                    dyn_book.add(eid, 'DYN_GRP', 'SLOPE', k.name, d)
                    want[(eid, 'SLOPE', k.name)] = e.evaluate(d, ENV)

        sch = dyn_book.schema()
        self.assertGreaterEqual(sch.num_nodes, 4)
        self.assertGreaterEqual(sch.num_params, 1)
        self.assertGreaterEqual(len(sch.formulas), 1)

        with p.ComputeBasis(dyn_book) as dyn_cb:
            tables = dyn_book.tables([self.scenario], self.jacobian)
            self.assertIn('NODE_D_ID', tables['CBS_BASIS_TERM'].column_names)
            self.assertIn('PARAM_1', tables['CBS_BASIS_TERM'].column_names)
            for db in p.DATABASES:
                self.assertIn('SELECT', dyn_cb.plan(p.measures_query(book_or_schema=dyn_book), db).sql.upper(), db)
            con = dyn_cb.connect(tables)
            got = {
                (r['entityId'], r['measureCode'], r['factorId']): r['value']
                for r in dyn_cb.run(con, p.measures_query('BASE', book_or_schema=dyn_book)).to_pylist()
            }
            self.assertEqual(set(got), set(want))
            for k, v in want.items():
                self.assertAlmostEqual(got[k], v, delta=1e-10 * max(1.0, abs(v)), msg=str(k))

    def test_pure_native_symbolic_diff(self):
        """Verifies that Pure's built-in meta::legend::lite::diff symbolically differentiates
        user-defined Pure functions (including Black-Scholes and 2nd-order Gamma) and lowers to SQL."""
        import duckdb
        import math
        import textwrap
        from legend_lite import compiler

        model_src = textwrap.dedent("""\
            ###Pure
            Class demo::Option {
                id: String[1];
                spot: Float[1];
                strike: Float[1];
                vol: Float[1];
                rate: Float[1];
                expiry: Float[1];
            }
            ###Relational
            Database demo::DB (
                Table OPT (
                    ID VARCHAR(32),
                    SPOT DOUBLE,
                    STRIKE DOUBLE,
                    VOL DOUBLE,
                    RATE DOUBLE,
                    EXPIRY DOUBLE
                )
            )
            ###Mapping
            Mapping demo::M (
                *demo::Option: Relational {
                    ~mainTable [demo::DB] OPT
                    id: OPT.ID,
                    spot: OPT.SPOT,
                    strike: OPT.STRIKE,
                    vol: OPT.VOL,
                    rate: OPT.RATE,
                    expiry: OPT.EXPIRY
                }
            )
            ###Connection
            RelationalDatabaseConnection demo::Conn { store: demo::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime demo::RT { mappings: [demo::M]; connections: [ demo::DB: [ c0: demo::Conn ] ]; }

            ###Pure
            function demo::normalPdf(x: Float[1]): Float[1] {
                exp(-0.5 * $x * $x) / sqrt(2.0 * pi())
            }
            function demo::cnd(x: Float[1]): Float[1] {
                0.5 * (1.0 + erf($x / sqrt(2.0)))
            }
            function demo::bsCall(s: Float[1], k: Float[1], v: Float[1], r: Float[1], t: Float[1]): Float[1] {
                let volSqrtT = $v * sqrt($t);
                let d1 = (log($s / $k) + ($r + 0.5 * $v * $v) * $t) / $volSqrtT;
                let d2 = $d1 - $volSqrtT;
                $s * demo::cnd($d1) - $k * exp(-$r * $t) * demo::cnd($d2);
            }
        """)
        query = (
            "demo::Option.all()->project(~["
            "id: o|$o.id, "
            "pv: o|demo::bsCall($o.spot, $o.strike, $o.vol, $o.rate, $o.expiry), "
            "delta: o|diff({s | demo::bsCall($s, $o.strike, $o.vol, $o.rate, $o.expiry)}, $o.spot), "
            "gamma: o|diff({s | diff({s2 | demo::bsCall($s2, $o.strike, $o.vol, $o.rate, $o.expiry)}, $s)}, $o.spot), "
            "vega: o|diff({v | demo::bsCall($o.spot, $o.strike, $v, $o.rate, $o.expiry)}, $o.vol)"
            "])"
        )
        with compiler.HeldModel(model_src) as held:
            plan = held.plan(compiler.parse("|" + query), "demo::RT")
            self.assertIn("SELECT", plan.sql.upper())
            con = duckdb.connect(":memory:")
            con.execute("CREATE TABLE OPT (ID VARCHAR, SPOT DOUBLE, STRIKE DOUBLE, VOL DOUBLE, RATE DOUBLE, EXPIRY DOUBLE)")
            con.execute("INSERT INTO OPT VALUES ('OPT1', 100.0, 100.0, 0.20, 0.05, 1.0)")
            row = con.execute(plan.sql).fetchone()
            self.assertEqual(row[0], "OPT1")
            self.assertAlmostEqual(row[1], 10.450583572185565, delta=1e-12)
            self.assertAlmostEqual(row[2], 0.6368306511756191, delta=1e-12)
            d1 = 0.35
            expected_gamma = (math.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)) / (100.0 * 0.20 * 1.0)
            self.assertAlmostEqual(row[3], expected_gamma, delta=1e-12)
            self.assertAlmostEqual(row[4], 37.52403469169379, delta=1e-12)

    def test_automatic_projection_cse_and_hierarchical_sub_measures(self):
        """Verifies:
        (1) automatic SQL ProjectionCse on multi-derivative Pure project queries (no manual split() annotations),
        (2) zero host math evaluation in BasisBook.rows()/tables() (TENET_CHARTER.md C2.1), and
        (3) hierarchical SubMeasure factoring + Pure diff() (BasisBook.add_pure_diff) keeping composite multi-sum
            nonlinear formulas narrow (<= 4 node slots) regardless of underlying sum length."""
        import duckdb
        import textwrap
        from legend_lite import compiler

        # 1. Direct multi-output Pure query using e.to_pure and e.to_pure_diff -> automatic __pc0 SQL staging
        s_var, k_var, v_var, r_var, t_var = (
            e.Var('spot'), e.Var('strike'), e.Var('vol'), e.Var('rate'), e.Var('expiry')
        )
        df = e.exp(-r_var * t_var)
        vol_sqrt_t = v_var * e.sqrt(t_var)
        d1 = (e.log(s_var / k_var) + (r_var + 0.5 * v_var * v_var) * t_var) / vol_sqrt_t
        d2 = d1 - vol_sqrt_t
        inv_sqrt2 = 1.0 / math.sqrt(2.0)
        payoff = (
            s_var * 0.5 * (1.0 + e.erf(d1 * inv_sqrt2))
            - k_var * df * 0.5 * (1.0 + e.erf(d2 * inv_sqrt2))
        )

        direct_q = (
            'demo::Option.all()->project(~['
            f"pv: {e.to_pure(payoff, row_param='o', row_to_one=False)}, "
            f"delta: {e.to_pure_diff(payoff, s_var, row_param='o', row_to_one=False)}, "
            f"vega: {e.to_pure_diff(payoff, v_var, row_param='o', row_to_one=False)}, "
            f"rho: {e.to_pure_diff(payoff, r_var, row_param='o', row_to_one=False)}"
            '])'
        )
        self.assertIn('erf(', direct_q)
        self.assertIn('diff(', direct_q)

        model_src = textwrap.dedent("""\
            ###Pure
            Class demo::Option {
                id: String[1];
                spot: Float[1];
                strike: Float[1];
                vol: Float[1];
                rate: Float[1];
                expiry: Float[1];
            }
            ###Relational
            Database demo::DB (
                Table OPT (
                    ID VARCHAR(32),
                    SPOT DOUBLE,
                    STRIKE DOUBLE,
                    VOL DOUBLE,
                    RATE DOUBLE,
                    EXPIRY DOUBLE
                )
            )
            ###Mapping
            Mapping demo::M (
                *demo::Option: Relational {
                    ~mainTable [demo::DB] OPT
                    id: OPT.ID,
                    spot: OPT.SPOT,
                    strike: OPT.STRIKE,
                    vol: OPT.VOL,
                    rate: OPT.RATE,
                    expiry: OPT.EXPIRY
                }
            )
            ###Connection
            RelationalDatabaseConnection demo::Conn { store: demo::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime demo::RT { mappings: [demo::M]; connections: [ demo::DB: [ c0: demo::Conn ] ]; }
        """)
        with compiler.HeldModel(model_src) as held:
            plan = held.plan(compiler.parse('|' + direct_q), 'demo::RT')
            self.assertIn('SELECT', plan.sql.upper())
            con = duckdb.connect(':memory:')
            con.execute('CREATE TABLE OPT (ID VARCHAR, SPOT DOUBLE, STRIKE DOUBLE, VOL DOUBLE, RATE DOUBLE, EXPIRY DOUBLE)')
            con.execute("INSERT INTO OPT VALUES ('OPT1', 100.0, 100.0, 0.20, 0.05, 1.0)")
            row = con.execute(plan.sql).fetchone()
            opt_env = {'spot': 100.0, 'strike': 100.0, 'vol': 0.20, 'rate': 0.05, 'expiry': 1.0}
            self.assertAlmostEqual(row[0], e.evaluate(payoff, opt_env), delta=1e-12)
            self.assertAlmostEqual(row[1], e.evaluate(e.diff(payoff, s_var), opt_env), delta=1e-12)
            self.assertAlmostEqual(row[2], e.evaluate(e.diff(payoff, v_var), opt_env), delta=1e-12)
            self.assertAlmostEqual(row[3], e.evaluate(e.diff(payoff, r_var), opt_env), delta=1e-12)

        # 2. Hierarchical SubMeasure factoring + Pure diff() on multi-sum composite payoffs (12, 28, and 60-period sums)
        hier_book = p.BasisBook()
        want: dict[tuple[str, str, str], float] = {}
        for idx, n_legs in enumerate((12, 28, 60)):
            eid = f'COMP_{n_legs}'
            ann_sum = e.ZERO
            flt_sum = e.ZERO
            for j in range(1, n_legs + 1):
                t_j = 0.25 * j
                t_prev = 0.25 * (j - 1)
                df_j = AXIS.exp_neg(t_j, t_j)
                df_prev = AXIS.exp_neg(t_prev, t_prev)
                ann_sum = ann_sum + 0.25 * df_j
                flt_sum = flt_sum + (df_prev / df_j - 1.0) * df_j
            vol_sum = 0.6 * AXIS.lerp(1.5) + 0.4 * AXIS.lerp(3.5)
            fwd = flt_sum / ann_sum
            strike = 0.036 + 0.002 * idx
            std = vol_sum * math.sqrt(2.0)
            d1_c = (e.log(fwd / strike) + 0.5 * vol_sum * vol_sum * 2.0) / std
            d2_c = d1_c - std
            comp_pv = 1_000_000.0 * ann_sum * (
                fwd * 0.5 * (1.0 + e.erf(d1_c * inv_sqrt2))
                - strike * 0.5 * (1.0 + e.erf(d2_c * inv_sqrt2))
            )
            hier_book.add(eid, 'GRP', 'VALUE', '*', comp_pv)
            want[(eid, 'VALUE', '*')] = e.evaluate(comp_pv, ENV)
            for k in AXIS.knots:
                hier_book.add_pure_diff(eid, 'GRP', 'SLOPE', k.name, comp_pv, k)
                d_py = e.diff(comp_pv, k)
                if d_py is not e.ZERO:
                    want[(eid, 'SLOPE', k.name)] = e.evaluate(d_py, ENV)

        sch = hier_book.schema()
        self.assertEqual(sch.max_stage, 1)
        # Even with 60-period sums inside log() and erf(), node slot width stays <= 4 and all 3 tenors share <= 3 base formulas!
        self.assertLessEqual(sch.num_nodes, 4)
        base_formulas = [f for f in sch.formulas if f.base_op is None]
        self.assertLessEqual(len(base_formulas), 3)

        # Zero host math evaluation in rows() (TENET_CHARTER.md C2.1)
        raw_rows = hier_book.rows([self.scenario])
        self.assertTrue(all('NODE_VALUE' not in r for r in raw_rows['CBS_SHARED_NODE']))
        self.assertTrue(all(r['TERM_VALUE'] == 0.0 for r in raw_rows['CBS_BASIS_TERM']))

        rendered = p.render_pure_model(sch)
        self.assertIn('diff({_dv | compute_basis::cbsFormula_', rendered)

        with p.ComputeBasis(hier_book) as h_cb:
            for db in p.DATABASES:
                self.assertIn('SELECT', h_cb.plan(p.measures_query(book_or_schema=hier_book), db).sql.upper(), db)
            con = h_cb.connect(hier_book.tables([self.scenario], self.jacobian))
            got = {
                (r['entityId'], r['measureCode'], r['factorId']): r['value']
                for r in h_cb.run(con, p.measures_query('BASE', book_or_schema=hier_book)).to_pylist()
            }
            self.assertEqual(set(got), set(want))
            for k, v in want.items():
                self.assertAlmostEqual(got[k], v, delta=1e-10 * max(1.0, abs(v)), msg=str(k))


if __name__ == '__main__':
    unittest.main()
