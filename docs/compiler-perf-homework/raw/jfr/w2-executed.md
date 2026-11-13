execution samples: 268676

| phase | innermost | share | inclusive | share |
|---|---|---|---|---|
| plan | 114244 | 42.5% | 169463 | 63.1% |
| json | 92900 | 34.6% | 98577 | 36.7% |
| render | 40341 | 15.0% | 40341 | 15.0% |
| type | 6321 | 2.4% | 10652 | 4.0% |
| store-resolve | 5086 | 1.9% | 8423 | 3.1% |
| exec/result | 2768 | 1.0% | 262654 | 97.8% |
| names | 2152 | 0.8% | 2196 | 0.8% |
| lower | 1661 | 0.6% | 1758 | 0.7% |
| inline | 1075 | 0.4% | 1346 | 0.5% |
| parse | 853 | 0.3% | 855 | 0.3% |
| other | 699 | 0.3% | 0 | 0.0% |
| normalize | 454 | 0.2% | 746 | 0.3% |
| model | 122 | 0.0% | 6419 | 2.4% |
| hottest frame at the top of the stack | samples | share |
|---|---|---|
| `com.legend.cache.Sha256.digest` | 50179 | 18.7% |
| `java.lang.AbstractStringBuilder.append` | 18021 | 6.7% |
| `java.lang.AbstractStringBuilder.needsNewBuffer` | 9055 | 3.4% |
| `java.lang.String.length` | 8555 | 3.2% |
| `com.legend.json.Json.escapeTo` | 8451 | 3.1% |
| `com.legend.json.Json$Parser.parseString` | 8225 | 3.1% |
| `java.util.regex.Pattern$BmpCharProperty.match` | 8064 | 3.0% |
| `java.util.HashMap.getNode` | 5146 | 1.9% |
| `java.util.regex.Pattern.lambda$union$0` | 5138 | 1.9% |
| `com.legend.setup.CsvSeed.records` | 4729 | 1.8% |
| `java.lang.String.indexOf` | 4553 | 1.7% |
| `java.lang.AbstractStringBuilder.ensureCapacityNewCoder` | 4116 | 1.5% |
| `java.lang.String.getBytes` | 3839 | 1.4% |
| `java.lang.String.split` | 3729 | 1.4% |
| `java.util.regex.Pattern$BmpCharPropertyGreedy.match` | 3705 | 1.4% |
| hottest legend frame, inclusive | samples | share |
| `com.legend.integration.StressSuites.run` | 265337 | 98.8% |
| `com.legend.integration.StressTool.main` | 265246 | 98.7% |
| `com.legend.test.ServiceTestRunner.runOne` | 262631 | 97.8% |
| `com.legend.test.ServiceTestRunner.run` | 262540 | 97.7% |
| `com.legend.TestExecution.runProgram` | 259299 | 96.5% |
| `com.legend.testplan.TestPlanner.plan` | 258352 | 96.2% |
| `com.legend.planner.PlanMaker.programmed` | 160114 | 59.6% |
| `com.legend.testplan.TestDoor.planned` | 160080 | 59.6% |
| `com.legend.planner.PlanMaker.testProgram` | 160056 | 59.6% |
| `com.legend.testplan.TestKinds.plan` | 148649 | 55.3% |
| `com.legend.testplan.TestKinds.seed` | 148608 | 55.3% |
| `com.legend.testplan.TestKinds.lambda$seed$0` | 136445 | 50.8% |
| `com.legend.planner.LoadPlanner.seeds` | 136441 | 50.8% |
| `com.legend.setup.CsvSeed.planned` | 100218 | 37.3% |
| `com.legend.testexecutionplan.TestPlanJson.requireRoundTrip` | 96993 | 36.1% |
| `com.legend.testexecutionplan.TestPlanJson.roundTripDifference` | 96985 | 36.1% |
| `com.legend.setup.LoadSteps.rows` | 84673 | 31.5% |
| `com.legend.testexecutionplan.TestPlanJson.read` | 60961 | 22.7% |
| `com.legend.setup.RowLoad.statements` | 53025 | 19.7% |
| `com.legend.cache.Hash.ofUtf8` | 52061 | 19.4% |
| `com.legend.cache.Hash.of` | 51223 | 19.1% |
| `com.legend.setup.RowLoad.written` | 50645 | 18.8% |
| `com.legend.cache.Sha256.digest` | 50181 | 18.7% |
| `com.legend.sql.dialect.AnsiSqlRenderer.render` | 39770 | 14.8% |
| `com.legend.setup.CsvSeed.declaredSteps` | 36204 | 13.5% |
| `com.legend.setup.CsvSeed.lambda$declaredSteps$1` | 36166 | 13.5% |
| `com.legend.setup.CsvSeed.steps` | 36109 | 13.4% |
| `com.legend.json.Json.toCompact` | 34837 | 13.0% |
| `com.legend.json.Json.writeValue` | 33866 | 12.6% |
| `com.legend.testexecutionplan.TestPlanJson.write` | 33321 | 12.4% |
| `com.legend.setup.LoadSteps.blob` | 31528 | 11.7% |
| `com.legend.json.Json$Writer.writeString` | 30359 | 11.3% |
| `com.legend.json.Json.escapeTo` | 29907 | 11.1% |
| `com.legend.json.Json$Parser.parseValue` | 29666 | 11.0% |
| `com.legend.json.Json$Parser.parseObject` | 29657 | 11.0% |
| `com.legend.json.Json.parse` | 29655 | 11.0% |
| `com.legend.testexecutionplan.TestPlanJson.readSteps` | 29610 | 11.0% |
| `com.legend.json.Json$Parser.parseArray` | 29601 | 11.0% |
| `com.legend.testexecutionplan.TestPlanJson.readStep` | 29593 | 11.0% |
| `com.legend.executionplan.PlanJson.readNode` | 29582 | 11.0% |
| `com.legend.executionplan.PlanJson.nodeOf` | 29582 | 11.0% |
| `com.legend.json.Json$Parser.parseString` | 25842 | 9.6% |
| `com.legend.setup.RowLoad.inserts` | 24268 | 9.0% |
| `com.legend.setup.RowLoad.insert` | 24070 | 9.0% |
| `com.legend.executionplan.PlanJson.contentOf` | 22942 | 8.5% |
| `com.legend.executionplan.PlanJson.lambda$contentOf$1` | 22548 | 8.4% |
| `com.legend.setup.CsvSeed.blockSqls` | 21267 | 7.9% |
| `com.legend.setup.RowLoad.cell` | 19135 | 7.1% |
| `com.legend.sql.dialect.SqlWriter.join` | 17020 | 6.3% |
| `com.legend.sql.dialect.SqlWriter.list` | 16907 | 6.3% |
| `com.legend.sql.dialect.AnsiSqlRenderer.ident` | 14030 | 5.2% |
| `com.legend.sql.dialect.SqlWriter.expr` | 13421 | 5.0% |
| `com.legend.sql.dialect.AnsiSqlRenderer.expr` | 13169 | 4.9% |
| `com.legend.planner.LoadPlanner.load` | 11980 | 4.5% |
| `com.legend.setup.CsvSeed.records` | 11130 | 4.1% |
| `com.legend.planner.PlanMaker.planStatement` | 11099 | 4.1% |
| `com.legend.planner.LoadPlanner.identity` | 10263 | 3.8% |
| `com.legend.sql.dialect.AnsiSqlRenderer.ddlColumns` | 8991 | 3.3% |
| `com.legend.planner.ProductStatements.prepare` | 8397 | 3.1% |
| `com.legend.setup.CsvSeed.cells` | 8230 | 3.1% |
allocation (sampled weight): 299.60 GB
| phase | share |
|---|---|
| plan | 43.4% |
| json | 32.4% |
| render | 19.9% |
| store-resolve | 1.4% |
| type | 1.4% |
| names | 0.4% |
| parse | 0.4% |
| inline | 0.3% |
| exec/result | 0.2% |
| lower | 0.1% |
| other | 0.1% |
| normalize | 0.1% |
| model | 0.0% |
| allocating legend frame (innermost) | share |
| `com.legend.setup.CsvSeed.steps` | 9.7% |
| `com.legend.json.Json$Parser.parseString` | 9.1% |
| `com.legend.setup.RowLoad.cell` | 9.1% |
| `com.legend.json.Json.escapeTo` | 8.5% |
| `com.legend.setup.CsvSeed.cells` | 6.9% |
| `com.legend.sql.dialect.AnsiSqlRenderer.ident` | 4.0% |
| `com.legend.json.Json$Writer.append` | 3.7% |
| `com.legend.cache.Sha256.digest` | 3.7% |
| `com.legend.sql.dialect.AnsiSqlRenderer.expr` | 3.4% |
| `com.legend.setup.CsvSeed.records` | 3.4% |
| `com.legend.json.Json$Writer.toString` | 3.2% |
| `com.legend.cache.Hash.ofUtf8` | 3.1% |
| `com.legend.sql.dialect.SqlWriter.append` | 2.9% |
| `com.legend.sql.dialect.AnsiSqlRenderer.stringLit` | 2.8% |
| `com.legend.sql.dialect.SqlWriter.statement` | 1.7% |
| `com.legend.sql.dialect.AnsiSqlRenderer.render` | 1.5% |
| `com.legend.setup.LoadSteps.blob` | 1.3% |
| `com.legend.json.Json$Parser.parseObject` | 1.2% |
| `com.legend.json.Json$Obj.<init>` | 1.1% |
| `com.legend.setup.CsvSeed.rowLoad` | 1.0% |
