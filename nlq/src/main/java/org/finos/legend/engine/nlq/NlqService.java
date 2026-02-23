package org.finos.legend.engine.nlq;

import org.finos.legend.pure.dsl.PureParser;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Three-step NLQ-to-Pure pipeline:
 * 1. Semantic Router — identifies the root class
 * 2. Query Planner — builds a structured query plan
 * 3. Pure Generator — generates valid Pure query syntax
 *
 * Each step uses the LLM with focused context from the SemanticIndex.
 */
public class NlqService {

    private static final int DEFAULT_TOP_K = 15;

    // ==================== Pure Language Reference Prompt ====================

    private static final String PURE_GENERATOR_PROMPT = """
            You are a Pure language code generator. Generate a valid Pure query expression.

            ═══════════════════════════════════════════
            RULE 1: PROJECT FIRST
            ═══════════════════════════════════════════
            ALWAYS project columns before applying filters, sort, limit, extend, or distinct.
            The only exception is a "class filter" that navigates an association (see Rule 2).

            Standard pattern:
              ClassName.all()->project([lambdas], ['colAlias1', 'colAlias2'])->filter(...)->sort(...)->limit(...)

            ═══════════════════════════════════════════
            RULE 2: CLASS FILTER vs RELATION FILTER
            ═══════════════════════════════════════════
            There are TWO kinds of filter:

            (A) Class filter — BEFORE project. ONLY when filtering through an association:
                Trade.all()->filter({t|$t.counterparty.name == 'Goldman Sachs'})->project(...)

            (B) Relation filter — AFTER project. PREFERRED for simple property filters.
                Use property access on the row variable (NOT getString/getInteger — those do not exist):
                Trade.all()->project([t|$t.tradeId, t|$t.status], ['tradeId', 'status'])->filter({row|$row.status == 'NEW'})

            ═══════════════════════════════════════════
            RULE 3: COLUMN ALIAS CONVENTION
            ═══════════════════════════════════════════
            ALWAYS use camelCase property names as column aliases (NO spaces).
            This ensures relation filters can reference columns via property access.
              CORRECT: ->project([t|$t.tradeDate, t|$t.notional], ['tradeDate', 'notional'])
              WRONG:   ->project([t|$t.tradeDate, t|$t.notional], ['Trade Date', 'Notional'])

            ═══════════════════════════════════════════
            CORE OPERATIONS
            ═══════════════════════════════════════════

            --- project() ---
            Project columns from class properties. ALWAYS include relevant columns.
              Person.all()->project([p|$p.firstName, p|$p.lastName, p|$p.age], ['firstName', 'lastName', 'age'])
            Navigate associations with dot notation:
              Trade.all()->project([t|$t.tradeId, t|$t.counterparty.name, t|$t.trader.desk.name], ['tradeId', 'counterparty', 'desk'])

            --- filter() on relation ---
            After project, filter using property access on the row variable:
              ->filter({row|$row.age > 30})
              ->filter({row|$row.status == 'ACTIVE'})
              ->filter({row|$row.tradeDate >= %2026-01-01})
              ->filter({row|$row.name->contains('Smith')})
              ->filter({row|$row.name->startsWith('A')})
              ->filter({row|$row.amount > 0 && $row.status != 'CANCELLED'})

            --- groupBy() ---
            Aggregation with 3 arguments: [group lambdas], [agg lambdas], ['column names']
            IMPORTANT: groupBy() MUST be called after project(), never directly on Class.all().
            Aggregation lambdas apply the agg function directly: {r|$r.col->sum()}
              Trade.all()->project([t|$t.trader.desk.name, t|$t.notional], ['desk', 'notional'])->groupBy([{r|$r.desk}], [{r|$r.notional->sum()}], ['desk', 'totalNotional'])
            Multiple aggregations:
              Trade.all()->project([t|$t.side, t|$t.notional, t|$t.tradeId], ['side', 'notional', 'tradeId'])->groupBy([{r|$r.side}], [{r|$r.notional->sum()}, {r|$r.tradeId->count()}], ['side', 'totalNotional', 'count'])
            Available agg functions: sum(), avg(), count(), min(), max(), stdDev(), variance(), median(), mode()
            Percentile: {r|$r.salary->percentile(0.95, true)}

            --- sort() ---
            After project:
              ->sort('columnAlias')              // ascending
              ->sort(descending('amount'))        // descending
              ->sort('desk', descending('pnl'))   // multi-column

            --- limit() / take() / drop() / slice() ---
              ->limit(10)       // first 10 rows
              ->take(10)        // alias for limit
              ->drop(5)         // skip first 5 rows (OFFSET)
              ->slice(10, 20)   // rows 10 through 20

            --- distinct() ---
              ->distinct()      // deduplicate rows (after project)

            ═══════════════════════════════════════════
            WINDOW FUNCTIONS
            ═══════════════════════════════════════════
            Use extend(over(...), ~colName:{p,w,r|...}) AFTER project:

            Ranking:
              ->extend(over(), ~rowNum:{p,w,r|$p->rowNumber($r)})
              ->extend(over(~department, ~salary->desc()), ~rank:{p,w,r|$p->rank($w,$r)})
              ->extend(over(~department, ~salary->desc()), ~denseRank:{p,w,r|$p->denseRank($w,$r)})

            Value offset:
              ->extend(over(~department, ~salary->descending()), ~prevSalary:{p,w,r|$p->lag($r).salary})
              ->extend(over(~department, ~salary->descending()), ~nextSalary:{p,w,r|$p->lead($r).salary})

            Window aggregation:
              ->extend(over(~department), ~deptTotal:{p,w,r|$p->sum($w,$r).salary})

            Window spec:
              over()                              — whole result set
              over(~partitionCol)                  — partition only
              over(~partitionCol, ~orderCol->desc()) — partition + order

            ═══════════════════════════════════════════
            COMPUTED COLUMNS (extend without window)
            ═══════════════════════════════════════════
              ->extend(~margin:{row|$row.revenue - $row.cost})
              ->extend(~label:{row|if($row.pnl > 0, |'Profit', |'Loss')})

            ═══════════════════════════════════════════
            ADDITIONAL OPERATIONS
            ═══════════════════════════════════════════

            --- rename() ---
              ->rename(~oldName, ~newName)

            --- concatenate() (UNION) ---
              relation1->concatenate(relation2)

            --- join() (explicit) ---
              relation1->join(relation2, JoinKind.INNER, {a,b|$a.id == $b.id})
              JoinKind options: INNER, LEFT_OUTER

            ═══════════════════════════════════════════
            SCALAR FUNCTIONS (use in extend or filter)
            ═══════════════════════════════════════════

            String:  toUpper(), toLower(), length(), trim(),
                     contains('x'), startsWith('x'), endsWith('x'),
                     substring(start, end), indexOf('x'), splitPart('delim', n),
                     format('template %s', [args])

            Math:    round(), floor(), ceiling(), abs(), rem(n), sign()

            Date:    today(), now(),
                     dateDiff(date1, date2, DurationUnit.DAYS),
                     adjust(%2026-01-01, 7, DurationUnit.DAYS)

            Conditional: if(cond, |trueVal, |falseVal), coalesce(val1, val2)

            Parse/Cast: parseInteger('123'), parseFloat('1.5'), toString(42)

            ═══════════════════════════════════════════
            DATE LITERALS
            ═══════════════════════════════════════════
              %2026-02-20                    — StrictDate
              %2026-02-20T14:30:00           — DateTime
              today()                        — current date
              now()                          — current timestamp
              adjust(%2026-02-20, -30, DurationUnit.DAYS) — date arithmetic

            ═══════════════════════════════════════════
            LET BINDINGS
            ═══════════════════════════════════════════
              {|
                let cutoff = %2026-01-01;
                Trade.all()->project([t|$t.tradeDate, t|$t.notional], ['tradeDate', 'notional'])
                  ->filter({row|$row.tradeDate >= $cutoff})
              }

            ═══════════════════════════════════════════
            COMPLETE EXAMPLES
            ═══════════════════════════════════════════

            1. Simple project + relation filter:
               Person.all()->project([p|$p.firstName, p|$p.age], ['firstName', 'age'])->filter({row|$row.age > 30})

            2. Class filter (association navigation) + project:
               Trade.all()->filter({t|$t.counterparty.name == 'Goldman Sachs'})->project([t|$t.tradeId, t|$t.notional, t|$t.counterparty.name], ['tradeId', 'notional', 'counterparty'])

            3. GroupBy with class filter:
               DailyPnL.all()->filter({p|$p.desk.name == 'AMER Equity Swaps'})->project([p|$p.trader.name, p|$p.totalPnL], ['trader', 'totalPnL'])->groupBy([{r|$r.trader}], [{r|$r.totalPnL->sum()}], ['trader', 'totalPnL'])

            4. Project + sort + limit (top N):
               Trade.all()->project([t|$t.tradeId, t|$t.notional, t|$t.trader.name], ['tradeId', 'notional', 'trader'])->sort(descending('notional'))->limit(10)

            5. Project + window function:
               Trade.all()->project([t|$t.tradeId, t|$t.trader.desk.name, t|$t.notional], ['tradeId', 'desk', 'notional'])->extend(over(~desk, ~notional->desc()), ~rank:{p,w,r|$p->rank($w,$r)})

            6. Project + relation filter + sort:
               Trade.all()->project([t|$t.tradeId, t|$t.tradeDate, t|$t.status, t|$t.notional], ['tradeId', 'tradeDate', 'status', 'notional'])->filter({row|$row.tradeDate == today()})->sort(descending('notional'))

            7. GroupBy + relation filter on aggregate:
               Trade.all()->project([t|$t.trader.name, t|$t.notional], ['trader', 'notional'])->groupBy([{r|$r.trader}], [{r|$r.notional->sum()}], ['trader', 'totalNotional'])->filter({row|$row.totalNotional > 1000000})->sort(descending('totalNotional'))

            8. Complex: class filter + project + groupBy + sort + limit:
               DailyPnL.all()->filter({p|$p.desk.name == 'AMER Equity Swaps' && $p.pnlDate >= %2026-01-01})->project([p|$p.trader.name, p|$p.totalPnL], ['trader', 'totalPnL'])->groupBy([{r|$r.trader}], [{r|$r.totalPnL->sum()}, {r|$r.totalPnL->count()}], ['trader', 'totalPnL', 'days'])->sort(descending('totalPnL'))->limit(10)

            ═══════════════════════════════════════════
            KEY RULES
            ═══════════════════════════════════════════
            - Always start with ClassName.all()
            - ALWAYS project() before filter/sort/limit/extend/distinct/groupBy
            - Column aliases MUST be camelCase with NO spaces (e.g., 'tradeDate' not 'Trade Date')
            - Class filter ONLY for association navigation; everything else is relation filter
            - Relation filter uses property access: $row.colName (NOT getString/getInteger — those do not exist)
            - Use $variable references inside lambdas (e.g., $p, $row, $x)
            - Date literals: %YYYY-MM-DD or %YYYY-MM-DDThh:mm:ss
            - Navigate associations via dot notation (e.g., $t.trader.desk.name)
            - groupBy takes 3 args: [group lambdas], [agg lambdas], ['column names'] — agg lambdas use {r|$r.col->sum()}, NOT agg()
            - extend() for window functions always comes AFTER project()

            Return ONLY the Pure query expression. No explanation, no markdown, no code fences.
            """;

    private static final int MAX_RETRIES = 2;

    private final SemanticIndex index;
    private final PureModelBuilder modelBuilder;
    private final LlmClient llmClient;

    public NlqService(SemanticIndex index, PureModelBuilder modelBuilder, LlmClient llmClient) {
        this.index = index;
        this.modelBuilder = modelBuilder;
        this.llmClient = llmClient;
    }

    /**
     * Runs the full NLQ-to-Pure pipeline.
     *
     * @param question The natural language question
     * @param domain   Optional domain hint (e.g., "PnL", "Trading")
     * @return The NlqResult with the generated Pure query
     */
    public NlqResult process(String question, String domain) {
        long start = System.nanoTime();

        try {
            // Step 0: Retrieve relevant classes
            List<SemanticIndex.RetrievalResult> retrieved = index.retrieve(question, DEFAULT_TOP_K, domain);
            Set<String> classNames = retrieved.stream()
                    .map(SemanticIndex.RetrievalResult::qualifiedName)
                    .collect(Collectors.toSet());
            List<String> retrievedList = retrieved.stream()
                    .map(r -> simpleName(r.qualifiedName()))
                    .toList();

            String routerSchema = ModelSchemaExtractor.extractPrimarySchema(classNames, modelBuilder);
            String routingHints = ModelSchemaExtractor.extractRoutingHints(classNames, modelBuilder);

            // Step 1: Semantic Router — identify root class
            String rootClass = routeToRootClass(question, routerSchema, routingHints);

            // Step 2+3: Rebuild a focused schema around the root class for planner/generator.
            // The router needed all 15 candidates; planner/generator only need root + its associations.
            String focusedSchema = ModelSchemaExtractor.extractSchema(Set.of(rootClass), modelBuilder);

            // Step 2: Query Planner — build structured plan
            String queryPlan = planQuery(question, rootClass, focusedSchema);

            // Step 3: Pure Generator — generate Pure syntax (with parse-retry)
            String pureQuery = null;
            Exception lastParseError = null;
            for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
                pureQuery = generatePure(question, rootClass, queryPlan, focusedSchema);
                try {
                    PureParser.parse(pureQuery);
                    lastParseError = null;
                    break;
                } catch (Exception e) {
                    lastParseError = e;
                    if (attempt < MAX_RETRIES) {
                        System.out.printf("  [retry] parse failed (attempt %d/%d): %s%n",
                                attempt + 1, MAX_RETRIES + 1, e.getMessage());
                    }
                }
            }
            if (lastParseError != null) {
                long elapsed = (System.nanoTime() - start) / 1_000_000;
                return NlqResult.error("Parse validation failed after " + (MAX_RETRIES + 1) +
                        " attempts: " + lastParseError.getMessage(), retrievedList, elapsed);
            }

            long elapsed = (System.nanoTime() - start) / 1_000_000;

            return new NlqResult(
                    rootClass,
                    queryPlan,
                    pureQuery,
                    "Generated Pure query for " + rootClass,
                    true,
                    null,
                    retrievedList,
                    elapsed
            );
        } catch (Exception e) {
            long elapsed = (System.nanoTime() - start) / 1_000_000;
            return NlqResult.error(e.getMessage(), List.of(), elapsed);
        }
    }

    // ==================== Step 1: Semantic Router ====================

    private String routeToRootClass(String question, String schema, String routingHints) {
        String systemPrompt = """
                You are a data model expert. Given a data model schema and a natural language question,
                identify which class is the PRIMARY entity (root class) that the query should start from.
                
                The root class is the main entity being queried — the one that appears after ".all()" in a Pure query.
                
                KEY RULES:
                - Prefer the most SPECIFIC class that directly holds the data being asked about.
                - If a detail/child class exists for the concept, prefer it over the parent.
                - Look for "When to use" hints in the routing hints section — they disambiguate similar classes.
                - The root class should be the one whose properties you'd filter/aggregate on.
                - When in doubt, choose the class that has the columns you'd want to project or aggregate.
                
                Return ONLY a JSON object with this exact format (no markdown, no explanation):
                {"rootClass": "ClassName", "reasoning": "brief explanation"}
                """;

        StringBuilder userMessage = new StringBuilder();
        userMessage.append("Data Model:\n").append(schema);
        if (routingHints != null && !routingHints.isBlank()) {
            userMessage.append("\n\nRouting Hints:\n").append(routingHints);
        }
        userMessage.append("\n\nQuestion: ").append(question);

        Exception lastError = null;
        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            String response = llmClient.complete(systemPrompt, userMessage.toString());
            try {
                return extractJsonField(response, "rootClass");
            } catch (Exception e) {
                lastError = e;
                if (attempt < MAX_RETRIES) {
                    System.out.printf("  [retry] rootClass extraction failed (attempt %d/%d): %s%n",
                            attempt + 1, MAX_RETRIES + 1, e.getMessage());
                }
            }
        }
        throw new LlmClient.LlmException(
                "Could not extract rootClass after " + (MAX_RETRIES + 1) + " attempts: " + lastError.getMessage(),
                -1, lastError.getMessage());
    }

    // ==================== Step 2: Query Planner ====================

    private String planQuery(String question, String rootClass, String schema) {
        String systemPrompt = """
                You are a Pure language query planner. Given a data model, a root class, and a natural language question,
                produce a structured query plan as JSON.
                
                IMPORTANT: The first step should ALWAYS be projections — decide which columns to output.
                Then apply filters, aggregations, sorting, etc. on the projected relation.
                
                The plan should specify:
                - projections: list of property paths to select (REQUIRED, always first)
                - classFilters: list of filters that require association navigation (BEFORE project)
                - relationFilters: list of filters on simple projected columns (AFTER project)
                - groupBy: list of property paths to group by (if aggregation needed)
                - aggregations: list of {function, property, alias} objects
                - windowFunctions: list of {function, partitionBy, orderBy, alias} objects
                - sort: list of {column, direction} objects
                - limit: optional row limit
                - distinct: boolean (if deduplication needed)
                
                Navigation uses dot notation through associations (e.g., "desk.name", "trader.badge").
                
                Available filter operators: ==, !=, >, <, >=, <=, contains, in, startsWith, endsWith
                Available aggregation functions: sum, avg, count, min, max, stdDev, variance, median, mode, percentile
                Available window functions: rowNumber, rank, denseRank, lead, lag, sum, avg, count, min, max
                
                Classify each filter as either:
                - classFilter: requires navigating an association (e.g., "counterparty.name == 'Goldman'")
                - relationFilter: operates on a simple property of the root class (e.g., "status == 'NEW'")
                
                Return ONLY valid JSON (no markdown, no explanation).
                """;

        String userMessage = "Root Class: " + rootClass + "\n\nData Model:\n" + schema +
                "\n\nQuestion: " + question;

        return llmClient.complete(systemPrompt, userMessage);
    }

    // ==================== Step 3: Pure Generator ====================

    private String generatePure(String question, String rootClass, String queryPlan, String schema) {
        String systemPrompt = PURE_GENERATOR_PROMPT;

        String userMessage = "Root Class: " + rootClass +
                "\n\nQuery Plan:\n" + queryPlan +
                "\n\nData Model:\n" + schema +
                "\n\nQuestion: " + question;

        String response = llmClient.complete(systemPrompt, userMessage);

        // Clean up: remove markdown code fences if present
        response = response.strip();
        if (response.startsWith("```")) {
            response = response.replaceAll("^```[a-z]*\\n?", "").replaceAll("\\n?```$", "").strip();
        }

        return response;
    }

    // ==================== Helpers ====================

    private static String extractJsonField(String json, String field) {
        // Handle both with and without markdown code fences
        String cleaned = json.strip();
        if (cleaned.startsWith("```")) {
            cleaned = cleaned.replaceAll("^```[a-z]*\\n?", "").replaceAll("\\n?```$", "").strip();
        }

        Pattern p = Pattern.compile("\"" + field + "\"\\s*:\\s*\"([^\"]*?)\"");
        Matcher m = p.matcher(cleaned);
        if (m.find()) {
            return m.group(1);
        }
        throw new LlmClient.LlmException(
                "Could not extract '" + field + "' from LLM response: " + json, -1, json);
    }

    private static String simpleName(String qualifiedName) {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }
}
