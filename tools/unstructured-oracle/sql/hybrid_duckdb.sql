-- Hybrid on DuckDB: reciprocal rank fusion (k = 60) of the keyword and vector arms' lists, one row per sha256 carrying every
-- location that holds it, cut to the top max(5, the case's number of accepted pointers), which the judge passes in
-- (DECISIONS.md entry 3). No reranker: a cross-encoder is outside the engine (README).
--
-- Relevance is the fused score normalised by its theoretical maximum, 2 / 61 (rank 1 in both lists), a constant, which puts it on [0, 1] so
-- that it can be blended with recency at all: an undivided fused score is at most 0.033 and recency alone would decide.
-- A time-aware case ($time_aware) ranks by 0.7 * relevance + 0.3 * recency; every other case by relevance alone. Recency is
-- 0.5 ^ (days before its own corpus's latest date / 14). A hash held at several dates takes its most recent location's.
-- Ties break on the hash. The two arms below are their own files' lists, verbatim and without the keyword arm's floor; the
-- judge refuses a run if they drift. The floor (DECISIONS.md entry 5): when no passage's relevance reaches 0.89, before the
-- recency blend, the result set is empty. Parameters: $question, $vector, $time_aware, $cut.
WITH keyword_arm AS (
WITH scored AS (
    SELECT d.SHA256, fts_main_CO_PASSAGE_DISTINCT.match_bm25(d.SHA256, $question) AS SCORE
    FROM CO_PASSAGE_DISTINCT d
), ranked AS (
    SELECT SHA256, SCORE, row_number() OVER (ORDER BY round(SCORE, 9) DESC, SHA256) AS RANK
    FROM scored
    WHERE SCORE IS NOT NULL
    QUALIFY RANK <= 150
)
SELECT r.RANK,
       r.SHA256,
       r.SCORE,
       list({'source': p.SOURCE, 'path': p.FILE_PATH, 'charStart': p.CHAR_START, 'section': p.SECTION_OR_DOC, 'asOfDate': p.AS_OF_DATE}
            ORDER BY p.SOURCE, p.FILE_PATH, p.CHAR_START) AS LOCATIONS
FROM ranked r
JOIN CO_PASSAGE p ON p.SHA256 = r.SHA256
GROUP BY r.RANK, r.SHA256, r.SCORE
ORDER BY r.RANK
), vector_arm AS (
WITH scored AS (
    SELECT e.SHA256, array_cosine_similarity(e.EMBEDDING, $vector::FLOAT[384]) AS SCORE
    FROM CO_PASSAGE_EMBEDDING e
), ranked AS (
    SELECT SHA256, SCORE, row_number() OVER (ORDER BY SCORE DESC, SHA256) AS RANK
    FROM scored
    QUALIFY RANK <= 150
)
SELECT r.RANK,
       r.SHA256,
       r.SCORE,
       list({'source': p.SOURCE, 'path': p.FILE_PATH, 'charStart': p.CHAR_START, 'section': p.SECTION_OR_DOC, 'asOfDate': p.AS_OF_DATE}
            ORDER BY p.SOURCE, p.FILE_PATH, p.CHAR_START) AS LOCATIONS
FROM ranked r
JOIN CO_PASSAGE p ON p.SHA256 = r.SHA256
GROUP BY r.RANK, r.SHA256, r.SCORE
ORDER BY r.RANK
), both_lists AS (
    SELECT SHA256, RANK FROM keyword_arm
    UNION ALL
    SELECT SHA256, RANK FROM vector_arm
), fused AS (
    SELECT SHA256, SUM(1.0 / (60 + RANK)) / (2.0 / (60 + 1)) AS RELEVANCE
    FROM both_lists
    GROUP BY SHA256
), latest AS (
    SELECT SOURCE, MAX(AS_OF_DATE) AS LATEST_DATE FROM CO_PASSAGE GROUP BY SOURCE
), located AS (
    SELECT f.SHA256, f.RELEVANCE, p.SOURCE, p.FILE_PATH, p.CHAR_START, p.SECTION_OR_DOC, p.AS_OF_DATE,
           power(0.5, date_diff('day', p.AS_OF_DATE, l.LATEST_DATE) / 14.0) AS RECENCY
    FROM fused f
    JOIN CO_PASSAGE p ON p.SHA256 = f.SHA256
    JOIN latest l ON l.SOURCE = p.SOURCE
), surviving AS (
    SELECT SHA256,
           CASE WHEN $time_aware THEN 0.7 * MAX(RELEVANCE) + 0.3 * MAX(RECENCY) ELSE MAX(RELEVANCE) END AS SCORE,
           list({'source': SOURCE, 'path': FILE_PATH, 'charStart': CHAR_START, 'section': SECTION_OR_DOC, 'asOfDate': AS_OF_DATE} ORDER BY SOURCE, FILE_PATH, CHAR_START) AS LOCATIONS
    FROM located
    GROUP BY SHA256
), ranked AS (
    SELECT SHA256, SCORE, LOCATIONS, row_number() OVER (ORDER BY SCORE DESC, SHA256) AS RANK
    FROM surviving
)
SELECT RANK, SHA256, SCORE, LOCATIONS
FROM ranked
WHERE RANK <= $cut AND (SELECT MAX(RELEVANCE) FROM fused) >= 0.89
ORDER BY RANK
