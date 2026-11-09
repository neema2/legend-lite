-- The keyword arm on DuckDB: BM25 (fts), any term may match, top 150 distinct passages, each row carrying every location
-- that holds its hash. match_bm25 sums in parallel, so two passages with mathematically equal scores differ in the last
-- bits from one call to the next (measured: up to 8.9e-16); the score is rounded to 9 places before ranking so that a tie
-- is a tie and breaks on the hash, and a run is deterministic. Parameter: $question.
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
