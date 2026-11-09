-- The keyword arm on DuckDB: BM25 (fts), any term may match, top 150 distinct passages, each row carrying every location
-- that holds its hash. Ties break on the hash, so a run is deterministic. Parameter: $question.
WITH scored AS (
    SELECT d.SHA256, fts_main_CO_PASSAGE_DISTINCT.match_bm25(d.SHA256, $question) AS SCORE
    FROM CO_PASSAGE_DISTINCT d
), ranked AS (
    SELECT SHA256, SCORE, row_number() OVER (ORDER BY SCORE DESC, SHA256) AS RANK
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
