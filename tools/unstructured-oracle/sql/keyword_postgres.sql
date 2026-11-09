-- The keyword arm on Postgres: tsvector, @@ and ts_rank, top 150 distinct passages, each row carrying every location that
-- holds its hash. plainto_tsquery ANDs its terms, which a whole-sentence question almost never satisfies, so its ' & ' are
-- turned into ' | ': any term may match, as DuckDB's match_bm25 does. ts_rank has no inverse document frequency; that
-- difference from BM25 is left in, it is part of what is being measured. Ties break on the hash. The floor (DECISIONS.md
-- entry 5): a list whose best score is under 0.026 is no answer, and the result set is empty. Hybrid fuses the list inside
-- `arm`, without this floor, and has its own. Parameter: %(question)s.
WITH arm AS (
WITH query AS (
    SELECT replace(plainto_tsquery('english', %(question)s)::text, ' & ', ' | ')::tsquery AS Q
), distinct_passage AS (
    SELECT DISTINCT ON (SHA256) SHA256, to_tsvector('english', PASSAGE_TEXT) AS TERMS
    FROM CO_PASSAGE
    ORDER BY SHA256, FILE_PATH, ORDINAL
), scored AS (
    SELECT d.SHA256, ts_rank(d.TERMS, q.Q) AS SCORE
    FROM distinct_passage d, query q
    WHERE d.TERMS @@ q.Q
), ranked AS (
    SELECT SHA256, SCORE, row_number() OVER (ORDER BY SCORE DESC, SHA256) AS RANK
    FROM scored
    ORDER BY RANK
    LIMIT 150
)
SELECT r.RANK,
       r.SHA256,
       r.SCORE,
       json_agg(json_build_object('source', p.SOURCE, 'path', p.FILE_PATH, 'charStart', p.CHAR_START, 'section', p.SECTION_OR_DOC, 'asOfDate', p.AS_OF_DATE)
                ORDER BY p.SOURCE, p.FILE_PATH, p.CHAR_START) AS LOCATIONS
FROM ranked r
JOIN CO_PASSAGE p ON p.SHA256 = r.SHA256
GROUP BY r.RANK, r.SHA256, r.SCORE
ORDER BY r.RANK
)
SELECT RANK, SHA256, SCORE, LOCATIONS
FROM arm
WHERE (SELECT MAX(SCORE) FROM arm) >= 0.026
ORDER BY RANK
