-- The vector arm on Postgres: pgvector's cosine distance, top 150 distinct passages, each row carrying every location that
-- holds its hash. NEVER RUN: the repository's embedded Postgres has no pgvector, so this statement is written from
-- pgvector's documentation and the judge's vector row on Postgres is REFUSED. Ties break on the hash. Parameter: %(vector)s.
WITH scored AS (
    SELECT e.SHA256, 1 - (e.EMBEDDING <=> %(vector)s::vector(384)) AS SCORE
    FROM CO_PASSAGE_EMBEDDING e
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
