-- The vector arm on DuckDB: cosine similarity over the stored FLOAT[384], top 150 distinct passages, each row carrying
-- every location that holds its hash. A nearest-neighbour search always returns neighbours: there is no score cut here.
-- Ties break on the hash. Parameter: $vector (the question's vector, made by the pinned embedder).
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
