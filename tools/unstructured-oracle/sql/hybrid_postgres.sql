-- Hybrid on Postgres: reciprocal rank fusion (k = 60) of the keyword and vector arms' lists, one row per sha256 carrying every
-- location that holds it, cut to the top max(5, the case's number of accepted pointers), which the judge passes in
-- (DECISIONS.md entry 3). No reranker: a cross-encoder is outside the engine (README).
--
-- Relevance is the fused score normalised by its theoretical maximum, 2 / 61 (rank 1 in both lists), a constant, which puts it on [0, 1] so
-- that it can be blended with recency at all: an undivided fused score is at most 0.033 and recency alone would decide.
-- A time-aware case (the time_aware parameter) ranks by 0.7 * relevance + 0.3 * recency; every other case by relevance alone. Recency is
-- 0.5 ^ (days before its own corpus's latest date / 14). A hash held at several dates takes its most recent location's.
-- Ties break on the hash. The two arms below are their own files' statements, verbatim; the judge refuses a run if they
-- drift. NEVER RUN WHOLE: the vector arm needs pgvector, which the embedded Postgres lacks. Parameters: question, vector, time_aware, cut.
WITH keyword_arm AS (
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
), vector_arm AS (
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
           power(0.5::double precision, (l.LATEST_DATE - p.AS_OF_DATE) / 14.0::double precision) AS RECENCY
    FROM fused f
    JOIN CO_PASSAGE p ON p.SHA256 = f.SHA256
    JOIN latest l ON l.SOURCE = p.SOURCE
), surviving AS (
    SELECT SHA256,
           CASE WHEN %(time_aware)s THEN 0.7 * MAX(RELEVANCE) + 0.3 * MAX(RECENCY) ELSE MAX(RELEVANCE) END AS SCORE,
           json_agg(json_build_object('source', SOURCE, 'path', FILE_PATH, 'charStart', CHAR_START, 'section', SECTION_OR_DOC, 'asOfDate', AS_OF_DATE) ORDER BY SOURCE, FILE_PATH, CHAR_START) AS LOCATIONS
    FROM located
    GROUP BY SHA256
), ranked AS (
    SELECT SHA256, SCORE, LOCATIONS, row_number() OVER (ORDER BY SCORE DESC, SHA256) AS RANK
    FROM surviving
)
SELECT RANK, SHA256, SCORE, LOCATIONS
FROM ranked
WHERE RANK <= %(cut)s
ORDER BY RANK
