-- The keyword arm's index on DuckDB. create_fts_index needs one unique key column and CO_PASSAGE's key has three, so the
-- index is over one row per distinct passage: which is also the study's rule that results are deduped by sha256 before any
-- cut. Every option is stated rather than left to the extension's defaults.
INSTALL fts;
LOAD fts;
DROP TABLE IF EXISTS CO_PASSAGE_DISTINCT;
CREATE TABLE CO_PASSAGE_DISTINCT AS SELECT SHA256, any_value(PASSAGE_TEXT) AS PASSAGE_TEXT FROM CO_PASSAGE GROUP BY SHA256;
PRAGMA create_fts_index('CO_PASSAGE_DISTINCT', 'SHA256', 'PASSAGE_TEXT', stemmer = 'porter', stopwords = 'english',
                        ignore = '(\.|[^a-z])+', strip_accents = 1, lower = 1, overwrite = 1);
