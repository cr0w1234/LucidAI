-- Stage 1 — Recall-heavy unit filter with rank fusion (no weights)
-- Inputs:
--   %(query)s           :: text
--   %(vector)s          :: float4[] length 3072
--   %(model_name)s      :: text
--   %(include_docs)s    :: boolean
--   %(include_sections)s:: boolean
-- Output: top 75 mixed units (documents + sections) ordered by rank-fusion,
--         with separate identity fields so Python can split into two UUID lists.

WITH params(query_text, qvec, model_name, inc_docs, inc_secs) AS (
  VALUES (
    %(query)s::text,
    (%(vector)s::float4[])::vector(3072),
    %(model_name)s::text,
    %(include_docs)s::boolean,
    %(include_sections)s::boolean
  )
),

-- ---------------------------
-- Vector candidates (excerpt)
-- ---------------------------
doc_vec AS (
  SELECT
    'document'::text AS source_type,
    d.document_uuid  AS source_uuid,
    d.title,
    d.link,
    (1 - (dee.embedding <-> p.qvec))::float8 AS sem_score,
    NULL::float8 AS kw_score
  FROM prod.document_excerpt_embeddings_3072 dee
  JOIN prod.documents d ON d.document_uuid = dee.document_uuid
  JOIN params p ON p.inc_docs
  WHERE dee.model_name = p.model_name
  ORDER BY dee.embedding <-> p.qvec
  LIMIT 100
),
sec_vec AS (
  SELECT
    'section'::text  AS source_type,
    s.section_uuid   AS source_uuid,
    s.title,
    s.link,
    (1 - (see.embedding <-> p.qvec))::float8 AS sem_score,
    NULL::float8 AS kw_score
  FROM prod.section_excerpt_embedding_3072 see
  JOIN prod.sections s ON s.section_uuid = see.section_uuid
  JOIN params p ON p.inc_secs
  WHERE see.model_name = p.model_name
  ORDER BY see.embedding <-> p.qvec
  LIMIT 100
),

-- ---------------------------
-- Keyword candidates (content)
-- ---------------------------
doc_kw AS (
  SELECT
    'document'::text AS source_type,
    d.document_uuid  AS source_uuid,
    d.title,
    d.link,
    NULL::float8 AS sem_score,
    pgroonga_score(d.tableoid, d.ctid)::float8 AS kw_score
  FROM prod.documents d
  JOIN params p ON p.inc_docs
  WHERE d.content &@~ (
    p.query_text::text,  -- Explicit cast
    NULL::int[],         -- Explicit cast for weights
    ARRAY['scorer_tf_at_most($index, 2.0)']::text[],  -- Explicit cast
    'documents_content_pgroonga_idx'::text  -- Explicit cast
  )::pgroonga_full_text_search_condition_with_scorers
  ORDER BY pgroonga_score(d.tableoid, d.ctid) DESC
  LIMIT 100
),
sec_kw AS (
  SELECT
    'section'::text  AS source_type,
    s.section_uuid   AS source_uuid,
    s.title,
    s.link,
    NULL::float8 AS sem_score,
    pgroonga_score(s.tableoid, s.ctid)::float8 AS kw_score
  FROM prod.sections s
  JOIN params p ON p.inc_secs
  WHERE s.content &@~ (
    p.query_text::text,  -- Explicit cast
    NULL::int[],         -- Explicit cast for weights
    ARRAY['scorer_tf_at_most($index, 2.0)']::text[],  -- Explicit cast
    'sections_content_pgroonga_idx'::text  -- Explicit cast
  )::pgroonga_full_text_search_condition_with_scorers
  ORDER BY pgroonga_score(s.tableoid, s.ctid) DESC
  LIMIT 100
),

-- Combine all candidates
candidates AS (
  SELECT * FROM doc_vec
  UNION ALL
  SELECT * FROM sec_vec
  UNION ALL
  SELECT * FROM doc_kw
  UNION ALL
  SELECT * FROM sec_kw
),

-- Identity-level dedupe at unit level; keep best signals
combined AS (
  SELECT
    source_type,
    source_uuid,
    MAX(title) AS title,
    MAX(link)  AS link,
    COALESCE(MAX(sem_score), 0.0)::float8 AS best_sem_score,
    COALESCE(MAX(kw_score),  0.0)::float8 AS best_kw_score
  FROM candidates
  GROUP BY source_type, source_uuid
),

-- Per-signal ranks over the combined set
ranked AS (
  SELECT
    c.*,
    RANK() OVER (ORDER BY c.best_sem_score DESC NULLS LAST) AS rank_sem,
    RANK() OVER (ORDER BY c.best_kw_score  DESC NULLS LAST) AS rank_kw
  FROM combined c
),

-- Rank fusion (no weights)
final AS (
  SELECT
    source_type,
    source_uuid,
    title,
    link,
    best_sem_score,
    best_kw_score,
    LEAST(
      COALESCE(rank_sem, 2147483647),
      COALESCE(rank_kw,  2147483647)
    ) AS selector,
    rank_sem,
    rank_kw
  FROM ranked
)

SELECT
  source_type,
  source_uuid,
  title,
  link,
  best_sem_score,
  best_kw_score,
  selector,
  rank_sem,
  rank_kw
FROM final
ORDER BY selector ASC, rank_sem ASC, source_type ASC, source_uuid ASC
LIMIT 100;
