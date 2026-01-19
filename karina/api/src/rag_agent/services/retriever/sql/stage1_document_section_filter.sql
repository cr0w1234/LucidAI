WITH params(query_text, qvec, model_name, inc_docs, inc_secs) AS (
  VALUES (
    %(query_text)s::text,
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
    'doc_vec'::text AS stream_type,
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
  LIMIT 30
),
sec_vec AS (
  SELECT
    'sec_vec'::text AS stream_type,
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
  LIMIT 30
),

-- ---------------------------
-- Keyword candidates 
-- ---------------------------

doc_kw AS MATERIALIZED (
  SELECT
    'doc_kw'::text      AS stream_type,
    'document'::text    AS source_type,
    d.document_uuid     AS source_uuid,
    d.title,
    d.link,
    NULL::float8        AS sem_score,
    s.score::float8     AS kw_score
  FROM params p
  JOIN LATERAL (SELECT p.query_text::text AS q) q ON true
  JOIN LATERAL (
    SELECT
      dd.document_uuid,
      paradedb.score(dd.document_uuid) AS score
    FROM prod.documents dd
    WHERE
      dd.title   ||| q.q OR
      dd.excerpt ||| q.q OR
      dd.content ||| q.q
    ORDER BY paradedb.score(dd.document_uuid) DESC
    LIMIT 30
  ) s ON true
  JOIN prod.documents d ON d.document_uuid = s.document_uuid
),

sec_kw AS MATERIALIZED (
  SELECT
    'sec_kw'::text      AS stream_type,
    'section'::text     AS source_type,
    s.section_uuid      AS source_uuid,
    s.title,
    s.link,
    NULL::float8        AS sem_score,
    x.score::float8     AS kw_score
  FROM params p
  JOIN LATERAL (SELECT p.query_text::text AS q) q ON true
  JOIN LATERAL (
    SELECT
      ss.section_uuid,
      paradedb.score(ss.section_uuid) AS score
    FROM prod.sections ss
    WHERE
    ss.title   ||| q.q OR
      ss.excerpt ||| q.q OR
      ss.content ||| q.q
    ORDER BY paradedb.score(ss.section_uuid) DESC
    LIMIT 30
  ) x ON true
  JOIN prod.sections s ON s.section_uuid = x.section_uuid
),

-- Combine all candidates
candidates AS (
  SELECT stream_type, source_type, source_uuid, title, link, sem_score, kw_score FROM doc_vec
  UNION ALL
  SELECT stream_type, source_type, source_uuid, title, link, sem_score, kw_score FROM sec_vec
  UNION ALL
  SELECT stream_type, source_type, source_uuid, title, link, sem_score, kw_score FROM doc_kw
  UNION ALL
  SELECT stream_type, source_type, source_uuid, title, link, sem_score, kw_score FROM sec_kw
),

dedup AS (
  SELECT
    source_type,
    source_uuid,
    MAX(title) AS title,
    MAX(link)  AS link,
    MAX(sem_score) AS best_sem_score,
    MAX(kw_score)  AS best_kw_score,
    array_agg(DISTINCT stream_type) AS retrieval_sources
  FROM candidates
  GROUP BY source_type, source_uuid
)

SELECT * FROM dedup;