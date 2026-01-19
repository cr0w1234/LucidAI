-- Stage 2 — Chunk retrieval with identity dedupe and authoritative 70/30 fusion
-- Inputs:
--   %(query_text)s          :: text
--   %(vector)s         :: float4[] length 3072
--   %(model_name)s     :: text
--   %(document_uuids)s :: uuid[]
--   %(section_uuids)s  :: uuid[]
--   %(vector_weight)s  :: float8 (use 0.7)
--   %(keyword_weight)s :: float8 (use 0.3)
-- Output: top %TOP_K% chunks across docs+sections with combined score

WITH params(query_text, qvec, model_name, doc_uuids, sec_uuids, w_sem, w_kw) AS (
  VALUES (
    %(query_text)s::text,
    (%(vector)s::float4[])::vector(3072),
    %(model_name)s::text,
    %(document_uuids)s::uuid[],
    %(section_uuids)s::uuid[],
    %(vector_weight)s::float8,
    %(keyword_weight)s::float8
  )
),

-- ---------------------------
-- Vector chunk candidates
-- ---------------------------
doc_chunk_vec AS (
  SELECT
    'document'::text      AS source_type,
    dc.document_uuid      AS source_uuid,
    dc.chunk_uuid,
    d.link,
    dc.content_chunk,
   (1 - (dce.embedding <-> p.qvec))::float8 AS sem_score,
    NULL::float8 AS kw_score
  FROM prod.document_chunks_embedding_3072 dce
  JOIN prod.document_chunks dc ON dc.chunk_uuid = dce.chunk_uuid
  JOIN prod.documents d ON d.document_uuid = dc.document_uuid
  JOIN params p ON TRUE
  WHERE dce.model_name = p.model_name
    AND dc.document_uuid = ANY(p.doc_uuids)
  ORDER BY dce.embedding <-> p.qvec
  LIMIT 120
  ),

sec_chunk_vec AS (
  SELECT
    'section'::text       AS source_type,
    sc.section_uuid       AS source_uuid,
    sc.chunk_uuid,
    s.link,
    sc.content_chunk,
    (1 - (sce.embedding <-> p.qvec))::float8 AS sem_score,
    NULL::float8 AS kw_score
  FROM prod.section_chunks_embedding_3072 sce
  JOIN prod.section_chunks sc ON sc.chunk_uuid = sce.chunk_uuid
  JOIN prod.sections s ON s.section_uuid = sc.section_uuid
  JOIN params p ON TRUE
  WHERE sce.model_name = p.model_name
    AND sc.section_uuid = ANY(p.sec_uuids)
  ORDER BY sce.embedding <-> p.qvec
  LIMIT 120
),

-- ---------------------------
-- Keyword chunk candidates — force ParadeDB score computation
-- ---------------------------

doc_chunk_kw AS MATERIALIZED (
  SELECT
    'doc_chunk_kw'::text  AS stream_type,
    'document'::text      AS source_type,
    dc.document_uuid      AS source_uuid,
    d.link,
    dc.chunk_uuid,
    dc.content_chunk,
    NULL::float8          AS sem_score,
    x.score::float8       AS kw_score
  FROM params p
  JOIN LATERAL (
    SELECT
      dc2.chunk_uuid,
      paradedb.score(dc2.chunk_uuid) AS score
    FROM prod.document_chunks dc2
    WHERE dc2.document_uuid = ANY(p.doc_uuids)
      AND dc2.content_chunk ||| p.query_text
    ORDER BY paradedb.score(dc2.chunk_uuid) DESC
    LIMIT 120
  ) x ON true
  JOIN prod.document_chunks dc ON dc.chunk_uuid = x.chunk_uuid
  JOIN prod.documents d ON d.document_uuid = dc.document_uuid
),

sec_chunk_kw AS MATERIALIZED (
  SELECT
    'sec_chunk_kw'::text  AS stream_type,
    'section'::text       AS source_type,
    sc.section_uuid       AS source_uuid,
    s.link,
    sc.chunk_uuid,
    sc.content_chunk,
    NULL::float8          AS sem_score,
    x.score::float8       AS kw_score
  FROM params p
  JOIN LATERAL (
    SELECT
      sc2.chunk_uuid,
      paradedb.score(sc2.chunk_uuid) AS score
    FROM prod.section_chunks sc2
    WHERE sc2.section_uuid = ANY(p.sec_uuids)
      AND sc2.content_chunk ||| p.query_text
    ORDER BY paradedb.score(sc2.chunk_uuid) DESC
    LIMIT 120
  ) x ON true
  JOIN prod.section_chunks sc ON sc.chunk_uuid = x.chunk_uuid
  JOIN prod.sections s ON s.section_uuid = sc.section_uuid
),

-- ---------------------------
-- Identity-level dedupe per type (join vec+kw by chunk_uuid), then union
-- ---------------------------
doc_chunks AS (
  SELECT
    COALESCE(v.source_type, k.source_type) AS source_type,
    COALESCE(v.source_uuid, k.source_uuid) AS source_uuid,
    COALESCE(v.chunk_uuid,  k.chunk_uuid)  AS chunk_uuid,
    COALESCE(v.link,        k.link)        AS link,
    COALESCE(v.content_chunk, k.content_chunk) AS content_chunk,
    COALESCE(v.sem_score, 0.0)::float8 AS sem_score,
    COALESCE(k.kw_score,  0.0)::float8 AS kw_score
  FROM doc_chunk_vec v
  FULL OUTER JOIN doc_chunk_kw k USING (chunk_uuid)
),

sec_chunks AS (
  SELECT
    COALESCE(v.source_type, k.source_type) AS source_type,
    COALESCE(v.source_uuid, k.source_uuid) AS source_uuid,
    COALESCE(v.chunk_uuid,  k.chunk_uuid)  AS chunk_uuid,
    COALESCE(v.link,        k.link)        AS link,
    COALESCE(v.content_chunk, k.content_chunk) AS content_chunk,
    COALESCE(v.sem_score, 0.0)::float8 AS sem_score,
    COALESCE(k.kw_score,  0.0)::float8 AS kw_score
  FROM sec_chunk_vec v
  FULL OUTER JOIN sec_chunk_kw k USING (chunk_uuid)
),

all_chunks AS (
  SELECT * FROM doc_chunks
  UNION ALL
  SELECT * FROM sec_chunks
),

-- ---------------------------
-- Normalize keyword across the union; clamp sem >= 0
-- ---------------------------
scored AS (
  SELECT
    source_type,
    source_uuid,
    chunk_uuid,
    link,
    content_chunk,
    GREATEST(0.0, sem_score) AS sem_norm,
    CASE WHEN max_kw = 0 THEN 0 ELSE kw_score / max_kw END AS kw_norm
  FROM (
    SELECT c.*, MAX(kw_score) OVER () AS max_kw
    FROM all_chunks c
  ) x
),

ranked AS (
  SELECT
    s.*,
    (p.w_sem * s.sem_norm + p.w_kw * s.kw_norm) AS combined_score
  FROM scored s
  JOIN params p ON TRUE
)

SELECT
  source_type,
  source_uuid,
  content_chunk,
  link,
  combined_score
FROM ranked
ORDER BY combined_score DESC, sem_norm DESC, chunk_uuid ASC
LIMIT %TOP_K%;