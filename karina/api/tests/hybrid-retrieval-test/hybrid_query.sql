-- %(query)s = query_text (text)
-- %(vector)s = query_vector (float4[] length 3072)
-- %(vector_weight)s = vector_weight (float8)
-- %(keyword_weight)s = keyword_weight (float8)

WITH params(query_text, qvec, w_sem, w_kw) AS (
  VALUES (%(query)s::text, 
  (%(vector)s::float4[])::vector(3072), 
  %(vector_weight)s::float8, 
  %(keyword_weight)s::float8)
),
raw AS (
  SELECT
    c.chunk_uuid,
    dc.content_chunk,
    (1 - (c.embedding <-> p.qvec))::float8 AS sem_raw,
    CASE WHEN dc.content_chunk &@ p.query_text
         THEN pgroonga_score(dc.tableoid, dc.ctid)::float8
         ELSE 0::float8
    END AS kw_raw
  FROM prod.chunks_embeddings_3072 c
  JOIN prod.document_chunks dc USING (chunk_uuid)
  JOIN params p ON TRUE
),
norm AS (
  SELECT
    chunk_uuid,
    content_chunk,
    GREATEST(0.0, sem_raw) AS sem_norm,
    CASE WHEN max_kw = 0 THEN 0 ELSE kw_raw / max_kw END AS kw_norm
  FROM (
    SELECT r.*, MAX(kw_raw) OVER () AS max_kw
    FROM raw r
  ) x
)
SELECT
  n.content_chunk
FROM norm n
JOIN params p ON TRUE
ORDER BY (p.w_sem * n.sem_norm + p.w_kw * n.kw_norm) DESC
LIMIT %TOP_K%;
