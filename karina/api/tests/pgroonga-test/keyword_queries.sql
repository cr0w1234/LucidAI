-- %(query)s = query_text (text)

WITH params(query_text) AS (
  VALUES (%(query)s::text)
),
raw AS (
  SELECT
    dc.chunk_uuid,
    dc.content_chunk,
    CASE
      WHEN dc.content_chunk &@ p.query_text
      THEN pgroonga_score(dc.tableoid, dc.ctid)::float8
      ELSE 0::float8
    END AS kw_raw
  FROM prod.document_chunks dc
  JOIN params p ON TRUE
),
norm AS (
  SELECT
    chunk_uuid,
    content_chunk,
    CASE
      WHEN max_kw = 0 THEN 0
      ELSE kw_raw / max_kw
    END AS kw_norm
  FROM (
    SELECT r.*, MAX(kw_raw) OVER () AS max_kw
    FROM raw r
  ) x
)
SELECT
  content_chunk
FROM norm
ORDER BY kw_norm DESC
LIMIT %TOP_K%;