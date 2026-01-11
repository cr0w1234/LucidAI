prod.sections

INSERT INTO prod.sections (
  section_uuid, title, link, content,
  heading_number, subheading_number, heading_page_number, excerpt, section_parent_heading_uuid
)
SELECT
  (doc->>'section_uuid')::uuid,
  doc->>'title',
  doc->>'link',
  (doc->'content'->>'full_text') AS content,     -- only the text; chunks are NOT stored here
  (doc->>'heading_number')::int,
  (doc->>'subheading_number')::int,
  (doc->>'heading_page_number')::int,
  doc->>'excerpt',
  NULLIF(doc->>'section_parent_heading_uuid','')::uuid
FROM staging.sections_raw
ON CONFLICT (section_uuid) DO UPDATE
SET title                     = EXCLUDED.title,
    link                      = EXCLUDED.link,
    content                   = EXCLUDED.content,
    heading_number            = EXCLUDED.heading_number,
    subheading_number         = EXCLUDED.subheading_number,
    heading_page_number       = EXCLUDED.heading_page_number,
    excerpt                   = EXCLUDED.excerpt,
    section_parent_heading_uuid = EXCLUDED.section_parent_heading_uuid;

prod.section_chunks

INSERT INTO prod.section_chunks (
  content_chunk, chunk_uuid, chunk_index, section_uuid
)
SELECT
  doc->>'content_chunk',
  (doc->>'chunk_uuid')::uuid,
  (doc->>'chunk_index')::int,
  (doc->>'section_uuid')::uuid
FROM staging.section_chunks_raw
ON CONFLICT (chunk_uuid) DO UPDATE
SET content_chunk = EXCLUDED.content_chunk,
    chunk_index   = EXCLUDED.chunk_index,
    section_uuid  = EXCLUDED.section_uuid;

prod.section_excerpt_embedding_3072

WITH src AS (
  SELECT
    (doc->>'section_uuid')::uuid AS section_uuid,
    doc->>'model_name'           AS model_name,
    (ARRAY(
       SELECT x::float8
       FROM jsonb_array_elements_text(doc->'embedding') AS t(x)
    ))::vector(3072)             AS embedding,
    NOW()                        AS created_at   -- timestamp for inserts/updates
  FROM staging.section_excerpt_embeddings_raw
  WHERE doc ? 'section_uuid'
    AND jsonb_typeof(doc->'embedding') = 'array'
    AND jsonb_array_length(doc->'embedding') = 3072
),
upsert AS (
  INSERT INTO prod.section_excerpt_embedding_3072 (
    section_uuid, model_name, embedding, created_at
  )
  SELECT section_uuid, model_name, embedding, created_at
  FROM src
  ON CONFLICT (section_uuid, model_name) DO UPDATE
  SET embedding  = EXCLUDED.embedding,
      created_at = EXCLUDED.created_at
  RETURNING 1
)
SELECT count(*) AS rows_upserted FROM upsert;

prod.section_chunks_embedding_3072

WITH src AS (
  SELECT
    (doc->>'chunk_uuid')::uuid   AS chunk_uuid,
    doc->>'model_name'           AS model_name,
    (ARRAY(
       SELECT x::float8
       FROM jsonb_array_elements_text(doc->'embedding') AS t(x)
    ))::vector(3072)             AS embedding,
    NOW()                        AS created_at         -- <== new timestamp column
  FROM staging.section_chunks_embeddings_raw
  WHERE doc ? 'chunk_uuid'
    AND jsonb_typeof(doc->'embedding') = 'array'
    AND jsonb_array_length(doc->'embedding') = 3072
),
upsert AS (
  INSERT INTO prod.section_chunks_embedding_3072 (
    chunk_uuid, model_name, embedding, created_at
  )
  SELECT chunk_uuid, model_name, embedding, created_at
  FROM src
  ON CONFLICT (chunk_uuid, model_name) DO UPDATE
  SET embedding  = EXCLUDED.embedding,
      created_at = EXCLUDED.created_at    -- refresh timestamp on update
  RETURNING 1
)
SELECT count(*) AS rows_upserted FROM upsert;


