TRUNCATE staging.sections_raw;
TRUNCATE staging.section_chunks_raw;
TRUNCATE staging.section_excerpt_embeddings_raw;
TRUNCATE staging.section_chunks_embeddings_raw;

\copy staging.sections_raw (doc) FROM '/Users/leonardjin/Dev/csha/csha-ai-agent-api/doc-etl/data/staged/sections.jsonl' WITH (FORMAT text)

\copy staging.section_chunks_raw (doc) FROM '/Users/leonardjin/Dev/csha/csha-ai-agent-api/doc-etl/data/staged/section_chunks.jsonl' WITH (FORMAT text)

\copy staging.section_excerpt_embeddings_raw (doc) FROM '/Users/leonardjin/Dev/csha/csha-ai-agent-api/doc-etl/data/staged/section_excerpt_embeddings.jsonl' WITH (FORMAT text)

\copy staging.section_chunks_embeddings_raw  (doc) FROM '/Users/leonardjin/Dev/csha/csha-ai-agent-api/doc-etl/data/staged/section_chunks_embeddings.jsonl'  WITH (FORMAT text)
