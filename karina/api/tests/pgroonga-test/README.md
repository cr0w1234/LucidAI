# Pgroonga Test Suite

This directory contains standalone tests to verify pgroonga full-text search is working correctly, independent of the main RAG chain.

## Purpose

These tests help diagnose keyword search issues by:
- Verifying pgroonga extension is installed
- Checking if pgroonga indexes exist
- Testing various query formats
- Showing sample results

## Files

- `test_pgroonga.py` - Python script that runs comprehensive pgroonga tests
- `test_queries.sql` - SQL queries you can run directly in psql
- `create_pgroonga_indexes.sql` - SQL script to create required pgroonga indexes
- `README.md` - This file

## Usage

### Option 1: Python Script (Recommended)

```bash
# From the api directory
cd tests/pgroonga_test
python test_pgroonga.py

# Or with a custom query
python test_pgroonga.py "contracts MOUs"
```

The script will:
1. Check if pgroonga extension is installed
2. Verify table counts
3. Check for pgroonga indexes
4. Test multiple query formats
5. Show sample results
6. Provide a summary

### Option 2: SQL Queries

Run the queries in `test_queries.sql` directly in your database client:

```bash
psql $DATABASE_URL -f test_queries.sql
```

Or copy individual queries into your database client.

## What to Look For

### If pgroonga is working:
- ✓ Extension is installed
- ✓ Indexes exist
- ✓ Simple queries like "contracts" return results with scores > 0

### If pgroonga is NOT working:
- ✗ Extension not installed (need to run `CREATE EXTENSION pgroonga;`)
- ✗ No indexes found (need to create indexes)
- ✗ Queries return 0 results even for common terms
- ✗ All scores are NULL or 0

## Common Issues

1. **Extension not installed**: Run `CREATE EXTENSION pgroonga;` in your database
2. **Indexes missing**: **CRITICAL** - Run `create_pgroonga_indexes.sql` to create indexes
   - Without indexes, `pgroonga_score()` returns 0.0 or NULL
   - Without indexes, performance is very slow
   - Indexes are required for proper scoring
3. **Scores are 0.0**: This usually means indexes are missing or need to be rebuilt
   - Even if matches are found, scores will be 0.0 without indexes
   - Run `create_pgroonga_indexes.sql` to fix this
4. **Indexes not built**: May need to rebuild indexes after data changes: `REINDEX INDEX documents_content_pgroonga_idx;`
5. **Query syntax**: pgroonga uses `&@` operator, not standard PostgreSQL full-text search

## Creating Indexes

**IMPORTANT**: pgroonga indexes are required for:
- Proper scoring (scores will be 0.0 without them)
- Good performance
- Accurate ranking

To create indexes:
```bash
psql $DSN -f create_pgroonga_indexes.sql
```

Or run the SQL directly in your database client.

## Next Steps

If tests show pgroonga is not working:
1. Check database logs for errors
2. Verify pgroonga extension version
3. Check if indexes need to be rebuilt
4. Verify the `content` column has the correct data type for pgroonga

