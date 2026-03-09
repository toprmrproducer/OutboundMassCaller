-- =====================================================
-- RapidXAI Supabase One-Time Setup
-- Run this ONCE in Supabase Dashboard -> SQL Editor
-- before starting the application for the first time.
-- =====================================================

-- Step 1: Enable required extensions
-- (Cannot be done from application code on Supabase)
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;

-- Step 2: Add business_id columns if upgrading
-- from a previous version without them
ALTER TABLE IF EXISTS calls
    ADD COLUMN IF NOT EXISTS business_id UUID;

ALTER TABLE IF EXISTS active_calls
    ADD COLUMN IF NOT EXISTS business_id UUID;

-- Step 3: Create index on knowledge_base for
-- fast vector similarity search
CREATE INDEX IF NOT EXISTS idx_knowledge_base_embedding
    ON knowledge_base
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- Step 4: Verify extensions are active
SELECT extname, extversion
FROM pg_extension
WHERE extname IN ('pgcrypto', 'vector');

-- You should see both pgcrypto and vector in the results.
-- If not, enable them in Dashboard -> Database -> Extensions first.
