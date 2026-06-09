-- Migration: add Stage 2 extended-fact columns.
-- Run once on every Postgres instance (dev + server) before scoring.
-- Safe to re-run: IF NOT EXISTS guards.

ALTER TABLE news_articles
  ADD COLUMN IF NOT EXISTS extracted_facts     JSONB,
  ADD COLUMN IF NOT EXISTS ai_sector_pick_hint TEXT,
  ADD COLUMN IF NOT EXISTS stage2_prompt       TEXT,
  ADD COLUMN IF NOT EXISTS company_connections JSONB;
