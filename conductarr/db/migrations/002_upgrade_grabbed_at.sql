-- Add upgrade_grabbed_at column to track when a grab was dispatched.
-- Used by count_grabbed_not_in_jobmap to survive restarts.
ALTER TABLE queue_items ADD COLUMN upgrade_grabbed_at TEXT;
