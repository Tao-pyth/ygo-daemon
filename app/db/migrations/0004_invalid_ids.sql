CREATE TABLE IF NOT EXISTS invalid_ids(
  id INTEGER PRIMARY KEY,
  reason TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_invalid_ids_created_at ON invalid_ids(created_at);
