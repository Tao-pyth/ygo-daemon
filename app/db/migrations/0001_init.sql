CREATE TABLE IF NOT EXISTS kv_store(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS request_queue(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  konami_id INTEGER NOT NULL,
  state TEXT NOT NULL DEFAULT 'PENDING',
  attempts INTEGER NOT NULL DEFAULT 0,
  added_at TEXT NOT NULL,
  last_error TEXT
);

CREATE TABLE IF NOT EXISTS cards_raw(
  card_id INTEGER PRIMARY KEY,
  konami_id INTEGER,
  json TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  dbver_hash TEXT,
  source TEXT NOT NULL,
  fetch_status TEXT NOT NULL DEFAULT 'OK'
);

CREATE TABLE IF NOT EXISTS cards_index(
  card_id INTEGER PRIMARY KEY,
  konami_id INTEGER,
  name TEXT,
  type TEXT,
  race TEXT,
  attribute TEXT,
  level INTEGER,
  atk INTEGER,
  def INTEGER,
  archetype TEXT,
  ban_tcg TEXT,
  ban_ocg TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_files(
  path TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  added_at TEXT NOT NULL,
  processed_at TEXT,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_queue_state ON request_queue(state, id);
CREATE INDEX IF NOT EXISTS idx_raw_konami ON cards_raw(konami_id);
CREATE INDEX IF NOT EXISTS idx_cards_raw_fetch_status ON cards_raw(fetch_status);
CREATE INDEX IF NOT EXISTS idx_index_konami ON cards_index(konami_id);
