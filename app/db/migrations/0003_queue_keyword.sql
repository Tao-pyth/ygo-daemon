CREATE TABLE IF NOT EXISTS request_queue_new(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  konami_id INTEGER,
  keyword TEXT,
  state TEXT NOT NULL DEFAULT 'PENDING',
  attempts INTEGER NOT NULL DEFAULT 0,
  added_at TEXT NOT NULL,
  last_error TEXT,
  CHECK (
    (konami_id IS NOT NULL AND keyword IS NULL)
    OR (konami_id IS NULL AND keyword IS NOT NULL)
  )
);

INSERT INTO request_queue_new(id, konami_id, keyword, state, attempts, added_at, last_error)
SELECT id, konami_id, NULL, state, attempts, added_at, last_error
FROM request_queue;

DROP TABLE request_queue;
ALTER TABLE request_queue_new RENAME TO request_queue;

CREATE INDEX IF NOT EXISTS idx_queue_state ON request_queue(state, id);
CREATE INDEX IF NOT EXISTS idx_queue_konami_state ON request_queue(konami_id, state);
CREATE INDEX IF NOT EXISTS idx_queue_keyword_state ON request_queue(keyword, state);
