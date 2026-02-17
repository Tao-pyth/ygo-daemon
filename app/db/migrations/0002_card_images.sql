CREATE TABLE IF NOT EXISTS card_images(
  card_id INTEGER PRIMARY KEY,
  image_url TEXT,
  image_path TEXT,
  fetch_status TEXT NOT NULL DEFAULT 'NEED_FETCH',
  last_error TEXT,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(card_id) REFERENCES cards_raw(card_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_card_images_fetch_status ON card_images(fetch_status, card_id);
