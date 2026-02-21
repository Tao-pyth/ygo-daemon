ALTER TABLE dsl_dictionary_patterns RENAME TO dsl_dictionary_patterns_old;

CREATE TABLE dsl_dictionary_patterns(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ruleset_id INTEGER NOT NULL,
  category TEXT NOT NULL,
  template TEXT NOT NULL,
  count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'candidate',
  dict_ruleset_version TEXT NOT NULL,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK (status IN ('candidate', 'accepted', 'rejected')),
  CHECK (category IN ('cost_patterns', 'action_patterns', 'restriction_patterns', 'trigger_patterns', 'condition_patterns', 'unclassified_patterns')),
  UNIQUE(ruleset_id, template)
);

INSERT INTO dsl_dictionary_patterns(
  id, ruleset_id, category, template, count, status, dict_ruleset_version, first_seen_at, last_seen_at, updated_at
)
SELECT
  id, 1, category, template, count, status, dict_ruleset_version, first_seen_at, last_seen_at, updated_at
FROM dsl_dictionary_patterns_old;

DROP TABLE dsl_dictionary_patterns_old;

CREATE INDEX IF NOT EXISTS idx_dsl_dictionary_patterns_ruleset_category
  ON dsl_dictionary_patterns(ruleset_id, category, status, count DESC);

ALTER TABLE dsl_dictionary_terms RENAME TO dsl_dictionary_terms_old;

CREATE TABLE dsl_dictionary_terms(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ruleset_id INTEGER NOT NULL,
  term_type TEXT NOT NULL,
  normalized_term TEXT NOT NULL,
  placeholder TEXT NOT NULL,
  count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'candidate',
  dict_ruleset_version TEXT NOT NULL,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK (status IN ('candidate', 'accepted', 'rejected')),
  CHECK (term_type IN ('zone_dictionary', 'target_dictionary')),
  UNIQUE(ruleset_id, term_type, normalized_term)
);

INSERT INTO dsl_dictionary_terms(
  id, ruleset_id, term_type, normalized_term, placeholder, count, status, dict_ruleset_version, first_seen_at, last_seen_at, updated_at
)
SELECT
  id, 1, term_type, normalized_term, placeholder, count, status, dict_ruleset_version, first_seen_at, last_seen_at, updated_at
FROM dsl_dictionary_terms_old;

DROP TABLE dsl_dictionary_terms_old;

CREATE INDEX IF NOT EXISTS idx_dsl_dictionary_terms_ruleset_type
  ON dsl_dictionary_terms(ruleset_id, term_type, count DESC);

CREATE TABLE IF NOT EXISTS dict_build_processed_cards(
  card_id INTEGER NOT NULL,
  ruleset_id INTEGER NOT NULL,
  processed_at TEXT NOT NULL,
  PRIMARY KEY (card_id, ruleset_id)
);

CREATE INDEX IF NOT EXISTS idx_dict_build_processed_ruleset_time
  ON dict_build_processed_cards(ruleset_id, processed_at);

CREATE INDEX IF NOT EXISTS idx_dict_build_processed_card_id
  ON dict_build_processed_cards(card_id);

INSERT INTO kv_store(key, value)
VALUES('dict_build:latest_ruleset_id', '2')
ON CONFLICT(key) DO NOTHING;
