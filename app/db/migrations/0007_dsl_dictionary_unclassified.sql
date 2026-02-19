ALTER TABLE dsl_dictionary_patterns RENAME TO dsl_dictionary_patterns_old;

CREATE TABLE dsl_dictionary_patterns(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
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
  UNIQUE(category, template, dict_ruleset_version)
);

INSERT INTO dsl_dictionary_patterns(
  id, category, template, count, status, dict_ruleset_version, first_seen_at, last_seen_at, updated_at
)
SELECT
  id, category, template, count, status, dict_ruleset_version, first_seen_at, last_seen_at, updated_at
FROM dsl_dictionary_patterns_old;

DROP TABLE dsl_dictionary_patterns_old;

CREATE INDEX IF NOT EXISTS idx_dsl_dictionary_patterns_category ON dsl_dictionary_patterns(category, status, count DESC);
