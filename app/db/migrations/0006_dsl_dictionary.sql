CREATE TABLE IF NOT EXISTS dsl_dictionary_patterns(
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
  CHECK (category IN ('cost_patterns', 'action_patterns', 'restriction_patterns', 'trigger_patterns')),
  UNIQUE(category, template, dict_ruleset_version)
);

CREATE TABLE IF NOT EXISTS dsl_dictionary_terms(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
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
  UNIQUE(term_type, normalized_term, dict_ruleset_version)
);

CREATE INDEX IF NOT EXISTS idx_dsl_dictionary_patterns_category ON dsl_dictionary_patterns(category, status, count DESC);
CREATE INDEX IF NOT EXISTS idx_dsl_dictionary_terms_type ON dsl_dictionary_terms(term_type, status, count DESC);
