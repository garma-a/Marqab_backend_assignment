create table if not exists rule_versions (
  rule_id text not null,
  version integer not null check (version > 0),
  definition jsonb not null,
  status text not null check (status in ('DRAFT', 'PUBLISHED', 'RETIRED')),
  created_at timestamptz not null default clock_timestamp(),
  primary key (rule_id, version)
);

create table if not exists signal_events (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  scope_id text not null,
  source_id text not null,
  signal_code text not null,
  event_id text not null,
  observed_at timestamptz not null,
  received_at timestamptz not null,
  value numeric not null,
  content_hash text not null,
  lateness_status text not null default 'ON_TIME'
    check (lateness_status in ('ON_TIME', 'LATE', 'TOO_LATE')),
  created_at timestamptz not null default clock_timestamp(),
  unique (tenant_id, scope_id, source_id, signal_code, event_id)
);

create index if not exists signal_events_series_observed_idx
  on signal_events (tenant_id, scope_id, source_id, signal_code, observed_at, event_id);

create table if not exists series_heads (
  tenant_id text not null,
  scope_id text not null,
  source_id text not null,
  signal_code text not null,
  watermark_at timestamptz,
  updated_at timestamptz not null default clock_timestamp(),
  primary key (tenant_id, scope_id, source_id, signal_code)
);

create table if not exists decision_records (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  scope_id text not null,
  source_id text not null,
  signal_code text not null,
  rule_id text not null,
  rule_version integer not null,
  input_event_ids text[] not null default '{}',
  window_started_at timestamptz,
  window_ended_at timestamptz,
  outcome text not null check (outcome in ('UNCONFIRMED', 'CONFIRMED')),
  decision_fingerprint text not null,
  supersedes_decision_id uuid references decision_records(id) on delete restrict,
  decision_kind text not null default 'ORIGINAL'
    check (decision_kind in ('ORIGINAL', 'CORRECTION')),
  created_at timestamptz not null default clock_timestamp(),
  foreign key (rule_id, rule_version) references rule_versions(rule_id, version) on delete restrict
);

create index if not exists decision_records_series_created_idx
  on decision_records (tenant_id, scope_id, source_id, signal_code, created_at desc);

create table if not exists current_signal_state (
  tenant_id text not null,
  scope_id text not null,
  source_id text not null,
  signal_code text not null,
  decision_id uuid not null references decision_records(id) on delete restrict,
  outcome text not null check (outcome in ('UNCONFIRMED', 'CONFIRMED')),
  updated_at timestamptz not null default clock_timestamp(),
  primary key (tenant_id, scope_id, source_id, signal_code)
);

create table if not exists command_outbox (
  id uuid primary key default gen_random_uuid(),
  decision_id uuid not null references decision_records(id) on delete restrict,
  command_type text not null,
  payload jsonb not null,
  status text not null default 'PENDING'
    check (status in ('PENDING', 'PROCESSING', 'PROCESSED', 'FAILED')),
  created_at timestamptz not null default clock_timestamp()
);

create index if not exists command_outbox_claim_idx
  on command_outbox (status, created_at);
