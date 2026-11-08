-- Experiment schema v1. Run once with the migration owner, never a gateway role.
CREATE SCHEMA IF NOT EXISTS lcm_experiment;
CREATE TABLE IF NOT EXISTS lcm_experiment.schema_version(version integer PRIMARY KEY CHECK(version=1));
INSERT INTO lcm_experiment.schema_version VALUES(1) ON CONFLICT DO NOTHING;
CREATE SEQUENCE IF NOT EXISTS lcm_experiment.conversation_ids START 1000000;
CREATE SEQUENCE IF NOT EXISTS lcm_experiment.message_ids START 1000000;
CREATE TABLE IF NOT EXISTS lcm_experiment.conversations (
  organization_id text NOT NULL, agent_id text NOT NULL, conversation_id bigint NOT NULL,
  data jsonb NOT NULL, revision integer NOT NULL DEFAULT 0, next_seq integer NOT NULL DEFAULT 0,
  PRIMARY KEY(organization_id, agent_id, conversation_id), UNIQUE(organization_id, conversation_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS lcm_active_sessions ON lcm_experiment.conversations
 (organization_id,agent_id,(data->>'session_key')) WHERE (data->>'active')::int=1;
CREATE TABLE IF NOT EXISTS lcm_experiment.messages (
  organization_id text NOT NULL, agent_id text NOT NULL, conversation_id bigint NOT NULL,
  message_id bigint NOT NULL, seq integer NOT NULL, data jsonb NOT NULL,
  content text NOT NULL, indexed_content text NOT NULL,
  search tsvector GENERATED ALWAYS AS(to_tsvector('simple'::regconfig,indexed_content)) STORED,
  PRIMARY KEY(organization_id,agent_id,conversation_id,message_id), UNIQUE(organization_id,message_id),
  UNIQUE(organization_id,agent_id,conversation_id,seq),
  FOREIGN KEY(organization_id,agent_id,conversation_id) REFERENCES lcm_experiment.conversations ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS lcm_transcript_identity ON lcm_experiment.messages
 (organization_id,agent_id,conversation_id,(data->>'transcript_entry_id')) WHERE data->>'transcript_entry_id' IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS lcm_stable_identity ON lcm_experiment.messages
 (organization_id,agent_id,conversation_id,(data->>'stable_event_key')) WHERE data->>'stable_event_key' IS NOT NULL;
CREATE INDEX IF NOT EXISTS lcm_message_fts ON lcm_experiment.messages USING gin(search);
CREATE INDEX IF NOT EXISTS lcm_message_tail ON lcm_experiment.messages(organization_id,agent_id,conversation_id,seq DESC);
CREATE TABLE IF NOT EXISTS lcm_experiment.summaries (
  organization_id text NOT NULL, agent_id text NOT NULL, conversation_id bigint NOT NULL,
  summary_id text NOT NULL, depth integer NOT NULL CHECK(depth>=0), data jsonb NOT NULL, content text NOT NULL,
  search tsvector GENERATED ALWAYS AS(to_tsvector('simple'::regconfig,content)) STORED,
  PRIMARY KEY(organization_id,agent_id,conversation_id,summary_id), UNIQUE(organization_id,summary_id),
  FOREIGN KEY(organization_id,agent_id,conversation_id) REFERENCES lcm_experiment.conversations ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS lcm_summary_fts ON lcm_experiment.summaries USING gin(search);
CREATE TABLE IF NOT EXISTS lcm_experiment.summary_messages (
  organization_id text NOT NULL, agent_id text NOT NULL, conversation_id bigint NOT NULL,
  summary_id text NOT NULL, message_id bigint NOT NULL, ordinal integer NOT NULL CHECK(ordinal>=0), data jsonb NOT NULL,
  PRIMARY KEY(organization_id,agent_id,conversation_id,summary_id,message_id),
  UNIQUE(organization_id,agent_id,conversation_id,summary_id,ordinal),
  FOREIGN KEY(organization_id,agent_id,conversation_id,summary_id) REFERENCES lcm_experiment.summaries ON DELETE CASCADE,
  FOREIGN KEY(organization_id,agent_id,conversation_id,message_id) REFERENCES lcm_experiment.messages
);
CREATE TABLE IF NOT EXISTS lcm_experiment.summary_parents (
  organization_id text NOT NULL, agent_id text NOT NULL, conversation_id bigint NOT NULL,
  summary_id text NOT NULL, parent_summary_id text NOT NULL, ordinal integer NOT NULL CHECK(ordinal>=0), data jsonb NOT NULL,
  PRIMARY KEY(organization_id,agent_id,conversation_id,summary_id,parent_summary_id),
  UNIQUE(organization_id,agent_id,conversation_id,summary_id,ordinal), CHECK(summary_id<>parent_summary_id),
  FOREIGN KEY(organization_id,agent_id,conversation_id,summary_id) REFERENCES lcm_experiment.summaries ON DELETE CASCADE,
  FOREIGN KEY(organization_id,agent_id,conversation_id,parent_summary_id) REFERENCES lcm_experiment.summaries
);
CREATE INDEX IF NOT EXISTS lcm_summary_reverse_edges ON lcm_experiment.summary_parents
 (organization_id,agent_id,conversation_id,parent_summary_id,ordinal);
CREATE TABLE IF NOT EXISTS lcm_experiment.context_items (
  organization_id text NOT NULL, agent_id text NOT NULL, conversation_id bigint NOT NULL,
  ordinal integer NOT NULL CHECK(ordinal>=0), message_id bigint, summary_id text, data jsonb NOT NULL,
  PRIMARY KEY(organization_id,agent_id,conversation_id,ordinal),
  CHECK((message_id IS NULL) <> (summary_id IS NULL)),
  FOREIGN KEY(organization_id,agent_id,conversation_id) REFERENCES lcm_experiment.conversations ON DELETE CASCADE,
  FOREIGN KEY(organization_id,agent_id,conversation_id,message_id) REFERENCES lcm_experiment.messages,
  FOREIGN KEY(organization_id,agent_id,conversation_id,summary_id) REFERENCES lcm_experiment.summaries
);
CREATE TABLE IF NOT EXISTS lcm_experiment.large_files (
  organization_id text NOT NULL, agent_id text NOT NULL, conversation_id bigint NOT NULL,
  file_id text NOT NULL, data jsonb NOT NULL,
  PRIMARY KEY(organization_id,agent_id,conversation_id,file_id), UNIQUE(organization_id,file_id),
  FOREIGN KEY(organization_id,agent_id,conversation_id) REFERENCES lcm_experiment.conversations ON DELETE CASCADE
);
-- Exact logical rows for state families whose engine adapters are not yet ported.
-- Kept separate from core rows; this is not an operational pending-compaction API.
CREATE TABLE IF NOT EXISTS lcm_experiment.auxiliary_rows (
  organization_id text NOT NULL, table_name text NOT NULL, row_key text NOT NULL,
  agent_id text, conversation_id bigint, data jsonb NOT NULL,
  PRIMARY KEY(organization_id,table_name,row_key),
  FOREIGN KEY(organization_id,agent_id,conversation_id) REFERENCES lcm_experiment.conversations ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS lcm_experiment.turn_receipts (
  organization_id text NOT NULL, agent_id text NOT NULL, conversation_id bigint NOT NULL,
  receipt_key text NOT NULL, fingerprint text NOT NULL, result jsonb NOT NULL,
  PRIMARY KEY(organization_id,agent_id,conversation_id,receipt_key),
  FOREIGN KEY(organization_id,agent_id,conversation_id) REFERENCES lcm_experiment.conversations ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS lcm_experiment.import_chunks (
  fixture_hash text NOT NULL, organization_id text NOT NULL, conversation_id bigint NOT NULL,
  PRIMARY KEY(fixture_hash,organization_id,conversation_id)
);
CREATE TABLE IF NOT EXISTS lcm_experiment.import_metadata (
  fixture_hash text PRIMARY KEY, metadata jsonb NOT NULL
);
-- Tenant role mapping is owner-managed; runtime roles cannot change identity.
CREATE TABLE IF NOT EXISTS lcm_experiment.tenant_roles(role_name name PRIMARY KEY, organization_id text NOT NULL);
ALTER TABLE lcm_experiment.tenant_roles ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_role_identity ON lcm_experiment.tenant_roles;
CREATE POLICY tenant_role_identity ON lcm_experiment.tenant_roles USING(role_name=current_user);
DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['conversations','messages','summaries','summary_messages','summary_parents','context_items','large_files','auxiliary_rows','turn_receipts'] LOOP
    EXECUTE format('ALTER TABLE lcm_experiment.%I ENABLE ROW LEVEL SECURITY',t);
    EXECUTE format('DROP POLICY IF EXISTS tenant_identity ON lcm_experiment.%I',t);
    EXECUTE format('CREATE POLICY tenant_identity ON lcm_experiment.%I USING(organization_id=(SELECT organization_id FROM lcm_experiment.tenant_roles WHERE role_name=current_user)) WITH CHECK(organization_id=(SELECT organization_id FROM lcm_experiment.tenant_roles WHERE role_name=current_user))',t);
  END LOOP;
END $$;
