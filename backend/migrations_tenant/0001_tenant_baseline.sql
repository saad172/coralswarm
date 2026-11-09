-- 0001_tenant_baseline.sql — squashed baseline for the per-org TENANT tree.
--
-- Generated (NOT hand-authored) from the control tree at all migrations through
-- 0077, via: apply control tree to a scratch DB -> DROP the 14 control tables
-- CASCADE (strips cross-plane FKs to users/accounts while keeping the bare uuid
-- columns) -> pg_dump --schema-only of the 39 tenant tables. Regenerate the same
-- way; do NOT hand-edit to diverge. The parity CI job (check_schema_parity.sh)
-- fails if this baseline and the control tree disagree on any tenant table.
--
-- Cross-DB references (account_id, owner_id, ingested_by, created_by, ...) are
-- bare uuid columns with no FK — users/accounts live in the control DB.
--
-- PostgreSQL database dump
--




--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: vector; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;


--
-- Name: job_status; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.job_status AS ENUM (
    'pending',
    'running',
    'done',
    'failed',
    'dead'
);


--
-- Name: backfill_atom_embed_versioning(integer); Type: PROCEDURE; Schema: public; Owner: -
--

CREATE PROCEDURE public.backfill_atom_embed_versioning(IN batch_size integer DEFAULT 5000)
    LANGUAGE plpgsql
    AS $$
DECLARE
    rows_updated INT;
BEGIN
    LOOP
        UPDATE atoms
        SET    embed_model     = 'bge-small-en-v1.5',
               chunker_version = 1
        WHERE  atom_id IN (
            SELECT atom_id
            FROM   atoms
            WHERE  embedding IS NOT NULL
              AND  embed_model IS NULL
            LIMIT  batch_size
        );
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        COMMIT;
        EXIT WHEN rows_updated = 0;
    END LOOP;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: activity_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.activity_events (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    user_id uuid,
    graph_id uuid,
    event_type text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT activity_events_event_type_check CHECK ((event_type = ANY (ARRAY['atom_created'::text, 'query_run'::text, 'node_created'::text, 'graph_created'::text, 'atom_ingested'::text, 'node_updated'::text, 'supersession_created'::text])))
);


--
-- Name: atoms; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.atoms (
    atom_id uuid DEFAULT gen_random_uuid() NOT NULL,
    graph_id uuid NOT NULL,
    source_type text NOT NULL,
    source_id text NOT NULL,
    content_blob_key text NOT NULL,
    embedding public.vector(384),
    participants text[] DEFAULT '{}'::text[] NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    ingested_by uuid,
    scope text DEFAULT 'private'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    embed_failed_at timestamp with time zone,
    content_tsv tsvector,
    content_inline text,
    fts_language text DEFAULT 'english'::text NOT NULL,
    channel_id text,
    channel_name text,
    author_id text,
    author_name text,
    workspace_id text,
    embed_model text,
    chunker_version smallint,
    embed_attempts integer DEFAULT 0 NOT NULL,
    header_len integer,
    project text,
    mcp_client_name text
);


--
-- Name: coding_session_shares; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.coding_session_shares (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    graph_id uuid NOT NULL,
    session_id text NOT NULL,
    token text NOT NULL,
    visibility text DEFAULT 'link'::text NOT NULL,
    created_by uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    revoked_at timestamp with time zone,
    CONSTRAINT coding_session_shares_visibility_check CHECK ((visibility = ANY (ARRAY['link'::text, 'team'::text])))
);


--
-- Name: coding_sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.coding_sessions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    graph_id uuid NOT NULL,
    session_id text NOT NULL,
    project_key text,
    title text,
    repo_path text,
    remote_sanitized text,
    hostname text,
    harness_version text,
    model text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    last_activity_at timestamp with time zone DEFAULT now() NOT NULL,
    raw_blob_key text,
    raw_inline text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: connector_identities; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.connector_identities (
    provider text NOT NULL,
    workspace_id text NOT NULL,
    external_user_id text NOT NULL,
    canonical_name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: conversation_messages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.conversation_messages (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    conversation_id uuid NOT NULL,
    seq integer NOT NULL,
    role text NOT NULL,
    text text DEFAULT ''::text NOT NULL,
    citations jsonb DEFAULT '[]'::jsonb NOT NULL,
    sources jsonb DEFAULT '[]'::jsonb NOT NULL,
    mode text,
    timing jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    scope_graph_ids jsonb DEFAULT '[]'::jsonb NOT NULL,
    CONSTRAINT conversation_messages_role_check CHECK ((role = ANY (ARRAY['user'::text, 'assistant'::text])))
);


--
-- Name: conversation_oceans; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.conversation_oceans (
    conversation_id uuid NOT NULL,
    graph_id uuid NOT NULL
);


--
-- Name: conversation_surfaced_edges; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.conversation_surfaced_edges (
    conversation_id uuid NOT NULL,
    edge_id uuid NOT NULL
);


--
-- Name: conversations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.conversations (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    graph_id uuid NOT NULL,
    user_id uuid NOT NULL,
    title text DEFAULT ''::text NOT NULL,
    pinned boolean DEFAULT false NOT NULL,
    shared boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    mode text DEFAULT 'shared'::text NOT NULL,
    CONSTRAINT conversations_mode_check CHECK ((mode = ANY (ARRAY['shared'::text, 'private'::text])))
);


--
-- Name: graph_subscriptions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.graph_subscriptions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    subscription_id text NOT NULL,
    resource text NOT NULL,
    client_state text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    tenant_id text DEFAULT ''::text NOT NULL
);


--
-- Name: graphs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.graphs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    kind text DEFAULT 'personal'::text NOT NULL,
    is_primary boolean DEFAULT false NOT NULL,
    self_node_id uuid,
    CONSTRAINT graphs_kind_check CHECK ((kind = ANY (ARRAY['personal'::text, 'team'::text, 'org'::text])))
);


--
-- Name: jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.jobs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    status public.job_status DEFAULT 'pending'::public.job_status NOT NULL,
    idempotency_key text,
    attempts integer DEFAULT 0 NOT NULL,
    max_attempts integer DEFAULT 3 NOT NULL,
    run_after timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: llm_usage; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.llm_usage (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid,
    graph_id uuid,
    task text NOT NULL,
    model text NOT NULL,
    input_tokens integer DEFAULT 0 NOT NULL,
    output_tokens integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    cache_creation_input_tokens bigint DEFAULT 0 NOT NULL,
    cache_read_input_tokens bigint DEFAULT 0 NOT NULL
);


--
-- Name: mcp_tool_calls; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.mcp_tool_calls (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid,
    user_id uuid,
    client_id text,
    token_jti text,
    tool text NOT NULL,
    scope_used text,
    graph_id uuid,
    target_id uuid,
    args_digest text,
    args_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    outcome text NOT NULL,
    error_code text,
    duration_ms bigint,
    request_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: meeting_shares; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.meeting_shares (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    graph_id uuid NOT NULL,
    source_id text NOT NULL,
    token text NOT NULL,
    visibility text DEFAULT 'link'::text NOT NULL,
    created_by uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    revoked_at timestamp with time zone,
    CONSTRAINT meeting_shares_visibility_check CHECK ((visibility = ANY (ARRAY['link'::text, 'team'::text])))
);


--
-- Name: meeting_transcripts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.meeting_transcripts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    graph_id uuid NOT NULL,
    source_id text NOT NULL,
    raw_blob_key text,
    raw_inline text,
    duration_ms bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    segments jsonb,
    summary jsonb,
    summary_status text DEFAULT 'pending'::text NOT NULL,
    summary_model text,
    summary_error text,
    summary_updated_at timestamp with time zone,
    CONSTRAINT meeting_transcripts_summary_status_check CHECK ((summary_status = ANY (ARRAY['pending'::text, 'running'::text, 'done'::text, 'failed'::text, 'blocked'::text])))
);


--
-- Name: node_atoms; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.node_atoms (
    node_id uuid NOT NULL,
    atom_id uuid NOT NULL
);


--
-- Name: node_merges; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.node_merges (
    merge_id uuid DEFAULT gen_random_uuid() NOT NULL,
    graph_id uuid NOT NULL,
    keep_id uuid NOT NULL,
    dup_id uuid NOT NULL,
    dup_snapshot jsonb NOT NULL,
    moved_atom_ids jsonb DEFAULT '[]'::jsonb NOT NULL,
    repointed_edges jsonb DEFAULT '[]'::jsonb NOT NULL,
    repointed_supersessions jsonb DEFAULT '[]'::jsonb NOT NULL,
    repointed_promotions jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    undone_at timestamp with time zone,
    self_repointed boolean DEFAULT false NOT NULL
);


--
-- Name: node_promotions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.node_promotions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    source_node_id uuid NOT NULL,
    target_graph_id uuid NOT NULL,
    target_node_id uuid NOT NULL,
    promoted_by uuid,
    from_version integer NOT NULL,
    promoted_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: nodes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nodes (
    node_id uuid DEFAULT gen_random_uuid() NOT NULL,
    graph_id uuid NOT NULL,
    type text NOT NULL,
    title text NOT NULL,
    body jsonb DEFAULT '{}'::jsonb NOT NULL,
    scope text DEFAULT 'private'::text NOT NULL,
    owner_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version integer DEFAULT 1 NOT NULL,
    CONSTRAINT decisions_require_decided_at CHECK (((type <> 'decision'::text) OR ((body ->> 'decided_at'::text) IS NOT NULL))),
    CONSTRAINT nodes_type_check CHECK ((type = ANY (ARRAY['entity'::text, 'decision'::text, 'thread'::text, 'reference'::text])))
);


--
-- Name: non_supersession_pair; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.non_supersession_pair (
    node_a_id uuid NOT NULL,
    node_b_id uuid NOT NULL,
    marked_by uuid,
    marked_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT non_supersession_pair_ordered CHECK ((node_a_id < node_b_id))
);


--
-- Name: oauth_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.oauth_tokens (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    provider text NOT NULL,
    scope text NOT NULL,
    access_token bytea NOT NULL,
    refresh_token bytea,
    token_type text DEFAULT 'Bearer'::text NOT NULL,
    expires_at timestamp with time zone,
    provider_user_id text,
    provider_workspace_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    user_token_encrypted bytea
);


--
-- Name: op_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.op_log (
    seq bigint NOT NULL,
    op_id uuid NOT NULL,
    entity text NOT NULL,
    entity_id text NOT NULL,
    op_type text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    version bigint DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    device_id text NOT NULL,
    graph_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT op_log_op_type_check CHECK ((op_type = ANY (ARRAY['upsert'::text, 'delete'::text])))
);


--
-- Name: op_log_seq_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.op_log_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: op_log_seq_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.op_log_seq_seq OWNED BY public.op_log.seq;


--
-- Name: outlook_watched_conversations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.outlook_watched_conversations (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    graph_id uuid NOT NULL,
    ingested_by uuid NOT NULL,
    conversation_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_ingested_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: outlook_workflows; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.outlook_workflows (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    user_id uuid NOT NULL,
    graph_id uuid NOT NULL,
    name text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    filter jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: projects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.projects (
    graph_id uuid NOT NULL,
    project_key text NOT NULL,
    display_name text,
    aliases text[] DEFAULT '{}'::text[] NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: queries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.queries (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    user_id uuid NOT NULL,
    text text NOT NULL,
    scope text DEFAULT 'personal'::text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    answer text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone
);


--
-- Name: query_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.query_history (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    graph_id uuid NOT NULL,
    user_id uuid,
    query_text text NOT NULL,
    answer_summary text DEFAULT ''::text NOT NULL,
    citation_count integer DEFAULT 0 NOT NULL,
    input_tokens integer DEFAULT 0 NOT NULL,
    output_tokens integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: relationship_edge; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.relationship_edge (
    edge_id uuid DEFAULT gen_random_uuid() NOT NULL,
    graph_id uuid NOT NULL,
    src_node_id uuid NOT NULL,
    predicate text NOT NULL,
    dst_node_id uuid NOT NULL,
    origin text DEFAULT 'stated'::text NOT NULL,
    status text DEFAULT 'suggested'::text NOT NULL,
    confidence real DEFAULT 1.0 NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    version integer DEFAULT 1 NOT NULL,
    CONSTRAINT relationship_edge_no_self CHECK ((src_node_id <> dst_node_id)),
    CONSTRAINT relationship_edge_origin_check CHECK ((origin = ANY (ARRAY['stated'::text, 'inferred'::text]))),
    CONSTRAINT relationship_edge_status_check CHECK ((status = ANY (ARRAY['suggested'::text, 'confirmed'::text, 'rejected'::text])))
);


--
-- Name: slack_huddle_captures; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.slack_huddle_captures (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    channel_id text NOT NULL,
    huddle_thread_ts text NOT NULL,
    opted_by text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    processed_at timestamp with time zone,
    CONSTRAINT slack_huddle_captures_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'processing'::text, 'processed'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: slack_integrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.slack_integrations (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    workspace_id text NOT NULL,
    workspace_name text NOT NULL,
    bot_token text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: supersession_edge; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.supersession_edge (
    superseded_id uuid NOT NULL,
    superseder_id uuid NOT NULL,
    confirmed_by uuid,
    confirmed_at timestamp with time zone,
    source text NOT NULL
);


--
-- Name: sync_cursors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sync_cursors (
    peer text NOT NULL,
    cursor bigint DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: team_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.team_keys (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    graph_id uuid NOT NULL,
    version integer DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by uuid NOT NULL
);


--
-- Name: team_member_key_envelopes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.team_member_key_envelopes (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    team_key_id uuid NOT NULL,
    user_id uuid NOT NULL,
    wrapped_key bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: team_members; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.team_members (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    team_id uuid NOT NULL,
    user_id uuid NOT NULL,
    role text NOT NULL,
    joined_at timestamp with time zone DEFAULT now() NOT NULL,
    departing_at timestamp with time zone,
    CONSTRAINT team_members_role_check CHECK ((role = ANY (ARRAY['admin'::text, 'member'::text])))
);


--
-- Name: teams; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.teams (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    graph_id uuid NOT NULL,
    name text NOT NULL,
    created_by uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: teams_watched_resources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.teams_watched_resources (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    account_id uuid NOT NULL,
    user_id uuid NOT NULL,
    graph_id uuid NOT NULL,
    kind text NOT NULL,
    team_id text DEFAULT ''::text NOT NULL,
    channel_id text DEFAULT ''::text NOT NULL,
    chat_id text DEFAULT ''::text NOT NULL,
    display_name text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_ingested_at timestamp with time zone DEFAULT now() NOT NULL,
    tenant_id text DEFAULT ''::text NOT NULL,
    auth_mode text DEFAULT 'delegated'::text NOT NULL,
    history_choice text,
    CONSTRAINT teams_watched_resources_auth_mode_check CHECK ((auth_mode = ANY (ARRAY['delegated'::text, 'app'::text]))),
    CONSTRAINT teams_watched_resources_kind_check CHECK ((kind = ANY (ARRAY['channel'::text, 'chat'::text])))
);


--
-- Name: transcript_corrections; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.transcript_corrections (
    correction_id uuid DEFAULT gen_random_uuid() NOT NULL,
    graph_id uuid NOT NULL,
    atom_id uuid NOT NULL,
    source_id text NOT NULL,
    surface_form text NOT NULL,
    canonical text NOT NULL,
    node_id uuid,
    confidence real,
    status text DEFAULT 'applied'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    kind text DEFAULT 'name'::text NOT NULL,
    origin text DEFAULT 'extraction'::text NOT NULL,
    occurrence integer,
    CONSTRAINT transcript_corrections_kind_check CHECK ((kind = ANY (ARRAY['name'::text, 'speaker'::text]))),
    CONSTRAINT transcript_corrections_origin_check CHECK ((origin = ANY (ARRAY['extraction'::text, 'user'::text]))),
    CONSTRAINT transcript_corrections_status_check CHECK ((status = ANY (ARRAY['applied'::text, 'reverted'::text])))
);


--
-- Name: op_log seq; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.op_log ALTER COLUMN seq SET DEFAULT nextval('public.op_log_seq_seq'::regclass);


--
-- Name: activity_events activity_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_events
    ADD CONSTRAINT activity_events_pkey PRIMARY KEY (id);


--
-- Name: atoms atoms_graph_id_source_type_source_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.atoms
    ADD CONSTRAINT atoms_graph_id_source_type_source_id_key UNIQUE (graph_id, source_type, source_id);


--
-- Name: atoms atoms_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.atoms
    ADD CONSTRAINT atoms_pkey PRIMARY KEY (atom_id);


--
-- Name: coding_session_shares coding_session_shares_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.coding_session_shares
    ADD CONSTRAINT coding_session_shares_pkey PRIMARY KEY (id);


--
-- Name: coding_session_shares coding_session_shares_token_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.coding_session_shares
    ADD CONSTRAINT coding_session_shares_token_key UNIQUE (token);


--
-- Name: coding_sessions coding_sessions_graph_id_session_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.coding_sessions
    ADD CONSTRAINT coding_sessions_graph_id_session_id_key UNIQUE (graph_id, session_id);


--
-- Name: coding_sessions coding_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.coding_sessions
    ADD CONSTRAINT coding_sessions_pkey PRIMARY KEY (id);


--
-- Name: connector_identities connector_identities_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.connector_identities
    ADD CONSTRAINT connector_identities_pkey PRIMARY KEY (provider, workspace_id, external_user_id);


--
-- Name: conversation_messages conversation_messages_conversation_id_seq_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversation_messages
    ADD CONSTRAINT conversation_messages_conversation_id_seq_key UNIQUE (conversation_id, seq);


--
-- Name: conversation_messages conversation_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversation_messages
    ADD CONSTRAINT conversation_messages_pkey PRIMARY KEY (id);


--
-- Name: conversation_oceans conversation_oceans_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversation_oceans
    ADD CONSTRAINT conversation_oceans_pkey PRIMARY KEY (conversation_id, graph_id);


--
-- Name: conversation_surfaced_edges conversation_surfaced_edges_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversation_surfaced_edges
    ADD CONSTRAINT conversation_surfaced_edges_pkey PRIMARY KEY (conversation_id, edge_id);


--
-- Name: conversations conversations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversations
    ADD CONSTRAINT conversations_pkey PRIMARY KEY (id);


--
-- Name: graph_subscriptions graph_subscriptions_account_id_resource_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.graph_subscriptions
    ADD CONSTRAINT graph_subscriptions_account_id_resource_key UNIQUE (account_id, resource);


--
-- Name: graph_subscriptions graph_subscriptions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.graph_subscriptions
    ADD CONSTRAINT graph_subscriptions_pkey PRIMARY KEY (id);


--
-- Name: graph_subscriptions graph_subscriptions_subscription_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.graph_subscriptions
    ADD CONSTRAINT graph_subscriptions_subscription_id_key UNIQUE (subscription_id);


--
-- Name: graphs graphs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.graphs
    ADD CONSTRAINT graphs_pkey PRIMARY KEY (id);


--
-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);


--
-- Name: llm_usage llm_usage_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.llm_usage
    ADD CONSTRAINT llm_usage_pkey PRIMARY KEY (id);


--
-- Name: mcp_tool_calls mcp_tool_calls_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.mcp_tool_calls
    ADD CONSTRAINT mcp_tool_calls_pkey PRIMARY KEY (id);


--
-- Name: meeting_shares meeting_shares_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.meeting_shares
    ADD CONSTRAINT meeting_shares_pkey PRIMARY KEY (id);


--
-- Name: meeting_shares meeting_shares_token_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.meeting_shares
    ADD CONSTRAINT meeting_shares_token_key UNIQUE (token);


--
-- Name: meeting_transcripts meeting_transcripts_graph_id_source_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.meeting_transcripts
    ADD CONSTRAINT meeting_transcripts_graph_id_source_id_key UNIQUE (graph_id, source_id);


--
-- Name: meeting_transcripts meeting_transcripts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.meeting_transcripts
    ADD CONSTRAINT meeting_transcripts_pkey PRIMARY KEY (id);


--
-- Name: node_atoms node_atoms_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.node_atoms
    ADD CONSTRAINT node_atoms_pkey PRIMARY KEY (node_id, atom_id);


--
-- Name: node_merges node_merges_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.node_merges
    ADD CONSTRAINT node_merges_pkey PRIMARY KEY (merge_id);


--
-- Name: node_promotions node_promotions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.node_promotions
    ADD CONSTRAINT node_promotions_pkey PRIMARY KEY (id);


--
-- Name: node_promotions node_promotions_source_node_id_target_graph_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.node_promotions
    ADD CONSTRAINT node_promotions_source_node_id_target_graph_id_key UNIQUE (source_node_id, target_graph_id);


--
-- Name: nodes nodes_graph_type_title_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT nodes_graph_type_title_unique UNIQUE (graph_id, type, title);


--
-- Name: nodes nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT nodes_pkey PRIMARY KEY (node_id);


--
-- Name: non_supersession_pair non_supersession_pair_node_a_id_node_b_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.non_supersession_pair
    ADD CONSTRAINT non_supersession_pair_node_a_id_node_b_id_key UNIQUE (node_a_id, node_b_id);


--
-- Name: oauth_tokens oauth_tokens_account_id_provider_provider_workspace_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.oauth_tokens
    ADD CONSTRAINT oauth_tokens_account_id_provider_provider_workspace_id_key UNIQUE (account_id, provider, provider_workspace_id);


--
-- Name: oauth_tokens oauth_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.oauth_tokens
    ADD CONSTRAINT oauth_tokens_pkey PRIMARY KEY (id);


--
-- Name: op_log op_log_op_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.op_log
    ADD CONSTRAINT op_log_op_id_key UNIQUE (op_id);


--
-- Name: op_log op_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.op_log
    ADD CONSTRAINT op_log_pkey PRIMARY KEY (seq);


--
-- Name: outlook_watched_conversations outlook_watched_conversations_account_id_conversation_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outlook_watched_conversations
    ADD CONSTRAINT outlook_watched_conversations_account_id_conversation_id_key UNIQUE (account_id, conversation_id);


--
-- Name: outlook_watched_conversations outlook_watched_conversations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outlook_watched_conversations
    ADD CONSTRAINT outlook_watched_conversations_pkey PRIMARY KEY (id);


--
-- Name: outlook_workflows outlook_workflows_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outlook_workflows
    ADD CONSTRAINT outlook_workflows_pkey PRIMARY KEY (id);


--
-- Name: projects projects_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_pkey PRIMARY KEY (graph_id, project_key);


--
-- Name: queries queries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.queries
    ADD CONSTRAINT queries_pkey PRIMARY KEY (id);


--
-- Name: query_history query_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.query_history
    ADD CONSTRAINT query_history_pkey PRIMARY KEY (id);


--
-- Name: relationship_edge relationship_edge_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.relationship_edge
    ADD CONSTRAINT relationship_edge_pkey PRIMARY KEY (edge_id);


--
-- Name: relationship_edge relationship_edge_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.relationship_edge
    ADD CONSTRAINT relationship_edge_unique UNIQUE (graph_id, src_node_id, predicate, dst_node_id);


--
-- Name: slack_huddle_captures slack_huddle_captures_account_id_channel_id_huddle_thread_t_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.slack_huddle_captures
    ADD CONSTRAINT slack_huddle_captures_account_id_channel_id_huddle_thread_t_key UNIQUE (account_id, channel_id, huddle_thread_ts);


--
-- Name: slack_huddle_captures slack_huddle_captures_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.slack_huddle_captures
    ADD CONSTRAINT slack_huddle_captures_pkey PRIMARY KEY (id);


--
-- Name: slack_integrations slack_integrations_account_id_workspace_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.slack_integrations
    ADD CONSTRAINT slack_integrations_account_id_workspace_id_key UNIQUE (account_id, workspace_id);


--
-- Name: slack_integrations slack_integrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.slack_integrations
    ADD CONSTRAINT slack_integrations_pkey PRIMARY KEY (id);


--
-- Name: supersession_edge supersession_edge_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.supersession_edge
    ADD CONSTRAINT supersession_edge_pkey PRIMARY KEY (superseded_id, superseder_id);


--
-- Name: sync_cursors sync_cursors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sync_cursors
    ADD CONSTRAINT sync_cursors_pkey PRIMARY KEY (peer);


--
-- Name: team_keys team_keys_graph_id_version_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_keys
    ADD CONSTRAINT team_keys_graph_id_version_key UNIQUE (graph_id, version);


--
-- Name: team_keys team_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_keys
    ADD CONSTRAINT team_keys_pkey PRIMARY KEY (id);


--
-- Name: team_member_key_envelopes team_member_key_envelopes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_member_key_envelopes
    ADD CONSTRAINT team_member_key_envelopes_pkey PRIMARY KEY (id);


--
-- Name: team_member_key_envelopes team_member_key_envelopes_team_key_id_user_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_member_key_envelopes
    ADD CONSTRAINT team_member_key_envelopes_team_key_id_user_id_key UNIQUE (team_key_id, user_id);


--
-- Name: team_members team_members_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_members
    ADD CONSTRAINT team_members_pkey PRIMARY KEY (id);


--
-- Name: team_members team_members_team_id_user_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_members
    ADD CONSTRAINT team_members_team_id_user_id_key UNIQUE (team_id, user_id);


--
-- Name: teams teams_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.teams
    ADD CONSTRAINT teams_pkey PRIMARY KEY (id);


--
-- Name: teams_watched_resources teams_watched_resources_account_id_kind_team_id_channel_id__key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.teams_watched_resources
    ADD CONSTRAINT teams_watched_resources_account_id_kind_team_id_channel_id__key UNIQUE (account_id, kind, team_id, channel_id, chat_id);


--
-- Name: teams_watched_resources teams_watched_resources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.teams_watched_resources
    ADD CONSTRAINT teams_watched_resources_pkey PRIMARY KEY (id);


--
-- Name: transcript_corrections transcript_corrections_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.transcript_corrections
    ADD CONSTRAINT transcript_corrections_pkey PRIMARY KEY (correction_id);


--
-- Name: atoms_embedding_hnsw_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX atoms_embedding_hnsw_idx ON public.atoms USING hnsw (embedding public.vector_cosine_ops) WITH (m='16', ef_construction='64') WHERE (embedding IS NOT NULL);


--
-- Name: idx_activity_events_account_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_activity_events_account_created ON public.activity_events USING btree (account_id, created_at DESC);


--
-- Name: idx_activity_events_account_type_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_activity_events_account_type_created ON public.activity_events USING btree (account_id, event_type, created_at DESC);


--
-- Name: idx_activity_events_graph_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_activity_events_graph_created ON public.activity_events USING btree (graph_id, created_at DESC) WHERE (graph_id IS NOT NULL);


--
-- Name: idx_atoms_content_tsv; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_atoms_content_tsv ON public.atoms USING gin (content_tsv);


--
-- Name: idx_atoms_embed_failed_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_atoms_embed_failed_at ON public.atoms USING btree (embed_failed_at) WHERE (embed_failed_at IS NOT NULL);


--
-- Name: idx_atoms_fts_language; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_atoms_fts_language ON public.atoms USING btree (fts_language) WHERE (fts_language <> 'english'::text);


--
-- Name: idx_atoms_graph_author; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_atoms_graph_author ON public.atoms USING btree (graph_id, author_id);


--
-- Name: idx_atoms_graph_channel; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_atoms_graph_channel ON public.atoms USING btree (graph_id, channel_id);


--
-- Name: idx_atoms_graph_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_atoms_graph_created ON public.atoms USING btree (graph_id, created_at DESC);


--
-- Name: idx_atoms_graph_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_atoms_graph_source ON public.atoms USING btree (graph_id, source_type);


--
-- Name: idx_atoms_graph_source_id_pattern; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_atoms_graph_source_id_pattern ON public.atoms USING btree (graph_id, source_id text_pattern_ops);


--
-- Name: idx_atoms_graph_timestamp; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_atoms_graph_timestamp ON public.atoms USING btree (graph_id, "timestamp" DESC);


--
-- Name: idx_atoms_graph_workspace; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_atoms_graph_workspace ON public.atoms USING btree (graph_id, workspace_id);


--
-- Name: idx_coding_session_shares_graph_session; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_coding_session_shares_graph_session ON public.coding_session_shares USING btree (graph_id, session_id);


--
-- Name: idx_coding_session_shares_token_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_coding_session_shares_token_active ON public.coding_session_shares USING btree (token) WHERE (revoked_at IS NULL);


--
-- Name: idx_coding_sessions_graph_last_activity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_coding_sessions_graph_last_activity ON public.coding_sessions USING btree (graph_id, last_activity_at DESC);


--
-- Name: idx_coding_sessions_graph_project; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_coding_sessions_graph_project ON public.coding_sessions USING btree (graph_id, project_key);


--
-- Name: idx_conversation_messages_thread; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_conversation_messages_thread ON public.conversation_messages USING btree (conversation_id, seq);


--
-- Name: idx_conversation_oceans_graph; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_conversation_oceans_graph ON public.conversation_oceans USING btree (graph_id);


--
-- Name: idx_conversations_owner; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_conversations_owner ON public.conversations USING btree (graph_id, user_id, updated_at DESC);


--
-- Name: idx_conversations_shared; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_conversations_shared ON public.conversations USING btree (graph_id, updated_at DESC) WHERE shared;


--
-- Name: idx_graph_subscriptions_expiry; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_graph_subscriptions_expiry ON public.graph_subscriptions USING btree (expires_at);


--
-- Name: idx_huddle_captures_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_huddle_captures_account ON public.slack_huddle_captures USING btree (account_id);


--
-- Name: idx_huddle_captures_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_huddle_captures_pending ON public.slack_huddle_captures USING btree (created_at) WHERE (status = 'pending'::text);


--
-- Name: idx_jobs_idem_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_jobs_idem_pending ON public.jobs USING btree (idempotency_key) WHERE (status = 'pending'::public.job_status);


--
-- Name: idx_jobs_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_jobs_pending ON public.jobs USING btree (run_after, id) WHERE (status = 'pending'::public.job_status);


--
-- Name: idx_llm_usage_account_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_llm_usage_account_created ON public.llm_usage USING btree (account_id, created_at);


--
-- Name: idx_mcp_tool_calls_account_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mcp_tool_calls_account_created ON public.mcp_tool_calls USING btree (account_id, created_at DESC);


--
-- Name: idx_mcp_tool_calls_user_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mcp_tool_calls_user_created ON public.mcp_tool_calls USING btree (user_id, created_at DESC);


--
-- Name: idx_meeting_shares_graph_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_meeting_shares_graph_source ON public.meeting_shares USING btree (graph_id, source_id);


--
-- Name: idx_meeting_shares_token_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_meeting_shares_token_active ON public.meeting_shares USING btree (token) WHERE (revoked_at IS NULL);


--
-- Name: idx_meeting_transcripts_graph_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_meeting_transcripts_graph_created ON public.meeting_transcripts USING btree (graph_id, created_at DESC);


--
-- Name: idx_node_atoms_atom_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_node_atoms_atom_id ON public.node_atoms USING btree (atom_id);


--
-- Name: idx_node_atoms_node_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_node_atoms_node_id ON public.node_atoms USING btree (node_id);


--
-- Name: idx_node_merges_graph_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_node_merges_graph_active ON public.node_merges USING btree (graph_id, created_at DESC) WHERE (undone_at IS NULL);


--
-- Name: idx_node_promotions_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_node_promotions_source ON public.node_promotions USING btree (source_node_id);


--
-- Name: idx_node_promotions_target; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_node_promotions_target ON public.node_promotions USING btree (target_graph_id);


--
-- Name: idx_nodes_aliases_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_nodes_aliases_gin ON public.nodes USING gin (((body -> 'aliases'::text)));


--
-- Name: idx_nodes_entity_title_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_nodes_entity_title_trgm ON public.nodes USING gin (title public.gin_trgm_ops) WHERE (type = 'entity'::text);


--
-- Name: idx_nodes_graph_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_nodes_graph_type ON public.nodes USING btree (graph_id, type);


--
-- Name: idx_nodes_graph_type_lower_title; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_nodes_graph_type_lower_title ON public.nodes USING btree (graph_id, type, lower(title));


--
-- Name: idx_nodes_graph_updated_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_nodes_graph_updated_at ON public.nodes USING btree (graph_id, updated_at DESC);


--
-- Name: idx_oauth_tokens_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_oauth_tokens_account ON public.oauth_tokens USING btree (account_id, provider);


--
-- Name: idx_op_log_entity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_op_log_entity ON public.op_log USING btree (entity, entity_id, seq DESC);


--
-- Name: idx_op_log_seq_device; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_op_log_seq_device ON public.op_log USING btree (seq, device_id);


--
-- Name: idx_op_log_tombstones; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_op_log_tombstones ON public.op_log USING btree (op_type, updated_at) WHERE (op_type = 'delete'::text);


--
-- Name: idx_outlook_workflows_account_enabled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_outlook_workflows_account_enabled ON public.outlook_workflows USING btree (account_id) WHERE enabled;


--
-- Name: idx_outlook_workflows_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_outlook_workflows_user ON public.outlook_workflows USING btree (account_id, user_id);


--
-- Name: idx_queries_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_queries_account ON public.queries USING btree (account_id, created_at DESC);


--
-- Name: idx_query_history_account_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_query_history_account_created ON public.query_history USING btree (account_id, created_at DESC);


--
-- Name: idx_query_history_graph; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_query_history_graph ON public.query_history USING btree (graph_id);


--
-- Name: idx_relationship_edge_dst; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_relationship_edge_dst ON public.relationship_edge USING btree (dst_node_id);


--
-- Name: idx_relationship_edge_graph_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_relationship_edge_graph_status ON public.relationship_edge USING btree (graph_id, status);


--
-- Name: idx_relationship_edge_src; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_relationship_edge_src ON public.relationship_edge USING btree (src_node_id);


--
-- Name: idx_slack_integrations_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_slack_integrations_account ON public.slack_integrations USING btree (account_id);


--
-- Name: idx_supersession_edge_superseded_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_supersession_edge_superseded_id ON public.supersession_edge USING btree (superseded_id);


--
-- Name: idx_supersession_edge_superseder_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_supersession_edge_superseder_id ON public.supersession_edge USING btree (superseder_id);


--
-- Name: idx_team_members_team_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_team_members_team_active ON public.team_members USING btree (team_id, departing_at) WHERE (departing_at IS NULL);


--
-- Name: idx_teams_watched_channel; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_teams_watched_channel ON public.teams_watched_resources USING btree (account_id, channel_id);


--
-- Name: idx_teams_watched_chat; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_teams_watched_chat ON public.teams_watched_resources USING btree (account_id, chat_id);


--
-- Name: idx_transcript_corrections_graph_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_transcript_corrections_graph_source ON public.transcript_corrections USING btree (graph_id, source_id);


--
-- Name: idx_watched_conversations_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_watched_conversations_account ON public.outlook_watched_conversations USING btree (account_id);


--
-- Name: uq_graphs_one_org_per_account; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_graphs_one_org_per_account ON public.graphs USING btree (account_id) WHERE (kind = 'org'::text);


--
-- Name: uq_graphs_one_primary_per_account; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_graphs_one_primary_per_account ON public.graphs USING btree (account_id) WHERE is_primary;


--
-- Name: activity_events activity_events_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.activity_events
    ADD CONSTRAINT activity_events_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: atoms atoms_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.atoms
    ADD CONSTRAINT atoms_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: coding_session_shares coding_session_shares_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.coding_session_shares
    ADD CONSTRAINT coding_session_shares_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: coding_session_shares coding_session_shares_graph_id_session_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.coding_session_shares
    ADD CONSTRAINT coding_session_shares_graph_id_session_id_fkey FOREIGN KEY (graph_id, session_id) REFERENCES public.coding_sessions(graph_id, session_id) ON DELETE CASCADE;


--
-- Name: coding_sessions coding_sessions_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.coding_sessions
    ADD CONSTRAINT coding_sessions_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: conversation_messages conversation_messages_conversation_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversation_messages
    ADD CONSTRAINT conversation_messages_conversation_id_fkey FOREIGN KEY (conversation_id) REFERENCES public.conversations(id) ON DELETE CASCADE;


--
-- Name: conversation_oceans conversation_oceans_conversation_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversation_oceans
    ADD CONSTRAINT conversation_oceans_conversation_id_fkey FOREIGN KEY (conversation_id) REFERENCES public.conversations(id) ON DELETE CASCADE;


--
-- Name: conversation_oceans conversation_oceans_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversation_oceans
    ADD CONSTRAINT conversation_oceans_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: conversation_surfaced_edges conversation_surfaced_edges_conversation_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversation_surfaced_edges
    ADD CONSTRAINT conversation_surfaced_edges_conversation_id_fkey FOREIGN KEY (conversation_id) REFERENCES public.conversations(id) ON DELETE CASCADE;


--
-- Name: conversations conversations_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conversations
    ADD CONSTRAINT conversations_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: graphs graphs_self_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.graphs
    ADD CONSTRAINT graphs_self_node_id_fkey FOREIGN KEY (self_node_id) REFERENCES public.nodes(node_id) ON DELETE SET NULL;


--
-- Name: meeting_shares meeting_shares_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.meeting_shares
    ADD CONSTRAINT meeting_shares_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: node_atoms node_atoms_atom_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.node_atoms
    ADD CONSTRAINT node_atoms_atom_id_fkey FOREIGN KEY (atom_id) REFERENCES public.atoms(atom_id) ON DELETE CASCADE;


--
-- Name: node_atoms node_atoms_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.node_atoms
    ADD CONSTRAINT node_atoms_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.nodes(node_id) ON DELETE CASCADE;


--
-- Name: node_merges node_merges_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.node_merges
    ADD CONSTRAINT node_merges_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: node_promotions node_promotions_source_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.node_promotions
    ADD CONSTRAINT node_promotions_source_node_id_fkey FOREIGN KEY (source_node_id) REFERENCES public.nodes(node_id) ON DELETE CASCADE;


--
-- Name: node_promotions node_promotions_target_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.node_promotions
    ADD CONSTRAINT node_promotions_target_graph_id_fkey FOREIGN KEY (target_graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: node_promotions node_promotions_target_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.node_promotions
    ADD CONSTRAINT node_promotions_target_node_id_fkey FOREIGN KEY (target_node_id) REFERENCES public.nodes(node_id) ON DELETE CASCADE;


--
-- Name: nodes nodes_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT nodes_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: non_supersession_pair non_supersession_pair_node_a_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.non_supersession_pair
    ADD CONSTRAINT non_supersession_pair_node_a_id_fkey FOREIGN KEY (node_a_id) REFERENCES public.nodes(node_id) ON DELETE CASCADE;


--
-- Name: non_supersession_pair non_supersession_pair_node_b_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.non_supersession_pair
    ADD CONSTRAINT non_supersession_pair_node_b_id_fkey FOREIGN KEY (node_b_id) REFERENCES public.nodes(node_id) ON DELETE CASCADE;


--
-- Name: outlook_watched_conversations outlook_watched_conversations_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outlook_watched_conversations
    ADD CONSTRAINT outlook_watched_conversations_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: outlook_workflows outlook_workflows_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.outlook_workflows
    ADD CONSTRAINT outlook_workflows_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: projects projects_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: query_history query_history_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.query_history
    ADD CONSTRAINT query_history_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: relationship_edge relationship_edge_dst_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.relationship_edge
    ADD CONSTRAINT relationship_edge_dst_node_id_fkey FOREIGN KEY (dst_node_id) REFERENCES public.nodes(node_id) ON DELETE CASCADE;


--
-- Name: relationship_edge relationship_edge_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.relationship_edge
    ADD CONSTRAINT relationship_edge_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: relationship_edge relationship_edge_src_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.relationship_edge
    ADD CONSTRAINT relationship_edge_src_node_id_fkey FOREIGN KEY (src_node_id) REFERENCES public.nodes(node_id) ON DELETE CASCADE;


--
-- Name: supersession_edge supersession_edge_superseded_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.supersession_edge
    ADD CONSTRAINT supersession_edge_superseded_id_fkey FOREIGN KEY (superseded_id) REFERENCES public.nodes(node_id) ON DELETE CASCADE;


--
-- Name: supersession_edge supersession_edge_superseder_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.supersession_edge
    ADD CONSTRAINT supersession_edge_superseder_id_fkey FOREIGN KEY (superseder_id) REFERENCES public.nodes(node_id) ON DELETE CASCADE;


--
-- Name: team_keys team_keys_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_keys
    ADD CONSTRAINT team_keys_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: team_member_key_envelopes team_member_key_envelopes_team_key_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_member_key_envelopes
    ADD CONSTRAINT team_member_key_envelopes_team_key_id_fkey FOREIGN KEY (team_key_id) REFERENCES public.team_keys(id) ON DELETE CASCADE;


--
-- Name: team_members team_members_team_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.team_members
    ADD CONSTRAINT team_members_team_id_fkey FOREIGN KEY (team_id) REFERENCES public.teams(id) ON DELETE CASCADE;


--
-- Name: teams teams_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.teams
    ADD CONSTRAINT teams_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- Name: teams_watched_resources teams_watched_resources_graph_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.teams_watched_resources
    ADD CONSTRAINT teams_watched_resources_graph_id_fkey FOREIGN KEY (graph_id) REFERENCES public.graphs(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--


