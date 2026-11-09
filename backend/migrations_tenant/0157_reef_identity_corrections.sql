REVOKE ALL ON public.source_purge_events,public.source_purge_dependencies FROM PUBLIC,coralswarm_tenant_app;
GRANT SELECT ON public.source_purge_events,public.source_purge_dependencies TO coralswarm_tenant_app;

CREATE TABLE public.reef_identity_corrections (
 correction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 graph_id UUID NOT NULL,
 node_id UUID NOT NULL,
 label TEXT CHECK(label IS NULL OR (length(label) BETWEEN 1 AND 512 AND btrim(label)<>'')),
 recorded_by UUID,
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 state TEXT NOT NULL DEFAULT 'active' CHECK(state IN('active','superseded','invalidated')),
 support_atom_id UUID NOT NULL,
 support_kind TEXT NOT NULL CHECK(support_kind IN('legacy','bound')),
 source_version_id UUID,
 source_item_id UUID,
 content_revision BIGINT NOT NULL CHECK(content_revision>0),
 input_sha256 TEXT NOT NULL CHECK(input_sha256 ~ '^[0-9a-f]{64}$'),
 CHECK((label IS NULL)=(recorded_by IS NULL)),
 CHECK(state<>'active' OR label IS NOT NULL),
 CHECK((support_kind='legacy' AND source_version_id IS NULL AND source_item_id IS NULL)
    OR (support_kind='bound' AND source_version_id IS NOT NULL AND source_item_id IS NOT NULL)),
 FOREIGN KEY(node_id,graph_id) REFERENCES public.nodes(node_id,graph_id) ON DELETE CASCADE,
 FOREIGN KEY(support_atom_id,graph_id) REFERENCES public.atoms(atom_id,graph_id) ON DELETE CASCADE,
 FOREIGN KEY(node_id,support_atom_id) REFERENCES public.node_atoms(node_id,atom_id) ON DELETE CASCADE,
 FOREIGN KEY(source_version_id,source_item_id,graph_id) REFERENCES public.source_versions(version_id,source_item_id,graph_id) ON DELETE CASCADE,
 FOREIGN KEY(source_version_id,support_atom_id) REFERENCES public.source_version_chunks(source_version_id,atom_id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX reef_identity_one_active ON public.reef_identity_corrections(graph_id,node_id) WHERE state='active';
CREATE INDEX reef_identity_support ON public.reef_identity_corrections(graph_id,support_atom_id,node_id);
CREATE INDEX reef_identity_source ON public.reef_identity_corrections(graph_id,source_item_id,source_version_id) WHERE source_version_id IS NOT NULL;

CREATE FUNCTION public.reef_identity_atom_fingerprint(a public.atoms) RETURNS TEXT
 LANGUAGE SQL STABLE SET search_path=pg_catalog,public,pg_temp SET TimeZone='UTC' AS $$
 SELECT encode(sha256(convert_to(jsonb_build_array('reef_identity_atom_v1',
  a.graph_id,a.atom_id,a.content_inline,a.content_blob_key,
  a.timestamp,a.created_at,
  a.participants,a.source_type,a.source_id,a.header_len,a.scope,a.ingested_by,
  a.source_evidence_bound,a.derived_copy_bound,a.evidence_retracted,
  a.promoted_from_atom_id,a.promoted_from_revision,a.content_revision,
  a.title,a.title_kind)::text,'UTF8')),'hex')
$$;
CREATE FUNCTION public.reef_identity_support_valid(c public.reef_identity_corrections) RETURNS BOOLEAN LANGUAGE SQL STABLE SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT c.label IS NOT NULL AND EXISTS(
  SELECT 1 FROM public.atoms a JOIN public.node_atoms l ON l.atom_id=a.atom_id AND l.node_id=c.node_id
   JOIN public.nodes n ON n.node_id=l.node_id AND n.graph_id=a.graph_id
  WHERE a.graph_id=c.graph_id AND a.atom_id=c.support_atom_id
   AND a.content_revision=c.content_revision AND public.reef_identity_atom_fingerprint(a)=c.input_sha256
   AND public.library_atom_permitted(c.graph_id,a.atom_id)
   AND ((c.support_kind='legacy' AND NOT a.source_evidence_bound
     AND NOT EXISTS(SELECT 1 FROM public.source_version_chunks m WHERE m.graph_id=a.graph_id AND m.atom_id=a.atom_id))
    OR (c.support_kind='bound' AND a.source_evidence_bound AND EXISTS(
     SELECT 1 FROM public.source_version_chunks m
      JOIN public.source_versions v ON v.version_id=m.source_version_id AND v.graph_id=m.graph_id
      JOIN public.source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
     WHERE m.graph_id=c.graph_id AND m.atom_id=a.atom_id AND m.source_version_id=c.source_version_id
      AND s.source_item_id=c.source_item_id AND s.current_version_id=v.version_id AND s.access_state='permitted'))))
$$;
CREATE FUNCTION public.reef_identity_record_integrity() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_RELID<>'public.reef_identity_corrections'::regclass OR TG_LEVEL<>'ROW' OR TG_WHEN<>'BEFORE' OR TG_OP NOT IN('INSERT','UPDATE') THEN
  RAISE EXCEPTION 'invalid reef identity trigger context' USING ERRCODE='42501';
 END IF;
 IF TG_OP='UPDATE' THEN
  IF (to_jsonb(NEW)-ARRAY['state','label','recorded_by']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','label','recorded_by'])
   OR (OLD.state<>'active' AND NEW.state IS DISTINCT FROM OLD.state)
   OR (NEW.label IS DISTINCT FROM OLD.label AND NEW.label IS NOT NULL)
   OR (NEW.recorded_by IS DISTINCT FROM OLD.recorded_by AND NEW.recorded_by IS NOT NULL)
   OR (OLD.label IS NULL AND NEW.label IS NOT NULL)
   OR (NEW.state='active' AND (NEW.label IS NULL OR NEW.recorded_by IS NULL)) THEN
   RAISE EXCEPTION 'reef identity event is immutable; record a new supported interpretation' USING ERRCODE='23514';
  END IF;
  RETURN NEW;
 END IF;
 NEW.recorded_at=clock_timestamp();
 IF NULLIF(current_setting('app.user_id',true),'') IS NULL
  OR NEW.recorded_by IS DISTINCT FROM NULLIF(current_setting('app.user_id',true),'')::uuid
  OR COALESCE(current_setting('app.tenant_actor',true),'')='system'
  OR NEW.state<>'active' OR NOT public.reef_identity_support_valid(NEW)
  OR NOT EXISTS(SELECT 1 FROM public.nodes n JOIN public.graphs g ON g.id=n.graph_id
   WHERE n.node_id=NEW.node_id AND n.graph_id=NEW.graph_id AND n.title=NEW.label
    AND g.account_id=NULLIF(current_setting('app.account_id',true),'')::uuid) THEN
  RAISE EXCEPTION 'reef identity requires authenticated actor, owned graph and exact current support' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER reef_identity_record_integrity BEFORE INSERT OR UPDATE ON public.reef_identity_corrections
 FOR EACH ROW EXECUTE FUNCTION public.reef_identity_record_integrity();

ALTER TABLE public.reef_identity_corrections ENABLE ROW LEVEL SECURITY;
CREATE POLICY reef_identity_read ON public.reef_identity_corrections FOR SELECT USING(
 graph_id IN(SELECT public.tenant_rls_visible_graph_ids())
 AND EXISTS(SELECT 1 FROM public.graphs g WHERE g.id=reef_identity_corrections.graph_id AND g.account_id=NULLIF(current_setting('app.account_id',true),'')::uuid)
 AND public.reef_identity_support_valid(reef_identity_corrections));
CREATE POLICY reef_identity_insert ON public.reef_identity_corrections FOR INSERT WITH CHECK(
 graph_id IN(SELECT public.tenant_rls_visible_graph_ids())
 AND EXISTS(SELECT 1 FROM public.graphs g WHERE g.id=reef_identity_corrections.graph_id AND g.account_id=NULLIF(current_setting('app.account_id',true),'')::uuid)
 AND recorded_by=NULLIF(current_setting('app.user_id',true),'')::uuid
 AND COALESCE(current_setting('app.tenant_actor',true),'')<>'system'
 AND public.reef_identity_support_valid(reef_identity_corrections));
CREATE POLICY reef_identity_retire ON public.reef_identity_corrections FOR UPDATE USING(
 graph_id IN(SELECT public.tenant_rls_visible_graph_ids())
 AND EXISTS(SELECT 1 FROM public.graphs g WHERE g.id=reef_identity_corrections.graph_id AND g.account_id=NULLIF(current_setting('app.account_id',true),'')::uuid)
 AND NULLIF(current_setting('app.user_id',true),'') IS NOT NULL
 AND COALESCE(current_setting('app.tenant_actor',true),'')<>'system') WITH CHECK(
 graph_id IN(SELECT public.tenant_rls_visible_graph_ids())
 AND EXISTS(SELECT 1 FROM public.graphs g WHERE g.id=reef_identity_corrections.graph_id AND g.account_id=NULLIF(current_setting('app.account_id',true),'')::uuid)
 AND state<>'active');

ALTER FUNCTION public.library_node_title(UUID,UUID) RENAME TO library_node_title_before_reef_identity;
ALTER FUNCTION public.library_node_title_before_reef_identity(UUID,UUID) SET search_path=pg_catalog,public,pg_temp;
ALTER FUNCTION public.library_atom_permitted(UUID,UUID) SET search_path=pg_catalog,public,pg_temp;
ALTER FUNCTION public.library_permitted_atoms(UUID,UUID[]) SET search_path=pg_catalog,public,pg_temp;
CREATE FUNCTION public.library_node_title(g UUID,n_id UUID) RETURNS TEXT LANGUAGE SQL STABLE SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT COALESCE(public.library_node_title_before_reef_identity(g,n_id),(
  SELECT substring(n.title FOR 256) FROM public.nodes n WHERE n.graph_id=g AND n.node_id=n_id
   AND EXISTS(SELECT 1 FROM public.reef_identity_corrections c WHERE c.graph_id=g AND c.node_id=n_id
    AND c.state='active' AND c.label=n.title AND public.reef_identity_support_valid(c))))
$$;

CREATE FUNCTION public.reef_identity_text_vectors(body TEXT) RETURNS SETOF tsvector
 LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF body IS NULL OR body='' THEN RETURN; END IF;
 IF octet_length(body)<=32768 THEN
  RETURN NEXT to_tsvector('english'::regconfig,body);
  RETURN;
 END IF;
 RETURN QUERY
  WITH recognized AS (
   SELECT l.lexeme,row_number() OVER(ORDER BY d.ordinality,l.ordinality) AS position
   FROM ts_debug('english'::regconfig,body) WITH ORDINALITY d
    CROSS JOIN LATERAL unnest(d.lexemes) WITH ORDINALITY l(lexeme,ordinality)
   WHERE octet_length(d.token)<=2047 AND l.lexeme IS NOT NULL AND octet_length(l.lexeme) BETWEEN 1 AND 2047
  )
  SELECT string_agg(array_to_tsvector(ARRAY[lexeme])::text||':'||(((position-1)%128)+1)::text,' ' ORDER BY position)::tsvector
  FROM recognized GROUP BY (position-1)/128 ORDER BY (position-1)/128;
END $$;

CREATE FUNCTION public.reef_identity_independent_title(g UUID,n_id UUID,t TEXT,excluded_atoms UUID[],excluded_versions UUID[])
 RETURNS BOOLEAN LANGUAGE SQL STABLE SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT EXISTS(SELECT 1 FROM public.node_atoms l JOIN public.atoms a ON a.atom_id=l.atom_id AND a.graph_id=g
  WHERE l.node_id=n_id AND NOT a.atom_id=ANY(excluded_atoms) AND public.library_atom_permitted(g,a.atom_id)
   AND t<>'' AND (position(lower(t) IN lower(COALESCE(a.content_inline,'')))>0
    OR EXISTS(SELECT 1 FROM unnest(a.participants) p WHERE lower(p)=lower(t))))
 OR EXISTS(SELECT 1 FROM public.node_semantic_atoms l
  JOIN public.semantic_atoms a ON a.semantic_atom_id=l.semantic_atom_id AND a.graph_id=l.graph_id
  JOIN public.source_versions v ON v.version_id=a.source_version_id AND v.graph_id=a.graph_id
  JOIN public.source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id
  CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(a.entities)='array' THEN a.entities ELSE '[]'::jsonb END) e
  WHERE l.graph_id=g AND l.node_id=n_id AND NOT v.version_id=ANY(excluded_versions)
   AND s.access_state='permitted' AND lower(e->>'mention')=lower(t))
 OR EXISTS(SELECT 1 FROM public.reef_identity_corrections c WHERE c.graph_id=g AND c.node_id=n_id
  AND c.state='active' AND c.label=t AND NOT c.support_atom_id=ANY(excluded_atoms)
  AND (c.source_version_id IS NULL OR NOT c.source_version_id=ANY(excluded_versions))
  AND public.reef_identity_support_valid(c))
$$;

-- Scan literal occurrences rather than interpolate labels into a regex.
-- Only the fixed outside-character class is a regex: punctuation, backslashes
-- and Unicode in a human label cannot acquire pattern syntax. Both boundaries
-- must be outside a name token, so Ann does not match annual planning.
CREATE FUNCTION public.reef_identity_contains_label(value TEXT,unsupported_labels TEXT[]) RETURNS BOOLEAN
 LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE folded TEXT:=lower(value); label TEXT; start_at INTEGER; found_at INTEGER; end_at INTEGER;
BEGIN
 IF value IS NULL THEN RETURN false; END IF;
 FOR label IN SELECT lower(l) FROM unnest(unsupported_labels) l WHERE l IS NOT NULL AND l<>'' LOOP
  start_at:=1;
  LOOP
   found_at:=strpos(substring(folded FROM start_at),label);
   EXIT WHEN found_at=0;
   found_at:=found_at+start_at-1;
   end_at:=found_at+length(label);
   IF (found_at=1 OR substring(folded FROM found_at-1 FOR 1)!~'[[:alnum:]_]')
    AND (end_at>length(folded) OR substring(folded FROM end_at FOR 1)!~'[[:alnum:]_]') THEN
    RETURN true;
   END IF;
   start_at:=found_at+1;
  END LOOP;
 END LOOP;
 RETURN false;
END $$;
CREATE FUNCTION public.reef_identity_sanitize_json(value JSONB,unsupported_labels TEXT[]) RETURNS JSONB
 LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE result JSONB; member RECORD; sanitized JSONB; decoded JSONB;
BEGIN
 IF value IS NULL THEN RETURN NULL; END IF;
 CASE jsonb_typeof(value)
 WHEN 'string' THEN
  -- Decode structured string values before literal matching, so a mixed
  -- serialized object retains its unrelated fields rather than losing all B.
  BEGIN
   decoded:=(value#>>'{}')::jsonb;
  EXCEPTION WHEN data_exception THEN
   IF public.reef_identity_contains_label(value#>>'{}',unsupported_labels) THEN RETURN NULL; END IF;
   RETURN value;
  END;
  -- A textual alias such as "2026" or "true" remains textual identity:
  -- decoding it as a primitive must not bypass matching the original string.
  -- Actual typed numeric/boolean/null metadata is preserved by the ELSE branch.
  IF jsonb_typeof(decoded) NOT IN('object','array','string') THEN
   IF public.reef_identity_contains_label(value#>>'{}',unsupported_labels) THEN RETURN NULL; END IF;
   RETURN value;
  END IF;
  sanitized:=public.reef_identity_sanitize_json(decoded,unsupported_labels);
  IF sanitized IS NULL THEN RETURN NULL; END IF;
  IF sanitized=decoded THEN RETURN value; END IF;
  RETURN to_jsonb(sanitized::text);
 WHEN 'object' THEN
  result:='{}'::jsonb;
  FOR member IN SELECT key,element FROM jsonb_each(value) e(key,element) LOOP
   IF NOT public.reef_identity_contains_label(member.key,unsupported_labels) THEN
    sanitized:=public.reef_identity_sanitize_json(member.element,unsupported_labels);
    IF sanitized IS NOT NULL THEN result:=result||jsonb_build_object(member.key,sanitized); END IF;
   END IF;
  END LOOP;
  RETURN result;
 WHEN 'array' THEN
  result:='[]'::jsonb;
  FOR member IN SELECT element FROM jsonb_array_elements(value) WITH ORDINALITY e(element,position) ORDER BY position LOOP
   sanitized:=public.reef_identity_sanitize_json(member.element,unsupported_labels);
   IF sanitized IS NOT NULL THEN result:=result||jsonb_build_array(sanitized); END IF;
  END LOOP;
  RETURN result;
 ELSE RETURN value;
 END CASE;
END $$;
CREATE FUNCTION public.reef_identity_sanitize_body(body JSONB,unsupported_labels TEXT[]) RETURNS JSONB
 LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE decoded JSONB; sanitized JSONB;
BEGIN
 IF body IS NULL OR COALESCE(cardinality(unsupported_labels),0)=0 THEN RETURN body; END IF;
 IF jsonb_typeof(body)='string' THEN
  decoded:=(body#>>'{}')::jsonb;
  sanitized:=COALESCE(public.reef_identity_sanitize_json(decoded,unsupported_labels),'{}'::jsonb);
  IF sanitized=decoded THEN RETURN body; END IF;
  RETURN to_jsonb(sanitized::text);
 END IF;
 RETURN COALESCE(public.reef_identity_sanitize_json(body,unsupported_labels),'{}'::jsonb);
EXCEPTION WHEN data_exception OR program_limit_exceeded THEN
 IF jsonb_typeof(body)='string' THEN RETURN to_jsonb('{}'::text); END IF;
 RETURN '{}'::jsonb;
END $$;

CREATE FUNCTION public.reef_identity_erase_support(g UUID,selected_atoms UUID[],selected_versions UUID[],only_node UUID[],excluded_atoms UUID[],excluded_versions UUID[],skip_node_update BOOLEAN DEFAULT false)
 RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE affected RECORD; placeholder TEXT; changed_node public.nodes%ROWTYPE; node_changed BOOLEAN; unsupported_labels TEXT[];
BEGIN
 skip_node_update:=skip_node_update OR EXISTS(SELECT 1 FROM public.source_purge_events e
  WHERE e.graph_id=g AND e.event_kind='graph_deleted' AND e.sql_processed_at IS NULL);
 FOR affected IN SELECT c.node_id,array_agg(DISTINCT c.label) labels FROM public.reef_identity_corrections c
  WHERE c.graph_id=g AND c.label IS NOT NULL
   AND (selected_atoms IS NULL OR c.support_atom_id=ANY(selected_atoms))
   AND (selected_versions IS NULL OR c.source_version_id=ANY(selected_versions))
   AND (only_node IS NULL OR c.node_id=ANY(only_node)) GROUP BY c.node_id
 LOOP
  UPDATE public.reef_identity_corrections c SET state=CASE WHEN c.state='active' THEN 'invalidated' ELSE c.state END,label=NULL,recorded_by=NULL
   WHERE c.graph_id=g AND c.node_id=affected.node_id
    AND (selected_atoms IS NULL OR c.support_atom_id=ANY(selected_atoms))
    AND (selected_versions IS NULL OR c.source_version_id=ANY(selected_versions));
  SELECT COALESCE(array_agg(label),'{}'::text[]) INTO unsupported_labels FROM unnest(affected.labels) l(label)
   WHERE NOT public.reef_identity_independent_title(g,affected.node_id,label,excluded_atoms,excluded_versions);
  placeholder:='[Evidence unavailable] '||affected.node_id::text;
  node_changed:=false;
  IF NOT skip_node_update THEN
   UPDATE public.nodes n SET
    title=CASE WHEN n.title=ANY(unsupported_labels) THEN placeholder ELSE n.title END,
    body=public.reef_identity_sanitize_body(n.body,unsupported_labels),
    digest_coral_count=CASE WHEN public.reef_identity_sanitize_body(n.body,unsupported_labels)->'digest' IS DISTINCT FROM n.body->'digest' THEN 0 ELSE n.digest_coral_count END,
    version=n.version+1,updated_at=now()
   WHERE n.graph_id=g AND n.node_id=affected.node_id
    AND (n.title=ANY(unsupported_labels) OR public.reef_identity_sanitize_body(n.body,unsupported_labels) IS DISTINCT FROM n.body)
   RETURNING n.* INTO changed_node;
   node_changed:=FOUND;
  END IF;
  UPDATE public.op_log o SET payload=
   CASE WHEN o.payload ? 'body' THEN jsonb_set(
    CASE WHEN o.payload->>'title'=ANY(unsupported_labels) THEN jsonb_set(o.payload,'{title}',to_jsonb(placeholder)) ELSE o.payload END,
    '{body}',public.reef_identity_sanitize_body(o.payload->'body',unsupported_labels))
   ELSE CASE WHEN o.payload->>'title'=ANY(unsupported_labels) THEN jsonb_set(o.payload,'{title}',to_jsonb(placeholder)) ELSE o.payload END END
   WHERE o.graph_id=g AND o.entity='node' AND o.entity_id=affected.node_id::text;
  UPDATE public.node_merges m SET dup_snapshot=
   CASE WHEN m.dup_snapshot ? 'body' THEN jsonb_set(
    CASE WHEN m.dup_snapshot->>'title'=ANY(unsupported_labels) THEN jsonb_set(m.dup_snapshot,'{title}',to_jsonb(placeholder)) ELSE m.dup_snapshot END,
    '{body}',public.reef_identity_sanitize_body(m.dup_snapshot->'body',unsupported_labels))
   ELSE CASE WHEN m.dup_snapshot->>'title'=ANY(unsupported_labels) THEN jsonb_set(m.dup_snapshot,'{title}',to_jsonb(placeholder)) ELSE m.dup_snapshot END END
   WHERE m.graph_id=g AND m.dup_id=affected.node_id;
  IF node_changed THEN
   INSERT INTO public.op_log(op_id,entity,entity_id,op_type,payload,version,updated_at,device_id,graph_id)
   VALUES(gen_random_uuid(),'node',changed_node.node_id::text,'upsert',
    jsonb_build_object('node_id',changed_node.node_id,'graph_id',changed_node.graph_id,
     'type',changed_node.type,'title',changed_node.title,'body',changed_node.body::text,
     'scope',changed_node.scope,'owner_id',changed_node.owner_id,
     'created_at',to_char(changed_node.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'),
     'updated_at',to_char(changed_node.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"'),
     'version',changed_node.version),changed_node.version,changed_node.updated_at,'cloud',changed_node.graph_id);
  END IF;
 END LOOP;
END $$;

CREATE FUNCTION public.reef_identity_node_changed() RETURNS TRIGGER
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_RELID<>'public.nodes'::regclass OR TG_LEVEL<>'ROW' OR TG_WHEN<>'BEFORE' OR TG_OP NOT IN('UPDATE','DELETE') THEN
  RAISE EXCEPTION 'invalid reef identity trigger context' USING ERRCODE='42501';
 END IF;
 IF TG_OP='DELETE' THEN
  PERFORM public.reef_identity_erase_support(OLD.graph_id,NULL,NULL,ARRAY[OLD.node_id],
   ARRAY(SELECT l.atom_id FROM public.node_atoms l WHERE l.node_id=OLD.node_id),
   ARRAY(SELECT a.source_version_id FROM public.node_semantic_atoms l JOIN public.semantic_atoms a ON a.graph_id=l.graph_id AND a.semantic_atom_id=l.semantic_atom_id WHERE l.graph_id=OLD.graph_id AND l.node_id=OLD.node_id),true);
  RETURN OLD;
 END IF;
 IF NEW.title IS DISTINCT FROM OLD.title OR NEW.graph_id IS DISTINCT FROM OLD.graph_id THEN
  UPDATE public.reef_identity_corrections SET state='invalidated' WHERE graph_id=OLD.graph_id AND node_id=OLD.node_id AND state='active';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER aa_reef_identity_node_change BEFORE UPDATE OF title,graph_id OR DELETE ON public.nodes
 FOR EACH ROW EXECUTE FUNCTION public.reef_identity_node_changed();

CREATE FUNCTION public.reef_identity_atom_changed() RETURNS TRIGGER
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_RELID<>'public.atoms'::regclass OR TG_LEVEL<>'ROW' OR TG_WHEN<>'BEFORE' OR TG_OP NOT IN('UPDATE','DELETE') THEN
  RAISE EXCEPTION 'invalid reef identity trigger context' USING ERRCODE='42501';
 END IF;
 IF TG_OP='DELETE' THEN
  PERFORM public.reef_identity_erase_support(OLD.graph_id,ARRAY[OLD.atom_id],NULL,NULL,ARRAY[OLD.atom_id],'{}'::uuid[]);
  RETURN OLD;
 END IF;
 IF public.reef_identity_atom_fingerprint(NEW) IS DISTINCT FROM public.reef_identity_atom_fingerprint(OLD) THEN
  NEW.content_revision:=OLD.content_revision+1;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER aa_reef_identity_atom_revision BEFORE UPDATE OR DELETE ON public.atoms
 FOR EACH ROW EXECUTE FUNCTION public.reef_identity_atom_changed();
CREATE FUNCTION public.reef_identity_atom_updated() RETURNS TRIGGER
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_RELID<>'public.atoms'::regclass OR TG_LEVEL<>'ROW' OR TG_WHEN<>'AFTER' OR TG_OP<>'UPDATE' THEN
  RAISE EXCEPTION 'invalid reef identity trigger context' USING ERRCODE='42501';
 END IF;
 IF public.reef_identity_atom_fingerprint(NEW) IS DISTINCT FROM public.reef_identity_atom_fingerprint(OLD) THEN
  PERFORM public.reef_identity_erase_support(OLD.graph_id,ARRAY[OLD.atom_id],NULL,NULL,'{}'::uuid[],'{}'::uuid[]);
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER aa_reef_identity_atom_cleanup AFTER UPDATE ON public.atoms
 FOR EACH ROW EXECUTE FUNCTION public.reef_identity_atom_updated();

CREATE FUNCTION public.reef_identity_link_changed() RETURNS TRIGGER
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE g UUID;
BEGIN
 IF TG_RELID<>'public.node_atoms'::regclass OR TG_LEVEL<>'ROW' OR TG_WHEN<>'BEFORE' OR TG_OP NOT IN('UPDATE','DELETE') THEN
  RAISE EXCEPTION 'invalid reef identity trigger context' USING ERRCODE='42501';
 END IF;
 IF TG_OP='UPDATE' AND NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
 SELECT graph_id INTO g FROM public.nodes WHERE node_id=OLD.node_id;
 IF g IS NOT NULL THEN
  PERFORM public.reef_identity_erase_support(g,ARRAY[OLD.atom_id],NULL,ARRAY[OLD.node_id],ARRAY[OLD.atom_id],'{}'::uuid[]);
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
CREATE TRIGGER aa_reef_identity_unlink BEFORE UPDATE OR DELETE ON public.node_atoms
 FOR EACH ROW EXECUTE FUNCTION public.reef_identity_link_changed();

CREATE FUNCTION public.reef_identity_mapping_changed() RETURNS TRIGGER
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE excluded UUID[];
BEGIN
 IF TG_RELID<>'public.source_version_chunks'::regclass OR TG_LEVEL<>'ROW' OR TG_WHEN<>'BEFORE' OR TG_OP NOT IN('UPDATE','DELETE') THEN
  RAISE EXCEPTION 'invalid reef identity trigger context' USING ERRCODE='42501';
 END IF;
 IF TG_OP='UPDATE' AND NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
 excluded:=CASE WHEN EXISTS(SELECT 1 FROM public.source_version_chunks m
  JOIN public.source_versions v ON v.graph_id=m.graph_id AND v.version_id=m.source_version_id
  JOIN public.source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
  WHERE m.graph_id=OLD.graph_id AND m.atom_id=OLD.atom_id AND m.source_version_id<>OLD.source_version_id AND s.access_state='permitted')
  THEN '{}'::uuid[] ELSE ARRAY[OLD.atom_id] END;
 PERFORM public.reef_identity_erase_support(OLD.graph_id,ARRAY[OLD.atom_id],ARRAY[OLD.source_version_id],NULL,excluded,'{}'::uuid[]);
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
CREATE TRIGGER aa_reef_identity_mapping_loss BEFORE UPDATE OR DELETE ON public.source_version_chunks
 FOR EACH ROW EXECUTE FUNCTION public.reef_identity_mapping_changed();

CREATE FUNCTION public.reef_identity_source_changed() RETURNS TRIGGER
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE versions UUID[]; members UUID[]; pointer_only BOOLEAN:=false;
BEGIN
 IF TG_RELID NOT IN('public.source_items'::regclass,'public.source_versions'::regclass) OR TG_LEVEL<>'ROW' OR TG_WHEN<>'BEFORE' OR TG_OP NOT IN('UPDATE','DELETE') THEN
  RAISE EXCEPTION 'invalid reef identity trigger context' USING ERRCODE='42501';
 END IF;
 IF TG_OP='UPDATE' AND NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
 IF TG_RELID='public.source_items'::regclass THEN
  IF TG_OP='UPDATE' THEN
   IF (NEW.graph_id,NEW.source_item_id,NEW.source_type,NEW.provider_namespace,NEW.source_id,NEW.current_version_id,NEW.access_state)
    IS NOT DISTINCT FROM (OLD.graph_id,OLD.source_item_id,OLD.source_type,OLD.provider_namespace,OLD.source_id,OLD.current_version_id,OLD.access_state) THEN RETURN NEW; END IF;
  END IF;
  IF TG_OP='UPDATE' THEN
   pointer_only:=(NEW.graph_id,NEW.source_item_id,NEW.source_type,NEW.provider_namespace,NEW.source_id,NEW.access_state)
    IS NOT DISTINCT FROM (OLD.graph_id,OLD.source_item_id,OLD.source_type,OLD.provider_namespace,OLD.source_id,OLD.access_state);
  END IF;
  IF pointer_only THEN versions:=ARRAY[OLD.current_version_id];
  ELSE SELECT array_agg(version_id) INTO versions FROM public.source_versions WHERE graph_id=OLD.graph_id AND source_item_id=OLD.source_item_id; END IF;
 ELSE
  IF TG_OP='UPDATE' THEN
   IF (to_jsonb(NEW)-ARRAY['extraction_status','extraction_error','next_start_char'])
    IS NOT DISTINCT FROM (to_jsonb(OLD)-ARRAY['extraction_status','extraction_error','next_start_char']) THEN RETURN NEW; END IF;
  END IF;
  versions:=ARRAY[OLD.version_id];
 END IF;
 SELECT COALESCE(array_agg(DISTINCT atom_id),'{}'::uuid[]) INTO members FROM public.source_version_chunks WHERE graph_id=OLD.graph_id AND source_version_id=ANY(COALESCE(versions,'{}'::uuid[]));
 PERFORM public.reef_identity_erase_support(OLD.graph_id,members,versions,NULL,
  CASE WHEN pointer_only THEN '{}'::uuid[] ELSE members END,
  CASE WHEN pointer_only THEN '{}'::uuid[] ELSE COALESCE(versions,'{}'::uuid[]) END);
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
CREATE TRIGGER aa_reef_identity_source_loss BEFORE UPDATE OR DELETE ON public.source_items
 FOR EACH ROW EXECUTE FUNCTION public.reef_identity_source_changed();
CREATE TRIGGER aa_reef_identity_version_loss BEFORE UPDATE OR DELETE ON public.source_versions
 FOR EACH ROW EXECUTE FUNCTION public.reef_identity_source_changed();

CREATE FUNCTION public.reef_identity_purge_dependency() RETURNS TRIGGER
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE excluded UUID[]; versions UUID[];
BEGIN
 IF TG_RELID<>'public.source_purge_dependencies'::regclass OR TG_LEVEL<>'ROW' OR TG_WHEN<>'AFTER' OR TG_OP<>'INSERT' THEN
  RAISE EXCEPTION 'invalid reef identity trigger context' USING ERRCODE='42501';
 END IF;
 IF NEW.kind='atom' AND EXISTS(SELECT 1 FROM public.source_purge_events e JOIN public.graphs g ON g.id=e.graph_id
   JOIN public.graphs affected ON affected.id=NEW.graph_id AND affected.account_id=e.account_id
   WHERE e.event_id=NEW.event_id AND g.account_id=e.account_id
    AND EXISTS(SELECT 1 FROM public.atoms a WHERE a.graph_id=NEW.graph_id AND a.atom_id=NEW.object_id)) THEN
  SELECT array_agg(d.object_id) INTO excluded FROM public.source_purge_dependencies d WHERE d.event_id=NEW.event_id AND d.graph_id=NEW.graph_id AND d.kind='atom';
  SELECT COALESCE(array_agg(v.version_id),'{}'::uuid[]) INTO versions FROM public.source_versions v JOIN public.source_purge_events e ON e.event_id=NEW.event_id AND e.graph_id=v.graph_id AND e.source_item_id=v.source_item_id;
  PERFORM public.reef_identity_erase_support(NEW.graph_id,ARRAY[NEW.object_id],NULL,NULL,COALESCE(excluded,ARRAY[NEW.object_id]),versions);
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER aa_reef_identity_purge_dependency AFTER INSERT ON public.source_purge_dependencies
 FOR EACH ROW EXECUTE FUNCTION public.reef_identity_purge_dependency();

REVOKE ALL ON public.reef_identity_corrections FROM PUBLIC;
REVOKE ALL ON FUNCTION public.reef_identity_atom_fingerprint(public.atoms),public.reef_identity_support_valid(public.reef_identity_corrections),
 public.reef_identity_contains_label(TEXT,TEXT[]),public.reef_identity_sanitize_json(JSONB,TEXT[]),public.reef_identity_sanitize_body(JSONB,TEXT[]),public.reef_identity_record_integrity(),public.library_node_title(UUID,UUID),public.reef_identity_text_vectors(TEXT),public.reef_identity_independent_title(UUID,UUID,TEXT,UUID[],UUID[]),
 public.reef_identity_erase_support(UUID,UUID[],UUID[],UUID[],UUID[],UUID[],BOOLEAN),public.reef_identity_node_changed(),public.reef_identity_atom_changed(),public.reef_identity_atom_updated(),
 public.reef_identity_link_changed(),public.reef_identity_mapping_changed(),public.reef_identity_source_changed(),public.reef_identity_purge_dependency() FROM PUBLIC;
DO $$ DECLARE r TEXT; BEGIN
 FOREACH r IN ARRAY ARRAY['coralswarm_app','coralswarm_tenant_app'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN
   EXECUTE format('REVOKE ALL ON FUNCTION public.reef_identity_contains_label(TEXT,TEXT[]),public.reef_identity_sanitize_json(JSONB,TEXT[]),public.reef_identity_sanitize_body(JSONB,TEXT[]),public.reef_identity_independent_title(UUID,UUID,TEXT,UUID[],UUID[]),public.reef_identity_erase_support(UUID,UUID[],UUID[],UUID[],UUID[],UUID[],BOOLEAN),public.reef_identity_node_changed(),public.reef_identity_atom_changed(),public.reef_identity_atom_updated(),public.reef_identity_link_changed(),public.reef_identity_mapping_changed(),public.reef_identity_source_changed(),public.reef_identity_purge_dependency(),public.reef_identity_record_integrity() FROM %I',r);
   -- Only pure caller-value transforms are callable by this plane's app.
   -- No retained relation, privileged routine or authority lookup is reachable.
   IF r='coralswarm_tenant_app' THEN
    EXECUTE format('GRANT EXECUTE ON FUNCTION public.reef_identity_contains_label(TEXT,TEXT[]),public.reef_identity_sanitize_json(JSONB,TEXT[]),public.reef_identity_sanitize_body(JSONB,TEXT[]) TO %I',r);
   END IF;
   EXECUTE format('REVOKE ALL ON public.reef_identity_corrections FROM %I',r);
   EXECUTE format('GRANT SELECT,INSERT,UPDATE ON public.reef_identity_corrections TO %I',r);
   EXECUTE format('GRANT EXECUTE ON FUNCTION public.reef_identity_atom_fingerprint(public.atoms),public.reef_identity_support_valid(public.reef_identity_corrections),public.library_node_title(UUID,UUID),public.reef_identity_text_vectors(TEXT) TO %I',r);
  END IF;
 END LOOP;
END $$;
