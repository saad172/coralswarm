#!/usr/bin/env node
/**
 * Draft Composio capture recipes from Composio's own tool metadata.
 *
 *   node scripts/composio/draft_recipe.mjs fetch [--all | <slug>...] [--refresh]
 *   node scripts/composio/draft_recipe.mjs classify [--validation <validate_draft_recipes output>]
 *   node scripts/composio/draft_recipe.mjs draft <slug>... | --eligible
 *
 * Only metadata endpoints are read (GET /api/v3/toolkits/{slug}, GET /api/v3/tools).
 * Nothing is created at Composio and no toolkit tool is executed. The auth-config
 * call each draft needs is written into its review file, never sent.
 *
 * Env: COMPOSIO_API_KEY (else read from COMPOSIO_ENV_FILE or ../coralswarm/.env),
 * COMPOSIO_CATALOG_DIR (cache, default outputs/composio-catalog next to the repo).
 *
 * ---------------------------------------------------------------------------
 * Data shape
 * ---------------------------------------------------------------------------
 * @typedef {'oauth2'|'api_key'|'other'|'none'} AuthKind
 *
 * @typedef {Object} Hints   Composio tool tags, reduced to the MCP hints.
 * @property {boolean} read_only     `readOnlyHint`
 * @property {boolean} destructive   `destructiveHint`
 * @property {boolean} deprecated    `deprecated` tag or `is_deprecated`
 *
 * @typedef {Object} CatalogTool
 * @property {string} slug
 * @property {string} name
 * @property {string} description
 * @property {string} version
 * @property {string[]} tags
 * @property {Hints} hints
 * @property {string[]} scopes            Composio's per-tool OAuth scopes.
 * @property {object|null} scope_requirements
 * @property {object} input_parameters    JSON Schema.
 * @property {object} output_parameters   JSON Schema of the execute response; recipe paths are relative to its `data`.
 *
 * @typedef {Object} CatalogAuth
 * @property {string} mode                OAUTH2, API_KEY, BEARER_TOKEN, ...
 * @property {boolean} managed            Composio-managed credentials exist for this mode.
 * @property {string[]|null} default_scopes  The `scopes` field default Composio requests (all scopes it knows), when exposed.
 *
 * @typedef {Object} Catalog   One cached file per toolkit: <cache>/toolkits/<slug>.json
 * @property {string} slug
 * @property {string} name
 * @property {string} version             Latest toolkit version (tools are listed at this version).
 * @property {string[]} categories
 * @property {boolean} no_auth
 * @property {CatalogAuth[]} auth
 * @property {CatalogTool[]} tools        Non-deprecated tools at the latest version.
 * @property {string} fetched_at
 * @property {string|null} error          Fetch failure; the toolkit is then classified `no_list_tool`.
 *
 * @typedef {'native_refused'|'no_list_tool'|'full_scan_only'|'filter'|'sort_stop'} Verdict
 *
 * @typedef {Object} ItemFields   Recipe `fields` inferred from the item schema (pointers relative to the item).
 * @property {string|null} id
 * @property {string|null} updated_at
 * @property {string|null} created_at
 * @property {string|null} title
 * @property {string|null} body
 * @property {string|null} state
 * @property {string|null} reference
 * @property {{kind:string,id:string,name:string|null}|null} container
 * @property {{object:string,id:string,name:string}|null} author
 *
 * @typedef {Object} Evidence   Why a stream tool qualifies.
 * @property {string} tool
 * @property {'filter'|'sort_stop'} change
 * @property {'direct'|'structured'|'query'|'graphql'|'sort'} kind  How the tool takes the change argument.
 * @property {string} param               Input property that carries the watermark or the sort.
 * @property {'since'|'since_minutes'|'sort'} slot
 * @property {*} value                    Argument value to draft (`{{since}}`, `-{{since_minutes}}m`, a sort token or object), or null if hand-written.
 * @property {{param:string,value:*}|null} direction  Separate newest-first argument for sort_stop.
 * @property {string} text                Schema text (name, type, enum, examples, description) that justified it.
 * @property {string|null} cursor_param   Input property for the continuation token.
 * @property {string|null} page_size_param
 * @property {string|null} items_ptr      Item array within the output `data`.
 * @property {{has_more:string|null,is_last:string|null,next:string|null}|null} paging
 * @property {ItemFields|null} fields
 * @property {string[]} gaps              Engine/recipe mismatches (GAP_TEXT); ENGINE_BLOCKING ones stop a recipe loading or running as-is.
 * @property {string[]} container_params  Required inputs left unfilled; non-empty means not workspace-wide.
 *
 * @typedef {Object} Classification   One row of the eligibility table.
 * @property {string} slug
 * @property {string} name
 * @property {Verdict} verdict
 * @property {string} why                 filter/sort_stop: ok | engine_gap. full_scan_only: no_change_param | container_scoped | requires_input | item_lacks_id_or_updated | no_desc_direction. no_list_tool: no_read_tools | no_item_array_output | fetch_error.
 * @property {AuthKind} auth_kind         Strongest auth Composio offers (oauth2 > api_key > other > none).
 * @property {boolean} managed_oauth      Composio-managed OAuth exists, so a narrowed auth config needs no own app.
 * @property {boolean} read_only_narrowable  OAuth whose needed scopes are known and none is write-ish; API-key toolkits never are.
 * @property {number} tool_count
 * @property {string[]} read_tools        Rule-8 candidates: readOnlyHint ∩ no write verb − destructiveHint, plus GraphQL query_or_mutation tools.
 * @property {string[]} list_tools        read_tools whose output holds an item array (or GraphQL).
 * @property {Evidence[]} evidence        filter / sort_stop only; best first.
 * @property {{tool:string,param:string,change:string,why:string,text:string}[]} rejected  Change-capable tools that were ruled out, with the reason.
 * @property {{tool:string,ptr:string,kind:string}|null} namespace  No-input tool and id path for the workspace (or user) namespace.
 * @property {string[]} read_scopes       One scope per Composio scope_requirements clause of the best stream + namespace tools.
 * @property {string} scope_source
 * @property {boolean} has_text           The best stream's item has a title or body path.
 * @property {boolean} recipe_ready       Engine-ready, narrowable, managed OAuth, namespace found, and has text.
 * @property {string|null} draft_validation  `ok` or recipe.rs's first error for scripts/composio/drafts/<slug>.json, when classify is given --validation.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '../..');
const BASE = 'https://backend.composio.dev';
const CACHE = process.env.COMPOSIO_CATALOG_DIR
  || '/home/dev/Developer/personal/coralswarm/outputs/composio-catalog';
const TOOLKIT_INDEX = process.env.COMPOSIO_TOOLKIT_INDEX
  || '/home/dev/Developer/personal/coralswarm/outputs/composio-live-capture-evidence/toolkits_all.json';
const DRAFTS = path.join(HERE, 'drafts');
const RECIPE_RS = path.join(REPO, 'backend/src/connectors/composio/recipe.rs');

// Composio's limit is 2,000 req/min per organization, shared with production polling.
const MIN_INTERVAL_MS = 200;

function die(msg) {
  console.error(msg);
  process.exit(1);
}

function apiKey() {
  if (process.env.COMPOSIO_API_KEY) return process.env.COMPOSIO_API_KEY;
  const file = process.env.COMPOSIO_ENV_FILE
    || '/home/dev/Developer/personal/coralswarm/coralswarm/.env';
  const m = fs.existsSync(file) && fs.readFileSync(file, 'utf8').match(/^COMPOSIO_API_KEY=(.*)$/m);
  if (!m) die('COMPOSIO_API_KEY not set and not found in ' + file);
  return m[1].trim().replace(/^["']|["']$/g, '');
}

let lastRequest = 0;
async function get(pathAndQuery, key) {
  for (let attempt = 0; ; attempt++) {
    const wait = lastRequest + MIN_INTERVAL_MS - Date.now();
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    lastRequest = Date.now();
    let res;
    try {
      res = await fetch(BASE + pathAndQuery, { headers: { 'x-api-key': key }, redirect: 'error' });
    } catch (e) {
      if (attempt < 4) { await new Promise((r) => setTimeout(r, 2000 * (attempt + 1))); continue; }
      throw new Error(`${pathAndQuery}: ${e.message}`);
    }
    if ((res.status === 429 || res.status >= 500) && attempt < 4) {
      await new Promise((r) => setTimeout(r, 5000 * (attempt + 1)));
      continue;
    }
    const text = await res.text();
    if (!res.ok) throw new Error(`${pathAndQuery}: HTTP ${res.status} ${text.slice(0, 200)}`);
    return JSON.parse(text);
  }
}

// ---------------------------------------------------------------------------
// fetch
// ---------------------------------------------------------------------------

const catalogPath = (slug) => path.join(CACHE, 'toolkits', `${slug}.json`);

function writeAtomic(file, data) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, typeof data === 'string' ? data : JSON.stringify(data));
  fs.renameSync(tmp, file);
}

function hintsOf(t) {
  const tags = t.tags || [];
  return {
    read_only: tags.includes('readOnlyHint'),
    destructive: tags.includes('destructiveHint'),
    deprecated: tags.includes('deprecated') || !!t.is_deprecated,
  };
}

function defaultScopes(detail) {
  const optional = detail?.fields?.auth_config_creation?.optional || [];
  const required = detail?.fields?.auth_config_creation?.required || [];
  const field = [...required, ...optional].find((f) => f.name === 'scopes');
  if (!field || typeof field.default !== 'string') return null;
  return field.default.split(/[\s,]+/).filter(Boolean);
}

async function fetchToolkit(slug, key) {
  const catalog = { slug, name: slug, version: '', categories: [], no_auth: false, auth: [], tools: [], fetched_at: new Date().toISOString(), error: null };
  try {
    const tk = await get(`/api/v3/toolkits/${encodeURIComponent(slug)}`, key);
    catalog.name = tk.name || slug;
    catalog.version = tk.meta?.version || '';
    catalog.categories = (tk.meta?.categories || []).map((c) => c.id || c.name);
    catalog.no_auth = !!tk.no_auth;
    const managed = new Set(tk.composio_managed_auth_schemes || []);
    catalog.auth = (tk.auth_config_details || []).map((d) => ({
      mode: d.mode,
      managed: managed.has(d.mode),
      default_scopes: defaultScopes(d),
    }));
    const tools = [];
    let cursor = null;
    do {
      const q = new URLSearchParams({ toolkit_slug: slug, limit: '100', toolkit_versions: 'latest', include_deprecated: 'false' });
      if (cursor) q.set('cursor', cursor);
      const page = await get(`/api/v3/tools?${q}`, key);
      tools.push(...(page.items || []));
      cursor = page.next_cursor || null;
    } while (cursor);
    for (const t of tools) {
      let full = t;
      if (!t.input_parameters || !t.output_parameters) {
        full = await get(`/api/v3/tools/${encodeURIComponent(t.slug)}?toolkit_versions=latest`, key);
      }
      const hints = hintsOf(full);
      if (hints.deprecated) continue;
      catalog.tools.push({
        slug: full.slug,
        name: full.name || '',
        description: full.description || '',
        version: full.version || '',
        tags: full.tags || [],
        hints,
        scopes: full.scopes || [],
        scope_requirements: full.scope_requirements ?? null,
        input_parameters: full.input_parameters || {},
        output_parameters: full.output_parameters || {},
      });
    }
    if (!catalog.version) catalog.version = catalog.tools[0]?.version || '';
  } catch (e) {
    catalog.error = e.message;
  }
  return catalog;
}

async function cmdFetch(args) {
  const refresh = args.includes('--refresh');
  let slugs = args.filter((a) => !a.startsWith('--'));
  if (args.includes('--all')) {
    const index = JSON.parse(fs.readFileSync(TOOLKIT_INDEX, 'utf8'));
    slugs = (index.items || index).map((t) => t.slug);
  }
  if (!slugs.length) die('fetch: give --all or toolkit slugs');
  const key = apiKey();
  let done = 0, skipped = 0, failed = 0;
  for (const slug of slugs) {
    const file = catalogPath(slug);
    if (!refresh && fs.existsSync(file)) {
      const cached = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (!cached.error) { skipped++; continue; }
    }
    const catalog = await fetchToolkit(slug, key);
    writeAtomic(file, catalog);
    if (catalog.error) failed++;
    done++;
    if ((done + skipped) % 50 === 0 || catalog.error) {
      console.error(`[${done + skipped}/${slugs.length}] ${slug} tools=${catalog.tools.length}${catalog.error ? ' ERROR ' + catalog.error : ''}`);
    }
  }
  console.error(`fetch: ${done} fetched, ${skipped} cached, ${failed} failed`);
}

// ---------------------------------------------------------------------------
// recipe.rs is the source of truth for the native list and the write verbs
// ---------------------------------------------------------------------------

function rustStringList(name) {
  const src = fs.readFileSync(RECIPE_RS, 'utf8');
  const m = src.match(new RegExp(`const ${name}: &\\[[^=]*= &\\[([\\s\\S]*?)\\];`));
  if (!m) die(`cannot find ${name} in recipe.rs`);
  return m[1];
}
const NATIVE = new Set([...rustStringList('NATIVE_TOOLKITS').matchAll(/\("([a-z0-9_]+)"/g)].map((m) => m[1]));
const WRITE_VERBS = new Set([...rustStringList('WRITE_VERBS').matchAll(/"([A-Z]+)"/g)].map((m) => m[1]));
const GRAPHQL_DOCUMENT_ARG = 'query_or_mutation';

// ---------------------------------------------------------------------------
// schema helpers
// ---------------------------------------------------------------------------

const norm = (s) => String(s).toLowerCase().replace(/[^a-z0-9]/g, '');
const ptrSeg = (k) => String(k).replace(/~/g, '~0').replace(/\//g, '~1');
const clip = (s, n = 220) => (s.length > n ? s.slice(0, n - 1) + '…' : s);
const oneLine = (s) => String(s ?? '').replace(/\s+/g, ' ').trim();

function resolver(root) {
  const defs = { ...(root?.definitions || {}), ...(root?.$defs || {}) };
  const deref = (s) => {
    for (let n = 0; s && s.$ref && n < 20; n++) s = defs[s.$ref.split('/').pop()];
    return s || {};
  };
  const branches = (s, depth = 0) => {
    s = deref(s);
    const alts = s.anyOf || s.oneOf || (s.allOf && s.allOf.length === 1 ? s.allOf : null);
    if (!alts || depth > 5) return [s];
    return alts.map(deref).filter((b) => b.type !== 'null').flatMap((b) => branches(b, depth + 1));
  };
  const props = (s) => {
    const out = {};
    for (const b of branches(s)) for (const [k, v] of Object.entries(b.properties || {})) if (!(k in out)) out[k] = v;
    return out;
  };
  const typeOf = (s) => {
    const own = deref(s);
    if (own.type && own.type !== 'null') return Array.isArray(own.type) ? own.type.find((t) => t !== 'null') : own.type;
    return branches(s).map((b) => (Array.isArray(b.type) ? b.type.find((t) => t !== 'null') : b.type)).find(Boolean) || '';
  };
  return { deref, branches, props, typeOf };
}

/** Everything a schema says about one property, lower-cased, for matching. */
function propText(name, s, r) {
  const b = r.branches(s);
  const parts = [name, r.typeOf(s), s.format || b.map((x) => x.format).find(Boolean) || ''];
  const en = b.flatMap((x) => x.enum || []);
  if (en.length) parts.push('enum=' + JSON.stringify(en));
  const def = s.default ?? b.map((x) => x.default).find((x) => x !== undefined);
  if (def !== undefined) parts.push('default=' + JSON.stringify(def));
  const ex = s.examples || b.flatMap((x) => x.examples || []);
  if (ex.length) parts.push('examples=' + JSON.stringify(ex));
  parts.push(oneLine(s.description || b.map((x) => x.description).find(Boolean) || ''));
  return parts.join(' ');
}

function inputProps(tool) {
  const root = tool.input_parameters || {};
  const r = resolver(root);
  const props = r.props(root);
  const required = new Set(root.required || []);
  return Object.entries(props).map(([name, s]) => {
    const b = r.branches(s);
    return {
      name,
      n: norm(name),
      type: r.typeOf(s),
      format: s.format || b.map((x) => x.format).find(Boolean) || '',
      enum: b.flatMap((x) => x.enum || []),
      examples: s.examples || b.flatMap((x) => x.examples || []),
      default: s.default ?? b.map((x) => x.default).find((x) => x !== undefined),
      description: oneLine(s.description || b.map((x) => x.description).find(Boolean) || ''),
      required: required.has(name),
      text: propText(name, s, r),
    };
  });
}

/** Arrays of objects inside the tool output's `data`; pointers are relative to `data`. */
function outputShape(tool) {
  const root = tool.output_parameters || {};
  const r = resolver(root);
  const data = r.props(root).data;
  const arrays = [];
  const scalars = [];
  if (!data) return { arrays, scalars, r };
  const walk = (s, ptr, depth, seen) => {
    if (depth > 5) return;
    for (const b of r.branches(s)) {
      if (seen.has(b)) continue;
      seen.add(b);
      if (b.type === 'array' || b.items) {
        const items = b.items || {};
        const itemProps = r.props(items);
        if (Object.keys(itemProps).length) arrays.push({ ptr, items, props: itemProps, depth });
        continue;
      }
      const p = r.props(b);
      if (Object.keys(p).length) {
        for (const [k, v] of Object.entries(p)) walk(v, `${ptr}/${ptrSeg(k)}`, depth + 1, new Set(seen));
      } else if (ptr) {
        scalars.push({ ptr, name: ptr.split('/').pop(), type: r.typeOf(b) || b.type || '', description: oneLine(b.description || '') });
      }
    }
  };
  walk(data, '', 0, new Set());
  return { arrays, scalars, r };
}

/** Scalar leaves of one item schema, as pointers relative to the item. */
function itemLeaves(shape, arr, maxDepth = 3) {
  const { r } = shape;
  const out = [];
  const walk = (props, ptr, depth) => {
    for (const [k, v] of Object.entries(props)) {
      const p = `${ptr}/${ptrSeg(k)}`;
      const t = r.typeOf(v);
      const sub = r.props(v);
      if (t === 'object' || Object.keys(sub).length) {
        out.push({ ptr: p, name: k, type: 'object', depth });
        if (depth < maxDepth) walk(sub, p, depth + 1);
      } else {
        const b = r.branches(v);
        out.push({ ptr: p, name: k, type: t, depth, format: v.format || b.map((x) => x.format).find(Boolean) || '', description: oneLine(v.description || '') });
      }
    }
  };
  walk(arr.props, '', 0);
  return out.map((l, i) => [l, i]).sort((a, b) => a[0].depth - b[0].depth || a[1] - b[1]).map(([l]) => l);
}

// ---------------------------------------------------------------------------
// classification
// ---------------------------------------------------------------------------

const UPDATED_RX = /(updated|modified|lastedited|editedtime|edittime|updatetime|modifytime|modtime|systemmodstamp|lastchanged|changedat|datechanged|changedsince|changedafter|changetime)/;
const NOT_UPDATED_RX = /(unmodified|notmodified|updatedby|modifiedby|editedby|lasteditedby|updater|modifier|isupdated|ismodified|updatedcount|modifiedcount|updatedfields)/;
const isUpdatedName = (name) => UPDATED_RX.test(norm(name)) && !NOT_UPDATED_RX.test(norm(name));
const LOWER_BOUND_RX = /(after|since|from|gte|gt$|min$|start|begin|newer|later|ge$)/;
const UPPER_BOUND_RX = /(before|until|till|to$|lte|lt$|max$|end$|ending|older|earlier|le$)/;
const SINCE_ALONE = new Set(['since', 'modifiedsince', 'ifmodifiedsince', 'updatedsince', 'from', 'after', 'fromdate', 'startdate', 'starttime', 'sincedate', 'sincetime', 'mindate', 'datefrom', 'timefrom', 'fromtime', 'start']);
const SINCE_DESC_RX = /\b(updated|modified|changed|edited)\b[^.;]{0,50}\b(after|since|on or after|later than|newer than|>=)|\b(after|since)\b[^.;]{0,30}\b(last )?(updated|modified|changed|edited)\b|if-modified-since/i;
const QUERY_PARAM_NAMES = new Set(['jql', 'soql', 'sosl', 'cql', 'query', 'q', 'filter', 'filters', 'search', 'where', 'filterbyformula', 'formula', 'criteria', 'conditions', 'filtergroups', 'odatafilter', 'searchquery', 'querystring', 'sql', 'expression', 'filterexpression', 'searchcriteria', 'filterquery']);
const QUERY_UPDATED_RX = /([\w.$]*(?:updated|modified|last_?edited|lastmodifieddate|systemmodstamp)[\w.]*)["'`]?[^.\n;]{0,80}?(>=|=>|>|\bgte?\b|\bafter\b|on_or_after|\bsince\b|\$gte?)/i;
const RELATIVE_MINUTES_RX = /(^|[\s'"(=])-\d+m\b|last_n_minutes|\bminutes? ago\b|now\(\)\s*-\s*\d+\s*m/i;
const CURSOR_NAMES = new Set(['cursor', 'nextcursor', 'startcursor', 'after', 'aftercursor', 'pagetoken', 'nextpagetoken', 'continuationtoken', 'continuation', 'startingafter', 'nexttoken', 'paginationtoken', 'pagecursor', 'marker', 'bookmark', 'scrollid', 'offsettoken', 'nextpage', 'next', 'pagingtoken', 'starttoken', 'skiptoken', 'pageafter', 'cursorafter', 'searchafter', 'nextpagecursor', 'startingtoken', 'pagetokennext', 'continuationkey', 'lastevaluatedkey', 'startkey']);
const NUMERIC_PAGING_NAMES = new Set(['page', 'pagenumber', 'pageno', 'pagenum', 'offset', 'start', 'startat', 'skip', 'pageindex', 'startindex', 'from', 'currentpage', 'pagenumberparam']);
const PAGE_SIZE_NAMES = new Set(['limit', 'pagesize', 'perpage', 'maxresults', 'first', 'count', 'size', 'top', 'maxitems', 'maxrecords', 'resultsperpage', 'numresults', 'pagelimit', 'batchsize', 'max', 'pagelength', 'resultlimit', 'maxcount', 'rows', 'num', 'take', 'itemsperpage', 'recordsperpage', 'maxpagesize']);
const HAS_MORE_NAMES = new Set(['hasmore', 'hasnextpage', 'hasnext', 'more', 'moreresults', 'hasmoreresults', 'hasmoreitems', 'morepages', 'moreitems', 'hasmorepages', 'morerecords']);
const IS_LAST_NAMES = new Set(['islast', 'islastpage', 'last', 'lastpage']);
const NEXT_NAMES = new Set(['nextcursor', 'nextpagetoken', 'endcursor', 'cursor', 'next', 'nexttoken', 'continuationtoken', 'continuation', 'after', 'bookmark', 'nextpage', 'scrollid', 'paginationtoken', 'nextpagecursor', 'pagetoken', 'startcursor', 'offsettoken', 'marker', 'nextmarker', 'skiptoken', 'aftercursor', 'nextkey', 'lastevaluatedkey']);
const SORT_FIELD_RX = /(sort|order|^timestamp$|^ordering$)/;
const DIRECTION_NAMES = /^(direction|sortdirection|sortorder|order|orderdirection|sortdir|dir|ascending|descending|desc|asc|sortascending|orderasc|sortdesc|ordertype|sorttype|sortmode)$/;
const DESC_VALUE_RX = /^(desc|descending|newest|newestfirst|latest|-1|dsc)$/i;
const ITEM_GENERIC = new Set(['results', 'items', 'data', 'records', 'values', 'list', 'entries', 'objects', 'nodes', 'edges', 'hits', 'rows', 'elements', 'collection', 'content', 'response', 'result', 'value', 'docs', 'documents']);

function readToolInfo(catalog) {
  const prefix = catalog.slug.toUpperCase() + '_';
  const out = [];
  for (const t of catalog.tools) {
    const rest = t.slug.startsWith(prefix) ? t.slug.slice(prefix.length) : t.slug;
    const verb = rest.split('_').find((w) => WRITE_VERBS.has(w)) || null;
    const graphql = inputProps(t).some((p) => p.name === GRAPHQL_DOCUMENT_ARG);
    const eligible = !t.hints.deprecated && !verb && ((t.hints.read_only && !t.hints.destructive) || graphql);
    if (eligible) out.push({ tool: t, graphql, prefixed: t.slug.startsWith(prefix) });
  }
  return out;
}

function sinceSlot(p) {
  const text = p.text.toLowerCase();
  const ex = [...p.examples, p.default].filter((x) => x !== undefined && x !== null).map(String);
  if (/unix|epoch|milliseconds since|seconds since/.test(text) || ((p.type === 'integer' || p.type === 'number') && !ex.some((e) => /^\d{4}-/.test(e))) || (ex.length && ex.every((e) => /^\d{9,}$/.test(e)))) {
    return { slot: 'since', value: '{{since}}', gaps: ['epoch_since'] };
  }
  if (ex.some((e) => /^-\d+m$/.test(e))) return { slot: 'since_minutes', value: '-{{since_minutes}}m', gaps: [] };
  if (p.format === 'date-time' || /iso ?8601|rfc ?3339|date-?time|\d{4}-\d\d-\d\dt\d\d|yyyy-mm-ddthh|hh:mm/.test(text) || ex.some((e) => /\d{4}-\d\d-\d\dT\d\d/.test(e))) {
    return { slot: 'since', value: '{{since}}', gaps: [] };
  }
  if (p.format === 'date' || /yyyy-mm-dd/.test(text) || ex.some((e) => /^\d{4}-\d\d-\d\d$/.test(e))) {
    return { slot: 'since', value: '{{since}}', gaps: ['date_only_since'] };
  }
  return { slot: 'since', value: '{{since}}', gaps: ['since_format_unverified'] };
}

/** Replace the single date-like string in an example filter object with the since slot. */
function sinceTemplateFromExample(example) {
  let replaced = 0;
  const walk = (v) => {
    if (typeof v === 'string' && /^\d{4}-\d\d-\d\d/.test(v)) { replaced++; return '{{since}}'; }
    if (Array.isArray(v)) return v.map(walk);
    if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, walk(x)]));
    return v;
  };
  const out = walk(example);
  return replaced === 1 ? out : null;
}

function filterEvidence(tool, props, graphql) {
  const found = [];
  if (graphql) {
    const t = oneLine(tool.description + ' ' + (props.find((p) => p.name === GRAPHQL_DOCUMENT_ARG)?.description || ''));
    const m = t.match(/[^.]{0,80}\b(gte?|updatedAt|filter)\b[^.]{0,80}/i);
    if (/\bgte?\b/.test(t) && /filter/i.test(t)) {
      found.push({ param: GRAPHQL_DOCUMENT_ARG, slot: 'since', value: null, kind: 'graphql', text: clip(m ? m[0].trim() : t), gaps: ['graphql_query_handwritten'] });
    }
    return found;
  }
  for (const p of props) {
    const n = p.n;
    if (UPPER_BOUND_RX.test(n) && !LOWER_BOUND_RX.test(n)) continue;
    // A conditional GET (304 when unchanged) returns everything or nothing; it does not filter.
    if (/\b304\b|not modified/i.test(p.description)) continue;
    const direct = (isUpdatedName(p.name) && (LOWER_BOUND_RX.test(n.replace(UPDATED_RX, '')) || SINCE_DESC_RX.test(p.description)))
      || n === 'ifmodifiedsince'
      || (SINCE_ALONE.has(n) && SINCE_DESC_RX.test(p.description));
    if (direct && p.type !== 'boolean' && p.type !== 'array') {
      const s = sinceSlot(p);
      found.push({ param: p.name, slot: s.slot, value: s.value, kind: 'direct', text: clip(p.text), gaps: s.gaps });
      continue;
    }
    if (QUERY_PARAM_NAMES.has(n)) {
      const m = p.text.match(QUERY_UPDATED_RX);
      if (!m) continue;
      if (p.type === 'object' || p.type === 'array') {
        const ex = p.examples.find((e) => e && typeof e === 'object' && QUERY_UPDATED_RX.test(JSON.stringify(e)));
        const tpl = ex ? sinceTemplateFromExample(ex) : null;
        found.push({ param: p.name, slot: 'since', value: tpl, kind: 'structured', text: clip(m[0] + ' … ' + p.text), gaps: tpl ? [] : ['filter_object_handwritten'] });
      } else {
        const rel = RELATIVE_MINUTES_RX.test(p.text);
        found.push({ param: p.name, slot: rel ? 'since_minutes' : 'since', value: null, kind: 'query', text: clip(m[0] + ' … ' + p.text), gaps: [rel ? 'query_text_handwritten' : 'since_embedded_in_text'] });
      }
    }
  }
  return found;
}

function updatedToken(values) {
  return values.map(String).find((v) => isUpdatedName(v.replace(/^[-+]/, '')) || UPDATED_RX.test(norm(v)));
}

function sortEvidence(props) {
  const found = [];
  for (const p of props) {
    if (!SORT_FIELD_RX.test(p.n)) continue;
    if (!UPDATED_RX.test(norm(p.text))) continue;
    const objExample = p.examples.find((e) => e && typeof e === 'object' && UPDATED_RX.test(norm(JSON.stringify(e))) && /desc|"ascending":false|descending/i.test(JSON.stringify(e)));
    if (objExample && (p.type === 'array' || p.type === 'object')) {
      found.push({ param: p.name, value: objExample, direction: null, text: clip(p.text), gaps: [] });
      continue;
    }
    if (p.type === 'array' || p.type === 'object') {
      found.push({ param: p.name, value: null, direction: null, text: clip(p.text), gaps: ['sort_object_handwritten'] });
      continue;
    }
    const quoted = [...p.description.matchAll(/[`'"]([-+]?[A-Za-z_.:]+(?:\s+(?:asc|desc))?)[`'"]/g)].map((m) => m[1]);
    const candidates = [...p.enum, ...p.examples.filter((e) => typeof e === 'string'), ...quoted];
    const token = updatedToken(candidates.filter((v) => /desc|^-/i.test(String(v)))) || updatedToken(candidates);
    if (!token) continue;
    if (/desc/i.test(token) || token.startsWith('-')) {
      found.push({ param: p.name, value: token, direction: null, text: clip(p.text), gaps: [] });
      continue;
    }
    let direction = null;
    for (const d of props) {
      if (d === p || !DIRECTION_NAMES.test(d.n)) continue;
      if (d.type === 'boolean') {
        if (/^(ascending|asc|sortascending|orderasc)$/.test(d.n)) direction = { param: d.name, value: false };
        if (/^(descending|desc|sortdesc)$/.test(d.n)) direction = { param: d.name, value: true };
      } else {
        const dv = [...d.enum, ...d.examples, ...[...d.description.matchAll(/[`'"]([A-Za-z-]+)[`'"]/g)].map((m) => m[1])].map(String).find((v) => DESC_VALUE_RX.test(v));
        if (dv) direction = { param: d.name, value: dv };
        else if (/\bdesc(ending)?\b/i.test(d.text)) direction = { param: d.name, value: /descending/i.test(d.text) ? 'descending' : 'desc' };
      }
      if (direction) break;
    }
    if (!direction && /prefix(ed)? (it )?with (a )?['"`]?-|['"`]-['"`] (prefix )?for desc|use ['"`]?-\w+/i.test(p.description)) {
      found.push({ param: p.name, value: '-' + token, direction: null, text: clip(p.text), gaps: [] });
      continue;
    }
    if (!direction && /(default(s)?( to| is)? (desc|descending|newest)|newest first|most recent(ly)? (updated )?first|descending by default)/i.test(p.text)) {
      found.push({ param: p.name, value: token, direction: null, text: clip(p.text), gaps: ['direction_default_desc'] });
      continue;
    }
    if (!direction) {
      found.push({ param: p.name, value: token, direction: null, text: clip(p.text), gaps: ['no_desc_direction'] });
      continue;
    }
    found.push({ param: p.name, value: token, direction, text: clip(p.text + ' | ' + direction.param + '=' + JSON.stringify(direction.value)), gaps: [] });
  }
  return found;
}

function pickItems(shape) {
  if (!shape.arrays.length) return null;
  const score = (a) => {
    const hasUpdated = Object.keys(a.props).some(isUpdatedName);
    const last = a.ptr.split('/').pop();
    return a.depth * 10 + (hasUpdated ? 0 : 5) + (ITEM_GENERIC.has(norm(last)) || /s$/.test(last) ? 0 : 1);
  };
  return [...shape.arrays].sort((a, b) => score(a) - score(b))[0];
}

const PAGING_WORDS = ['cursor', 'token', 'after', 'marker', 'bookmark', 'offset', 'scroll', 'continuation', 'key'];

function pagingOf(shape, items, cursorParam) {
  const outside = shape.scalars.filter((s) => !items || !s.ptr.startsWith(items.ptr + '/'));
  const byName = (set, type) => outside.filter((s) => set.has(norm(s.name)) && (!type || s.type === type)).sort((a, b) => a.ptr.length - b.ptr.length)[0];
  const hasMore = byName(HAS_MORE_NAMES, 'boolean');
  const isLast = hasMore ? null : byName(IS_LAST_NAMES, 'boolean');
  const hint = cursorParam ? PAGING_WORDS.filter((w) => norm(cursorParam).includes(w)) : [];
  const nextScore = (s) => {
    const n = norm(s.name);
    const url = /url|link/.test(n) || /\burl\b|https?:/i.test(s.description);
    return (n === norm(cursorParam || '') ? -5 : 0) - hint.filter((w) => n.includes(w)).length * 2 + (url ? 4 : 0) + s.ptr.length / 100;
  };
  const next = outside.filter((s) => NEXT_NAMES.has(norm(s.name)) && s.type === 'string').sort((a, b) => nextScore(a) - nextScore(b))[0];
  return { has_more: hasMore?.ptr ?? null, is_last: isLast?.ptr ?? null, next: next?.ptr ?? null };
}

const ID_NAMES = ['id', 'uuid', 'gid', 'guid'];
const UPDATED_PREFERRED = ['updatedat', 'updated', 'lasteditedtime', 'modifiedat', 'modified', 'lastmodified', 'lastmodifieddate', 'lastmodifieddatetime', 'updatetime', 'modifiedtime', 'datemodified', 'dateupdated', 'lastupdated', 'updatedtime', 'updatedon', 'modifiedon', 'systemmodstamp'];
const CREATED_NAMES = ['createdat', 'created', 'createdtime', 'datecreated', 'creationdate', 'createdon', 'createddate', 'createddatetime', 'createtime'];
const TITLE_NAMES = ['title', 'name', 'subject', 'summary', 'headline', 'displayname', 'filename', 'label'];
const BODY_NAMES = ['description', 'body', 'content', 'text', 'message', 'notes', 'note', 'comment', 'markdown', 'plaintext', 'bodytext', 'html', 'bodyhtml', 'snippet', 'details', 'descriptionhtml', 'contenttext', 'textcontent', 'preview', 'excerpt'];
const STATE_NAMES = ['state', 'status', 'stage', 'workflowstate'];
const REF_NAMES = ['identifier', 'key', 'number', 'reference', 'shortid', 'ticketnumber', 'displayid', 'issuekey'];
const CONTAINER_NAMES = ['project', 'board', 'space', 'list', 'folder', 'workspace', 'team', 'repository', 'repo', 'channel', 'database', 'base', 'notebook', 'collection', 'section', 'portal', 'group', 'organization', 'company', 'account', 'pipeline', 'category', 'forum', 'brand', 'site'];
const AUTHOR_OBJECTS = ['creator', 'createdby', 'author', 'reporter', 'user', 'owner', 'submitter', 'requester', 'from', 'sender', 'postedby', 'createdbyuser'];
const PERSON_NAME = ['displayname', 'name', 'fullname', 'username', 'login', 'realname'];

/** Recipe `fields` inferred from an item schema; null members could not be inferred. */
function inferFields(shape, items) {
  if (!items) return null;
  const leaves = itemLeaves(shape, items);
  // Depth-1 paths only under envelope objects (JSON:API attributes, Jira fields), never inside e.g. an icon.
  const WRAPPERS = new Set(['attributes', 'fields', 'data', 'record', 'values', 'node', 'item']);
  const wrapped = (l) => l.depth === 0 || (l.depth === 1 && WRAPPERS.has(norm(l.ptr.split('/')[1])));
  const top = (names, pred = () => true, depth = 0) => {
    for (let d = 0; d <= depth; d++) {
      for (const nm of names) {
        const hit = leaves.find((l) => l.depth === d && wrapped(l) && norm(l.name) === nm && pred(l));
        if (hit) return hit;
      }
    }
    return null;
  };
  const scalar = (l) => l.type !== 'object' && l.type !== 'array' && l.type !== 'boolean';
  const id = top(ID_NAMES, scalar);
  let updated = top(UPDATED_PREFERRED, scalar, 1);
  if (!updated) updated = leaves.find((l) => wrapped(l) && scalar(l) && isUpdatedName(l.name)) || null;
  // Atlassian v2 and similar keep the last edit as the latest version's timestamp (version.createdAt).
  if (!updated) {
    updated = leaves.find((l) => l.depth === 1 && scalar(l) && /^(version|latestversion|lastversion|history|lastmodified|lastupdated|modification)$/.test(norm(l.ptr.split('/')[1])) && /(createdat|when|date|time|at|timestamp)$/.test(norm(l.name))) || null;
  }
  const created = top(CREATED_NAMES, scalar, 1);
  const title = top(TITLE_NAMES, (l) => l.type === 'string', 1);
  const body = top(BODY_NAMES, (l) => l.type === 'string' || l.type === 'object', 1);
  let state = top(STATE_NAMES, (l) => l.type === 'string');
  if (!state) {
    const obj = top(STATE_NAMES, (l) => l.type === 'object');
    const name = obj && leaves.find((l) => l.ptr === `${obj.ptr}/name`);
    if (name) state = name;
  }
  const reference = top(REF_NAMES, (l) => l.type === 'string' || l.type === 'integer');
  let container = null;
  for (const nm of CONTAINER_NAMES) {
    const obj = leaves.find((l) => l.depth === 0 && l.type === 'object' && norm(l.name) === nm);
    const cid = obj && leaves.find((l) => l.ptr === `${obj.ptr}/id` || l.ptr === `${obj.ptr}/gid`);
    if (cid) {
      const cname = ['name', 'title', 'key'].map((k) => leaves.find((l) => l.ptr === `${obj.ptr}/${k}`)).find(Boolean);
      container = { kind: nm, id: cid.ptr, name: cname?.ptr ?? null };
      break;
    }
  }
  let author = null;
  for (const nm of AUTHOR_OBJECTS) {
    const obj = leaves.find((l) => l.depth <= 1 && l.type === 'object' && norm(l.name) === nm);
    if (!obj) continue;
    const aid = leaves.find((l) => l.ptr === `${obj.ptr}/id` || norm(l.name) === 'accountid' && l.ptr.startsWith(obj.ptr + '/') && l.depth === obj.depth + 1);
    const aname = PERSON_NAME.map((k) => leaves.find((l) => l.depth === obj.depth + 1 && l.ptr.startsWith(obj.ptr + '/') && norm(l.name) === k)).find(Boolean);
    if (aid && aname) {
      author = { object: obj.ptr, id: aid.ptr.slice(obj.ptr.length), name: aname.ptr.slice(obj.ptr.length) };
      break;
    }
  }
  return {
    id: id?.ptr ?? null,
    updated_at: updated?.ptr ?? null,
    updated_type: updated?.type ?? null,
    created_at: created?.ptr ?? null,
    title: title?.ptr ?? null,
    body: body?.ptr ?? null,
    body_type: body?.type ?? null,
    state: state?.ptr ?? null,
    reference: reference?.ptr ?? null,
    container,
    author,
  };
}

function streamCandidate(catalog, info, ev, change) {
  const tool = info.tool;
  const props = inputProps(tool);
  const shape = outputShape(tool);
  const items = info.graphql ? null : pickItems(shape);
  const gaps = [...ev.gaps];
  const tokenLike = (p) => p.type === 'string' && /token|opaque|cursor|next_page|next page|from the previous response|returned by/i.test(p.description);
  const cursor = props.find((p) => p.name !== ev.param && p.type !== 'integer' && p.type !== 'number' && (CURSOR_NAMES.has(p.n) || (NUMERIC_PAGING_NAMES.has(p.n) && tokenLike(p))));
  const numeric = props.find((p) => NUMERIC_PAGING_NAMES.has(p.n) && p.name !== ev.param);
  const pageSize = props.find((p) => PAGE_SIZE_NAMES.has(p.n) && (p.type === 'integer' || p.type === 'number' || p.type === 'string') && p.name !== ev.param);
  const fields = info.graphql ? null : inferFields(shape, items);
  let paging = null;
  if (!info.graphql) {
    if (!cursor) gaps.push(numeric ? `numeric_paging:${numeric.name}` : 'no_cursor_param');
    paging = pagingOf(shape, items, cursor?.name);
    if (!paging.has_more && !paging.is_last) gaps.push('no_more_flag');
    if (!paging.next) gaps.push('no_next_token');
    if (!items) gaps.push('no_items_path');
  }
  if (!info.prefixed) gaps.push('tool_prefix_mismatch');
  const filled = new Set([ev.param, ev.direction?.param, cursor?.name, pageSize?.name].filter(Boolean));
  const unfilled = props.filter((p) => p.required && !filled.has(p.name) && p.default === undefined);
  // A required search expression is not a container: a fixed match-all expression can fill it.
  const queryLike = (p) => QUERY_PARAM_NAMES.has(p.n) && p.type !== 'boolean';
  const blocking = unfilled.filter((p) => !p.enum.length && !queryLike(p));
  for (const p of unfilled.filter((p) => p.enum.length)) gaps.push(`required_choice:${p.name}`);
  for (const p of unfilled.filter((p) => !p.enum.length && queryLike(p))) gaps.push(`required_query_handwritten:${p.name}`);
  return {
    tool: tool.slug,
    change,
    param: ev.param,
    slot: change === 'filter' ? ev.slot : 'sort',
    text: ev.text,
    cursor_param: cursor?.name ?? null,
    page_size_param: pageSize?.name ?? null,
    gaps,
    container_params: blocking.map((p) => p.name),
    kind: ev.kind || 'sort',
    value: ev.value,
    direction: ev.direction || null,
    items_ptr: items?.ptr ?? null,
    paging,
    fields,
    item_ok: info.graphql || !!(fields?.id && fields?.updated_at),
    has_text: info.graphql || !!(fields?.body || fields?.title),
  };
}

const ENGINE_BLOCKING = /^(epoch_since|date_only_since|since_embedded_in_text|no_cursor_param|numeric_paging|no_more_flag|no_next_token|no_items_path|tool_prefix_mismatch|no_desc_direction)/;

function rankCandidate(c) {
  const blocking = c.gaps.filter((g) => ENGINE_BLOCKING.test(g)).length;
  const handwritten = c.gaps.filter((g) => /handwritten|unverified|required_choice|direction_default/.test(g)).length;
  const kindRank = { direct: 0, structured: 1, sort: 1, query: 2, graphql: 3 }[c.kind] ?? 2;
  const text = c.kind === 'graphql' || c.fields?.body ? 0 : c.fields?.title ? 3 : 6;
  return blocking * 100 + handwritten * 10 + text * 3 + kindRank * 2 + (c.change === 'filter' ? 0 : 1);
}

function authKind(catalog) {
  const modes = catalog.auth.map((a) => a.mode);
  if (modes.includes('OAUTH2')) return 'oauth2';
  if (modes.some((m) => m === 'API_KEY' || m === 'BEARER_TOKEN')) return 'api_key';
  if (modes.some((m) => m !== 'NO_AUTH')) return 'other';
  return 'none';
}

/** Pick one scope per all_of clause, preferring scopes many clauses share and never write-ish ones. */
function chooseScopes(tools, catalog) {
  const clauses = [];
  for (const t of tools) {
    const req = t.scope_requirements?.all_of;
    if (Array.isArray(req) && req.length) clauses.push(...req.map((c) => c.any_of || []).filter((c) => c.length));
    else if (t.scopes?.length) clauses.push(...t.scopes.map((s) => [s]));
  }
  const WRITEISH = /(write|manage|admin|create|delete|edit|modify|insert|update|send|post|full|\.all$|:all$|^all$|\*)/i;
  const freq = new Map();
  for (const c of clauses) for (const s of c) freq.set(s, (freq.get(s) || 0) + 1);
  const chosen = new Set();
  for (const c of clauses) {
    if (c.some((s) => chosen.has(s))) continue;
    const best = [...c].sort((a, b) => (WRITEISH.test(a) - WRITEISH.test(b)) || (freq.get(b) - freq.get(a)) || a.length - b.length)[0];
    chosen.add(best);
  }
  let source = 'tool scope_requirements';
  if (!chosen.size) {
    const oauth = catalog.auth.find((a) => a.mode === 'OAUTH2');
    for (const s of oauth?.default_scopes || []) if (/read|readonly|view|\bget\b/i.test(s) && !WRITEISH.test(s)) chosen.add(s);
    source = chosen.size ? 'toolkit default scopes filtered to read-like names' : 'none';
  }
  const scopes = [...chosen].sort();
  return { scopes, source, write: scopes.filter((s) => WRITEISH.test(s)) };
}

const WHOAMI_RX = /(WHO_AM_I|WHOAMI|CURRENT_USER|AUTHENTICATED_USER|GET_ME$|_ME$|GET_MY_|GET_SELF|ACCOUNT_INFO|GET_ACCOUNT$|ACCOUNT_DETAILS|GET_ORGANI[SZ]ATION$|GET_WORKSPACE$|GET_COMPANY$|USER_?INFO|GET_PROFILE$|TOKEN_INFO|AUTH_TEST|SERVER_INFO|GET_INSTANCE|GET_PORTAL|SITE_INFO|GET_CURRENT_|GET_MY_PROFILE|GET_ACCOUNT_INFORMATION)/;
const WORKSPACE_WORDS = ['workspace', 'organization', 'organisation', 'org', 'team', 'company', 'tenant', 'site', 'portal', 'account', 'hub', 'instance', 'cloud', 'domain', 'business', 'shop', 'store', 'subdomain', 'network', 'enterprise'];

/** A no-input read tool whose output carries an immutable workspace (or, failing that, user) id. */
function findNamespace(catalog, reads) {
  const options = [];
  for (const info of reads) {
    if (info.graphql || !WHOAMI_RX.test(info.tool.slug)) continue;
    const props = inputProps(info.tool);
    if (props.some((p) => p.required && p.default === undefined)) continue;
    const shape = outputShape(info.tool);
    for (const sc of shape.scalars) {
      if (sc.type === 'boolean' || sc.type === 'object') continue;
      const segs = sc.ptr.split('/').filter(Boolean).map(norm);
      const leaf = segs[segs.length - 1];
      const owner = segs.length > 1 ? segs[segs.length - 2] : '';
      const word = WORKSPACE_WORDS.find((w) => leaf === `${w}id` || leaf === `${w}uuid` || ((leaf === 'id' || leaf === 'gid' || leaf === 'uuid') && owner === w));
      // "account" is a workspace in some APIs (Harvest, Zendesk) and a person in others (Atlassian accountId).
      if (word) options.push({ tool: info.tool.slug, ptr: sc.ptr, kind: word === 'org' ? 'organization' : word, score: word === 'account' ? 5 : 0 });
      else if ((leaf === 'id' || leaf === 'gid' || leaf === 'accountid' || leaf === 'userid') && segs.length <= 2 && !/s$/.test(owner)) {
        options.push({ tool: info.tool.slug, ptr: sc.ptr, kind: 'user', score: 10 });
      }
    }
  }
  options.sort((a, b) => a.score - b.score || a.ptr.length - b.ptr.length);
  return options[0] || null;
}

function classifyCatalog(catalog) {
  const base = {
    slug: catalog.slug,
    name: catalog.name,
    auth_kind: authKind(catalog),
    managed_oauth: catalog.auth.some((a) => a.mode === 'OAUTH2' && a.managed),
    read_only_narrowable: false,
    tool_count: catalog.tools.length,
    read_tools: [],
    list_tools: [],
    evidence: [],
    rejected: [],
    read_scopes: [],
    scope_source: 'none',
    namespace: null,
    has_text: false,
    recipe_ready: false,
  };
  if (NATIVE.has(catalog.slug)) return { ...base, verdict: 'native_refused', why: 'native_connector' };
  if (catalog.error) return { ...base, verdict: 'no_list_tool', why: 'fetch_error' };
  const reads = readToolInfo(catalog);
  base.read_tools = reads.map((r) => r.tool.slug);
  const lists = reads.filter((r) => r.graphql || outputShape(r.tool).arrays.length);
  base.list_tools = lists.map((r) => r.tool.slug);
  if (!lists.length) return { ...base, verdict: 'no_list_tool', why: reads.length ? 'no_item_array_output' : 'no_read_tools' };
  const candidates = [];
  for (const info of lists) {
    const props = inputProps(info.tool);
    for (const ev of filterEvidence(info.tool, props, info.graphql)) candidates.push(streamCandidate(catalog, info, ev, 'filter'));
    if (!info.graphql) for (const ev of sortEvidence(props)) candidates.push(streamCandidate(catalog, info, ev, 'sort_stop'));
  }
  const rejectWhy = (c) => {
    if (c.container_params.length) {
      const scoped = c.container_params.some((p) => /(id|ids|key|gid|url)$/i.test(p) || /^id[A-Z_]/.test(p));
      return `${scoped ? 'container_scoped' : 'requires_input'}:${c.container_params.join(',')}`;
    }
    if (c.gaps.includes('no_desc_direction')) return 'no_desc_direction';
    if (!c.item_ok) return 'item_lacks_id_or_updated';
    return null;
  };
  const usable = candidates.filter((c) => !rejectWhy(c));
  base.rejected = candidates.filter((c) => !usable.includes(c)).map((c) => ({ tool: c.tool, change: c.change, param: c.param, why: rejectWhy(c), text: c.text }));
  if (!usable.length) {
    const whys = base.rejected.map((x) => x.why.split(':')[0]);
    const why = ['container_scoped', 'requires_input', 'item_lacks_id_or_updated', 'no_desc_direction'].find((w) => whys.includes(w)) || 'no_change_param';
    return { ...base, verdict: 'full_scan_only', why };
  }
  usable.sort((a, b) => rankCandidate(a) - rankCandidate(b));
  const best = usable[0];
  const toolBySlug = new Map(catalog.tools.map((t) => [t.slug, t]));
  const ns = findNamespace(catalog, reads);
  const sc = chooseScopes([toolBySlug.get(best.tool), ns && toolBySlug.get(ns.tool)].filter(Boolean), catalog);
  const engineReady = !best.gaps.some((g) => ENGINE_BLOCKING.test(g) || /handwritten/.test(g));
  const row = {
    ...base,
    verdict: best.change,
    why: engineReady ? 'ok' : 'engine_gap',
    evidence: usable,
    namespace: ns,
    read_scopes: sc.scopes,
    scope_source: sc.source,
    read_only_narrowable: base.auth_kind === 'oauth2' && sc.scopes.length > 0 && !sc.write.length,
  };
  row.has_text = !!(best.kind === 'graphql' || best.fields?.body || best.fields?.title);
  row.recipe_ready = engineReady && row.read_only_narrowable && row.managed_oauth && !!ns && row.has_text;
  return row;
}

function loadCatalogs() {
  const dir = path.join(CACHE, 'toolkits');
  if (!fs.existsSync(dir)) die(`no cache at ${dir}; run fetch first`);
  return fs.readdirSync(dir).filter((f) => f.endsWith('.json')).sort()
    .map((f) => JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')));
}

const VERDICTS = ['native_refused', 'no_list_tool', 'full_scan_only', 'filter', 'sort_stop'];
const csvCell = (v) => {
  const s = Array.isArray(v) ? v.join(' ') : String(v ?? '');
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};
const mdCell = (s) => oneLine(s).replace(/\|/g, '\\|');

/** DRAFT_OK / DRAFT_FAIL lines printed by recipe::tests::validate_draft_recipes. */
function readValidation(args) {
  const i = args.indexOf('--validation');
  if (i < 0) return null;
  const out = new Map();
  for (const line of fs.readFileSync(args[i + 1], 'utf8').split('\n')) {
    const ok = line.match(/^DRAFT_OK ([a-z0-9_]+)$/);
    const fail = line.match(/^DRAFT_FAIL ([a-z0-9_]+): (?:\1: )?(.*)$/);
    if (ok) out.set(ok[1], 'ok');
    if (fail) out.set(fail[1], fail[2].replace(/ at line \d+ column \d+$/, ''));
  }
  return out;
}

async function cmdClassify(args) {
  const catalogs = loadCatalogs();
  const validation = readValidation(args);
  const rows = catalogs.map(classifyCatalog);
  for (const r of rows) r.draft_validation = validation?.get(r.slug) ?? null;
  writeAtomic(path.join(CACHE, 'classification.json'), JSON.stringify(rows, null, 1));
  const outDir = path.join(REPO, 'docs/connectors');
  const header = ['slug', 'name', 'verdict', 'why', 'auth_kind', 'managed_oauth', 'read_only_narrowable', 'recipe_ready', 'tool_count', 'read_tool_count', 'list_tool_count', 'change_tool', 'change_param', 'slot', 'gaps', 'read_scopes', 'namespace_tool', 'draft_validation', 'rejected'];
  const csv = [header.join(',')];
  for (const r of rows) {
    const e = r.evidence[0];
    const rejected = [...new Set(r.rejected.map((x) => `${x.tool}.${x.param}:${x.why}`))].slice(0, 5);
    csv.push([r.slug, r.name, r.verdict, r.why, r.auth_kind, r.managed_oauth, r.read_only_narrowable, r.recipe_ready, r.tool_count, r.read_tools.length, r.list_tools.length, e?.tool, e?.param, e?.slot, e?.gaps, r.read_scopes, r.namespace?.tool, r.draft_validation, rejected].map(csvCell).join(','));
  }
  writeAtomic(path.join(outDir, 'composio-eligibility.csv'), csv.join('\n') + '\n');
  writeAtomic(path.join(outDir, 'composio-eligibility.md'), renderEligibilityMd(rows, args));
  const counts = Object.fromEntries(VERDICTS.map((v) => [v, rows.filter((r) => r.verdict === v).length]));
  console.log(JSON.stringify({ toolkits: rows.length, ...counts }));
}

const NAMED = ['jira', 'linear', 'salesforce', 'intercom', 'notion', 'zendesk', 'airtable', 'asana', 'monday', 'figma', 'clickup', 'confluence', 'hubspot', 'trello', 'dropbox'];

function renderEligibilityMd(rows) {
  const L = [];
  const count = (pred) => rows.filter(pred).length;
  L.push('# Composio toolkit eligibility for recipe capture');
  L.push('');
  L.push(`Generated by \`node scripts/composio/draft_recipe.mjs classify\` from Composio tool metadata (latest toolkit versions, deprecated tools excluded) fetched ${new Date().toISOString().slice(0, 10)}. All ${rows.length} rows: [composio-eligibility.csv](composio-eligibility.csv).`);
  if (rows.some((r) => r.draft_validation !== null)) L.push('', 'The `draft loads` column is recipe.rs validation of `scripts/composio/drafts/<slug>.json`: `COMPOSIO_DRAFT_RECIPES=scripts/composio/drafts cargo test --lib validate_draft_recipes -- --ignored --nocapture > log`, then `classify --validation log`. It reports the first error only; each `<slug>.review.md` lists every blocker the script found.');
  L.push('');
  L.push('A toolkit is eligible when a workspace-wide read list tool either filters by update time (`filter`) or sorts by update time newest-first (`sort_stop`). Read tools are Composio `readOnlyHint` tools whose slug names no write verb from `recipe.rs` and that are not `destructiveHint`, plus GraphQL `query_or_mutation` tools (fixed query only). A change parameter on a tool that also requires a container id (project, base, database) is `full_scan_only` / `container_scoped`: the engine has no per-container fan-out.');
  L.push('');
  L.push('`why` = `engine_gap` means the provider can find changes but the engine or recipe format cannot express the call as-is (see gaps). Gap codes: `epoch_since` (filter takes a Unix time; the engine binds RFC 3339), `date_only_since` (day granularity fails the self-check), `since_embedded_in_text` (only `{{since_minutes}}` may be spliced into text), `numeric_paging`/`no_cursor_param` (streams need a string cursor), `no_more_flag`/`no_next_token` (no `has_more`/`is_last`/next path in the output schema), `tool_prefix_mismatch`, `*_handwritten` (the call needs a hand-written query/filter), `since_format_unverified`, `direction_default_desc`, `required_choice:<param>`.');
  L.push('');
  L.push('## Counts');
  L.push('');
  L.push('| verdict | toolkits | oauth2 | api_key | other | none | managed OAuth | read-only narrowable |');
  L.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const v of VERDICTS) {
    const rs = rows.filter((r) => r.verdict === v);
    L.push(`| \`${v}\` | ${rs.length} | ${rs.filter((r) => r.auth_kind === 'oauth2').length} | ${rs.filter((r) => r.auth_kind === 'api_key').length} | ${rs.filter((r) => r.auth_kind === 'other').length} | ${rs.filter((r) => r.auth_kind === 'none').length} | ${rs.filter((r) => r.managed_oauth).length} | ${rs.filter((r) => r.read_only_narrowable).length} |`);
  }
  L.push(`| **total** | ${rows.length} | ${count((r) => r.auth_kind === 'oauth2')} | ${count((r) => r.auth_kind === 'api_key')} | ${count((r) => r.auth_kind === 'other')} | ${count((r) => r.auth_kind === 'none')} | ${count((r) => r.managed_oauth)} | ${count((r) => r.read_only_narrowable)} |`);
  L.push('');
  const fsWhy = {};
  for (const r of rows.filter((r) => r.verdict === 'full_scan_only' || r.verdict === 'no_list_tool')) fsWhy[`${r.verdict}/${r.why}`] = (fsWhy[`${r.verdict}/${r.why}`] || 0) + 1;
  L.push('Ineligible breakdown: ' + Object.entries(fsWhy).sort().map(([k, v]) => `\`${k}\` ${v}`).join(', ') + '.');
  L.push('');
  const eligible = rows.filter((r) => r.verdict === 'filter' || r.verdict === 'sort_stop');
  const e = (pred) => eligible.filter(pred).length;
  const ineligibleWith = (prefix) => rows.filter((r) => !eligible.includes(r) && r.rejected.some((x) => x.why.startsWith(prefix))).length;
  L.push('## How the match was tightened');
  L.push('');
  L.push('Toolkits that had a change parameter but were dropped by each rule (a toolkit can hit several):');
  L.push('');
  L.push(`- the tool also requires a container id or other free input (not workspace-wide): ${ineligibleWith('container_scoped') + ineligibleWith('requires_input')} toolkits;`);
  L.push(`- the item schema has no id or no update-time field (\`fields.id\` and \`fields.updated_at\` are mandatory, and the self-check reads \`updated_at\`): ${ineligibleWith('item_lacks')};`);
  L.push(`- the sort cannot be made newest-first: ${ineligibleWith('no_desc')};`);
  L.push('- conditional GETs (`If-Modified-Since` answered with 304) are not filters and are not counted.');
  L.push('');
  L.push('## Tiers among eligible toolkits');
  L.push('');
  L.push('| tier | toolkits |');
  L.push('| --- | ---: |');
  L.push(`| eligible (\`filter\` or \`sort_stop\`) | ${eligible.length} |`);
  L.push(`| … OAuth 2 offered | ${e((r) => r.auth_kind === 'oauth2')} |`);
  L.push(`| … Composio-managed OAuth | ${e((r) => r.managed_oauth)} |`);
  L.push(`| … read-only scopes known (narrowable) | ${e((r) => r.read_only_narrowable)} |`);
  L.push(`| … best stream needs no engine change (\`why\` = ok) | ${e((r) => r.why === 'ok')} |`);
  L.push(`| … workspace/user id tool found for the namespace | ${e((r) => !!r.namespace)} |`);
  if (eligible.some((r) => r.draft_validation !== null)) L.push(`| … draft passes recipe.rs validation (\`validate_draft_recipes\`) | ${e((r) => r.draft_validation === 'ok')} (${eligible.filter((r) => r.draft_validation === 'ok').map((r) => `\`${r.slug}\``).join(', ')}) |`);
  L.push(`| recipe-ready: managed OAuth, narrowable, engine-ready, namespace and a title or body path | ${e((r) => r.recipe_ready)} (${eligible.filter((r) => r.recipe_ready).map((r) => `\`${r.slug}\``).join(', ') || 'none'}) |`);
  L.push('');
  const blockingGap = (g) => ENGINE_BLOCKING.test(g) || /handwritten/.test(g);
  const unlocked = (drop) => eligible.filter((r) => r.evidence.some((c) => c.gaps.filter((g) => blockingGap(g) && !drop.some((d) => g.startsWith(d))).length === 0)).length;
  L.push('Engine changes that would unlock more eligible toolkits (any qualifying stream left with no blocking gap; cumulative):');
  L.push('');
  L.push(`- today: ${unlocked([])};`);
  L.push(`- treat a present, non-empty next token as "more" when there is no \`has_more\`/\`is_last\`: ${unlocked(['no_more_flag'])};`);
  L.push(`- plus numeric page/offset paging for streams: ${unlocked(['no_more_flag', 'numeric_paging', 'no_cursor_param'])};`);
  L.push(`- plus an epoch-time \`since\` slot: ${unlocked(['no_more_flag', 'numeric_paging', 'no_cursor_param', 'epoch_since'])}.`);
  const nsRx = /^container_scoped:(team_?id|workspace_?id|workspace_?gid|organization_?id|org_?id|account_?id|site_?id)$/i;
  const nsUnlock = rows.filter((r) => !eligible.includes(r) && r.rejected.some((x) => nsRx.test(x.why)));
  L.push(`- separately, a \`{{namespace}}\` slot (the bound workspace id as a call argument) would make ${nsUnlock.length} container-scoped toolkits workspace-wide: ${nsUnlock.map((r) => `\`${r.slug}\``).join(', ')}.`);
  L.push('');
  const section = (title, list) => {
    L.push(`## ${title} (${list.length})`);
    L.push('');
    L.push('| toolkit | verdict | auth | managed OAuth | narrowable | draft loads | tool | param / slot | gaps | read scopes | schema evidence |');
    L.push('| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |');
    for (const r of list) {
      const e = r.evidence[0];
      const loads = r.draft_validation === null ? 'not run' : r.draft_validation === 'ok' ? 'yes' : `no: ${mdCell(r.draft_validation)}`;
      L.push(`| \`${r.slug}\` | ${r.verdict} | ${r.auth_kind} | ${r.managed_oauth ? 'yes' : 'no'} | ${r.read_only_narrowable ? 'yes' : 'no'} | ${loads} | \`${e.tool}\` | \`${e.param}\` / ${e.slot} | ${e.gaps.join(' ') || '-'} | ${r.read_scopes.join(' ') || '-'} | ${mdCell(clip(e.text, 160))} |`);
    }
    L.push('');
  };
  const byTier = (a, b) => (b.managed_oauth - a.managed_oauth) || (b.auth_kind === 'oauth2') - (a.auth_kind === 'oauth2') || a.slug.localeCompare(b.slug);
  section('Eligible, engine-ready', eligible.filter((r) => r.why === 'ok').sort(byTier));
  section('Eligible, engine gap', eligible.filter((r) => r.why !== 'ok').sort(byTier));
  L.push('## Named candidates');
  L.push('');
  L.push('| toolkit | verdict | why | best evidence or nearest miss |');
  L.push('| --- | --- | --- | --- |');
  for (const slug of NAMED) {
    const r = rows.find((x) => x.slug === slug);
    if (!r) { L.push(`| \`${slug}\` | not in catalog | | |`); continue; }
    const e = r.evidence[0];
    const miss = r.rejected.map((x) => `\`${x.tool}\`.\`${x.param}\` ${x.why}`).slice(0, 3).join('; ');
    const detail = e ? `\`${e.tool}\`.\`${e.param}\` (${e.slot}${e.gaps.length ? '; ' + e.gaps.join(' ') : ''}): ${mdCell(clip(e.text, 140))}` : (miss || `${r.list_tools.length} list tools, none with an update-time filter or sort`);
    L.push(`| \`${slug}\` | ${r.verdict} | ${r.why} | ${detail} |`);
  }
  L.push('');
  return L.join('\n');
}

// ---------------------------------------------------------------------------
// draft
// ---------------------------------------------------------------------------

const GAP_TEXT = {
  epoch_since: 'the filter takes a Unix time; the engine binds `{{since}}` as an RFC 3339 string',
  date_only_since: 'the filter has day granularity; items updated earlier that day would fail the self-check (`updated_at >= since`)',
  since_format_unverified: 'the schema does not say which time format the filter accepts; `{{since}}` renders RFC 3339 UTC with milliseconds',
  since_embedded_in_text: 'the watermark must be written into query text; only `{{since_minutes}}` may be spliced into text',
  query_text_handwritten: 'the query text supports relative minutes but the query itself must be written by hand',
  filter_object_handwritten: 'the filter object must be written by hand (no schema example names the update-time field)',
  sort_object_handwritten: 'the sort object must be written by hand (no schema example sorts by update time descending)',
  graphql_query_handwritten: 'a fixed GraphQL `query` document must be written by hand (introspect the provider schema)',
  no_cursor_param: 'the tool has no continuation-token input; streams must pass `{{cursor}}`',
  numeric_paging: 'the tool pages by number/offset; streams page by an opaque string token',
  no_more_flag: 'the output schema has no `has_more`/`is_last` boolean',
  no_next_token: 'the output schema has no next-page token path',
  no_items_path: 'no item array in the output schema',
  tool_prefix_mismatch: 'the tool slug does not start with the toolkit prefix; recipe.rs rejects it',
  direction_default_desc: 'newest-first relies on the provider default, not an explicit argument',
  required_choice: 'a required enum argument must be chosen',
  required_query_handwritten: 'a required search expression must be fixed by hand to match everything',
};
const gapText = (g) => {
  const [code, arg] = g.split(':');
  return `\`${g}\`: ${GAP_TEXT[code] || code}${arg && code.startsWith('required_') ? ` (\`${arg}\`)` : ''}`;
};

const snake = (s) => String(s).replace(/([a-z0-9])([A-Z])/g, '$1_$2').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
const singular = (w) => (/ies$/.test(w) ? w.slice(0, -3) + 'y' : /(ss|us)$/.test(w) ? w : /(x|ch|sh|ss)es$/.test(w) ? w.slice(0, -2) : /s$/.test(w) ? w.slice(0, -1) : w);
const TOOL_STOP = new Set(['LIST', 'SEARCH', 'GET', 'ALL', 'FIND', 'FETCH', 'QUERY', 'RETRIEVE', 'V1', 'V2', 'V21', 'V3', 'FOR', 'BY', 'THE', 'A', 'AN', 'OF', 'IN', 'WITH', 'USING', 'PAGINATED', 'MULTIPLE', 'OLD', 'NEW', 'ACCESS', 'USER', 'SPECIFIC', 'ACCOUNT', 'FROM', 'AND']);

function kindOf(c, catalog) {
  const last = (c.items_ptr || '').split('/').filter(Boolean).pop() || '';
  let base = last && !ITEM_GENERIC.has(norm(last)) ? last : '';
  if (!base) {
    const prefixWords = new Set(catalog.slug.toUpperCase().split('_'));
    const words = c.tool.slice(catalog.slug.length + 1).split('_').filter((w) => !TOOL_STOP.has(w) && !prefixWords.has(w) && !/^\d+$/.test(w));
    base = words.pop() || 'item';
  }
  return singular(snake(base)) || 'item';
}

const streamKind = (c, catalog) => (c.kind === 'graphql' ? 'item' : kindOf(c, catalog));

// One stream per content kind: two lists of the same entity would capture it under two identities.
function pickStreams(row, catalog) {
  const ready = row.evidence.filter((c) => !c.gaps.some((g) => ENGINE_BLOCKING.test(g)) && (c.kind === 'graphql' || c.fields?.body || c.fields?.title));
  const pool = ready.length ? ready : row.evidence.slice(0, 1);
  const out = [];
  for (const c of pool) {
    if (out.length === 3) break;
    if (out.some((o) => o.tool === c.tool || streamKind(o, catalog) === streamKind(c, catalog))) continue;
    out.push(c);
  }
  return out;
}

function buildStream(c, catalog, used) {
  const review = [];
  const blockers = c.gaps.filter((g) => ENGINE_BLOCKING.test(g) || /handwritten/.test(g)).map(gapText);
  review.push(...c.gaps.filter((g) => !ENGINE_BLOCKING.test(g) && !/handwritten/.test(g)).map(gapText));
  const kind = streamKind(c, catalog);
  if (c.kind === 'graphql') review.push('stream kind `item` is a placeholder: name it after the entity the hand-written query returns');
  used.add(kind);
  const args = {};
  if (c.change === 'filter') {
    if (c.value !== null && c.value !== undefined) args[c.param] = c.value;
  } else {
    if (c.value !== null && c.value !== undefined) args[c.param] = c.value;
    if (c.direction) args[c.direction.param] = c.direction.value;
  }
  if (c.page_size_param) args[c.page_size_param] = '{{page_size}}';
  if (c.cursor_param) args[c.cursor_param] = '{{cursor}}';
  const stream = { kind, change: c.change, call: { tool: c.tool, args } };
  if (c.items_ptr !== null) stream.items = c.items_ptr;
  if (c.paging) {
    const paging = {};
    if (c.paging.has_more) paging.has_more = c.paging.has_more;
    else if (c.paging.is_last) paging.is_last = c.paging.is_last;
    if (c.paging.next) paging.next = c.paging.next;
    stream.paging = paging;
  }
  const f = c.fields;
  if (f) {
    const fields = {};
    if (f.id) fields.id = f.id;
    if (f.updated_at) fields.updated_at = f.created_at && f.created_at !== f.updated_at ? [f.updated_at, f.created_at] : f.updated_at;
    if (f.container) fields.container = { kind: f.container.kind, id: f.container.id, ...(f.container.name ? { name: f.container.name } : {}) };
    if (f.title) fields.title = f.title;
    if (f.body) fields.body = f.body;
    if (f.state) fields.state = f.state;
    if (f.reference) fields.reference = f.reference;
    if (f.author) fields.author = f.author;
    stream.fields = fields;
    if (!f.title && !f.body) review.push('no title or body path inferred: the captured atom would carry only provenance; add a `children` list or richer fields');
    if (f.body_type === 'object') review.push(`body \`${f.body}\` is an object; the engine renders strings and Atlassian documents only`);
    if (f.updated_type && f.updated_type !== 'string') review.push(`updated_at \`${f.updated_at}\` is ${f.updated_type}; the engine compares RFC 3339 strings`);
    review.push(`field paths inferred by name from the output schema: ${Object.entries(fields).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(', ')}`);
  } else {
    review.push('no field paths: write `items`, `paging` and `fields` from the hand-written query');
  }
  return { stream, review, blockers };
}

function draftToolkit(slug) {
  const file = catalogPath(slug);
  if (!fs.existsSync(file)) throw new Error(`${slug}: not cached; run fetch ${slug}`);
  const catalog = JSON.parse(fs.readFileSync(file, 'utf8'));
  const row = classifyCatalog(catalog);
  if (row.verdict !== 'filter' && row.verdict !== 'sort_stop') throw new Error(`${slug}: verdict ${row.verdict} (${row.why}); nothing to draft`);
  const toolBySlug = new Map(catalog.tools.map((t) => [t.slug, t]));
  const chosen = pickStreams(row, catalog);
  const used = new Set();
  const built = chosen.map((c) => ({ c, ...buildStream(c, catalog, used) }));
  const blockers = [];
  const review = [];
  const readTools = new Set(built.map((b) => b.c.tool));
  let namespace;
  if (row.namespace) {
    readTools.add(row.namespace.tool);
    namespace = { kind: row.namespace.kind, source: { lookup: { call: { tool: row.namespace.tool, args: {} }, id: [row.namespace.ptr] } } };
    review.push(`namespace: \`${row.namespace.tool}\` \`${row.namespace.ptr}\` (kind \`${row.namespace.kind}\`)${row.namespace.kind === 'user' ? ' is a user id, not a workspace id; find a workspace-level id if the provider has one' : '; confirm it is immutable and workspace-level'}`);
  } else {
    blockers.push('no namespace: no no-input "who am I"/workspace tool with an id in its output; recipe.rs requires `namespace`');
  }
  const sc = chooseScopes([...readTools].map((t) => toolBySlug.get(t)), catalog);
  if (!sc.scopes.length) blockers.push(row.auth_kind === 'oauth2' ? 'no read scope known: Composio lists no scopes for these tools or the toolkit (provider may not use OAuth scopes); recipe.rs requires at least one' : `auth is ${row.auth_kind}: a credential that cannot be narrowed to read-only scopes; recipe.rs requires at least one scope`);
  if (sc.write.length) blockers.push(`Composio says these tools need write-ish scopes: ${sc.write.join(', ')}`);
  if (!row.managed_oauth) blockers.push('no Composio-managed OAuth: a narrowed auth config needs our own OAuth app (`use_custom_auth`)');
  if (sc.source !== 'tool scope_requirements' && sc.scopes.length) review.push(`scopes come from ${sc.source}, not from the tools`);
  if (built.length > 1) review.push(`streams ${built.map((b) => `\`${b.stream.kind}\``).join(', ')} come from different list tools; drop any that list a subset of another (e.g. one ticket type), or the same item is captured under two identities`);
  for (const b of built) {
    blockers.push(...b.blockers.map((x) => `stream \`${b.stream.kind}\`: ${x}`));
    review.push(...b.review.map((x) => `stream \`${b.stream.kind}\`: ${x}`));
  }
  const recipe = {
    toolkit: catalog.slug,
    label: catalog.name,
    version: catalog.version,
    scopes: { required: sc.scopes },
    read_tools: [...readTools].sort(),
    ...(namespace ? { namespace } : {}),
    streams: built.map((b) => b.stream),
    limits: { history: 'nothing from before you connect is imported' },
    references: [`https://docs.composio.dev/toolkits/${catalog.slug}.md`],
  };
  return { recipe, row, catalog, built, blockers, review, sc };
}

function renderReview({ recipe, row, catalog, built, blockers, review, sc }) {
  const toolBySlug = new Map(catalog.tools.map((t) => [t.slug, t]));
  const L = [];
  L.push(`# ${catalog.name} (\`${catalog.slug}\`) recipe draft`);
  L.push('');
  L.push(`Verdict \`${row.verdict}\`; auth \`${row.auth_kind}\`; Composio-managed OAuth ${row.managed_oauth ? 'yes' : 'no'}; read-only narrowable ${row.read_only_narrowable ? 'yes' : 'no'}; toolkit version \`${catalog.version}\`; ${catalog.tools.length} tools, ${row.read_tools.length} read-tool candidates. Generated by \`node scripts/composio/draft_recipe.mjs draft ${catalog.slug}\` from metadata fetched ${catalog.fetched_at.slice(0, 10)}. Validate with \`COMPOSIO_DRAFT_RECIPES=scripts/composio/drafts/${catalog.slug}.json cargo test --lib validate_draft_recipes -- --ignored --nocapture\`.`);
  L.push('');
  L.push('## Blockers');
  L.push('');
  L.push(blockers.length ? blockers.map((b) => `- ${b}`).join('\n') : '- none found by the script');
  L.push('');
  L.push('## Needs review');
  L.push('');
  L.push([...review, 'run one read-only dry run against a real grant before enabling (see `live_linear_dry_run`)'].map((b) => `- ${b}`).join('\n'));
  L.push('');
  L.push('## Auth config (not executed)');
  L.push('');
  const body = JSON.stringify({ toolkit: { slug: catalog.slug }, auth_config: { type: 'use_composio_managed_auth', credentials: { scopes: sc.scopes.join(',') } } });
  L.push('```sh');
  L.push(`curl -sS -X POST ${BASE}/api/v3/auth_configs \\`);
  L.push('  -H "x-api-key: $COMPOSIO_API_KEY" -H "content-type: application/json" \\');
  L.push(`  -d '${body}'`);
  L.push('```');
  L.push('');
  L.push(`Store the returned id as \`COMPOSIO_${catalog.slug.toUpperCase()}_AUTH_CONFIG_ID\`.${row.managed_oauth ? '' : ' Composio has no managed OAuth for this toolkit, so this call will fail as written.'}`);
  L.push('');
  L.push('## read_tools');
  L.push('');
  L.push('| tool | readOnlyHint | destructiveHint | Composio scopes | used for |');
  L.push('| --- | --- | --- | --- | --- |');
  for (const t of recipe.read_tools) {
    const tool = toolBySlug.get(t);
    const use = recipe.namespace?.source?.lookup?.call?.tool === t ? 'namespace' : recipe.streams.filter((s) => s.call.tool === t).map((s) => `stream \`${s.kind}\``).join(', ');
    L.push(`| \`${t}\` | ${tool.hints.read_only ? 'yes' : 'no'} | ${tool.hints.destructive ? 'yes' : 'no'} | ${tool.scopes.join(' ') || '-'} | ${use} |`);
  }
  L.push('');
  L.push('## Scopes');
  L.push('');
  L.push(`Source: ${sc.source}. Toolkit default scope list: ${catalog.auth.find((a) => a.mode === 'OAUTH2')?.default_scopes?.join(', ') || '(none exposed)'}.`);
  L.push('');
  L.push('| scope | required by |');
  L.push('| --- | --- |');
  for (const s of sc.scopes) {
    const by = recipe.read_tools.filter((t) => {
      const tool = toolBySlug.get(t);
      return (tool.scope_requirements?.all_of || []).some((c) => (c.any_of || []).includes(s)) || tool.scopes.includes(s);
    });
    L.push(`| \`${s}\` | ${by.map((t) => `\`${t}\``).join(', ') || '(toolkit default list)'} |`);
  }
  if (!sc.scopes.length) L.push('| (none) | |');
  L.push('');
  L.push('## Change evidence');
  L.push('');
  L.push('| stream | tool | param | slot | schema text |');
  L.push('| --- | --- | --- | --- | --- |');
  for (const b of built) L.push(`| \`${b.stream.kind}\` | \`${b.c.tool}\` | \`${b.c.param}\` | ${b.c.slot} | ${mdCell(b.c.text)} |`);
  L.push('');
  const others = row.evidence.filter((c) => !built.some((b) => b.c === c));
  if (others.length) {
    L.push('## Other qualifying tools (not drafted)');
    L.push('');
    L.push('| tool | change | param | gaps |');
    L.push('| --- | --- | --- | --- |');
    for (const c of others.slice(0, 25)) L.push(`| \`${c.tool}\` | ${c.change} | \`${c.param}\` | ${c.gaps.join(' ') || '-'} |`);
    if (others.length > 25) L.push(`| … ${others.length - 25} more | | | |`);
    L.push('');
  }
  return L.join('\n');
}

async function cmdDraft(args) {
  let slugs = args.filter((a) => !a.startsWith('--'));
  if (args.includes('--eligible')) {
    slugs = loadCatalogs().map(classifyCatalog).filter((r) => r.verdict === 'filter' || r.verdict === 'sort_stop').map((r) => r.slug);
  }
  if (!slugs.length) die('draft: give toolkit slugs or --eligible');
  const outDir = process.env.COMPOSIO_DRAFTS_DIR || DRAFTS;
  let failed = 0;
  for (const slug of slugs) {
    try {
      const d = draftToolkit(slug);
      writeAtomic(path.join(outDir, `${slug}.json`), JSON.stringify(d.recipe, null, 2) + '\n');
      writeAtomic(path.join(outDir, `${slug}.review.md`), renderReview(d));
      console.log(`${slug}: ${d.recipe.streams.length} stream(s), ${d.blockers.length} blocker(s)`);
    } catch (e) {
      failed++;
      console.error(e.message);
    }
  }
  if (failed && slugs.length === 1) process.exit(1);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

const [cmd, ...rest] = process.argv.slice(2);
const commands = { fetch: cmdFetch, classify: cmdClassify, draft: cmdDraft };
if (!commands[cmd]) die('usage: draft_recipe.mjs fetch|classify|draft ...');
await commands[cmd](rest);
