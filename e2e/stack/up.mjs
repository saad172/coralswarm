#!/usr/bin/env node
// Boot the e2e stack: pgvector Postgres + S3 (R2 stand-in) in Docker, the
// provider fakes, the debug `--features e2e` backend and the standalone web
// build. Detaches and records pids in e2e/.stack/state.json; `down.mjs`
// tears it down. Idempotent: a second run restarts the processes and resets
// the databases.
//
// Databases (the template rule, e2e/README.md):
//   1. The CONTROL tree is applied from zero by the real `migrate_control`
//      binary into `cs_e2e_tpl_<hash of the migration files>` — once per
//      migration set; a later boot with the same files reuses it.
//   2. Every boot clones it: `CREATE DATABASE <E2E_DB_NAME|w$SHARD> TEMPLATE`.
//   3. `migrate_control --check` and `migrate_tenants` then run against the
//      clone exactly as `backend/release.sh` runs them on a deploy.
//   Tenant databases are created by the backend's own provisioning job
//   (`TENANT_PROVISIONER=local`), which applies the TENANT tree from zero.
//
// Usage: node stack/up.mjs [--no-web] [--no-backend] [--no-db-assert]

import { spawn, spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { assertDatabases } from "./assert-db.mjs";
import {
  LOCAL_API_BASE, BINS, BUCKET, CONTAINERS, CONTROL_DB, E2E_DIR, FAKE, IMAGES, PORTS, REPO, STATE_DIR, LOCAL_WEB_BASE, WEB_DIR,
  REMOTE, TARGET, backendEnv, pgUrl, requireClerk,
} from "./config.mjs";

const args = new Set(process.argv.slice(2));
const t0 = Date.now();
const timings = {};
const log = (msg) => console.log(`[e2e up +${((Date.now() - t0) / 1000).toFixed(1)}s] ${msg}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function sh(cmd, argv, opts = {}) {
  const r = spawnSync(cmd, argv, { encoding: "utf8", ...opts });
  if (r.status !== 0 && !opts.allowFail) {
    throw new Error(`${cmd} ${argv.join(" ")} exited ${r.status}\n${r.stdout ?? ""}${r.stderr ?? ""}`);
  }
  return r;
}

async function step(name, fn) {
  const s = Date.now();
  const out = await fn();
  timings[name] = Date.now() - s;
  log(`${name} (${(timings[name] / 1000).toFixed(1)}s)`);
  return out;
}

function psql(sql, db = "postgres") {
  return sh("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-U", "postgres", "-d", db, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1"], {
    input: sql,
  }).stdout.trim();
}

async function waitFor(what, probe, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    try {
      if (await probe()) return;
    } catch (e) {
      last = e;
    }
    await sleep(250);
  }
  throw new Error(`e2e up: timed out waiting for ${what}${last ? `: ${last.message}` : ""}`);
}

const httpOk = (url) => async () => (await fetch(url)).ok;

function containerRunning(name) {
  const r = sh("docker", ["inspect", "-f", "{{.State.Running}}", name], { allowFail: true });
  return r.status === 0 && r.stdout.trim() === "true";
}

function startContainer(name, argv) {
  if (containerRunning(name)) return false;
  sh("docker", ["rm", "-f", name], { allowFail: true });
  sh("docker", ["run", "-d", "--name", name, ...argv]);
  return true;
}

function readState() {
  try {
    return JSON.parse(fs.readFileSync(path.join(STATE_DIR, "state.json"), "utf8"));
  } catch {
    return { pids: {} };
  }
}

export function killPid(pid) {
  if (!pid) return;
  if (process.platform === "win32") spawnSync("taskkill", ["/PID", String(pid), "/T", "/F"], { stdio: "ignore" });
  else {
    try {
      process.kill(-pid, "SIGTERM");
    } catch {
      try {
        process.kill(pid, "SIGTERM");
      } catch {}
    }
  }
}

// Every pid is recorded the moment its process starts, so a boot that fails
// half-way leaves nothing running that the next `up`/`down` cannot find.
const live = { pids: {} };
function recordState(extra = {}) {
  Object.assign(live, extra);
  fs.writeFileSync(path.join(STATE_DIR, "state.json"), JSON.stringify(live, null, 2));
}

function launch(name, cmd, argv, env, cwd) {
  const logPath = path.join(STATE_DIR, `${name}.log`);
  const fd = fs.openSync(logPath, "w");
  const child = spawn(cmd, argv, {
    cwd,
    env: { ...process.env, ...env },
    stdio: ["ignore", fd, fd],
    detached: true,
    windowsHide: true,
  });
  child.on("error", (e) => fs.appendFileSync(logPath, `\nspawn error: ${e.message}\n`));
  child.unref();
  live.pids[name] = child.pid;
  recordState();
  return { pid: child.pid, log: logPath };
}

function migrationHash() {
  const h = crypto.createHash("sha256");
  const dir = path.join(REPO, "backend", "migrations_control");
  for (const f of fs.readdirSync(dir).sort()) {
    h.update(f);
    h.update(fs.readFileSync(path.join(dir, f)));
  }
  return h.digest("hex").slice(0, 12);
}

async function main() {
  if (REMOTE) throw new Error(`e2e up: E2E_TARGET=${TARGET} points at a deployed stack; there is nothing to boot`);
  requireClerk();
  fs.mkdirSync(STATE_DIR, { recursive: true });
  // GitHub App JWT verification is part of the default provider stack.
  // Generate an ephemeral matching pair unless the caller supplies both files.
  if (!process.env.E2E_GITHUB_PRIVATE_KEY_FILE && !process.env.E2E_GITHUB_PUBLIC_KEY_FILE) {
    const pair = crypto.generateKeyPairSync("rsa", { modulusLength: 2048,
      privateKeyEncoding: { type: "pkcs8", format: "pem" },
      publicKeyEncoding: { type: "spki", format: "pem" } });
    process.env.E2E_GITHUB_PRIVATE_KEY_FILE = path.join(STATE_DIR, "github-private.pem");
    process.env.E2E_GITHUB_PUBLIC_KEY_FILE = path.join(STATE_DIR, "github-public.pem");
    fs.writeFileSync(process.env.E2E_GITHUB_PRIVATE_KEY_FILE, pair.privateKey, { mode: 0o600 });
    fs.writeFileSync(process.env.E2E_GITHUB_PUBLIC_KEY_FILE, pair.publicKey);
  } else if (!process.env.E2E_GITHUB_PRIVATE_KEY_FILE || !process.env.E2E_GITHUB_PUBLIC_KEY_FILE) {
    throw new Error("e2e: provide both GitHub key files or neither");
  }

  for (const [name, bin] of Object.entries(BINS)) {
    if (!args.has("--no-backend") && !fs.existsSync(bin)) {
      throw new Error(`e2e up: ${name} binary missing at ${bin}. Build it: CORALSWARM_E2E_BUILD=1 CARGO_PROFILE_DEV_DEBUG=0 cargo build -p coralswarm-backend --features e2e --bin coralswarm-backend --bin migrate_control --bin migrate_tenants`);
    }
  }
  const webServer = path.join(WEB_DIR, ".next", "standalone", "web", "server.js");
  if (!args.has("--no-web") && !fs.existsSync(webServer)) {
    throw new Error(`e2e up: web standalone build missing at ${webServer}. Build it: node e2e/stack/build-web.mjs`);
  }

  // Previous processes from this checkout's stack.
  const prev = readState();
  for (const pid of Object.values(prev.pids ?? {})) killPid(pid);
  recordState({ pids: {} });

  // Windows: run a copy whose main-thread stack is widened (see the helper);
  // cargo's own artifact is never modified.
  let backendExe = BINS.backend;
  if (process.platform === "win32" && !args.has("--no-backend")) {
    backendExe = path.join(STATE_DIR, path.basename(BINS.backend));
    await sleep(500); // let a just-killed backend release its image
    fs.rmSync(backendExe, { force: true });
    fs.copyFileSync(BINS.backend, backendExe);
    widenWindowsMainStack(backendExe);
  }

  await step("containers", async () => {
    startContainer(CONTAINERS.pg, [
      // Canonical scale fixtures exceed Docker's 64 MiB shared-memory default.
      "--shm-size", "1g",
      "-p", `${PORTS.pg}:5432`,
      "-e", "POSTGRES_PASSWORD=postgres",
      "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
      IMAGES.pg,
      // The e2e cluster is disposable: durability off for speed.
      "-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off", "-c", "max_connections=300",
    ]);
    startContainer(CONTAINERS.s3, [
      "-p", `${PORTS.s3}:9090`,
      "-e", `COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS=${BUCKET}`,
      IMAGES.s3,
    ]);
    await waitFor("postgres", async () => sh("docker", ["exec", CONTAINERS.pg, "pg_isready", "-U", "postgres", "-h", "127.0.0.1"], { allowFail: true }).status === 0);
    // pg_isready can pass during the image's init restart; a query cannot.
    await waitFor("postgres query", async () => psql("SELECT 1") === "1");
  });

  await step("ci roles", async () => {
    // The cluster-wide serving roles the control tree expects to exist, created
    // by the same script ci.yml's Postgres jobs use.
    const id = sh("docker", ["inspect", "-f", "{{.Id}}", CONTAINERS.pg]).stdout.trim();
    sh("bash", [path.join(REPO, "scripts", "prepare_ci_pg_roles.sh"), "--container", id]);
  });

  const tpl = `cs_e2e_tpl_${migrationHash()}`;
  const ownerEnv = (db) => ({ MIGRATOR_DATABASE_URL: pgUrl("postgres", db), DATABASE_URL: pgUrl("postgres", db), RUST_LOG: "warn" });

  await step("control template", async () => {
    const exists = psql(`SELECT 1 FROM pg_database WHERE datname='${tpl}'`) === "1";
    if (exists) return log(`template ${tpl} is current; reusing it`);
    const building = `${tpl}_building`;
    psql(`DROP DATABASE IF EXISTS ${building} WITH (FORCE)`);
    psql(`CREATE DATABASE ${building}`);
    const r = spawnSync(BINS.migrateControl, [], { env: { ...process.env, ...ownerEnv(building) }, encoding: "utf8" });
    if (r.status !== 0) throw new Error(`migrate_control (from zero) failed:\n${r.stdout}${r.stderr}`);
    // Stripe catalog ids, as `provision_stripe` writes them in prod: the
    // trial Checkout prices off `seat_tiers.stripe_price_id`, and the fake
    // Stripe echoes whatever price it is given.
    psql(
      `UPDATE seat_tiers SET stripe_price_id = 'price_e2e_seat_' || code WHERE stripe_price_id IS NULL;
       UPDATE plans SET stripe_price_id = 'price_e2e_plan_' || code WHERE stripe_price_id IS NULL;
       UPDATE credit_packs SET stripe_price_id = 'price_e2e_pack_' || code WHERE stripe_price_id IS NULL;`,
      building,
    );
    for (const old of psql(`SELECT datname FROM pg_database WHERE datname LIKE 'cs_e2e_tpl_%' AND datname NOT LIKE '%_building'`).split("\n").filter(Boolean)) {
      psql(`ALTER DATABASE ${old} WITH IS_TEMPLATE false ALLOW_CONNECTIONS true; DROP DATABASE ${old} WITH (FORCE)`);
    }
    psql(`ALTER DATABASE ${building} RENAME TO ${tpl}`);
    psql(`ALTER DATABASE ${tpl} WITH IS_TEMPLATE true ALLOW_CONNECTIONS false`);
  });

  await step("roles", async () => {
    // The provisioning worker's login (control 0381 makes coralswarm_provisioner
    // a NOLOGIN group; prod grants it to a login the same way).
    psql(`DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='cs_e2e_provisioner') THEN
        CREATE ROLE cs_e2e_provisioner LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS IN ROLE coralswarm_provisioner;
      END IF; END $$;`);
  });

  await step("control database", async () => {
    // Tenant databases of an earlier run belong to orgs the fresh control
    // plane has never heard of.
    for (const db of psql(`SELECT datname FROM pg_database WHERE datname LIKE 'coralswarm_tenant_%'`).split("\n").filter(Boolean)) {
      psql(`DROP DATABASE IF EXISTS "${db}" WITH (FORCE)`);
    }
    psql(`DROP DATABASE IF EXISTS ${CONTROL_DB} WITH (FORCE)`);
    psql(`CREATE DATABASE ${CONTROL_DB} TEMPLATE ${tpl}`);
  });

  if (!args.has("--no-backend")) {
    await step("release migrations", async () => {
      for (const [bin, argv] of [[BINS.migrateControl, ["--check"]], [BINS.migrateTenants, []]]) {
        const r = spawnSync(bin, argv, { env: { ...process.env, ...ownerEnv(CONTROL_DB) }, encoding: "utf8" });
        if (r.status !== 0) throw new Error(`${path.basename(bin)} ${argv.join(" ")} failed (exit ${r.status}):\n${r.stdout}${r.stderr}`);
      }
    });
  }

  // The isolation invariants no HTTP request can see (stack/assert-db.mjs):
  // serving role, RLS shape, definer search_path, migrator split, tenant tree
  // under a non-superuser owner. A failure fails the boot.
  //
  // They depend only on the tree, so CI runs them once per run: on shard 1
  // (every other CI boot skips them; E2E_DB_ASSERT=1 forces them). Every
  // local boot runs them.
  const ciSkips = !!process.env.CI && (process.env.E2E_SHARD_INDEX ?? "0") !== "1" && process.env.E2E_DB_ASSERT !== "1";
  if (!args.has("--no-backend") && !args.has("--no-db-assert") && !ciSkips) {
    await step("db assertions", () => assertDatabases({ template: tpl, backendExe }));
  }

  await step("fakes", async () => {
    launch("fakes", process.execPath, [path.join(E2E_DIR, "stack", "fakes.mjs")], { E2E_FAKES_PORT: String(PORTS.fakes), E2E_S3_PORT: String(PORTS.s3), E2E_S3_PROXY_PORT: String(PORTS.s3Proxy) }, E2E_DIR).pid;
    await waitFor("fakes", httpOk(`http://127.0.0.1:${PORTS.fakes}/__health`), 15_000);
  });

  if (!args.has("--no-backend")) {
    await step("backend", async () => {
      const b = launch("backend", backendExe, [], backendEnv(), REPO);
      await waitFor("backend /readyz", httpOk(`http://127.0.0.1:${PORTS.api}/readyz`), 90_000).catch((e) => {
        throw new Error(`${e.message}\n--- backend.log (tail) ---\n${tail(b.log)}`);
      });
    });
  }

  if (!args.has("--no-web")) {
    await step("web", async () => {
      const w = launch("web", process.execPath, [webServer], { PORT: String(PORTS.web), HOSTNAME: "0.0.0.0", NODE_ENV: "production" }, path.dirname(webServer));
      await waitFor("web", httpOk(`http://127.0.0.1:${PORTS.web}/`), 60_000).catch((e) => {
        throw new Error(`${e.message}\n--- web.log (tail) ---\n${tail(w.log)}`);
      });
    });
  }

  recordState({ controlDb: CONTROL_DB, template: tpl, timings, api: LOCAL_API_BASE, web: LOCAL_WEB_BASE });
  log(`stack up: api ${LOCAL_API_BASE}  web ${LOCAL_WEB_BASE}  db ${CONTROL_DB}  (${Object.entries(timings).map(([k, v]) => `${k} ${(v / 1000).toFixed(1)}s`).join(", ")})`);
}

// Windows gives the main thread 1 MiB, and the debug backend's `run()` future
// overflows it at boot (Linux's 8 MiB is enough, so CI never needs this). Raise
// the reserve on the stack's private copy with MSVC's editbin.
function widenWindowsMainStack(exePath) {
  const roots = ["C:/Program Files/Microsoft Visual Studio", "C:/Program Files (x86)/Microsoft Visual Studio"];
  const found = [];
  const walk = (dir, depth) => {
    if (depth > 9 || found.length) return;
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      const p = path.join(dir, e.name);
      if (e.isFile() && e.name.toLowerCase() === "editbin.exe" && /hostx64[\\/]x64/i.test(p)) found.push(p);
      else if (e.isDirectory() && /^(20\d\d|BuildTools|Community|Professional|Enterprise|VC|Tools|MSVC|bin|Hostx64|x64|[\d.]+)$/i.test(e.name)) walk(p, depth + 1);
    }
  };
  for (const r of roots) walk(r, 0);
  if (!found.length) return log("warning: editbin.exe not found; a debug backend may overflow the 1 MiB Windows main stack");
  sh(found[0], ["/NOLOGO", "/STACK:67108864", exePath]);
}

function tail(file, n = 60) {
  try {
    return fs.readFileSync(file, "utf8").split("\n").slice(-n).join("\n");
  } catch {
    return "(no log)";
  }
}

main().catch((e) => {
  console.error(e.message ?? e);
  process.exit(1);
});
