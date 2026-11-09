#!/usr/bin/env node
// N-1 compatibility (CI redesign decision 11, README "N-1 compatibility").
// Swap the stack's backend for a DIFFERENT binary, normally the production
// release's, on the same database: stop the e2e backend `up.mjs` started (it
// has already migrated and been seeded through), then boot `DIR/coralswarm-
// backend` against the same control plane and tenant databases.
//
//   node stack/n1-backend.mjs --bin-dir DIR                    # run it natively
//   node stack/n1-backend.mjs --bin-dir DIR --image IMAGE      # run it in IMAGE (Linux, --network host)
//   node stack/n1-backend.mjs --stop
//
// A release binary is built without the `e2e` feature, so it honours none of
// the e2e-only seams. Its environment is therefore the stack's minus what a
// release build cannot use:
//   * R2 is unset (R2_ENDPOINT_OVERRIDE is e2e-only; a release build would
//     talk to real R2 with fake keys). Blob storage is disabled; inline
//     content is served from Postgres.
//   * Stripe's base-URL override is e2e-only; nothing in the N-1 scenarios
//     calls Stripe.
//   * OpenRouter cannot be the fake. A release build panics on a non-https
//     OPENROUTER_BASE_URL (the SSRF guard in `llm::openai::with_base_url`),
//     its embedder ignores the variable altogether, and at boot it embeds one
//     string against the REAL OpenRouter, refusing to start if that fails
//     (`run()`, "cloud embed probe failed"). So OPENROUTER_BASE_URL is unset
//     and OPENROUTER_API_KEY is N1_OPENROUTER_API_KEY, a real key (CI secret
//     E2E_N1_OPENROUTER_API_KEY). The standing extraction sweep is switched
//     off so the light write in `n1/serve.spec.ts` does not buy an LLM call;
//     what remains is the boot probe and the capture's embedding.
//     No release-build switch avoids that probe; see the README.
//   * Clerk is the real dev instance, as for every backend in the stack.
//
// `--image` exists because release binaries are built in a Debian trixie
// container (deploy.yml `build-backend-release`) and may need a newer glibc
// than the runner has; production runs them on `debian:trixie-slim`
// (backend/Dockerfile.prebuilt), and so does this.

import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { PORTS, STACK_ID, STATE_DIR, backendEnv } from "./config.mjs";

const argv = process.argv.slice(2);
const opt = (name) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : undefined;
};
const CONTAINER = `cs-e2e-n1-${STACK_ID}`;
const statePath = path.join(STATE_DIR, "state.json");
const readState = () => {
  try {
    return JSON.parse(fs.readFileSync(statePath, "utf8"));
  } catch {
    return { pids: {} };
  }
};
const writeState = (s) => fs.writeFileSync(statePath, JSON.stringify(s, null, 2));
const log = (m) => console.log(`[e2e n1] ${m}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ready = () => fetch(`http://127.0.0.1:${PORTS.api}/readyz`).then((r) => r.ok, () => false);

function killPid(pid) {
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

/** The stack's backend env, minus what a release build cannot honour. */
export function n1Env() {
  const env = backendEnv();
  const dropped = ["STRIPE_API_BASE", "OPENROUTER_BASE_URL", "OPENROUTER_GENERATION_LOOKUP_DISABLED", "STANDING_PROCESSING_ENABLED", "SOURCE_EXTRACTION_PROFILE"];
  for (const k of Object.keys(env)) if (k.startsWith("R2_") || dropped.includes(k)) delete env[k];
  const realKey = (process.env.N1_OPENROUTER_API_KEY ?? "").trim();
  if (realKey) env.OPENROUTER_API_KEY = realKey;
  else if (process.env.CI) {
    // A release build cannot boot without it; fail here, not 180 s later.
    throw new Error("N-1: N1_OPENROUTER_API_KEY is unset. A release backend probes the real OpenRouter at boot and refuses to start without a working key; add the E2E_N1_OPENROUTER_API_KEY secret (stack/n1-backend.mjs).");
  }
  // (Locally a debug build falls back to a stub embedder without one.)
  return env;
}

function stop() {
  const state = readState();
  if (state.pids?.n1backend) killPid(state.pids.n1backend);
  spawnSync("docker", ["rm", "-f", CONTAINER], { stdio: "ignore" });
  if (state.pids) delete state.pids.n1backend;
  if (fs.existsSync(statePath)) writeState(state);
}

async function main() {
  if (argv.includes("--stop")) return stop();
  const binDir = opt("--bin-dir");
  if (!binDir) throw new Error("usage: n1-backend.mjs --bin-dir DIR [--image IMAGE] | --stop");
  const exe = path.resolve(binDir, `coralswarm-backend${process.platform === "win32" ? ".exe" : ""}`);
  if (!fs.existsSync(exe)) throw new Error(`no backend binary at ${exe}`);
  const env = n1Env(); // before stopping anything: it can refuse
  stop();

  // Stop the stack's own (new) backend: the old one takes over its port and
  // its databases.
  const state = readState();
  if (state.pids?.backend) {
    killPid(state.pids.backend);
    delete state.pids.backend;
    writeState(state);
    for (let i = 0; i < 40 && (await ready()); i++) await sleep(250);
    if (await ready()) throw new Error(`the stack's backend is still answering on ${PORTS.api} after being stopped`);
    log("stopped the stack's backend");
  }

  const logPath = path.join(STATE_DIR, "n1-backend.log");
  const image = opt("--image");
  let exited = null; // exit code once the process is gone
  if (image) {
    // An env file, not `-e K=V`: the Clerk secret stays out of argv.
    const envFile = path.join(STATE_DIR, "n1.env");
    fs.writeFileSync(envFile, Object.entries(env).map(([k, v]) => `${k}=${v}`).join("\n"), { mode: 0o600 });
    const r = spawnSync("docker", [
      "run", "-d", "--name", CONTAINER, "--network", "host",
      "-v", `${path.resolve(binDir)}:/n1:ro`, "--env-file", envFile, image,
      // The runtime packages backend/Dockerfile.prebuilt installs.
      "sh", "-c", "apt-get update -qq >/dev/null && apt-get install -y -qq --no-install-recommends ca-certificates libgomp1 libssl3 >/dev/null && exec /n1/coralswarm-backend",
    ], { encoding: "utf8" });
    fs.rmSync(envFile, { force: true });
    if (r.status !== 0) throw new Error(`docker run failed: ${r.stderr}`);
    log(`started ${exe} in ${image} (container ${CONTAINER})`);
  } else {
    const fd = fs.openSync(logPath, "w");
    const child = spawn(exe, [], { env: { ...process.env, ...env }, stdio: ["ignore", fd, fd], detached: true, windowsHide: true });
    child.on("exit", (code, signal) => (exited = code ?? signal ?? "exited"));
    child.unref();
    const s = readState();
    s.pids = { ...(s.pids ?? {}), n1backend: child.pid };
    writeState(s);
    log(`started ${exe} (pid ${child.pid})`);
  }

  const logs = () =>
    image
      ? (({ stdout, stderr }) => `${stdout}${stderr}`)(spawnSync("docker", ["logs", "--tail", "80", CONTAINER], { encoding: "utf8" }))
      : fs.readFileSync(logPath, "utf8").split("\n").slice(-80).join("\n");
  const deadline = Date.now() + 180_000;
  for (;;) {
    if (await ready()) break;
    if (image && spawnSync("docker", ["inspect", "-f", "{{.State.Running}}", CONTAINER], { encoding: "utf8" }).stdout.trim() !== "true") exited = "container stopped";
    if (exited !== null || Date.now() > deadline) {
      const out = logs();
      const hint = /cloud embed probe failed|OPENROUTER_API_KEY unset/.test(out)
        ? "\nThe release backend's boot-time embed probe against the real OpenRouter failed: check N1_OPENROUTER_API_KEY (secret E2E_N1_OPENROUTER_API_KEY in CI)."
        : "";
      throw new Error(`the N-1 backend ${exited !== null ? `exited (${exited})` : "did not become ready in 180 s"} before serving:\n${out}${hint}`);
    }
    await sleep(500);
  }
  log(`ready on ${PORTS.api}`);
}

if (process.argv[1]?.endsWith("n1-backend.mjs")) {
  main().catch((e) => {
    console.error(e.message ?? e);
    process.exit(1);
  });
}
