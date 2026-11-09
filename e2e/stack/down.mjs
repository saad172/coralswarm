#!/usr/bin/env node
// Stop the e2e stack's processes and remove its containers.
// `--keep-containers` leaves Postgres (and its migrated template) running so
// the next `up` skips the migrate-from-zero step.

import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { CONTAINERS, STATE_DIR } from "./config.mjs";

function killPid(pid) {
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

const statePath = path.join(STATE_DIR, "state.json");
let state = { pids: {} };
try {
  state = JSON.parse(fs.readFileSync(statePath, "utf8"));
} catch {}
for (const [name, pid] of Object.entries(state.pids ?? {})) {
  killPid(pid);
  console.log(`[e2e down] stopped ${name} (${pid})`);
}
fs.rmSync(statePath, { force: true });
// Only generated stack-local keys; caller-supplied keys remain caller-owned.
for (const name of ["github-private.pem", "github-public.pem"]) {
  fs.rmSync(path.join(STATE_DIR, name), { force: true });
}

if (!process.argv.includes("--keep-containers")) {
  for (const name of Object.values(CONTAINERS)) {
    spawnSync("docker", ["rm", "-f", name], { stdio: "ignore" });
    console.log(`[e2e down] removed ${name}`);
  }
}
