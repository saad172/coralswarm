#!/usr/bin/env node
// Build web/ the way deploy.yml's `build-web-release` does (standalone), but
// pointed at the e2e backend and the Clerk DEV instance, then lay the bundle
// out the way web/Dockerfile does (static + public beside server.js).
//
// Usage: node stack/build-web.mjs [--skip-install]

import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { LOCAL_API_BASE, WEB_DIR, requireClerk } from "./config.mjs";

const { publishableKey } = requireClerk();
const npm = process.platform === "win32" ? "npm.cmd" : "npm";
const run = (argv, env = {}) => {
  const r = spawnSync(npm, argv, { cwd: WEB_DIR, stdio: "inherit", env: { ...process.env, ...env }, shell: process.platform === "win32" });
  if (r.status !== 0) process.exit(r.status ?? 1);
};

const started = Date.now();
if (!process.argv.includes("--skip-install") && !fs.existsSync(path.join(WEB_DIR, "node_modules"))) run(["ci", "--no-audit", "--no-fund"]);
run(["run", "build"], {
  NEXT_OUTPUT_MODE: "standalone",
  // Capture only; ordinary production builds never set this flag. Active only
  // when a coordinated E2E document also installs its nonce before app code.
  NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS: "1",
  NEXT_TELEMETRY_DISABLED: "1",
  NEXT_PUBLIC_API_BASE: LOCAL_API_BASE,
  // Empty: no apex derivation, every call uses NEXT_PUBLIC_API_BASE (localhost).
  NEXT_PUBLIC_WEB_APEX: "",
  NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY: publishableKey,

});

const standalone = path.join(WEB_DIR, ".next", "standalone", "web");
fs.cpSync(path.join(WEB_DIR, ".next", "static"), path.join(standalone, ".next", "static"), { recursive: true });
fs.cpSync(path.join(WEB_DIR, "public"), path.join(standalone, "public"), { recursive: true });
console.log(`[e2e build-web] done in ${((Date.now() - started) / 1000).toFixed(1)}s -> ${standalone}`);
