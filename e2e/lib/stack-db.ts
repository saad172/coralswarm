// A read-only look at the local stack's control database, through the
// stack's own Postgres container (the same `docker exec psql` that
// `stack/up.mjs` and `stack/assert-db.mjs` use). For the one thing no HTTP
// route can observe: a `jobs` row with no `account_id`, which is what every
// webhook delivery becomes (`JobKind::IngestReceive`). `GET /v1/jobs/{id}`
// answers only for the caller's own account, and a receive job belongs to
// none until a worker resolves it.
//
// Scenarios still seed through the real API and never write SQL (README
// "Seeding"); this runs `SELECT`s only, and only against the local stack.

import { spawnSync } from "node:child_process";
import { CONTAINERS, CONTROL_DB, REMOTE, TARGET } from "../stack/config.mjs";

/** One `SELECT`'s rows, each row the columns as text. */
export function selectRows(sql: string): string[][] {
  if (REMOTE) throw new Error(`e2e: the control database is local-stack only; E2E_TARGET=${TARGET} runs @smoke scenarios`);
  if (!/^\s*select\b/i.test(sql)) throw new Error(`e2e: stack-db runs SELECT only, got: ${sql.slice(0, 40)}`);
  const r = spawnSync(
    "docker",
    ["exec", "-i", CONTAINERS.pg, "psql", "-U", "postgres", "-d", CONTROL_DB, "-X", "-q", "-t", "-A", "-F", "\u001f", "-v", "ON_ERROR_STOP=1", "-c", sql],
    { encoding: "utf8" },
  );
  if (r.status !== 0) throw new Error(`e2e: stack-db query failed (exit ${r.status}): ${r.stdout}${r.stderr}`);
  return r.stdout
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => line.split("\u001f"));
}
