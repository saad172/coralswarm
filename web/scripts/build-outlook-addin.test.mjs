import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { buildOutlookAddin } from "./build-outlook-addin.mjs";
const directory = mkdtempSync(resolve(tmpdir(), "cs-outlook-public-"));
try {
  const output = resolve(directory, "outlook");
  buildOutlookAddin(output, {
    NEXT_PUBLIC_WEB_APEX: "coralswarm.example",
    NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY: "pk_test_" + Buffer.from("clerk.example.test$").toString("base64"),
    CLERK_ACCOUNTS_HOST: "accounts.example.test",
  });
  const manifest = readFileSync(resolve(output, "manifest.xml"), "utf8");
  // The panes are Next.js routes under /outlook now, not static pages.
  assert(manifest.includes("https://app.coralswarm.example/outlook/mail"));
  assert(manifest.includes("https://app.coralswarm.example/outlook/meeting"));
  assert(!manifest.includes("/src/"));
  assert(!manifest.includes("addin.coralswarm.example"));
  assert(!manifest.includes("{{"));
  assert(manifest.includes("api://app.coralswarm.example/00000000-0000-0000-0000-000000000000"));
  assert(manifest.includes("https://clerk.example.test"));
  assert(!existsSync(resolve(output, "src")));
  assert(readFileSync(resolve(output, "assets/icon-64.png")).length > 0);
  // No Clerk key is needed any more: defaults apply.
  buildOutlookAddin(resolve(directory, "no-key"), { NEXT_PUBLIC_WEB_APEX: "coralswarm.example" });
  assert.throws(() => buildOutlookAddin(output, { NEXT_PUBLIC_WEB_APEX: "https://bad.example/path" }), /hostname/);
  console.log("Outlook same-web public hosting build passed.");
} finally { rmSync(directory, { recursive: true, force: true }); }
