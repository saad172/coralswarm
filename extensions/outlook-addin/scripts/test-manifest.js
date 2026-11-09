// Guards the Outlook manifest and its build. No dependencies: plain string
// checks plus a balanced-tag pass (the real schema check is
// `npm run validate`, which needs the office-addin-manifest tooling installed).
const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const root = path.join(__dirname, "..");
const manifest = fs.readFileSync(path.join(root, "manifest.xml"), "utf8");
const AAD = "00000000-0000-0000-0000-000000000000";

// ── well-formed: every opened tag closes in order ──────────────────────────
{
  const stripped = manifest.replace(/<!--[\s\S]*?-->/g, "").replace(/<\?xml[^>]*\?>/g, "");
  const stack = [];
  for (const [, closing, name, selfClosing] of stripped.matchAll(/<(\/?)([A-Za-z][\w:.-]*)(?:\s[^>]*?)?(\/?)>/g)) {
    if (selfClosing) continue;
    if (closing) assert.equal(stack.pop(), name, `mismatched </${name}>`);
    else stack.push(name);
  }
  assert.deepEqual(stack, [], "unclosed tags: " + stack.join(","));
}

// ── Microsoft SSO ──────────────────────────────────────────────────────────
assert(manifest.includes(`<Id>${AAD}</Id>`), "WebApplicationInfo Id");
assert(manifest.includes(`<Resource>api://app.coralswarm.example/${AAD}</Resource>`), "WebApplicationInfo Resource");
assert(/<WebApplicationInfo>[\s\S]*<Scope>profile<\/Scope>[\s\S]*<Scope>openid<\/Scope>[\s\S]*<\/WebApplicationInfo>/.test(manifest), "WebApplicationInfo scopes");
// WebApplicationInfo lives in the nested V1_1 block, which is where Outlook reads it.
assert(/xsi:type="VersionOverridesV1_1"[\s\S]*<WebApplicationInfo>/.test(manifest), "WebApplicationInfo must sit inside VersionOverridesV1_1");
assert(manifest.includes("mailappversionoverrides/1.1"));
assert(manifest.includes("<AppDomain>https://login.microsoftonline.com</AppDomain>"), "Microsoft sign-in domain");
// SSO/NAA are feature-detected, never required: a required set would make the add-in
// uninstallable on clients without it and remove the Clerk fallback.
assert(!/Name="(NestedAppAuth|IdentityAPI)"/.test(manifest.replace(/<!--[\s\S]*?-->/g, "")), "NAA/SSO must not be required sets");

// ── surfaces, URLs, labels ─────────────────────────────────────────────────
for (const surface of ["MessageReadCommandSurface", "AppointmentOrganizerCommandSurface", "AppointmentAttendeeCommandSurface"]) {
  assert.equal(manifest.split(`xsi:type="${surface}"`).length - 1, 2, `${surface} in both VersionOverrides blocks`);
}
assert(manifest.includes('<bt:Url id="taskpaneUrl" DefaultValue="{{ADDIN_HOST}}/mail"/>'));
assert(manifest.includes('<bt:Url id="meetingPaneUrl" DefaultValue="{{ADDIN_HOST}}/meeting"/>'));
assert(manifest.includes('<SourceLocation DefaultValue="{{ADDIN_HOST}}/mail"/>'));
assert(!manifest.includes("/src/"), "the static taskpane/meeting pages are gone");
for (const id of ["ingestLabel", "meetingLabel", "ingestTip", "meetingTip", "groupLabel"]) {
  const values = [...manifest.matchAll(new RegExp(`<bt:String id="${id}" DefaultValue="([^"]*)"`, "g"))].map((m) => m[1]);
  assert.equal(values.length, 2, `${id} in both blocks`);
  assert.equal(values[0], values[1], `${id} identical in both blocks`);
}
assert(manifest.includes('id="ingestLabel" DefaultValue="Save to CoralSwarm"'));
assert(manifest.includes('id="meetingLabel" DefaultValue="Save meeting to CoralSwarm"'));
assert(!/Grow|Capture meeting transcript/.test(manifest), "retired labels");
// Compose (unsaved draft): the organizer surface opens the same pane, which only needs CustomProperties
// (ReadItem). Widening to ReadWriteItem would be a permission prompt for nothing.
for (const block of manifest.split('xsi:type="AppointmentOrganizerCommandSurface"').slice(1)) {
  assert(/<Action xsi:type="ShowTaskpane"><SourceLocation resid="meetingPaneUrl"\/><\/Action>/.test(block.split("</ExtensionPoint>")[0]), "organizer surface opens the meeting pane");
}
assert(manifest.includes("<Permissions>ReadItem</Permissions>"), "ReadItem is enough for compose markers");
const version = (manifest.match(/<Version>([\d.]+)<\/Version>/) || [])[1];
const newer = (a, b) => { for (let i = 0; i < 4; i++) { const d = (Number(a.split(".")[i]) || 0) - (Number(b.split(".")[i]) || 0); if (d) return d > 0; } return false; };
assert(version && newer(version, "1.0.1.0"), "version bumped past 1.0.1.0");

// ── no trace of the old static host anywhere in this package ───────────────
function walk(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    if (entry.name === "node_modules" || entry.name === "package-lock.json" || entry.name === "test-manifest.js") return [];
    const full = path.join(dir, entry.name);
    return entry.isDirectory() ? walk(full) : /\.(png|svg)$/.test(entry.name) ? [] : [full];
  });
}
for (const file of walk(root)) {
  const text = fs.readFileSync(file, "utf8");
  assert(!text.includes("addin.coralswarm.example"), `${path.relative(root, file)} still names addin.coralswarm.example`);
}
for (const gone of ["Dockerfile", "fly.toml", "nginx.conf", "server.js", "src"]) assert(!fs.existsSync(path.join(root, gone)), `${gone} must be removed`);

// ── the build: manifest.local.xml for production resolves every placeholder ─
const local = path.join(root, "manifest.local.xml");
try {
  execFileSync(process.execPath, [path.join(__dirname, "build-manifest.js"), "https://app.coralswarm.example/outlook"], {
    env: { ...process.env, NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY: "pk_test_" + Buffer.from("clerk.example.test$").toString("base64"), CLERK_ACCOUNTS_HOST: "accounts.example.test" },
    stdio: "pipe",
  });
  const built = fs.readFileSync(local, "utf8");
  assert(!built.includes("{{"), "unsubstituted placeholder");
  assert(built.includes("https://app.coralswarm.example/outlook/mail") && built.includes("https://app.coralswarm.example/outlook/meeting"));
  assert(built.includes("https://app.coralswarm.example/outlook/assets/icon-64.png"));
  assert(built.includes("<AppDomain>https://clerk.example.test</AppDomain>"));
} finally { fs.rmSync(local, { force: true }); }

console.log("Outlook manifest checks passed.");
