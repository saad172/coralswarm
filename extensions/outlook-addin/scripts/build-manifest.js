// Produce manifest.local.xml from manifest.xml by substituting the templated
// hosts: {{ADDIN_HOST}} (the web app's /outlook base: the panes are Next.js routes
// under it, assets/ is static) and the two Clerk hosts (used by the Clerk fallback
// sign-in only).
//
//   node scripts/build-manifest.js https://localhost:3000/outlook        (next dev --experimental-https)
//   node scripts/build-manifest.js https://app.coralswarm.example/outlook    (production; what
//                                                                        web/scripts/build-outlook-addin.mjs runs)
//
// The Clerk hosts are templated rather than hardcoded because the Frontend API
// host follows whichever publishable key the build uses:
// a build against a test instance would otherwise ship a manifest naming
// production's Clerk domains, and `AppDomains` would not cover the domains
// clerk-js actually opens.
//
//   CLERK_FRONTEND_API   defaults to the value derived from
//                        NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY, else clerk.coralswarm.example
//   CLERK_ACCOUNTS_HOST  defaults to accounts.coralswarm.example
//
// `CLERK_ACCOUNTS_HOST` is not derivable from the key (a production instance
// uses a custom `accounts.<apex>`, a development one a `*.accounts.dev` host),
// so it is env-only with a production default.

const fs = require("fs");
const path = require("path");

const HOSTNAME_RE = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$/i;

// A Clerk publishable key is `pk_(live|test)_` + base64 of "<frontend-api>$".
// `Buffer.from(x, "base64")` never throws on malformed input, so the decode alone
// proves nothing: the result is validated and "" returned on anything that is
// not a hostname.
function frontendApiFromKey(key) {
  if (!key) return "";
  let decoded;
  try {
    decoded = Buffer.from(key.replace(/^pk_(live|test)_/, ""), "base64").toString("utf8");
  } catch {
    return "";
  }
  if (!decoded.endsWith("$")) return "";
  const host = decoded.slice(0, -1);
  return HOSTNAME_RE.test(host) ? host : "";
}

function requireHost(name, value) {
  if (!HOSTNAME_RE.test(value)) {
    console.error(`${name} is not a hostname: ${JSON.stringify(value)}`);
    process.exit(1);
  }
  return value;
}

const host = (process.argv[2] || "https://localhost:3000/outlook").replace(/\/$/, "");

const frontendApi = requireHost(
  "CLERK_FRONTEND_API",
  process.env.CLERK_FRONTEND_API ||
    frontendApiFromKey(process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY) ||
    "clerk.coralswarm.example",
);
const accountsHost = requireHost(
  "CLERK_ACCOUNTS_HOST",
  process.env.CLERK_ACCOUNTS_HOST || "accounts.coralswarm.example",
);

const src = path.join(__dirname, "..", "manifest.xml");
const out = path.join(__dirname, "..", "manifest.local.xml");

const xml = fs
  .readFileSync(src, "utf8")
  .replace(/\{\{ADDIN_HOST\}\}/g, host)
  .replace(/\{\{CLERK_FRONTEND_API\}\}/g, `https://${frontendApi}`)
  .replace(/\{\{CLERK_ACCOUNTS_HOST\}\}/g, `https://${accountsHost}`);

if (xml.includes("{{")) {
  console.error("manifest.xml still contains an unsubstituted {{…}} placeholder");
  process.exit(1);
}

fs.writeFileSync(out, xml);
console.log(`Wrote ${out} (host: ${host}, clerk: ${frontendApi}, accounts: ${accountsHost})`);
