#!/usr/bin/env node
// Mint Clerk-shaped OAuth test tokens (and serve the matching JWKS) for CI's
// `mcp-e2e` job (docs/plans/remote-mcp-v1.md §5 Phase 4) and local smoke runs
// against a backend booted with MCP_ENABLED=1.
//
// This is a TEST-ONLY tool. The RSA keypair below is the exact throwaway
// fixture already committed in `backend/src/clerk.rs`'s `test_support`
// module (`TEST_PRIV_PEM` / `TEST_KID` / `TEST_N` / `TEST_E`) — duplicated
// here (not imported — this is a Node script, that's a Rust `#[cfg(test)]`
// module) so CI can mint tokens the booted backend binary's REAL
// `clerk::verify_oauth` path will accept when pointed at the JWKS this script
// serves via `CLERK_JWKS_URL`. It is never used for anything but a local
// backend process talking to its own advertised test issuer
// (`https://example.clerk.accounts.dev`, matching `clerk::test_support::ISSUER`).
// Keep this key in sync with `backend/src/clerk.rs` if that fixture ever
// rotates.
//
// Usage:
//   MCP_RESOURCE_URL=http://127.0.0.1:8080/mcp node scripts/mcp-mint-test-token.mjs mint [--scope read|write] [--sub <id>]
//     Alternatively pass --resource <exact MCP resource URL>.
//     Prints one signed RS256 JWT to stdout.
//
//   node scripts/mcp-mint-test-token.mjs serve-jwks [--port <port>]
//     Serves the matching JWKS document at
//     http://127.0.0.1:<port>/.well-known/jwks.json until killed. Point
//     CLERK_JWKS_URL at this URL and CLERK_ISSUER at the issuer below.
//
// No npm dependencies — RS256 signing uses Node's built-in `crypto` module
// so CI never needs an `npm install` step just to mint a test token.

import { createSign } from "node:crypto";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";

// ── Shared fixture (mirror of backend/src/clerk.rs's test_support module) ────

const TEST_PRIV_PEM = `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAyFjQlKieXg+zR/1bqt+8xitNNJKf3/fFvtVM6LZNfhV0Qy6L
g63lHuxYPsqcCMi4cGSja54wwaKIHKGxLhyAmrEiv/YWCNUZFi8a8SBiImGg3thD
sV/RftbEYuD9+RG5/LU08jTyJYdS0o9qSRreIM/dsTNodZA3M9OSIZF7aVetio2r
gUJsd3rNBBH9m5SLEgmWPi4k6Yx2dHT5JGSPXqoITgwqQdniVRmTS6ySt9vUKax5
PnOFabChyYZbd5d47TPqdVoxN/tYdCtWMAQkmluHPsL9KiNG9u86ufaFKisCtIyj
FdxOZT9MCyYleDwrSiAaZ/uu1HgOFTPPXnaUqwIDAQABAoIBAFUGRDbivJsEpLxH
jLCe1TlwxQors8fUGkrbsREN1SNOLZIZXcIbeaI/AVBcog2kSYoBW81fTCfRN1yX
Tl9YX0fHJw06fa1v6n6crR1p9dvLfP813ryXAoyGzr975qzeVgUmTfsAd0dv/7k2
fg/aZCyequH6BDnoY1UOU/UFDc/tUWBnxjAaywhytl0q5IXfsXiqKoY/eOl1Da9j
qdVDA2S4bkkqHnxAwASU9MrnB+HezUfslbFFsy/y4Qc2+GP5vfXZUQshqAzVlgJF
6m1cuNrHWtCEy0Hr8RyE7rfAQplgiSAj4ktKqbJ60KImtbYCh5SRdKa2owWOag3Y
T1/oREECgYEA/S0bAr0tSJAoOkSySWTbqOpbkpLsnUqhwyUkqwFWTHZtBJGVTqik
pwCPaJlZIUmwlqaskDBLZystVGLQjX5CC+JRT/wnLZCt+5w5AkId4CSOEL+WmUo0
lc3ANCOMQlEpls8DTA+qBCGJbBp2luhkOKhCGq+fv+7Xc2IhovUBtksCgYEAypTd
orz8eKxaTn1Gczu8j91w4vsHqSJmpQVHsnb9zEjECGgQ6fQsEipY6TWGrSYhKP9v
KEyv847Igol03yZVGrkHTVPsrFF+W9dw6JX6dbCD3A756mhnNDntDAMGv/mDlZ9y
B9QbXLog7eTtvhrV/bRGh8ufAfBfvraxs22hHyECgYAJtzhpI39gYH/atJYu8Nrp
IY1AE7lW0+1fQsakdVmtd4H5gei76wbejcvzFKdx+pT3QxAuPFw4alTD/0LHm0fH
kvjR2/hY7Etw827Qol7pIh39kIyj91ALS6kLly2jwd+ZtPoIajV5ERxn9KAGbIak
EfjvczvrLqw/9k0aJtLY/wKBgQCLPE4pMCOG/U8Ea24kfYZYiyavm0HkqdRKz5o7
uzUtV0YFUSFu63fu9FQv1a4/C9n2x7mqxSqaCCUgM0G0rjAIMewm5evhdBGZJjZg
ZdTDVxMKCPEUf7WPW9DlVJ8rC1+5UuGZJ9u2m+dOGu6cnu09ds6TPwyG1KO5/O+e
F1eKgQKBgDqAg7bUgZRU0MrdJ3jKjM/H3PeCG5Vr1KfyadxtkZ/Z55pUxofnh2Da
RDMphBnWCCCjvNJbU/Q/3eInHtUuPv+5eFL3k1bRYSpsMtuROSqT2GhZSVsvDseD
4erYQT+8X+MMVHvLNdGdeKw/OsPPDzuuDmuOldk7EeOqHkfHD7v1
-----END RSA PRIVATE KEY-----`;

const TEST_KID = "test-key-1";
const TEST_N =
  "yFjQlKieXg-zR_1bqt-8xitNNJKf3_fFvtVM6LZNfhV0Qy6Lg63lHuxYPsqcCMi4cGSja54wwaKIHKGxLhyAmrEiv_YWCNUZFi8a8SBiImGg3thDsV_RftbEYuD9-RG5_LU08jTyJYdS0o9qSRreIM_dsTNodZA3M9OSIZF7aVetio2rgUJsd3rNBBH9m5SLEgmWPi4k6Yx2dHT5JGSPXqoITgwqQdniVRmTS6ySt9vUKax5PnOFabChyYZbd5d47TPqdVoxN_tYdCtWMAQkmluHPsL9KiNG9u86ufaFKisCtIyjFdxOZT9MCyYleDwrSiAaZ_uu1HgOFTPPXnaUqw";
const TEST_E = "AQAB";
export const ISSUER = "https://example.clerk.accounts.dev";

// ── Base64url helpers ─────────────────────────────────────────────────────────

function base64url(input) {
  return Buffer.from(input)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

/** Sign OAuth access claims as an RS256 JWT, tagged with TEST_KID — the
 * mirror of backend/src/clerk.rs test_support's `mint_oauth()`. */
function mint(claims) {
  const header = { alg: "RS256", typ: "at+jwt", kid: TEST_KID };
  const encodedHeader = base64url(JSON.stringify(header));
  const encodedPayload = base64url(JSON.stringify(claims));
  const signingInput = `${encodedHeader}.${encodedPayload}`;
  const signature = createSign("RSA-SHA256").update(signingInput).sign(TEST_PRIV_PEM);
  const encodedSignature = base64url(signature);
  return `${signingInput}.${encodedSignature}`;
}

function jwks() {
  return {
    keys: [{ use: "sig", kty: "RSA", alg: "RS256", kid: TEST_KID, n: TEST_N, e: TEST_E }],
  };
}

// ── CLI ────────────────────────────────────────────────────────────────────────

function parseFlags(args) {
  const flags = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--")) {
      const key = args[i].slice(2);
      const value = args[i + 1] && !args[i + 1].startsWith("--") ? args[++i] : "true";
      flags[key] = value;
    }
  }
  return flags;
}

function cmdMint(args) {
  const flags = parseFlags(args);
  const resource = flags.resource || process.env.MCP_RESOURCE_URL;
  if (!resource || !resource.trim() || resource === "true") {
    console.error("mint requires --resource or MCP_RESOURCE_URL matching the backend's MCP resource");
    process.exit(1);
  }
  const scope = flags.scope === "read" ? "coralswarm:read" : "coralswarm:read coralswarm:write";
  const sub = flags.sub || `mcp_smoke_${randomUUID()}`;
  const now = Math.floor(Date.now() / 1000);
  const token = mint({
    iss: ISSUER,
    aud: resource,
    sub,
    exp: now + 3600,
    iat: now,
    scope,
    client_id: "mcp_smoke_client",
    jti: randomUUID(),
  });
  process.stdout.write(token + "\n");
}

function cmdServeJwks(args) {
  const flags = parseFlags(args);
  const port = Number(flags.port || 8091);
  const body = JSON.stringify(jwks());
  const server = createServer((req, res) => {
    if (req.url === "/.well-known/jwks.json") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(body);
      return;
    }
    res.writeHead(404);
    res.end();
  });
  server.listen(port, "127.0.0.1", () => {
    // Single stdout line CI greps for readiness; keep it stable.
    console.log(`jwks listening on http://127.0.0.1:${port}/.well-known/jwks.json`);
  });
}

const [, , command, ...rest] = process.argv;
switch (command) {
  case "mint":
    cmdMint(rest);
    break;
  case "serve-jwks":
    cmdServeJwks(rest);
    break;
  default:
    console.error("usage: mcp-mint-test-token.mjs <mint|serve-jwks> [--scope read|write] [--sub <id>] [--resource <URL>] [--port <port>]");
    process.exit(1);
}
