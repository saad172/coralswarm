"use client";

/* DEV-ONLY — loopback paste pad for a JWKS-minted extract token.
   Not in the nav. Hidden on any non-loopback host. Delete before merge
   if this branch ships without a local-verify story. */

import { useEffect, useState } from "react";
import { Button } from "@/components/ui";
import { devBearerAllowed, readDevBearer, writeDevBearer } from "@/lib/devBearer";

export default function DevGraphTokenPage() {
  const [token, setToken] = useState("");
  const [loopback, setLoopback] = useState(false);

  useEffect(() => {
    const ok = devBearerAllowed();
    setLoopback(ok);
    if (ok) setToken(readDevBearer() ?? "");
  }, []);

  if (!loopback) {
    return (
      <div className="grid min-h-screen place-items-center px-[24px] font-mono text-[13px] text-ink-3">
        This page only runs on localhost.
      </div>
    );
  }

  return (
    <div className="mx-auto flex min-h-screen max-w-[520px] flex-col justify-center gap-[16px] px-[24px]">
      <h1 className="font-display text-[28px] italic text-ink">Local graph token</h1>
      <p className="text-[14px] leading-[1.45] text-ink-2">
        Paste the JWKS-minted bearer from{" "}
        <code className="font-mono text-[12px]">~/.cs-graph-ops/graph-you-hub-3.2.dev-token</code>
        . This is stored in <code className="font-mono text-[12px]">localStorage.cs_dev_token</code>{" "}
        and sent as <code className="font-mono text-[12px]">Authorization: Bearer</code> on this
        loopback stack only.
      </p>
      <textarea
        value={token}
        onChange={(e) => setToken(e.target.value)}
        rows={6}
        spellCheck={false}
        className="w-full rounded-field border border-line bg-bg-2 px-[12px] py-[10px] font-mono text-[11px] text-ink outline-none focus:border-ink-3"
        aria-label="Dev bearer token"
      />
      <div className="flex flex-wrap gap-[8px]">
        <Button
          variant="primary"
          onClick={() => {
            writeDevBearer(token);
            // Full load so ClerkAuthProvider remounts and boots with the token.
            window.location.assign("/graph");
          }}
          disabled={!token.trim()}
        >
          Open /graph
        </Button>
        <Button
          onClick={() => {
            writeDevBearer(null);
            setToken("");
          }}
        >
          Clear
        </Button>
      </div>
    </div>
  );
}
