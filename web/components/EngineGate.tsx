"use client";

import { useState } from "react";
import { useEngine } from "@/lib/engine/provider";

// Passphrase gate for the on-device private engine (device-trust). Rendered only
// when the engine is enabled. An enrolled device auto-unlocks (status "ready")
// with no prompt; this shows only for first-time setup or new-device enrollment.
export default function EngineGate({ children }: { children: React.ReactNode }) {
  const eng = useEngine();
  const [pp, setPp] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [confirmReset, setConfirmReset] = useState(false);

  if (!eng || eng.status === "ready") return <>{children}</>;
  if (eng.status === "checking") {
    return (
      <div className="grid h-screen place-items-center font-mono text-sm text-ink-3">
        opening your private workspace…
      </div>
    );
  }

  const isSetup = eng.status === "needs-setup";

  async function go() {
    if (!eng || !pp) return;
    setBusy(true);
    setErr(null);
    try {
      if (isSetup) {
        await eng.setup(pp);
      } else if (!(await eng.unlock(pp))) {
        setErr("Wrong passphrase for this account.");
      }
    } catch {
      setErr("Couldn't unlock the private workspace.");
    } finally {
      setBusy(false);
    }
  }

  async function doReset() {
    if (!eng) return;
    setBusy(true);
    setErr(null);
    try {
      await eng.reset();
      setConfirmReset(false);
      setPp("");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="reef-bg grid min-h-screen place-items-center bg-bg px-6">
      <div className="panel w-full max-w-[420px] shadow-pop">
        <h1 className="font-display text-[26px] leading-[1.15]">
          {isSetup ? (
            <>
              Set a <em className="italic text-brand">private-workspace</em> passphrase
            </>
          ) : (
            <>
              Enroll this <em className="italic text-brand">device</em>
            </>
          )}
        </h1>
        <p className="mb-5 mt-2 text-sm leading-[1.55] text-ink-2">
          {isSetup
            ? "Encrypts your private items on-device. You'll only need this passphrase again to add a new device — this one stays unlocked. The server never sees it or your private content."
            : "Enter your private-workspace passphrase once to unlock it on this device. After this, this device stays unlocked."}
        </p>
        <input
          className="field"
          type="password"
          placeholder="passphrase"
          value={pp}
          onChange={(e) => setPp(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && go()}
          autoFocus
        />
        <button type="button" className="btn-primary mt-3 w-full" disabled={busy || !pp} onClick={go}>
          {busy ? "…" : isSetup ? "Create & unlock →" : "Unlock this device →"}
        </button>
        {err && <p className="mt-3 text-sm text-bad">{err}</p>}

        {!isSetup && (
          <div className="mt-4 border-t border-line pt-3">
            {confirmReset ? (
              <div className="text-[12px] text-ink-2">
                <p className="mb-2">
                  Reset discards the private-workspace key everywhere — locally-stored private items
                  become unreadable. Continue?
                </p>
                <div className="flex gap-2">
                  <button type="button" className="btn-secondary text-[12px] text-bad" disabled={busy} onClick={doReset}>
                    Reset & start over
                  </button>
                  <button type="button" className="btn-secondary text-[12px]" onClick={() => setConfirmReset(false)}>
                    Cancel
                  </button>
                </div>
              </div>
            ) : (
              <button
                type="button"
                className="font-mono text-[11px] text-ink-3 transition-colors hover:text-brand-2"
                onClick={() => setConfirmReset(true)}
              >
                Forgot your passphrase?
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
