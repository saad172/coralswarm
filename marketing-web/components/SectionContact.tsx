"use client";

import Script from "next/script";
import { useEffect, useState } from "react";
import { DotMandala } from "./logos";
import { captureFirstTouch, readFirstTouch } from "./firstTouch";

/* Block 7 — the single contact form. It joins the waitlist AND emails
 * the founder, so it posts to /api/contact — see worker/index.ts. The
 * First/Last inputs are joined client-side into the single `name` the
 * worker, the D1 tables and the notification email already expect.
 *
 * Turnstile guards the endpoint. The message field is free text on an
 * unauthenticated POST that ends up in a human's inbox, so a bare
 * per-IP rate limit is not enough on its own.
 *
 * The site key falls back to Cloudflare's documented always-passes TEST
 * key so `next dev` works with no configuration. Production must set
 * NEXT_PUBLIC_TURNSTILE_SITE_KEY; the Worker independently fails closed
 * when its own secret is missing, so shipping without real keys breaks
 * the form loudly rather than silently disabling the check. */

const TURNSTILE_TEST_SITE_KEY = "1x00000000000000000000AA";
const SITE_KEY = process.env.NEXT_PUBLIC_TURNSTILE_SITE_KEY || TURNSTILE_TEST_SITE_KEY;

type Scope = "personal" | "enterprise";
type Status = "idle" | "submitting" | "error" | "done";

function segStyle(on: boolean): React.CSSProperties {
  return {
    flex: "1 1 150px",
    display: "flex",
    flexDirection: "column",
    gap: 4,
    alignItems: "flex-start",
    textAlign: "left",
    padding: "12px 14px",
    cursor: "pointer",
    borderRadius: "var(--r-control)",
    transition: "var(--transition-control)",
    border: `1px solid ${on ? "var(--brand)" : "var(--line)"}`,
    background: on ? "var(--brand-soft)" : "var(--bg)",
    color: on ? "var(--ink)" : "var(--ink-2)",
    boxShadow: on ? "var(--shadow-row)" : "none",
  };
}

const labelStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: 5,
  font: "500 11.5px/1 var(--sans)",
  color: "var(--ink-2)",
};

export function SectionContact() {
  const [scope, setScope] = useState<Scope>("personal");
  const [status, setStatus] = useState<Status>("idle");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    captureFirstTouch();
  }, []);

  const enterprise = scope === "enterprise";
  const messageLabel = enterprise
    ? "What are you hoping this solves for your team?"
    : "What would you ask it first?";

  async function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    if (status === "submitting") return;

    const form = e.currentTarget;
    const data = new FormData(form);
    const token = String(data.get("cf-turnstile-response") || "");

    if (!token) {
      setError("Verification is still running — give it a second and try again.");
      setStatus("error");
      return;
    }

    setStatus("submitting");
    setError(null);

    try {
      const ft = readFirstTouch();
      const res = await fetch("/api/contact", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: [data.get("first_name"), data.get("last_name")]
            .map((v) => String(v || "").trim())
            .filter(Boolean)
            .join(" "),
          email: String(data.get("email") || ""),
          message: String(data.get("message") || ""),
          scope,
          turnstile_token: token,
          utm_source: ft?.utm_source ?? null,
          utm_medium: ft?.utm_medium ?? null,
          utm_campaign: ft?.utm_campaign ?? null,
          referrer: ft?.referrer ?? null,
          landing_path: ft?.landing_path ?? null,
          first_seen_at: ft?.first_seen ?? null,
        }),
      });

      const body = (await res.json().catch(() => null)) as { ok?: boolean; error?: string } | null;

      if (!res.ok || !body?.ok) {
        setError(body?.error ?? "Something went wrong. Please try again.");
        setStatus("error");
        // A consumed Turnstile token cannot be reused, so re-arm the
        // widget before the visitor's next attempt.
        window.turnstile?.reset();
        return;
      }

      setStatus("done");
    } catch {
      setError("Network error — please try again.");
      setStatus("error");
      window.turnstile?.reset();
    }
  }

  return (
    <section id="contact" style={{ background: "var(--bg)", borderTop: "1px solid var(--line)" }}>
      <Script src="https://challenges.cloudflare.com/turnstile/v0/api.js" strategy="lazyOnload" />

      <div className="cs-band">
        <div style={{ display: "flex", flexWrap: "wrap", gap: 48, alignItems: "flex-start" }}>
          <div style={{ flex: "1 1 420px", maxWidth: 600 }}>
            <div data-reveal className="cs-eyebrow">ready to &ldquo;dive&rdquo; in?</div>
            <h2
              data-reveal
              data-delay="80"
              style={{
                font: "400 clamp(28px,3.4vw,36px)/1.2 var(--display)",
                letterSpacing: "var(--tracking-title)",
                color: "var(--ink)",
                margin: "14px 0 20px",
              }}
            >
              Contact us.
            </h2>

            <div
              data-reveal
              data-delay="200"
              style={{
                background: "var(--bg)",
                border: "1px solid var(--line)",
                borderRadius: "var(--r-card)",
                boxShadow: "var(--shadow-card)",
                padding: 24,
              }}
            >
              {status === "done" ? (
                <div>
                  <div style={{ font: "400 20px/1.35 var(--display)", color: "var(--ink)" }}>
                    You&apos;re on the list — and I&apos;ll write back within one working day.
                  </div>
                  <div style={{ font: "400 12px/1.7 var(--mono)", color: "var(--ink-3)", marginTop: 10 }}>
                    one invite email when it&apos;s your turn, nothing in between. no sequence, no drip.
                  </div>
                </div>
              ) : (
                <form onSubmit={onSubmit} style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                  <div>
                    <div className="cs-eyebrow">use case&hellip;</div>
                    <div style={{ display: "flex", gap: 10, marginTop: 10, flexWrap: "wrap" }}>
                      <button
                        type="button"
                        aria-pressed={!enterprise}
                        onClick={() => setScope("personal")}
                        style={segStyle(!enterprise)}
                      >
                        <span style={{ font: "500 13px/1.3 var(--sans)" }}>Personal</span>
                      </button>
                      <button
                        type="button"
                        aria-pressed={enterprise}
                        onClick={() => setScope("enterprise")}
                        style={segStyle(enterprise)}
                      >
                        <span style={{ font: "500 13px/1.3 var(--sans)" }}>Enterprise</span>
                      </button>
                    </div>
                  </div>

                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                    <label style={{ ...labelStyle, flex: "1 1 150px" }}>
                      First name
                      <input name="first_name" type="text" required placeholder="First" className="cs-field" />
                    </label>
                    <label style={{ ...labelStyle, flex: "1 1 150px" }}>
                      Last name
                      <input name="last_name" type="text" required placeholder="Last" className="cs-field" />
                    </label>
                  </div>

                  <label style={labelStyle}>
                    Email
                    <input
                      name="email"
                      type="email"
                      required
                      placeholder="you@company.com"
                      className="cs-field"
                    />
                  </label>

                  <label style={labelStyle}>
                    {messageLabel}
                    <textarea
                      name="message"
                      rows={3}
                      placeholder="Optional — the messier the better."
                      className="cs-field"
                      style={{ lineHeight: 1.55, resize: "vertical" }}
                    />
                  </label>

                  <div className="cf-turnstile" data-sitekey={SITE_KEY} data-appearance="interaction-only" />

                  <div>
                    <button
                      type="submit"
                      className="cs-btn-solid"
                      disabled={status === "submitting"}
                      style={{
                        borderRadius: "var(--r-field)",
                        font: "500 13.5px/1 var(--sans)",
                        padding: "11px 18px",
                        opacity: status === "submitting" ? 0.7 : 1,
                      }}
                    >
                      {status === "submitting" ? "Sending…" : "Join the waitlist"}
                    </button>
                  </div>

                  {status === "error" && error && (
                    <div role="alert" style={{ font: "400 11px/1.6 var(--mono)", color: "var(--bad)" }}>
                      {error}
                    </div>
                  )}
                </form>
              )}
            </div>
          </div>

          <div
            data-reveal
            data-delay="260"
            style={{
              flex: "1 1 260px",
              maxWidth: 340,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              alignSelf: "stretch",
              padding: "12px 0",
            }}
          >
            <div className="cs-orbit-mandala">
              <DotMandala size={260} />
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
