/* CoralSwarm mobile — core: device wiring, top bar, bottom tab nav, router,
   Ask screens (home / answer / across-oceans), composer + bottom sheets.
   Reuses window data/icons/atoms from shell-shared.jsx. IIFE-scoped;
   pages register into window.__m and screens into window.__mscreens. */
(function () {
  const { useState, useRef, useEffect } = React;
  const DS = window.CoralSwarmDesignSystem_91cd0b;
  window.__m = window.__m || {};

  /* ── tokens for the phone ── */
  const SAFE_BOTTOM = 26;            // home indicator clearance
  const TABBAR_H = 64;

  const TABS = [
    { key: "ask", label: "Ask", hue: "coral", Icon: window.IconAsk },
    { key: "reefs", label: "Reefs", hue: "teal", Icon: window.IconReefs },
    { key: "review", label: "Review", hue: "amber", Icon: window.IconReview, dot: true },
    { key: "currents", label: "Currents", hue: "plum", Icon: window.IconCurrents },
    { key: "more", label: "More", hue: "pink", Icon: IconDots },
  ];
  function IconDots(p) { return <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" {...p}><circle cx="5" cy="12" r="1.4" fill="currentColor" stroke="none" /><circle cx="12" cy="12" r="1.4" fill="currentColor" stroke="none" /><circle cx="19" cy="12" r="1.4" fill="currentColor" stroke="none" /></svg>; }
  const IconBack = (p) => <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M15 18l-6-6 6-6" /></svg>;

  /* ── shared: bottom sheet ── */
  function Sheet({ open, onClose, title, children }) {
    if (!open) return null;
    return (
      <div style={{ position: "absolute", inset: 0, zIndex: 80, display: "flex", flexDirection: "column", justifyContent: "flex-end" }}>
        <div onClick={onClose} style={{ position: "absolute", inset: 0, background: "color-mix(in srgb, var(--ink) 32%, transparent)", animation: "mfade var(--dur) var(--ease)" }} />
        <div style={{ position: "relative", background: "var(--bg)", borderRadius: "22px 22px 0 0", padding: `14px 16px ${SAFE_BOTTOM + 12}px`, boxShadow: "var(--shadow-lg)", animation: "mrise 0.26s var(--ease)" }}>
          <div style={{ width: 38, height: 4, borderRadius: 999, background: "var(--bg-3)", margin: "0 auto 14px" }} />
          {title && <div style={{ fontFamily: "var(--mono)", fontSize: 10.5, letterSpacing: "0.16em", textTransform: "uppercase", color: "var(--ink-3)", marginBottom: 12, paddingLeft: 4 }}>{title}</div>}
          {children}
        </div>
      </div>
    );
  }

  function OceanSheet({ open, onClose, oceans, setOceans }) {
    return (
      <Sheet open={open} onClose={onClose} title="ask across oceans">
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          {window.OCEANS.map((o) => {
            const on = oceans[o.name];
            return (
              <button key={o.name} onClick={() => setOceans((s) => ({ ...s, [o.name]: !s[o.name] }))} style={{ display: "flex", alignItems: "center", gap: 12, padding: "12px 10px", borderRadius: "var(--radius-md)", border: "none", background: on ? "var(--bg-2)" : "transparent", cursor: "pointer", textAlign: "left" }}>
                <span style={{ width: 22, height: 22, borderRadius: 7, flex: "none", display: "grid", placeItems: "center", background: on ? `var(--hue-${o.hue})` : "var(--bg)", border: `1.5px solid ${on ? `var(--hue-${o.hue})` : "var(--line)"}`, color: "#fff", fontSize: 13 }}>{on ? "✓" : ""}</span>
                <window.HueDot hue={o.hue} size={9} />
                <span style={{ flex: 1, fontSize: 16, color: "var(--ink)" }}>{o.name}</span>
                <span style={{ fontFamily: "var(--mono)", fontSize: 10, letterSpacing: "0.08em", textTransform: "uppercase", color: "var(--ink-3)" }}>{o.badge}</span>
              </button>
            );
          })}
        </div>
        <DS.Button variant="primary" size="md" style={{ width: "100%", marginTop: 14 }} onClick={onClose}>Done</DS.Button>
      </Sheet>
    );
  }

  const MODELS = [
    { id: "pro", name: "swarm-pro", desc: "Most thorough · cross-ocean synthesis", dot: "coral" },
    { id: "fast", name: "swarm-fast", desc: "Quick answers, lighter retrieval", dot: "amber" },
    { id: "device", name: "on-device", desc: "Private · never leaves your machine", dot: "teal" },
  ];
  function ModelSheet({ open, onClose, model, setModel }) {
    return (
      <Sheet open={open} onClose={onClose} title="model">
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {MODELS.map((x) => (
            <button key={x.id} onClick={() => { setModel(x.id); onClose(); }} style={{ display: "flex", alignItems: "center", gap: 12, padding: "13px 12px", borderRadius: "var(--radius-lg)", border: `1px solid ${x.id === model ? "var(--brand)" : "var(--line)"}`, background: x.id === model ? "color-mix(in srgb, var(--brand) 7%, var(--bg))" : "var(--bg)", cursor: "pointer", textAlign: "left", boxShadow: x.id === model ? "var(--ring)" : "none" }}>
              <window.HueDot hue={x.dot} size={9} />
              <span style={{ flex: 1 }}>
                <span style={{ display: "block", fontSize: 15, fontWeight: 500, color: "var(--ink)" }}>{x.name}</span>
                <span style={{ display: "block", fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)", marginTop: 2 }}>{x.desc}</span>
              </span>
              {x.id === model && <span style={{ color: "var(--brand-2)", fontSize: 16 }}>✓</span>}
            </button>
          ))}
        </div>
      </Sheet>
    );
  }

  /* ── mobile composer (pinned) ── */
  function MComposer({ placeholder, model, setModel, oceans, setOceans, onSend, big }) {
    const [sheet, setSheet] = useState(null);
    const chosen = window.OCEANS.filter((o) => oceans[o.name]);
    const oceanLabel = chosen.length <= 1 ? (chosen[0]?.name || "No ocean") : `${chosen[0].name} +${chosen.length - 1}`;
    const m = MODELS.find((x) => x.id === model);
    return (
      <div style={{ borderRadius: 22, background: "var(--bg)", border: "1px solid var(--line)", boxShadow: "var(--shadow-lg)", padding: 7 }}>
        <div onClick={onSend} style={{ display: "flex", alignItems: "center", gap: 10, padding: big ? "12px 8px 10px 14px" : "9px 6px 8px 13px", cursor: "pointer" }}>
          <window.IconAsk width={18} height={18} style={{ color: "var(--ink-3)", flex: "none" }} />
          <span style={{ flex: 1, fontFamily: "var(--display)", fontSize: big ? 19 : 17, fontStyle: "italic", color: "var(--ink-3)" }}>{placeholder}</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 7, padding: "0 6px 4px" }}>
          <button onClick={() => setSheet("ocean")} style={chipS}><window.HueDot hue={chosen[0]?.hue || "coral"} size={7} />{oceanLabel}<span style={{ fontFamily: "var(--mono)", fontSize: 10, color: "var(--ink-3)" }}>▾</span></button>
          <button onClick={() => setSheet("model")} style={chipS}>{m.name}<span style={{ fontFamily: "var(--mono)", fontSize: 10, color: "var(--ink-3)" }}>▾</span></button>
          <span onClick={onSend} style={{ marginLeft: "auto", width: 36, height: 36, borderRadius: 12, flex: "none", display: "grid", placeItems: "center", background: "var(--brand)", color: "#fff", boxShadow: "var(--shadow-md)", cursor: "pointer" }}><window.IconUp width={17} height={17} /></span>
        </div>
        <OceanSheet open={sheet === "ocean"} onClose={() => setSheet(null)} oceans={oceans} setOceans={setOceans} />
        <ModelSheet open={sheet === "model"} onClose={() => setSheet(null)} model={model} setModel={setModel} />
      </div>
    );
  }
  const chipS = { display: "inline-flex", alignItems: "center", gap: 6, padding: "6px 10px", borderRadius: "var(--radius-pill)", fontFamily: "var(--sans)", fontSize: 12, cursor: "pointer", whiteSpace: "nowrap", background: "var(--bg-2)", border: "1px solid var(--line)", color: "var(--ink-2)" };

  /* ════ ASK ════ */
  function AskHome({ go, oceans, setOceans, model, setModel }) {
    const recent = [
      { q: "What did we decide about auth?", hue: "coral", meta: "12 turns · today", k: "decision" },
      { q: "How did we speed up embeddings?", hue: "teal", meta: "5 turns · yesterday", k: "meeting" },
      { q: "Summarize the Henderson reef", hue: "plum", meta: "3 turns · Mon", k: "system" },
    ];
    return (
      <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
        <div style={{ flex: 1, overflowY: "auto", padding: "8px 18px 16px" }}>
          <div style={{ textAlign: "center", padding: "22px 0 18px" }}>
            <div style={{ fontFamily: "var(--mono)", fontSize: 10, letterSpacing: "0.18em", textTransform: "uppercase", color: "var(--ink-3)", marginBottom: 12 }}>documentation, by the swarm</div>
            <div style={{ fontFamily: "var(--display)", fontSize: 40, lineHeight: 1.02, letterSpacing: "-0.02em", color: "var(--ink)" }}>Ask your <em style={{ fontStyle: "italic", color: "var(--brand)" }}>ocean</em></div>
          </div>
          <div style={{ display: "flex", gap: 8, overflowX: "auto", paddingBottom: 6, margin: "0 -18px", padding: "0 18px 4px", scrollbarWidth: "none" }}>
            {window.SUGGESTIONS.map((s) => (
              <button key={s} onClick={() => go("answer")} style={{ flex: "none", padding: "8px 14px", borderRadius: "var(--radius-pill)", fontFamily: "var(--mono)", fontSize: 11.5, color: "var(--ink-2)", background: "var(--bg)", border: "1px solid var(--line)", whiteSpace: "nowrap", cursor: "pointer" }}>{s}</button>
            ))}
          </div>
          <button onClick={() => go("oceans")} style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 8, width: "100%", marginTop: 10, padding: "11px", borderRadius: "var(--radius-pill)", border: "1px dashed color-mix(in srgb, var(--brand) 40%, var(--line))", background: "color-mix(in srgb, var(--brand) 6%, var(--bg))", color: "var(--brand-2)", fontSize: 13, fontFamily: "var(--sans)", cursor: "pointer" }}>
            <span style={{ display: "inline-flex" }}><window.HueDot hue="coral" size={6} /><window.HueDot hue="teal" size={6} style={{ marginLeft: -2 }} /><window.HueDot hue="plum" size={6} style={{ marginLeft: -2 }} /></span>
            Ask across all oceans
          </button>

          <div style={{ display: "flex", alignItems: "center", gap: 10, margin: "22px 0 12px" }}>
            <span style={{ fontFamily: "var(--mono)", fontSize: 10, letterSpacing: "0.16em", textTransform: "uppercase", color: "var(--ink-3)" }}>jump back in</span>
            <span style={{ flex: 1, height: 1, background: "var(--line)" }} />
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {recent.map((c) => (
              <button key={c.q} onClick={() => go("answer")} style={{ textAlign: "left", borderRadius: 16, background: "var(--bg)", border: "1px solid var(--line)", boxShadow: "var(--shadow-sm)", padding: 15, cursor: "pointer" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 7, marginBottom: 8, fontFamily: "var(--mono)", fontSize: 9, letterSpacing: "0.12em", textTransform: "uppercase", color: "var(--ink-3)" }}><window.HueDot hue={c.hue} size={7} /> {c.k}</div>
                <div style={{ fontFamily: "var(--display)", fontSize: 19, fontStyle: "italic", lineHeight: 1.25, color: "var(--ink)", marginBottom: 9 }}>{c.q}</div>
                <div style={{ fontFamily: "var(--mono)", fontSize: 10, color: "var(--ink-3)" }}>{c.meta}</div>
              </button>
            ))}
          </div>
        </div>
        <div style={{ flex: "none", padding: "8px 16px 12px", background: "linear-gradient(to top, var(--bg) 70%, transparent)" }}>
          <MComposer placeholder="Ask anything…" big model={model} setModel={setModel} oceans={oceans} setOceans={setOceans} onSend={() => go("answer")} />
        </div>
      </div>
    );
  }

  function AskAnswer({ oceans, setOceans, model, setModel }) {
    const [hot, setHot] = useState(null);
    return (
      <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
        <div style={{ flex: 1, overflowY: "auto", padding: "10px 18px 16px" }}>
          <div style={{ display: "flex", gap: 11, alignItems: "flex-start", marginBottom: 20 }}>
            <window.Avatar hue="pink" size={26}>you</window.Avatar>
            <div style={{ fontFamily: "var(--display)", fontSize: 22, fontStyle: "italic", lineHeight: 1.3, color: "var(--ink)", paddingTop: 1 }}>{window.QUESTION}</div>
          </div>
          <div style={{ display: "flex", gap: 11, alignItems: "flex-start" }}>
            <span style={{ width: 26, height: 26, borderRadius: 8, flex: "none", display: "grid", placeItems: "center", background: "var(--ink)", marginTop: 1 }}><span style={{ width: 15, height: 15, display: "block" }}><DS.DotMandala size={15} density={0.7} /></span></span>
            <div style={{ minWidth: 0, flex: 1 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 9, flexWrap: "wrap" }}>
                <DS.Badge hue="coral" style={{ fontSize: 10.5, padding: "2px 8px" }}>synthesized</DS.Badge>
                <span style={{ fontFamily: "var(--mono)", fontSize: 9.5, color: "var(--ink-3)" }}>swarm-pro · 2 oceans · 1.20s</span>
              </div>
              <div><window.Answer hot={hot} setHot={setHot} size={15.5} /></div>
              <div style={{ marginTop: 16 }}>
                <div style={{ fontFamily: "var(--mono)", fontSize: 10, letterSpacing: "0.14em", textTransform: "uppercase", color: "var(--ink-3)", marginBottom: 8 }}>cited corals · 3</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
                  {window.CITES.map((c) => {
                    const on = hot === c.n;
                    return (
                      <div key={c.n} onClick={() => setHot(on ? null : c.n)} style={{ borderRadius: "var(--radius-md)", background: on ? "color-mix(in srgb, var(--brand-3) 40%, var(--bg))" : "var(--bg)", border: "1px solid var(--line)", borderLeft: `2px solid var(--hue-${c.hue})`, padding: "10px 12px", cursor: "pointer" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 7, marginBottom: 5 }}>
                          <span style={{ display: "grid", placeItems: "center", width: 16, height: 16, borderRadius: 5, fontFamily: "var(--mono)", fontSize: 10, background: on ? "var(--brand)" : "var(--brand-3)", color: on ? "#fff" : "var(--brand-2)" }}>{c.n}</span>
                          <span style={{ fontFamily: "var(--mono)", fontSize: 9, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--ink-3)" }}>{c.kind}</span>
                        </div>
                        <div style={{ fontSize: 13, lineHeight: 1.4, color: "var(--ink)", marginBottom: 5 }}>{c.label}</div>
                        <div style={{ fontFamily: "var(--mono)", fontSize: 9.5, color: "var(--ink-3)" }}>{c.meta}</div>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>
        </div>
        <div style={{ flex: "none", padding: "8px 16px 12px", background: "linear-gradient(to top, var(--bg) 70%, transparent)" }}>
          <MComposer placeholder="Ask a follow-up…" model={model} setModel={setModel} oceans={oceans} setOceans={setOceans} onSend={() => {}} />
        </div>
      </div>
    );
  }

  const LANES = [
    { ocean: "Henderson", hue: "coral", badge: "primary", sources: [
      { n: 1, kind: "decision", hue: "coral", label: "Adopt Clerk for all auth surfaces", meta: "decision · 03-14" },
      { n: 2, kind: "meeting", hue: "teal", label: "Security review — auth options", meta: "zoom · 03-09" },
    ] },
    { ocean: "Core Platform", hue: "teal", badge: "team", sources: [
      { n: 3, kind: "slack", hue: "amber", label: "desktop should use device-code, not a webview", meta: "#eng-desktop · 03-11" },
      { n: 4, kind: "decision", hue: "coral", label: "Session store moved to Redis", meta: "decision · 04-02" },
    ] },
    { ocean: "Acme", hue: "plum", badge: "org", sources: [
      { n: 5, kind: "reference", hue: "plum", label: "SSO rollout plan — Q2", meta: "notion · 05-20" },
    ] },
  ];
  const BLEND = [
    { t: "Across your three oceans the answer converges: " }, { b: "Clerk for every auth surface" }, { t: ", with org-scoped sessions" }, { c: 1 },
    { t: ". Henderson ruled out a custom build after the March security review" }, { c: 2 }, { t: "; Core Platform settled the desktop flow on a device-code grant" }, { c: 3 },
    { t: " and moved the session store to Redis" }, { c: 4 }, { t: "; and Acme's SSO rollout is slated for Q2 on the same Clerk org mapping" }, { c: 5 }, { t: "." },
  ];
  function AskOceans({ oceans, setOceans, model, setModel }) {
    const [hot, setHot] = useState(null);
    const Cc = ({ n }) => { const on = hot === n; return <span onClick={() => setHot(on ? null : n)} style={{ display: "inline-flex", alignItems: "center", justifyContent: "center", minWidth: 16, height: 16, padding: "0 5px", margin: "0 1px", borderRadius: 5, fontFamily: "var(--mono)", fontSize: 10, verticalAlign: "1px", cursor: "pointer", background: on ? "var(--brand)" : "var(--brand-3)", color: on ? "#fff" : "var(--brand-2)" }}>{n}</span>; };
    return (
      <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
        <div style={{ flex: 1, overflowY: "auto", padding: "10px 18px 16px" }}>
          <div style={{ display: "flex", gap: 11, alignItems: "flex-start", marginBottom: 18 }}>
            <window.Avatar hue="pink" size={26}>you</window.Avatar>
            <div style={{ fontFamily: "var(--display)", fontSize: 21, fontStyle: "italic", lineHeight: 1.3, color: "var(--ink)" }}>What did we decide about auth across the org?</div>
          </div>
          <div style={{ display: "flex", gap: 11, alignItems: "flex-start", marginBottom: 22 }}>
            <span style={{ width: 26, height: 26, borderRadius: 8, flex: "none", display: "grid", placeItems: "center", background: "var(--ink)", marginTop: 1 }}><span style={{ width: 15, height: 15, display: "block" }}><DS.DotMandala size={15} density={0.7} /></span></span>
            <div style={{ minWidth: 0, flex: 1 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 9, flexWrap: "wrap" }}>
                <DS.Badge hue="coral" style={{ fontSize: 10.5, padding: "2px 8px" }}>blended</DS.Badge>
                <span style={{ fontFamily: "var(--mono)", fontSize: 9.5, color: "var(--ink-3)" }}>5 sources · 3 oceans · 1.84s</span>
              </div>
              <div style={{ fontFamily: "var(--sans)", fontSize: 15.5, lineHeight: 1.62, color: "var(--ink)" }}>
                {BLEND.map((p, i) => p.c != null ? <Cc key={i} n={p.c} /> : p.b ? <strong key={i} style={{ fontWeight: 600 }}>{p.b}</strong> : <span key={i}>{p.t}</span>)}
              </div>
            </div>
          </div>
          <div style={{ fontFamily: "var(--mono)", fontSize: 10, letterSpacing: "0.14em", textTransform: "uppercase", color: "var(--ink-3)", marginBottom: 12 }}>drawn from 3 oceans</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {LANES.map((lane) => (
              <div key={lane.ocean} style={{ borderRadius: "var(--radius-lg)", background: "var(--bg-2)", border: "1px solid var(--line)", borderTop: `3px solid var(--hue-${lane.hue})`, padding: "13px 13px 14px" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 11 }}>
                  <window.HueDot hue={lane.hue} size={9} />
                  <span style={{ flex: 1, fontSize: 14.5, fontWeight: 500, color: "var(--ink)" }}>{lane.ocean}</span>
                  <span style={{ fontFamily: "var(--mono)", fontSize: 9, letterSpacing: "0.08em", textTransform: "uppercase", color: "var(--ink-3)" }}>{lane.badge}</span>
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
                  {lane.sources.map((s) => {
                    const on = hot === s.n;
                    return (
                      <div key={s.n} onClick={() => setHot(on ? null : s.n)} style={{ borderRadius: "var(--radius-md)", background: "var(--bg)", border: "1px solid var(--line)", borderLeft: `2px solid var(--hue-${s.hue})`, padding: "9px 11px", cursor: "pointer", boxShadow: on ? "var(--shadow-md)" : "none" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 5 }}>
                          <span style={{ display: "grid", placeItems: "center", width: 15, height: 15, borderRadius: 5, fontFamily: "var(--mono)", fontSize: 9.5, background: on ? "var(--brand)" : "var(--brand-3)", color: on ? "#fff" : "var(--brand-2)" }}>{s.n}</span>
                          <span style={{ fontFamily: "var(--mono)", fontSize: 8.5, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--ink-3)" }}>{s.kind}</span>
                        </div>
                        <div style={{ fontSize: 12.5, lineHeight: 1.4, color: "var(--ink)", marginBottom: 5 }}>{s.label}</div>
                        <div style={{ fontFamily: "var(--mono)", fontSize: 9, color: "var(--ink-3)" }}>{s.meta}</div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        </div>
        <div style={{ flex: "none", padding: "8px 16px 12px", background: "linear-gradient(to top, var(--bg) 70%, transparent)" }}>
          <MComposer placeholder="Ask across oceans…" model={model} setModel={setModel} oceans={oceans} setOceans={setOceans} onSend={() => {}} />
        </div>
      </div>
    );
  }

  /* Ask router: home → answer / oceans, with a back affordance handled by top bar */
  function AskScreen({ sub, setSub, oceans, setOceans, model, setModel }) {
    const go = (s) => setSub(s);
    if (sub === "answer") return <AskAnswer oceans={oceans} setOceans={setOceans} model={model} setModel={setModel} />;
    if (sub === "oceans") return <AskOceans oceans={oceans} setOceans={setOceans} model={model} setModel={setModel} />;
    return <AskHome go={go} oceans={oceans} setOceans={setOceans} model={model} setModel={setModel} />;
  }

  Object.assign(window.__m, { AskScreen, Sheet, OceanSheet, ModelSheet, IconBack, TABS, SAFE_BOTTOM, TABBAR_H });
})();
