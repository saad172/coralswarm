/* CoralSwarm mobile — pages (Reefs, Review, Currents, More + workspace),
   the shell (top bar, bottom tab bar) and the root MobileApp router.
   IIFE-scoped. Exports window.MobileApp. */
(function () {
  const { useState } = React;
  const DS = window.CoralSwarmDesignSystem_91cd0b;
  const M = window.__m;
  const { IconBack, TABS, SAFE_BOTTOM, TABBAR_H } = M;

  const Eyebrow = ({ children }) => <div style={{ fontFamily: "var(--mono)", fontSize: 10, letterSpacing: "0.18em", textTransform: "uppercase", color: "var(--ink-3)", marginBottom: 8 }}>{children}</div>;
  const Title = ({ children }) => <div style={{ fontFamily: "var(--display)", fontSize: 32, lineHeight: 1.04, letterSpacing: "-0.02em", color: "var(--ink)" }}>{children}</div>;
  const Sub = ({ children }) => <p style={{ fontSize: 13.5, color: "var(--ink-2)", margin: "8px 0 0", lineHeight: 1.5 }}>{children}</p>;
  const SecLabel = ({ children, style }) => <div style={{ fontFamily: "var(--mono)", fontSize: 10, letterSpacing: "0.16em", textTransform: "uppercase", color: "var(--ink-3)", margin: "0 0 11px", ...style }}>{children}</div>;
  const ScrollPage = ({ children, pad = "14px 18px 24px" }) => <div style={{ height: "100%", overflowY: "auto", padding: pad }}>{children}</div>;

  function Chips({ items, active, onPick }) {
    return (
      <div style={{ display: "flex", gap: 7, overflowX: "auto", padding: "0 0 4px", margin: "14px 0 16px", scrollbarWidth: "none" }}>
        {items.map((t) => {
          const on = (t.key || t) === active;
          return (
            <button key={t.key || t} onClick={() => onPick(t.key || t)} style={{ flex: "none", display: "inline-flex", alignItems: "center", gap: 6, padding: "7px 13px", borderRadius: "var(--radius-pill)", cursor: "pointer", fontFamily: "var(--sans)", fontSize: 12.5, whiteSpace: "nowrap", fontWeight: on ? 500 : 400, background: on ? "var(--ink)" : "var(--bg)", color: on ? "var(--bg)" : "var(--ink-2)", border: `1px solid ${on ? "var(--ink)" : "var(--line)"}` }}>
              {t.hue && <window.HueDot hue={t.hue} size={6} />}{t.label || t}{t.count != null && <span style={{ fontFamily: "var(--mono)", fontSize: 10, opacity: 0.7 }}>{t.count}</span>}
            </button>
          );
        })}
      </div>
    );
  }

  function Toggle({ on: init }) {
    const [on, setOn] = useState(init);
    return (
      <button onClick={() => setOn((v) => !v)} style={{ width: 42, height: 24, borderRadius: 999, border: "none", cursor: "pointer", padding: 2, flex: "none", display: "flex", justifyContent: on ? "flex-end" : "flex-start", background: on ? "var(--brand)" : "var(--bg-3)", transition: "background var(--dur) var(--ease)" }}>
        <span style={{ width: 20, height: 20, borderRadius: "50%", background: "#fff", boxShadow: "var(--shadow-sm)" }} />
      </button>
    );
  }

  /* ════ REEFS ════ */
  const REEFS = [
    { kind: "decision", hue: "coral", title: "Auth standardization", body: "Clerk adopted across web + desktop, with org-scoped sessions.", prov: "8 corals · 2d ago" },
    { kind: "entity", hue: "pink", title: "Clerk", body: "Auth provider. Owns sessions, org mapping, device-code grant.", prov: "14 corals · 3 reefs" },
    { kind: "thread", hue: "amber", title: "#eng-desktop", body: "Desktop build — auth, auto-update, packaging.", prov: "212 corals · 4h ago" },
    { kind: "reference", hue: "plum", title: "ADR-0007 · device auth", body: "Decision record for the desktop auth flow.", prov: "1 doc · 2 decisions" },
    { kind: "decision", hue: "coral", title: "Embedding pipeline v2", body: "Batched ONNX embeddings; 3.4× throughput.", prov: "6 corals · 1w ago" },
    { kind: "entity", hue: "pink", title: "Henderson", body: "Design-partner customer. Backs the primary ocean.", prov: "40 corals" },
  ];
  const REEF_TABS = [{ key: "all", label: "All", count: 47 }, { key: "entity", label: "Entities", hue: "pink" }, { key: "decision", label: "Decisions", hue: "coral" }, { key: "thread", label: "Threads", hue: "amber" }, { key: "reference", label: "References", hue: "plum" }];
  function ReefsPage() {
    const [tab, setTab] = useState("all");
    const list = ["entity", "decision", "thread", "reference"].includes(tab) ? REEFS.filter((r) => r.kind === tab) : REEFS;
    return (
      <ScrollPage>
        <Eyebrow>browse · 47 reefs</Eyebrow>
        <Title>Reefs</Title>
        <Chips items={REEF_TABS} active={tab} onPick={setTab} />
        <div style={{ display: "flex", flexDirection: "column", gap: 11 }}>
          {list.map((r, i) => <DS.Card key={i} kind={`${r.kind} reef`} hue={r.hue} title={r.title} prov={r.prov}>{r.body}</DS.Card>)}
        </div>
      </ScrollPage>
    );
  }

  /* ════ REVIEW ════ */
  const CANDIDATES = [
    { title: "Default to swarm-pro for shared oceans", detail: "“Let's just make pro the default when a conversation is shared — fast is fine for solo.”", conf: 0.86, prov: "slack · #product · 06-18" },
    { title: "Sunset the legacy /v1 ingest endpoint", detail: "“Nothing's hit v1 in three weeks. We agreed to drop it next release.”", conf: 0.79, prov: "zoom · platform sync · 06-17" },
    { title: "On-device model ships to the free tier", detail: "“The on-device retrieval is good enough to give free users — let's commit.”", conf: 0.72, prov: "slack · #leadership · 06-15" },
  ];
  function ReviewPage() {
    return (
      <ScrollPage>
        <Eyebrow>review · 3 pending</Eyebrow>
        <Title>Review</Title>
        <Sub>Decisions the swarm surfaced. Confirm to save as corals, or dismiss.</Sub>
        <div style={{ display: "flex", flexDirection: "column", gap: 12, marginTop: 18 }}>
          {CANDIDATES.map((c, i) => (
            <div key={i} style={{ background: "var(--bg)", border: "1px solid var(--line)", borderLeft: "3px solid var(--hue-amber)", borderRadius: "var(--radius-lg)", padding: 16 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
                <span style={{ fontFamily: "var(--mono)", fontSize: 9, letterSpacing: "0.13em", textTransform: "uppercase", color: "var(--ink-3)", display: "inline-flex", alignItems: "center", gap: 6 }}><window.HueDot hue="amber" size={7} />candidate</span>
                <span style={{ marginLeft: "auto", fontFamily: "var(--mono)", fontSize: 10, color: "var(--ink-3)" }}>{Math.round(c.conf * 100)}% conf.</span>
              </div>
              <div style={{ fontFamily: "var(--display)", fontSize: 20, color: "var(--ink)", marginBottom: 6, lineHeight: 1.2 }}>{c.title}</div>
              <p style={{ fontFamily: "var(--display)", fontStyle: "italic", fontSize: 15, color: "var(--ink-2)", lineHeight: 1.45, margin: "0 0 13px" }}>{c.detail}</p>
              <div style={{ display: "flex", gap: 9 }}>
                <DS.Button variant="primary" size="sm" style={{ flex: 1 }}>✓ Confirm</DS.Button>
                <DS.Button variant="ghost" size="sm" style={{ flex: 1 }}>Dismiss</DS.Button>
              </div>
              <div style={{ fontFamily: "var(--mono)", fontSize: 9.5, color: "var(--ink-3)", marginTop: 10, textAlign: "center" }}>{c.prov}</div>
            </div>
          ))}
        </div>
      </ScrollPage>
    );
  }

  /* ════ CURRENTS ════ */
  const FEED = [
    { day: "Today", events: [
      { time: "09:42", hue: "coral", subj: "Decision confirmed", text: "Adopt Clerk for all auth surfaces", ocean: "Henderson" },
      { time: "09:10", hue: "teal", subj: "Meeting synced", text: "Security review — 41 min", ocean: "Henderson" },
      { time: "08:30", hue: "amber", subj: "3 candidates surfaced", text: "ready for review", ocean: "Core Platform" },
    ] },
    { day: "Yesterday", events: [
      { time: "17:20", hue: "plum", subj: "Reef merged", text: "#eng-desktop + Desktop build", ocean: "Core Platform" },
      { time: "14:05", hue: "teal", subj: "128 messages backfilled", text: "from Slack", ocean: "Acme" },
      { time: "11:30", hue: "pink", subj: "Lena offboarding started", text: "4 reefs reassigned", ocean: "Henderson" },
    ] },
  ];
  function CurrentsPage() {
    return (
      <ScrollPage>
        <Eyebrow>activity</Eyebrow>
        <Title>Currents</Title>
        <div style={{ marginTop: 18 }}>
          {FEED.map((grp) => (
            <div key={grp.day} style={{ marginBottom: 22 }}>
              <SecLabel>{grp.day}</SecLabel>
              <div style={{ position: "relative", paddingLeft: 20 }}>
                <span style={{ position: "absolute", left: 4, top: 8, bottom: 8, width: 1, background: "var(--line)" }} />
                <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                  {grp.events.map((e, i) => (
                    <div key={i} style={{ position: "relative" }}>
                      <span style={{ position: "absolute", left: -20, top: 4, width: 9, height: 9, borderRadius: "50%", background: `var(--hue-${e.hue})`, border: "2px solid var(--bg)" }} />
                      <div style={{ fontSize: 14, color: "var(--ink-2)", lineHeight: 1.4 }}><strong style={{ color: "var(--ink)", fontWeight: 600 }}>{e.subj}</strong> — {e.text}</div>
                      <div style={{ fontFamily: "var(--mono)", fontSize: 10, color: "var(--ink-3)", marginTop: 3, display: "flex", alignItems: "center", gap: 7 }}>{e.time}<window.HueDot hue={e.hue} size={5} />{e.ocean}</div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ))}
        </div>
      </ScrollPage>
    );
  }

  /* ════ WORKSPACE pages (reached from More) ════ */
  const CONNECTORS = [
    { name: "Slack", hue: "amber", on: true, info: "4 channels · 2m ago" },
    { name: "Zoom", hue: "teal", on: true, info: "auto-transcribe · 12 wk" },
    { name: "Notion", hue: "plum", on: false, info: "docs & databases" },
    { name: "Microsoft Teams", hue: "teal", on: true, info: "3 channels · 1h ago" },
    { name: "Google Drive", hue: "coral", on: false, info: "docs, slides, sheets" },
    { name: "Linear", hue: "plum", on: false, info: "issues & projects" },
  ];
  function ConnectorsPage() {
    return (
      <ScrollPage>
        <Eyebrow>workspace · 3 connected</Eyebrow>
        <Title>Connectors</Title>
        <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 18 }}>
          {CONNECTORS.map((c) => (
            <div key={c.name} style={{ display: "flex", alignItems: "center", gap: 12, padding: 14, borderRadius: "var(--radius-lg)", background: "var(--bg)", border: "1px solid var(--line)" }}>
              <span style={{ width: 38, height: 38, flex: "none", borderRadius: 10, display: "grid", placeItems: "center", fontFamily: "var(--mono)", fontSize: 15, fontWeight: 500, color: "#fff", background: `var(--hue-${c.hue})` }}>{c.name[0]}</span>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 14.5, fontWeight: 500, color: "var(--ink)" }}>{c.name}</div>
                <div style={{ fontFamily: "var(--mono)", fontSize: 10, color: "var(--ink-3)", marginTop: 2 }}>{c.info}</div>
              </div>
              {c.on ? <span style={{ fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ok)", display: "inline-flex", alignItems: "center", gap: 5 }}><span style={{ width: 7, height: 7, borderRadius: "50%", background: "var(--ok)" }} />on</span> : <DS.Button variant="secondary" size="sm">Connect</DS.Button>}
            </div>
          ))}
        </div>
      </ScrollPage>
    );
  }

  const PEOPLE = [
    { name: "Alex Khan", role: "Engineering lead", reefs: 12, hue: "coral", risk: null },
    { name: "Lena Ortiz", role: "Backend engineer", reefs: 4, hue: "pink", risk: "offboarding" },
    { name: "Marco Diaz", role: "Product", reefs: 7, hue: "teal", risk: null },
    { name: "Priya Nair", role: "Design", reefs: 3, hue: "amber", risk: null },
    { name: "Theo Reyes", role: "Data", reefs: 9, hue: "plum", risk: "low coverage" },
  ];
  function TeamPage() {
    return (
      <ScrollPage>
        <Eyebrow>workspace · 5 members</Eyebrow>
        <Title>Team</Title>
        <div style={{ background: "var(--bg)", border: "1px solid var(--line)", borderLeft: "3px solid var(--hue-pink)", borderRadius: "var(--radius-lg)", padding: 15, margin: "16px 0", display: "flex", alignItems: "center", gap: 13 }}>
          <window.Avatar hue="pink" size={38}>lo</window.Avatar>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 14, color: "var(--ink)" }}><strong style={{ fontWeight: 600 }}>Lena</strong> offboards in 6 days</div>
            <div style={{ fontSize: 12, color: "var(--ink-2)", marginTop: 2 }}>4 reefs need reassigning</div>
          </div>
          <DS.Button variant="primary" size="sm">Reassign</DS.Button>
        </div>
        <div style={{ background: "var(--bg)", border: "1px solid var(--line)", borderRadius: "var(--radius-lg)", overflow: "hidden" }}>
          {PEOPLE.map((p, i) => (
            <div key={p.name} style={{ display: "flex", alignItems: "center", gap: 12, padding: "13px 15px", borderTop: i ? "1px solid var(--line)" : "none" }}>
              <window.Avatar hue={p.hue} size={32}>{p.name.split(" ").map((w) => w[0]).join("").toLowerCase()}</window.Avatar>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 14, fontWeight: 500, color: "var(--ink)" }}>{p.name}</div>
                <div style={{ fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)" }}>{p.reefs} reefs · {p.role}</div>
              </div>
              {p.risk ? <DS.Badge hue={p.risk === "offboarding" ? "pink" : "amber"} variant="outline">{p.risk}</DS.Badge> : <span style={{ width: 8, height: 8, borderRadius: "50%", background: "var(--ok)" }} />}
            </div>
          ))}
        </div>
      </ScrollPage>
    );
  }

  function AddPage() {
    const [tab, setTab] = useState("paste");
    return (
      <ScrollPage>
        <Eyebrow>workspace</Eyebrow>
        <Title>Add corals</Title>
        <Chips items={[{ key: "paste", label: "Paste" }, { key: "upload", label: "Upload" }, { key: "connect", label: "From a source" }]} active={tab} onPick={setTab} />
        {tab === "upload" ? (
          <div style={{ border: "1.5px dashed var(--line)", borderRadius: "var(--radius-lg)", padding: "40px 18px", textAlign: "center", background: "var(--bg-2)" }}>
            <div style={{ fontFamily: "var(--display)", fontSize: 20, fontStyle: "italic", color: "var(--ink-2)" }}>Tap to choose a file</div>
            <p style={{ fontSize: 12.5, color: "var(--ink-3)", margin: "6px 0 14px" }}>PDF, audio, transcript — up to 50MB</p>
            <DS.Button variant="secondary" size="sm">Browse</DS.Button>
          </div>
        ) : tab === "connect" ? (
          <div style={{ display: "flex", flexDirection: "column", gap: 9 }}>
            {[["Slack", "amber"], ["Zoom", "teal"], ["Notion", "plum"]].map(([n, h]) => (
              <div key={n} style={{ display: "flex", alignItems: "center", gap: 11, padding: 13, borderRadius: "var(--radius-md)", border: "1px solid var(--line)", background: "var(--bg)" }}>
                <span style={{ width: 34, height: 34, flex: "none", borderRadius: 9, display: "grid", placeItems: "center", color: "#fff", background: `var(--hue-${h})`, fontFamily: "var(--mono)", fontSize: 14 }}>{n[0]}</span>
                <span style={{ flex: 1, fontSize: 14, color: "var(--ink)" }}>{n}</span>
                <DS.Button variant="ghost" size="sm">Pull →</DS.Button>
              </div>
            ))}
          </div>
        ) : (
          <textarea placeholder="Paste a decision, a message, meeting notes…" style={{ width: "100%", minHeight: 150, resize: "none", border: "1px solid var(--line)", borderRadius: "var(--radius-lg)", background: "var(--bg-2)", padding: 14, fontFamily: "var(--sans)", fontSize: 14.5, lineHeight: 1.55, color: "var(--ink)", outline: "none", boxSizing: "border-box" }} />
        )}
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 14 }}>
          <button style={{ display: "flex", alignItems: "center", gap: 7, padding: "8px 13px", borderRadius: "var(--radius-pill)", background: "var(--bg-2)", border: "1px solid var(--line)", cursor: "pointer" }}><window.HueDot hue="coral" size={7} /><span style={{ fontSize: 13, color: "var(--ink)" }}>Henderson</span><span style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>▾</span></button>
          <DS.Button variant="primary" size="md" style={{ flex: 1 }}>Add to ocean</DS.Button>
        </div>
      </ScrollPage>
    );
  }

  function WorkflowsPage() {
    const FLOWS = [
      { trigger: "a decision is confirmed", action: "post to #decisions", hue: "coral", on: true },
      { trigger: "a meeting syncs", action: "extract action items", hue: "teal", on: true },
      { trigger: "a member offboards", action: "reassign their reefs", hue: "pink", on: true },
      { trigger: "every Monday 9am", action: "email a decisions digest", hue: "amber", on: false },
    ];
    return (
      <ScrollPage>
        <Eyebrow>workspace · 3 active</Eyebrow>
        <Title>Workflows</Title>
        <div style={{ display: "flex", flexDirection: "column", gap: 11, marginTop: 18 }}>
          {FLOWS.map((f, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 12, padding: 15, borderRadius: "var(--radius-lg)", background: "var(--bg)", border: "1px solid var(--line)" }}>
              <window.HueDot hue={f.hue} size={10} />
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 13.5, color: "var(--ink)" }}><span style={{ fontFamily: "var(--mono)", fontSize: 9.5, textTransform: "uppercase", letterSpacing: "0.08em", color: "var(--ink-3)" }}>when </span>{f.trigger}</div>
                <div style={{ fontSize: 13.5, color: "var(--ink)", marginTop: 3 }}><span style={{ fontFamily: "var(--mono)", fontSize: 9.5, textTransform: "uppercase", letterSpacing: "0.08em", color: "var(--ink-3)" }}>do </span>{f.action}</div>
              </div>
              <Toggle on={f.on} />
            </div>
          ))}
        </div>
      </ScrollPage>
    );
  }

  function SettingsPage() {
    const [model, setModel] = useState("pro");
    const MODELS = [["pro", "swarm-pro", "Most thorough · cross-ocean", "coral"], ["fast", "swarm-fast", "Quick, lighter retrieval", "amber"], ["device", "on-device", "Private · stays on device", "teal"]];
    return (
      <ScrollPage>
        <Eyebrow>workspace</Eyebrow>
        <Title>Settings</Title>
        <SecLabel style={{ margin: "20px 0 11px" }}>default model</SecLabel>
        <div style={{ display: "flex", flexDirection: "column", gap: 9 }}>
          {MODELS.map(([id, n, d, h]) => (
            <button key={id} onClick={() => setModel(id)} style={{ display: "flex", alignItems: "center", gap: 12, padding: 14, borderRadius: "var(--radius-lg)", border: `1px solid ${model === id ? "var(--brand)" : "var(--line)"}`, background: model === id ? "color-mix(in srgb, var(--brand) 7%, var(--bg))" : "var(--bg)", cursor: "pointer", textAlign: "left", boxShadow: model === id ? "var(--ring)" : "none" }}>
              <window.HueDot hue={h} size={9} />
              <span style={{ flex: 1 }}>
                <span style={{ display: "block", fontSize: 14.5, fontWeight: 500, color: "var(--ink)" }}>{n}</span>
                <span style={{ display: "block", fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)", marginTop: 2 }}>{d}</span>
              </span>
              <span style={{ width: 18, height: 18, borderRadius: "50%", border: `1.5px solid ${model === id ? "var(--brand)" : "var(--line)"}`, display: "grid", placeItems: "center" }}>{model === id && <span style={{ width: 9, height: 9, borderRadius: "50%", background: "var(--brand)" }} />}</span>
            </button>
          ))}
        </div>
        <SecLabel style={{ margin: "26px 0 4px" }}>preferences</SecLabel>
        {[["Run on-device retrieval in parallel", true], ["Show timing on answers", true], ["Share usage telemetry", false]].map(([l, v]) => (
          <div key={l} style={{ display: "flex", alignItems: "center", gap: 14, padding: "14px 0", borderBottom: "1px solid var(--line)" }}>
            <span style={{ flex: 1, fontSize: 14, color: "var(--ink)" }}>{l}</span>
            <Toggle on={v} />
          </div>
        ))}
      </ScrollPage>
    );
  }

  /* ════ MORE (workspace launcher) ════ */
  const MORE = [
    { key: "team", label: "Team & Offboarding", hue: "pink", Icon: window.IconTeam, meta: "5 members · 1 offboarding" },
    { key: "add", label: "Add corals", hue: "teal", Icon: window.IconAdd, meta: "paste, upload, or pull" },
    { key: "connectors", label: "Connectors", hue: "amber", Icon: window.IconConnectors, meta: "3 connected" },
    { key: "workflows", label: "Workflows", hue: "coral", Icon: window.IconWorkflows, meta: "3 active" },
    { key: "settings", label: "Settings", hue: "plum", Icon: window.IconSettings, meta: "model, privacy, billing" },
  ];
  function MorePage({ open }) {
    return (
      <ScrollPage>
        <div style={{ display: "flex", alignItems: "center", gap: 12, padding: "6px 2px 18px" }}>
          <window.Avatar hue="coral" size={44}>sk</window.Avatar>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 17, fontWeight: 500, color: "var(--ink)" }}>Alex Khan</div>
            <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>Alex@coralswarm.example</div>
          </div>
        </div>
        <SecLabel>workspace</SecLabel>
        <div style={{ background: "var(--bg)", border: "1px solid var(--line)", borderRadius: "var(--radius-lg)", overflow: "hidden" }}>
          {MORE.map((it, i) => (
            <button key={it.key} onClick={() => open(it.key)} style={{ display: "flex", alignItems: "center", gap: 13, width: "100%", padding: "14px 15px", borderTop: i ? "1px solid var(--line)" : "none", background: "transparent", border: "none", cursor: "pointer", textAlign: "left" }}>
              <span style={{ width: 34, height: 34, flex: "none", borderRadius: 10, display: "grid", placeItems: "center", background: `color-mix(in srgb, var(--hue-${it.hue}) 18%, var(--bg))`, color: `var(--hue-${it.hue})` }}><it.Icon width={18} height={18} /></span>
              <span style={{ flex: 1 }}>
                <span style={{ display: "block", fontSize: 15, color: "var(--ink)" }}>{it.label}</span>
                <span style={{ display: "block", fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)", marginTop: 2 }}>{it.meta}</span>
              </span>
              <span style={{ color: "var(--ink-3)", fontSize: 18 }}>›</span>
            </button>
          ))}
        </div>
        <button style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 9, width: "100%", marginTop: 16, padding: 13, borderRadius: "var(--radius-pill)", background: "var(--bg)", border: "1px solid var(--line)", color: "var(--ink-2)", fontSize: 13.5, cursor: "pointer", fontFamily: "var(--sans)" }}>
          <window.IconLogout width={15} height={15} /> Sign out
        </button>
      </ScrollPage>
    );
  }
  const WORKSPACE_PAGES = { team: TeamPage, add: AddPage, connectors: ConnectorsPage, workflows: WorkflowsPage, settings: SettingsPage };
  const WORKSPACE_TITLES = { team: "Team & Offboarding", add: "Add corals", connectors: "Connectors", workflows: "Workflows", settings: "Settings" };

  /* ════ SHELL: top bar + bottom tab bar ════ */
  function TopBar({ tab, sub, stackTitle, onBack }) {
    const back = stackTitle || (tab === "ask" && sub !== "home");
    const askLabels = { answer: "Auth decisions", oceans: "Across all oceans" };
    const title = stackTitle ? WORKSPACE_TITLES[stackTitle] : (tab === "ask" && sub !== "home" ? askLabels[sub] : null);
    return (
      <div style={{ flex: "none", height: 50, display: "flex", alignItems: "center", gap: 10, padding: "0 12px", borderBottom: "1px solid var(--line)", background: "var(--bg)" }}>
        {back ? (
          <>
            <button onClick={onBack} style={{ width: 34, height: 34, display: "grid", placeItems: "center", borderRadius: 9, border: "none", background: "transparent", color: "var(--ink)", cursor: "pointer", flex: "none" }}><IconBack /></button>
            <span style={{ fontFamily: "var(--display)", fontSize: 18, fontStyle: "italic", color: "var(--ink)", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{title}</span>
          </>
        ) : (
          <>
            <span style={{ width: 26, height: 26, display: "block", marginLeft: 2, flex: "none" }}><DS.DotMandala size={26} density={0.8} /></span>
            <DS.Wordmark size={16} />
            <button style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 7, padding: "6px 11px", borderRadius: "var(--radius-pill)", background: "var(--bg-2)", border: "1px solid var(--line)", cursor: "pointer", flex: "none" }}>
              <window.HueDot hue="coral" size={7} /><span style={{ fontSize: 12.5, fontWeight: 500, color: "var(--ink)" }}>Henderson</span><span style={{ fontFamily: "var(--mono)", fontSize: 10, color: "var(--ink-3)" }}>▾</span>
            </button>
          </>
        )}
      </div>
    );
  }

  function TabBar({ tab, onTab }) {
    return (
      <div style={{ flex: "none", height: TABBAR_H + SAFE_BOTTOM, paddingBottom: SAFE_BOTTOM, display: "flex", borderTop: "1px solid var(--line)", background: "color-mix(in srgb, var(--bg) 88%, transparent)", backdropFilter: "blur(12px)" }}>
        {TABS.map((t) => {
          const on = tab === t.key;
          return (
            <button key={t.key} onClick={() => onTab(t.key)} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 4, border: "none", background: "transparent", cursor: "pointer", color: on ? "var(--brand-2)" : "var(--ink-3)", position: "relative" }}>
              <span style={{ position: "relative" }}>
                <t.Icon width={22} height={22} />
                {t.dot && <span style={{ position: "absolute", top: -2, right: -3, width: 6, height: 6, borderRadius: "50%", background: "var(--hue-amber)" }} />}
              </span>
              <span style={{ fontFamily: "var(--mono)", fontSize: 9.5, letterSpacing: "0.04em", fontWeight: on ? 600 : 400 }}>{t.label}</span>
            </button>
          );
        })}
      </div>
    );
  }

  /* ════ ROOT ════ */
  function MobileApp() {
    const [tab, setTab] = useState("ask");
    const [askSub, setAskSub] = useState("home");
    const [stack, setStack] = useState(null);   // workspace page opened from More
    const [oceans, setOceans] = useState({ Henderson: true, "Core Platform": true, Acme: false, Scratch: false });
    const [model, setModel] = useState("pro");

    const onTab = (k) => { setStack(null); if (k === "ask" && tab === "ask") setAskSub("home"); setTab(k); };
    const onBack = () => { if (stack) setStack(null); else setAskSub("home"); };

    let body;
    if (stack) { const P = WORKSPACE_PAGES[stack]; body = <P />; }
    else if (tab === "ask") body = <M.AskScreen sub={askSub} setSub={setAskSub} oceans={oceans} setOceans={setOceans} model={model} setModel={setModel} />;
    else if (tab === "reefs") body = <ReefsPage />;
    else if (tab === "review") body = <ReviewPage />;
    else if (tab === "currents") body = <CurrentsPage />;
    else if (tab === "more") body = <MorePage open={(k) => setStack(k)} />;

    return (
      <div style={{ display: "flex", flexDirection: "column", height: "100%", background: "var(--bg)", fontFamily: "var(--sans)", color: "var(--ink)", overflow: "hidden" }}>
        <TopBar tab={tab} sub={askSub} stackTitle={stack} onBack={onBack} />
        <div style={{ flex: 1, minHeight: 0, position: "relative" }}>{body}</div>
        <TabBar tab={tab} onTab={onTab} />
      </div>
    );
  }

  window.MobileApp = MobileApp;
})();
