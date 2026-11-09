/* Shared fixture data + the swarm canvas for the connectors redesign mocks. */
(function () {
  const ms = (status) => {
    const base = { key: "microsoft", name: "Microsoft", provider: "microsoft", letter: "M", hue: "plum", status, sourcesShort: "Outlook mail, Calendar, Teams", what: "Outlook mail, Calendar, Teams chats, channels and meetings." };
    const S = (mail, cal, chats) => [
      { n: "Outlook mail", s: mail[0], meta: mail[1] }, { n: "Calendar", s: cal[0], meta: cal[1] }, { n: "Teams chats", s: chats[0], meta: chats[1] },
      { n: "Teams channels", s: "off", meta: "off" }, { n: "Teams meetings", s: "off", meta: "off" }];
    return ({
      not: { ...base, sources: S(["off", "—"], ["off", "—"], ["off", "—"]) },
      setup: { ...base, fresh: "2 of 5 sources chosen · scope not set yet", sources: S(["on", "not set"], ["on", "not set"], ["off", "off"]) },
      active: { ...base, fresh: "3 of 5 sources · last captured 3 min ago", sources: S(["on", "4 threads"], ["on", "everything"], ["on", "6 chats"]) },
      importing: { ...base, fresh: "3 of 5 sources · importing 30 days of mail · 18%", progress: 18, sources: S(["importing", "18%"], ["on", "everything"], ["on", "6 chats"]) },
      attention: { ...base, message: "Teams chats stopped on Oct 28 — Microsoft returned a permission error. Open Microsoft and retry.", sources: S(["on", "4 threads"], ["on", "everything"], ["error", "stopped Oct 28"]) },
      reconnect: { ...base, message: "Your Microsoft sign-in expired on Oct 27. Reconnect to resume all 3 sources.", sources: S(["paused", "stopped"], ["paused", "stopped"], ["paused", "stopped"]) },
    })[status];
  };
  const slack = (status) => {
    const base = { key: "slack", name: "Slack", provider: "slack", letter: "S", hue: "amber", status, sourcesShort: "Channels", what: "Messages and threads in channels you pick. Never DMs." };
    const ch = (s, meta) => [{ n: "Channels", s, meta }];
    return ({
      not: { ...base, sources: ch("off", "—") },
      setup: { ...base, fresh: "No channels picked yet · nothing captured", sources: ch("off", "none picked") },
      active: { ...base, fresh: "14 channels · last captured 3 min ago", sources: ch("on", "14 channels") },
      importing: { ...base, fresh: "14 channels · importing 30 days · 41%", progress: 41, sources: ch("importing", "41%") },
      attention: { ...base, message: "3 channels were archived in Slack on Oct 26 and stopped capturing. Pick others or remove them.", sources: ch("error", "3 archived") },
      reconnect: { ...base, message: "Slack revoked access on Oct 27. Reconnect to resume.", sources: ch("paused", "stopped") },
    })[status];
  };
  const tk = (name, letter, hue, what, extra = {}) => ({ key: name.toLowerCase(), name, provider: name.toLowerCase(), letter, hue, beta: true, status: "not", sourcesShort: "Everything I can see", what, sources: [{ n: "Workspace", s: "off", meta: "—" }], ...extra });
  const google = (status) => ({ key: "google", name: "Google", provider: "google", letter: "G", hue: "coral", status, fresh: status === "not" ? undefined : "3 of 4 sources · importing 30 days · 62%", progress: 62, sourcesShort: "Gmail, Calendar, Meet, Chat", what: "Gmail, Calendar, Meet and Chat.", sources: [{ n: "Gmail", s: "on", meta: "5 threads" }, { n: "Calendar", s: "importing", meta: "62%" }, { n: "Meet", s: "on", meta: "everything" }, { n: "Chat", s: "off", meta: "off" }] });
  const github = (status) => ({ key: "github", name: "GitHub", provider: "github", letter: "GH", hue: "pink", status, fresh: "No repos picked yet · nothing captured", sourcesShort: "Repositories", what: "Pull requests, reviews, issues, releases and commits. Never your source code.", sources: [{ n: "Repositories", s: "off", meta: "none picked" }] });
  const toolkits = () => [
    tk("Jira", "J", "teal", "Issues, comments and status changes across your site."),
    tk("Confluence", "C", "teal", "Pages and comments in the spaces you can see."),
    tk("HubSpot", "H", "amber", "Deals, notes and logged emails on your records."),
    tk("Zendesk", "Z", "pink", "Tickets with their public and internal replies."),
    tk("Dart", "D", "coral", "Tasks, docs and comments in your workspace."),
  ];
  const connectors = () => [
    slack("active"), ms("attention"), google("importing"), github("setup"),
    tk("Linear", "L", "plum", "Issues, projects and comments across your workspace.", { status: "active", fresh: "Everything I can see · last captured 1h ago", sources: [{ n: "Workspace", s: "on", meta: "everything" }] }),
    ...toolkits(),
  ];
  const fresh = () => [slack("not"), ms("not"), google("not"), github("not"), tk("Linear", "L", "plum", "Issues, projects and comments across your workspace."), ...toolkits()];

  const clients = [{ key: "claude", label: "Claude" }, { key: "claude-code", label: "Claude Code" }, { key: "chatgpt", label: "ChatGPT" }, { key: "cursor", label: "Cursor" }, { key: "codex", label: "Codex" }];
  const steps = {
    claude: [{ t: "In Claude, open Settings → Connectors and choose Add custom connector." }, { t: "Name it CoralSwarm and paste the address." }, { t: "Click Connect, then sign in with your CoralSwarm account." }],
    "claude-code": [{ t: "In your terminal, add the server:", code: "claude mcp add --transport http coralswarm https://api.coralswarm.example/mcp" }, { t: "Start Claude Code and run /mcp, then choose CoralSwarm to sign in." }],
    chatgpt: [{ t: "In ChatGPT, open Settings → Connectors → Advanced and turn on developer mode." }, { t: "Choose Create, name it CoralSwarm, and paste the address." }, { t: "Sign in with your CoralSwarm account when ChatGPT asks." }],
    cursor: [{ t: "Open Cursor Settings → MCP and choose Add new server." }, { t: "Add this to mcp.json:", code: "{ \"mcpServers\": { \"coralswarm\": { \"url\": \"https://api.coralswarm.example/mcp\" } } }" }, { t: "Click Needs login beside CoralSwarm and sign in." }],
    codex: [{ t: "In your terminal, add the server:", code: "codex mcp add coralswarm --url https://api.coralswarm.example/mcp" }, { t: "Then sign in:", code: "codex mcp login coralswarm" }],
  };
  const can = {
    claude: { add: "Save a conversation's decisions and notes to your ocean when you ask it to.", ask: "Answer from your ocean inside a chat, with the sources it used." },
    "claude-code": { add: "Save what a coding session decided and why, alongside the session summary.", ask: "Look up what the team decided before it changes code." },
    chatgpt: { add: "Save notes and decisions from a chat when you ask it to.", ask: "Answer from your ocean inside a chat, with the sources it used." },
    cursor: { add: "Save the reasoning behind a change from the editor.", ask: "Pull in decisions and context about the code you're editing." },
    codex: { add: "Save what a task decided and why, when it finishes.", ask: "Look up the team's decisions before it starts a task." },
  };

  /* The swarm canvas: the brand mark's real ring geometry, animated. Multi-hue
     dots (your tools) flow in, merge into the mark, leave as coral (answers). */
  let Swarm = null;
  function makeSwarm(React) {
    if (Swarm) return Swarm;
    const RINGS = [{ r: 0, n: 1, s: 0.085 }, { r: 0.14, n: 6, s: 0.06 }, { r: 0.25, n: 10, s: 0.05 }, { r: 0.35, n: 14, s: 0.042 }, { r: 0.45, n: 18, s: 0.034 }];
    const HUES = ["coral", "pink", "amber", "teal", "plum"];
    const FB = ["#f77764", "#eb7290", "#f4b860", "#2a9d8f", "#7c5295"];
    Swarm = function Swarm({ lanes = 9, gap = 13 }) {
      const ref = React.useRef(null);
      React.useEffect(() => {
        const cv = ref.current; if (!cv) return;
        const ctx = cv.getContext("2d");
        const reduce = window.matchMedia && matchMedia("(prefers-reduced-motion: reduce)").matches;
        let hue = FB.slice(), coral = hue[0], lastRead = -10, W = 0, H = 0, raf, fb, alive = true, parts = [], last = 0, pulse = 0;
        const readTokens = () => { const cs = getComputedStyle(document.documentElement); hue = HUES.map((h, i) => cs.getPropertyValue("--hue-" + h).trim() || FB[i]); coral = hue[0]; };
        const fit = () => { const r = cv.parentElement.getBoundingClientRect(); const d = window.devicePixelRatio || 1; W = r.width; H = r.height; cv.width = W * d; cv.height = H * d; ctx.setTransform(d, 0, 0, d, 0, 0); };
        fit(); const ro = new ResizeObserver(fit); ro.observe(cv.parentElement);
        const mandala = (cx, cy, S, t) => RINGS.forEach((ring, ri) => {
          const w = ri ? (0.22 / ri) * (ri % 2 ? -1 : 1) : 0;
          for (let i = 0; i < ring.n; i++) {
            const a = ri === 0 ? 0 : (i / ring.n) * Math.PI * 2 + ri * 0.4 + t * w;
            const br = 1 + 0.04 * Math.sin(t * 1.6 - ri * 0.7) + pulse * (ri === 0 ? 0.25 : 0.06);
            ctx.fillStyle = hue[(i + ri) % 5]; ctx.beginPath();
            ctx.arc(cx + Math.cos(a) * ring.r * S * br, cy + Math.sin(a) * ring.r * S * br, ring.s * S, 0, Math.PI * 2); ctx.fill();
          }
        });
        const frame = (ms) => {
          const t = ms / 1000, dt = Math.min(0.05, t - (last || t)); last = t;
          if (Math.abs(t - lastRead) > 1) { readTokens(); lastRead = t; }
          ctx.clearRect(0, 0, W, H); pulse = Math.max(0, pulse - dt * 2.5);
          const cx = W * 0.5, cy = H * 0.46, S = H * 0.62, g = Math.min(gap, H / (lanes + 3));
          if (parts.length < 150 && Math.random() < dt * 22 * Math.min(1, W / 900)) { const lane = Math.floor(Math.random() * lanes); parts.push({ x: -8, lane, sp: 70 + Math.random() * 50, c: lane % 5, r: 1.8 + Math.random() * 1.3 }); }
          parts = parts.filter(p => p.x < W + 10);
          parts.forEach(p => {
            p.x += p.sp * dt * (Math.abs(p.x - cx) < S * 0.4 ? 0.55 : 1);
            const bump = Math.exp(-(((p.x - cx) / (W * 0.16)) ** 2));
            const y = cy + (p.lane - (lanes - 1) / 2) * g * (1 - bump);
            const hidden = Math.abs(p.x - cx) < S * 0.36;
            if (hidden && !p.hit) { p.hit = true; pulse = Math.min(1, pulse + 0.12); }
            if (hidden) return;
            ctx.globalAlpha = Math.max(0, Math.min(1, (p.x + 8) / 60, (W + 10 - p.x) / 60));
            ctx.fillStyle = p.x > cx ? coral : hue[p.c];
            ctx.beginPath(); ctx.arc(p.x, y, p.x > cx ? p.r * 1.15 : p.r, 0, Math.PI * 2); ctx.fill();
          });
          ctx.globalAlpha = 1; mandala(cx, cy, S, t);
          if (!reduce && alive) schedule();
        };
        const schedule = () => { raf = requestAnimationFrame(frame); clearTimeout(fb); fb = setTimeout(() => { if (alive) { cancelAnimationFrame(raf); frame(performance.now()); } }, 200); };
        for (let i = 0; i < 160; i++) frame(performance.now() - (160 - i) * 33);
        if (!reduce) schedule();
        return () => { alive = false; cancelAnimationFrame(raf); clearTimeout(fb); ro.disconnect(); };
      }, []);
      return React.createElement("canvas", { ref, "aria-hidden": true, style: { display: "block", width: "100%", height: "100%" } });
    };
    return Swarm;
  }

  window.CSKit = { ms, slack, google, github, tk, toolkits, connectors, fresh, clients, steps, can, makeSwarm };
})();
