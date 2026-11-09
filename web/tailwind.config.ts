import type { Config } from "tailwindcss";

// CoralSwarm — product design system.
//
// Source of truth: the 2026-07 redesign handoff
// (New UI Design/design_handoff_coralswarm_redesign/README.md + designs/*.dc.html).
// Mirrored as CSS variables in app/globals.css, which components read through
// color-mix() and inline styles.
//
// The redesign flips the canvas from WARM CREAM to COOL NEUTRAL. Per the
// handoff, this is done by rewriting token VALUES in place — never by adding a
// parallel set of names — so hue-keyed components keep working untouched:
//
//   token      was (cream)  →  now (cool neutral)
//   bg         #FBF7F2      →  #FFFFFF
//   bg-2       #F2EBE0      →  #F5F6F8
//   bg-3       #E8DCC8      →  #EBECEF
//   ink        #1A1410      →  #14161A
//   ink-2      #4A3F36      →  #4B5058
//   ink-3      #8A7B6E      →  #8A9099
//   line       #D9CCB8      →  #E4E6EA
//
// The coral brand, the five hues, `ok` and `warn` are UNCHANGED.
const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
    // NOTE: the kit is NOT moving out of web/. Q6=(a) of
    // docs/plans/capture/07-ui-track.md deleted packages/shared-ui and put
    // nothing in its place — this file plus web/components/ui/ ARE the
    // shared kit.
    // The "../packages/**/src/**/*.{ts,tsx}" glob that used to sit here was
    // pre-emptive cover for that move; packages/ now holds Rust only, so it
    // matched nothing. If a kit file ever does leave web/, re-add a glob for
    // it: this config is built almost entirely from the project tokens below
    // and uses barely any stock Tailwind, so those utilities are purged
    // SILENTLY on such a move while build, typecheck and tests all still pass.
  ],
  theme: {
    extend: {
      colors: {
        // ---- neutrals ----
        // bg is now a true white content surface; bg-2 is the sidebar/canvas
        // inset; bg-3 is the document backdrop behind frames.
        bg: {
          DEFAULT: "#FFFFFF",
          "2": "#F5F6F8",
          "3": "#EBECEF",
          // Row hover / pressed field. Shares a value with line-2 by design —
          // a hovered row reads as the same weight as an internal rule.
          hover: "#EDEEF1",
          // Alternating rows and quiet cards, a hair off white.
          subtle: "#FCFCFD",
          "subtle-2": "#FAFBFC",
        },
        ink: {
          DEFAULT: "#14161A",
          "2": "#4B5058",
          "3": "#8A9099",
          // Faintest text and disabled glyphs — new in the redesign.
          "4": "#A6ABB3",
          // legacy aliases (pre-redesign names, kept so existing screens compile)
          muted: "#4B5058",
          faint: "#8A9099",
        },
        // Three border weights: standard, internal divider, faintest in-card rule.
        line: { DEFAULT: "#E4E6EA", "2": "#EDEEF1", "3": "#F2F3F5" },

        // ---- brand ----
        // NOTE ON brand-3: the handoff's token table proposes `brand-3` for the
        // DESTRUCTIVE red #C24A3A. That name is already taken here by a LIGHT
        // coral wash used as a background at ~15 call sites (.cite, .cs-hl,
        // Chip, the meeting/permission selected-tile fills). Repointing it at a
        // dark red would turn every one of those washes into a dark block with
        // dark text on it. So:
        //   • destructive keeps its existing home, `bad` (#C24A3A) — already
        //     exactly the value the handoff asks for;
        //   • brand-3 is retuned to the handoff's `brand-soft` (#FFE1D9), the
        //     coral chip/avatar background, which is what it was always doing;
        //   • `brand-soft` is added as the canonical name for that value. Prefer
        //     it in new code; brand-3 is retained as an alias.
        brand: {
          DEFAULT: "#F77764",
          "2": "#D14A38",
          "3": "#FFE1D9",
          soft: "#FFE1D9",
        },
        accent: "#2A9D8F",
        // Text on a filled brand (coral) control.
        "on-brand": "var(--on-brand)",
        "focus-ring": "var(--focus-ring)",

        // ---- hues (unchanged by the redesign) ----
        // coral = decisions/brand · pink = people · amber = sources/candidates ·
        // teal = ingested content · plum = systems
        hue: {
          coral: "#F77764",
          pink: "#EB7290",
          amber: "#F4B860",
          teal: "#2A9D8F",
          plum: "#7C5295",
        },
        ok: "#3F8A5B",
        // `soft` and `line` are the 18% wash and 45% border of `--warn`
        // (globals.css), the hue-tint recipe applied to the status colour.
        // DEFAULT keeps `text-warn`, `bg-warn/[10%]` and `border-warn/40`.
        warn: { DEFAULT: "#D89A3A", soft: "var(--warn-soft)", line: "var(--border-warn)" },
        bad: "#C24A3A",

        // ---- legacy semantic names, remapped onto the cool palette ----
        // DEPRECATED: these are the pre-redesign "Light Editorial" names. They
        // resolve to the new values so old screens keep compiling during the
        // redesign; do not reach for them in new code. `surface2`, `line2`,
        // `kelp`, `tide`, `gold` and `ocean` had zero call sites and are gone.
        // ---- Home (2026-10 handoff): mirrored from globals.css `:root` ----
        scrim: "var(--scrim)",
        src: {
          chat: "var(--src-chat)",
          meetings: "var(--src-meetings)",
          "agent-sessions": "var(--src-agent-sessions)",
          "docs-mail": "var(--src-docs-mail)",
          "code-tickets": "var(--src-code-tickets)",
        },
        kind: {
          meeting: "var(--kind-meeting)",
          slack: "var(--kind-slack)",
          agent: "var(--kind-agent)",
          doc: "var(--kind-doc)",
          note: "var(--kind-note)",
        },
        heat: {
          "1": "var(--heat-1)",
          "2": "var(--heat-2)",
          "3": "var(--heat-3)",
          "4": "var(--heat-4)",
        },
        canvas: "#F5F6F8",
        surface: "#FFFFFF",
        coral: { DEFAULT: "#F77764", dark: "#D14A38", soft: "#FFE1D9" },
      },
      fontFamily: {
        display: ["var(--font-display)", "Georgia", "serif"],
        sans: ["var(--font-sans)", "system-ui", "sans-serif"],
        mono: ["var(--font-mono)", "ui-monospace", "monospace"],
      },
      // Redesign radius scale. Canonical names describe the thing they wrap;
      // r1–r5 are the legacy scale remapped onto it (the old 16/24px steps have
      // no equivalent — the new system tops out at 14px, so both land there).
      // Add-in pane type scale (Teams side panel / Outlook task pane), used by
      // web/components/addin/kit.tsx. Compact because the panes are ~320px wide.
      fontSize: {
        pane: ["13.5px", "1.5"],
        "pane-sm": ["12.5px", "1.45"],
        "pane-xs": ["12px", "1.4"],
        "pane-2xs": ["11px", "1.4"],
        "pane-label": ["10px", "1.4"],
        "pane-title": ["22px", "1.2"],
        greeting: ["44px", "1.08"],
        // `--type-ask`: the Home Ask box.
        ask: ["16.5px", "1.5"],
        // UI text roles (`--type-lead`, `--type-item`, `--type-note`,
        // `--type-micro`): the sans and mono sizes under the display roles.
        // The `pane*` sizes above are the add-in panes' scale only.
        lead: ["13.5px", "1.6"],
        item: ["13px", "1.5"],
        note: ["12.5px", "1.55"],
        micro: ["10px", "1.5"],
        question: ["28px", "1.15"],
        "detail-title": ["26px", "1.15"],
      },
      letterSpacing: { "pane-label": ".14em", "eyebrow-wide": "var(--tracking-eyebrow-wide)" },
      width: { panel: "var(--panel-w)", "panel-lg": "var(--panel-w-lg)" },
      maxWidth: { home: "1160px", thread: "var(--thread-max)" },
      maxHeight: { composer: "var(--composer-max-h)" },
      height: { "chat-header": "var(--chat-header-h)" },
      transitionDuration: {
        fast: "var(--dur-fast)",
        pop: "var(--dur-pop)",
        exit: "var(--dur-exit)",
        enter: "var(--dur-enter)",
        collapse: "var(--dur-collapse)",
        drawer: "var(--dur-drawer)",
        reveal: "var(--dur-reveal)",
        settle: "var(--dur-settle)",
        slow: "var(--dur-slow)",
      },
      transitionTimingFunction: { out: "var(--ease-out)", exit: "var(--ease-exit)", pop: "var(--ease-pop)" },
      animation: {
        shimmer: "cs-shimmer 1.8s linear infinite",
        "stage-in": "cs-stage-in var(--dur-enter) var(--ease-out) both",
        "pop-in": "cs-pop-in var(--dur-pop) var(--ease-out) both",
        skeleton: "cs-skeleton 2s var(--ease) infinite",
        // The "new since your last visit" dot (Home's What's new).
        "new-dot": "cs-pulse 2.4s var(--ease) infinite",
      },
      borderRadius: {
        badge: "6px",
        bar: "2px", // sparkline day-bars
        row: "8px",
        field: "9px", // also the sidebar item radius
        control: "10px",
        card: "12px",
        frame: "14px",
        hud: "24px",
        pill: "999px",
        cite: "var(--r-cite)",
        mark: "var(--r-mark)",
        r1: "6px",
        r2: "8px",
        r3: "12px",
        r4: "14px",
        r5: "14px",
      },
      // Flatter than the cream system: the redesign leans on 1px borders for
      // separation and uses shadow only to lift a row or float a surface.
      boxShadow: {
        row: "0 1px 2px rgba(20,22,26,.06)",
        card: "0 1px 2px rgba(20,22,26,.05)",
        // Interpolated between `card` and `frame` for in-app popovers and
        // menus, which the handoff's four-shadow scale doesn't cover (its
        // `frame` level is for the 1440x900 screen frames in the mock document,
        // far too heavy for a dropdown).
        pop: "0 12px 28px -12px rgba(20,22,26,.16)",
        frame: "0 28px 56px -20px rgba(20,22,26,.22)",
        overlay: "0 22px 54px -16px rgba(0,0,0,.5)",
        // legacy levels → nearest redesign step
        "1": "0 1px 2px rgba(20,22,26,.06)",
        "2": "0 12px 28px -12px rgba(20,22,26,.16)",
        "3": "0 28px 56px -20px rgba(20,22,26,.22)",
      },
    },
  },
  plugins: [],
};

export default config;
