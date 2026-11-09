/* Real brand marks for connector tiles.

   These replaced hue-tinted letter glyphs ("S" for Slack, "M" for Microsoft).
   A letter is fine as a placeholder but wrong on a screen whose whole job is
   "connect the tool you already use" — people scan connector lists by logo,
   and a coral "G" reads as a CoralSwarm thing rather than as Gmail.

   INLINE SVG, NOT REMOTE IMAGES, deliberately. A remote logo would be a
   third-party request on every connector screen. These are a few hundred bytes each.

   Paths are the official marks at their published viewBoxes, so each keeps its
   own coordinate system rather than being re-fitted to a shared grid.

   BRAND COLOUR IS NOT A DESIGN TOKEN. Every other colour in this app comes
   from tailwind.config.ts, and these deliberately do not — they are Slack's
   and Google's palettes, not ours, and re-tinting them to our hues is exactly
   what makes a logo look wrong. This file is the only sanctioned place in the
   app for foreign hex values; do not copy the pattern elsewhere. */

export type BrandKey =
  | "slack"
  | "microsoft"
  | "gmail"
  | "zoom"
  | "google"
  | "github"
  | "jira"
  | "confluence"
  | "linear"
  | "hubspot"
  | "zendesk"
  | "dart";

const BRANDS: Record<BrandKey, { viewBox: string; body: React.ReactNode }> = {
  // Slack — four-colour pinwheel. Official mark, 122.8 grid.
  slack: {
    viewBox: "0 0 122.8 122.8",
    body: (
      <>
        <path
          fill="#E01E5A"
          d="M25.8 77.6c0 7.1-5.8 12.9-12.9 12.9S0 84.7 0 77.6s5.8-12.9 12.9-12.9h12.9v12.9zm6.5 0c0-7.1 5.8-12.9 12.9-12.9s12.9 5.8 12.9 12.9v32.3c0 7.1-5.8 12.9-12.9 12.9s-12.9-5.8-12.9-12.9V77.6z"
        />
        <path
          fill="#36C5F0"
          d="M45.2 25.8c-7.1 0-12.9-5.8-12.9-12.9S38.1 0 45.2 0s12.9 5.8 12.9 12.9v12.9H45.2zm0 6.5c7.1 0 12.9 5.8 12.9 12.9s-5.8 12.9-12.9 12.9H12.9C5.8 58.1 0 52.3 0 45.2s5.8-12.9 12.9-12.9h32.3z"
        />
        <path
          fill="#2EB67D"
          d="M97 45.2c0-7.1 5.8-12.9 12.9-12.9s12.9 5.8 12.9 12.9-5.8 12.9-12.9 12.9H97V45.2zm-6.5 0c0 7.1-5.8 12.9-12.9 12.9s-12.9-5.8-12.9-12.9V12.9C64.7 5.8 70.5 0 77.6 0s12.9 5.8 12.9 12.9v32.3z"
        />
        <path
          fill="#ECB22E"
          d="M77.6 97c7.1 0 12.9 5.8 12.9 12.9s-5.8 12.9-12.9 12.9-12.9-5.8-12.9-12.9V97h12.9zm0-6.5c-7.1 0-12.9-5.8-12.9-12.9s5.8-12.9 12.9-12.9h32.3c7.1 0 12.9 5.8 12.9 12.9s-5.8 12.9-12.9 12.9H77.6z"
        />
      </>
    ),
  },

  /* Microsoft — the four-square corporate mark, NOT the Teams mark. This
     connector is Microsoft Graph: it covers Teams meetings AND Outlook
     calendar AND mail, so the Teams logo would under-describe it. If Teams
     ever becomes its own connector it should get its own entry here. */
  microsoft: {
    viewBox: "0 0 23 23",
    body: (
      <>
        <path fill="#F25022" d="M1 1h10v10H1z" />
        <path fill="#7FBA00" d="M12 1h10v10H12z" />
        <path fill="#00A4EF" d="M1 12h10v10H1z" />
        <path fill="#FFB900" d="M12 12h10v10H12z" />
      </>
    ),
  },

  // Gmail — the envelope mark on Google's 48 grid.
  gmail: {
    viewBox: "0 0 48 48",
    body: (
      <>
        <path fill="#4CAF50" d="M45 16.2l-5 2.75-5 4.75V40h7a3 3 0 0 0 3-3V16.2z" />
        <path fill="#1E88E5" d="M3 16.2l3.614 1.71L13 23.7V40H6a3 3 0 0 1-3-3V16.2z" />
        <path fill="#E53935" d="M35 11.2L24 19.45 13 11.2l-1 5.8 1 6.7 11 8.25 11-8.25 1-6.7z" />
        <path
          fill="#C62828"
          d="M3 12.298V16.2l10 7.5V11.2L9.876 8.859A4.298 4.298 0 0 0 7.298 8 4.298 4.298 0 0 0 3 12.298z"
        />
        <path
          fill="#FBC02D"
          d="M45 12.298V16.2l-10 7.5V11.2l3.124-2.341A4.298 4.298 0 0 1 40.702 8 4.298 4.298 0 0 1 45 12.298z"
        />
      </>
    ),
  },

  // Google — the four-colour "G", 48 grid. The connector is Workspace (Gmail,
  // Calendar, Meet, Chat), so it wears the company mark, not Gmail's envelope.
  google: {
    viewBox: "0 0 48 48",
    body: (
      <>
        <path
          fill="#FFC107"
          d="M43.611 20.083H42V20H24v8h11.303c-1.649 4.657-6.08 8-11.303 8-6.627 0-12-5.373-12-12s5.373-12 12-12c3.059 0 5.842 1.154 7.961 3.039l5.657-5.657C34.046 6.053 29.268 4 24 4 12.955 4 4 12.955 4 24s8.955 20 20 20 20-8.955 20-20c0-1.341-.138-2.65-.389-3.917z"
        />
        <path
          fill="#FF3D00"
          d="M6.306 14.691l6.571 4.819C14.655 15.108 18.961 12 24 12c3.059 0 5.842 1.154 7.961 3.039l5.657-5.657C34.046 6.053 29.268 4 24 4 16.318 4 9.656 8.337 6.306 14.691z"
        />
        <path
          fill="#4CAF50"
          d="M24 44c5.166 0 9.86-1.977 13.409-5.192l-6.19-5.238C29.211 35.091 26.715 36 24 36c-5.202 0-9.619-3.317-11.283-7.946l-6.522 5.025C9.505 39.556 16.227 44 24 44z"
        />
        <path
          fill="#1976D2"
          d="M43.611 20.083H42V20H24v8h11.303a12.04 12.04 0 0 1-4.087 5.571l.003-.002 6.19 5.238C36.971 39.205 44 34 44 24c0-1.341-.138-2.65-.389-3.917z"
        />
      </>
    ),
  },

  // GitHub — the Octocat mark, 24 grid, on GitHub's black.
  github: {
    viewBox: "0 0 24 24",
    body: (
      <path
        fill="#181717"
        d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12"
      />
    ),
  },

  // Jira — the three stacked chevrons, 24 grid.
  jira: {
    viewBox: "0 0 24 24",
    body: (
      <path
        fill="#0052CC"
        d="M11.571 11.513H0a5.218 5.218 0 0 0 5.232 5.215h2.13v2.057A5.215 5.215 0 0 0 12.575 24V12.518a1.005 1.005 0 0 0-1.005-1.005zm5.723-5.756H5.736a5.215 5.215 0 0 0 5.215 5.214h2.129v2.058a5.218 5.218 0 0 0 5.215 5.214V6.758a1.001 1.001 0 0 0-1.001-1.001zM23.013 0H11.455a5.215 5.215 0 0 0 5.215 5.215h2.129v2.057A5.215 5.215 0 0 0 24 12.483V1.005A1.001 1.001 0 0 0 23.013 0z"
      />
    ),
  },

  // Confluence — the two opposed swooshes, 24 grid.
  confluence: {
    viewBox: "0 0 24 24",
    body: (
      <path
        fill="#172B4D"
        d="M.87 18.257c-.248.382-.53.875-.763 1.245a.764.764 0 0 0 .255 1.04l4.965 3.054a.764.764 0 0 0 1.058-.26c.199-.332.454-.763.733-1.221 1.967-3.247 3.945-2.853 7.508-1.146l4.957 2.337a.764.764 0 0 0 1.028-.382l2.364-5.346a.764.764 0 0 0-.382-1.01c-1.046-.49-3.126-1.462-4.99-2.358C9.4 10.768 4.627 11.43.87 18.257zM23.131 5.743c.249-.405.531-.875.764-1.25a.764.764 0 0 0-.256-1.034L18.675.404a.764.764 0 0 0-1.058.26c-.195.335-.451.763-.734 1.225-1.966 3.246-3.945 2.85-7.508 1.146L4.437.694a.764.764 0 0 0-1.027.382L1.046 6.422a.764.764 0 0 0 .382 1.01c1.045.49 3.125 1.46 4.99 2.358 8.182 3.903 12.955 3.241 16.713-3.047z"
      />
    ),
  },

  // Linear — the striped disc, 24 grid.
  linear: {
    viewBox: "0 0 24 24",
    body: (
      <path
        fill="#5E6AD2"
        d="M2.886 4.18A11.982 11.982 0 0 1 11.99 0C18.624 0 24 5.376 24 12.009c0 3.64-1.62 6.903-4.18 9.105L2.887 4.177zM1.817 5.626l16.556 16.556c-.524.33-1.075.62-1.65.866L.951 7.277c.247-.575.537-1.126.866-1.65zM.322 9.163l14.515 14.515c-.71.172-1.443.282-2.195.322L0 11.358a12 12 0 0 1 .322-2.195zm-.17 4.862l9.823 9.824a12.02 12.02 0 0 1-9.824-9.824z"
      />
    ),
  },

  // HubSpot — the sprocket, 24 grid.
  hubspot: {
    viewBox: "0 0 24 24",
    body: (
      <path
        fill="#FF7A59"
        d="M18.164 7.93V5.084a2.198 2.198 0 0 0 1.267-1.978v-.067A2.2 2.2 0 0 0 17.238.845h-.067a2.2 2.2 0 0 0-2.193 2.193v.067a2.196 2.196 0 0 0 1.252 1.973l.013.006v2.852a6.22 6.22 0 0 0-2.969 1.31l.012-.01-7.828-6.095A2.497 2.497 0 1 0 4.3 4.656l-.012.006 7.697 5.991a6.176 6.176 0 0 0-1.038 3.446c0 1.343.425 2.588 1.147 3.607l-.013-.02-2.342 2.343a1.968 1.968 0 0 0-.58-.095h-.002a2.033 2.033 0 1 0 2.033 2.033 1.978 1.978 0 0 0-.1-.595l.005.014 2.317-2.317a6.247 6.247 0 1 0 4.782-11.134l-.036-.005zm-.964 9.378a3.206 3.206 0 1 1 3.215-3.207v.002a3.206 3.206 0 0 1-3.211 3.205h-.004z"
      />
    ),
  },

  // Zendesk — the split-Z, 24 grid.
  zendesk: {
    viewBox: "0 0 24 24",
    body: (
      <path
        fill="#03363D"
        d="M12.914 2.904V16.29L24 2.905H12.914zM0 2.906C0 5.966 2.483 8.45 5.543 8.45s5.543-2.484 5.543-5.544H0zm11.086 4.807L0 21.096h11.086V7.713zm1.828 13.19c3.06 0 5.542-2.483 5.542-5.543h-11.085c0 3.06 2.483 5.543 5.543 5.543z"
      />
    ),
  },

  /* Dart — NOT an official mark: we hold no vector for it, so this is a plain
     "D" drawn on the 24 grid in the coral hue it is assigned (a token, so it
     follows the theme). Swap in the real mark when brand supplies one. */
  dart: {
    viewBox: "0 0 24 24",
    body: <path fill="var(--hue-coral)" fillRule="evenodd" d="M5 3h7a9 9 0 0 1 0 18H5V3zm4 4v10h3a5 5 0 0 0 0-10H9z" />,
  },

  // Zoom — white camera on the brand blue tile.
  zoom: {
    viewBox: "0 0 48 48",
    body: (
      <>
        <rect width="48" height="48" rx="11" fill="#2D8CFF" />
        <path
          fill="#fff"
          d="M11 18.5A2.5 2.5 0 0 1 13.5 16h13a2.5 2.5 0 0 1 2.5 2.5v11a2.5 2.5 0 0 1-2.5 2.5h-13a2.5 2.5 0 0 1-2.5-2.5v-11zM31 21.6l5.2-3.7c.7-.5 1.8-.1 1.8.8v10.6c0 .9-1.1 1.3-1.8.8L31 26.4v-4.8z"
        />
      </>
    ),
  },
};

export function isBrandKey(s: string): s is BrandKey {
  return s in BRANDS;
}

/** Brand mark for a connector. `size` is the box in px; the mark fills it.
 *  Renders nothing for a non-brand key (e.g. "paste") — callers fall back to
 *  their own glyph. Decorative by default: the provider name is always beside
 *  it, so announcing the logo too would just repeat it. */
export function ProviderIcon({ provider, size = 22 }: { provider: string; size?: number }) {
  if (!isBrandKey(provider)) return null;
  const { viewBox, body } = BRANDS[provider];
  return (
    <svg width={size} height={size} viewBox={viewBox} aria-hidden="true" focusable="false">
      {body}
    </svg>
  );
}

/** The framed mark used by every connector tile: brand logo where we have one,
 *  the caller's letter where we don't ("P" for paste, "+" for a grouped row).
 *
 *  A brand logo sits on a NEUTRAL tile, not the hue tint the letters used —
 *  Slack's four colours over a plum or amber wash muddies both, and the tint
 *  was only ever there to give a bare letter something to sit in. Non-brand
 *  entries keep the tinted treatment, which still reads correctly for them. */
export function ProviderMark({
  provider,
  letter,
  hue,
  box = 38,
}: {
  provider: string;
  /** Fallback glyph for non-brand entries. */
  letter?: string;
  /** Hue token name, used only for the non-brand fallback. */
  hue?: string;
  box?: number;
}) {
  const branded = isBrandKey(provider);

  /* "ink" is a neutral, not one of the five hues — there is no `--hue-ink`, so
     interpolating it would emit an invalid custom property that silently
     resolves to nothing. Grouped rows ("Gmail, Zoom", "+") use it deliberately
     to read as "not a specific product". */
  const neutral = hue === "ink";
  const hueVar = `var(--hue-${hue ?? "coral"})`;

  return (
    <span
      className={
        branded
          ? "grid flex-none place-items-center rounded-row border border-line bg-bg"
          : "grid flex-none place-items-center rounded-row font-display italic"
      }
      style={
        branded
          ? { width: box, height: box }
          : {
              width: box,
              height: box,
              fontSize: Math.round(box * 0.55),
              background: neutral ? "var(--bg-2)" : `color-mix(in srgb, ${hueVar} 18%, transparent)`,
              color: neutral ? "var(--ink-2)" : hueVar,
            }
      }
    >
      {branded ? <ProviderIcon provider={provider} size={Math.round(box * 0.58)} /> : letter}
    </span>
  );
}
