import { render, type ComponentType } from "preact";
import { installFakeOpenAi } from "../src/test/fakeOpenAi";
import { AskOceanTemplate } from "../src/templates/AskOcean";
import { SearchAtomsTemplate } from "../src/templates/SearchAtoms";
import { GetReefTemplate } from "../src/templates/GetReef";
import { RecentActivityTemplate } from "../src/templates/RecentActivity";
import type { DisplayMode, Theme, UserAgentInfo } from "../src/openai";
import type { AtomDetail } from "../src/types";

// Every fixture, eagerly loaded (Vite glob import) so the harness can list
// and pick any of them without a network round trip.
const fixtures = import.meta.glob("../fixtures/*.json", { eager: true }) as Record<
  string,
  { default: { toolInput?: unknown; toolOutput?: unknown } }
>;

function fixtureKey(template: string, variant: string): string {
  return `../fixtures/${template}.${variant}.json`;
}

const TEMPLATES: Record<string, { component: ComponentType; defaultFixture: string; variants: string[] }> = {
  ask_ocean: { component: AskOceanTemplate, defaultFixture: "synthesized", variants: ["synthesized", "empty", "working"] },
  search_atoms: { component: SearchAtomsTemplate, defaultFixture: "default", variants: ["default", "empty"] },
  get_reef: { component: GetReefTemplate, defaultFixture: "default", variants: ["default", "empty"] },
  recent_activity: { component: RecentActivityTemplate, defaultFixture: "default", variants: ["default", "busy", "empty"] },
};

// get_atom's fixture is a raw AtomDetail (get_atom's structuredContent has
// no toolInput/toolOutput envelope of its own here — it's a `callTool`
// response, not a page-level tool output).
const atomFixture = fixtures["../fixtures/get_atom.body_omitted.json"]?.default as unknown as AtomDetail | undefined;

function renderMenu(root: HTMLElement) {
  const links = Object.entries(TEMPLATES).flatMap(([template, cfg]) =>
    cfg.variants.flatMap((variant) =>
      (["light", "dark"] as const).map((theme) => ({ template, variant, theme })),
    ),
  );
  root.innerHTML = `
    <div style="max-width:700px;font-family:inherit">
      <h1 style="font-size:20px;">CoralSwarm widgets — dev harness</h1>
      <p style="color:#666;font-size:13px;">
        URL params: <code>?template=&fixture=&theme=&displayMode=&maxHeight=&platform=&touch=</code>
      </p>
      <ul style="line-height:1.9;padding-left:18px;">
        ${links
          .map(
            ({ template, variant, theme }) =>
              `<li><a href="?template=${template}&fixture=${variant}&theme=${theme}">${template} · ${variant} · ${theme}</a></li>`,
          )
          .join("")}
        <li><a href="?template=ask_ocean&fixture=synthesized&displayMode=fullscreen">ask_ocean · synthesized · fullscreen</a></li>
      </ul>
    </div>
  `;
}

/** Translate the harness's `?platform=&touch=` params into the legacy
 *  `window.openai.userAgent` shape. `platform=mobile|tablet` implies touch
 *  unless `?touch=false` says otherwise; `?touch` alone works on any
 *  platform (a touchscreen laptop). Returns `null` when neither param is
 *  present, so the default run exercises the browser-only fallback. */
function legacyUserAgent(platform: string | null, touch: string | null): UserAgentInfo | null {
  if (!platform && touch === null) return null;
  const type: NonNullable<UserAgentInfo["device"]>["type"] =
    platform === "mobile" ? "mobile" : platform === "tablet" ? "tablet" : platform === "desktop" ? "desktop" : "unknown";
  const touchy = touch !== null ? touch !== "false" && touch !== "0" : type === "mobile" || type === "tablet";
  return { device: { type }, capabilities: { touch: touchy, hover: !touchy } };
}

function main() {
  const root = document.getElementById("harness");
  if (!root) return;

  const params = new URLSearchParams(window.location.search);
  const template = params.get("template");
  // A direct lookup (rather than `template in TEMPLATES` followed by a
  // non-null-asserted re-lookup) also correctly rejects an inherited
  // property name like `toString` that `in` would otherwise accept.
  const cfg = template ? TEMPLATES[template] : undefined;

  if (!template || !cfg) {
    renderMenu(root);
    return;
  }
  const variant = params.get("fixture") ?? cfg.defaultFixture;
  const theme = (params.get("theme") as Theme | null) ?? "light";
  const displayMode = (params.get("displayMode") as DisplayMode | null) ?? "inline";
  const maxHeightParam = params.get("maxHeight");
  const maxHeight = maxHeightParam ? Number(maxHeightParam) : null;
  // This harness speaks the LEGACY surface only, so platform/touch arrive
  // the way a legacy host describes them — `userAgent.device.type` plus
  // capabilities — not as the standard context's own fields. `useHostLayout`
  // translates them (see `deriveLegacyLayout`).
  const userAgent = legacyUserAgent(params.get("platform"), params.get("touch"));

  document.body.setAttribute("data-theme", theme);

  const fixtureModule = fixtures[fixtureKey(template, variant)];
  if (!fixtureModule) {
    root.textContent = `No fixture named "${template}.${variant}.json".`;
    return;
  }
  const fixture = fixtureModule.default;

  installFakeOpenAi({
    toolInput: fixture.toolInput,
    toolOutput: fixture.toolOutput,
    theme,
    displayMode,
    maxHeight,
    userAgent,
    callToolHandlers: {
      // get_atom: only ever asked for the one body_omitted fixture atom in
      // these fixtures; anything else returns undefined (no result), same
      // as a real host with no data for that id.
      get_atom: (args) => (atomFixture && args.atom_id === atomFixture.atom_id ? atomFixture : undefined),
      // get_task_result: resolve the "working" ask_ocean fixture into the
      // "synthesized" one's completed body after the first poll, so the
      // harness demonstrates the Working -> Answer transition live.
      get_task_result: () => (fixtures[fixtureKey("ask_ocean", "synthesized")]?.default as { toolOutput?: unknown } | undefined)?.toolOutput,
    },
  });

  const Component = cfg.component;
  render(<Component />, root);
}

main();
