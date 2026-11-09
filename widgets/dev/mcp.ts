import { JSONRPCMessageSchema } from "@modelcontextprotocol/core";

// Deliberately hosts the compiled, single-file artifacts rather than source
// components. No window.openai injection: every update crosses postMessage.
const html = import.meta.glob<string>("../dist/*.html", { query: "?raw", import: "default", eager: true });
const fixtures = import.meta.glob<{ toolInput?: unknown; toolOutput?: unknown }>("../fixtures/*.json", { import: "default", eager: true });
const template = document.querySelector<HTMLSelectElement>("#template")!;
const fixture = document.querySelector<HTMLSelectElement>("#fixture")!;
const theme = document.querySelector<HTMLSelectElement>("#theme")!;
const mode = document.querySelector<HTMLSelectElement>("#displayMode")!;
const width = document.querySelector<HTMLInputElement>("#width")!;
const widthPreset = document.querySelector<HTMLSelectElement>("#widthPreset")!;
const height = document.querySelector<HTMLInputElement>("#height")!;
const maxHeight = document.querySelector<HTMLInputElement>("#maxHeight")!;
const platform = document.querySelector<HTMLSelectElement>("#platform")!;
const touch = document.querySelector<HTMLInputElement>("#touch")!;
const hover = document.querySelector<HTMLInputElement>("#hover")!;
const safeAreaBottom = document.querySelector<HTMLInputElement>("#safeAreaBottom")!;
const frame = document.querySelector<HTMLIFrameElement>("#widget")!;
const messages = document.querySelector<HTMLPreElement>("#messages")!;
const params = new URLSearchParams(location.search);
let resultTimer: ReturnType<typeof setTimeout> | undefined;
let sizeEvents = 0;

for (const control of [template, theme, mode, width, height, maxHeight, platform, safeAreaBottom]) {
  const value = params.get(control.id);
  if (value) control.value = value;
}
// Checkboxes carry their value in `checked`, not `value` — and `hover`
// defaults to ON, so an absent param must leave the markup default alone
// rather than reading as "false".
for (const box of [touch, hover]) {
  const value = params.get(box.id);
  if (value !== null) box.checked = value !== "false" && value !== "0";
}

function log(message: string) {
  messages.textContent += `${message}\n`;
  messages.scrollTop = messages.scrollHeight;
}

function send(message: object) {
  frame.contentWindow?.postMessage({ jsonrpc: "2.0", ...message }, "*");
}

function context() {
  const bottom = Number(safeAreaBottom.value) || 0;
  return {
    theme: theme.value,
    displayMode: mode.value,
    availableDisplayModes: ["inline", "fullscreen", "pip"],
    // `height` is the frame as it IS (the clipping-bug repro); `maxHeight` is
    // a genuine cap. The widget must treat them differently, so the harness
    // has to be able to send either one.
    containerDimensions: {
      width: Number(width.value),
      ...(height.value ? { height: Number(height.value) } : {}),
      ...(maxHeight.value ? { maxHeight: Number(maxHeight.value) } : {}),
    },
    platform: platform.value,
    deviceCapabilities: { touch: touch.checked, hover: hover.checked },
    // A real mobile host reports all four insets; only the bottom one is
    // adjustable here because it's the one that actually threatens a
    // widget's own footer/controls (the home indicator).
    ...(bottom ? { safeAreaInsets: { top: 0, right: 0, bottom, left: 0 } } : {}),
    locale: "en-US",
  };
}

function sendResult() {
  const fixtureKey = template.value === "result-card-v1" ? fixture.value : `${template.value}.${fixture.value}`;
  const selected = fixtures[`../fixtures/${fixtureKey}.json`];
  if (!selected) return;
  send({ method: "ui/notifications/tool-input", params: { arguments: selected.toolInput ?? {} } });
  send({ method: "ui/notifications/tool-result", params: { content: [], structuredContent: selected.toolOutput } });
  log(`host → tool-input / tool-result (${template.value}: ${fixtureKey})`);
}

/** The fullscreen frame box, in px. A real host hands a fullscreen widget a
 *  fixed shell (its own conversation pane) and ignores the widget's
 *  intrinsic-height reports; only an INLINE widget's height follows
 *  `size-changed`. */
const FULLSCREEN_FRAME_HEIGHT = 800;

function resize() {
  frame.style.width = `${Number(width.value)}px`;
  if (mode.value === "fullscreen") frame.style.height = `${FULLSCREEN_FRAME_HEIGHT}px`;
  document.body.style.background = theme.value === "dark" ? "#24262a" : "#f7f8fa";
  document.body.style.color = theme.value === "dark" ? "#ffffff" : "#14161a";
}

function reload() {
  clearTimeout(resultTimer);
  messages.textContent = "";
  sizeEvents = 0;
  resize();
  const source = html[`../dist/${template.value}.html`];
  if (!source) { log("Missing compiled HTML. Run npm run build first."); return; }
  // Mimic the deployed sandbox's strict external asset allowlist. The
  // SDK must work without unsafe-eval and the designed fonts must load.
  const policy = template.value === "result-card-v1"
    ? "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:; connect-src 'none'"
    : "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src data:; connect-src 'none'";
  frame.srcdoc = source.replace("<head>", `<head><meta http-equiv="Content-Security-Policy" content="${policy}">`);
}

function selectFixture() {
  const variants = template.value === "result-card-v1" ? ["ask_ocean.synthesized", "search_atoms.default", "get_reef.default", "recent_activity.default", "ask_ocean.working", "ask_ocean.empty"] : template.value === "ask_ocean" ? ["synthesized", "empty", "working"] : template.value === "recent_activity" ? ["default", "busy", "empty"] : ["default", "empty"];
  fixture.replaceChildren(...variants.map((name) => new Option(name, name)));
  const selected = params.get("fixture");
  if (selected && variants.includes(selected)) fixture.value = selected;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

window.addEventListener("message", (event) => {
  if (event.source !== frame.contentWindow || event.origin !== "null") return;
  const candidate: unknown = event.data;
  if (!isRecord(candidate) || candidate.jsonrpc !== "2.0") return;
  const invalid = () => send({
    id: typeof candidate.id === "string" || typeof candidate.id === "number" ? candidate.id : null,
    error: { code: -32600, message: "Invalid request" },
  });
  const parsed = JSONRPCMessageSchema.safeParse(candidate);
  if (!parsed.success) { invalid(); return; }
  const message = parsed.data;
  if (!("method" in message)) { log("widget → response"); return; }
  const id = "id" in message ? message.id : undefined;
  const payload = isRecord(message.params) ? message.params : {};
  const reply = (result: object) => { if (id !== undefined) send({ id, result }); };
  if (message.method !== "ui/notifications/size-changed") log(`widget → ${message.method}${typeof payload.name === "string" ? ` ${payload.name}` : ""}`);
  switch (message.method) {
    case "ui/initialize":
      if (id === undefined || typeof payload.protocolVersion !== "string"
          || !isRecord(payload.appInfo) || typeof payload.appInfo.name !== "string"
          || typeof payload.appInfo.version !== "string" || !isRecord(payload.appCapabilities)) {
        invalid(); break;
      }
      reply({ protocolVersion: payload.protocolVersion, hostInfo: { name: "CoralSwarm protocol preview", version: "1.0.0" }, hostCapabilities: { serverTools: {}, openLinks: {} }, hostContext: context() });
      break;
    case "ui/notifications/initialized":
      resultTimer = setTimeout(sendResult, Number(params.get("delay") ?? 200));
      break;
    case "tools/call": {
      const name = payload.name;
      const args = payload.arguments;
      if (id === undefined || typeof name !== "string" || (args !== undefined && !isRecord(args))) {
        invalid(); break;
      }
      const atom: unknown = fixtures["../fixtures/get_atom.body_omitted.json"];
      const output = name === "get_task_result"
        ? fixtures["../fixtures/ask_ocean.synthesized.json"]?.toolOutput
        : name === "get_atom" && isRecord(atom) && args?.atom_id === atom.atom_id ? atom : undefined;
      reply(output ? { content: [], structuredContent: output } : { content: [{ type: "text", text: "Tool not available in this fixture." }], isError: true });
      break;
    }
    case "ui/request-display-mode":
      if (id === undefined || (payload.mode !== "inline" && payload.mode !== "fullscreen" && payload.mode !== "pip")) {
        invalid(); break;
      }
      mode.value = payload.mode;
      width.value = mode.value === "fullscreen" ? "1100" : "700";
      resize();
      reply({ mode: mode.value });
      send({ method: "ui/notifications/host-context-changed", params: context() });
      break;
    case "ui/open-link":
      if (id === undefined || typeof payload.url !== "string") { invalid(); break; }
      log(`host received link: ${payload.url}`);
      reply({});
      break;
    case "ui/notifications/size-changed":
      if (typeof payload.width !== "number" || typeof payload.height !== "number"
          || !Number.isFinite(payload.width) || !Number.isFinite(payload.height)) break;
      document.querySelector("#size-events")!.textContent = `Size notifications: ${++sizeEvents} · ${payload.width} × ${payload.height}px`;
      // See FULLSCREEN_FRAME_HEIGHT: only inline/pip follow the report.
      if (mode.value !== "fullscreen") frame.style.height = `${payload.height}px`;
      break;
    default:
      if (id !== undefined) send({ id, error: { code: -32601, message: "Unknown host method" } });
  }
});

template.addEventListener("change", () => { selectFixture(); reload(); });
fixture.addEventListener("change", sendResult);
// Picking a preset drives the numeric width field (the single source the
// rest of the harness reads), then falls through to the same notify path.
widthPreset.addEventListener("change", () => {
  if (!widthPreset.value) return;
  width.value = widthPreset.value;
  resize();
  send({ method: "ui/notifications/host-context-changed", params: context() });
});
for (const control of [theme, mode, width, height, maxHeight, platform, touch, hover, safeAreaBottom]) control.addEventListener("change", () => {
  resize();
  send({ method: "ui/notifications/host-context-changed", params: context() });
});
document.querySelector("#send-result")!.addEventListener("click", sendResult);
document.querySelector("#cancel")!.addEventListener("click", () => send({ method: "ui/notifications/tool-cancelled", params: {} }));
document.querySelector("#reload")!.addEventListener("click", reload);
selectFixture();
reload();
