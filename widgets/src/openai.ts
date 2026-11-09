// Shared MCP Apps bridge with compatible window.openai and standalone previews.
// Protocol lifecycle/validation comes from the official ext-apps SDK.

import { useLayoutEffect, useState } from "preact/hooks";
import { App, type McpUiHostContext } from "@modelcontextprotocol/ext-apps";
import { ParentTransport, acceptsParentMessage } from "./mcpTransport";

export type DisplayMode = "inline" | "pip" | "fullscreen";
export type Theme = "light" | "dark";

/** Display modes these widgets actually implement. Declared on
 *  `ui/initialize` so a host keeps its expand/fullscreen chrome — the spec
 *  tells hosts to offer only modes the view advertised, and omitting the
 *  list is how Claude/ChatGPT collapse that control on some surfaces. */
export const WIDGET_DISPLAY_MODES: DisplayMode[] = ["inline", "fullscreen"];

export interface SafeArea {
  insets?: { top?: number; right?: number; bottom?: number; left?: number };
}

export interface ToolResponseMetadata {
  status?: string;
  call_tool_result?: unknown;
  mcp_tool_result?: unknown;
  [key: string]: unknown;
}

/** Legacy `window.openai.userAgent` — the OpenAI Apps SDK's device
 *  descriptor. Distinct from the STANDARD host context's own `userAgent`
 *  field, which the ext-apps schema types as a plain host-identifier
 *  `string` (see `node_modules/@modelcontextprotocol/ext-apps/dist/src/
 *  spec.types.d.ts`'s `McpUiHostContext.userAgent`) — a different field
 *  with the same name, not the same shape. Never read the standard
 *  context's `userAgent` as this type. */
export interface UserAgentInfo {
  device?: { type?: "mobile" | "tablet" | "desktop" | "unknown" };
  capabilities?: { hover?: boolean; touch?: boolean };
}

/** The narrow legacy `window.openai` surface these widgets still support.
 * The standard MCP Apps surface is supplied by the official SDK. */
export interface OpenAiGlobals<TOutput = unknown, TInput = unknown> {
  toolInput: TInput | null;
  toolOutput: TOutput | null;
  toolResponseMetadata: ToolResponseMetadata | null;
  widgetState: unknown;
  theme: Theme;
  displayMode: DisplayMode;
  maxHeight: number | null;
  safeArea: SafeArea | null;
  locale: string | null;
  userAgent?: UserAgentInfo | null;
}

/** Standard-host-only layout signals with no legacy `window.openai` field of
 *  the same name (a legacy host describes them, if at all, through
 *  `userAgent` above instead — see `deriveLegacyLayout`). Kept as a
 *  separate type from `OpenAiGlobals` (the documented legacy surface) so
 *  adding these doesn't imply `window.openai` itself ever carries them. */
export interface HostLayoutFields {
  width: number | null;
  platform: "web" | "desktop" | "mobile" | null;
  touch: boolean | null;
  hover: boolean | null;
  /** Host-advertised modes from `HostContext.availableDisplayModes`.
   *  `null` means the host never sent the field (legacy / no-host). */
  availableDisplayModes: DisplayMode[] | null;
}

/** Everything the bridge singleton tracks per mounted widget: the legacy
 *  globals plus the standard-only layout fields above. */
type BridgeGlobals = Partial<OpenAiGlobals> & Partial<HostLayoutFields>;

/** A legacy host's `userAgent.device.type`/`capabilities` translated into
 *  the same `platform`/`touch`/`hover` vocabulary the standard host context
 *  uses, so both sources feed one set of fields. "tablet" folds into
 *  "mobile" for layout purposes (per `useHostLayout`'s contract) and always
 *  implies touch; "desktop" (a device form factor, not a HOST platform)
 *  maps to "web" — the legacy schema can't distinguish a native desktop
 *  app host from a browser tab on a desktop computer, and "web" is the
 *  safer default (it's also what the browser-only fallback assumes).
 *  "unknown"/absent yields no signal (`null`), leaving the field to the
 *  next source in `useHostLayout`'s precedence. */
function deriveLegacyLayout(ua: UserAgentInfo | null | undefined): Pick<HostLayoutFields, "platform" | "touch" | "hover"> {
  const type = ua?.device?.type;
  const platform: HostLayoutFields["platform"] =
    type === "mobile" || type === "tablet" ? "mobile" : type === "desktop" ? "web" : null;
  const touch: HostLayoutFields["touch"] = type === "tablet" ? true : ua?.capabilities?.touch ?? null;
  const hover: HostLayoutFields["hover"] = ua?.capabilities?.hover ?? null;
  return { platform, touch, hover };
}

/** Merge a legacy `userAgent`'s derived platform/touch/hover into a patch,
 *  without clobbering a value the patch (or an earlier merge) already set
 *  explicitly. Shared by the initial synchronous read and every subsequent
 *  `openai:set_globals` patch so both paths behave identically. */
function withLegacyLayout(patch: BridgeGlobals): BridgeGlobals {
  if (!patch.userAgent) return patch;
  const derived = deriveLegacyLayout(patch.userAgent);
  if (derived.platform !== null) patch.platform ??= derived.platform;
  if (derived.touch !== null) patch.touch ??= derived.touch;
  if (derived.hover !== null) patch.hover ??= derived.hover;
  return patch;
}

/** The MCP `CallToolResult` envelope `window.openai.callTool` actually
 *  resolves with (per developers.openai.com/apps-sdk/build/custom-ux) — the
 *  same shape the backend's `rmcp::CallToolResult` produces on the wire
 *  (`structured_content` -> `structuredContent`). Application data lives at
 *  `.structuredContent`, never at the top level of the resolved value. */
export interface ToolCallResult<T = unknown> {
  structuredContent?: T;
  content?: unknown[];
  isError?: boolean;
  [key: string]: unknown;
}

export interface OpenAiApi extends OpenAiGlobals {
  setWidgetState: (state: unknown) => void;
  callTool: (name: string, args: Record<string, unknown>) => Promise<ToolCallResult>;
  sendFollowUpMessage: (opts: { prompt: string; scrollToBottom?: boolean }) => void;
  requestDisplayMode: (opts: { mode: DisplayMode }) => Promise<unknown> | void;
  requestModal?: (opts: { params?: unknown; template?: string }) => void;
  requestClose?: () => void;
  notifyIntrinsicHeight?: (height: number) => void;
  openExternal: (opts: { href: string; redirectUrl?: boolean }) => void;
  setOpenInAppUrl?: (opts: { href: string }) => void;
}

declare global {
  interface Window {
    openai?: OpenAiApi;
  }
}

export const SET_GLOBALS_EVENT_TYPE = "openai:set_globals";

function readInitialGlobals(): BridgeGlobals {
  let fallback: Partial<OpenAiGlobals> = {};
  try {
    const text = document.getElementById("cs-data")?.textContent;
    const value: unknown = text ? JSON.parse(text) : null;
    if (value && typeof value === "object") fallback = value;
  } catch { /* Malformed preview data should leave a usable empty widget. */ }
  return withLegacyLayout({ ...fallback, ...window.openai });
}

interface Bridge {
  globals: BridgeGlobals;
  listeners: Set<() => void>;
  app: App | null;
  connected: boolean;
  standardData: boolean;
  standardContext: Set<keyof BridgeGlobals>;
  invocation: number;
  dispose: () => void;
}

let bridge: Bridge | undefined;

function patchBridge(current: Bridge, patch: BridgeGlobals) {
  if (bridge !== current) return;
  Object.assign(current.globals, patch);
  for (const listener of current.listeners) listener();
}

/** True once the standard host has ever supplied `key` via
 *  `contextGlobals` — used outside this module only for diagnostics/tests;
 *  templates should read resolved fields (`useHostLayout`, `useOpenAiGlobal`)
 *  rather than this precedence detail directly. */
export function hasStandardContext(key: keyof BridgeGlobals): boolean {
  return bridge?.standardContext.has(key) ?? false;
}

function contextGlobals(context: McpUiHostContext): BridgeGlobals {
  const patch: BridgeGlobals = {};
  if (context.theme) patch.theme = context.theme;
  if (context.displayMode) patch.displayMode = context.displayMode;
  if (context.locale) patch.locale = context.locale;
  if (context.safeAreaInsets) patch.safeArea = { insets: context.safeAreaInsets };
  if (context.platform) patch.platform = context.platform;
  if (context.deviceCapabilities) {
    if ("touch" in context.deviceCapabilities) patch.touch = context.deviceCapabilities.touch ?? null;
    if ("hover" in context.deviceCapabilities) patch.hover = context.deviceCapabilities.hover ?? null;
  }
  if (context.availableDisplayModes) {
    const modes = context.availableDisplayModes.filter(
      (mode): mode is DisplayMode => mode === "inline" || mode === "fullscreen" || mode === "pip",
    );
    // Assign even when empty so `requestFullscreen()` sees an explicit
    // "host supports none of these" list instead of treating an omitted
    // field as unknown (and so a later empty update can clear a prior
    // `["fullscreen"]` value rather than leaving it stuck).
    patch.availableDisplayModes = modes;
  }
  if (context.containerDimensions) {
    const dimensions = context.containerDimensions;
    // A fixed height describes the current iframe, not an intrinsic-size
    // limit. Feeding it back into the inline layout traps the SDK's resize
    // observer at a stale height and clips newly rendered content.
    if ("height" in dimensions) patch.maxHeight = null;
    else if ("maxHeight" in dimensions) patch.maxHeight = dimensions.maxHeight ?? null;
    // Width has no such feedback loop (nothing measures against it and
    // reports back), so a `maxWidth` cap is fine to reuse as a width
    // ESTIMATE for `useHostLayout`'s size tiers — unlike maxHeight above,
    // this can't reproduce the stale-cap bug.
    if ("width" in dimensions) patch.width = dimensions.width;
    else if ("maxWidth" in dimensions) patch.width = dimensions.maxWidth ?? null;
  }
  return patch;
}

/** One connection and subscription set per mounted widget, shared by every
 * hook. Handlers are installed before ui/initialize so a result delivered
 * immediately after initialization cannot disappear between React effects. */
function getBridge(): Bridge {
  if (bridge) return bridge;
  const current: Bridge = {
    globals: readInitialGlobals(), listeners: new Set(), app: null,
    connected: false, standardData: false, standardContext: new Set(), invocation: 0, dispose: () => {},
  };
  bridge = current;
  let parentOrigin: string | undefined;
  const globals = (event: Event) => {
    // Once the standard host supplies data, late legacy-global echoes must
    // never roll the widget back to a prior answer or working task.
    const patch = (event as CustomEvent<{ globals?: BridgeGlobals }>).detail?.globals;
    if (!patch) return;
    const allowed: BridgeGlobals = { ...patch };
    if (current.standardData) {
      delete allowed.toolInput;
      delete allowed.toolOutput;
      delete allowed.toolResponseMetadata;
    }
    // A legacy `userAgent` echo also derives platform/touch/hover under
    // their own keys so the standardContext filtering right below (never
    // let a legacy echo override a field the standard host already
    // supplied) covers them too, even though they arrive under a
    // different wire key than the standard fields they correspond to.
    withLegacyLayout(allowed);
    // Context fields omitted by the standard host can still be supplied
    // by the legacy bridge, without overriding standard theme/layout.
    for (const key of current.standardContext) delete allowed[key];
    patchBridge(current, allowed);
  };
  const fallback = (event: MessageEvent) => {
    if (!acceptsParentMessage(event, parentOrigin) || current.standardData || current.connected || window.openai) return;
    const data = event.data;
    if (!data || data.type !== "cs:tool-output") return;
    parentOrigin ??= event.origin;
    const patch: Partial<OpenAiGlobals> = {};
    if ("toolOutput" in data) patch.toolOutput = data.toolOutput ?? null;
    if ("toolInput" in data) patch.toolInput = data.toolInput ?? null;
    patchBridge(current, patch);
  };
  window.addEventListener(SET_GLOBALS_EVENT_TYPE, globals);
  window.addEventListener("message", fallback);
  let stopResize: (() => void) | undefined;
  current.dispose = () => {
    if (bridge !== current) return;
    bridge = undefined;
    current.connected = false;
    window.removeEventListener(SET_GLOBALS_EVENT_TYPE, globals);
    window.removeEventListener("message", fallback);
    window.removeEventListener("pagehide", current.dispose);
    stopResize?.();
    void current.app?.close();
  };
  window.addEventListener("pagehide", current.dispose, { once: true });

  // A top-level preview has no MCP host. Keep its inline/OpenAI fallback
  // usable without a self-addressed handshake or a dangling request timer.
  if (window.parent !== window) {
    const app = new App(
      { name: "CoralSwarm widgets", version: "2.0.0" },
      { availableDisplayModes: WIDGET_DISPLAY_MODES },
      { autoResize: false },
    );
    current.app = app;
    const input = ({ arguments: args }: { arguments?: Record<string, unknown> }) => {
      current.standardData = true;
      current.invocation++;
      patchBridge(current, { toolInput: args ?? {}, toolOutput: null, toolResponseMetadata: null });
    };
    app.addEventListener("toolinput", input);
    app.addEventListener("toolinputpartial", input);
    app.addEventListener("toolresult", (result) => {
      current.standardData = true;
      patchBridge(current, {
        toolOutput: result.structuredContent ?? (result.isError ? {
          error: { code: "widget_tool_failed", message: "The tool could not complete this request." },
        } : null),
        toolResponseMetadata: result._meta ?? null,
      });
    });
    app.addEventListener("toolcancelled", () => {
      current.standardData = true;
      current.invocation++;
      patchBridge(current, { toolOutput: { error: { code: "cancelled", message: "The request was cancelled." } } });
    });
    const hostContext = (context: McpUiHostContext) => {
      const patch = contextGlobals(context);
      for (const key of Object.keys(patch) as Array<keyof BridgeGlobals>) current.standardContext.add(key);
      patchBridge(current, patch);
    };
    app.addEventListener("hostcontextchanged", hostContext);
    // The host removes the iframe after this response. pagehide/unmount
    // closes transport and observers without dropping the teardown reply.
    app.onteardown = async () => { stopResize?.(); return {}; };
    void app.connect(new ParentTransport(), { timeout: 10_000 }).then(() => {
      if (bridge !== current) return;
      current.connected = true;
      hostContext(app.getHostContext() ?? {});
      if (typeof ResizeObserver !== "undefined") stopResize = app.setupSizeChangedNotifications();
    }).catch(() => {
      // Legacy OpenAI hosts and standalone iframes may not speak MCP Apps.
      // Existing fallback data and future OpenAI updates still render.
      current.connected = false;
    });
  }
  return current;
}

export function hasOpenAiHost(): boolean {
  return typeof window !== "undefined" && window.openai != null;
}

/** Shared subscription primitive behind every single-field hook below
 *  (legacy `OpenAiGlobals` keys and the standard-only `HostLayoutFields`
 *  keys alike) — both are just properties of the same bridge object. `T` is
 *  supplied explicitly by each typed wrapper below rather than derived from
 *  `key` via an indexed-access return type, so a caller generic over only
 *  ONE of the two key unions (as both wrappers are) still type-checks. */
function useBridgeField<T>(key: keyof BridgeGlobals, fallback: T): T {
  const read = (): T => {
    const source = bridge?.globals ?? readInitialGlobals();
    return (source[key] as T | undefined) ?? fallback;
  };
  const [value, setValue] = useState<T>(read);
  useLayoutEffect(() => {
    const current = getBridge();
    const update = () => setValue((current.globals[key] as T | undefined) ?? fallback);
    current.listeners.add(update);
    update();
    return () => {
      current.listeners.delete(update);
      if (current.listeners.size === 0) current.dispose();
    };
  }, [key, fallback]);
  return value;
}

/** Historical hook name retained so the approved templates need no changes.
 * All globals now update from MCP Apps notifications as well as OpenAI. */
export function useOpenAiGlobal<K extends keyof OpenAiGlobals>(
  key: K, fallback: OpenAiGlobals[K],
): OpenAiGlobals[K] {
  return useBridgeField<OpenAiGlobals[K]>(key, fallback);
}

/** `useHostLayout` (`./layout.ts`)'s low-level building block — one
 *  standard-only field (already legacy-merged and precedence-resolved by
 *  the bridge; see `HostLayoutFields`/`deriveLegacyLayout`). */
export function useHostLayoutField<K extends keyof HostLayoutFields>(
  key: K, fallback: HostLayoutFields[K],
): HostLayoutFields[K] {
  return useBridgeField<HostLayoutFields[K]>(key, fallback);
}

export function useToolOutput<TOutput>(): TOutput | null {
  return useOpenAiGlobal("toolOutput", null) as TOutput | null;
}

export function useToolInput<TInput>(): TInput | null {
  return useOpenAiGlobal("toolInput", null) as TInput | null;
}

export function useTheme(): Theme {
  return useOpenAiGlobal("theme", "light");
}

export function useDisplayMode(): DisplayMode {
  return useOpenAiGlobal("displayMode", "inline");
}

export function useMaxHeight(): number | null {
  return useOpenAiGlobal("maxHeight", null);
}

/** The widget's only interactive tool permissions match the server's app
 * allowlist. Both bridges return the same MCP CallToolResult envelope. */
export async function callTool<T = unknown>(name: string, args: Record<string, unknown>): Promise<T | null> {
  if (name !== "get_atom" && name !== "get_task_result") {
    throw new Error(`Tool ${name} is not available to widgets.`);
  }
  const current = bridge;
  const invocation = current?.invocation;
  const stale = () => current && (bridge !== current || invocation !== current.invocation);
  let result: ToolCallResult | undefined;
  try {
    result = current?.connected && current.app
      ? await current.app.callServerTool({ name, arguments: args })
      : await window.openai?.callTool(name, args);
  } catch (error) {
    // Rejections from a previous invocation are stale too. In particular,
    // they must not set the new task's error before effect cleanup runs.
    if (stale()) return null;
    throw error;
  }
  // A slow citation fetch/task poll from the previous invocation must not
  // populate the new invocation's card after its input has arrived.
  if (stale()) return null;
  if (result?.isError) throw new Error("The requested tool could not complete this request.");
  return (result?.structuredContent as T | undefined) ?? null;
}

export function requestFullscreen(): void {
  const current = bridge;
  const modes = current?.globals.availableDisplayModes;
  if (modes && !modes.includes("fullscreen")) return;
  if (current?.connected && current.app) {
    void current.app.requestDisplayMode({ mode: "fullscreen" }).then(({ mode }) => {
      patchBridge(current, { displayMode: mode });
    }).catch(() => { /* Hosts may decline a display mode change. */ });
    return;
  }
  void Promise.resolve(window.openai?.requestDisplayMode({ mode: "fullscreen" })).catch(() => {});
}

export function openExternalLink(href: string): void {
  if (bridge?.connected && bridge.app) {
    void bridge.app.openLink({ url: href }).catch(() => {});
  } else if (window.openai) {
    window.openai.openExternal({ href });
  } else {
    window.open(href, "_blank", "noopener,noreferrer");
  }
}
