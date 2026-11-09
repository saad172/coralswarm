import { useLayoutEffect, useState } from "preact/hooks";
import type { OpenAiApi } from "../openai";

export const INITIALIZE_ID = "coralswarm-result-card-v1";
interface LegacyContext {
  output: unknown;
  theme: "light" | "dark";
  displayMode: "inline" | "fullscreen";
  maxHeight: number | null;
  insets: { top: number; right: number; bottom: number; left: number };
  touch: boolean;
}
export function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
}
function finite(value: unknown): number | null { return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null; }
function context(previous: LegacyContext, globals: Record<string, unknown>): LegacyContext {
  const safe = record(record(globals.safeArea)?.insets) ?? record(globals.safeAreaInsets);
  const dimensions = record(globals.containerDimensions);
  const capabilities = record(globals.deviceCapabilities) ?? record(record(globals.userAgent)?.capabilities);
  return {
    ...previous,
    theme: globals.theme === "dark" || globals.theme === "light" ? globals.theme : previous.theme,
    displayMode: globals.displayMode === "inline" || globals.displayMode === "fullscreen" ? globals.displayMode : previous.displayMode,
    maxHeight: "maxHeight" in globals ? finite(globals.maxHeight) : dimensions && "maxHeight" in dimensions ? finite(dimensions.maxHeight) : previous.maxHeight,
    insets: safe ? { top: finite(safe.top) ?? 0, right: finite(safe.right) ?? 0, bottom: finite(safe.bottom) ?? 0, left: finite(safe.left) ?? 0 } : previous.insets,
    touch: typeof capabilities?.touch === "boolean" ? capabilities.touch : previous.touch,
  };
}
function initial(): LegacyContext {
  const dark = window.matchMedia?.("(prefers-color-scheme: dark)").matches;
  const base: LegacyContext = { output: null, theme: dark ? "dark" : "light", displayMode: "inline", maxHeight: null, insets: { top: 0, right: 0, bottom: 0, left: 0 }, touch: window.matchMedia?.("(pointer: coarse)").matches ?? false };
  return context({ ...base, output: window.openai?.toolOutput ?? null }, record(window.openai) ?? {});
}

/** Cached v1 callers retain their fixed handshake and notification-only
 * contract. Do not load the v2 SDK here or send a second initialization. */
export function useLegacyBridge(): LegacyContext {
  const [state, setState] = useState(initial);
  useLayoutEffect(() => {
    let standardData = false;
    let origin: string | undefined;
    const post = (message: object) => window.parent.postMessage({ jsonrpc: "2.0", ...message }, origin && origin !== "null" ? origin : "*");
    const receive = (event: MessageEvent) => {
      if (event.source !== window.parent || (origin !== undefined && event.origin !== origin)) return;
      const message = record(event.data);
      if (message?.jsonrpc !== "2.0") return;
      if (message.id === INITIALIZE_ID) {
        if (!message.error) {
          origin ??= event.origin;
          const hostContext = record(record(message.result)?.hostContext);
          if (hostContext) setState(previous => context(previous, hostContext));
          post({ method: "ui/notifications/initialized" });
        }
        return;
      }
      const params = record(message.params);
      switch (message.method) {
        case "ui/notifications/tool-input":
        case "ui/notifications/tool-input-partial":
          origin ??= event.origin; standardData = true;
          setState(previous => ({ ...previous, output: null }));
          break;
        case "ui/notifications/tool-cancelled":
          origin ??= event.origin; standardData = true;
          setState(previous => ({ ...previous, output: { status: "cancelled" } }));
          break;
        case "ui/notifications/tool-result":
          origin ??= event.origin; standardData = true;
          // A missing/null structuredContent historically kept the current
          // result. isError is the one additional actionable host signal.
          if (params?.structuredContent != null) setState(previous => ({ ...previous, output: params.structuredContent }));
          else if (params?.isError) setState(previous => ({ ...previous, output: { error: { code: "tool_failed", message: "The tool could not complete this request." } } }));
          break;
        case "ui/notifications/host-context-changed":
          origin ??= event.origin;
          if (params) setState(previous => context(previous, params));
      }
    };
    const globals = (event: Event) => {
      const patch = record(record((event as CustomEvent).detail)?.globals);
      if (!patch) return;
      setState(previous => context({ ...previous, output: !standardData && "toolOutput" in patch ? patch.toolOutput ?? null : previous.output }, patch));
    };
    window.addEventListener("message", receive);
    window.addEventListener("openai:set_globals", globals);
    if (window.parent !== window) post({ id: INITIALIZE_ID, method: "ui/initialize", params: {
      protocolVersion: "2026-01-26", appInfo: { name: "CoralSwarm result card", version: "1.0.0" },
      appCapabilities: { availableDisplayModes: ["inline"] },
    } });
    return () => { window.removeEventListener("message", receive); window.removeEventListener("openai:set_globals", globals); };
  }, []);
  return state;
}

/** Old OpenAI hosts may offer task polling. Notification-only MCP parents
 * did not grant tools/call, so keep waiting for their next real result. */
export function legacyTaskCaller(): Pick<OpenAiApi, "callTool"> | undefined {
  return typeof window.openai?.callTool === "function" ? window.openai : undefined;
}
