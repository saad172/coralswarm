// A minimal, spyable `window.openai` stand-in — shared by the dev harness
// (widgets/dev/main.tsx) and the vitest suite so both exercise the exact
// same mock shape.

import type { DisplayMode, OpenAiApi, Theme, ToolCallResult, ToolResponseMetadata, UserAgentInfo } from "../openai";
import { SET_GLOBALS_EVENT_TYPE } from "../openai";

export interface FakeOpenAiOptions {
  toolOutput?: unknown;
  toolInput?: unknown;
  theme?: Theme;
  displayMode?: DisplayMode;
  maxHeight?: number | null;
  /** The legacy OpenAI Apps SDK device descriptor — see `UserAgentInfo`'s
   *  own doc comment for how it differs from the STANDARD host context's
   *  `userAgent` (a plain string), which `installMcpHost`'s `context`
   *  option carries instead. */
  userAgent?: UserAgentInfo | null;
  /** `callTool` responses keyed by tool name — a function receives `args`
   *  and returns the structuredContent to hand back (or `undefined` to
   *  simulate "no result"). */
  callToolHandlers?: Record<string, (args: Record<string, unknown>) => unknown>;
}

export interface FakeOpenAi extends OpenAiApi {
  /** Every `callTool` invocation, in order — for test assertions. */
  callToolCalls: Array<{ name: string; args: Record<string, unknown> }>;
  /** Push a globals patch and fire `openai:set_globals`, exactly like a real
   *  host update. */
  setGlobals: (patch: Partial<OpenAiApi>) => void;
}

export function installFakeOpenAi(options: FakeOpenAiOptions = {}): FakeOpenAi {
  const state = {
    toolInput: options.toolInput ?? null,
    toolOutput: options.toolOutput ?? null,
    toolResponseMetadata: null as ToolResponseMetadata | null,
    widgetState: null as unknown,
    theme: options.theme ?? "light",
    displayMode: options.displayMode ?? "inline",
    maxHeight: options.maxHeight ?? null,
    safeArea: null,
    locale: "en-US",
    userAgent: options.userAgent ?? null,
  };

  const callToolCalls: FakeOpenAi["callToolCalls"] = [];

  const api: FakeOpenAi = {
    ...state,
    callToolCalls,
    setWidgetState: (next: unknown) => {
      state.widgetState = next;
    },
    callTool: async (name: string, args: Record<string, unknown>): Promise<ToolCallResult> => {
      callToolCalls.push({ name, args });
      const handler = options.callToolHandlers?.[name];
      // Real MCP `CallToolResult` envelope shape (structuredContent, never
      // bare data) — see `ToolCallResult`'s own doc. Wrapping here, rather
      // than in every test's handler, is what makes production `callTool`'s
      // `.structuredContent` unwrap actually exercised by the test suite
      // instead of silently matching a mismatched fake shape.
      return { structuredContent: handler ? handler(args) : undefined };
    },
    sendFollowUpMessage: () => {},
    requestDisplayMode: async ({ mode }) => {
      api.setGlobals({ displayMode: mode });
    },
    requestClose: () => {},
    notifyIntrinsicHeight: () => {},
    openExternal: () => {},
    setOpenInAppUrl: () => {},
    setGlobals: (patch: Partial<OpenAiApi>) => {
      Object.assign(api, patch);
      Object.assign(state, patch);
      if (typeof window !== "undefined") {
        window.dispatchEvent(new CustomEvent(SET_GLOBALS_EVENT_TYPE, { detail: { globals: patch } }));
      }
    },
  };

  if (typeof window !== "undefined") {
    window.openai = api;
  }

  return api;
}

export function uninstallFakeOpenAi(): void {
  if (typeof window !== "undefined") {
    delete window.openai;
  }
}
