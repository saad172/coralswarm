// Single source of truth for "how should this render given the host's
// viewport and platform" — every template reads layout from `useHostLayout`
// instead of hardcoding pixel breakpoints. See openai.ts's `HostLayoutFields`
// / `deriveLegacyLayout` for how the underlying bridge resolves each field
// across the standard MCP Apps host context, the legacy `window.openai`
// surface, and (when both are silent) live browser signals.

import { useEffect, useRef, useState } from "preact/hooks";
import { useDisplayMode, useHostLayoutField, useMaxHeight, useOpenAiGlobal, type DisplayMode } from "./openai";

export type SizeTier = "compact" | "regular" | "wide";
export type HostPlatform = "web" | "desktop" | "mobile";

export interface SafeAreaInsets {
  top: number;
  right: number;
  bottom: number;
  left: number;
}

export interface HostLayout {
  /** Container width in px — see `useLiveWidth`'s doc comment for exactly
   *  how this is sourced between a host-supplied value and this window's
   *  own `innerWidth`. */
  width: number;
  /** The host's `containerDimensions.maxHeight` cap ONLY — never a fixed
   *  `height` (see `openai.ts`'s `contextGlobals`). `null` means uncapped. */
  maxHeight: number | null;
  displayMode: DisplayMode;
  platform: HostPlatform;
  touch: boolean;
  hover: boolean;
  /** Derived from `width`: <480 compact, 480–899 regular, >=900 wide. */
  size: SizeTier;
  safeArea: SafeAreaInsets;
  /** Host-advertised display modes, or `null` when the host never sent them. */
  availableDisplayModes: DisplayMode[] | null;
}

/** Below this, even a phone in portrait would be misclassified "regular" —
 *  chosen to match a small phone's CSS viewport width, not a raw device
 *  pixel count. */
const COMPACT_MAX = 480;
/** Below this, a tablet/split-screen/PiP-sized container still gets the
 *  roomier "regular" treatment rather than "wide"'s multi-column layouts. */
const REGULAR_MAX = 900;

function sizeOf(width: number): SizeTier {
  if (width < COMPACT_MAX) return "compact";
  if (width < REGULAR_MAX) return "regular";
  return "wide";
}

/** A reasonable guess before any real signal (host message or a resize
 *  event) has arrived — deliberately "regular", not "compact" or "wide",
 *  so a slow-to-report host doesn't flash an extreme layout first. */
const DEFAULT_WIDTH = 700;

function readInnerWidth(): number {
  return typeof window !== "undefined" && Number.isFinite(window.innerWidth) && window.innerWidth > 0
    ? window.innerWidth
    : DEFAULT_WIDTH;
}

function matches(query: string): boolean {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") return false;
  try {
    return window.matchMedia(query).matches;
  } catch {
    // A widget sandbox that throws on an unsupported media feature should
    // degrade to "no", not crash the render.
    return false;
  }
}

/** Live width, kept current via a `resize` listener — the only one of the
 *  host-vs-browser signals below that changes mid-session without a fresh
 *  host message (platform/touch/hover don't change after mount). */
function useLiveWidth(): { liveWidth: number; resized: boolean } {
  const [liveWidth, setLiveWidth] = useState(readInnerWidth);
  const resizedRef = useRef(false);
  useEffect(() => {
    const onResize = () => {
      resizedRef.current = true;
      setLiveWidth(readInnerWidth());
    };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);
  return { liveWidth, resized: resizedRef.current };
}

/** Combines every source for one `HostLayout` per render. Width precedence
 *  is the one subtle case (see `useLiveWidth`'s doc comment): a host-
 *  supplied `containerDimensions.width`/`maxWidth` wins ONLY until this
 *  window has observed its OWN `resize` event at least once. A `resize`
 *  here is a direct DOM fact about the iframe's actual current box — no
 *  message round trip, never stale — so once observed it stays authoritative
 *  even over a later host-context update (a host that resizes its iframe
 *  should also push an updated width; if it doesn't, the live value is
 *  still correct and the stale message-based one is not). Before any
 *  resize, the host's snapshot is preferred over `innerWidth` because nothing
 *  has yet proven the browser signal is live rather than a leftover default. */
export function useHostLayout(): HostLayout {
  const displayMode = useDisplayMode();
  const maxHeight = useMaxHeight();
  const hostWidth = useHostLayoutField("width", null);
  const hostPlatform = useHostLayoutField("platform", null);
  const hostTouch = useHostLayoutField("touch", null);
  const hostHover = useHostLayoutField("hover", null);
  const hostModes = useHostLayoutField("availableDisplayModes", null);
  const safeAreaGlobal = useOpenAiGlobal("safeArea", null);
  const { liveWidth, resized } = useLiveWidth();

  const width = resized ? liveWidth : hostWidth ?? liveWidth;
  const platform: HostPlatform = hostPlatform ?? "web";
  const touch = hostTouch ?? matches("(pointer: coarse)");
  const hover = hostHover ?? matches("(hover: hover)");

  const insets = safeAreaGlobal?.insets;
  const safeArea: SafeAreaInsets = {
    top: insets?.top ?? 0,
    right: insets?.right ?? 0,
    bottom: insets?.bottom ?? 0,
    left: insets?.left ?? 0,
  };

  return { width, maxHeight, displayMode, platform, touch, hover, size: sizeOf(width), safeArea, availableDisplayModes: hostModes };
}

/** The iframe viewport height, for the ONE place a widget needs a real
 *  bound rather than its intrinsic height (ask_ocean's fullscreen shell,
 *  whose two panes each scroll). `dvh` tracks a mobile browser's collapsing
 *  toolbars where `vh` does not, so it is preferred wherever the engine
 *  supports it; `CSS.supports` is checked once at module load because the
 *  answer cannot change mid-session. Never use this for an INLINE widget —
 *  inline height comes from the content, reported to the host by the SDK's
 *  size-changed notifications. */
export const VIEWPORT_HEIGHT: string =
  typeof CSS !== "undefined" && typeof CSS.supports === "function" && CSS.supports("height", "100dvh")
    ? "100dvh"
    : "100vh";

/** Horizontal gutter for a `size` tier, given today's (pre-responsive)
 *  value: unchanged at regular/wide, ~12px at compact. Every template's
 *  hardcoded 14–18px content-section padding runs through this so they all
 *  shrink together on a narrow host instead of each carrying its own
 *  breakpoint math. */
export function gutter(size: SizeTier, today: number): number {
  return size === "compact" ? Math.min(today, 12) : today;
}

/** `true` when any of `insets` is non-zero — guards an optional padding
 *  block so a desktop/web host (the overwhelming common case, always
 *  zeroes) never adds empty inline-style properties. */
export function hasSafeArea(insets: SafeAreaInsets): boolean {
  return insets.top > 0 || insets.right > 0 || insets.bottom > 0 || insets.left > 0;
}

/** Every safe-area inset as a padding style fragment for a template's
 *  outermost wrapper. All four sides are applied: a host reports a non-zero
 *  inset to say "your box extends under system UI on that edge", and no
 *  template has a fixed header of its own that would already absorb the top
 *  one — `Card`'s and `Working`'s headers are the first thing inside their
 *  border, so dropping `top` would put them under a notch or host overlay.
 *  `undefined` values are omitted from the rendered style entirely rather
 *  than serialized as `0px`, matching how the rest of this codebase spells
 *  "no value" in an inline style object. */
export function safeAreaPadding(insets: SafeAreaInsets): { paddingTop?: number; paddingBottom?: number; paddingLeft?: number; paddingRight?: number } {
  if (!hasSafeArea(insets)) return {};
  return {
    paddingTop: insets.top || undefined,
    paddingBottom: insets.bottom || undefined,
    paddingLeft: insets.left || undefined,
    paddingRight: insets.right || undefined,
  };
}
