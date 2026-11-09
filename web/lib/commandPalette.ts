/* The hidden Cmd-K title search is mounted once, in AppShell. Anything else
   that wants to open it (the chat find fallback line) dispatches this event
   rather than reaching into the shell's state. CommandPalette listens for it. */
export const OPEN_COMMAND_PALETTE_EVENT = "coralswarm:open-command-palette";

export function openCommandPalette(): void {
  if (typeof window !== "undefined") window.dispatchEvent(new Event(OPEN_COMMAND_PALETTE_EVENT));
}
