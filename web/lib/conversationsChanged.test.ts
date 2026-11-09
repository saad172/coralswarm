import { describe, expect, it, vi } from "vitest";
import { emitConversationsChanged, onConversationsChanged } from "./conversationsChanged";

describe("conversationsChanged", () => {
  it("notifies every subscriber, with the deleted id", () => {
    const a = vi.fn();
    const b = vi.fn();
    const offA = onConversationsChanged(a);
    const offB = onConversationsChanged(b);

    emitConversationsChanged("conv-1");

    expect(a).toHaveBeenCalledWith("conv-1");
    expect(b).toHaveBeenCalledWith("conv-1");
    offA();
    offB();
  });

  it("passes undefined when nothing was deleted", () => {
    const fn = vi.fn();
    const off = onConversationsChanged(fn);
    emitConversationsChanged();
    expect(fn).toHaveBeenCalledWith(undefined);
    off();
  });

  it("stops notifying after unsubscribe", () => {
    const fn = vi.fn();
    onConversationsChanged(fn)();
    emitConversationsChanged("conv-1");
    expect(fn).not.toHaveBeenCalled();
  });

  it("survives a listener that unsubscribes while being notified", () => {
    // The Chats screen unsubscribes on unmount, and one listener's reaction
    // (abandoning the open thread) can unmount another — so the emit must not
    // skip whoever came after it in the set.
    const later = vi.fn();
    const off = onConversationsChanged(() => off());
    const offLater = onConversationsChanged(later);

    expect(() => emitConversationsChanged("conv-1")).not.toThrow();
    expect(later).toHaveBeenCalledWith("conv-1");
    offLater();
  });
});
