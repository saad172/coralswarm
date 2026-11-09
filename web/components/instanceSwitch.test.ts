import { describe, expect, it, vi } from "vitest";
import type { Instance } from "./useInstanceSwitcher";
import { activeOrganizationFor, performInstanceSwitch } from "./instanceSwitch";

/* The switch's ORDER is the subject here, not the fact that both steps ran.
 *
 * `void setActive(...); reload();` calls both — it just reloads before Clerk
 * has persisted the new active organization, so the tab comes back on the
 * previous tenant. Any test whose strongest assertion is
 * `expect(reload).toHaveBeenCalled()` is GREEN on that broken version and is
 * therefore worthless. Every test below is written so it goes red on it:
 * `setActive` is held open on a deferred promise and the log records when it
 * SETTLES, not just when it was called. */

const personal: Instance = { kind: "personal", id: "personal", name: "Personal" };
const org: Instance = { kind: "org", id: "org_2abc", name: "Acme", slug: "acme" };

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  // An unobserved rejection between construction and the `await` would warn;
  // the tests always attach a handler via performInstanceSwitch.
  return { promise, resolve, reject };
}

/** A `setActive` that never settles on its own, plus a log both dependencies
 *  write to, so the assertions can see the reload land relative to the
 *  SETTLEMENT of setActive rather than merely after its invocation. */
function gatedDeps() {
  const gate = deferred<void>();
  const log: string[] = [];
  const calls: Array<{ organization: string | null }> = [];
  const deps = {
    setActive: async (params: { organization: string | null }) => {
      calls.push(params);
      log.push("setActive:called");
      await gate.promise;
      log.push("setActive:settled");
    },
    reload: () => {
      log.push("reload");
    },
  };
  return { gate, log, calls, deps };
}

/** Drain microtasks AND one macrotask turn, so "hasn't reloaded yet" cannot
 *  pass merely because the reload was one `.then()` away. */
async function flush() {
  await new Promise((resolve) => setTimeout(resolve, 0));
  await new Promise((resolve) => setTimeout(resolve, 0));
}

describe("performInstanceSwitch ordering", () => {
  it("does not reload until setActive has settled", async () => {
    const { gate, log, deps } = gatedDeps();

    const pending = performInstanceSwitch(org, deps);
    await flush();

    // THE assertion. Under `void setActive(...); reload();` the log already
    // reads ["setActive:called", "reload"] here.
    expect(log).toEqual(["setActive:called"]);

    gate.resolve();
    await pending;

    expect(log).toEqual(["setActive:called", "setActive:settled", "reload"]);
  });

  it("reloads exactly once", async () => {
    const { gate, deps } = gatedDeps();
    const reload = vi.fn(deps.reload);

    const pending = performInstanceSwitch(org, { ...deps, reload });
    gate.resolve();
    await pending;

    expect(reload).toHaveBeenCalledTimes(1);
  });

  it("resolves only after the reload has been requested", async () => {
    const { gate, log, deps } = gatedDeps();

    const pending = performInstanceSwitch(personal, deps);
    gate.resolve();
    await pending;

    // The promise the hook hands its caller is not done before the reload is
    // asked for, so `void switchTo(...)` at the call site cannot be observed
    // as "finished" while the old tenant's state is still on screen.
    expect(log.at(-1)).toBe("reload");
  });
});

describe("performInstanceSwitch target mapping", () => {
  it("hands Clerk a null organization for the personal instance", async () => {
    const { gate, calls, deps } = gatedDeps();
    const pending = performInstanceSwitch(personal, deps);
    gate.resolve();
    await pending;

    // `null` is how Clerk spells "no org". Sending the literal id "personal"
    // would be a request to activate an organization that does not exist.
    expect(calls).toEqual([{ organization: null }]);
  });

  it("hands Clerk the organization id for an org instance", async () => {
    const { gate, calls, deps } = gatedDeps();
    const pending = performInstanceSwitch(org, deps);
    gate.resolve();
    await pending;

    expect(calls).toEqual([{ organization: "org_2abc" }]);
  });

  it("maps targets to Clerk's active-organization value", () => {
    expect(activeOrganizationFor(personal)).toBeNull();
    expect(activeOrganizationFor(org)).toBe("org_2abc");
  });
});

describe("performInstanceSwitch failure", () => {
  it("does NOT reload when setActive rejects", async () => {
    const { gate, log, deps } = gatedDeps();

    const pending = performInstanceSwitch(org, deps);
    gate.reject(new Error("clerk said no"));

    await expect(pending).rejects.toThrow("clerk said no");

    // A failed switch left the session on the tenant it was already on.
    // Reloading would spend the user's page state to show them the same
    // tenant back — and would be indistinguishable, on screen, from a switch
    // that succeeded and landed somewhere else.
    expect(log).toEqual(["setActive:called"]);
  });

  it("propagates the rejection instead of swallowing it", async () => {
    const boom = new Error("network");
    const reload = vi.fn();
    const deps = {
      setActive: () => Promise.reject(boom),
      reload,
    };

    await expect(performInstanceSwitch(org, deps)).rejects.toBe(boom);
    expect(reload).not.toHaveBeenCalled();
  });
});
