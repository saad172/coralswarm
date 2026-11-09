"use client";

import { createContext, useContext, useLayoutEffect } from "react";
import { beginCriticalRead } from "./transport";

/** Declare required deferred work in its already-mounted parent, before
 * starting initialization. The lease survives a zero-network quiet window
 * and is cancelled with its page/account generation. */
export function createPageReadRegistration() {
  const leases = new Map<symbol, () => void>();
  const listeners = new Set<() => void>();
  const notify = () => listeners.forEach((listener) => listener());
  return {
    subscribe: (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
    snapshot: () => leases.size,
    acquire: (_name: string) => {
      const key = Symbol();
      const release = beginCriticalRead();
      leases.set(key, release); notify();
      return () => { if (leases.delete(key)) { release(); notify(); } };
    },
    dispose: () => { leases.forEach((release) => release()); leases.clear(); notify(); },
  };
}
export const PageReadRegistrationContext = createContext<ReturnType<typeof createPageReadRegistration> | null>(null);

export function useCriticalPageReadLease(name: string, waiting: boolean) {
  const registration = useContext(PageReadRegistrationContext);
  useLayoutEffect(() => {
    if (!waiting) return;
    // Standalone hosts still hold transport priority; app routes additionally
    // publish the lease through their boundary for browser observation.
    return registration ? registration.acquire(name) : beginCriticalRead();
  }, [registration, name, waiting]);
}
