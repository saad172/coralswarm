"use client";

/* Final state: the purge has run (401 `account_deleted`) or is running. A
   plain static page (no auth boot, outside the `(app)` group) so it renders
   for a signed-out user and never bounces to /login. */
export default function AccountDeletedPage() {
  return (
    <main className="mx-auto flex min-h-screen max-w-[460px] flex-col justify-center px-6 text-center">
      <h1 className="text-[22px] font-semibold tracking-[-0.01em] text-ink">Your account is being deleted</h1>
      <p className="mt-3 text-[14px] leading-[1.6] text-ink-3">
        We are removing your workspaces, items, connections, credentials and keys. This can&apos;t be undone and
        nothing more is needed from you.
      </p>
    </main>
  );
}
