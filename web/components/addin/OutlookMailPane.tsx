"use client";

import { useState } from "react";
import { AddinApp } from "./AddinApp";
import dynamic from "next/dynamic";
import { MailContent } from "./MailContent";
import { useOfficeItem } from "./useOfficeItem";
import { createPaneClient } from "@/lib/addin/oauthClient";
import { createOutlookHost } from "@/lib/addin/outlookHost";
import { ssoEmailApi } from "@/lib/addin/emailTransport";
import { apiBase } from "@/lib/apiBase";

import { mark } from "@/lib/addin/perf";

// The Clerk sign-in is only the fallback for SSO off/unavailable: keep it (and Clerk's hooks) out of the
// pane's first-load bundle so the Microsoft SSO path hydrates without it.
const ClerkEmailPane = dynamic(() => import("./ClerkEmailPane").then((m) => m.ClerkEmailPane), { ssr: false });

mark("pane-module");

/** The Outlook message-read task pane (route /outlook/mail). Microsoft SSO first;
 *  the existing Clerk sign-in when SSO is off or unavailable. */
export function OutlookMailPane() {
  const [{ host, client, api }] = useState(() => {
    const outlook = createOutlookHost();
    void outlook.warm(); // office.js, MSAL and the NAA client start now, in parallel, not one after another
    const created = createPaneClient({ host: outlook, apiBase, dialog: { kind: "outlook", office: outlook.office } });
    return { host: outlook, client: created, api: ssoEmailApi(created) };
  });
  const { office, item } = useOfficeItem(host.office);
  const openToday = () => { void host.openLink(`${window.location.origin}/today`); };
  return (
    <AddinApp client={client} host={host} surface="email" label="CoralSwarm email" fallback={<ClerkEmailPane office={office} item={item} openToday={openToday} />}>
      {(context) => (
        <MailContent api={api} item={item} office={office} workspaceName={context.session.workspace.name} toast={context.toast} onUnauthorized={context.reconnectRequired} openToday={openToday} />
      )}
    </AddinApp>
  );
}
