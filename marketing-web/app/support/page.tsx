import type { Metadata } from "next";
import Link from "next/link";
import { LegalPage, LegalProse, h2, p, ul } from "@/components/LegalPage";

const TITLE = "Support — CoralSwarm";
const DESCRIPTION = "Get help with your CoralSwarm account, connections, permissions, and captured content.";

export const metadata: Metadata = {
  title: TITLE,
  description: DESCRIPTION,
  alternates: { canonical: "/support" },
  openGraph: { type: "website", title: TITLE, description: DESCRIPTION, url: "/support" },
  twitter: { card: "summary", title: TITLE, description: DESCRIPTION },
};

export default function SupportPage() {
  return (
    <LegalPage eyebrow="help" title="CoralSwarm Support" updated="October 29, 2026">
      <LegalProse
        blocks={[
          h2("Get help"),
          p(
            <>
              For account, installation, connection, or privacy questions, email{" "}
              <a href="mailto:person@example.com">person@example.com</a>.
              Include the platform you are using, the steps you tried, and the error message.
            </>
          ),
          p(
            "Do not send passwords, access tokens, or confidential workplace content. " +
            "Remove private information from screenshots before sharing them."
          ),
          h2("Set up a connection"),
          ul([
            <>Sign in to <a href="https://app.coralswarm.example">CoralSwarm</a> with the account you intend to use.</>,
            <>Open <a href="https://app.coralswarm.example/connectors">Connectors</a>, choose the connection, and follow its sign-in instructions. Review the account and permissions shown before approving access.</>,
            "For a workplace account, your administrator may need to approve installation or requested permissions. Contact them if your organization blocks the connection.",
            "For a CoralSwarm add-in or AI assistant connection, follow the setup shown by that host. If an installation option is unavailable, contact us for help with access.",
          ]),
          h2("Troubleshoot sign-in and missing content"),
          ul([
            "Check that CoralSwarm and the connected platform are signed in to the intended accounts. If you use multiple oceans, check which ocean you have selected.",
            "In the web app, connector sign-in continues in the same browser tab. If sign-in does not start, retry the connection and contact us if it still fails.",
            "If permission was denied or has expired, review the connection in CoralSwarm and reconnect when appropriate. Your administrator may need to approve it again.",
            "If expected content is missing, check the connection status and your access to the original source. Tell us which source and error are involved without sending its private content.",
          ]),
          h2("Disconnect and manage access"),
          p(
            <>
              To disconnect a workplace connector, open{" "}
              <a href="https://app.coralswarm.example/connectors">Connectors</a> in CoralSwarm
              and use its Disconnect control, or open Manage where offered to manage individual installations.
              Organization connections may require an administrator.
              You can also review CoralSwarm's access in the connected platform's account or app settings.
            </>
          ),
          p(
            "For an AI assistant connection, use the host's app or connector settings to remove it. " +
            "Disconnecting a connection does not by itself delete content already saved in CoralSwarm. " +
            "Contact us for help with deleting stored content or your account."
          ),
          h2("Privacy and terms"),
          p(
            <>
              Read our <Link href="/privacy">Privacy Policy</Link> for information about data and
              access, and our <Link href="/terms">Terms of Service</Link> for service terms.
            </>
          ),
        ]}
      />
    </LegalPage>
  );
}
