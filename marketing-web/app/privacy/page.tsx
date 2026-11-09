import type { Metadata } from "next";
import { LegalPage, LegalProse, h2, h3, p, ul } from "@/components/LegalPage";

const TITLE = "Privacy Policy — CoralSwarm";
const DESCRIPTION =
  "How CoralSwarm collects, uses, and protects data across meeting capture, coding-session capture, connectors (Slack, Microsoft, Google), and AI processing.";

export const metadata: Metadata = {
  title: TITLE,
  description: DESCRIPTION,
  alternates: { canonical: "/privacy" },
  openGraph: { type: "website", title: TITLE, description: DESCRIPTION, url: "/privacy" },
  twitter: { card: "summary", title: TITLE, description: DESCRIPTION },
};

const CONTACT = <a href="mailto:person@example.com">person@example.com</a>;
const OPENROUTER_PROVIDERS = (
  <a href="https://openrouter.ai/providers" target="_blank" rel="noopener noreferrer">
    openrouter.ai/providers
  </a>
);

export default function PrivacyPolicy() {
  return (
    <LegalPage eyebrow="legal" title="Privacy Policy" updated="October 29, 2026">
      <LegalProse
        blocks={[
          p(
            <>
              This Privacy Policy explains how CoralSwarm ("CoralSwarm," "we," "us") collects, uses,
              and shares information when you use the CoralSwarm service — our web app, the CoralSwarm
              plugin and MCP integration for Claude Code, our connectors to third-party workplace
              tools, and this website, coralswarm.example (together, the "Service"). It applies
              whether you're a waitlisted visitor, a signed-up user, or a member of an organization
              using CoralSwarm.
            </>
          ),
          p(
            <>
              CoralSwarm is currently a pre-release product. This policy describes how the product
              behaves today; as features change we'll update it and the "Last updated" date above.
            </>
          ),

          h2("Information we collect"),

          h3("Account and waitlist information"),
          p(
            <>
              When you join the waitlist or contact us through this site, we collect the name and
              work email you provide, any free-text message you send us, your approximate location
              (a country code derived from your IP address at the time of submission — we do not
              store the raw IP address), and first-touch attribution data your browser captures when
              you land on the site (the referring page and any UTM campaign parameters).
            </>
          ),
          p(
            <>
              If you create a CoralSwarm account, our authentication provider, Clerk, manages sign-in
              and we receive the name and email address associated with your account.
            </>
          ),

          h3("Content you capture"),
          p(
            <>
              CoralSwarm's purpose is turning your team's day-to-day work into searchable, answerable
              knowledge. Depending on which features you use, this includes:
            </>
          ),
          ul([
            <>
              <strong>Meeting transcripts</strong> — captured through the meeting connectors
              described in "Connectors" below. Meeting capture is always user-initiated; CoralSwarm
              does not silently record meetings.
            </>,
            <>
              <strong>Coding-session summaries</strong> — captured by the CoralSwarm plugin and MCP
              integration for Claude Code, generated from your coding sessions in that tool.
            </>,
            <>
              <strong>Content from workplace tools you connect</strong> — see "Connectors" below.
            </>,
          ]),
          p(
            <>
              We refer to all of the above as "Captured Content." Captured Content is processed to
              extract discrete facts, decisions, and references ("atoms"), grouped into topics, and
              made searchable and answerable within your account or organization.
            </>
          ),

          h3("Connectors (third-party integrations)"),
          p(
            <>
              Connectors only ingest content for an account or workspace that has explicitly
              authorized them through the provider's own OAuth consent screen. Today's connectors,
              and exactly what each one accesses:
            </>
          ),
          ul([
            <>
              <strong>Slack</strong> — once a workspace installs the CoralSwarm Slack app, we hold a
              workspace-level bot token (not a token per individual member) and can read messages and
              huddle transcripts in the channels CoralSwarm is invited to.
            </>,
            <>
              <strong>Microsoft</strong> — with your delegated authorization, we access Outlook mail
              and calendar, and Teams channels, chats, and meetings you or a Teams admin
              authorize, using Microsoft Graph scopes limited to <code>Mail.Read</code>,{" "}
              <code>Calendars.Read</code>, <code>offline_access</code>, <code>openid</code>,{" "}
              <code>profile</code>, <code>User.Read</code>, <code>Team.ReadBasic.All</code>,{" "}
              <code>Channel.ReadBasic.All</code>, <code>ChannelMessage.Read.All</code>,{" "}
              <code>Chat.Read</code>, <code>OnlineMeetings.Read</code>, and{" "}
              <code>OnlineMeetingTranscript.Read.All</code>. Separately, when a Teams admin
              installs the CoralSwarm Teams app to a specific channel, chat, or meeting
              (resource-specific consent — nothing is ingested until they do), that app holds
              its own scoped permissions for that channel, chat, or meeting only:{" "}
              <code>ChannelMessage.Read.Group</code>, <code>ChatMessage.Read.Chat</code>,{" "}
              <code>TeamMember.Read.Group</code>, <code>ChannelSettings.Read.Group</code>,{" "}
              <code>OnlineMeetingTranscript.Read.Chat</code>, and{" "}
              <code>OnlineMeeting.ReadBasic.Chat</code>.
            </>,
            <>
              <strong>Google</strong> — the permissions depend on the Google features you connect.
              The backend connectors request read-only access to Calendar events ({" "}
              <code>calendar.readonly</code>), Gmail messages and threads ({" "}
              <code>gmail.readonly</code>), or Meet conference records and transcript entries ({" "}
              <code>meetings.space.readonly</code>). The Chat connector requests{" "}
              <code>chat.spaces.readonly</code>, <code>chat.messages.readonly</code>,{" "}
              <code>chat.memberships.readonly</code>, and <code>directory.readonly</code> to read
              conversations, membership, and directory information for identity matching. Google
              identity scopes identify the account that authorizes a connection. Content saved
              through these connectors becomes Captured Content and is processed as described
              in this policy.
            </>,
            <>
              <strong>Google Workspace add-ons</strong> — the Gmail and Calendar add-on separately
              requests <code>gmail.addons.current.message.metadata</code> to display context from
              the email you open, <code>calendar.addons.current.event.read</code> to identify the
              current event's Meet meeting, and <code>calendar.events.readonly</code> to display
              the saved event's title. It also requests <code>gmail.addons.execute</code>,{" "}
              <code>calendar.addons.execute</code>, and <code>script.external_request</code> to run
              its cards and communicate with CoralSwarm and our authentication provider. The
              add-on signs you into CoralSwarm and your selected workspace through Clerk, with
              sign-in and refresh credentials stored in per-user Apps Script User Properties.
              The Meet side panel uses a separate Clerk sign-in and holds credentials in memory
              while open. Installing an add-on does not authorize the backend to read your
              mailbox or Meet transcripts: those features need the separate Google connection.
              Saving an email requires your save action or an enabled capture setting; Meet
              transcript capture requires approval for that meeting in Calendar or Meet and an
              available transcript.
            </>,
          ]),
          p(
            <>
              You can disconnect any connector at any time from within the product. Disconnecting
              revokes CoralSwarm's access token with that provider and stops further ingestion; it
              does not automatically delete content already captured into your account — email us
              at {CONTACT} to request that.
            </>
          ),

          h3("Technical and log data"),
          p(
            <>
              OAuth access and refresh tokens for connectors are encrypted at rest. We also collect
              ordinary operational data needed to run and secure the Service: request logs, error and
              crash reports, and device or browser metadata.
            </>
          ),

          h3("Product telemetry and analytics"),
          p(
            <>
              This website logs a small, first-party, cookieless pageview event — page, referrer,
              any UTM campaign parameters, whether it looks like your first visit, and a coarse
              country derived from the request — to help us see which channel drove a visit. No
              cross-site tracking cookies.
            </>
          ),
          p(
            <>
              Our applications and services also collect usage telemetry, on our own infrastructure:
              which features you use (request route and method), event counts, response status and
              outcome, timings (including queue time), and error indicators. Telemetry is
              metadata only — it never includes meeting audio, transcripts, meeting titles, captured
              content, file names, or the text of the questions you ask.
            </>
          ),
          p(
            <>
              We use this data to understand which features provide value and to improve the
              product.
            </>
          ),

          h2("How we use information"),
          ul([
            "To operate the Service — capturing, transcribing, extracting, indexing, and answering questions over your Captured Content.",
            "To respond to your inquiries and manage the waitlist, including sending invite and confirmation emails.",
            "To improve, debug, and secure the Service.",
            "To comply with legal obligations.",
          ]),

          h2("AI processing of your content"),
          p(
            <>
              To turn Captured Content into extracted facts, topics, and cited answers, CoralSwarm
              sends relevant portions of your content to OpenRouter, Inc., an AI model routing
              service, which forwards it to an inference provider for processing. This happens as
              part of delivering the Service to you.
            </>
          ),
          p(
            <>
              We do not select that provider by name. On every request we require OpenRouter to route
              only to endpoints it designates as zero-data-retention and as not retaining or training
              on request data, and we have disabled training-eligible routing at the account level. A
              request that cannot be served under those constraints fails rather than falling back to
              a provider that does not meet them. Which provider serves a given request therefore
              varies, and the set changes as providers change what they offer. OpenRouter's current
              providers and their data policies are listed at {OPENROUTER_PROVIDERS}.
            </>
          ),
          p(
            <>
              For this automatic processing, we do not send your Captured Content to any AI or model
              provider outside that arrangement. (If you connect your own AI assistant through MCP,
              see "AI assistant access (MCP)" below — that is content you request, sent to a client
              and provider of your choosing.)
            </>
          ),

          h2("AI assistant access (MCP)"),
          p(
            <>
              CoralSwarm runs an MCP (Model Context Protocol) server that lets you connect your own AI
              assistants (for example, Claude Code, or another MCP-compatible client) to query the
              knowledge you've captured. Access is authenticated through your CoralSwarm account via
              Clerk OAuth and scoped to what you've authorized: an assistant you connect can read
              knowledge already in your account or organization, and nothing belonging to other users
              or organizations. When you ask that assistant a question, CoralSwarm sends the specific
              searchable content, transcripts, and cited answers it requests to that client and,
              where applicable, its underlying model provider, under that provider's own terms and
              privacy policy — not CoralSwarm's.
            </>
          ),

          h2("How we share information"),
          p("We do not sell your information. We share it only with the service providers (\"subprocessors\") that host and operate CoralSwarm, and only as needed to run the Service:"),
          ul([
            <>
              <strong>Fly.io</strong> — application hosting for the CoralSwarm backend, and
              our control-plane database (accounts, organization membership and routing, and
              content for personal, non-organization accounts).
            </>,
            <>
              <strong>Neon</strong> — for organization customers, a dedicated database per
              organization holding that organization&rsquo;s Captured Content.
            </>,
            <>
              <strong>Cloudflare</strong> — website hosting, first-party pageview logging
              (described above), object storage for Captured Content (files, recordings), the
              waitlist database, and outbound email delivery.
            </>,
            <>
              <strong>OpenRouter, Inc.</strong> — AI model routing, together with the inference
              providers it routes to on our behalf under the zero-data-retention and no-training
              constraints described above. That set is not fixed; OpenRouter's current providers are
              listed at {OPENROUTER_PROVIDERS}.
            </>,
            <>
              <strong>Clerk</strong> — account authentication.
            </>,
          ]),
          p(
            "We may also disclose information if required by law, or to protect the rights, property, or safety of CoralSwarm, our users, or others."
          ),

          h2("Data retention"),
          p(
            <>
              We retain waitlist and account information for as long as your waitlist entry or
              account exists. Unsubscribing via the link included in any waitlist email removes you
              from future emails. You can request deletion of your waitlist entry, account, or
              Captured Content at any time — see "Contact us" below.
            </>
          ),

          h2("Your choices and rights"),
          ul([
            "Unsubscribe from waitlist emails at any time via the link in the email.",
            "Disconnect any connector at any time from within the product.",
            "Request a copy or deletion of your data by emailing us.",
          ]),
          p(
            <>
              CoralSwarm doesn't yet have a self-serve data export or deletion tool — requests are
              handled manually by emailing {CONTACT}.
            </>
          ),

          h2("Meeting recording and consent"),
          p(
            <>
              CoralSwarm's meeting capture is initiated by you or someone on your team — it is never
              automatic. Many jurisdictions have laws requiring the consent of some or all
              participants before a conversation is recorded. You are solely responsible for knowing
              and complying with the recording-consent laws that apply to your meetings, including
              obtaining any required consent from participants, before using CoralSwarm to record or
              transcribe a conversation.
            </>
          ),

          h2("Security"),
          p(
            <>
              Connector OAuth tokens are encrypted at rest, and data is transmitted over encrypted
              connections (TLS). We restrict internal access to Captured Content to what's needed to
              operate the Service. No method of storage or transmission is perfectly secure, and we
              can't guarantee absolute security.
            </>
          ),

          h2("Children's privacy"),
          p(
            "CoralSwarm is not directed to, and we do not knowingly collect information from, anyone under 18. If you believe a minor has provided us information, contact us and we'll delete it."
          ),

          h2("International users"),
          p(
            <>
              CoralSwarm is operated from the United States. The subprocessors listed above may
              process information in the United States and other countries where they operate. By
              using the Service, you consent to this transfer and processing.
            </>
          ),

          h2("Changes to this policy"),
          p(
            <>
              We may update this policy as CoralSwarm's product evolves. We'll update the "Last
              updated" date above, and if a change is material we'll make reasonable efforts to
              notify you — for example, by email.
            </>
          ),

          h2("Contact us"),
          p(<>Questions about this policy or your data? Email {CONTACT}.</>),
        ]}
      />
    </LegalPage>
  );
}
