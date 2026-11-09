import type { Metadata } from "next";
import { LegalPage, LegalProse, h2, p, ul } from "@/components/LegalPage";

const TITLE = "Terms of Service — CoralSwarm";
const DESCRIPTION = "The terms governing use of CoralSwarm's pre-release knowledge-capture product.";

export const metadata: Metadata = {
  title: TITLE,
  description: DESCRIPTION,
  alternates: { canonical: "/terms" },
  openGraph: { type: "website", title: TITLE, description: DESCRIPTION, url: "/terms" },
  twitter: { card: "summary", title: TITLE, description: DESCRIPTION },
};

const CONTACT = <a href="mailto:person@example.com">person@example.com</a>;

export default function TermsOfService() {
  return (
    <LegalPage eyebrow="legal" title="Terms of Service" updated="October 29, 2026">
      <LegalProse
        blocks={[
          p(
            <>
              These Terms of Service ("Terms") govern your access to and use of CoralSwarm —
              currently offered as a pre-release, waitlist-based product — including our web
              app, the CoralSwarm plugin and MCP integration for Claude Code, our connectors
              to third-party workplace tools, and coralswarm.example (together, the "Service"). By
              joining the waitlist, creating an account, or otherwise using the Service, you agree to
              these Terms.
            </>
          ),

          h2("Who we are"),
          p(
            "CoralSwarm is an early-stage, pre-release product. Features, availability, and any pricing described on this site are subject to change without notice as the product develops."
          ),

          h2("Eligibility"),
          p(
            "You must be at least 18 years old and able to form a binding contract to use the Service. If you're using CoralSwarm on behalf of an organization, you confirm you have authority to bind that organization to these Terms."
          ),

          h2("Accounts"),
          p(
            "You're responsible for maintaining the confidentiality of your account credentials and for all activity that happens under your account. Notify us promptly of any unauthorized use."
          ),

          h2("Acceptable use"),
          p("When using the Service, you agree not to:"),
          ul([
            "Use it for any unlawful purpose or in violation of any applicable law;",
            "Attempt to reverse-engineer, decompile, or gain unauthorized access to the Service or its underlying systems;",
            "Capture, store, or share content through the Service that you don't have the right to capture, store, or share;",
            "Interfere with or disrupt the Service, its infrastructure, or other users' use of it;",
            "Use the Service to violate any third party's privacy or intellectual-property rights.",
          ]),

          h2("Meeting recording and consent"),
          p(
            <>
              CoralSwarm can record and transcribe meetings at your direction. Recording-consent laws
              vary by jurisdiction, and some require the consent of every participant before a
              conversation is recorded. You are solely responsible for complying with all applicable
              laws when you use CoralSwarm to record or transcribe a meeting, including obtaining any
              consent required from participants. CoralSwarm is not responsible for your compliance
              with these laws.
            </>
          ),

          h2("Connectors and third-party services"),
          p(
            <>
              CoralSwarm integrates with third-party services — currently Slack, Microsoft, and
              Google — that you may choose to connect ("Connectors"). Connecting a Connector is your
              decision and requires authorization through that provider's own consent flow (or, for
              Slack, your workspace admin installing the CoralSwarm app). Your use of any connected
              third-party service remains subject to that provider's own terms and privacy policy. We
              are not responsible for the availability, content, or practices of third-party services.
            </>
          ),

          h2("Your content"),
          p(
            <>
              You retain ownership of the content you capture or connect through the Service ("Your
              Content"). You grant CoralSwarm a limited license to access, process, store, and
              transmit Your Content solely to provide and improve the Service to you — including
              sending relevant content to our AI-processing subprocessor, as described in our{" "}
              <a href="/privacy">Privacy Policy</a>. You represent that you have the necessary rights
              to Your Content and that providing it to CoralSwarm doesn't violate any law or third
              party's rights.
            </>
          ),

          h2("Our intellectual property"),
          p(
            "The Service — including its software, design, and branding — is owned by CoralSwarm and protected by intellectual-property laws. These Terms don't grant you any rights to CoralSwarm's trademarks, logos, or brand assets beyond what's necessary to use the Service."
          ),

          h2("Fees and pricing"),
          p(
            "CoralSwarm is currently offered on a pre-release, waitlist basis. Any pricing shown on this site is indicative and subject to change before general availability. We'll communicate pricing and billing terms clearly before charging you for the Service."
          ),

          h2("Disclaimer of warranties"),
          p(
            'THE SERVICE IS PROVIDED "AS IS" AND "AS AVAILABLE," WITHOUT WARRANTIES OF ANY KIND, WHETHER EXPRESS, IMPLIED, OR STATUTORY, INCLUDING WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. AS A PRE-RELEASE PRODUCT, THE SERVICE MAY CONTAIN BUGS, EXPERIENCE DOWNTIME, OR CHANGE SUBSTANTIALLY WITHOUT NOTICE.'
          ),

          h2("Limitation of liability"),
          p(
            "TO THE MAXIMUM EXTENT PERMITTED BY LAW, CORALSWARM WILL NOT BE LIABLE FOR ANY INDIRECT, INCIDENTAL, SPECIAL, CONSEQUENTIAL, OR PUNITIVE DAMAGES, OR ANY LOSS OF DATA, REVENUE, OR PROFITS, ARISING FROM YOUR USE OF THE SERVICE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGES. OUR TOTAL LIABILITY FOR ANY CLAIM ARISING OUT OF THESE TERMS OR THE SERVICE WILL NOT EXCEED THE AMOUNT YOU PAID US, IF ANY, IN THE 12 MONTHS BEFORE THE CLAIM AROSE."
          ),

          h2("Termination"),
          p(
            "You may stop using the Service at any time. We may suspend or terminate your access if you violate these Terms, or, during this pre-release period, at our discretion with reasonable notice where practical."
          ),

          h2("Governing law"),
          p(
            "These Terms are governed by the laws of the State of Texas, without regard to conflict-of-law principles. Any dispute arising from these Terms or the Service will be resolved in the state or federal courts located in Texas, and you consent to their jurisdiction."
          ),

          h2("Changes to these terms"),
          p(
            'We may update these Terms as the Service evolves. We\'ll update the "Last updated" date above, and if a change is material we\'ll make reasonable efforts to notify you — for example, by email.'
          ),

          h2("Contact us"),
          p(<>Questions about these Terms? Email {CONTACT}.</>),
        ]}
      />
    </LegalPage>
  );
}
