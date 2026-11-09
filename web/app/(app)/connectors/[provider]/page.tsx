import { notFound } from "next/navigation";
import { ConnectorDetail } from "@/components/connectors/ConnectorDetail";
export default async function Page({ params }: { params: Promise<{ provider: string }> }) {
 const { provider } = await params;
 if (!["slack","google","microsoft","github","jira","linear","confluence","hubspot","zendesk","dart"].includes(provider)) notFound();
 return <ConnectorDetail provider={provider} />;
}
