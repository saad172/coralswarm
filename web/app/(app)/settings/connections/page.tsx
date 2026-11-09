"use client";
import Link from "next/link";
import { SettingsShell } from "@/components/settings/SettingsShell";
import { SettingsHeading } from "@/components/settings/SettingsHeading";
export default function SettingsAccessPage() {
 return <SettingsShell><SettingsHeading title="Connections">Manage your connected tools and capture sources.</SettingsHeading><p>Find your CoralSwarm MCP address and manage your connections on <Link href="/connectors" className="text-brand-2 underline">Connectors</Link>.</p></SettingsShell>;
}
