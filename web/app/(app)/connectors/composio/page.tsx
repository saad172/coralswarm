"use client";
import { redirect } from "next/navigation";
import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { parseToolkit } from "@/components/connectors/composioModel";
import { ErrorNote, Loading } from "@/components/ui";

/* One static page for every recipe-registered toolkit. Static export cannot
   serve an unenumerated `[toolkit]` segment, so the slug is a query param. */
function ToolkitCapture() {
  const toolkit = parseToolkit(useSearchParams().get("toolkit"));
  if (toolkit) redirect(`/connectors/${toolkit}`);
  return <ErrorNote>Choose a connector from the Connectors page.</ErrorNote>;
}

export default function ComposioCapturePage() {
  return <Suspense fallback={<Loading />}><ToolkitCapture /></Suspense>;
}
