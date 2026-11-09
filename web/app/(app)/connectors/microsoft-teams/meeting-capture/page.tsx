import { Suspense } from "react";
import { MeetingTranscriptCapture } from "@/components/connectors/MeetingTranscriptCapture";
export default function MeetingCapturePage() {
  return <Suspense fallback={<p>Loading meeting capture…</p>}><MeetingTranscriptCapture /></Suspense>;
}
