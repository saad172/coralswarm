import type { TopicSummary } from "@/lib/connectorsApi";
export function TopicStrip({ topics }: { topics: TopicSummary[] }) {
  return <section aria-label="Topics" className="flex flex-wrap gap-3">{topics.length ? topics.map(t => <span key={t.id} className="rounded-control border border-line px-3 py-2">{t.label} · {t.total} <span className="text-ink-3">(+{t.last_7_days} this week)</span></span>) : <p className="text-ink-3">Topics appear as captures arrive.</p>}</section>;
}
