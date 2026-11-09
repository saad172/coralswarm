import { Mono } from "@/components/ui";

/* Where each kind of content is processed. This is the disclosure that used to
   sat on the Answers page next to the old "Local only" choice. That choice is
   gone (everything is processed in the cloud) and so is that page, so this lives on Privacy. Plain words only: workspace, topic/item, source. */
export const CONTENT_PROCESSING_CARDS = [
  {
    title: "Indexing & extraction",
    state: "cloud",
    tag: "always",
    detail:
      "Your saved items are embedded and their topics extracted by cloud AI providers, under zero-data-retention terms. None of them may train on your data.",
  },
  {
    title: "Answer synthesis",
    state: "cloud",
    tag: undefined,
    detail:
      "Your question, matched excerpts, prior conversation, and relevant context are sent to the cloud model. No training, zero data retention.",
  },
] as const;

export function ContentProcessingDisclosure() {
  return (
    <section className="mt-[26px]" aria-labelledby="content-processing-heading">
      <h2 id="content-processing-heading" className="mb-[9px]">
        <Mono className="tracking-[0.13em]">where your content is processed</Mono>
      </h2>
      <div className="grid grid-cols-1 gap-[10px] sm:grid-cols-2">
        {CONTENT_PROCESSING_CARDS.map((card) => {
          const color = "var(--warn)";
          return (
            <div
              key={card.title}
              className="rounded-card border px-[14px] py-[13px]"
              style={{
                borderColor: `color-mix(in srgb, ${color} 40%, transparent)`,
                background: `color-mix(in srgb, ${color} 5%, var(--bg))`,
              }}
            >
              <div className="mb-[7px] flex items-center gap-[7px]">
                <span className="h-[7px] w-[7px] flex-none rounded-pill" style={{ background: color }} />
                <span className="font-mono text-[9px] uppercase tracking-[0.08em]" style={{ color }}>
                  {card.state}
                </span>
                {card.tag && <Mono className="ml-auto text-ink-4">{card.tag}</Mono>}
              </div>
              <div className="mb-[3px] text-[13.5px] font-medium text-ink">{card.title}</div>
              <div className="text-[12.5px] leading-[1.45] text-ink-3">{card.detail}</div>
            </div>
          );
        })}
      </div>
    </section>
  );
}
