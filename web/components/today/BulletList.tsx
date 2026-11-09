import { parseBulletLines } from "@/lib/journal";

/* The hour + day tiers' bounded bullet-list rendering (ticket 15's prompt
   contract change, ticket 17's Answer). `text` is LLM prose compiled over
   untrusted captured content — split on newlines into list items and never
   interpreted as markdown/HTML. `parseBulletLine`'s "thread label — detail"
   split is plain string slicing on a fixed delimiter, not markup parsing:
   both halves still render as ordinary React text children. `compact` is the
   week view's day-gist row styling (smaller, muted, tighter). */

export function BulletList({ text, compact = false, className }: { text: string; compact?: boolean; className?: string }) {
  const lines = parseBulletLines(text);
  if (lines.length === 0) return null;
  return (
    <ul className={className ?? "flex flex-col gap-[4px]"}>
      {lines.map((line, i) => (
        <li
          key={i}
          className={
            compact
              ? "relative pl-[13px] text-[12.5px] leading-[1.5] text-ink-2 before:absolute before:left-0 before:top-[7px] before:h-[4px] before:w-[4px] before:rounded-pill before:bg-line-2 before:content-['']"
              : "relative pl-[15px] text-[13.5px] leading-[1.6] text-ink before:absolute before:left-[1px] before:top-[9px] before:h-[5px] before:w-[5px] before:rounded-pill before:bg-brand before:content-['']"
          }
        >
          {line.label ? (
            <>
              <span className="font-medium text-ink">{line.label}</span>
              {" — "}
              {line.detail}
            </>
          ) : (
            line.detail
          )}
        </li>
      ))}
    </ul>
  );
}
