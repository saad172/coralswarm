import { cn } from "./cn";

/* Sparkline — seven day-bars, oldest to newest, scaled to the busiest day.

   Teal throughout: quiet bars use `.spark-bar` (teal mixed 55% into the
   surface), today's bar is full teal, and a zero day is a `line` stub so the
   week still reads as seven slots. `labels` become per-bar tooltips. */
export function Sparkline({
  values,
  labels,
  height = 28,
  className,
}: {
  /** Counts per day, oldest first. Anything but seven is drawn as given. */
  values: number[];
  labels?: string[];
  height?: number;
  className?: string;
}) {
  const max = Math.max(1, ...values);
  return (
    <div
      role="img"
      aria-label={`${values.reduce((n, v) => n + v, 0)} this week`}
      className={cn("flex items-end gap-[3px]", className)}
      style={{ height }}
    >
      {values.map((v, i) => (
        <span
          key={i}
          title={labels?.[i] ? `${labels[i]} · ${v}` : String(v)}
          data-bar=""
          className={cn(
            "min-h-[2px] flex-1 rounded-bar",
            v === 0 ? "bg-line" : i === values.length - 1 ? "bg-hue-teal" : "spark-bar",
          )}
          style={{ height: `${Math.round((v / max) * 100)}%` }}
        />
      ))}
    </div>
  );
}
