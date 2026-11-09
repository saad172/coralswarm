import { cn } from "./cn";

export type StepState = "done" | "current" | "upcoming";

/** The numbered step-progress rail — onboarding's four or five steps, the
 *  connect wizard's header. A step is "done" (checkmark, ok green) once its
 *  index is behind `currentIndex`, "current" (ink, bold) at it, "upcoming"
 *  (outline, muted) ahead of it.
 *
 *  `compact` labels only the current step, for a narrow header where the full
 *  row of names won't fit. The other steps keep their numbered dot, and keep
 *  their name for assistive tech. */
export function StepRail({
  steps,
  currentIndex,
  compact = false,
  className,
}: {
  steps: string[];
  currentIndex: number;
  compact?: boolean;
  className?: string;
}) {
  return (
    <div className={cn("flex items-center gap-[8px]", className)}>
      {steps.map((label, i) => {
        const state: StepState = i < currentIndex ? "done" : i === currentIndex ? "current" : "upcoming";
        return (
          <div key={label} className="flex items-center gap-[8px]">
            {i > 0 && <span className="h-px w-[18px] flex-none bg-line" />}
            <span className="inline-flex items-center gap-[6px]" aria-current={state === "current" ? "step" : undefined}>
              <span
                className={cn(
                  "grid h-[19px] w-[19px] flex-none place-items-center rounded-pill font-mono text-[9.5px] font-medium",
                  state === "done" && "bg-ok text-white",
                  state === "current" && "bg-ink text-white",
                  state === "upcoming" && "border border-line bg-bg text-ink-4",
                )}
              >
                {state === "done" ? "✓" : i + 1}
              </span>
              <span
                className={cn(
                  "whitespace-nowrap text-[12px]",
                  state === "current" ? "font-medium text-ink" : "font-normal text-ink-4",
                  compact && state !== "current" && "sr-only",
                )}
              >
                {label}
              </span>
            </span>
          </div>
        );
      })}
    </div>
  );
}
