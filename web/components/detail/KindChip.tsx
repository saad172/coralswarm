import type { ReactNode } from "react";
import { Chip, hueVar, cn, type Hue } from "@/components/ui";
import { kindForeground } from "@/components/topic/topicMeta";

/* Hue-tinted kind label for a detail hero — "coral · slack", "coding
   session", "meeting transcript". Reuses TopicHeader/topicMeta's contrast fix
   (`kindForeground`): coral and amber read poorly at their raw hue value, so
   those two borrow their darker `-2`/`warn` companions instead. */
export function KindChip({
  hue,
  children,
  className,
}: {
  hue: Hue;
  children: ReactNode;
  className?: string;
}) {
  return (
    <Chip
      className={cn("uppercase", className)}
      style={{ color: kindForeground(hue), background: `color-mix(in srgb, ${hueVar(hue)} 16%, transparent)` }}
    >
      {children}
    </Chip>
  );
}
