import type { ReactNode } from "react";

/* The 25px serif section heading + lead paragraph every settings/org content
   column opens with ("What CoralSwarm may watch", "Who may do what", ...).
   Distinct from the kit's `PageTitle` (27px, the App Router page-level role) —
   this is the smaller in-column heading the redesign uses one level down, so
   it's its own component rather than an override of PageTitle's size. */
export function SettingsHeading({
  title,
  italicWord,
  children,
}: {
  title: string;
  italicWord?: string;
  children?: ReactNode;
}) {
  const i = italicWord ? title.indexOf(italicWord) : -1;
  return (
    <div className="mb-[14px]">
      <h1 className="mb-[4px] font-display text-[25px] font-normal leading-[1.25] tracking-[-0.01em] text-ink">
        {i === -1 ? (
          title
        ) : (
          <>
            {title.slice(0, i)}
            <em className="italic text-brand-2">{italicWord}</em>
            {title.slice(i + italicWord!.length)}
          </>
        )}
      </h1>
      {children && <p className="max-w-[74ch] text-[13.5px] leading-[1.6] text-ink-3">{children}</p>}
    </div>
  );
}
