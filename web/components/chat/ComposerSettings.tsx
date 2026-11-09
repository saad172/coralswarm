"use client";

import { useRef, useState } from "react";
import { Popover, Seg, Eyebrow, LevelDial } from "@/components/ui";
import { IconSettings } from "@/components/icons";
import { DEFAULT_MODEL, DEFAULT_EFFORT, MODEL_TIERS, MODELS, EFFORT_LEVELS, effortAppliesToModel, type ModelTier, type EffortLevel, type AnswerStyle } from "@/lib/askScope";

export function ComposerSettings({ model, setModel, effort, setEffort, style, setStyle, scoped = false, disabled = false }: {
  model: ModelTier; setModel: (value: ModelTier) => void;
  effort: EffortLevel; setEffort: (value: EffortLevel) => void;
  style: AnswerStyle; setStyle: (value: AnswerStyle) => void;
  scoped?: boolean; disabled?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const showModel = !scoped || style !== "sources-only";
  const showEffort = !scoped && effortAppliesToModel(model);
  const summary = [
    showModel && model !== DEFAULT_MODEL ? MODELS[model].label.toLowerCase() : "",
    showEffort && effort !== DEFAULT_EFFORT ? effort : "",
    style !== "detailed" ? style.replace("-", " ") : "",
  ].filter(Boolean).join(" · ");
  return <div className="relative flex min-w-0 items-center gap-2">
    <button ref={triggerRef} type="button" aria-label="Answer settings" aria-expanded={open && !disabled}
      disabled={disabled} onClick={() => setOpen(!open)}
      className="grid h-8 w-8 place-items-center rounded-control text-ink-2 hover:bg-bg-hover disabled:opacity-40">
      <IconSettings width={15} height={15} />
    </button>
    <span className="truncate font-mono text-[10px] text-ink-3">{summary}</span>
    <Popover open={open && !disabled} onClose={() => { setOpen(false); triggerRef.current?.focus(); }} side="top" triggerRef={triggerRef} className="max-w-[calc(100vw-48px)] animate-[cs-pop-in_var(--dur-pop)_var(--ease-out)]">
      <div className="space-y-3 p-3" role="group" aria-label="Answer settings" onKeyDown={event => {
        if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
        const target = event.target as HTMLElement;
        if (target.getAttribute("role") !== "tab") return;
        const tabs = Array.from(target.parentElement?.querySelectorAll<HTMLButtonElement>('[role="tab"]') ?? []);
        const index = tabs.indexOf(target as HTMLButtonElement);
        const next = event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : (index + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) % tabs.length;
        event.preventDefault();
        tabs[next].focus();
        tabs[next].click();
      }}>
        {showModel && <div><Eyebrow>Model</Eyebrow><Seg options={MODEL_TIERS.map(key => ({ key, label: MODELS[key].label }))} value={model} onChange={setModel} /></div>}
        <div><Eyebrow>Style</Eyebrow><Seg options={[{ key: "concise", label: "Concise" }, { key: "detailed", label: "Detailed" }, { key: "sources-only", label: "Sources only" }]} value={style} onChange={setStyle} /></div>
        {showEffort && <div><Eyebrow>Reasoning effort</Eyebrow><LevelDial ariaLabel="Reasoning effort" options={EFFORT_LEVELS} value={effort} onChange={setEffort} /></div>}
      </div>
    </Popover>
  </div>;
}
