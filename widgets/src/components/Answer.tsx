import type { Tokens } from "../theme";
import { parseInline, splitParagraphs } from "../answerParse";
import { CiteChip } from "./CiteChip";
import { Fragment } from "preact";

interface AnswerProps {
  t: Tokens;
  text: string;
  activeChip: number | null;
  onChipClick: (index: number) => void;
  fontSize?: number;
}

/** Renders ask_ocean's `answer` markdown-lite prose: paragraphs, `**bold**`
 *  lead-ins, and `[n]` markers as clickable CiteChips. */
export function Answer({ t, text, activeChip, onChipClick, fontSize = 15.5 }: AnswerProps) {
  const paragraphs = splitParagraphs(text);
  return (
    <div style={{ fontSize, lineHeight: 1.7, color: t.ink }}>
      {paragraphs.map((para, pi) => (
        <p key={pi} style={{ margin: pi === paragraphs.length - 1 ? 0 : "0 0 12px" }}>
          {parseInline(para).map((tok, ti) => {
            if (tok.type === "text") return <Fragment key={ti}>{tok.value}</Fragment>;
            if (tok.type === "bold") return <strong key={ti}>{tok.value}</strong>;
            return <CiteChip key={ti} t={t} index={tok.index} active={activeChip === tok.index} onClick={() => onChipClick(tok.index)} />;
          })}
        </p>
      ))}
    </div>
  );
}
