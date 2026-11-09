import type { Tokens } from "../theme";
import type { CitationDetail } from "../types";
import { AnswerUi, citationEvidence } from "../answerUi";

interface SourceCardProps {
  t: Tokens;
  index: number;
  active: boolean;
  citation: CitationDetail;
  onClick: () => void;
}

/** State stays in AskOcean; the same compact evidence renderer serves web. */
export function SourceCard({ t, index, active, citation, onClick }: SourceCardProps) {
  return <AnswerUi.EvidenceCard source={citationEvidence(t, citation, index)} active={active} onSelect={onClick} />;
}
