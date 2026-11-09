import { useEffect, useState } from "preact/hooks";
import type { Tokens } from "../theme";
import { Card } from "./Card";
import { Mandala } from "./Mandala";
import { AnswerUi } from "../answerUi";

interface WorkingProps {
  t: Tokens;
  /** Known ocean count, when derivable from toolInput's `ocean_ids`. */
  oceanCount?: number;
  /** Elapsed-seconds anchor — defaults to "now" at mount. */
  startedAt?: number;
  /** Host activity is opaque; never invent backend stages for a widget. */
  label?: string;
}

/** Working.dc.html — covers the gap between a tool call starting and its
 *  result landing (immediate case), and the legacy task_id polling window
 *  (get_task_result poll case). */
export function Working({ t, oceanCount, startedAt, label = "Waiting for your answer…" }: WorkingProps) {
  const [elapsed, setElapsed] = useState(0);
  useEffect(() => {
    const start = startedAt ?? Date.now();
    const id = setInterval(() => setElapsed(Math.floor((Date.now() - start) / 1000)), 1000);
    return () => clearInterval(id);
  }, [startedAt]);

  const status = oceanCount ? `${oceanCount} ocean${oceanCount === 1 ? "" : "s"} in scope` : "Waiting for a result";

  return (
    <Card t={t} status={status}>
      <div style={{ padding: 14 }}>
        <AnswerUi.Progress label={label} elapsedSeconds={elapsed} mark={<Mandala size={22} />} />
      </div>
    </Card>
  );
}
