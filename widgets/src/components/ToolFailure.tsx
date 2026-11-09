import { Card } from "./Card";
import { Empty } from "./Empty";
import type { Tokens } from "../theme";

export interface ToolFailureBody { error: { code: string; message: string } }

export function isToolFailure(output: unknown): output is ToolFailureBody {
  if (!output || typeof output !== "object" || !("error" in output)) return false;
  const error = output.error;
  return !!error && typeof error === "object"
    && "code" in error && typeof error.code === "string"
    && "message" in error && typeof error.message === "string";
}

/** Reuse the approved card/empty-state treatment for host errors too. */
export function ToolFailure({ t, output, tool }: { t: Tokens; output: ToolFailureBody; tool: string }) {
  return <Card t={t} status={`${tool} error`}>
    <Empty t={t} title={output.error.code === "cancelled" ? "Request cancelled." : "Something went wrong."} body={output.error.message} meta={output.error.code} />
  </Card>;
}
