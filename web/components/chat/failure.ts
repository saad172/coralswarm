/** Why an ask ended without an answer, in the words the thread shows. The
 *  codes are the server's `failure_reason`; an unknown one reads generically. */
export function failureCopy(reason: string): string {
  switch (reason) {
    case "db_busy":
      return "Couldn’t answer: the database was busy.";
    case "unavailable":
      return "Couldn’t answer: the service was unavailable.";
    case "evidence_changed":
      return "Couldn’t answer: the sources changed while it was being written.";
    case "conversation_changed":
      return "Couldn’t answer: this chat changed while it was being written.";
    case "model_error":
      return "Couldn’t answer: the answer model didn’t respond.";
    case "access_changed":
      return "Couldn’t answer: access to the sources changed.";
    case "no_reply":
      return "This question never got an answer.";
    default:
      return "Couldn’t answer this question.";
  }
}
