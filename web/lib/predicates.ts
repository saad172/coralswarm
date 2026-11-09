/* Human-readable verbs for relationship-edge predicate ids (ADR-0008).

   Mirrors the registry in backend/src/relationships/predicates.rs and the
   phrasing of `humanize_predicate` in backend/src/routes/queries.rs, so an
   edge reads the same on the Graph canvas as it does in an Ask answer:
   "<src> works on <dst>", "<src> is the expert in <dst>". Unknown ids fall
   back to the bare id with underscores spaced, which is how the backend
   already renders the interpersonal/org predicates — those read fine as-is. */

const VERBS: Record<string, string> = {
  // Kinship
  spouse_of: "is the spouse of",
  parent_of: "is a parent of",
  child_of: "is a child of",
  sibling_of: "is a sibling of",
  cousin_of: "is a cousin of",
  grandparent_of: "is a grandparent of",
  grandchild_of: "is a grandchild of",
  aunt_uncle_of: "is an aunt or uncle of",
  niece_nephew_of: "is a niece or nephew of",
  parent_in_law_of: "is a parent-in-law of",
  sibling_in_law_of: "is a sibling-in-law of",
  // Professional
  manages: "manages",
  reports_to: "reports to",
  colleague_of: "is a colleague of",
  founder_of: "founded",
  works_at: "works at",
  mentors: "mentors",
  mentored_by: "is mentored by",
  advises: "advises",
  advised_by: "is advised by",
  business_partner_of: "is a business partner of",
  investor_in: "invests in",
  member_of: "is a member of",
  leads_team: "leads",
  peer_of: "is a peer of",
  // Social
  friend_of: "is a friend of",
  knows: "knows",
  // Topical (same strings as the backend's humanize_predicate)
  related_to: "is related to",
  works_on: "works on",
  worked_on_by: "is worked on by",
  responsible_for: "is responsible for",
  responsibility_of: "is the responsibility of",
  mentions: "mentions",
  mentioned_by: "is mentioned by",
  expert_in: "is the expert in",
  expertise_of: "is an area of expertise of",
  co_occurs_with: "shares items with",
};

/** Verb phrase for a predicate id, e.g. `works_on` → "works on". */
export function predicateVerb(predicate: string | undefined | null): string {
  if (!predicate) return "is linked to";
  return VERBS[predicate] ?? predicate.replace(/_/g, " ");
}

/** Short noun form for chips/legends, e.g. `works_on` → "works on",
 *  `expert_in` → "expert in". Drops the leading copula so it fits a badge. */
export function predicateShort(predicate: string | undefined | null): string {
  const v = predicateVerb(predicate);
  return v.replace(/^is (an? |the )?/, "");
}
