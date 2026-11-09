import crypto from "node:crypto";
import { expect, test } from "@playwright/test";
import { addMember, createReadyOrg, ingestNote, personalUser } from "../lib/isolation";
import { writeManifest } from "./manifest";

// N-1 phase 1, on THIS tree's e2e backend: write the data a rollback would
// leave behind, through the real API (plus the one e2e seeding route for
// extraction). `serve.spec.ts` reads it back through the production binary.
test("seed the data the production binary must serve @n1-seed", async ({ request }) => {
  test.setTimeout(60_000); // one org provisioning plus two users; setup, not a scenario

  const [a, stranger] = await Promise.all([personalUser(request, "n1-a"), personalUser(request, "n1-stranger")]);
  const ocean = await a.api.personalGraph();
  await stranger.api.graphs();
  const secret = `n1-personal-${crypto.randomUUID()}`;
  const { sourceId, atomIds } = await ingestNote(a.api, ocean.id, `The ${secret} plan is owned by Priya. It ships on March 14.`);
  const seeded = await a.api.json("POST", "/v1/e2e/seed/extraction", { graph_id: ocean.id, source_type: "note", source_id: sourceId });
  expect(seeded.semantic_atoms).toBeGreaterThan(0);

  const org = await createReadyOrg(request, "n1-org");
  const mine = (await org.adminInOrg.graphs()).find((g) => g.kind === "member");
  expect(mine, "the org admin has no private ocean").toBeTruthy();
  const orgSecret = `n1-org-${crypto.randomUUID()}`;
  const orgNote = await ingestNote(org.adminInOrg, mine!.id, `The ${orgSecret} review is on Friday.`);
  const member = await addMember(request, org, "n1-member");

  writeManifest({
    personal: { userId: a.user.id, email: a.email, oceanId: ocean.id, sourceId, atomId: atomIds[0], secret },
    stranger: { userId: stranger.user.id, email: stranger.email },
    org: {
      orgId: org.orgId,
      oceanId: org.oceanId,
      privateOceanId: mine!.id,
      admin: { userId: org.admin.user.id, email: org.admin.email },
      member: { userId: member.user.id, email: member.email },
      atomId: orgNote.atomIds[0],
      secret: orgSecret,
    },
  });
});
