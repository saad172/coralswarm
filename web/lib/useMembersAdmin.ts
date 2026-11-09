"use client";

import { useQueryClient } from "@tanstack/react-query";
import { useApp } from "./providers";
import { usePageSection } from "./pageSections";
import { pageSectionPath } from "./pageSectionWire";
import { pageReadAuthorityRevision } from "./pageReadAuthority";
import type { MembersAdminPayload } from "./pageSectionTypes/membersAdmin";

export function useMembersAdmin() {
  const { queryScope } = useApp();
  const client = useQueryClient();
  const section = usePageSection<MembersAdminPayload>("members_admin");
  return { ...section, queryScope, authorityRevision: pageReadAuthorityRevision(client,
    JSON.stringify([queryScope, pageSectionPath("members_admin")])) };
}

export const membersAdminKey = (scope: string) => ["members_admin", scope] as const;
