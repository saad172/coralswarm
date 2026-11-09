// Every actual Next App Router page has an explicit budget entry. Next owns
// routing; scripts/check-page-routes.mjs fails on missing, duplicate or dead
// entries. Disabled/admin/refusal/redirect pages are exercised, never skipped.
export interface PageRoute {
  route: string;
  source: string;
  budget: "page" | "addin" | "dev";
  access: "public" | "authenticated" | "admin";
  samples: readonly string[];
  action?: { kind: "connector-callback"; reason: string; destinations: readonly string[]; picker?: { path: string; reason: string } };
  redirect?: { to: string; query: "preserve" | "drop" | "installation_id" | "toolkit" | "missing-id";
    anonymous?: { to: string; reason: string } };
  refusals?: readonly { path: string; status: number; reason: string }[];
}
export const pageRoutes = [
  {
    "route": "/add",
    "source": "(app)/add/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/add"
    ]
  },
  {
    "route": "/addin/approve",
    "refusals": [
      {
        "path": "/v1/addin/pair/info",
        "status": 404,
        "reason": "The declared expired-code representative must show its real refusal without pairing a device."
      }
    ],
    "source": "(app)/addin/approve/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/addin/approve?kind=microsoft&code={pairCode}"
    ]
  },
  {
    "route": "/ask",
    "source": "(app)/ask/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/ask"
    ],
    "redirect": {
      "to": "/chats",
      "query": "preserve"
    }
  },
  {
    "route": "/atlas",
    "source": "(app)/atlas/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/atlas"
    ],
    "redirect": {
      "to": "/home",
      "query": "drop"
    }
  },
  {
    "route": "/capture",
    "source": "(app)/capture/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/capture"
    ],
    "redirect": {
      "to": "/today",
      "query": "drop"
    }
  },
  {
    "route": "/chats",
    "source": "(app)/chats/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/chats",
      "/chats?id={conversation}"
    ]
  },
  {
    "route": "/connectors/[provider]",
    "source": "(app)/connectors/[provider]/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/connectors/google",
      "/connectors/microsoft"
    ]
  },
  {
    "route": "/connectors/composio",
    "source": "(app)/connectors/composio/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/connectors/composio",
      "/connectors/composio?toolkit=jira"
    ],
    "redirect": {
      "to": "/connectors",
      "query": "toolkit"
    }
  },
  {
    "route": "/connectors/github",
    "source": "(app)/connectors/github/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/connectors/github"
    ]
  },
  {
    "route": "/connectors/github/setup",
    "source": "(app)/connectors/github/setup/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/connectors/github/setup",
      "/connectors/github/setup?installation_id=123"
    ],
    "redirect": {
      "to": "/connectors/github",
      "query": "installation_id"
    }
  },
  {
    "route": "/connectors/google-chat",
    "source": "(app)/connectors/google-chat/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/connectors/google-chat"
    ],
    "redirect": {
      "to": "/connectors/google",
      "query": "drop"
    }
  },
  {
    "route": "/connectors/jira",
    "source": "(app)/connectors/jira/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/connectors/jira"
    ]
  },
  {
    "route": "/connectors/linear",
    "source": "(app)/connectors/linear/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/connectors/linear"
    ]
  },
  {
    "route": "/connectors/microsoft-teams/meeting-capture",
    "source": "(app)/connectors/microsoft-teams/meeting-capture/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/connectors/microsoft-teams/meeting-capture"
    ]
  },
  {
    "route": "/connectors/microsoft-teams",
    "source": "(app)/connectors/microsoft-teams/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/connectors/microsoft-teams"
    ],
    "redirect": {
      "to": "/connectors/microsoft",
      "query": "drop"
    }
  },
  {
    "route": "/connectors",
    "source": "(app)/connectors/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/connectors",
      "/connectors?setup=slack"
    ]
  },
  {
    "route": "/connectors/return",
    "action": {
      "destinations": ["/connectors", "/onboarding/connectors"],
      "picker": { "path": "/v1/connectors/{provider}/sources/{source}/scope-items", "reason": "Live provider choices during the acknowledged callback action, bound to the exact source in its verified computed detail and actual preceding cursor." },
      "kind": "connector-callback",
      "reason": "A successful OAuth callback completes an interaction, then awaits a real Stored refresh; count its critical reads separately after the verified refresh POST."
    },
    "source": "(app)/connectors/return/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/connectors/return?provider=slack"
    ]
  },
  {
    "route": "/connectors/slack",
    "source": "(app)/connectors/slack/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/connectors/slack"
    ]
  },
  {
    "route": "/coral",
    "source": "(app)/coral/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/coral?id={atom}"
    ]
  },
  {
    "route": "/currents",
    "source": "(app)/currents/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/currents"
    ],
    "redirect": {
      "to": "/today",
      "query": "preserve"
    }
  },
  {
    "route": "/download",
    "source": "(app)/download/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/download"
    ],
    "redirect": {
      "to": "/connectors",
      "query": "drop"
    }
  },
  {
    "route": "/graph",
    "source": "(app)/graph/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/graph?ocean={graph}"
    ]
  },
  {
    "route": "/home",
    "source": "(app)/home/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/home"
    ]
  },
  {
    "route": "/join/[org]",
    "source": "(app)/join/[org]/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/join/{orgSlug}"
    ]
  },
  {
    "route": "/library/[[...slug]]",
    "source": "(app)/library/[[...slug]]/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/library",
      "/library/history"
    ],
    "redirect": {
      "to": "/home",
      "query": "drop"
    }
  },
  {
    "route": "/library/people",
    "source": "(app)/library/people/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/library/people"
    ],
    "redirect": {
      "to": "/people",
      "query": "drop"
    }
  },
  {
    "route": "/meeting",
    "source": "(app)/meeting/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/meeting?graph_id={graph}&source_id={source}"
    ]
  },
  {
    "route": "/meetings",
    "source": "(app)/meetings/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/meetings"
    ],
    "redirect": {
      "to": "/home",
      "query": "drop"
    }
  },
  {
    "route": "/microsoft-teams",
    "source": "(app)/microsoft-teams/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/microsoft-teams"
    ],
    "redirect": {
      "to": "/connectors/microsoft-teams",
      "query": "drop"
    }
  },
  {
    "route": "/more",
    "source": "(app)/more/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/more"
    ]
  },
  {
    "route": "/people",
    "source": "(app)/people/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/people"
    ]
  },
  {
    "route": "/profile",
    "source": "(app)/profile/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/profile"
    ]
  },
  {
    "route": "/reef",
    "source": "(app)/reef/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/reef"
    ],
    "redirect": {
      "to": "/workspaces",
      "query": "preserve"
    }
  },
  {
    "route": "/reefs",
    "source": "(app)/reefs/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/reefs"
    ],
    "redirect": {
      "to": "/workspaces",
      "query": "preserve"
    }
  },
  {
    "route": "/session",
    "source": "(app)/session/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/session"
    ],
    "redirect": {
      "to": "/home",
      "query": "drop"
    }
  },
  {
    "route": "/sessions",
    "source": "(app)/sessions/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/sessions"
    ],
    "redirect": {
      "to": "/home",
      "query": "drop"
    }
  },
  {
    "route": "/settings/access",
    "source": "(app)/settings/access/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/settings/access"
    ],
    "redirect": {
      "to": "/settings/connections",
      "query": "preserve"
    }
  },
  {
    "route": "/settings/account",
    "source": "(app)/settings/account/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/settings/account"
    ],
    "redirect": {
      "to": "/settings",
      "query": "preserve"
    }
  },
  {
    "route": "/settings/answers",
    "source": "(app)/settings/answers/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/settings/answers"
    ],
    "redirect": {
      "to": "/settings/privacy",
      "query": "preserve"
    }
  },
  {
    "route": "/settings/billing",
    "source": "(app)/settings/billing/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/settings/billing"
    ]
  },
  {
    "route": "/settings/capture",
    "source": "(app)/settings/capture/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/settings/capture"
    ],
    "redirect": {
      "to": "/settings/privacy",
      "query": "preserve"
    }
  },
  {
    "route": "/settings/connections",
    "source": "(app)/settings/connections/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/settings/connections"
    ]
  },
  {
    "route": "/settings/organization/billing",
    "source": "(app)/settings/organization/billing/page.tsx",
    "budget": "page",
    "access": "admin",
    "samples": [
      "/settings/organization/billing"
    ]
  },
  {
    "route": "/settings/organization/connectors",
    "source": "(app)/settings/organization/connectors/page.tsx",
    "budget": "page",
    "access": "admin",
    "samples": [
      "/settings/organization/connectors"
    ]
  },
  {
    "route": "/settings/organization/members",
    "source": "(app)/settings/organization/members/page.tsx",
    "budget": "page",
    "access": "admin",
    "samples": [
      "/settings/organization/members"
    ]
  },
  {
    "route": "/settings/organization/permissions",
    "source": "(app)/settings/organization/permissions/page.tsx",
    "budget": "page",
    "access": "admin",
    "samples": [
      "/settings/organization/permissions"
    ]
  },
  {
    "route": "/settings/organization/policy",
    "source": "(app)/settings/organization/policy/page.tsx",
    "budget": "page",
    "access": "admin",
    "samples": [
      "/settings/organization/policy"
    ],
    "redirect": {
      "to": "/settings/organization/permissions",
      "query": "preserve"
    }
  },
  {
    "route": "/settings/organization/storage",
    "source": "(app)/settings/organization/storage/page.tsx",
    "budget": "page",
    "access": "admin",
    "samples": [
      "/settings/organization/storage"
    ],
    "redirect": {
      "to": "/settings/organization/usage",
      "query": "preserve"
    }
  },
  {
    "route": "/settings/organization/usage",
    "source": "(app)/settings/organization/usage/page.tsx",
    "budget": "page",
    "access": "admin",
    "samples": [
      "/settings/organization/usage"
    ]
  },
  {
    "route": "/settings",
    "source": "(app)/settings/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/settings"
    ]
  },
  {
    "route": "/settings/privacy",
    "source": "(app)/settings/privacy/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/settings/privacy"
    ]
  },
  {
    "route": "/settings/retraction-history",
    "source": "(app)/settings/retraction-history/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/settings/retraction-history"
    ],
    "redirect": {
      "to": "/settings/privacy",
      "query": "preserve"
    }
  },
  {
    "route": "/settings/usage",
    "source": "(app)/settings/usage/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/settings/usage"
    ]
  },
  {
    "route": "/source",
    "source": "(app)/source/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/source?graph_id={graph}&source_item_id={sourceItem}"
    ]
  },
  {
    "route": "/team",
    "source": "(app)/team/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/team"
    ],
    "redirect": {
      "to": "/people",
      "query": "drop"
    }
  },
  {
    "route": "/today",
    "source": "(app)/today/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/today",
      "/today?z=week",
      "/today?z=month"
    ]
  },
  {
    "route": "/topics",
    "source": "(app)/topics/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/topics?id={node}",
      "/topics"
    ],
    "redirect": {
      "to": "/home",
      "query": "missing-id"
    }
  },
  {
    "route": "/workflows",
    "source": "(app)/workflows/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/workflows"
    ],
    "redirect": {
      "to": "/connectors",
      "query": "drop"
    }
  },
  {
    "route": "/workspaces/manage",
    "source": "(app)/workspaces/manage/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/workspaces/manage?id={sharedGraph}"
    ]
  },
  {
    "route": "/workspaces",
    "source": "(app)/workspaces/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/workspaces",
      "/workspaces?id={graph}",
      "/workspaces?id={sharedGraph}"
    ]
  },
  {
    "route": "/account/delete/cancel",
    "source": "account/delete/cancel/page.tsx",
    "budget": "page",
    "access": "public",
    "samples": [
      "/account/delete/cancel"
    ]
  },
  {
    "route": "/account/delete/pending",
    "source": "account/delete/pending/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/account/delete/pending"
    ]
  },
  {
    "route": "/account-deleted",
    "source": "account-deleted/page.tsx",
    "budget": "page",
    "access": "public",
    "samples": [
      "/account-deleted"
    ]
  },
  {
    "route": "/addin/auth/callback",
    "source": "addin/auth/callback/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/addin/auth/callback"
    ]
  },
  {
    "route": "/addin/auth/start",
    "source": "addin/auth/start/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/addin/auth/start"
    ]
  },
  {
    "route": "/dev/graph-token",
    "source": "dev/graph-token/page.tsx",
    "budget": "dev",
    "access": "authenticated",
    "samples": [
      "/dev/graph-token"
    ]
  },
  {
    "route": "/dev/out-of-credits",
    "source": "dev/out-of-credits/page.tsx",
    "budget": "dev",
    "access": "authenticated",
    "samples": [
      "/dev/out-of-credits"
    ]
  },
  {
    "route": "/dev/promote-progress",
    "source": "dev/promote-progress/page.tsx",
    "budget": "dev",
    "access": "authenticated",
    "samples": [
      "/dev/promote-progress"
    ]
  },
  {
    "route": "/dev/usage-page",
    "source": "dev/usage-page/page.tsx",
    "budget": "dev",
    "access": "authenticated",
    "samples": [
      "/dev/usage-page"
    ]
  },
  {
    "route": "/login",
    "source": "login/page.tsx",
    "budget": "page",
    "access": "public",
    "samples": [
      "/login"
    ]
  },
  {
    "route": "/meet/sidepanel",
    "source": "meet/sidepanel/page.tsx",
    "budget": "addin",
    "access": "authenticated",
    "samples": [
      "/meet/sidepanel"
    ]
  },
  {
    "route": "/onboarding/answer",
    "source": "onboarding/answer/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/answer"
    ]
  },
  {
    "route": "/onboarding/connectors",
    "source": "onboarding/connectors/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/connectors"
    ]
  },
  {
    "route": "/onboarding/join",
    "source": "onboarding/join/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/join"
    ]
  },
  {
    "route": "/onboarding/org/invite",
    "source": "onboarding/org/invite/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/org/invite"
    ]
  },
  {
    "route": "/onboarding/org",
    "source": "onboarding/org/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/org"
    ]
  },
  {
    "route": "/onboarding/org/policy",
    "source": "onboarding/org/policy/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/org/policy"
    ]
  },
  {
    "route": "/onboarding/org/sources",
    "source": "onboarding/org/sources/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/org/sources"
    ]
  },
  {
    "route": "/onboarding/org/workspaces",
    "source": "onboarding/org/workspaces/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/org/workspaces"
    ]
  },
  {
    "route": "/onboarding",
    "source": "onboarding/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding"
    ]
  },
  {
    "route": "/onboarding/secure",
    "source": "onboarding/secure/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/secure"
    ]
  },
  {
    "route": "/onboarding/setup",
    "source": "onboarding/setup/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/setup"
    ],
    "redirect": {
      "to": "/onboarding/connectors",
      "query": "preserve"
    }
  },
  {
    "route": "/onboarding/team/canceled",
    "source": "onboarding/team/canceled/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/team/canceled"
    ]
  },
  {
    "route": "/onboarding/team",
    "source": "onboarding/team/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/team"
    ]
  },
  {
    "route": "/onboarding/team/return",
    "source": "onboarding/team/return/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/onboarding/team/return"
    ]
  },
  {
    "route": "/org/deletion",
    "refusals": [
      {
        "path": "/v1/orgs/{org_id}/deletion",
        "status": 404,
        "reason": "The backend returns 404 when ORG_DELETION_ENABLED is off; render the existing Not available refusal without claiming loaded data."
      }
    ],
    "source": "org/deletion/page.tsx",
    "budget": "page",
    "access": "authenticated",
    "samples": [
      "/org/deletion"
    ]
  },
  {
    "route": "/outlook/mail",
    "source": "outlook/mail/page.tsx",
    "budget": "addin",
    "access": "authenticated",
    "samples": [
      "/outlook/mail"
    ]
  },
  {
    "route": "/outlook/meeting/connected",
    "source": "outlook/meeting/connected/page.tsx",
    "budget": "addin",
    "access": "authenticated",
    "samples": [
      "/outlook/meeting/connected"
    ]
  },
  {
    "route": "/outlook/meeting",
    "source": "outlook/meeting/page.tsx",
    "budget": "addin",
    "access": "authenticated",
    "samples": [
      "/outlook/meeting"
    ]
  },
  {
    "route": "/",
    "source": "page.tsx",
    "budget": "page",
    "access": "public",
    "samples": [
      "/"
    ],
    "redirect": {
      "to": "/home",
      "query": "drop",
      "anonymous": {
        "to": "/login",
        "reason": "The root bookmark enters /home; AppLayout sends the anonymous audience to /login before Home mounts."
      }
    }
  },
  {
    "route": "/shared",
    "refusals": [
      {
        "path": "/v1/shared/{token}",
        "status": 409,
        "reason": "Legacy whole-original sharing is disabled in favor of selected evidence consent."
      },
      {
        "path": "/v1/meetings/shares/view/{token}",
        "status": 409,
        "reason": "The authenticated legacy whole-original viewer preserves the same consent refusal."
      }
    ],
    "source": "shared/page.tsx",
    "budget": "page",
    "access": "public",
    "samples": [
      "/shared?t={shareToken}"
    ]
  },
  {
    "route": "/signup",
    "source": "signup/page.tsx",
    "budget": "page",
    "access": "public",
    "samples": [
      "/signup",
      "/signup?pt={prefillToken}"
    ]
  },
  {
    "route": "/teams/meeting/configure",
    "source": "teams/meeting/configure/page.tsx",
    "budget": "addin",
    "access": "authenticated",
    "samples": [
      "/teams/meeting/configure"
    ]
  },
  {
    "route": "/teams/meeting/connected",
    "source": "teams/meeting/connected/page.tsx",
    "budget": "addin",
    "access": "authenticated",
    "samples": [
      "/teams/meeting/connected"
    ]
  },
  {
    "route": "/teams/meeting",
    "source": "teams/meeting/page.tsx",
    "budget": "addin",
    "access": "authenticated",
    "samples": [
      "/teams/meeting"
    ]
  }
] as const satisfies readonly PageRoute[];
