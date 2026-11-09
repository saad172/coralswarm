FROM node:22-alpine AS builder
WORKDIR /build
RUN corepack enable && corepack prepare pnpm@9.15.0 --activate
COPY . .
ARG NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY
ENV NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY=$NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY NEXT_PUBLIC_API_BASE=http://localhost:8080 NEXT_PUBLIC_WEB_APEX="" NEXT_TELEMETRY_DISABLED=1
RUN pnpm install --frozen-lockfile
RUN pnpm --filter @coralswarm/web build

FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1 \
    PORT=8080 \
    HOSTNAME=0.0.0.0

# The standalone bundle traces into web/ (monorepo layout). Static assets are not
# included in the trace, so copy them alongside server.js.
COPY --from=builder /build/web/.next/standalone ./
COPY --from=builder /build/web/.next/static ./web/.next/static
# `public/` is NOT in the standalone trace either (Next's own docs say to copy
# it yourself), and this image used to say there was no public/ dir at all —
# true until the Gmail Add-on needed a stable, unauthenticated logo URL. The
# add-on's Apps Script manifest hardcodes
# `https://app.coralswarm.com/addin/gmail-icon.png`, so if this COPY is
# dropped the standalone server 404s it and Gmail renders the add-on card with
# no icon. Destination must be `./web/public` (NOT `./public`): the standalone
# server is `web/server.js` and next-server resolves `public/` relative to its
# own dir, the same reason `.next/static` lands under `web/` above.
COPY --from=builder /build/web/public ./web/public

EXPOSE 8080
CMD ["node", "web/server.js"]
