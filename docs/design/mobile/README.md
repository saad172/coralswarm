# CoralSwarm Mobile — design reference

Imported from the CoralSwarm design pack ("CoralSwarm Mobile"). This is the
visual source of truth for the mobile (phone-shell) experience, the companion to
the desktop prototype in `docs/design/coralswarm-prototype.html`.

## Files
- `coralswarm-mobile.html` — entry point; wires React + the device frame and
  mounts `MobileApp`.
- `ios-frame.jsx` — iOS 26 device bezel, status bar, home indicator.
- `shell-shared.jsx` — shared atoms (icons, `HueDot`, `Avatar`), nav model and
  the sample Ask content.
- `mobile-app.jsx` — Ask screens (home / answer / across-oceans), composer and
  bottom sheets.
- `mobile-pages.jsx` — Reefs, Review, Currents, More + the workspace pages, and
  the shell (top bar + bottom tab bar) and root router.
- `_ds/` — the design-system token sheet (`styles.css`) and component bundle the
  prototype loads. Tokens here match `web/app/globals.css` and
  `web/tailwind.config.ts` one-to-one.

## How it maps into the live app
The mobile shell is implemented responsively in the Next.js app rather than as a
separate surface — the same routes serve web and mobile:

| Design piece            | Live implementation |
|-------------------------|---------------------|
| Bottom tab bar          | `web/components/MobileNav.tsx` (`< lg`) |
| Mobile top bar + ocean sheet | `web/components/Shell.tsx` (`lg:hidden` blocks) |
| "More" workspace launcher | `web/app/(app)/more/page.tsx` |
| Ask / Reefs / Review / Currents | existing routes, now mobile-responsive |

To preview the original prototype, serve this folder over HTTP (the babel
`<script src>` tags need a server) and open `coralswarm-mobile.html`.

## Desktop chrome — Console direction

At `lg+`, `web/components/Shell.tsx` implements the **Console** direction from
the same design pack: a collapsible icon rail (78px ⇄ 238px, persisted in
`localStorage` as `cs_rail_x`) with hover tooltips, and a header carrying an
italic-serif page title, a "New chat" action, the ocean switcher, and the
command-palette search. Mobile (`< lg`) keeps the phone shell described above.
