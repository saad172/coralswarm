Deprecated for web. The remaining consumer is `widgets/src/answerUi.ts`. Migrate widgets before removing this package.

# Answer UI

Shared DOM and scoped CSS for the CoralSwarm web app and MCP widgets. This source
module has no runtime dependencies. Inject React.createElement or Preact.h into
`createAnswerUi` through a typed adapter; import `src/styles.css` once per app.

Progress is a presentation component: supply real stage labels and optional
elapsed seconds. Evidence cards, lists and detail render supplied titles,
excerpts and provenance. Omit `number` for non-citation evidence; preserve the
original number for citations. State, timers, selection, host actions and source
normalization belong to the consumer. `CardFrame` provides shared header/body/
footer structure without dictating host sizing or navigation.

Consumers map `--csau-*` variables to their established surface, ink, line,
accent, font and radius tokens. All shared selectors are scoped with `csau-`.
The web adapter and its component tests exercise the same factory used by the
Preact widget adapter; each caller's build typechecks the generic interface.
