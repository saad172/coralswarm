import { render } from "preact";
import type { ComponentType } from "preact";

/** Mounts a template into `#root`. Shared by every `entries/*.tsx` so the
 *  mount boilerplate (and any future global error boundary) lives in one
 *  place. */
export function mount(Template: ComponentType) {
  const root = document.getElementById("root");
  if (!root) return;
  render(<Template />, root);
}
