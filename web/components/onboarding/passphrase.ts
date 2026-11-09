// A small, boring word list for suggesting a memorable multi-word passphrase
// on 5b (Step 1 · Secure). This is convenience UI only — it never touches
// lib/engine's crypto and generates nothing that is itself stored; the
// engine only ever sees whatever string ends up in the passphrase field
// (generated here, or typed by hand) when `setup()`/`unlock()` is called.
// Deliberately not cryptographically audited word-list wisdom (EFF-style) —
// just enough entropy to demonstrate the flow. Uses the Web Crypto RNG
// rather than Math.random for the selection.
const WORDS = [
  "amber", "anchor", "atlas", "basin", "birch", "boulder", "bramble", "brook",
  "canyon", "cedar", "cinder", "cliff", "clover", "copper", "coral", "cove",
  "current", "delta", "drift", "ember", "estuary", "fern", "fjord", "flint",
  "forge", "granite", "harbor", "hollow", "inlet", "juniper", "kelp", "lagoon",
  "lichen", "maple", "marsh", "meadow", "mesa", "moss", "orchard", "otter",
  "pebble", "pine", "prairie", "quartz", "reef", "ridge", "river", "sable",
  "shale", "shoal", "spruce", "summit", "tidal", "timber", "tundra", "vale",
  "willow", "zephyr",
];

function randomWord(): string {
  const bytes = new Uint32Array(1);
  crypto.getRandomValues(bytes);
  return WORDS[bytes[0] % WORDS.length];
}

/** A `count`-word passphrase, hyphen-joined (e.g. "cedar-otter-flint-marsh"). */
export function generatePassphrase(count = 4): string {
  return Array.from({ length: count }, randomWord).join("-");
}

/** Coarse strength read for the 4-bar meter: word count is the only signal
 *  that matters for a generated passphrase (each word is a fixed ~5.8 bits
 *  from a 58-word list; a hand-typed one falls back to length). Returns
 *  0–4 filled bars. */
export function passphraseStrength(pp: string): number {
  const words = pp.split(/[-\s]+/).filter(Boolean).length;
  if (words >= 3) return Math.min(4, words);
  // Hand-typed, no word separators — fall back to a length heuristic.
  if (pp.length >= 20) return 4;
  if (pp.length >= 14) return 3;
  if (pp.length >= 8) return 2;
  return pp.length > 0 ? 1 : 0;
}
