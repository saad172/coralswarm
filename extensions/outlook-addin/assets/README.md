# Add-in icons

Office requires PNG icons at these exact sizes, referenced by `manifest.xml`:

| File | Size | Used for |
|---|---|---|
| `icon-16.png` | 16×16 | ribbon (small) |
| `icon-32.png` | 32×32 | ribbon (medium) |
| `icon-64.png` | 64×64 | store/`IconUrl` |
| `icon-80.png` | 80×80 | ribbon (large) |
| `icon-128.png` | 128×128 | store/`HighResolutionIconUrl` |

The mark is the CoralSwarm **DotMandala** — five concentric rings of hue-cycled
dots (coral → pink → amber → teal → plum), geometry-identical to `<DotMandala>`
in `web/components/ui.tsx`. It is the only CoralSwarm mark; never substitute a
glyph or wrap it in a container shape (per the brand guidelines).

`brand-mark.svg` (in this folder) is the source the task pane and login dialog
render inline. The PNGs are generated from the same geometry:

```
node scripts/build-mark.js --icons   # writes brand-mark.svg + icon-*.png (uses macOS `sips`)
```
