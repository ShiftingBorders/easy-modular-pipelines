# Logo assets

The logo shows a hand placing a module between two others.

- [logo.svg](logo.svg): wordmark for light backgrounds.
- [logo-dark.svg](logo-dark.svg): wordmark for dark backgrounds.
- [logo-monochrome.svg](logo-monochrome.svg): single-color version.
- [logo-mark.svg](logo-mark.svg): symbol without the wordmark, for light backgrounds.

These four SVGs have transparent backgrounds and tightly fitted canvases.
The three squares share the same dimensions, corner radius, and outline width.
Lettering is converted to paths from the local Manrope font; no external fonts
or raster images are required. See the [Manrope OFL](../../dashboard/static/fonts/Manrope-OFL.txt).

The main README displays the full logo at 420 px wide. Use the standalone mark
at 64 px or larger to keep the finger outlines legible. A dedicated favicon
variant is not provided.

## Feature logos

Each illustration has light and dark variants. The dedicated feature pages
display them at the very top, before the page title, at 420 px wide, matching
the main README logo width. A `picture` element selects the dark variant for
`prefers-color-scheme: dark` and uses the light variant as the default image,
following the main README. Each SVG canvas is fitted to its artwork with a small
margin, preserving the drawing's proportions. All variants remain vector images.

| Light variant | Dark variant | Documentation placement |
| --- | --- | --- |
| [logo-compare-light.svg](logo-compare-light.svg) | [logo-compare-dark.svg](logo-compare-dark.svg) | [Module identity and integrity checks](../module_integrity.md) |
| [logo-execution-light.svg](logo-execution-light.svg) | [logo-execution-dark.svg](logo-execution-dark.svg) | [Controlled experiment execution](../execution.md) |
| [logo-restore-light.svg](logo-restore-light.svg) | [logo-restore-dark.svg](logo-restore-dark.svg) | [Snapshots and recovery](../restoration.md) |
| [logo-packing-light.svg](logo-packing-light.svg) | [logo-packing-dark.svg](logo-packing-dark.svg) | [Experiment packaging and exchange](../packaging.md) |
| [logo-observe-light.svg](logo-observe-light.svg) | [logo-observe-dark.svg](logo-observe-dark.svg) | [Execution history and observation](../observation.md) |
