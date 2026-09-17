# The demo kit

Every grampy demo looks like the same brick lab: a light bench seen from
above, white trays, bricks in primary colours, a row of studs as a rule.

- `lab.css` — the tokens (light and dark), the page chrome (masthead, tally,
  controls, side panel, journal log, code panels, legend) and the bench
  (belts, dashed waiting pads, trays, lanes, bins, bricks, sparkle and puff).
- `lab.js` — `window.Lab`: SVG helpers, `tray`, `belt`, `brick`, `paint`,
  `place`, `sparkle`, `puff`, and `highlight` / `showSource` to show Python
  read straight from the scenario the page runs.

A new demo loads the kit before its own files and keeps only what is its own:

```html
<link rel="stylesheet" href="kit/lab.css">
<link rel="stylesheet" href="my-demo.css">
<script src="kit/lab.js"></script>
<script src="my-demo.js"></script>
```

Fonts: Rubik for headings, IBM Plex Sans and Mono for the rest (Google Fonts).
