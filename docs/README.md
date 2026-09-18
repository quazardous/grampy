# grampy documentation

- [The rules](rules.md) — every mechanism, spelled out: the claim rule, joins
  and choices, history and loops, time, lanes, policies, limits, versions.
- [Items](items.md) — the canonical way in: hand grampy your own objects and
  let handlers answer what needs the data.
- [grampy's tables next to yours](your-data.md) — what you may read, what
  you must not write, and what the columns mean.
- [Drivers and candidates](drivers.md) — the tables you declare, how
  eligibility is passed, and how ids become your own objects.
- [Writing a driver](writing-a-driver.md) — the capabilities a driver offers,
  what each method may return, and certifying it with the shared contract.
- [Operating in production](operating.md) — the indexes worth adding, measured;
  what grows; the janitor; and monitoring with `journal.snapshot`: a growing,
  a starving or a stuck node, read between two samples.
- [Drawings](drawings.md) — Mermaid flowchart, Mermaid state diagram, Graphviz,
  with live counts.
- [The same notions in other tools](concepts.md) — Graphile Worker, BullMQ,
  Hatchet, Inngest, Temporal, Oban, with a source per cell.
- [The demo](demo/) — the brick sorter that runs the library in a browser,
  published at <https://quazardous.github.io/grampy/>; its shared look lives in
  [the kit](demo/kit/README.md).

Back to the [README](../README.md).
