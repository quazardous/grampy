"use strict";

// The brick sorter. The page draws; grampy decides. Every half second the
// Python world advances (scenario.py, running the real journal in Pyodide) and
// returns, for each brick, where the journal says it stands. Nothing here moves
// a brick anywhere the journal did not put it. The look and the shapes come
// from the demo kit (kit/lab.css, kit/lab.js).
//
// The bench seen from above: new bricks and bricks sent back wait in the inbox
// lane, each station is a tray holding the bricks it works on, waiting happens
// on the dashed pads, and the line ends in one tray per colour, plus a waste
// bin for the TNT the bomb squad could not save.

const { el, text, escapeHtml, scatter, BRICK } = Lab;

const STATION = { w: 150, h: 96 };
const BIN = { x: 1224, y: 16, w: 84, h: 86, gap: 4 };
const WASTE = { x: 1224, y: 424, w: 172, h: 124 };
const QUEUE = { columns: 2, rows: 5 };
const INBOX = { columns: 4, rows: 2 };
const KEPT_PER_BIN = 16;
const KEPT_IN_WASTE = 24;
const TICK_MS = 500;

const $ = (id) => document.getElementById(id);
const bench = $("bench");

let world = null;
let playing = true;
let selected = null;
let colours = [];
let returnable = [];
const seen = new Map();          // brick id -> {place, revealed}
const nodes = {};                // node name -> {x, y, spec, count, overflow}
const bins = {};                 // colour -> {x, y, count, kept}
const waste = { kept: [] };

// -- the bench, drawn once from the graph document ------------------------------

function drawBench(graph, layout, palette) {
  bench.replaceChildren();
  Lab.defs(bench);

  for (const [name, spec] of Object.entries(graph.nodes)) {
    const [layer, row] = layout[name];
    nodes[name] = { x: 120 + layer * 232, y: 40 + row * 200, spec };
  }

  const pads = el("g", {}, bench);
  for (const node of Object.values(nodes)) {
    if (!node.spec.lane) drawQueuePad(pads, node);
  }

  const belts = el("g", {}, bench);
  for (const [name, spec] of Object.entries(graph.nodes)) {
    for (const parent of spec.parents || []) {
      const failing = spec.on && spec.on[parent] && spec.on[parent].includes("failed");
      Lab.belt(belts, beltPath(nodes[parent], nodes[name]), { failing });
    }
  }
  drawReturnBelt(belts, "defuse");
  straightBelt(belts, nodes.pack, BIN.x - 4);
  straightBelt(belts, nodes.reject, WASTE.x - 4);

  const trays = el("g", {}, bench);
  for (const [name, node] of Object.entries(nodes)) drawStation(trays, name, node);
  drawBins(trays, palette);
  drawWaste(trays);

  el("g", { id: "bricks" }, bench);
  el("g", { id: "effects" }, bench);
}

function drawQueuePad(parent, node) {
  const w = QUEUE.columns * 28 + 8;
  el("rect", { class: "queue-pad", x: node.x - w - 8, y: node.y + 4, width: w, height: STATION.h - 8, rx: 6 }, parent);
  node.overflow = text(parent, { x: node.x - w / 2 - 8, y: node.y + STATION.h + 14,
                                 "text-anchor": "middle", class: "bench-label" }, "");
}

function beltPath(from, to) {
  const x1 = from.x + STATION.w, y1 = from.y + STATION.h / 2;
  const x2 = to.x, y2 = to.y + STATION.h / 2;
  if (x2 - x1 > 300) {
    // A belt skipping a layer runs under the stations in between, not through them.
    const low = Math.max(from.y, to.y) + STATION.h + 30;
    return `M${from.x + STATION.w / 2},${from.y + STATION.h} L${from.x + STATION.w / 2},${low} ` +
           `L${to.x + STATION.w / 2},${low} L${to.x + STATION.w / 2},${to.y + STATION.h}`;
  }
  const mid = (x1 + x2) / 2;
  return `M${x1},${y1} C${mid},${y1} ${mid},${y2} ${x2},${y2}`;
}

function straightBelt(parent, node, x) {
  const y = node.y + STATION.h / 2;
  Lab.belt(parent, `M${node.x + STATION.w},${y} L${x},${y}`);
}

function drawReturnBelt(parent, name) {
  const n = nodes[name];
  const top = n.y - 36;
  Lab.belt(parent, `M${n.x + STATION.w - 22},${n.y} L${n.x + STATION.w - 22},${top} L${n.x + 22},${top} L${n.x + 22},${n.y}`);
  text(parent, { x: n.x + STATION.w / 2, y: top - 14, "text-anchor": "middle", class: "bench-label" },
       "retry belt · 8s → 16s → 32s");
}

function drawStation(parent, name, node) {
  const { spec } = node;
  const kinds = ["station", name];
  if (spec.choice) kinds.push("choice");
  if (spec.wait) kinds.push("wait");
  if (spec.lane) kinds.push("lane");
  const g = Lab.tray(parent, node.x, node.y, STATION.w, STATION.h, kinds.join(" "));
  if (spec.wait) el("rect", { class: "stripe", x: 5, y: 5, width: STATION.w - 10, height: 5 }, g);
  if (spec.lane) el("line", { class: "slot", x1: 44, y1: 5, x2: STATION.w - 44, y2: 5 }, g);
  text(g, { x: 14, y: 28, class: "name" }, name);
  text(g, { x: 14, y: 42, class: "role" }, describe(spec));
  node.count = text(g, { x: STATION.w - 12, y: STATION.h - 12, "text-anchor": "end", class: "count" }, "");
  if (spec.lane) {
    node.overflow = text(parent, { x: node.x + STATION.w / 2, y: node.y + STATION.h + 16,
                                   "text-anchor": "middle", class: "bench-label" }, "");
  }
}

function drawBins(parent, palette) {
  palette.forEach((colour, i) => {
    const x = BIN.x + (i % 2) * (BIN.w + BIN.gap);
    const y = BIN.y + Math.floor(i / 2) * (BIN.h + BIN.gap);
    const g = Lab.tray(parent, x, y, BIN.w, BIN.h, "bin");
    el("rect", { class: `tab ${colour}`, x: 11, y: BIN.h - 19, width: 16, height: 8, rx: 2 }, g);
    const count = text(g, { x: BIN.w - 12, y: BIN.h - 11, "text-anchor": "end", class: "count" }, "0");
    bins[colour] = { x, y, count, kept: [] };
  });
  text(parent, { x: BIN.x + BIN.w + BIN.gap / 2, y: BIN.y + 3 * (BIN.h + BIN.gap) + 14,
                 "text-anchor": "middle", class: "bench-label" }, "sorted · click one to send it back");
}

function drawWaste(parent) {
  const g = el("g", { class: "waste", transform: `translate(${WASTE.x},${WASTE.y})` }, parent);
  el("rect", { class: "waste-body", width: WASTE.w, height: WASTE.h, rx: 8, filter: "url(#lab-lift)" }, g);
  el("rect", { class: "waste-floor", x: 8, y: 16, width: WASTE.w - 16, height: WASTE.h - 24, rx: 5 }, g);
  el("rect", { class: "stripe", x: 8, y: 5, width: WASTE.w - 16, height: 6, rx: 2 }, g);
  text(parent, { x: WASTE.x + WASTE.w / 2, y: WASTE.y + WASTE.h + 16, "text-anchor": "middle",
                 class: "bench-label" }, "waste · TNT past saving");
}

function describe(spec) {
  if (spec.lane) {
    const lane = spec.lane;
    // The blue rim says "lane"; the role line has room for the timings only.
    return [lane.cooldown && `cooldown ${lane.cooldown}`, lane.max_wait && `max ${lane.max_wait}`]
      .filter(Boolean).join(" · ") || "lane";
  }
  if (spec.optional) return "optional · salvage skips it";
  if (spec.choice) return "choice · colour or TNT";
  if (spec.wait) return `waits for squad · ⏱ ${spec.timeout}`;
  if (spec.retry) return `retry ×${spec.retry.limit} · exponential`;
  if (spec.need) return `on failed · ${spec.need}/${spec.parents.length}`;
  if ((spec.parents || []).length > 1) return "joins both routes";
  return spec.lease ? `lease ${spec.lease}` : "";
}

// -- where a brick goes --------------------------------------------------------

function inBin(brick) {
  const bin = bins[brick.colour];
  return {
    x: bin.x + 10 + scatter(brick.id, 1) * (BIN.w - 20 - BRICK.w),
    y: bin.y + 12 + scatter(brick.id, 2) * (BIN.h - 44 - BRICK.h),
    turn: scatter(brick.id, 3) < 0.4,
  };
}

function inWaste(id) {
  return {
    x: WASTE.x + 14 + scatter(id, 4) * (WASTE.w - 28 - BRICK.w),
    y: WASTE.y + 22 + scatter(id, 5) * (WASTE.h - 38 - BRICK.h),
    turn: scatter(id, 6) < 0.5,
  };
}

function target(brick, slots) {
  const node = nodes[brick.node];
  const key = `${brick.place}:${brick.node}`;
  const i = slots.get(key) || 0;
  slots.set(key, i + 1);
  switch (brick.place) {
    case "inbox":
      return { x: node.x + 12 + (i % INBOX.columns) * 33, y: node.y + 56 + Math.floor(i / INBOX.columns) * 20,
               hidden: i >= INBOX.columns * INBOX.rows };
    case "queue": {
      const column = i % QUEUE.columns, row = Math.floor(i / QUEUE.columns);
      return { x: node.x - 38 - column * 28, y: node.y + 12 + row * 17, hidden: row >= QUEUE.rows };
    }
    case "station":
      return { x: node.x + 14 + i * 30, y: node.y + 62 };
    case "shelf":
      return { x: node.x + 12 + (i % 4) * 32, y: node.y + 54 + Math.floor(i / 4) * 18, hidden: i >= 8 };
    case "retry":
      return { x: node.x + 30 + i * 28, y: node.y - 43 };
    case "shipped":
      return inBin(brick);
    case "boom":
      return inWaste(brick.id);
    default:
      return { x: 10, y: 10 };
  }
}

// -- one frame ------------------------------------------------------------------

function keep(list, g, cap) {
  g.dataset.kept = "1";
  list.push(g);
  while (list.length > cap) list.shift().remove();
}

function unkeep(id) {
  const g = $("bricks").querySelector(`[data-id="${id}"][data-kept]`);
  if (!g) return;
  for (const holder of [...Object.values(bins), waste]) {
    holder.kept = holder.kept.filter((x) => x !== g);
  }
  g.remove();
}

function newBrick(id) {
  const g = Lab.brick({
    onClick: () => (g.dataset.kept && g.classList.contains("returnable") ? sendBack(id) : select(id)),
  });
  g.dataset.id = id;
  return g;
}

function render(state) {
  $("clock").textContent = state.clock;
  $("shipped").textContent = state.shipped;
  $("exploded").textContent = state.exploded;
  $("inbox-count").textContent = (state.counts.inbox && state.counts.inbox.waiting) || 0;
  returnable = state.returnable;
  for (const [colour, count] of Object.entries(state.sorted)) {
    if (bins[colour]) bins[colour].count.textContent = count;
  }

  const layer = $("bricks");
  const effects = $("effects");
  const alive = new Set();
  const slots = new Map();
  const overflow = {};
  let nextCooldown = null;
  for (const brick of state.bricks) {
    alive.add(brick.id);
    let g = layer.querySelector(`[data-id="${brick.id}"]:not([data-kept])`);
    const before = seen.get(brick.id);
    if (!g) {
      const kept = layer.querySelector(`[data-id="${brick.id}"][data-kept]`);
      const from = kept ? kept.style.transform : `translate(10px, ${nodes.inbox.y + 40}px)`;
      unkeep(brick.id);
      g = newBrick(brick.id);
      g.style.transform = from;
      layer.appendChild(g);
      g.getBoundingClientRect();
    }
    const at = target(brick, slots);
    // WHERE IT WAS DEFUSED, not where it is going. A defused brick becomes
    // claimable at `pack` in the same breath, so by the time this runs it has
    // already been given the packing queue's slot — and the sparkle would go
    // off there instead of at the defuse station.
    const was = before && before.at ? before.at : at;
    if (at.hidden) overflow[brick.node] = (overflow[brick.node] || 0) + 1;
    if (brick.place === "inbox" && brick.cooldown > 0) {
      nextCooldown = nextCooldown === null ? brick.cooldown : Math.min(nextCooldown, brick.cooldown);
    }
    const revealing = brick.tnt && brick.revealed && before && !before.revealed;
    // A BRICK BEING DEFUSED STAYS PUT while it happens. It becomes claimable
    // at `pack` the instant `defuse` concludes, so moving it first would play
    // the whole reveal at the packing queue — which is not where it was
    // defused.
    if (!revealing) Lab.place(g, at);
    g.style.opacity = at.hidden ? "0" : "";
    g.classList.toggle("working", brick.place === "station");
    g.classList.toggle("selected", brick.id === selected);
    g.classList.remove("returnable");

    if (revealing) {
      Lab.paint(g, brick, true, colours, brick.crate === "salvage");
      g.classList.add("revealing");
      Lab.sparkle(effects, was.x, was.y);
      setTimeout(() => {
        g.classList.remove("revealing");
        Lab.place(g, at);          // …and only then does it move on
      }, 900);
    } else {
      Lab.paint(g, brick, brick.revealed, colours, brick.crate === "salvage");
    }
    if (brick.place === "boom" && !g.classList.contains("wasted")) {
      g.classList.add("wasted");
      Lab.puff(effects, at.x, at.y);
    }
    if (brick.place !== "boom") g.classList.remove("wasted");
    seen.set(brick.id, { place: brick.place, revealed: brick.revealed, at });
  }

  for (const g of [...layer.children]) {
    const id = Number(g.dataset.id);
    if (g.dataset.kept) {
      g.classList.toggle("returnable", returnable.includes(id));
      continue;
    }
    if (alive.has(id)) continue;
    const last = seen.get(id);
    seen.delete(id);
    g.classList.remove("working", "selected");
    const colour = colours.find((c) => g.classList.contains(c));
    if (last && last.place === "shipped" && colour) keep(bins[colour].kept, g, KEPT_PER_BIN);
    else if (last && last.place === "boom") keep(waste.kept, g, KEPT_IN_WASTE);
    else g.remove();
  }

  const body = $("counts").querySelector("tbody");
  body.replaceChildren();
  for (const [name, counts] of Object.entries(state.counts)) {
    const row = document.createElement("tr");
    const cells = [name, counts.waiting ?? "", counts.running, counts.scheduled, counts.done, counts.failed,
                   counts.omitted];
    cells.forEach((value, i) => {
      const td = document.createElement("td");
      td.textContent = value;
      if (i > 0 && (value === 0 || value === "")) td.className = "zero";
      row.appendChild(td);
    });
    body.appendChild(row);
    const marks = [["⧖", counts.waiting], ["▶", counts.running], ["⏳", counts.scheduled], ["✓", counts.done],
                   ["✗", counts.failed]].filter(([, n]) => n).map(([m, n]) => `${m}${n}`).join(" ");
    const node = nodes[name];
    if (!node) continue;
    node.count.textContent = marks;
    if (node.overflow) {
      const extra = overflow[name] ? `+${overflow[name]} waiting` : "";
      const next = name === "inbox" && nextCooldown !== null ? `next in ${nextCooldown}s` : "";
      node.overflow.textContent = [extra, next].filter(Boolean).join(" · ");
    }
  }

  $("calls").innerHTML = state.calls.slice().reverse().map((line) => {
    const html = escapeHtml(line)
      .replace(/(journal\.\w+)/, '<span class="fn">$1</span>')
      .replace(/(#.*)$/, '<span class="note">$1</span>');
    return `<li>${html}</li>`;
  }).join("");

  if (selected !== null) showDetail();
}

function select(id) {
  selected = id;
  showDetail();
}

function sendBack(id) {
  if (!world || !world.send_back(id)) return;
  unkeep(id);
  render(JSON.parse(world.tick(0)));
}

function showDetail() {
  const box = $("detail");
  const data = JSON.parse(world.detail(selected));
  if (!data) {
    box.innerHTML = `<p class="hint">Brick ${selected} has left the line.</p>`;
    return;
  }
  const progress = Object.entries(data.progress).map(([n, s]) => `${n}: ${s}`).join(", ") || "waiting in the inbox";
  const history = data.history.map((e) =>
    `<li>${escapeHtml(e.archived_at.slice(11, 19))} ${escapeHtml(e.node)} · ${escapeHtml(e.status)} · ${escapeHtml(e.reason)}` +
    `${e.lease && e.reason === "lane" ? ` · ${escapeHtml(e.lease)}` : ""}</li>`).join("");
  const colour = data.colour ? escapeHtml(data.colour) : "hidden until defused";
  box.innerHTML = `
    <dl>
      <dt>Brick</dt><dd>${data.id}${data.tnt ? " · TNT" : ""} · ${colour} · v${data.version}</dd>
      <dt>Crate</dt><dd>${escapeHtml(data.crate)}</dd>
      <dt>Journal</dt><dd>${escapeHtml(progress)}</dd>
    </dl>
    ${history ? `<ol>${history}</ol>` : '<p class="hint">Nothing archived yet.</p>'}`;
}

// -- wiring ---------------------------------------------------------------------

function step() {
  if (!world || !playing) return;
  const speed = Number($("speed").value);
  render(JSON.parse(world.tick((TICK_MS / 1000) * speed)));
}

function bindControls(pyodide) {
  $("play").addEventListener("click", () => {
    playing = !playing;
    $("play").textContent = playing ? "Pause" : "Play";
    $("play").setAttribute("aria-pressed", String(playing));
  });
  $("add").addEventListener("click", () => world.add_bricks(10));
  $("add-tnt").addEventListener("click", () => world.add_bricks(1, true));
  $("squad").addEventListener("click", () => world.call_squad());
  $("send-back").addEventListener("click", () => {
    if (returnable.length) sendBack(returnable[Math.floor(Math.random() * returnable.length)]);
  });
  const sliders = [["arrivals", "arrivals_per_minute", 1], ["tnt", "tnt_share", 100],
                   ["failure", "defuse_failure", 100], ["returns", "returns_share", 100],
                   ["salvage", "salvage_share", 100]];
  for (const [id, setting, scale] of sliders) {
    $(id).addEventListener("input", () => {
      $(`${id}-out`).textContent = $(id).value;
      const values = pyodide.toPy({ [setting]: Number($(id).value) / scale });
      world.configure(values);
      values.destroy();
    });
  }
  $("auto-squad").addEventListener("change", () => {
    const values = pyodide.toPy({ squad_auto: $("auto-squad").checked });
    world.configure(values);
    values.destroy();
  });
}

async function main() {
  const status = $("status");
  Lab.highlightAll();
  Lab.showSource($("graph-source"), "scenario.py", "graph",
                 "from quazardous.grampy import Document, Graph, Lane, Node\n" +
                 "from quazardous.grampy.timing import Retry\n\n");
  Lab.showSource($("adapter-source"), "scenario.py", "adapter",
                 "from quazardous.grampy.items import Adapter\n\n");
  try {
    const pyodide = await loadPyodide();
    status.textContent = "Loading grampy…";
    const archive = await fetch("dist/grampy-demo.zip");
    if (!archive.ok) throw new Error("dist/grampy-demo.zip is missing — run `python docs/demo/build.py` first.");
    pyodide.unpackArchive(await archive.arrayBuffer(), "zip", { extractDir: "/home/pyodide/demo" });
    pyodide.runPython("import sys; sys.path.insert(0, '/home/pyodide/demo')");
    const scenario = pyodide.pyimport("scenario");
    const fixed = JSON.parse(scenario.static());
    colours = fixed.colours;
    drawBench(fixed.graph, fixed.layout, colours);
    world = scenario.World(7);
    world.add_bricks(8);
    world.add_bricks(2, true);
    bindControls(pyodide);
    status.textContent = "Running the real grampy journal in your browser (Pyodide). Click a brick to read its journal; click a sorted one to send it back.";
    render(JSON.parse(world.tick(0.5)));
    setInterval(step, TICK_MS);
  } catch (error) {
    status.textContent = `The line could not start: ${error.message}`;
    throw error;
  }
}

main();
