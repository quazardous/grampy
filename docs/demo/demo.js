"use strict";

// The page draws; grampy decides. Every half second the Python world advances
// (docs/demo/scenario.py, running the real journal in Pyodide) and returns,
// for each brick, where the journal says it stands. Nothing here moves a
// brick anywhere the journal did not put it.
//
// The floor is a table seen from above: each station is a white tray holding
// the bricks it works on, waiting happens on the wood outside the trays, and
// the line ends in one tray per colour, plus a waste bin for the TNT the bomb
// squad could not save.

const SVG = "http://www.w3.org/2000/svg";
const STATION = { w: 176, h: 100 };
const BRICK = { w: 24, h: 14 };
const BIN = { x: 1116, y: 16, w: 84, h: 86, gap: 4 };
const WASTE = { x: 1116, y: 424, w: 172, h: 124 };
const QUEUE = { columns: 2, rows: 5 };
const KEPT_PER_BIN = 16;
const KEPT_IN_WASTE = 24;
const TICK_MS = 500;

const $ = (id) => document.getElementById(id);
const floor = $("floor");

let world = null;
let playing = true;
let selected = null;
let colours = [];
const seen = new Map();          // brick id -> {place, revealed}
const nodes = {};                // node name -> {x, y, spec, count, overflow}
const bins = {};                 // colour -> {x, y, count, kept}
const waste = { kept: [] };

function el(name, attrs = {}, parent = null) {
  const node = document.createElementNS(SVG, name);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
  if (parent) parent.appendChild(node);
  return node;
}

function text(parent, attrs, content) {
  const node = el("text", attrs, parent);
  node.textContent = content;
  return node;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

// A stable pseudo-random number in [0, 1) per brick, so a brick lands in the
// same spot of its tray on every frame.
function scatter(id, salt) {
  const x = Math.sin(id * 12.9898 + salt * 78.233) * 43758.5453;
  return x - Math.floor(x);
}

// -- the floor, drawn once from the graph document -----------------------------

function drawFloor(graph, layout, palette) {
  floor.replaceChildren();
  const defs = el("defs", {}, floor);
  const pattern = el("pattern", { id: "hazard", width: 10, height: 10, patternUnits: "userSpaceOnUse",
                                  patternTransform: "rotate(45)" }, defs);
  el("rect", { width: 10, height: 10, fill: "var(--hazard)" }, pattern);
  el("rect", { width: 5, height: 10, fill: "var(--hazard-ink)" }, pattern);
  const shadow = el("filter", { id: "lift", x: "-10%", y: "-10%", width: "130%", height: "140%" }, defs);
  el("feDropShadow", { dx: 0, dy: 3, stdDeviation: 3, "flood-color": "#3a2410", "flood-opacity": 0.3 }, shadow);

  for (const [name, spec] of Object.entries(graph.nodes)) {
    const [layer, row] = layout[name];
    nodes[name] = { x: 110 + layer * 270, y: 40 + row * 200, spec };
  }

  const pads = el("g", {}, floor);
  for (const node of Object.values(nodes)) drawQueuePad(pads, node);

  const belts = el("g", {}, floor);
  for (const [name, spec] of Object.entries(graph.nodes)) {
    for (const parent of spec.parents || []) drawBelt(belts, parent, name, spec);
  }
  drawReturnBelt(belts, "defuse");
  drawBeltTo(belts, nodes.pack, BIN.x - 4);
  drawBeltTo(belts, nodes.reject, WASTE.x - 4);

  const stations = el("g", {}, floor);
  for (const [name, node] of Object.entries(nodes)) drawStation(stations, name, node);
  drawBins(stations, palette);
  drawWaste(stations);

  el("g", { id: "bricks" }, floor);
  el("g", { id: "effects" }, floor);
}

function drawQueuePad(parent, node) {
  const w = QUEUE.columns * 28 + 8;
  el("rect", { class: "queue-pad", x: node.x - w - 8, y: node.y + 4, width: w, height: STATION.h - 8, rx: 6 }, parent);
  node.overflow = text(parent, { x: node.x - w / 2 - 8, y: node.y + STATION.h + 14,
                                 "text-anchor": "middle", class: "floor-label" }, "");
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

function drawBelt(parent, from, to, spec) {
  const failing = spec.on && spec.on[from] && spec.on[from].includes("failed");
  const d = beltPath(nodes[from], nodes[to]);
  el("path", { d, class: `belt${failing ? " belt-fail" : ""}` }, parent);
  el("path", { d, class: "belt-marks" }, parent);
}

function drawBeltTo(parent, node, x) {
  const y = node.y + STATION.h / 2;
  const d = `M${node.x + STATION.w},${y} L${x},${y}`;
  el("path", { d, class: "belt" }, parent);
  el("path", { d, class: "belt-marks" }, parent);
}

function drawReturnBelt(parent, name) {
  const n = nodes[name];
  const top = n.y - 36;
  const d = `M${n.x + STATION.w - 22},${n.y} L${n.x + STATION.w - 22},${top} L${n.x + 22},${top} L${n.x + 22},${n.y}`;
  el("path", { d, class: "belt belt-return" }, parent);
  el("path", { d, class: "belt-marks" }, parent);
  text(parent, { x: n.x + STATION.w / 2, y: top - 16, "text-anchor": "middle", class: "floor-label" },
       "retry belt · 8s → 16s → 32s");
}

function tray(parent, x, y, w, h, kinds) {
  const g = el("g", { class: `tray ${kinds}`, transform: `translate(${x},${y})` }, parent);
  el("rect", { class: "tray-rim", width: w, height: h, rx: 10, filter: "url(#lift)" }, g);
  el("rect", { class: "tray-floor", x: 6, y: 6, width: w - 12, height: h - 12, rx: 6 }, g);
  return g;
}

function drawStation(parent, name, node) {
  const { spec } = node;
  const kinds = ["station", name];
  if (spec.choice) kinds.push("choice");
  if (spec.wait) kinds.push("wait");
  const g = tray(parent, node.x, node.y, STATION.w, STATION.h, kinds.join(" "));
  if (spec.wait) el("rect", { class: "stripe", x: 6, y: 6, width: STATION.w - 12, height: 6 }, g);
  text(g, { x: 16, y: 31, class: "name" }, name);
  text(g, { x: 16, y: 46, class: "role" }, describe(spec));
  node.count = text(g, { x: STATION.w - 14, y: STATION.h - 14, "text-anchor": "end", class: "count" }, "");
}

function drawBins(parent, palette) {
  palette.forEach((colour, i) => {
    const x = BIN.x + (i % 2) * (BIN.w + BIN.gap);
    const y = BIN.y + Math.floor(i / 2) * (BIN.h + BIN.gap);
    const g = tray(parent, x, y, BIN.w, BIN.h, "bin");
    el("rect", { class: `tab ${colour}`, x: 11, y: BIN.h - 19, width: 16, height: 8, rx: 2 }, g);
    const count = text(g, { x: BIN.w - 12, y: BIN.h - 11, "text-anchor": "end", class: "count" }, "0");
    bins[colour] = { x, y, count, kept: [] };
  });
  text(parent, { x: BIN.x + BIN.w + BIN.gap / 2, y: BIN.y + 3 * (BIN.h + BIN.gap) + 14,
                 "text-anchor": "middle", class: "floor-label" }, "sorted by colour");
}

function drawWaste(parent) {
  const g = el("g", { class: "waste", transform: `translate(${WASTE.x},${WASTE.y})` }, parent);
  el("rect", { class: "waste-body", width: WASTE.w, height: WASTE.h, rx: 8, filter: "url(#lift)" }, g);
  el("rect", { class: "waste-floor", x: 8, y: 16, width: WASTE.w - 16, height: WASTE.h - 24, rx: 5 }, g);
  el("rect", { class: "stripe", x: 8, y: 5, width: WASTE.w - 16, height: 6, rx: 2 }, g);
  text(parent, { x: WASTE.x + WASTE.w / 2, y: WASTE.y + WASTE.h + 16, "text-anchor": "middle",
                 class: "floor-label" }, "waste · TNT past saving");
}

function describe(spec) {
  if (spec.choice) return "choice · colour or TNT";
  if (spec.wait) return `waits for squad · ⏱ ${spec.timeout}`;
  if (spec.retry) return `retry ×${spec.retry.limit} · exponential`;
  if (spec.need) return `on failed · need ${spec.need}/${spec.parents.length}`;
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
    case "queue": {
      const column = i % QUEUE.columns, row = Math.floor(i / QUEUE.columns);
      return { x: node.x - 38 - column * 28, y: node.y + 12 + row * 17, hidden: row >= QUEUE.rows };
    }
    case "station":
      return { x: node.x + 16 + i * 30, y: node.y + 64 };
    case "shelf":
      return { x: node.x + 16 + (i % 5) * 30, y: node.y + 56 + Math.floor(i / 5) * 18, hidden: i >= 10 };
    case "retry":
      return { x: node.x + 34 + i * 28, y: node.y - 43 };
    case "shipped":
      return inBin(brick);
    case "boom":
      return inWaste(brick.id);
    default:
      return { x: 10, y: 10 };
  }
}

function brickShape(brick) {
  const g = el("g", { class: "brick", tabindex: "0", role: "button" });
  const body = el("g", { class: "body" }, g);
  el("rect", { class: "plate", width: BRICK.w, height: BRICK.h, rx: 2 }, body);
  for (const cy of [4.2, 9.8]) {
    for (const cx of [3.6, 9.2, 14.8, 20.4]) el("circle", { class: "stud", cx, cy, r: 2.1 }, body);
  }
  el("rect", { class: "band", x: 3, y: 3.5, width: BRICK.w - 6, height: 7, rx: 1 }, body);
  text(body, { class: "stamp", x: BRICK.w / 2, y: 9.3, "text-anchor": "middle" }, "TNT");
  const debris = el("g", { class: "debris" }, g);
  el("path", { d: "M1,9 L6,3 L11,6 L9,12 L3,13 Z" }, debris);
  el("path", { d: "M12,4 L19,2 L23,8 L17,12 Z" }, debris);
  el("path", { class: "ember", d: "M8,11 L14,9 L16,14 L10,15 Z" }, debris);
  const open = () => select(brick.id);
  g.addEventListener("click", open);
  g.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); } });
  return g;
}

function paint(g, brick, revealed) {
  g.classList.toggle("tnt", brick.tnt);
  g.classList.toggle("unknown", !revealed);
  for (const colour of colours) g.classList.toggle(colour, revealed && brick.colour === colour);
  const what = revealed ? brick.colour : "colour hidden";
  g.setAttribute("aria-label", `Brick ${brick.id}, ${what}${brick.tnt ? ", TNT" : ""}`);
}

function place(g, at) {
  g.style.transform = `translate(${at.x}px, ${at.y}px)`;
  const body = g.querySelector(".body");
  if (at.turn) body.setAttribute("transform", `rotate(90 ${BRICK.w / 2} ${BRICK.h / 2})`);
  else body.removeAttribute("transform");
}

// -- effects ------------------------------------------------------------------------

function sparkle(x, y) {
  const g = el("g", { class: "sparkle", transform: `translate(${x + BRICK.w / 2},${y + BRICK.h / 2})` }, $("effects"));
  el("circle", { class: "halo", r: 22 }, g);
  const star = "M0,-8 L1.8,-1.8 L8,0 L1.8,1.8 L0,8 L-1.8,1.8 L-8,0 L-1.8,-1.8 Z";
  for (let k = 0; k < 12; k++) {
    const angle = (360 * k) / 12 + 14;
    const holder = el("g", { transform: `rotate(${angle})` }, g);
    const spark = el("path", { d: star, class: `spark s${k % 3}` }, holder);
    spark.style.setProperty("--reach", `${24 + (k % 3) * 10}px`);
    spark.style.animationDelay = `${k * 30}ms`;
  }
  setTimeout(() => g.remove(), 1400);
}

function puff(x, y) {
  const g = el("g", { class: "puff", transform: `translate(${x + BRICK.w / 2},${y + BRICK.h / 2})` }, $("effects"));
  el("circle", { class: "flash", r: 11 }, g);
  for (let k = 0; k < 6; k++) {
    const a = (Math.PI * 2 * k) / 6;
    const smoke = el("circle", { class: "smoke", cx: Math.cos(a) * 9, cy: Math.sin(a) * 9, r: 6 }, g);
    smoke.style.animationDelay = `${k * 40}ms`;
  }
  setTimeout(() => g.remove(), 1300);
}

// -- one frame ------------------------------------------------------------------

function keep(list, g, cap) {
  g.dataset.kept = "1";
  g.removeAttribute("tabindex");
  list.push(g);
  while (list.length > cap) list.shift().remove();
}

function render(state) {
  $("clock").textContent = state.clock;
  $("shipped").textContent = state.shipped;
  $("exploded").textContent = state.exploded;
  for (const [colour, count] of Object.entries(state.sorted)) {
    if (bins[colour]) bins[colour].count.textContent = count;
  }

  const layer = $("bricks");
  const alive = new Set();
  const slots = new Map();
  const overflow = {};
  for (const brick of state.bricks) {
    alive.add(brick.id);
    let g = layer.querySelector(`[data-id="${brick.id}"]:not([data-kept])`);
    const before = seen.get(brick.id);
    if (!g) {
      g = brickShape(brick);
      g.dataset.id = brick.id;
      g.style.transform = `translate(12px, ${nodes.scan.y + 40}px)`;
      layer.appendChild(g);
      g.getBoundingClientRect();
    }
    const at = target(brick, slots);
    if (at.hidden) overflow[brick.node] = (overflow[brick.node] || 0) + 1;
    place(g, at);
    g.style.opacity = at.hidden ? "0" : "";
    g.classList.toggle("working", brick.place === "station");
    g.classList.toggle("selected", brick.id === selected);

    if (brick.tnt && brick.revealed && before && !before.revealed) {
      // Defused: the brick shows its true colour once it has arrived.
      setTimeout(() => {
        paint(g, brick, true);
        g.classList.add("revealing");
        sparkle(at.x, at.y);
      }, 450);
      setTimeout(() => g.classList.remove("revealing"), 1600);
    } else {
      paint(g, brick, brick.revealed);
    }
    if (brick.place === "boom" && !g.classList.contains("wasted")) {
      // Past saving: a puff of smoke, and what lands in the bin is rubble.
      g.classList.add("wasted");
      puff(at.x, at.y);
    }
    seen.set(brick.id, { place: brick.place, revealed: brick.revealed });
  }

  for (const g of [...layer.children]) {
    if (g.dataset.kept) continue;
    const id = Number(g.dataset.id);
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
    const cells = [name, counts.running, counts.scheduled, counts.done, counts.failed, counts.omitted];
    cells.forEach((value, i) => {
      const td = document.createElement("td");
      td.textContent = value;
      if (i > 0 && value === 0) td.className = "zero";
      row.appendChild(td);
    });
    body.appendChild(row);
    const marks = [["▶", counts.running], ["⏳", counts.scheduled], ["✓", counts.done], ["✗", counts.failed]]
      .filter(([, n]) => n).map(([m, n]) => `${m}${n}`).join(" ");
    if (nodes[name]) {
      nodes[name].count.textContent = marks;
      nodes[name].overflow.textContent = overflow[name] ? `+${overflow[name]} waiting` : "";
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

function showDetail() {
  const box = $("detail");
  const data = JSON.parse(world.detail(selected));
  if (!data) {
    box.innerHTML = `<p class="hint">Brick ${selected} has left the line.</p>`;
    return;
  }
  const progress = Object.entries(data.progress).map(([n, s]) => `${n}: ${s}`).join(", ") || "not claimed yet";
  const history = data.history.map((e) =>
    `<li>${escapeHtml(e.archived_at.slice(11, 19))} ${escapeHtml(e.node)} · ${escapeHtml(e.status)} · ${escapeHtml(e.reason)}</li>`).join("");
  const colour = data.colour ? escapeHtml(data.colour) : "hidden until defused";
  box.innerHTML = `
    <dl>
      <dt>Brick</dt><dd>${data.id}${data.tnt ? " · TNT" : ""} · ${colour}</dd>
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
  const sliders = [["arrivals", "arrivals_per_minute", 1], ["tnt", "tnt_share", 100], ["failure", "defuse_failure", 100]];
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
    drawFloor(fixed.graph, fixed.layout, colours);
    world = scenario.World(7);
    world.add_bricks(8);
    world.add_bricks(2, true);
    bindControls(pyodide);
    status.textContent = "Running the real grampy journal in your browser (Pyodide). Click a brick to read its journal.";
    render(JSON.parse(world.tick(0.5)));
    setInterval(step, TICK_MS);
  } catch (error) {
    status.textContent = `The factory could not start: ${error.message}`;
    throw error;
  }
}

main();
