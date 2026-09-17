"use strict";

// The page draws; grampy decides. Every half second the Python world advances
// (docs/demo/scenario.py, running the real journal in Pyodide) and returns,
// for each brick, where the journal says it stands. Nothing here moves a
// brick anywhere the journal did not put it.

const SVG = "http://www.w3.org/2000/svg";
const STATION = { w: 176, h: 96 };
const BRICK = { w: 20, h: 12 };
const TICK_MS = 500;

const $ = (id) => document.getElementById(id);
const floor = $("floor");

let world = null;
let playing = true;
let selected = null;
const seen = new Map();          // brick id -> last place
const nodes = {};                // node name -> {x, y, spec}

function el(name, attrs = {}, parent = null) {
  const node = document.createElementNS(SVG, name);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
  if (parent) parent.appendChild(node);
  return node;
}

function escapeHtml(text) {
  return text.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

// -- the floor, drawn once from the graph document -----------------------------

function drawFloor(graph, layout) {
  floor.replaceChildren();
  const defs = el("defs", {}, floor);
  const pattern = el("pattern", { id: "hazard", width: 12, height: 12, patternUnits: "userSpaceOnUse",
                                  patternTransform: "rotate(45)" }, defs);
  el("rect", { width: 12, height: 12, fill: "var(--hazard)" }, pattern);
  el("rect", { width: 6, height: 12, fill: "var(--hazard-ink)" }, pattern);

  for (const [name, spec] of Object.entries(graph.nodes)) {
    const [layer, row] = layout[name];
    nodes[name] = { x: 60 + layer * 290, y: 44 + row * 190, spec };
  }

  const belts = el("g", {}, floor);
  for (const [name, spec] of Object.entries(graph.nodes)) {
    for (const parent of spec.parents || []) drawBelt(belts, parent, name, spec);
  }
  drawReturnBelt(belts, "defuse");
  drawExit(belts, "pack");

  const stations = el("g", {}, floor);
  for (const [name, node] of Object.entries(nodes)) drawStation(stations, name, node);

  el("g", { id: "bricks" }, floor);
  el("g", { id: "effects" }, floor);
}

function beltPath(from, to) {
  const x1 = from.x + STATION.w, y1 = from.y + STATION.h / 2;
  const x2 = to.x, y2 = to.y + STATION.h / 2;
  if (x2 - x1 > 300) {
    // A belt skipping a layer runs under the stations in between, not through them.
    const low = Math.max(from.y, to.y) + STATION.h + 18;
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

function drawReturnBelt(parent, name) {
  const n = nodes[name];
  const top = n.y - 34;
  const d = `M${n.x + STATION.w - 20},${n.y} L${n.x + STATION.w - 20},${top} L${n.x + 20},${top} L${n.x + 20},${n.y}`;
  el("path", { d, class: "belt belt-return" }, parent);
  el("path", { d, class: "belt-marks" }, parent);
  const label = el("text", { x: n.x + STATION.w / 2, y: top - 14, "text-anchor": "middle", class: "return-label" }, parent);
  label.textContent = "retry belt · 8s → 16s → 32s";
}

function drawExit(parent, name) {
  const n = nodes[name];
  const d = `M${n.x + STATION.w},${n.y + STATION.h / 2} L${n.x + STATION.w + 70},${n.y + STATION.h / 2}`;
  el("path", { d, class: "belt" }, parent);
  el("path", { d, class: "belt-marks" }, parent);
  const label = el("text", { x: n.x + STATION.w + 35, y: n.y + STATION.h / 2 + 28,
                             "text-anchor": "middle", class: "exit-label" }, parent);
  label.textContent = "shipped";
}

function drawStation(parent, name, node) {
  const { spec } = node;
  const kinds = ["station", name];
  if (spec.choice) kinds.push("choice");
  const g = el("g", { class: kinds.join(" "), transform: `translate(${node.x},${node.y})` }, parent);
  el("rect", { class: "body", width: STATION.w, height: STATION.h, rx: spec.choice ? 2 : 10 }, g);
  if (spec.wait) el("rect", { class: "shelf", x: 8, y: STATION.h - 22, width: STATION.w - 16, height: 8, rx: 2 }, g);
  if (spec.choice) el("rect", { class: "stripe", width: STATION.w, height: 7 }, g);
  const title = el("text", { x: 12, y: 30, class: "name" }, g);
  title.textContent = name;
  const role = el("text", { x: 12, y: 47, class: "role" }, g);
  role.textContent = describe(spec);
  node.count = el("text", { x: STATION.w - 10, y: 30, "text-anchor": "end", class: "count" }, g);
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

function target(brick, slots) {
  const node = nodes[brick.node];
  const key = `${brick.place}:${brick.node}`;
  const i = slots.get(key) || 0;
  slots.set(key, i + 1);
  switch (brick.place) {
    case "queue": {
      const column = i % 3, row = Math.floor(i / 3);
      return { x: node.x - 26 - column * 24, y: node.y + 24 + row * 16, hidden: row > 3 };
    }
    case "station":
      return { x: node.x + 14 + i * 26, y: node.y + 60 };
    case "shelf":
      return { x: node.x + 14 + (i % 7) * 22, y: node.y + STATION.h - 36 - Math.floor(i / 7) * 14, hidden: i >= 21 };
    case "retry":
      return { x: node.x + 30 + i * 26, y: node.y - 41 };
    case "shipped": {
      const pack = nodes.pack;
      return { x: pack.x + STATION.w + 80, y: pack.y + STATION.h / 2 - 6 };
    }
    case "boom": {
      const reject = nodes.reject;
      return { x: reject.x + STATION.w / 2 - 10, y: reject.y + 62 };
    }
    default:
      return { x: 10, y: 10 };
  }
}

function brickShape(brick) {
  const g = el("g", { class: `brick ${brick.tnt ? "tnt" : brick.colour}`, tabindex: "0",
                      role: "button", "aria-label": `Brick ${brick.id}${brick.tnt ? ", TNT" : ""}` });
  el("rect", { width: BRICK.w, height: BRICK.h, rx: 2 }, g);
  el("circle", { cx: 5.5, cy: -1, r: 2.6 }, g);
  el("circle", { cx: 14.5, cy: -1, r: 2.6 }, g);
  if (brick.tnt) {
    el("rect", { class: "band", y: 4, width: BRICK.w, height: 4 }, g);
    const t = el("text", { x: 10, y: 10.5, "text-anchor": "middle" }, g);
    t.textContent = "TNT";
  }
  const open = () => select(brick.id);
  g.addEventListener("click", open);
  g.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); open(); } });
  return g;
}

function explode(x, y) {
  const g = el("g", { class: "boom", transform: `translate(${x + 10},${y + 6})` }, $("effects"));
  const inner = el("g", {}, g);
  el("circle", { class: "core", r: 12 }, inner);
  for (let k = 0; k < 8; k++) {
    const a = (Math.PI * 2 * k) / 8;
    el("circle", { cx: Math.cos(a) * 16, cy: Math.sin(a) * 16, r: 4 }, inner);
  }
  setTimeout(() => g.remove(), 1000);
}

// -- one frame ------------------------------------------------------------------

function render(state) {
  $("clock").textContent = state.clock;
  $("shipped").textContent = state.shipped;
  $("exploded").textContent = state.exploded;

  const layer = $("bricks");
  const alive = new Set();
  const slots = new Map();
  for (const brick of state.bricks) {
    alive.add(brick.id);
    let g = layer.querySelector(`[data-id="${brick.id}"]`);
    if (!g) {
      g = brickShape(brick);
      g.dataset.id = brick.id;
      g.style.transform = `translate(0px, ${nodes.scan.y + 30}px)`;
      layer.appendChild(g);
      g.getBoundingClientRect();
    }
    const at = target(brick, slots);
    g.style.transform = `translate(${at.x}px, ${at.y}px)`;
    g.style.opacity = at.hidden ? "0" : "";
    g.classList.toggle("working", brick.place === "station");
    g.classList.toggle("selected", brick.id === selected);
    const before = seen.get(brick.id);
    if (brick.place === "boom" && before !== "boom") {
      setTimeout(() => { explode(at.x, at.y); g.style.opacity = "0"; }, 650);
    }
    if (brick.place === "shipped" && before !== "shipped") {
      setTimeout(() => g.classList.add("shipped"), 700);
    }
    seen.set(brick.id, brick.place);
  }
  for (const g of [...layer.children]) {
    if (!alive.has(Number(g.dataset.id))) { seen.delete(Number(g.dataset.id)); g.remove(); }
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
    if (nodes[name]) nodes[name].count.textContent = marks;
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
  box.innerHTML = `
    <dl>
      <dt>Brick</dt><dd>${data.id}${data.tnt ? " · TNT" : ` · ${escapeHtml(data.colour)}`}</dd>
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
    drawFloor(fixed.graph, fixed.layout);
    world = scenario.World(7);
    world.add_bricks(6);
    world.add_bricks(1, true);
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
