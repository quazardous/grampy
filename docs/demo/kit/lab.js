"use strict";

// grampy demo kit — what every demo draws the same way: SVG helpers, trays,
// bricks, belts, effects, and the Python snippets. A demo draws its own bench
// with these and keeps its own logic; the look lives in lab.css.
//
//   Lab.defs(svg)                       patterns and filters the kit uses
//   Lab.tray(parent, x, y, w, h, kinds) a white tray; returns its group
//   Lab.belt(parent, d, {failing})      a conveyor along an SVG path
//   Lab.brick({onClick})                a brick seen from above
//   Lab.paint(g, brick, revealed, colours)
//   Lab.sparkle(layer, x, y), Lab.puff(layer, x, y)
//   Lab.highlight(code)                 a Python snippet as HTML
//   Lab.showSource(box, url, block, header)
//                                       a block of a source file between
//                                       `# --8<-- [start:block]` markers

(function () {
  const SVG = "http://www.w3.org/2000/svg";
  const BRICK = { w: 24, h: 14 };

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

  // A stable pseudo-random number in [0, 1) for an id: the same spot each frame.
  function scatter(id, salt) {
    const x = Math.sin(id * 12.9898 + salt * 78.233) * 43758.5453;
    return x - Math.floor(x);
  }

  function defs(svg) {
    const d = el("defs", {}, svg);
    const pattern = el("pattern", { id: "lab-hazard", width: 10, height: 10, patternUnits: "userSpaceOnUse",
                                    patternTransform: "rotate(45)" }, d);
    el("rect", { width: 10, height: 10, fill: "var(--hazard)" }, pattern);
    el("rect", { width: 5, height: 10, fill: "var(--hazard-ink)" }, pattern);
    const lift = el("filter", { id: "lab-lift", x: "-10%", y: "-10%", width: "130%", height: "140%" }, d);
    el("feDropShadow", { dx: 0, dy: 2, stdDeviation: 3, "flood-color": "var(--shadow)", "flood-opacity": 1 }, lift);
    return d;
  }

  function tray(parent, x, y, w, h, kinds = "") {
    const g = el("g", { class: `tray ${kinds}`.trim(), transform: `translate(${x},${y})` }, parent);
    el("rect", { class: "tray-rim", width: w, height: h, rx: 10, filter: "url(#lab-lift)" }, g);
    el("rect", { class: "tray-floor", x: 5, y: 5, width: w - 10, height: h - 10, rx: 7 }, g);
    return g;
  }

  function belt(parent, d, { failing = false } = {}) {
    el("path", { d, class: `belt${failing ? " belt-fail" : ""}` }, parent);
    el("path", { d, class: "belt-marks" }, parent);
  }

  function brick({ onClick } = {}) {
    const g = el("g", { class: "brick", tabindex: "0", role: "button" });
    const body = el("g", { class: "body" }, g);
    el("rect", { class: "plate", width: BRICK.w, height: BRICK.h, rx: 2 }, body);
    for (const cy of [4.2, 9.8]) {
      for (const cx of [3.6, 9.2, 14.8, 20.4]) el("circle", { class: "stud", cx, cy, r: 2.1 }, body);
    }
    el("rect", { class: "band", x: 3, y: 3.5, width: BRICK.w - 6, height: 7, rx: 1 }, body);
    text(body, { class: "stamp", x: BRICK.w / 2, y: 9.3, "text-anchor": "middle" }, "TNT");
    const badge = el("g", { class: "badge", transform: "translate(13,-9)" }, g);
    el("rect", { width: 20, height: 10, rx: 5 }, badge);
    text(badge, { x: 10, y: 7.6, "text-anchor": "middle" }, "");
    el("circle", { class: "mark", cx: 2.6, cy: -2.4, r: 2.4 }, g);
    const debris = el("g", { class: "debris" }, g);
    el("path", { d: "M1,9 L6,3 L11,6 L9,12 L3,13 Z" }, debris);
    el("path", { d: "M12,4 L19,2 L23,8 L17,12 Z" }, debris);
    el("path", { class: "ember", d: "M8,11 L14,9 L16,14 L10,15 Z" }, debris);
    if (onClick) {
      g.addEventListener("click", onClick);
      g.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onClick(e); }
      });
    }
    return g;
  }

  // `marked` shows where a subject comes from: a small dot on its corner.
  function paint(g, brick, revealed, colours, marked = false) {
    g.classList.toggle("marked", Boolean(marked));
    g.classList.toggle("tnt", Boolean(brick.tnt));
    g.classList.toggle("unknown", !revealed);
    for (const colour of colours) g.classList.toggle(colour, revealed && brick.colour === colour);
    const version = brick.version || 1;
    g.classList.toggle("versioned", version > 1);
    g.querySelector(".badge text").textContent = `v${version}`;
    const what = revealed ? brick.colour : "colour hidden";
    g.setAttribute("aria-label", `Brick ${brick.id}, ${what}${brick.tnt ? ", TNT" : ""}` +
                   (version > 1 ? `, version ${version}` : ""));
  }

  function place(g, at) {
    g.style.transform = `translate(${at.x}px, ${at.y}px)`;
    const body = g.querySelector(".body");
    if (at.turn) body.setAttribute("transform", `rotate(90 ${BRICK.w / 2} ${BRICK.h / 2})`);
    else body.removeAttribute("transform");
  }

  function sparkle(layer, x, y) {
    const g = el("g", { class: "sparkle", transform: `translate(${x + BRICK.w / 2},${y + BRICK.h / 2})` }, layer);
    el("circle", { class: "halo", r: 22 }, g);
    const star = "M0,-8 L1.8,-1.8 L8,0 L1.8,1.8 L0,8 L-1.8,1.8 L-8,0 L-1.8,-1.8 Z";
    for (let k = 0; k < 12; k++) {
      const holder = el("g", { transform: `rotate(${(360 * k) / 12 + 14})` }, g);
      const spark = el("path", { d: star, class: `spark s${k % 3}` }, holder);
      spark.style.setProperty("--reach", `${24 + (k % 3) * 10}px`);
      spark.style.animationDelay = `${k * 30}ms`;
    }
    setTimeout(() => g.remove(), 1400);
  }

  function puff(layer, x, y) {
    const g = el("g", { class: "puff", transform: `translate(${x + BRICK.w / 2},${y + BRICK.h / 2})` }, layer);
    el("circle", { class: "flash", r: 11 }, g);
    for (let k = 0; k < 6; k++) {
      const a = (Math.PI * 2 * k) / 6;
      const smoke = el("circle", { class: "smoke", cx: Math.cos(a) * 9, cy: Math.sin(a) * 9, r: 6 }, g);
      smoke.style.animationDelay = `${k * 40}ms`;
    }
    setTimeout(() => g.remove(), 1300);
  }

  function highlight(code) {
    const pattern = /(#[^\n]*)|("(?:[^"\\]|\\.)*")|\b(from|import|for|in|None|True|False)\b|\b([A-Z][A-Za-z]+)\b/g;
    let out = "", last = 0;
    for (const m of code.matchAll(pattern)) {
      out += escapeHtml(code.slice(last, m.index));
      const kind = m[1] ? "com" : m[2] ? "str" : m[3] ? "kw" : "cls";
      out += `<span class="${kind}">${escapeHtml(m[0])}</span>`;
      last = m.index + m[0].length;
    }
    return out + escapeHtml(code.slice(last));
  }

  async function showSource(box, url, block, header = "") {
    try {
      const response = await fetch(url);
      if (!response.ok) throw new Error(response.statusText);
      const source = await response.text();
      const start = `# --8<-- [start:${block}]\n`, end = `# --8<-- [end:${block}]`;
      const from = source.indexOf(start), to = source.indexOf(end);
      if (from < 0 || to < from) throw new Error(`no ${block} block`);
      box.innerHTML = highlight(header + source.slice(from + start.length, to).trimEnd());
    } catch (error) {
      box.textContent = `${url} could not be read (${error.message}).`;
    }
  }

  function highlightAll(root = document) {
    for (const box of root.querySelectorAll("code.python[data-static]")) {
      box.innerHTML = highlight(box.textContent);
    }
  }

  window.Lab = { SVG, BRICK, el, text, escapeHtml, scatter, defs, tray, belt, brick, paint, place,
                 sparkle, puff, highlight, showSource, highlightAll };
})();
