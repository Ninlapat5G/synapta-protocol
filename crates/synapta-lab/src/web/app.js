// Synapta Lab page. Plain JS, no libraries. Sections, top to bottom:
//   state · server calls · map layout · map drawing · map input (drag, pan, zoom, cables)
//   · packets (animation) · trace log + replay · side panel (node, call, scenarios, network)
//   · polling + start-up
"use strict";

// ---------------------------------------------------------------- state

const S = {
  nodes: new Map(),     // id -> NodeView from /api/nodes
  pos: new Map(),       // id -> {x, y}: where nodes are drawn
  ringPos: new Map(),   // ring name -> {x, y}
  catalog: [],          // [{name, description, input_schema}]
  selected: null,       // node id
  tool: "select",       // select | cable | cut
  view: { x: 0, y: 0, k: 1 },
  showLinks: false,
  // trace
  events: [],           // every Sent / link event received, oldest first (capped)
  lastSeq: 0,
  requests: new Map(),  // request key -> method, to name the responses
  mode: "live",         // live | replay
  cursor: 0,            // replay: index into visibleEvents()
  playing: null,        // replay timer
  speed: 1,
  jobs: [],             // jobs submitted from the Call tab
  eventsPerSecond: 0,
};

const MAX_EVENTS = 50000;   // kept in the page for replay
const MAX_PACKETS = 400;    // drawn at once; more are skipped (still in the log)
const TABLE_ROWS = 300;
const NODE_W = 50, NODE_H = 32, HUB_R = 26;
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const svgNS = "http://www.w3.org/2000/svg";

function el(tag, attrs = {}, parent) {
  const e = document.createElementNS(svgNS, tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (parent) parent.appendChild(e);
  return e;
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function toast(message, bad = false) {
  const t = $("#toast");
  t.textContent = message;
  t.className = "toast show" + (bad ? " bad" : "");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => (t.className = "toast"), bad ? 5000 : 2200);
}

/** A button that needs a second click within 3 s (instead of a browser confirm box). */
function confirmClick(button, action) {
  button.addEventListener("click", () => {
    if (button.dataset.armed) {
      delete button.dataset.armed;
      button.textContent = button.dataset.label;
      action();
      return;
    }
    button.dataset.label = button.textContent;
    button.dataset.armed = "1";
    button.textContent = "Sure?";
    setTimeout(() => {
      if (button.dataset.armed) {
        delete button.dataset.armed;
        button.textContent = button.dataset.label;
      }
    }, 3000);
  });
}

// ---------------------------------------------------------------- server calls

async function api(method, path, body) {
  const options = { method, headers: {} };
  if (method !== "GET") {
    // The lab only accepts changes sent as JSON (see http.rs).
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(body ?? null);
  }
  const response = await fetch(path, options);
  const text = await response.text();
  let data;
  try { data = JSON.parse(text); } catch { data = text; }
  if (!response.ok) {
    const error = data && data.error;
    throw new Error((error && (error.message || error)) || response.statusText);
  }
  return data;
}

async function act(promise, done) {
  try {
    const result = await promise;
    if (done) toast(done);
    await refreshNodes();
    return result;
  } catch (e) {
    toast(e.message, true);
    return undefined;
  }
}

// ---------------------------------------------------------------- map layout

function allRings() {
  const rings = new Set(S.ringPos.keys());
  for (const n of S.nodes.values()) n.setup.rings.forEach(r => rings.add(r));
  return [...rings].sort(naturally);
}

function naturally(a, b) {
  return a.localeCompare(b, undefined, { numeric: true });
}

function ringMembers(ring) {
  return [...S.nodes.values()].filter(n => n.setup.rings.includes(ring)).map(n => n.setup.id).sort(naturally);
}

function circleRadius(members) {
  return Math.max(120, (members * 46) / (2 * Math.PI));
}

/** Where everything would go on a tidy map: rings in a grid, each ring's nodes around it,
 * nodes in several rings between those rings. Used for anything without a position yet,
 * or for everything when the owner asks to re-arrange. */
function tidySlots() {
  const rings = allRings();
  const members = new Map(rings.map(r => [r, ringMembers(r).filter(id => S.nodes.get(id).setup.rings.length === 1)]));
  const biggest = Math.max(1, ...[...members.values()].map(m => m.length));
  const spacing = 2 * circleRadius(biggest) + 180;
  const cols = Math.max(1, Math.ceil(Math.sqrt(rings.length)));
  const ringSlots = new Map();
  rings.forEach((r, i) => ringSlots.set(r, { x: (i % cols) * spacing, y: Math.floor(i / cols) * spacing }));

  const nodeSlots = new Map();
  for (const [ring, ids] of members) {
    const c = ringSlots.get(ring), radius = circleRadius(ids.length);
    ids.forEach((id, i) => {
      const a = (2 * Math.PI * i) / ids.length - Math.PI / 2;
      nodeSlots.set(id, { x: c.x + radius * Math.cos(a), y: c.y + radius * Math.sin(a) });
    });
  }
  // Bridges: halfway between their rings, spread out if several share the same rings.
  const shared = new Map();
  for (const n of S.nodes.values()) {
    const rs = n.setup.rings;
    if (rs.length < 2) continue;
    const key = [...rs].sort().join("|");
    const k = shared.get(key) || 0;
    shared.set(key, k + 1);
    const cx = rs.reduce((s, r) => s + ringSlots.get(r).x, 0) / rs.length;
    const cy = rs.reduce((s, r) => s + ringSlots.get(r).y, 0) / rs.length;
    nodeSlots.set(n.setup.id, { x: cx + k * 60, y: cy + 40 + k * 20 });
  }
  // Nodes in no ring: a row under everything.
  let loose = 0;
  const bottom = (Math.ceil(rings.length / cols) || 1) * spacing;
  for (const n of S.nodes.values()) {
    if (n.setup.rings.length === 0) nodeSlots.set(n.setup.id, { x: (loose++ % 20) * 70, y: bottom + Math.floor(loose / 20) * 70 });
  }
  return { ringSlots, nodeSlots };
}

function placeMissing() {
  let slots = null;
  const get = () => (slots ||= tidySlots());
  for (const r of allRings()) if (!S.ringPos.has(r)) S.ringPos.set(r, get().ringSlots.get(r));
  for (const id of S.nodes.keys()) if (!S.pos.has(id)) S.pos.set(id, get().nodeSlots.get(id) || viewCentre());
}

function arrangeAll() {
  const { ringSlots, nodeSlots } = tidySlots();
  S.ringPos = new Map(ringSlots);
  S.pos = new Map([...S.nodes.keys()].map(id => [id, nodeSlots.get(id)]));
  draw();
  fitView();
  saveLayout();
}

function layoutJson() {
  return { nodes: Object.fromEntries(S.pos), rings: Object.fromEntries(S.ringPos) };
}

function saveLayout() {
  clearTimeout(saveLayout.timer);
  saveLayout.timer = setTimeout(() => api("PUT", "/api/layout", layoutJson()).catch(() => {}), 400);
}

function applyLayout(layout) {
  S.pos = new Map(Object.entries((layout && layout.nodes) || {}));
  S.ringPos = new Map(Object.entries((layout && layout.rings) || {}));
}

// ---------------------------------------------------------------- map drawing

const svg = $("#map");
const layers = {
  links: $("#layer-links"), cables: $("#layer-cables"), rings: $("#layer-rings"),
  nodes: $("#layer-nodes"), packets: $("#layer-packets"),
};
const drawn = { nodes: new Map(), rings: new Map(), cables: new Map(), links: new Map() };
const GLYPH = { server: "SRV", desktop: "PC", mobile: "MOB", embedded: "IOT", edge: "EDGE", client: "CLI" };

function applyView() {
  const { x, y, k } = S.view;
  $("#viewport").setAttribute("transform", `translate(${x},${y}) scale(${k})`);
}

function viewCentre() {
  const r = svg.getBoundingClientRect();
  return { x: (r.width / 2 - S.view.x) / S.view.k, y: (r.height / 2 - S.view.y) / S.view.k };
}

function fitView() {
  const points = [...S.pos.values(), ...S.ringPos.values()];
  const r = svg.getBoundingClientRect();
  if (!points.length || !r.width) return;
  const xs = points.map(p => p.x), ys = points.map(p => p.y);
  const minX = Math.min(...xs) - 80, maxX = Math.max(...xs) + 80;
  const minY = Math.min(...ys) - 80, maxY = Math.max(...ys) + 80;
  const k = Math.min(2, Math.max(0.03, Math.min(r.width / (maxX - minX), r.height / (maxY - minY))));
  S.view = { k, x: r.width / 2 - ((minX + maxX) / 2) * k, y: r.height / 2 - ((minY + maxY) / 2) * k };
  applyView();
}

/** Brings the drawing in line with S: adds, updates and removes elements by key. */
function draw() {
  placeMissing();
  const rings = allRings();

  // Rings
  for (const ring of rings) {
    let g = drawn.rings.get(ring);
    if (!g) {
      g = el("g", { class: "ring-hub", "data-ring": ring }, layers.rings);
      el("circle", { r: HUB_R }, g);
      el("text", { y: 4 }, g).textContent = ring;
      el("text", { y: HUB_R + 14, class: "count" }, g);
      drawn.rings.set(ring, g);
    }
    const members = ringMembers(ring).length;
    g.lastChild.textContent = `${members} node${members === 1 ? "" : "s"}`;
  }
  removeMissing(drawn.rings, new Set(rings));

  // Nodes
  for (const [id, n] of S.nodes) {
    let g = drawn.nodes.get(id);
    if (!g) {
      g = el("g", { class: "node", "data-node": id }, layers.nodes);
      el("rect", { class: "body", x: -NODE_W / 2, y: -NODE_H / 2, width: NODE_W, height: NODE_H, rx: 7 }, g);
      el("text", { class: "glyph" }, g);
      el("text", { class: "label", y: NODE_H / 2 + 13 }, g).textContent = id;
      el("circle", { class: "badge", cx: NODE_W / 2, cy: -NODE_H / 2, r: 8, visibility: "hidden" }, g);
      el("text", { class: "badge-text", x: NODE_W / 2, y: -NODE_H / 2, visibility: "hidden" }, g);
      drawn.nodes.set(id, g);
    }
    const active = (n.resources && n.resources.active_jobs) || 0;
    g.setAttribute("class", `node ${n.running ? "on" : "off"}${active ? " busy" : ""}${S.selected === id ? " selected" : ""}`);
    g.children[1].textContent = GLYPH[n.setup.node_type] || "N";
    const [badge, badgeText] = [g.children[3], g.children[4]];
    badge.setAttribute("visibility", active ? "visible" : "hidden");
    badgeText.setAttribute("visibility", active ? "visible" : "hidden");
    badgeText.textContent = active;
  }
  removeMissing(drawn.nodes, new Set(S.nodes.keys()));

  // Cables: one per (node, ring). A wide invisible twin takes the clicks.
  const cableKeys = new Set();
  for (const n of S.nodes.values()) {
    for (const ring of n.setup.rings) {
      const key = `${n.setup.id}|${ring}`;
      cableKeys.add(key);
      if (!drawn.cables.has(key)) {
        const g = el("g", { "data-cable": key }, layers.cables);
        el("line", { class: "cable" }, g);
        el("line", { class: "cable hit" }, g);
        drawn.cables.set(key, g);
      }
    }
  }
  removeMissing(drawn.cables, cableKeys);

  // Live links (READY), only when asked for: a big ring has thousands.
  const linkKeys = new Set();
  if (S.showLinks) {
    for (const n of S.nodes.values()) {
      for (const peer of n.links) {
        if (!S.nodes.has(peer)) continue;
        const key = [n.setup.id, peer].sort().join("|");
        linkKeys.add(key);
        if (!drawn.links.has(key)) drawn.links.set(key, el("line", { class: "link" }, layers.links));
      }
    }
  }
  removeMissing(drawn.links, linkKeys);

  placeAll();
  $("#empty").hidden = S.nodes.size > 0 || rings.length > 0;
}

function removeMissing(map, keep) {
  for (const [key, e] of map) {
    if (!keep.has(key)) {
      e.remove();
      map.delete(key);
    }
  }
}

/** Moves every element to its position (after a drag or a layout change). */
function placeAll() {
  for (const [ring, g] of drawn.rings) {
    const p = S.ringPos.get(ring);
    if (p) g.setAttribute("transform", `translate(${p.x},${p.y})`);
  }
  for (const [id, g] of drawn.nodes) {
    const p = S.pos.get(id);
    if (p) g.setAttribute("transform", `translate(${p.x},${p.y})`);
  }
  for (const [key, g] of drawn.cables) {
    const [id, ring] = key.split("|");
    const a = S.pos.get(id), b = S.ringPos.get(ring);
    if (!a || !b) continue;
    for (const line of g.children) setLine(line, a, b);
  }
  for (const [key, line] of drawn.links) {
    const [x, y] = key.split("|");
    const a = S.pos.get(x), b = S.pos.get(y);
    if (a && b) setLine(line, a, b);
  }
}

function setLine(line, a, b) {
  line.setAttribute("x1", a.x); line.setAttribute("y1", a.y);
  line.setAttribute("x2", b.x); line.setAttribute("y2", b.y);
}

// ---------------------------------------------------------------- map input

const pointers = new Map();   // pointerId -> {x, y} screen
let gesture = null;           // what the current press is doing
let cableFrom = null;         // cable tool: {node} or {ring} picked first

function toWorld(clientX, clientY) {
  const r = svg.getBoundingClientRect();
  return { x: (clientX - r.left - S.view.x) / S.view.k, y: (clientY - r.top - S.view.y) / S.view.k };
}

function zoomAt(clientX, clientY, factor) {
  const r = svg.getBoundingClientRect();
  const sx = clientX - r.left, sy = clientY - r.top;
  const k = Math.min(4, Math.max(0.03, S.view.k * factor));
  S.view.x = sx - ((sx - S.view.x) * k) / S.view.k;
  S.view.y = sy - ((sy - S.view.y) * k) / S.view.k;
  S.view.k = k;
  applyView();
}

function setTool(tool) {
  S.tool = tool;
  cableFrom = null;
  $("#cable-preview").setAttribute("visibility", "hidden");
  $$(".tool").forEach(b => b.classList.toggle("active", b.dataset.tool === tool));
  svg.setAttribute("class", `tool-${tool}`);
  hint();
}

function hint(text) {
  const defaults = {
    select: "",
    cable: "Cable: click a node, then the ring to plug it into.",
    cut: "Cut: click a cable to unplug that node from the ring.",
  };
  $("#map-hint").textContent = text ?? defaults[S.tool];
}

svg.addEventListener("pointerdown", e => {
  svg.setPointerCapture(e.pointerId);
  pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
  if (pointers.size === 2) {
    // Two fingers: pinch to zoom, whatever the tool.
    const [a, b] = [...pointers.values()];
    gesture = { kind: "pinch", distance: Math.hypot(a.x - b.x, a.y - b.y) };
    return;
  }
  const nodeEl = e.target.closest("[data-node]");
  const ringEl = e.target.closest("[data-ring]");
  const cableEl = e.target.closest("[data-cable]");
  const start = { x: e.clientX, y: e.clientY };

  if (S.tool === "cut" && cableEl) {
    const [id, ring] = cableEl.dataset.cable.split("|");
    const n = S.nodes.get(id);
    act(api("PUT", `/api/nodes/${id}/rings`, n.setup.rings.filter(r => r !== ring)), `${id} unplugged from ${ring}`);
    return;
  }
  if (S.tool === "cable" && (nodeEl || ringEl)) {
    cablePick(nodeEl ? { node: nodeEl.dataset.node } : { ring: ringEl.dataset.ring }, e);
    return;
  }
  if (nodeEl) {
    gesture = { kind: "drag-node", id: nodeEl.dataset.node, start, moved: false };
  } else if (ringEl) {
    gesture = { kind: "drag-ring", ring: ringEl.dataset.ring, start, moved: false };
  } else {
    gesture = { kind: "pan", start, view: { ...S.view }, moved: false };
  }
});

svg.addEventListener("pointermove", e => {
  if (pointers.has(e.pointerId)) pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
  if (cableFrom) {
    const from = cableFrom.node ? S.pos.get(cableFrom.node) : S.ringPos.get(cableFrom.ring);
    const to = toWorld(e.clientX, e.clientY);
    const line = $("#cable-preview");
    setLine(line, from, to);
    line.setAttribute("visibility", "visible");
  }
  if (!gesture) return;
  if (gesture.kind === "pinch") {
    if (pointers.size < 2) return;
    const [a, b] = [...pointers.values()];
    const distance = Math.hypot(a.x - b.x, a.y - b.y);
    zoomAt((a.x + b.x) / 2, (a.y + b.y) / 2, distance / gesture.distance);
    gesture.distance = distance;
    return;
  }
  const dx = e.clientX - gesture.start.x, dy = e.clientY - gesture.start.y;
  if (Math.hypot(dx, dy) > 4) gesture.moved = true;
  if (!gesture.moved) return;
  if (gesture.kind === "pan") {
    S.view.x = gesture.view.x + dx;
    S.view.y = gesture.view.y + dy;
    applyView();
  } else if (gesture.kind === "drag-node") {
    S.pos.set(gesture.id, toWorld(e.clientX, e.clientY));
    placeAll();
  } else if (gesture.kind === "drag-ring") {
    S.ringPos.set(gesture.ring, toWorld(e.clientX, e.clientY));
    placeAll();
  }
});

function endPress(e) {
  pointers.delete(e.pointerId);
  if (!gesture) return;
  const g = gesture;
  if (g.kind === "pinch") {
    if (pointers.size === 0) gesture = null;
    return;
  }
  gesture = null;
  if ((g.kind === "drag-node" || g.kind === "drag-ring") && g.moved) saveLayout();
  if (g.kind === "drag-node" && !g.moved) select(g.id);
  if (g.kind === "pan" && !g.moved) select(null);
}
svg.addEventListener("pointerup", endPress);
svg.addEventListener("pointercancel", endPress);

svg.addEventListener("wheel", e => {
  e.preventDefault();
  zoomAt(e.clientX, e.clientY, Math.exp(-e.deltaY * 0.0015));
}, { passive: false });

/** Cable tool: the first click picks a node or ring, the second plugs them together. */
function cablePick(picked, e) {
  if (!cableFrom) {
    cableFrom = picked;
    hint(picked.node ? `Now click the ring to plug ${picked.node} into.` : `Now click the node to plug into ${picked.ring}.`);
    svg.dispatchEvent(new PointerEvent("pointermove", { clientX: e.clientX, clientY: e.clientY }));
    return;
  }
  const node = cableFrom.node || picked.node, ring = cableFrom.ring || picked.ring;
  cableFrom = null;
  $("#cable-preview").setAttribute("visibility", "hidden");
  hint();
  if (!node || !ring) {
    toast("A cable goes from a node to a ring.", true);
    return;
  }
  const n = S.nodes.get(node);
  if (n.setup.rings.includes(ring)) {
    toast(`${node} is already in ${ring}`);
    return;
  }
  act(api("PUT", `/api/nodes/${node}/rings`, [...n.setup.rings, ring]), `${node} plugged into ${ring}`);
}

document.addEventListener("keydown", e => {
  if (e.key === "Escape") setTool("select");
});
$$(".tool").forEach(b => b.addEventListener("click", () => setTool(b.dataset.tool)));
$("#zoom-in").addEventListener("click", () => { const r = svg.getBoundingClientRect(); zoomAt(r.left + r.width / 2, r.top + r.height / 2, 1.3); });
$("#zoom-out").addEventListener("click", () => { const r = svg.getBoundingClientRect(); zoomAt(r.left + r.width / 2, r.top + r.height / 2, 1 / 1.3); });
$("#zoom-fit").addEventListener("click", fitView);
// A rotated phone or resized window would leave the drawing off-screen.
let resizeTimer;
window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(fitView, 200);
});

/** The view centre, or the nearest point around it that nothing is drawn on yet. */
function freeSpot() {
  const c = viewCentre();
  const taken = [...S.pos.values(), ...S.ringPos.values()];
  for (let i = 0; i < 400; i++) {
    const a = i * 2.4, r = 30 * Math.sqrt(i);
    const p = { x: c.x + r * Math.cos(a), y: c.y + r * Math.sin(a) };
    if (taken.every(q => Math.hypot(q.x - p.x, q.y - p.y) > 75)) return p;
  }
  return c;
}

function freeId(prefix, taken) {
  for (let i = 1; ; i++) if (!taken.has(`${prefix}${i}`)) return `${prefix}${i}`;
}

$("#add-node").addEventListener("click", async () => {
  const id = freeId("node-", new Set(S.nodes.keys()));
  S.pos.set(id, freeSpot());
  const caps = ["math.add", "system.echo"].map(name => ({ name, allow: ["*"] }));
  const done = await act(api("POST", "/api/nodes", { id, capabilities: caps }), `${id} added — plug it into a ring with Cable`);
  if (done) {
    select(id);
    saveLayout();
  }
});

$("#add-ring").addEventListener("click", () => {
  const ring = freeId("ring-", new Set(allRings()));
  S.ringPos.set(ring, freeSpot());
  draw();
  saveLayout();
  setTool("cable");
  toast(`${ring} added — now plug nodes into it`);
});

$("#add-many").addEventListener("click", () => $("#many-dialog").showModal());
$("#many-dialog").addEventListener("close", async () => {
  if ($("#many-dialog").returnValue !== "add") return;
  const f = new FormData($("#many-form"));
  const count = Math.min(5000, Math.max(1, +f.get("count")));
  const prefix = String(f.get("prefix") || "n").toLowerCase();
  const rings = splitList(f.get("rings"));
  const capabilities = splitList(f.get("caps")).map(name => ({ name, allow: ["*"] }));
  const taken = new Set(S.nodes.keys());
  const setups = [];
  for (let i = 0; i < count; i++) {
    const id = freeId(`${prefix}-`, taken);
    taken.add(id);
    setups.push({ id, rings, capabilities });
  }
  toast(`Adding ${count} nodes…`);
  const result = await act(api("POST", "/api/nodes", setups));
  if (result) toast(result.failed.length ? `${result.failed.length} failed: ${result.failed[0].error}` : `${count} nodes added`, result.failed.length > 0);
});

function splitList(text) {
  return String(text || "").split(/[,\s]+/).map(s => s.trim()).filter(Boolean);
}

// ---------------------------------------------------------------- packets

const PACKET_COLOR = { ping: "var(--p-ping)", advert: "var(--p-advert)", work: "var(--p-work)", relay: "var(--p-relay)", ok: "var(--p-ok)", err: "var(--p-err)" };
const packets = new Set();

/** What a frame is about, for colour and filters: ping, advert, work, relay, or "" (other). */
function topic(e) {
  const m = e.method || "";
  if (m === "ping") return "ping";
  if (m === "adverts.sync" || m === "advert") return "advert";
  if (m === "relay") return "relay";
  if (m === "invoke" || m.startsWith("job")) return "work";
  return "";
}

function colourOf(e) {
  const f = e.frame && e.frame.frame;
  if (f === "error" || (f === "response" && e.frame.ok === false)) return PACKET_COLOR.err;
  if (f === "response" && topic(e) !== "ping" && topic(e) !== "advert") return PACKET_COLOR.ok;
  return PACKET_COLOR[topic(e)] || PACKET_COLOR.ping;
}

function launch(e, reverse = false) {
  if (e.kind !== "sent") {
    flashNode(e.node);
    return;
  }
  if (packets.size >= MAX_PACKETS) return;
  const from = S.pos.get(reverse ? e.peer : e.node), to = S.pos.get(reverse ? e.node : e.peer);
  if (!from || !to) return;
  const dot = el("circle", { class: "packet", r: 5, fill: colourOf(e), cx: from.x, cy: from.y }, layers.packets);
  packets.add({ dot, from, to, t0: performance.now(), duration: 700 / S.speed });
  if (packets.size === 1) requestAnimationFrame(animate);
}

function animate(now) {
  for (const p of packets) {
    const t = (now - p.t0) / p.duration;
    if (t >= 1) {
      p.dot.remove();
      packets.delete(p);
      continue;
    }
    const ease = t < 0.5 ? 2 * t * t : 1 - (2 - 2 * t) ** 2 / 2;
    p.dot.setAttribute("cx", p.from.x + (p.to.x - p.from.x) * ease);
    p.dot.setAttribute("cy", p.from.y + (p.to.y - p.from.y) * ease);
  }
  if (packets.size) requestAnimationFrame(animate);
}

function flashNode(id) {
  const g = drawn.nodes.get(id);
  if (!g) return;
  g.classList.add("flash");
  setTimeout(() => g.classList.remove("flash"), 400);
}

// ---------------------------------------------------------------- trace log + replay

/** Names the frame: a response carries no method, so it borrows its request's. */
function annotate(e) {
  const f = e.frame;
  if (!f) return e;
  const opposite = { outbound: "inbound", inbound: "outbound" };
  if (f.frame === "request" || f.frame === "notify" || f.frame === "event") {
    e.method = f.method;
    if (f.frame === "request") {
      S.requests.set(`${e.node}>${e.peer}:${e.channel}:${f.id}`, f.method);
      if (S.requests.size > 20000) S.requests.delete(S.requests.keys().next().value);
    }
  } else if (f.id != null) {
    const key = `${e.peer}>${e.node}:${opposite[e.channel]}:${f.id}`;
    e.method = S.requests.get(key);
    if (e.method) S.requests.delete(key);
  }
  return e;
}

function visible(e) {
  const t = topic(e);
  if ($("#hide-ping").checked && t === "ping") return false;
  if ($("#hide-adverts").checked && t === "advert") return false;
  const only = $("#trace-node").value;
  return !only || e.node === only || e.peer === only;
}

function visibleEvents() {
  return S.events.filter(visible);
}

async function pollTrace() {
  try {
    let fresh = 0;
    for (let page = 0; page < 10; page++) {
      const p = await api("GET", `/api/trace?after=${S.lastSeq}&limit=5000`);
      S.lastSeq = p.last;
      for (const raw of p.events) {
        // Each frame is reported twice (sender and receiver); keep the sender's copy.
        if (raw.kind === "received") continue;
        const e = annotate(raw);
        S.events.push(e);
        fresh++;
        if (S.mode === "live" && visible(e)) launch(e);
      }
      if (p.events.length < 5000) break;
    }
    if (S.events.length > MAX_EVENTS) {
      const drop = S.events.length - MAX_EVENTS;
      S.events.splice(0, drop);
      S.cursor = Math.max(0, S.cursor - drop);
    }
    S.eventsPerSecond = S.eventsPerSecond * 0.7 + (fresh / 0.3) * 0.3;
    if (fresh) renderTrace();
  } catch {
    /* the stats line already says when the lab is unreachable */
  }
}

function describe(e) {
  if (e.kind === "link_up") return ["link up", ""];
  if (e.kind === "link_down") return ["link down", e.reason || ""];
  const f = e.frame;
  const name = f.frame === "request" || f.frame === "notify" || f.frame === "event" ? f.method : `${f.frame}${e.method ? " · " + e.method : ""}`;
  const status = f.ok === false ? " ✗" : f.ok === true ? " ✓" : "";
  return [name + status, f.detail || ""];
}

function renderTrace() {
  const rows = visibleEvents();
  const body = $("#trace-table tbody");
  const log = $("#trace-log");
  let start, end;
  if (S.mode === "replay") {
    start = Math.max(0, S.cursor - TABLE_ROWS / 2);
    end = Math.min(rows.length, start + TABLE_ROWS);
    $("#scrub").max = rows.length;
    $("#scrub").value = S.cursor;
    $("#cursor").textContent = `${S.cursor} / ${rows.length}`;
  } else {
    start = Math.max(0, rows.length - TABLE_ROWS);
    end = rows.length;
  }
  const t0 = S.events.length ? S.events[0].at_us : 0;
  let html = "";
  for (let i = start; i < end; i++) {
    const e = rows[i];
    const [what, detail] = describe(e);
    const kindClass = e.kind === "link_down" ? "k-down" : e.kind === "link_up" ? "k-up" : "";
    const current = S.mode === "replay" && i === S.cursor - 1 ? ' class="current"' : "";
    html += `<tr data-i="${i}"${current}><td>${e.seq}</td><td>${((e.at_us - t0) / 1e6).toFixed(3)}</td>`
      + `<td>${esc(e.node)}</td><td>→</td><td>${esc(e.peer)}</td>`
      + `<td class="${kindClass}"><span class="dot" style="background:${e.kind === "sent" ? colourOf(e) : "transparent"}"></span>${esc(what)}</td>`
      + `<td class="wrap">${esc(detail)}</td></tr>`;
  }
  body.innerHTML = html;
  if (S.mode === "replay") {
    const current = body.querySelector("tr.current");
    if (current) current.scrollIntoView({ block: "center" });
  } else if (followLog) {
    log.scrollTop = log.scrollHeight;
  }
}

// Live log follows the newest row unless the owner scrolled up to read.
let followLog = true;
$("#trace-log").addEventListener("scroll", () => {
  const log = $("#trace-log");
  if (S.mode === "live") followLog = log.scrollTop + log.clientHeight >= log.scrollHeight - 30;
});

function setMode(mode) {
  S.mode = mode;
  stopPlaying();
  $("#mode-live").classList.toggle("active", mode === "live");
  $("#mode-replay").classList.toggle("active", mode === "replay");
  $("#replay-controls").hidden = mode !== "replay";
  if (mode === "replay") S.cursor = 0;
  renderTrace();
}

function step(direction) {
  const rows = visibleEvents();
  if (direction > 0 && S.cursor < rows.length) {
    launch(rows[S.cursor]);
    S.cursor++;
  } else if (direction < 0 && S.cursor > 0) {
    S.cursor--;
    launch(rows[S.cursor], true);
  } else {
    stopPlaying();
  }
  renderTrace();
}

function stopPlaying() {
  clearInterval(S.playing);
  S.playing = null;
  $("#play").textContent = "▶";
}

$("#mode-live").addEventListener("click", () => setMode("live"));
$("#mode-replay").addEventListener("click", () => setMode("replay"));
$("#step-fwd").addEventListener("click", () => step(1));
$("#step-back").addEventListener("click", () => step(-1));
$("#play").addEventListener("click", () => {
  if (S.playing) return stopPlaying();
  $("#play").textContent = "⏸";
  S.playing = setInterval(() => step(1), 500 / S.speed);
});
$("#scrub").addEventListener("input", e => {
  S.cursor = +e.target.value;
  renderTrace();
});
$("#speed").addEventListener("change", e => {
  S.speed = +e.target.value;
  if (S.playing) { stopPlaying(); $("#play").click(); }
});
// Pings are left out on the server too (they would crowd everything else out of its buffer).
$("#hide-ping").addEventListener("change", e => api("PUT", "/api/trace/heartbeats", !e.target.checked).catch(() => {}));
["#hide-ping", "#hide-adverts", "#trace-node"].forEach(id => $(id).addEventListener("change", () => {
  S.cursor = Math.min(S.cursor, visibleEvents().length);
  renderTrace();
}));
$("#trace-clear").addEventListener("click", async () => {
  await api("DELETE", "/api/trace").catch(() => {});
  clearTrace();
});
$("#trace-toggle").addEventListener("click", () => {
  const collapsed = $("#trace").classList.toggle("collapsed");
  $("#trace-toggle").textContent = collapsed ? "▴" : "▾";
});
$("#trace-table tbody").addEventListener("click", e => {
  const row = e.target.closest("tr");
  if (!row) return;
  if (S.mode !== "replay") setMode("replay");
  S.cursor = +row.dataset.i + 1;
  launch(visibleEvents()[S.cursor - 1]);
  renderTrace();
});

function clearTrace() {
  S.events = [];
  S.requests.clear();
  S.cursor = 0;
  renderTrace();
}

// ---------------------------------------------------------------- side panel: tabs + node

$$(".tabs button").forEach(b => b.addEventListener("click", () => showTab(b.dataset.tab)));

function showTab(name) {
  $$(".tabs button").forEach(b => b.classList.toggle("active", b.dataset.tab === name));
  $$(".tab").forEach(t => (t.hidden = t.id !== `tab-${name}`));
}

function select(id) {
  S.selected = id;
  draw();
  renderNode(true);
  if (id) {
    showTab("node");
    $("#call-from").value = id;
    refreshRoutes();
  }
}

/** Fills the node tab. `fillForm` also resets the setup form (not on every poll: the owner
 * may be typing in it). */
function renderNode(fillForm) {
  const n = S.selected && S.nodes.get(S.selected);
  $("#node-none").hidden = !!n;
  $("#node-panel").hidden = !n;
  if (!n) return;
  const s = n.setup;
  $("#node-title").textContent = s.id;
  const state = $("#node-state");
  state.textContent = n.running ? "running" : "off";
  state.className = `pill ${n.running ? "on" : "off"}`;
  $("#power-on").disabled = n.running;
  $("#power-off").disabled = !n.running;
  $("#power-crash").disabled = !n.running;
  const r = n.resources;
  const facts = [
    ["address", n.address || "—"],
    ["rings", s.rings.join(", ") || "none — use Cable"],
    ["links", n.links.length ? `${n.links.length}: ${n.links.join(", ")}` : "none"],
    ["jobs", r ? `${r.active_jobs} running / ${r.max_concurrent_jobs} max · ${n.jobs_run} run` : "—"],
  ];
  $("#node-facts").innerHTML = facts.map(([k, v]) => `<dt>${k}</dt><dd>${esc(v)}</dd>`).join("");
  if (!fillForm) return;

  const f = $("#node-form");
  f.name.value = s.name;
  f.node_type.value = s.node_type;
  f.rings.value = s.rings.join(", ");
  f.max_concurrent_jobs.value = s.max_concurrent_jobs;
  f.cpu_percent.value = s.cpu_percent;
  f.memory_mb.value = s.memory_mb;
  f.availability.value = s.availability;
  const offered = new Map(s.capabilities.map(c => [c.name, c.allow.join(", ")]));
  $("#cap-list").innerHTML = S.catalog.map(c => `
    <label class="cap-row" title="${esc(c.description)}">
      <input type="checkbox" data-cap="${esc(c.name)}" ${offered.has(c.name) ? "checked" : ""}>
      <span>${esc(c.name)}</span>
      <input type="text" data-allow="${esc(c.name)}" value="${esc(offered.get(c.name) ?? "*")}" aria-label="who may call ${esc(c.name)}">
    </label>`).join("");
}

$("#node-form").addEventListener("submit", e => {
  e.preventDefault();
  const n = S.nodes.get(S.selected);
  if (!n) return;
  const f = e.target;
  const capabilities = $$("[data-cap]", f).filter(c => c.checked).map(c => ({
    name: c.dataset.cap,
    allow: splitList($(`[data-allow="${c.dataset.cap}"]`, f).value),
  }));
  const setup = {
    ...n.setup,
    name: f.name.value.trim(),
    node_type: f.node_type.value,
    rings: splitList(f.rings.value),
    capabilities,
    max_concurrent_jobs: +f.max_concurrent_jobs.value,
    cpu_percent: +f.cpu_percent.value,
    memory_mb: +f.memory_mb.value,
    availability: f.availability.value,
  };
  act(api("PUT", `/api/nodes/${n.setup.id}`, setup), `${n.setup.id} updated`).then(() => renderNode(true));
});

$("#power-on").addEventListener("click", () => act(api("POST", `/api/nodes/${S.selected}/power`, { on: true }), "powered on"));
$("#power-off").addEventListener("click", () => act(api("POST", `/api/nodes/${S.selected}/power`, { off: "graceful" }), "shut down"));
$("#power-crash").addEventListener("click", () => act(api("POST", `/api/nodes/${S.selected}/power`, { off: "crash" }), "crashed — peers notice by heartbeat"));
confirmClick($("#node-delete"), async () => {
  const id = S.selected;
  await act(api("DELETE", `/api/nodes/${id}`), `${id} removed`);
  S.pos.delete(id);
  select(null);
  saveLayout();
});

async function refreshRoutes() {
  const n = S.selected && S.nodes.get(S.selected);
  const table = $("#routes");
  if (!n || !n.running) {
    table.innerHTML = `<tr><td class="muted">${n ? "powered off" : ""}</td></tr>`;
    return;
  }
  try {
    const routes = await api("GET", `/api/nodes/${n.setup.id}/routes`);
    const rows = Object.entries(routes).sort((a, b) => a[1].hops - b[1].hops || naturally(a[0], b[0]));
    table.innerHTML = rows.length
      ? "<tr><th>to</th><th>next hop</th><th>hops</th></tr>" + rows.map(([to, r]) =>
        `<tr><td>${esc(to)}</td><td>${r.hops === 1 ? '<span class="muted">direct</span>' : esc(r.next_hop)}</td><td>${r.hops}</td></tr>`).join("")
      : '<tr><td class="muted">no routes yet</td></tr>';
  } catch (e) {
    table.innerHTML = `<tr><td class="muted">${esc(e.message)}</td></tr>`;
  }
}

// ---------------------------------------------------------------- side panel: call

const SAMPLE_INPUT = {
  "math.add": { a: 1, b: 2 },
  "system.echo": { hello: "lab" },
  "system.info": {},
  "test.execute": { suite: "smoke", steps: 10, step_ms: 300 },
  "agent.execute": { prompt: "hello" },
};

$("#call-capability").addEventListener("change", e => {
  $("#call-input").value = JSON.stringify(SAMPLE_INPUT[e.target.value] ?? {}, null, 1);
});

$("#call-form").addEventListener("submit", async e => {
  e.preventDefault();
  const action = e.submitter ? e.submitter.dataset.action : "invoke";
  const from = $("#call-from").value, capability = $("#call-capability").value, target = $("#call-target").value || null;
  if (!from) return toast("Pick the node that makes the call.", true);
  let input;
  try { input = JSON.parse($("#call-input").value || "{}"); } catch { return toast("Input is not valid JSON.", true); }
  const out = $("#call-result");
  out.innerHTML = '<p class="muted">…</p>';
  try {
    if (action === "explain") {
      const q = `capability=${encodeURIComponent(capability)}${target ? `&target=${target}` : ""}`;
      const decision = await api("GET", `/api/nodes/${from}/explain?${q}`);
      out.innerHTML = resultBox(decision.selected ? `would run on <b>${esc(decision.selected)}</b>` : "no node can take it now", !!decision.selected, null, decision);
      return;
    }
    const reply = await api("POST", `/api/nodes/${from}/${action === "submit" ? "jobs" : "invoke"}`, { capability, input, target });
    if (reply.ok) {
      const v = reply.ok;
      const title = action === "submit" ? `job <b>${esc(v.job_id)}</b> started on <b>${esc(v.executed_by)}</b>` : `ran on <b>${esc(v.executed_by)}</b>`;
      out.innerHTML = resultBox(title, true, action === "submit" ? null : v.output, v.routing);
      if (action === "submit") addJob(from, v.executed_by, v.job_id, capability);
    } else {
      const f = reply.failed;
      out.innerHTML = resultBox(`failed: ${esc(f.error.code)} — ${esc(f.error.message)}`, false, null, f.routing);
    }
  } catch (err) {
    out.innerHTML = resultBox(esc(err.message), false);
  }
});

function resultBox(title, ok, output, routing) {
  let html = `<div class="result ${ok ? "ok" : "bad"}"><div>${title}</div>`;
  if (output !== null && output !== undefined) html += `<pre>${esc(JSON.stringify(output, null, 1))}</pre>`;
  if (routing) html += routingTable(routing);
  return html + "</div>";
}

function routingTable(d) {
  if (!d.candidates || !d.candidates.length) {
    return `<p class="muted small">no candidates (${d.nodes_without_capability} known nodes lack ${esc(d.capability)})</p>`;
  }
  const rows = [...d.candidates].sort((a, b) => (a.rank ?? 1e9) - (b.rank ?? 1e9));
  return `<p class="muted small">policy: ${esc(d.policy)} · lower load first, then fewer hops</p>
    <table class="table"><tr><th>#</th><th>node</th><th>load‰</th><th>hops</th><th>via</th><th>why not</th></tr>`
    + rows.map(c => `<tr class="${c.node_id === d.selected ? "picked" : c.eligible ? "" : "no"}">
      <td>${c.rank ?? ""}</td><td>${esc(c.node_id)}</td><td>${c.load_permille}</td><td>${c.hops}</td>
      <td>${esc(c.via || "")}</td><td class="wrap">${esc(c.rejection ? rejectionText(c.rejection) : "")}</td></tr>`).join("")
    + "</table>";
}

function rejectionText(r) {
  return typeof r === "string" ? r : Object.entries(r).map(([k, v]) => (typeof v === "object" ? k : `${k}: ${v}`)).join(", ");
}

function addJob(from, executor, jobId, capability) {
  S.jobs.unshift({ from, executor, jobId, capability, info: null, error: null });
  S.jobs.length = Math.min(S.jobs.length, 30);
  renderJobs();
}

async function refreshJobs() {
  for (const job of S.jobs) {
    if (job.info && job.info.state !== "running") continue;
    try {
      const r = await api("GET", `/api/nodes/${job.from}/jobs/${job.executor}/${job.jobId}`);
      job.info = r.job;
      job.error = null;
    } catch (e) {
      job.error = e.message;
    }
  }
  renderJobs();
}

function renderJobs() {
  const box = $("#jobs");
  if (!S.jobs.length) return;
  box.innerHTML = S.jobs.map((j, i) => {
    const info = j.info || {};
    const state = j.error ? `⚠ ${j.error}` : info.state ? `${info.state} ${info.percent ?? 0}% ${info.message || ""}` : "…";
    const cancel = info.state === "running" ? `<button data-cancel="${i}" class="small">Cancel</button>` : "";
    return `<div class="job"><div><b>${esc(j.capability)}</b> on ${esc(j.executor)} <span class="muted">from ${esc(j.from)}</span>
      <div class="muted">${esc(state)}</div><div class="bar-mini"><i style="width:${info.percent ?? 0}%"></i></div></div>${cancel}</div>`;
  }).join("");
}

$("#jobs").addEventListener("click", e => {
  const b = e.target.closest("[data-cancel]");
  if (!b) return;
  const j = S.jobs[+b.dataset.cancel];
  act(api("POST", `/api/nodes/${j.from}/jobs/${j.executor}/${j.jobId}/cancel`), "cancel sent");
});

// ---------------------------------------------------------------- side panel: scenarios + network

$$(".scenario").forEach(form => form.addEventListener("submit", async e => {
  e.preventDefault();
  const name = form.dataset.scenario;
  const params = {};
  for (const [k, v] of new FormData(form)) {
    if (v === "") continue;
    params[k] = /^\d+$/.test(v) ? +v : v;
  }
  const button = form.querySelector("button");
  button.disabled = true;
  const report = $("#scenario-report");
  report.hidden = false;
  report.textContent = `running ${name}…`;
  try {
    const result = await api("POST", `/api/scenarios/${name}`, params);
    report.textContent = JSON.stringify(result, null, 1);
    if (name === "rings") {
      clearTrace();
      S.pos.clear();
      S.ringPos.clear();
      await refreshNodes();
      arrangeAll();
    }
  } catch (err) {
    report.textContent = `failed: ${err.message}`;
  } finally {
    button.disabled = false;
    refreshNodes();
  }
}));

$("#save").addEventListener("click", async () => {
  await api("PUT", "/api/layout", layoutJson()).catch(() => {});
  const topology = await api("GET", "/api/topology");
  const blob = new Blob([JSON.stringify(topology, null, 1)], { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `synapta-lab-${new Date().toISOString().slice(0, 16).replace(/[:T]/g, "-")}.json`;
  a.click();
  URL.revokeObjectURL(a.href);
});

$("#load").addEventListener("change", async e => {
  const file = e.target.files[0];
  e.target.value = "";
  if (!file) return;
  let topology;
  try { topology = JSON.parse(await file.text()); } catch { return toast("That file is not JSON.", true); }
  toast(`Loading ${topology.nodes ? topology.nodes.length : 0} nodes…`);
  const done = await act(api("PUT", "/api/topology", topology), "topology loaded");
  if (done) {
    clearTrace();
    applyLayout(topology.layout);
    select(null);
    draw();
    fitView();
  }
});

confirmClick($("#reset"), async () => {
  await act(api("POST", "/api/reset"), "all nodes removed");
  S.pos.clear();
  S.ringPos.clear();
  clearTrace();
  select(null);
  saveLayout();
});

$("#show-links").addEventListener("change", e => {
  S.showLinks = e.target.checked;
  draw();
});
$("#arrange").addEventListener("click", arrangeAll);

function renderRings() {
  const rows = allRings().map(r => {
    const ids = ringMembers(r);
    const on = ids.filter(id => S.nodes.get(id).running).length;
    const remove = ids.length ? "" : `<button class="small" data-remove-ring="${esc(r)}">remove</button>`;
    return `<tr><td>${esc(r)}</td><td>${ids.length} nodes</td><td>${on} running</td><td>${remove}</td></tr>`;
  });
  $("#ring-table").innerHTML = rows.join("") || '<tr><td class="muted">no rings</td></tr>';
}

$("#ring-table").addEventListener("click", e => {
  const b = e.target.closest("[data-remove-ring]");
  if (!b) return;
  S.ringPos.delete(b.dataset.removeRing);
  draw();
  renderRings();
  saveLayout();
});

/** Keeps every node/capability picker in step with the lab. */
function renderPickers() {
  const ids = [...S.nodes.keys()].sort(naturally);
  const running = ids.filter(id => S.nodes.get(id).running);
  fillSelect($("#call-from"), running.map(id => [id, id]));
  fillSelect($("#call-target"), [["", "best node (routing decides)"], ...ids.map(id => [id, id])]);
  fillSelect($("#trace-node"), [["", "all"], ...ids.map(id => [id, id])]);
  $$(".node-select").forEach(s => fillSelect(s, running.map(id => [id, id])));
  const bridges = ids.filter(id => S.nodes.get(id).setup.rings.length >= 2);
  $$(".bridge-select").forEach(s => fillSelect(s, [["", "first bridge found"], ...bridges.map(id => [id, id])]));
}

function fillSelect(select, options) {
  const key = options.map(o => o[0]).join("\n");
  if (select.dataset.key === key) return;
  const keep = select.value;
  select.innerHTML = options.map(([v, t]) => `<option value="${esc(v)}">${esc(t)}</option>`).join("");
  select.dataset.key = key;
  if (options.some(o => o[0] === keep)) select.value = keep;
}

// ---------------------------------------------------------------- polling + start-up

async function refreshNodes() {
  try {
    const list = await api("GET", "/api/nodes");
    S.nodes = new Map(list.map(n => [n.setup.id, n]));
    if (S.selected && !S.nodes.has(S.selected)) S.selected = null;
    draw();
    renderNode(false);
    renderPickers();
    renderRings();
    const running = list.filter(n => n.running).length;
    const busy = list.reduce((s, n) => s + ((n.resources && n.resources.active_jobs) || 0), 0);
    $("#stats").textContent = `${list.length} nodes · ${running} running · ${allRings().length} rings · ${busy} jobs running · ${Math.round(S.eventsPerSecond)} msg/s`;
  } catch {
    $("#stats").textContent = "lab not reachable — is synapta-lab still running?";
  }
}

function every(ms, task) {
  let busy = false;
  setInterval(async () => {
    if (busy || document.hidden) return;
    busy = true;
    try { await task(); } finally { busy = false; }
  }, ms);
}

async function start() {
  try {
    const info = await api("GET", "/api/info");
    S.catalog = info.catalog;
    $("#version").textContent = `v${info.version}`;
    $("#hide-ping").checked = !info.heartbeats;
    fillSelect($("#call-capability"), info.catalog.map(c => [c.name, c.name]));
    $$(".cap-select").forEach(s => fillSelect(s, info.catalog.map(c => [c.name, c.name])));
    $("#call-capability").value = "math.add";
    $("#call-input").value = JSON.stringify(SAMPLE_INPUT["math.add"], null, 1);
    $$(".cap-select").forEach(s => (s.value = "math.add"));
    const topology = await api("GET", "/api/topology");
    applyLayout(topology.layout);
  } catch (e) {
    toast(`Could not reach the lab: ${e.message}`, true);
  }
  setTool("select");
  await refreshNodes();
  fitView();
  every(1000, refreshNodes);
  every(300, pollTrace);
  every(1000, refreshJobs);
  every(2000, refreshRoutes);
}

start();
