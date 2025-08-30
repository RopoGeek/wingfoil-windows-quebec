// script.js — spot mapping loader (UI vs Fetch)

const SPOTS_URL = 'https://raw.githubusercontent.com/RopoGeek/wingfoil-windows-quebec/main/spots.json'; // update if path differs

let SPOTS = [];
let currentSpotId = null;

const map = L.map('map', { zoomControl: true }).setView([46.85, -71.10], 11);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);
const markers = {};

async function bootstrap() {
  const res = await fetch(SPOTS_URL);
  if (!res.ok) throw new Error('Cannot load spots.json');
  const json = await res.json();
  SPOTS = json.spots || [];

  // Populate selects
  const spotSel = document.getElementById('spotSel');
  spotSel.innerHTML = '';
  for (const s of SPOTS) {
    const opt = document.createElement('option');
    opt.value = s.id; opt.textContent = s.name; spotSel.appendChild(opt);
  }
  spotSel.addEventListener('change', () => selectSpot(spotSel.value));

  // Add markers from UI coords
  for (const s of SPOTS) {
    const m = L.marker([s.ui.lat, s.ui.lon]).addTo(map).bindPopup(s.name);
    m.on('click', () => selectSpot(s.id));
    markers[s.id] = m;
  }

  // Default to first spot
  if (SPOTS.length) selectSpot(SPOTS[0].id);

  // Model dropdown
  const modelSel = document.getElementById('modelSel');
  modelSel.addEventListener('change', () => fetchForecast());
}

function getSpot(id) { return SPOTS.find(s => s.id === id); }

function selectSpot(id) {
  currentSpotId = id;
  const s = getSpot(id);
  document.getElementById('currentSpotName').textContent = s.name;
  map.setView([s.ui.lat, s.ui.lon], 12);
  document.getElementById('spotSel').value = id;
  fetchForecast();
}

async function fetchForecast() {
  const s = getSpot(currentSpotId);
  const model = document.getElementById('modelSel').value;

  // IMPORTANT: use fetch coords for model request
  const { lat, lon } = s.fetch;
  // TODO: replace the mock with your real endpoint call. Example:
  // const rows = await getModelForecast(model, lat, lon);
  const rows = mockForecast(model, lat, lon); // temporary
  renderTable(rows);
  document.getElementById('status').textContent = `Showing ${rows.length} rows for ${s.name} (${model.toUpperCase()}).`;
}

function mockForecast(model, lat, lon) {
  const out = []; const now = new Date();
  for (let i = 0; i < 8; i++) {
    const t = new Date(now.getTime() + i * 3600 * 1000);
    const speed = Math.round(8 + 5 * Math.sin(i / 2) + (model === 'hrdps' ? 2 : 0));
    const dir = Math.round((90 + i * 30) % 360);
    out.push({ time: t, speed, dir, tideLevel: (i % 4) - 1 });
  }
  return out;
}

function renderTable(rows) {
  const tb = document.querySelector('#forecastTbl tbody');
  tb.innerHTML = '';
  for (const r of rows) {
    const tr = document.createElement('tr');
    const timeLocal = r.time.toLocaleString(undefined, { hour: '2-digit', minute: '2-digit', month: 'short', day: '2-digit' });
    tr.innerHTML = `<td class="tide-cell"><span class="tide-bar" data-level="${r.tideLevel ?? ''}"></span>${timeLocal}</td><td>${r.speed}</td><td>${r.dir}</td>`;
    tb.appendChild(tr);
  }
  applyTideBarStyles();
}

function applyTideBarStyles() {
  const mapLevelToClass = (lvl) => ({
    '-1': 'tide-low',
    '0': 'tide-mid',
    '1': 'tide-high',
    '2': 'tide-very-high'
  }[String(lvl)] || 'tide-unknown');

  document.querySelectorAll('.tide-bar').forEach(el => {
    const lvl = el.getAttribute('data-level');
    el.className = 'tide-bar ' + mapLevelToClass(lvl);
  });
}

window.addEventListener('DOMContentLoaded', bootstrap);
