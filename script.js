(async function(){
  // ---------- Config ----------
  let VIEW = 'daylight'; // 'daylight' | 'all'
  const HOUR_RANGE = { daylight:[6,21], all:[0,23] };
  const TZ = 'America/Toronto';

  // Spots for MAP DISPLAY ONLY (forecast values come from forecast.json)
  const spots = [
    {key:'beauport',     label:'Baie de Beauport',            lat:46.83938157700642,  lon:-71.18913476445113},
    {key:'ste_anne',     label:'Ste-Anne-de-Beaupré',         lat:47.0153,            lon:-70.9280},
    {key:'st_jean',      label:'St-Jean, Île d’Orléans',      lat:46.915308,          lon:-70.89627},
    {key:'ange_gardien', label:'Ange-Gardien',                lat:46.907944,          lon:-71.090028},
  ];
  const spotByKey = Object.fromEntries(spots.map(s=>[s.key,s]));

  // 16-point sectors
  const SECTORS = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW'];
  const SECTOR_CENTERS = {
    'N':0,'NNE':22.5,'NE':45,'ENE':67.5,'E':90,'ESE':112.5,'SE':135,'SSE':157.5,
    'S':180,'SSW':202.5,'SW':225,'WSW':247.5,'W':270,'WNW':292.5,'NW':315,'NNW':337.5
  };

  // Defaults per spot
  const defaultSettings = {
    beauport:     { min:10, dirs: new Set(SECTORS),                tides: new Set(['rising','falling']) },
    ste_anne:     { min:10, dirs: new Set(['SSW','SW','WSW']),     tides: new Set(['rising']) },
    st_jean:      { min:10, dirs: new Set(['NNE','NE','ENE']),     tides: new Set(['falling']) },
    ange_gardien: { min:10, dirs: new Set(['SSW','SW','WSW']),     tides: new Set(['rising']) },
  };

  // ---------- DOM refs ----------
  const tabBar   = document.getElementById('tabs');
  const matrices = document.getElementById('matrices');
  const infoBar  = document.getElementById('infoBar');
  const gustSel  = document.getElementById('minGustSelect');
  const dirGrid  = document.getElementById('dirGrid');
  const tideRise = document.getElementById('tideRise');
  const tideFall = document.getElementById('tideFall');
  const btnDay   = document.getElementById('tgl-daylight');
  const btnAll   = document.getElementById('tgl-all');

  // ---------- Helpers ----------
  const toLocal = s => new Date(s);
  const dayKey = d => d.toLocaleDateString('en-CA', { timeZone: TZ });
  const dayLabel = d => d.toLocaleDateString('fr-CA', { weekday:'short', month:'short', day:'2-digit', timeZone:TZ });
  const hourStr = d => d.toLocaleString('en-CA',{ hour:'2-digit', hour12:false, timeZone:TZ });

  function degToCardinal16(deg){
    if (deg==null || isNaN(deg)) return '';
    const ix = Math.round(((deg%360)/22.5)) % 16;
    return SECTORS[ix];
  }
  function arrowSVG(deg, size=12){
    if (deg==null || isNaN(deg)) return '';
    const rot = (180 + Number(deg)) % 360; // FROM direction
    return `<svg class="arrow" viewBox="0 0 24 24" style="transform:rotate(${rot}deg);width:${size}px;height:${size}px">
      <path d="M12 4 L8 10 H11 V20 H13 V10 H16 Z" fill="currentColor"></path>
    </svg>`;
  }
  function sectorOf(deg){
    if (deg==null || isNaN(deg)) return null;
    const i = Math.round(((deg%360)/22.5)) % 16;
    return SECTORS[i];
  }
  function strengthColor(kn){
    if (kn == null || isNaN(kn)) return 'var(--neutral)';
    const v = Number(kn);
    if (v >= 30) return 'var(--r2)';
    if (v >= 25) return 'var(--r1)';
    if (v >= 21) return 'var(--y2)';
    if (v >= 18) return 'var(--y1)';
    if (v >= 14) return 'var(--g2)';
    return 'var(--g1)'; // 10–14
  }
  function tideStripeColor(state){
    const s = (state||'').toLowerCase();
    if (s==='rising')  return 'var(--tide-rise)';
    if (s==='falling') return 'var(--tide-fall)';
    if (s==='slack')   return 'var(--tide-slack)';
    return 'var(--tide-unk)';
  }

  // ---------- Load forecast.json ----------
  let data;
  try{
    data = await fetch('forecast.json?v=' + Date.now(), {cache:'no-store'}).then(r=>r.json());
  }catch(e){
    document.body.insertAdjacentHTML('beforeend',
      '<p style="color:#b00"><span class="lang-fr">Impossible de charger</span> / <span class="lang-en">Could not load</span> forecast.json.</p>');
    return;
  }
  const hours = data.hours || [];
  if (!hours.length){
    document.body.insertAdjacentHTML('beforeend',
      '<p class="muted"><span class="lang-fr">Aucune donnée pour le moment.</span> / <span class="lang-en">No data yet.</span></p>');
    return;
  }

  // ---------- Group by day ----------
  const byDayAll = new Map();
  for(const row of hours){
    const t = toLocal(row.time);
    const k = dayKey(t);
    if(!byDayAll.has(k)) byDayAll.set(k, []);
    byDayAll.get(k).push(row);
  }
  for(const arr of byDayAll.values()){
    arr.sort((a,b)=> new Date(a.time)-new Date(b.time));
  }
  const dayOrder = Array.from(byDayAll.keys()).sort((a,b)=> new Date(a)-new Date(b));

  // ---------- Per-spot settings (localStorage) ----------
  const MIN_KN_OPTIONS = Array.from({length:18}, (_,i)=> i+8); // 8..25 inclusive
  function loadSettings(spotKey){
    const raw = localStorage.getItem('wf_settings_'+spotKey);
    if(!raw) return structuredClone(defaultSettings[spotKey]);
    try{
      const o = JSON.parse(raw);
      return {
        min: o.min ?? defaultSettings[spotKey].min,
        dirs: new Set(o.dirs ?? Array.from(defaultSettings[spotKey].dirs)),
        tides: new Set(o.tides ?? Array.from(defaultSettings[spotKey].tides)),
      };
    }catch(_){ return structuredClone(defaultSettings[spotKey]); }
  }
  function saveSettings(spotKey, s){
    localStorage.setItem('wf_settings_'+spotKey, JSON.stringify({
      min: s.min, dirs: Array.from(s.dirs), tides: Array.from(s.tides)
    }));
  }
  let settings = {
    beauport:loadSettings('beauport'),
    ste_anne:loadSettings('ste_anne'),
    st_jean:loadSettings('st_jean'),
    ange_gardien:loadSettings('ange_gardien')
  };
  let currentSpot = 'beauport';

  // Fill UI choices
  MIN_KN_OPTIONS.forEach(v=>{
    const opt = document.createElement('option');
    opt.value = v; opt.textContent = v + ' kn';
    gustSel.appendChild(opt);
  });
  SECTORS.forEach(sec=>{
    const div = document.createElement('div');
    div.className = 'chip';
    div.textContent = sec;
    div.dataset.sec = sec;
    dirGrid.appendChild(div);
  });

  function refreshSettingsPanel(){
    const s = settings[currentSpot];
    gustSel.value = s.min;
    [...dirGrid.children].forEach(ch=>{
      ch.classList.toggle('on', s.dirs.has(ch.dataset.sec));
    });
    tideRise.checked = s.tides.has('rising');
    tideFall.checked = s.tides.has('falling');
    drawSpotSectorsOnMap(currentSpot);
  }

  // Direction chip clicks
  dirGrid.addEventListener('click', (e)=>{
    const chip = e.target.closest('.chip');
    if(!chip) return;
    const s = settings[currentSpot];
    const sec = chip.dataset.sec;
    if(s.dirs.has(sec)) s.dirs.delete(sec); else s.dirs.add(sec);
    chip.classList.toggle('on');
    saveSettings(currentSpot, s);
    render(VIEW);
    drawSpotSectorsOnMap(currentSpot);
  });

  // Presets
  document.querySelectorAll('.preset').forEach(p=>{
    p.addEventListener('click', ()=>{
      const which = p.dataset.preset;
      const s = settings[currentSpot];
      if(which==='any'){ s.dirs = new Set(SECTORS); }
      else if(which==='sw'){ s.dirs = new Set(['SSW','SW','WSW']); }
      else if(which==='ne'){ s.dirs = new Set(['NNE','NE','ENE']); }
      else if(which==='none'){ s.dirs = new Set(); }
      saveSettings(currentSpot, s);
      refreshSettingsPanel();
      render(VIEW);
      drawSpotSectorsOnMap(currentSpot);
    });
  });

  // Gust select & tide checkboxes
  gustSel.addEventListener('change', ()=>{
    const s = settings[currentSpot];
    s.min = Number(gustSel.value);
    saveSettings(currentSpot, s);
    render(VIEW);
  });
  tideRise.addEventListener('change', ()=>{
    const s = settings[currentSpot];
    if(tideRise.checked) s.tides.add('rising'); else s.tides.delete('rising');
    saveSettings(currentSpot, s);
    render(VIEW);
  });
  tideFall.addEventListener('change', ()=>{
    const s = settings[currentSpot];
    if(tideFall.checked) s.tides.add('falling'); else s.tides.delete('falling');
    saveSettings(currentSpot, s);
    render(VIEW);
  });

  // ---------- Tabs ----------
  spots.forEach((s,i)=>{
    const tab = document.createElement('button');
    tab.className = 'tab' + (i===0?' active':'');
    tab.textContent = s.label;
    tab.dataset.target = s.key;
    tabBar.appendChild(tab);
  });

  // ---------- Rules check ----------
  function sectorOf(deg){
    if (deg==null || isNaN(deg)) return null;
    const i = Math.round(((deg%360)/22.5)) % 16;
    return SECTORS[i];
  }
  function meetsUserRules(spotKey, gust, dir_deg, tide){
    const s = settings[spotKey];
    if (gust == null || isNaN(gust) || Number(gust) < Number(s.min)) return false;
    if (dir_deg != null && !isNaN(dir_deg)) {
      const sec = sectorOf(Number(dir_deg));
      if (s.dirs.size > 0 && !s.dirs.has(sec)) return false;
    } else {
      return false;
    }
    const t = String(tide||'unknown').toLowerCase();
    if (s.tides.size > 0 && !s.tides.has(t)) return false;
    return true;
  }

  // ---------- Color helpers ----------
  function strengthColor(kn){
    if (kn == null || isNaN(kn)) return 'var(--neutral)';
    const v = Number(kn);
    if (v >= 30) return 'var(--r2)';
    if (v >= 25) return 'var(--r1)';
    if (v >= 21) return 'var(--y2)';
    if (v >= 18) return 'var(--y1)';
    if (v >= 14) return 'var(--g2)';
    return 'var(--g1)';
  }
  function tideStripeColor(state){
    const s = (state||'').toLowerCase();
    if (s==='rising')  return 'var(--tide-rise)';
    if (s==='falling') return 'var(--tide-fall)';
    if (s==='slack')   return 'var(--tide-slack)';
    return 'var(--tide-unk)';
  }
  function arrowSVG(deg, size=12){
    if (deg==null || isNaN(deg)) return '';
    const rot = (180 + Number(deg)) % 360;
    return `<svg class="arrow" viewBox="0 0 24 24" style="transform:rotate(${rot}deg);width:${size}px;height:${size}px">
      <path d="M12 4 L8 10 H11 V20 H13 V10 H16 Z" fill="currentColor"></path>
    </svg>`;
  }
  function degToCardinal16(deg){
    if (deg==null || isNaN(deg)) return '';
    const ix = Math.round(((deg%360)/22.5)) % 16;
    return SECTORS[ix];
  }

  // ---------- Build one matrix ----------
  function buildMatrix(spotKey, mode, byDayAll, dayOrder){
    const [HSTART, HEND] = (mode==='daylight' ? [6,21] : [0,23]);

    const container = document.createElement('div');
    container.className = 'matrix-wrap';
    const tbl = document.createElement('table');

    // Header
    const thead = document.createElement('thead');
    const htr = document.createElement('tr');

    const corner = document.createElement('th');
    corner.className = 'hour';
    corner.textContent = 'Heure / Hour';
    htr.appendChild(corner);

    dayOrder.forEach(dk=>{
      const thStripe = document.createElement('th');
      thStripe.className = 'tideStripeHead';
      htr.appendChild(thStripe);

      const th = document.createElement('th');
      th.className = 'day';
      const dt = new Date(dk + 'T00:00:00-04:00');
      th.innerHTML = `<div class="day-head"><div class="date">${dayLabel(dt)}</div></div>`;
      htr.appendChild(th);
    });
    thead.appendChild(htr);
    tbl.appendChild(thead);

    // Build map[day][hour] = row[spotKey]
    const map = {};
    dayOrder.forEach(k=>{
      map[k] = {};
      const arr = byDayAll.get(k) || [];
      arr.forEach(row=>{
        const h = hourStr(new Date(row.time));
        map[k][h] = row[spotKey] || {};
      });
    });

    // Body
    const tbody = document.createElement('tbody');
    for(let hour=HSTART; hour<=HEND; hour++){
      const hrStr = hour.toString().padStart(2,'0');
      const tr = document.createElement('tr');

      const th = document.createElement('th');
      th.className = 'hour';
      th.innerHTML = `<span class="hour-label">${hrStr}</span>`;
      tr.appendChild(th);

      dayOrder.forEach(dk=>{
        const obj = (map[dk] && map[dk][hrStr]) ? map[dk][hrStr] : {};
        const tideState = (obj && obj.tide) ? obj.tide : 'unknown';

        // tide stripe
        const tdStripe = document.createElement('td');
        tdStripe.className = 'tideStripe';
        tdStripe.style.background = tideStripeColor(tideState);
        tr.appendChild(tdStripe);

        // data cell
        const td = document.createElement('td');
        let bg;
        if (obj.wind_kn == null) {
          bg = 'var(--neutral)';
        } else if (meetsUserRules(spotKey, obj.wind_kn, obj.dir_deg, tideState)) {
          bg = strengthColor(obj.wind_kn);
        } else {
          bg = 'var(--blank)';
        }

        const gust = (obj.wind_kn == null ? '-' : obj.wind_kn);
        const avg  = obj.wind_avg_kn;
        const deg  = obj.dir_deg;
        const card = deg==null ? '' : degToCardinal16(deg);
        const tip  = `${dk} ${hrStr}:00 — gust ${gust} kn${avg!=null?`, avg ${avg} kn`:''}${card?` (${card}${deg!=null?` ${deg}°`:''})`:''} • tide: ${tideState}`;

        td.innerHTML = `<div class="cell" title="${tip}" tabindex="0" role="button"
            data-spot="${spotKey}"
            data-day="${dk}"
            data-hour="${hrStr}"
            data-gust="${gust}"
            data-avg="${avg==null?'':avg}"
            data-deg="${deg==null?'':deg}"
            data-card="${card}"
            data-tide="${tideState}"
          >${deg!=null ? arrowSVG(deg) : ''}</div>`;
        td.querySelector('.cell').style.background = bg;
        tr.appendChild(td);
      });

      tbody.appendChild(tr);
    }

    tbl.appendChild(tbody);
    container.appendChild(tbl);
    return container;
  }

  // ---------- Render all ----------
  function render(mode){
    matrices.innerHTML = '';
    const views = {};

    spots.forEach((s,i)=>{
      const v = buildMatrix(s.key, mode, byDayAll, dayOrder);
      v.style.display = i===0 ? 'block':'none';
      v.id = 'view-'+s.key;
      matrices.appendChild(v);
      views[s.key] = v;

      // cell interactions
      v.addEventListener('click', onCellActivate);
      v.addEventListener('keydown', (ev)=>{
        if(ev.key==='Enter' || ev.key===' '){
          const el = ev.target.closest('.cell');
          if(el){ ev.preventDefault(); onCellActivate(ev); }
        }
      });
    });

    // Tab switching
    tabBar.onclick = (e)=>{
      const btn = e.target.closest('.tab');
      if(!btn) return;
      [...tabBar.children].forEach(b=>b.classList.remove('active'));
      btn.classList.add('active');
      const target = btn.dataset.target;
      Object.values(views).forEach(v=>v.style.display='none');
      views[target].style.display = 'block';
      currentSpot = target;
      refreshSettingsPanel();
      infoBar.innerHTML = '<span class="info-dim"><span class="lang-fr">Touchez une cellule</span> / <span class="lang-en">Tap a cell</span></span>';
      centerMapOnSpot(target);
      drawSpotSectorsOnMap(target);
    };
  }

  // ---------- Info bar from cell ----------
  function onCellActivate(ev){
    const cell = ev.target.closest('.cell');
    if(!cell) return;
    const dk = cell.dataset.day;
    const hr = cell.dataset.hour;
    const gust = cell.dataset.gust || '-';
    const avg  = cell.dataset.avg ? `${cell.dataset.avg} kn` : '';
    const deg  = cell.dataset.deg;
    const card = cell.dataset.card || '';
    const tide = (cell.dataset.tide || 'unknown');
    const arrow = (deg ? arrowSVG(deg,18) : '');
    infoBar.innerHTML = `
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
        <div><b>${dk}</b> ${hr}:00</div>
        <div>• <span class="lang-fr">Rafale</span> / <span class="lang-en">Gust</span>: <b>${gust} kn</b>${avg?` (avg ${avg})`:''}</div>
        <div>• <span class="lang-fr">Direction</span> / <span class="lang-en">Dir</span>: <b>${card}${deg?` ${deg}°`:''}</b> ${arrow}</div>
        <div>• <span class="lang-fr">Marée</span> / <span class="lang-en">Tide</span>: <b>${tide}</b></div>
      </div>`;
  }

  // ---------- Map (smaller, above filters) ----------
  const mapEl = document.getElementById('map');
  const map = L.map(mapEl, { zoomControl:true, attributionControl:true });
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: '&copy; OSM contributors'
  }).addTo(map);
  let marker = null;
  let sectorLayers = [];

  function centerMapOnSpot(spotKey){
    const s = spotByKey[spotKey];
    if(!s) return;
    map.setView([s.lat, s.lon], 12);
    if (marker) { map.removeLayer(marker); }
    marker = L.marker([s.lat, s.lon]).addTo(map).bindPopup(s.label);
  }

  // Sector helpers
  function destPoint(lat, lon, brgDeg, distKm){
    const R = 6371.0088;
    const δ = distKm / R;
    const φ1 = lat * Math.PI/180;
    const λ1 = lon * Math.PI/180;
    const θ = brgDeg * Math.PI/180;
    const sinφ1 = Math.sin(φ1), cosφ1 = Math.cos(φ1);
    const sinδ = Math.sin(δ), cosδ = Math.cos(δ);
    const sinφ2 = sinφ1*cosδ + cosφ1*sinδ*Math.cos(θ);
    const φ2 = Math.asin(sinφ2);
    const y = Math.sin(θ)*sinδ*cosφ1;
    const x = cosδ - sinφ1*sinφ2;
    const λ2 = λ1 + Math.atan2(y, x);
    return [φ2*180/Math.PI, ((λ2*180/Math.PI+540)%360)-180];
  }
  function drawSector(lat, lon, startDeg, endDeg, radiusKm=8, steps=40){
    const pts = [];
    pts.push([lat,lon]);
    const sweep = ((endDeg - startDeg + 360) % 360);
    const step = Math.max(1, Math.round(sweep/steps));
    for(let d=0; d<=sweep; d+=step){
      const brg = (startDeg + d) % 360;
      pts.push(destPoint(lat, lon, brg, radiusKm));
    }
    pts.push(destPoint(lat, lon, endDeg, radiusKm));
    pts.push([lat,lon]);
    return L.polygon(pts, {color:'#0a5', weight:1, fillColor:'#0a5', fillOpacity:0.18});
  }
  function clearSectors(){ sectorLayers.forEach(l=> map.removeLayer(l)); sectorLayers = []; }
  function dirsToRanges(dirSet){
    if (!dirSet || dirSet.size===0) return [];
    const spans = Array.from(dirSet).map(name=>{
      const c = SECTOR_CENTERS[name];
      return [(c-11.25+360)%360, (c+11.25)%360];
    });
    const covered = new Array(360).fill(false);
    spans.forEach(([a,b])=>{
      if(a<=b){
        for(let d=Math.floor(a); d<=Math.ceil(b); d++) covered[(d+360)%360]=true;
      }else{
        for(let d=Math.floor(a); d<360; d++) covered[d]=true;
        for(let d=0; d<=Math.ceil(b); d++) covered[d]=true;
      }
    });
    const ranges=[];
    let inRun=false, start=0;
    for(let d=0; d<360; d++){
      if(covered[d] && !inRun){ inRun=true; start=d; }
      if(!covered[d] && inRun){ inRun=false; ranges.push([start, d-1]); }
    }
    if(inRun) ranges.push([start, 359]);
    if(ranges.length>1 && ranges[0][0]===0 && ranges[ranges.length-1][1]===359){
      const last = ranges.pop();
      ranges[0][0] = last[0];
    }
    return ranges;
  }
  function drawSpotSectorsOnMap(spotKey){
    const s = spotByKey[spotKey];
    if(!s) return;
    clearSectors();
    const dirs = settings[spotKey].dirs;
    const ranges = dirsToRanges(dirs);
    ranges.forEach(([a,b])=>{
      const poly = drawSector(s.lat, s.lon, a, b, 8, 40);
      poly.addTo(map);
      sectorLayers.push(poly);
    });
  }

  // map init & collapsible resize
  map.setView([46.88, -71.05], 10);
  centerMapOnSpot('beauport');
  drawSpotSectorsOnMap('beauport');
  document.getElementById('mapSection').addEventListener('toggle', ()=>{
    setTimeout(()=> map.invalidateSize(), 200);
  });

  // ---------- View toggles ----------
  function setToggle(active){
    btnDay.classList.toggle('active', active==='daylight');
    btnAll.classList.toggle('active', active==='all');
  }
  btnDay.onclick = ()=>{ VIEW='daylight'; setToggle(VIEW); render(VIEW); };
  btnAll.onclick = ()=>{ VIEW='all'; setToggle(VIEW); render(VIEW); };

  // ---------- First paint ----------
  refreshSettingsPanel();
  render(VIEW);
})();

