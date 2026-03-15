"""
Static HTML dashboard page templates for M14 — Output Layer.

These pages load the exported JSON files via fetch() and require a simple
HTTP server (e.g. ``python -m http.server``) or static hosting (S3, GitHub
Pages, Netlify).  No build step or framework is required.

All user-supplied strings from the database are HTML-escaped via the
``esc()`` helper before insertion to prevent XSS.
"""

# ---------------------------------------------------------------------------
# Shared JS snippet: HTML-escape helper, badge builder
# ---------------------------------------------------------------------------
_SHARED_JS = """
  function esc(s) {
    if (s == null) return '';
    const d = document.createElement('div');
    d.textContent = String(s);
    return d.innerHTML;
  }
  function badge(level) {
    if (!level) return '';
    return '<span class="badge badge-' + esc(level) + '">' + esc(level.toUpperCase()) + '</span>';
  }
"""

# ---------------------------------------------------------------------------
# index.html — Alert feed
# ---------------------------------------------------------------------------
INDEX_HTML = (
    """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>CAM \u2014 Alert Feed</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body { font-family: system-ui, -apple-system, sans-serif; max-width: 1100px;
           margin: 0 auto; padding: 1rem 1.5rem; color: #222; }
    header { border-bottom: 2px solid #333; padding-bottom: 0.5rem; margin-bottom: 1rem; }
    header h1 { margin: 0; font-size: 1.4rem; }
    nav a { margin-right: 1rem; text-decoration: none; color: #0066cc; }
    nav a:hover { text-decoration: underline; }
    .meta-bar { font-size: 0.82em; color: #666; margin-bottom: 1rem; }
    .alert-card { border-left: 5px solid #ccc; padding: 0.6rem 1rem;
                  margin: 0.5rem 0; border-radius: 0 4px 4px 0; }
    .critical { border-color: #c0392b; background: #fdf0ef; }
    .elevated  { border-color: #e67e22; background: #fdf5ec; }
    .watch     { border-color: #f1c40f; background: #fdfbec; }
    .entity-name { font-weight: 600; font-size: 1.05em; }
    .badge { display: inline-block; border-radius: 3px; padding: 1px 6px;
             font-size: 0.75em; font-weight: 700; color: #fff; margin-left: 0.4rem; }
    .badge-critical { background: #c0392b; }
    .badge-elevated  { background: #e67e22; }
    .badge-watch     { background: #d4aa00; }
    .details-row { font-size: 0.85em; color: #555; margin-top: 0.15rem; }
    #filter { padding: 0.4rem 0.6rem; width: 300px; margin-bottom: 0.75rem;
              border: 1px solid #ccc; border-radius: 4px; font-size: 0.95em; }
  </style>
</head>
<body>
  <header>
    <h1>Corporate Accountability Monitor</h1>
    <nav>
      <a href="index.html">Alerts</a>
      <a href="industries.html">Industries</a>
    </nav>
  </header>
  <div class="meta-bar" id="meta-bar">Loading\u2026</div>
  <input type="text" id="filter" placeholder="Filter by name or NAICS\u2026" oninput="applyFilter()">
  <div id="alerts-container"></div>
  <script>
"""
    + _SHARED_JS
    + """
    let allAlerts = [];

    function renderAlerts(alerts) {
      const c = document.getElementById('alerts-container');
      if (!alerts.length) { c.textContent = 'No active alerts match your filter.'; return; }
      const fragment = document.createDocumentFragment();
      for (const a of alerts) {
        const div = document.createElement('div');
        div.className = 'alert-card ' + esc(a.alert_level || '');

        const nameDiv = document.createElement('div');
        nameDiv.className = 'entity-name';

        const link = document.createElement('a');
        link.href = 'entity.html?id=' + encodeURIComponent(a.entity_id);
        link.textContent = a.canonical_name || a.entity_id;
        nameDiv.appendChild(link);
        nameDiv.insertAdjacentHTML('beforeend', badge(a.alert_level));

        const detailDiv = document.createElement('div');
        detailDiv.className = 'details-row';
        detailDiv.textContent = 'Score: ' + (a.composite_score * 100).toFixed(1) + '%'
          + ' \u00b7 Date: ' + (a.score_date || 'N/A')
          + ' \u00b7 NAICS: ' + (a.naics_code || 'N/A');

        div.appendChild(nameDiv);
        div.appendChild(detailDiv);
        fragment.appendChild(div);
      }
      c.textContent = '';
      c.appendChild(fragment);
    }

    function applyFilter() {
      const q = document.getElementById('filter').value.toLowerCase();
      renderAlerts(q ? allAlerts.filter(a =>
        (a.canonical_name || '').toLowerCase().includes(q) ||
        (a.naics_code || '').includes(q)) : allAlerts);
    }

    Promise.all([fetch('alerts.json').then(r => r.json()),
                 fetch('meta.json').then(r => r.json())])
      .then(function(results) {
        const alerts = results[0], meta = results[1];
        allAlerts = alerts;
        document.getElementById('meta-bar').textContent =
          'Exported: ' + meta.exported_at
          + ' \u00b7 ' + meta.entity_count + ' entities'
          + ' \u00b7 ' + meta.alert_count + ' active alerts';
        renderAlerts(alerts);
      })
      .catch(function() {
        document.getElementById('meta-bar').textContent =
          'Error loading data. Serve this directory with: python -m http.server 8000';
      });
  </script>
</body>
</html>
"""
)

# ---------------------------------------------------------------------------
# entity.html — Entity detail
# ---------------------------------------------------------------------------
ENTITY_HTML = (
    """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>CAM \u2014 Entity Detail</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body { font-family: system-ui, -apple-system, sans-serif; max-width: 1100px;
           margin: 0 auto; padding: 1rem 1.5rem; color: #222; }
    header { border-bottom: 2px solid #333; padding-bottom: 0.5rem; margin-bottom: 1rem; }
    nav a { text-decoration: none; color: #0066cc; margin-right: 1rem; }
    nav a:hover { text-decoration: underline; }
    h2 { font-size: 1.1rem; margin: 1.2rem 0 0.4rem; border-bottom: 1px solid #eee; padding-bottom: 0.2rem; }
    table { border-collapse: collapse; width: 100%; margin-bottom: 1rem; }
    th, td { border: 1px solid #ddd; padding: 0.35rem 0.6rem; text-align: left; font-size: 0.9em; }
    th { background: #f4f4f4; font-weight: 600; }
    .score-big { font-size: 2rem; font-weight: 700; }
    .badge { display: inline-block; border-radius: 3px; padding: 2px 8px;
             font-size: 0.8em; font-weight: 700; color: #fff; margin-left: 0.4rem; vertical-align: middle; }
    .badge-critical { background: #c0392b; }
    .badge-elevated  { background: #e67e22; }
    .badge-watch     { background: #d4aa00; }
    .meta-row { color: #666; font-size: 0.88em; margin-bottom: 0.5rem; }
  </style>
</head>
<body>
  <header>
    <nav><a href="index.html">\u2190 Alert Feed</a> | <a href="industries.html">Industries</a></nav>
  </header>
  <div id="content"><p>Loading\u2026</p></div>
  <script>
"""
    + _SHARED_JS
    + """
    const id = new URLSearchParams(location.search).get('id');

    function makeRow(cells) {
      const tr = document.createElement('tr');
      for (const text of cells) {
        const td = document.createElement('td');
        td.textContent = text == null ? '\u2014' : String(text);
        tr.appendChild(td);
      }
      return tr;
    }

    function makeTable(headers, rows) {
      const table = document.createElement('table');
      const thead = document.createElement('thead');
      const hr = document.createElement('tr');
      for (const h of headers) {
        const th = document.createElement('th');
        th.textContent = h;
        hr.appendChild(th);
      }
      thead.appendChild(hr);
      table.appendChild(thead);
      const tbody = document.createElement('tbody');
      for (const r of rows) tbody.appendChild(r);
      table.appendChild(tbody);
      return table;
    }

    if (!id) {
      document.getElementById('content').textContent =
        'No entity ID in URL. Go to the alert feed to select an entity.';
    } else {
      fetch('entities/' + encodeURIComponent(id) + '.json')
        .then(function(r) { if (!r.ok) throw new Error(r.status); return r.json(); })
        .then(function(e) {
          document.title = 'CAM \u2014 ' + (e.canonical_name || id);
          const cur = e.current_score;
          const score = cur ? (cur.composite_score * 100).toFixed(1) + '%' : 'N/A';
          const level = cur ? cur.alert_level : null;

          const content = document.getElementById('content');
          content.textContent = '';

          const h1 = document.createElement('h1');
          h1.textContent = e.canonical_name || id;
          h1.insertAdjacentHTML('beforeend', badge(level));
          content.appendChild(h1);

          const meta = document.createElement('p');
          meta.className = 'meta-row';
          meta.textContent = 'Composite score: ';
          const scoreBig = document.createElement('span');
          scoreBig.className = 'score-big';
          scoreBig.textContent = score;
          meta.appendChild(scoreBig);
          meta.append(
            ' \u00b7 NAICS: ' + (e.naics_code || 'N/A')
            + ' \u00b7 Ticker: ' + (e.ticker || 'N/A')
            + ' \u00b7 As of: ' + (cur ? cur.score_date : 'N/A')
          );
          content.appendChild(meta);

          // Component breakdown
          const compH = document.createElement('h2');
          compH.textContent = 'Component Breakdown';
          content.appendChild(compH);
          const compScores = cur && cur.component_scores ? cur.component_scores : {};
          const compRows = Object.entries(compScores)
            .sort(function(a, b) { return b[1] - a[1]; })
            .map(function(kv) { return makeRow([kv[0], (kv[1] * 100).toFixed(1) + '%']); });
          content.appendChild(makeTable(['Component', 'Score'],
            compRows.length ? compRows : [makeRow(['No component data', ''])]));

          // Evidence
          const evH = document.createElement('h2');
          evH.textContent = 'Evidence (top signals)';
          content.appendChild(evH);
          const evRows = (e.top_evidence || []).map(function(ev) {
            return makeRow([ev.signal_type, ev.evidence || '', ev.signal_date || null]);
          });
          content.appendChild(makeTable(['Source', 'Evidence', 'Date'],
            evRows.length ? evRows : [makeRow(['No evidence', '', ''])]));

          // Score history
          const histH = document.createElement('h2');
          histH.textContent = 'Score History (last 30 entries)';
          content.appendChild(histH);
          const histRows = (e.score_history || []).slice(0, 30).map(function(h) {
            return makeRow([h.score_date, (h.composite_score * 100).toFixed(1) + '%', h.alert_level || null]);
          });
          content.appendChild(makeTable(['Date', 'Score', 'Level'],
            histRows.length ? histRows : [makeRow(['No history', '', ''])]));
        })
        .catch(function() {
          document.getElementById('content').textContent =
            'Entity not found. Make sure the export has run and you are serving '
            + 'from the output directory (python -m http.server 8000).';
        });
    }
  </script>
</body>
</html>
"""
)

# ---------------------------------------------------------------------------
# industries.html — Industry grouping
# ---------------------------------------------------------------------------
INDUSTRIES_HTML = (
    """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>CAM \u2014 Industry View</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body { font-family: system-ui, -apple-system, sans-serif; max-width: 1100px;
           margin: 0 auto; padding: 1rem 1.5rem; color: #222; }
    header { border-bottom: 2px solid #333; padding-bottom: 0.5rem; margin-bottom: 1rem; }
    header h1 { margin: 0; font-size: 1.4rem; }
    nav a { text-decoration: none; color: #0066cc; margin-right: 1rem; }
    nav a:hover { text-decoration: underline; }
    #filter { padding: 0.4rem 0.6rem; width: 320px; margin-bottom: 0.75rem;
              border: 1px solid #ccc; border-radius: 4px; font-size: 0.95em; }
    details { border: 1px solid #ddd; margin: 0.4rem 0; border-radius: 4px; }
    summary { padding: 0.5rem 1rem; cursor: pointer; background: #f7f7f7;
              font-weight: 600; user-select: none; list-style: none; }
    summary::-webkit-details-marker { display: none; }
    summary::before { content: '\u25b6\u00a0'; font-size: 0.75em; }
    details[open] > summary::before { content: '\u25bc\u00a0'; }
    summary:hover { background: #eee; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; padding: 0.3rem 0.6rem; font-size: 0.88em; text-align: left; }
    th { background: #f4f4f4; }
    .critical { color: #c0392b; font-weight: 600; }
    .elevated  { color: #e67e22; font-weight: 600; }
    .watch     { color: #b8860b; font-weight: 600; }
  </style>
</head>
<body>
  <header>
    <h1>CAM \u2014 Industry View</h1>
    <nav>
      <a href="index.html">Alerts</a>
      <a href="industries.html">Industries</a>
    </nav>
  </header>
  <input type="text" id="filter" placeholder="Filter by NAICS code or company name\u2026"
         oninput="applyFilter()">
  <div id="groups"><p style="color:#888">Loading\u2026</p></div>
  <script>
"""
    + _SHARED_JS
    + """
    let allEntities = [];

    function naicsGroup(e) {
      return ((e.naics_code || '') + '').slice(0, 2) || 'N/A';
    }

    function renderGroups(entities) {
      const byNaics = {};
      for (const e of entities) {
        const k = naicsGroup(e);
        (byNaics[k] = byNaics[k] || []).push(e);
      }
      const sorted = Object.entries(byNaics).sort(function(a, b) {
        function avg(arr) { return arr.reduce(function(s, e) { return s + (e.composite_score || 0); }, 0) / arr.length; }
        return avg(b[1]) - avg(a[1]);
      });

      const g = document.getElementById('groups');
      g.textContent = '';
      if (!sorted.length) {
        const p = document.createElement('p');
        p.style.color = '#888';
        p.textContent = 'No entities match.';
        g.appendChild(p);
        return;
      }

      for (const entry of sorted) {
        const naics = entry[0], ents = entry[1];
        const avg = ents.reduce(function(s, e) { return s + (e.composite_score || 0); }, 0) / ents.length;

        const details = document.createElement('details');
        const summary = document.createElement('summary');
        summary.textContent = 'NAICS ' + naics + ' \u2014 ' + ents.length
          + (ents.length === 1 ? ' entity' : ' entities')
          + ', avg score ' + (avg * 100).toFixed(1) + '%';
        details.appendChild(summary);

        const table = document.createElement('table');
        const thead = document.createElement('thead');
        const hr = document.createElement('tr');
        for (const h of ['Entity', 'Score', 'Level', 'Date']) {
          const th = document.createElement('th');
          th.textContent = h;
          hr.appendChild(th);
        }
        thead.appendChild(hr);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        for (const e of ents) {
          const tr = document.createElement('tr');

          const td1 = document.createElement('td');
          const link = document.createElement('a');
          link.href = 'entity.html?id=' + encodeURIComponent(e.id);
          link.textContent = e.canonical_name || e.id;
          td1.appendChild(link);

          const td2 = document.createElement('td');
          td2.textContent = e.composite_score != null
            ? (e.composite_score * 100).toFixed(1) + '%' : 'N/A';

          const td3 = document.createElement('td');
          td3.className = e.alert_level || '';
          td3.textContent = e.alert_level || '\u2014';

          const td4 = document.createElement('td');
          td4.textContent = e.score_date || '\u2014';

          tr.appendChild(td1); tr.appendChild(td2);
          tr.appendChild(td3); tr.appendChild(td4);
          tbody.appendChild(tr);
        }
        table.appendChild(tbody);
        details.appendChild(table);
        g.appendChild(details);
      }
    }

    function applyFilter() {
      const q = document.getElementById('filter').value.toLowerCase();
      renderGroups(q ? allEntities.filter(function(e) {
        return (e.canonical_name || '').toLowerCase().includes(q)
            || naicsGroup(e).includes(q);
      }) : allEntities);
    }

    fetch('entities.json')
      .then(function(r) { return r.json(); })
      .then(function(entities) {
        allEntities = entities.sort(function(a, b) {
          return (b.composite_score || 0) - (a.composite_score || 0);
        });
        renderGroups(allEntities);
      })
      .catch(function() {
        document.getElementById('groups').textContent =
          'Error loading data. Serve from the output directory: python -m http.server 8000';
      });
  </script>
</body>
</html>
"""
)

# Map of filename -> HTML content for all dashboard pages
HTML_PAGES: dict[str, str] = {
    "index.html": INDEX_HTML,
    "entity.html": ENTITY_HTML,
    "industries.html": INDUSTRIES_HTML,
}
