import {ALL, defaults, validateData, validateState, stateFromURL, stateToURL, filterRows, summarize, percentChange, formatPercentChange, periodDays, exportCSV, exportJSON} from './model.mjs';
const $ = id => document.getElementById(id);
const fmt = value => new Intl.NumberFormat('en-US').format(value);
const money = cents => new Intl.NumberFormat('en-US', {style: 'currency', currency: 'USD'}).format(cents / 100);
const element = (tag, text, className) => {const node = document.createElement(tag); node.textContent = text; if (className) node.className = className; return node;};
function error(message) {$('error').textContent = message; $('error').hidden = false;}
function clearError() {$('error').hidden = true;}
let data, state;

function setForm() {
  for (const key of ['start', 'end', 'compareStart', 'compareEnd', 'borough']) $('filters').elements[key].value = state[key];
}

function metric(label, current, previous, format, detail) {
  const card = element('article', '', 'metric');
  card.append(element('p', label, 'metric-label'));
  card.append(element('p', current === null ? 'No observations' : format(current), `metric-value${current === null ? ' missing' : ''}`));
  const line = element('p', previous === null ? 'Comparison: no observations' : `Comparison: ${format(previous)}`, 'metric-compare');
  const delta = percentChange(current, previous);
  if (delta !== null && !detail) line.append(element('strong', ` · ${formatPercentChange(delta)}`));
  card.append(line);
  return card;
}

function render() {
  $('share-panel').hidden = true;
  const rows = filterRows(data, state), comparison = filterRows(data, state, true);
  const current = summarize(rows), previous = summarize(comparison);
  $('selection-title').textContent = state.borough;
  $('period-caption').textContent = `${state.start} – ${state.end} compared with ${state.compareStart} – ${state.compareEnd} · NYC local pickup dates`;
  $('summary').replaceChildren(metric('Observed valid trips', current.trips, previous.trips, fmt),
    metric('Recorded total amount · USD', current.total_amount_cents, previous.total_amount_cents, money),
    metric('Dates with observed rows', current.observed ? current.observedDays : null, previous.observed ? previous.observedDays : null, fmt, true));
  $('comparison-note').textContent = `Current period: ${periodDays(state.start, state.end)} calendar days; comparison: ${periodDays(state.compareStart, state.compareEnd)}. Percent change uses comparison-period totals; unavailable for missing observations or a zero denominator. Overlapping periods may include the same rows.`;
  $('coverage-note').textContent = `This selection has observed valid sample rows on ${current.observedDays} of ${periodDays(state.start, state.end)} selected calendar days. The full release contains ${data.coverage.observed_dates.length} observed dates across two source months.`;
  const byBorough = new Map();
  for (const row of rows) byBorough.set(row.pickup_borough, (byBorough.get(row.pickup_borough) || 0) + row.trips);
  const ordered = [...byBorough].sort((a,b) => b[1]-a[1] || a[0].localeCompare(b[0]));
  $('bars').replaceChildren();
  const max = Math.max(1, ...ordered.map(row => row[1]));
  for (const [borough, trips] of ordered) {
    const bar = element('div', '', 'bar-row'), track = element('div', '', 'bar-track'), fill = element('div', '', 'bar-fill');
    track.setAttribute('aria-hidden', 'true'); fill.style.width = `${100 * trips / max}%`; track.append(fill);
    bar.append(element('span', borough), track, element('span', fmt(trips), 'bar-number')); $('bars').append(bar);
  }
  if (!rows.length) $('bars').append(element('p', 'No observed sample rows for these filters.', 'muted'));
  $('rows').replaceChildren();
  for (const row of rows) {
    const tr = document.createElement('tr');
    tr.append(element('td', row.pickup_date), element('td', row.pickup_borough), element('td', fmt(row.trips), 'numeric'), element('td', money(row.total_amount_cents), 'numeric'));
    $('rows').append(tr);
  }
  $('table-caption').textContent = `${rows.length} observed date/borough groups · CSV contains only the current period; JSON includes both periods and source identity.`;
  $('empty').hidden = rows.length > 0;
  $('csv').disabled = !rows.length;
  $('json').disabled = false;
  $('results').hidden = false;
}

function download(content, type, extension) {
  const url = URL.createObjectURL(new Blob([content], {type})), a = document.createElement('a');
  a.href = url; a.download = `nyc-sample-${state.start}-${state.end}-${state.borough.toLowerCase().replaceAll(' ', '-')}.${extension}`;
  a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function init() {
  try {
    const [dataResponse, manifestResponse] = await Promise.all([fetch('data/data.json'), fetch('data/manifest.json')]);
    if (!dataResponse.ok || !manifestResponse.ok) throw Error('Sample files could not be loaded. Reload or serve docs with python -m http.server; opening file:// is unsupported.');
    const bytes = await dataResponse.arrayBuffer(), manifest = await manifestResponse.json();
    const digest = [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(b => b.toString(16).padStart(2,'0')).join('');
    if (digest !== manifest.files.find(file => file.file === 'data.json')?.sha256) throw Error('Data checksum mismatch. Reload the complete published release.');
    data = validateData(JSON.parse(new TextDecoder().decode(bytes)), manifest);
    for (const borough of [ALL, ...data.coverage.boroughs]) {const option = element('option', borough); option.value = borough; $('borough').append(option);}
    try {state = stateFromURL(location.search, data);} catch (exc) {state = defaults(data); error(`${exc.message} Showing the default sample selection.`);}
    setForm(); render();
    for (const [label, value] of [['Source rows', data.quality.input_rows], ['Valid trips', data.quality.valid_rows], ['Quarantined rows', data.quality.rejected_rows]]) {
      const group = document.createElement('div'); group.append(element('dt', label), element('dd', fmt(value))); $('quality').append(group);
    }
    $('scope-description').textContent = `${data.scope} The release has observations on ${data.coverage.observed_dates.length} dates from ${data.coverage.first_observed_date} to ${data.coverage.last_observed_date}, in ${data.coverage.daily_borough_rows} borough/date groups. Observation on a date does not mean all trips on that date are included. Quality counts above cover the whole release, independently of the active filters.`;
    $('release').textContent = `Release ${data.release_id} · Model SHA-256 ${data.model_sha256}. Counts and amounts reconcile exactly to the published facts; download checksums are recorded in the manifest.`;
    $('filters').hidden = false;
    $('loading').hidden = true;
  } catch (exc) {$('loading').hidden = true; error(`Unable to open the explorer: ${exc.message}`);}
}

$('filters').addEventListener('submit', event => {
  event.preventDefault();
  try {
    const candidate = {...Object.fromEntries(new FormData(event.currentTarget)), release: data.release_id};
    validateState(candidate, data); state = candidate; clearError(); render();
    history.replaceState(null, '', `${location.pathname}?${stateToURL(state)}`);
  } catch (exc) {error(`${exc.message} Previous results are still displayed.`);}
});
$('reset').addEventListener('click', () => {state = defaults(data); setForm(); clearError(); render(); history.replaceState(null, '', location.pathname);});
$('share').addEventListener('click', async () => {
  const url = `${location.origin}${location.pathname}?${stateToURL(state)}`;
  history.replaceState(null, '', url);
  $('share-url').value = url; $('share-panel').hidden = false; $('share-url').focus(); $('share-url').select();
  try {await navigator.clipboard.writeText(url); $('share').textContent = 'Link copied ✓'; setTimeout(() => {$('share').textContent = 'Copy share link';}, 2000);}
  catch {error('Clipboard access is unavailable. Copy the selected share link below to share this selection.');}
});
$('csv').addEventListener('click', () => download(exportCSV(data, state), 'text/csv;charset=utf-8', 'csv'));
$('json').addEventListener('click', () => download(JSON.stringify(exportJSON(data, state), null, 2) + '\n', 'application/json', 'json'));
addEventListener('popstate', () => {if (!data) return; try {state = stateFromURL(location.search, data); setForm(); clearError(); render();} catch (exc) {error(exc.message);}});
init();
