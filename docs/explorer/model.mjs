export const ALL = 'All boroughs';
export const SAMPLE_HASHES = {
  '2023-01': 'f517dc1958e3bfc29c74a7fb7c7e1c112e2fca05c48e06c970d6ab3aab2f6558',
  '2023-02': 'ef77b0d663ca4927a2cd9fc12ab77fd3d55252b12a5673ef37581ca260a730bd',
};

export function isDate(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value)
    && !Number.isNaN(Date.parse(`${value}T00:00:00Z`))
    && new Date(`${value}T00:00:00Z`).toISOString().slice(0, 10) === value;
}

export function validateData(data, manifest) {
  if (data.schema_version !== 1 || !Array.isArray(data.rows) || !data.rows.length || data.rows.length > 10000)
    throw Error('Unsupported or empty release. Reload the published sample.');
  if (!manifest || data.release_id !== manifest.release_id || data.model_sha256 !== manifest.model_sha256
      || JSON.stringify(data.inputs) !== JSON.stringify(manifest.inputs)) throw Error('Release identity mismatch.');
  if (data.inputs.length !== 2 || data.inputs.some(input => input.sha256 !== SAMPLE_HASHES[input.source_month] || input.rows !== 15000)
      || new Set(data.inputs.map(input => input.source_month)).size !== 2) throw Error('This explorer requires the two documented bundled samples.');
  const seen = new Set();
  for (const row of data.rows) {
    const key = `${row.pickup_date}|${row.pickup_borough}`;
    if (!isDate(row.pickup_date) || !Object.hasOwn(SAMPLE_HASHES, row.pickup_date.slice(0, 7))
        || typeof row.pickup_borough !== 'string' || !row.pickup_borough.length || seen.has(key)
        || !Number.isSafeInteger(row.trips) || row.trips < 0
        || !Number.isSafeInteger(row.total_amount_cents) || row.total_amount_cents < 0) throw Error(`Invalid daily-borough record: ${key}`);
    seen.add(key);
  }
  const totals = summarize(data.rows);
  if (totals.trips !== data.totals.trips || totals.total_amount_cents !== data.totals.total_amount_cents
      || totals.trips !== data.quality.valid_rows || data.quality.input_rows !== 30000
      || data.quality.valid_rows + data.quality.rejected_rows !== 30000
      || totals.trips !== manifest.reconciliation.trips || totals.total_amount_cents !== manifest.reconciliation.total_amount_cents)
    throw Error('Release totals do not reconcile.');
  return data;
}

export function defaults(data) {
  return {start: '2023-01-01', end: '2023-01-31', compareStart: '2023-02-01', compareEnd: '2023-02-28', borough: ALL, release: data.release_id};
}

export function validateState(state, data) {
  for (const key of ['start', 'end', 'compareStart', 'compareEnd']) if (!isDate(state[key])) throw Error(`${key}: enter a valid date.`);
  if (state.start > state.end) throw Error('Current period: start must be on or before end.');
  if (state.compareStart > state.compareEnd) throw Error('Comparison period: start must be on or before end.');
  if (state.borough !== ALL && !data.coverage.boroughs.includes(state.borough)) throw Error('borough: choose a borough in this release.');
  if (state.release !== data.release_id) throw Error('This link refers to a different release. Use Reset to view the available sample.');
  return state;
}

export function stateFromURL(search, data) {
  const state = defaults(data), params = new URLSearchParams(search);
  for (const key of Object.keys(state)) {
    if (params.getAll(key).length > 1) throw Error(`Duplicate URL field: ${key}.`);
    if (params.has(key)) state[key] = params.get(key);
  }
  return validateState(state, data);
}

export function stateToURL(state) { return new URLSearchParams(state).toString(); }

export function filterRows(data, state, comparison = false) {
  validateState(state, data);
  const start = comparison ? state.compareStart : state.start;
  const end = comparison ? state.compareEnd : state.end;
  return data.rows.filter(row => row.pickup_date >= start && row.pickup_date <= end
    && (state.borough === ALL || row.pickup_borough === state.borough));
}

export function summarize(rows) {
  if (!rows.length) return {observed: false, trips: null, total_amount_cents: null, observedDays: 0};
  const result = {observed: true, trips: 0, total_amount_cents: 0, observedDays: new Set(rows.map(row => row.pickup_date)).size};
  for (const row of rows) {
    result.trips += row.trips;
    result.total_amount_cents += row.total_amount_cents;
  }
  if (!Number.isSafeInteger(result.trips) || !Number.isSafeInteger(result.total_amount_cents)) throw Error('Unsafe aggregate integer.');
  return result;
}

export function percentChange(current, previous) {
  return current === null || previous === null || previous === 0 ? null : (current - previous) / previous * 100;
}

export function formatPercentChange(change) {
  const rounded = change.toFixed(1);
  return Number(rounded) === 0 ? '0.0%' : `${change > 0 ? '+' : ''}${rounded}%`;
}

export function periodDays(start, end) { return (Date.parse(end) - Date.parse(start)) / 86400000 + 1; }
export function amountUSD(cents) { return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, '0')}`; }
function csvCell(value) { return `"${String(value).replaceAll('"', '""')}"`; }

export function exportCSV(data, state) {
  const fields = ['release_id', 'period_start', 'period_end', 'borough_filter', 'pickup_date', 'pickup_borough', 'trips', 'total_amount_cents', 'recorded_total_amount_usd'];
  const rows = filterRows(data, state).map(row => [data.release_id, state.start, state.end, state.borough, row.pickup_date, row.pickup_borough, row.trips, row.total_amount_cents, amountUSD(row.total_amount_cents)]);
  return [fields, ...rows].map(row => row.map(csvCell).join(',')).join('\r\n') + '\r\n';
}

export function exportJSON(data, state) {
  const rows = filterRows(data, state), comparisonRows = filterRows(data, state, true);
  return {release_id: data.release_id, model_sha256: data.model_sha256, inputs: data.inputs,
    scope: data.scope, filters: state, current: {totals: summarize(rows), rows},
    comparison: {totals: summarize(comparisonRows), rows: comparisonRows}};
}
