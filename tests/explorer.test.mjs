import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {ALL, defaults, isDate, validateData, validateState, filterRows, summarize, percentChange, formatPercentChange, stateFromURL, stateToURL, exportCSV, exportJSON} from '../docs/explorer/model.mjs';
const folder = new URL('../docs/explorer/data/', import.meta.url);
const data = JSON.parse(readFileSync(new URL('data.json', folder)));
const manifest = JSON.parse(readFileSync(new URL('manifest.json', folder)));

test('published bytes, source identities and totals agree', () => {
  assert.equal(createHash('sha256').update(readFileSync(new URL('../taxi_platform/analyst.py', import.meta.url))).digest('hex'), manifest.exporter_sha256);
  for (const file of manifest.files) assert.equal(createHash('sha256').update(readFileSync(new URL(file.file, folder))).digest('hex'), file.sha256);
  assert.equal(validateData(data, manifest), data);
  for (const mutate of [d => d.release_id = 'other', d => d.inputs[0].sha256 = 'changed', d => d.rows[0].trips++, d => d.rows.push(d.rows[0])]) {
    const bad = structuredClone(data); mutate(bad); assert.throws(() => validateData(bad, manifest));
  }
});

test('hand arithmetic: filtered periods, cents, missing versus measured zero', () => {
  const fixture = {...data, coverage: {boroughs: ['Manhattan', 'Queens']}, rows: [
    {pickup_date: '2023-01-01', pickup_borough: 'Manhattan', trips: 2, total_amount_cents: 1011},
    {pickup_date: '2023-01-02', pickup_borough: 'Manhattan', trips: 3, total_amount_cents: 2002},
    {pickup_date: '2023-01-01', pickup_borough: 'Queens', trips: 4, total_amount_cents: 777},
    {pickup_date: '2023-02-01', pickup_borough: 'Manhattan', trips: 1, total_amount_cents: 2222},
  ]};
  const state = {...defaults(fixture), borough: 'Manhattan'};
  assert.deepEqual(summarize(filterRows(fixture, state)), {observed: true, trips: 5, total_amount_cents: 3013, observedDays: 2});
  assert.equal(summarize(filterRows(fixture, state, true)).trips, 1);
  assert.equal(percentChange(5,1), 400);
  assert.equal(percentChange(0,5), -100);
  assert.equal(percentChange(5,0), null);
  assert.equal(percentChange(null,5), null);
  assert.deepEqual(summarize([]), {observed: false, trips: null, total_amount_cents: null, observedDays: 0});
  assert.equal(summarize([{pickup_date: '2023-01-01', trips: 0, total_amount_cents: 0}]).trips, 0);
  const csv = exportCSV(fixture, state);
  assert.equal(csv.trim().split('\r\n').length, 3);
  assert.ok(csv.includes('"10.11"') && csv.includes('"20.02"'));
  assert.ok(!csv.includes('Queens') && !csv.includes('22.22'));
  const json = exportJSON(fixture, state);
  assert.equal(json.current.totals.total_amount_cents, 3013);
  assert.equal(json.comparison.totals.total_amount_cents, 2222);
  assert.deepEqual(json.inputs, data.inputs);
});

test('URL round trips inclusive filters and rejects ambiguous or impossible dates', () => {
  const state = {...defaults(data), borough: 'Manhattan', start: '2023-01-01', end: '2023-01-01'};
  assert.deepEqual(stateFromURL(`?${stateToURL(state)}`, data), state);
  assert.equal(isDate('2023-02-30'), false);
  assert.equal(isDate('2024-02-29'), true);
  assert.throws(() => stateFromURL('?start=2023-02-30', data), /start/);
  assert.throws(() => stateFromURL('?borough=imaginary', data), /borough/);
  assert.throws(() => stateFromURL('?start=2023-02-01&start=2023-01-01', data), /Duplicate/);
  assert.throws(() => stateFromURL('?release=other', data), /different release/);
  assert.throws(() => validateState({...state, start: '2023-02-01'}, data), /start must/);
  assert.ok(filterRows(data, state).every(row => row.pickup_date === '2023-01-01' && row.pickup_borough === 'Manhattan'));
});

test('no-data periods are not zero, exports preserve scope and no phantom rows', () => {
  const state = {...defaults(data), start: '2024-01-01', end: '2024-01-31', borough: ALL};
  assert.equal(filterRows(data, state).length, 0);
  assert.equal(exportCSV(data, state).trim().split('\r\n').length, 1);
  const json = exportJSON(data, state);
  assert.equal(json.current.totals.trips, null);
  assert.equal(json.current.rows.length, 0);
  assert.equal(json.scope, data.scope);
});

test('tiny changes round to unsigned zero while material changes retain direction', () => {
  assert.equal(formatPercentChange(-0.0001), '0.0%');
  assert.equal(formatPercentChange(0.049), '0.0%');
  assert.equal(formatPercentChange(-0.051), '-0.1%');
  assert.equal(formatPercentChange(0.051), '+0.1%');
});
