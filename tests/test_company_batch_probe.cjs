// Actual browser main against an inert context. No source access or profile use.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const probePath = path.resolve(__dirname, '../scripts/probe-linkedin-company.cjs');
const probe = require(probePath), helper = require('../scripts/company-bootstrap.cjs');
const target = 'https://www.linkedin.com/company/linkedin/posts/';
const recipe = 'feedDashOrganizationalPageUpdatesByOrganizationalPageRelevanceFeed';
const urlFor = start => 'https://www.linkedin.com/voyager/api/graphql?queryId=voyagerFeedDashOrganizationalPageUpdates.synthetic&variables=(organizationalPageUrn:urn:li:fsd_organizationalPage:1337,start:' + start + ',count:10)';
function body(start, count) {
  const roots = Array.from({length: count}, (_, i) => 'urn:li:fsd_update:' + (start + i + 1));
  return {data: {data: {[recipe]: {$type: 'com.linkedin.restli.common.CollectionResponse', '*elements': roots,
    paging: {start, count, total: 230}}}}, included: roots.map(entityUrn => ({entityUrn,
      $type: 'com.linkedin.voyager.dash.feed.Update', commentary: {text: 'synthetic publication'}}))};
}
function html() {
  const wrapper = {method: 'GET', status: 200, request: '/voyager/api/graphql?queryId=voyagerFeedDashOrganizationalPageUpdates.synthetic&variables=(organizationalPageUrn:urn:li:fsd_organizationalPage:1337)',
    body: 'private-inert-reference', headers: {cookie: 'private-session-header'}};
  return `<code>${JSON.stringify(wrapper)}</code><code id="private-inert-reference">${JSON.stringify(body(0, 3)).replaceAll('"', '&quot;')}</code>`;
}
async function run(budget, fault = null) {
  let observer, router, continuation, scrolls = 0, closed = 0, launches = 0;
  const output = [], writes = [], routes = [], frame = {};
  const request = (url, main = false, method = 'GET') => ({url: () => url, method: () => method,
    isNavigationRequest: () => main, frame: () => frame});
  const response = (url, value, main = false, status = 200) => ({url: () => url,
    request: () => request(url, main), status: () => status,
    headerValue: async () => main ? 'text/html' : 'application/json', body: async () => Buffer.from(value)});
  async function emit(url, value, main = false, status = 200, method = 'GET') {
    let action;
    await router({request: () => request(url, main, method), abort: async () => {action = 'abort';},
      continue: async () => {action = 'continue';}});
    routes.push({url, method, action});
    if (action === 'continue') { const valueResponse = response(url, value, main, status); observer(valueResponse); return valueResponse; }
  }
  const page = {mainFrame: () => frame, on: (_, fn) => {observer = fn;}, url: () => target,
    goto: async () => {
      const navigation = await emit(target, html(), true);
      await emit('https://www.linkedin.com/voyager/api/graphql?queryId=voyagerOrganizationDashCompanies.synthetic',
        JSON.stringify({included: [{entityUrn: 'urn:li:fsd_company:1337'}]}));
      await emit(fault === 'wrong_scope' ? urlFor(13) : urlFor(3), JSON.stringify(body(3, 10)));
      if (fault === 'budget') for (let i = 0; i < 15; i++) await emit('https://www.linkedin.com/voyager/api/graphql?queryId=voyagerOrganizationDashCompanies.synthetic', '{}');
      if (fault === 'write') await emit(target, '{}', false, 200, 'POST');
      return navigation;
    }, waitForTimeout: async () => {}, waitForResponse: async () => {
      if (fault === 'missing') throw new Error('synthetic absent continuation');
      return new Promise(resolve => {continuation = resolve;});
    }, evaluate: async fn => {
      if (String(fn).includes('scrollTo')) { scrolls++; if (fault === 'missing') return; const observed = await emit(urlFor(13), JSON.stringify(body(13, 10))); if (continuation) continuation(observed); return; }
      return String(fn).includes('signed_in:') ? {signed_in: true, warning: false, signin: false} : false;
    }};
  const context = {pages: () => [page], route: async (_, fn) => {router = fn;}, close: async () => {
    if (++closed === 1 && fault === 'late_security') observer(response(urlFor(13), '{"errors":[{"code":"CHALLENGE"}]}'));
  }};
  const fakeRequire = value => value === 'node:fs' ? {existsSync: name => !String(name).endsWith('linkedin-acquisition-hold.json'),
    writeFileSync: name => writes.push(String(name))} : value === './company-bootstrap.cjs' ? helper :
    String(value).endsWith('playwright') ? {chromium: {launchPersistentContext: async () => {launches++; return context;}}} : require(value);
  fakeRequire.main = {};
  const sandbox = {module: {exports: {}}, require: fakeRequire, __dirname: path.dirname(probePath), Buffer, URL, setTimeout, clearTimeout,
    process: {argv: ['node', probePath, target, '--runtime-capture', '--runtime-company-batch', '--batch-page-budget=' + budget,
      '--bootstrap-deadline=' + new Date(Date.now() + 60000).toISOString()], env: {}},
    console: {log: value => output.push(JSON.parse(value)), error: () => {}}};
  const source = fs.readFileSync(probePath, 'utf8').replace('require.main === module ? parseProbeArgs(process.argv.slice(2)) : parseProbeArgs([])', 'parseProbeArgs(process.argv.slice(2))');
  vm.runInNewContext(source + '\nmodule.exports.testMain = main;', sandbox);
  await sandbox.module.exports.testMain();
  assert.equal(output.length, 1); assert.equal(launches, 1); assert.equal(closed, 1);
  assert.ok(writes.every(name => name.endsWith('linkedin-acquisition-hold.json')));
  assert.ok(!JSON.stringify(output).includes('private-inert-reference'));
  assert.ok(!JSON.stringify(output).includes('private-session-header'));
  return {receipt: output[0], scrolls, routes};
}
test('explicit batch flags remain bounded and reject mixed or missing scope', () => {
  const flags = ['--runtime-company-batch', '--runtime-capture', '--batch-page-budget=3', '--bootstrap-deadline=2026-10-06T23:59:59Z'];
  assert.equal(probe.parseProbeArgs(flags).batchPageBudget, 3);
  for (const args of [flags.slice(1), flags.filter(x => !x.startsWith('--batch-page-budget')), [...flags, '--runtime-initial-page'],
    [...flags, '--continuation'], [...flags, '--boundary'], [...flags, '--bootstrap'], [...flags, '--batch-page-budget=2'],
    flags.map(x => x === '--batch-page-budget=3' ? '--batch-page-budget=4' : x), [...flags, target.replace('linkedin/posts', 'other/posts')]])
    assert.throws(() => probe.parseProbeArgs(args));
});
test('strict batch request scope refuses duplicate or foreign fields', () => {
  assert.deepEqual(probe.batchRequestScope(new URL(urlFor(13))), {start: 13, count: 10});
  for (const value of [urlFor(3).replace('1337', '1338'), urlFor(3).replace('count:10', 'count:10,count:10'),
    urlFor(3).replace('start:3', 'start:3,start:13'), urlFor(3).replace('count:10', 'count:3')])
    assert.throws(() => probe.batchRequestScope(new URL(value)));
});
test('actual main keeps distinct source pages and only one optional ordinary scroll', async () => {
  for (const budget of [2, 3]) {
    const {receipt, scrolls} = await run(budget);
    assert.equal(scrolls, budget - 2); assert.equal(receipt.session_verified, true);
    assert.deepEqual(receipt.batch_pages.map(row => [row.start, row.count]), [[0, 3], [3, 10], [13, 10]].slice(0, budget));
    assert.equal(receipt.batch_shortfall_reason, null); assert.equal(receipt.native_reads_admitted, budget);
  }
});
test('missing ordinary continuation retains captured pages without terminal inference', async () => {
  const {receipt, scrolls} = await run(3, 'missing');
  assert.equal(scrolls, 1); assert.equal(receipt.batch_pages.length, 2);
  assert.equal(receipt.batch_shortfall_reason, 'continuation_not_observed');
  assert.equal(receipt.failure_kind, undefined);
});
test('buffered security intervention releases no pages and preserves hold', async () => {
  const {receipt} = await run(3, 'late_security');
  assert.equal(receipt.stopped.security_hold, true); assert.deepEqual(receipt.batch_pages, []);
  assert.deepEqual(receipt.responses, []); assert.equal(receipt.session_verified, false);
});
test('batch keeps global read/write guards and rejects wrong response scope', async () => {
  const limited = await run(2, 'budget'); assert.equal(limited.receipt.native_reads_admitted, 12);
  assert.ok(limited.routes.some(row => row.action === 'abort'));
  const writes = await run(2, 'write'); assert.equal(writes.receipt.blocked_nonread_requests, 1);
  const wrong = await run(2, 'wrong_scope'); assert.equal(wrong.receipt.batch_pages.length, 0);
});
