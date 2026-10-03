// Offline account-stop and evidence-fidelity checks; no browser/network launch.
const test = require('node:test');
const assert = require('node:assert/strict');
const { collectionProjection, describe, semanticSignal, parseProbeArgs, bootstrapReceipt, isExactBootstrapNavigation, initialPageProjection } = require('../scripts/probe-linkedin-company.cjs');
const recipe = 'feedDashOrganizationalPageUpdatesByOrganizationalPageRelevanceFeed';
const body = roots => ({data: {data: {[recipe]: {
  $type: 'com.linkedin.restli.common.CollectionResponse', '*elements': roots,
  paging: {start: 3, count: 10, total: 230},
}}}, included: [{entityUrn: 'urn:li:fsd_update:999'}]});

test('collection roots preserve exact source order beyond generic array projection limit', () => {
  const roots = Array.from({length: 10}, (_, index) => `urn:li:fsd_update:${index}`);
  assert.deepEqual(collectionProjection(body(roots)).fields['*elements'], roots);
});
test('source root list is never substituted from included entities or silently truncated', () => {
  assert.equal(collectionProjection({included: body([]).included}).status, 'recipe_absent_or_drifted');
  assert.equal(collectionProjection(body(['not a native reference'])).status, 'invalid_root_references');
  assert.equal(collectionProjection(body(Array(101).fill('urn:li:fsd_update:1'))).status, 'root_projection_limit');
});
test('projected graph resolves exact collection roots beyond unrelated included updates', () => {
  const wanted = 'urn:li:fsd_update:99';
  const unrelated = Array.from({length: 12}, (_, index) => ({
    entityUrn: `urn:li:fsd_update:${index}`, $type: 'com.linkedin.voyager.dash.feed.Update',
  }));
  const source = body([wanted]);
  source.included = [...unrelated, {entityUrn: wanted, $type: 'com.linkedin.voyager.dash.feed.Update'}];
  assert.deepEqual(describe(source).projected_graph.map(node => node.entityUrn), [wanted]);
});
test('projected graph seed order follows collection order and records missing roots', () => {
  const roots = ['urn:li:fsd_update:2', 'urn:li:fsd_update:1', 'urn:li:fsd_update:3'];
  const source = body(roots);
  source.included = [1, 2].map(id => ({
    entityUrn: `urn:li:fsd_update:${id}`, $type: 'com.linkedin.voyager.dash.feed.Update',
  }));
  const result = describe(source);
  assert.deepEqual(result.projected_graph.map(node => node.entityUrn), roots.slice(0, 2));
  assert.ok(result.unresolved_native_references.includes(roots[2]));
});
test('drifted collection does not invent graph roots from included updates', () => {
  const source = {included: [{entityUrn: 'urn:li:fsd_update:1', $type: 'com.linkedin.voyager.dash.feed.Update'}]};
  assert.deepEqual(describe(source).projected_graph, []);
});
test('exact roots retain the declared hundred-root bound and reachable references', () => {
  const roots = Array.from({length: 100}, (_, index) => `urn:li:fsd_update:${index}`);
  const source = body(roots);
  source.included = roots.map(entityUrn => ({entityUrn, $type: 'com.linkedin.voyager.dash.feed.Update'}));
  source.included[0]['*socialDetail'] = 'urn:li:fsd_socialDetail:1';
  source.included.push({entityUrn: 'urn:li:fsd_socialDetail:1',
    $type: 'com.linkedin.voyager.dash.feed.SocialDetail',
    '*totalSocialActivityCounts': 'urn:li:fsd_socialActivityCounts:1'});
  source.included.push({entityUrn: 'urn:li:fsd_socialActivityCounts:1', numLikes: 0});
  const result = describe(source);
  assert.deepEqual(result.collection_projection.fields['*elements'], roots);
  const graphIds = result.projected_graph.map(node => node.entityUrn);
  assert.ok(roots.every(identifier => graphIds.includes(identifier)));
  assert.ok(graphIds.includes('urn:li:fsd_socialDetail:1'));
  assert.ok(graphIds.includes('urn:li:fsd_socialActivityCounts:1'));
});
test('semantic security candidates stop only from explicit bounded error envelopes', () => {
  assert.equal(semanticSignal({data: {data: {errors: [{code: 'CHALLENGE'}]}}}).security, true);
  assert.equal(semanticSignal({error: {status: 429}}).security, true);
  assert.equal(semanticSignal({errors: [{code: 'AUTH_REQUIRED'}]}).reason, 'authentication_required');
});
test('post text, unknown error codes and ambiguous status values are not semantic stop evidence', () => {
  assert.equal(semanticSignal({included: [{text: 'CHALLENGE captcha account restricted', errors: [{status: 403}]}]}), null);
  assert.equal(semanticSignal({errors: [{code: {value: 'CHALLENGE'}, status: '429'}]}), null);
});

test('exact collection errors stop security and cannot become captured fields', () => {
  for (const error of [{code: 'CHALLENGE'}, {status: 403}]) {
    const source = body(['urn:li:fsd_update:999']);
    source.data.data[recipe].errors = [error];
    assert.equal(semanticSignal(source).security, true);
    assert.equal(collectionProjection(source).status, 'semantic_error');
    assert.equal(collectionProjection(source).fields, undefined);
  }
});
test('unknown nonempty errors at each supported envelope cannot be stripped into success', () => {
  for (const select of [source => source, source => source.data,
    source => source.data.data, source => source.data.data[recipe]]) {
    const source = body(['urn:li:fsd_update:999']);
    select(source).error = {code: 'UNKNOWN_SERVER_ERROR', message: 'private source text'};
    assert.equal(semanticSignal(source), null);
    assert.deepEqual(collectionProjection(source), {status: 'semantic_error'});
    assert.deepEqual(describe(source).projected_graph, []);
  }
});
test('authentication never masks a supported challenge or restriction', () => {
  const source = body([]);
  source.errors = [{code: 'AUTH_REQUIRED'}, {status: 403}];
  assert.equal(semanticSignal(source).security, true);
  source.errors = [{code: 'AUTH_REQUIRED'}];
  source.data.data[recipe].error = {code: 'CHALLENGE'};
  assert.equal(semanticSignal(source).security, true);
});
test('deep representation and token inputs refuse capture without stack overflow', () => {
  const deep = JSON.parse('{"elements":'.repeat(8000) + '{}' + '}'.repeat(8000));
  const result = describe(deep);
  assert.equal(result.traversal_bounded, true);
  assert.equal(result.collection_projection.status, 'traversal_projection_limit');
  const source = body([]);
  source.paginationToken = deep;
  const tokenResult = describe(source);
  assert.equal(tokenResult.traversal_bounded, true);
  assert.equal(tokenResult.collection_projection.status, 'traversal_projection_limit');
  assert.equal(tokenResult.continuation_candidates[0].value_sha256, null);
});
test('security errors after twenty entries and mixed statuses are not hidden by auth', () => {
  const source = body([]);
  source.errors = Array(21).fill({code: 'AUTH_REQUIRED'});
  source.errors.push({status: 401, code: 'RATE_LIMITED'});
  assert.equal(semanticSignal(source).security, true);
  for (const value of [[], {}, null, false, 0, '']) {
    source.errors = value;
    assert.equal(collectionProjection(source).status, 'captured');
  }
  source.errors = 'unclassified error';
  assert.deepEqual(collectionProjection(source), {status: 'semantic_error'});
});
test('wide traversal refuses capture and duplicate diagnostics preserve bounded first encounter order', () => {
  const source = body([]);
  source.elements = Array(80001).fill(null);
  const wide = describe(source);
  assert.equal(wide.traversal_bounded, true);
  assert.deepEqual(wide.collection_projection, {status: 'traversal_projection_limit'});
  const duplicateSource = body([]);
  const identifiers = Array.from({length: 35}, (_, index) => `urn:li:fsd_update:${index}`);
  duplicateSource.included = [...identifiers, ...identifiers, ...identifiers]
    .map(entityUrn => ({entityUrn}));
  assert.deepEqual(describe(duplicateSource).duplicate_entity_ids, identifiers.slice(0, 30));
  const clean = body([]);
  clean.paginationToken = {token: ['opaque', 1]};
  const hash = describe(clean).continuation_candidates[0];
  assert.match(hash.value_sha256, /^[a-f0-9]{64}$/);
  assert.equal(hash.hash_bounded, false);
});
test('bootstrap mode requires explicit deadline and rejects conflicting or unknown flags', () => {
  const deadline = '--bootstrap-deadline=2026-10-03T23:59:59+05:30';
  assert.equal(parseProbeArgs(['--bootstrap', '--runtime-capture', deadline]).bootstrap, true);
  for (const args of [['--bootstrap'], [deadline], ['--unknown'],
    ['--bootstrap', deadline, '--continuation'], ['--bootstrap', deadline, '--boundary'],
    ['--bootstrap', deadline, '--bootstrap'], ['--bootstrap', '--bootstrap-deadline=2026-10-03T23:59:59'],
    ['--bootstrap', deadline, 'https://www.linkedin.com/company/other/posts/']]) {
    assert.throws(() => parseProbeArgs(args));
  }
});
test('bootstrap receipt is exact metadata-only even with authored normal-mode fields in memory', () => {
  const receipt = {session_verified: true, stopped: null, failure_kind: null, native_reads_admitted: 2,
    navigation_status: 200, responses: [{text: 'private authored publication'}],
    dom: {cookie: 'private session'}, target: 'private extra', account_writes_blocked: true};
  const observation = {status: 'absent', candidates: []};
  assert.deepEqual(bootstrapReceipt(receipt, observation), {session_verified: true, stopped: null,
    failure_kind: null, native_reads_admitted: 2, navigation_status: 200,
    account_writes_blocked: true, bootstrap_observation: observation});
  receipt.stopped = {reason: 'possible_semantic_challenge', security_hold: true, pathname: 'private', observed_at: 'private'};
  const stopped = bootstrapReceipt(receipt, observation);
  assert.deepEqual(stopped.stopped, {reason: 'challenge', security_hold: true});
  assert.equal(stopped.session_verified, false);
  receipt.failure_kind = 'RangeError';
  assert.equal(bootstrapReceipt(receipt, observation).failure_kind, 'Error');
  receipt.stopped = null;
  for (const failure of ['BootstrapLimit', 'Error', 'DeadlineElapsed']) {
    receipt.failure_kind = failure;
    assert.equal(bootstrapReceipt(receipt, observation).session_verified, false);
  }
});
test('bootstrap document provenance requires the exact declared origin and company path', () => {
  assert.equal(isExactBootstrapNavigation('https://www.linkedin.com/company/linkedin/posts/'), true);
  for (const value of ['https://www.linkedin.com/company/other-company/posts/',
    'https://other.example/company/linkedin/posts/', 'http://www.linkedin.com/company/linkedin/posts/',
    'https://www.linkedin.com/company/linkedin/posts/?redirect=1',
    'https://www.linkedin.com/company/linkedin/posts/#other',
    'https://www.linkedin.com/company/linkedin/', 'https://private@www.linkedin.com/company/linkedin/posts/',
    'not a URL']) assert.equal(isExactBootstrapNavigation(value), false);
});
test('structural mode is explicit and cannot mix with exact bootstrap or source paging', () => {
  const deadline = '--bootstrap-deadline=2026-10-03T23:59:59+05:30';
  const options = parseProbeArgs(['--bootstrap-structure', '--runtime-capture', deadline]);
  assert.equal(options.bootstrapStructure, true);
  assert.equal(options.bootstrap, true);
  for (const args of [['--bootstrap-structure'], ['--bootstrap-structure', '--bootstrap', deadline],
    ['--bootstrap-structure', '--continuation', deadline], ['--bootstrap-structure', '--boundary', deadline]])
    assert.throws(() => parseProbeArgs(args));
});
test('body mode requires explicit deadline and stays distinct from other bootstrap modes', () => {
  const deadline = '--bootstrap-deadline=2026-10-03T23:59:59+05:30';
  assert.equal(parseProbeArgs(['--bootstrap-body', '--runtime-capture', deadline]).bootstrapBody, true);
  for (const args of [['--bootstrap-body'], ['--bootstrap-body', '--bootstrap', deadline],
    ['--bootstrap-body', '--bootstrap-structure', deadline], ['--bootstrap-body', '--boundary', deadline]])
    assert.throws(() => parseProbeArgs(args));
});

test('resolver mode is distinct, deadline bounded and incompatible with source paging', () => {
  const deadline = '--bootstrap-deadline=2026-10-03T23:59:59+05:30';
  const options = parseProbeArgs(['--bootstrap-resolver', '--runtime-capture', deadline]);
  assert.equal(options.bootstrapResolver, true);
  assert.equal(options.bootstrap, true);
  for (const args of [['--bootstrap-resolver'], ['--bootstrap-resolver', '--bootstrap', deadline],
    ['--bootstrap-resolver', '--bootstrap-body', deadline], ['--bootstrap-resolver', '--bootstrap-structure', deadline],
    ['--bootstrap-resolver', '--continuation', deadline], ['--bootstrap-resolver', '--boundary', deadline]])
    assert.throws(() => parseProbeArgs(args));
});

const initialRoots = ['urn:li:fsd_update:3', 'urn:li:fsd_update:1', 'urn:li:fsd_update:2'];
function initialFixture() {
  const source = body(initialRoots);
  source.data.data[recipe].paging = {start: 0, count: 3, total: 230};
  source.included = initialRoots.map(entityUrn => ({entityUrn, $type: 'com.linkedin.voyager.dash.feed.Update',
    commentary: {text: 'synthetic allowed publication'}}));
  const envelope = {method: 'GET', status: 200,
    request: '/voyager/api/graphql?queryId=voyagerFeedDashOrganizationalPageUpdates.synthetic&variables=(organizationalPageUrn:urn:li:fsd_organizationalPage:1337)',
    body: 'initial-ref', headers: {cookie: 'synthetic forbidden session'}};
  const html = `<code>${JSON.stringify(envelope)}</code><code id="initial-ref"><!--${JSON.stringify(source).replaceAll('"', '&quot;')}--></code>`;
  return {html, source, envelope};
}
const initialNavigation = html => ({observed_at: new Date().toISOString(), method: 'GET', origin: 'https://www.linkedin.com',
  pathname: '/company/linkedin/posts/', status: 200, content_type: 'text/html',
  body_sha256: require('node:crypto').createHash('sha256').update(html).digest('hex')});

test('initial runtime mode is explicit and exact target deadline guarded', () => {
  const deadline = '--bootstrap-deadline=2026-10-03T23:59:59+05:30';
  assert.equal(parseProbeArgs(['--runtime-initial-page', '--runtime-capture', deadline]).runtimeInitialPage, true);
  for (const args of [['--runtime-initial-page'], ['--runtime-initial-page', deadline],
    ['--runtime-initial-page', '--runtime-capture'], ['--runtime-initial-page', '--runtime-capture', '--bootstrap-resolver', deadline],
    ['--runtime-initial-page', '--runtime-capture', '--continuation', deadline],
    ['--runtime-initial-page', '--runtime-capture', deadline, 'https://www.linkedin.com/company/other/posts/']])
    assert.throws(() => parseProbeArgs(args));
});

test('initial projection preserves separate navigation wrapper decoded hashes and exact three roots', () => {
  const {html} = initialFixture();
  const result = initialPageProjection(html, initialNavigation(html));
  assert.equal(result.representation_kind, 'initial_document_inert_reference');
  assert.deepEqual(result.representation.collection_projection.fields['*elements'], initialRoots);
  assert.deepEqual(result.representation.projected_graph.map(node => node.entityUrn), initialRoots);
  assert.equal(result.representation.collection_projection.fields.paging.start, 0);
  assert.equal(result.representation.collection_projection.fields.paging.count, 3);
  assert.equal(new Set([result.body_sha256, result.wrapper_sha256, result.navigation.body_sha256]).size, 3);
  assert.ok(!Object.hasOwn(result.request_parameters.fields, 'start'));
  assert.ok(!Object.hasOwn(result.request_parameters.fields, 'count'));
  for (const prohibited of ['initial-ref', 'synthetic forbidden session', 'headers'])
    assert.ok(!JSON.stringify(result).includes(prohibited));
  for (const navigation of [{...initialNavigation(html), status: 302}, {...initialNavigation(html), pathname: '/company/other/posts/'}])
    assert.throws(() => initialPageProjection(html, navigation));
  assert.throws(() => initialPageProjection(html + `<code>${JSON.stringify(initialFixture().envelope)}</code>`, initialNavigation(html)));
  const invalidDuplicate = html + `<code>${JSON.stringify({...initialFixture().envelope, body: null})}</code>`;
  assert.throws(() => initialPageProjection(invalidDuplicate, initialNavigation(invalidDuplicate)));
  assert.throws(() => initialPageProjection(html.replace('initial-ref', 'missing-ref'), initialNavigation(html)));
});

test('actual initial runtime main emits memory-only page separately and settles late source stops', async () => {
  const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
  const script = path.resolve(__dirname, '../scripts/probe-linkedin-company.cjs');
  const code = fs.readFileSync(script, 'utf8').replace(
    'require.main === module ? parseProbeArgs(process.argv.slice(2)) : parseProbeArgs([])', 'parseProbeArgs(process.argv.slice(2))');
  const target = 'https://www.linkedin.com/company/linkedin/posts/';
  for (const mode of ['success', 'off_target', 'late_security']) {
    const {html} = initialFixture();
    let callback, closed = false;
    const frame = {}, outputs = [], writes = [];
    const response = (url, content, status = 200, main = true) => ({url: () => url, status: () => status,
      request: () => ({method: () => 'GET', isNavigationRequest: () => main, frame: () => frame}),
      headerValue: async () => main ? 'text/html' : 'application/json', body: async () => Buffer.from(content)});
    const nav = response(mode === 'off_target' ? target.replace('linkedin/posts', 'other/posts') : target, html);
    const page = {mainFrame: () => frame, on: (_, handler) => {callback = handler;}, goto: async () => {callback(nav); return nav;},
      waitForTimeout: async () => {}, url: () => mode === 'off_target' ? target.replace('linkedin/posts', 'other/posts') : target,
      evaluate: async fn => String(fn).includes('signed_in:') ? {signed_in: true, warning: false, signin: false} : false};
    const context = {pages: () => [page], route: async () => {}, close: async () => {
      if (!closed && mode === 'late_security') callback(response('https://www.linkedin.com/voyager/api/graphql?queryId=voyagerFeedDashOrganizationalPageUpdates.synthetic', '{"errors":[{"code":"CHALLENGE"}]}', 200, false));
      closed = true;
    }};
    const fakeRequire = value => value === 'node:fs' ? {existsSync: value => !String(value).endsWith('linkedin-acquisition-hold.json'),
      writeFileSync: (file, data) => writes.push(file)} : value === './company-bootstrap.cjs' ? require('../scripts/company-bootstrap.cjs') :
      String(value).endsWith('playwright') ? {chromium: {launchPersistentContext: async () => context}} : require(value);
    fakeRequire.main = {};
    const sandbox = {module: {exports: {}}, require: fakeRequire, __dirname: path.dirname(script), Buffer, URL, setTimeout, clearTimeout,
      process: {argv: ['node', script, '--runtime-initial-page', '--runtime-capture', '--bootstrap-deadline=' + new Date(Date.now() + 60000).toISOString()], env: {}},
      console: {log: value => outputs.push(JSON.parse(value)), error: () => {}}};
    vm.runInNewContext(code + '\nmodule.exports.testMain = main;', sandbox);
    await sandbox.module.exports.testMain();
    assert.equal(outputs.length, 1);
    const receipt = outputs[0];
    assert.equal(receipt.page_mode, 'initial_document');
    assert.equal(receipt.session_verified, mode === 'success');
    assert.equal(!!receipt.initial_page, mode === 'success');
    assert.ok(!Object.hasOwn(receipt, 'dom'));
    assert.ok(writes.every(file => String(file).endsWith('linkedin-acquisition-hold.json')));
    if (mode === 'success') assert.equal(receipt.responses.length, 1);
    if (mode === 'late_security') assert.equal(receipt.stopped.security_hold, true);
  }
});
