'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { summarizeBootstrapCandidate, summarizeBootstrapStructure, summarizeBootstrapBodyEnvelope,
  summarizeBootstrapResolverEnvelope, summarizeBootstrapResolverCandidate, buildBootstrapReferenceIndex,
  bootstrapRequestMetadata, inspectEmbeddedBootstrap, summarizeBootstrapObservations } = require('../scripts/company-bootstrap.cjs');
const {semanticSignal} = require('../scripts/probe-linkedin-company.cjs');
const recipe = 'feedDashOrganizationalPageUpdatesByOrganizationalPageRelevanceFeed';
const roots = ['urn:li:fsd_update:3', 'urn:li:fsd_update:1', 'urn:li:fsd_update:2'];
const body = (refs = roots, paging = { start: 0, count: 3, total: 200 }) => ({
  data: { data: { [recipe]: {
    $type: 'com.linkedin.restli.common.CollectionResponse', '*elements': refs, paging,
    text: 'publication text must not escape', headers: { cookie: 'private' },
  } } }, included: [{ entityUrn: 'urn:li:fsd_update:999', text: 'unrelated' }],
});

test('exact start-zero collection reports three ordered refs and paging only', () => {
  assert.deepEqual(summarizeBootstrapCandidate(body()), {
    status: 'observed_start_zero', recipe, paging: { start: 0, count: 3, total: 200 }, root_ids: roots,
  });
});
test('nonzero current shape remains a nonzero candidate', () => {
  assert.deepEqual(summarizeBootstrapCandidate(body(roots, { start: 3, count: 10, total: 200 })), {
    status: 'observed_nonzero', recipe, paging: { start: 3, count: 10, total: 200 }, root_ids: roots,
  });
});
test('absent exact path never searches included updates or alternative recipes', () => {
  for (const value of [{}, { included: body().included }, { data: {} },
    { data: { data: { otherRecipe: body().data.data[recipe] } } },
    { data: { [recipe]: body().data.data[recipe] } }]) {
    assert.deepEqual(summarizeBootstrapCandidate(value), { status: 'absent', recipe, paging: null, root_ids: [] });
  }
});
test('changed wrappers or collection type are drifted', () => {
  for (const value of [null, [], { data: [] }, { data: { data: null } },
    { data: { data: { [recipe]: null } } },
    { data: { data: { [recipe]: { $type: 'different.CollectionResponse' } } } }]) {
    assert.equal(summarizeBootstrapCandidate(value).status, 'drifted');
  }
});
test('empty roots do not produce terminal or complete claims', () => {
  assert.deepEqual(summarizeBootstrapCandidate(body([], { start: 0, count: 0, total: 0 })), {
    status: 'observed_start_zero', recipe, paging: { start: 0, count: 0, total: 0 }, root_ids: [],
  });
  assert.equal(summarizeBootstrapCandidate(body([], { start: 10, count: 10, total: 10 })).status, 'observed_nonzero');
});
test('paging requires every strict nonnegative safe integer', () => {
  for (const key of ['start', 'count', 'total']) {
    for (const invalid of [true, false, -1, 1.5, '0', null, undefined, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
      const paging = { start: 0, count: 3, total: 200, [key]: invalid };
      assert.equal(summarizeBootstrapCandidate(body(roots, paging)).status, 'invalid', `${key}: ${String(invalid)}`);
    }
    const paging = { start: 0, count: 3, total: 200 };
    delete paging[key];
    assert.equal(summarizeBootstrapCandidate(body(roots, paging)).status, 'invalid');
  }
  assert.equal(summarizeBootstrapCandidate(body(roots, { start: 0, count: 0, total: Number.MAX_SAFE_INTEGER })).status, 'observed_start_zero');
});
test('duplicates, malformed refs and oversized lists are invalid without truncation', () => {
  for (const refs of [null, {}, Array(1), [roots[0], roots[0]], ['not-native'], [true],
    ['urn:li:unknown:1'], ['urn:li:fsd_update:'], ['urn:li:fsd_update:1\n'], ['urn:li:fsd_update:1?token=private'],
    [`urn:li:fsd_update:${'1'.repeat(1000)}`],
    Array.from({ length: 101 }, (_, index) => `urn:li:fsd_update:${index}`)]) {
    assert.deepEqual(summarizeBootstrapCandidate(body(refs)), { status: 'invalid', recipe, paging: null, root_ids: [] });
  }
  const hundred = Array.from({ length: 100 }, (_, index) => `urn:li:fsd_update:${index}`);
  assert.deepEqual(summarizeBootstrapCandidate(body(hundred)).root_ids, hundred);
});
test('missing or malformed paging is invalid', () => {
  for (const paging of [null, [], 'paging']) {
    assert.equal(summarizeBootstrapCandidate(body(roots, paging)).status, 'invalid');
  }
  const source = body();
  delete source.data.data[recipe].paging;
  assert.equal(summarizeBootstrapCandidate(source).status, 'invalid');
});
test('metadata result neither mutates nor aliases source fields', () => {
  const source = body();
  const before = JSON.stringify(source);
  const summary = summarizeBootstrapCandidate(source);
  summary.root_ids.push('urn:li:fsd_update:4');
  summary.paging.start = 5;
  assert.equal(JSON.stringify(source), before);
});
test('semantic errors in supported envelopes invalidate valid-looking bootstrap roots', () => {
  for (const at of ['body', 'data', 'nested', 'collection']) {
    for (const error of [{ code: 'CHALLENGE' }, { code: 'UNRECOGNIZED_SOURCE_ERROR' }]) {
      for (const key of ['errors', 'error']) {
        const source = body();
        const container = { body: source, data: source.data, nested: source.data.data,
          collection: source.data.data[recipe] }[at];
        container[key] = key === 'errors' ? [error] : error;
        assert.deepEqual(summarizeBootstrapCandidate(source), {
          status: 'invalid', recipe, paging: null, root_ids: [],
        }, `${at}.${key}: ${error.code}`);
      }
    }
  }
});
test('authored included content is not an error envelope', () => {
  const source = body();
  source.included.push({ entityUrn: roots[0], text: 'CHALLENGE error access restricted',
    errors: [{ code: 'CHALLENGE' }], error: { code: 'UNKNOWN' } });
  assert.equal(summarizeBootstrapCandidate(source).status, 'observed_start_zero');
});
test('opaque nonempty errors invalidate candidates; empty error values do not', () => {
  for (const at of ['body', 'data', 'nested', 'collection']) {
    for (const key of ['errors', 'error']) {
      for (const value of ['opaque source error', true, 1, ['unknown'], { message: 'unknown' }]) {
        const source = body();
        ({ body: source, data: source.data, nested: source.data.data,
          collection: source.data.data[recipe] })[at][key] = value;
        assert.deepEqual(summarizeBootstrapCandidate(source), {
          status: 'invalid', recipe, paging: null, root_ids: [],
        });
      }
      for (const value of [[], {}, null, false, 0, '']) {
        const source = body();
        ({ body: source, data: source.data, nested: source.data.data,
          collection: source.data.data[recipe] })[at][key] = value;
        assert.equal(summarizeBootstrapCandidate(source).status, 'observed_start_zero');
      }
    }
  }
});

test('initial-document inert script and comment-code JSON yield metadata provenance only', () => {
  const json = JSON.stringify(body());
  const html = `<script type="application/json">${json}</script><code id="source"><!--${json}--></code>`;
  const observed = inspectEmbeddedBootstrap(html);
  assert.equal(observed.status, 'observed');
  assert.equal(observed.candidates.length, 2);
  for (const candidate of observed.candidates) {
    assert.equal(candidate.source, 'initial_document_json');
    assert.equal(candidate.status, 'observed_start_zero');
    assert.deepEqual(candidate.root_ids, roots);
    assert.match(candidate.body_sha256, /^[a-f0-9]{64}$/);
    assert.deepEqual(Object.keys(candidate).sort(), ['source', 'body_sha256', 'status', 'recipe', 'paging', 'root_ids'].sort());
  }
  assert.ok(!JSON.stringify(observed).includes('publication text'));
  assert.ok(!JSON.stringify(observed).includes('cookie'));
});
test('embedded extraction excludes executable JS, DOM links and included-only roots', () => {
  assert.deepEqual(inspectEmbeddedBootstrap(`<script>${JSON.stringify(body())}</script><a href="${roots[0]}">text</a>`),
    {status: 'absent', candidates: []});
  const onlyIncluded = inspectEmbeddedBootstrap(`<script type='application/json'>${JSON.stringify({included: body().included})}</script>`);
  assert.equal(onlyIncluded.status, 'absent');
  assert.deepEqual(onlyIncluded.candidates[0].root_ids, []);
});
test('embedded semantic errors reach stop inspection while unknown errors invalidate metadata', () => {
  const source = body();
  source.data.data[recipe].errors = [{code: 'CHALLENGE'}];
  let inspected = 0;
  const observation = inspectEmbeddedBootstrap(`<code><!--${JSON.stringify(source)}--></code>`, candidate => {
    assert.deepEqual(candidate.data.data[recipe].errors, [{code: 'CHALLENGE'}]);
    inspected++;
  });
  assert.equal(inspected, 1);
  assert.equal(observation.status, 'invalid');
  assert.deepEqual(observation.candidates[0].root_ids, []);
});
test('embedded byte and candidate limits refuse completeness without truncation claims', () => {
  assert.deepEqual(inspectEmbeddedBootstrap('x'.repeat(8_000_001)), {status: 'bounded', candidates: []});
  const html = `<code>${JSON.stringify(body())}</code>`.repeat(101);
  const result = inspectEmbeddedBootstrap(html);
  assert.equal(result.status, 'bounded');
  assert.equal(result.candidates.length, 100);
  assert.equal(summarizeBootstrapObservations([], true).status, 'bounded');
});

test('structural inventory finds non-API collection topology without calling included membership', () => {
  const source = {data: {wrapper: {$type: 'com.linkedin.restli.common.CollectionResponse',
    '*elements': roots, paging: {start: 0, count: 3, total: 231}}},
    included: [{$type: 'com.linkedin.voyager.dash.feed.Update', entityUrn: roots[0],
      text: 'authored content must never escape', '*socialDetail': 'urn:li:fsd_socialDetail:1'}]};
  const result = summarizeBootstrapStructure(source);
  assert.equal(result.status, 'observed');
  const collection = result.nodes.find(node => node.path === '$.data.wrapper');
  assert.equal(collection.type_name, 'com.linkedin.restli.common.CollectionResponse');
  assert.deepEqual(collection.reference_fields.find(field => field.key === '*elements'),
    {key: '*elements', ids: roots, state: 'value', count: 3});
  const included = result.nodes.find(node => node.path === '$.included[0]');
  assert.equal(included.type_name, 'com.linkedin.voyager.dash.feed.Update');
  assert.ok(included.reference_fields.some(field => field.ids.includes('urn:li:fsd_socialDetail:1')));
  assert.ok(!JSON.stringify(result).includes('authored content'));
  assert.ok(!JSON.stringify(result).includes('source_complete'));
});
test('structural privacy filtering omits sensitive names and values and declares bounds', () => {
  const source = {data: {author: 'private authored string',
    metadata: {paginationToken: 'private token'},
    csrf: 'private csrf', entityUrn: 'urn:li:fsd_update:1',
    '*secretReference': 'urn:li:fsd_update:secret',
    ['ghp_' + 'A'.repeat(32)]: 'private credential'}};
  const result = summarizeBootstrapStructure(source);
  assert.equal(result.status, 'bounded');
  const serialized = JSON.stringify(result);
  for (const prohibited of ['private', 'paginationToken', 'csrf', 'secretReference', 'ghp_'])
    assert.ok(!serialized.includes(prohibited), prohibited);
  const html = `<code>${JSON.stringify(source)}</code>`;
  const observation = inspectEmbeddedBootstrap(html, () => {}, true);
  assert.equal(observation.status, 'bounded');
  assert.equal(observation.candidates.length, 1);
  assert.ok(observation.candidates[0].structure.nodes.some(node => node.reference_fields.length > 0));
});
test('structural depth width reference and node bounds remain explicit', () => {
  const deep = JSON.parse('{"data":'.repeat(8000) + '{}' + '}'.repeat(8000));
  assert.equal(summarizeBootstrapStructure(deep).status, 'bounded');
  assert.equal(summarizeBootstrapStructure({elements: Array(80001).fill(null)}).status, 'bounded');
  const refs = Array.from({length: 101}, (_, index) => `urn:li:fsd_update:${index}`);
  const result = summarizeBootstrapStructure({'*elements': refs});
  assert.equal(result.status, 'bounded');
  assert.equal(result.nodes[0].reference_fields[0].count, 101);
  assert.equal(result.nodes[0].reference_fields[0].ids.length, 100);
  const html = `<code><!--${JSON.stringify({data: {'*elements': roots}})}--></code>`;
  const embedded = inspectEmbeddedBootstrap(html, () => {}, true);
  assert.equal(embedded.candidates[0].structure.status, 'observed');
  assert.deepEqual(Object.keys(embedded.candidates[0]).sort(), ['source', 'body_sha256', 'structure'].sort());
});
test('structure-only unsupported encoded or malformed candidates remain explicitly bounded', () => {
  for (const html of ['<code>{&quot;data&quot;:{}}</code>',
    '<script type="application/json">{invalid}</script>', '<script>window.data = {};</script>']) {
    assert.deepEqual(inspectEmbeddedBootstrap(html, () => {}, true), {status: 'bounded', candidates: []});
  }
});

const feedRequest = '/voyager/api/graphql?queryId=voyagerFeedDashOrganizationalPageUpdates.synthetic&variables=(organizationalPageUrn:urn%3Ali%3Afsd_organizationalPage%3A1337,start:3,count:10)';
test('one body layer yields exact collection plus inventory without private envelope values', () => {
  const source = {request: feedRequest, status: 200, method: 'GET',
    headers: {cookie: 'private header'}, body: JSON.stringify(body())};
  const result = summarizeBootstrapBodyEnvelope(source);
  assert.deepEqual(result.envelope, {method: 'GET', status: 200, request_family: 'feed',
    target_binding: 'matched', body_encoding: 'json_string'});
  assert.equal(result.collection.status, 'observed_start_zero');
  assert.deepEqual(result.collection.root_ids, roots);
  assert.ok(result.structure.nodes.some(node => node.type_name === 'com.linkedin.restli.common.CollectionResponse'));
  assert.match(result.body_sha256, /^[a-f0-9]{64}$/);
  const serialized = JSON.stringify(result);
  for (const prohibited of ['private header', '/voyager/api', 'publication text', '1337,start', 'organizationalPageUrn'])
    assert.ok(!serialized.includes(prohibited));
  const html = `<code><!--${JSON.stringify(source)}--></code>`;
  const observed = inspectEmbeddedBootstrap(html, () => {}, 'body');
  assert.equal(observed.candidates[0].source, 'initial_document_body_json');
  assert.match(observed.candidates[0].outer_body_sha256, /^[a-f0-9]{64}$/);
  assert.notEqual(observed.candidates[0].outer_body_sha256, observed.candidates[0].body_sha256);
});
test('unsupported body encodings never recursively decode or invent a body digest', () => {
  for (const value of [JSON.stringify(JSON.stringify(body())), 'eyJkYXRhIjp7fX0=', true, 7]) {
    const result = summarizeBootstrapBodyEnvelope({method: 'GET', status: 200, request: feedRequest, body: value});
    assert.ok(['invalid', 'unsupported'].includes(result.envelope.body_encoding));
    assert.equal(result.body_sha256, null);
    assert.equal(result.collection.status, 'invalid');
    assert.deepEqual(result.structure.nodes, []);
  }
  assert.equal(summarizeBootstrapBodyEnvelope({method: 'POST', body: body()}).envelope.body_encoding, 'invalid');
  assert.equal(summarizeBootstrapBodyEnvelope({body: null}).envelope.body_encoding, 'null');
  assert.equal(summarizeBootstrapBodyEnvelope({}).envelope.body_encoding, 'missing');
});
test('decoded and response-status security signals survive body unwrap and bounds', () => {
  const source = body();
  source.data.data[recipe].errors = [{code: 'CHALLENGE'}];
  const signals = [];
  const result = summarizeBootstrapBodyEnvelope({method: 'GET', status: 401, request: feedRequest,
    body: JSON.stringify(source)}, candidate => { const signal = semanticSignal(candidate); if (signal) signals.push(signal); });
  assert.equal(result.collection.status, 'invalid');
  assert.ok(signals.some(signal => signal.reason === 'authentication_required'));
  assert.ok(signals.some(signal => signal.security));
  const deep = JSON.parse('{"data":'.repeat(8000) + '{}' + '}'.repeat(8000));
  deep.errors = [{code: 'CHALLENGE'}];
  const boundedSignals = [];
  const bounded = summarizeBootstrapBodyEnvelope({method: 'GET', status: 200, request: feedRequest, body: deep}, candidate => {
    const signal = semanticSignal(candidate); if (signal) boundedSignals.push(signal);
  });
  assert.equal(bounded.envelope.body_encoding, 'bounded');
  assert.equal(bounded.body_sha256, null);
  assert.ok(boundedSignals.some(signal => signal.security));
});
test('feed request binding uses explicit observed request identity with unknown or mismatched kept distinct', () => {
  assert.deepEqual(bootstrapRequestMetadata(feedRequest), {request_family: 'feed', target_binding: 'matched'});
  assert.equal(bootstrapRequestMetadata(feedRequest.replace('1337', '999')).target_binding, 'mismatched');
  assert.equal(bootstrapRequestMetadata(feedRequest.replace('organizationalPageUrn', 'organizationalPage')).target_binding, 'unknown');
  assert.deepEqual(bootstrapRequestMetadata('https://other.example' + feedRequest), {request_family: 'other', target_binding: 'unknown'});
  const foreign = summarizeBootstrapBodyEnvelope({method: 'GET', status: 200,
    request: feedRequest.replace('1337', '999'), body: JSON.stringify(body())});
  assert.equal(foreign.body_sha256, null);
  assert.equal(foreign.collection.status, 'invalid');
  assert.deepEqual(foreign.structure.nodes, []);
  const unknown = summarizeBootstrapBodyEnvelope({method: 'GET', status: 200,
    request: feedRequest.replace('organizationalPageUrn', 'organizationalPage'), body: JSON.stringify(body())});
  assert.equal(unknown.collection.status, 'invalid');
  assert.ok(unknown.structure.nodes.length > 0);
});
test('unrelated cached response statuses do not establish scoped account restriction', () => {
  const signals = [];
  const source = body();
  source.errors = [{code: 'CHALLENGE'}];
  summarizeBootstrapBodyEnvelope({method: 'GET', status: 403,
    request: '/voyager/api/identity/unrelated', body: JSON.stringify(source)}, candidate => {
      const signal = semanticSignal(candidate); if (signal) signals.push(signal);
    });
  assert.deepEqual(signals, []);
});

test('resolver admits unique same-document direct and single entity JSON without exporting IDs', () => {
  const source = {method: 'GET', status: 200, request: feedRequest, body: 'bpr-reference'};
  for (const [content, encoding] of [[JSON.stringify(body()), 'reference_json'],
    [JSON.stringify(body()).replaceAll('"', '&quot;'), 'reference_entity_json'],
    [JSON.stringify(body()).replaceAll('"', '&#x22;'), 'reference_entity_json']]) {
    const html = `<code>${JSON.stringify(source)}</code><code id="bpr-reference"><!--${content}--></code>`;
    const observed = inspectEmbeddedBootstrap(html, () => {}, 'resolver');
    const result = observed.candidates[0];
    assert.equal(result.envelope.body_encoding, encoding);
    assert.equal(result.envelope.body_length, 13);
    assert.equal(result.collection.status, 'observed_start_zero');
    assert.deepEqual(result.collection.root_ids, roots);
    assert.match(result.body_sha256, /^[a-f0-9]{64}$/);
    const serialized = JSON.stringify(result);
    for (const privateValue of ['bpr-reference', 'publication text', 'private', '/voyager/api'])
      assert.ok(!serialized.includes(privateValue));
  }
});

test('resolver missing duplicate noninert and chain references refuse without body evidence', () => {
  const source = {method: 'GET', status: 200, request: feedRequest, body: 'ref'};
  for (const [html, encoding] of [['', 'reference_missing'],
    ['<code id="ref">{}</code><code id="ref">{}</code>', 'reference_ambiguous'],
    ['<script id="ref">{}</script>', 'reference_unsupported'],
    ['<code id="ref">second</code><code id="second">{}</code>', 'reference_unsupported'],
    ['<code id="ref">{&amp;quot;data&amp;quot;:{}}</code>', 'reference_unsupported']]) {
    const result = summarizeBootstrapResolverEnvelope(source, () => {}, buildBootstrapReferenceIndex(html));
    assert.equal(result.envelope.body_encoding, encoding);
    assert.equal(result.body_sha256, null);
    assert.deepEqual(result.collection.root_ids, []);
    assert.deepEqual(result.structure.nodes, []);
  }
  for (const value of ['https://example.test/payload', '{broken', 'eyJkYXRhIjp7fX0='])
    assert.equal(summarizeBootstrapResolverEnvelope({...source, body: value}).envelope.body_encoding, 'reference_unsupported');
});

test('resolver preserves bounds, body byte length, foreign refusal and decoded security', () => {
  const source = {method: 'GET', status: 401, request: feedRequest, body: 'ref'};
  const challenge = body();
  challenge.data.data[recipe].errors = [{code: 'CHALLENGE'}];
  const index = buildBootstrapReferenceIndex(`<script type="application/json" id="ref">${JSON.stringify(challenge)}</script>`);
  const signals = [];
  const result = summarizeBootstrapResolverEnvelope(source, candidate => {
    const signal = semanticSignal(candidate); if (signal) signals.push(signal);
  }, index);
  assert.equal(result.envelope.body_encoding, 'reference_json');
  assert.equal(result.collection.status, 'invalid');
  assert.ok(signals.some(signal => signal.security));
  assert.ok(signals.some(signal => signal.reason === 'authentication_required'));
  const foreign = summarizeBootstrapResolverEnvelope({...source, request: feedRequest.replace('1337', '999')},
    () => assert.fail('foreign callback'), index);
  assert.equal(foreign.body_sha256, null);
  assert.deepEqual(foreign.structure.nodes, []);
  const boundedIndex = buildBootstrapReferenceIndex('<code></code>'.repeat(2001));
  assert.equal(summarizeBootstrapResolverEnvelope(source, () => {}, boundedIndex).envelope.body_encoding, 'bounded');
  assert.equal(inspectEmbeddedBootstrap('<code></code>'.repeat(2001), () => {}, 'resolver').status, 'bounded');
  assert.equal(summarizeBootstrapResolverEnvelope({...source, body: 'x'.repeat(8_000_001)}).envelope.body_length, null);
  assert.equal(summarizeBootstrapResolverEnvelope({...source, body: 'é'}).envelope.body_length, 2);
  assert.equal(summarizeBootstrapResolverEnvelope({...source, body: body()}).envelope.body_length, null);
  assert.ok(!Object.hasOwn(summarizeBootstrapBodyEnvelope({...source, body: body()}).envelope, 'body_length'));
});

test('resolver exact roots share the structural sensitive value filter', () => {
  for (const suffix of ['GHP_' + 'a'.repeat(30), 'SK-' + 'a'.repeat(30), 'csrfCredential', 'paginationToken']) {
    const privateBody = body(['urn:li:activity:' + suffix]);
    const source = {method: 'GET', status: 200, request: feedRequest, body: 'ref'};
    const index = buildBootstrapReferenceIndex(`<code id="ref">${JSON.stringify(privateBody)}</code>`);
    for (const result of [summarizeBootstrapResolverEnvelope(source, () => {}, index),
      summarizeBootstrapResolverEnvelope({...source, body: JSON.stringify(privateBody)})]) {
      assert.equal(result.collection.status, 'invalid');
      assert.deepEqual(result.collection.root_ids, []);
      assert.ok(!JSON.stringify(result).includes(suffix));
    }
    assert.equal(summarizeBootstrapResolverCandidate(privateBody).status, 'invalid');
    assert.equal(summarizeBootstrapCandidate(privateBody).status, 'observed_start_zero');
  }
});
