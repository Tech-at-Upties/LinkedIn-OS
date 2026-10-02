// Offline account-stop and evidence-fidelity checks; no browser/network launch.
const test = require('node:test');
const assert = require('node:assert/strict');
const { collectionProjection, semanticSignal } = require('../scripts/probe-linkedin-company.cjs');
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
test('semantic security candidates stop only from explicit bounded error envelopes', () => {
  assert.equal(semanticSignal({data: {data: {errors: [{code: 'CHALLENGE'}]}}}).security, true);
  assert.equal(semanticSignal({error: {status: 429}}).security, true);
  assert.equal(semanticSignal({errors: [{code: 'AUTH_REQUIRED'}]}).reason, 'authentication_required');
});
test('post text, unknown error codes and ambiguous status values are not semantic stop evidence', () => {
  assert.equal(semanticSignal({included: [{text: 'CHALLENGE captcha account restricted', errors: [{status: 403}]}]}), null);
  assert.equal(semanticSignal({errors: [{code: {value: 'CHALLENGE'}, status: '429'}]}), null);
});
