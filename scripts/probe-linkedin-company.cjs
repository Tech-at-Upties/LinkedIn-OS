/* One authorized, read-only company navigation. Never export session material. */
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { summarizeBootstrapCandidate, summarizeBootstrapStructure, summarizeBootstrapBodyEnvelope,
  summarizeBootstrapResolverEnvelope, summarizeBootstrapResolverCandidate,
  bootstrapRequestMetadata, summarizeBootstrapObservations, inspectEmbeddedBootstrap } = require('./company-bootstrap.cjs');

const root = path.resolve(__dirname, '..');
const profile = path.join(root, '.local', 'linkedin-test-browser');
const holdPath = path.join(root, '.local', 'linkedin-acquisition-hold.json');
function parseProbeArgs(args) {
  const options = { observeContinuation: false, observeBoundary: false, runtimeCapture: false,
    runtimeInitialPage: false, runtimeCompanyBatch: false, batchPageBudget: null, bootstrap: false, bootstrapStructure: false, bootstrapBody: false, bootstrapResolver: false, bootstrapDeadline: null, target: 'https://www.linkedin.com/company/linkedin/posts/' };
  let targetSeen = false;
  const seen = new Set();
  for (const argument of args) {
    if (!argument.startsWith('--')) {
      if (targetSeen) throw new Error('Only one target is allowed');
      options.target = argument; targetSeen = true; continue;
    }
    const name = argument.split('=')[0];
    if (seen.has(name)) throw new Error('Duplicate mode argument');
    seen.add(name);
    const flags = { '--continuation': 'observeContinuation', '--boundary': 'observeBoundary',
      '--runtime-capture': 'runtimeCapture', '--runtime-initial-page': 'runtimeInitialPage', '--runtime-company-batch': 'runtimeCompanyBatch', '--bootstrap': 'bootstrap', '--bootstrap-structure': 'bootstrapStructure', '--bootstrap-body': 'bootstrapBody', '--bootstrap-resolver': 'bootstrapResolver' };
    if (flags[argument]) options[flags[argument]] = true;
    else if (name === '--batch-page-budget' && /^--batch-page-budget=[23]$/.test(argument)) {
      options.batchPageBudget = Number(argument.at(-1));
    } else if (name === '--bootstrap-deadline' && argument.includes('=')) {
      const value = argument.slice(argument.indexOf('=') + 1);
      if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value)
          || !Number.isFinite(Date.parse(value))) throw new Error('Aware bootstrap deadline required');
      options.bootstrapDeadline = Date.parse(value);
    } else throw new Error('Unknown mode argument');
  }
  if ([options.bootstrap, options.bootstrapStructure, options.bootstrapBody, options.bootstrapResolver].filter(Boolean).length > 1)
    throw new Error('Choose one bootstrap representation mode');
  if (options.bootstrapStructure || options.bootstrapBody || options.bootstrapResolver) options.bootstrap = true;
  if (options.runtimeInitialPage && (!options.runtimeCapture || options.bootstrap)) throw new Error('Initial runtime mode requires exclusive runtime capture');
  if (options.runtimeCompanyBatch && (!options.runtimeCapture || options.bootstrap || options.runtimeInitialPage)
      || options.runtimeCompanyBatch !== (options.batchPageBudget !== null)) throw new Error('Batch runtime mode requires exclusive capture and page budget');
  if (options.observeContinuation && options.observeBoundary
      || (options.runtimeCapture || options.bootstrap) && (options.observeContinuation || options.observeBoundary))
    throw new Error('Incompatible acquisition modes');
  if ((options.bootstrap || options.runtimeInitialPage || options.runtimeCompanyBatch) !== (options.bootstrapDeadline !== null)) throw new Error('Bounded document mode requires its deadline');
  if ((options.bootstrap || options.runtimeInitialPage || options.runtimeCompanyBatch) && options.target !== 'https://www.linkedin.com/company/linkedin/posts/')
    throw new Error('Bootstrap target differs from its declared scope');
  return options;
}
const options = require.main === module ? parseProbeArgs(process.argv.slice(2)) : parseProbeArgs([]);
const { observeContinuation, observeBoundary, runtimeCapture, runtimeInitialPage, runtimeCompanyBatch, batchPageBudget, bootstrap, bootstrapStructure, bootstrapBody, bootstrapResolver, bootstrapDeadline } = options;
const boundedDocumentMode = bootstrap || runtimeInitialPage || runtimeCompanyBatch;
const bootstrapBodyMode = bootstrapBody || bootstrapResolver || runtimeInitialPage || runtimeCompanyBatch;
const runId = new Date().toISOString().replace(/[:.]/g, '-');
const resultPath = path.join(root, 'docs', 'results', `company-read-${runId}.json`);
const sourceRoot = process.env.NOS_SOURCE_ROOT || path.resolve(root, '..', 'NOS-V1');
const target = options.target;
const parsedTarget = new URL(target);
function isExactBootstrapNavigation(value) {
  try { return new URL(value).href === 'https://www.linkedin.com/company/linkedin/posts/'; }
  catch { return false; }
}
if (parsedTarget.origin !== 'https://www.linkedin.com' ||
    !/^\/company\/[a-zA-Z0-9-]+\/(?:posts\/)?$/.test(parsedTarget.pathname) || parsedTarget.search) {
  throw new Error('Only an exact public LinkedIn company path is allowed.');
}

const sha = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
const sourceUrn = value => typeof value === 'string' &&
  /^urn:li:(?:activity|share|ugcPost|organization|fsd_company|fsd_update):[A-Za-z0-9_(),:-]+$/.test(value);
const safeKey = key => /^\*?[a-zA-Z0-9_.$-]{1,100}$/.test(key);
const projectionKeys = new Set([
  '$type', '__typename', 'entityUrn', 'urn', 'id', 'activityUrn', 'shareUrn',
  'organizationUrn', 'actor', 'author', 'commentary', 'text', 'textDirection',
  'resharedUpdate', 'originalUpdate', 'rootShare', 'socialDetail', 'totalSocialActivityCounts',
  'numLikes', 'numComments', 'numShares', 'numReactions', 'content',
  'publishedAt', 'createdAt', 'lastModifiedAt', 'paging', 'start', 'count', 'total',
  'actorUrn', 'name', 'description', 'subDescription', 'accessibilityText',
  'updateMetadata', 'metadata', 'header', 'socialActivityCounts', 'socialContent',
  'reactionTypeCounts', 'reactionType', 'likesCount', 'commentsCount', 'sharesCount',
  'entity', 'rootEntity', 'contentUrn', 'authorUrn', 'reshare', 'repost',
  'navigationContext', 'navigationUrl', 'url', 'elements', 'universalName', 'vanityName',
  'backendUrn', 'preDashEntityUrn', 'actionTarget', 'threadUrn', 'reshareUpdateUrn',
  'numImpressions', 'shareUrl', 'hideCommentsCount', 'hideReactionsCount',
  'hideSocialActivityCounts', 'hideRepostsCount', 'hideViewsCount',
  'attributes', 'detailData', 'companyName', 'company', 'profileName', 'profile', 'length',
]);
function projection(node, depth = 0) {
  if (depth > 7 || node == null) return node == null ? node : '[depth limit]';
  if (typeof node === 'string') {
    let text = node.slice(0, 5000).replace(/(?:gh[pousr]_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9]{20,})/g, '[credential literal redacted]');
    if (/^https?:\/\//.test(text)) {
      const url = new URL(text);
      if (url.hostname !== 'www.linkedin.com') return '[external URL omitted]';
      text = url.origin + url.pathname;
    }
    return text;
  }
  if (typeof node === 'number' || typeof node === 'boolean') return node;
  if (Array.isArray(node)) return node.slice(0, 8).map(item => projection(item, depth + 1));
  const out = {};
  for (const [key, value] of Object.entries(node)) {
    if (projectionKeys.has(key.replace(/^\*/, ''))) out[key] = projection(value, depth + 1);
  }
  return out;
}
const feedRecipe = 'feedDashOrganizationalPageUpdatesByOrganizationalPageRelevanceFeed';
const traversalNodeLimit = 80000;
const traversalDepthLimit = 64;
function errorContainers(body) {
  return [body, body?.data, body?.data?.data, body?.data?.data?.[feedRecipe]]
    .filter(value => value && typeof value === 'object' && !Array.isArray(value));
}
function hasSemanticErrors(body) {
  return errorContainers(body).some(container => ['errors', 'error'].some(key => {
    const value = container[key];
    if (Array.isArray(value)) return value.length > 0;
    if (value && typeof value === 'object') return Object.keys(value).length > 0;
    return Boolean(value);
  }));
}
function collectionProjection(body) {
  if (hasSemanticErrors(body)) return { status: 'semantic_error' };
  const collection = body?.data?.data?.[feedRecipe];
  if (!collection || collection.$type !== 'com.linkedin.restli.common.CollectionResponse')
    return { status: 'recipe_absent_or_drifted' };
  const roots = collection['*elements'];
  if (!Array.isArray(roots) || !roots.every(sourceUrn)) return { status: 'invalid_root_references' };
  // Do not replace a source list with the included entity list or silently truncate it.
  if (roots.length > 100) return { status: 'root_projection_limit' };
  return { status: 'captured', recipe: feedRecipe, fields: {
    $type: collection.$type, '*elements': roots, paging: projection(collection.paging),
  } };
}
function semanticSignal(body) {
  // Error-envelope candidates only. Publication text cannot establish this signal.
  let authentication = null;
  for (const container of errorContainers(body)) {
    for (const key of ['errors', 'error']) {
      const errors = Array.isArray(container[key]) ? container[key] : [container[key]];
      for (const error of errors) {
        if (!error || typeof error !== 'object' || Array.isArray(error)) continue;
        if (error.code === 'CHALLENGE') return { reason: 'possible_semantic_challenge', security: true };
        if ([403, 429, 999].includes(error.status) || ['ACCESS_DENIED', 'RATE_LIMITED'].includes(error.code))
          return { reason: 'possible_semantic_restriction', security: true };
        if (error.status === 401 || error.code === 'AUTH_REQUIRED') authentication = { reason: 'authentication_required', security: false };
      }
    }
  }
  return authentication;
}
function boundedValueHash(value) {
  if (value == null) return { value_sha256: null, bounded: false };
  const pending = [{ value, depth: 0 }];
  let visited = 0;
  while (pending.length) {
    const current = pending.pop();
    if (++visited > traversalNodeLimit || current.depth > traversalDepthLimit)
      return { value_sha256: null, bounded: true };
    if (current.value && typeof current.value === 'object') {
      const children = Object.values(current.value);
      if (children.length + pending.length > traversalNodeLimit - visited)
        return { value_sha256: null, bounded: true };
      for (const child of children) pending.push({ value: child, depth: current.depth + 1 });
    }
  }
  try { return { value_sha256: sha(JSON.stringify(value)), bounded: false }; }
  catch { return { value_sha256: null, bounded: true }; }
}
function describe(body) {
  const types = {}, ids = new Set(), publications = [], continuations = [], shapes = {};
  let visited = 0, traversalBounded = false;
  const pending = [{ node: body, at: '$', depth: 0 }];
  function schedule(node, at, depth) {
    const keys = Array.isArray(node) ? null : Object.keys(node).filter(safeKey);
    const length = keys ? keys.length : node.length;
    const remaining = traversalNodeLimit - visited - pending.length;
    if (length > remaining) traversalBounded = true;
    for (let index = Math.min(length, Math.max(0, remaining)) - 1; index >= 0; index--) {
      const key = keys ? keys[index] : index;
      pending.push({ node: node[key], at: keys ? `${at}.${key}` : `${at}[${key}]`, depth });
    }
  }
  while (pending.length) {
    const { node, at, depth } = pending.pop();
    if (++visited > traversalNodeLimit) { traversalBounded = true; break; }
    if (depth > traversalDepthLimit) { traversalBounded = true; continue; }
    if (!node || typeof node !== 'object') continue;
    if (Array.isArray(node)) {
      schedule(node, at, depth + 1);
      continue;
    }
    const type = node.$type || node.__typename;
    if (typeof type === 'string' && /^[a-zA-Z0-9_.$-]{1,180}$/.test(type)) {
      types[type] = (types[type] || 0) + 1;
      shapes[type] = [...new Set([...(shapes[type] || []), ...Object.keys(node).filter(safeKey)])].slice(0, 60);
    }
    for (const [key, value] of Object.entries(node)) {
      if (sourceUrn(value)) ids.add(value);
      if (/^(?:paging|pagination|nextCursor|paginationToken|nextPageToken)$/.test(key)) {
        const hashed = boundedValueHash(value);
        if (hashed.bounded) traversalBounded = true;
        continuations.push({ path: `${at}.${key}`, kind: typeof value,
          projection: /^(?:paging|pagination)$/.test(key) ? projection(value) : undefined,
          present: value != null, value_sha256: hashed.value_sha256, hash_bounded: hashed.bounded });
      }
    }
    if (sourceUrn(node.entityUrn) && /(?:activity|share|ugcPost|fsd_update):/.test(node.entityUrn) &&
        publications.length < 12) publications.push({ path: at, fields: projection(node) });
    schedule(node, at, depth + 1);
  }
  const included = Array.isArray(body.included) ? body.included : [];
  const collection = traversalBounded ? { status: 'traversal_projection_limit' } : collectionProjection(body);
  const index = new Map(), duplicates = new Set();
  for (const node of included) {
    if (!node || typeof node !== 'object' || typeof node.entityUrn !== 'string') continue;
    if (index.has(node.entityUrn)) duplicates.add(node.entityUrn);
    index.set(node.entityUrn, node);
  }
  const graph = new Map(), missing = new Set();
  function resolve(node) {
    if (!node || graph.has(node.entityUrn) || graph.size >= 180) return;
    const selected = projection(node);
    if (typeof node.$type === 'string' && node.$type.includes('.identity.profile.')) {
      graph.set(node.entityUrn, { entityUrn: node.entityUrn, $type: node.$type }); return;
    }
    graph.set(node.entityUrn, selected);
    function refs(value) {
      if (typeof value === 'string' && value.startsWith('urn:li:')) {
        const referred = index.get(value);
        if (referred) resolve(referred); else if (value !== node.entityUrn) missing.add(value);
      } else if (Array.isArray(value)) value.forEach(refs);
      else if (value && typeof value === 'object') Object.values(value).forEach(refs);
    }
    refs(selected);
  }
  if (collection.status === 'captured') {
    for (const identifier of collection.fields['*elements']) {
      const node = index.get(identifier);
      if (node && (graph.has(identifier) || graph.size < 180)) resolve(node);
      else missing.add(identifier);
    }
  }
  return { top_keys: Object.keys(body).filter(safeKey).slice(0, 25), entity_types: types, entity_shapes: shapes,
    source_native_ids: [...ids].slice(0, 40), publication_candidates: publications,
    continuation_candidates: continuations.slice(0, 12), traversal_bounded: traversalBounded,
    projected_graph: [...graph.values()], unresolved_native_references: [...missing].slice(0, 60),
    collection_projection: collection,
    projection_limits: { array_elements: 8, string_characters: 5000, depth: 7,
      graph_entities: 180, update_candidates: 12, exact_collection_roots: 100,
      traversal_nodes: traversalNodeLimit, traversal_depth: traversalDepthLimit },
    duplicate_entity_ids: [...duplicates].slice(0, 30) };
}

function requestParameters(url) {
  const variables = url.searchParams.get('variables') || '';
  const fields = {};
  for (const match of variables.matchAll(/(?:^|[,(])([A-Za-z][A-Za-z0-9_]*)\s*:\s*([^,)]+)/g)) {
    const [, key, encodedValue] = match;
    let value;
    try { value = decodeURIComponent(encodedValue); } catch { value = encodedValue; }
    fields[key] = /^\d+$/.test(value) ? Number(value) : /^urn:li:fsd_company:\d+$/.test(value)
      ? value : { present: true, value_sha256: sha(value) };
  }
  return { variables_sha256: sha(variables), fields };
}

function initialPageProjection(html, navigation, onBody = () => {}) {
  if (!navigation || navigation.origin !== 'https://www.linkedin.com' ||
      navigation.pathname !== '/company/linkedin/posts/' || navigation.status !== 200 ||
      !['GET', 'HEAD'].includes(navigation.method) || !/html/i.test(navigation.content_type || '') ||
      !/^[a-f0-9]{64}$/.test(navigation.body_sha256 || '') || typeof html !== 'string' ||
      sha(Buffer.from(html)) !== navigation.body_sha256) throw new Error('Initial navigation unproved');
  const rows = [];
  let invalid = false;
  const observation = inspectEmbeddedBootstrap(html, onBody, 'resolver', ({source, body, result, wrapper_sha256}) => {
    if (result.envelope.request_family !== 'feed' || result.envelope.target_binding !== 'matched') return;
    if (!['reference_json', 'reference_entity_json'].includes(result.envelope.body_encoding) ||
        result.envelope.status !== 200 || !['GET', 'HEAD'].includes(result.envelope.method) ||
        result.collection.status !== 'observed_start_zero' || result.collection.paging.count !== 3 ||
        result.collection.root_ids.length !== 3) { invalid = true; return; }
    const url = new URL(source.request, navigation.origin);
    const operation = url.searchParams.get('queryId');
    if (url.pathname !== '/voyager/api/graphql' || !/^[a-zA-Z0-9_.-]{1,180}$/.test(operation || ''))
      { invalid = true; return; }
    const representation = describe(body);
    if (representation.traversal_bounded || representation.collection_projection.status !== 'captured' ||
        representation.duplicate_entity_ids.length || result.collection.root_ids.some(root =>
          !representation.projected_graph.some(node => node.entityUrn === root))) { invalid = true; return; }
    rows.push({representation_kind: 'initial_document_inert_reference', observed_at: navigation.observed_at,
      method: result.envelope.method, origin: url.origin, pathname: url.pathname, status: result.envelope.status,
      content_type: 'application/json', operation_id: operation, request_parameters: requestParameters(url),
      body_sha256: result.body_sha256, wrapper_sha256,
      navigation: {observed_at: navigation.observed_at, method: navigation.method, origin: navigation.origin,
        pathname: navigation.pathname, status: navigation.status, body_sha256: navigation.body_sha256}, representation});
  });
  const matched = observation.candidates.filter(candidate => candidate.envelope?.request_family === 'feed' &&
    candidate.envelope.target_binding === 'matched');
  if (invalid || rows.length !== 1 || matched.length !== 1 || observation.candidates.length >= 100)
    throw new Error('Unique initial collection unproved');
  return rows[0];
}

function batchRequestScope(url) {
  if (url.origin !== 'https://www.linkedin.com' || url.pathname !== '/voyager/api/graphql'
      || url.searchParams.getAll('queryId').length !== 1 || url.searchParams.getAll('variables').length !== 1
      || !/^voyagerFeedDashOrganizationalPageUpdates\.[A-Za-z0-9_.-]+$/.test(url.searchParams.get('queryId') || ''))
    throw new Error('Batch feed request differs');
  const variables = url.searchParams.get('variables'), fields = {};
  for (const key of ['organizationalPageUrn', 'start', 'count']) {
    const matches = [...variables.matchAll(new RegExp('(?:^|[,(])' + key + '\\s*:\\s*([^,)]+)', 'g'))];
    if (matches.length !== 1) throw new Error('Batch request field is ambiguous');
    fields[key] = decodeURIComponent(matches[0][1]);
  }
  if (fields.organizationalPageUrn !== 'urn:li:fsd_organizationalPage:1337'
      || !['3', '13'].includes(fields.start) || fields.count !== '10') throw new Error('Batch request scope differs');
  return {start: Number(fields.start), count: 10};
}

function batchPages(receipt, budget) {
  if (receipt.stopped || receipt.failure_kind) return [];
  if (![2, 3].includes(budget) || !receipt.initial_page) throw new Error('Batch initial page absent');
  const pages = [{page_mode: 'initial_document', start: 0, count: 3, record: receipt.initial_page}];
  const feeds = receipt.responses.filter(record => (record.operation_id || '').startsWith('voyagerFeedDashOrganizationalPageUpdates.'));
  if (feeds.length < 1 || feeds.length > budget - 1) throw new Error('Batch feed response count differs');
  for (let index = 0; index < feeds.length; index++) {
    const record = feeds[index], start = index === 0 ? 3 : 13, collection = record.representation?.collection_projection;
    const request = record.request_parameters?.fields;
    if (record.status !== 200 || !['GET', 'HEAD'].includes(record.method) || record.origin !== 'https://www.linkedin.com'
        || record.pathname !== '/voyager/api/graphql' || !/json/i.test(record.content_type || '')
        || request?.start !== start || request?.count !== 10
        || request?.organizationalPageUrn?.value_sha256 !== sha('urn:li:fsd_organizationalPage:1337')
        || collection?.status !== 'captured' || collection.fields.paging?.start !== start || collection.fields.paging?.count !== 10
        || collection.fields['*elements'].length > 10 || record.representation.traversal_bounded
        || record.representation.duplicate_entity_ids.length) throw new Error('Batch response scope unproved');
    pages.push({page_mode: 'api_following', start, count: 10, record});
  }
  return pages;
}

async function main() {
  // The user authorized bounded read-only testing on 2026-10-03.
  // Scope, request limits and security holds are enforced independently of login.
  if (!fs.existsSync(profile)) throw new Error('The dedicated project profile is absent.');
  if (fs.existsSync(holdPath)) throw new Error('A durable acquisition hold exists; no request was made.');
  const { chromium } = require(path.join(sourceRoot, 'M3', 'node_modules', 'playwright'));
  const receipt = { started_at: new Date().toISOString(), timezone: 'Asia/Calcutta', target,
    evidence_class: 'allowlisted_source_projection', original_bodies_retained: false,
    session_reported_by_user: true, session_verified: false, account_writes_blocked: true,
    responses: [], blocked_nonread_requests: 0, native_read_budget: 12, stopped: null };
  if (runtimeInitialPage) { receipt.page_mode = 'initial_document'; receipt.initial_page = null; }
  if (runtimeCompanyBatch) { receipt.batch_kind = 'company-source-batch/1'; receipt.batch_page_budget = batchPageBudget; receipt.initial_page = null; }
  let context, stopped = null, nativeReads = 0, nativeCandidates = 0, scopedBlocked = 0, feedReads = 0;
  let firstFeedRequest = null;
  let batchScrollArmed = false;
  let deadlineTimer = null, bootstrapBounded = false, bootstrapScopeMismatch = false;
  const bootstrapCandidates = [];
  const pending = [];
  function stop(reason, pathname, security) {
    // A later buffered security response must upgrade an authentication stop.
    if (stopped && (stopped.security_hold || !security)) return;
    stopped = { reason, pathname, observed_at: new Date().toISOString(), security_hold: security };
    receipt.stopped = stopped;
    if (security) fs.writeFileSync(holdPath, JSON.stringify({
      scope: 'project_linkedin_acquisition', ...stopped, identity_substitution_allowed: false,
    }, null, 2) + '\n');
  }
  try {
    if (boundedDocumentMode && (bootstrapDeadline <= Date.now() || bootstrapDeadline - Date.now() > 600000)) {
      receipt.failure_kind = 'DeadlineElapsed';
      stop('budget_exhausted', parsedTarget.pathname, false);
      return;
    }
    context = await chromium.launchPersistentContext(profile, {
      channel: 'chrome', headless: true, timeout: 20000,
      viewport: { width: 1280, height: 900 },
    });
    if (boundedDocumentMode) deadlineTimer = setTimeout(() => {
      receipt.failure_kind = 'DeadlineElapsed';
      stop('budget_exhausted', parsedTarget.pathname, false);
      context.close().catch(() => {});
    }, Math.max(1, bootstrapDeadline - Date.now()));
    const page = context.pages()[0] || await context.newPage();
    await context.route('**/*', async route => {
      const request = route.request();
      const url = new URL(request.url());
      if (boundedDocumentMode && bootstrapScopeMismatch) return route.abort('blockedbyclient');
      if (boundedDocumentMode && Date.now() >= bootstrapDeadline) {
        receipt.failure_kind = 'DeadlineElapsed';
        stop('budget_exhausted', url.pathname, false);
        return route.abort('blockedbyclient');
      }
      if (!['GET', 'HEAD'].includes(request.method())) {
        receipt.blocked_nonread_requests++; return route.abort('blockedbyclient');
      }
      if (stopped && /(?:^|\.)linkedin\.com$/.test(url.hostname)) return route.abort('blockedbyclient');
      if (/(?:^|\.)linkedin\.com$/.test(url.hostname) && /\/(?:checkpoint|challenge)\b/.test(url.pathname)) {
        stop('challenge_path', url.pathname, true); return route.abort('blockedbyclient');
      }
      if (boundedDocumentMode && request.isNavigationRequest() && request.frame() === page.mainFrame()
          && !isExactBootstrapNavigation(url.href)) {
        if (/(?:^|\.)linkedin\.com$/.test(url.hostname) && /\/(?:login|authwall|uas\/login)\b/.test(url.pathname))
          stop('authentication_required', url.pathname, false);
        bootstrapScopeMismatch = true;
        receipt.failure_kind = 'Error';
        return route.abort('blockedbyclient');
      }
      if (url.hostname === 'www.linkedin.com' && url.pathname.startsWith('/voyager/api/')) {
        nativeCandidates++;
        const operation = url.searchParams.get('queryId') || '';
        const scoped = operation.startsWith('voyagerOrganizationDashCompanies.') ||
          operation.startsWith('voyagerFeedDashOrganizationalPageUpdates.');
        if (!scoped) { scopedBlocked++; return route.abort('blockedbyclient'); }
        if (runtimeCompanyBatch && operation.startsWith('voyagerFeedDashOrganizationalPageUpdates.')) {
          try {
            const scope = batchRequestScope(url);
            if (feedReads >= batchPageBudget - 1 || scope.start !== (feedReads === 0 ? 3 : 13)
                || feedReads > 0 && !batchScrollArmed) return route.abort('blockedbyclient');
          } catch { receipt.failure_kind = 'Error'; return route.abort('blockedbyclient'); }
        }
        if (bootstrapBodyMode && bootstrapRequestMetadata(url.href).target_binding === 'mismatched') {
          receipt.failure_kind = 'Error'; bootstrapScopeMismatch = true;
          return route.abort('blockedbyclient');
        }
        if (nativeReads >= receipt.native_read_budget) return route.abort('blockedbyclient');
        if (operation.startsWith('voyagerFeedDashOrganizationalPageUpdates.') &&
            feedReads++ >= (runtimeCompanyBatch ? batchPageBudget - 1 : 1 + Number(observeContinuation) + Number(observeBoundary)))
          return route.abort('blockedbyclient');
        nativeReads++;
      }
      return route.continue();
    });
    page.on('response', response => {
      const url = new URL(response.url());
      const mainNavigation = response.request().isNavigationRequest() && response.request().frame() === page.mainFrame();
      if (boundedDocumentMode && mainNavigation && !isExactBootstrapNavigation(url.href)) {
        bootstrapScopeMismatch = true;
        receipt.failure_kind = 'Error';
        return;
      }
      if (url.hostname !== 'www.linkedin.com' ||
          !(mainNavigation ||
            /^(voyagerOrganizationDashCompanies|voyagerFeedDashOrganizationalPageUpdates)\./.test(url.searchParams.get('queryId') || ''))) return;
      if (!firstFeedRequest && (url.searchParams.get('queryId') || '').startsWith('voyagerFeedDashOrganizationalPageUpdates.'))
        firstFeedRequest = response.request();
      pending.push((async () => {
        const status = response.status();
        if ([403, 429, 999].includes(status)) stop(`restricted_http_${status}`, url.pathname, true);
        if (status === 401) stop('authentication_required', url.pathname, false);
        const record = { observed_at: new Date().toISOString(), method: response.request().method(),
          origin: url.origin, pathname: url.pathname, status,
          content_type: await response.headerValue('content-type'),
          query_keys: [...url.searchParams.keys()].filter(safeKey),
          operation_id: /^[a-zA-Z0-9_.-]{1,180}$/.test(url.searchParams.get('queryId') || '')
            ? url.searchParams.get('queryId') : null,
          request_parameters: requestParameters(url) };
        try {
          const bytes = await response.body();
          record.body_sha256 = sha(bytes); record.body_bytes = bytes.length;
          if ((runtimeInitialPage || runtimeCompanyBatch) && mainNavigation) {
            if (receipt.initial_page) throw new Error('Multiple initial documents');
            receipt.initial_page = initialPageProjection(bytes.toString('utf8'), record, body => {
              const signal = semanticSignal(body);
              if (signal) stop(signal.reason, url.pathname, signal.security);
            });
          }
          if (bootstrap) {
            if (bytes.length > 8_000_000) {
              bootstrapBounded = true; receipt.failure_kind = 'BootstrapLimit';
              stop('budget_exhausted', url.pathname, false);
            } else if (/json/i.test(record.content_type || '')) {
              const body = JSON.parse(bytes.toString('utf8'));
              const binding = bootstrapRequestMetadata(url.href);
              if (bootstrapBodyMode && !mainNavigation && binding.target_binding === 'mismatched') {
                receipt.failure_kind = 'Error'; bootstrapScopeMismatch = true;
                return;
              }
              if (!bootstrapBodyMode || !mainNavigation) {
                const signal = semanticSignal(body);
                if (signal) stop(signal.reason, url.pathname, signal.security);
              }
              if (bootstrapCandidates.length >= 100) {
                bootstrapBounded = true; receipt.failure_kind = 'BootstrapLimit';
                stop('budget_exhausted', url.pathname, false);
              } else if (bootstrapBodyMode && mainNavigation) bootstrapCandidates.push({
                source: 'initial_document_body_json', outer_body_sha256: record.body_sha256,
                ...(bootstrapResolver ? summarizeBootstrapResolverEnvelope : summarizeBootstrapBodyEnvelope)(body, decoded => {
                  const signal = semanticSignal(decoded);
                  if (signal) stop(signal.reason, url.pathname, signal.security);
                }) });
              else bootstrapCandidates.push({
                source: mainNavigation ? 'initial_document_json' :
                  (record.operation_id || '').startsWith('voyagerFeedDashOrganizationalPageUpdates.')
                  ? 'native_feed_json' : 'native_company_json',
                body_sha256: record.body_sha256,
                ...(bootstrapBodyMode ? {outer_body_sha256: null,
                  envelope: {method: response.request().method(), status, ...binding, body_encoding: 'json_object',
                    ...(bootstrapResolver ? {body_length: null} : {})},
                  collection: binding.request_family === 'feed' && binding.target_binding === 'matched' ?
                    (bootstrapResolver ? summarizeBootstrapResolverCandidate : summarizeBootstrapCandidate)(body) : summarizeBootstrapCandidate({error: {code: 'TARGET_UNVERIFIED'}}),
                  structure: summarizeBootstrapStructure(body)} :
                  bootstrapStructure ? {structure: summarizeBootstrapStructure(body)} : summarizeBootstrapCandidate(body)) });
            } else if (mainNavigation && url.pathname === parsedTarget.pathname && /html/i.test(record.content_type || '')) {
              const observation = inspectEmbeddedBootstrap(bytes.toString('utf8'), body => {
                const signal = semanticSignal(body);
                if (signal) stop(signal.reason, url.pathname, signal.security);
              }, bootstrapResolver ? 'resolver' : bootstrapBody ? 'body' : bootstrapStructure);
              if (bootstrapStructure || bootstrapBodyMode) {
                const remaining = Math.max(0, 100 - bootstrapCandidates.length);
                bootstrapCandidates.push(...observation.candidates.slice(0, remaining));
                if (observation.status === 'bounded') bootstrapBounded = true;
                if (observation.candidates.length > remaining) {
                  bootstrapBounded = true; receipt.failure_kind = 'BootstrapLimit';
                  stop('budget_exhausted', url.pathname, false);
                }
              } else if (observation.status === 'bounded' || observation.candidates.length + bootstrapCandidates.length > 100) {
                bootstrapBounded = true; receipt.failure_kind = 'BootstrapLimit';
                stop('budget_exhausted', url.pathname, false);
              } else bootstrapCandidates.push(...observation.candidates);
            }
            return;
          }
          if (/json/i.test(record.content_type || '') && bytes.length <= 8_000_000) {
            const body = JSON.parse(bytes.toString('utf8'));
            const signal = semanticSignal(body);
            if (signal) stop(signal.reason, url.pathname, signal.security);
            record.representation = describe(body);
          }
        } catch {
          record.body_observation_failed = true;
          if (boundedDocumentMode) receipt.failure_kind = 'Error';
        }
        receipt.responses.push(record);
      })());
    });
    const navigation = await page.goto(target, { waitUntil: 'domcontentloaded', timeout: 30000 });
    receipt.navigation_status = navigation ? navigation.status() : null;
    await page.waitForTimeout(5000);
    await Promise.allSettled(pending);
    // Check the landed page before admitting continuation or a boundary read.
    const landedUrl = new URL(page.url());
    if (/\/(?:checkpoint|challenge)\b/.test(landedUrl.pathname)) stop('challenge_path', landedUrl.pathname, true);
    if (/\/(?:login|authwall|uas\/login)\b/.test(landedUrl.pathname)) stop('authentication_required', landedUrl.pathname, false);
    if (boundedDocumentMode && !isExactBootstrapNavigation(landedUrl.href)) throw new Error('Bootstrap navigation scope mismatch');
    if (!stopped) {
      const warning = await page.evaluate(() => /verify your identity|security verification|account (?:has been )?restricted|temporarily restricted|unusual activity|captcha/i.test(document.body?.innerText || ''));
      if (warning) stop('possible_security_warning_text', landedUrl.pathname, true);
    }
    if (runtimeCompanyBatch && batchPageBudget === 3 && !stopped && !receipt.failure_kind) {
      // A single ordinary scroll after proving the separately observed first pages.
      batchPages(receipt, 2);
      batchScrollArmed = true;
      const continuation = page.waitForResponse(response => {
        try { return batchRequestScope(new URL(response.url())).start === 13; } catch { return false; }
      }, {timeout: 12000}).then(() => true).catch(() => false);
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      receipt.continuation_response_observed = await continuation;
      await page.waitForTimeout(1000);
    }
    if (observeContinuation && !stopped) {
      const continuation = page.waitForResponse(response =>
        (new URL(response.url()).searchParams.get('queryId') || '').startsWith('voyagerFeedDashOrganizationalPageUpdates.'),
        { timeout: 12000 }).then(() => true).catch(() => false);
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      receipt.continuation_response_observed = await continuation;
      await page.waitForTimeout(1000);
    }
    if (observeBoundary && !stopped && firstFeedRequest) {
      const feed = receipt.responses.find(item => (item.operation_id || '').startsWith('voyagerFeedDashOrganizationalPageUpdates.'));
      const paging = feed?.representation?.continuation_candidates?.find(item => item.path.endsWith('.paging'))?.projection;
      if (Number.isSafeInteger(paging?.total) && paging.total >= 0 && paging.total <= 1000) {
        // One discriminating read using the exact observed GET recipe. No endpoint sweep.
        const url = new URL(firstFeedRequest.url());
        const variables = url.searchParams.get('variables');
        if (!variables || !/(?:^|[,(])start:\d+/.test(variables)) throw new Error('Observed recipe has no start argument');
        url.searchParams.set('variables', variables.replace(/([,(]start:)\d+/, `$1${paging.total}`));
        const csrf = await firstFeedRequest.headerValue('csrf-token');
        if (!csrf) throw new Error('Observed request lacks its required attachment');
        receipt.boundary_probe = { start: paging.total, count: paging.count, route_family: feed.operation_id };
        await page.evaluate(async ({ url, attachment }) => {
          const result = await fetch(url, { method: 'GET', credentials: 'include', headers: {
            'csrf-token': attachment, 'accept': 'application/vnd.linkedin.normalized+json+2.1',
            'x-restli-protocol-version': '2.0.0',
          } });
          await result.arrayBuffer();
        }, { url: url.toString(), attachment: csrf });
        // The attachment stays in process memory. It is never returned, logged or persisted.
        await page.waitForTimeout(500);
      }
    }
    const finalUrl = new URL(page.url());
    receipt.final_origin = finalUrl.origin; receipt.final_pathname = finalUrl.pathname;
    if (/\/(?:checkpoint|challenge)\b/.test(finalUrl.pathname)) stop('challenge_path', finalUrl.pathname, true);
    if (/\/(?:login|authwall|uas\/login)\b/.test(finalUrl.pathname)) stop('authentication_required', finalUrl.pathname, false);
    if (boundedDocumentMode && !isExactBootstrapNavigation(finalUrl.href)) throw new Error('Bootstrap final scope mismatch');
    if (!stopped) {
      if (boundedDocumentMode) {
        const session = await page.evaluate(() => ({
          signed_in: !!document.querySelector('.global-nav__me, .global-nav__me-photo, [data-test-global-nav-link="me"]'),
          warning: /verify your identity|security verification|account (?:has been )?restricted|temporarily restricted|unusual activity|captcha/i.test(document.body?.innerText || ''),
          signin: /sign in to linkedin|join linkedin|sign in to see/i.test(document.body?.innerText || ''),
        }));
        if (session.warning) stop('possible_security_warning_text', finalUrl.pathname, true);
        if (session.signin && !session.signed_in) stop('authentication_required', finalUrl.pathname, false);
        receipt.session_verified = session.signed_in && !stopped;
      } else {
        receipt.dom = await page.evaluate(() => {
        const text = document.body?.innerText || '';
        const urns = [...new Set((document.documentElement.innerHTML.match(
          /urn:li:(?:activity|share|ugcPost|organization|fsd_company):[0-9]+/g) || []))];
        return { title: document.title, heading: document.querySelector('h1')?.innerText?.slice(0, 120) || null,
          signed_in_navigation: !!document.querySelector('.global-nav__me, .global-nav__me-photo, [data-test-global-nav-link="me"]'),
          challenge_text: /verify your identity|security verification|account (?:has been )?restricted|temporarily restricted|unusual activity|captcha/i.test(text),
          has_signin_prompt: /sign in to linkedin|join linkedin|sign in to see/i.test(text),
          company_post_links: [...new Set([...document.querySelectorAll('a[href]')].map(a => a.href)
            .filter(url => /^https:\/\/www\.linkedin\.com\/(?:feed\/update\/urn:li:(?:activity|share|ugcPost):[0-9]+|posts\/[^?]+)[/]?(?:\?.*)?$/.test(url))
            .map(url => url.split('?')[0]))].slice(0, 12),
          native_urn_candidates: urns.slice(0, 30),
          script_kinds: [...new Set([...document.scripts].map(s => s.type || 'javascript'))],
        };
      });
      if (receipt.dom.challenge_text) stop('challenge_or_restriction_text', finalUrl.pathname, true);
      receipt.session_verified = receipt.dom.signed_in_navigation && !stopped;
      }
    }
    await Promise.allSettled(pending);
    receipt.native_reads_admitted = nativeReads;
    receipt.native_request_candidates = nativeCandidates;
    receipt.unrelated_native_reads_blocked = scopedBlocked;
  } catch (error) {
    receipt.failure_kind = receipt.failure_kind || error.name || 'Error';
    receipt.failure_stage = context ? 'navigation_or_capture' : 'browser_launch';
    receipt.browser_install_not_found = !context && /executable.*doesn.t exist|distribution.*not found/i.test(error.message || '');
    // Browser error messages can contain full URLs. Do not log them or their stacks.
  } finally {
    if (deadlineTimer) clearTimeout(deadlineTimer);
    if (context) {
      if (boundedDocumentMode) {
        try { await context.close(); }
        catch { receipt.failure_kind = receipt.failure_kind || 'Error'; }
      } else await context.close();
    }
    // Closing ends response admission; settle every admitted observer before output.
    const settled = await Promise.allSettled(pending);
    if (boundedDocumentMode && settled.some(result => result.status === 'rejected'))
      receipt.failure_kind = receipt.failure_kind || 'Error';
    receipt.native_reads_admitted = nativeReads;
    receipt.native_request_candidates = nativeCandidates;
    receipt.unrelated_native_reads_blocked = scopedBlocked;
    receipt.finished_at = new Date().toISOString();
    if (runtimeInitialPage && (!receipt.initial_page || receipt.stopped || receipt.failure_kind)) {
      receipt.initial_page = null;
      receipt.session_verified = false;
      receipt.failure_kind = receipt.failure_kind || 'Error';
    }
    if (runtimeCompanyBatch) {
      try {
        receipt.batch_pages = batchPages(receipt, batchPageBudget);
        receipt.batch_shortfall_reason = receipt.batch_pages.length === 2 && batchPageBudget === 3 ? 'continuation_not_observed' : null;
      } catch { receipt.batch_pages = []; receipt.failure_kind = receipt.failure_kind || 'Error'; }
      // Release publication projections only through the distinct batch page list.
      receipt.initial_page = null;
      receipt.responses = receipt.responses.filter(record => !(record.operation_id || '').startsWith('voyagerFeedDashOrganizationalPageUpdates.'));
      if (receipt.stopped || receipt.failure_kind) { receipt.batch_pages = []; receipt.responses = []; receipt.session_verified = false; }
    }
    if (bootstrap) {
      console.log(JSON.stringify(bootstrapReceipt(receipt,
        summarizeBootstrapObservations(bootstrapCandidates, bootstrapBounded))));
    } else if (runtimeCapture) {
      // The runtime commits this projection to its expiring source Journal.
      // Do not create another unrestricted publication/DOM receipt on disk.
      delete receipt.dom;
      const value = JSON.stringify(receipt);
      console.log(Buffer.byteLength(value) <= 2_000_000 ? value : JSON.stringify({ failure_kind: 'ProjectionLimit' }));
    } else {
      fs.writeFileSync(resultPath, JSON.stringify(receipt, null, 2) + '\n');
      console.log(JSON.stringify({ result: path.relative(root, resultPath),
        session_verified: receipt.session_verified, stopped: receipt.stopped?.reason || null,
        navigation_status: receipt.navigation_status, native_responses: receipt.responses.length,
        failure_kind: receipt.failure_kind || null }));
    }
  }
}
function bootstrapReceipt(receipt, observation) {
  const stop = receipt.stopped;
  const reason = stop ? stop.reason === 'authentication_required' ? 'authentication_required' :
    stop.reason === 'budget_exhausted' ? 'budget_exhausted' :
    /challenge/.test(stop.reason) ? 'challenge' : /restricted|restriction/.test(stop.reason) ? 'restricted' : 'operator_stop' : null;
  const failures = new Set(['Error', 'TimeoutError', 'ProjectionLimit', 'BootstrapLimit', 'DeadlineElapsed']);
  return { session_verified: receipt.session_verified === true && !stop && !receipt.failure_kind,
    stopped: stop ? { reason, security_hold: stop.security_hold === true } : null,
    failure_kind: receipt.failure_kind ? failures.has(receipt.failure_kind) ? receipt.failure_kind : 'Error' : null,
    native_reads_admitted: receipt.native_reads_admitted,
    navigation_status: receipt.navigation_status ?? null, account_writes_blocked: true,
    bootstrap_observation: observation };
}
module.exports = { projection, describe, requestParameters, collectionProjection, semanticSignal, parseProbeArgs, bootstrapReceipt, isExactBootstrapNavigation, initialPageProjection, batchRequestScope, batchPages };
if (require.main === module) main().catch(error => {
  console.error(JSON.stringify({ failure_kind: error.name || 'Error' })); process.exitCode = 1;
});
