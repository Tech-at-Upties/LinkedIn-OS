/* One authorized, read-only company navigation. Never export session material. */
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const root = path.resolve(__dirname, '..');
const profile = path.join(root, '.local', 'linkedin-test-browser');
const holdPath = path.join(root, '.local', 'linkedin-acquisition-hold.json');
const observeContinuation = process.argv.includes('--continuation');
const observeBoundary = process.argv.includes('--boundary');
if (observeContinuation && observeBoundary) throw new Error('Choose one additional read experiment.');
const runId = new Date().toISOString().replace(/[:.]/g, '-');
const resultPath = path.join(root, 'docs', 'results', `company-read-${runId}.json`);
const sourceRoot = process.env.NOS_SOURCE_ROOT || path.resolve(root, '..', 'NOS-V1');
const target = process.argv.slice(2).find(arg => !arg.startsWith('--')) || 'https://www.linkedin.com/company/linkedin/posts/';
const parsedTarget = new URL(target);
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
function collectionProjection(body) {
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
  const containers = [body, body?.data, body?.data?.data];
  for (const container of containers) {
    if (!container || typeof container !== 'object' || Array.isArray(container)) continue;
    for (const key of ['errors', 'error']) {
      const errors = Array.isArray(container[key]) ? container[key] : [container[key]];
      for (const error of errors.slice(0, 20)) {
        if (!error || typeof error !== 'object' || Array.isArray(error)) continue;
        if (error.code === 'CHALLENGE') return { reason: 'possible_semantic_challenge', security: true };
        if (error.status === 401 || error.code === 'AUTH_REQUIRED') return { reason: 'authentication_required', security: false };
        if ([403, 429, 999].includes(error.status) || ['ACCESS_DENIED', 'RATE_LIMITED'].includes(error.code))
          return { reason: 'possible_semantic_restriction', security: true };
      }
    }
  }
  return null;
}
function describe(body) {
  const types = {}, ids = new Set(), publications = [], continuations = [], shapes = {};
  let visited = 0;
  function visit(node, at) {
    if (++visited > 80000 || !node || typeof node !== 'object') return;
    if (Array.isArray(node)) { node.forEach((item, i) => visit(item, `${at}[${i}]`)); return; }
    const type = node.$type || node.__typename;
    if (typeof type === 'string' && /^[a-zA-Z0-9_.$-]{1,180}$/.test(type)) {
      types[type] = (types[type] || 0) + 1;
      shapes[type] = [...new Set([...(shapes[type] || []), ...Object.keys(node).filter(safeKey)])].slice(0, 60);
    }
    for (const [key, value] of Object.entries(node)) {
      if (sourceUrn(value)) ids.add(value);
      if (/^(?:paging|pagination|nextCursor|paginationToken|nextPageToken)$/.test(key)) {
        continuations.push({ path: `${at}.${key}`, kind: typeof value,
          projection: /^(?:paging|pagination)$/.test(key) ? projection(value) : undefined,
          present: value != null, value_sha256: value == null ? null : sha(JSON.stringify(value)) });
      }
    }
    if (sourceUrn(node.entityUrn) && /(?:activity|share|ugcPost|fsd_update):/.test(node.entityUrn) &&
        publications.length < 12) publications.push({ path: at, fields: projection(node) });
    for (const [key, value] of Object.entries(node)) if (safeKey(key)) visit(value, `${at}.${key}`);
  }
  visit(body, '$');
  const included = Array.isArray(body.included) ? body.included : [];
  const index = new Map(included.filter(node => typeof node.entityUrn === 'string').map(node => [node.entityUrn, node]));
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
  included.filter(node => node.$type === 'com.linkedin.voyager.dash.feed.Update').slice(0, 12).forEach(resolve);
  const duplicates = included.map(node => node.entityUrn).filter((id, i, arr) => id && arr.indexOf(id) !== i);
  return { top_keys: Object.keys(body).filter(safeKey).slice(0, 25), entity_types: types, entity_shapes: shapes,
    source_native_ids: [...ids].slice(0, 40), publication_candidates: publications,
    continuation_candidates: continuations.slice(0, 12), traversal_bounded: visited > 80000,
    projected_graph: [...graph.values()], unresolved_native_references: [...missing].slice(0, 60),
    collection_projection: collectionProjection(body),
    projection_limits: { array_elements: 8, string_characters: 5000, depth: 7,
      graph_entities: 180, update_candidates: 12, exact_collection_roots: 100 },
    duplicate_entity_ids: [...new Set(duplicates)].slice(0, 30) };
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
  let context, stopped = null, nativeReads = 0, nativeCandidates = 0, scopedBlocked = 0, feedReads = 0;
  let firstFeedRequest = null;
  const pending = [];
  function stop(reason, pathname, security) {
    if (stopped) return;
    stopped = { reason, pathname, observed_at: new Date().toISOString(), security_hold: security };
    receipt.stopped = stopped;
    if (security) fs.writeFileSync(holdPath, JSON.stringify({
      scope: 'project_linkedin_acquisition', ...stopped, identity_substitution_allowed: false,
    }, null, 2) + '\n');
  }
  try {
    context = await chromium.launchPersistentContext(profile, {
      channel: 'chrome', headless: true, timeout: 20000,
      viewport: { width: 1280, height: 900 },
    });
    await context.route('**/*', async route => {
      const request = route.request();
      const url = new URL(request.url());
      if (!['GET', 'HEAD'].includes(request.method())) {
        receipt.blocked_nonread_requests++; return route.abort('blockedbyclient');
      }
      if (stopped && /(?:^|\.)linkedin\.com$/.test(url.hostname)) return route.abort('blockedbyclient');
      if (/(?:^|\.)linkedin\.com$/.test(url.hostname) && /\/(?:checkpoint|challenge)\b/.test(url.pathname)) {
        stop('challenge_path', url.pathname, true); return route.abort('blockedbyclient');
      }
      if (url.hostname === 'www.linkedin.com' && url.pathname.startsWith('/voyager/api/')) {
        nativeCandidates++;
        const operation = url.searchParams.get('queryId') || '';
        const scoped = operation.startsWith('voyagerOrganizationDashCompanies.') ||
          operation.startsWith('voyagerFeedDashOrganizationalPageUpdates.');
        if (!scoped) { scopedBlocked++; return route.abort('blockedbyclient'); }
        if (nativeReads >= receipt.native_read_budget) return route.abort('blockedbyclient');
        if (operation.startsWith('voyagerFeedDashOrganizationalPageUpdates.') &&
            feedReads++ >= 1 + Number(observeContinuation) + Number(observeBoundary))
          return route.abort('blockedbyclient');
        nativeReads++;
      }
      return route.continue();
    });
    const page = context.pages()[0] || await context.newPage();
    page.on('response', response => {
      const url = new URL(response.url());
      const mainNavigation = response.request().isNavigationRequest() && response.request().frame() === page.mainFrame();
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
          if (/json/i.test(record.content_type || '') && bytes.length <= 8_000_000) {
            const body = JSON.parse(bytes.toString('utf8'));
            const signal = semanticSignal(body);
            if (signal) stop(signal.reason, url.pathname, signal.security);
            record.representation = describe(body);
          }
        } catch { record.body_observation_failed = true; }
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
    if (!stopped) {
      const warning = await page.evaluate(() => /verify your identity|security verification|account (?:has been )?restricted|temporarily restricted|unusual activity|captcha/i.test(document.body?.innerText || ''));
      if (warning) stop('possible_security_warning_text', landedUrl.pathname, true);
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
    if (!stopped) {
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
    await Promise.allSettled(pending);
    receipt.native_reads_admitted = nativeReads;
    receipt.native_request_candidates = nativeCandidates;
    receipt.unrelated_native_reads_blocked = scopedBlocked;
  } catch (error) {
    receipt.failure_kind = error.name || 'Error';
    // Browser error messages can contain full URLs. Do not log them or their stacks.
  } finally {
    if (context) await context.close();
    receipt.finished_at = new Date().toISOString();
    fs.writeFileSync(resultPath, JSON.stringify(receipt, null, 2) + '\n');
    console.log(JSON.stringify({ result: path.relative(root, resultPath),
      session_verified: receipt.session_verified, stopped: receipt.stopped?.reason || null,
      navigation_status: receipt.navigation_status, native_responses: receipt.responses.length,
      failure_kind: receipt.failure_kind || null }));
  }
}
module.exports = { projection, describe, requestParameters, collectionProjection, semanticSignal };
if (require.main === module) main().catch(error => {
  console.error(JSON.stringify({ failure_kind: error.name || 'Error' })); process.exitCode = 1;
});
