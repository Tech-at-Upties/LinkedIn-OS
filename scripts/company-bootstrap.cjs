// Pure shape inspection of the observed company collection. No source access.
'use strict';
const crypto = require('node:crypto');

const recipe = 'feedDashOrganizationalPageUpdatesByOrganizationalPageRelevanceFeed';
const collectionType = 'com.linkedin.restli.common.CollectionResponse';
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const own = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
const nativeRoot = value => typeof value === 'string' && value.length <= 1000 &&
  /^urn:li:(?:activity|share|ugcPost|organization|fsd_company|fsd_update):[A-Za-z0-9_(),:-]+$/.test(value) &&
  !/[\r\n]/.test(value);
const pagingInteger = value => Number.isSafeInteger(value) && value >= 0;
const nonempty = value => Array.isArray(value) ? value.length > 0 :
  record(value) ? Object.keys(value).length > 0 : Boolean(value);
const credentialLiteral = /(?:gh[pousr]_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9]{20,})/i;
const sensitiveStructure = /csrf|cookie|password|secret|authorization|bearer|token|api[_-]?key/i;
const structureKey = value => typeof value === 'string' &&
  /^\*?[A-Za-z_$][A-Za-z0-9_.$-]{0,99}$/.test(value) && !credentialLiteral.test(value) && !sensitiveStructure.test(value);
const structureUrn = value => typeof value === 'string' && value.length <= 1000 &&
  /^urn:li:[A-Za-z][A-Za-z0-9_]*:[A-Za-z0-9_(),:.-]{1,1000}$/.test(value) && !credentialLiteral.test(value) && !sensitiveStructure.test(value);

function summarizeBootstrapStructure(body) {
  const rootKeys = record(body) ? Object.keys(body).filter(structureKey).slice(0, 100) : [];
  if (hasSemanticErrors(body)) return { status: 'invalid', root_keys: rootKeys, nodes: [] };
  const nodes = [], pending = [{value: body, path: '$', depth: 0}];
  let visited = 0, bounded = false, relevant = false;
  while (pending.length) {
    const {value, path, depth} = pending.pop();
    if (++visited > 80000) { bounded = true; break; }
    if (depth > 64 || path.length > 2000) { bounded = true; continue; }
    const kind = value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value;
    if (!['object', 'array'].includes(kind) && path !== '$') continue;
    const allKeys = record(value) ? Object.keys(value) : [];
    const keys = allKeys.filter(structureKey);
    if (keys.length !== allKeys.length || keys.length > 100) bounded = true;
    const type = record(value) ? value.$type || value.__typename : null;
    const typeName = typeof type === 'string' && type.length <= 200 &&
      /^com\.linkedin\.[A-Za-z0-9_.$]+$/.test(type) && !credentialLiteral.test(type) && !sensitiveStructure.test(type) ? type : null;
    if (typeof type === 'string' && typeName === null) bounded = true;
    const refs = [];
    for (const key of keys) {
      const item = value[key];
      if (!key.startsWith('*') && !/(?:Urn|^urn$|^elements$|^included$)$/.test(key) && !structureUrn(item)) continue;
      if (refs.length >= 20) { bounded = true; break; }
      const entries = Array.isArray(item) ? item : item == null ? [] : [item];
      const valid = entries.every(structureUrn);
      const unique = valid ? [...new Set(entries)] : [];
      if (unique.length > 100) bounded = true;
      refs.push({key, ids: unique.slice(0, 100), state: item === null ? 'null' : valid ? 'value' : 'invalid',
        count: entries.length});
    }
    const matches = typeName !== null || refs.length > 0 || keys.some(key => ['elements', '*elements', 'included', 'paging'].includes(key));
    if (matches) relevant = true;
    if (path === '$' || matches || kind === 'array' && /\.(?:\*?elements|included)$/.test(path)) {
      if (nodes.length >= 100) { bounded = true; break; }
      nodes.push({path, kind, keys: keys.slice(0, 100), type_name: typeName, reference_fields: refs});
    }
    if (kind === 'object' || kind === 'array') {
      const length = kind === 'array' ? value.length : keys.length;
      const remaining = Math.max(0, 80000 - visited - pending.length);
      if (length > remaining) bounded = true;
      for (let index = Math.min(length, remaining) - 1; index >= 0; index--) {
        const key = kind === 'array' ? index : keys[index];
        pending.push({value: value[key], path: kind === 'array' ? `${path}[${key}]` : `${path}.${key}`, depth: depth + 1});
      }
    }
  }
  return {status: bounded ? 'bounded' : relevant ? 'observed' : 'absent', root_keys: rootKeys, nodes};
}

function bootstrapRequestMetadata(request) {
  let request_family = 'unknown', target_binding = 'unknown';
  if (typeof request !== 'string' || request.length > 20000) return {request_family, target_binding};
  try {
    const url = new URL(request, 'https://www.linkedin.com');
    if (url.origin !== 'https://www.linkedin.com' || !url.pathname.startsWith('/voyager/api/')
        || url.username || url.password) return {request_family: 'other', target_binding};
    if (url.searchParams.getAll('queryId').length !== 1) return {request_family, target_binding};
    const operation = url.searchParams.get('queryId');
    request_family = operation.startsWith('voyagerFeedDashOrganizationalPageUpdates.') ? 'feed' :
      operation.startsWith('voyagerOrganizationDashCompanies.') ? 'company' : 'other';
    if (request_family === 'feed' && url.searchParams.getAll('variables').length === 1) {
      const variables = url.searchParams.get('variables');
      const matches = [...variables.matchAll(/(?:^|[,(])organizationalPageUrn\s*:\s*([^,)]+)/g)];
      if (matches.length === 1) {
        const value = decodeURIComponent(matches[0][1]);
        if (/^urn:li:fsd_organizationalPage:\d+$/.test(value))
          target_binding = value === 'urn:li:fsd_organizationalPage:1337' ? 'matched' : 'mismatched';
      }
    }
  } catch { /* Opaque request remains unknown; never persist it. */ }
  return {request_family, target_binding};
}

function summarizeBootstrapBodyEnvelope(source, onBody = () => {}, onDecoded = () => {}) {
  const method = source?.method === 'GET' || source?.method === 'HEAD' ? source.method :
    source?.method == null ? null : 'other';
  const status = Number.isInteger(source?.status) &&
    (source.status >= 100 && source.status <= 599 || source.status === 999) ? source.status : null;
  const envelope = {method, status, ...bootstrapRequestMetadata(source?.request), body_encoding: 'missing'};
  const scoped = ['GET', 'HEAD'].includes(method) &&
    (envelope.request_family === 'company' || envelope.request_family === 'feed' && envelope.target_binding === 'matched');
  const rejected = encoding => {
    envelope.body_encoding = encoding;
    return {body_sha256: null, envelope, collection: summarizeBootstrapCandidate({error: {code: 'BODY_UNAVAILABLE'}}),
      structure: {status: encoding === 'bounded' ? 'bounded' : 'invalid', root_keys: [], nodes: []}};
  };
  // HTTP and outer/decoded semantic signals are independently inspected in memory.
  if (envelope.target_binding === 'mismatched') return rejected('invalid');
  if (scoped) onBody(source);
  if (scoped && status !== null) onBody({error: {status}});
  if (!record(source) || !own(source, 'body')) return rejected('missing');
  if (source.body === null) return rejected('null');
  if (method === 'other' || hasSemanticErrors(source)) return rejected('invalid');
  let body, bytes;
  if (typeof source.body === 'string') {
    if (Buffer.byteLength(source.body) > 8_000_000) return rejected('bounded');
    try { body = JSON.parse(source.body); }
    catch { return rejected('invalid'); }
    if (!record(body) && !Array.isArray(body)) return rejected('unsupported');
    if (scoped) onBody(body);
    bytes = source.body;
    envelope.body_encoding = 'json_string';
  } else if (record(source.body) || Array.isArray(source.body)) {
    body = source.body;
    if (scoped) onBody(body);
    const pending = [{value: body, depth: 0}];
    let visited = 0;
    while (pending.length) {
      const current = pending.pop();
      if (++visited > 80000 || current.depth > 64) return rejected('bounded');
      if (current.value && typeof current.value === 'object') {
        const children = Object.values(current.value);
        if (children.length + pending.length > 80000 - visited) return rejected('bounded');
        for (const value of children) pending.push({value, depth: current.depth + 1});
      }
    }
    try { bytes = JSON.stringify(body); }
    catch { return rejected('bounded'); }
    if (Buffer.byteLength(bytes) > 8_000_000) return rejected('bounded');
    envelope.body_encoding = 'json_object';
  } else return rejected('unsupported');
  onDecoded(body);
  return {body_sha256: crypto.createHash('sha256').update(bytes).digest('hex'), envelope,
    collection: scoped && envelope.request_family === 'feed' && envelope.target_binding === 'matched' ? summarizeBootstrapCandidate(body) :
      summarizeBootstrapCandidate({error: {code: 'TARGET_UNVERIFIED'}}), structure: summarizeBootstrapStructure(body)};
}

function hasSemanticErrors(body) {
  const containers = [body, body?.data, body?.data?.data, body?.data?.data?.[recipe]];
  return containers.filter(record).some(container =>
    ['errors', 'error'].some(key => own(container, key) && nonempty(container[key])));
}

function buildBootstrapReferenceIndex(html) {
  const entries = new Map();
  if (typeof html !== 'string' || Buffer.byteLength(html) > 8_000_000) return {entries, bounded: true};
  let count = 0;
  for (const match of html.matchAll(/<(script|code)\b([^>]*)>([\s\S]*?)<\/\1\s*>/gi)) {
    if (++count > 2000) return {entries, bounded: true};
    const [, tag, attributes, content] = match;
    const id = attributes.match(/(?:^|\s)id\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/i);
    if (!id) continue;
    const value = id[1] ?? id[2] ?? id[3];
    const type = attributes.match(/(?:^|\s)type\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/i);
    const inert = tag.toLowerCase() === 'code' || type &&
      ['application/json', 'application/ld+json'].includes((type[1] ?? type[2] ?? type[3]).toLowerCase().trim());
    const previous = entries.get(value);
    entries.set(value, previous ? {ambiguous: true} : {content, inert});
  }
  return {entries, bounded: false};
}

function decodeInertEntities(text) {
  const named = {quot: '"', apos: "'", amp: '&', lt: '<', gt: '>'};
  return text.replace(/&(#x[0-9a-f]+|#[0-9]+|quot|apos|amp|lt|gt);/gi, (whole, entity) => {
    if (entity[0] !== '#') return named[entity.toLowerCase()];
    const code = entity[1].toLowerCase() === 'x' ? parseInt(entity.slice(2), 16) : Number(entity.slice(1));
    return code > 0 && code <= 0x10ffff && !(code >= 0xd800 && code <= 0xdfff) ? String.fromCodePoint(code) : whole;
  });
}

function summarizeBootstrapResolverEnvelope(source, onBody = () => {}, index = {entries: new Map(), bounded: false}, onResolved = () => {}) {
  const length = typeof source?.body === 'string' ? Buffer.byteLength(source.body) : null;
  let decoded;
  const initial = summarizeBootstrapBodyEnvelope(source, onBody, body => { decoded = body; });
  initial.collection = resolverCollection(initial.collection);
  initial.envelope.body_length = length !== null && length <= 8_000_000 ? length : null;
  if (initial.envelope.body_encoding !== 'invalid' || typeof source?.body !== 'string' ||
      initial.envelope.target_binding === 'mismatched' || initial.envelope.method === 'other' || hasSemanticErrors(source)) {
    if (decoded) onResolved(decoded, initial);
    return initial;
  }
  const reject = encoding => {
    initial.envelope.body_encoding = encoding;
    if (encoding === 'bounded') initial.structure.status = 'bounded';
    return initial;
  };
  if (!/^[A-Za-z_][A-Za-z0-9_:.-]{0,999}$/.test(source.body)) return reject('reference_unsupported');
  // A truncated index cannot establish uniqueness or absence.
  if (index.bounded) return reject('bounded');
  const entry = index.entries.get(source.body);
  if (!entry) return reject('reference_missing');
  if (entry.ambiguous) return reject('reference_ambiguous');
  if (!entry.inert) return reject('reference_unsupported');
  let json = entry.content.trim();
  if (json.startsWith('<!--') && json.endsWith('-->')) json = json.slice(4, -3).trim();
  let encoding = 'reference_json';
  try { JSON.parse(json); }
  catch {
    json = decodeInertEntities(json);
    encoding = 'reference_entity_json';
    try { JSON.parse(json); } catch { return reject('reference_unsupported'); }
  }
  // Reuse the one-layer decoder and security inspection; never resolve another ID.
  const result = summarizeBootstrapBodyEnvelope({...source, body: json}, onBody, body => { decoded = body; });
  result.collection = resolverCollection(result.collection);
  result.envelope.body_length = initial.envelope.body_length;
  if (result.envelope.body_encoding === 'json_string') result.envelope.body_encoding = encoding;
  else if (result.envelope.body_encoding !== 'bounded') result.envelope.body_encoding = 'reference_unsupported';
  if (decoded) onResolved(decoded, result);
  return result;
}

function summarizeBootstrapCandidate(body) {
  const result = status => ({ status, recipe, paging: null, root_ids: [] });
  if (!record(body)) return result('drifted');
  if (hasSemanticErrors(body)) return result('invalid');
  if (!own(body, 'data')) return result('absent');
  if (!record(body.data)) return result('drifted');
  if (!own(body.data, 'data')) return result('absent');
  if (!record(body.data.data)) return result('drifted');
  if (!own(body.data.data, recipe)) return result('absent');
  const collection = body.data.data[recipe];
  if (!record(collection) || collection.$type !== collectionType) return result('drifted');
  const roots = collection['*elements'];
  const paging = collection.paging;
  if (!Array.isArray(roots) || roots.length > 100 || !Array.from(roots).every(nativeRoot) ||
      new Set(roots).size !== roots.length || !record(paging) ||
      !['start', 'count', 'total'].every(key => own(paging, key) && pagingInteger(paging[key]))) {
    return result('invalid');
  }
  // Source-reported shape only. Neither total nor empty roots establishes an end.
  return {
    status: paging.start === 0 ? 'observed_start_zero' : 'observed_nonzero',
    recipe,
    paging: { start: paging.start, count: paging.count, total: paging.total },
    root_ids: roots.slice(),
  };
}

function resolverCollection(collection) {
  return collection.root_ids.some(value => credentialLiteral.test(value) || sensitiveStructure.test(value)) ?
    summarizeBootstrapCandidate({error: {code: 'PRIVATE_REFERENCE'}}) : collection;
}

function summarizeBootstrapResolverCandidate(body) {
  return resolverCollection(summarizeBootstrapCandidate(body));
}

function summarizeBootstrapObservations(candidates, bounded = false) {
  return { status: bounded ? 'bounded' :
    candidates.some(candidate => candidate.collection?.status?.startsWith('observed_')) ? 'observed' :
    candidates.some(candidate => candidate.structure?.status === 'bounded') ? 'bounded' :
    candidates.some(candidate => candidate.structure?.status === 'observed' || candidate.status?.startsWith('observed_')) ? 'observed' :
    candidates.some(candidate => ['invalid', 'drifted'].includes(candidate.structure?.status || candidate.status)) ? 'invalid' : 'absent',
  candidates };
}

function inspectEmbeddedBootstrap(html, onBody = () => {}, structural = false, onResolved = () => {}) {
  const candidates = [];
  let unsupportedEncoding = false;
  if (typeof html !== 'string' || Buffer.byteLength(html) > 8_000_000)
    return summarizeBootstrapObservations(candidates, true);
  const referenceIndex = structural === 'resolver' ? buildBootstrapReferenceIndex(html) : null;
  if (referenceIndex?.bounded) return summarizeBootstrapObservations(candidates, true);
  // Only inert JSON text is parsed. Never evaluate hydration scripts or infer roots.
  const tags = /<(script|code)\b([^>]*)>([\s\S]*?)<\/\1\s*>/gi;
  for (const match of html.matchAll(tags)) {
    const [, tag, attributes, content] = match;
    if (tag.toLowerCase() === 'script') {
      const type = attributes.match(/\btype\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/i);
      if (!type || !['application/json', 'application/ld+json']
        .includes((type[1] || type[2] || type[3]).toLowerCase().trim())) {
        if (structural && content.trim()) unsupportedEncoding = true;
        continue;
      }
    }
    let json = content.trim();
    if (json.startsWith('<!--') && json.endsWith('-->')) json = json.slice(4, -3).trim();
    if (!/^[{\[]/.test(json)) {
      if (structural && json) unsupportedEncoding = true;
      continue;
    }
    if (candidates.length >= 100) return summarizeBootstrapObservations(candidates, true);
    let parsed;
    try { parsed = JSON.parse(json); }
    catch { if (structural) unsupportedEncoding = true; continue; }
    if (!['body', 'resolver'].includes(structural)) onBody(parsed);
    candidates.push({ source: 'initial_document_json',
      body_sha256: crypto.createHash('sha256').update(json).digest('hex'),
      ...(['body', 'resolver'].includes(structural) ? {source: 'initial_document_body_json',
        outer_body_sha256: crypto.createHash('sha256').update(json).digest('hex'),
        ...(structural === 'resolver' ? summarizeBootstrapResolverEnvelope(parsed, onBody, referenceIndex,
          (body, result) => onResolved({source: parsed, body, result,
            wrapper_sha256: crypto.createHash('sha256').update(json).digest('hex')})) :
          summarizeBootstrapBodyEnvelope(parsed, onBody))} :
        structural ? {structure: summarizeBootstrapStructure(parsed)} : summarizeBootstrapCandidate(parsed)) });
  }
  return summarizeBootstrapObservations(candidates, unsupportedEncoding);
}

module.exports = { summarizeBootstrapCandidate, summarizeBootstrapStructure, summarizeBootstrapBodyEnvelope,
  summarizeBootstrapResolverEnvelope, summarizeBootstrapResolverCandidate, buildBootstrapReferenceIndex,
  bootstrapRequestMetadata, summarizeBootstrapObservations, inspectEmbeddedBootstrap };
