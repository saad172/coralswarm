// Strict Server-Timing evidence. A missing/invalid metric is never a 0ms pass.
const TOKEN = /^[!#$%&'*+.^_`|~0-9a-z-]+$/i;
function splitQuoted(text, delimiter) {
  const parts = []; let current = ''; let quoted = false; let escaped = false;
  for (const char of text) {
    if (escaped) { current += char; escaped = false; continue; }
    if (char === '\\' && quoted) { current += char; escaped = true; continue; }
    if (char === '"') quoted = !quoted;
    if (char === delimiter && !quoted) { parts.push(current.trim()); current = ''; }
    else current += char;
  }
  if (quoted || escaped) throw new Error('invalid Server-Timing quoted string');
  parts.push(current.trim()); return parts;
}
function parameterValue(value) {
  if (value.startsWith('"') && value.endsWith('"')) return value.slice(1, -1).replace(/\\(.)/g, '$1');
  if (!TOKEN.test(value)) throw new Error('invalid Server-Timing parameter');
  return value;
}
export function parsePageReadTiming(header, expected = true) {
  if (typeof header !== 'string' || !header.trim()) {
    if (expected) throw new Error('missing Server-Timing on expected page_read response');
    return null;
  }
  const metrics = new Map();
  for (const metric of splitQuoted(header, ',')) {
    const [name, ...parameters] = splitQuoted(metric, ';');
    if (!TOKEN.test(name) || metrics.has(name.toLowerCase())) throw new Error('invalid/duplicate Server-Timing metric');
    const params = new Map();
    for (const parameter of parameters) {
      const equal = parameter.indexOf('=');
      const key = (equal < 0 ? parameter : parameter.slice(0, equal)).trim().toLowerCase();
      if (!TOKEN.test(key) || params.has(key)) throw new Error('invalid/duplicate Server-Timing parameter');
      params.set(key, equal < 0 ? null : parameterValue(parameter.slice(equal + 1).trim()));
    }
    metrics.set(name.toLowerCase(), params);
  }
  if (metrics.get('class')?.get('desc') !== 'page_read') {
    if (expected) throw new Error('expected class;desc="page_read" in Server-Timing');
    return null;
  }
  const duration = metrics.get('total')?.get('dur');
  if (typeof duration !== 'string' || !/^(?:\d+(?:\.\d*)?|\.\d+)$/.test(duration) || !Number.isFinite(Number(duration))) {
    throw new Error('page_read Server-Timing requires a valid nonnegative total;dur');
  }
  return Number(duration);
}
export function summarize(samples, min = 20) {
  if (samples.length < min || samples.some((v) => !Number.isFinite(v) || v < 0)) throw new Error(`insufficient/invalid timing samples: ${samples.length}, required ${min}`);
  const sorted = [...samples].sort((a, b) => a - b);
  return { samples: [...samples], count: samples.length, p50_ms: sorted[Math.ceil(sorted.length * .5) - 1], p95_ms: sorted[Math.ceil(sorted.length * .95) - 1], max_ms: sorted.at(-1) };
}
export function assertSpeed(samples) {
  const stats = summarize(samples);
  if (stats.p95_ms >= 500) throw new Error(`page_read p95 ${stats.p95_ms}ms must be <500ms`);
  return stats;
}
