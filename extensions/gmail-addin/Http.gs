/**
 * Thin `UrlFetchApp` wrapper — every backend call this add-on makes goes
 * through here, so "non-2xx", "network failure" and "malformed JSON body"
 * are each handled exactly once instead of at every call site.
 *
 * `muteHttpExceptions: true` is load-bearing: without it, `UrlFetchApp.fetch`
 * THROWS on a non-2xx response instead of returning it, which would turn
 * every expected 401/403/404 from the backend into an uncaught Apps
 * Script exception instead of a status this add-on can render a Card for.
 */
function callBackend_(method, url, payload, bearerToken) {
  var options = {
    method: method,
    contentType: 'application/json',
    muteHttpExceptions: true,
  };
  if (payload !== undefined && payload !== null) {
    options.payload = JSON.stringify(payload);
  }
  if (bearerToken) {
    options.headers = { Authorization: 'Bearer ' + bearerToken };
  }

  var response;
  try {
    response = UrlFetchApp.fetch(url, options);
  } catch (err) {
    // A genuine network failure (DNS, timeout, TLS) — distinct from a non-2xx
    // response, which `muteHttpExceptions` already turns into an ordinary
    // return above.
    return { status: 0, body: null, ok: false, networkError: String(err) };
  }

  var code = response.getResponseCode();
  var text = response.getContentText();
  var body = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch (parseErr) {
    body = null;
  }
  return { status: code, body: body, ok: code >= 200 && code < 300 };
}
