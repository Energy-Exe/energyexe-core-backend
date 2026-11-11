// CloudFront Function (viewer request) for the client portal's /ingest/* paths.
// The portal sends PostHog analytics to its own address (/ingest/...), so ad
// blockers that block PostHog's domain don't drop it. This strips the /ingest
// prefix before CloudFront forwards the request to PostHog's EU servers.
function handler(event) {
  var request = event.request;
  request.uri = request.uri.replace(/^\/ingest/, '') || '/';
  return request;
}
