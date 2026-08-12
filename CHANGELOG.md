# Changelog

## Unreleased

- Prepared a sanitized source snapshot for public review.
- Retained the research-only pipeline, official TWSE provider contracts, synthetic mock provider, generic scheduler components, offline tests, and minimal fixtures.
- Removed private planning context, deployment activation material, runtime artifacts, production evidence, and machine-specific benchmark result snapshots.
- Kept live network smoke checks separate from the offline regression suite.
- Hardened authenticated E.SUN requests to reject redirects and unverified
  effective URLs before accepting a response.
- Added explicit response-body limits and bounded per-attempt and cumulative
  retry delays for official market-data transports.

No production deployment or scheduler activation is claimed by this snapshot.
