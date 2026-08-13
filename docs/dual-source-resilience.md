# v1.1 Dual-Source Resilience

The v1.1 contract extends the read-only research pipeline without changing
legacy `twse_baseline` semantics.

## Authority and roles

- TWSE remains the canonical authority for selected research observations.
- E.SUN can provide supplemental observations only after the explicit
  eligibility and identity contract passes.
- E.SUN observations can also remain validation evidence; they are never
  relabeled as canonical.
- A provider identity mismatch, malformed payload, or credential-bearing
  artifact reference is rejected fail closed.

## Dataset lifecycle

1. A complete TWSE history creates a `canonical_complete` dataset.
2. A qualified temporary TWSE gap can create an immutable `provisional_mixed`
   dataset with per-observation provenance.
3. Later TWSE observations create a new immutable reconciled child. The
   provisional parent and its report identity are not updated in place.
4. Equal and discrepant reconciliation outcomes remain explicit, and replay
   of the same dataset identity is zero-write.

Required coverage is derived from the frozen legal-short listing-history
contract and authoritative listing evidence when the normal history window is
not legally available. A normal candidate remains governed by the standard
window; no symbol-specific hard-coded count is introduced.

## Storage and compatibility

Migration `0012_dual_source_dataset_versions.sql` is additive schema v12
support for dataset versions, observations, artifacts, and DS5 metadata. It
does not rewrite legacy v1 rows, legacy S4 identities, or legacy reports.

The v1 path continues to use `twse_baseline`, the frozen universe and ranking
methodology, and the existing success/partial-success and strict-replay
contracts. Provisional status is carried only by the v1.1 dual-source path.

All examples and regression fixtures are local and deterministic. The project
does not include production databases, market-data dumps, credentials,
activation evidence, scheduler state, or runtime reports.
