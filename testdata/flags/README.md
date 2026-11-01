# Flag conformance vectors

Shared by the Go gateway (`go/internal/flageval`), the TS SDK and any other evaluator. Every
implementation must reproduce `expected` exactly.

- `vectors_basic.json`: ~30 hand-checked vectors; hashes computed independently (Python `hashlib`).
- `vectors.json`: 10,000 generated vectors (`cd go && go run ./internal/flageval/gen`), seeded and
  deterministic. Expected values come from the Go reference evaluator.

## File format (`format: "lucida-flag-vectors/1"`)

```json
{
  "format": "lucida-flag-vectors/1",
  "hash_vectors": [{"input": "f1.s.u1", "numerator": 3614806184395237, "hash": 0.8026482110946003}],
  "flags":   [ Flag, ... ],
  "vectors": [{"name": "...", "flag": "<flag key>", "needs": ["distinct_id", ...], "context": Context, "expected": Result}]
}
```

To run a vector: evaluate **all** `flags` for `context` (prerequisites and `flag` filters resolve
within this set), then compare the entry for `vector.flag` with `expected`.

`needs` (optional, informational) lists the Context fields the flag and its prerequisite closure may
read: `distinct_id`, `groups`, `person_properties`, `group_properties`, `device_properties`,
`geo_properties`, `app_version`, `cohorts` (server-only). A client that lacks one of these should
treat the result as undecidable locally. `go test` checks it against `flageval.Needs`.

### Hash

`numerator = parseInt(sha256_hex(input)[0:13], 16)` (< 2^52); `hash = numerator / 2^52` (16^13). Exact
in JS doubles. `hash_vectors` pin this. Rollout input: `"{flag.key}.{flag.salt}.{bucketing_key}"`;
variant input: `"{flag.key}.{flag.salt}.variant.{bucketing_key}"` (UTF-8 bytes).

### Flag

`key`, `type` (`boolean|multivariate|json`), `active` (false -> `inactive`), `kill_switch` (->
`kill_switch`), `salt`, `aggregation {kind: person|group, group_type}`, `variants [{key, rollout_pct,
payload}]`, `conditions [{filters, rollout_pct, variant_override}]`, `prerequisites [{flag, value}]`,
`payload` (json flags).

Filter: `{type: person|group|cohort|device|geo|app_version|flag, key, op, value, group_type}`.
Ops: `eq neq contains ncontains regex nregex gt gte lt lte is_set is_not_set in not_in`, plus the spec v2 set below.

### Context

`distinct_id`, `bucketing_key` (overrides `distinct_id` for person aggregation), `groups`
(type -> key), `person_properties`, `group_properties` (type -> props), `device_properties`,
`geo_properties`, `app_version`.

### Result (`expected`)

`value` (`false` | `true` | variant key), `variant` (multivariate only), `payload` (variant payload, or
flag payload for json), `reason`, `condition_index` (-1 if no condition decided). Absent `variant` /
`payload` mean none.

### Algorithm

1. `active=false` -> `inactive`; `kill_switch` -> `kill_switch` (value false).
2. Each prerequisite must already evaluate to its `value` (`true` matches any on-value including a
   variant; otherwise equality). If a prerequisite's result is `requires_server`, the dependent is
   `requires_server` too (value false, `condition_index` -1), transitively, unless another prerequisite
   definitely fails (`prerequisite_failed` wins). Missing -> `prerequisite_missing`; failed -> `prerequisite_failed`;
   on or depending on a cycle -> `prerequisite_cycle`.
3. Bucketing key: person -> `bucketing_key` else `distinct_id`; group -> `groups[group_type]`. Empty ->
   `no_bucketing_key`.
4. Conditions in order. A condition matches when all non-cohort filters pass and
   `hash * 100 < rollout_pct`. If all local filters pass but the condition has a cohort filter ->
   result `requires_server` (value false, `condition_index` = that condition). Else next condition.
   None -> `no_condition_match`.
5. Match: boolean -> `true`; json -> `true` + flag `payload`; multivariate -> variant: `variant_override`
   if it names a variant, else the first variant where `variantHash * 100 < cumulative rollout_pct`
   (cumulative sum in float64, left to right); if percentages total < 100 the last variant takes the
   remainder.

### Operator semantics

A missing or null property matches only `is_not_set`; every other op (including `neq`, `ncontains`,
`nregex`, `not_in`) is false. `eq/neq`: numbers compare numerically, number never equals string.
`gt/gte/lt/lte`: both sides coerced to numbers (numeric strings ok), else false. `contains/regex`
need string operands. `regex` is unanchored, RE2-compatible syntax kept to the JS-compatible subset.
`in/not_in`: `value` is an array, membership by `eq`. `app_version` filters compare dotted numeric
versions (leading `v`, `-pre`/`+build` suffix ignored, missing segments = 0); unparsable -> false.
`flag` filters read the resolved result value of flag `key`.

### Device aggregation, namespace, holdout (`vectors_g10.json`)

- `aggregation.kind = "device"` buckets on `Context.device_id` (empty -> `no_bucketing_key`; `distinct_id` is never used).
- A `flag` filter whose referenced flag resolved to `requires_server` is undecidable locally: like a cohort
  filter, the condition yields `requires_server` only when every other filter passed; a failing local filter
  wins (`no_condition_match`).
- `namespace {salt, start, end, total}`: after the bucketing key check, `bucket = floor(hash("namespace", salt, key) * total)`
  (hash input `"namespace.{salt}.{key}"`); outside `[start, end)` -> value false, reason `namespace_excluded`, index -1.
- `holdout {salt, percentage}`: checked before the namespace; `hash("holdout", salt, key) * 100 < percentage`
  (input `"holdout.{salt}.{key}"`) -> value false, reason `holdout`, index -1.

### Multi-kind contexts (`vectors_g18.json`, LC-FLG-18)

One evaluation can carry several contexts, one per kind (`user`, `device`, `org`):
`Context.contexts = {"<kind>": {"key": "...", "properties": {...}}}`.

- Filter `{type: "context", context_kind: user|device|org, key, op, value}` reads the property `key` of that kind's
  context with the usual operator semantics (missing property matches only `is_not_set`). An unknown
  `context_kind` never matches, not even `is_not_set`.
- `aggregation {kind: "context", context_kind}` buckets on that kind's `key` (empty -> `no_bucketing_key`).
- Fallbacks keep single-kind callers working: without `contexts.user`, the user context is
  `bucketing_key` or `distinct_id` with `person_properties`; without `contexts.device`, it is `device_id` with
  `device_properties`. `org` has no fallback. An explicit `contexts.<kind>` always wins over its fallback.
- Stored flags select it with `stickiness: "context:<kind>"`.
- `needs` for such flags includes `contexts` (plus the fallback fields for `user` and `device`).

Expected values in `vectors_g18.json` were derived from this text with an independent hash script, then
checked against the Go evaluator (`go/internal/flageval`); the Rust API validates the same definitions
(`lc-experiment` `flag::tests::shared_g18_vectors_use_valid_definitions`).

### Sticky bucketing (`vectors_sticky.json`, LC-EXT-29)

A flag with `sticky: true` keeps a unit's stored assignment when its allocation (rollout percentages,
variant weights) changes. The caller passes the stored values in `Context.sticky_assignments`
(`{"<flag key>": true | "<variant key>"}`), keyed by the flag's bucketing key in its store.

- Steps 1-3, holdout and namespace run unchanged (inactive, kill switch, prerequisites, no bucketing key,
  holdout and namespace exclusions all beat a stored value).
- In the condition loop, once a condition's filters pass (and it is not `requires_server`), a usable stored
  value decides: value = the stored value, reason `sticky_bucket`, `condition_index` = that condition, the
  rollout hash is not checked. Multivariate: the stored value must name a variant (its `payload` is served)
  and the condition must not carry a `variant_override` naming a variant (the override wins). Boolean and
  json flags: the stored value must be `true` (json serves the flag `payload`). Anything else (false, an
  unknown variant, a wrong type) is ignored and the condition is evaluated normally.
- Flags without `sticky` ignore `sticky_assignments`.
- Persisting: after a `condition_match` with a value other than `false` on a sticky flag, store the value
  for (flag key, bucketing key) unless one is already stored. `sticky_bucket` results are already stored.

Expected values were derived with an independent Python script from this text, then checked against the Go
evaluator. Encrypted remote config (LC-FLG-26) adds `encrypted_payload` (a sealed string) to json flags;
evaluators ignore it.

### Spec v2 (`vectors_ops.json`)

New fields: `Flag.early_exit`, `Flag.app_contexts`, `Condition.aggregation`, `Context.app_contexts`,
`Context.now`. New reasons: `out_of_rollout`, `app_context_excluded`. The operator list is shared with the
query compiler (`crates/lc-query` `Op`), so a filter means the same thing in a flag and in a chart.

- **App contexts.** Checked first, before `active`: a flag with non-empty `app_contexts` whose list shares
  no entry with a non-empty `Context.app_contexts` -> value false, reason `app_context_excluded`, index -1.
  A request that declares no app contexts is not filtered. Servers and SDKs drop such flags from their
  output instead of reporting them as off.
- **Per-condition aggregation.** `condition.aggregation` (same shape as the flag's) replaces the flag's unit
  for that condition's rollout hash and variant hash. A condition whose unit is absent (empty key) is
  skipped. The flag-level `no_bucketing_key` check in step 3 applies only when no condition has its own
  aggregation, or when the flag has a `holdout` or `namespace` (they hash the flag-level unit). If every
  condition was skipped for a missing unit the result is `no_bucketing_key`, else `no_condition_match`.
  Sticky assignments apply only to conditions without their own aggregation.
- **Early exit.** With `early_exit: true`, the first condition whose filters pass decides: a rollout miss
  there returns value false, reason `out_of_rollout`, `condition_index` = that condition, instead of trying
  the next condition. Conditions whose filters fail still fall through.
- **Operators** (on a set property; missing/null still matches only `is_not_set`):
  - `icontains` / `not_icontains`: case-insensitive substring (lowercase both sides); string operands.
  - `starts_with` / `not_starts_with`, `ends_with` / `not_ends_with`: case-sensitive; string operands.
  - `between` / `not_between`: `value` is `[lo, hi]` (two numbers, inclusive); the property is coerced like
    `gt`. A wrong shape or non-numeric property is false for both.
  - `is_date_before` / `is_date_after`: strict `<` / `>` on instants. A property or value may be a number
    (unix seconds), an RFC 3339 timestamp (offsets and fractions allowed) or `YYYY-MM-DD` (UTC midnight).
    The value may also be relative, `-?N` + unit `h|d|w|m|y` (`-7d`, `3h`): `now` minus N units, with fixed
    lengths h = 3600 s, d = 86400 s, w = 7 d, m = 30 d, y = 365 d. `now` is `Context.now` (RFC 3339) when set,
    else the wall clock. Anything unparsable is false.
  - `semver_eq|neq|gt|gte|lt|lte`: the `app_version` comparison (leading `v`, `-pre`/`+build` dropped, missing
    segments 0, leading zeros allowed, so `3.10` = `3.10.0` = `v3.010`) on any string property.
  - `semver_tilde` (`1.2.3` -> `[1.2.3, 1.3.0)`, single segment `1` -> `[1.0.0, 2.0.0)`), `semver_caret`
    (`1.2.3` -> `[1.2.3, 2.0.0)`, `0.2.3` -> `[0.2.3, 0.3.0)`, `0.0.3` -> `[0.0.3, 0.0.4)`), `semver_wildcard`
    (last segment `*`, `x` or `X`: `1.2.*` -> `[1.2, 1.3)`, `*` -> any parsable version; no wildcard -> false).
    The value carries no `~`/`^` prefix; the operator says which range.

Expected values were derived from this text with an independent Python script (hashlib for the three
hash-dependent vectors, pinned in `hash_vectors`), then checked against the Go evaluator.
