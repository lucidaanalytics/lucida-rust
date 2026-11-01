//! Local flag evaluator; behaviour pinned by `testdata/flags` vectors (see `testdata/flags/README.md`).
//! Flags and contexts are the JSON wire format, evaluated in place.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use regex::Regex;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::eval_v2::{match_v2, parse_now};

/// Reason of a flag that only the server can decide (cohort filters).
pub const REQUIRES_SERVER: &str = "requires_server";
const UNIT: f64 = 4_503_599_627_370_496.0; // 16^13

/// `false`, `true` or a variant key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlagValue {
    Bool(bool),
    Variant(String),
}

impl FlagValue {
    /// True for `true` and for any variant.
    #[must_use]
    pub fn is_on(&self) -> bool {
        !matches!(self, Self::Bool(false))
    }

    #[must_use]
    pub fn to_json(&self) -> Value {
        match self {
            Self::Bool(b) => Value::Bool(*b),
            Self::Variant(s) => Value::String(s.clone()),
        }
    }
}

/// Outcome of evaluating one flag.
#[derive(Debug, Clone, PartialEq)]
pub struct FlagResult {
    pub value: FlagValue,
    pub reason: &'static str,
    /// -1 when no condition decided the result.
    pub condition_index: i64,
    pub variant: Option<String>,
    pub payload: Option<Value>,
}

/// Hash of `text` in `[0, 1)`: the first 13 hex digits of its SHA-256 over 16^13.
#[must_use]
#[allow(clippy::cast_precision_loss)] // 52-bit integer, exact in f64
pub fn hash01(text: &str) -> f64 {
    let d = Sha256::digest(text.as_bytes());
    let mut n = 0u64;
    for b in &d[..7] {
        n = (n << 8) | u64::from(*b);
    }
    (n >> 4) as f64 / UNIT
}

fn off(reason: &'static str, condition_index: i64) -> FlagResult {
    FlagResult {
        value: FlagValue::Bool(false),
        reason,
        condition_index,
        variant: None,
        payload: None,
    }
}

pub(crate) fn to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) if !s.contains('_') => s.trim().parse().ok(),
        _ => None,
    }
}

fn equal(a: &Value, b: &Value) -> bool {
    if a.is_number() || b.is_number() {
        return matches!((a.as_f64(), b.as_f64()), (Some(x), Some(y)) if x == y);
    }
    a == b
}

pub(crate) fn semver(v: &Value) -> Option<Vec<u64>> {
    let s = v.as_str()?.trim();
    let s = s.strip_prefix('v').unwrap_or(s);
    let s = s.find(['-', '+']).map_or(s, |i| &s[..i]);
    s.split('.')
        .map(|p| {
            if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) {
                p.parse().ok()
            } else {
                None
            }
        })
        .collect()
}

pub(crate) fn cmp_semver(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    let (mut pa, mut pb) = (semver(a)?, semver(b)?);
    let n = pa.len().max(pb.len());
    pa.resize(n, 0);
    pb.resize(n, 0);
    Some(pa.cmp(&pb))
}

fn regex_match(pattern: &str, s: &str) -> Option<bool> {
    static CACHE: OnceLock<Mutex<HashMap<String, Option<Regex>>>> = OnceLock::new();
    let mut cache = CACHE
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let re = cache
        .entry(pattern.to_owned())
        .or_insert_with(|| Regex::new(pattern).ok());
    re.as_ref().map(|r| r.is_match(s))
}

/// Apply `op` to a property. Missing/null properties match only `is_not_set`.
fn match_op(op: &str, prop: Option<&Value>, want: &Value, semver_mode: bool, now: f64) -> bool {
    use std::cmp::Ordering::{Equal, Greater, Less};
    let prop = prop.filter(|v| !v.is_null());
    match op {
        "is_set" => return prop.is_some(),
        "is_not_set" => return prop.is_none(),
        _ => {}
    }
    let Some(prop) = prop else { return false };
    match op {
        "eq" | "neq" => {
            let eq = if semver_mode {
                match cmp_semver(prop, want) {
                    Some(c) => c == Equal,
                    None => return false,
                }
            } else {
                equal(prop, want)
            };
            eq == (op == "eq")
        }
        "contains" | "ncontains" | "regex" | "nregex" => {
            let (Some(s), Some(w)) = (prop.as_str(), want.as_str()) else {
                return false;
            };
            if op.ends_with("contains") {
                s.contains(w) == (op == "contains")
            } else {
                regex_match(w, s).is_some_and(|m| m == (op == "regex"))
            }
        }
        "gt" | "gte" | "lt" | "lte" => {
            let ord = if semver_mode {
                cmp_semver(prop, want)
            } else {
                to_f64(prop)
                    .zip(to_f64(want))
                    .and_then(|(x, y)| x.partial_cmp(&y))
            };
            matches!(
                (op, ord),
                ("gt", Some(Greater))
                    | ("gte", Some(Greater | Equal))
                    | ("lt", Some(Less))
                    | ("lte", Some(Less | Equal))
            )
        }
        "in" | "not_in" => want
            .as_array()
            .is_some_and(|l| l.iter().any(|w| equal(prop, w)) == (op == "in")),
        _ => match_v2(op, prop, want, now),
    }
}

fn str_of<'a>(v: &'a Value, k: &str) -> &'a str {
    v.get(k).and_then(Value::as_str).unwrap_or("")
}

fn filter_matches(fl: &Value, ctx: &Value, resolved: &HashMap<String, FlagResult>) -> bool {
    let want = fl.get("value").unwrap_or(&Value::Null);
    let op = str_of(fl, "op");
    let key = str_of(fl, "key");
    let now = parse_now(ctx);
    let props: Option<&Value> = match str_of(fl, "type") {
        "app_version" => {
            let v = ctx
                .get("app_version")
                .filter(|v| v.as_str().is_some_and(|s| !s.is_empty()));
            return match_op(op, v, want, true, now);
        }
        "flag" => {
            return resolved
                .get(key)
                .is_some_and(|r| match_op(op, Some(&r.value.to_json()), want, false, now));
        }
        "person" => ctx.get("person_properties"),
        "group" => ctx
            .get("group_properties")
            .and_then(|g| g.get(str_of(fl, "group_type"))),
        "device" => ctx.get("device_properties"),
        "geo" => ctx.get("geo_properties"),
        _ => return false,
    };
    match_op(op, props.and_then(|p| p.get(key)), want, false, now)
}

fn value_matches(got: &FlagValue, want: &Value) -> bool {
    match (want, got) {
        (Value::Bool(b), FlagValue::Bool(g)) => b == g,
        (Value::Bool(b), FlagValue::Variant(_)) => *b, // a variant counts as "on"
        (Value::String(w), FlagValue::Variant(g)) => w == g,
        _ => false,
    }
}

fn bucketing_key(f: &Value, ctx: &Value) -> String {
    unit_key(f.get("aggregation").unwrap_or(&Value::Null), ctx)
}

/// The bucketing unit of an aggregation (a flag's, or one condition's own).
fn unit_key(agg: &Value, ctx: &Value) -> String {
    match str_of(agg, "kind") {
        "group" => {
            let g = ctx
                .get("groups")
                .and_then(|g| g.get(str_of(agg, "group_type")));
            return g.and_then(Value::as_str).unwrap_or("").to_owned();
        }
        "device" => return str_of(ctx, "device_id").to_owned(),
        _ => {}
    }
    let bk = str_of(ctx, "bucketing_key");
    (if bk.is_empty() {
        str_of(ctx, "distinct_id")
    } else {
        bk
    })
    .to_owned()
}

fn list<'a>(v: &'a Value, k: &str) -> &'a [Value] {
    v.get(k)
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

fn pick_variant<'a>(f: &'a Value, cond: &Value, key: &str) -> &'a Value {
    let variants = list(f, "variants");
    let over = str_of(cond, "variant_override");
    if !over.is_empty()
        && let Some(v) = variants.iter().find(|v| str_of(v, "key") == over)
    {
        return v;
    }
    let h = hash01(&format!(
        "{}.{}.variant.{key}",
        str_of(f, "key"),
        str_of(f, "salt")
    )) * 100.0;
    let mut cum = 0.0;
    for v in variants {
        cum += v.get("rollout_pct").and_then(Value::as_f64).unwrap_or(0.0);
        if h < cum {
            return v;
        }
    }
    &variants[variants.len() - 1] // percentages summed < 100: the last variant takes the remainder
}

/// A stored value of a sticky flag decides once a condition's filters passed (LC-EXT-29).
fn sticky(f: &Value, cond: &Value, i: i64, ctx: &Value) -> Option<FlagResult> {
    if f.get("sticky").and_then(Value::as_bool) != Some(true) {
        return None;
    }
    let stored = ctx.get("sticky_assignments")?.get(str_of(f, "key"))?;
    let mut r = FlagResult {
        value: FlagValue::Bool(true),
        reason: "sticky_bucket",
        condition_index: i,
        variant: None,
        payload: None,
    };
    let variants = list(f, "variants");
    if str_of(f, "type") == "multivariate" && !variants.is_empty() {
        let over = str_of(cond, "variant_override");
        if variants.iter().any(|v| str_of(v, "key") == over) {
            return None; // the override wins
        }
        let k = stored.as_str()?;
        let v = variants.iter().find(|v| str_of(v, "key") == k)?;
        (r.value, r.variant) = (FlagValue::Variant(k.to_owned()), Some(k.to_owned()));
        r.payload = v.get("payload").filter(|p| !p.is_null()).cloned();
    } else if stored.as_bool() != Some(true) {
        return None;
    } else if str_of(f, "type") == "json" {
        r.payload = f.get("payload").filter(|p| !p.is_null()).cloned();
    }
    Some(r)
}

/// A flag scoped to app contexts is dropped when the request declares contexts and shares none.
fn app_context_allowed(f: &Value, ctx: &Value) -> bool {
    let (want, have) = (list(f, "app_contexts"), list(ctx, "app_contexts"));
    want.is_empty() || have.is_empty() || want.iter().any(|w| have.contains(w))
}

#[allow(clippy::too_many_lines)]
fn evaluate(f: &Value, ctx: &Value, resolved: &HashMap<String, FlagResult>) -> FlagResult {
    if !app_context_allowed(f, ctx) {
        return off("app_context_excluded", -1);
    }
    if f.get("active").and_then(Value::as_bool) != Some(true) {
        return off("inactive", -1);
    }
    if f.get("kill_switch").and_then(Value::as_bool) == Some(true) {
        return off("kill_switch", -1);
    }
    let mut server = false;
    for p in list(f, "prerequisites") {
        let Some(r) = resolved.get(str_of(p, "flag")) else {
            return off("prerequisite_missing", -1);
        };
        if r.reason == REQUIRES_SERVER {
            server = true; // a definite failure elsewhere still wins
        } else if r.reason == "prerequisite_cycle"
            || !value_matches(&r.value, p.get("value").unwrap_or(&Value::Null))
        {
            return off("prerequisite_failed", -1);
        }
    }
    if server {
        return off(REQUIRES_SERVER, -1);
    }
    let key = bucketing_key(f, ctx);
    let conds = list(f, "conditions");
    let per_condition = conds
        .iter()
        .any(|c| c.get("aggregation").is_some_and(Value::is_object));
    let flag_level = f.get("holdout").is_some_and(Value::is_object)
        || f.get("namespace").is_some_and(Value::is_object);
    if key.is_empty() && (!per_condition || flag_level) {
        return off("no_bucketing_key", -1);
    }
    if let Some(h) = f.get("holdout").filter(|h| h.is_object()) {
        let pct = h.get("percentage").and_then(Value::as_f64).unwrap_or(0.0);
        if hash01(&format!("holdout.{}.{key}", str_of(h, "salt"))) * 100.0 < pct {
            return off("holdout", -1);
        }
    }
    if let Some(ns) = f.get("namespace").filter(|n| n.is_object()) {
        let num = |k: &str| ns.get(k).and_then(Value::as_f64).unwrap_or(0.0);
        if num("total") > 0.0 {
            let b =
                (hash01(&format!("namespace.{}.{key}", str_of(ns, "salt"))) * num("total")).floor();
            if b < num("start") || b >= num("end") {
                return off("namespace_excluded", -1);
            }
        }
    }
    let mut skipped = 0;
    for (i, cond) in conds.iter().enumerate() {
        let i = i64::try_from(i).unwrap_or(i64::MAX);
        let own = cond.get("aggregation").filter(|a| a.is_object());
        let ckey = own.map_or_else(|| key.clone(), |a| unit_key(a, ctx));
        if ckey.is_empty() {
            skipped += 1;
            continue;
        }
        let filters = list(cond, "filters");
        // cohort, or a flag filter on a requires_server flag: undecidable locally
        let is_cohort = |fl: &&Value| match str_of(fl, "type") {
            "cohort" => true,
            "flag" => resolved
                .get(str_of(fl, "key"))
                .is_some_and(|r| r.reason == REQUIRES_SERVER),
            _ => false,
        };
        if !filters
            .iter()
            .filter(|fl| !is_cohort(fl))
            .all(|fl| filter_matches(fl, ctx, resolved))
        {
            continue;
        }
        if filters.iter().any(|fl| is_cohort(&fl)) {
            return off(REQUIRES_SERVER, i);
        }
        // the sticky store keys on the flag-level unit
        if own.is_none()
            && let Some(r) = sticky(f, cond, i, ctx)
        {
            return r;
        }
        let rollout = cond
            .get("rollout_pct")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        if hash01(&format!(
            "{}.{}.{ckey}",
            str_of(f, "key"),
            str_of(f, "salt")
        )) * 100.0
            >= rollout
        {
            if f.get("early_exit").and_then(Value::as_bool) == Some(true) {
                return off("out_of_rollout", i);
            }
            continue;
        }
        let mut r = FlagResult {
            value: FlagValue::Bool(true),
            reason: "condition_match",
            condition_index: i,
            variant: None,
            payload: None,
        };
        if str_of(f, "type") == "multivariate" && !list(f, "variants").is_empty() {
            let v = pick_variant(f, cond, &ckey);
            let k = str_of(v, "key").to_owned();
            (r.value, r.variant) = (FlagValue::Variant(k.clone()), Some(k));
            r.payload = v.get("payload").filter(|p| !p.is_null()).cloned();
        } else if str_of(f, "type") == "json" {
            r.payload = f.get("payload").filter(|p| !p.is_null()).cloned();
        }
        return r;
    }
    off(
        if !conds.is_empty() && skipped == conds.len() {
            "no_bucketing_key"
        } else {
            "no_condition_match"
        },
        -1,
    )
}

fn deps(f: &Value) -> Vec<String> {
    let mut d: Vec<String> = list(f, "prerequisites")
        .iter()
        .map(|p| str_of(p, "flag").to_owned())
        .collect();
    for cond in list(f, "conditions") {
        d.extend(
            list(cond, "filters")
                .iter()
                .filter(|fl| str_of(fl, "type") == "flag")
                .map(|fl| str_of(fl, "key").to_owned()),
        );
    }
    d
}

struct Walk<'a> {
    by_key: HashMap<&'a str, &'a Value>,
    ctx: &'a Value,
    out: HashMap<String, FlagResult>,
    state: HashMap<String, u8>, // 1 = visiting, 2 = done
}

impl Walk<'_> {
    fn visit(&mut self, key: &str) {
        let Some(&f) = self.by_key.get(key) else {
            return;
        };
        if self.state.contains_key(key) {
            return;
        }
        self.state.insert(key.to_owned(), 1);
        let mut cyc = false;
        for dep in deps(f) {
            self.visit(&dep);
            cyc |= self.state.get(&dep) == Some(&1)
                || self
                    .out
                    .get(&dep)
                    .is_some_and(|r| r.reason == "prerequisite_cycle");
        }
        let r = if cyc {
            off("prerequisite_cycle", -1)
        } else {
            evaluate(f, self.ctx, &self.out)
        };
        self.out.insert(key.to_owned(), r);
        self.state.insert(key.to_owned(), 2);
    }
}

/// Evaluate every flag for `ctx`, resolving prerequisites and `flag` filters depth-first.
#[must_use]
pub fn evaluate_all(flags: &[Value], ctx: &Value) -> HashMap<String, FlagResult> {
    let mut w = Walk {
        by_key: flags.iter().map(|f| (str_of(f, "key"), f)).collect(),
        ctx,
        out: HashMap::new(),
        state: HashMap::new(),
    };
    for f in flags {
        w.visit(str_of(f, "key"));
    }
    w.out
}
