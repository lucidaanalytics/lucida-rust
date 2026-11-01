//! Spec v2 operators (`testdata/flags/README.md`, "Spec v2"); port of `go/internal/flageval/ops_v2.go`.

use std::cmp::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::eval::{cmp_semver, semver, to_f64};

/// Fixed unit lengths so every evaluator agrees: m = 30 days, y = 365 days.
fn relative_seconds(s: &str) -> Option<f64> {
    let s = s.trim().strip_prefix('-').unwrap_or_else(|| s.trim());
    let unit = match s.chars().last()? {
        'h' => 3600.0,
        'd' => 86_400.0,
        'w' => 7.0 * 86_400.0,
        'm' => 30.0 * 86_400.0,
        'y' => 365.0 * 86_400.0,
        _ => return None,
    };
    let n = &s[..s.len() - 1];
    (!n.is_empty() && n.len() <= 6 && n.bytes().all(|b| b.is_ascii_digit()))
        .then(|| n.parse::<f64>().ok().map(|n| n * unit))?
}

fn digits(s: &str, n: usize) -> Option<i64> {
    (s.len() == n && s.bytes().all(|b| b.is_ascii_digit())).then(|| s.parse().ok())?
}

/// Days since 1970-01-01 of a valid civil date (H. Hinnant).
fn days_from_civil(y: i64, m: i64, d: i64) -> Option<i64> {
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=12).contains(&m) || d < 1 || d > dim[usize::try_from(m - 1).ok()?] {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe - 719_468)
}

fn parse_days(s: &str) -> Option<i64> {
    let (y, rest) = s.split_once('-')?;
    let (m, d) = rest.split_once('-')?;
    days_from_civil(digits(y, 4)?, digits(m, 2)?, digits(d, 2)?)
}

/// `YYYY-MM-DD` as unix seconds at UTC midnight.
#[allow(clippy::cast_precision_loss)]
fn parse_date(s: &str) -> Option<f64> {
    Some((parse_days(s)? * 86_400) as f64)
}

/// RFC 3339 (`Z` or `+hh:mm` offset, optional fraction) as unix seconds.
fn parse_rfc3339(s: &str) -> Option<f64> {
    let (date, time) = s.split_once('T')?;
    let (tm, offset) = if let Some(t) = time.strip_suffix('Z') {
        (t, 0)
    } else {
        let i = time.rfind(['+', '-'])?;
        let (t, o) = time.split_at(i);
        let sign = if o.starts_with('-') { -1 } else { 1 };
        let (oh, om) = o[1..].split_once(':')?;
        (t, sign * (digits(oh, 2)? * 3600 + digits(om, 2)? * 60))
    };
    let (hms, frac) = tm.split_once('.').map_or((tm, ""), |(a, b)| (a, b));
    if (tm.contains('.') && (frac.is_empty() || !frac.bytes().all(|b| b.is_ascii_digit())))
        || hms.len() != 8
    {
        return None;
    }
    let (h, m, sec) = (
        digits(&hms[..2], 2)?,
        digits(&hms[3..5], 2)?,
        digits(&hms[6..], 2)?,
    );
    if &hms[2..3] != ":" || &hms[5..6] != ":" || h > 23 || m > 59 || sec > 59 {
        return None;
    }
    let fraction = if frac.is_empty() {
        0.0
    } else {
        format!("0.{frac}").parse().ok()?
    };
    #[allow(clippy::cast_precision_loss)]
    Some((parse_days(date)? * 86_400 + h * 3600 + m * 60 + sec - offset) as f64 + fraction)
}

fn wall_clock() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

/// The evaluation clock in unix seconds: `ctx.now` (RFC 3339) when it parses, else the wall clock.
pub(crate) fn parse_now(ctx: &Value) -> f64 {
    ctx.get("now")
        .and_then(Value::as_str)
        .and_then(parse_rfc3339)
        .unwrap_or_else(wall_clock)
}

fn instant(v: &Value, now: f64, rel: bool) -> Option<f64> {
    if let Value::Number(n) = v {
        return n.as_f64();
    }
    let s = v.as_str()?.trim();
    if rel && let Some(secs) = relative_seconds(s) {
        return Some(now - secs);
    }
    parse_rfc3339(s).or_else(|| parse_date(s))
}

fn cmp_parts(a: &[u64], b: &[u64]) -> Ordering {
    (0..a.len().max(b.len()))
        .map(|i| a.get(i).unwrap_or(&0).cmp(b.get(i).unwrap_or(&0)))
        .find(|o| o.is_ne())
        .unwrap_or(Ordering::Equal)
}

/// Half-open `[lo, hi)` of a tilde, caret or wildcard value.
fn semver_range(op: &str, want: &Value) -> Option<(Vec<u64>, Vec<u64>)> {
    let s = want.as_str()?.trim();
    let parts: Vec<&str> = s.strip_prefix('v').unwrap_or(s).split('.').collect();
    if op == "semver_wildcard" {
        if !matches!(*parts.last()?, "*" | "x" | "X") {
            return None;
        }
        if parts.len() == 1 {
            return Some((vec![0], vec![1 << 30]));
        }
        let lo = semver(&Value::from(parts[..parts.len() - 1].join(".")))?;
        let mut hi = lo.clone();
        *hi.last_mut()? += 1;
        return Some((lo, hi));
    }
    let mut p = semver(want)?;
    p.resize(p.len().max(3), 0);
    let hi = if op == "semver_tilde" && parts.len() == 1 {
        vec![p[0] + 1, 0, 0]
    } else if op == "semver_tilde" || (p[0] == 0 && p[1] > 0) {
        vec![p[0], p[1] + 1, 0]
    } else if p[0] > 0 {
        vec![p[0] + 1, 0, 0]
    } else {
        vec![0, 0, p[2] + 1]
    };
    Some((p, hi))
}

/// Apply a v2 operator to a set property. Unknown operators never match.
pub(crate) fn match_v2(op: &str, prop: &Value, want: &Value, now: f64) -> bool {
    match op {
        "icontains" | "not_icontains" | "starts_with" | "not_starts_with" | "ends_with"
        | "not_ends_with" => {
            let (Some(s), Some(w)) = (prop.as_str(), want.as_str()) else {
                return false;
            };
            if op.ends_with("icontains") {
                s.to_lowercase().contains(&w.to_lowercase()) == (op == "icontains")
            } else if op.ends_with("starts_with") {
                s.starts_with(w) == (op == "starts_with")
            } else {
                s.ends_with(w) == (op == "ends_with")
            }
        }
        "between" | "not_between" => {
            let (Some(x), Some([lo, hi])) = (to_f64(prop), want.as_array().map(Vec::as_slice))
            else {
                return false;
            };
            let (Some(lo), Some(hi)) = (
                lo.as_f64().filter(|_| lo.is_number()),
                hi.as_f64().filter(|_| hi.is_number()),
            ) else {
                return false;
            };
            (x >= lo && x <= hi) == (op == "between")
        }
        "is_date_before" | "is_date_after" => {
            match (instant(prop, now, false), instant(want, now, true)) {
                (Some(p), Some(w)) if op == "is_date_before" => p < w,
                (Some(p), Some(w)) => p > w,
                _ => false,
            }
        }
        "semver_eq" | "semver_neq" | "semver_gt" | "semver_gte" | "semver_lt" | "semver_lte" => {
            let Some(c) = cmp_semver(prop, want) else {
                return false;
            };
            match op {
                "semver_eq" => c == Ordering::Equal,
                "semver_neq" => c != Ordering::Equal,
                "semver_gt" => c == Ordering::Greater,
                "semver_gte" => c != Ordering::Less,
                "semver_lt" => c == Ordering::Less,
                _ => c != Ordering::Greater,
            }
        }
        "semver_tilde" | "semver_caret" | "semver_wildcard" => {
            match (semver(prop), semver_range(op, want)) {
                (Some(v), Some((lo, hi))) => {
                    cmp_parts(&v, &lo) != Ordering::Less && cmp_parts(&v, &hi) == Ordering::Less
                }
                _ => false,
            }
        }
        _ => false,
    }
}
