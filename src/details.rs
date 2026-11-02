//! Public flag details (value, payload, variant, reason, ...) and the `fallbacks` option.

use std::collections::HashMap;

use serde_json::Value;

use crate::core::Resolved;
use crate::eval::FlagValue;

/// Reason of a value served from [`crate::Options::fallbacks`].
pub const FALLBACK_REASON: &str = "fallback";

/// Full result of one flag read: [`crate::Client::get_feature_flag_details`].
#[derive(Debug, Clone, PartialEq)]
pub struct FlagDetails {
    pub key: String,
    pub value: FlagValue,
    pub payload: Option<Value>,
    /// The variant key of a multivariate result.
    pub variant: Option<String>,
    /// Raw Lucida reason (gateway `reason` or the local evaluator's), `fallback` for a fallback value.
    pub reason: String,
    /// -1 when no condition decided the result.
    pub condition_index: i64,
    pub request_id: Option<String>,
    pub locally_evaluated: bool,
}

impl FlagDetails {
    pub(crate) fn new(key: &str, r: Resolved) -> Self {
        Self {
            key: key.to_owned(),
            value: r.value,
            payload: r.payload,
            variant: r.variant,
            reason: r.reason,
            condition_index: r.condition_index,
            request_id: r.request_id,
            locally_evaluated: r.locally,
        }
    }
}

pub(crate) fn fallback(fallbacks: &HashMap<String, FlagValue>, key: &str) -> Option<Resolved> {
    fallbacks.get(key).map(|v| Resolved {
        value: v.clone(),
        payload: None,
        locally: false,
        request_id: None,
        reason: FALLBACK_REASON.to_owned(),
        condition_index: -1,
        variant: None,
    })
}
