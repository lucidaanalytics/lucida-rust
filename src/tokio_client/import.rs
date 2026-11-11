//! Backfill import for [`AsyncClient`](super::AsyncClient): `POST /v1/s2s/batch` with the secret key.

use serde_json::Value;

use super::AsyncClient;
use crate::core::{Error, Event, MAX_BATCH, batch_payload, wire};
use crate::hooks::build_wire;

impl AsyncClient {
    /// Historical import via `POST /v1/s2s/batch` (secret key, timestamps kept), chunks of 1000.
    /// Super properties, `disable_geoip` and `before_send` apply (an event the hook drops is not sent);
    /// [`import_events_raw`](Self::import_events_raw) sends the events as given.
    ///
    /// # Errors
    /// `NoSecretKey`, or `Status` at the first rejected chunk.
    pub async fn import_events(&self, events: &[Event]) -> Result<usize, Error> {
        self.import(events, false).await
    }

    /// [`import_events`](Self::import_events) without super properties or `before_send`.
    ///
    /// # Errors
    /// `NoSecretKey`, or `Status` at the first rejected chunk.
    pub async fn import_events_raw(&self, events: &[Event]) -> Result<usize, Error> {
        self.import(events, true).await
    }

    async fn import(&self, events: &[Event], raw: bool) -> Result<usize, Error> {
        let i = &self.inner;
        let secret = i.opts.secret_key.as_ref().ok_or(Error::NoSecretKey)?;
        if i.opts.disabled {
            return Ok(0);
        }
        let wires: Vec<Value> = if raw {
            events.iter().map(wire).collect()
        } else {
            events
                .iter()
                .filter_map(|e| build_wire(&i.opts, &i.shared, e))
                .collect()
        };
        let mut sent = 0;
        for chunk in wires.chunks(MAX_BATCH) {
            let payload = batch_payload(None, chunk.to_vec());
            let (status, _, _) = i
                .request(
                    &format!("{}/v1/s2s/batch", i.opts.host),
                    &payload,
                    Some(secret),
                    None,
                )
                .await;
            if !(200..300).contains(&status) {
                return Err(Error::Status(status));
            }
            sent += chunk.len();
        }
        Ok(sent)
    }
}
