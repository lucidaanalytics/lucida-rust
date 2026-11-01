//! Minimal reader for `GET /v1/flags/stream`: it only needs to know when a `definitions` event ends.

/// Feed raw body bytes; reports whether at least one `definitions` event completed.
#[derive(Default)]
pub(crate) struct SseEvents {
    line: Vec<u8>,
    event: String,
}

impl SseEvents {
    pub fn push(&mut self, bytes: &[u8]) -> bool {
        let mut fired = false;
        for &b in bytes {
            if b != b'\n' {
                // keep the buffer bounded against a server that never sends a newline
                if self.line.len() < 1 << 20 {
                    self.line.push(b);
                }
                continue;
            }
            let raw = std::mem::take(&mut self.line);
            let line = String::from_utf8_lossy(&raw);
            let line = line.trim_end_matches('\r');
            if line.is_empty() {
                fired |= self.event == "definitions";
                self.event.clear();
            } else if let Some(v) = line.strip_prefix("event:") {
                v.trim().clone_into(&mut self.event);
            }
        }
        fired
    }
}

#[cfg(test)]
mod tests {
    use super::SseEvents;

    #[test]
    fn fires_on_definitions_across_chunks_and_ignores_others() {
        let mut s = SseEvents::default();
        assert!(!s.push(b": keepalive\n\nevent: defin"));
        assert!(!s.push(b"itions\r\ndata: {\"etag\":\"x\"}\r\n"));
        assert!(s.push(b"\r\n"));
        assert!(!s.push(b"event: other\ndata: 1\n\n"));
        assert!(s.push(b"event: definitions\n\nevent: definitions\n\n"));
    }
}
