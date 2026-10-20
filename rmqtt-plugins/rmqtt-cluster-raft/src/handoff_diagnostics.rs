//! Bounded, maintenance-only observations. None of these values authorizes
//! delivery, replay, retirement or a change to a pinned receiver.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rmqtt::types::{ClientId, NodeId};

pub(crate) const FIRST_WAIT_LOG_AGE: Duration = Duration::from_secs(5);
const REPEAT_INTERVAL: Duration = Duration::from_secs(30);
const CHANGE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ForwardAttemptStage {
    MissingClient,
    Encode,
    SendCall,
    DecodeReply,
    ReplyType,
    ReplyShape,
    RemoteError,
    GrpcReplyType,
}

impl ForwardAttemptStage {
    pub(crate) fn request_may_have_been_sent(self) -> bool {
        !matches!(self, Self::MissingClient | Self::Encode)
    }
}

#[derive(Debug)]
pub(crate) struct ForwardAttemptError {
    pub stage: ForwardAttemptStage,
    pub error: anyhow::Error,
}

impl ForwardAttemptError {
    pub(crate) fn new(stage: ForwardAttemptStage, error: anyhow::Error) -> Self {
        Self { stage, error }
    }

    pub(crate) fn diagnostic(&self, target_node_id: NodeId) -> AttemptDiagnostic {
        // An unexpected gRPC reply can contain a whole session snapshot.
        // Do not copy its Debug representation into a diagnostic log.
        let detail = if self.stage == ForwardAttemptStage::GrpcReplyType {
            "unexpected gRPC reply type; contents omitted".to_owned()
        } else {
            bounded_detail(&format!("{:#}", self.error))
        };
        AttemptDiagnostic {
            target_node_id,
            result: "transport_error",
            stage: Some(self.stage),
            request_may_have_been_sent: Some(self.stage.request_may_have_been_sent()),
            detail,
        }
    }
}

#[derive(Debug)]
pub(crate) struct AttemptDiagnostic {
    pub target_node_id: NodeId,
    pub result: &'static str,
    pub stage: Option<ForwardAttemptStage>,
    pub request_may_have_been_sent: Option<bool>,
    pub detail: String,
}

pub(crate) fn bounded_detail(detail: &str) -> String {
    const MAX_CHARS: usize = 512;
    let mut chars = detail.chars();
    let mut bounded = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        bounded.push_str("...[truncated]");
    }
    bounded
}

pub(crate) struct WaitLogThrottle<K> {
    last: Option<(K, Instant)>,
}

impl<K: PartialEq> WaitLogThrottle<K> {
    pub(crate) fn new() -> Self {
        Self { last: None }
    }

    pub(crate) fn should_log(&mut self, key: K, now: Instant, age: Duration) -> bool {
        if age < FIRST_WAIT_LOG_AGE {
            return false;
        }
        if let Some((previous, logged_at)) = &self.last {
            let elapsed = now.saturating_duration_since(*logged_at);
            if elapsed < REPEAT_INTERVAL && (previous == &key || elapsed < CHANGE_INTERVAL) {
                return false;
            }
        }
        self.last = Some((key, now));
        true
    }
}

/// Repeated Pending/Retryable maintenance receives only. Both cardinality and
/// output are bounded even when a caller continually supplies new client IDs.
pub(crate) struct ReceiverLogThrottle {
    clients: HashMap<ClientId, Instant>,
    window_start: Instant,
    emitted: usize,
}

impl ReceiverLogThrottle {
    pub(crate) fn new() -> Self {
        Self { clients: HashMap::new(), window_start: Instant::now(), emitted: 0 }
    }

    pub(crate) fn should_log(&mut self, client: &ClientId, now: Instant) -> bool {
        if now.saturating_duration_since(self.window_start) >= REPEAT_INTERVAL {
            self.window_start = now;
            self.emitted = 0;
            self.clients.retain(|_, last| now.saturating_duration_since(*last) < REPEAT_INTERVAL);
        }
        if self.emitted >= 20
            || self
                .clients
                .get(client)
                .is_some_and(|last| now.saturating_duration_since(*last) < REPEAT_INTERVAL)
        {
            return false;
        }
        // At most 20 new entries per window; retain also bounds the map.
        self.clients.insert(client.clone(), now);
        self.emitted += 1;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wait_logs_are_delayed_repeated_and_bounded_under_state_flapping() {
        let now = Instant::now();
        let mut throttle = WaitLogThrottle::new();
        assert!(!throttle.should_log("no_owner", now, Duration::from_secs(4)));
        assert!(throttle.should_log("no_owner", now, FIRST_WAIT_LOG_AGE));
        assert!(!throttle.should_log("no_owner", now + Duration::from_secs(29), FIRST_WAIT_LOG_AGE));
        assert!(!throttle.should_log("pinned", now + Duration::from_secs(1), FIRST_WAIT_LOG_AGE));
        assert!(throttle.should_log("pinned", now + CHANGE_INTERVAL, FIRST_WAIT_LOG_AGE));
        assert!(!throttle.should_log("no_owner", now + Duration::from_secs(6), FIRST_WAIT_LOG_AGE));
        assert!(throttle.should_log("pinned", now + Duration::from_secs(35), FIRST_WAIT_LOG_AGE));
    }

    #[test]
    fn receiver_logs_bound_repeated_clients_and_new_client_floods() {
        let mut throttle = ReceiverLogThrottle::new();
        let now = throttle.window_start;
        for index in 0..20 {
            let client = ClientId::from(format!("client-{index}"));
            assert!(throttle.should_log(&client, now));
            assert!(!throttle.should_log(&client, now));
        }
        assert!(!throttle.should_log(&"new-client".into(), now));
        assert!(throttle.should_log(&"client-0".into(), now + REPEAT_INTERVAL));
        assert_eq!(throttle.clients.len(), 1);
    }

    #[test]
    fn bounded_details_preserve_utf8_and_omit_excess_data() {
        let detail = "消息".repeat(400);
        let bounded = bounded_detail(&detail);
        assert!(bounded.ends_with("...[truncated]"));
        assert_eq!(bounded.chars().count(), 512 + "...[truncated]".len());
    }
}
