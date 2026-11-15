//! Low-latency networking: TCP socket tuning and N-way connection racing.

pub mod tuned_ws;

/// How to dial a venue socket.
#[derive(Debug, Clone)]
pub struct NetConfig {
    /// Apply `TCP_NODELAY`, the large `SO_RCVBUF` and `TCP_QUICKACK`. The
    /// benchmark suite disables this to measure the default OS behaviour.
    pub tuned: bool,
    /// Parallel TLS + WebSocket candidates to race. `1` disables racing.
    pub race_candidates: usize,
    /// `SO_RCVBUF`; a large receive buffer absorbs news-event bursts instead of
    /// letting the kernel drop packets.
    pub recv_buffer_bytes: usize,
    /// Apply `TCP_QUICKACK` on Linux.
    pub quickack: bool,
    /// Optional explicit local source port, to keep two connections on distinct
    /// 4-tuples when the host would otherwise hash them to the same NIC queue.
    pub bind_port_hint: Option<u16>,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            tuned: true,
            race_candidates: 1,
            recv_buffer_bytes: 2 * 1024 * 1024,
            quickack: true,
            bind_port_hint: None,
        }
    }
}
