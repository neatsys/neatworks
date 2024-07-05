use std::time::Duration;

pub mod client;
pub mod messages;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PublicParameters {
    pub num_replica: usize,
    pub num_faulty: usize,

    pub max_batch_size: usize,

    pub client_resend_interval: Duration,
    pub view_change_delay: Duration,
}
