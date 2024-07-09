use std::time::Duration;

pub mod client;
pub mod messages;
pub mod replica;
#[cfg(test)]
pub mod tests;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PublicParameters {
    pub num_replica: usize,
    pub num_faulty: usize,

    pub num_concurrent: usize,
    pub max_batch_size: usize,

    pub client_resend_interval: Duration,
    pub progress_prepare_interval: Duration,
    pub view_change_delay: Duration,
    pub progress_view_change_interval: Duration,
    pub state_transfer_delay: Duration,
}

impl PublicParameters {
    pub fn durations(client_resend_interval: Duration) -> Self {
        Self {
            client_resend_interval,
            progress_prepare_interval: client_resend_interval / 5,
            // keep track of the timing of start sending ViewChange for a view, do not repeat; alarm
            // (at most) once for each view
            // i don't know what's a good delay to alarm; tentatively choose this to hopefully
            // resolve the liveness issue before next client resending, while leave enough time for
            // primary to continue trying
            view_change_delay: client_resend_interval / 2,
            // for periodically resending ViewChange after `DoViewChange` is alarmed
            // it is ok this is shorter than `ProgressPrepare`, as this is not enabled before
            // `DoViewChange` timeout, which is longer than `ProgressPrepare`
            progress_view_change_interval: client_resend_interval / 10,
            state_transfer_delay: client_resend_interval * 10, // TODO

            num_replica: Default::default(),
            num_faulty: Default::default(),
            num_concurrent: Default::default(),
            max_batch_size: Default::default(),
        }
    }
}
