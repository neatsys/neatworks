use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invite {
    chunk: [u8; 32],
    //
}
