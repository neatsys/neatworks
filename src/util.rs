use std::{fmt::Debug, hash::Hash};

use serde::{Deserialize, Serialize};

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, derive_more::Deref, Serialize, Deserialize,
)]
pub struct Payload(pub Vec<u8>);

impl Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(s) = std::str::from_utf8(&self.0) {
            write!(f, "b{s:?}")
        } else {
            write!(
                f,
                "Payload({}{})",
                self.0
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .take(32)
                    .collect::<Vec<_>>()
                    .concat(),
                if self.0.len() > 32 { ".." } else { "" }
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Request<A> {
    pub client_id: u32,
    pub client_addr: A,
    pub seq: u32,
    pub op: Payload,
}
