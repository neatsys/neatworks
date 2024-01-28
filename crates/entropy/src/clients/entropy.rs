use serde::{Deserialize, Serialize};

use crate::{GetOk, PutOk};

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum Message {
    PutOk(PutOk),
    GetOk(GetOk),
}

pub type MessageNet<T> = augustus::net::MessageNet<T, Message>;
