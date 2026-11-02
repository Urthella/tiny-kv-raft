//! A replicated key-value store on [openraft], with an in-process network that can fail and a
//! checker that decides whether what clients saw could have happened on one correct copy.

pub mod chaos;
pub mod cluster;
pub mod lincheck;
pub mod network;
pub mod store;

use std::io::Cursor;

use serde::{Deserialize, Serialize};

pub type NodeId = u64;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Set {
    pub key: String,
    pub value: String,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Set,
        R = (),
);

pub type Raft = openraft::Raft<TypeConfig>;
