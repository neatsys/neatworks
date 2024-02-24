// a reduced implementation of YCSB core workload. there's no `impl App` here,
// other modules may contain implementors that work with `Op` and `Result`
// table name is omitted, and no support to multiple fields so field name is
// also omitted
// only nondeterministic value is implemented, and data integrity is always off
// `insertstart` is not supported. the load phase is supposed to bypass the
// evaluated protocols and directly perform on stores. use seeded RNG to build
// store with deterministic content
// only hashed insertion order is implemented
// only zipfian, latest and uniform request distributions are implemented

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Read(String),
    Scan(String, usize),
    Update(String, String),
    Insert(String, String),
    Delete(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Result {
    ReadOk(String),
    ScanOk(Vec<String>),
    Ok,
}

pub struct Workload<R> {
    rng: R,
    settings: WorkloadSettings,
}

#[derive(Debug)]
pub struct WorkloadSettings {
    pub record_count: usize,
    pub operation_count: usize,
    pub field_length: usize,
    pub field_length_distr: Distr,
    pub read_proportion: f64,
    pub update_proportion: f64,
    pub insert_proportion: f64,
    pub read_modify_write_proportion: f64,
    pub scan_proportion: f64,
    pub max_scan_length: usize,
    pub scan_length_distr: Distr,
    pub request_distr: Distr,
}

#[derive(Debug)]
pub enum Distr {
    Constant,
    Uniform,
    Zipfian,
    Latest,
}
