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

use std::{
    collections::HashMap,
    convert::Infallible,
    hash::{BuildHasher, RandomState},
    iter::repeat_with,
};

use bincode::Options;
use rand::{
    distributions::{Alphanumeric, Distribution as _, Uniform},
    Rng,
};
use rand_distr::Zeta;
use serde::{Deserialize, Serialize};

use crate::{message::Payload, workload::WorkloadWithSavedOp};

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

    // `keyseqence`
    insert_key_num: usize,
    // `transactioninsertkeysequence`
    next_insert_num: usize,
    inserting_names: HashMap<usize, String>,
    inserted_num: usize,

    field_length: Gen,
    key_num: Gen,
    scan_len: Gen,
    rmw_update: Option<Op>,
}

#[derive(Debug)]
pub struct WorkloadSettings {
    pub record_count: usize,
    pub operation_count: usize,
    pub field_length: usize,
    pub field_length_distr: SettingsDistr,
    pub read_proportion: f32,
    pub update_proportion: f32,
    pub insert_proportion: f32,
    pub read_modify_write_proportion: f32,
    pub scan_proportion: f32,
    pub max_scan_length: usize,
    pub scan_length_distr: SettingsDistr,
    pub request_distr: SettingsDistr,
    pub zero_padding: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum SettingsDistr {
    Constant,
    Uniform,
    Zipfian,
    Latest,
}

enum Gen {
    Constant(usize),
    Uniform(Uniform<usize>),
    Zipf(GenZipf),
}

struct GenZipf {
    min: usize,
    item_count: usize,
    zeta: Zeta<f32>,
}

impl Gen {
    fn new(distr: SettingsDistr, n: usize) -> anyhow::Result<Self> {
        Ok(match distr {
            SettingsDistr::Constant => Self::Constant(n),
            SettingsDistr::Uniform => Self::Uniform(Uniform::new(1, n)),
            SettingsDistr::Zipfian => Self::Zipf(GenZipf {
                min: 0,
                item_count: n,
                zeta: Zeta::new(0.99)?,
            }),
            SettingsDistr::Latest => anyhow::bail!("unimplemented"),
        })
    }

    fn gen(&self, rng: &mut impl Rng) -> usize {
        match self {
            Self::Constant(n) => *n,
            Self::Uniform(uniform) => uniform.sample(rng),
            Self::Zipf(gen) => gen.gen(rng),
        }
    }
}

impl GenZipf {
    fn gen(&self, rng: &mut impl Rng) -> usize {
        let r = self.zeta.sample(rng) as u64;
        self.min + RandomState::new().hash_one(r) as usize % self.item_count
    }
}

impl<R> Workload<R> {
    pub fn new(rng: R, settings: WorkloadSettings) -> anyhow::Result<Self> {
        Ok(Self {
            rng,
            insert_key_num: 0,
            next_insert_num: settings.record_count,
            inserting_names: Default::default(),
            inserted_num: settings.record_count,
            field_length: Gen::new(settings.field_length_distr, settings.field_length)?,
            key_num: Gen::new(settings.request_distr, settings.record_count)?,
            scan_len: Gen::new(settings.scan_length_distr, settings.max_scan_length)?,
            settings,
            rmw_update: None,
        })
    }

    fn build_key_name(&self, key_num: usize) -> String {
        let key = RandomState::new().hash_one(key_num).to_string();
        let mut pre_key = String::from("user");
        for _ in 0..self.settings.zero_padding - key.len() {
            pre_key += &"0"
        }
        pre_key + &key
    }
}

impl<R: Rng> Workload<R> {
    fn build_value(&mut self) -> String {
        let field_len = self.field_length.gen(&mut self.rng);
        repeat_with(|| char::from(Alphanumeric.sample(&mut self.rng)))
            .take(field_len)
            .collect()
    }

    fn startup_insert(&mut self) -> Op {
        self.insert_key_num += 1;
        let key = self.build_key_name(self.insert_key_num);
        let value = self.build_value();
        Op::Insert(key, value)
    }

    pub fn startup_ops(&mut self) -> impl Iterator<Item = Op> + '_ {
        let record_count = self.settings.record_count;
        repeat_with(|| self.startup_insert()).take(record_count)
    }

    fn key_num(&mut self) -> usize {
        let mut key_num;
        while {
            key_num = self.key_num.gen(&mut self.rng);
            key_num >= self.inserted_num
        } {}
        key_num
    }

    fn read(&mut self) -> Op {
        let key_num = self.key_num();
        let key_name = self.build_key_name(key_num);
        Op::Read(key_name)
    }

    fn read_modify_write(&mut self) -> [Op; 2] {
        let key_num = self.key_num();
        let key_name = self.build_key_name(key_num);
        let value = self.build_value();
        [Op::Read(key_name.clone()), Op::Update(key_name, value)]
    }

    fn scan(&mut self) -> Op {
        let key_num = self.key_num();
        let key_name = self.build_key_name(key_num);
        let len = self.scan_len.gen(&mut self.rng);
        Op::Scan(key_name, len)
    }

    fn update(&mut self) -> Op {
        let key_num = self.key_num();
        let key_name = self.build_key_name(key_num);
        let value = self.build_value();
        Op::Update(key_name, value)
    }

    fn insert(&mut self) -> Op {
        self.next_insert_num += 1;
        let key_num = self.next_insert_num;
        let key_name = self.build_key_name(key_num);
        self.inserting_names.insert(key_num, key_name.clone());
        let value = self.build_value();
        Op::Insert(key_name, value)
    }

    fn op(&mut self) -> Op {
        if let Some(op) = self.rmw_update.take() {
            return op;
        }
        let mut x = self.rng.gen::<f32>();
        if x < self.settings.read_proportion {
            return self.read();
        }
        x -= self.settings.read_proportion;
        if x < self.settings.update_proportion {
            return self.update();
        }
        x -= self.settings.update_proportion;
        if x < self.settings.insert_proportion {
            return self.insert();
        }
        x -= self.settings.insert_proportion;
        if x < self.settings.scan_proportion {
            return self.scan();
        }
        let [op, update_op] = self.read_modify_write();
        self.rmw_update.get_or_insert(update_op);
        op
    }
}

impl<R: Rng> WorkloadWithSavedOp for Workload<R> {
    type Dry = Infallible; // TODO compatible with model checking

    fn dehydrate(self) -> Self::Dry {
        unimplemented!()
    }

    fn next_op(&mut self) -> anyhow::Result<Option<Payload>> {
        Ok(Some(Payload(bincode::options().serialize(&self.op())?)))
    }

    fn on_result(&mut self, Payload(op): Payload, _: Payload) -> anyhow::Result<()> {
        if let Op::Update(key_name, _) = bincode::options().deserialize(&op)? {
            let key_num = *self
                .inserting_names
                .iter()
                .find(|(_, name)| **name == key_name)
                .ok_or(anyhow::anyhow!("missing key name"))?
                .0;
            self.inserting_names.remove(&key_num).unwrap();
            while self.inserted_num < self.next_insert_num
                && !self.inserting_names.contains_key(&self.inserted_num)
            {
                self.inserted_num += 1
            }
        }
        Ok(())
    }
}
