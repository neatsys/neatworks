// a reduced implementation of YCSB core workload. there's no `impl App` here,
// other modules may contain implementors that work with `Op` and `Result`
//
// detailed difference with upstream
// * table name is omitted, and no support to multiple fields so field name is
//   also omitted
// * deterministic and data integrity are not implemented
// * `insertstart`/`insertcount` are removed. the load phase is supposed to
//   bypass the evaluated protocols and directly perform on `impl App`s with
//   `startup_ops`. use seeded RNG to build store with deterministic content
// * key chooser distribution i.e. request distribution is based on
//   `recordcount` rather than `insertstart`/`insertcount`. i don't understand
//   why restrict each client to operate on nonoverlapping keys anyway
// * `operationcount` is optional
// * zipfian request distribution only ranges in up to `recordcount` instead of
//   taking the inserted keys during evaluation into account. the other
//   distributions only range up to `recordcount` (`insertstart + insertcount`
//   in upstream) as well. this change enables optional `operationcount`
// * only hashed insertion order is implemented
// * only zipfian, latest and uniform request distributions are implemented
// * zero padding length is default to 60 which correponding to 64 byte keys

use std::{
    collections::HashSet,
    hash::{BuildHasher, RandomState},
    iter::repeat_with,
};

use bincode::Options;
use rand::{
    distributions::{Alphanumeric, Distribution as _, Uniform},
    Rng,
};
use rand_distr::{WeightedAliasIndex, Zeta};
use serde::{Deserialize, Serialize};

use crate::message::Payload;

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
    inserting_nums: HashSet<usize>,
    inserted_num: usize,

    field_length: Gen,
    key_num: Gen,
    scan_len: Gen,
    transaction: WeightedAliasIndex<f32>,

    transaction_count: usize,
    rmw_update: Option<Op>,
}

#[derive(Debug)]
pub struct WorkloadSettings {
    pub record_count: usize,
    pub operation_count: Option<usize>,
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

impl WorkloadSettings {
    pub fn new(record_count: usize) -> Self {
        Self {
            record_count,
            operation_count: None,
            field_length: 100,
            field_length_distr: SettingsDistr::Constant,
            read_proportion: 0.95,
            update_proportion: 0.05,
            insert_proportion: 0.,
            read_modify_write_proportion: 0.,
            scan_proportion: 0.,
            max_scan_length: 1000,
            scan_length_distr: SettingsDistr::Uniform,
            request_distr: SettingsDistr::Uniform,
            zero_padding: 60,
        }
    }
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
                min: 1,
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
            inserting_nums: Default::default(),
            inserted_num: settings.record_count,
            field_length: Gen::new(settings.field_length_distr, settings.field_length)?,
            key_num: Gen::new(settings.request_distr, settings.record_count)?,
            scan_len: Gen::new(settings.scan_length_distr, settings.max_scan_length)?,
            transaction: WeightedAliasIndex::new(vec![
                settings.read_proportion,
                settings.update_proportion,
                settings.insert_proportion,
                settings.scan_proportion,
                settings.read_modify_write_proportion,
            ])?,
            settings,
            transaction_count: 0,
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
    const TRANSACTION_FN: [fn(&mut Self) -> Op; 4] =
        [Self::read, Self::update, Self::insert, Self::scan];

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
        let key_num = self.next_insert_num;
        self.next_insert_num += 1;
        let key_name = self.build_key_name(key_num);
        self.inserting_nums.insert(key_num);
        let value = self.build_value();
        Op::Insert(key_name, value)
    }

    fn op(&mut self) -> Option<Op> {
        if let Some(op) = self.rmw_update.take() {
            return Some(op);
        }
        if Some(self.transaction_count) == self.settings.operation_count {
            return None;
        }
        let i = self.transaction.sample(&mut self.rng);
        if let Some(f) = Self::TRANSACTION_FN.get(i) {
            Some(f(self))
        } else {
            let [op, update_op] = self.read_modify_write();
            self.rmw_update.get_or_insert(update_op);
            Some(op)
        }
    }
}

impl<R: Rng> crate::workload::Workload for Workload<R> {
    type Attach = Option<usize>;

    fn next_op(&mut self) -> anyhow::Result<Option<(Payload, Self::Attach)>> {
        let Some(op) = self.op() else { return Ok(None) };
        Ok(Some((
            Payload(bincode::options().serialize(&op)?),
            if matches!(op, Op::Insert(..)) {
                Some(self.next_insert_num - 1)
            } else {
                None
            },
        )))
    }

    fn on_result(&mut self, _: Payload, key_num: Self::Attach) -> anyhow::Result<()> {
        if let Some(key_num) = key_num {
            let removed = self.inserting_nums.remove(&key_num);
            if !removed {
                anyhow::bail!("missing insert key number")
            }
            while self.inserted_num < self.next_insert_num
                && !self.inserting_nums.contains(&self.inserted_num)
            {
                self.inserted_num += 1
            }
        }
        Ok(())
    }
}
