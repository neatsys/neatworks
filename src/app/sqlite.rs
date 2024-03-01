use std::sync::Mutex;

use bincode::Options;
use rusqlite::{params_from_iter, Connection, OptionalExtension as _};

use super::{
    ycsb::{Op, Result},
    App,
};

pub struct Sqlite {
    connection: Mutex<Connection>, // hope there's better way to be `Sync`
    field_count: usize,
}

impl Sqlite {
    pub fn new(field_count: usize) -> anyhow::Result<Self> {
        let connection = Connection::open_in_memory()?;
        let statement = format!(
            "CREATE TABLE users (ycsb_key VARCHAR PRIMARY KEY, {});",
            (0..field_count)
                .map(|i| format!("field{i} VARCHAR"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        connection.execute(&statement, ())?;
        Ok(Self {
            connection: Mutex::new(connection),
            field_count,
        })
    }
}

impl App for Sqlite {
    fn execute(&mut self, op: &[u8]) -> anyhow::Result<Vec<u8>> {
        let result = match bincode::options().deserialize(op)? {
            Op::Read(key) => {
                if let Some(values) = self
                    .connection
                    .lock()
                    .map_err(|err| anyhow::anyhow!(err.to_string()))?
                    .prepare_cached("SELECT * FROM users WHERE ycsb_key = ?1")?
                    .query_row((key,), |row| {
                        (0..self.field_count)
                            .map(|i| row.get(i))
                            .collect::<std::result::Result<Vec<_>, _>>()
                    })
                    .optional()?
                {
                    Result::ReadOk(values)
                } else {
                    Result::NotFound
                }
            }
            Op::Update(key, field, value) => {
                if self
                    .connection
                    .lock()
                    .map_err(|err| anyhow::anyhow!(err.to_string()))?
                    .prepare_cached(&format!(
                        "UPDATE users SET field{field} = ?2 WHERE ycsb_key = ?1"
                    ))?
                    .execute((key, value))?
                    == 0
                {
                    Result::NotFound
                } else {
                    Result::Ok
                }
            }
            Op::Insert(key, values) => {
                self.connection
                    .lock()
                    .map_err(|err| anyhow::anyhow!(err.to_string()))?
                    .prepare_cached(&format!(
                        "INSERT INTO users (ycsb_key, {}) VALUES({})",
                        (0..self.field_count)
                            .map(|i| format!("field{i}"))
                            .collect::<Vec<_>>()
                            .join(", "),
                        (0..self.field_count + 1)
                            .map(|i| format!("?{}", i + 1))
                            .collect::<Vec<_>>()
                            .join(", "),
                    ))?
                    .execute(params_from_iter([key].into_iter().chain(values)))?;
                Result::Ok
            }
            Op::Scan(key, count) => {
                let connection = self
                    .connection
                    .lock()
                    .map_err(|err| anyhow::anyhow!(err.to_string()))?;
                let mut statement = connection.prepare_cached(
                    "SELECT * FROM users WHERE ycsb_key >= ?1 ORDER BY ycsb_key LIMIT ?2",
                )?;
                let mut rows = statement.query((key, count))?;
                let mut records = Vec::new();
                while let Some(row) = rows.next()? {
                    let record = (0..self.field_count)
                        // .map(|i| row.get(i))
                        // ration explained in `btree`
                        .map(|i| {
                            use std::hash::{BuildHasher, BuildHasherDefault};
                            Ok(format!(
                                "{:x}",
                                BuildHasherDefault::<rustc_hash::FxHasher>::default()
                                    .hash_one(row.get::<_, String>(i)?)
                                    & 0xf
                            ))
                        })
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    records.push(record)
                }
                Result::ScanOk(records)
            }
            Op::Delete(key) => {
                if self
                    .connection
                    .lock()
                    .map_err(|err| anyhow::anyhow!(err.to_string()))?
                    .prepare_cached("DELETE FROM users WHERE ycsb_key = ?1")?
                    .execute((key,))?
                    == 0
                {
                    Result::NotFound
                } else {
                    Result::Ok
                }
            }
        };
        Ok(bincode::options().serialize(&result)?)
    }
}
