/*
 * Copyright 2021 Cargill Incorporated
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ------------------------------------------------------------------------------
 */

//! lmdb-backed ReceiptStore implementation.
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::ops::{Bound, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};
use std::path::Path;
use std::sync::Arc;

use lmdb_zero as lmdb;
use lmdb_zero::error::LmdbResultExt;
use transact::{
    protocol::receipt::TransactionReceipt,
    protos::{FromBytes, IntoBytes},
};

use crate::error::{InternalError, InvalidStateError};
use crate::receipt::store::{ReceiptStore, ReceiptStoreError};

// 32-bit architectures
#[cfg(any(target_arch = "x86", target_arch = "arm"))]
const DEFAULT_SIZE: usize = 1 << 30; // 1024 ** 3

// Remaining architectures are assumed to be 64-bit
#[cfg(not(any(target_arch = "x86", target_arch = "arm")))]
const DEFAULT_SIZE: usize = 1 << 40; // 1024 ** 4

const NUM_DBS: u32 = 3; // main DB, index-to-key DB, and key-to-index DB

const ITER_CACHE_SIZE: usize = 64; // num of items at a time that are loaded into memory by iter

impl From<lmdb::Error> for ReceiptStoreError {
    fn from(err: lmdb::Error) -> Self {
        Self::InternalError(InternalError::from_source(Box::new(err)))
    }
}

pub struct LmdbDatabase {
    env: Arc<lmdb::Environment>,
    main_db: Arc<lmdb::Database<'static>>,
    index_to_key_db: Arc<lmdb::Database<'static>>,
    key_to_index_db: Arc<lmdb::Database<'static>>,
}

pub struct LmdbReceiptStore {
    databases: Arc<HashMap<String, LmdbDatabase>>,
    current_db: String,
}

impl LmdbReceiptStore {
    pub fn new(
        dir_path: &Path,
        files: &[String],
        current_db: String,
        size: Option<usize>,
    ) -> Result<Self, ReceiptStoreError> {
        // check to see if the name given for current_db exists in the list of file names given
        if !files.contains(&current_db) {
            return Err(ReceiptStoreError::InvalidStateError(
                InvalidStateError::with_message(format!(
                    "Current database name {} must match one of the database file names",
                    current_db
                )),
            ));
        }

        let flags = lmdb::open::MAPASYNC
            | lmdb::open::WRITEMAP
            | lmdb::open::NORDAHEAD
            | lmdb::open::NOSUBDIR;

        let mut dbs = HashMap::with_capacity(files.len());

        for file in files {
            let mut builder = lmdb::EnvBuilder::new().map_err(|err| {
                ReceiptStoreError::InternalError(InternalError::from_source_with_message(
                    Box::new(err),
                    "failed to get environment builder".to_string(),
                ))
            })?;
            builder.set_maxdbs(NUM_DBS).map_err(|err| {
                ReceiptStoreError::InternalError(InternalError::from_source_with_message(
                    Box::new(err),
                    "failed to set MAX_DBS".to_string(),
                ))
            })?;
            builder
                .set_mapsize(size.unwrap_or(DEFAULT_SIZE))
                .map_err(|err| {
                    ReceiptStoreError::InternalError(InternalError::from_source_with_message(
                        Box::new(err),
                        "failed to set MAP_SIZE".to_string(),
                    ))
                })?;

            let path = dir_path.join(file.clone());
            let path_str = path.to_str().ok_or_else(|| {
                ReceiptStoreError::InternalError(InternalError::with_message(format!(
                    "invalid path: {}",
                    path.display(),
                )))
            })?;
            let env = Arc::new(unsafe {
                builder.open(path_str, flags, 0o600).map_err(|err| {
                    ReceiptStoreError::InternalError(InternalError::from_source_with_message(
                        Box::new(err),
                        "database not found".to_string(),
                    ))
                })
            }?);

            let main_db = Arc::new(
                lmdb::Database::open(
                    env.clone(),
                    Some("main"),
                    &lmdb::DatabaseOptions::new(lmdb::db::CREATE),
                )
                .map_err(|err| {
                    ReceiptStoreError::InternalError(InternalError::from_source_with_message(
                        Box::new(err),
                        "failed to open main database".to_string(),
                    ))
                })?,
            );

            let index_to_key_db = Arc::new(
                lmdb::Database::open(
                    env.clone(),
                    Some("index_to_key"),
                    &lmdb::DatabaseOptions::new(lmdb::db::CREATE),
                )
                .map_err(|err| {
                    ReceiptStoreError::InternalError(InternalError::from_source_with_message(
                        Box::new(err),
                        "failed to open index-to-key database".to_string(),
                    ))
                })?,
            );

            let key_to_index_db = Arc::new(
                lmdb::Database::open(
                    env.clone(),
                    Some("key_to_index"),
                    &lmdb::DatabaseOptions::new(lmdb::db::CREATE),
                )
                .map_err(|err| {
                    ReceiptStoreError::InternalError(InternalError::from_source_with_message(
                        Box::new(err),
                        "failed to open key-to-index database".to_string(),
                    ))
                })?,
            );

            let database = LmdbDatabase {
                env,
                main_db,
                index_to_key_db,
                key_to_index_db,
            };
            dbs.insert(file.clone(), database);
        }
        Ok(LmdbReceiptStore {
            databases: Arc::new(dbs),
            current_db,
        })
    }

    pub fn set_current_db(&mut self, database: String) -> Result<(), ReceiptStoreError> {
        if self.databases.contains_key(&database) {
            self.current_db = database;
            Ok(())
        } else {
            return Err(ReceiptStoreError::InvalidStateError(
                InvalidStateError::with_message(format!(
                    "database {} does not exist in the receipt store",
                    database
                )),
            ));
        }
    }

    fn get_index_by_id(&self, id: String) -> Result<Option<u64>, ReceiptStoreError> {
        let db: &LmdbDatabase = self.databases.get(&self.current_db).ok_or_else(|| {
            ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(format!(
                "Unable to retrieve current database: {}",
                self.current_db
            )))
        })?;

        let txn = lmdb::ReadTransaction::new(db.env.clone())?;
        let access = txn.access();

        access
            .get::<_, [u8]>(&db.key_to_index_db, &id.into_bytes())
            .to_opt()?
            .map(|index| index.try_into().map(u64::from_ne_bytes))
            .transpose()
            .map_err(|err| {
                ReceiptStoreError::InternalError(InternalError::from_source(Box::new(err)))
            })
    }
}

impl ReceiptStore for LmdbReceiptStore {
    fn get_txn_receipt_by_id(
        &self,
        id: String,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        let db: &LmdbDatabase = self.databases.get(&self.current_db).ok_or_else(|| {
            ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(format!(
                "Unable to retrieve current database: {}",
                self.current_db
            )))
        })?;

        let txn = lmdb::ReadTransaction::new(db.env.clone())?;
        let access = txn.access();

        access
            .get::<_, [u8]>(&db.main_db, &id.into_bytes())
            .to_opt()?
            .map(|val| TransactionReceipt::from_bytes(val))
            .transpose()
            .map_err(|err| {
                ReceiptStoreError::InternalError(InternalError::from_source(Box::new(err)))
            })
    }

    fn get_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        let db: &LmdbDatabase = self.databases.get(&self.current_db).ok_or_else(|| {
            ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(format!(
                "Unable to retrieve current database: {}",
                self.current_db
            )))
        })?;

        let txn = lmdb::ReadTransaction::new(db.env.clone())?;
        let access = txn.access();

        Ok(
            match access
                .get::<_, [u8]>(&db.index_to_key_db, &index.to_ne_bytes().to_vec())
                .to_opt()?
            {
                Some(key) => access
                    .get::<_, [u8]>(&db.main_db, key)
                    .to_opt()?
                    .map(|val| TransactionReceipt::from_bytes(val))
                    .transpose()
                    .map_err(|err| {
                        ReceiptStoreError::InternalError(InternalError::from_source(Box::new(err)))
                    })?,
                None => None,
            },
        )
    }

    fn add_txn_receipts(&self, receipts: Vec<TransactionReceipt>) -> Result<(), ReceiptStoreError> {
        for receipt in receipts {
            let db: &LmdbDatabase = self.databases.get(&self.current_db).ok_or_else(|| {
                ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(format!(
                    "Unable to retrieve current database: {}",
                    self.current_db
                )))
            })?;
            let txn = lmdb::WriteTransaction::new(db.env.clone())?;

            {
                let index = self.count_txn_receipts()?;
                let id = receipt.transaction_id.clone();

                let mut access = txn.access();

                let id_bytes = &id.into_bytes();

                if access
                    .get::<_, [u8]>(&db.main_db, id_bytes)
                    .to_opt()?
                    .is_some()
                {
                    return Err(ReceiptStoreError::InvalidStateError(
                        InvalidStateError::with_message("value already exists for key".to_string()),
                    ));
                }
                if access
                    .get::<_, [u8]>(&db.index_to_key_db, &index.to_ne_bytes().to_vec())
                    .to_opt()?
                    .is_some()
                {
                    return Err(ReceiptStoreError::InvalidStateError(
                        InvalidStateError::with_message(
                            "value already exists at index".to_string(),
                        ),
                    ));
                }

                let receipt_bytes = &receipt.into_bytes().map_err(|err| {
                    ReceiptStoreError::InternalError(InternalError::from_source_with_message(
                        Box::new(err),
                        "unable to convert receipt to bytes".to_string(),
                    ))
                })?;

                access.put(&db.main_db, id_bytes, receipt_bytes, lmdb::put::NOOVERWRITE)?;
                access.put(
                    &db.index_to_key_db,
                    &index.to_ne_bytes().to_vec(),
                    id_bytes,
                    lmdb::put::NOOVERWRITE,
                )?;
                access.put(
                    &db.key_to_index_db,
                    id_bytes,
                    &index.to_ne_bytes().to_vec(),
                    lmdb::put::NOOVERWRITE,
                )?;
            }

            txn.commit()?;
        }
        Ok(())
    }

    fn remove_txn_receipt_by_id(
        &self,
        id: String,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        let db: &LmdbDatabase = self.databases.get(&self.current_db).ok_or_else(|| {
            ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(format!(
                "Unable to retrieve current database: {}",
                self.current_db
            )))
        })?;
        let txn = lmdb::WriteTransaction::new(db.env.clone())?;

        let id_bytes = &id.into_bytes();

        let receipt = {
            let mut access = txn.access();

            if let Some(index) = access
                .get::<_, [u8]>(&db.key_to_index_db, id_bytes)
                .to_opt()?
                .map(Vec::from)
            {
                let txn_receipt = access
                    .get::<_, [u8]>(&db.main_db, id_bytes)
                    .to_opt()?
                    .ok_or_else(|| {
                        ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(
                            "value missing from main store".to_string(),
                        ))
                    })
                    .and_then(|receipt| {
                        TransactionReceipt::from_bytes(receipt).map_err(|err| {
                            ReceiptStoreError::InternalError(InternalError::from_source(Box::new(
                                err,
                            )))
                        })
                    })?;

                access.del_key(&db.main_db, id_bytes)?;
                access.del_key(&db.index_to_key_db, &index)?;
                access.del_key(&db.key_to_index_db, id_bytes)?;

                Some(txn_receipt)
            } else {
                None
            }
        };

        txn.commit()?;

        Ok(receipt)
    }

    fn remove_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        let db: &LmdbDatabase = self.databases.get(&self.current_db).ok_or_else(|| {
            ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(format!(
                "Unable to retrieve current database: {}",
                self.current_db
            )))
        })?;
        let txn = lmdb::WriteTransaction::new(db.env.clone())?;

        let receipt = {
            let mut access = txn.access();

            if let Some(id) = access
                .get::<_, [u8]>(&db.index_to_key_db, &index.to_ne_bytes().to_vec())
                .to_opt()?
                .map(Vec::from)
            {
                let txn_receipt = access
                    .get::<_, [u8]>(&db.main_db, &id)
                    .to_opt()?
                    .ok_or_else(|| {
                        ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(
                            "value missing from main store".to_string(),
                        ))
                    })
                    .and_then(|txn_receipt| {
                        TransactionReceipt::from_bytes(txn_receipt).map_err(|err| {
                            ReceiptStoreError::InternalError(InternalError::from_source(Box::new(
                                err,
                            )))
                        })
                    })?;

                access.del_key(&db.main_db, &id)?;
                access.del_key(&db.index_to_key_db, &index.to_ne_bytes().to_vec())?;
                access.del_key(&db.key_to_index_db, &id)?;

                Some(txn_receipt)
            } else {
                None
            }
        };

        txn.commit()?;

        Ok(receipt)
    }

    fn count_txn_receipts(&self) -> Result<u64, ReceiptStoreError> {
        let db: &LmdbDatabase = self.databases.get(&self.current_db).ok_or_else(|| {
            ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(format!(
                "Unable to retrieve current database: {}",
                self.current_db
            )))
        })?;
        lmdb::ReadTransaction::new(db.env.clone())?
            .db_stat(&db.main_db)?
            .entries
            .try_into()
            .map_err(|err| {
                ReceiptStoreError::InternalError(InternalError::from_source(Box::new(err)))
            })
    }

    fn list_receipts_since(
        &self,
        id: Option<String>,
    ) -> Result<Box<dyn Iterator<Item = TransactionReceipt>>, ReceiptStoreError> {
        let db: &LmdbDatabase = self.databases.get(&self.current_db).ok_or_else(|| {
            ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(format!(
                "Unable to retrieve current database: {}",
                self.current_db
            )))
        })?;

        let iter = match id {
            Some(id) => {
                let index = self.get_index_by_id(id.clone())?.ok_or_else(|| {
                    ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(format!(
                        "unable to find id {} in receipt store",
                        id
                    )))
                })?;
                let range = LmdbReceiptStoreRange {
                    start: Bound::Excluded(index),
                    end: Bound::Unbounded,
                };
                LmdbReceiptStoreIter::new(
                    db.env.clone(),
                    db.index_to_key_db.clone(),
                    db.main_db.clone(),
                    Some(range),
                )
            }
            None => {
                let range = LmdbReceiptStoreRange {
                    start: Bound::Unbounded,
                    end: Bound::Unbounded,
                };
                LmdbReceiptStoreIter::new(
                    db.env.clone(),
                    db.index_to_key_db.clone(),
                    db.main_db.clone(),
                    Some(range),
                )
            }
        };
        Ok(Box::new(iter))
    }
}

/// A range that defines the start and end bounds for a range iterator on an `LmdbReceiptStore`.
pub struct LmdbReceiptStoreRange {
    pub start: Bound<u64>,
    pub end: Bound<u64>,
}

impl LmdbReceiptStoreRange {
    fn contains(&self, item: &u64) -> bool {
        let lower = match &self.start {
            Bound::Included(start_idx) => item >= start_idx,
            Bound::Excluded(start_idx) => item > start_idx,
            Bound::Unbounded => true,
        };
        let upper = match &self.end {
            Bound::Included(end_idx) => item <= end_idx,
            Bound::Excluded(end_idx) => item < end_idx,
            Bound::Unbounded => true,
        };
        lower && upper
    }
}

impl From<Range<u64>> for LmdbReceiptStoreRange {
    fn from(range: Range<u64>) -> Self {
        Self {
            start: Bound::Included(range.start),
            end: Bound::Excluded(range.end),
        }
    }
}

impl From<RangeInclusive<u64>> for LmdbReceiptStoreRange {
    fn from(range: RangeInclusive<u64>) -> Self {
        let (start, end) = range.into_inner();
        Self {
            start: Bound::Included(start),
            end: Bound::Included(end),
        }
    }
}

impl From<RangeFull> for LmdbReceiptStoreRange {
    fn from(_: RangeFull) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

impl From<RangeFrom<u64>> for LmdbReceiptStoreRange {
    fn from(range: RangeFrom<u64>) -> Self {
        Self {
            start: Bound::Included(range.start),
            end: Bound::Unbounded,
        }
    }
}

impl From<RangeTo<u64>> for LmdbReceiptStoreRange {
    fn from(range: RangeTo<u64>) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Excluded(range.end),
        }
    }
}

impl From<RangeToInclusive<u64>> for LmdbReceiptStoreRange {
    fn from(range: RangeToInclusive<u64>) -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Included(range.end),
        }
    }
}

impl From<(Bound<u64>, Bound<u64>)> for LmdbReceiptStoreRange {
    fn from((start, end): (Bound<u64>, Bound<u64>)) -> Self {
        Self { start, end }
    }
}

/// An iterator over entries in a `LmdbReceiptStore`. It uses an
/// in-memory cache of items that it retrieves from the database using a cursor. The cache is
/// refilled when it is emptied, and its size is defined by the ITER_CACHE_SIZE constant.
///
/// An optional `LmdbReceiptStoreRange` may be provided to get a subset of entries from the
/// database.
struct LmdbReceiptStoreIter {
    env: Arc<lmdb::Environment>,
    index_to_key_db: Arc<lmdb::Database<'static>>,
    main_db: Arc<lmdb::Database<'static>>,
    cache: VecDeque<TransactionReceipt>,
    range: LmdbReceiptStoreRange,
}

impl LmdbReceiptStoreIter {
    fn new(
        env: Arc<lmdb::Environment>,
        index_to_key_db: Arc<lmdb::Database<'static>>,
        main_db: Arc<lmdb::Database<'static>>,
        range: Option<LmdbReceiptStoreRange>,
    ) -> Self {
        let mut iter = Self {
            env,
            index_to_key_db,
            main_db,
            cache: VecDeque::new(),
            range: range.unwrap_or_else(|| (..).into()), // default to unbounded range
        };

        // Load cache
        if let Err(err) = iter.reload_cache() {
            error!("Failed to load iterator's initial cache: {}", err);
        }

        iter
    }

    fn reload_cache(&mut self) -> Result<(), String> {
        let txn = lmdb::ReadTransaction::new(self.env.clone()).map_err(|err| err.to_string())?;
        let access = txn.access();
        let mut index_cursor = txn
            .cursor(self.index_to_key_db.clone())
            .map_err(|err| err.to_string())?;

        // Set the cursor to the start of the range and get the first entry
        let mut first_entry = Some(match &self.range.start {
            Bound::Included(index) => {
                // Get the first entry >= index; that will be the first entry.
                index_cursor.seek_range_k::<[u8], [u8]>(&access, &index.to_ne_bytes().to_vec())
            }
            Bound::Excluded(index) => {
                // Get the first entry >= index. If that entry == index, get the next entry since this
                // is an exclusive bound.
                match index_cursor
                    .seek_range_k::<[u8], [u8]>(&access, &index.to_ne_bytes().to_vec())
                {
                    // If this is the same as the range's index,
                    Ok((found_index, _))
                        if found_index == index.to_ne_bytes().to_vec().as_slice() =>
                    {
                        index_cursor.next::<[u8], [u8]>(&access)
                    }
                    other => other,
                }
            }
            Bound::Unbounded => {
                // Starting from the beginning
                index_cursor.first::<[u8], [u8]>(&access)
            }
        });

        // Load up to ITER_CACHE_SIZE entries; stop if all entries have been read or if the end of
        // the range is reached.
        for _ in 0..ITER_CACHE_SIZE {
            let next_entry = first_entry
                .take()
                .unwrap_or_else(|| index_cursor.next::<[u8], [u8]>(&access));
            match next_entry {
                Ok((index, id)) => {
                    let index = index
                        .try_into()
                        .map(u64::from_ne_bytes)
                        .map_err(|err| err.to_string())?;
                    // If this index is in the range, add the value to the cache; otherwise, exit.
                    if !self.range.contains(&index) {
                        break;
                    } else {
                        self.cache.push_back(
                            TransactionReceipt::from_bytes(
                                access
                                    .get::<_, [u8]>(&self.main_db, id)
                                    .map_err(|err| err.to_string())?,
                            )
                            .map_err(|err| err.to_string())?,
                        );
                        // Update the range start to reflect only what's left
                        self.range.start = Bound::Excluded(index);
                    }
                }
                Err(lmdb::error::Error::Code(lmdb::error::NOTFOUND)) => break,
                Err(err) => return Err(err.to_string()),
            }
        }

        Ok(())
    }
}

impl Iterator for LmdbReceiptStoreIter {
    // type Item = Result<TransactionReceipt, ReceiptStoreError>;
    type Item = TransactionReceipt;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(err) = self.reload_cache() {
            error!("Failed to load iterator's cache: {}", err);
        }
        // self.cache.pop_front().map(Ok)
        self.cache.pop_front()
    }
}
