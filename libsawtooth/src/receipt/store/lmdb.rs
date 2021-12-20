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
use log::error;
use transact::{
    protocol::receipt::TransactionReceipt,
    protos::{FromBytes, IntoBytes},
};

use crate::error::{InternalError, InvalidStateError};
use crate::receipt::store::{ReceiptIter, ReceiptStore, ReceiptStoreError};

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
                    let error_msg = format!("{}: \"{}\"", err, path_str);
                    ReceiptStoreError::InternalError(InternalError::from_source_with_message(
                        Box::new(err),
                        error_msg,
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
            .map(TransactionReceipt::from_bytes)
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
                    .map(TransactionReceipt::from_bytes)
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

    fn list_receipts_since(&self, id: Option<String>) -> Result<ReceiptIter, ReceiptStoreError> {
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

    fn clone_box(&self) -> Box<dyn ReceiptStore> {
        Box::new(Self {
            databases: self.databases.clone(),
            current_db: self.current_db.clone(),
        })
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

    fn reload_cache(&mut self) -> Result<(), ReceiptStoreError> {
        let txn = lmdb::ReadTransaction::new(self.env.clone())?;
        let access = txn.access();
        let mut index_cursor = txn.cursor(self.index_to_key_db.clone()).map_err(|err| {
            ReceiptStoreError::InternalError(InternalError::from_source_with_message(
                Box::new(err),
                "failed to get database cursor".to_string(),
            ))
        })?;

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
                    let index = index.try_into().map(u64::from_ne_bytes).map_err(|err| {
                        ReceiptStoreError::InternalError(InternalError::from_source_with_message(
                            Box::new(err),
                            "unable to convert from bytes to index".to_string(),
                        ))
                    })?;
                    // If this index is in the range, add the value to the cache; otherwise, exit.
                    if !self.range.contains(&index) {
                        break;
                    } else {
                        self.cache.push_back(
                            TransactionReceipt::from_bytes(
                                access.get::<_, [u8]>(&self.main_db, id).map_err(|err| {
                                    ReceiptStoreError::InternalError(InternalError::from_source(
                                        Box::new(err),
                                    ))
                                })?,
                            )
                            .map_err(|err| {
                                ReceiptStoreError::InternalError(
                                    InternalError::from_source_with_message(
                                        Box::new(err),
                                        "unable to convert from bytes to transaction receipt"
                                            .to_string(),
                                    ),
                                )
                            })?,
                        );
                        // Update the range start to reflect only what's left
                        self.range.start = Bound::Excluded(index);
                    }
                }
                Err(lmdb::error::Error::Code(lmdb::error::NOTFOUND)) => break,
                Err(err) => {
                    return Err(ReceiptStoreError::InternalError(
                        InternalError::from_source_with_message(
                            Box::new(err),
                            "unable to get next database entry".to_string(),
                        ),
                    ))
                }
            }
        }

        Ok(())
    }
}

impl Iterator for LmdbReceiptStoreIter {
    type Item = Result<TransactionReceipt, ReceiptStoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cache.is_empty() {
            if let Err(err) = self.reload_cache() {
                return Some(Err(err));
            }
        };
        self.cache.pop_front().map(Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transact::protocol::receipt::{Event, StateChange, TransactionResult};

    /// Verify that a new `LmdbReceiptStore` can be created with a single LMDB instance
    ///
    /// 1. Create a new `LmdbReceiptStore` with only one file and set it as the `current_db`
    /// 2. Check process was successful
    #[test]
    fn test_lmdb_receipt_store_single_file() {
        let (file, mut path) = get_temp_db_path_and_file(1);

        let test_result = std::panic::catch_unwind(|| {
            LmdbReceiptStore::new(&path.as_path(), &[file[0].clone()], file[0].clone(), None)
                .expect("Failed to create LMDB receipt store");
        });

        path.push(file[0].clone());
        std::fs::remove_file(path.as_path()).expect("Failed to remove temp DB file");

        assert!(test_result.is_ok());
    }

    /// Verify that a new `LmdbReceiptStore` can be created with multiple LMDB instances
    ///
    /// 1. Create a new `LmdbReceiptStore` with two files and set the second file as the `current_db`
    /// 2. Check process was successful
    #[test]
    fn test_lmdb_receipt_store_multiple_files() {
        let (files, path) = get_temp_db_path_and_file(2);

        let test_result = std::panic::catch_unwind(|| {
            LmdbReceiptStore::new(
                &path.as_path(),
                &[files[0].clone(), files[1].clone()],
                files[1].clone(),
                Some(1024 * 1024),
            )
            .expect("Failed to create LMDB receipt store");
        });

        for i in 0..files.len() {
            let mut temp_path = path.clone();
            temp_path.push(files[i].clone());
            std::fs::remove_file(temp_path.as_path()).expect("Failed to remove temp DB file");
        }

        assert!(test_result.is_ok());
    }

    /// Verify that a new `LmdbReceiptStore` can be created with multiple LMDB instances and the
    /// current database can be changed
    ///
    /// 1. Create a new `LmdbReceiptStore` with two files and set the second file as the `current_db`
    /// 2. Change the `current_db` to be the first file
    /// 3. Attempt to change the `current_db` to be a file that does not exist
    /// 4. Check that this action returns an error
    /// 5. Check that the `current_db` is still the first file
    #[test]
    fn test_lmdb_receipt_store_change_current_db() {
        let (files, mut path) = get_temp_db_path_and_file(2);

        let test_result = std::panic::catch_unwind(|| {
            let mut receipt_store = LmdbReceiptStore::new(
                &path.as_path(),
                &[files[0].clone(), files[1].clone()],
                files[1].clone(),
                Some(1024 * 1024),
            )
            .expect("Failed to create LMDB receipt store");

            assert!(receipt_store.set_current_db(files[0].clone()).is_ok());
            assert!(receipt_store
                .set_current_db("nonexistent_file".to_string())
                .is_err());

            assert_eq!(receipt_store.current_db, files[0]);
        });

        path.push(files[0].clone());
        std::fs::remove_file(path.as_path()).expect("Failed to remove temp DB file");

        assert!(test_result.is_ok());
    }

    /// Verify that a list of transaction receipts can be added to a `LmdbReceiptStore`
    ///
    /// 1. Create a new `LmdbReceiptStore` with only one file and set it as the `current_db`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Check that the number of transaction receipts in the store is 10
    #[test]
    fn test_lmdb_receipt_store_add_receipts() {
        let (files, mut path) = get_temp_db_path_and_file(1);

        let test_result = std::panic::catch_unwind(|| {
            let receipt_store =
                LmdbReceiptStore::new(&path.as_path(), &[files[0].clone()], files[0].clone(), None)
                    .expect("Failed to create LMDB receipt store");

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("failed to add transaction receipts");

            let num_receipts = receipt_store
                .count_txn_receipts()
                .expect("failed to count transaction receipts");

            assert_eq!(num_receipts, 10);
        });

        path.push(files[0].clone());
        std::fs::remove_file(path.as_path()).expect("Failed to remove temp DB file");

        assert!(test_result.is_ok());
    }

    /// Verify that a transaction receipt can be retrieved from the `LmdbReceiptStore`
    /// by id
    ///
    /// 1. Create a new `LmdbReceiptStore` with only one file and set it as the `current_db`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Retrieve the first receipt in the store by id
    /// 4. Check that the fields of the retrieved receipt contain the expected values
    /// 5. Retrieve the second receipt in the store by id
    /// 6. Check that the fields of the retrieved receipt contain the expected values
    #[test]
    fn test_lmdb_receipt_store_get_receipt_by_id() {
        let (files, mut path) = get_temp_db_path_and_file(1);

        let test_result = std::panic::catch_unwind(|| {
            let receipt_store =
                LmdbReceiptStore::new(&path.as_path(), &[files[0].clone()], files[0].clone(), None)
                    .expect("Failed to create LMDB receipt store");

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("failed to add transaction receipts");

            let first_receipt = receipt_store
                .get_txn_receipt_by_id("0".to_string())
                .expect("failed to get transaction receipt with id 0");

            match first_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => assert_eq!(
                    events[0].attributes[0],
                    ("a0".to_string(), "b0".to_string())
                ),
                _ => panic!("transaction result should be valid"),
            }

            let second_receipt = receipt_store
                .get_txn_receipt_by_id("1".to_string())
                .expect("failed to get transaction receipt with id 0");

            match second_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => assert_eq!(
                    events[0].attributes[0],
                    ("a1".to_string(), "b1".to_string())
                ),
                _ => panic!("transaction result should be valid"),
            }
        });

        path.push(files[0].clone());
        std::fs::remove_file(path.as_path()).expect("Failed to remove temp DB file");

        assert!(test_result.is_ok());
    }

    /// Verify that a transaction receipt can be retrieved from the `LmdbReceiptStore`
    /// by index
    ///
    /// 1. Create a new `LmdbReceiptStore` with only one file and set it as the `current_db`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Retrieve the first receipt in the store by index
    /// 4. Check that the fields of the retrieved receipt contain the expected values
    /// 5. Retrieve the second receipt in the store by index
    /// 6. Check that the fields of the retrieved receipt contain the expected values
    #[test]
    fn test_lmdb_receipt_store_get_receipt_by_index() {
        let (files, mut path) = get_temp_db_path_and_file(1);

        let test_result = std::panic::catch_unwind(|| {
            let receipt_store =
                LmdbReceiptStore::new(&path.as_path(), &[files[0].clone()], files[0].clone(), None)
                    .expect("Failed to create LMDB receipt store");

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("failed to add transaction receipts");

            let first_receipt = receipt_store
                .get_txn_receipt_by_index(0)
                .expect("failed to get transaction receipt at idndex 0");

            match first_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => assert_eq!(
                    events[0].attributes[0],
                    ("a0".to_string(), "b0".to_string())
                ),
                _ => panic!("transaction result should be valid"),
            }

            let second_receipt = receipt_store
                .get_txn_receipt_by_index(1)
                .expect("failed to get transaction receipt at idndex 0");

            match second_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => assert_eq!(
                    events[0].attributes[0],
                    ("a1".to_string(), "b1".to_string())
                ),
                _ => panic!("transaction result should be valid"),
            }
        });

        path.push(files[0].clone());
        std::fs::remove_file(path.as_path()).expect("Failed to remove temp DB file");

        assert!(test_result.is_ok());
    }

    /// Verify that a transaction receipt can be removed from the `LmdbReceiptStore`
    /// by id
    ///
    /// 1. Create a new `LmdbReceiptStore` with only one file and set it as the `current_db`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Remove the first receipt from the store by id
    /// 4. Check that the fields of the returned receipt contain the expected values
    /// 5. Check that attempting to retrieve the deleted receipt by id returns None
    /// 6. Check that the number of receipts in the database is now 9
    #[test]
    fn test_lmdb_receipt_store_remove_receipt_by_id() {
        let (files, mut path) = get_temp_db_path_and_file(1);

        let test_result = std::panic::catch_unwind(|| {
            let receipt_store =
                LmdbReceiptStore::new(&path.as_path(), &[files[0].clone()], files[0].clone(), None)
                    .expect("Failed to create LMDB receipt store");

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("failed to add transaction receipts");

            let first_receipt = receipt_store
                .remove_txn_receipt_by_id("0".to_string())
                .expect("failed to get transaction receipt with id 0");

            match first_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => assert_eq!(
                    events[0].attributes[0],
                    ("a0".to_string(), "b0".to_string())
                ),
                _ => panic!("transaction result should be valid"),
            }

            assert!(receipt_store
                .get_txn_receipt_by_id("0".to_string())
                .expect("error getting receipt")
                .is_none());

            let num_receipts = receipt_store
                .count_txn_receipts()
                .expect("failed to count transaction receipts");

            assert_eq!(num_receipts, 9);
        });

        path.push(files[0].clone());
        std::fs::remove_file(path.as_path()).expect("Failed to remove temp DB file");

        assert!(test_result.is_ok());
    }

    /// Verify that a transaction receipt can be removed from the `LmdbReceiptStore`
    /// by index
    ///
    /// 1. Create a new `LmdbReceiptStore` with only one file and set it as the `current_db`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Remove the first receipt from the store by index
    /// 4. Check that the fields of the returned receipt contain the expected values
    /// 5. Check that attempting to retrieve the deleted receipt by id returns None
    /// 6. Check that the number of receipts in the database is now 9
    #[test]
    fn test_lmdb_receipt_store_remove_receipt_by_index() {
        let (files, mut path) = get_temp_db_path_and_file(1);

        let test_result = std::panic::catch_unwind(|| {
            let receipt_store =
                LmdbReceiptStore::new(&path.as_path(), &[files[0].clone()], files[0].clone(), None)
                    .expect("Failed to create LMDB receipt store");

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("failed to add transaction receipts");

            let first_receipt = receipt_store
                .remove_txn_receipt_by_index(0)
                .expect("failed to get transaction receipt at idndex 0");

            match first_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => assert_eq!(
                    events[0].attributes[0],
                    ("a0".to_string(), "b0".to_string())
                ),
                _ => panic!("transaction result should be valid"),
            }

            assert!(receipt_store
                .get_txn_receipt_by_index(0)
                .expect("error getting receipt")
                .is_none());

            let num_receipts = receipt_store
                .count_txn_receipts()
                .expect("failed to count transaction receipts");

            assert_eq!(num_receipts, 9);
        });

        path.push(files[0].clone());
        std::fs::remove_file(path.as_path()).expect("Failed to remove temp DB file");

        assert!(test_result.is_ok());
    }

    /// Verify that the total number of transaction receipts in a `LmdbReceiptStore` can
    /// be retrieved
    ///
    /// 1. Create a new `LmdbReceiptStore` with only one file and set it as the `current_db`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Retrieve the total number of transactions in the store
    /// 4. Verify that the number of transactions returned is 10
    #[test]
    fn test_lmdb_receipt_store_count_receipts() {
        let (files, mut path) = get_temp_db_path_and_file(1);

        let test_result = std::panic::catch_unwind(|| {
            let receipt_store =
                LmdbReceiptStore::new(&path.as_path(), &[files[0].clone()], files[0].clone(), None)
                    .expect("Failed to create LMDB receipt store");

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("failed to add transaction receipts");

            let num_receipts = receipt_store
                .count_txn_receipts()
                .expect("failed to count transaction receipts");

            assert_eq!(num_receipts, 10);
        });

        path.push(files[0].clone());
        std::fs::remove_file(path.as_path()).expect("Failed to remove temp DB file");

        assert!(test_result.is_ok());
    }

    /// Verify that all transaction receipts in a `LmdbReceiptStore` can be listed
    ///
    /// 1. Create a new `LmdbReceiptStore` with only one file and set it as the `current_db`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Call `list_receipts_since` on the receipt store, passing in None to indicate all
    ///    receipts should be listed
    /// 4. Check that the receipts are returned in order and that various fields
    ///    contain the expected values
    /// 5. Check that the number of receipts returned is 10
    #[test]
    fn test_lmdb_receipt_store_list_all_receipts() {
        let (files, mut path) = get_temp_db_path_and_file(1);

        let test_result = std::panic::catch_unwind(|| {
            let receipt_store =
                LmdbReceiptStore::new(&path.as_path(), &[files[0].clone()], files[0].clone(), None)
                    .expect("Failed to create LMDB receipt store");

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("failed to add transaction receipts");

            let all_receipts = receipt_store
                .list_receipts_since(None)
                .expect("failed to list all transaction receipts");

            let mut total = 0;
            for (i, receipt) in all_receipts.enumerate() {
                match receipt
                    .expect("failed to get transaction receipt")
                    .transaction_result
                {
                    TransactionResult::Valid { events, .. } => assert_eq!(
                        events[0].attributes[0],
                        (format!("a{}", i), format!("b{}", i))
                    ),
                    _ => panic!("transaction result should be valid"),
                }
                total += 1;
            }
            assert_eq!(total, 10);
        });

        path.push(files[0].clone());
        std::fs::remove_file(path.as_path()).expect("Failed to remove temp DB file");

        assert!(test_result.is_ok());
    }

    /// Verify that all transaction receipts in a `LmdbReceiptStore` added since a specified
    /// receipt can be listed
    ///
    /// 1. Create a new `LmdbReceiptStore` with only one file and set it as the `current_db`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Call `list_receipts_since` on the receipt store, passing in an id to indicate all
    ///    receipts added since that reciept should be listed
    /// 4. Check that the receipts are returned in order and that various fields
    ///    contain the expected values
    /// 5. Check that the number of receipts returned is 7
    #[test]
    fn test_lmdb_receipt_store_list_some_receipts() {
        let (files, mut path) = get_temp_db_path_and_file(1);

        let test_result = std::panic::catch_unwind(|| {
            let receipt_store =
                LmdbReceiptStore::new(&path.as_path(), &[files[0].clone()], files[0].clone(), None)
                    .expect("Failed to create LMDB receipt store");

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("failed to add transaction receipts");

            let all_receipts = receipt_store
                .list_receipts_since(Some("2".to_string()))
                .expect("failed to list all transaction receipts");

            let mut id = 3;
            let mut total = 0;
            for receipt in all_receipts {
                match receipt
                    .expect("failed to get transaction receipt")
                    .transaction_result
                {
                    TransactionResult::Valid { events, .. } => assert_eq!(
                        events[0].attributes[0],
                        (format!("a{}", id), format!("b{}", id))
                    ),
                    _ => panic!("transaction result should be valid"),
                }
                id += 1;
                total += 1;
            }
            assert_eq!(total, 7);
        });

        path.push(files[0].clone());
        std::fs::remove_file(path.as_path()).expect("Failed to remove temp DB file");

        assert!(test_result.is_ok());
    }

    /// Verify that `LmdbReceiptStoreIter` accurately lists all receipts when
    /// the number of receipts in the database is more than ITER_CACHE_SIZE
    ///
    /// 1. Create a new `LmdbReceiptStore` with only one file and set it as the `current_db`
    /// 2. Generate 100 transaction receipts and add them to the receipt store
    /// 3. Call `list_receipts_since` on the receipt store, passing in None to indicate all
    ///    receipts should be listed
    /// 4. Check that the receipts are returned in order and that various fields
    ///    contain the expected values
    /// 5. Check that the number of receipts returned is 100
    #[test]
    fn test_lmdb_receipt_store_list_all_receipts_100() {
        let (files, mut path) = get_temp_db_path_and_file(1);

        let test_result = std::panic::catch_unwind(|| {
            let receipt_store =
                LmdbReceiptStore::new(&path.as_path(), &[files[0].clone()], files[0].clone(), None)
                    .expect("Failed to create LMDB receipt store");

            let txn_receipts = create_txn_receipts(100);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("failed to add transaction receipts");

            let all_receipts = receipt_store
                .list_receipts_since(None)
                .expect("failed to list all transaction receipts");

            let mut total = 0;
            for (i, receipt) in all_receipts.enumerate() {
                match receipt
                    .expect("failed to get transaction receipt")
                    .transaction_result
                {
                    TransactionResult::Valid { events, .. } => assert_eq!(
                        events[0].attributes[0],
                        (format!("a{}", i), format!("b{}", i))
                    ),
                    _ => panic!("transaction result should be valid"),
                }
                total += 1;
            }
            assert_eq!(total, 100);
        });

        path.push(files[0].clone());
        std::fs::remove_file(path.as_path()).expect("Failed to remove temp DB file");

        assert!(test_result.is_ok());
    }

    /// Verify that a `LmdbReceiptStore` with multiple LMDB instances can change the
    /// `current_db` and perform various actions on the receipts
    ///
    /// 1. Create a new `LmdbReceiptStore` with two files and set the second file as the `current_db`
    /// 2. Generate 10 transaction receipts and add them to the second database
    /// 3. Change the `current_db` to be the second database
    /// 4. Generate 10 transaction receipts and add them to the first database
    /// 5. Remove the first receipt from the current database
    /// 6. Check that the fields of the returned receipt contain the expected values
    /// 7. Check that the total number of receipts is now 9
    /// 8. Change the `current_db` to be the second database
    /// 9. Check that the total number of receipts is still 10
    #[test]
    fn test_lmdb_receipt_store_change_current_db_and_modify_receipts() {
        let (files, path) = get_temp_db_path_and_file(2);

        let test_result = std::panic::catch_unwind(|| {
            let mut receipt_store = LmdbReceiptStore::new(
                &path.as_path(),
                &[files[0].clone(), files[1].clone()],
                files[1].clone(),
                Some(1024 * 1024),
            )
            .expect("Failed to create LMDB receipt store");

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("failed to add transaction receipts");

            assert!(receipt_store.set_current_db(files[0].clone()).is_ok());
            assert_eq!(receipt_store.current_db, files[0]);

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("failed to add transaction receipts");

            let first_receipt = receipt_store
                .remove_txn_receipt_by_id("0".to_string())
                .expect("failed to get transaction receipt with id 0");

            match first_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => assert_eq!(
                    events[0].attributes[0],
                    ("a0".to_string(), "b0".to_string())
                ),
                _ => panic!("transaction result should be valid"),
            }

            let num_receipts = receipt_store
                .count_txn_receipts()
                .expect("failed to count transaction receipts");

            assert_eq!(num_receipts, 9);

            assert!(receipt_store.set_current_db(files[1].clone()).is_ok());
            assert_eq!(receipt_store.current_db, files[1]);

            let num_receipts = receipt_store
                .count_txn_receipts()
                .expect("failed to count transaction receipts");

            assert_eq!(num_receipts, 10);
        });

        for i in 0..files.len() {
            let mut temp_path = path.clone();
            temp_path.push(files[i].clone());
            std::fs::remove_file(temp_path.as_path()).expect("Failed to remove temp DB file");
        }

        assert!(test_result.is_ok());
    }

    fn get_temp_db_path_and_file(number_of_files: u8) -> (Vec<String>, std::path::PathBuf) {
        let temp_db_path = std::env::temp_dir();
        let thread_id = std::thread::current().id();

        let mut file_names = Vec::new();

        for i in 0..number_of_files {
            let file_name = format!("store-{:?}-{}.lmdb", thread_id, i);
            file_names.push(file_name);
        }
        (file_names, temp_db_path)
    }

    fn create_txn_receipts(num_receipts: u8) -> Vec<TransactionReceipt> {
        let mut receipts = Vec::new();

        for i in 0..num_receipts as u8 {
            let event = Event {
                event_type: "event".to_string(),
                attributes: vec![(format!("a{}", i), format!("b{}", i))],
                data: "data".to_string().into_bytes(),
            };
            let state_change = StateChange::Set {
                key: i.to_string(),
                value: i.to_string().into_bytes(),
            };
            let txn_result = TransactionResult::Valid {
                state_changes: vec![state_change],
                events: vec![event],
                data: vec!["data".to_string().into_bytes()],
            };
            let receipt = TransactionReceipt {
                transaction_id: i.to_string(),
                transaction_result: txn_result,
            };
            receipts.push(receipt);
        }
        receipts
    }
}
