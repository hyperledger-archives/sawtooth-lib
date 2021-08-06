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

// ! A Diesel implementation of `ReceiptStore`.

pub mod models;
mod operations;
pub mod schema;

use diesel::r2d2::{ConnectionManager, Pool};
use transact::protocol::receipt::TransactionReceipt;

use crate::receipt::store::{error::ReceiptStoreError, ReceiptStore};

use operations::add_txn_receipts::ReceiptStoreAddTxnReceiptsOperation as _;
use operations::count_txn_receipts::ReceiptStoreCountTxnReceiptsOperation as _;
use operations::get_txn_receipt_by_id::ReceiptStoreGetTxnReceiptByIdOperation as _;
use operations::get_txn_receipt_by_index::ReceiptStoreGetTxnReceiptByIndexOperation as _;
use operations::list_receipts_since::ReceiptStoreListReceiptsSinceOperation as _;
use operations::remove_txn_receipt_by_id::ReceiptStoreRemoveTxnReceiptByIdOperation as _;
use operations::remove_txn_receipt_by_index::ReceiptStoreRemoveTxnReceiptByIndexOperation as _;
use operations::ReceiptStoreOperations;

/// A database-backed ReceiptStore, powered by [`Diesel`](https://crates.io/crates/diesel).
pub struct DieselReceiptStore<C: diesel::Connection + 'static> {
    connection_pool: Pool<ConnectionManager<C>>,
}

impl<C: diesel::Connection> DieselReceiptStore<C> {
    /// Creates a new `DieselReceiptStore`.
    ///
    /// # Arguments
    ///
    ///  * `connection_pool`: connection pool for the database
    pub fn new(connection_pool: Pool<ConnectionManager<C>>) -> Self {
        DieselReceiptStore { connection_pool }
    }
}

impl Clone for DieselReceiptStore<diesel::sqlite::SqliteConnection> {
    fn clone(&self) -> Self {
        Self {
            connection_pool: self.connection_pool.clone(),
        }
    }
}

impl ReceiptStore for DieselReceiptStore<diesel::sqlite::SqliteConnection> {
    fn get_txn_receipt_by_id(
        &self,
        id: String,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?).get_txn_receipt_by_id(&id)
    }

    fn get_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?).get_txn_receipt_by_index(index)
    }

    fn add_txn_receipts(&self, receipts: Vec<TransactionReceipt>) -> Result<(), ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?).add_txn_receipts(receipts)
    }

    fn remove_txn_receipt_by_id(
        &self,
        id: String,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?).remove_txn_receipt_by_id(id)
    }

    fn remove_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?)
            .remove_txn_receipt_by_index(index)
    }

    fn count_txn_receipts(&self) -> Result<u64, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?).count_txn_receipts()
    }

    fn list_receipts_since(
        &self,
        id: Option<String>,
    ) -> Result<Box<dyn Iterator<Item = TransactionReceipt>>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?).list_receipts_since(id)
    }
}
