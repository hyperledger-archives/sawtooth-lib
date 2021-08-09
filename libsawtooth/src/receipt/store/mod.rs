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

//! Data store for transaction receipts.

#[cfg(feature = "diesel")]
pub mod diesel;
mod error;
#[cfg(feature = "transaction-receipt-store-lmdb")]
pub mod lmdb;

use transact::protocol::receipt::TransactionReceipt;

pub use error::ReceiptStoreError;

/// Interface for performing CRUD operations on transaction receipts
pub trait ReceiptStore: Sync + Send {
    /// Retrieves the receipt with the given ID from underlying storage
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the transaction receipt to be retrieved
    fn get_txn_receipt_by_id(
        &self,
        id: String,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError>;

    /// Retrieves the receipt at the given index from underlying storage
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the transaction receipt to be retrieved
    fn get_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError>;

    /// Adds transaction receipts to the underlying storage
    ///
    /// # Arguments
    ///
    /// * `receipts` - A vector of the transaction receipts to be added
    fn add_txn_receipts(&self, receipts: Vec<TransactionReceipt>) -> Result<(), ReceiptStoreError>;

    /// Removes the transaction receipt with the given ID from underlying storage
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the transaction receipt to be removed
    fn remove_txn_receipt_by_id(
        &self,
        id: String,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError>;

    /// Removes the transaction receipt at the given index from underlying storage
    ///
    /// # Arguments
    ///
    ///  * `index` - The index of the transaction receipt to be removed
    fn remove_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError>;

    /// Gets the total number of transaction receipts
    fn count_txn_receipts(&self) -> Result<u64, ReceiptStoreError>;

    /// List transaction receipts that have been added to the store since the
    /// provided ID
    ///
    /// # Arguments
    ///
    /// * `id` - The transaction ID of the receipt preceding the receipts to be
    ///          listed, if no id is provided all receipts are listed
    fn list_receipts_since(
        &self,
        id: Option<String>,
    ) -> Result<Box<dyn Iterator<Item = TransactionReceipt>>, ReceiptStoreError>;
}
