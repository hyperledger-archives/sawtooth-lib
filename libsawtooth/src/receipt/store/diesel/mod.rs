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

//! A [`Diesel`](https://crates.io/crates/diesel) backend for [`ReceiptStore`].
//!
//! This module contains the [`DieselReceiptStore`], which provides an implementation
//! of the [`ReceiptStore`] trait.
//!
//! [`DieselReceiptStore`]: struct.DieselReceiptStore.html
//! [`ReceiptStore`]: trait.ReceiptStore.html

pub mod models;
mod operations;
pub mod schema;

use diesel::r2d2::{ConnectionManager, Pool};
use transact::protocol::receipt::TransactionReceipt;

use crate::receipt::store::{error::ReceiptStoreError, ReceiptIter, ReceiptStore};

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
    service_id: Option<String>,
}

impl<C: diesel::Connection> DieselReceiptStore<C> {
    /// Creates a new `DieselReceiptStore`.
    ///
    /// # Arguments
    ///
    ///  * `connection_pool`: connection pool for the database
    pub fn new(connection_pool: Pool<ConnectionManager<C>>, service_id: Option<String>) -> Self {
        DieselReceiptStore {
            connection_pool,
            service_id,
        }
    }
}

#[cfg(feature = "sqlite")]
impl Clone for DieselReceiptStore<diesel::sqlite::SqliteConnection> {
    fn clone(&self) -> Self {
        Self {
            connection_pool: self.connection_pool.clone(),
            service_id: self.service_id.clone(),
        }
    }
}

#[cfg(feature = "postgres")]
impl Clone for DieselReceiptStore<diesel::pg::PgConnection> {
    fn clone(&self) -> Self {
        Self {
            connection_pool: self.connection_pool.clone(),
            service_id: self.service_id.clone(),
        }
    }
}

#[cfg(feature = "sqlite")]
impl ReceiptStore for DieselReceiptStore<diesel::sqlite::SqliteConnection> {
    fn get_txn_receipt_by_id(
        &self,
        id: String,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .get_txn_receipt_by_id(&id)
    }

    fn get_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .get_txn_receipt_by_index(index)
    }

    fn add_txn_receipts(&self, receipts: Vec<TransactionReceipt>) -> Result<(), ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .add_txn_receipts(receipts)
    }

    fn remove_txn_receipt_by_id(
        &self,
        id: String,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .remove_txn_receipt_by_id(id)
    }

    fn remove_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .remove_txn_receipt_by_index(index)
    }

    fn count_txn_receipts(&self) -> Result<u64, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .count_txn_receipts()
    }

    fn list_receipts_since(&self, id: Option<String>) -> Result<ReceiptIter, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .list_receipts_since(id)
    }
}

#[cfg(feature = "postgres")]
impl ReceiptStore for DieselReceiptStore<diesel::pg::PgConnection> {
    fn get_txn_receipt_by_id(
        &self,
        id: String,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .get_txn_receipt_by_id(&id)
    }

    fn get_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .get_txn_receipt_by_index(index)
    }

    fn add_txn_receipts(&self, receipts: Vec<TransactionReceipt>) -> Result<(), ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .add_txn_receipts(receipts)
    }

    fn remove_txn_receipt_by_id(
        &self,
        id: String,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .remove_txn_receipt_by_id(id)
    }

    fn remove_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .remove_txn_receipt_by_index(index)
    }

    fn count_txn_receipts(&self) -> Result<u64, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .count_txn_receipts()
    }

    fn list_receipts_since(&self, id: Option<String>) -> Result<ReceiptIter, ReceiptStoreError> {
        ReceiptStoreOperations::new(&*self.connection_pool.get()?, self.service_id.clone())
            .list_receipts_since(id)
    }
}

#[cfg(all(test, feature = "sqlite"))]
pub mod tests {
    use super::*;

    use transact::protocol::receipt::{Event, StateChange, TransactionResult};

    use crate::migrations::run_sqlite_migrations;

    use diesel::{
        r2d2::{ConnectionManager, Pool},
        sqlite::SqliteConnection,
    };

    #[test]
    /// Test that the ReceiptStore sqlite migrations can be run successfully
    fn test_sqlite_migrations() {
        create_connection_pool_and_migrate();
    }

    /// Verify that a list of transaction receipts can be added to a SQLite `DieselReceiptStore`
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Check that the number of transaction receipts in the store is 10
    #[test]
    fn test_sqlite_add_receipts() {
        let test_result = std::panic::catch_unwind(|| {
            let pool = create_connection_pool_and_migrate();

            let receipt_store =
                DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("Unable to add receipts");

            let num_receipts = receipt_store
                .count_txn_receipts()
                .expect("failed to count transaction receipts");

            assert_eq!(num_receipts, 10);
        });

        assert!(test_result.is_ok());
    }

    /// Verify that a transaction receipt can be retrieved from the SQLite `DieselReceiptStore`
    /// by id
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Retrieve the first receipt in the store by id
    /// 4. Check that the fields of the retrieved receipt contain the expected values
    ///    and are in the expected order
    /// 5. Retrieve the second receipt in the store by id
    /// 6. Check that the fields of the retrieved receipt contain the expected values
    ///    and are in the expected order
    #[test]
    fn test_sqlite_get_receipt_by_id() {
        let test_result = std::panic::catch_unwind(|| {
            let pool = create_connection_pool_and_migrate();

            let receipt_store =
                DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("Unable to add receipts");

            let first_receipt = receipt_store
                .get_txn_receipt_by_id("0".to_string())
                .expect("failed to get transaction receipt with id 0");

            match first_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => {
                    assert_eq!(
                        events[0].attributes[0],
                        ("a0".to_string(), "b0".to_string())
                    );
                    assert_eq!(
                        events[0].attributes[1],
                        ("c0".to_string(), "d0".to_string())
                    );
                    assert_eq!(
                        events[1].attributes[0],
                        ("e0".to_string(), "f0".to_string())
                    );
                    assert_eq!(
                        events[1].attributes[1],
                        ("g0".to_string(), "h0".to_string())
                    );
                }
                _ => panic!("transaction result should be valid"),
            }

            let second_receipt = receipt_store
                .get_txn_receipt_by_id("1".to_string())
                .expect("failed to get transaction receipt with id 0");

            match second_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => {
                    assert_eq!(
                        events[0].attributes[0],
                        ("a1".to_string(), "b1".to_string())
                    );
                    assert_eq!(
                        events[0].attributes[1],
                        ("c1".to_string(), "d1".to_string())
                    );
                    assert_eq!(
                        events[1].attributes[0],
                        ("e1".to_string(), "f1".to_string())
                    );
                    assert_eq!(
                        events[1].attributes[1],
                        ("g1".to_string(), "h1".to_string())
                    );
                }
                _ => panic!("transaction result should be valid"),
            }
        });

        assert!(test_result.is_ok());
    }

    /// Verify that a transaction receipt can be retrieved from the SQLite `DieselReceiptStore`
    /// by index
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Retrieve the first receipt in the store by index
    /// 4. Check that the fields of the retrieved receipt contain the expected values
    ///    and are in the expected order
    /// 5. Retrieve the second receipt in the store by index
    /// 6. Check that the fields of the retrieved receipt contain the expected values
    ///    and are in the expected order
    #[test]
    fn test_sqlite_get_receipt_by_index() {
        let test_result = std::panic::catch_unwind(|| {
            let pool = create_connection_pool_and_migrate();

            let receipt_store =
                DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("Unable to add receipts");

            let first_receipt = receipt_store
                .get_txn_receipt_by_index(1)
                .expect("failed to get transaction receipt at index 1");

            match first_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => {
                    assert_eq!(
                        events[0].attributes[0],
                        ("a0".to_string(), "b0".to_string())
                    );
                    assert_eq!(
                        events[0].attributes[1],
                        ("c0".to_string(), "d0".to_string())
                    );
                    assert_eq!(
                        events[1].attributes[0],
                        ("e0".to_string(), "f0".to_string())
                    );
                    assert_eq!(
                        events[1].attributes[1],
                        ("g0".to_string(), "h0".to_string())
                    );
                }
                _ => panic!("transaction result should be valid"),
            }

            let second_receipt = receipt_store
                .get_txn_receipt_by_index(2)
                .expect("failed to get transaction receipt at index 2");

            match second_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => {
                    assert_eq!(
                        events[0].attributes[0],
                        ("a1".to_string(), "b1".to_string())
                    );
                    assert_eq!(
                        events[0].attributes[1],
                        ("c1".to_string(), "d1".to_string())
                    );
                    assert_eq!(
                        events[1].attributes[0],
                        ("e1".to_string(), "f1".to_string())
                    );
                    assert_eq!(
                        events[1].attributes[1],
                        ("g1".to_string(), "h1".to_string())
                    );
                }
                _ => panic!("transaction result should be valid"),
            }
        });

        assert!(test_result.is_ok());
    }

    /// Verify that the total number of transaction receipts in a SQLite `DieselReceiptStore`
    /// can be retrieved
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Retrieve the total number of transactions in the store
    /// 4. Verify that the number of transactions returned is 10
    #[test]
    fn test_sqlite_count_receipts() {
        let test_result = std::panic::catch_unwind(|| {
            let pool = create_connection_pool_and_migrate();

            let receipt_store =
                DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("Unable to add receipts");

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

        assert!(test_result.is_ok());
    }

    /// Verify that all transaction receipts in a SQLite `DieselReceiptStore` can be listed
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Call `list_receipts_since` on the receipt store, passing in None to indicate all
    ///    receipts should be listed
    /// 4. Check that the receipts are returned in order and that various fields
    ///    contain the expected values
    /// 5. Check that the number of receipts returned is 10
    #[test]
    fn test_sqlite_list_all_receipts() {
        let test_result = std::panic::catch_unwind(|| {
            let pool = create_connection_pool_and_migrate();

            let receipt_store =
                DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("Unable to add receipts");

            let all_receipts = receipt_store
                .list_receipts_since(None)
                .expect("failed to list all transaction receipts");

            let mut total = 0;
            for (i, receipt) in all_receipts.enumerate() {
                match receipt
                    .expect("failed to get transaction receipt")
                    .transaction_result
                {
                    TransactionResult::Valid { events, .. } => {
                        assert_eq!(
                            events[0].attributes[0],
                            (format!("a{}", i), format!("b{}", i))
                        );
                        assert_eq!(
                            events[0].attributes[1],
                            (format!("c{}", i), format!("d{}", i))
                        );
                        assert_eq!(
                            events[1].attributes[0],
                            (format!("e{}", i), format!("f{}", i))
                        );
                        assert_eq!(
                            events[1].attributes[1],
                            (format!("g{}", i), format!("h{}", i))
                        );
                    }
                    _ => panic!("transaction result should be valid"),
                }
                total += 1;
            }
            assert_eq!(total, 10);
        });

        assert!(test_result.is_ok());
    }

    /// Verify that all transaction receipts in a SQLite `DieselReceiptStore`
    /// added since a specified receipt can be listed
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Call `list_receipts_since` on the receipt store, passing in an id to indicate all
    ///    receipts added since that receipt should be listed
    /// 4. Check that the receipts are returned in order and that various fields
    ///    contain the expected values
    /// 5. Check that the number of receipts returned is 7
    #[test]
    fn test_sqlite_list_receipts_since() {
        let test_result = std::panic::catch_unwind(|| {
            let pool = create_connection_pool_and_migrate();

            let receipt_store =
                DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("Unable to add receipts");

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
                    TransactionResult::Valid { events, .. } => {
                        assert_eq!(
                            events[0].attributes[0],
                            (format!("a{}", id), format!("b{}", id))
                        );
                        assert_eq!(
                            events[0].attributes[1],
                            (format!("c{}", id), format!("d{}", id))
                        );
                        assert_eq!(
                            events[1].attributes[0],
                            (format!("e{}", id), format!("f{}", id))
                        );
                        assert_eq!(
                            events[1].attributes[1],
                            (format!("g{}", id), format!("h{}", id))
                        );
                    }
                    _ => panic!("transaction result should be valid"),
                }
                id += 1;
                total += 1;
            }
            assert_eq!(total, 7);
        });

        assert!(test_result.is_ok());
    }

    /// Verify that a transaction receipt can be removed from the SQLite `DieselReceiptStore`
    /// by id
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Remove the first receipt from the store by id
    /// 4. Check that the fields of the returned receipt contain the expected values
    /// 5. Check that attempting to retrieve the deleted receipt by id returns None
    /// 6. Check that the number of receipts in the database is now 9
    #[test]
    fn test_sqlite_remove_receipt_by_id() {
        let test_result = std::panic::catch_unwind(|| {
            let pool = create_connection_pool_and_migrate();

            let receipt_store =
                DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("Unable to add receipts");

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

        assert!(test_result.is_ok());
    }

    /// Verify that a transaction receipt can be removed from the SQLite `DieselReceiptStore`
    /// by index
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 10 transaction receipts and add them to the receipt store
    /// 3. Remove the first receipt from the store by index
    /// 4. Check that the fields of the returned receipt contain the expected values
    /// 5. Check that attempting to retrieve the deleted receipt by index returns None
    /// 6. Check that the number of receipts in the database is now 9
    #[test]
    fn test_sqlite_remove_receipt_by_index() {
        let test_result = std::panic::catch_unwind(|| {
            let pool = create_connection_pool_and_migrate();

            let receipt_store =
                DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

            let txn_receipts = create_txn_receipts(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("Unable to add receipts");

            let first_receipt = receipt_store
                .remove_txn_receipt_by_index(1)
                .expect("failed to get transaction receipt at index 1");

            match first_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => assert_eq!(
                    events[0].attributes[0],
                    ("a0".to_string(), "b0".to_string())
                ),
                _ => panic!("transaction result should be valid"),
            }

            assert!(receipt_store
                .get_txn_receipt_by_index(1)
                .expect("error getting receipt")
                .is_none());

            let num_receipts = receipt_store
                .count_txn_receipts()
                .expect("failed to count transaction receipts");

            assert_eq!(num_receipts, 9);
        });

        assert!(test_result.is_ok());
    }

    /// Verify that transaction receipts that don't have any event_attributes
    /// can be added to and retrieved from the SQLite `DieselReceiptStore`
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 10 transaction receipts, some with no event attributes, and add them
    ///    to the receipt store
    /// 3. Retrieve the first receipt in the store by index
    /// 4. Check that the fields of the retrieved receipt contain the expected values
    ///    and are in the expected order
    /// 5. Retrieve the second receipt in the store by index
    /// 6. Check that the fields of the retrieved receipt contain the expected values
    ///    and are in the expected order
    #[test]
    fn test_sqlite_get_receipt_no_event_attributes() {
        let test_result = std::panic::catch_unwind(|| {
            let pool = create_connection_pool_and_migrate();

            let receipt_store =
                DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

            let mut receipts = Vec::new();

            for i in 0..10 as u8 {
                let event1 = Event {
                    event_type: "event".to_string(),
                    attributes: vec![],
                    data: "data".to_string().into_bytes(),
                };
                let event2 = Event {
                    event_type: "event".to_string(),
                    attributes: vec![
                        (format!("e{}", i), format!("f{}", i)),
                        (format!("g{}", i), format!("h{}", i)),
                    ],
                    data: "data".to_string().into_bytes(),
                };
                let state_change1 = StateChange::Set {
                    key: i.to_string(),
                    value: i.to_string().into_bytes(),
                };
                let state_change2 = StateChange::Set {
                    key: i.to_string(),
                    value: format!("value{}", i).into_bytes(),
                };
                let txn_result = TransactionResult::Valid {
                    state_changes: vec![state_change1, state_change2],
                    events: vec![event1, event2],
                    data: vec!["data".to_string().into_bytes()],
                };
                let receipt = TransactionReceipt {
                    transaction_id: i.to_string(),
                    transaction_result: txn_result,
                };
                receipts.push(receipt);
            }

            receipt_store
                .add_txn_receipts(receipts)
                .expect("Unable to add receipts");

            let first_receipt = receipt_store
                .get_txn_receipt_by_index(1)
                .expect("failed to get transaction receipt at index 1");

            match first_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => {
                    assert!(events[0].attributes.is_empty());
                    assert_eq!(
                        events[1].attributes[0],
                        ("e0".to_string(), "f0".to_string())
                    );
                    assert_eq!(
                        events[1].attributes[1],
                        ("g0".to_string(), "h0".to_string())
                    );
                }
                _ => panic!("transaction result should be valid"),
            }

            let second_receipt = receipt_store
                .get_txn_receipt_by_index(2)
                .expect("failed to get transaction receipt at index 2");

            match second_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => {
                    assert!(events[0].attributes.is_empty());
                    assert_eq!(
                        events[1].attributes[0],
                        ("e1".to_string(), "f1".to_string())
                    );
                    assert_eq!(
                        events[1].attributes[1],
                        ("g1".to_string(), "h1".to_string())
                    );
                }
                _ => panic!("transaction result should be valid"),
            }
        });

        assert!(test_result.is_ok());
    }

    /// Verify that transaction receipts that don't have any associated events
    /// can be added to and retrieved from the SQLite `DieselReceiptStore`
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 10 transaction receipts with no events and add them
    ///    to the receipt store
    /// 3. Retrieve the first receipt in the store by index
    /// 4. Check that the fields of the retrieved receipt contain the expected values
    ///    and are in the expected order
    /// 5. Retrieve the second receipt in the store by index
    /// 6. Check that the fields of the retrieved receipt contain the expected values
    ///    and are in the expected order
    #[test]
    fn test_sqlite_get_receipt_no_events() {
        let test_result = std::panic::catch_unwind(|| {
            let pool = create_connection_pool_and_migrate();

            let receipt_store =
                DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

            let mut receipts = Vec::new();

            for i in 0..10 as u8 {
                let state_change1 = StateChange::Set {
                    key: i.to_string(),
                    value: i.to_string().into_bytes(),
                };
                let state_change2 = StateChange::Set {
                    key: i.to_string(),
                    value: format!("value{}", i).into_bytes(),
                };
                let txn_result = TransactionResult::Valid {
                    state_changes: vec![state_change1, state_change2],
                    events: vec![],
                    data: vec!["data".to_string().into_bytes()],
                };
                let receipt = TransactionReceipt {
                    transaction_id: i.to_string(),
                    transaction_result: txn_result,
                };
                receipts.push(receipt);
            }

            receipt_store
                .add_txn_receipts(receipts)
                .expect("Unable to add receipts");

            let first_receipt = receipt_store
                .get_txn_receipt_by_index(1)
                .expect("failed to get transaction receipt at index 1");

            match first_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => {
                    assert!(events.is_empty());
                }
                _ => panic!("transaction result should be valid"),
            }

            let second_receipt = receipt_store
                .get_txn_receipt_by_index(2)
                .expect("failed to get transaction receipt at index 2");

            match second_receipt.unwrap().transaction_result {
                TransactionResult::Valid { events, .. } => {
                    assert!(events.is_empty());
                }
                _ => panic!("transaction result should be valid"),
            }
        });

        assert!(test_result.is_ok());
    }

    /// Verify that transaction receipts that don't have any associated state changes
    /// can be added to and retrieved from the SQLite `DieselReceiptStore`
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 10 transaction receipts with no state changes and add them
    ///    to the receipt store
    /// 3. Retrieve the first receipt in the store by index
    /// 4. Check that the fields of the retrieved receipt contain the expected values
    ///    and are in the expected order
    /// 5. Retrieve the second receipt in the store by index
    /// 6. Check that the fields of the retrieved receipt contain the expected values
    ///    and are in the expected order
    #[test]
    fn test_sqlite_get_receipt_no_state_changes() {
        let test_result = std::panic::catch_unwind(|| {
            let pool = create_connection_pool_and_migrate();

            let receipt_store =
                DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

            let mut receipts = Vec::new();

            for i in 0..10 as u8 {
                let event1 = Event {
                    event_type: "event".to_string(),
                    attributes: vec![],
                    data: "data".to_string().into_bytes(),
                };
                let event2 = Event {
                    event_type: "event".to_string(),
                    attributes: vec![
                        (format!("e{}", i), format!("f{}", i)),
                        (format!("g{}", i), format!("h{}", i)),
                    ],
                    data: "data".to_string().into_bytes(),
                };
                let txn_result = TransactionResult::Valid {
                    state_changes: vec![],
                    events: vec![event1, event2],
                    data: vec!["data".to_string().into_bytes()],
                };
                let receipt = TransactionReceipt {
                    transaction_id: i.to_string(),
                    transaction_result: txn_result,
                };
                receipts.push(receipt);
            }

            receipt_store
                .add_txn_receipts(receipts)
                .expect("Unable to add receipts");

            let first_receipt = receipt_store
                .get_txn_receipt_by_index(1)
                .expect("failed to get transaction receipt at index 1");

            match first_receipt.unwrap().transaction_result {
                TransactionResult::Valid { state_changes, .. } => {
                    assert!(state_changes.is_empty());
                }
                _ => panic!("transaction result should be valid"),
            }

            let second_receipt = receipt_store
                .get_txn_receipt_by_index(2)
                .expect("failed to get transaction receipt at index 2");

            match second_receipt.unwrap().transaction_result {
                TransactionResult::Valid { state_changes, .. } => {
                    assert!(state_changes.is_empty());
                }
                _ => panic!("transaction result should be valid"),
            }
        });

        assert!(test_result.is_ok());
    }

    /// Verify that all transaction receipts in a SQLite `DieselReceiptStore` can be listed
    /// when given a list of receipts with varying transaction results
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 10 transaction receipts with varying results and add them to the receipt store
    /// 3. Call `list_receipts_since` on the receipt store, passing in None to indicate all
    ///    receipts should be listed
    /// 4. Check that the receipts are returned in order and that various fields
    ///    contain the expected values
    /// 5. Check that the number of receipts returned is 10
    #[test]
    fn test_sqlite_list_all_receipts_varying_results() {
        let test_result = std::panic::catch_unwind(|| {
            let pool = create_connection_pool_and_migrate();

            let receipt_store =
                DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

            let txn_receipts = create_txn_receipts_mixed_results(10);

            receipt_store
                .add_txn_receipts(txn_receipts)
                .expect("Unable to add receipts");

            let all_receipts = receipt_store
                .list_receipts_since(None)
                .expect("failed to list all transaction receipts");

            let mut total = 0;
            for (i, receipt) in all_receipts.enumerate() {
                if i % 2 == 0 {
                    match receipt
                        .expect("failed to get transaction receipt")
                        .transaction_result
                    {
                        TransactionResult::Valid { events, .. } => {
                            assert_eq!(
                                events[0].attributes[0],
                                (format!("a{}", i), format!("b{}", i))
                            );
                            assert_eq!(
                                events[0].attributes[1],
                                (format!("c{}", i), format!("d{}", i))
                            );
                            assert_eq!(
                                events[1].attributes[0],
                                (format!("e{}", i), format!("f{}", i))
                            );
                            assert_eq!(
                                events[1].attributes[1],
                                (format!("g{}", i), format!("h{}", i))
                            );
                        }
                        _ => panic!("transaction result should be valid"),
                    }
                } else {
                    match receipt
                        .expect("failed to get transaction receipt")
                        .transaction_result
                    {
                        TransactionResult::Invalid {
                            error_message,
                            error_data,
                        } => {
                            assert_eq!(
                                error_message,
                                format!("an error occurred in transaction: {}", i)
                            );
                            assert_eq!(error_data, "error_data".to_string().into_bytes());
                        }
                        _ => panic!("transaction result should be invalid"),
                    }
                }
                total += 1;
            }
            assert_eq!(total, 10);
        });

        assert!(test_result.is_ok());
    }

    /// Verify that all transaction receipts in a SQLite `DieselReceiptStore` are returned
    /// in the correct order when added to the database one at a time
    ///
    /// 1. Create a new `DieselReceiptStore`
    /// 2. Generate 20 transaction receipts and individually add the first 10 to the receipt store
    /// 3. Add the remaining 10 transaction receipts at the same time in a vector
    /// 4. Call `list_receipts_since` on the receipt store, passing in None to indicate all
    ///    receipts should be listed
    /// 5. Check that the receipts are returned in order and that various fields
    ///    contain the expected values
    /// 6. Check that the number of receipts returned is 20
    #[test]
    fn test_sqlite_list_receipts_order() {
        let pool = create_connection_pool_and_migrate();

        let receipt_store = DieselReceiptStore::new(pool, Some("ABCDE-12345::AAaa".to_string()));

        let txn_receipts = create_txn_receipts(20);

        for r in txn_receipts[0..10].to_vec() {
            receipt_store
                .add_txn_receipts(vec![r])
                .expect("Unable to add individual receipt");
        }

        receipt_store
            .add_txn_receipts(txn_receipts[10..].to_vec())
            .expect("Unable to add remaining 10 receipts");

        let all_receipts = receipt_store
            .list_receipts_since(None)
            .expect("failed to list all transaction receipts");

        let mut total = 0;
        for (i, receipt) in all_receipts.enumerate() {
            match receipt
                .expect("failed to get transaction receipt")
                .transaction_result
            {
                TransactionResult::Valid { events, .. } => {
                    assert_eq!(
                        events[0].attributes[0],
                        (format!("a{}", i), format!("b{}", i))
                    );
                    assert_eq!(
                        events[0].attributes[1],
                        (format!("c{}", i), format!("d{}", i))
                    );
                    assert_eq!(
                        events[1].attributes[0],
                        (format!("e{}", i), format!("f{}", i))
                    );
                    assert_eq!(
                        events[1].attributes[1],
                        (format!("g{}", i), format!("h{}", i))
                    );
                }
                _ => panic!("transaction result should be valid"),
            }
            total += 1;
        }
        assert_eq!(total, 20);
    }

    /// Creates a connection pool for an in-memory SQLite database with only a single connection
    /// available. Each connection is backed by a different in-memory SQLite database, so limiting
    /// the pool to a single connection ensures that the same DB is used for all operations.
    fn create_connection_pool_and_migrate() -> Pool<ConnectionManager<SqliteConnection>> {
        let connection_manager = ConnectionManager::<SqliteConnection>::new(":memory:");
        let pool = Pool::builder()
            .max_size(1)
            .build(connection_manager)
            .expect("Failed to build connection pool");

        run_sqlite_migrations(&*pool.get().expect("Failed to get connection for migrations"))
            .expect("Failed to run migrations");

        pool
    }

    fn create_txn_receipts(num_receipts: u8) -> Vec<TransactionReceipt> {
        let mut receipts = Vec::new();

        for i in 0..num_receipts as u8 {
            let event1 = Event {
                event_type: "event".to_string(),
                attributes: vec![
                    (format!("a{}", i), format!("b{}", i)),
                    (format!("c{}", i), format!("d{}", i)),
                ],
                data: "data".to_string().into_bytes(),
            };
            let event2 = Event {
                event_type: "event".to_string(),
                attributes: vec![
                    (format!("e{}", i), format!("f{}", i)),
                    (format!("g{}", i), format!("h{}", i)),
                ],
                data: "data".to_string().into_bytes(),
            };
            let state_change1 = StateChange::Set {
                key: i.to_string(),
                value: i.to_string().into_bytes(),
            };
            let state_change2 = StateChange::Set {
                key: i.to_string(),
                value: format!("value{}", i).into_bytes(),
            };
            let txn_result = TransactionResult::Valid {
                state_changes: vec![state_change1, state_change2],
                events: vec![event1, event2],
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

    fn create_txn_receipts_mixed_results(num_receipts: u8) -> Vec<TransactionReceipt> {
        let mut receipts = Vec::new();

        for i in 0..num_receipts as u8 {
            if i % 2 == 0 {
                let event1 = Event {
                    event_type: "event".to_string(),
                    attributes: vec![
                        (format!("a{}", i), format!("b{}", i)),
                        (format!("c{}", i), format!("d{}", i)),
                    ],
                    data: "data".to_string().into_bytes(),
                };
                let event2 = Event {
                    event_type: "event".to_string(),
                    attributes: vec![
                        (format!("e{}", i), format!("f{}", i)),
                        (format!("g{}", i), format!("h{}", i)),
                    ],
                    data: "data".to_string().into_bytes(),
                };
                let state_change1 = StateChange::Set {
                    key: i.to_string(),
                    value: i.to_string().into_bytes(),
                };
                let state_change2 = StateChange::Set {
                    key: i.to_string(),
                    value: format!("value{}", i).into_bytes(),
                };
                let txn_result = TransactionResult::Valid {
                    state_changes: vec![state_change1, state_change2],
                    events: vec![event1, event2],
                    data: vec!["data".to_string().into_bytes()],
                };
                let receipt = TransactionReceipt {
                    transaction_id: i.to_string(),
                    transaction_result: txn_result,
                };
                receipts.push(receipt);
            } else {
                let txn_result = TransactionResult::Invalid {
                    error_message: format!("an error occurred in transaction: {}", i),
                    error_data: "error_data".to_string().into_bytes(),
                };
                let receipt = TransactionReceipt {
                    transaction_id: i.to_string(),
                    transaction_result: txn_result,
                };
                receipts.push(receipt);
            }
        }
        receipts
    }
}
