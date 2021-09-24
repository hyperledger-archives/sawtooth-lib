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

//! Provides database operations for the `DieselReceiptStore`.

pub(super) mod add_txn_receipts;
pub(super) mod count_txn_receipts;
pub(super) mod get_txn_receipt_by_id;
pub(super) mod get_txn_receipt_by_index;
pub(super) mod list_receipts_since;
pub(super) mod remove_txn_receipt_by_id;
pub(super) mod remove_txn_receipt_by_index;

pub struct ReceiptStoreOperations<'a, C> {
    conn: &'a C,
    service_id: Option<String>,
}

impl<'a, C: diesel::Connection> ReceiptStoreOperations<'a, C> {
    pub fn new(conn: &'a C, service_id: Option<String>) -> Self {
        ReceiptStoreOperations { conn, service_id }
    }
}
