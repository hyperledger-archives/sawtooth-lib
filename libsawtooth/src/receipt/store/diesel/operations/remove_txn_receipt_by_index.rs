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

//! Provides the remove transaction receipt by index operation for `DieselReceiptStore`

use diesel::sql_types::Text;

use crate::error::InvalidStateError;
use crate::transact::protocol::receipt::TransactionReceipt;

use super::{
    get_txn_receipt_by_index::ReceiptStoreGetTxnReceiptByIndexOperation,
    remove_txn_receipt_by_id::ReceiptStoreRemoveTxnReceiptByIdOperation, ReceiptStoreOperations,
};

use crate::receipt::store::ReceiptStoreError;

pub(in crate::receipt::store::diesel) trait ReceiptStoreRemoveTxnReceiptByIndexOperation {
    fn remove_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError>;
}

impl<'a, 's, C> ReceiptStoreRemoveTxnReceiptByIndexOperation for ReceiptStoreOperations<'a, 's, C>
where
    C: diesel::Connection,
    String: diesel::deserialize::FromSql<Text, C::Backend>,
    i64: diesel::deserialize::FromSql<diesel::sql_types::BigInt, C::Backend>,
    i32: diesel::deserialize::FromSql<diesel::sql_types::Integer, C::Backend>,
    i16: diesel::deserialize::FromSql<diesel::sql_types::SmallInt, C::Backend>,
    Vec<u8>: diesel::deserialize::FromSql<diesel::sql_types::Binary, C::Backend>,
{
    fn remove_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        self.conn
            .transaction::<Option<TransactionReceipt>, _, _>(|| {
                let id = self
                    .get_txn_receipt_by_index(index)?
                    .ok_or_else(|| {
                        ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(
                            format!("A transaction receipt with index {} does not exist", index),
                        ))
                    })?
                    .transaction_id;
                self.remove_txn_receipt_by_id(id)
            })
    }
}
