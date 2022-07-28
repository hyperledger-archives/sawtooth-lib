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

//! Provides the remove transaction receipt by id operation for `DieselReceiptStore`

use diesel::{delete, prelude::*, sql_types::Text};

use crate::transact::protocol::receipt::TransactionReceipt;

use super::{
    get_txn_receipt_by_id::ReceiptStoreGetTxnReceiptByIdOperation, ReceiptStoreOperations,
};

use crate::receipt::store::{diesel::schema::transaction_receipt, ReceiptStoreError};

pub(in crate::receipt::store::diesel) trait ReceiptStoreRemoveTxnReceiptByIdOperation {
    fn remove_txn_receipt_by_id(
        &self,
        id: String,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError>;
}

impl<'a, 's, C> ReceiptStoreRemoveTxnReceiptByIdOperation for ReceiptStoreOperations<'a, 's, C>
where
    C: diesel::Connection,
    String: diesel::deserialize::FromSql<Text, C::Backend>,
    i64: diesel::deserialize::FromSql<diesel::sql_types::BigInt, C::Backend>,
    i32: diesel::deserialize::FromSql<diesel::sql_types::Integer, C::Backend>,
    i16: diesel::deserialize::FromSql<diesel::sql_types::SmallInt, C::Backend>,
    Vec<u8>: diesel::deserialize::FromSql<diesel::sql_types::Binary, C::Backend>,
{
    fn remove_txn_receipt_by_id(
        &self,
        id: String,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        self.conn
            .transaction::<Option<TransactionReceipt>, _, _>(|| {
                let rec = self.get_txn_receipt_by_id(&id)?;
                delete(transaction_receipt::table.find(&id)).execute(self.conn)?;
                Ok(rec)
            })
    }
}
