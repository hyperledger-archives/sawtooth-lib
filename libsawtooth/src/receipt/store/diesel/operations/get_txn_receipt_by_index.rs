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

//! Provides the get transaction receipt by index operation for `DieselReceiptStore`

use std::convert::TryFrom;

use diesel::{
    prelude::*,
    sql_types::{Integer, Text},
};
use transact::protocol::receipt::TransactionReceipt;

use crate::error::InternalError;

use super::{
    get_txn_receipt_by_id::ReceiptStoreGetTxnReceiptByIdOperation, ReceiptStoreOperations,
};

use crate::receipt::store::{
    diesel::{models::TransactionReceiptModel, schema::transaction_receipt},
    ReceiptStoreError,
};

pub(in crate::receipt::store::diesel) trait ReceiptStoreGetTxnReceiptByIndexOperation {
    fn get_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError>;
}

impl<'a, 's, C> ReceiptStoreGetTxnReceiptByIndexOperation for ReceiptStoreOperations<'a, 's, C>
where
    C: diesel::Connection,
    String: diesel::deserialize::FromSql<Text, C::Backend>,
    i64: diesel::deserialize::FromSql<diesel::sql_types::BigInt, C::Backend>,
    i32: diesel::deserialize::FromSql<Integer, C::Backend>,
    i16: diesel::deserialize::FromSql<diesel::sql_types::SmallInt, C::Backend>,
    Vec<u8>: diesel::deserialize::FromSql<diesel::sql_types::Binary, C::Backend>,
{
    fn get_txn_receipt_by_index(
        &self,
        index: u64,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        self.conn
            .transaction::<Option<TransactionReceipt>, _, _>(|| {
                let index = i64::try_from(index).map_err(|_| {
                    ReceiptStoreError::InternalError(InternalError::with_message(
                        "Unable to convert index into i64".to_string(),
                    ))
                })?;
                let mut query = transaction_receipt::table.into_boxed();
                if let Some(service_id) = &self.service_id {
                    query = query.filter(transaction_receipt::service_id.eq(service_id));
                };
                let txn_receipt: TransactionReceiptModel = match query
                    .select(transaction_receipt::all_columns)
                    .filter(transaction_receipt::idx.eq(index))
                    .first::<TransactionReceiptModel>(self.conn)
                    .optional()?
                {
                    Some(receipt) => receipt,
                    None => return Ok(None),
                };
                ReceiptStoreOperations::new(self.conn, self.service_id)
                    .get_txn_receipt_by_id(&txn_receipt.transaction_id)
            })
    }
}
