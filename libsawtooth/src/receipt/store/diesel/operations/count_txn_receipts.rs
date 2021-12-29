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

//! Provides the count transaction receipts operation for `DieselReceiptStore`

use std::convert::TryFrom;

use diesel::{dsl::count_star, prelude::*, sql_types::Text};

use crate::error::InternalError;

use super::ReceiptStoreOperations;

use crate::receipt::store::{diesel::schema::transaction_receipt, ReceiptStoreError};

pub(in crate::receipt::store::diesel) trait ReceiptStoreCountTxnReceiptsOperation {
    fn count_txn_receipts(&self) -> Result<u64, ReceiptStoreError>;
}

impl<'a, 's, C> ReceiptStoreCountTxnReceiptsOperation for ReceiptStoreOperations<'a, 's, C>
where
    C: diesel::Connection,
    String: diesel::deserialize::FromSql<Text, C::Backend>,
    i64: diesel::deserialize::FromSql<diesel::sql_types::BigInt, C::Backend>,
    i32: diesel::deserialize::FromSql<diesel::sql_types::Integer, C::Backend>,
{
    fn count_txn_receipts(&self) -> Result<u64, ReceiptStoreError> {
        self.conn.transaction::<u64, _, _>(|| {
            let mut query = transaction_receipt::table.into_boxed();
            if let Some(service_id) = &self.service_id {
                query = query.filter(transaction_receipt::service_id.eq(service_id));
            };
            let count = query
                .select(transaction_receipt::all_columns)
                .select(count_star())
                .first::<i64>(self.conn)?;

            u64::try_from(count).map_err(|_| {
                ReceiptStoreError::InternalError(InternalError::with_message(
                    "The total number of transaction receipts is larger than the max u64"
                        .to_string(),
                ))
            })
        })
    }
}
