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

//! Database representations used to implement a diesel backend for the `ReceiptStore`.

use std::convert::TryFrom;
use std::io::Write;

use diesel::{
    backend::Backend,
    deserialize::{self, FromSql},
    expression::{helper_types::AsExprOf, AsExpression},
    serialize::{self, Output, ToSql},
    sql_types::SmallInt,
};
use transact::protocol::receipt::Event;

use crate::error::InternalError;
use crate::receipt::store::diesel::schema::{
    invalid_transaction_result, transaction_receipt, valid_transaction_result_data,
    valid_transaction_result_event, valid_transaction_result_event_attribute,
    valid_transaction_result_state_change,
};
use crate::receipt::store::error::ReceiptStoreError;

#[derive(
    Debug, PartialEq, Eq, Associations, Identifiable, Insertable, Queryable, QueryableByName,
)]
#[table_name = "transaction_receipt"]
#[primary_key(transaction_id)]
pub struct TransactionReceiptModel {
    pub transaction_id: String,
    pub idx: i64,
    pub service_id: Option<String>,
}

#[derive(
    Debug, PartialEq, Eq, Associations, Identifiable, Insertable, Queryable, QueryableByName,
)]
#[table_name = "invalid_transaction_result"]
#[belongs_to(TransactionReceiptModel, foreign_key = "transaction_id")]
#[primary_key(transaction_id)]
pub struct InvalidTransactionResultModel {
    pub transaction_id: String,
    pub error_message: String,
    pub error_data: Vec<u8>,
}

#[derive(
    Debug, PartialEq, Eq, Associations, Identifiable, Insertable, Queryable, QueryableByName,
)]
#[table_name = "valid_transaction_result_data"]
#[belongs_to(TransactionReceiptModel, foreign_key = "transaction_id")]
#[primary_key(id)]
pub struct ValidTransactionResultDataModel {
    pub id: i64,
    pub transaction_id: String,
    pub data: Vec<u8>,
    pub position: i32,
}

#[derive(AsChangeset, Insertable, PartialEq, Eq, Debug)]
#[table_name = "valid_transaction_result_data"]
pub struct NewValidTransactionResultDataModel {
    pub transaction_id: String,
    pub data: Vec<u8>,
    pub position: i32,
}

#[derive(
    Debug, PartialEq, Eq, Associations, Identifiable, Insertable, Queryable, QueryableByName,
)]
#[table_name = "valid_transaction_result_event"]
#[belongs_to(TransactionReceiptModel, foreign_key = "transaction_id")]
#[primary_key(event_id)]
pub struct ValidTransactionResultEventModel {
    pub event_id: i64,
    pub transaction_id: String,
    pub event_type: String,
    pub data: Vec<u8>,
    pub position: i32,
}

#[derive(AsChangeset, Insertable, PartialEq, Eq, Debug)]
#[table_name = "valid_transaction_result_event"]
pub struct NewValidTransactionResultEventModel {
    pub transaction_id: String,
    pub event_type: String,
    pub data: Vec<u8>,
    pub position: i32,
}

#[derive(
    Debug, PartialEq, Eq, Associations, Identifiable, Insertable, Queryable, QueryableByName,
)]
#[table_name = "valid_transaction_result_event_attribute"]
#[belongs_to(TransactionReceiptModel, foreign_key = "transaction_id")]
#[primary_key(transaction_id, event_id, key)]
pub struct ValidTransactionResultEventAttributeModel {
    pub event_id: i64,
    pub transaction_id: String,
    pub key: String,
    pub value: String,
    pub position: i32,
}

impl ValidTransactionResultEventAttributeModel {
    // Creates a list of `ValidTransactionResultEventAttributeModel` from an `Event`
    pub(super) fn list_from_event_with_ids(
        transaction_id: &str,
        event_id: i64,
        event: &Event,
    ) -> Result<Vec<ValidTransactionResultEventAttributeModel>, ReceiptStoreError> {
        event
            .attributes
            .iter()
            .enumerate()
            .map(|(idx, (key, value))| {
                Ok(ValidTransactionResultEventAttributeModel {
                    event_id,
                    transaction_id: transaction_id.to_string(),
                    key: key.to_string(),
                    value: value.to_string(),
                    position: i32::try_from(idx).map_err(|_| {
                        ReceiptStoreError::InternalError(InternalError::with_message(
                            "Unable to convert index into i32".to_string(),
                        ))
                    })?,
                })
            })
            .collect()
    }
}

#[derive(
    Debug, PartialEq, Eq, Associations, Identifiable, Insertable, Queryable, QueryableByName,
)]
#[table_name = "valid_transaction_result_state_change"]
#[belongs_to(TransactionReceiptModel, foreign_key = "transaction_id")]
#[primary_key(id)]
pub struct ValidTransactionResultStateChangeModel {
    pub id: i64,
    pub transaction_id: String,
    pub state_change_type: StateChangeTypeModel,
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub position: i32,
}

#[derive(Insertable, PartialEq, Eq, Debug)]
#[table_name = "valid_transaction_result_state_change"]
pub struct NewValidTransactionResultStateChangeModel {
    pub transaction_id: String,
    pub state_change_type: StateChangeTypeModel,
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub position: i32,
}

#[repr(i16)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, FromSqlRow)]
pub enum StateChangeTypeModel {
    Set = 1,
    Delete = 2,
}

impl<DB> ToSql<SmallInt, DB> for StateChangeTypeModel
where
    DB: Backend,
    i16: ToSql<SmallInt, DB>,
{
    fn to_sql<W: Write>(&self, out: &mut Output<W, DB>) -> serialize::Result {
        (*self as i16).to_sql(out)
    }
}

impl AsExpression<SmallInt> for StateChangeTypeModel {
    type Expression = AsExprOf<i16, SmallInt>;

    fn as_expression(self) -> Self::Expression {
        <i16 as AsExpression<SmallInt>>::as_expression(self as i16)
    }
}

impl<'a> AsExpression<SmallInt> for &'a StateChangeTypeModel {
    type Expression = AsExprOf<i16, SmallInt>;

    fn as_expression(self) -> Self::Expression {
        <i16 as AsExpression<SmallInt>>::as_expression((*self) as i16)
    }
}

impl<DB> FromSql<SmallInt, DB> for StateChangeTypeModel
where
    DB: Backend,
    i16: FromSql<SmallInt, DB>,
{
    fn from_sql(bytes: Option<&DB::RawValue>) -> deserialize::Result<Self> {
        match i16::from_sql(bytes)? {
            1 => Ok(StateChangeTypeModel::Set),
            2 => Ok(StateChangeTypeModel::Delete),
            int => Err(format!("Invalid state change type {}", int).into()),
        }
    }
}
