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

//! Provides the add transaction receipts operation for `DieselReceiptStore`

use std::convert::TryFrom;

use diesel::{insert_into, prelude::*};
use transact::protocol::receipt::{StateChange, TransactionReceipt, TransactionResult};

use crate::error::{ConstraintViolationError, ConstraintViolationType, InternalError};

use super::ReceiptStoreOperations;

use crate::receipt::store::{
    diesel::{
        models::{
            InvalidTransactionResultModel, NewValidTransactionResultDataModel,
            NewValidTransactionResultEventModel, NewValidTransactionResultStateChangeModel,
            StateChangeTypeModel, TransactionReceiptModel,
            ValidTransactionResultEventAttributeModel, ValidTransactionResultEventModel,
        },
        schema::{
            invalid_transaction_result, transaction_receipt, valid_transaction_result_data,
            valid_transaction_result_event, valid_transaction_result_event_attribute,
            valid_transaction_result_state_change,
        },
    },
    ReceiptStoreError,
};

pub(in crate::receipt::store::diesel) trait ReceiptStoreAddTxnReceiptsOperation {
    fn add_txn_receipts(&self, receipts: Vec<TransactionReceipt>) -> Result<(), ReceiptStoreError>;
}

impl<'a> ReceiptStoreAddTxnReceiptsOperation
    for ReceiptStoreOperations<'a, diesel::sqlite::SqliteConnection>
{
    fn add_txn_receipts(&self, receipts: Vec<TransactionReceipt>) -> Result<(), ReceiptStoreError> {
        self.conn.transaction::<(), _, _>(|| {
            for (index, receipt) in receipts.iter().enumerate() {
                if transaction_receipt::table
                    .filter(transaction_receipt::transaction_id.eq(receipt.transaction_id.to_string()))
                    .first::<TransactionReceiptModel>(self.conn)
                    .optional()?
                    .is_some()
                {
                    return Err(ReceiptStoreError::ConstraintViolationError(
                        ConstraintViolationError::with_violation_type(ConstraintViolationType::Unique),
                    ));
                }

                let id = &receipt.transaction_id;

                // Create the TransactionReceiptModel and insert it into the
                // transaction_receipt table
                let transaction_receipt_model = TransactionReceiptModel {
                    transaction_id: id.to_string(),
                    idx: i64::try_from(index).map_err(|_| {
                        ReceiptStoreError::InternalError(InternalError::with_message(
                            "Unable to convert index into i64".to_string(),
                        ))
                    })?,
                };
                insert_into(transaction_receipt::table)
                    .values(transaction_receipt_model)
                    .execute(self.conn)?;

                match &receipt.transaction_result {
                    TransactionResult::Valid{ state_changes, events, data } => {
                        // Create a vector of `ValidTransactionResultDataModels` from
                        // the valid transaction result's data field
                        let valid_transaction_result_data_models: Vec<NewValidTransactionResultDataModel> = data
                            .iter()
                            .enumerate()
                            .map(|(i, d)| {
                                let index = match i32::try_from(i) {
                                    Ok(index) => index,
                                    Err(_) => return Err(ReceiptStoreError::InternalError(InternalError::with_message("Unable to convert index into i32".to_string()))),
                                };
                                Ok(NewValidTransactionResultDataModel {
                                    transaction_id: id.to_string(),
                                    data: d.to_vec(),
                                    position: index,
                                })
                            }).collect::<Result<Vec<NewValidTransactionResultDataModel>, _>>()?;

                        insert_into(valid_transaction_result_data::table)
                            .values(valid_transaction_result_data_models)
                            .execute(self.conn)?;


                        // Iterate through the valid transaction result events, creating
                        // a list of `ValidTransactionResultEventModels` each with a corresponding
                        // list of `ValidTransactionResultEventAttributeModels` while maintaining
                        // the original order
                        for (i, event) in events.iter().enumerate() {
                            let event_model = NewValidTransactionResultEventModel {
                                transaction_id: id.to_string(),
                                event_type: event.event_type.to_string(),
                                data: event.data.to_vec(),
                                position: i32::try_from(i).map_err(|_| {
                                    ReceiptStoreError::InternalError(InternalError::with_message(
                                        "Unable to convert index into i32".to_string(),
                                    ))
                                })?,
                            };

                            insert_into(valid_transaction_result_event::table)
                                .values(event_model)
                                .execute(self.conn)?;

                            // Get the event_id that was generated for the event when it was
                            // inserted into the table
                            let event_id: i64 = valid_transaction_result_event::table
                                .order(valid_transaction_result_event::event_id.desc())
                                .first::<ValidTransactionResultEventModel>(self.conn)?
                                .event_id;

                            let event_attribute_models: Vec<ValidTransactionResultEventAttributeModel> =
                                ValidTransactionResultEventAttributeModel::list_from_event_with_ids(id, event_id, event)?;
                            insert_into(valid_transaction_result_event_attribute::table)
                                .values(event_attribute_models)
                                .execute(self.conn)?;
                        }

                        // Create a list of state changes, maintaining original order
                        let state_change_models: Vec<NewValidTransactionResultStateChangeModel> = state_changes
                            .iter()
                            .enumerate()
                            .map(|(i, s)| {
                                let ((key, value), state_change_type) = match s {
                                    StateChange::Set { key, value } => ((key, Some(value.to_vec())), StateChangeTypeModel::Set),
                                    StateChange::Delete { key } => ((key, None), StateChangeTypeModel::Delete),
                                };
                                Ok(NewValidTransactionResultStateChangeModel {
                                    transaction_id: id.to_string(),
                                    state_change_type,
                                    key: key.to_string(),
                                    value,
                                    position: i32::try_from(i).map_err(|_| {
                                        ReceiptStoreError::InternalError(InternalError::with_message(
                                            "Unable to convert index into i32".to_string(),
                                        ))
                                    })?,
                                })
                            }).collect::<Result<Vec<NewValidTransactionResultStateChangeModel>, ReceiptStoreError>>()?;

                        insert_into(valid_transaction_result_state_change::table)
                            .values(state_change_models)
                            .execute(self.conn)?;

                    }
                    TransactionResult::Invalid{ error_message, error_data } => {
                        let invalid_transaction_result_model = InvalidTransactionResultModel {
                            transaction_id: id.to_string(),
                            error_message: error_message.to_string(),
                            error_data: error_data.to_vec(),
                        };

                        insert_into(invalid_transaction_result::table)
                            .values(invalid_transaction_result_model)
                            .execute(self.conn)?;
                    }
                }
            }
            Ok(())
        })
    }
}
