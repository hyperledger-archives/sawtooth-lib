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

//! Provides the get transaction receipt by id operation for `DieselReceiptStore`

use std::collections::HashMap;

use diesel::{
    prelude::*,
    sql_types::{Integer, Text},
};
use transact::protocol::receipt::{Event, StateChange, TransactionReceipt, TransactionResult};

use crate::error::InvalidStateError;

use super::ReceiptStoreOperations;

use crate::receipt::store::{
    diesel::{
        models::{
            InvalidTransactionResultModel, StateChangeTypeModel, TransactionReceiptModel,
            ValidTransactionResultDataModel, ValidTransactionResultEventAttributeModel,
            ValidTransactionResultEventModel, ValidTransactionResultStateChangeModel,
        },
        schema::{
            invalid_transaction_result, transaction_receipt, valid_transaction_result_data,
            valid_transaction_result_event, valid_transaction_result_event_attribute,
            valid_transaction_result_state_change,
        },
    },
    ReceiptStoreError,
};

pub(in crate::receipt::store::diesel) trait ReceiptStoreGetTxnReceiptByIdOperation {
    fn get_txn_receipt_by_id(
        &self,
        id: &str,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError>;
}

impl<'a, C> ReceiptStoreGetTxnReceiptByIdOperation for ReceiptStoreOperations<'a, C>
where
    C: diesel::Connection,
    String: diesel::deserialize::FromSql<Text, C::Backend>,
    i64: diesel::deserialize::FromSql<diesel::sql_types::BigInt, C::Backend>,
    i32: diesel::deserialize::FromSql<Integer, C::Backend>,
    i16: diesel::deserialize::FromSql<diesel::sql_types::SmallInt, C::Backend>,
    Vec<u8>: diesel::deserialize::FromSql<diesel::sql_types::Binary, C::Backend>,
{
    fn get_txn_receipt_by_id(
        &self,
        id: &str,
    ) -> Result<Option<TransactionReceipt>, ReceiptStoreError> {
        self.conn
            .transaction::<Option<TransactionReceipt>, _, _>(|| {
                let mut query = transaction_receipt::table.into_boxed();
                if let Some(service_id) = &self.service_id {
                    query = query.filter(transaction_receipt::service_id.eq(service_id));
                };
                let txn_receipt: TransactionReceiptModel = match query
                    .select(transaction_receipt::all_columns)
                    .filter(transaction_receipt::transaction_id.eq(id.to_string()))
                    .first::<TransactionReceiptModel>(self.conn)
                    .optional()?
                {
                    Some(receipt) => receipt,
                    None => return Ok(None),
                };

                // Check if an entry exists in the `invalid_transaction_result` with the
                // given `transaction_id`, if not gather the data from the appropriate
                // tables to build a valid transaction result
                match invalid_transaction_result::table
                    .select(invalid_transaction_result::all_columns)
                    .filter(invalid_transaction_result::transaction_id.eq(id.to_string()))
                    .first::<InvalidTransactionResultModel>(self.conn)
                    .optional()?
                {
                    None => {
                        // Collect the state change entries with the associated `transaction_id`
                        let state_change_vec: Vec<StateChange> =
                            valid_transaction_result_state_change::table
                                .select(valid_transaction_result_state_change::all_columns)
                                .filter(
                                    valid_transaction_result_state_change::transaction_id
                                        .eq(id.to_string()),
                                )
                                .order(valid_transaction_result_state_change::position)
                                .load::<ValidTransactionResultStateChangeModel>(self.conn)?
                                .iter()
                                .map(|change| match change.state_change_type {
                                    StateChangeTypeModel::Set => Ok(StateChange::Set {
                                        key: change.key.to_string(),
                                        value: change
                                            .value
                                            .as_ref()
                                            .ok_or_else(|| {
                                                ReceiptStoreError::InvalidStateError(
                                                    InvalidStateError::with_message(
                                                        "Missing state change value".to_string(),
                                                    ),
                                                )
                                            })?
                                            .to_vec(),
                                    }),
                                    StateChangeTypeModel::Delete => Ok(StateChange::Delete {
                                        key: change.key.to_string(),
                                    }),
                                })
                                .collect::<Result<Vec<StateChange>, ReceiptStoreError>>()?;

                        // Collect the valid transaction result data entries with the associated
                        // `transaction_id`
                        let data_vec: Vec<Vec<u8>> = valid_transaction_result_data::table
                            .select(valid_transaction_result_data::all_columns)
                            .filter(
                                valid_transaction_result_data::transaction_id.eq(id.to_string()),
                            )
                            .order(valid_transaction_result_data::position)
                            .load::<ValidTransactionResultDataModel>(self.conn)?
                            .iter()
                            .map(|data| data.data.to_vec())
                            .collect();

                        // HashMap to collect events and map them to `event_id`
                        let mut events: HashMap<i64, ValidTransactionResultEventModel> =
                            HashMap::new();

                        // HashMap to collect event_attributes and map them to `event_id`
                        let mut event_attributes: HashMap<
                            i64,
                            Vec<ValidTransactionResultEventAttributeModel>,
                        > = HashMap::new();

                        // Collect all event entries and associated event attributes
                        for (event, opt_attribute) in valid_transaction_result_event::table
                            .filter(
                                valid_transaction_result_event::transaction_id.eq(id.to_string()),
                            )
                            // Using `left_join` here ensures that if an event does not have any
                            // associated attributes in the `valid_transaction_result_event_attribute` table
                            // the event will still be collected
                            .left_join(
                                valid_transaction_result_event_attribute::table
                                    .on(valid_transaction_result_event::transaction_id
                                    .eq(valid_transaction_result_event_attribute::transaction_id)
                                    .and(
                                        valid_transaction_result_event::event_id
                                            .eq(valid_transaction_result_event_attribute::event_id),
                                    )),
                            )
                            // Make the attribute nullable for the case that an event does not
                            // have any associated attributes
                            .select((
                                valid_transaction_result_event::all_columns,
                                valid_transaction_result_event_attribute::all_columns.nullable(),
                            ))
                            .load::<(
                                ValidTransactionResultEventModel,
                                Option<ValidTransactionResultEventAttributeModel>,
                            )>(self.conn)?
                        {
                            if let Some(event_attribute_model) = opt_attribute {
                                if let Some(attributes) = event_attributes.get_mut(&event.event_id)
                                {
                                    attributes.push(event_attribute_model);
                                } else {
                                    event_attributes
                                        .insert(event.event_id, vec![event_attribute_model]);
                                }
                            }
                            // insert new event into events hashmap if it doesn't exist
                            events.entry(event.event_id).or_insert(event);
                        }

                        // Sort the event models and their attributes by position and convert
                        // them to `Events`
                        let mut sorted_events: Vec<ValidTransactionResultEventModel> =
                            events.into_iter().map(|(_, event)| event).collect();
                        sorted_events.sort_by_key(|event| event.position);
                        let event_vec = sorted_events
                            .into_iter()
                            .map(|event| {
                                let event_attributes: Vec<(String, String)> =
                                    if let Some(attributes) =
                                        event_attributes.get_mut(&event.event_id)
                                    {
                                        attributes.sort_by_key(|attr| attr.position);
                                        attributes
                                            .iter()
                                            .map(|attr| {
                                                (attr.key.to_string(), attr.value.to_string())
                                            })
                                            .collect::<Vec<(String, String)>>()
                                    } else {
                                        vec![]
                                    };
                                Event {
                                    event_type: event.event_type,
                                    attributes: event_attributes,
                                    data: event.data,
                                }
                            })
                            .collect::<Vec<Event>>();

                        let result = TransactionResult::Valid {
                            state_changes: state_change_vec,
                            events: event_vec,
                            data: data_vec,
                        };

                        Ok(Some(TransactionReceipt {
                            transaction_id: txn_receipt.transaction_id,
                            transaction_result: result,
                        }))
                    }
                    Some(invalid_result_model) => {
                        let result = TransactionResult::Invalid {
                            error_message: invalid_result_model.error_message,
                            error_data: invalid_result_model.error_data,
                        };

                        Ok(Some(TransactionReceipt {
                            transaction_id: txn_receipt.transaction_id,
                            transaction_result: result,
                        }))
                    }
                }
            })
    }
}
