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

//! Provides the list transaction receipt since operation for `DieselReceiptStore`

use std::collections::HashMap;

use diesel::{prelude::*, sql_types::Text};

use crate::error::InvalidStateError;
use crate::transact::protocol::receipt::{
    Event, StateChange, TransactionReceipt, TransactionResult,
};

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
    ReceiptIter, ReceiptStoreError,
};

pub(in crate::receipt::store::diesel) trait ReceiptStoreListReceiptsSinceOperation {
    fn list_receipts_since(&self, id: Option<String>) -> Result<ReceiptIter, ReceiptStoreError>;
}

impl<'a, 's, C> ReceiptStoreListReceiptsSinceOperation for ReceiptStoreOperations<'a, 's, C>
where
    C: diesel::Connection,
    String: diesel::deserialize::FromSql<Text, C::Backend>,
    i64: diesel::deserialize::FromSql<diesel::sql_types::BigInt, C::Backend>,
    i32: diesel::deserialize::FromSql<diesel::sql_types::Integer, C::Backend>,
    i16: diesel::deserialize::FromSql<diesel::sql_types::SmallInt, C::Backend>,
    Vec<u8>: diesel::deserialize::FromSql<diesel::sql_types::Binary, C::Backend>,
{
    fn list_receipts_since(&self, id: Option<String>) -> Result<ReceiptIter, ReceiptStoreError> {
        self.conn.transaction::<ReceiptIter, _, _>(|| {
            // Collect the `TransactionReceiptModels` of all transaction receipts
            // that are to be listed
            let mut query = transaction_receipt::table.into_boxed();
            if let Some(service_id) = &self.service_id {
                query = query.filter(transaction_receipt::service_id.eq(service_id));
            };
            let transaction_receipt_models: Vec<TransactionReceiptModel> = match id {
                Some(id) => query
                    .filter(transaction_receipt::transaction_id.gt(id))
                    .select(transaction_receipt::all_columns)
                    .load(self.conn)?,
                None => query
                    .select(transaction_receipt::all_columns)
                    .load(self.conn)?,
            };

            let transaction_ids: Vec<String> = transaction_receipt_models
                .iter()
                .map(|txn_receipt_model| txn_receipt_model.transaction_id.to_string())
                .collect();

            // HashMap of `transaction_id` and its corresponding `index`
            let transaction_receipt_indexes: HashMap<String, i64> = transaction_receipt_models
                .iter()
                .map(|receipt| (receipt.transaction_id.to_string(), receipt.idx))
                .collect();

            // Maps IndexedEvents to its corresponding `transaction_id` and `event_id`
            let mut events: HashMap<(String, i64), IndexedEvent> = HashMap::new();

            for (event, opt_attribute) in valid_transaction_result_event::table
                .filter(valid_transaction_result_event::transaction_id.eq_any(&transaction_ids))
                // Using `left_join` here ensures that if an event does not have any
                // associated attributes in the `valid_transaction_result_event_attribute` table
                // the event will still be collected
                .left_join(
                    valid_transaction_result_event_attribute::table.on(
                        valid_transaction_result_event::event_id
                            .eq(valid_transaction_result_event_attribute::event_id)
                            .and(
                                valid_transaction_result_event_attribute::transaction_id
                                    .eq(valid_transaction_result_event::transaction_id),
                            ),
                    ),
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
                if let Some(attribute_model) = opt_attribute {
                    if let Some(event) =
                        events.get_mut(&(event.transaction_id.to_string(), event.event_id))
                    {
                        event.attributes.push(attribute_model);
                    } else {
                        // Insert new event if it does not exist
                        events
                            .entry((event.transaction_id.to_string(), event.event_id))
                            .or_insert_with(|| IndexedEvent {
                                position: event.position,
                                event_type: event.event_type,
                                attributes: vec![attribute_model],
                                data: event.data,
                            });
                    }
                }
            }

            // For storing `Events` mapped to `transaction_ids`
            let mut result_events: HashMap<String, Vec<Event>> = HashMap::new();

            let mut ordered_events: Vec<((String, i64), IndexedEvent)> =
                events.into_iter().collect();
            ordered_events.sort_by_key(|((_, _), indexed_event)| indexed_event.position);

            for ((transaction_id, _), mut indexed_event) in ordered_events.into_iter() {
                indexed_event
                    .attributes
                    .sort_by_key(|attribute| attribute.position);
                let event = Event {
                    event_type: indexed_event.event_type,
                    attributes: indexed_event
                        .attributes
                        .iter()
                        .map(|attribute_model| {
                            (
                                attribute_model.key.to_string(),
                                attribute_model.value.to_string(),
                            )
                        })
                        .collect::<Vec<(String, String)>>(),
                    data: indexed_event.data,
                };

                if let Some(event_list) = result_events.get_mut(&transaction_id) {
                    event_list.push(event);
                } else {
                    result_events.insert(transaction_id.to_string(), vec![event]);
                }
            }

            // Collect valid transaction result state changes
            let mut state_changes = valid_transaction_result_state_change::table
                .select(valid_transaction_result_state_change::all_columns)
                .filter(
                    valid_transaction_result_state_change::transaction_id.eq_any(&transaction_ids),
                )
                .load::<ValidTransactionResultStateChangeModel>(self.conn)?;

            state_changes.sort_by_key(|state_change| state_change.position);

            // For storing `StateChanges` mapped to `transaction_ids`
            let mut result_state_changes: HashMap<String, Vec<StateChange>> = HashMap::new();

            for state_change in state_changes.into_iter() {
                let state_change_entry = match state_change.state_change_type {
                    StateChangeTypeModel::Set => StateChange::Set {
                        key: state_change.key,
                        value: state_change.value.ok_or_else(|| {
                            ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(
                                "Missing state change value".to_string(),
                            ))
                        })?,
                    },
                    StateChangeTypeModel::Delete => StateChange::Delete {
                        key: state_change.key,
                    },
                };

                if let Some(state_change_list) =
                    result_state_changes.get_mut(&state_change.transaction_id)
                {
                    state_change_list.push(state_change_entry);
                } else {
                    result_state_changes.insert(
                        state_change.transaction_id.to_string(),
                        vec![state_change_entry],
                    );
                }
            }

            // Collect valid transaction result data
            let mut data = valid_transaction_result_data::table
                .select(valid_transaction_result_data::all_columns)
                .filter(valid_transaction_result_data::transaction_id.eq_any(&transaction_ids))
                .load::<ValidTransactionResultDataModel>(self.conn)?;

            // For storing transaction result data mapped to `transaction_ids`
            let mut result_data: HashMap<String, Vec<Vec<u8>>> = HashMap::new();

            data.sort_by_key(|data| data.position);

            for data in data.into_iter() {
                let data_entry = data.data;

                if let Some(data_list) = result_data.get_mut(&data.transaction_id) {
                    data_list.push(data_entry);
                } else {
                    result_data.insert(data.transaction_id.to_string(), vec![data_entry]);
                }
            }

            // For storing `TransactionReceipt` and `index`
            let mut transaction_receipts: Vec<(i64, TransactionReceipt)> = Vec::new();

            // Build all transaction receipts with valid transaction results
            // Get the `transaction_ids` of all transaction receipts that don't
            // have an entry in the `invalid_transaction_result` table
            for id in transaction_receipt::table
                .select(transaction_receipt::transaction_id)
                .filter(transaction_receipt::transaction_id.eq_any(&transaction_ids))
                .filter(
                    transaction_receipt::transaction_id.ne_all(
                        invalid_transaction_result::table
                            .select(invalid_transaction_result::transaction_id)
                            .load::<String>(self.conn)?,
                    ),
                )
                .load::<String>(self.conn)?
            {
                let state_changes = match result_state_changes.get(&id.to_string()) {
                    Some(c) => c.to_vec(),
                    None => Vec::new(),
                };
                let events = match result_events.get(&id.to_string()) {
                    Some(e) => e.to_vec(),
                    None => Vec::new(),
                };
                let data = match result_data.get(&id.to_string()) {
                    Some(d) => d.to_vec(),
                    None => Vec::new(),
                };
                let res = TransactionResult::Valid {
                    state_changes,
                    events,
                    data,
                };
                let transaction_receipt = TransactionReceipt {
                    transaction_id: id.to_string(),
                    transaction_result: res,
                };
                let index = transaction_receipt_indexes
                    .get(&id.to_string())
                    .ok_or_else(|| {
                        ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(
                            format!("transaction id {} does not exist in the selection", id),
                        ))
                    })?;
                transaction_receipts.push((*index, transaction_receipt));
            }

            // Get invalid transaction results
            for invalid_result in invalid_transaction_result::table
                .select(invalid_transaction_result::all_columns)
                .filter(invalid_transaction_result::transaction_id.eq_any(&transaction_ids))
                .load::<InvalidTransactionResultModel>(self.conn)?
                .into_iter()
            {
                let id = invalid_result.transaction_id;
                let res = TransactionResult::Invalid {
                    error_message: invalid_result.error_message.to_string(),
                    error_data: invalid_result.error_data,
                };
                let transaction_receipt = TransactionReceipt {
                    transaction_id: id.to_string(),
                    transaction_result: res,
                };
                // Get the index that corresponds to the transaction receipt's `transaction_id`
                let index = transaction_receipt_indexes
                    .get(&id.to_string())
                    .ok_or_else(|| {
                        ReceiptStoreError::InvalidStateError(InvalidStateError::with_message(
                            format!("transaction id {} does not exist in the selection", id),
                        ))
                    })?;
                transaction_receipts.push((*index, transaction_receipt));
            }

            // Sort the transaction receipts by index
            let mut sorted_receipts: Vec<(i64, TransactionReceipt)> =
                transaction_receipts.into_iter().collect();
            sorted_receipts.sort_by_key(|(i, _)| *i);

            Ok(Box::new(
                sorted_receipts.into_iter().map(|(_, receipt)| Ok(receipt)),
            ))
        })
    }
}

// Native representation of an Event with an added `position` field to be used
// for ordering
struct IndexedEvent {
    position: i32,
    event_type: String,
    attributes: Vec<ValidTransactionResultEventAttributeModel>,
    data: Vec<u8>,
}
