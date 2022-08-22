/*
 * Copyright 2018-2020 Cargill Incorporated
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

//! Helper functions to convert transacts implementation of TransactionReceipt and StateChange
//! to the local version

#[cfg(feature = "transaction-receipt-store")]
pub mod store;

use std::convert::TryFrom;

use crate::protos::events::{Event, Event_Attribute};
use crate::protos::transaction_receipt::{StateChange, StateChange_Type, TransactionReceipt};
use crate::transact::protocol::receipt::{
    StateChange as TransactStateChange, TransactionReceipt as TransactTransactionReceipt,
    TransactionResult,
};

impl From<TransactStateChange> for StateChange {
    fn from(change: TransactStateChange) -> Self {
        let mut state_change = StateChange::new();

        match change {
            TransactStateChange::Set { key, value } => {
                state_change.set_field_type(StateChange_Type::SET);
                state_change.set_address(key);
                state_change.set_value(value);
            }
            TransactStateChange::Delete { key } => {
                state_change.set_field_type(StateChange_Type::DELETE);
                state_change.set_address(key);
            }
        }

        state_change
    }
}

impl TryFrom<TransactTransactionReceipt> for TransactionReceipt {
    type Error = &'static str;

    fn try_from(txn_receipt: TransactTransactionReceipt) -> Result<Self, Self::Error> {
        let mut proto_txn_receipt = TransactionReceipt::new();
        match txn_receipt.transaction_result {
            TransactionResult::Valid {
                state_changes,
                events,
                data,
            } => {
                proto_txn_receipt.set_state_changes(
                    state_changes
                        .iter()
                        .map(|sc| StateChange::from(sc.clone()))
                        .collect(),
                );
                proto_txn_receipt.set_events(
                    events
                        .iter()
                        .map(|e| {
                            let mut event = Event::new();
                            event.set_event_type(e.event_type.to_string());
                            event.set_data(e.data.to_vec());
                            event.set_attributes(
                                e.attributes
                                    .iter()
                                    .map(|(key, value)| {
                                        let mut attributes = Event_Attribute::new();
                                        attributes.set_key(key.into());
                                        attributes.set_value(value.into());
                                        attributes
                                    })
                                    .collect(),
                            );
                            event
                        })
                        .collect(),
                );
                proto_txn_receipt.set_data(protobuf::RepeatedField::from_vec(data));
                proto_txn_receipt.set_transaction_id(txn_receipt.transaction_id);

                Ok(proto_txn_receipt)
            }
            TransactionResult::Invalid { .. } => Err("Invalid transaction results not supported"),
        }
    }
}
