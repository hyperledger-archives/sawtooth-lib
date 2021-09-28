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

table! {
    transaction_receipt (transaction_id) {
        transaction_id -> Text,
        idx -> Int8,
        service_id -> Nullable<Text>,
    }
}

table! {
    invalid_transaction_result (transaction_id) {
        transaction_id -> Text,
        error_message -> Text,
        error_data -> Binary,
    }
}

table! {
    valid_transaction_result_data (id) {
        id -> Int8,
        transaction_id -> Text,
        data -> Binary,
        position -> Integer,
    }
}

table! {
    valid_transaction_result_event (event_id) {
        event_id -> Int8,
        transaction_id -> Text,
        event_type -> Text,
        data -> Binary,
        position -> Integer,
    }
}

table! {
    valid_transaction_result_event_attribute (event_id, transaction_id, key) {
        event_id -> Int8,
        transaction_id -> Text,
        key -> Text,
        value -> Text,
        position -> Integer,
    }
}

table! {
    valid_transaction_result_state_change (id) {
        id -> Int8,
        transaction_id -> Text,
        state_change_type -> SmallInt,
        key -> Text,
        value -> Nullable<Binary>,
        position -> Integer,
    }
}

allow_tables_to_appear_in_same_query!(
    transaction_receipt,
    invalid_transaction_result,
    valid_transaction_result_data,
    valid_transaction_result_event,
    valid_transaction_result_event_attribute,
    valid_transaction_result_state_change,
);
