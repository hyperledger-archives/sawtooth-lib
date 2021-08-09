--- Copyright 2018-2021 Cargill Incorporated
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS transaction_receipt (
    transaction_id              TEXT PRIMARY KEY,
    idx                         BIGINT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS invalid_transaction_result (
    transaction_id              TEXT PRIMARY KEY,
    error_message               TEXT NOT NULL,
    error_data                  BYTEA NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES transaction_receipt(transaction_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS valid_transaction_result_data (
    id                          BIGSERIAL PRIMARY KEY,
    transaction_id              TEXT NOT NULL,
    data                        BYTEA NOT NULL,
    position                    INTEGER NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES transaction_receipt(transaction_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS valid_transaction_result_event (
    event_id                    BIGSERIAL PRIMARY KEY,
    transaction_id              TEXT NOT NULL,
    event_type                  TEXT NOT NULL,
    data                        BYTEA NOT NULL,
    position                    INTEGER NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES transaction_receipt(transaction_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS valid_transaction_result_event_attribute (
    event_id                    INTEGER NOT NULL,
    transaction_id              TEXT NOT NULL,
    key                         TEXT NOT NULL,
    value                       TEXT NOT NULL,
    position                    INTEGER NOT NULL,
    PRIMARY KEY (event_id, transaction_id, key),
    FOREIGN KEY (transaction_id) REFERENCES transaction_receipt(transaction_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS valid_transaction_result_state_change (
    id                          BIGSERIAL PRIMARY KEY,
    transaction_id              TEXT NOT NULL,
    state_change_type           INTEGER NOT NULL,
    key                         TEXT NOT NULL,
    value                       BYTEA,
    position                    INTEGER NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES transaction_receipt(transaction_id) ON DELETE CASCADE
);
