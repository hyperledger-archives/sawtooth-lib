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

-- Recreate the table with the unique constraint
CREATE TABLE IF NOT EXISTS transaction_receipt_copy (
    transaction_id              TEXT PRIMARY KEY,
    idx                         BIGINT NOT NULL UNIQUE
);

INSERT INTO transaction_receipt_copy(transaction_id, idx)
    SELECT transaction_id, idx FROM transaction_receipt;
DROP TABLE transaction_receipt;
ALTER TABLE transaction_receipt_copy RENAME TO transaction_receipt;
