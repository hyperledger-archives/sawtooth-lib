/*
 * Copyright 2019-2021 Cargill Incorporated
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
 * -----------------------------------------------------------------------------
 */

//! Transact structs for batches, transactions and receipts.
//!
//! These structs cover the core protocols of the Transact system.  Batches of transactions are
//! scheduled and executed.  The resuls of execution are stored in transaction receipts.

#[cfg(feature = "transact-protocol-batch")]
pub use crate::protocol::batch;
pub use crate::protocol::command;
#[cfg(feature = "transact-key-value-state")]
pub use crate::protocol::key_value_state;
pub use crate::protocol::receipt;
#[cfg(any(feature = "transact-protocol-sabre", feature = "family-sabre"))]
pub use crate::protocol::sabre;
#[cfg(feature = "transact-protocol-transaction")]
pub use crate::protocol::transaction;
