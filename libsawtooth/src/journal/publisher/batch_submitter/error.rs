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

//! Errors for the batch submitter

use std::error::Error;

use transact::protocol::batch::BatchPair;

/// Errors that can occur when submitting batches
#[derive(Debug)]
pub enum BatchSubmitterError {
    /// The pending batches pool is full; the batch could not be submitted.
    PoolFull(BatchPair),
    /// The pending batches pool has shutdown; no more batches can be submitted.
    PoolShutdown,
}

impl Error for BatchSubmitterError {}

impl std::fmt::Display for BatchSubmitterError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::PoolFull(_) => f.write_str("batch pool is full"),
            Self::PoolShutdown => f.write_str("batch receiver has disconnected"),
        }
    }
}
