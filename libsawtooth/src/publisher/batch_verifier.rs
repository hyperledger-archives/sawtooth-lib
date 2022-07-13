/*
 * Copyright 2022 Cargill Incorporated
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

//! Contains traits related to `Batch` verification

use crate::error::InternalError;

use super::Batch;
use super::PublishingContext;

/// Result of executing a batch.
pub trait BatchExecutionResult: Send {}

/// A Factory to create `BatchVerifier` instances
pub trait BatchVerifierFactory {
    type Batch: Batch;
    type Context: PublishingContext;
    type ExecutionResult: BatchExecutionResult;

    // allow type complexity here because of the use of associated types
    #[allow(clippy::type_complexity)]
    /// Start execution of batches in relation to a specific context by
    /// returning a `BatchVerifier`
    fn start(
        &mut self,
        context: Self::Context,
    ) -> Result<
        Box<
            dyn BatchVerifier<
                Batch = Self::Batch,
                Context = Self::Context,
                ExecutionResult = Self::ExecutionResult,
            >,
        >,
        InternalError,
    >;
}

/// Verify the contents of a `Batch` and its transactions
pub trait BatchVerifier: Send {
    type Batch: Batch;
    type Context: PublishingContext;
    type ExecutionResult: BatchExecutionResult;

    /// Add a new batch to the verifier
    fn add_batch(&mut self, batch: Self::Batch) -> Result<(), InternalError>;

    /// Finalize the verification of the batches, returning the execution results of batches
    /// that have completed execution.
    fn finalize(&mut self) -> Result<Vec<Self::ExecutionResult>, InternalError>;

    /// Cancel exeuction of batches, discarding all excution results
    fn cancel(&mut self) -> Result<(), InternalError>;
}
