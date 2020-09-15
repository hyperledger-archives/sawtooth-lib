// Copyright 2018-2020 Cargill Incorporated
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Traits for generating batches to inject into blocks before they're published.

pub mod block_info;
mod default_factory;

use transact::protocol::batch::BatchPair;

use crate::protocol::block::BlockPair;

use super::BlockPublisherError;

pub(super) use default_factory::DefaultBatchInjectorFactory;

/// Generates batches to be injected into a block
pub trait BatchInjector: Send {
    /// Returns an ordered list of batches to inject at the beginning of a block
    ///
    /// # Arguments
    ///
    /// * `previous_block` - The parent of the block the batches will be injected into
    fn block_start(
        &self,
        previous_block: &BlockPair,
    ) -> Result<Vec<BatchPair>, BlockPublisherError>;
}

/// Constructs batch injectors
pub trait BatchInjectorFactory: Send {
    /// Returns all batch injectors that are enabled
    fn create_injectors(
        &self,
        previous_block: &BlockPair,
    ) -> Result<Vec<Box<dyn BatchInjector>>, BlockPublisherError>;
}
