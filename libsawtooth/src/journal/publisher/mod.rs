/*
 * Copyright 2018 Intel Corporation
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

// allow borrowed box, this is required to use PublisherState trait
 #![allow(clippy::borrowed_box)]

pub mod batch_injector;
pub mod batch_pool;
mod error;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use transact::protocol::batch::Batch;

use crate::execution::execution_platform::ExecutionPlatform;
use crate::journal::block_manager::BlockRef;
use crate::journal::candidate_block::CandidateBlock;
use crate::protocol::block::BlockPair;

use batch_pool::PendingBatchesPool;
pub use error::BlockPublisherError;

#[derive(Debug)]
pub enum InitializeBlockError {
    BlockInProgress,
    MissingPredecessor,
}

#[derive(Debug)]
pub enum FinalizeBlockError {
    BlockNotInitialized,
    BlockEmpty,
}

pub trait BatchObserver: Send + Sync {
    fn notify_batch_pending(&self, batch: &Batch);
}

/// Broadcasts blocks to the network
pub trait BlockBroadcaster: Send {
    /// Broadcast the block to the network
    fn broadcast(&self, block: BlockPair) -> Result<(), BlockPublisherError>;
}

pub trait PublisherState: Send + Sync {
    fn pending_batches(&self) -> &PendingBatchesPool;

    fn mut_pending_batches(&mut self) -> &mut PendingBatchesPool;

    fn chain_head(&mut self, block: Option<BlockPair>);

    fn candidate_block(&mut self) -> &mut Option<Box<dyn CandidateBlock>>;

    fn set_candidate_block(
        &mut self,
        candidate_block: Option<Box<dyn CandidateBlock>>,
    ) -> Option<Box<dyn CandidateBlock>>;

    fn block_references(&mut self) -> &mut HashMap<String, BlockRef>;

    fn batch_observers(&self) -> &[Box<dyn BatchObserver>];

    fn transaction_executor(&self) -> &Box<dyn ExecutionPlatform>;
}

pub trait SyncPublisher: Send + Sync {
    fn box_clone(&self) -> Box<dyn SyncPublisher>;

    fn state(&self) -> &Arc<RwLock<Box<dyn PublisherState>>>;

    fn on_chain_updated(
        &self,
        state: &mut Box<dyn PublisherState>,
        chain_head: BlockPair,
        committed_batches: Vec<Batch>,
        uncommitted_batches: Vec<Batch>,
    );

    fn on_chain_updated_internal(
        &mut self,
        chain_head: BlockPair,
        committed_batches: Vec<Batch>,
        uncommitted_batches: Vec<Batch>,
    );

    fn on_batch_received(&self, batch: Batch);

    fn cancel_block(&self, state: &mut Box<dyn PublisherState>, unref_block: bool);

    fn initialize_block(
        &self,
        state: &mut Box<dyn PublisherState>,
        previous_block: &BlockPair,
        ref_block: bool,
    ) -> Result<(), InitializeBlockError>;

    fn finalize_block(
        &self,
        state: &mut Box<dyn PublisherState>,
        consensus_data: &[u8],
        force: bool,
    ) -> Result<String, FinalizeBlockError>;

    fn summarize_block(
        &self,
        state: &mut Box<dyn PublisherState>,
        force: bool,
    ) -> Result<Vec<u8>, FinalizeBlockError>;

    fn stopped(&self) -> bool;

    fn stop(&self);
}

impl Clone for Box<dyn SyncPublisher> {
    fn clone(&self) -> Box<dyn SyncPublisher> {
        self.box_clone()
    }
}
