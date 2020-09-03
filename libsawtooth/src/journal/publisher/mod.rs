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

    fn candidate_block(&mut self) -> &mut Option<CandidateBlock>;

    fn set_candidate_block(
        &mut self,
        candidate_block: Option<CandidateBlock>,
    ) -> Option<CandidateBlock>;

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

/// Tracks the state of the block that's being built
struct CandidateBlock {
    /// If `Some`, the maximum number of batches that are allowed in the block. If `None`, there is
    /// no limit.
    max_batches_per_block: Option<usize>,
    /// Used to verify that batch signers are authorized to make transactions
    permission_verifier: PermissionVerifier,
    /// The block that's being built on top of
    previous_block: BlockPair,
    /// A reference to the previous block that's tracked by the block manager. This reference is not
    /// actually used by the publisher, but it's kept here to ensure that the block manager doesn't
    /// throw away the block while the publisher is building on top of it. The reference is simply
    /// dropped when the candidate block is dropped (on finalization).
    _previous_block_reference: BlockRef,
    /// Handles execution of the transactions that will be in the block
    scheduler: Box<dyn Scheduler>,
    /// Verifies that the block is valid according to the block validation rules
    validation_rule_enforcer: ValidationRuleEnforcer,

    /// Tracks which scheduled batches were injected (not taken from the pending batches pool)
    injected_batch_ids: HashSet<String>,
    /// Tracks the IDs of all batches that have been scheduled, in the order they were scheduled
    scheduled_batch_ids: Vec<String>,
    /// Tracks the IDs of all transactions that have been scheduled
    scheduled_txn_ids: HashSet<Vec<u8>>,

    /// Used to receive batch execution results from the scheduler. This channel is consumed when
    /// the block is completed (summarized/finalized) or cancelled.
    batch_results: Receiver<Option<BatchExecutionResult>>,
    /// Saves all batches that were executed and will be put in the block on finalization. This is
    /// populated when the block is completed (summarized/finalized).
    executed_batches: Vec<Batch>,
    /// Saves the state root hash that will be put in the block on finalization. This is populated
    /// when the block is completed (summarized/finalized).
    state_root_hash: Option<String>,
    /// Saves the summary of the block's batches. This is populated when the block is completed
    /// (summarized/finalized).
    summary: Option<Vec<u8>>,
}

impl CandidateBlock {
    /// Creates a new candidate block
    pub fn new(
        batch_results: Receiver<Option<BatchExecutionResult>>,
        max_batches_per_block: Option<usize>,
        permission_verifier: PermissionVerifier,
        previous_block: BlockPair,
        previous_block_reference: BlockRef,
        scheduler: Box<dyn Scheduler>,
        validation_rule_enforcer: ValidationRuleEnforcer,
    ) -> Self {
        Self {
            max_batches_per_block,
            permission_verifier,
            previous_block,
            _previous_block_reference: previous_block_reference,
            scheduler,
            validation_rule_enforcer,

            injected_batch_ids: Default::default(),
            scheduled_batch_ids: Default::default(),
            scheduled_txn_ids: Default::default(),

            batch_results,
            executed_batches: Default::default(),
            state_root_hash: Default::default(),
            summary: Default::default(),
        }
    }

    /// Returns `true` if batches can still be scheduled; returns `false` otherwise.
    ///
    /// Batches can be scheduled for the candidate block when both of these conditions are true:
    /// * The maximum number of batches that a block can contain have not already been scheduled
    /// * The scheduler has not been finalized
    pub fn can_schedule_batch(&self) -> bool {
        let is_full = self
            .max_batches_per_block
            .map(|max| self.scheduled_batch_ids.len() >= max)
            .unwrap_or(false);
        // If a summary has been computed for the candidate block, the scheduler was finalized
        let is_finalized = self.summary.is_some();

        !is_full && !is_finalized
    }
}
