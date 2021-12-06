/*
 * Copyright 2018 Intel Corporation
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

//! Block construction and publication
//!
//! The [`BlockPublisher`] builds blocks by executing batches taken from the [`PendingBatchesPool`].
//! The pending batches pool is populated by the [`BatchSubmitter`], which is provided by the
//! publisher. Batches added to the pool are sent to all [`BatchObserver`]s that are known to the
//! publisher, and published blocks are broadcast using the [`BlockBroadcaster`].
//!
//! [`BlockPublisher`]: struct.BlockPublisher.html
//! [`BatchSubmitter`]: struct.BatchSubmitter.html
//! [`BatchObserver`]: trait.BatchObserver.html
//! [`BlockBroadcaster`]: trait.BlockBroadcaster.html
//! [`PendingBatchesPool`]: batch_pool/struct.PendingBatchesPool.html

pub mod batch_injector;
pub mod batch_pool;
mod batch_submitter;
mod builder;
pub mod chain_head_lock;
mod error;
#[cfg(feature = "pending-batch-queue")]
pub mod pending_batch_queue;

use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::{
    mpsc::{channel, Receiver, Sender},
    Arc, Mutex, RwLock,
};
use std::thread;

use cylinder::Signer;
use transact::{
    execution::executor::ExecutionTaskSubmitter,
    protocol::{
        batch::{Batch, BatchPair},
        receipt::TransactionResult,
    },
    scheduler::{BatchExecutionResult, Scheduler, SchedulerError, SchedulerFactory},
    state::Write as _,
};

use crate::hashlib::sha256_digest_strs;
use crate::journal::{
    block_manager::{BlockManager, BlockManagerError, BlockRef},
    commit_store::CommitStore,
    validation_rule_enforcer::{ValidationRuleEnforcer, ValidationRuleEnforcerError},
};
use crate::permissions::verifier::PermissionVerifier;
use crate::protocol::block::{BlockBuilder, BlockPair};
use crate::state::{
    identity_view::IdentityView, merkle::CborMerkleState, settings_view::SettingsView,
    state_view_factory::StateViewFactory,
};

use batch_injector::BatchInjectorFactory;
use batch_pool::PendingBatchesPool;
use batch_submitter::BackpressureMessage;
pub use batch_submitter::{BatchSubmitter, BatchSubmitterError};
pub use builder::BlockPublisherBuilder;
pub use error::{
    BlockCancellationError, BlockCompletionError, BlockInitializationError, BlockPublisherError,
};

/// The name of the setting where the "max batches per block" value is stored
const MAX_BATCHES_PER_BLOCK_SETTING: &str = "sawtooth.publisher.max_batches_per_block";

/// Constructs and publishes blocks
pub struct BlockPublisher {
    /// Creates `BatchInjector`s that will be used to inject batches into published blocks
    batch_injector_factory: Box<dyn BatchInjectorFactory>,
    /// Used to submit batches. The publisher only has one submitter.
    batch_submitter: Option<BatchSubmitter>,
    /// Broadcasts blocks to the network when completed
    block_broadcaster: Box<dyn BlockBroadcaster>,
    /// The block manager counts block references; the publisher uses it to create a reference to
    /// the in-progress block's previous block to ensure that it isn't dropped while building on top
    /// of it.
    block_manager: BlockManager,
    /// The in-progress block, if there is one. This is shared between the publisher itself and its
    /// background thread.
    candidate_block: Arc<Mutex<Option<CandidateBlock>>>,
    /// Used to prevent duplicate batches and transactions by verifying that they are not already
    /// committed in a previous block.
    commit_store: CommitStore,
    /// Executes transactions on behalf of `Scheduler`s
    execution_task_submitter: ExecutionTaskSubmitter,
    /// Used to send a shutdown signal to the publisher's background thread
    internal_sender: Sender<BlockPublisherMessage>,
    /// Used to wait for the publisher's background thread to shutdown
    internal_thread_handle: thread::JoinHandle<()>,
    /// Observers that will be notified when a transaction is invalid
    invalid_transaction_observers: Vec<Box<dyn InvalidTransactionObserver>>,
    /// Used to calculate the state root hash that results from executing all of the transactions in
    /// a block; the state root hash is added to the block when it is published.
    merkle_state: CborMerkleState,
    /// Bounded pool of pending batches to add to blocks. This is shared between the publisher
    /// itself and its background thread.
    pending_batches: Arc<RwLock<PendingBatchesPool>>,
    /// Creates schedulers that the publisher will use for executing transactions
    scheduler_factory: Box<dyn SchedulerFactory>,
    /// Used to sign blocks created by the publisher
    signer: Box<dyn Signer>,
    /// Creates views of state that are used by the publisher to check configuration settings
    state_view_factory: StateViewFactory,
}

impl BlockPublisher {
    /// Creates a new [`BlockPublisherBuilder`](struct.BlockPublisherBuilder.html) for constructing
    /// a block publisher
    pub fn builder() -> BlockPublisherBuilder {
        BlockPublisherBuilder::new()
    }

    /// Removes and returns the [`BatchSubmitter`](struct.BatchSubmitter.html) from the block
    /// publisher
    ///
    /// The publisher only has one submitter, so it can only be taken once. The first call will
    /// return the submitter; subsequent calls will return `None`.
    pub fn take_batch_submitter(&mut self) -> Option<BatchSubmitter> {
        self.batch_submitter.take()
    }

    pub fn get_chain_head_lock(&self) -> chain_head_lock::ChainHeadLock {
        chain_head_lock::ChainHeadLock::new(Arc::clone(&self.pending_batches))
    }

    /// Checks if the batch with the given ID is in the publisher's pending batches pool
    pub fn has_batch(&self, batch_id: &str) -> Result<bool, BlockPublisherError> {
        self.pending_batches
            .read()
            .map(|pending_batches| pending_batches.contains(batch_id))
            .map_err(|_| BlockPublisherError::Internal("Pending batches lock poisoned".into()))
    }

    /// Notifies the block publisher that there is a new chain head
    ///
    /// The block publisher uses the provided chain head and committed/uncommitted batches to update
    /// the pending batches pool.
    ///
    /// # Arguments
    ///
    /// * `new_chain_head` - The block that was just committed as the chain head
    /// * `committed_batches` - All batches in the new chain that were not in the previous chain
    /// * `uncommitted_batches` - All batches that were in the old chain but are not in the new one
    fn on_chain_updated(
        pending_batches: &mut PendingBatchesPool,
        new_chain_head: BlockPair,
        committed_batches: Vec<BatchPair>,
        uncommitted_batches: Vec<BatchPair>,
    ) {
        // Use the new chain head as a sample for calculating the upper bound of the batch pool
        pending_batches.update_limit(new_chain_head.block().batches().len());
        // Rebuild the pending batch pool based on the batches that were committed and uncommitted
        pending_batches.rebuild(Some(committed_batches), Some(uncommitted_batches));
    }

    /// Starts a new block on top of an existing block and schedules as many transactions as the
    /// block and pending batches pool allow
    ///
    /// # Arguments
    ///
    /// * `previous_block` - The block that the new one will be built on top of
    ///
    /// # Errors
    ///
    /// * Returns a `BlockInProgress` error if there is already an in-progress block
    /// * Returns a `MissingPredecessor` error if the previous block is not known to the
    ///   [`BlockManager`](../block_manager/struct.BlockManager.html)
    pub fn initialize_block(&self, previous_block: BlockPair) -> Result<(), BlockPublisherError> {
        let mut candidate_block = self
            .candidate_block
            .lock()
            .map_err(|_| BlockPublisherError::Internal("Candidate block lock poisoned".into()))?;

        if candidate_block.is_some() {
            Err(BlockInitializationError::BlockInProgress.into())
        } else {
            *candidate_block = Some(self.new_candidate_block(previous_block)?);
            Ok(())
        }
    }

    /// Completes the in-progress block and returns a summary of the batches in the block
    ///
    /// Any batches that were scheduled but have not yet been executed will be descheduled.
    ///
    /// # Errors
    ///
    /// * Returns a `BlockNotInitialized` error if there is no in-progress block
    /// * Returns a `BlockEmpty` error if there are no valid, non-injected batches in the block. In
    ///   this case, the block is not completed and any unexecuted batches are added back to the
    ///   scheduler; the caller will need to wait for these batches to be executed or new batches
    ///   to submitted and executed before they can successfully summarize the block.
    pub fn summarize_block(&self) -> Result<Vec<u8>, BlockPublisherError> {
        let mut candidate_block = self
            .candidate_block
            .lock()
            .map_err(|_| BlockPublisherError::Internal("Candidate block lock poisoned".into()))?;

        if let Some(candidate_block) = candidate_block.as_mut() {
            self.complete_candidate_block(candidate_block)?;
            candidate_block.summary.clone().ok_or_else(|| {
                BlockPublisherError::Internal(
                    "Candidate block completed but summary not set".into(),
                )
            })
        } else {
            Err(BlockCompletionError::BlockNotInitialized.into())
        }
    }

    /// Completes the in-progress block, broadcasts it to the network, and returns its ID
    ///
    /// Any batches that were scheduled but have not yet been executed will be descheduled.
    ///
    /// # Errors
    ///
    /// * Returns a `BlockNotInitialized` error if there is no in-progress block
    /// * Returns a `BlockEmpty` error if there are no valid, non-injected batches in the block. In
    ///   this case, the block is not completed and any unexecuted batches are added back to the
    ///   scheduler; the caller will need to wait for these batches to be executed or new batches
    ///   to be submitted and executed before they can successfully finalize the block.
    pub fn finalize_block(&self, consensus_data: Vec<u8>) -> Result<String, BlockPublisherError> {
        let mut candidate_block_opt = self
            .candidate_block
            .lock()
            .map_err(|_| BlockPublisherError::Internal("Candidate block lock poisoned".into()))?;

        if let Some(mut candidate_block) = candidate_block_opt.take() {
            match self.complete_candidate_block(&mut candidate_block) {
                Ok(_) => {}
                Err(err) => {
                    // If the block was just empty, put it back so the caller can try again later
                    if let BlockPublisherError::BlockCompletionFailed(
                        BlockCompletionError::BlockEmpty,
                    ) = err
                    {
                        candidate_block_opt.replace(candidate_block);
                    }
                    return Err(err);
                }
            }

            // Remove all batches from the pool that were executed for this block
            let executed_batch_ids = candidate_block
                .valid_batches
                .iter()
                .chain(candidate_block.invalid_batches.iter())
                .map(|batch| batch.header_signature())
                .collect::<HashSet<_>>();
            self.pending_batches
                .write()
                .map_err(|_| BlockPublisherError::Internal("Pending Batches lock poisoned".into()))?
                .update(executed_batch_ids);

            let state_root_hash = candidate_block.state_root_hash.ok_or_else(|| {
                BlockPublisherError::Internal(
                    "Candidate block completed but state root hash not set".into(),
                )
            })?;

            let block = BlockBuilder::new()
                .with_block_num(candidate_block.previous_block.header().block_num() + 1)
                .with_previous_block_id(
                    candidate_block
                        .previous_block
                        .block()
                        .header_signature()
                        .to_string(),
                )
                .with_consensus(consensus_data)
                .with_state_root_hash(hex::decode(state_root_hash)?)
                .with_batches(candidate_block.valid_batches)
                .build_pair(&*self.signer)?;

            let block_id = block.block().header_signature().to_string();

            let publishing_details = BlockPublishingDetails {
                injected_batch_ids: candidate_block.injected_batch_ids.into_iter().collect(),
            };

            self.block_broadcaster
                .broadcast(block, publishing_details)?;

            Ok(block_id)
        } else {
            Err(BlockCompletionError::BlockNotInitialized.into())
        }
    }

    /// Aborts the in-progress block
    ///
    /// # Errors
    ///
    /// * Returns a `BlockNotInitialized` error if there is no in-progress block
    pub fn cancel_block(&self) -> Result<(), BlockPublisherError> {
        let mut candidate_block = self
            .candidate_block
            .lock()
            .map_err(|_| BlockPublisherError::Internal("Candidate block lock poisoned".into()))?;

        if let Some(mut candidate_block) = candidate_block.take() {
            candidate_block.scheduler.finalize()?;
            candidate_block.scheduler.cancel()?;
            Ok(())
        } else {
            Err(BlockCancellationError::BlockNotInitialized.into())
        }
    }

    /// Returns a `ShutdownSignaler` that can be used to shutdown the block publisher
    pub fn shutdown_signaler(&self) -> ShutdownSignaler {
        ShutdownSignaler {
            sender: self.internal_sender.clone(),
        }
    }

    /// Blocks until the block publisher has shutdown
    ///
    /// This is meant to allow one process to shutdown the block publisher via the shutdown handle
    /// while another process waits for that process to complete.
    pub fn await_shutdown(self) {
        debug!("Awaiting shutdown of block publisher...");
        if let Err(err) = self.internal_thread_handle.join() {
            error!(
                "Block publisher thread did not shutdown correctly: {:?}",
                err
            );
        }
    }

    /// Creates a new candidate block on top of an existing block and schedules as many transactions
    /// as the block and pending batches pool allow
    ///
    /// # Arguments
    ///
    /// * `previous_block` - The block that the new one will be built on top of
    ///
    /// # Errors
    ///
    /// * Returns a `MissingPredecessor` error if the previous block is not known to the
    ///   [`BlockManager`](../block_manager/struct.BlockManager.html)
    fn new_candidate_block(
        &self,
        previous_block: BlockPair,
    ) -> Result<CandidateBlock, BlockPublisherError> {
        // Initialize the scheduler
        let mut scheduler = self
            .scheduler_factory
            .create_scheduler(hex::encode(previous_block.header().state_root_hash()))?;
        let (sender, receiver) = channel();
        scheduler.set_result_callback(Box::new(move |batch_result| {
            if sender.send(batch_result).is_err() {
                warn!("Batch execution result receiver dropped while sending results");
            }
        }))?;
        self.execution_task_submitter
            .submit(scheduler.take_task_iterator()?, scheduler.new_notifier()?)?;

        // Initialize the candidate block
        let settings_view = self
            .state_view_factory
            .create_view::<SettingsView>(previous_block.header().state_root_hash())?;
        let max_batches_per_block = settings_view
            .get_setting_u32(MAX_BATCHES_PER_BLOCK_SETTING, None)?
            .map(|i| i as usize);
        let validation_rule_enforcer =
            ValidationRuleEnforcer::new(&settings_view, self.signer.public_key()?.into_bytes())
                .map_err(|err| BlockPublisherError::Internal(err.to_string()))?;
        let identity_view = self
            .state_view_factory
            .create_view::<IdentityView>(previous_block.header().state_root_hash())?;
        let permission_verifier = PermissionVerifier::new(Box::new(identity_view));
        let previous_block_reference = self
            .block_manager
            .ref_block(previous_block.block().header_signature())
            .map_err(|err| match err {
                BlockManagerError::UnknownBlock => {
                    BlockInitializationError::MissingPredecessor.into()
                }
                err => BlockPublisherError::Internal(err.to_string()),
            })?;
        let mut candidate_block = CandidateBlock::new(
            receiver,
            max_batches_per_block,
            permission_verifier,
            previous_block,
            previous_block_reference,
            scheduler,
            validation_rule_enforcer,
        );

        // Get batches to inject at the beginning of the block and schedule them
        let injected_batches = self
            .batch_injector_factory
            .create_injectors(&candidate_block.previous_block)?
            .into_iter()
            .try_fold::<_, _, Result<_, BlockPublisherError>>(vec![], |mut batches, injector| {
                for batch in injector.block_start(&candidate_block.previous_block)? {
                    candidate_block
                        .injected_batch_ids
                        .insert(batch.batch().header_signature().to_string());
                    batches.push(batch);
                }
                Ok(batches)
            })?;
        schedule_batches(injected_batches, &self.commit_store, &mut candidate_block)?;

        // Schedule as many batches as the candidate block and the number of batches in the pool
        // allow
        {
            let pending_batches = self.pending_batches.read().map_err(|_| {
                BlockPublisherError::Internal("Pending Batches lock poisoned".into())
            })?;
            for batch in pending_batches.iter() {
                if candidate_block.can_schedule_batch() {
                    schedule_batch(batch.clone(), &self.commit_store, &mut candidate_block)?;
                } else {
                    break;
                }
            }
        }

        Ok(candidate_block)
    }

    /// Completes the in-progress block and saves the results
    ///
    /// This is a helper method for the `summarize_block` and `finalize_block` methods that does the
    /// following:
    ///
    /// * Ensures the block is not empty and does not contain only injected batches; if the block
    ///   does not contain any non-injected batches, a `BlockEmpty` error is returned and the
    ///   candidate block is left uncompleted.
    /// * Deschedules any unexecuted batches
    /// * Finalizes the scheduler and collects all execution results
    /// * Computes and saves:
    ///   * All executed batches
    ///   * The resulting state root hash of the block
    ///   * The summary of the batches
    fn complete_candidate_block(
        &self,
        mut candidate_block: &mut CandidateBlock,
    ) -> Result<(), BlockPublisherError> {
        // Check if candidate block is already completed
        if candidate_block.summary.is_some() {
            return Ok(());
        }

        // If only injected batches (or no batches at all) have been scheduled, the block is empty
        // and the caller will just have to wait until new batches are submitted.
        if candidate_block
            .scheduled_batch_ids
            .iter()
            .all(|id| candidate_block.injected_batch_ids.contains(id))
        {
            return Err(BlockCompletionError::BlockEmpty.into());
        }

        candidate_block.scheduler.finalize()?;

        let mut batch_results = HashMap::new();

        // Wait for all injected batches to finish executing
        let mut injected_batch_ids = candidate_block.injected_batch_ids.clone();
        loop {
            match candidate_block.batch_results.recv() {
                Ok(Some(batch_result)) => {
                    injected_batch_ids.remove(batch_result.batch.batch().header_signature());

                    batch_results.insert(
                        batch_result.batch.batch().header_signature().to_string(),
                        batch_result,
                    );

                    if injected_batch_ids.is_empty() {
                        break;
                    }
                }
                Ok(None) => {
                    return Err(BlockPublisherError::Internal(
                        "Executor returned `None` result when more results were expected".into(),
                    ))
                }
                Err(_) => {
                    return Err(BlockPublisherError::Internal(
                        "Executor result sender disconnected when more results were expected"
                            .into(),
                    ))
                }
            }
        }

        // Wait for at least one non-injected batch to finish executing
        if batch_results.len() == candidate_block.injected_batch_ids.len() {
            // Only injected batches have finished executing, so wait for one more batch to execute
            match candidate_block.batch_results.recv() {
                Ok(Some(batch_result)) => {
                    batch_results.insert(
                        batch_result.batch.batch().header_signature().to_string(),
                        batch_result,
                    );
                }
                Ok(None) => {
                    return Err(BlockPublisherError::Internal(
                        "Executor returned `None` result when more results were expected".into(),
                    ))
                }
                Err(_) => {
                    return Err(BlockPublisherError::Internal(
                        "Executor result sender disconnected when more results were expected"
                            .into(),
                    ))
                }
            }
        }

        // Cancel the scheduler
        candidate_block.scheduler.cancel()?;

        // Collect any additional batch execution results that may have completed before the
        // scheduler was cancelled
        while let Ok(Some(batch_result)) = candidate_block.batch_results.recv() {
            batch_results.insert(
                batch_result.batch.batch().header_signature().to_string(),
                batch_result,
            );
        }

        // Parse the batch results into valid batches, invalid batches, and state changes
        let (valid_batches, invalid_batches, state_changes) = candidate_block
            // Get all batch execution results in the order they were originally scheduled in
            .scheduled_batch_ids
            .iter()
            .filter_map(|id| batch_results.remove(id))
            // Split batches and state changes
            .try_fold::<_, _, Result<_, BlockPublisherError>>(
                (vec![], vec![], vec![]),
                |(mut valid_batches, mut invalid_batches, mut state_changes),
                 BatchExecutionResult { batch, receipts }| {
                    let mut batch_is_invalid = false;

                    for receipt in receipts {
                        // If the transaction was invalid notify the observers and mark the batch
                        // as invalid
                        if let TransactionResult::Invalid {
                            error_message,
                            error_data,
                        } = receipt.transaction_result
                        {
                            for observer in &self.invalid_transaction_observers {
                                observer.notify_transaction_invalid(
                                    &receipt.transaction_id,
                                    &error_message,
                                    &error_data,
                                );
                            }
                            batch_is_invalid = true;
                        } else {
                            // Convert the receipt to a list of state changes and add them to the
                            // running list of state changes
                            Vec::try_from(receipt)
                                .map(|mut changes| state_changes.append(&mut changes))
                                .map_err(|err| {
                                    BlockPublisherError::Internal(format!(
                                        "Could not convert valid transaction receipt into state \
                                     changes: {}",
                                        err,
                                    ))
                                })?;
                        }
                    }

                    if batch_is_invalid {
                        debug!(
                            "Batch contains invalid transaction(s), dropping batch: {}",
                            batch.batch().header_signature()
                        );
                        invalid_batches.push(batch.take().0);
                    } else {
                        if batch.batch().trace() {
                            trace!("TRACE {}: executed", batch.batch().header_signature());
                        }
                        valid_batches.push(batch.take().0);
                    }

                    Ok((valid_batches, invalid_batches, state_changes))
                },
            )?;

        // If only injected batches (or no batches at all) were valid, the candidate block needs to
        // be restarted because this one was empty
        let num_non_injected_valid_batches = valid_batches
            .iter()
            .filter(|batch| {
                !candidate_block
                    .injected_batch_ids
                    .contains(batch.header_signature())
            })
            .count();
        if num_non_injected_valid_batches == 0 {
            let mut new_candidate_block =
                self.new_candidate_block(candidate_block.previous_block.clone())?;
            std::mem::swap(candidate_block, &mut new_candidate_block);
            return Err(BlockCompletionError::BlockEmpty.into());
        }

        // Save the batches that were executed and the resulting state root hash of the state
        // changes
        candidate_block.valid_batches = valid_batches;
        candidate_block.invalid_batches = invalid_batches;
        candidate_block.state_root_hash = Some(self.merkle_state.compute_state_id(
            &hex::encode(candidate_block.previous_block.header().state_root_hash()),
            &state_changes,
        )?);

        // Compute and save the summary
        let batch_ids = candidate_block
            .valid_batches
            .iter()
            .map(|batch| batch.header_signature().to_string())
            .collect::<Vec<_>>();
        candidate_block.summary = Some(sha256_digest_strs(&batch_ids));

        Ok(())
    }
}

/// Schedules the given batch
///
/// This is a convenience wrapper for `schedule_batches`
fn schedule_batch(
    batch: BatchPair,
    commit_store: &CommitStore,
    candidate_block: &mut CandidateBlock,
) -> Result<(), BlockPublisherError> {
    schedule_batches(vec![batch], commit_store, candidate_block)
}

/// Schedules the given batches
///
/// Performs the following actions:
/// * Verifies all batches' signers are permitted to make transactions
/// * Verifies that none of the batches or transactions are already committed or scheduled
/// * Verifies that all transaction dependencies are satisfied
/// * Verifies that the batches don't violate the block validation rules
/// * Schedules all batches for the candidate block
fn schedule_batches(
    batches: Vec<BatchPair>,
    commit_store: &CommitStore,
    candidate_block: &mut CandidateBlock,
) -> Result<(), BlockPublisherError> {
    'batch: for batch in batches {
        let batch_id = batch.batch().header_signature().to_string();
        let txn_ids = batch.header().transaction_ids().to_vec();

        // Verify that the batch signer is permitted to make transactions based on the current
        // state
        if !candidate_block
            .permission_verifier
            .is_batch_signer_authorized(&batch)?
        {
            debug!(
                "Batch signer is not authorized, dropping batch: {}",
                batch_id
            );
            continue;
        }

        // Validate that the batch is not a duplicate
        if candidate_block.scheduled_batch_ids.contains(&batch_id) {
            debug!(
                "Dropping duplicate of already-scheduled batch: {}",
                batch_id
            );
            continue;
        }
        if commit_store.contains_batch(&batch_id)? {
            debug!(
                "Dropping duplicate of already-committed batch: {}",
                batch_id
            );
            continue;
        }

        // Validate that no transactions are duplicates and that all dependencies are satisfied
        for (i, txn) in batch.batch().transactions().iter().enumerate() {
            if candidate_block.scheduled_txn_ids.contains(&txn_ids[i]) {
                debug!(
                    "Transaction already scheduled in another batch, rejecting \
                     batch/transaction: ({}, {})",
                    batch_id,
                    txn.header_signature(),
                );
                continue 'batch;
            }
            if commit_store.contains_transaction(txn.header_signature())? {
                debug!(
                    "Transaction already committed, rejecting batch/transaction: ({}, {})",
                    batch_id,
                    txn.header_signature(),
                );
                continue 'batch;
            }

            let txn_header = match txn.clone().into_pair() {
                Ok(txn_pair) => txn_pair.take().1,
                Err(_) => {
                    debug!(
                        "Invalid transaction header, rejecting batch/transaction: ({}, {})",
                        batch_id,
                        txn.header_signature(),
                    );
                    continue 'batch;
                }
            };
            for dep in txn_header.dependencies() {
                let dep_str = hex::encode(dep);
                if !candidate_block.scheduled_txn_ids.contains(dep)
                    && !commit_store.contains_transaction(&dep_str)?
                {
                    debug!(
                        "Missing dependency {}, rejecting batch/transaction: ({}, {})",
                        dep_str,
                        batch_id,
                        txn.header_signature(),
                    );
                    continue 'batch;
                }
            }
        }

        match candidate_block
            .validation_rule_enforcer
            .add_batches(vec![batch.batch()])
        {
            Ok(true) => {}
            Ok(false) => {
                debug!(
                    "Block validation rules violated, rejecting batch: {}",
                    batch_id
                );
                continue;
            }
            Err(ValidationRuleEnforcerError::InvalidBatches(_)) => {
                debug!("Rejecting invalid batch: {}", batch_id);
                continue;
            }
            Err(err) => return Err(BlockPublisherError::Internal(err.to_string())),
        }

        if batch.batch().trace() {
            trace!("TRACE {}: scheduling", batch_id);
        }

        match candidate_block.scheduler.add_batch(batch) {
            Ok(_) => {
                candidate_block.scheduled_batch_ids.push(batch_id);
                candidate_block
                    .scheduled_txn_ids
                    .extend(txn_ids.into_iter());
            }
            Err(SchedulerError::DuplicateBatch(_)) => {} // Ignore duplicate batch
            Err(err) => return Err(BlockPublisherError::Internal(err.to_string())),
        }
    }

    Ok(())
}

/// Runs the block publisher's background thread and returns a handle for joining the thread
///
/// The publisher's background thread is responsible for the following:
/// * Receiving batches from the batch submitter
/// * Adding new batches to the pending batches pool
/// * Notifying all batch observers about new batches
/// * Adding new batches to an in-progress block if there is one
/// * Notifyng the batch sender when the pending batches pool is full or unblocked
fn start_publisher_thread(
    backpressure_sender: Sender<BackpressureMessage>,
    batch_observers: Vec<Box<dyn BatchObserver>>,
    pending_batches: Arc<RwLock<PendingBatchesPool>>,
    candidate_block: Arc<Mutex<Option<CandidateBlock>>>,
    commit_store: CommitStore,
    state_view_factory: StateViewFactory,
    receiver: Receiver<BlockPublisherMessage>,
) -> std::io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name("Block Publisher".into())
        .spawn(move || {
            let pool_full = false;
            loop {
                match receiver.recv() {
                    Ok(BlockPublisherMessage::NewBatch(batch)) => {
                        // Verify that the batch signer is permitted
                        let chain_head = match commit_store.get_chain_head() {
                            Ok(chain_head) => chain_head,
                            Err(err) => {
                                error!("Failed to get chain head from commit store: {}", err);
                                continue;
                            }
                        };
                        let identity_view = match state_view_factory
                            .create_view::<IdentityView>(chain_head.header().state_root_hash())
                        {
                            Ok(identity_view) => identity_view,
                            Err(err) => {
                                error!("Failed to get identity view: {}", err);
                                continue;
                            }
                        };
                        let permission_verifier = PermissionVerifier::new(Box::new(identity_view));
                        match permission_verifier.is_batch_signer_authorized(&batch) {
                            Ok(true) => {}
                            Ok(false) => {
                                debug!(
                                    "Ignoring batch because signer is not authorized: {}",
                                    batch.batch().header_signature()
                                );
                                continue;
                            }
                            Err(err) => {
                                error!("Failed to perform batch permission verification: {}", err);
                                continue;
                            }
                        }

                        // Check if the batch is already committed
                        match commit_store.contains_batch(batch.batch().header_signature()) {
                            Ok(true) => {
                                debug!(
                                    "Ignoring batch that is already committed: {}",
                                    batch.batch().header_signature()
                                );
                                continue;
                            }
                            Ok(false) => {}
                            Err(err) => {
                                error!("Failed to check if batch is already committed: {}", err);
                                continue;
                            }
                        }

                        // Add the batch to the pool
                        match pending_batches.write() {
                            Ok(mut pending_batches) => {
                                // If the batch is already in the pool, don't do anything further
                                // with the batch
                                if !pending_batches.append(batch.clone()) {
                                    continue;
                                }

                                // Notify batch observers
                                for observer in &batch_observers {
                                    observer.notify_batch_pending(batch.batch());
                                }

                                // Notify the batch submitter if the batches pool has filled up or
                                // is open again
                                if !pool_full
                                    && pending_batches.is_full()
                                    && backpressure_sender
                                        .send(BackpressureMessage::PoolFull)
                                        .is_err()
                                {
                                    warn!("Batch submitter disconnected");
                                } else if pool_full
                                    && !pending_batches.is_full()
                                    && backpressure_sender
                                        .send(BackpressureMessage::PoolUnblocked)
                                        .is_err()
                                {
                                    warn!("Batch submitter disconnected");
                                }
                            }
                            Err(_) => {
                                error!("Pending batches pool lock poisoned");
                                break;
                            }
                        }

                        // If currently building a block and it's not already full, schedule the
                        // batch
                        match candidate_block.lock() {
                            Ok(mut candidate_block) => {
                                if let Some(candidate_block) = candidate_block.as_mut() {
                                    if candidate_block.can_schedule_batch() {
                                        if let Err(err) =
                                            schedule_batch(batch, &commit_store, candidate_block)
                                        {
                                            error!("Failed to schedule batch: {}", err);
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                error!("Candidate block lock poisoned");
                                break;
                            }
                        }
                    }
                    Ok(BlockPublisherMessage::Shutdown) => break,
                    Err(_) => {
                        warn!(
                            "All senders have disconnected; shutting down block publisher thread"
                        );
                        break;
                    }
                }
            }
        })
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
    /// Saves all batches that were valid and will be put in the block on finalization. This is
    /// populated when the block is completed (summarized/finalized).
    valid_batches: Vec<Batch>,
    /// Saves all batches that were invalid so they can be removed from the pending batches pool.
    /// This is populated when the block is completed (summarized/finalized).
    invalid_batches: Vec<Batch>,
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
            valid_batches: Default::default(),
            invalid_batches: Default::default(),
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

/// Sends a shutdown signal to the block publisher
#[derive(Clone)]
pub struct ShutdownSignaler {
    /// Sends messages to the block publisher's background thread
    sender: Sender<BlockPublisherMessage>,
}

impl ShutdownSignaler {
    /// Signals the block publisher to shutdown
    pub fn shutdown(self) {
        if self.sender.send(BlockPublisherMessage::Shutdown).is_err() {
            warn!("Block publisher is no longer running");
        }
    }
}

/// Messages sent to the the block publisher's background thread
#[derive(Debug)]
enum BlockPublisherMessage {
    /// A new batch has been submitted
    NewBatch(BatchPair),
    /// The block publisher should shutdown
    Shutdown,
}

impl From<BatchPair> for BlockPublisherMessage {
    fn from(batch: BatchPair) -> Self {
        Self::NewBatch(batch)
    }
}

/// Notified by the block publisher whenever a new batch is added to the pending batches pool
pub trait BatchObserver: Send + Sync {
    /// Notifies the observer of the given batch
    fn notify_batch_pending(&self, batch: &Batch);
}

/// Notified by the block publisher whenever an executed transaction is invalid
pub trait InvalidTransactionObserver: Send + Sync {
    /// Notifies the observer that the given transaction is invalid
    fn notify_transaction_invalid(
        &self,
        transaction_id: &str,
        error_message: &str,
        error_data: &[u8],
    );
}

/// Details about the block collected during the publishing process.
pub struct BlockPublishingDetails {
    /// The batch IDs of those that have been injected by publishing validator.
    injected_batch_ids: Vec<String>,
}

impl BlockPublishingDetails {
    /// Return the IDs of the batches that were injected during the publishing process.
    pub fn injected_batch_ids(&self) -> &[String] {
        &self.injected_batch_ids
    }
}

/// Broadcasts blocks to the network
pub trait BlockBroadcaster: Send {
    /// Broadcast the block to the network
    fn broadcast(
        &self,
        block: BlockPair,
        publishing_details: BlockPublishingDetails,
    ) -> Result<(), BlockPublisherError>;
}
