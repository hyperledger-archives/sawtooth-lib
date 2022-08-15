/*
 * Copyright 2020 Cargill Incorporated
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

//! Used to verify and rebuild missing state

use std::error::Error;
use std::sync::mpsc::{channel, Receiver, Sender};

use crate::{
    journal::commit_store::{ByHeightDirection, CommitStore, CommitStoreByHeightIterator},
    protocol::block::BlockPair,
    state::{
        merkle::CborMerkleState, settings_view::SettingsView, state_view_factory::StateViewFactory,
    },
};
use log::{error, info};

use crate::transact::{
    database::error::DatabaseError,
    execution::executor::ExecutionTaskSubmitter,
    protocol::batch::Batch,
    protocol::receipt::TransactionResult,
    scheduler::{BatchExecutionResult, SchedulerError, SchedulerFactory},
    state::{StateChange, Write},
};

/// Verify the state root hash of all blocks is in state and if not,
/// reconstruct the missing state. Assumes that there are no "holes" in
/// state, ie starting from genesis, state is present for all blocks up to some
/// point and then not at all.
pub fn verify_state(
    commit_store: &CommitStore,
    state_view_factory: &StateViewFactory,
    initial_state_root: &str,
    execution_task_submitter: &ExecutionTaskSubmitter,
    merkle_state: &CborMerkleState,
    scheduler_factory: &dyn SchedulerFactory,
) -> Result<(), StateVerificationError> {
    // check if we should do state verification
    let start =
        search_for_present_state_root(commit_store, state_view_factory, initial_state_root)?;

    let (start_block, prev_state_root) = match start {
        Some((block, state_root)) => (block, state_root),
        None => {
            info!("Skipping state verification: chain head's state root is present");
            return Ok(());
        }
    };

    info!(
        "Recomputing missing state from block {}",
        start_block.header().block_num()
    );

    let blocks = commit_store.get_block_by_height_iter(
        Some(start_block.header().block_num()),
        ByHeightDirection::Increasing,
    );

    process_blocks(
        &prev_state_root,
        blocks,
        execution_task_submitter,
        merkle_state,
        state_view_factory,
        scheduler_factory,
    )
}

/// Search through the blockstore and return a tuple containing:
///   - the first block with a missing state root
///   - the state root of that blocks predecessor
fn search_for_present_state_root(
    commit_store: &CommitStore,
    state_view_factory: &StateViewFactory,
    initial_state_root: &str,
) -> Result<Option<(BlockPair, String)>, StateVerificationError> {
    // If there is no chain to process, then we are done.
    let block = match commit_store.get_chain_head() {
        Ok(block) => block,
        Err(DatabaseError::NotFoundError(_)) => return Ok(None),
        Err(err) => {
            return Err(StateVerificationError::InvalidChainError(format!(
                "Unable to check for chain head: {}",
                err
            )))
        }
    };

    // Check the head first
    if state_view_factory
        .create_view::<SettingsView>(block.header().state_root_hash())
        .is_ok()
    {
        return Ok(None);
    }

    let mut previous_state_root = initial_state_root.to_string();
    for block in commit_store.get_block_by_height_iter(None, ByHeightDirection::Increasing) {
        if state_view_factory
            .create_view::<SettingsView>(block.header().state_root_hash())
            .is_err()
        {
            return Ok(Some((block, previous_state_root)));
        }
        previous_state_root = hex::encode(block.header().state_root_hash());
    }

    // This should never happen, since we already checked that the chain head
    // didn't have a state root
    Err(StateVerificationError::InvalidChainError(
        "Chain head state missing but all blocks had state root present".to_string(),
    ))
}

/// Interate through commited blocks and build the block's state if it is missing
fn process_blocks(
    initial_state_root: &str,
    blocks: CommitStoreByHeightIterator,
    execution_task_submitter: &ExecutionTaskSubmitter,
    merkle_state: &CborMerkleState,
    state_view_factory: &StateViewFactory,
    scheduler_factory: &dyn SchedulerFactory,
) -> Result<(), StateVerificationError> {
    let mut previous_state_root = initial_state_root.to_string();
    for block in blocks {
        info!("Verifying state for block {}", block.header().block_num());
        match state_view_factory.create_view::<SettingsView>(block.header().state_root_hash()) {
            Ok(_) => (),
            Err(_) => {
                // If creating the view fails, the root is missing so we should
                // recompute it and verify it
                execute_batches(
                    &previous_state_root,
                    execution_task_submitter,
                    merkle_state,
                    block.block().batches().to_vec(),
                    scheduler_factory,
                    hex::encode(block.header().state_root_hash()),
                )?
            }
        }

        previous_state_root = hex::encode(block.header().state_root_hash());
    }
    Ok(())
}

/// Used to combine errors and Batch results
#[derive(Debug)]
enum SchedulerEvent {
    Result(BatchExecutionResult),
    Error(SchedulerError),
    Complete,
}

/// Validate all the batches provided. If the state hash matches the block state hash,
/// commit changes.
fn execute_batches(
    previous_state_root: &str,
    execution_task_submitter: &ExecutionTaskSubmitter,
    merkle_state: &CborMerkleState,
    batches: Vec<Batch>,
    scheduler_factory: &dyn SchedulerFactory,
    expected_state_root_hash: String,
) -> Result<(), StateVerificationError> {
    let mut scheduler = scheduler_factory.create_scheduler(previous_state_root.to_string())?;
    let (result_tx, result_rx): (Sender<SchedulerEvent>, Receiver<SchedulerEvent>) = channel();
    let error_tx = result_tx.clone();
    // Add callback to convert batch result option to scheduler event
    scheduler.set_result_callback(Box::new(move |batch_result| {
        let scheduler_event = match batch_result {
            Some(result) => SchedulerEvent::Result(result),
            None => SchedulerEvent::Complete,
        };
        if result_tx.send(scheduler_event).is_err() {
            error!("Unable to send batch result; receiver must have dropped");
        }
    }))?;
    // add callback to convert error into scheduler event
    scheduler.set_error_callback(Box::new(move |err| {
        if error_tx.send(SchedulerEvent::Error(err)).is_err() {
            error!("Unable to send scheduler error; receiver must have dropped");
        }
    }))?;

    for batch in batches.into_iter() {
        let batch_pair = batch.into_pair().map_err(|err| {
            StateVerificationError::ExecutionError(format!(
                "Unable to convert batch into BatchPair: {:?}",
                err
            ))
        })?;

        scheduler.add_batch(batch_pair).map_err(|err| {
            StateVerificationError::ExecutionError(format!(
                "While adding a batch to the schedule: {:?}",
                err
            ))
        })?;
    }

    scheduler.finalize().map_err(|err| {
        StateVerificationError::ExecutionError(format!(
            "During call to scheduler.finalize: {:?}",
            err
        ))
    })?;

    execution_task_submitter
        .submit(scheduler.take_task_iterator()?, scheduler.new_notifier()?)
        .map_err(|err| {
            StateVerificationError::ExecutionError(format!(
                "During call to ExecutionTaskSubmitter.submit: {}",
                err
            ))
        })?;

    let mut execution_results = vec![];
    loop {
        match result_rx.recv() {
            Ok(SchedulerEvent::Result(result)) => execution_results.push(result),
            Ok(SchedulerEvent::Complete) => break,
            Ok(SchedulerEvent::Error(err)) => {
                return Err(StateVerificationError::ExecutionError(format!(
                    "During execution: {:?}",
                    err
                )))
            }
            Err(err) => {
                return Err(StateVerificationError::ExecutionError(format!(
                    "Error while trying to receive scheduler event: {:?}",
                    err
                )))
            }
        }
    }

    let mut changes = vec![];
    for batch_result in execution_results {
        if !batch_result.receipts.is_empty() {
            for receipt in batch_result.receipts {
                match receipt.transaction_result.clone() {
                    TransactionResult::Invalid { .. } => {
                        return Err(StateVerificationError::ExecutionError(format!(
                            "Failed validation: batch {} was invalid",
                            batch_result.batch.batch().header_signature(),
                        )));
                    }
                    TransactionResult::Valid { state_changes, .. } => {
                        changes.append(
                            &mut state_changes.into_iter().map(StateChange::from).collect(),
                        );
                    }
                };
            }
        } else {
            return Err(StateVerificationError::ExecutionError(format!(
                "Failed validation: batch {} did not have transaction results",
                &batch_result.batch.batch().header_signature(),
            )));
        };
    }

    let new_root = merkle_state
        .compute_state_id(&previous_state_root.to_string(), &changes)
        .map_err(|err| {
            StateVerificationError::ExecutionError(format!(
                "During ending state hash calculation: {:?}",
                err
            ))
        })?;

    if new_root != expected_state_root_hash {
        return Err(StateVerificationError::InvalidChainError(format!(
            "Computed state root {} does not match state root in block {}",
            new_root, expected_state_root_hash
        )));
    };

    merkle_state
        .commit(&previous_state_root.to_string(), &changes)
        .map_err(|err| {
            StateVerificationError::ExecutionError(format!(
                "During ending state hash calculation: {:?}",
                err
            ))
        })?;

    Ok(())
}

/// Represents errors raised during state verification
#[derive(Debug)]
pub enum StateVerificationError {
    /// Unable to rebuild state because of error during batch execution
    ExecutionError(String),
    /// The chain in the blockstore is not valid
    InvalidChainError(String),
}

impl Error for StateVerificationError {}

impl std::fmt::Display for StateVerificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            StateVerificationError::ExecutionError(ref msg) => {
                write!(f, "Unable to validate batches: {}", msg)
            }
            StateVerificationError::InvalidChainError(ref msg) => {
                write!(f, "Unable to check chain: {}", msg)
            }
        }
    }
}

impl From<SchedulerError> for StateVerificationError {
    fn from(other: SchedulerError) -> Self {
        match other {
            SchedulerError::DuplicateBatch(ref batch_id) => StateVerificationError::ExecutionError(
                format!("Validation failure, duplicate batch {}", batch_id),
            ),
            error => StateVerificationError::ExecutionError(error.to_string()),
        }
    }
}
