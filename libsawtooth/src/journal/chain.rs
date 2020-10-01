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

#![allow(unknown_lints)]

use std::collections::HashMap;
use std::io;
use std::marker::Send;
use std::marker::Sync;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::thread;
use std::time::Duration;

use transact::protocol::batch::BatchPair;
use transact::state::Write;

use crate::{
    consensus::notifier::ConsensusNotifier,
    journal::publisher::chain_head_lock::ChainHeadLock,
    journal::{
        block_manager::{BlockManager, BlockManagerError, BlockRef},
        block_validator::{
            BlockValidationResult, BlockValidationResultStore, BlockValidator, ValidationError,
        },
        block_wrapper::BlockStatus,
        chain_id_manager::ChainIdManager,
        fork_cache::ForkCache,
        NULL_BLOCK_IDENTIFIER,
    },
    protocol::block::BlockPair,
    protos::transaction_receipt::TransactionReceipt,
    scheduler::TxnExecutionResult,
    state::merkle::CborMerkleState,
    state::state_pruning_manager::StatePruningManager,
};

pub const COMMIT_STORE: &str = "commit_store";

#[derive(Debug)]
pub enum ChainReadError {
    GeneralReadError(String),
}

impl std::error::Error for ChainReadError {}

impl std::fmt::Display for ChainReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ChainReadError::GeneralReadError(msg) => write!(f, "unable to read chain: {}", msg),
        }
    }
}

pub trait ChainReader: Send + Sync {
    fn chain_head(&self) -> Result<Option<BlockPair>, ChainReadError>;
    fn count_committed_transactions(&self) -> Result<usize, ChainReadError>;
    fn get_block_by_block_num(&self, block_num: u64) -> Result<Option<BlockPair>, ChainReadError>;
    fn get_block_by_block_id(&self, block_id: &str) -> Result<Option<BlockPair>, ChainReadError>;
}

const RECV_TIMEOUT_MILLIS: u64 = 100;

#[derive(Debug)]
pub enum ChainControllerError {
    QueueRecvError(RecvError),
    ChainIdError(io::Error),
    ChainUpdateError(String),
    ChainReadError(ChainReadError),
    ForkResolutionError(String),
    BlockValidationError(ValidationError),
    BrokenQueue,
    ConsensusError(String),
}

impl From<RecvError> for ChainControllerError {
    fn from(err: RecvError) -> Self {
        ChainControllerError::QueueRecvError(err)
    }
}

impl From<io::Error> for ChainControllerError {
    fn from(err: io::Error) -> Self {
        ChainControllerError::ChainIdError(err)
    }
}

impl From<ValidationError> for ChainControllerError {
    fn from(err: ValidationError) -> Self {
        ChainControllerError::BlockValidationError(err)
    }
}

impl From<ChainReadError> for ChainControllerError {
    fn from(err: ChainReadError) -> Self {
        ChainControllerError::ChainReadError(err)
    }
}

impl From<ForkResolutionError> for ChainControllerError {
    fn from(err: ForkResolutionError) -> Self {
        ChainControllerError::ForkResolutionError(err.0)
    }
}

impl From<BlockManagerError> for ChainControllerError {
    fn from(err: BlockManagerError) -> Self {
        ChainControllerError::ChainUpdateError(format!("{:?}", err))
    }
}

pub trait ChainObserver: Send + Sync {
    fn chain_update(&mut self, block: &BlockPair, receipts: &[TransactionReceipt]);
}

/// Holds the results of Block Validation.
struct ForkResolutionResult<'a> {
    pub block: &'a BlockPair,
    pub chain_head: Option<&'a BlockPair>,

    pub new_chain: Vec<BlockPair>,
    pub current_chain: Vec<BlockPair>,

    pub committed_batches: Vec<BatchPair>,
    pub uncommitted_batches: Vec<BatchPair>,

    pub transaction_count: usize,
}

/// Indication that an error occured during fork resolution.
#[derive(Debug)]
struct ForkResolutionError(String);

struct ChainControllerState {
    block_manager: BlockManager,
    block_references: HashMap<String, BlockRef>,
    chain_reader: Box<dyn ChainReader>,
    chain_head: Option<BlockRef>,
    chain_id_manager: ChainIdManager,
    observers: Vec<Box<dyn ChainObserver>>,
    state_pruning_manager: StatePruningManager,
    fork_cache: ForkCache,
    merkle_state: CborMerkleState,
    initial_state_hash: String,
}

impl ChainControllerState {
    fn build_fork<'a>(
        &mut self,
        block: &'a BlockPair,
        chain_head: &'a BlockPair,
    ) -> Result<ForkResolutionResult<'a>, ChainControllerError> {
        let new_block = block.clone();

        let new_chain = self
            .block_manager
            .branch_diff(
                new_block.block().header_signature(),
                chain_head.block().header_signature(),
            )?
            .collect::<Vec<_>>();
        let current_chain = self
            .block_manager
            .branch_diff(
                chain_head.block().header_signature(),
                new_block.block().header_signature(),
            )?
            .collect::<Vec<_>>();

        let committed_batches = new_chain
            .iter()
            .try_fold::<_, _, Result<_, ChainControllerError>>(vec![], |mut batches, block| {
                let mut new_batches = block
                    .block()
                    .batches()
                    .iter()
                    .cloned()
                    .map(|batch| batch.into_pair())
                    .collect::<Result<_, _>>()
                    .map_err(|err| {
                        ChainControllerError::ChainUpdateError(format!(
                            "Failed to parse batches from committed block: {}",
                            err,
                        ))
                    })?;
                batches.append(&mut new_batches);
                Ok(batches)
            })?;
        let uncommitted_batches = current_chain
            .iter()
            .try_fold::<_, _, Result<_, ChainControllerError>>(vec![], |mut batches, block| {
                let mut new_batches = block
                    .block()
                    .batches()
                    .iter()
                    .cloned()
                    .map(|batch| batch.into_pair())
                    .collect::<Result<_, _>>()
                    .map_err(|err| {
                        ChainControllerError::ChainUpdateError(format!(
                            "Failed to parse batches from uncommitted block: {}",
                            err,
                        ))
                    })?;
                batches.append(&mut new_batches);
                Ok(batches)
            })?;

        let transaction_count = committed_batches.iter().fold(0, |mut txn_count, batch| {
            txn_count += batch.batch().transactions().len();
            txn_count
        });

        let result = ForkResolutionResult {
            block,
            chain_head: Some(chain_head),
            new_chain,
            current_chain,
            committed_batches,
            uncommitted_batches,
            transaction_count,
        };

        info!(
            "Building fork resolution for chain head '{}' against new block '{}'",
            chain_head.block(),
            new_block.block()
        );
        if let Some(prior_heads_successor) = result.new_chain.get(0) {
            if prior_heads_successor.header().previous_block_id()
                != chain_head.block().header_signature()
            {
                counter!("chain.ChainController.chain_head_moved_to_fork_count", 1);
            }
        }

        Ok(result)
    }

    fn check_chain_head_updated(
        &self,
        expected_chain_head: &BlockPair,
        block: &BlockPair,
    ) -> Result<bool, ChainControllerError> {
        let actual_chain_head = self.chain_reader.chain_head()?;

        let chain_head_updated = actual_chain_head.as_ref().map(|actual_chain_head| {
            actual_chain_head.block().header_signature()
                != expected_chain_head.block().header_signature()
        });

        if chain_head_updated.unwrap_or(false) {
            warn!(
                "Chain head updated from {} to {} while resolving \
                 fork for block {}. Reprocessing resolution.",
                expected_chain_head.block(),
                actual_chain_head.as_ref().unwrap().block(),
                block.block()
            );
            return Ok(true);
        }

        Ok(false)
    }
}

pub struct ChainController {
    state: Arc<RwLock<ChainControllerState>>,
    stop_handle: Arc<Mutex<Option<ChainThreadStopHandle>>>,

    consensus_notifier: Arc<dyn ConsensusNotifier>,
    block_validator: Option<BlockValidator>,
    block_validation_results: BlockValidationResultStore,

    // Request Queue
    request_sender: Option<Sender<ChainControllerRequest>>,

    state_pruning_block_depth: u32,
    chain_head_lock: ChainHeadLock,
}

impl ChainController {
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        block_manager: BlockManager,
        block_validator: BlockValidator,
        chain_reader: Box<dyn ChainReader>,
        chain_head_lock: ChainHeadLock,
        block_validation_results: BlockValidationResultStore,
        consensus_notifier: Box<dyn ConsensusNotifier>,
        data_dir: String,
        state_pruning_block_depth: u32,
        observers: Vec<Box<dyn ChainObserver>>,
        state_pruning_manager: StatePruningManager,
        fork_cache_keep_time: Duration,
        merkle_state: CborMerkleState,
        initial_state_hash: String,
    ) -> Self {
        let mut chain_controller = ChainController {
            state: Arc::new(RwLock::new(ChainControllerState {
                block_manager,
                block_references: HashMap::new(),
                chain_reader,
                chain_id_manager: ChainIdManager::new(data_dir),
                observers,
                chain_head: None,
                state_pruning_manager,
                fork_cache: ForkCache::new(fork_cache_keep_time),
                merkle_state,
                initial_state_hash,
            })),
            block_validator: Some(block_validator),
            block_validation_results,
            stop_handle: Arc::new(Mutex::new(None)),
            request_sender: None,
            state_pruning_block_depth,
            chain_head_lock,
            consensus_notifier: Arc::from(consensus_notifier),
        };

        chain_controller.initialize_chain_head();

        chain_controller
    }

    pub fn chain_head(&self) -> Option<BlockPair> {
        let state = self
            .state
            .read()
            .expect("No lock holder should have poisoned the lock");

        if let Some(head) = &state.chain_head {
            self.get_block(head.block_id())
        } else {
            None
        }
    }

    pub fn block_validation_result(&self, block_id: &str) -> Option<BlockValidationResult> {
        self.block_validation_results.get(block_id).or_else(|| {
            if self
                .state
                .read()
                .expect("Unable to acquire read lock, due to poisoning")
                .chain_reader
                .get_block_by_block_id(block_id)
                .expect("ChainReader errored reading from the database")
                .is_some()
            {
                let result = BlockValidationResult {
                    block_id: block_id.into(),
                    execution_results: vec![],
                    num_transactions: 0,
                    status: BlockStatus::Valid,
                    state_changes: vec![],
                };
                return Some(result);
            }
            None
        })
    }

    pub fn validate_block(&self, block: &BlockPair) {
        // If there is already a result for this block, no need to validate it
        if self
            .block_validation_results
            .get(block.block().header_signature())
            .is_some()
        {
            return;
        }

        if self.request_sender.is_some() {
            // Create block validation result, marked as in-validation
            self.block_validation_results.insert(BlockValidationResult {
                block_id: block.block().header_signature().to_string(),
                execution_results: vec![],
                num_transactions: 0,
                status: BlockStatus::InValidation,
                state_changes: vec![],
            });

            // Submit for validation
            let sender = self.request_sender.as_ref().expect(
                "Attempted to submit block for validation before starting the chain controller",
            );
            if let Err(err) =
                self.request_sender
                    .as_ref()
                    .unwrap()
                    .send(ChainControllerRequest::ValidateBlock {
                        blocks: vec![block.clone()],
                        response_sender: sender.clone(),
                    })
            {
                error!("Unable to submit block for validation: {}", err);
            }
        } else {
            debug!(
                "Attempting to submit block for validation {} before chain controller is started; \
                Ignoring",
                block.block().header_signature()
            );
        }
    }

    pub fn ignore_block(&self, block: &BlockPair) {
        let mut state = self
            .state
            .write()
            .expect("No lock holder should have poisoned the lock");

        // Drop Ref-C: Consensus is not interested in this block anymore
        match state
            .block_references
            .remove(block.block().header_signature())
        {
            Some(_) => info!("Ignored block {}", block.block()),
            None => debug!(
                "Could not ignore block {}; consensus has already decided on it",
                block.block().header_signature()
            ),
        }
    }

    pub fn fail_block(&self, block: &BlockPair) {
        let mut state = self
            .state
            .write()
            .expect("No lock holder should have poisoned the lock");

        // Drop Ref-C: Consensus is not interested in this block anymore
        match state
            .block_references
            .remove(block.block().header_signature())
        {
            Some(_) => {
                self.block_validation_results
                    .fail_block(block.block().header_signature());
                info!("Failed block {}", block.block());
            }
            None => debug!(
                "Could not fail block {}; consensus has already decided on it",
                block.block().header_signature()
            ),
        }
    }

    fn get_block(&self, block_id: &str) -> Option<BlockPair> {
        let state = self
            .state
            .read()
            .expect("No lock holder should have poisoned the lock");

        state.block_manager.get(&[block_id]).next().unwrap_or(None)
    }

    pub fn commit_block(&self, block: BlockPair) {
        if let Some(sender) = self.request_sender.as_ref() {
            if let Err(err) = sender.send(ChainControllerRequest::CommitBlock(block)) {
                error!("Unable to add block to block queue: {}", err);
            }
        } else {
            debug!(
                "Attempting to commit block {} before chain controller is started; Ignoring",
                block.block()
            );
        }
    }

    pub fn queue_block(&self, block_id: &str) {
        if self.request_sender.is_some() {
            if let Err(err) = self
                .request_sender
                .as_ref()
                .unwrap()
                .send(ChainControllerRequest::QueueBlock(block_id.into()))
            {
                error!("Unable to add block to block queue: {}", err);
            }
        } else {
            debug!(
                "Attempting to queue block {} before chain controller is started; Ignoring",
                block_id
            );
        }
    }

    fn initialize_chain_head(&mut self) {
        // we need to check to see if a genesis block was created and stored,
        // before this controller was started
        let mut state = self
            .state
            .write()
            .expect("No lock holder should have poisoned the lock");

        let chain_head = state
            .chain_reader
            .chain_head()
            .expect("Invalid block store. Head of the block chain cannot be determined");

        if let Some(chain_head) = chain_head {
            info!(
                "Chain controller initialized with chain head: {}",
                chain_head.block()
            );

            // Create Ref-C: External reference for the chain head will be held until it is
            // superceded by a new chain head.
            state.chain_head = Some(
                state
                    .block_manager
                    .ref_block(chain_head.block().header_signature())
                    .expect("Failed to reference chain head"),
            );

            gauge!(
                "chain.ChainController.block_num",
                chain_head.header().block_num() as i64
            );

            let mut guard = self.chain_head_lock.acquire();

            guard.notify_on_chain_updated(chain_head, vec![], vec![]);
        }
    }

    pub fn start(&mut self) {
        // duplicating what happens at the constructor time, but there are multiple
        // points in the lifetime of this object where the value of the
        // block store's chain head may have been set
        self.initialize_chain_head();
        let mut stop_handle = self.stop_handle.lock().unwrap();
        if stop_handle.is_none() {
            let (request_sender, request_receiver) = channel();

            self.request_sender = Some(request_sender.clone());

            let exit_flag = Arc::new(AtomicBool::new(false));

            let mut block_validator = self
                .block_validator
                .take()
                .expect("Unable to take block validator");

            block_validator.start();

            let mut chain_thread = ChainThread::new(
                request_receiver,
                exit_flag.clone(),
                self.state.clone(),
                self.chain_head_lock.clone(),
                self.consensus_notifier.clone(),
                self.state_pruning_block_depth,
                self.block_validation_results.clone(),
                block_validator,
                request_sender,
            );
            *stop_handle = Some(ChainThreadStopHandle::new(exit_flag));
            let chain_thread_builder = thread::Builder::new().name("ChainThread".into());
            chain_thread_builder
                .spawn(move || {
                    if let Err(err) = chain_thread.run() {
                        error!("Error occurred during ChainController loop: {:?}", err);
                    }
                })
                .expect("Unable to start ChainThread");
        }
    }

    pub fn stop(&mut self) {
        let mut stop_handle = self.stop_handle.lock().unwrap();
        if stop_handle.is_some() {
            let handle: ChainThreadStopHandle = stop_handle.take().unwrap();
            handle.stop();
        }
    }
}

/// This is used by a non-genesis journal when it has received the
/// genesis block from the genesis validator
fn set_genesis(
    state: &mut ChainControllerState,
    chain_head_lock: &ChainHeadLock,
    block: &BlockPair,
    block_validator: &BlockValidator,
    block_validation_results: &BlockValidationResultStore,
) -> Result<(), ChainControllerError> {
    if block.header().previous_block_id() == NULL_BLOCK_IDENTIFIER {
        let chain_id = state.chain_id_manager.get_block_chain_id()?;
        if chain_id
            .as_ref()
            .map(|block_id| block_id != block.block().header_signature())
            .unwrap_or(false)
        {
            warn!(
                "Block id does not match block chain id {}. Ignoring initial chain head: {}",
                chain_id.unwrap(),
                block.block().header_signature()
            );
        } else {
            block_validator.validate_block(&block)?;

            if chain_id.is_none() {
                state
                    .chain_id_manager
                    .save_block_chain_id(block.block().header_signature())?;
            }

            state
                .block_manager
                .persist(block.block().header_signature(), COMMIT_STORE)?;

            // Create Ref-C: External reference for the chain head will be held until it is
            // superceded by a new chain head.
            state.chain_head = Some(
                state
                    .block_manager
                    .ref_block(block.block().header_signature())?,
            );

            match block_validation_results.get(block.block().header_signature()) {
                Some(validation_results) => {
                    state
                        .merkle_state
                        .commit(&state.initial_state_hash, &validation_results.state_changes)
                        .map_err(|err| {
                            ChainControllerError::ChainUpdateError(format!(
                                "Unable to commit genesis block: {}",
                                err
                            ))
                        })?;
                    let receipts: Vec<TransactionReceipt> = validation_results.execution_results;
                    for observer in &mut state.observers {
                        observer.chain_update(&block, receipts.as_slice());
                    }
                }
                None => {
                    error!(
                        "While committing {}, found block missing execution results",
                        block.block(),
                    );
                }
            }

            let mut guard = chain_head_lock.acquire();
            guard.notify_on_chain_updated(block.clone(), vec![], vec![]);
        }
    }

    Ok(())
}

fn on_block_received(
    block_id: &str,
    state: &mut ChainControllerState,
    consensus_notifier: &Arc<dyn ConsensusNotifier>,
    chain_head_lock: &ChainHeadLock,
    block_validator: &BlockValidator,
    block_validation_results: &BlockValidationResultStore,
) -> Result<(), ChainControllerError> {
    if state.chain_head.is_none() {
        if let Some(Some(block)) = state.block_manager.get(&[&block_id]).next() {
            if let Err(err) = set_genesis(
                state,
                &chain_head_lock,
                &block,
                &block_validator,
                &block_validation_results,
            ) {
                warn!(
                    "Unable to set chain head; genesis block {} is not valid: {:?}",
                    block.block().header_signature(),
                    err
                );
            }
        } else {
            warn!("Received block not in block manager");
        }
        return Ok(());
    }

    let block = {
        if let Some(Some(block)) = state.block_manager.get(&[&block_id]).next() {
            // Create Ref-C: Hold this reference until consensus renders a {commit, ignore, or
            // fail} opinion on the block.
            match state.block_manager.ref_block(&block_id) {
                Ok(block_ref) => {
                    state
                        .block_references
                        .insert(block_ref.block_id().to_owned(), block_ref);
                    consensus_notifier.notify_block_new(&block);
                    Some(block)
                }
                Err(err) => {
                    error!(
                        "Unable to ref block {} received from completer; ignoring: {:?}",
                        &block_id, err
                    );
                    None
                }
            }
        } else {
            warn!(
                "Received block id for block not in block manager: {}",
                block_id
            );
            None
        }
    };

    if let Some(block) = block {
        // Transfer Ref-B: Implicitly transfer ownership of the external reference placed on
        // this block by the completer. The ForkCache is now responsible for unrefing the block
        // when it expires. This happens when either 1) the block is replaced in the cache by
        // another block which extends it, at which point this block will have an int. ref.
        // count of at least 1, or 2) the fork becomes inactive the block is purged, at which
        // point the block may be dropped if no other ext. ref's exist.
        if let Some(previous_block_id) = state
            .fork_cache
            .insert(&block_id, Some(block.header().previous_block_id()))
        {
            // Drop Ref-B: This fork was extended and so this block has an int. ref. count of
            // at least one, so we can drop the ext. ref. placed on the block to keep the fork
            // around.
            match state.block_manager.unref_block(&previous_block_id) {
                Ok(true) => {
                    panic!(
                        "Block {:?} was unref'ed because it was the head of a fork that was
                        just extended. The unref caused the block to drop, but it should have
                        had an internal reference count of at least 1.",
                        previous_block_id,
                    );
                }
                Ok(false) => (),
                Err(err) => error!(
                    "Failed to unref expired block {}: {:?}",
                    previous_block_id, err
                ),
            }
        }

        for block_id in state.fork_cache.purge() {
            // Drop Ref-B: The fork is no longer active, and we have to drop the ext. ref.
            // placed on the block to keep the fork around.
            if let Err(err) = state.block_manager.unref_block(&block_id) {
                error!("Failed to unref expired block {}: {:?}", block_id, err);
            }
        }
    }

    Ok(())
}

fn handle_block_commit(
    block: &BlockPair,
    state: &mut ChainControllerState,
    chain_head_lock: &ChainHeadLock,
    consensus_notifier: &Arc<dyn ConsensusNotifier>,
    state_pruning_block_depth: u32,
    block_validation_results: &BlockValidationResultStore,
) -> Result<(), ChainControllerError> {
    {
        loop {
            let chain_head = state
                .chain_reader
                .chain_head()
                .map_err(|err| {
                    error!("Error reading chain head: {:?}", err);
                    err
                })?
                .expect(
                    "Attempting to handle block commit before a genesis block has been
                    committed",
                );
            let result = state.build_fork(block, &chain_head).map_err(|err| {
                error!(
                    "Error occured while building fork resolution result: {:?}",
                    err,
                );
                err
            })?;

            let mut chain_head_guard = chain_head_lock.acquire();
            let chain_head_updated = state
                .check_chain_head_updated(&chain_head, &block)
                .map_err(|err| {
                    error!(
                        "Error occured while checking if chain head updated: {:?}",
                        err,
                    );
                    err
                })?;
            if chain_head_updated {
                continue;
            }

            // Move Ref-C: Consensus has decided this block should become the new chain
            // head, so the ChainController will maintain ownership of this ext. ref until a
            // new chain head replaces it.
            state.chain_head = Some(
                state
                    .block_references
                    .remove(block.block().header_signature())
                    .ok_or_else(|| {
                        ChainControllerError::ConsensusError(
                            "Consensus has already decided on this block".into(),
                        )
                    })?,
            );

            let new_roots = result
                .new_chain
                .iter()
                .map(|block| block.header().state_root_hash())
                .collect::<Vec<_>>();
            let current_roots = result
                .current_chain
                .iter()
                .map(|block| (block.header().block_num(), block.header().state_root_hash()))
                .collect::<Vec<_>>();
            state
                .state_pruning_manager
                .update_queue(new_roots.as_slice(), current_roots.as_slice());

            for blk in result.new_chain.iter().rev() {
                let previous_blocks_state_hash = state
                    .block_manager
                    .get(&[blk.header().previous_block_id()])
                    .next()
                    .unwrap_or(None)
                    .map(|b| hex::encode(b.header().state_root_hash().to_vec()))
                    .ok_or_else(|| {
                        ChainControllerError::ChainUpdateError(format!(
                            "Unable to find block {}",
                            blk.header().previous_block_id()
                        ))
                    })?;

                match block_validation_results.get(blk.block().header_signature()) {
                    Some(validation_results) => {
                        state
                            .merkle_state
                            .commit(
                                &previous_blocks_state_hash,
                                &validation_results.state_changes,
                            )
                            .map_err(|err| {
                                ChainControllerError::ChainUpdateError(format!(
                                    "Unable to commit state changes for block {}: {}",
                                    block.block().header_signature(),
                                    err
                                ))
                            })?;
                        let receipts: Vec<TransactionReceipt> =
                            validation_results.execution_results;
                        for observer in &mut state.observers {
                            observer.chain_update(&block, receipts.as_slice());
                        }
                    }
                    None => {
                        error!(
                            "While committing {}, found block {} missing execution results",
                            block.block(),
                            blk.block(),
                        );
                    }
                }
            }

            state
                .block_manager
                .persist(block.block().header_signature(), COMMIT_STORE)
                .map_err(|err| {
                    error!("Error persisting new chain head: {:?}", err);
                    err
                })?;

            block.block().batches().iter().for_each(|batch| {
                if batch.trace() {
                    debug!(
                        "TRACE: {}: ChainController.on_block_validated",
                        batch.header_signature()
                    )
                }
            });

            let total_committed_txns = match state.chain_reader.count_committed_transactions() {
                Ok(count) => count,
                Err(err) => {
                    error!(
                        "Unable to read total committed transactions count: {:?}",
                        err
                    );
                    0
                }
            };

            gauge!(
                "chain.ChainController.committed_transactions_gauge",
                total_committed_txns as i64
            );

            info!("Chain head updated to {}", block.block());

            consensus_notifier.notify_block_commit(block.block().header_signature());

            counter!(
                "chain.ChainController.committed_transactions_count",
                result.transaction_count as u64
            );
            gauge!(
                "chain.ChainController.block_num",
                block.header().block_num() as i64
            );

            chain_head_guard.notify_on_chain_updated(
                block.clone(),
                result.committed_batches,
                result.uncommitted_batches,
            );

            let chain_head_block_num = block.header().block_num();
            if chain_head_block_num + 1 > u64::from(state_pruning_block_depth) {
                let prune_at = chain_head_block_num - (u64::from(state_pruning_block_depth));
                match state.chain_reader.get_block_by_block_num(prune_at) {
                    Ok(Some(block)) => state
                        .state_pruning_manager
                        .add_to_queue(block.header().block_num(), block.header().state_root_hash()),
                    Ok(None) => warn!("No block at block height {}; ignoring...", prune_at),
                    Err(err) => error!("Unable to fetch block at height {}: {:?}", prune_at, err),
                }

                // Execute pruning:
                state.state_pruning_manager.execute(prune_at)
            }

            // Updated the block, so we're done
            break;
        }
    }

    Ok(())
}

fn on_block_validated(
    state: &mut ChainControllerState,
    block: &BlockPair,
    result: &BlockValidationResult,
    consensus_notifier: &Arc<dyn ConsensusNotifier>,
    validation_sender: &Sender<ChainControllerRequest>,
    block_validator: &BlockValidator,
) {
    counter!("chain.ChainController.blocks_considered_count", 1);

    match result.status {
        BlockStatus::Valid => {
            // Keep Ref-C: The block has been validated so ownership of the ext. ref. is
            // maintained. The consensus engine is responsible for rendering an opinion of
            // either commit, fail, or ignore, at which time the ext. ref. will be accounted
            // for (moved into chain head in case of commit, dropped otherwise)
            consensus_notifier.notify_block_valid(block.block().header_signature());
        }
        BlockStatus::Invalid => {
            consensus_notifier.notify_block_invalid(block.block().header_signature());

            // Drop Ref-C: The block has been found to be invalid, and we are no longer
            // interested in it. The invalid result will be cached for a period.
            if state
                .block_references
                .remove(block.block().header_signature())
                .is_none()
            {
                error!(
                    "Reference not found for invalid block {}",
                    block.block().header_signature()
                );
            }
        }
        _ => error!(
            "on_block_validated() called for block {}, but result was {:?}",
            block.block().header_signature(),
            result.status,
        ),
    }

    // notify block validation results received
    block_validator.process_pending(block, validation_sender);
}

impl<'a> From<&'a TxnExecutionResult> for TransactionReceipt {
    fn from(result: &'a TxnExecutionResult) -> Self {
        let mut receipt = TransactionReceipt::new();

        receipt.set_data(protobuf::RepeatedField::from_vec(result.data.to_vec()));
        receipt.set_state_changes(result.state_changes.clone().into_iter().collect());
        receipt.set_events(result.events.clone().into_iter().collect());
        receipt.set_transaction_id(result.signature.clone());

        receipt
    }
}

/// Messages handling by the chain controller's thread
#[derive(Debug)]
pub enum ChainControllerRequest {
    /// queue a block to be validated
    QueueBlock(String),
    /// handle committing a block
    CommitBlock(BlockPair),
    /// handle a block validation result
    BlockValidation(BlockValidationResult),
    /// submit block directly to the block validator for validation
    ValidateBlock {
        blocks: Vec<BlockPair>,
        response_sender: Sender<ChainControllerRequest>,
    },
}

struct ChainThread {
    request_receiver: Receiver<ChainControllerRequest>,
    exit: Arc<AtomicBool>,
    state: Arc<RwLock<ChainControllerState>>,
    chain_head_lock: ChainHeadLock,
    consensus_notifier: Arc<dyn ConsensusNotifier>,
    state_pruning_block_depth: u32,
    block_validation_results: BlockValidationResultStore,
    block_validator: BlockValidator,
    validation_sender: Sender<ChainControllerRequest>,
}

trait StopHandle: Clone {
    fn stop(&self);
}

impl ChainThread {
    #![allow(clippy::too_many_arguments)]
    fn new(
        request_receiver: Receiver<ChainControllerRequest>,
        exit_flag: Arc<AtomicBool>,
        state: Arc<RwLock<ChainControllerState>>,
        chain_head_lock: ChainHeadLock,
        consensus_notifier: Arc<dyn ConsensusNotifier>,
        state_pruning_block_depth: u32,
        block_validation_results: BlockValidationResultStore,
        block_validator: BlockValidator,
        validation_sender: Sender<ChainControllerRequest>,
    ) -> Self {
        ChainThread {
            request_receiver,
            exit: exit_flag,
            state,
            chain_head_lock,
            consensus_notifier,
            state_pruning_block_depth,
            block_validation_results,
            block_validator,
            validation_sender,
        }
    }

    fn run(&mut self) -> Result<(), ChainControllerError> {
        loop {
            let request = match self
                .request_receiver
                .recv_timeout(Duration::from_millis(RECV_TIMEOUT_MILLIS))
            {
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if self.exit.load(Ordering::Relaxed) {
                        break;
                    } else {
                        continue;
                    }
                }
                Err(err) => {
                    error!("Request queue broke: {}", err);
                    break;
                }
                Ok(block_id) => block_id,
            };

            match request {
                ChainControllerRequest::QueueBlock(block_id) => {
                    let mut state = self
                        .state
                        .write()
                        .expect("No lock holder should have poisoned the lock");

                    if let Err(err) = on_block_received(
                        &block_id,
                        &mut state,
                        &self.consensus_notifier,
                        &self.chain_head_lock,
                        &self.block_validator,
                        &self.block_validation_results,
                    ) {
                        error!("Error was return from on block received: {:?}", err);
                    }

                    if self.exit.load(Ordering::Relaxed) {
                        break;
                    }
                }
                ChainControllerRequest::CommitBlock(block) => {
                    let mut state = self
                        .state
                        .write()
                        .expect("No lock holder should have poisoned the lock");

                    if let Err(err) = handle_block_commit(
                        &block,
                        &mut state,
                        &self.chain_head_lock,
                        &self.consensus_notifier,
                        self.state_pruning_block_depth,
                        &self.block_validation_results,
                    ) {
                        error!("Error was return from handle block commit: {:?}", err);
                    }
                }
                ChainControllerRequest::ValidateBlock {
                    blocks,
                    response_sender,
                } => {
                    self.block_validator
                        .submit_blocks_for_verification(blocks, response_sender);
                }
                ChainControllerRequest::BlockValidation(block_validation_result) => {
                    self.block_validation_results
                        .insert(block_validation_result.clone());
                    let mut state = self
                        .state
                        .write()
                        .expect("No lock holder should have poisoned the lock");
                    if let Some(block) = state
                        .block_manager
                        .get(&[&block_validation_result.block_id])
                        .next()
                        .unwrap_or(None)
                    {
                        on_block_validated(
                            &mut state,
                            &block,
                            &block_validation_result,
                            &self.consensus_notifier,
                            &self.validation_sender,
                            &self.block_validator,
                        )
                    } else {
                        error!(
                            "Received a block validation result for a block that is not in the \
                            BlockManager"
                        );
                    }
                }
            }
        }
        self.block_validator.stop();
        Ok(())
    }
}

impl From<BlockValidationResult> for ChainControllerRequest {
    fn from(result: BlockValidationResult) -> Self {
        ChainControllerRequest::BlockValidation(result)
    }
}

#[derive(Clone)]
struct ChainThreadStopHandle {
    exit: Arc<AtomicBool>,
}

impl ChainThreadStopHandle {
    fn new(exit_flag: Arc<AtomicBool>) -> Self {
        ChainThreadStopHandle { exit: exit_flag }
    }
}

impl StopHandle for ChainThreadStopHandle {
    fn stop(&self) {
        self.exit.store(true, Ordering::Relaxed)
    }
}
