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

use transact::execution::executor::ExecutionTaskSubmitter;
use transact::protocol::receipt::TransactionResult;
use transact::scheduler::{BatchExecutionResult, SchedulerError, SchedulerFactory};
use transact::state::{StateChange, Write};

use std::convert::TryFrom;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{channel, Receiver, RecvTimeoutError, Sender},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;

use crate::{
    journal::{
        block_manager::BlockManager,
        block_scheduler::BlockScheduler,
        block_wrapper::BlockStatus,
        chain::ChainControllerRequest,
        chain_commit_state::{
            validate_no_duplicate_batches, validate_no_duplicate_transactions,
            validate_transaction_dependencies, ChainCommitStateError,
        },
        validation_rule_enforcer::ValidationRuleEnforcer,
    },
    permissions::verifier::PermissionVerifier,
    protocol::block::BlockPair,
    protos::transaction_receipt::TransactionReceipt,
    state::{
        identity_view::IdentityView, merkle::CborMerkleState, settings_view::SettingsView,
        state_view_factory::StateViewFactory,
    },
};

const BLOCK_VALIDATION_RESULT_CACHE_SIZE: usize = 512;

const BLOCK_VALIDATION_QUEUE_RECV_TIMEOUT: u64 = 100;

type BlockValidationResultCache =
    uluru::LRUCache<[uluru::Entry<BlockValidationResult>; BLOCK_VALIDATION_RESULT_CACHE_SIZE]>;

#[derive(Clone, Default)]
pub struct BlockValidationResultStore {
    validation_result_cache: Arc<Mutex<BlockValidationResultCache>>,
}

impl BlockValidationResultStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, result: BlockValidationResult) {
        self.validation_result_cache
            .lock()
            .expect("The mutex is poisoned")
            .insert(result)
    }

    pub fn get(&self, block_id: &str) -> Option<BlockValidationResult> {
        self.validation_result_cache
            .lock()
            .expect("The mutex is poisoned")
            .find(|r| r.block_id == block_id)
            .cloned()
    }

    pub fn fail_block(&self, block_id: &str) {
        if let Some(ref mut result) = self
            .validation_result_cache
            .lock()
            .expect("The mutex is poisoned")
            .find(|r| r.block_id == block_id)
        {
            result.status = BlockStatus::Invalid
        }
    }
}

impl BlockStatusStore for BlockValidationResultStore {
    fn status(&self, block_id: &str) -> BlockStatus {
        self.validation_result_cache
            .lock()
            .expect("The mutex is poisoned")
            .find(|r| r.block_id == block_id)
            .map(|r| r.status.clone())
            .unwrap_or(BlockStatus::Unknown)
    }
}

#[derive(Clone, Debug)]
pub struct BlockValidationResult {
    pub block_id: String,
    pub execution_results: Vec<TransactionReceipt>,
    pub num_transactions: u64,
    pub status: BlockStatus,
    pub state_changes: Vec<StateChange>,
}

impl BlockValidationResult {
    #[allow(dead_code)]
    fn new(
        block_id: String,
        execution_results: Vec<TransactionReceipt>,
        num_transactions: u64,
        status: BlockStatus,
        state_changes: Vec<StateChange>,
    ) -> Self {
        BlockValidationResult {
            block_id,
            execution_results,
            num_transactions,
            status,
            state_changes,
        }
    }
}

pub trait BlockStatusStore: Clone + Send + Sync {
    fn status(&self, block_id: &str) -> BlockStatus;
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    BlockValidationFailure(String),
    BlockValidationError(String),
}

impl From<ChainCommitStateError> for ValidationError {
    fn from(other: ChainCommitStateError) -> Self {
        match other {
            ChainCommitStateError::DuplicateBatch(ref batch_id) => {
                ValidationError::BlockValidationFailure(format!(
                    "Validation failure, duplicate batch {}",
                    batch_id
                ))
            }
            ChainCommitStateError::DuplicateTransaction(ref txn_id) => {
                ValidationError::BlockValidationFailure(format!(
                    "Validation failure, duplicate transaction {}",
                    txn_id
                ))
            }
            ChainCommitStateError::MissingDependency(ref txn_id) => {
                ValidationError::BlockValidationFailure(format!(
                    "Validation failure, missing dependency {}",
                    txn_id
                ))
            }
            ChainCommitStateError::Error(reason) => ValidationError::BlockValidationError(reason),
        }
    }
}

impl From<SchedulerError> for ValidationError {
    fn from(other: SchedulerError) -> Self {
        match other {
            SchedulerError::DuplicateBatch(ref batch_id) => {
                ValidationError::BlockValidationFailure(format!(
                    "Validation failure, duplicate batch {}",
                    batch_id
                ))
            }
            error => ValidationError::BlockValidationError(error.to_string()),
        }
    }
}

type InternalSender = Sender<(BlockPair, Sender<ChainControllerRequest>)>;
type InternalReceiver = Receiver<(BlockPair, Sender<ChainControllerRequest>)>;

pub struct BlockValidator {
    channel: (InternalSender, Option<InternalReceiver>),
    validation_thread_exit: Arc<AtomicBool>,
    block_scheduler: BlockScheduler<BlockValidationResultStore>,
    block_status_store: BlockValidationResultStore,
    block_manager: BlockManager,
    transaction_executor: Option<ExecutionTaskSubmitter>,
    scheduler_factory: Option<Box<dyn SchedulerFactory>>,
    view_factory: StateViewFactory,
    initial_state_hash: String,
    merkle_state: Option<CborMerkleState>,
}

impl BlockValidator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        block_manager: BlockManager,
        transaction_executor: ExecutionTaskSubmitter,
        block_status_store: BlockValidationResultStore,
        view_factory: StateViewFactory,
        scheduler_factory: Box<dyn SchedulerFactory>,
        initial_state_hash: String,
        merkle_state: CborMerkleState,
    ) -> Self {
        let (tx, rx) = channel();
        let channel = (tx, Some(rx));

        BlockValidator {
            channel,
            transaction_executor: Some(transaction_executor),
            validation_thread_exit: Arc::new(AtomicBool::new(false)),
            block_scheduler: BlockScheduler::new(block_manager.clone(), block_status_store.clone()),
            block_status_store,
            block_manager,
            scheduler_factory: Some(scheduler_factory),
            view_factory,
            initial_state_hash,
            merkle_state: Some(merkle_state),
        }
    }

    pub fn stop(&self) {
        self.validation_thread_exit.store(true, Ordering::Relaxed);
    }

    fn setup_thread(
        &self,
        rcv: Receiver<(BlockPair, Sender<ChainControllerRequest>)>,
        error_return_sender: Sender<(BlockPair, Sender<ChainControllerRequest>)>,
        transaction_executor: ExecutionTaskSubmitter,
        merkle_state: CborMerkleState,
        scheduler_factory: Box<dyn SchedulerFactory>,
    ) {
        let backgroundthread = thread::Builder::new();

        let validation1: Box<dyn BlockValidation<ReturnValue = ()>> = Box::new(
            DuplicatesAndDependenciesValidation::new(self.block_manager.clone()),
        );

        let validation2: Box<dyn BlockValidation<ReturnValue = ()>> =
            Box::new(OnChainRulesValidation::new(self.view_factory.clone()));

        let validation3: Box<dyn BlockValidation<ReturnValue = ()>> =
            Box::new(PermissionValidation::new(self.view_factory.clone()));

        let validations = vec![validation1, validation2, validation3];

        let state_validation = BatchesInBlockValidation::new(
            transaction_executor,
            merkle_state,
            scheduler_factory,
            self.initial_state_hash.to_string(),
        );

        let block_validations = BlockValidationProcessor::new(
            self.block_manager.clone(),
            validations,
            state_validation,
        );

        let exit = self.validation_thread_exit.clone();
        backgroundthread
            .spawn(move || loop {
                let (block, results_sender) = match rcv
                    .recv_timeout(Duration::from_millis(BLOCK_VALIDATION_QUEUE_RECV_TIMEOUT))
                {
                    Err(RecvTimeoutError::Timeout) => {
                        if exit.load(Ordering::Relaxed) {
                            break;
                        }
                        continue;
                    }
                    Err(err) => {
                        error!("BlockValidation queue shut down unexpectedly: {}", err);
                        break;
                    }
                    Ok(b) => b,
                };

                if exit.load(Ordering::Relaxed) {
                    break;
                }
                let block_id = block.block().header_signature().to_string();

                match block_validations.validate_block(&block) {
                    Ok(result) => {
                        info!("Block {} passed validation", block_id);
                        if let Err(err) = results_sender.send(ChainControllerRequest::from(result))
                        {
                            warn!("During handling valid block: {:?}", err);
                            exit.store(true, Ordering::Relaxed);
                        }
                    }
                    Err(ValidationError::BlockValidationFailure(ref reason)) => {
                        warn!("Block {} failed validation: {}", &block_id, reason);
                        if let Err(err) = results_sender.send(ChainControllerRequest::from(
                            BlockValidationResult {
                                block_id,
                                execution_results: vec![],
                                num_transactions: 0,
                                status: BlockStatus::Invalid,
                                state_changes: vec![],
                            },
                        )) {
                            warn!("During handling block failure: {:?}", err);
                            exit.store(true, Ordering::Relaxed);
                        }
                    }
                    Err(err) => {
                        warn!("Error during block validation: {:?}", err);
                        if let Err(err) = error_return_sender.send((block, results_sender)) {
                            warn!("During handling retry after an error: {:?}", err);
                            exit.store(true, Ordering::Relaxed);
                        }
                    }
                }
            })
            .expect("The background thread had an error");
    }

    pub fn start(&mut self) {
        let receiver = {
            let (_, rx) = &mut self.channel;
            rx.take()
                .expect("For a single call of start, there will always be receivers to take")
        };

        let transaction_executor = self
            .transaction_executor
            .take()
            .expect("setup_thread should never be called twice");

        let merkle_state = self
            .merkle_state
            .take()
            .expect("setup_thread should never be called twice");

        let scheduler_factory = self
            .scheduler_factory
            .take()
            .expect("setup_thread should never be called twice");

        let (tx, _) = &self.channel;

        self.setup_thread(
            receiver,
            tx.clone(),
            transaction_executor,
            merkle_state,
            scheduler_factory,
        );
    }

    fn return_sender(&self) -> InternalSender {
        let (tx, _) = &self.channel;
        tx.clone()
    }

    pub fn submit_blocks_for_verification(
        &self,
        blocks: Vec<BlockPair>,
        response_sender: Sender<ChainControllerRequest>,
    ) {
        for block in self.block_scheduler.schedule(blocks.to_vec()) {
            let tx = self.return_sender();
            if let Err(err) = tx.send((block, response_sender.clone())) {
                warn!("During submit blocks for verification: {:?}", err);
                self.validation_thread_exit.store(true, Ordering::Relaxed);
            }
        }
    }

    pub fn process_pending(
        &self,
        block: &BlockPair,
        response_sender: &Sender<ChainControllerRequest>,
    ) {
        for block in self.block_scheduler.done(block.block().header_signature()) {
            let tx = self.return_sender();
            if let Err(err) = tx.send((block, response_sender.clone())) {
                warn!("During process pending: {:?}", err);
                self.validation_thread_exit.store(true, Ordering::Relaxed);
            }
        }
    }

    pub fn validate_block(&self, block: &BlockPair) -> Result<(), ValidationError> {
        let (tx, rx) = channel();
        self.submit_blocks_for_verification(vec![block.clone()], tx);
        match rx.recv() {
            Ok(ChainControllerRequest::BlockValidation(block_validation_result)) => {
                self.block_status_store.insert(block_validation_result);
                Ok(())
            }
            Ok(response) => Err(ValidationError::BlockValidationError(format!(
                "Received unexpected response: {:?}",
                response
            ))),
            Err(err) => Err(ValidationError::BlockValidationError(format!(
                "Unable to receive block validation result: {}",
                err
            ))),
        }
    }
}

trait StateBlockValidation {
    fn validate_block(
        &self,
        block: BlockPair,
        previous_state_root: Option<&[u8]>,
    ) -> Result<BlockValidationResult, ValidationError>;
}

/// A generic block validation. Returns a ValidationError::BlockValidationFailure on
/// validation failure. It is a dependent validation if it can return
/// ValidationError::BlockStoreUpdated and is an independent validation otherwise
trait BlockValidation: Send {
    type ReturnValue;

    fn validate_block(
        &self,
        block: &BlockPair,
        previous_state_root: Option<&[u8]>,
    ) -> Result<Self::ReturnValue, ValidationError>;
}

struct BlockValidationProcessor<SBV: BlockValidation<ReturnValue = BlockValidationResult>> {
    block_manager: BlockManager,
    validations: Vec<Box<dyn BlockValidation<ReturnValue = ()>>>,
    state_validation: SBV,
}

impl<SBV: BlockValidation<ReturnValue = BlockValidationResult>> BlockValidationProcessor<SBV> {
    fn new(
        block_manager: BlockManager,
        validations: Vec<Box<dyn BlockValidation<ReturnValue = ()>>>,
        state_validation: SBV,
    ) -> Self {
        BlockValidationProcessor {
            block_manager,
            validations,
            state_validation,
        }
    }

    fn validate_block(&self, block: &BlockPair) -> Result<BlockValidationResult, ValidationError> {
        let previous_blocks_state_hash = self
            .block_manager
            .get(&[block.header().previous_block_id()])
            .next()
            .unwrap_or(None)
            .map(|b| b.header().state_root_hash().to_vec());

        for validation in &self.validations {
            match validation.validate_block(&block, previous_blocks_state_hash.as_deref()) {
                Ok(()) => (),
                Err(err) => return Err(err),
            }
        }

        self.state_validation
            .validate_block(&block, previous_blocks_state_hash.as_deref())
    }
}

/// Validate that all the batches are valid and all the transactions produce
/// the expected state hash.
struct BatchesInBlockValidation {
    transaction_executor: ExecutionTaskSubmitter,
    merkle_state: CborMerkleState,
    scheduler_factory: Box<dyn SchedulerFactory>,
    initial_state_hash: String,
}

impl BatchesInBlockValidation {
    fn new(
        transaction_executor: ExecutionTaskSubmitter,
        merkle_state: CborMerkleState,
        scheduler_factory: Box<dyn SchedulerFactory>,
        initial_state_hash: String,
    ) -> Self {
        BatchesInBlockValidation {
            transaction_executor,
            merkle_state,
            scheduler_factory,
            initial_state_hash,
        }
    }
}

/// Used to combine errors and Batch results
enum SchedulerEvent {
    Result(BatchExecutionResult),
    Error(SchedulerError),
    Complete,
}

impl BlockValidation for BatchesInBlockValidation {
    type ReturnValue = BlockValidationResult;

    fn validate_block(
        &self,
        block: &BlockPair,
        previous_state_root: Option<&[u8]>,
    ) -> Result<BlockValidationResult, ValidationError> {
        let ending_state_hash = hex::encode(block.header().state_root_hash());

        let state_root = previous_state_root
            .map(hex::encode)
            .unwrap_or_else(|| self.initial_state_hash.clone());

        let mut scheduler = self
            .scheduler_factory
            .create_scheduler(state_root.to_string())?;

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

        for batch in block.block().batches() {
            let batch_pair = batch.clone().into_pair().map_err(|err| {
                ValidationError::BlockValidationError(format!(
                    "Unable to convert block's batch into BatchPair: {:?}",
                    err
                ))
            })?;

            scheduler.add_batch(batch_pair).map_err(|err| {
                ValidationError::BlockValidationError(format!(
                    "While adding a batch to the schedule: {:?}",
                    err
                ))
            })?;
        }

        scheduler.finalize().map_err(|err| {
            ValidationError::BlockValidationError(format!(
                "During call to scheduler.finalize: {:?}",
                err
            ))
        })?;

        self.transaction_executor
            .submit(scheduler.take_task_iterator()?, scheduler.new_notifier()?)
            .map_err(|err| {
                ValidationError::BlockValidationError(format!(
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
                    return Err(ValidationError::BlockValidationError(format!(
                        "During execution: {:?}",
                        err
                    )))
                }
                Err(err) => {
                    return Err(ValidationError::BlockValidationError(format!(
                        "Error while trying to receive scheduler event: {:?}",
                        err
                    )))
                }
            }
        }

        let mut results = vec![];
        let mut changes = vec![];
        for batch_result in execution_results {
            if !batch_result.receipts.is_empty() {
                for receipt in batch_result.receipts {
                    match receipt.transaction_result.clone() {
                        TransactionResult::Invalid { .. } => {
                            return Err(ValidationError::BlockValidationFailure(format!(
                                "Block {} failed validation: batch {} was invalid due to \
                            transaction {}",
                                block.block().header_signature(),
                                &batch_result.batch.batch().header_signature(),
                                &receipt.transaction_id
                            )));
                        }
                        TransactionResult::Valid { state_changes, .. } => {
                            changes.append(
                                &mut state_changes.into_iter().map(StateChange::from).collect(),
                            );
                            let result = TransactionReceipt::try_from(receipt).map_err(|err| {
                                ValidationError::BlockValidationError(format!(
                                    "Unable to convert returned Transact receipt into \
                                    TransactionReceipt: {}",
                                    err
                                ))
                            })?;
                            results.push(result);
                        }
                    };
                }
            } else {
                return Err(ValidationError::BlockValidationFailure(format!(
                    "Block {} failed validation: batch {} did not have transaction results",
                    block.block().header_signature(),
                    &batch_result.batch.batch().header_signature(),
                )));
            };
        }

        let actual_ending_state_hash = self
            .merkle_state
            .compute_state_id(&state_root, &changes)
            .map_err(|err| {
                ValidationError::BlockValidationError(format!(
                    "During ending state hash calculation: {:?}",
                    err
                ))
            })?;

        if ending_state_hash != actual_ending_state_hash {
            return Err(ValidationError::BlockValidationFailure(format!(
                "Block {} failed validation: expected state hash {}, validation found state hash {}",
                block.block().header_signature(),
                hex::encode(ending_state_hash),
                hex::encode(actual_ending_state_hash)
            )));
        }

        Ok(BlockValidationResult {
            block_id: block.block().header_signature().to_string(),
            num_transactions: results.len() as u64,
            execution_results: results,
            status: BlockStatus::Valid,
            state_changes: changes,
        })
    }
}

struct DuplicatesAndDependenciesValidation {
    block_manager: BlockManager,
}

impl DuplicatesAndDependenciesValidation {
    fn new(block_manager: BlockManager) -> Self {
        DuplicatesAndDependenciesValidation { block_manager }
    }
}

impl BlockValidation for DuplicatesAndDependenciesValidation {
    type ReturnValue = ();

    fn validate_block(&self, block: &BlockPair, _: Option<&[u8]>) -> Result<(), ValidationError> {
        let batch_ids = block
            .block()
            .batches()
            .iter()
            .map(|b| b.header_signature())
            .collect::<Vec<_>>();
        validate_no_duplicate_batches(
            &self.block_manager,
            block.header().previous_block_id(),
            batch_ids.as_slice(),
        )?;

        let txn_ids = block
            .block()
            .batches()
            .iter()
            .flat_map(|batch| batch.transactions())
            .map(|txn| txn.header_signature())
            .collect::<Vec<_>>();
        validate_no_duplicate_transactions(
            &self.block_manager,
            block.header().previous_block_id(),
            txn_ids.as_slice(),
        )?;

        let transactions = block
            .block()
            .batches()
            .iter()
            .flat_map(|batch| batch.transactions())
            .cloned()
            .collect::<Vec<_>>();
        validate_transaction_dependencies(
            &self.block_manager,
            block.header().previous_block_id(),
            &transactions,
        )?;
        Ok(())
    }
}

struct PermissionValidation {
    state_view_factory: StateViewFactory,
}

impl PermissionValidation {
    fn new(state_view_factory: StateViewFactory) -> Self {
        Self { state_view_factory }
    }
}

impl BlockValidation for PermissionValidation {
    type ReturnValue = ();

    fn validate_block(
        &self,
        block: &BlockPair,
        previous_state_root: Option<&[u8]>,
    ) -> Result<(), ValidationError> {
        if block.header().block_num() != 0 {
            let state_root = previous_state_root.ok_or_else(|| {
                ValidationError::BlockValidationError(format!(
                    "During permission check of block ({}, {}) previous state root was missing",
                    block.block().header_signature(),
                    block.header().block_num(),
                ))
            })?;

            let identity_view: IdentityView = self
                .state_view_factory
                .create_view(&state_root)
                .map_err(|err| {
                    ValidationError::BlockValidationError(format!(
                        "During permission check of block ({}, {}) state root was not \
                            found in state: {}",
                        block.block().header_signature(),
                        block.header().block_num(),
                        err
                    ))
                })?;
            let permission_verifier = PermissionVerifier::new(Box::new(identity_view));
            for batch in block.block().batches() {
                let batch_pair = batch.clone().into_pair().map_err(|_| {
                    ValidationError::BlockValidationFailure(format!(
                        "Validation failure, invalid batch header {}",
                        batch.header_signature()
                    ))
                })?;
                let authorized = permission_verifier
                    .is_batch_signer_authorized(&batch_pair)
                    .map_err(|err| {
                        ValidationError::BlockValidationError(format!(
                            "During permission check of block ({}, {}), unable to read \
                        permissions: {}",
                            block.block().header_signature(),
                            block.header().block_num(),
                            err
                        ))
                    })?;
                if !authorized {
                    return Err(ValidationError::BlockValidationFailure(format!(
                        "Block {} failed permission verification: batch {} signer is \
                            not authorized",
                        block.block().header_signature(),
                        batch.header_signature()
                    )));
                }
            }
        }
        Ok(())
    }
}

struct OnChainRulesValidation {
    view_factory: StateViewFactory,
}

impl OnChainRulesValidation {
    fn new(view_factory: StateViewFactory) -> Self {
        OnChainRulesValidation { view_factory }
    }
}

impl BlockValidation for OnChainRulesValidation {
    type ReturnValue = ();

    fn validate_block(
        &self,
        block: &BlockPair,
        previous_state_root: Option<&[u8]>,
    ) -> Result<(), ValidationError> {
        if block.header().block_num() != 0 {
            let state_root = previous_state_root.ok_or_else(|| {
                ValidationError::BlockValidationError(format!(
                    "During validate_on_chain_rules for block ({}, {}), previous state root was \
                     missing",
                    block.block().header_signature(),
                    block.header().block_num(),
                ))
            })?;

            let settings_view: SettingsView =
                self.view_factory.create_view(&state_root).map_err(|err| {
                    ValidationError::BlockValidationError(format!(
                        "During validate_on_chain_rules, error creating settings view: {:?}",
                        err
                    ))
                })?;
            let mut validation_rule_enforcer = ValidationRuleEnforcer::new(
                &settings_view,
                block.header().signer_public_key().to_vec(),
            )
            .map_err(|err| ValidationError::BlockValidationError(err.to_string()))?;
            validation_rule_enforcer
                .add_batches(block.block().batches())
                .map_err(|err| ValidationError::BlockValidationFailure(err.to_string()))?;
            if !validation_rule_enforcer.validate(true) {
                return Err(ValidationError::BlockValidationFailure(format!(
                    "Block {} failed validation rules",
                    block.block().header_signature()
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};
    use transact::protocol::batch::Batch;

    use crate::journal::block_store::{BlockStore, BlockStoreError};
    use crate::journal::NULL_BLOCK_IDENTIFIER;
    use crate::protocol::block::{BlockBuilder, BlockPair};

    use std::sync::Mutex;

    #[test]
    fn test_validation_processor_genesis() {
        let block_manager = BlockManager::new();
        let block_a = create_block(NULL_BLOCK_IDENTIFIER, vec![]);

        let validation1: Box<dyn BlockValidation<ReturnValue = ()>> = Box::new(Mock1::new(Ok(())));

        let validation2: Box<dyn BlockValidation<ReturnValue = ()>> =
            Box::new(Mock2::new(Ok(()), Ok(())));
        let validations = vec![validation1, validation2];
        let state_block_validation = Mock1::new(Ok(BlockValidationResult::new(
            "".into(),
            vec![],
            0,
            BlockStatus::Valid,
            vec![],
        )));

        let validation_processor =
            BlockValidationProcessor::new(block_manager, validations, state_block_validation);
        assert!(validation_processor.validate_block(&block_a).is_ok());
    }

    /*
     * Test mocks that are stand-ins for the individual validation handlers
     */

    struct Mock1<R>
    where
        R: Clone,
    {
        result: R,
    }

    impl<R: Clone> Mock1<R> {
        fn new(result: R) -> Self {
            Mock1 { result }
        }
    }

    impl BlockValidation for Mock1<Result<BlockValidationResult, ValidationError>> {
        type ReturnValue = BlockValidationResult;

        fn validate_block(
            &self,
            _block: &BlockPair,
            _previous_state_root: Option<&[u8]>,
        ) -> Result<BlockValidationResult, ValidationError> {
            self.result.clone()
        }
    }

    impl BlockValidation for Mock1<Result<(), ValidationError>> {
        type ReturnValue = ();

        fn validate_block(
            &self,
            _block: &BlockPair,
            _previous_state_root: Option<&[u8]>,
        ) -> Result<(), ValidationError> {
            self.result.clone()
        }
    }

    struct Mock2<R>
    where
        R: Clone,
    {
        first: R,
        every_other: R,
        called: Mutex<bool>,
    }

    impl<R: Clone> Mock2<R> {
        fn new(first: R, every_other: R) -> Self {
            Mock2 {
                first,
                every_other,
                called: Mutex::new(false),
            }
        }
    }

    impl BlockValidation for Mock2<Result<(), ValidationError>> {
        type ReturnValue = ();

        fn validate_block(
            &self,
            _block: &BlockPair,
            _previous_state_root: Option<&[u8]>,
        ) -> Result<(), ValidationError> {
            if *self.called.lock().expect("Error acquiring Mock2 lock") {
                return self.every_other.clone();
            }
            {
                let mut called = self.called.lock().expect("Error acquiring Mock2 lock");
                *called = true;
            }
            self.first.clone()
        }
    }

    impl BlockStore for Mock1<Option<BlockPair>> {
        fn iter(&self) -> Result<Box<dyn Iterator<Item = BlockPair>>, BlockStoreError> {
            Ok(Box::new(self.result.clone().into_iter()))
        }

        fn get<'a>(
            &'a self,
            _block_ids: &[&str],
        ) -> Result<Box<dyn Iterator<Item = BlockPair> + 'a>, BlockStoreError> {
            unimplemented!();
        }

        fn put(&mut self, _block: Vec<BlockPair>) -> Result<(), BlockStoreError> {
            unimplemented!();
        }

        fn delete(&mut self, _block_ids: &[&str]) -> Result<Vec<BlockPair>, BlockStoreError> {
            unimplemented!();
        }
    }

    fn create_block(previous_block_id: &str, batches: Vec<Batch>) -> BlockPair {
        BlockBuilder::new()
            .with_block_num(0)
            .with_previous_block_id(previous_block_id.into())
            .with_state_root_hash(vec![])
            .with_batches(batches)
            .build_pair(&*new_signer())
            .expect("Failed to build block pair")
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }
}
