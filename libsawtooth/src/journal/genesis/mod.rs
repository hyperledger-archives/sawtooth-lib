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
pub mod builder;
pub mod error;

use std::convert::TryFrom;
use std::fs::{remove_file, File};
use std::io::Read;
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver, Sender};

use cylinder::Signer;
use log::{debug, error, info};

use crate::journal::block_manager::BlockManager;
use crate::journal::chain::ChainObserver;
use crate::protocol::block::BlockBuilder;
use crate::protocol::genesis::GenesisData;
use crate::protos::transaction_receipt::TransactionReceipt;
use crate::protos::FromBytes;
use crate::state::merkle::CborMerkleState;
use crate::state::settings_view::SettingsView;
use crate::state::state_view_factory::StateViewFactory;
use crate::transact::execution::executor::ExecutionTaskSubmitter;
use crate::transact::protocol::receipt::TransactionResult;
use crate::transact::scheduler::{BatchExecutionResult, SchedulerError, SchedulerFactory};
use crate::transact::state::{StateChange, Write};

use super::{chain::ChainReader, chain_id_manager::ChainIdManager, NULL_BLOCK_IDENTIFIER};

use self::error::GenesisError;

/// Used to combine errors and `Batch` results returned from the `Scheduler`
enum SchedulerEvent {
    Result(BatchExecutionResult),
    Error(SchedulerError),
    Complete,
}

/// The `GenesisController` is in charge of checking if this is the genesis node and creating
/// the genesis block from the genesis batch file
pub struct GenesisController {
    transaction_executor: ExecutionTaskSubmitter,
    scheduler_factory: Box<dyn SchedulerFactory>,
    block_manager: BlockManager,
    chain_reader: Box<dyn ChainReader>,
    state_view_factory: StateViewFactory,
    identity_signer: Box<dyn Signer>,
    chain_id_manager: ChainIdManager,
    observers: Vec<Box<dyn ChainObserver>>,
    initial_state_root: String,
    merkle_state: CborMerkleState,
    genesis_file_path: PathBuf,
}

impl GenesisController {
    /// Determines if the system should be put in genesis mode
    ///
    /// Returns:
    ///      bool: return whether or not a genesis block is required to be created
    ///
    /// Returns error if there is invalid combination of the following: genesis.batch, existing
    /// chain head, and block chain id.
    pub fn requires_genesis(&self) -> Result<bool, GenesisError> {
        // check if the genesis batch file exists
        let genesis_path = self.genesis_file_path.as_path();
        if genesis_path.is_file() {
            debug!("Genesis batch file: {}", self.genesis_file_path.display());
        } else {
            debug!("Genesis batch file: not found");
        }

        // check if there is a chain head
        let chain_head = self.chain_reader.chain_head().map_err(|err| {
            GenesisError::InvalidGenesisState(format!("Unable to read chain head: {}", err))
        })?;

        if let Some(ref chain_head_inner) = chain_head {
            if genesis_path.is_file() {
                return Err(GenesisError::InvalidGenesisState(format!(
                    "Cannot have a genesis batch file and an existing chain (chain head: {})",
                    chain_head_inner.block().header_signature()
                )));
            }
        }

        // check if there is a block chain id
        let block_chain_id = self.chain_id_manager.get_block_chain_id().map_err(|err| {
            GenesisError::InvalidGenesisState(format!("Unable to read block chain id: {}", err))
        })?;

        if let Some(ref block_chain_id_inner) = block_chain_id {
            if genesis_path.is_file() {
                return Err(GenesisError::InvalidGenesisState(format!(
                    "Cannot have a genesis batch file and join an existing network (chain_id: {})",
                    block_chain_id_inner
                )));
            }
        };

        if !genesis_path.is_file() && chain_head.is_none() {
            info!("No chain head and not the genesis node: starting in peering mode")
        }

        Ok(genesis_path.is_file() && chain_head.is_none() && block_chain_id.is_none())
    }

    /// Starts the genesis block creation process.
    ///
    /// A GenesisError is returned if a genesis block is unable to be produced, or the resulting
    /// block-chain-id is unable to be saved.
    pub fn start(&mut self) -> Result<(), GenesisError> {
        let mut batch_file = File::open(self.genesis_file_path.clone()).map_err(|_| {
            GenesisError::InvalidGenesisState(format!(
                "Unable to open genesis batch file: {}",
                self.genesis_file_path.display()
            ))
        })?;

        let mut contents = vec![];
        batch_file.read_to_end(&mut contents).map_err(|_| {
            GenesisError::InvalidGenesisState(format!(
                "Unable to read entire genesis batch file: {}",
                self.genesis_file_path.display()
            ))
        })?;
        let genesis_data = GenesisData::from_bytes(&contents).map_err(|_| {
            GenesisError::InvalidGenesisData(
                "Unable to create GenesisData from bytes in genesis batch file".to_string(),
            )
        })?;

        let mut scheduler = self
            .scheduler_factory
            .create_scheduler(self.initial_state_root.to_string())
            .map_err(|err| {
                GenesisError::BatchValidationError(format!("Unable to create scheduler: {:?}", err))
            })?;

        let (result_tx, result_rx): (Sender<SchedulerEvent>, Receiver<SchedulerEvent>) = channel();
        let error_tx = result_tx.clone();
        // Add callback to convert batch result option to scheduler event
        scheduler
            .set_result_callback(Box::new(move |batch_result| {
                let scheduler_event = match batch_result {
                    Some(result) => SchedulerEvent::Result(result),
                    None => SchedulerEvent::Complete,
                };
                if result_tx.send(scheduler_event).is_err() {
                    error!("Unable to send batch result; receiver must have dropped");
                }
            }))
            .map_err(|err| {
                GenesisError::BatchValidationError(format!(
                    "Unable to set result callback on scheduler: {}",
                    err
                ))
            })?;

        // add callback to convert error into scheduler event
        scheduler
            .set_error_callback(Box::new(move |err| {
                if error_tx.send(SchedulerEvent::Error(err)).is_err() {
                    error!("Unable to send scheduler error; receiver must have dropped");
                }
            }))
            .map_err(|err| {
                GenesisError::BatchValidationError(format!(
                    "Unable to set error callback on scheduler: {}",
                    err
                ))
            })?;

        debug!("Adding {} genesis batches", genesis_data.batches().len());

        for batch in genesis_data.take_batches() {
            scheduler.add_batch(batch).map_err(|err| {
                GenesisError::BatchValidationError(format!(
                    "While adding a batch to the schedule: {:?}",
                    err
                ))
            })?;
        }

        scheduler.finalize().map_err(|err| {
            GenesisError::BatchValidationError(format!(
                "During call to scheduler.finalize: {:?}",
                err
            ))
        })?;

        self.transaction_executor
            .submit(
                scheduler.take_task_iterator().map_err(|_| {
                    GenesisError::BatchValidationError(
                        "Unable to take task iterator from scheduler".to_string(),
                    )
                })?,
                scheduler.new_notifier().map_err(|_| {
                    GenesisError::BatchValidationError(
                        "Unable to get new notifier from scheduler".to_string(),
                    )
                })?,
            )
            .map_err(|err| {
                GenesisError::BatchValidationError(format!(
                    "During call to ExecutionTaskSubmitter.submit: {}",
                    err
                ))
            })?;

        // collect results from the scheduler
        let mut execution_results = vec![];
        loop {
            match result_rx.recv() {
                Ok(SchedulerEvent::Result(result)) => execution_results.push(result),
                Ok(SchedulerEvent::Complete) => break,
                Ok(SchedulerEvent::Error(err)) => {
                    return Err(GenesisError::BatchValidationError(format!(
                        "During execution: {:?}",
                        err
                    )))
                }
                Err(err) => {
                    return Err(GenesisError::BatchValidationError(format!(
                        "Error while trying to receive scheduler event: {:?}",
                        err
                    )))
                }
            }
        }

        // Collect reciepts and changes from execution results
        let mut receipts = vec![];
        let mut changes = vec![];
        let mut batches = vec![];
        for batch_result in execution_results {
            if !batch_result.receipts.is_empty() {
                for receipt in batch_result.receipts {
                    match receipt.transaction_result {
                        TransactionResult::Invalid { .. } => {
                            return Err(GenesisError::BatchValidationError(format!(
                                "Genesis batch {} was invalid",
                                &batch_result.batch.batch().header_signature(),
                            )));
                        }
                        TransactionResult::Valid {
                            ref state_changes, ..
                        } => {
                            changes.append(
                                &mut state_changes
                                    .iter()
                                    .map(|change| StateChange::from(change.clone()))
                                    .collect(),
                            );
                            let result = TransactionReceipt::try_from(receipt).map_err(|err| {
                                GenesisError::BatchValidationError(format!(
                                    "Unable to convert returned Transact receipt into \
                                    TransactionReceipt: {}",
                                    err
                                ))
                            })?;
                            receipts.push(result);
                        }
                    };
                }
            } else {
                return Err(GenesisError::BatchValidationError(format!(
                    "batch {} did not have transaction results",
                    &batch_result.batch.batch().header_signature(),
                )));
            };

            let (batch, _) = batch_result.batch.take();
            batches.push(batch);
        }

        // commit state and get new state hash
        let new_state_hash = self
            .merkle_state
            .commit(&self.initial_state_root, &changes)
            .map_err(|err| {
                GenesisError::InvalidGenesisState(format!(
                    "Unable to commit state from genesis batches {}",
                    err
                ))
            })?;

        debug!("Produced state hash {} for genesis block", new_state_hash);

        let new_state_hash_bytes = hex::decode(new_state_hash.clone()).map_err(|_| {
            GenesisError::InvalidGenesisState(format!(
                "Cannot convert resulting state hash to bytes: {}",
                new_state_hash
            ))
        })?;

        // Validate required settings are set
        let settings_view = self
            .state_view_factory
            .create_view::<SettingsView>(&new_state_hash_bytes)
            .map_err(|err| {
                GenesisError::InvalidGenesisState(format!("Cannot create settings view: {}", err))
            })?;

        let name = settings_view
            .get_setting_str("sawtooth.consensus.algorithm.name", None)
            .map_err(|err| {
                GenesisError::InvalidGenesisState(format!(
                    "Cannot check for sawtooth.consensus.algorithm.name : {}",
                    err
                ))
            })?;
        let version = settings_view
            .get_setting_str("sawtooth.consensus.algorithm.version", None)
            .map_err(|err| {
                GenesisError::InvalidGenesisState(format!(
                    "Cannot check for sawtooth.consensus.algorithm.version : {}",
                    err
                ))
            })?;

        if name.is_none() || version.is_none() {
            return Err(GenesisError::LocalConfigurationError(
                "sawtooth.consensus.algorithm.name \
                and sawtooth.consensus.algorithm.version must be set in the \
                genesis block"
                    .to_string(),
            ));
        }

        let block_pair = self
            .generate_genesis_block()
            .with_batches(batches)
            .with_state_root_hash(new_state_hash_bytes)
            .build_pair(&*self.identity_signer)
            .map_err(|err| GenesisError::BlockGenerationError(err.to_string()))?;

        let block_id = block_pair.block().header_signature().to_string();

        for observer in &mut self.observers {
            observer.chain_update(&block_pair, receipts.as_slice());
        }

        info!("Genesis block created");
        self.block_manager.put(vec![block_pair]).map_err(|err| {
            GenesisError::InvalidGenesisState(format!(
                "Unable to put genesis block into block manager: {}",
                err
            ))
        })?;

        self.block_manager
            .persist(&block_id, "commit_store")
            .map_err(|err| {
                GenesisError::InvalidGenesisState(format!(
                    "Unable to persist genesis block into block manager: {}",
                    err
                ))
            })?;

        self.chain_id_manager
            .save_block_chain_id(&block_id)
            .map_err(|err| {
                GenesisError::InvalidGenesisState(format!(
                    "Unable to save block chain id in the chain id manager: {}",
                    err
                ))
            })?;

        debug!("Deleting genesis file");
        remove_file(self.genesis_file_path.as_path()).map_err(|err| {
            GenesisError::InvalidGenesisState(format!(
                "Unable to remove genesis batch file: {}",
                err
            ))
        })?;

        Ok(())
    }

    fn generate_genesis_block(&self) -> BlockBuilder {
        BlockBuilder::new()
            .with_block_num(0)
            .with_previous_block_id(NULL_BLOCK_IDENTIFIER.to_string())
            .with_consensus(b"Genesis".to_vec())
    }
}
