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

use std::path::PathBuf;

use cylinder::Signer;

use crate::journal::block_manager::BlockManager;
use crate::journal::chain::ChainObserver;
use crate::journal::{chain::ChainReader, chain_id_manager::ChainIdManager};
use crate::state::merkle::CborMerkleState;
use crate::state::state_view_factory::StateViewFactory;
use crate::transact::execution::executor::ExecutionTaskSubmitter;
use crate::transact::scheduler::SchedulerFactory;

use super::error::GenesisControllerBuildError;
use super::GenesisController;

const GENESIS_FILE: &str = "genesis.batch";

/// Builder for creating a `GenesisController`
#[derive(Default)]
pub struct GenesisControllerBuilder {
    transaction_executor: Option<ExecutionTaskSubmitter>,
    scheduler_factory: Option<Box<dyn SchedulerFactory>>,
    block_manager: Option<BlockManager>,
    chain_reader: Option<Box<dyn ChainReader>>,
    state_view_factory: Option<StateViewFactory>,
    identity_signer: Option<Box<dyn Signer>>,
    data_dir: Option<String>,
    chain_id_manager: Option<ChainIdManager>,
    observers: Option<Vec<Box<dyn ChainObserver>>>,
    initial_state_root: Option<String>,
    merkle_state: Option<CborMerkleState>,
}

impl GenesisControllerBuilder {
    // Creates an empty builder
    pub fn new() -> Self {
        GenesisControllerBuilder::default()
    }

    pub fn with_transaction_executor(
        mut self,
        executor: ExecutionTaskSubmitter,
    ) -> GenesisControllerBuilder {
        self.transaction_executor = Some(executor);
        self
    }

    pub fn with_scheduler_factory(
        mut self,
        scheduler_factory: Box<dyn SchedulerFactory>,
    ) -> GenesisControllerBuilder {
        self.scheduler_factory = Some(scheduler_factory);
        self
    }

    pub fn with_block_manager(mut self, block_manager: BlockManager) -> GenesisControllerBuilder {
        self.block_manager = Some(block_manager);
        self
    }

    pub fn with_chain_reader(
        mut self,
        chain_reader: Box<dyn ChainReader>,
    ) -> GenesisControllerBuilder {
        self.chain_reader = Some(chain_reader);
        self
    }

    pub fn with_state_view_factory(
        mut self,
        state_view_factory: StateViewFactory,
    ) -> GenesisControllerBuilder {
        self.state_view_factory = Some(state_view_factory);
        self
    }

    pub fn with_data_dir(mut self, data_dir: String) -> GenesisControllerBuilder {
        self.data_dir = Some(data_dir);
        self
    }

    pub fn with_chain_id_manager(
        mut self,
        chain_id_manager: ChainIdManager,
    ) -> GenesisControllerBuilder {
        self.chain_id_manager = Some(chain_id_manager);
        self
    }

    pub fn with_observers(
        mut self,
        observers: Vec<Box<dyn ChainObserver>>,
    ) -> GenesisControllerBuilder {
        self.observers = Some(observers);
        self
    }

    pub fn with_initial_state_root(
        mut self,
        initial_state_root: String,
    ) -> GenesisControllerBuilder {
        self.initial_state_root = Some(initial_state_root);
        self
    }

    pub fn with_merkle_state(mut self, merkle_state: CborMerkleState) -> GenesisControllerBuilder {
        self.merkle_state = Some(merkle_state);
        self
    }

    pub fn with_identity_signer(
        mut self,
        identity_signer: Box<dyn Signer>,
    ) -> GenesisControllerBuilder {
        self.identity_signer = Some(identity_signer);
        self
    }

    /// Builds the `GenesisJounral`
    ///
    /// Returns an error if one of the required fields are not provided
    pub fn build(self) -> Result<GenesisController, GenesisControllerBuildError> {
        let transaction_executor = self.transaction_executor.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'transaction_executor' field is required".to_string(),
            )
        })?;

        let scheduler_factory = self.scheduler_factory.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'scheduler_factory' field is required".to_string(),
            )
        })?;

        let block_manager = self.block_manager.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'block_manager' field is required".to_string(),
            )
        })?;

        let chain_reader = self.chain_reader.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'chain_reader' field is required".to_string(),
            )
        })?;

        let state_view_factory = self.state_view_factory.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'state_view_factory' field is required".to_string(),
            )
        })?;

        let data_dir = self.data_dir.ok_or_else(|| {
            GenesisControllerBuildError::MissingField("'data_dir' field is required".to_string())
        })?;

        let chain_id_manager = self.chain_id_manager.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'chain_id_manager' field is required".to_string(),
            )
        })?;

        let observers = self.observers.ok_or_else(|| {
            GenesisControllerBuildError::MissingField("'observers' field is required".to_string())
        })?;

        let initial_state_root = self.initial_state_root.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'initial_state_root' field is required".to_string(),
            )
        })?;

        let merkle_state = self.merkle_state.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'merkle_state' field is required".to_string(),
            )
        })?;

        let identity_signer = self.identity_signer.ok_or_else(|| {
            GenesisControllerBuildError::MissingField(
                "'identity_signer' field is required".to_string(),
            )
        })?;

        let mut genesis_path_buf = PathBuf::from(data_dir);
        genesis_path_buf.push(GENESIS_FILE);

        Ok(GenesisController {
            transaction_executor,
            scheduler_factory,
            block_manager,
            chain_reader,
            state_view_factory,
            identity_signer,
            chain_id_manager,
            observers,
            genesis_file_path: genesis_path_buf,
            initial_state_root,
            merkle_state,
        })
    }
}
