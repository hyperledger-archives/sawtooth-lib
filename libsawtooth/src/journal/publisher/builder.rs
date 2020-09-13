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

//! Constructor for `BlockPublisher`

use std::sync::{mpsc::channel, Arc, Mutex, RwLock};

use cylinder::Signer;
use transact::{execution::executor::ExecutionTaskSubmitter, scheduler::SchedulerFactory};

use crate::journal::{block_manager::BlockManager, commit_store::CommitStore};
use crate::state::{merkle::CborMerkleState, state_view_factory::StateViewFactory};

use super::{
    batch_injector::DefaultBatchInjectorFactory, start_publisher_thread, BatchInjectorFactory,
    BatchObserver, BatchSubmitter, BlockBroadcaster, BlockPublisher, BlockPublisherError,
    PendingBatchesPool,
};

/// Constructs a new `BlockPublisher`
#[derive(Default)]
pub struct BlockPublisherBuilder {
    batch_injector_factory: Option<Box<dyn BatchInjectorFactory>>,
    batch_observers: Vec<Box<dyn BatchObserver>>,
    block_broadcaster: Option<Box<dyn BlockBroadcaster>>,
    block_manager: Option<BlockManager>,
    commit_store: Option<CommitStore>,
    execution_task_submitter: Option<ExecutionTaskSubmitter>,
    merkle_state: Option<CborMerkleState>,
    scheduler_factory: Option<Box<dyn SchedulerFactory>>,
    signer: Option<Box<dyn Signer>>,
    state_view_factory: Option<StateViewFactory>,
}

impl BlockPublisherBuilder {
    /// Creates a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the batch injector factory that will be used by the publisher
    pub fn with_batch_injector_factory(
        mut self,
        batch_injector_factory: Box<dyn BatchInjectorFactory>,
    ) -> Self {
        self.batch_injector_factory = Some(batch_injector_factory);
        self
    }

    /// Sets the batch observers that will be notified by the publisher
    pub fn with_batch_observers(mut self, batch_observers: Vec<Box<dyn BatchObserver>>) -> Self {
        self.batch_observers = batch_observers;
        self
    }

    /// Sets the block broadcaster that will be used by the publisher
    pub fn with_block_broadcaster(mut self, block_broadcaster: Box<dyn BlockBroadcaster>) -> Self {
        self.block_broadcaster = Some(block_broadcaster);
        self
    }

    /// Sets the block manager that will be used by the publisher
    pub fn with_block_manager(mut self, block_manager: BlockManager) -> Self {
        self.block_manager = Some(block_manager);
        self
    }

    /// Sets the commit store that will be used by the publisher
    pub fn with_commit_store(mut self, commit_store: CommitStore) -> Self {
        self.commit_store = Some(commit_store);
        self
    }

    /// Sets the executor that will be used by the publisher
    pub fn with_execution_task_submitter(
        mut self,
        execution_task_submitter: ExecutionTaskSubmitter,
    ) -> Self {
        self.execution_task_submitter = Some(execution_task_submitter);
        self
    }

    /// Sets the Merkle state that will be used by the publisher
    pub fn with_merkle_state(mut self, merkle_state: CborMerkleState) -> Self {
        self.merkle_state = Some(merkle_state);
        self
    }

    /// Sets the scheduler factory that will be used by the publisher
    pub fn with_scheduler_factory(mut self, scheduler_factory: Box<dyn SchedulerFactory>) -> Self {
        self.scheduler_factory = Some(scheduler_factory);
        self
    }

    /// Sets the signer that will be used by the publisher
    pub fn with_signer(mut self, signer: Box<dyn Signer>) -> Self {
        self.signer = Some(signer);
        self
    }

    /// Sets the state view factory that will be used by the publisher
    pub fn with_state_view_factory(mut self, state_view_factory: StateViewFactory) -> Self {
        self.state_view_factory = Some(state_view_factory);
        self
    }

    /// Creates and starts the block publisher
    pub fn start(self) -> Result<BlockPublisher, BlockPublisherError> {
        let block_broadcaster = self.block_broadcaster.ok_or_else(|| {
            BlockPublisherError::StartupFailed("No block broadcaster provided".into())
        })?;
        let block_manager = self.block_manager.ok_or_else(|| {
            BlockPublisherError::StartupFailed("No block manager provided".into())
        })?;
        let commit_store = self
            .commit_store
            .ok_or_else(|| BlockPublisherError::StartupFailed("No commit store provided".into()))?;
        let execution_task_submitter = self
            .execution_task_submitter
            .ok_or_else(|| BlockPublisherError::StartupFailed("No executor provided".into()))?;
        let merkle_state = self
            .merkle_state
            .ok_or_else(|| BlockPublisherError::StartupFailed("No merkle state provided".into()))?;
        let scheduler_factory = self.scheduler_factory.ok_or_else(|| {
            BlockPublisherError::StartupFailed("No scheduler factory provided".into())
        })?;
        let signer = self
            .signer
            .ok_or_else(|| BlockPublisherError::StartupFailed("No signer provided".into()))?;
        let state_view_factory = self.state_view_factory.ok_or_else(|| {
            BlockPublisherError::StartupFailed("No state view factory provided".into())
        })?;

        let batch_injector_factory = self.batch_injector_factory.unwrap_or_else(|| {
            Box::new(DefaultBatchInjectorFactory::new(
                signer.clone(),
                state_view_factory.clone(),
            ))
        });

        let candidate_block = Arc::new(Mutex::new(None));
        let pending_batches = Arc::new(RwLock::new(PendingBatchesPool::default()));

        let (internal_sender, internal_receiver) = channel();
        let (backpressure_sender, backpressure_receiver) = channel();

        let batch_submitter = Some(BatchSubmitter::new(
            internal_sender.clone(),
            backpressure_receiver,
        ));

        let internal_thread_handle = start_publisher_thread(
            backpressure_sender,
            self.batch_observers,
            pending_batches.clone(),
            candidate_block.clone(),
            commit_store.clone(),
            state_view_factory.clone(),
            internal_receiver,
        )
        .map_err(|err| BlockPublisherError::StartupFailed(err.to_string()))?;

        Ok(BlockPublisher {
            batch_injector_factory,
            batch_submitter,
            pending_batches,
            block_broadcaster,
            block_manager,
            candidate_block,
            commit_store,
            execution_task_submitter,
            internal_sender,
            internal_thread_handle,
            merkle_state,
            scheduler_factory,
            signer,
            state_view_factory,
        })
    }
}
