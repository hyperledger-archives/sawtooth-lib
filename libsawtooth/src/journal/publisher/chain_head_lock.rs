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

use std::sync::{Arc, RwLock, RwLockWriteGuard};

use transact::protocol::batch::BatchPair;

use crate::protocol::block::BlockPair;

use super::{batch_pool::PendingBatchesPool, BlockPublisher};

/// Abstracts acquiring the lock used by the BlockPublisher without exposing access to the
/// publisher itself.
#[derive(Clone)]
pub struct ChainHeadLock {
    lock: Arc<RwLock<PendingBatchesPool>>,
}

impl ChainHeadLock {
    pub fn new(lock: Arc<RwLock<PendingBatchesPool>>) -> Self {
        ChainHeadLock { lock }
    }

    pub fn acquire(&self) -> ChainHeadGuard {
        ChainHeadGuard {
            state: self.lock.write().expect("Lock is not poisoned"),
        }
    }
}

/// RAII type that represents having acquired the lock used by the BlockPublisher
pub struct ChainHeadGuard<'a> {
    state: RwLockWriteGuard<'a, PendingBatchesPool>,
}

impl<'a> ChainHeadGuard<'a> {
    pub fn notify_on_chain_updated(
        &mut self,
        chain_head: BlockPair,
        committed_batches: Vec<BatchPair>,
        uncommitted_batches: Vec<BatchPair>,
    ) {
        if let Err(err) = BlockPublisher::on_chain_updated(
            &mut self.state,
            chain_head,
            committed_batches,
            uncommitted_batches,
        ) {
            error!(
                "An unexpected error occurred when notifying the publisher of a chain update: {}",
                err
            );
        }
    }
}
