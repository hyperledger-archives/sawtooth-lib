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

//! Submitter for the pending batches pool

mod error;

use std::sync::mpsc::{Receiver, Sender, TryRecvError};

use transact::protocol::batch::BatchPair;

use super::BlockPublisherMessage;

pub use error::BatchSubmitterError;

/// Sends batches to the pending batches pool and tracks whether or not the pool is full
pub struct BatchSubmitter {
    /// Used to send the batches to the publisher
    batch_sender: Sender<BlockPublisherMessage>,
    /// Used to receive notifications when the pool is full or openned up
    backpressure_receiver: Receiver<BackpressureMessage>,
    /// Tracks whether or not the pool is full
    pool_full: bool,
}

impl BatchSubmitter {
    /// Creates a new `BatchSubmitter`
    pub(super) fn new(
        batch_sender: Sender<BlockPublisherMessage>,
        backpressure_receiver: Receiver<BackpressureMessage>,
    ) -> Self {
        Self {
            batch_sender,
            backpressure_receiver,
            pool_full: false,
        }
    }

    /// Checks if the batch pool is full
    pub fn is_batch_pool_full(&mut self) -> Result<bool, BatchSubmitterError> {
        self.check_backpressure()?;
        Ok(self.pool_full)
    }

    /// Submits the given batch to the publisher
    ///
    /// # Arguments
    ///
    /// * `batch` - The batch pair to send to the publisher
    /// * `force` - If `true`, the batch will be sent to the publisher even if the pool is full
    ///
    /// # Errors
    ///
    /// * Returns a `PoolFull` error containing the submitted batch when the pool is full; if
    ///   `force`, this error will not be returned.
    /// * Returns a `PoolShutdown` error if the publisher is no longer receiving batches
    pub fn submit(&mut self, batch: BatchPair, force: bool) -> Result<(), BatchSubmitterError> {
        self.check_backpressure()?;

        if self.pool_full && !force {
            Err(BatchSubmitterError::PoolFull(batch))
        } else {
            self.batch_sender
                .send(batch.into())
                .map_err(|_| BatchSubmitterError::PoolShutdown)
        }
    }

    fn check_backpressure(&mut self) -> Result<(), BatchSubmitterError> {
        match self.backpressure_receiver.try_recv() {
            Ok(BackpressureMessage::PoolFull) => self.pool_full = true,
            Ok(BackpressureMessage::PoolUnblocked) => self.pool_full = false,
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => return Err(BatchSubmitterError::PoolShutdown),
        }

        Ok(())
    }
}

/// Indicates if the pending batches pool has filled up or re-opened
pub enum BackpressureMessage {
    /// The pending batches pool has filled up
    PoolFull,
    /// The pending batches pool has been drained enough that it is no longer full
    PoolUnblocked,
}
