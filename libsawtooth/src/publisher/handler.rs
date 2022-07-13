/*
 * Copyright 2022 Cargill Incorporated
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

//! Contains the `PublishHandle` struct used for interating publishing thread

use std::sync::mpsc::Sender;
use std::thread;

use log::error;

use crate::artifact::Artifact;
use crate::error::InternalError;

use super::message::PublishMessage;

/// Handler for interating with a publishing thread
pub struct PublishHandle<R>
where
    R: Artifact,
{
    sender: Option<Sender<PublishMessage>>,
    join_handle: Option<thread::JoinHandle<Result<Option<R>, String>>>,
}

impl<R> PublishHandle<R>
where
    R: Artifact,
{
    /// Create a new `PublishHandle`
    ///
    /// Arguments
    ///
    /// * `sender` - The `Sender` for seting updates to the publishing thread
    /// * `join_handle` - The `JoinHandle` to the publishing thread, used to get the published
    ///    `Artifact`
    pub fn new(
        sender: Sender<PublishMessage>,
        join_handle: thread::JoinHandle<Result<Option<R>, String>>,
    ) -> Self {
        Self {
            sender: Some(sender),
            join_handle: Some(join_handle),
        }
    }

    /// Finish constructing the block, returning a result that contains the bytes that consensus
    /// must agree upon and a list of TransactionReceipts. Any batches that are not finished
    /// processing, will be returned to the pending state.
    pub fn finish(mut self) -> Result<R, InternalError> {
        if let Some(sender) = self.sender.take() {
            sender
                .send(PublishMessage::Finish)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;
            match self
                .join_handle
                .take()
                .ok_or_else(|| InternalError::with_message("Missing join handle".into()))?
                .join()
                .map_err(|err| {
                    InternalError::with_message(format!(
                        "Unable to call join on join handle: {:?}",
                        err
                    ))
                })? {
                Ok(Some(result)) => Ok(result),
                // no result returned
                Ok(None) => Err(InternalError::with_message(
                    "Publishing was finalized, should have received results".into(),
                )),
                Err(err) => Err(InternalError::with_message(err)),
            }
        } else {
            // already called finish or cancel
            Err(InternalError::with_message(
                "Publishing has already been finished or canceled".into(),
            ))
        }
    }

    /// Cancel the currently building block, putting all batches back into a pending state
    pub fn cancel(mut self) -> Result<(), InternalError> {
        if let Some(sender) = self.sender.take() {
            sender
                .send(PublishMessage::Cancel)
                .map_err(|err| InternalError::from_source(Box::new(err)))?;
            match self
                .join_handle
                .take()
                .ok_or_else(|| InternalError::with_message("Missing join handle".into()))?
                .join()
                .map_err(|err| {
                    InternalError::with_message(format!(
                        "Unable to call join on join handle: {:?}",
                        err
                    ))
                })? {
                // Did not expect any results
                Ok(Some(_)) => Err(InternalError::with_message(
                    "Publishing was cancel, should not have got results".into(),
                )),
                Ok(None) => Ok(()),
                Err(err) => Err(InternalError::with_message(err)),
            }
        } else {
            // already called finish or cancel
            Err(InternalError::with_message(
                "Publishing has already been finished or canceled".into(),
            ))
        }
    }

    /// Notify that there is a batch added to the pending queue
    pub fn next_batch(&self) -> Result<(), InternalError> {
        if let Some(sender) = &self.sender {
            sender
                .send(PublishMessage::Next)
                .map_err(|err| InternalError::from_source(Box::new(err)))
        } else {
            Ok(())
        }
    }
}

impl<R> Drop for PublishHandle<R>
where
    R: Artifact,
{
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            match sender.send(PublishMessage::Dropped) {
                Ok(_) => (),
                Err(_) => {
                    error!("Unable to shutdown Publisher thread")
                }
            }
        }
    }
}
