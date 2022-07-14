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

//! Conatains the struct for starting publishing of a new artifact

use std::sync::mpsc::channel;
use std::thread;

use crate::artifact::{Artifact, ArtifactCreator, ArtifactCreatorFactory};
use crate::error::InternalError;

use super::message::PublishMessage;
use super::{
    Batch, BatchExecutionResult, BatchVerifierFactory, PendingBatches, PublishHandle,
    PublishingContext,
};

/// Factory to start publishing a new artificat
///
/// The `PublishFactory` starts up the exeuction and verification of batches by
/// spinning up a thread and returning a `PublishHandle` that can be used to
/// control the thread execution.
///
// allow type complexity here because of the use of associated types
#[allow(clippy::type_complexity)]
pub struct PublishFactory<B, C, R, I>
where
    B: 'static + Batch + Clone,
    C: 'static + PublishingContext + Clone,
    R: 'static + Artifact,
    I: 'static + BatchExecutionResult,
{
    artifact_creator_factory: Box<
        dyn ArtifactCreatorFactory<
            ArtifactCreator = Box<dyn ArtifactCreator<Context = C, Input = Vec<I>, Artifact = R>>,
        >,
    >,
    batch_verifier_factory:
        Box<dyn BatchVerifierFactory<Batch = B, Context = C, ExecutionResult = I>>,
}

/// Create a new `PublishFactory`
///
/// Arguments
///
/// * `artifact_creator_factory` - a factory for creating a new artifact creator for a new
///    publishing thread
/// * `batch_verifier_factory` - a factory for creating a new batch verifier for a new publishing
///    thread
///
// allow type complexity here because of the use of associated types
#[allow(clippy::type_complexity)]
impl<B, C, R, I> PublishFactory<B, C, R, I>
where
    B: 'static + Batch + Clone,
    C: 'static + PublishingContext + Clone,
    R: 'static + Artifact,
    I: 'static + BatchExecutionResult,
{
    pub fn new(
        artifact_creator_factory: Box<
            dyn ArtifactCreatorFactory<
                ArtifactCreator = Box<
                    dyn ArtifactCreator<Context = C, Input = Vec<I>, Artifact = R>,
                >,
            >,
        >,
        batch_verifier_factory: Box<
            dyn BatchVerifierFactory<Batch = B, Context = C, ExecutionResult = I>,
        >,
    ) -> Self {
        Self {
            artifact_creator_factory,
            batch_verifier_factory,
        }
    }
}

impl<B, C, R, I> PublishFactory<B, C, R, I>
where
    B: 'static + Batch + Clone,
    C: 'static + PublishingContext + Clone,
    R: 'static + Artifact,
    I: 'static + BatchExecutionResult,
{
    /// Start building the next `Artifact`
    ///
    /// # Arguments
    ///
    /// * `context` - Implementation specific context for the publisher
    /// * `batches` - An interator the returns the next batch to execute
    ///
    /// Returns a PublishHandle that can be used to finish or cancel the executing batch
    ///
    pub fn start(
        &mut self,
        mut context: C,
        mut batches: Box<dyn PendingBatches<B>>,
    ) -> Result<PublishHandle<R>, InternalError> {
        let (sender, rc) = channel();
        let mut verifier = self.batch_verifier_factory.start(context.clone())?;
        let artifact_creator = self.artifact_creator_factory.new_creator()?;
        let join_handle = thread::spawn(move || loop {
            // drain the queue
            while let Some(batch) = batches.next().map_err(|err| err.reduce_to_string())? {
                verifier
                    .add_batch(batch)
                    .map_err(|err| err.reduce_to_string())?;
            }

            // Check to see if the batch result should be finished/canceled
            match rc.recv() {
                Ok(PublishMessage::Cancel) => {
                    verifier.cancel().map_err(|err| err.reduce_to_string())?;
                    return Ok(None);
                }
                Ok(PublishMessage::Finish) => {
                    let results = verifier.finalize().map_err(|err| err.reduce_to_string())?;

                    return Ok(Some(
                        artifact_creator
                            .create(&mut context, results)
                            .map_err(|err| err.reduce_to_string())?,
                    ));
                }
                Ok(PublishMessage::Dropped) => {
                    return Ok(None);
                }
                Ok(PublishMessage::Next) => {
                    while let Some(batch) = batches.next().map_err(|err| err.reduce_to_string())? {
                        verifier
                            .add_batch(batch)
                            .map_err(|err| err.reduce_to_string())?;
                    }
                }
                Err(err) => {
                    return Err(InternalError::from_source(Box::new(err)).reduce_to_string());
                }
            };
        });

        Ok(PublishHandle::new(sender, join_handle))
    }
}
