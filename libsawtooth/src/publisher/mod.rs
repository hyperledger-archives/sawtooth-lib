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

//! Contains traits and struct required publishing new artifacts

mod batch;
mod batch_verifier;
mod context;
mod factory;
mod handler;
mod message;
mod pending_batches;

pub use batch::Batch;
pub use batch_verifier::{BatchExecutionResult, BatchVerifier, BatchVerifierFactory};
pub use context::PublishingContext;
pub use factory::PublishFactory;
pub use handler::PublishHandle;
pub use pending_batches::PendingBatches;

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    use crate::artifact::{Artifact, ArtifactCreator, ArtifactCreatorFactory};
    use crate::error::InternalError;

    /// Verify that the `PublisherFactory` and `PublishHandle` work as expected.
    ///
    /// 1. Create an `ArtifactCreatorFactory` and `BatchVerifierFactory` that uses the test
    ///    implementation of the traits
    /// 2. Create the `PublishFactory`
    /// 3. Create a batch iterator with 1 batch and a genesis publishing context
    /// 4. Start publishing thread, verify a publish handle is returned
    /// 5. Use `PublishHandle` to notify thread of next batch and finish artifact
    /// 6. Verify the correct artifact is returned and the context was updated
    /// 7. Start a publishing thread with same context and new pending batches
    /// 8. Cancel publishing and verify the context was not updated
    /// 9. Start publishing with same context and new pending batches
    /// 10. Verify a new artifact is returned and that the context was updated
    #[test]
    fn test_artifact_publishing() {
        let artifact_creator_factory = Box::new(TestArtifactCreatorFactory {});
        let batch_verifier_factory = Box::new(TestBatchVerifierFactory {});

        let mut publish_factory: PublishFactory<
            TestBatch,
            Arc<Mutex<TestContext>>,
            TestArtifact,
            TestBatchExecutionResult,
        > = PublishFactory::new(artifact_creator_factory, batch_verifier_factory);

        let pending_batches = Box::new(BatchIter {
            batches: vec![TestBatch {
                value: "value_1".to_string(),
            }],
        });

        let context = Arc::new(Mutex::new(TestContext {
            current_block_height: 0,
            current_state_value: "genesis".to_string(),
        }));

        // Start publishing for first batch
        let publish_handle = publish_factory
            .start(context.clone(), pending_batches)
            .expect("Unable to start publishing thread");

        publish_handle
            .next_batch()
            .expect("Unable to notify publisher thread of new batch");
        // Finish the publishing of the artifact
        let artifact = publish_handle
            .finish()
            .expect("Unable to finalize publishing thread");

        assert_eq!(artifact.block_height, 1);
        assert_eq!(artifact.current_value, "value_1".to_string());

        let context_unlocked = context.lock().unwrap();
        assert_eq!(context_unlocked.current_block_height, 1);
        assert_eq!(context_unlocked.current_state_value, "value_1".to_string());

        drop(context_unlocked);

        // Start publishing for execution that will be canceled
        let pending_batches = Box::new(BatchIter {
            batches: vec![TestBatch {
                value: "value_bad".to_string(),
            }],
        });

        let publish_handle = publish_factory
            .start(context.clone(), pending_batches)
            .expect("Unable to start publishing thread");

        publish_handle
            .next_batch()
            .expect("Unable to notify publisher thread of new batch");

        // cancel publishing
        publish_handle
            .cancel()
            .expect("Unable to finalize publishing thread");

        // verify that the context was not updated
        let context_unlocked = context.lock().unwrap();
        assert_eq!(context_unlocked.current_block_height, 1);
        assert_eq!(context_unlocked.current_state_value, "value_1".to_string());
        drop(context_unlocked);

        let pending_batches = Box::new(BatchIter {
            batches: vec![TestBatch {
                value: "value_2".to_string(),
            }],
        });

        // start publishing a new artifact
        let publish_handle = publish_factory
            .start(context.clone(), pending_batches)
            .expect("Unable to start publishing thread");

        publish_handle
            .next_batch()
            .expect("Unable to notify publisher thread of new batch");

        // Finalize the new artifact
        let artifact = publish_handle
            .finish()
            .expect("Unable to finalize publishing thread");

        // verify the correct artifact was returned and the context updated
        assert_eq!(artifact.block_height, 2);
        assert_eq!(artifact.current_value, "value_2".to_string());

        let context_unlocked = context.lock().unwrap();
        assert_eq!(context_unlocked.current_block_height, 2);
        assert_eq!(context_unlocked.current_state_value, "value_2".to_string());
    }

    #[derive(Clone)]
    struct TestBatch {
        value: String,
    }

    impl Batch for TestBatch {}

    #[derive(Clone)]
    struct TestContext {
        current_block_height: i32,
        current_state_value: String,
    }

    impl PublishingContext for Arc<Mutex<TestContext>> {}

    #[derive(Clone)]
    struct TestArtifact {
        block_height: i32,
        current_value: String,
    }

    impl Artifact for TestArtifact {
        type Identifier = i32;

        /// Returns a reference to this artifact's identifier.
        fn artifact_id(&self) -> &Self::Identifier {
            &self.block_height
        }
    }

    struct TestArtifactCreator {}

    impl ArtifactCreator for TestArtifactCreator {
        /// The context that contains extraneous information required to create the
        /// `Artifact`.
        type Context = Arc<Mutex<TestContext>>;
        /// The type of input for the specific `Artifact`
        type Input = Vec<TestBatchExecutionResult>;
        /// The `Artifact` type
        type Artifact = TestArtifact;

        /// Creates a new `Artifact`
        ///
        /// Returns a an `Artifact` created from the context and input
        fn create(
            &self,
            context: &mut Self::Context,
            input: Self::Input,
        ) -> Result<Self::Artifact, InternalError> {
            let mut context = context.lock().unwrap();
            context.current_block_height = context.current_block_height + 1;
            context.current_state_value = input[0].value.to_string();
            Ok(TestArtifact {
                block_height: context.current_block_height,
                current_value: input[0].value.to_string(),
            })
        }
    }

    struct TestArtifactCreatorFactory {}

    impl ArtifactCreatorFactory for TestArtifactCreatorFactory {
        /// The `ArtifactCreator` type
        type ArtifactCreator = Box<
            dyn ArtifactCreator<
                Context = Arc<Mutex<TestContext>>,
                Input = Vec<TestBatchExecutionResult>,
                Artifact = TestArtifact,
            >,
        >;

        /// Returns a new `ArtifactCreator`
        fn new_creator(&self) -> Result<Self::ArtifactCreator, InternalError> {
            Ok(Box::new(TestArtifactCreator {}))
        }
    }

    struct TestBatchExecutionResult {
        value: String,
    }

    impl BatchExecutionResult for TestBatchExecutionResult {}

    struct TestBatchVerifier {
        current_value: Option<String>,
    }

    impl BatchVerifier for TestBatchVerifier {
        type Batch = TestBatch;
        type Context = Arc<Mutex<TestContext>>;
        type ExecutionResult = TestBatchExecutionResult;

        /// Add a new batch to the verifier
        fn add_batch(&mut self, batch: Self::Batch) -> Result<(), InternalError> {
            self.current_value = Some(batch.value);
            Ok(())
        }

        /// Finalize the verification of the batches, returning the execution results of batches
        /// that have completed execution.
        fn finalize(&mut self) -> Result<Vec<Self::ExecutionResult>, InternalError> {
            Ok(vec![TestBatchExecutionResult {
                value: self
                    .current_value
                    .as_ref()
                    .ok_or_else(|| InternalError::with_message("no value set".into()))?
                    .clone(),
            }])
        }

        /// Cancel exeuction of batches, discarding all excution results
        fn cancel(&mut self) -> Result<(), InternalError> {
            self.current_value = None;
            Ok(())
        }
    }

    struct TestBatchVerifierFactory {}

    impl BatchVerifierFactory for TestBatchVerifierFactory {
        type Batch = TestBatch;
        type Context = Arc<Mutex<TestContext>>;
        type ExecutionResult = TestBatchExecutionResult;

        // allow type complexity here because of the use of associated types
        #[allow(clippy::type_complexity)]
        /// Start execution of batches in relation to a specific context by
        /// returning a `BatchVerifier`
        fn start(
            &mut self,
            _context: Self::Context,
        ) -> Result<
            Box<
                dyn BatchVerifier<
                    Batch = Self::Batch,
                    Context = Self::Context,
                    ExecutionResult = Self::ExecutionResult,
                >,
            >,
            InternalError,
        > {
            Ok(Box::new(TestBatchVerifier {
                current_value: None,
            }))
        }
    }

    struct BatchIter {
        batches: Vec<TestBatch>,
    }

    impl PendingBatches<TestBatch> for BatchIter {
        fn next(&mut self) -> Result<Option<TestBatch>, InternalError> {
            Ok(self.batches.pop())
        }
    }
}
