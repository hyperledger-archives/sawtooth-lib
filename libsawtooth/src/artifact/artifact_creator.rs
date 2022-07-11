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

use crate::error::InternalError;

/// An artifact creator.
///
/// Used to create new instances of `Artifact`.
pub trait ArtifactCreator: Send {
    /// The context that contains extraneous information required to create the
    /// `Artifact`.
    type Context;
    /// The type of input for the specific `Artifact`
    type Input;
    /// The `Artifact` type
    type Artifact;

    /// Creates a new `Artifact`
    ///
    /// Returns a an `Artifact` created from the context and input
    fn create(
        &self,
        context: &mut Self::Context,
        input: Self::Input,
    ) -> Result<Self::Artifact, InternalError>;
}

/// An artifact creator factory.
///
/// Used to create new instances of `ArtifactCreator`.
pub trait ArtifactCreatorFactory {
    /// The `ArtifactCreator` type
    type ArtifactCreator;

    /// Returns a new `ArtifactCreator`
    fn new_creator(&self) -> Result<Self::ArtifactCreator, InternalError>;
}
