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

use crate::store::StoreError;

use super::Artifact;

/// An artifact store.
///
/// Implementations of an `ArtifactStore` are dependent on the specific structure of the artifact
/// itself.
pub trait ArtifactStore {
    type Artifact: Artifact;

    /// Save an artifact to the store, with a given artifact identifier.
    ///
    /// This method should store the artifact in a way that it may be looked up by its defined
    /// identifier.
    fn create_artifact(&self, artifact: Self::Artifact) -> Result<(), StoreError>;

    /// Return an artifact for the given identifier, if it exists.
    ///
    /// This method should use the defined identifier type to retrieve an artifact from the
    /// database.
    fn get_artifact(
        &self,
        identifier: &<Self::Artifact as Artifact>::Identifier,
    ) -> Result<Option<Self::Artifact>, StoreError>;

    /// Delete the artifact, if it is unreferenced.
    ///
    /// The underlying implementation should maintain any references required by other components
    /// and subsystems to the stored artifacts. If there are no references to the artifact, then
    /// it is safe to delete the artifact.
    fn delete_if_unref_artifact(
        &self,
        identifier: &<Self::Artifact as Artifact>::Identifier,
    ) -> Result<(), StoreError>;
}
