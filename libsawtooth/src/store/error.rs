// Copyright 2022 Cargill Incorporated
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

use crate::error::{ConstraintViolationError, InternalError, ResourceTemporarilyUnavailableError};
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};

/// A common store error.
///
/// This error covers the common set of variants pertinent to all Store implementations.
#[derive(Debug)]
pub enum StoreError {
    Internal(InternalError),
    ConstraintViolation(ConstraintViolationError),
    ResourceTemporarilyUnavailable(ResourceTemporarilyUnavailableError),
}

impl Display for StoreError {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            StoreError::Internal(err) => f.write_str(&err.to_string()),
            StoreError::ConstraintViolation(err) => f.write_str(&err.to_string()),
            StoreError::ResourceTemporarilyUnavailable(err) => f.write_str(&err.to_string()),
        }
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StoreError::Internal(err) => Some(err),
            StoreError::ConstraintViolation(err) => Some(err),
            StoreError::ResourceTemporarilyUnavailable(err) => Some(err),
        }
    }
}

impl From<InternalError> for StoreError {
    fn from(e: InternalError) -> Self {
        Self::Internal(e)
    }
}

impl From<ConstraintViolationError> for StoreError {
    fn from(e: ConstraintViolationError) -> Self {
        Self::ConstraintViolation(e)
    }
}

impl From<ResourceTemporarilyUnavailableError> for StoreError {
    fn from(e: ResourceTemporarilyUnavailableError) -> Self {
        Self::ResourceTemporarilyUnavailable(e)
    }
}
