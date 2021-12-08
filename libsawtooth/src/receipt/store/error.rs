/*
 * Copyright 2021 Cargill Incorporated
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

use std::error::Error;

#[cfg(feature = "diesel")]
use crate::error::ConstraintViolationType;
use crate::error::{
    ConstraintViolationError, InternalError, InvalidStateError, ResourceTemporarilyUnavailableError,
};

#[derive(Debug)]
pub enum ReceiptStoreError {
    ConstraintViolationError(ConstraintViolationError),
    InternalError(InternalError),
    InvalidStateError(InvalidStateError),
    ResourceTemporarilyUnavailableError(ResourceTemporarilyUnavailableError),
}

impl Error for ReceiptStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConstraintViolationError(err) => Some(err),
            Self::InternalError(err) => Some(err),
            Self::InvalidStateError(err) => Some(err),
            Self::ResourceTemporarilyUnavailableError(err) => Some(err),
        }
    }
}

impl std::fmt::Display for ReceiptStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ConstraintViolationError(err) => err.fmt(f),
            Self::InternalError(err) => err.fmt(f),
            Self::InvalidStateError(err) => err.fmt(f),
            Self::ResourceTemporarilyUnavailableError(err) => err.fmt(f),
        }
    }
}

impl From<InternalError> for ReceiptStoreError {
    fn from(err: InternalError) -> Self {
        Self::InternalError(err)
    }
}

impl From<ConstraintViolationError> for ReceiptStoreError {
    fn from(err: ConstraintViolationError) -> Self {
        Self::ConstraintViolationError(err)
    }
}

impl From<InvalidStateError> for ReceiptStoreError {
    fn from(err: InvalidStateError) -> Self {
        Self::InvalidStateError(err)
    }
}

impl From<ResourceTemporarilyUnavailableError> for ReceiptStoreError {
    fn from(err: ResourceTemporarilyUnavailableError) -> Self {
        Self::ResourceTemporarilyUnavailableError(err)
    }
}

#[cfg(feature = "diesel")]
impl From<diesel::r2d2::PoolError> for ReceiptStoreError {
    fn from(err: diesel::r2d2::PoolError) -> Self {
        ReceiptStoreError::ResourceTemporarilyUnavailableError(
            ResourceTemporarilyUnavailableError::from_source(Box::new(err)),
        )
    }
}

#[cfg(feature = "diesel")]
impl From<diesel::result::Error> for ReceiptStoreError {
    fn from(err: diesel::result::Error) -> Self {
        match err {
            diesel::result::Error::DatabaseError(db_err_kind, _) => match db_err_kind {
                diesel::result::DatabaseErrorKind::UniqueViolation => {
                    ReceiptStoreError::ConstraintViolationError(
                        ConstraintViolationError::from_source_with_violation_type(
                            ConstraintViolationType::Unique,
                            Box::new(err),
                        ),
                    )
                }
                diesel::result::DatabaseErrorKind::ForeignKeyViolation => {
                    ReceiptStoreError::ConstraintViolationError(
                        ConstraintViolationError::from_source_with_violation_type(
                            ConstraintViolationType::ForeignKey,
                            Box::new(err),
                        ),
                    )
                }
                _ => ReceiptStoreError::InternalError(InternalError::from_source(Box::new(err))),
            },
            _ => ReceiptStoreError::InternalError(InternalError::from_source(Box::new(err))),
        }
    }
}
