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

//! Errors for the block publisher

use std::error::Error;

use cylinder::SigningError;
use hex::FromHexError;
use transact::{
    database::DatabaseError, execution::executor::ExecutorError, scheduler::SchedulerError,
    state::StateWriteError,
};

use crate::permissions::IdentityError;
use crate::protocol::ProtocolBuildError;
use crate::state::{error::StateDatabaseError, settings_view::SettingsViewError};

/// Errors that can occur in the block publisher
#[derive(Debug)]
pub enum BlockPublisherError {
    /// An error occurred when attempting to cancel a block
    BlockCancellationFailed(BlockCancellationError),
    /// An error occurred when attempting to initialize a block
    BlockInitializationFailed(BlockInitializationError),
    /// An error occurred when attempting to summarize a block
    BlockCompletionFailed(BlockCompletionError),
    /// An internal error that can't be handled externally
    Internal(String),
    /// An error occurred when attempting to start the block publisher
    StartupFailed(String),
}

impl Error for BlockPublisherError {}

impl std::fmt::Display for BlockPublisherError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::BlockCancellationFailed(err) => err.fmt(f),
            Self::BlockInitializationFailed(err) => err.fmt(f),
            Self::BlockCompletionFailed(err) => err.fmt(f),
            Self::Internal(msg) => f.write_str(msg),
            Self::StartupFailed(msg) => f.write_str(msg),
        }
    }
}

impl From<BlockCancellationError> for BlockPublisherError {
    fn from(err: BlockCancellationError) -> Self {
        Self::BlockCancellationFailed(err)
    }
}

impl From<BlockInitializationError> for BlockPublisherError {
    fn from(err: BlockInitializationError) -> Self {
        Self::BlockInitializationFailed(err)
    }
}

impl From<BlockCompletionError> for BlockPublisherError {
    fn from(err: BlockCompletionError) -> Self {
        Self::BlockCompletionFailed(err)
    }
}

impl From<SigningError> for BlockPublisherError {
    fn from(err: SigningError) -> Self {
        Self::Internal(err.to_string())
    }
}

impl From<FromHexError> for BlockPublisherError {
    fn from(err: FromHexError) -> Self {
        Self::Internal(err.to_string())
    }
}

impl From<DatabaseError> for BlockPublisherError {
    fn from(err: DatabaseError) -> Self {
        Self::Internal(err.to_string())
    }
}

impl From<ExecutorError> for BlockPublisherError {
    fn from(err: ExecutorError) -> Self {
        Self::Internal(err.to_string())
    }
}

impl From<SchedulerError> for BlockPublisherError {
    fn from(err: SchedulerError) -> Self {
        Self::Internal(err.to_string())
    }
}

impl From<StateWriteError> for BlockPublisherError {
    fn from(err: StateWriteError) -> Self {
        Self::Internal(err.to_string())
    }
}

impl From<IdentityError> for BlockPublisherError {
    fn from(err: IdentityError) -> Self {
        Self::Internal(err.to_string())
    }
}

impl From<ProtocolBuildError> for BlockPublisherError {
    fn from(err: ProtocolBuildError) -> Self {
        Self::Internal(err.to_string())
    }
}

impl From<StateDatabaseError> for BlockPublisherError {
    fn from(err: StateDatabaseError) -> Self {
        Self::Internal(err.to_string())
    }
}

impl From<SettingsViewError> for BlockPublisherError {
    fn from(err: SettingsViewError) -> Self {
        Self::Internal(err.to_string())
    }
}

/// Errors specific to block cancellation
#[derive(Debug)]
pub enum BlockCancellationError {
    /// A block is not being built
    BlockNotInitialized,
}

impl Error for BlockCancellationError {}

impl std::fmt::Display for BlockCancellationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::BlockNotInitialized => f.write_str("block not initialized"),
        }
    }
}

/// Errors specific to block initialization
#[derive(Debug)]
pub enum BlockInitializationError {
    /// A block is already in progress, so a new one cannot be initialized
    BlockInProgress,
    /// The previous block was not found
    MissingPredecessor,
}

impl Error for BlockInitializationError {}

impl std::fmt::Display for BlockInitializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::BlockInProgress => f.write_str("block already in progress"),
            Self::MissingPredecessor => f.write_str("missing previous block"),
        }
    }
}

/// Errors specific to block completion
#[derive(Debug)]
pub enum BlockCompletionError {
    /// A block is not being built
    BlockNotInitialized,
    /// The in-progress block does not have any batches
    BlockEmpty,
}

impl Error for BlockCompletionError {}

impl std::fmt::Display for BlockCompletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::BlockNotInitialized => f.write_str("block not initialized"),
            Self::BlockEmpty => f.write_str("no batches in block"),
        }
    }
}
