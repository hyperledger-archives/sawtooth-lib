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

//! Structs that cover the core protocols of the Sawtooth system.

#[cfg(feature = "transact-protocol-batch")]
pub mod batch;
#[cfg(feature = "protocol-block")]
pub mod block;
pub mod block_info;
pub mod command;
#[cfg(feature = "protocol-genesis")]
pub mod genesis;
pub mod identity;
#[cfg(feature = "transact-key-value-state")]
pub mod key_value_state;
#[cfg(feature = "transact")]
pub mod receipt;
#[cfg(any(feature = "transact-protocol-sabre", feature = "family-sabre"))]
pub mod sabre;
pub mod setting;
#[cfg(feature = "transact-protocol-transaction")]
pub mod transaction;

#[cfg(feature = "cylinder")]
use cylinder::SigningError;

use crate::protos::ProtoConversionError;

/// Errors that may occur when building a protocol object
#[derive(Debug)]
pub enum ProtocolBuildError {
    MissingField(String),
    DeserializationError(String),
    SigningError(String),
}

impl std::error::Error for ProtocolBuildError {
    fn description(&self) -> &str {
        match *self {
            Self::MissingField(ref msg) => msg,
            Self::DeserializationError(ref msg) => msg,
            Self::SigningError(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for ProtocolBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::MissingField(ref s) => write!(f, "missing a required field: {}", s),
            Self::DeserializationError(ref s) => write!(f, "failed to deserialize: {}", s),
            Self::SigningError(ref s) => write!(f, "failed to sign: {}", s),
        }
    }
}

impl From<ProtoConversionError> for ProtocolBuildError {
    fn from(err: ProtoConversionError) -> Self {
        Self::DeserializationError(format!("{}", err))
    }
}

#[cfg(feature = "cylinder")]
impl From<SigningError> for ProtocolBuildError {
    fn from(err: SigningError) -> Self {
        Self::SigningError(format!("{}", err))
    }
}
