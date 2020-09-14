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
use std::error::Error;

/// Represents errors raised while building
#[derive(Debug)]
pub enum GenesisError {
    BatchValidationError(String),
    BlockGenerationError(String),
    InvalidGenesisState(String),
    InvalidGenesisData(String),
    LocalConfigurationError(String),
}

impl Error for GenesisError {}

impl std::fmt::Display for GenesisError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            GenesisError::BatchValidationError(ref msg) => {
                write!(f, "Unable to validate batches {}", msg)
            }
            GenesisError::BlockGenerationError(ref msg) => {
                write!(f, "Unable to create genesisi block {}", msg)
            }
            GenesisError::InvalidGenesisState(ref msg) => write!(f, "{}", msg),
            GenesisError::InvalidGenesisData(ref msg) => write!(f, "{}", msg),
            GenesisError::LocalConfigurationError(ref msg) => {
                write!(f, "Unable to start validator: {}", msg)
            }
        }
    }
}

/// Errors that may occur when building the GenesisController
#[derive(Debug)]
pub enum GenesisControllerBuildError {
    MissingField(String),
}

impl std::error::Error for GenesisControllerBuildError {
    fn description(&self) -> &str {
        match *self {
            Self::MissingField(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for GenesisControllerBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::MissingField(ref s) => write!(f, "missing a required field: {}", s),
        }
    }
}
