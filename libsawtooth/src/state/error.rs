/*
 * Copyright 2018 Intel Corporation
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
use std::fmt;

use cbor::decoder::DecodeError;
use cbor::encoder::EncodeError;
use protobuf::ProtobufError;
use transact::database::error::DatabaseError;
use transact::state::merkle::StateDatabaseError as TransactStateDatabaseError;

use crate::error::InternalError;

#[derive(Debug)]
pub enum StateDatabaseError {
    NotFound(String),
    DeserializationError(DecodeError),
    SerializationError(EncodeError),
    ChangeLogEncodingError(String),
    Internal(InternalError),
    InvalidRecord,
    InvalidHash(String),
    #[allow(dead_code)]
    InvalidChangeLogIndex(String),
    DatabaseError(DatabaseError),
    ProtobufConversionError(Box<dyn Error>),
    UnknownError,
}

impl fmt::Display for StateDatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            StateDatabaseError::NotFound(ref msg) => write!(f, "Value not found: {}", msg),
            StateDatabaseError::DeserializationError(ref err) => {
                write!(f, "Unable to deserialize entry: {}", err)
            }
            StateDatabaseError::SerializationError(ref err) => {
                write!(f, "Unable to serialize entry: {}", err)
            }
            StateDatabaseError::ChangeLogEncodingError(ref msg) => {
                write!(f, "Unable to serialize change log entry: {}", msg)
            }
            StateDatabaseError::Internal(ref err) => f.write_str(&err.to_string()),
            StateDatabaseError::InvalidRecord => write!(f, "A node was malformed"),
            StateDatabaseError::InvalidHash(ref msg) => {
                write!(f, "The given hash is invalid: {}", msg)
            }
            StateDatabaseError::InvalidChangeLogIndex(ref msg) => {
                write!(f, "A change log entry was missing or malformed: {}", msg)
            }
            StateDatabaseError::DatabaseError(ref err) => {
                write!(f, "A database error occurred: {}", err)
            }
            StateDatabaseError::ProtobufConversionError(ref err) => {
                write!(f, "A protobuf conversion error occurred: {}", err)
            }
            StateDatabaseError::UnknownError => write!(f, "An unknown error occurred"),
        }
    }
}

impl Error for StateDatabaseError {
    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            StateDatabaseError::NotFound(_) => None,
            StateDatabaseError::DeserializationError(ref err) => Some(err),
            StateDatabaseError::SerializationError(ref err) => Some(err),
            StateDatabaseError::ChangeLogEncodingError(_) => None,
            StateDatabaseError::Internal(ref err) => Some(err),
            StateDatabaseError::InvalidRecord => None,
            StateDatabaseError::InvalidHash(_) => None,
            StateDatabaseError::InvalidChangeLogIndex(_) => None,
            StateDatabaseError::DatabaseError(ref err) => Some(err),
            StateDatabaseError::ProtobufConversionError(ref err) => Some(&**err),
            StateDatabaseError::UnknownError => None,
        }
    }
}

impl From<TransactStateDatabaseError> for StateDatabaseError {
    fn from(err: TransactStateDatabaseError) -> Self {
        match err {
            TransactStateDatabaseError::NotFound(msg) => StateDatabaseError::NotFound(msg),
            TransactStateDatabaseError::DeserializationError(err) => {
                StateDatabaseError::DeserializationError(err)
            }
            TransactStateDatabaseError::SerializationError(err) => {
                StateDatabaseError::SerializationError(err)
            }
            TransactStateDatabaseError::ChangeLogEncodingError(msg) => {
                StateDatabaseError::ChangeLogEncodingError(msg)
            }
            TransactStateDatabaseError::InvalidRecord => StateDatabaseError::InvalidRecord,
            TransactStateDatabaseError::InvalidHash(msg) => StateDatabaseError::InvalidHash(msg),
            TransactStateDatabaseError::InvalidChangeLogIndex(msg) => {
                StateDatabaseError::InvalidChangeLogIndex(msg)
            }
            TransactStateDatabaseError::DatabaseError(err) => {
                StateDatabaseError::DatabaseError(err)
            }
            TransactStateDatabaseError::ProtobufConversionError(err) => {
                StateDatabaseError::ProtobufConversionError(Box::new(err))
            }
            TransactStateDatabaseError::UnknownError => StateDatabaseError::UnknownError,
        }
    }
}

impl From<DatabaseError> for StateDatabaseError {
    fn from(err: DatabaseError) -> Self {
        StateDatabaseError::DatabaseError(err)
    }
}

impl From<EncodeError> for StateDatabaseError {
    fn from(err: EncodeError) -> Self {
        StateDatabaseError::SerializationError(err)
    }
}

impl From<DecodeError> for StateDatabaseError {
    fn from(err: DecodeError) -> Self {
        StateDatabaseError::DeserializationError(err)
    }
}

impl From<ProtobufError> for StateDatabaseError {
    fn from(error: ProtobufError) -> Self {
        use self::ProtobufError::*;
        match error {
            IoError(err) => StateDatabaseError::ChangeLogEncodingError(format!("{}", err)),
            WireError(err) => StateDatabaseError::ChangeLogEncodingError(format!("{:?}", err)),
            Utf8(err) => StateDatabaseError::ChangeLogEncodingError(format!("{}", err)),
            MessageNotInitialized { message: err } => {
                StateDatabaseError::ChangeLogEncodingError(err.to_string())
            }
        }
    }
}
