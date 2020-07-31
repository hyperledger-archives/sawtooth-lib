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

use std::io::Cursor;

use cbor::decoder::GenericDecoder;
use cbor::value::Bytes;
use cbor::value::Value;

use transact::state::merkle::MerkleRadixTree;

use crate::state::error::StateDatabaseError;

use super::{StateIter, StateReader};

pub fn decode_cbor_value(cbor_bytes: &[u8]) -> Result<Vec<u8>, StateDatabaseError> {
    let input = Cursor::new(cbor_bytes);
    let mut decoder = GenericDecoder::new(cbor::Config::default(), input);
    let decoded_value = decoder.value()?;

    match decoded_value {
        Value::Bytes(Bytes::Bytes(bytes)) => Ok(bytes),
        _ => Err(StateDatabaseError::InvalidRecord),
    }
}

pub struct DecodedMerkleStateReader {
    merkle_database: MerkleRadixTree,
}

impl DecodedMerkleStateReader {
    pub fn new(merkle_database: MerkleRadixTree) -> Self {
        Self { merkle_database }
    }
}

impl StateReader for DecodedMerkleStateReader {
    fn contains(&self, address: &str) -> Result<bool, StateDatabaseError> {
        self.merkle_database
            .contains(address)
            .map_err(StateDatabaseError::from)
    }

    fn get(&self, address: &str) -> Result<Option<Vec<u8>>, StateDatabaseError> {
        Ok(
            match self
                .merkle_database
                .get_value(address)
                .map_err(StateDatabaseError::from)?
            {
                Some(bytes) => Some(decode_cbor_value(&bytes)?),
                None => None,
            },
        )
    }

    fn leaves(&self, prefix: Option<&str>) -> Result<Box<StateIter>, StateDatabaseError> {
        Ok(Box::new(
            self.merkle_database
                .leaves(prefix)
                .map_err(StateDatabaseError::from)?
                .map(|value| {
                    value
                        .map_err(StateDatabaseError::from)
                        .and_then(|(address, bytes)| {
                            decode_cbor_value(&bytes).map(|new_bytes| (address, new_bytes))
                        })
                }),
        ))
    }
}
