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

//! Genesis protocol

use protobuf::Message;
use transact::protocol::batch::BatchPair;

use crate::protos::{
    batch::Batch as BatchProto, genesis::GenesisData as GenesisDataProto, FromBytes, FromNative,
    FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

/// A named set of batches
#[derive(Debug, Clone, PartialEq)]
pub struct GenesisData {
    batches: Vec<BatchPair>,
}

impl GenesisData {
    pub fn batches(&self) -> &Vec<BatchPair> {
        &self.batches
    }

    pub fn take_batches(self) -> Vec<BatchPair> {
        self.batches
    }
}

impl FromBytes<GenesisData> for GenesisData {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        protobuf::parse_from_bytes::<GenesisDataProto>(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get GenesisData from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<GenesisData> for GenesisDataProto {
    fn from_native(genesis_data: GenesisData) -> Result<Self, ProtoConversionError> {
        let mut genesis_data_proto = GenesisDataProto::new();
        genesis_data_proto.set_batches(
            genesis_data
                .batches
                .into_iter()
                .map(BatchProto::from_native)
                .collect::<Result<_, _>>()?,
        );

        Ok(genesis_data_proto)
    }
}

impl FromProto<GenesisDataProto> for GenesisData {
    fn from_proto(genesis_data: GenesisDataProto) -> Result<Self, ProtoConversionError> {
        Ok(GenesisData {
            batches: genesis_data
                .get_batches()
                .to_vec()
                .into_iter()
                .map(BatchPair::from_proto)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl IntoBytes for GenesisData {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from GenesisData".to_string(),
            )
        })
    }
}

impl IntoNative<GenesisData> for GenesisDataProto {}
impl IntoProto<GenesisDataProto> for GenesisData {}
