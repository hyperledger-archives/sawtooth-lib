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

//! Implementations of Sawtooth's protobuf conversion traits for the
//! [`Transact`](https://crates.io/crates/transact) batch protocol.

use protobuf::Message;
use transact::protocol::batch::{Batch, BatchHeader, BatchPair};

use crate::protos::{
    batch::{Batch as BatchProto, BatchHeader as BatchHeaderProto, BatchList},
    FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

impl FromBytes<Batch> for Batch {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Batch from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<Batch> for BatchProto {
    fn from_native(batch: Batch) -> Result<Self, ProtoConversionError> {
        let mut proto_batch = BatchProto::new();

        proto_batch.set_header(batch.header().into());
        proto_batch.set_header_signature(batch.header_signature().into());
        proto_batch.set_transactions(
            batch
                .transactions()
                .iter()
                .cloned()
                .map(IntoProto::into_proto)
                .collect::<Result<_, _>>()?,
        );
        proto_batch.set_trace(batch.trace());

        Ok(proto_batch)
    }
}

impl FromProto<BatchProto> for Batch {
    fn from_proto(batch: BatchProto) -> Result<Self, ProtoConversionError> {
        let bytes = batch.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError("Unable to get bytes from Batch".to_string())
        })?;

        <Batch as transact::protos::FromBytes<_>>::from_bytes(&bytes).map_err(|_| {
            ProtoConversionError::DeserializationError("Unable to get Batch from bytes".to_string())
        })
    }
}

impl IntoBytes for Batch {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError("Unable to get bytes from Batch".to_string())
        })
    }
}

impl IntoNative<Batch> for BatchProto {}
impl IntoProto<BatchProto> for Batch {}

impl FromBytes<Vec<Batch>> for Vec<Batch> {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Vec<Batch> from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<Vec<Batch>> for BatchList {
    fn from_native(batches: Vec<Batch>) -> Result<Self, ProtoConversionError> {
        let batches = batches
            .into_iter()
            .map(FromNative::from_native)
            .collect::<Result<_, _>>()?;

        let mut batch_list_proto = BatchList::new();
        batch_list_proto.set_batches(batches);

        Ok(batch_list_proto)
    }
}

impl FromProto<BatchList> for Vec<Batch> {
    fn from_proto(batch_list: BatchList) -> Result<Self, ProtoConversionError> {
        batch_list
            .batches
            .into_iter()
            .map(FromProto::from_proto)
            .collect()
    }
}

impl IntoBytes for Vec<Batch> {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from Vec<Batch>".to_string(),
            )
        })
    }
}

impl IntoNative<Vec<Batch>> for BatchList {}
impl IntoProto<BatchList> for Vec<Batch> {}

impl FromBytes<BatchHeader> for BatchHeader {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get BatchHeader from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<BatchHeader> for BatchHeaderProto {
    fn from_native(header: BatchHeader) -> Result<Self, ProtoConversionError> {
        let mut batch_header_proto = BatchHeaderProto::new();

        batch_header_proto.set_signer_public_key(hex::encode(header.signer_public_key()));
        batch_header_proto
            .set_transaction_ids(header.transaction_ids().iter().map(hex::encode).collect());

        Ok(batch_header_proto)
    }
}

impl FromProto<BatchHeaderProto> for BatchHeader {
    fn from_proto(header: BatchHeaderProto) -> Result<Self, ProtoConversionError> {
        let bytes = header.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from BatchHeader".to_string(),
            )
        })?;

        <BatchHeader as transact::protos::FromBytes<_>>::from_bytes(&bytes).map_err(|_| {
            ProtoConversionError::DeserializationError(
                "Unable to get BatchHeader from bytes".to_string(),
            )
        })
    }
}

impl IntoBytes for BatchHeader {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from BatchHeader".to_string(),
            )
        })
    }
}

impl IntoNative<BatchHeader> for BatchHeaderProto {}
impl IntoProto<BatchHeaderProto> for BatchHeader {}

impl FromBytes<BatchPair> for BatchPair {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Batch::from_bytes(bytes)?
            .into_pair()
            .map_err(|err| ProtoConversionError::DeserializationError(err.to_string()))
    }
}

impl FromNative<BatchPair> for BatchProto {
    fn from_native(batch_pair: BatchPair) -> Result<Self, ProtoConversionError> {
        batch_pair.take().0.into_proto()
    }
}

impl FromProto<BatchProto> for BatchPair {
    fn from_proto(batch: BatchProto) -> Result<Self, ProtoConversionError> {
        Batch::from_proto(batch)?
            .into_pair()
            .map_err(|err| ProtoConversionError::DeserializationError(err.to_string()))
    }
}

impl IntoBytes for BatchPair {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.take().0.into_bytes()
    }
}

impl IntoNative<BatchPair> for BatchProto {}
impl IntoProto<BatchProto> for BatchPair {}

impl FromBytes<Vec<BatchPair>> for Vec<BatchPair> {
    fn from_bytes(bytes: &[u8]) -> Result<Vec<BatchPair>, ProtoConversionError> {
        <Vec<Batch> as FromBytes<_>>::from_bytes(bytes)?
            .into_iter()
            .map(Batch::into_pair)
            .collect::<Result<_, _>>()
            .map_err(|err| ProtoConversionError::DeserializationError(err.to_string()))
    }
}
