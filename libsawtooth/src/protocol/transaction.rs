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
//! [`Transact`](https://crates.io/crates/transact) transaction protocol.

use protobuf::Message;
use transact::protocol::transaction::{Transaction, TransactionHeader};

use crate::protos::{
    transaction::{Transaction as TransactionProto, TransactionHeader as TransactionHeaderProto},
    FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

impl FromBytes<Transaction> for Transaction {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        protobuf::parse_from_bytes::<TransactionProto>(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Transaction from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<Transaction> for TransactionProto {
    fn from_native(transaction: Transaction) -> Result<Self, ProtoConversionError> {
        let mut proto_transaction = TransactionProto::new();

        proto_transaction.set_header(transaction.header().into());
        proto_transaction.set_header_signature(transaction.header_signature().into());
        proto_transaction.set_payload(transaction.payload().into());

        Ok(proto_transaction)
    }
}

impl FromProto<TransactionProto> for Transaction {
    fn from_proto(transaction: TransactionProto) -> Result<Self, ProtoConversionError> {
        let bytes = transaction.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from Transaction".to_string(),
            )
        })?;

        <Transaction as transact::protos::FromBytes<_>>::from_bytes(&bytes).map_err(|_| {
            ProtoConversionError::DeserializationError(
                "Unable to get Transaction from bytes".to_string(),
            )
        })
    }
}

impl IntoBytes for Transaction {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from Transaction".to_string(),
            )
        })
    }
}

impl IntoNative<Transaction> for TransactionProto {}
impl IntoProto<TransactionProto> for Transaction {}

impl FromBytes<TransactionHeader> for TransactionHeader {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        protobuf::parse_from_bytes::<TransactionHeaderProto>(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get TransactionHeader from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<TransactionHeader> for TransactionHeaderProto {
    fn from_native(header: TransactionHeader) -> Result<Self, ProtoConversionError> {
        let mut transaction_header_proto = TransactionHeaderProto::new();

        transaction_header_proto.set_family_name(header.family_name().into());
        transaction_header_proto.set_family_version(header.family_version().into());
        transaction_header_proto.set_batcher_public_key(hex::encode(header.batcher_public_key()));
        transaction_header_proto
            .set_dependencies(header.dependencies().iter().map(hex::encode).collect());
        transaction_header_proto.set_inputs(header.inputs().iter().map(hex::encode).collect());
        transaction_header_proto.set_nonce(String::from_utf8(header.nonce().into())?);
        transaction_header_proto.set_outputs(header.outputs().iter().map(hex::encode).collect());
        transaction_header_proto.set_payload_sha512(hex::encode(header.payload_hash()));
        transaction_header_proto.set_signer_public_key(hex::encode(header.signer_public_key()));

        Ok(transaction_header_proto)
    }
}

impl FromProto<TransactionHeaderProto> for TransactionHeader {
    fn from_proto(header: TransactionHeaderProto) -> Result<Self, ProtoConversionError> {
        let bytes = header.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from TransactionHeader".to_string(),
            )
        })?;

        <TransactionHeader as transact::protos::FromBytes<_>>::from_bytes(&bytes).map_err(|_| {
            ProtoConversionError::DeserializationError(
                "Unable to get TransactionHeader from bytes".to_string(),
            )
        })
    }
}

impl IntoBytes for TransactionHeader {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from TransactionHeader".to_string(),
            )
        })
    }
}

impl IntoNative<TransactionHeader> for TransactionHeaderProto {}
impl IntoProto<TransactionHeaderProto> for TransactionHeader {}
