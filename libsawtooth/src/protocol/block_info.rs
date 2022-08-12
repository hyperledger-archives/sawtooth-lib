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

//! Sawtooth block protocol

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use protobuf::Message;

use crate::protos::{
    block_info::{BlockInfo as BlockInfoProto, BlockInfoTxn as BlockInfoTxnProto},
    FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

use super::ProtocolBuildError;

/// Block info data
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockInfo {
    block_num: u64,
    previous_block_id: String,
    signer_public_key: Vec<u8>,
    header_signature: String,
    timestamp: SystemTime,
    target_count: u64,
    sync_tolerance: u64,
}

impl BlockInfo {
    pub fn builder() -> BlockInfoBuilder {
        BlockInfoBuilder::default()
    }

    pub fn block_num(&self) -> u64 {
        self.block_num
    }

    pub fn previous_block_id(&self) -> &str {
        &self.previous_block_id
    }

    pub fn signer_public_key(&self) -> &[u8] {
        &self.signer_public_key
    }

    pub fn header_signature(&self) -> &str {
        &self.header_signature
    }

    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    pub fn target_count(&self) -> u64 {
        self.target_count
    }

    pub fn sync_tolerance(&self) -> u64 {
        self.sync_tolerance
    }
}

impl FromBytes<BlockInfo> for BlockInfo {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get BlockInfo from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<BlockInfo> for BlockInfoTxnProto {
    fn from_native(block_info: BlockInfo) -> Result<Self, ProtoConversionError> {
        let mut block_info_proto = BlockInfoProto::new();
        block_info_proto.set_block_num(block_info.block_num);
        block_info_proto.set_previous_block_id(block_info.previous_block_id);
        block_info_proto.set_signer_public_key(hex::encode(block_info.signer_public_key));
        block_info_proto.set_header_signature(block_info.header_signature);
        block_info_proto.set_timestamp(
            block_info
                .timestamp
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
        );

        let mut block_info_txn_proto = BlockInfoTxnProto::new();
        block_info_txn_proto.set_block(block_info_proto);
        block_info_txn_proto.set_target_count(block_info.target_count);
        block_info_txn_proto.set_sync_tolerance(block_info.sync_tolerance);

        Ok(block_info_txn_proto)
    }
}

impl FromProto<BlockInfoTxnProto> for BlockInfo {
    fn from_proto(mut block_info_txn: BlockInfoTxnProto) -> Result<Self, ProtoConversionError> {
        let block_info = block_info_txn.block.take().ok_or_else(|| {
            ProtoConversionError::DeserializationError("BlockInfoTxn.block not set".to_string())
        })?;
        Ok(BlockInfo {
            block_num: block_info.block_num,
            previous_block_id: block_info.previous_block_id,
            signer_public_key: hex::decode(block_info.signer_public_key)?,
            header_signature: block_info.header_signature,
            timestamp: UNIX_EPOCH
                .checked_add(Duration::from_secs(block_info.timestamp))
                .ok_or_else(|| {
                    ProtoConversionError::DeserializationError(
                        "BlockInfo timestamp overflow".to_string(),
                    )
                })?,
            target_count: block_info_txn.target_count,
            sync_tolerance: block_info_txn.sync_tolerance,
        })
    }
}

impl IntoBytes for BlockInfo {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from BlockInfo".to_string(),
            )
        })
    }
}

impl IntoNative<BlockInfo> for BlockInfoTxnProto {}
impl IntoProto<BlockInfoTxnProto> for BlockInfo {}

/// Builder for `BlockInfo`
#[derive(Default)]
pub struct BlockInfoBuilder {
    block_num: Option<u64>,
    previous_block_id: Option<String>,
    signer_public_key: Option<Vec<u8>>,
    header_signature: Option<String>,
    timestamp: Option<SystemTime>,
    target_count: Option<u64>,
    sync_tolerance: Option<u64>,
}

impl BlockInfoBuilder {
    pub fn with_block_num(mut self, block_num: u64) -> Self {
        self.block_num = Some(block_num);
        self
    }

    pub fn with_previous_block_id(mut self, previous_block_id: String) -> Self {
        self.previous_block_id = Some(previous_block_id);
        self
    }

    pub fn with_signer_public_key(mut self, signer_public_key: Vec<u8>) -> Self {
        self.signer_public_key = Some(signer_public_key);
        self
    }

    pub fn with_header_signature(mut self, header_signature: String) -> Self {
        self.header_signature = Some(header_signature);
        self
    }

    pub fn with_timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn with_target_count(mut self, target_count: u64) -> Self {
        self.target_count = Some(target_count);
        self
    }

    pub fn with_sync_tolerance(mut self, sync_tolerance: u64) -> Self {
        self.sync_tolerance = Some(sync_tolerance);
        self
    }

    pub fn build(self) -> Result<BlockInfo, ProtocolBuildError> {
        let block_num = self.block_num.ok_or_else(|| {
            ProtocolBuildError::MissingField("'block_num' field is required".to_string())
        })?;
        let previous_block_id = self.previous_block_id.ok_or_else(|| {
            ProtocolBuildError::MissingField("'previous_block_id' field is required".to_string())
        })?;
        let signer_public_key = self.signer_public_key.ok_or_else(|| {
            ProtocolBuildError::MissingField("'signer_public_key' field is required".to_string())
        })?;
        let header_signature = self.header_signature.ok_or_else(|| {
            ProtocolBuildError::MissingField("'header_signature' field is required".to_string())
        })?;
        let timestamp = self.timestamp.unwrap_or_else(SystemTime::now);
        let target_count = self.target_count.unwrap_or_default();
        let sync_tolerance = self.sync_tolerance.unwrap_or_default();

        Ok(BlockInfo {
            block_num,
            previous_block_id,
            signer_public_key,
            header_signature,
            timestamp,
            target_count,
            sync_tolerance,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BLOCK_NUM: u64 = 0;
    const PREVIOUS_BLOCK_ID: &str = "0123";
    const SIGNER_PUBLIC_KEY: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
    const HEADER_SIGNATURE: &str = "4567";
    const TARGET_COUNT: u64 = 1;
    const SYNC_TOLERANCE: u64 = 2;

    /// Verify that the `BlockInfoBuilder` can be successfully used in a chain.
    ///
    /// 1. Construct the block info using a builder chain
    /// 2. Verify that the resulting block info is correct
    #[test]
    fn builder_chain() {
        let timestamp = SystemTime::now();
        let block_info = BlockInfo::builder()
            .with_block_num(BLOCK_NUM)
            .with_previous_block_id(PREVIOUS_BLOCK_ID.into())
            .with_signer_public_key(SIGNER_PUBLIC_KEY.into())
            .with_header_signature(HEADER_SIGNATURE.into())
            .with_timestamp(timestamp)
            .with_target_count(TARGET_COUNT)
            .with_sync_tolerance(SYNC_TOLERANCE)
            .build()
            .expect("Failed to build block info");

        check_block_info(&block_info, timestamp);
    }

    /// Verify that the `BlockInfoBuilder` can be successfully used with separate calls to its
    /// methods.
    ///
    /// 1. Construct the block info using separate method calls and assignments of the builder
    /// 2. Verify that the resulting block info is correct
    #[test]
    fn builder_separate() {
        let timestamp = SystemTime::now();
        let mut builder = BlockInfo::builder();
        builder = builder.with_block_num(BLOCK_NUM);
        builder = builder.with_previous_block_id(PREVIOUS_BLOCK_ID.into());
        builder = builder.with_signer_public_key(SIGNER_PUBLIC_KEY.into());
        builder = builder.with_header_signature(HEADER_SIGNATURE.into());
        builder = builder.with_timestamp(timestamp);
        builder = builder.with_target_count(TARGET_COUNT);
        builder = builder.with_sync_tolerance(SYNC_TOLERANCE);

        let block_info = builder.build().expect("Failed to build block info");

        check_block_info(&block_info, timestamp);
    }

    /// Verify that the timestamp, target_count, and sync_tolerance fields can be excluded from the
    /// `BlockInfoBuilder`.
    #[test]
    fn builder_defaults() {
        BlockInfo::builder()
            .with_block_num(BLOCK_NUM)
            .with_previous_block_id(PREVIOUS_BLOCK_ID.into())
            .with_signer_public_key(SIGNER_PUBLIC_KEY.into())
            .with_header_signature(HEADER_SIGNATURE.into())
            .build()
            .expect("Failed to build block info");
    }

    /// Verify that the `BlockInfoBuilder` fails when any of the required fields are missing.
    ///
    /// 1. Attempt to build a block info without setting `block_num` and verify that it fails.
    /// 2. Attempt to build a block info without setting `previous_block_id` and verify that it
    ///    fails.
    /// 3. Attempt to build a block info without setting `signer_public_key` and verify that it
    ///    fails.
    /// 4. Attempt to build a block info without setting `header_signature` and verify that it
    ///    fails.
    #[test]
    fn builder_missing_fields() {
        match BlockInfo::builder()
            .with_previous_block_id(PREVIOUS_BLOCK_ID.into())
            .with_signer_public_key(SIGNER_PUBLIC_KEY.into())
            .with_header_signature(HEADER_SIGNATURE.into())
            .build()
        {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }

        match BlockInfo::builder()
            .with_block_num(BLOCK_NUM)
            .with_signer_public_key(SIGNER_PUBLIC_KEY.into())
            .with_header_signature(HEADER_SIGNATURE.into())
            .build()
        {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }

        match BlockInfo::builder()
            .with_block_num(BLOCK_NUM)
            .with_previous_block_id(PREVIOUS_BLOCK_ID.into())
            .with_header_signature(HEADER_SIGNATURE.into())
            .build()
        {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }

        match BlockInfo::builder()
            .with_block_num(BLOCK_NUM)
            .with_previous_block_id(PREVIOUS_BLOCK_ID.into())
            .with_signer_public_key(SIGNER_PUBLIC_KEY.into())
            .build()
        {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }
    }

    fn check_block_info(block_info: &BlockInfo, timestamp: SystemTime) {
        assert_eq!(block_info.block_num(), BLOCK_NUM);
        assert_eq!(block_info.previous_block_id(), PREVIOUS_BLOCK_ID);
        assert_eq!(block_info.signer_public_key(), SIGNER_PUBLIC_KEY);
        assert_eq!(block_info.header_signature(), HEADER_SIGNATURE);
        assert_eq!(block_info.timestamp(), timestamp);
        assert_eq!(block_info.target_count(), TARGET_COUNT);
        assert_eq!(block_info.sync_tolerance(), SYNC_TOLERANCE);
    }
}
