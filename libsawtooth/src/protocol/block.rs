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

//! Sawtooth block protocol

use protobuf::Message;

use crate::batch::Batch;
use crate::protos::{
    batch::Batch as BatchProto,
    block::{Block as BlockProto, BlockHeader as BlockHeaderProto},
    FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

use super::ProtocolBuildError;

/// A Sawtooth block
#[derive(Clone, Debug, PartialEq)]
pub struct Block {
    header: Vec<u8>,
    header_signature: String,
    batches: Vec<Batch>,
}

impl Block {
    pub fn header(&self) -> &[u8] {
        &self.header
    }

    pub fn header_signature(&self) -> &str {
        &self.header_signature
    }

    pub fn batches(&self) -> &[Batch] {
        &self.batches
    }

    pub fn into_pair(self) -> Result<BlockPair, ProtocolBuildError> {
        let header = BlockHeader::from_bytes(&self.header)?;

        Ok(BlockPair {
            block: self,
            header,
        })
    }
}

impl std::fmt::Display for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Block(header_signature: {}, {} batches)",
            self.header_signature,
            self.batches.len(),
        )
    }
}

impl FromBytes<Block> for Block {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        protobuf::parse_from_bytes::<BlockProto>(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Block from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<Block> for BlockProto {
    fn from_native(block: Block) -> Result<Self, ProtoConversionError> {
        let mut block_proto = BlockProto::new();

        block_proto.set_header(block.header);
        block_proto.set_header_signature(block.header_signature);
        block_proto.set_batches(block.batches.into_iter().map(BatchProto::from).collect());

        Ok(block_proto)
    }
}

impl FromProto<BlockProto> for Block {
    fn from_proto(block: BlockProto) -> Result<Self, ProtoConversionError> {
        Ok(Block {
            header: block.header,
            header_signature: block.header_signature,
            batches: block.batches.into_iter().map(Batch::from).collect(),
        })
    }
}

impl IntoBytes for Block {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError("Unable to get bytes from Block".to_string())
        })
    }
}

impl IntoNative<Block> for BlockProto {}
impl IntoProto<BlockProto> for Block {}

/// A Sawtooth block header
#[derive(Clone, Debug, PartialEq)]
pub struct BlockHeader {
    block_num: u64,
    previous_block_id: String,
    signer_public_key: String,
    batch_ids: Vec<String>,
    consensus: Vec<u8>,
    state_root_hash: String,
}

impl BlockHeader {
    pub fn block_num(&self) -> u64 {
        self.block_num
    }

    pub fn previous_block_id(&self) -> &str {
        &self.previous_block_id
    }

    pub fn signer_public_key(&self) -> &str {
        &self.signer_public_key
    }

    pub fn batch_ids(&self) -> &[String] {
        &self.batch_ids
    }

    pub fn consensus(&self) -> &[u8] {
        &self.consensus
    }

    pub fn state_root_hash(&self) -> &str {
        &self.state_root_hash
    }
}

impl std::fmt::Display for BlockHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "BlockHeader(block_num: {}, previous_block_id: {}, signer_public_key: {}, \
             state_root_hash: {}, {} batches)",
            self.block_num,
            self.previous_block_id,
            self.signer_public_key,
            self.state_root_hash,
            self.batch_ids.len(),
        )
    }
}

impl FromBytes<BlockHeader> for BlockHeader {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        protobuf::parse_from_bytes::<BlockHeaderProto>(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get BlockHeader from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<BlockHeader> for BlockHeaderProto {
    fn from_native(block_header: BlockHeader) -> Result<Self, ProtoConversionError> {
        let mut block_header_proto = BlockHeaderProto::new();

        block_header_proto.set_block_num(block_header.block_num);
        block_header_proto.set_previous_block_id(block_header.previous_block_id);
        block_header_proto.set_signer_public_key(block_header.signer_public_key);
        block_header_proto.set_batch_ids(block_header.batch_ids.into());
        block_header_proto.set_consensus(block_header.consensus);
        block_header_proto.set_state_root_hash(block_header.state_root_hash);

        Ok(block_header_proto)
    }
}

impl FromProto<BlockHeaderProto> for BlockHeader {
    fn from_proto(block_header: BlockHeaderProto) -> Result<Self, ProtoConversionError> {
        Ok(BlockHeader {
            block_num: block_header.block_num,
            previous_block_id: block_header.previous_block_id,
            signer_public_key: block_header.signer_public_key,
            batch_ids: block_header.batch_ids.into(),
            consensus: block_header.consensus,
            state_root_hash: block_header.state_root_hash,
        })
    }
}

impl IntoBytes for BlockHeader {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from BlockHeader".to_string(),
            )
        })
    }
}

impl IntoNative<BlockHeader> for BlockHeaderProto {}
impl IntoProto<BlockHeaderProto> for BlockHeader {}

/// A Sawtooth (block, block header) pair
#[derive(Clone, Debug, PartialEq)]
pub struct BlockPair {
    block: Block,
    header: BlockHeader,
}

impl BlockPair {
    pub fn block(&self) -> &Block {
        &self.block
    }

    pub fn header(&self) -> &BlockHeader {
        &self.header
    }

    pub fn take(self) -> (Block, BlockHeader) {
        (self.block, self.header)
    }
}

// /// Builder for the `Block` and `BlockPair` struct
// #[derive(Default, Clone)]
// pub struct BlockBuilder {
//     block_num: Option<u64>,
//     batches: Option<Vec<Batch>>,
// }
//
// impl PolicyBuilder {
//     pub fn new() -> Self {
//         Self::default()
//     }
//
//     pub fn with_block_num(mut self, block_num: u64) -> Self {
//         self.block_num = Some(block_num);
//         self
//     }
//
//     pub fn with_batches(mut self, batches: Vec<Batch>) -> Self {
//         self.batches = Some(batches);
//         self
//     }
//
//     pub fn build(self) -> Result<Policy, ProtocolBuildError> {
//         let block_num = self.block_num.ok_or_else(|| {
//             ProtocolBuildError::MissingField("'block_num' field is required".to_string())
//         })?;
//         let batches = self.batches.ok_or_else(|| {
//             ProtocolBuildError::MissingField("'batches' field is required".to_string())
//         })?;
//
//         Ok(Policy {
//             name,
//             entries: self.entries,
//         })
//     }
// }
