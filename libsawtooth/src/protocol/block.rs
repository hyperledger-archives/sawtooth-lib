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

use cylinder::Signer;
use protobuf::Message;

use crate::protos::{
    block::{Block as BlockProto, BlockHeader as BlockHeaderProto},
    FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};
use crate::transact::protocol::batch::Batch;

use super::ProtocolBuildError;

/// A Sawtooth block
#[derive(Clone, Debug, PartialEq, Eq)]
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
        Message::parse_from_bytes(bytes)
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
        block_proto.set_batches(
            block
                .batches
                .into_iter()
                .map(IntoProto::into_proto)
                .collect::<Result<_, _>>()?,
        );

        Ok(block_proto)
    }
}

impl FromProto<BlockProto> for Block {
    fn from_proto(block: BlockProto) -> Result<Self, ProtoConversionError> {
        Ok(Block {
            header: block.header,
            header_signature: block.header_signature,
            batches: block
                .batches
                .into_iter()
                .map(Batch::from_proto)
                .collect::<Result<_, _>>()?,
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockHeader {
    block_num: u64,
    previous_block_id: String,
    signer_public_key: Vec<u8>,
    batch_ids: Vec<String>,
    consensus: Vec<u8>,
    state_root_hash: Vec<u8>,
}

impl BlockHeader {
    pub fn block_num(&self) -> u64 {
        self.block_num
    }

    pub fn previous_block_id(&self) -> &str {
        &self.previous_block_id
    }

    pub fn signer_public_key(&self) -> &[u8] {
        &self.signer_public_key
    }

    pub fn batch_ids(&self) -> &[String] {
        &self.batch_ids
    }

    pub fn consensus(&self) -> &[u8] {
        &self.consensus
    }

    pub fn state_root_hash(&self) -> &[u8] {
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
            hex::encode(&self.signer_public_key),
            hex::encode(&self.state_root_hash),
            self.batch_ids.len(),
        )
    }
}

impl FromBytes<BlockHeader> for BlockHeader {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Message::parse_from_bytes(bytes)
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
        block_header_proto.set_signer_public_key(hex::encode(block_header.signer_public_key));
        block_header_proto.set_batch_ids(block_header.batch_ids.into());
        block_header_proto.set_consensus(block_header.consensus);
        block_header_proto.set_state_root_hash(hex::encode(block_header.state_root_hash));

        Ok(block_header_proto)
    }
}

impl FromProto<BlockHeaderProto> for BlockHeader {
    fn from_proto(block_header: BlockHeaderProto) -> Result<Self, ProtoConversionError> {
        Ok(BlockHeader {
            block_num: block_header.block_num,
            previous_block_id: block_header.previous_block_id,
            signer_public_key: hex::decode(block_header.signer_public_key)?,
            batch_ids: block_header.batch_ids.into(),
            consensus: block_header.consensus,
            state_root_hash: hex::decode(block_header.state_root_hash)?,
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
#[derive(Clone, Debug, PartialEq, Eq)]
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

impl FromBytes<BlockPair> for BlockPair {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Block::from_bytes(bytes)?
            .into_pair()
            .map_err(|err| ProtoConversionError::DeserializationError(err.to_string()))
    }
}

impl FromNative<BlockPair> for BlockProto {
    fn from_native(block_pair: BlockPair) -> Result<Self, ProtoConversionError> {
        block_pair.block.into_proto()
    }
}

impl FromProto<BlockProto> for BlockPair {
    fn from_proto(block: BlockProto) -> Result<Self, ProtoConversionError> {
        Block::from_proto(block)?
            .into_pair()
            .map_err(|err| ProtoConversionError::DeserializationError(err.to_string()))
    }
}

impl IntoBytes for BlockPair {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.block.into_bytes()
    }
}

impl IntoNative<BlockPair> for BlockProto {}
impl IntoProto<BlockProto> for BlockPair {}

/// Builder for [`Block`](struct.Block.html) and [`BlockPair`](struct.BlockPair.html)
#[derive(Default, Clone)]
pub struct BlockBuilder {
    block_num: Option<u64>,
    previous_block_id: Option<String>,
    consensus: Vec<u8>,
    state_root_hash: Option<Vec<u8>>,
    batches: Option<Vec<Batch>>,
}

impl BlockBuilder {
    /// Creates a new `BlockBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the block number for the block to be built
    pub fn with_block_num(mut self, block_num: u64) -> Self {
        self.block_num = Some(block_num);
        self
    }

    /// Sets the ID of the block previous to the one being built
    pub fn with_previous_block_id(mut self, previous_block_id: String) -> Self {
        self.previous_block_id = Some(previous_block_id);
        self
    }

    /// Sets the consensus bytes for of block to be built
    pub fn with_consensus(mut self, consensus: Vec<u8>) -> Self {
        self.consensus = consensus;
        self
    }

    /// Sets the state root hash of the block to be built
    pub fn with_state_root_hash(mut self, state_root_hash: Vec<u8>) -> Self {
        self.state_root_hash = Some(state_root_hash);
        self
    }

    /// Sets the batches that will be in the block to be built
    pub fn with_batches(mut self, batches: Vec<Batch>) -> Self {
        self.batches = Some(batches);
        self
    }

    /// Builds the `BlockPair`
    ///
    /// # Errors
    ///
    /// * Returns an error if any of the following are not set:
    ///   - `block_num`
    ///   - `previous_block_id`
    ///   - `state_root_hash`
    ///   - `batches`
    /// * Propogates any signing error that occurs
    pub fn build_pair(self, signer: &dyn Signer) -> Result<BlockPair, ProtocolBuildError> {
        let block_num = self.block_num.ok_or_else(|| {
            ProtocolBuildError::MissingField("'block_num' field is required".to_string())
        })?;
        let previous_block_id = self.previous_block_id.ok_or_else(|| {
            ProtocolBuildError::MissingField("'previous_block_id' field is required".to_string())
        })?;
        let state_root_hash = self.state_root_hash.ok_or_else(|| {
            ProtocolBuildError::MissingField("'state_root_hash' field is required".to_string())
        })?;
        let batches = self.batches.ok_or_else(|| {
            ProtocolBuildError::MissingField("'batches' field is required".to_string())
        })?;

        let signer_public_key = signer.public_key()?.as_slice().to_vec();

        let header = BlockHeader {
            block_num,
            previous_block_id,
            signer_public_key,
            batch_ids: batches
                .iter()
                .map(|batch| batch.header_signature().to_string())
                .collect(),
            consensus: self.consensus,
            state_root_hash,
        };

        let header_bytes = header.clone().into_bytes()?;
        let header_signature = signer.sign(&header_bytes)?.as_hex();

        let block = Block {
            header: header_bytes,
            header_signature,
            batches,
        };

        Ok(BlockPair { block, header })
    }

    /// Builds the `BlockPair`. This is a wrapper of the
    /// [`build_pair`](struct.BlockBuilder.html#method.build_pair) method.
    pub fn build(self, signer: &dyn Signer) -> Result<Block, ProtocolBuildError> {
        Ok(self.build_pair(signer)?.block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};

    use crate::transact::protocol::{
        batch::BatchBuilder,
        transaction::{HashMethod, TransactionBuilder},
    };

    const BLOCK_NUM: u64 = 0;
    const PREVIOUS_BLOCK_ID: &str = "0123";
    const CONSENSUS: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
    const STATE_ROOT_HASH: [u8; 4] = [0x05, 0x06, 0x07, 0x08];

    /// Verify that the `BlockBuilder` can be successfully used in a chain.
    ///
    /// 1. Initialize a signer
    /// 2. Construct the block using a builder chain
    /// 3. Verify that the resulting block pair is correct
    #[test]
    fn builder_chain() {
        let signer = new_signer();

        let pair = BlockBuilder::new()
            .with_block_num(BLOCK_NUM)
            .with_previous_block_id(PREVIOUS_BLOCK_ID.into())
            .with_consensus(CONSENSUS.into())
            .with_state_root_hash(STATE_ROOT_HASH.into())
            .with_batches(vec![batch_1(&*signer), batch_2(&*signer)])
            .build_pair(&*signer)
            .expect("Failed to build block pair");

        check_pair(&*signer, &pair);
    }

    /// Verify that the `BlockBuilder` can be successfully used with separate calls to its methods.
    ///
    /// 1. Initialize a signer
    /// 2. Construct the block using separate method calls and assignments of the builder
    /// 3. Verify that the resulting block pair is correct
    #[test]
    fn builder_separate() {
        let signer = new_signer();

        let mut builder = BlockBuilder::new();
        builder = builder.with_block_num(BLOCK_NUM);
        builder = builder.with_previous_block_id(PREVIOUS_BLOCK_ID.into());
        builder = builder.with_consensus(CONSENSUS.into());
        builder = builder.with_state_root_hash(STATE_ROOT_HASH.into());
        builder = builder.with_batches(vec![batch_1(&*signer), batch_2(&*signer)]);

        let pair = builder
            .build_pair(&*signer)
            .expect("Failed to build block pair");

        check_pair(&*signer, &pair);
    }

    /// Verify that the consensus field can be excluded from the `BlockBuilder`.
    ///
    /// 1. Initialize a signer
    /// 2. Verify that a block without a `consensus` value set builds succesfully
    #[test]
    fn builder_defaults() {
        let signer = new_signer();

        BlockBuilder::new()
            .with_block_num(BLOCK_NUM)
            .with_previous_block_id(PREVIOUS_BLOCK_ID.into())
            .with_state_root_hash(STATE_ROOT_HASH.into())
            .with_batches(vec![batch_1(&*signer), batch_2(&*signer)])
            .build_pair(&*new_signer())
            .expect("Failed to build block pair");
    }

    /// Verify that the `BlockBuilder` fails when any of the required fields are missing.
    ///
    /// 1. Initialize a signer
    /// 2. Attempt to build a block without setting `block_num` and verify that it fails.
    /// 3. Attempt to build a block without setting `previous_block_id` and verify that it fails.
    /// 4. Attempt to build a block without setting `state_root_hash` and verify that it fails.
    /// 5. Attempt to build a block without setting `batches` and verify that it fails.
    #[test]
    fn builder_missing_fields() {
        let signer = new_signer();

        match BlockBuilder::new()
            .with_previous_block_id(PREVIOUS_BLOCK_ID.into())
            .with_state_root_hash(STATE_ROOT_HASH.into())
            .with_batches(vec![batch_1(&*signer), batch_2(&*signer)])
            .build_pair(&*signer)
        {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }

        match BlockBuilder::new()
            .with_block_num(BLOCK_NUM)
            .with_state_root_hash(STATE_ROOT_HASH.into())
            .with_batches(vec![batch_1(&*signer), batch_2(&*signer)])
            .build_pair(&*signer)
        {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }

        match BlockBuilder::new()
            .with_block_num(BLOCK_NUM)
            .with_previous_block_id(PREVIOUS_BLOCK_ID.into())
            .with_batches(vec![batch_1(&*signer), batch_2(&*signer)])
            .build_pair(&*signer)
        {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }

        match BlockBuilder::new()
            .with_block_num(BLOCK_NUM)
            .with_previous_block_id(PREVIOUS_BLOCK_ID.into())
            .with_state_root_hash(STATE_ROOT_HASH.into())
            .build_pair(&*signer)
        {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }
    }

    fn check_pair(signer: &dyn Signer, pair: &BlockPair) {
        let signer_pub_key = signer
            .public_key()
            .expect("Failed to get signer public key");

        assert_eq!(pair.header().block_num(), BLOCK_NUM);
        assert_eq!(pair.header().previous_block_id(), PREVIOUS_BLOCK_ID);
        assert_eq!(pair.header().signer_public_key(), signer_pub_key.as_slice());
        assert_eq!(
            pair.header().batch_ids(),
            &[
                batch_1(signer).header_signature().to_string(),
                batch_2(signer).header_signature().to_string()
            ]
        );
        assert_eq!(pair.header().consensus(), CONSENSUS);
        assert_eq!(pair.header().state_root_hash(), STATE_ROOT_HASH);
        assert_eq!(
            pair.block().header(),
            pair.header()
                .clone()
                .into_bytes()
                .expect("Failed to get header bytes")
                .as_slice()
        );
        assert_eq!(pair.block().batches(), &[batch_1(signer), batch_2(signer)]);
    }

    fn batch_1(signer: &dyn Signer) -> Batch {
        let txn = TransactionBuilder::new()
            .with_family_name("test".into())
            .with_family_version("1.0".into())
            .with_inputs(vec![])
            .with_outputs(vec![])
            .with_payload_hash_method(HashMethod::Sha512)
            .with_payload(vec![])
            .with_nonce(vec![1])
            .build(signer)
            .expect("Failed to build txn");

        BatchBuilder::new()
            .with_transactions(vec![txn])
            .build(signer)
            .expect("Failed to build batch1")
    }

    fn batch_2(signer: &dyn Signer) -> Batch {
        let txn = TransactionBuilder::new()
            .with_family_name("test".into())
            .with_family_version("1.0".into())
            .with_inputs(vec![])
            .with_outputs(vec![])
            .with_payload_hash_method(HashMethod::Sha512)
            .with_payload(vec![])
            .with_nonce(vec![2])
            .build(signer)
            .expect("Failed to build txn");

        BatchBuilder::new()
            .with_transactions(vec![txn])
            .build(signer)
            .expect("Failed to build batch1")
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }
}
