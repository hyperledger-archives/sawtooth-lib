// Copyright 2018-2020 Cargill Incorporated
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Block info batch injector

use cylinder::Signer;

use crate::journal::publisher::BlockPublisherError;
use crate::protocol::{block::BlockPair, block_info::BlockInfo};
use crate::protos::IntoBytes;
use crate::transact::protocol::{
    batch::BatchPair,
    transaction::{HashMethod, TransactionBuilder},
};

use super::BatchInjector;

/// Name of the block info transaction family
const FAMILY_NAME: &str = "block_info";
/// Current version of the block info transaction family
const FAMILY_VERSION: &str = "1.0";
/// State namespace of the block info transaction family
const NAMESPACE: [u8; 4] = [0x00, 0xb1, 0x0c, 0x00];

/// The batch injector for block info transactions
pub struct BlockInfoInjector {
    signer: Box<dyn Signer>,
}

impl BlockInfoInjector {
    /// Creates a new block info injector
    pub fn new(signer: Box<dyn Signer>) -> Self {
        Self { signer }
    }
}

impl BatchInjector for BlockInfoInjector {
    fn block_start(
        &self,
        previous_block: &BlockPair,
    ) -> Result<Vec<BatchPair>, BlockPublisherError> {
        let txn_payload = BlockInfo::builder()
            .with_block_num(previous_block.header().block_num())
            .with_previous_block_id(previous_block.header().previous_block_id().into())
            .with_signer_public_key(previous_block.header().signer_public_key().into())
            .with_header_signature(previous_block.block().header_signature().into())
            .build()
            .map_err(|err| BlockPublisherError::Internal(err.to_string()))?
            .into_bytes()
            .map_err(|err| BlockPublisherError::Internal(err.to_string()))?;

        let batch = TransactionBuilder::new()
            .with_family_name(FAMILY_NAME.into())
            .with_family_version(FAMILY_VERSION.into())
            .with_inputs(vec![NAMESPACE.to_vec(), compute_config_address()])
            .with_outputs(vec![NAMESPACE.to_vec(), compute_config_address()])
            .with_payload_hash_method(HashMethod::Sha512)
            .with_payload(txn_payload)
            .into_batch_builder(&*self.signer)
            .map_err(|err| BlockPublisherError::Internal(err.to_string()))?
            .build_pair(&*self.signer)
            .map_err(|err| BlockPublisherError::Internal(err.to_string()))?;

        Ok(vec![batch])
    }
}

/// Computes the block info configuration address
fn compute_config_address() -> Vec<u8> {
    let mut address = vec![0x00, 0xb1, 0x0c, 0x01];
    address.extend((0..31).map(|_| 0x00));
    address
}
