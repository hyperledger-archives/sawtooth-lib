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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::protocol::block::BlockPair;

#[derive(Debug)]
pub enum BlockStoreError {
    Error(String),
    UnknownBlock,
}

pub trait BlockStore: Sync + Send {
    fn get<'a>(
        &'a self,
        block_ids: &[&str],
    ) -> Result<Box<dyn Iterator<Item = BlockPair> + 'a>, BlockStoreError>;

    fn delete(&mut self, block_ids: &[&str]) -> Result<Vec<BlockPair>, BlockStoreError>;

    fn put(&mut self, blocks: Vec<BlockPair>) -> Result<(), BlockStoreError>;

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = BlockPair> + 'a>, BlockStoreError>;
}

pub trait BatchIndex: Sync + Send {
    fn contains(&self, id: &str) -> Result<bool, BlockStoreError>;

    fn get_block_by_id(&self, id: &str) -> Result<Option<BlockPair>, BlockStoreError>;
}

pub trait TransactionIndex: Sync + Send {
    fn contains(&self, id: &str) -> Result<bool, BlockStoreError>;

    fn get_block_by_id(&self, id: &str) -> Result<Option<BlockPair>, BlockStoreError>;
}

pub trait IndexedBlockStore: BlockStore + TransactionIndex + BatchIndex {}

#[derive(Clone, Default)]
pub struct InMemoryBlockStore {
    state: Arc<Mutex<InMemoryBlockStoreState>>,
}

impl InMemoryBlockStore {
    fn get_block_by_block_id(&self, block_id: &str) -> Option<BlockPair> {
        self.state
            .lock()
            .expect("The mutex is not poisoned")
            .get_block_by_block_id(block_id)
            .cloned()
    }
}

impl IndexedBlockStore for InMemoryBlockStore {}

impl BlockStore for InMemoryBlockStore {
    fn get<'a>(
        &'a self,
        block_ids: &[&str],
    ) -> Result<Box<dyn Iterator<Item = BlockPair> + 'a>, BlockStoreError> {
        let block_ids_owned = block_ids.iter().map(|id| (*id).into()).collect();
        Ok(Box::new(InMemoryGetBlockIterator::new(
            self.clone(),
            block_ids_owned,
        )))
    }

    fn delete(&mut self, block_ids: &[&str]) -> Result<Vec<BlockPair>, BlockStoreError> {
        self.state
            .lock()
            .expect("The mutex is poisoned")
            .delete(block_ids)
    }

    fn put(&mut self, blocks: Vec<BlockPair>) -> Result<(), BlockStoreError> {
        self.state
            .lock()
            .expect("The mutex is poisoned")
            .put(blocks)
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = BlockPair> + 'a>, BlockStoreError> {
        let chain_head = self
            .state
            .lock()
            .expect("The mutex is poisoned")
            .chain_head_id
            .clone();
        Ok(Box::new(InMemoryIter::new(self.clone(), chain_head)))
    }
}

#[derive(Default)]
pub struct InMemoryBlockStoreState {
    block_by_block_id: HashMap<String, BlockPair>,
    chain_head_num: u64,
    chain_head_id: String,
}

impl InMemoryBlockStoreState {
    fn get_block_by_block_id(&self, block_id: &str) -> Option<&BlockPair> {
        self.block_by_block_id.get(block_id)
    }
}

impl InMemoryBlockStoreState {
    fn delete(&mut self, block_ids: &[&str]) -> Result<Vec<BlockPair>, BlockStoreError> {
        if block_ids
            .iter()
            .any(|block_id| !self.block_by_block_id.contains_key(*block_id))
        {
            return Err(BlockStoreError::UnknownBlock);
        }
        let blocks = block_ids.iter().map(|block_id| {
            let block = self
                .block_by_block_id
                .remove(*block_id)
                .expect("BlockPair removed during middle of delete operation");
            if block.header().block_num() <= self.chain_head_num {
                self.chain_head_id = block.header().previous_block_id().to_string();
                self.chain_head_num = block.header().block_num() - 1;
            }
            block
        });

        Ok(blocks.collect())
    }

    fn put(&mut self, blocks: Vec<BlockPair>) -> Result<(), BlockStoreError> {
        blocks.into_iter().for_each(|block| {
            if block.header().block_num() > self.chain_head_num {
                self.chain_head_id = block.block().header_signature().to_string();
                self.chain_head_num = block.header().block_num();
            }

            self.block_by_block_id
                .insert(block.block().header_signature().to_string(), block);
        });
        Ok(())
    }
}

impl BatchIndex for InMemoryBlockStore {
    fn contains(&self, id: &str) -> Result<bool, BlockStoreError> {
        Ok(self
            .iter()?
            .flat_map(|block| block.block().batches().to_vec())
            .any(|batch| batch.header_signature == id))
    }

    fn get_block_by_id(&self, id: &str) -> Result<Option<BlockPair>, BlockStoreError> {
        Ok(self
            .iter()?
            .find(|block| block.header().batch_ids().contains(&id.into())))
    }
}

impl TransactionIndex for InMemoryBlockStore {
    fn contains(&self, id: &str) -> Result<bool, BlockStoreError> {
        Ok(self
            .iter()?
            .flat_map(|block| block.block().batches().to_vec())
            .flat_map(|batch| batch.transactions)
            .any(|txn| txn.header_signature == id))
    }

    fn get_block_by_id(&self, id: &str) -> Result<Option<BlockPair>, BlockStoreError> {
        Ok(self.iter()?.find(|block| {
            block
                .block()
                .batches()
                .iter()
                .any(|batch| batch.transaction_ids.contains(&id.into()))
        }))
    }
}

struct InMemoryGetBlockIterator {
    blockstore: InMemoryBlockStore,
    block_ids: Vec<String>,
    index: usize,
}

impl InMemoryGetBlockIterator {
    fn new(blockstore: InMemoryBlockStore, block_ids: Vec<String>) -> InMemoryGetBlockIterator {
        InMemoryGetBlockIterator {
            blockstore,
            block_ids,
            index: 0,
        }
    }
}

impl Iterator for InMemoryGetBlockIterator {
    type Item = BlockPair;

    fn next(&mut self) -> Option<Self::Item> {
        let block = match self.block_ids.get(self.index) {
            Some(block_id) => self.blockstore.get_block_by_block_id(block_id),
            None => None,
        };
        self.index += 1;
        block
    }
}

struct InMemoryIter {
    blockstore: InMemoryBlockStore,
    head: String,
}

impl InMemoryIter {
    fn new(blockstore: InMemoryBlockStore, head: String) -> Self {
        InMemoryIter { blockstore, head }
    }
}

impl Iterator for InMemoryIter {
    type Item = BlockPair;

    fn next(&mut self) -> Option<Self::Item> {
        let block = self.blockstore.get_block_by_block_id(&self.head);
        if let Some(ref b) = block {
            self.head = b.header().previous_block_id().to_string();
        }
        block
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::journal::NULL_BLOCK_IDENTIFIER;
    use crate::protocol::block::{BlockBuilder, BlockPair};
    use crate::signing::hash::HashSigner;

    fn create_block(previous_block_id: &str, block_num: u64) -> BlockPair {
        BlockBuilder::new()
            .with_block_num(block_num)
            .with_previous_block_id(previous_block_id.into())
            .with_state_root_hash(vec![])
            .with_batches(vec![])
            .build_pair(&HashSigner::default())
            .expect("Failed to build block pair")
    }

    #[test]
    fn test_block_store() {
        let mut store = InMemoryBlockStore::default();

        let block_a = create_block(NULL_BLOCK_IDENTIFIER, 1);
        let block_b = create_block(block_a.block().header_signature(), 2);
        let block_c = create_block(block_b.block().header_signature(), 3);

        store
            .put(vec![block_a.clone(), block_b.clone(), block_c.clone()])
            .unwrap();
        assert_eq!(
            store
                .get(&[block_a.block().header_signature()])
                .unwrap()
                .next()
                .unwrap(),
            block_a
        );

        {
            let mut iterator = store.iter().unwrap();

            assert_eq!(iterator.next().unwrap(), block_c);
            assert_eq!(iterator.next().unwrap(), block_b);
            assert_eq!(iterator.next().unwrap(), block_a);
            assert_eq!(iterator.next(), None);
        }

        assert_eq!(
            store.delete(&[block_c.block().header_signature()]).unwrap(),
            vec![block_c]
        );
    }
}
