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

use log::error;
use transact::database::error::DatabaseError;
use transact::database::lmdb::LmdbDatabase;
use transact::database::lmdb::LmdbDatabaseWriter;
use transact::database::{DatabaseReader, DatabaseWriter};
use transact::protocol::{batch::Batch, transaction::Transaction};

use crate::journal::block_store::{
    BatchIndex, BlockStore, BlockStoreError, IndexedBlockStore, TransactionIndex,
};
use crate::journal::chain::{ChainReadError, ChainReader};
use crate::protocol::block::BlockPair;
use crate::protos::{FromBytes, IntoBytes};

/// Contains all committed blocks for the current chain
#[derive(Clone)]
pub struct CommitStore {
    db: LmdbDatabase,
}

impl CommitStore {
    pub fn new(db: LmdbDatabase) -> Self {
        CommitStore { db }
    }

    // Get

    fn read_block_from_main(
        reader: &dyn DatabaseReader,
        block_id: &[u8],
    ) -> Result<BlockPair, DatabaseError> {
        let packed = reader.get(block_id)?.ok_or_else(|| {
            DatabaseError::NotFoundError(format!("Block not found: {:?}", block_id))
        })?;
        BlockPair::from_bytes(&packed).map_err(|err| {
            DatabaseError::CorruptionError(format!(
                "Could not interpret stored data as a block: {}",
                err
            ))
        })
    }

    fn read_block_id_from_batch_index(
        reader: &dyn DatabaseReader,
        batch_id: &[u8],
    ) -> Result<Vec<u8>, DatabaseError> {
        reader
            .index_get("index_batch", batch_id)
            .and_then(|block_id| {
                block_id.ok_or_else(|| {
                    DatabaseError::NotFoundError(format!("Batch not found: {:?}", batch_id))
                })
            })
    }

    fn read_block_id_from_transaction_index(
        reader: &dyn DatabaseReader,
        transaction_id: &[u8],
    ) -> Result<Vec<u8>, DatabaseError> {
        reader
            .index_get("index_transaction", transaction_id)
            .and_then(|block_id| {
                block_id.ok_or_else(|| {
                    DatabaseError::NotFoundError(format!(
                        "Transaction not found: {:?}",
                        transaction_id
                    ))
                })
            })
    }

    fn read_block_id_from_block_num_index(
        reader: &dyn DatabaseReader,
        block_num: u64,
    ) -> Result<Vec<u8>, DatabaseError> {
        reader
            .index_get(
                "index_block_num",
                format!("0x{:0>16x}", block_num).as_bytes(),
            )
            .and_then(|block_id| {
                block_id.ok_or_else(|| {
                    DatabaseError::NotFoundError(format!("Block not found: {}", block_num))
                })
            })
    }

    fn read_chain_head_id_from_block_num_index(
        reader: &dyn DatabaseReader,
    ) -> Result<Vec<u8>, DatabaseError> {
        let mut cursor = reader.index_cursor("index_block_num")?;
        let (_, val) = cursor
            .seek_last()
            .ok_or_else(|| DatabaseError::NotFoundError("No chain head".into()))?;
        Ok(val)
    }

    pub fn get_by_block_id(&self, block_id: &str) -> Result<BlockPair, DatabaseError> {
        let reader = self.db.reader()?;
        Self::read_block_from_main(&reader, block_id.as_bytes())
    }

    pub fn get_by_block_num(&self, block_num: u64) -> Result<BlockPair, DatabaseError> {
        let reader = self.db.reader()?;
        let block_id = Self::read_block_id_from_block_num_index(&reader, block_num)?;
        Self::read_block_from_main(&reader, &block_id)
    }

    pub fn get_by_batch_id(&self, batch_id: &str) -> Result<BlockPair, DatabaseError> {
        let reader = self.db.reader()?;
        let block_id = Self::read_block_id_from_batch_index(&reader, batch_id.as_bytes())?;
        Self::read_block_from_main(&reader, &block_id)
    }

    pub fn get_by_transaction_id(&self, transaction_id: &str) -> Result<BlockPair, DatabaseError> {
        let reader = self.db.reader()?;
        let block_id =
            Self::read_block_id_from_transaction_index(&reader, transaction_id.as_bytes())?;
        Self::read_block_from_main(&reader, &block_id)
    }

    pub fn get_chain_head(&self) -> Result<BlockPair, DatabaseError> {
        let reader = self.db.reader()?;
        let chain_head_id = Self::read_chain_head_id_from_block_num_index(&reader)?;
        Self::read_block_from_main(&reader, &chain_head_id)
    }

    // Put

    fn write_block_to_main_db(
        writer: &mut LmdbDatabaseWriter,
        block: &BlockPair,
    ) -> Result<(), DatabaseError> {
        let packed = block.block().clone().into_bytes().map_err(|err| {
            DatabaseError::WriterError(format!("Failed to serialize block: {}", err))
        })?;
        writer.put(block.block().header_signature().as_bytes(), &packed)
    }

    fn write_block_num_to_index(
        writer: &mut LmdbDatabaseWriter,
        block_num: u64,
        header_signature: &str,
    ) -> Result<(), DatabaseError> {
        let block_num_index = format!("0x{:0>16x}", block_num);
        writer.index_put(
            "index_block_num",
            block_num_index.as_bytes(),
            header_signature.as_bytes(),
        )
    }

    fn write_batches_to_index(
        writer: &mut LmdbDatabaseWriter,
        block: &BlockPair,
    ) -> Result<(), DatabaseError> {
        for batch in block.block().batches().iter() {
            writer.index_put(
                "index_batch",
                batch.header_signature().as_bytes(),
                block.block().header_signature().as_bytes(),
            )?;
        }
        Ok(())
    }

    fn write_transctions_to_index(
        writer: &mut LmdbDatabaseWriter,
        block: &BlockPair,
    ) -> Result<(), DatabaseError> {
        for batch in block.block().batches().iter() {
            for txn in batch.transactions() {
                writer.index_put(
                    "index_transaction",
                    txn.header_signature().as_bytes(),
                    block.block().header_signature().as_bytes(),
                )?;
            }
        }
        Ok(())
    }

    pub fn put_blocks(&self, blocks: Vec<BlockPair>) -> Result<(), DatabaseError> {
        let mut writer = self.db.writer()?;
        for block in blocks {
            Self::put_block(&mut writer, block)?;
        }
        Box::new(writer).commit()
    }

    fn put_block(writer: &mut LmdbDatabaseWriter, block: BlockPair) -> Result<(), DatabaseError> {
        Self::write_block_to_main_db(writer, &block)?;
        Self::write_block_num_to_index(
            writer,
            block.header().block_num(),
            block.block().header_signature(),
        )?;
        Self::write_transctions_to_index(writer, &block)?;
        Self::write_batches_to_index(writer, &block)?;

        Ok(())
    }

    // Delete

    fn delete_block_from_main_db(
        writer: &mut LmdbDatabaseWriter,
        block: &BlockPair,
    ) -> Result<(), DatabaseError> {
        writer.delete(block.block().header_signature().as_bytes())
    }

    fn delete_block_num_from_index(
        writer: &mut LmdbDatabaseWriter,
        block_num: u64,
    ) -> Result<(), DatabaseError> {
        writer.index_delete(
            "index_block_num",
            format!("0x{:0>16x}", block_num).as_bytes(),
        )
    }

    fn delete_batches_from_index(
        writer: &mut LmdbDatabaseWriter,
        block: &BlockPair,
    ) -> Result<(), DatabaseError> {
        for batch in block.block().batches().iter() {
            writer.index_delete("index_batch", batch.header_signature().as_bytes())?;
        }
        Ok(())
    }

    fn delete_transactions_from_index(
        writer: &mut LmdbDatabaseWriter,
        block: &BlockPair,
    ) -> Result<(), DatabaseError> {
        for batch in block.block().batches().iter() {
            for txn in batch.transactions() {
                writer.index_delete("index_transaction", txn.header_signature().as_bytes())?;
            }
        }
        Ok(())
    }

    fn delete_block_by_id(
        writer: &mut LmdbDatabaseWriter,
        block_id: &str,
    ) -> Result<BlockPair, DatabaseError> {
        let block = Self::read_block_from_main(&*writer, block_id.as_bytes())?;

        Self::delete_block_from_main_db(writer, &block)?;
        Self::delete_block_num_from_index(writer, block.header().block_num())?;
        Self::delete_batches_from_index(writer, &block)?;
        Self::delete_transactions_from_index(writer, &block)?;

        Ok(block)
    }

    fn delete_blocks_by_ids(&self, block_ids: &[&str]) -> Result<Vec<BlockPair>, DatabaseError> {
        let mut blocks = Vec::new();
        let mut writer = self.db.writer()?;

        for block_id in block_ids {
            blocks.push(Self::delete_block_by_id(&mut writer, block_id)?);
        }

        Box::new(writer).commit()?;

        Ok(blocks)
    }

    // Legacy

    pub fn get_batch(&self, batch_id: &str) -> Result<Batch, DatabaseError> {
        self.get_by_batch_id(batch_id).and_then(|block| {
            block
                .block()
                .batches()
                .iter()
                .find(|batch| batch.header_signature() == batch_id)
                .cloned()
                .ok_or_else(|| DatabaseError::CorruptionError("Batch index corrupted".into()))
        })
    }

    pub fn get_transaction(&self, transaction_id: &str) -> Result<Transaction, DatabaseError> {
        self.get_by_transaction_id(transaction_id)
            .and_then(|block| {
                block
                    .block()
                    .batches()
                    .iter()
                    .flat_map(|batch| batch.transactions())
                    .find(|txn| txn.header_signature() == transaction_id)
                    .cloned()
                    .ok_or_else(|| {
                        DatabaseError::CorruptionError("Transaction index corrupted".into())
                    })
            })
    }

    pub fn get_batch_by_transaction(&self, transaction_id: &str) -> Result<Batch, DatabaseError> {
        self.get_by_transaction_id(transaction_id)
            .and_then(|block| {
                block
                    .block()
                    .batches()
                    .iter()
                    .find(|batch| {
                        !batch
                            .transactions()
                            .iter()
                            .any(|txn| txn.header_signature() == transaction_id)
                    })
                    .cloned()
                    .ok_or_else(|| {
                        DatabaseError::CorruptionError("Transaction index corrupted".into())
                    })
            })
    }

    pub fn contains_block(&self, block_id: &str) -> Result<bool, DatabaseError> {
        match self.db.reader()?.get(block_id.as_bytes())? {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    pub fn contains_batch(&self, batch_id: &str) -> Result<bool, DatabaseError> {
        match self
            .db
            .reader()?
            .index_get("index_batch", batch_id.as_bytes())?
        {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    pub fn contains_transaction(&self, transaction_id: &str) -> Result<bool, DatabaseError> {
        match self
            .db
            .reader()?
            .index_get("index_transaction", transaction_id.as_bytes())?
        {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    pub fn get_block_count(&self) -> Result<usize, DatabaseError> {
        let reader = self.db.reader()?;
        reader.count()
    }

    pub fn get_transaction_count(&self) -> Result<usize, DatabaseError> {
        let reader = self.db.reader()?;
        reader.index_count("index_transaction")
    }

    pub fn get_batch_count(&self) -> Result<usize, DatabaseError> {
        let reader = self.db.reader()?;
        reader.index_count("index_batch")
    }

    pub fn get_block_by_height_iter(
        &self,
        start: Option<u64>,
        direction: ByHeightDirection,
    ) -> CommitStoreByHeightIterator {
        let next = if start.is_none() {
            match &direction {
                ByHeightDirection::Increasing => Some(0),
                ByHeightDirection::Decreasing => Some(
                    self.get_chain_head()
                        .map(|head| head.header().block_num())
                        .unwrap_or(0),
                ),
            }
        } else {
            start
        };
        CommitStoreByHeightIterator {
            store: self.clone(),
            next,
            direction,
        }
    }
}

impl BlockStore for CommitStore {
    fn get<'a>(
        &'a self,
        block_ids: &[&str],
    ) -> Result<Box<dyn Iterator<Item = BlockPair> + 'a>, BlockStoreError> {
        Ok(Box::new(CommitStoreGetIterator {
            store: self.clone(),
            block_ids: block_ids.iter().map(|id| (*id).into()).collect(),
            index: 0,
        }))
    }

    fn delete(&mut self, block_ids: &[&str]) -> Result<Vec<BlockPair>, BlockStoreError> {
        self.delete_blocks_by_ids(block_ids)
            .map_err(|err| match err {
                DatabaseError::NotFoundError(_) => BlockStoreError::UnknownBlock,
                err => BlockStoreError::Error(format!("{:?}", err)),
            })
    }

    fn put(&mut self, blocks: Vec<BlockPair>) -> Result<(), BlockStoreError> {
        self.put_blocks(blocks).map_err(|err| match err {
            DatabaseError::NotFoundError(_) => BlockStoreError::UnknownBlock,
            err => BlockStoreError::Error(format!("{:?}", err)),
        })
    }

    fn iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = BlockPair> + 'a>, BlockStoreError> {
        match self.get_chain_head() {
            Ok(head) => Ok(Box::new(self.get_block_by_height_iter(
                Some(head.header().block_num()),
                ByHeightDirection::Decreasing,
            ))),
            Err(DatabaseError::NotFoundError(_)) => Ok(Box::new(
                self.get_block_by_height_iter(None, ByHeightDirection::Decreasing),
            )),
            Err(err) => Err(BlockStoreError::Error(format!("{:?}", err))),
        }
    }
}

impl BatchIndex for CommitStore {
    fn contains(&self, id: &str) -> Result<bool, BlockStoreError> {
        self.contains_batch(id)
            .map_err(|err| BlockStoreError::Error(format!("{:?}", err)))
    }

    fn get_block_by_id(&self, id: &str) -> Result<Option<BlockPair>, BlockStoreError> {
        match self.get_by_batch_id(id) {
            Ok(block) => Ok(Some(block)),
            Err(DatabaseError::NotFoundError(_)) => Ok(None),
            Err(err) => Err(BlockStoreError::Error(format!("{:?}", err))),
        }
    }
}

impl TransactionIndex for CommitStore {
    fn contains(&self, id: &str) -> Result<bool, BlockStoreError> {
        self.contains_transaction(id)
            .map_err(|err| BlockStoreError::Error(format!("{:?}", err)))
    }

    fn get_block_by_id(&self, id: &str) -> Result<Option<BlockPair>, BlockStoreError> {
        match self.get_by_transaction_id(id) {
            Ok(block) => Ok(Some(block)),
            Err(DatabaseError::NotFoundError(_)) => Ok(None),
            Err(err) => Err(BlockStoreError::Error(format!("{:?}", err))),
        }
    }
}

impl IndexedBlockStore for CommitStore {}

struct CommitStoreGetIterator {
    store: CommitStore,
    block_ids: Vec<String>,
    index: usize,
}

impl Iterator for CommitStoreGetIterator {
    type Item = BlockPair;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(block_id) = self.block_ids.get(self.index) {
            self.index += 1;
            match self.store.get_by_block_id(block_id) {
                Ok(block) => Some(block),
                Err(DatabaseError::NotFoundError(_)) => None,
                Err(err) => {
                    error!("Error getting next block: {:?}", err);
                    None
                }
            }
        } else {
            None
        }
    }
}

pub enum ByHeightDirection {
    Increasing,
    Decreasing,
}

pub struct CommitStoreByHeightIterator {
    store: CommitStore,
    next: Option<u64>,
    direction: ByHeightDirection,
}

impl Iterator for CommitStoreByHeightIterator {
    type Item = BlockPair;

    fn next(&mut self) -> Option<Self::Item> {
        let block = match self.next {
            None => return None,
            Some(next) => match self.store.get_by_block_num(next) {
                Ok(block) => Some(block),
                Err(DatabaseError::NotFoundError(_)) => None,
                Err(err) => {
                    error!("Error getting next block: {:?}", err);
                    return None;
                }
            },
        };
        if block.is_some() {
            self.next = match self.direction {
                ByHeightDirection::Increasing => self.next.map(|next| next + 1),
                ByHeightDirection::Decreasing => self.next.and_then(|next| next.checked_sub(1)),
            }
        }
        block
    }
}

fn map_block_database_result_to_chain_reader_result(
    result: Result<BlockPair, DatabaseError>,
) -> Result<Option<BlockPair>, ChainReadError> {
    match result {
        Ok(pair) => Ok(Some(pair)),
        Err(DatabaseError::NotFoundError(_)) => Ok(None),
        Err(err) => Err(ChainReadError::GeneralReadError(format!("{:?}", err))),
    }
}

impl ChainReader for CommitStore {
    fn chain_head(&self) -> Result<Option<BlockPair>, ChainReadError> {
        map_block_database_result_to_chain_reader_result(self.get_chain_head())
    }

    fn get_block_by_block_id(&self, block_id: &str) -> Result<Option<BlockPair>, ChainReadError> {
        map_block_database_result_to_chain_reader_result(self.get_by_block_id(block_id))
    }

    fn get_block_by_block_num(&self, block_num: u64) -> Result<Option<BlockPair>, ChainReadError> {
        map_block_database_result_to_chain_reader_result(self.get_by_block_num(block_num))
    }

    fn count_committed_transactions(&self) -> Result<usize, ChainReadError> {
        self.get_transaction_count()
            .map_err(|err| ChainReadError::GeneralReadError(format!("{:?}", err)))
    }
}
