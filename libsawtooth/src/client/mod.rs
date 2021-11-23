// Copyright 2020 Bitwise IO
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

//! A trait for implementing a sawtooth client.

mod error;
#[cfg(feature = "client-rest")]
pub mod rest;

pub use error::SawtoothClientError;

use std::time::Duration;

/// A trait that can be used to interact with a sawtooth node.
pub trait SawtoothClient {
    /// Get a single batch in the current blockchain.
    fn get_batch(&self, batch_id: String) -> Result<Option<Batch>, SawtoothClientError>;
    /// Get all existing batches in the current blockchain.
    fn list_batches(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<Batch, SawtoothClientError>>>, SawtoothClientError>;
    /// Get a single transaction in the current blockchain.
    fn get_transaction(
        &self,
        transaction_id: String,
    ) -> Result<Option<Transaction>, SawtoothClientError>;
    /// Get all existing transactions in the current blockchain.
    fn list_transactions(
        &self,
    ) -> Result<
        Box<dyn Iterator<Item = Result<Transaction, SawtoothClientError>>>,
        SawtoothClientError,
    >;
    /// Get a single block in the current blockchain.
    fn get_block(&self, block_id: String) -> Result<Option<Block>, SawtoothClientError>;
    /// Get all existing blocks in the current blockchain.
    fn list_blocks(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<Block, SawtoothClientError>>>, SawtoothClientError>;
    /// Get a single state entry from the current blockchain.
    fn get_state(&self, address: String) -> Result<Option<SingleState>, SawtoothClientError>;
    /// Get all existing state entries in the current blockchain.
    fn list_states(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Result<State, SawtoothClientError>>>, SawtoothClientError>;
    /// Get the committed statuses for one or more batches in the current blockchain.
    fn list_batch_status(
        &self,
        batch_ids: Vec<&str>,
        wait: Option<Duration>,
    ) -> Result<Option<Vec<Status>>, SawtoothClientError>;
    /// Send one or more batches to the validator.
    ///
    /// # Arguments
    ///
    /// * `filename` - The name of the file containing the batches to be submitted
    /// * `wait` - The time, in seconds, to wait for batches to submit
    /// * `size_limit` - The maximum batch list size, batches are split for processing if they exceed this size
    fn submit_batches(
        &self,
        filename: String,
        wait: Option<Duration>,
        size_limit: usize,
    ) -> Result<Vec<String>, SawtoothClientError>;
}

/// A struct that represents a batch.
#[derive(Debug)]
pub struct Batch {
    pub header: Header,
    pub header_signature: String,
    pub trace: bool,
    pub transactions: Vec<Transaction>,
}
#[derive(Debug)]
pub struct Header {
    pub signer_public_key: String,
    pub transaction_ids: Vec<String>,
}
#[derive(Debug)]
pub struct Transaction {
    pub header: TransactionHeader,
    pub header_signature: String,
    pub payload: String,
}
#[derive(Debug)]
pub struct TransactionHeader {
    pub batcher_public_key: String,
    pub dependencies: Vec<String>,
    pub family_name: String,
    pub family_version: String,
    pub inputs: Vec<String>,
    pub nonce: String,
    pub outputs: Vec<String>,
    pub payload_sha512: String,
    pub signer_public_key: String,
}
#[derive(Debug)]
pub struct Block {
    pub header: BlockHeader,
    pub header_signature: String,
    pub batches: Vec<Batch>,
}
#[derive(Debug)]
pub struct BlockHeader {
    pub batch_ids: Vec<String>,
    pub block_num: String,
    pub consensus: String,
    pub previous_block_id: String,
    pub signer_public_key: String,
    pub state_root_hash: String,
}
#[derive(Debug)]
pub struct State {
    pub address: String,
    pub data: Vec<u8>,
}
#[derive(Debug)]
pub struct SingleState {
    pub data: Vec<u8>,
    pub head: String,
}
#[derive(Debug)]
pub struct Status {
    pub id: String,
    pub invalid_transactions: Vec<InvalidTransaction>,
    pub status: String,
}
#[derive(Debug)]
pub struct InvalidTransaction {
    pub id: String,
    pub message: String,
    pub extended_data: Vec<u8>,
}
