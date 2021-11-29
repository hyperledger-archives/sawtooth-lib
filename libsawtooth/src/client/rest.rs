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

//! A client for interacting with sawtooth services.

use base64::{decode, encode};
use log::info;
use protobuf::Message;
use reqwest::{blocking::Client, header};
use serde::Deserialize;
use std::collections::VecDeque;
use std::fs::File;
use std::time::{Duration, Instant};

use crate::client::SawtoothClient;
use crate::client::{
    Batch as ClientBatch, Block as ClientBlock, BlockHeader as ClientBlockHeader,
    Header as ClientHeader, InvalidTransaction as ClientInvalidTransaction,
    SingleState as ClientSingleState, State as ClientState, Status as ClientStatus,
    Transaction as ClientTransaction, TransactionHeader as ClientTransactionHeader,
};
use crate::protos::batch::BatchList;

pub use super::error::SawtoothClientError;

/// A client that can be used to interact with sawtooth services. This client handles
/// communication with the REST API.
pub struct RestApiSawtoothClient {
    url: String,
    auth: Option<String>,
}

/// Builder for building a RestApiSawtoothClient
#[derive(Default)]
pub struct RestApiSawtoothClientBuilder {
    url: Option<String>,
    auth: Option<(String, String)>,
}

impl RestApiSawtoothClientBuilder {
    /// Creates a new RestApiSawtoothClientBuilder
    pub fn new() -> Self {
        RestApiSawtoothClientBuilder::default()
    }
    /// Sets the url
    ///
    /// # Arguments
    ///
    /// * `url` - The URL of the bind endpoint of the sawtooth REST API.
    pub fn with_url(mut self, url: &str) -> Self {
        self.url = Some(url.into());
        self
    }
    /// Sets auth
    ///
    /// # Arguments
    ///
    /// * `auth` - The credentials of the request authorizer.
    pub fn with_basic_auth(mut self, username: &str, password: &str) -> Self {
        self.auth = Some((username.into(), password.into()));
        self
    }
    /// Builds a RestApiSawtoothClient
    ///
    /// Returns an error if the url is not set
    pub fn build(self) -> Result<RestApiSawtoothClient, SawtoothClientError> {
        Ok(RestApiSawtoothClient {
            url: self.url.ok_or_else(|| {
                SawtoothClientError::new("Error building RestApiSawtoothClient, missing url")
            })?,
            auth: self.auth.map(|auth| format!("{}:{}", auth.0, auth.1)),
        })
    }
}

/// Implement the sawthooth client trait for the REST API sawtooth client
impl SawtoothClient for RestApiSawtoothClient {
    /// Get the batch with the given batch_id from the current blockchain
    fn get_batch(&self, batch_id: String) -> Result<Option<ClientBatch>, SawtoothClientError> {
        let url = format!("{}/batches/{}", &self.url, &batch_id);
        let error_msg = &format!("unable to get batch {}", batch_id);

        Ok(get::<Single<Batch>>(&url, error_msg, self.auth.as_deref())?
            .map(|singlebatch| singlebatch.data.into()))
    }
    /// List all batches in the current blockchain
    fn list_batches(
        &self,
    ) -> Result<
        Box<dyn Iterator<Item = Result<ClientBatch, SawtoothClientError>>>,
        SawtoothClientError,
    > {
        let url = format!("{}/batches", &self.url);

        Ok(Box::new(PagingIter::new(&url, self.auth.as_deref())?.map(
            |item: Result<Batch, _>| item.map(|batch| batch.into()),
        )))
    }
    /// Get the transaction with the given transaction_id from the current blockchain
    fn get_transaction(
        &self,
        transaction_id: String,
    ) -> Result<Option<ClientTransaction>, SawtoothClientError> {
        let url = format!("{}/transactions/{}", &self.url, &transaction_id);
        let error_msg = &format!("unable to get transaction {}", transaction_id);

        Ok(
            get::<Single<Transaction>>(&url, error_msg, self.auth.as_deref())?
                .map(|singletxn| singletxn.data.into()),
        )
    }
    /// List all transactions in the current blockchain.
    fn list_transactions(
        &self,
    ) -> Result<
        Box<dyn Iterator<Item = Result<ClientTransaction, SawtoothClientError>>>,
        SawtoothClientError,
    > {
        let url = format!("{}/transactions", &self.url);

        Ok(Box::new(PagingIter::new(&url, self.auth.as_deref())?.map(
            |item: Result<Transaction, _>| item.map(|txn| txn.into()),
        )))
    }
    /// Get the block with the given block_id from the current blockchain
    fn get_block(&self, block_id: String) -> Result<Option<ClientBlock>, SawtoothClientError> {
        let url = format!("{}/blocks/{}", &self.url, &block_id);
        let error_msg = &format!("unable to get block {}", block_id);

        Ok(get::<Single<Block>>(&url, error_msg, self.auth.as_deref())?
            .map(|singleblock| singleblock.data.into()))
    }
    /// List all blocks in the current blockchain
    fn list_blocks(
        &self,
    ) -> Result<
        Box<dyn Iterator<Item = Result<ClientBlock, SawtoothClientError>>>,
        SawtoothClientError,
    > {
        let url = format!("{}/blocks", &self.url);

        Ok(Box::new(PagingIter::new(&url, self.auth.as_deref())?.map(
            |item: Result<Block, _>| item.map(|block| block.into()),
        )))
    }
    /// Get the state entry with the given address from the current blockchain
    fn get_state(&self, address: String) -> Result<Option<ClientSingleState>, SawtoothClientError> {
        let url = format!("{}/state/{}", &self.url, &address);
        let error_msg = &format!("unable to get state at address {}", address);

        Ok(get::<SingleState>(&url, error_msg, self.auth.as_deref())?.map(convert_single_state))?
            .transpose()
    }
    /// List all state entries in the current blockchain
    fn list_states(
        &self,
    ) -> Result<
        Box<dyn Iterator<Item = Result<ClientState, SawtoothClientError>>>,
        SawtoothClientError,
    > {
        let url = format!("{}/state", &self.url);

        Ok(Box::new(
            PagingIter::new(&url, self.auth.as_deref())?
                .map(|item: Result<State, _>| item.map(convert_state)?),
        ))
    }
    /// List the committed status of one or more batches with the given batch_ids.
    fn list_batch_status(
        &self,
        batch_ids: Vec<&str>,
        wait: Option<Duration>,
    ) -> Result<Option<Vec<ClientStatus>>, SawtoothClientError> {
        let mut url = format!("{}/batch_statuses?", &self.url);

        if let Some(time) = wait {
            let wait_time = time.as_secs();
            url = url + &format!("wait={}&", wait_time);
        }

        let ids = batch_ids.join(",");
        url = url + &format!("id={}", ids);

        let error_msg = &format!("unable to get the status for batches: {}", ids);

        Ok(get::<StatusList>(&url, error_msg, self.auth.as_deref())?.map(convert_status_list))?
            .transpose()
    }
    /// Send one or more batches to the sawtooth REST API to be submitted to the validator
    fn submit_batches(
        &self,
        filename: String,
        wait: Option<Duration>,
        size_limit: usize,
    ) -> Result<Vec<String>, SawtoothClientError> {
        let mut url = format!("{}/batches", &self.url);
        if let Some(wait_time) = wait {
            url = url + &format!("?wait={}", wait_time.as_secs().to_string());
        }

        let mut batch_file = File::open(filename).map_err(|err| {
            SawtoothClientError::new_with_source("failed to open batch file", err.into())
        })?;

        let batch_list: BatchList = Message::parse_from_reader(&mut batch_file).map_err(|err| {
            SawtoothClientError::new_with_source("unable to parse file contents", err.into())
        })?;
        let len = batch_list.batches.len();
        let batch_ids = batch_list
            .batches
            .iter()
            .map(|batch| batch.header_signature.clone())
            .collect();

        let batch_lists = split_batches(batch_list, size_limit);

        let start = Instant::now();

        for list in batch_lists.into_iter() {
            let submit_list = list.write_to_bytes().map_err(|err| {
                SawtoothClientError::new_with_source(
                    "failed to get bytes from batch list",
                    err.into(),
                )
            })?;
            let mut request = Client::new()
                .post(&url)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(submit_list);
            if let Some(ref auth) = &self.auth {
                request = request.header("Authorization", format!("Basic {}", encode(auth)));
            }
            let response = request.send().map_err(|err| {
                SawtoothClientError::new_with_source("request failed", err.into())
            })?;
            if response.status().as_u16() == 401 {
                return Err(SawtoothClientError::new(&format!(
                    "{}, {}",
                    response.status(),
                    "failed to authenticate"
                )));
            }
            if response.status().as_u16() != 202 {
                let status = response.status();
                let msg: ErrorResponse = response.json().map_err(|err| {
                    SawtoothClientError::new_with_source(
                        "failed to deserialize error response body",
                        err.into(),
                    )
                })?;
                return Err(SawtoothClientError::new(&format!("{} {}", status, msg)));
            }
        }
        info!(
            "batches: {},  batch/sec: {}",
            len,
            if len == 0 {
                0.0f64
            } else {
                len as f64 / (((start.elapsed()).as_millis() as f64) / 1000f64)
            }
        );
        Ok(batch_ids)
    }
}

/// Split a batch list into multiple batch lists of size 'size_limit'
fn split_batches(batch_list: BatchList, size_limit: usize) -> Vec<BatchList> {
    let mut batch_lists = Vec::new();
    for chunk in batch_list.batches.chunks(size_limit) {
        let mut new_batch_list = BatchList::new();
        new_batch_list.set_batches(protobuf::RepeatedField::from_vec(chunk.to_vec()));
        batch_lists.push(new_batch_list);
    }
    batch_lists
}

/// used for deserializing single objects returned by the REST API.
fn get<T>(url: &str, error_msg: &str, auth: Option<&str>) -> Result<Option<T>, SawtoothClientError>
where
    T: for<'a> serde::de::Deserialize<'a> + Sized,
{
    let mut request = Client::new().get(url);
    if let Some(auth) = auth {
        request = request.header("Authorization", format!("Basic {}", encode(auth)));
    }
    let response = request
        .send()
        .map_err(|err| SawtoothClientError::new_with_source("request failed", err.into()))?;

    if response.status().is_success() {
        let obj: T = response.json().map_err(|err| {
            SawtoothClientError::new_with_source("failed to deserialize response body", err.into())
        })?;
        Ok(Some(obj))
    } else if response.status().as_u16() == 404 {
        Ok(None)
    } else if response.status().as_u16() == 401 {
        Err(SawtoothClientError::new(&format!(
            "{}, {}",
            response.status(),
            "failed to authenticate"
        )))
    } else {
        let status = response.status();
        let msg: ErrorResponse = response.json().map_err(|err| {
            SawtoothClientError::new_with_source(
                "failed to deserialize error response body",
                err.into(),
            )
        })?;
        Err(SawtoothClientError::new(&format!(
            "{} {} {}",
            error_msg, status, msg
        )))
    }
}

/// Iterator used for parsing and deserializing data returned by the REST API.
struct PagingIter<T>
where
    T: for<'a> serde::de::Deserialize<'a> + Sized,
{
    next: Option<String>,
    cache: VecDeque<T>,
    auth: Option<String>,
}

impl<T> PagingIter<T>
where
    T: for<'a> serde::de::Deserialize<'a> + Sized,
{
    /// Create a new 'PagingIter' which will make a call to the REST API and load the initial
    /// cache with the first page of items.
    fn new(url: &str, auth: Option<&str>) -> Result<Self, SawtoothClientError> {
        let mut new_iter = Self {
            next: Some(url.to_string()),
            cache: VecDeque::with_capacity(0),
            auth: auth.map(String::from),
        };
        new_iter.reload_cache()?;
        Ok(new_iter)
    }

    /// If another page of items exists, use the 'next' URL from the current page and
    /// reload the cache with the next page of items.
    fn reload_cache(&mut self) -> Result<(), SawtoothClientError> {
        if let Some(url) = &self.next.take() {
            let mut request = Client::new().get(url);
            if let Some(auth) = &self.auth {
                request = request.header("Authorization", format!("Basic {}", encode(auth)));
            }
            let response = request.send().map_err(|err| {
                SawtoothClientError::new_with_source("request failed", err.into())
            })?;
            if response.status().as_u16() == 401 {
                return Err(SawtoothClientError::new(&format!(
                    "{}, {}",
                    response.status(),
                    "failed to authenticate"
                )));
            }
            let page: Page<T> = response.json().map_err(|err| {
                SawtoothClientError::new_with_source(
                    "failed to deserialize response body",
                    err.into(),
                )
            })?;

            self.cache = page.data.into();

            self.next = page.paging.next.map(String::from);
        }
        Ok(())
    }
}

impl<T> Iterator for PagingIter<T>
where
    T: for<'a> serde::de::Deserialize<'a> + Sized,
{
    type Item = Result<T, SawtoothClientError>;
    /// Return the next item from the cache, if the cache is empty reload it.
    fn next(&mut self) -> Option<Self::Item> {
        if self.cache.is_empty() && self.next.is_some() {
            if let Err(err) = self.reload_cache() {
                return Some(Err(err));
            }
        };
        self.cache.pop_front().map(Ok)
    }
}

/// A struct that represents a page of items, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct Page<T: Sized> {
    #[serde(bound(deserialize = "T: Deserialize<'de>"))]
    data: Vec<T>,
    paging: PageInfo,
}

#[derive(Debug, Deserialize)]
struct PageInfo {
    next: Option<String>,
}

/// A struct that represents a single state object, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct SingleState {
    data: String,
    head: String,
}

/// A struct that represents a batch, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct Batch {
    header: Header,
    header_signature: String,
    trace: bool,
    transactions: Vec<Transaction>,
}

/// A struct that represents a header in a batch, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct Header {
    signer_public_key: String,
    transaction_ids: Vec<String>,
}

/// A struct that represents a transaction, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct Transaction {
    header: TransactionHeader,
    header_signature: String,
    payload: String,
}

/// A struct that represents a header in a transaction, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct TransactionHeader {
    batcher_public_key: String,
    dependencies: Vec<String>,
    family_name: String,
    family_version: String,
    inputs: Vec<String>,
    nonce: String,
    outputs: Vec<String>,
    payload_sha512: String,
    signer_public_key: String,
}

/// A struct that represents a block, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct Block {
    header: BlockHeader,
    header_signature: String,
    batches: Vec<Batch>,
}

/// A struct that represents a header in a block, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct BlockHeader {
    batch_ids: Vec<String>,
    block_num: String,
    consensus: String,
    previous_block_id: String,
    signer_public_key: String,
    state_root_hash: String,
}

/// A struct that represents a state, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
pub struct State {
    pub address: String,
    pub data: String,
}

/// A struct that represents the data returned by the REST API when retrieving a single item.
/// Used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct Single<T: Sized> {
    #[serde(bound(deserialize = "T: Deserialize<'de>"))]
    data: T,
}

/// A struct that represnts a list of batch statuses, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct StatusList {
    data: Vec<Status>,
}

/// A struct that represents a batch status, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct Status {
    id: String,
    invalid_transactions: Vec<InvalidTransaction>,
    status: String,
}

/// A struct that represents an invalid transaction used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct InvalidTransaction {
    id: String,
    message: String,
    extended_data: String,
}

impl From<Batch> for ClientBatch {
    fn from(batch: Batch) -> Self {
        let mut txns = Vec::new();
        for t in batch.transactions {
            let new_transaction: ClientTransaction = t.into();
            txns.push(new_transaction);
        }
        Self {
            header: batch.header.into(),
            header_signature: batch.header_signature,
            trace: batch.trace,
            transactions: txns,
        }
    }
}

impl From<Header> for ClientHeader {
    fn from(header: Header) -> Self {
        Self {
            signer_public_key: header.signer_public_key,
            transaction_ids: header.transaction_ids,
        }
    }
}

impl From<Transaction> for ClientTransaction {
    fn from(txn: Transaction) -> Self {
        Self {
            header: txn.header.into(),
            header_signature: txn.header_signature,
            payload: txn.payload,
        }
    }
}

impl From<TransactionHeader> for ClientTransactionHeader {
    fn from(txn_header: TransactionHeader) -> Self {
        Self {
            batcher_public_key: txn_header.batcher_public_key,
            dependencies: txn_header.dependencies,
            family_name: txn_header.family_name,
            family_version: txn_header.family_version,
            inputs: txn_header.inputs,
            nonce: txn_header.nonce,
            outputs: txn_header.outputs,
            payload_sha512: txn_header.payload_sha512,
            signer_public_key: txn_header.signer_public_key,
        }
    }
}

impl From<Block> for ClientBlock {
    fn from(block: Block) -> Self {
        let clientbatches = block
            .batches
            .into_iter()
            .map(|batch| batch.into())
            .collect();
        Self {
            header: block.header.into(),
            header_signature: block.header_signature,
            batches: clientbatches,
        }
    }
}

impl From<BlockHeader> for ClientBlockHeader {
    fn from(block_header: BlockHeader) -> Self {
        Self {
            batch_ids: block_header.batch_ids,
            block_num: block_header.block_num,
            consensus: block_header.consensus,
            previous_block_id: block_header.previous_block_id,
            signer_public_key: block_header.signer_public_key,
            state_root_hash: block_header.state_root_hash,
        }
    }
}

fn convert_state(state: State) -> Result<ClientState, SawtoothClientError> {
    Ok(ClientState {
        address: state.address,
        data: decode(state.data).map_err(|err| {
            SawtoothClientError::new_with_source("failed to decode state data", err.into())
        })?,
    })
}

fn convert_single_state(state: SingleState) -> Result<ClientSingleState, SawtoothClientError> {
    Ok(ClientSingleState {
        data: decode(state.data).map_err(|err| {
            SawtoothClientError::new_with_source("failed to decode state data", err.into())
        })?,
        head: state.head,
    })
}

fn convert_status_list(status_list: StatusList) -> Result<Vec<ClientStatus>, SawtoothClientError> {
    let mut statuses = Vec::new();
    for stat in status_list.data {
        let mut txns = Vec::new();
        for invalid_txn in stat.invalid_transactions {
            txns.push(ClientInvalidTransaction {
                id: invalid_txn.id,
                message: invalid_txn.message,
                extended_data: decode(invalid_txn.extended_data).map_err(|err| {
                    SawtoothClientError::new_with_source(
                        "failed to decode extended data",
                        err.into(),
                    )
                })?,
            });
        }
        statuses.push(ClientStatus {
            id: stat.id,
            invalid_transactions: txns,
            status: stat.status,
        });
    }
    Ok(statuses)
}

/// Used for deserializing error responses from the Sawtooth REST API.
#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: ErrData,
}

#[derive(Debug, Deserialize)]
struct ErrData {
    message: String,
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.error.message)
    }
}
