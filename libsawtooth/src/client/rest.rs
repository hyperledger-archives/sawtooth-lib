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

use reqwest::blocking::Client;
use serde::Deserialize;
use std::collections::VecDeque;

use crate::client::SawtoothClient;
use crate::client::{
    Batch as ClientBatch, Header as ClientHeader, Transaction as ClientTransaction,
    TransactionHeader as ClientTransactionHeader,
};

pub use super::error::SawtoothClientError;

/// A client that can be used to interact with sawtooth services. This client handles
/// communication with the REST API.
pub struct RestApiSawtoothClient {
    url: String,
}

impl RestApiSawtoothClient {
    /// Create a new 'RestApiSawtoothClient' with the given base 'url'. The URL should be the bind
    /// endpoint of the sawtooth REST API.
    pub fn new(url: &str) -> Self {
        Self { url: url.into() }
    }
}

/// Implement the sawthooth client trait for the REST API sawtooth client
impl SawtoothClient for RestApiSawtoothClient {
    /// Get the batch with the given batch_id from the current blockchain
    fn get_batch(&self, batch_id: String) -> Result<Option<ClientBatch>, SawtoothClientError> {
        let url = format!("{}/batches/{}", &self.url, &batch_id);
        let request = Client::new().get(&url);
        let response = request
            .send()
            .map_err(|err| SawtoothClientError::new_with_source("request failed", err.into()))?;

        if response.status().is_success() {
            let batch: SingleBatch = response.json().map_err(|err| {
                SawtoothClientError::new_with_source(
                    "failed to deserialize response body",
                    err.into(),
                )
            })?;
            let clientbatch: ClientBatch = batch.data.into();
            Ok(Some(clientbatch))
        } else if response.status().as_u16() == 404 {
            Ok(None)
        } else {
            let status = response.status();
            let msg: ErrorResponse = response.json().map_err(|err| {
                SawtoothClientError::new_with_source(
                    "failed to deserialize error response body",
                    err.into(),
                )
            })?;
            Err(SawtoothClientError::new(&format!(
                "failed to get batch with given batch_id: {}: {}",
                status, msg
            )))
        }
    }
    /// List all batches in the current blockchain
    fn list_batches(
        &self,
    ) -> Result<
        Box<dyn Iterator<Item = Result<ClientBatch, SawtoothClientError>>>,
        SawtoothClientError,
    > {
        let url = format!("{}/batches", &self.url);

        Ok(Box::new(BatchIter::new(&url).unwrap()))
    }
    /// List all transactions in the current blockchain.
    fn list_transactions(
        &self,
    ) -> Result<
        Box<dyn Iterator<Item = Result<ClientTransaction, SawtoothClientError>>>,
        SawtoothClientError,
    > {
        let url = format!("{}/transactions", &self.url);

        Ok(Box::new(TransactionIter::new(&url).unwrap()))
    }
}

/// Iterator used for parsing and deserializing batches.
struct BatchIter {
    next: Option<String>,
    cache: VecDeque<Batch>,
}

impl BatchIter {
    /// Create a new 'BatchIter' which will make a call to the REST API and load the initial
    /// cache with the first page of batches.
    fn new(url: &str) -> Result<Self, SawtoothClientError> {
        let mut new_iter = Self {
            next: Some(url.to_string()),
            cache: VecDeque::with_capacity(0),
        };
        new_iter.reload_cache()?;
        Ok(new_iter)
    }

    /// If another page of batches exisits, use the 'next' url from the current page and
    /// reload the cache with the next page of batches.
    fn reload_cache(&mut self) -> Result<(), SawtoothClientError> {
        if let Some(url) = &self.next.take() {
            let request = Client::new().get(url);
            let response = request.send().map_err(|err| {
                SawtoothClientError::new_with_source("request failed", err.into())
            })?;

            let page: BatchPage = response.json().map_err(|err| {
                SawtoothClientError::new_with_source(
                    "failed to deserialize response body",
                    err.into(),
                )
            })?;

            self.cache = page.data.into();

            self.next = page.next;
        }
        Ok(())
    }
}

impl Iterator for BatchIter {
    type Item = Result<ClientBatch, SawtoothClientError>;
    /// Return the next batch from the cache, if the cache is empty reload it.
    fn next(&mut self) -> Option<Self::Item> {
        if self.cache.is_empty() && self.next.is_some() {
            if let Err(err) = self.reload_cache() {
                return Some(Err(SawtoothClientError::new_with_source(
                    "Unable to load iterator cache: {}",
                    err.into(),
                )));
            }
        };
        let front = self.cache.pop_front()?;
        let batch: ClientBatch = front.into();
        Some(Ok(batch))
    }
}

/// Iterator used for parsing and deserializing transactions.
struct TransactionIter {
    next: Option<String>,
    cache: VecDeque<Transaction>,
}

impl TransactionIter {
    /// Create a new 'TransactionIter' which will make a call to the REST API and load the initial
    /// cache with the first page of transactions.
    fn new(url: &str) -> Result<Self, SawtoothClientError> {
        let mut new_iter = Self {
            next: Some(url.to_string()),
            cache: VecDeque::with_capacity(0),
        };
        new_iter.reload_cache()?;
        Ok(new_iter)
    }

    /// If another page of transactions exisits, use the 'next' url from the current page and
    /// reload the cache with the next page of transactions.
    fn reload_cache(&mut self) -> Result<(), SawtoothClientError> {
        if let Some(url) = &self.next.take() {
            let request = Client::new().get(url);
            let response = request.send().map_err(|err| {
                SawtoothClientError::new_with_source("request failed", err.into())
            })?;

            let page: TransactionPage = response.json().map_err(|err| {
                SawtoothClientError::new_with_source(
                    "failed to deserialize response body",
                    err.into(),
                )
            })?;

            self.cache = page.data.into();

            self.next = page.next;
        }
        Ok(())
    }
}

impl Iterator for TransactionIter {
    type Item = Result<ClientTransaction, SawtoothClientError>;
    /// Return the next transaction from the cache, if the cache is empty reload it.
    fn next(&mut self) -> Option<Self::Item> {
        if self.cache.is_empty() && self.next.is_some() {
            if let Err(err) = self.reload_cache() {
                return Some(Err(err));
            }
        };
        let front = self.cache.pop_front()?;
        let transaction: ClientTransaction = front.into();
        Some(Ok(transaction))
    }
}

/// A struct that represents a page of batches, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct BatchPage {
    data: Vec<Batch>,
    next: Option<String>,
}

/// A struct that represents a page of transactions, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct TransactionPage {
    data: Vec<Transaction>,
    next: Option<String>,
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

/// A struct that represents the data returned by the REST API when retrieving a single batch.
/// Used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
struct SingleBatch {
    data: Batch,
}

impl Into<ClientBatch> for Batch {
    fn into(self) -> ClientBatch {
        let mut txns = Vec::new();
        for t in self.transactions {
            let new_transaction: ClientTransaction = t.into();
            txns.push(new_transaction);
        }
        ClientBatch {
            header: self.header.into(),
            header_signature: self.header_signature,
            trace: self.trace,
            transactions: txns,
        }
    }
}

impl Into<ClientHeader> for Header {
    fn into(self) -> ClientHeader {
        ClientHeader {
            signer_public_key: self.signer_public_key,
            transaction_ids: self.transaction_ids,
        }
    }
}

impl Into<ClientTransaction> for Transaction {
    fn into(self) -> ClientTransaction {
        ClientTransaction {
            header: self.header.into(),
            header_signature: self.header_signature,
            payload: self.payload,
        }
    }
}

impl Into<ClientTransactionHeader> for TransactionHeader {
    fn into(self) -> ClientTransactionHeader {
        ClientTransactionHeader {
            batcher_public_key: self.batcher_public_key,
            dependencies: self.dependencies,
            family_name: self.family_name,
            family_version: self.family_version,
            inputs: self.inputs,
            nonce: self.nonce,
            outputs: self.outputs,
            payload_sha512: self.payload_sha512,
            signer_public_key: self.signer_public_key,
        }
    }
}

/// Used for deserializing error responses from the Sawtooth REST API.
#[derive(Debug, Deserialize)]
struct ErrorResponse {
    message: String,
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
