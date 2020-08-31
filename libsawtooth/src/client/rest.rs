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

use crate::client::Batch as ClientBatch;
use crate::client::SawtoothClient;

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

/// Implement the sawthooth client trait and define the function to list all existing batches
/// in the current blockchain.
impl SawtoothClient for RestApiSawtoothClient {
    fn list_batches(
        &self,
    ) -> Result<
        Box<dyn Iterator<Item = Result<ClientBatch, SawtoothClientError>>>,
        SawtoothClientError,
    > {
        let url = format!("{}/batches", &self.url);

        Ok(Box::new(BatchIter::new(&url).unwrap()))
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

            let page: Page = response.json().map_err(|err| {
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
        let batch = ClientBatch {
            signer_public_key: front.header.signer_public_key,
            header_signature: front.header_signature,
            txns: front.transactions.len(),
        };
        Some(Ok(batch))
    }
}

/// A struct that represents a page of batches, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
pub struct Page {
    data: Vec<Batch>,
    next: Option<String>,
}

/// A struct that represents a batch, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
pub struct Batch {
    pub header: Header,
    pub header_signature: String,
    pub transactions: Vec<Transaction>,
}

/// A struct that represents a header in a batch, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
pub struct Header {
    signer_public_key: String,
}

/// A struct that represents a transaction in a batch, used for deserializing JSON objects.
#[derive(Debug, Deserialize)]
pub struct Transaction {
    pub header_signature: String,
}
