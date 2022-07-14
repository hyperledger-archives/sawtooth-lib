// Copyright 2019 Cargill Incorporated
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

#![allow(clippy::extra_unused_lifetimes)]

#[macro_use]
#[cfg(all(feature = "diesel", feature = "transaction-receipt-store"))]
extern crate diesel;
#[macro_use]
#[cfg(feature = "diesel")]
extern crate diesel_migrations;
#[cfg(feature = "validator-internals")]
#[macro_use]
extern crate metrics;

#[cfg(feature = "artifact")]
pub mod artifact;
#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "validator-internals")]
pub mod consensus;
pub mod error;
#[cfg(feature = "validator-internals")]
pub mod hashlib;
#[cfg(feature = "validator-internals")]
pub mod journal;
pub mod migrations;
#[cfg(feature = "validator-internals")]
pub mod permissions;
pub mod protocol;
pub mod protos;
#[cfg(feature = "publisher")]
pub mod publisher;
#[cfg(any(feature = "transaction-receipt-store", feature = "validator-internals"))]
pub mod receipt;
#[cfg(feature = "validator-internals")]
pub mod scheduler;
#[cfg(feature = "validator-internals")]
pub mod state;
#[cfg(feature = "store")]
pub(crate) mod store;
