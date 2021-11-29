# Release Notes

## Changes in libsawtooth 0.7.1

- Make log dependency optional
- Remove `macro_use` of crate "log"

## Changes in libsawtooth 0.7.0

### Highlights

- Upgrade to Transact v0.4.
- Remove the redis-store, lmdb-store, btree-store, and stores features, which
  are replaced with domain-specific store traits such as ReceiptStore.
- Fix state_change_type when using PostgreSQL; previously the database column
  definition was incorrect resulting in errors.
- Fix several feature guards, including around the client::rest module and use
  of diesel.
- Update various dependencies including replacing openssl with sha2.

## Changes in libsawtooth 0.6.7

### Highlights

- Stabilize the `transaction-receipt-store` feature by moving it from
  experimental to stable.
- Stabilize the `transaction-receipt-store-lmdb` feature, this feature was
  renamed to `lmdb` and moved to stable.
- Stabilize the `postgres` feature by moving it from experimental to stable.
- Stabilize the `sqlite` feature by moving it from experimental to stable.

### Experimental Changes

- Update the `ReceiptStore`s `list_receipts_since` method to an iterator of
  `Result<TransactionReceipt, ReceiptStoreError>`
- Modify the Diesel backed receipt store implementation to have a `service_id`
  used to scope the diesel receipt store to a specific instance.

### Other Changes

- Disable Jenkins CI builds. GitHub Actions will be used going forward instead.
- Add the experimental features to the list of features that will be used to
  generate the rust documentation on docs.rs.

## Changes in libsawtooth 0.6.6

### Experimental Changes

- Add PendingBatchQueue to publisher behind the experimental feature
  'pending-batch-queue'.
- Add a `ReceiptStore` trait behind the experimental feature
  `transaction-receipt-store`.
- Add LMDB, SQLite and PostgreSQL implementations of the `ReceiptStore` trait.

### Other Changes

- Update depreciated `protobuf::parse_from_bytes` to `Message::parse_from_bytes`
- Add a DEFAULT_SIZE value for 32bit platforms.
- Update base64 dependency from 0.12 to 0.13.
- Update glob dependency from 0.2 to 0.3.
- Update reqwest dependency from 0.10 to 0.11.
- Update rand dependency from 0.4 to 0.8.
- Update redis dependency from 0.14 to 0.20.
- Update protobuf dependency from 2.19 to 2.23.
- Add an error module with generic errors `InternalError`, `InvalidStateError`,
  and `ResourceTemporarilyUnavailableError` that can be used throughout
  libsawtooth.
- Add a migrations module that handles SQLite and PostgreSQL migrations.

## Changes in libsawtooth 0.6.5

- Update the `cylinder` dependency to version `0.2`

## Changes in libsawtooth 0.6.4

### Experimental Changes

- In the sawtooth client, remove the existing constructor for
  `RestApiSawtoothClient` and create a builder. The builder includes a
  `with_url` function to set the url field of the struct.
- Modify the REST API backed implementation of the sawtooth client to allow
  for basic authentication when the REST API is behind a Basic Auth proxy. Add
  a function to the `RestApiSawtoothClientBuilder` to set the auth field when
  creating a new `RestApiSawtoothClient`.
- Add the `submit batches` command to the sawtooth client and REST API backed
  implementation. The function sends one or more batches to the sawtooth REST
  API to be submitted by the validator.

## Changes in libsawtooth 0.6.3

### Experimental Changes

- Fix a bug in the ChainController where, when a block was committed, consensus
  would be notified and the chain head updated before the state for that block
  was saved. This caused a race condition where state would be fetched with an
  invalid state root hash.
- Finalize the scheduler on cancel_block in the publisher. Before, the scheduler
  would only be cancelled. This would cause the scheduler to never shutdown.
- Fix a bug where, on a fork switch with multiple blocks, the new block would be
  sent to the chain observers with the receipts from another block.
- Add `list batch status` command to the sawtooth client and REST API backed
  client. This function retrieves the committed status of one or more batches
  with the given batch IDs.

## Changes in libsawtooth 0.6.2

### Experimental Changes

- Fix an issue with the use of `BlockValidator::validate_block` where the
  genesis block was never being marked as done in the `BlockScheduler`.

## Changes in libsawtooth 0.6.1

### Experimental Changes

- Add the `sawtooth::journal::state_verifier` module with its function
  `verify_state` to recompute any missing state root hashes.
- Fix a deadlock in the block publisher's spawned thread.
- Fix an integer underflow bug in the commit store.
- Add the following capabilities to the Sawtooth client:
  - Get transaction
  - Get block
  - Get state
  - List states
- Fix deserialization of errors in the REST API client by using the proper
  deserialization struct.
- Fix deserialization of paging in the REST API client's paged iterator by using
  the proper deserialization struct.

## Changes in libsawtooth 0.6.0

### Highlights
- Replace the `sawtooth::signing` module with the `cylinder` library, which
  provides a similar interface and implementations.
- Replace the execution platform with Transact.

### Experimental Changes
- Create a Sawtooth client to handle communications with the REST API. This
  feature is currently behind an experimental feature `client`. The client
  currently includes the following capabilities:
    - List batches
    - List transactions
    - Get a specific batch

### Other Changes
- Rework ChainController so it does not require `light_clone`. The
  `BlockValidator` has also been updated to return a `ChainControllerRequest` on
  block validation instead of the `BlockValidationResult` directly.
- Rewrite `validation rule enforcement` into a struct. This struct allows the
  block to be validated as it's constructed.
- Implement the "block info" batch injector in Rust.
- Update `PendingBatchPool` to use `BatchPair`s and change the update method to
  take a set of published batch IDs; while this implementation is potentially
  less efficient, it is more correct.
- Update `PermissionVerifier` to use `BatchPair`s.
- Add `CborMerkleState` for reading/writting cbor encoded state. This is
  required for backwards compatibility with Sawtooth state.
- Update `BlockValidator` to use Transact's execution transaction platform.
- Move the `BlockValidator` start/stop into the ChainController. This removes
  the requirement to need to clone the `BlockValidator`.
- Implement the GenesisController in Rust. The GenesisController now uses
  Transact's execution platform.
- Implement the `BlockPublisher` in Rust
- Remove the `TransactionCommitCache`, which is no longer be used by the block
  publisher.
- Remove the `CandidateBlock` trait, which has been replaced by an internal
  struct in the block publisher.
- Update the `ChainController::build_fork` method to return `BatchPair`s instead
  of `Batch`es.
- Move the `ChainHeadLock` to the publisher module since it's specific to the
  block publisher.
- Remove the `ExecutionPlatform` and `Scheduler` traits, since they are no
  longer used due to the Transact integration.


## Changes in libsawtooth 0.5.0

* Add the `protocol::block` module that provides the block protocol for
  Sawtooth, including protobuf conversions and a builder
* Remove the batch and transaction structs from Sawtooth and use the
  corresponding structs from Transact's protocols


## Changes in libsawtooth 0.4.0

* Add the `protocol` module with native setting and identity protocol structs
  and update code to use these structs instead of the corresponding protobufs
* Replace internal state and database implementations with implementations
  provided by Transact and remove the `database` module
* Add a permission verifier implementation that replaces the trait in the
  `gossip` module, use it for block validation, and remove the `gossip` module
  since it is now empty
* Make minor corrections and updates to Jenkins files


## Changes in libsawtooth 0.3.0

* Update transact dependency to 0.2, which enables storing the results of
  invalid transactions as transaction receipts
