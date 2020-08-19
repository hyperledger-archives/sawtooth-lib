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

use transact::protocol::transaction::Transaction;

use crate::journal::block_manager::BlockManager;

#[derive(Debug, PartialEq)]
pub enum ChainCommitStateError {
    MissingDependency(String),
    DuplicateTransaction(String),
    DuplicateBatch(String),
    Error(String),
}

fn check_no_duplicates(ids: &[&str]) -> Option<String> {
    for (i, id1) in ids.iter().enumerate() {
        if ids[i + 1..ids.len()].contains(id1) {
            return Some(id1.to_string());
        }
    }
    None
}

pub fn validate_no_duplicate_batches(
    block_manager: &BlockManager,
    branch_head_id: &str,
    batch_ids: &[&str],
) -> Result<(), ChainCommitStateError> {
    if let Some(batch_id) = check_no_duplicates(batch_ids) {
        return Err(ChainCommitStateError::DuplicateBatch(batch_id));
    }

    if let Some(batch_id) = block_manager
        .contains_any_batches(branch_head_id, batch_ids)
        .map_err(|err| {
            ChainCommitStateError::Error(format!("During validate_no_duplicate_batches: {:?}", err))
        })?
    {
        return Err(ChainCommitStateError::DuplicateBatch(batch_id));
    }
    Ok(())
}

pub fn validate_no_duplicate_transactions(
    block_manager: &BlockManager,
    branch_head_id: &str,
    transaction_ids: &[&str],
) -> Result<(), ChainCommitStateError> {
    if let Some(txn_id) = check_no_duplicates(transaction_ids) {
        return Err(ChainCommitStateError::DuplicateTransaction(txn_id));
    }

    if let Some(transaction_id) = block_manager
        .contains_any_transactions(branch_head_id, transaction_ids)
        .map_err(|err| {
            ChainCommitStateError::Error(format!(
                "During validate_no_duplicate_transactions: {:?}",
                err
            ))
        })?
    {
        return Err(ChainCommitStateError::DuplicateTransaction(transaction_id));
    }

    Ok(())
}

pub fn validate_transaction_dependencies(
    block_manager: &BlockManager,
    branch_head_id: &str,
    transactions: &[Transaction],
) -> Result<(), ChainCommitStateError> {
    let mut dependencies = vec![];
    let mut txn_ids = vec![];
    for txn in transactions {
        txn_ids.push(txn.header_signature());

        let txn_pair = txn.clone().into_pair().map_err(|err| {
            ChainCommitStateError::Error(format!(
                "Unable to deserialize transaction header: {}",
                err
            ))
        })?;
        for dep in txn_pair.header().dependencies() {
            let dep = hex::encode(dep);
            if !dependencies.contains(&dep) {
                dependencies.push(dep);
            }
        }
    }
    for dep in &dependencies {
        // Check for dependencies within the given block's batches
        if txn_ids.contains(&dep.as_str()) {
            continue;
        }

        let block_manager_contains_transaction = block_manager
            .contains_any_transactions(branch_head_id, &[dep])
            .map_err(|err| {
                ChainCommitStateError::Error(format!(
                    "During validate transaction dependencies: {:?}",
                    err
                ))
            })?
            .is_some();

        if block_manager_contains_transaction {
            continue;
        }
        return Err(ChainCommitStateError::MissingDependency(dep.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    use std::collections::HashMap;

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};
    use transact::protocol::{
        batch::{Batch, BatchBuilder},
        transaction::{HashMethod, Transaction, TransactionBuilder},
    };

    use crate::journal::block_store::InMemoryBlockStore;
    use crate::journal::NULL_BLOCK_IDENTIFIER;
    use crate::protocol::block::{BlockBuilder, BlockPair};

    /// Creates Chains of blocks that match this diagram
    /// chain4                    B4-4  - B5-4
    ///                         /
    /// chain1          B2-1 - B3-1- B4-1 -- B5-1
    ///               /
    /// chain0    B0-B1-B2 - B3 -- B4 ---  B5
    ///                    \
    /// chain2               B3-2 - B4-2 -- B5-2
    ///                          \
    /// chain3                      B4-3 -- B5-3
    ///
    ///  the batches in B2-1, for example, are B2-1b0 and B2-1b1
    ///  the transactions in b0, are B2-1b0t0, B2'b0t1, and B2-1b0t2
    ///
    fn create_chains_to_put_in_block_manager() -> Vec<Vec<(String, BlockPair)>> {
        let mut previous_block_id = NULL_BLOCK_IDENTIFIER.to_string();
        let mut block_num = 0;
        let chain0 = ["B0", "B1", "B2", "B3", "B4", "B5"]
            .iter()
            .map(|block_handle| {
                let block =
                    create_block_w_batches_txns(&previous_block_id, block_num, block_handle);
                previous_block_id = block.block().header_signature().to_string();
                block_num += 1;
                (block_handle.to_string(), block)
            })
            .collect::<Vec<_>>();

        let mut previous_block_id = chain0
            .iter()
            .find_map(|(handle, block)| if handle == "B1" { Some(block) } else { None })
            .expect("B1 not found")
            .block()
            .header_signature()
            .to_string();
        let mut block_num = 2;
        let chain1 = ["B2-1", "B3-1", "B4-1", "B5-1"]
            .iter()
            .map(|block_handle| {
                let block =
                    create_block_w_batches_txns(&previous_block_id, block_num, block_handle);
                previous_block_id = block.block().header_signature().to_string();
                block_num += 1;
                (block_handle.to_string(), block)
            })
            .collect::<Vec<_>>();

        let mut previous_block_id = chain1
            .iter()
            .find_map(|(handle, block)| if handle == "B3-1" { Some(block) } else { None })
            .expect("B3-1 not found")
            .block()
            .header_signature()
            .to_string();
        let mut block_num = 4;
        let chain4 = ["B4-4", "B5-4"]
            .iter()
            .map(|block_handle| {
                let block =
                    create_block_w_batches_txns(&previous_block_id, block_num, block_handle);
                previous_block_id = block.block().header_signature().to_string();
                block_num += 1;
                (block_handle.to_string(), block)
            })
            .collect::<Vec<_>>();

        let mut previous_block_id = chain0
            .iter()
            .find_map(|(handle, block)| if handle == "B2" { Some(block) } else { None })
            .expect("B2 not found")
            .block()
            .header_signature()
            .to_string();
        let block_num = 3;
        let chain2 = ["B3-2", "B4-2", "B5-2"]
            .iter()
            .map(|block_handle| {
                let block =
                    create_block_w_batches_txns(&previous_block_id, block_num, block_handle);
                previous_block_id = block.block().header_signature().to_string();
                (block_handle.to_string(), block)
            })
            .collect::<Vec<_>>();

        let mut previous_block_id = chain2
            .iter()
            .find_map(|(handle, block)| if handle == "B3-2" { Some(block) } else { None })
            .expect("B3-2 not found")
            .block()
            .header_signature()
            .to_string();
        let block_num = 4;
        let chain3 = ["B4-3", "B5-3"]
            .iter()
            .map(|block_handle| {
                let block =
                    create_block_w_batches_txns(&previous_block_id, block_num, block_handle);
                previous_block_id = block.block().header_signature().to_string();
                (block_handle.to_string(), block)
            })
            .collect::<Vec<_>>();
        vec![chain0, chain1, chain4, chain2, chain3]
    }

    #[test]
    fn test_no_duplicates() {
        assert_eq!(check_no_duplicates(&["1", "2", "3"]), None);
    }

    #[test]
    fn test_duplicates1() {
        assert_eq!(check_no_duplicates(&["1", "2", "1"]), Some("1".into()));
    }

    #[test]
    fn test_duplicates2() {
        assert_eq!(check_no_duplicates(&["1", "1", "2"]), Some("1".into()));
    }

    #[test]
    fn test_dependency_in_other_fork() {
        let (block_manager, blocks) = setup_state();

        // Txn B2b0t0
        let dependency = blocks.get("B2").expect("B2 not found").block().batches()[0]
            .transactions()[0]
            .header_signature();
        let transactions: Vec<Transaction> = ["B6b0t0", "B6b0t1", "B6b0t2"]
            .iter()
            .map(|t_id| create_transaction(vec![dependency.into()], t_id.as_bytes().to_vec()))
            .collect();

        block_manager
            .persist(
                blocks
                    .get("B5")
                    .expect("B5 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manager is able to persist all blocks known to it");

        assert_eq!(
            validate_transaction_dependencies(
                &block_manager,
                blocks
                    .get("B5-1")
                    .expect("B5-1 not found")
                    .block()
                    .header_signature(),
                &transactions
            ),
            Err(ChainCommitStateError::MissingDependency(dependency.into()))
        );
    }

    #[test]
    fn test_dependency_in_chain() {
        let (block_manager, blocks) = setup_state();

        // Txn B1b0t0
        let dependency = blocks.get("B1").expect("B1 not found").block().batches()[0]
            .transactions()[0]
            .header_signature();
        let transactions: Vec<Transaction> = ["B6b0t0", "B6b0t1", "B6b0t2"]
            .iter()
            .map(|t_id| create_transaction(vec![dependency.into()], t_id.as_bytes().to_vec()))
            .collect();

        block_manager
            .persist(
                blocks
                    .get("B5")
                    .expect("B5 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manager is able to persist all blocks known to it");

        assert_eq!(
            validate_transaction_dependencies(
                &block_manager,
                blocks
                    .get("B5-1")
                    .expect("B5-1 not found")
                    .block()
                    .header_signature(),
                &transactions
            ),
            Ok(())
        );
    }

    #[test]
    fn test_dependency_in_chain_chain_head_greater() {
        let (block_manager, blocks) = setup_state();

        // Txn B1b0t0
        let dependency = blocks.get("B1").expect("B1 not found").block().batches()[0]
            .transactions()[0]
            .header_signature();
        let transactions: Vec<Transaction> = ["B3-1b0t0", "B3-1b0t1", "B3-1b0t2"]
            .iter()
            .map(|t_id| create_transaction(vec![dependency.into()], t_id.as_bytes().to_vec()))
            .collect();

        block_manager
            .persist(
                blocks
                    .get("B5")
                    .expect("B5 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manager is able to persist all blocks known to it");

        assert_eq!(
            validate_transaction_dependencies(
                &block_manager,
                blocks
                    .get("B2")
                    .expect("B2 not found")
                    .block()
                    .header_signature(),
                &transactions
            ),
            Ok(())
        );
    }

    #[test]
    fn test_dependency_in_uncommitted() {
        let (block_manager, blocks) = setup_state();

        // Txn B4-3b0t0
        let dependency = blocks
            .get("B4-3")
            .expect("B4-3 not found")
            .block()
            .batches()[0]
            .transactions()[0]
            .header_signature();
        let transactions: Vec<Transaction> = ["B6b0t0", "B6b0t1", "B6b0t2"]
            .iter()
            .map(|t_id| create_transaction(vec![dependency.into()], t_id.as_bytes().to_vec()))
            .collect();

        block_manager
            .persist(
                blocks
                    .get("B1")
                    .expect("B1 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manager is able to persist all blocks known to it");

        assert_eq!(
            validate_transaction_dependencies(
                &block_manager,
                blocks
                    .get("B5-3")
                    .expect("B5-3 not found")
                    .block()
                    .header_signature(),
                &transactions
            ),
            Ok(())
        );
    }

    #[test]
    fn test_no_duplicate_batches() {
        let (block_manager, blocks) = setup_state();

        let batches = &["B6b0", "B6b1"];

        block_manager
            .persist(
                blocks
                    .get("B4")
                    .expect("B4 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manager is able to persist all blocks known to it");

        assert_eq!(
            validate_no_duplicate_batches(
                &block_manager,
                blocks
                    .get("B5")
                    .expect("B5 not found")
                    .block()
                    .header_signature(),
                batches,
            ),
            Ok(())
        );
    }

    #[test]
    fn test_no_duplicates_because_batches_in_other_fork() {
        let (block_manager, blocks) = setup_state();

        let b3 = blocks.get("B3").expect("B3 not found").block();
        let batches = &[
            b3.batches()[0].header_signature(),
            b3.batches()[1].header_signature(),
        ];

        block_manager
            .persist(
                blocks
                    .get("B5")
                    .expect("B5 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manager is able to persist all blocks known to it");

        assert_eq!(
            validate_no_duplicate_batches(
                &block_manager,
                blocks
                    .get("B5-2")
                    .expect("B5-2 not found")
                    .block()
                    .header_signature(),
                batches,
            ),
            Ok(())
        );
    }

    #[test]
    fn test_no_duplicate_batches_duplicate_in_branch() {
        let (block_manager, blocks) = setup_state();

        let b2 = blocks.get("B2").expect("B2 not found").block();
        let batches = &[
            b2.batches()[0].header_signature(),
            b2.batches()[1].header_signature(),
        ];

        block_manager
            .persist(
                blocks
                    .get("B5")
                    .expect("B5 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manage is able to persist all blocks known to it");

        assert!(validate_no_duplicate_batches(
            &block_manager,
            blocks
                .get("B5-2")
                .expect("B5-2 not found")
                .block()
                .header_signature(),
            batches,
        )
        .is_err(),);
    }

    #[test]
    fn test_no_duplicate_batches_duplicate_in_uncommitted() {
        let (block_manager, blocks) = setup_state();

        let b52 = blocks.get("B5-2").expect("B5-2 not found").block();
        let batches = &[
            b52.batches()[0].header_signature(),
            b52.batches()[1].header_signature(),
        ];

        block_manager
            .persist(
                blocks
                    .get("B5")
                    .expect("B5 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manager is able to persist all blocks known to it");

        assert!(validate_no_duplicate_batches(
            &block_manager,
            blocks
                .get("B5-2")
                .expect("B5-2 not found")
                .block()
                .header_signature(),
            batches,
        )
        .is_err(),);
    }

    #[test]
    fn test_no_duplicate_batches_duplicate_in_other_fork() {
        let (block_manager, blocks) = setup_state();

        let b2 = blocks.get("B2").expect("B2 not found").block();
        let batches = &[
            b2.batches()[0].header_signature(),
            b2.batches()[1].header_signature(),
        ];

        block_manager
            .persist(
                blocks
                    .get("B5")
                    .expect("B5 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manager is able to persist all blocks known to it");

        assert_eq!(
            validate_no_duplicate_batches(
                &block_manager,
                blocks
                    .get("B5-1")
                    .expect("B5-1 not found")
                    .block()
                    .header_signature(),
                batches,
            ),
            Ok(())
        );
    }

    #[test]
    fn test_no_duplicate_transactions() {
        let (block_manager, blocks) = setup_state();

        let transactions = &["B6b0t0", "B6b0t1", "B6b0t2", "B6b1t0", "B6b1t1", "B6b1t2"];

        block_manager
            .persist(
                blocks
                    .get("B5")
                    .expect("B5 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manager is able to persist all blocks known to it");

        assert_eq!(
            validate_no_duplicate_transactions(
                &block_manager,
                blocks
                    .get("B5")
                    .expect("B5 not found")
                    .block()
                    .header_signature(),
                transactions,
            ),
            Ok(())
        );
    }

    #[test]
    fn test_no_duplicate_transactions_duplicate_in_branch() {
        let (block_manager, blocks) = setup_state();

        let b2b0t2 = blocks.get("B2").expect("B2 not found").block().batches()[0].transactions()[2]
            .header_signature();
        let transactions = &["B6b0t0", "B6b0t1", b2b0t2];

        block_manager
            .persist(
                blocks
                    .get("B5-3")
                    .expect("B5-3 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manager is able to persist all blocks known to it");

        assert!(validate_no_duplicate_transactions(
            &block_manager,
            blocks
                .get("B5-2")
                .expect("B5-2 not found")
                .block()
                .header_signature(),
            transactions,
        )
        .is_err(),)
    }

    #[test]
    fn test_no_duplicate_transactions_duplicate_in_uncommitted() {
        let (block_manager, blocks) = setup_state();

        let b21b0t1 = blocks
            .get("B2-1")
            .expect("B2-1 not found")
            .block()
            .batches()[0]
            .transactions()[1]
            .header_signature();
        let transactions = &["B6b0t0", "B6b0t1", b21b0t1];

        block_manager
            .persist(
                blocks
                    .get("B5-3")
                    .expect("B5-3 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manager is able to persist all blocks known to it");

        assert!(validate_no_duplicate_transactions(
            &block_manager,
            blocks
                .get("B5-4")
                .expect("B5-4 not found")
                .block()
                .header_signature(),
            transactions,
        )
        .is_err(),);
    }

    #[test]
    fn test_no_duplicate_transactions_duplicate_in_other_fork() {
        let (block_manager, blocks) = setup_state();

        let b2b0t1 = blocks.get("B2").expect("B2 not found").block().batches()[0].transactions()[1]
            .header_signature();
        let transactions = &["B6b0t0", "B6b0t1", b2b0t1];

        block_manager
            .persist(
                blocks
                    .get("B5")
                    .expect("B5 not found")
                    .block()
                    .header_signature(),
                "commit",
            )
            .expect("The block manager is able to persist all blocks known to it");

        assert_eq!(
            validate_no_duplicate_transactions(
                &block_manager,
                blocks
                    .get("B5-1")
                    .expect("B5-1 not found")
                    .block()
                    .header_signature(),
                transactions,
            ),
            Ok(())
        );
    }

    #[test]
    fn test_before_genesis() {
        let (block_manager, blocks) = setup_state();

        let b1 = blocks.get("B1").expect("B1 not found").block();
        let b1b0 = &b1.batches()[0];
        let transactions = &[
            b1b0.transactions()[0].header_signature(),
            b1b0.transactions()[1].header_signature(),
            b1b0.transactions()[2].header_signature(),
        ];
        let batches = &[b1b0.header_signature(), b1.batches()[1].header_signature()];

        assert_eq!(
            validate_no_duplicate_batches(&block_manager, NULL_BLOCK_IDENTIFIER, batches,),
            Ok(())
        );

        assert_eq!(
            validate_no_duplicate_transactions(&block_manager, NULL_BLOCK_IDENTIFIER, transactions,),
            Ok(())
        );

        let b3 = blocks.get("B3").expect("B3 not found").block();
        let b3b0 = &b3.batches()[0];
        let transactions = &[
            b3b0.transactions()[0].header_signature(),
            b3b0.transactions()[1].header_signature(),
            b3b0.transactions()[2].header_signature(),
        ];
        let batches = &[b3b0.header_signature(), b3.batches()[1].header_signature()];

        assert_eq!(
            validate_no_duplicate_batches(
                &block_manager,
                blocks
                    .get("B2")
                    .expect("B2 not found")
                    .block()
                    .header_signature(),
                batches,
            ),
            Ok(())
        );

        assert_eq!(
            validate_no_duplicate_transactions(
                &block_manager,
                blocks
                    .get("B2")
                    .expect("B2 not found")
                    .block()
                    .header_signature(),
                transactions,
            ),
            Ok(())
        );

        let b3 = blocks.get("B3").expect("B3 not found").block();
        let b3b0 = &b3.batches()[0];
        let transactions = &[
            b3b0.transactions()[0].header_signature(),
            b3b0.transactions()[1].header_signature(),
            b3b0.transactions()[2].header_signature(),
        ];
        let batches = &[b3b0.header_signature(), b3.batches()[1].header_signature()];

        assert!(validate_no_duplicate_batches(
            &block_manager,
            blocks
                .get("B3")
                .expect("B3 not found")
                .block()
                .header_signature(),
            batches,
        )
        .is_err(),);
        assert!(validate_no_duplicate_transactions(
            &block_manager,
            blocks
                .get("B3")
                .expect("B3 not found")
                .block()
                .header_signature(),
            transactions,
        )
        .is_err(),);
    }

    fn create_block_w_batches_txns(
        previous_block_id: &str,
        block_num: u64,
        block_handle: &str,
    ) -> BlockPair {
        let batches = vec!["b0", "b1"]
            .into_iter()
            .map(|batch_handle: &str| {
                let txns = vec!["t0", "t1", "t2"]
                    .into_iter()
                    .map(|txn_handle: &str| {
                        let nonce = format!("{}{}{}", block_handle, batch_handle, txn_handle)
                            .as_bytes()
                            .to_vec();
                        create_transaction(vec![], nonce)
                    })
                    .collect();
                create_batch(txns)
            })
            .collect();

        create_block(previous_block_id, block_num, batches)
    }

    fn create_block(previous_block_id: &str, block_num: u64, batches: Vec<Batch>) -> BlockPair {
        BlockBuilder::new()
            .with_block_num(block_num)
            .with_previous_block_id(previous_block_id.into())
            .with_state_root_hash("".into())
            .with_batches(batches)
            .build_pair(&*new_signer())
            .expect("Failed to build block pair")
    }

    fn create_batch(transactions: Vec<Transaction>) -> Batch {
        BatchBuilder::new()
            .with_transactions(transactions)
            .build(&*new_signer())
            .expect("Failed to build batch")
    }

    fn create_transaction(dependencies: Vec<String>, nonce: Vec<u8>) -> Transaction {
        let dependencies = dependencies
            .iter()
            .map(hex::decode)
            .collect::<Result<_, _>>()
            .expect("Failed to convert dependencies to bytes");

        TransactionBuilder::new()
            .with_dependencies(dependencies)
            .with_family_name("test".into())
            .with_family_version("1.0".into())
            .with_inputs(vec![])
            .with_outputs(vec![])
            .with_payload_hash_method(HashMethod::SHA512)
            .with_payload(vec![])
            .with_nonce(nonce)
            .build(&*new_signer())
            .expect("Failed to build transaction")
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }

    fn setup_state() -> (BlockManager, HashMap<String, BlockPair>) {
        let block_manager = BlockManager::new();
        let branches = create_chains_to_put_in_block_manager();

        for branch in branches.clone().into_iter() {
            block_manager
                .put(
                    branch
                        .into_iter()
                        .map(|(_, block_pair)| block_pair)
                        .collect(),
                )
                .expect("The branches were created to be `put` in the block manager without error");
        }
        let block_store = Box::new(InMemoryBlockStore::default());
        block_manager
            .add_store("commit", block_store.clone())
            .expect("The block manager failed to add a blockstore");

        (
            block_manager,
            branches
                .into_iter()
                .flat_map(|branch| branch.into_iter())
                .collect(),
        )
    }
}
