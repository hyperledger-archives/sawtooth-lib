/*
 * Copyright 2018 Bitwise IO
 * Copyright 2020 Cargill Incorporated
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

//! Provides a permission verifier which checks permissions against batch and transaction
//! signatories.

use transact::protocol::{batch::BatchPair, transaction::Transaction};

use crate::protocol::identity::{Permission, Policy};

use super::error::IdentityError;
use super::IdentitySource;

// Roles
const ROLE_TRANSACTOR: &str = "transactor";
const ROLE_BATCH_TRANSACTOR: &str = "transactor.batch_signer";
const ROLE_TXN_TRANSACTOR: &str = "transactor.transaction_signer";

const POLICY_DEFAULT: &str = "default";

const ANY_KEY: &str = "*";

/// The PermissionVerifier validates that batches and their transactions are allowed according to
/// the roles and associated policies defined by an identity source.
///
/// The roles that this verifies against are:
///
/// - "transactor.transaction_signer"
/// - "transactor.batch_signer"
/// - "transactor"
/// - "default"
///
/// These roles are listed in the order of most-specific to least specific.
pub struct PermissionVerifier {
    identities: Box<dyn IdentitySource>,
}

impl PermissionVerifier {
    pub fn new(identities: Box<dyn IdentitySource>) -> Self {
        PermissionVerifier { identities }
    }

    /// Check the batch signing key against the allowed transactor
    /// permissions. The roles being checked are the following, from first
    /// to last:
    ///     "transactor.batch_signer"
    ///     "transactor"
    ///     "default"
    ///
    /// The first role that is set will be the one used to enforce if the
    /// batch signer is allowed.
    pub fn is_batch_signer_authorized(&self, batch: &BatchPair) -> Result<bool, IdentityError> {
        Self::is_batch_allowed(&*self.identities, batch, Some(POLICY_DEFAULT))
    }

    fn is_batch_allowed(
        identity_source: &dyn IdentitySource,
        batch: &BatchPair,
        default_policy: Option<&str>,
    ) -> Result<bool, IdentityError> {
        let policy_name: Option<String> = identity_source
            .get_role(ROLE_BATCH_TRANSACTOR)
            .and_then(|found| {
                if found.is_some() {
                    Ok(found)
                } else {
                    identity_source.get_role(ROLE_TRANSACTOR)
                }
            })?
            .map(|role| role.policy_name().to_string())
            .or_else(|| default_policy.map(ToString::to_string));

        let policy = if let Some(name) = policy_name {
            identity_source.get_policy_by_name(&name)?
        } else {
            None
        };

        let allowed = policy
            .map(|policy| Self::is_allowed(batch.header().signer_public_key(), &policy))
            .unwrap_or(true);

        if !allowed {
            debug!(
                "Batch Signer: {} is not permitted.",
                hex::encode(batch.header().signer_public_key())
            );
            return Ok(false);
        }

        Self::is_transaction_allowed(
            identity_source,
            batch.batch().transactions(),
            default_policy,
        )
    }

    /// Check the transaction signing key against the allowed transactor
    /// permissions. The roles being checked are the following, from first
    /// to last:
    ///     "transactor.transaction_signer.<TP_Name>"
    ///     "transactor.transaction_signer"
    ///     "transactor"
    ///
    /// If a default is supplied, that is used.
    ///
    /// The first role that is set will be the one used to enforce if the
    /// transaction signer is allowed.
    fn is_transaction_allowed(
        identity_source: &dyn IdentitySource,
        transactions: &[Transaction],
        default_policy: Option<&str>,
    ) -> Result<bool, IdentityError> {
        let general_txn_policy_name = identity_source
            .get_role(ROLE_TXN_TRANSACTOR)
            .and_then(|found| {
                if found.is_some() {
                    Ok(found)
                } else {
                    identity_source.get_role(ROLE_TRANSACTOR)
                }
            })?
            .map(|role| role.policy_name().to_string())
            .or_else(|| default_policy.map(ToString::to_string));

        let txn_headers = match transactions
            .iter()
            .cloned()
            .map(|txn| txn.into_pair().map(|txn_pair| txn_pair.take().1))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(txn_headers) => txn_headers,
            Err(err) => {
                debug!("Unable to deserialize transaction header: {}", err);
                return Ok(false);
            }
        };

        for txn_header in txn_headers {
            let policy_name = identity_source
                .get_role(&format!(
                    "{}.{}",
                    ROLE_TXN_TRANSACTOR,
                    txn_header.family_name()
                ))?
                .map(|role| role.policy_name().to_string());

            let policy =
                if let Some(name) = policy_name.as_ref().or(general_txn_policy_name.as_ref()) {
                    identity_source.get_policy_by_name(name)?
                } else {
                    None
                };

            if let Some(policy) = policy {
                if !Self::is_allowed(txn_header.signer_public_key(), &policy) {
                    debug!(
                        "Transaction Signer: {} is not permitted.",
                        hex::encode(txn_header.signer_public_key())
                    );
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn is_allowed(public_key: &[u8], policy: &Policy) -> bool {
        for permission in policy.permissions() {
            match permission {
                Permission::PermitKey(key) => {
                    if key == &hex::encode(public_key) || key == ANY_KEY {
                        return true;
                    }
                }
                Permission::DenyKey(key) => {
                    if key == &hex::encode(public_key) || key == ANY_KEY {
                        return false;
                    }
                }
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};
    use transact::protocol::{
        batch::BatchBuilder,
        transaction::{HashMethod, TransactionBuilder},
    };

    use crate::protocol::identity::{PolicyBuilder, Role, RoleBuilder};

    #[test]
    /// Test that if no roles are set and no default policy is set,
    /// permit all is used.
    fn allow_all_with_no_permissions() {
        let batch = create_batches(1, 1, &*new_signer())
            .into_iter()
            .nth(0)
            .unwrap();

        let permission_verifier = PermissionVerifier::new(Box::new(Arc::new(Mutex::new(
            TestIdentitySource::default(),
        ))));

        assert!(permission_verifier
            .is_batch_signer_authorized(&batch)
            .unwrap());
    }

    #[test]
    /// Test that if no roles are set, the default policy is used.
    ///     1. Set default policy to permit all. Batch should be allowed.
    ///     2. Set default policy to deny all. Batch should be rejected.
    fn default_policy_permission() {
        let batch = create_batches(1, 1, &*new_signer())
            .into_iter()
            .nth(0)
            .unwrap();

        {
            let mut on_chain_identities = TestIdentitySource::default();
            on_chain_identities.add_policy(create_policy(
                "default",
                vec![Permission::PermitKey("*".into())],
            ));
            let permission_verifier = on_chain_verifier(on_chain_identities);
            assert!(permission_verifier
                .is_batch_signer_authorized(&batch)
                .unwrap());
        }

        {
            let mut on_chain_identities = TestIdentitySource::default();
            on_chain_identities.add_policy(create_policy(
                "default",
                vec![Permission::DenyKey("*".into())],
            ));
            let permission_verifier = on_chain_verifier(on_chain_identities);
            assert!(!permission_verifier
                .is_batch_signer_authorized(&batch)
                .unwrap());
        }
    }

    #[test]
    /// Test that role: "transactor" is checked properly.
    ///     1. Set policy to permit signing key. Batch should be allowed.
    ///     2. Set policy to permit some other key. Batch should be rejected.
    fn transactor_role() {
        let signer = new_signer();
        let pub_key = signer.public_key().expect("Failed to get pub key");
        let batch = create_batches(1, 1, &*signer).into_iter().nth(0).unwrap();

        {
            let mut on_chain_identities = TestIdentitySource::default();
            on_chain_identities.add_policy(create_policy(
                "policy1",
                vec![Permission::PermitKey(pub_key.as_hex())],
            ));
            on_chain_identities.add_role(create_role("transactor", "policy1"));

            let permission_verifier = on_chain_verifier(on_chain_identities);
            assert!(permission_verifier
                .is_batch_signer_authorized(&batch)
                .unwrap());
        }
        {
            let mut on_chain_identities = TestIdentitySource::default();
            on_chain_identities.add_policy(create_policy(
                "policy1",
                vec![Permission::DenyKey(pub_key.as_hex())],
            ));
            on_chain_identities.add_role(create_role("transactor", "policy1"));

            let permission_verifier = on_chain_verifier(on_chain_identities);
            assert!(!permission_verifier
                .is_batch_signer_authorized(&batch)
                .unwrap());
        }
    }

    #[test]
    /// Test that role: "transactor.batch_signer" is checked properly.
    ///     1. Set policy to permit signing key. Batch should be allowed.
    ///     2. Set policy to permit some other key. Batch should be rejected.
    fn transactor_batch_signer_role() {
        let signer = new_signer();
        let pub_key = signer.public_key().expect("Failed to get pub key");
        let batch = create_batches(1, 1, &*signer).into_iter().nth(0).unwrap();

        {
            let mut on_chain_identities = TestIdentitySource::default();
            on_chain_identities.add_policy(create_policy(
                "policy1",
                vec![Permission::PermitKey(pub_key.as_hex())],
            ));
            on_chain_identities.add_role(create_role("transactor.batch_signer", "policy1"));

            let permission_verifier = on_chain_verifier(on_chain_identities);
            assert!(permission_verifier
                .is_batch_signer_authorized(&batch)
                .unwrap());
        }
        {
            let mut on_chain_identities = TestIdentitySource::default();
            on_chain_identities.add_policy(create_policy(
                "policy1",
                vec![Permission::DenyKey(pub_key.as_hex())],
            ));
            on_chain_identities.add_role(create_role("transactor.batch_signer", "policy1"));

            let permission_verifier = on_chain_verifier(on_chain_identities);
            assert!(!permission_verifier
                .is_batch_signer_authorized(&batch)
                .unwrap());
        }
    }

    #[test]
    /// Test that role: "transactor.transaction_signer" is checked properly.
    ///     1. Set policy to permit signing key. Batch should be allowed.
    ///     2. Set policy to permit some other key. Batch should be rejected.
    fn transactor_transaction_signer_role() {
        let signer = new_signer();
        let pub_key = signer.public_key().expect("Failed to get pub key");
        let batch = create_batches(1, 1, &*signer).into_iter().nth(0).unwrap();

        {
            let mut on_chain_identities = TestIdentitySource::default();
            on_chain_identities.add_policy(create_policy(
                "policy1",
                vec![Permission::PermitKey(pub_key.as_hex())],
            ));
            on_chain_identities.add_role(create_role("transactor.transaction_signer", "policy1"));

            let permission_verifier = on_chain_verifier(on_chain_identities);
            assert!(permission_verifier
                .is_batch_signer_authorized(&batch)
                .unwrap());
        }
        {
            let mut on_chain_identities = TestIdentitySource::default();
            on_chain_identities.add_policy(create_policy(
                "policy1",
                vec![Permission::PermitKey("other".to_string())],
            ));
            on_chain_identities.add_role(create_role("transactor.transaction_signer", "policy1"));

            let permission_verifier = on_chain_verifier(on_chain_identities);
            assert!(!permission_verifier
                .is_batch_signer_authorized(&batch)
                .unwrap());
        }
    }

    #[test]
    /// Test that role: "transactor.transaction_signer.intkey" is checked properly.
    ///     1. Set policy to permit signing key. Batch should be allowed.
    ///     2. Set policy to permit some other key. Batch should be rejected.
    fn transactor_transaction_signer_transaction_family() {
        let signer = new_signer();
        let pub_key = signer.public_key().expect("Failed to get pub key");
        let batch = create_batches(1, 1, &*signer).into_iter().nth(0).unwrap();

        {
            let mut on_chain_identities = TestIdentitySource::default();
            on_chain_identities.add_policy(create_policy(
                "policy1",
                vec![Permission::PermitKey(pub_key.as_hex())],
            ));
            on_chain_identities.add_role(create_role(
                "transactor.transaction_signer.intkey",
                "policy1",
            ));

            let permission_verifier = on_chain_verifier(on_chain_identities);
            assert!(permission_verifier
                .is_batch_signer_authorized(&batch)
                .unwrap());
        }
        {
            let mut on_chain_identities = TestIdentitySource::default();
            on_chain_identities.add_policy(create_policy(
                "policy1",
                vec![Permission::PermitKey("other".to_string())],
            ));
            on_chain_identities.add_role(create_role(
                "transactor.transaction_signer.intkey",
                "policy1",
            ));

            let permission_verifier = on_chain_verifier(on_chain_identities);
            assert!(!permission_verifier
                .is_batch_signer_authorized(&batch)
                .unwrap());
        }
    }

    fn on_chain_verifier(identity_source: TestIdentitySource) -> PermissionVerifier {
        PermissionVerifier::new(Box::new(Arc::new(Mutex::new(identity_source))))
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        let key = context.new_random_private_key();
        context.new_signer(key)
    }

    fn create_batches(count: u8, txns_per_batch: u8, signer: &dyn Signer) -> Vec<BatchPair> {
        (0..count)
            .map(|i| {
                let txns = (0..txns_per_batch)
                    .map(|j| {
                        TransactionBuilder::new()
                            .with_family_name("intkey".into())
                            .with_family_version("1.0".into())
                            .with_inputs(vec![])
                            .with_outputs(vec![])
                            .with_payload_hash_method(HashMethod::SHA512)
                            .with_payload(vec![])
                            .with_nonce(vec![i, j])
                            .build(signer)
                    })
                    .collect::<Result<_, _>>()
                    .expect("Failed to build transactions");

                BatchBuilder::new()
                    .with_transactions(txns)
                    .build_pair(signer)
            })
            .collect::<Result<_, _>>()
            .expect("Failed to build batches")
    }

    fn create_policy(name: &str, permissions: Vec<Permission>) -> Policy {
        PolicyBuilder::new()
            .with_name(name.into())
            .with_permissions(permissions)
            .build()
            .expect("Failed to build policy")
    }

    fn create_role(name: &str, policy_name: &str) -> Role {
        RoleBuilder::new()
            .with_name(name.into())
            .with_policy_name(policy_name.into())
            .build()
            .expect("Failed to build role")
    }

    #[derive(Default)]
    struct TestIdentitySource {
        policies: HashMap<String, Policy>,
        roles: HashMap<String, Role>,
    }

    impl TestIdentitySource {
        fn add_policy(&mut self, policy: Policy) {
            self.policies.insert(policy.name().into(), policy);
        }

        fn add_role(&mut self, role: Role) {
            self.roles.insert(role.name().into(), role);
        }
    }

    impl IdentitySource for Arc<Mutex<TestIdentitySource>> {
        fn get_role(&self, name: &str) -> Result<Option<Role>, IdentityError> {
            Ok(self
                .lock()
                .expect("lock was poisoned")
                .roles
                .get(name)
                .cloned())
        }

        fn get_policy_by_name(&self, name: &str) -> Result<Option<Policy>, IdentityError> {
            Ok(self
                .lock()
                .expect("lock was poisoned")
                .policies
                .get(name)
                .cloned())
        }
    }
}
