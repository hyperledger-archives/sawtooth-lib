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

use crate::{
    hashlib::sha256_digest_str,
    protocol::identity::{Policy, Role},
    protos::{FromBytes, ProtoConversionError},
    state::error::StateDatabaseError,
    state::StateReader,
};
use std::iter::repeat;

/// The namespace for storage
const POLICY_NS: &str = "00001d00";
const ROLE_NS: &str = "00001d01";
const MAX_KEY_PARTS: usize = 4;

#[derive(Debug)]
pub enum IdentityViewError {
    StateDatabaseError(StateDatabaseError),
    EncodingError(ProtoConversionError),
}

impl From<StateDatabaseError> for IdentityViewError {
    fn from(err: StateDatabaseError) -> Self {
        IdentityViewError::StateDatabaseError(err)
    }
}

impl From<ProtoConversionError> for IdentityViewError {
    fn from(err: ProtoConversionError) -> Self {
        IdentityViewError::EncodingError(err)
    }
}

/// Provides a view into global state which translates Role and Policy names
/// into the corresponding addresses, and returns the deserialized values from
/// state.
pub struct IdentityView {
    state_reader: Box<dyn StateReader>,
}

impl IdentityView {
    /// Creates an IdentityView from a given StateReader.
    pub fn new(state_reader: Box<dyn StateReader>) -> Self {
        IdentityView { state_reader }
    }

    /// Returns a single Role by name, if it exists.
    pub fn get_role(&self, name: &str) -> Result<Option<Role>, IdentityViewError> {
        self.get_identity_value(name, &role_address(name))
    }

    /// Returns all of the Roles under the Identity namespace
    pub fn get_roles(&self) -> Result<Vec<Role>, IdentityViewError> {
        self.get_identity_value_list(ROLE_NS)
    }

    /// Returns a single Policy by name, if it exists.
    pub fn get_policy(&self, name: &str) -> Result<Option<Policy>, IdentityViewError> {
        self.get_identity_value(name, &policy_address(name))
    }

    /// Returns all of the Policies under the Identity namespace
    pub fn get_policies(&self) -> Result<Vec<Policy>, IdentityViewError> {
        self.get_identity_value_list(POLICY_NS)
    }

    fn get_identity_value<I>(
        &self,
        name: &str,
        address: &str,
    ) -> Result<Option<I>, IdentityViewError>
    where
        I: Named,
        Vec<I>: FromBytes<Vec<I>>,
    {
        if !self.state_reader.contains(&address)? {
            return Ok(None);
        }

        Ok(self
            .state_reader
            .get(&address)?
            .map(|bytes| Vec::from_bytes(&bytes))
            .transpose()?
            .and_then(|list| list.into_iter().find(|item| item.name() == name)))
    }

    fn get_identity_value_list<I>(&self, prefix: &str) -> Result<Vec<I>, IdentityViewError>
    where
        I: Named,
        Vec<I>: FromBytes<Vec<I>>,
    {
        let mut res = Vec::new();
        for state_value in self.state_reader.leaves(Some(prefix))? {
            let (_, bytes) = state_value?;
            res.append(&mut Vec::from_bytes(&bytes)?);
        }
        res.sort_by(|a, b| a.name().cmp(b.name()));
        Ok(res)
    }
}

impl From<Box<dyn StateReader>> for IdentityView {
    fn from(state_reader: Box<dyn StateReader>) -> Self {
        IdentityView::new(state_reader)
    }
}

trait Named: Clone {
    fn name(&self) -> &str;
}

impl Named for Role {
    fn name(&self) -> &str {
        self.name()
    }
}

impl Named for Policy {
    fn name(&self) -> &str {
        self.name()
    }
}

fn role_address(name: &str) -> String {
    let mut address = String::new();
    address.push_str(ROLE_NS);
    address.push_str(
        &name
            .splitn(MAX_KEY_PARTS, '.')
            .chain(repeat(""))
            .enumerate()
            .map(|(i, part)| short_hash(part, if i == 0 { 14 } else { 16 }))
            .take(MAX_KEY_PARTS)
            .collect::<Vec<_>>()
            .join(""),
    );

    address
}

fn policy_address(name: &str) -> String {
    let mut address = String::new();
    address.push_str(POLICY_NS);
    address.push_str(&short_hash(name, 62));
    address
}

fn short_hash(s: &str, length: usize) -> String {
    sha256_digest_str(s)[..length].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use crate::protocol::identity::{Permission, Policy, PolicyBuilder, Role, RoleBuilder};
    use crate::protos::IntoBytes;

    #[test]
    fn addressing() {
        // These addresses were generated using the legacy python code
        assert_eq!(
            "00001d01e0d7826133ad07e3b0c44298fc1c14e3b0c44298fc1c14e3b0c44298fc1c14",
            &role_address("MY_ROLE")
        );
        assert_eq!(
            "00001d00237e86847d59b61cd902a533a1c9701327ed992dfe7ec1996ffc84bc8f0b4e",
            &policy_address("MY_POLICY")
        );
    }

    #[test]
    fn no_roles() {
        let mock_reader = MockStateReader::new(vec![]);
        let identity_view = IdentityView::new(Box::new(mock_reader));

        assert_eq!(None, identity_view.get_role("my_role").unwrap());
        let expect_empty: Vec<Role> = vec![];
        assert_eq!(expect_empty, identity_view.get_roles().unwrap());
    }

    #[test]
    fn get_role_by_name() {
        let mock_reader = MockStateReader::new(vec![
            role_entry("role1", "some_policy"),
            role_entry("role2", "some_other_policy"),
        ]);
        let identity_view = IdentityView::new(Box::new(mock_reader));

        assert_eq!(
            Some(role("role2", "some_other_policy")),
            identity_view.get_role("role2").unwrap()
        );
    }

    #[test]
    fn get_roles() {
        let mock_reader = MockStateReader::new(vec![
            role_entry("role1", "some_policy"),
            role_entry("role2", "some_other_policy"),
        ]);
        let identity_view = IdentityView::new(Box::new(mock_reader));

        assert_eq!(
            vec![
                role("role1", "some_policy"),
                role("role2", "some_other_policy"),
            ],
            identity_view.get_roles().unwrap()
        );
    }

    #[test]
    fn no_policies() {
        let mock_reader = MockStateReader::new(vec![]);
        let identity_view = IdentityView::new(Box::new(mock_reader));

        assert_eq!(None, identity_view.get_policy("my_policy").unwrap());
        let expect_empty: Vec<Policy> = vec![];
        assert_eq!(expect_empty, identity_view.get_policies().unwrap());
    }

    #[test]
    fn get_policy_by_name() {
        let mock_reader = MockStateReader::new(vec![
            policy_entry("policy1", &["some_pubkey"]),
            policy_entry("policy2", &["some_other_pubkey"]),
        ]);
        let identity_view = IdentityView::new(Box::new(mock_reader));

        assert_eq!(
            Some(policy("policy2", &["some_other_pubkey"])),
            identity_view.get_policy("policy2").unwrap()
        );
    }

    #[test]
    fn get_policies() {
        let mock_reader = MockStateReader::new(vec![
            policy_entry("policy1", &["some_pubkey"]),
            policy_entry("policy2", &["some_other_pubkey"]),
        ]);
        let identity_view = IdentityView::new(Box::new(mock_reader));

        assert_eq!(
            vec![
                policy("policy1", &["some_pubkey"]),
                policy("policy2", &["some_other_pubkey"]),
            ],
            identity_view.get_policies().unwrap()
        );
    }

    fn role(name: &str, policy_name: &str) -> Role {
        RoleBuilder::new()
            .with_name(name.into())
            .with_policy_name(policy_name.into())
            .build()
            .expect("Unable to build role")
    }

    fn role_entry(name: &str, policy_name: &str) -> (String, Vec<u8>) {
        (
            role_address(name),
            vec![role(name, policy_name)]
                .into_bytes()
                .expect("Unable to serialize roles"),
        )
    }

    fn policy(name: &str, permits: &[&str]) -> Policy {
        PolicyBuilder::new()
            .with_name(name.into())
            .with_permissions(
                permits
                    .into_iter()
                    .map(|key| Permission::PermitKey(key.to_owned().into()))
                    .collect(),
            )
            .build()
            .expect("Unable to build policy")
    }

    fn policy_entry(name: &str, permits: &[&str]) -> (String, Vec<u8>) {
        (
            policy_address(name),
            vec![policy(name, permits)]
                .into_bytes()
                .expect("Unable to serialize policies"),
        )
    }

    struct MockStateReader {
        state: HashMap<String, Vec<u8>>,
    }

    impl MockStateReader {
        fn new(values: Vec<(String, Vec<u8>)>) -> Self {
            MockStateReader {
                state: values.into_iter().collect(),
            }
        }
    }

    impl StateReader for MockStateReader {
        fn get(&self, address: &str) -> Result<Option<Vec<u8>>, StateDatabaseError> {
            Ok(self.state.get(address).cloned())
        }

        fn contains(&self, address: &str) -> Result<bool, StateDatabaseError> {
            Ok(self.state.contains_key(address))
        }

        fn leaves(
            &self,
            prefix: Option<&str>,
        ) -> Result<
            Box<dyn Iterator<Item = Result<(String, Vec<u8>), StateDatabaseError>>>,
            StateDatabaseError,
        > {
            let iterable: Vec<_> = self
                .state
                .iter()
                .filter(|(key, _)| key.starts_with(prefix.unwrap_or("")))
                .map(|(key, value)| Ok((key.clone().to_string(), value.clone())))
                .collect();

            Ok(Box::new(iterable.into_iter()))
        }
    }
}
