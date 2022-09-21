/*
 * Copyright 2018-2020 Cargill Incorporated
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

//! Sawtooth identity protocol

use protobuf::Message;

use crate::protos::{
    identity::{
        Policy as PolicyProto, PolicyList, Policy_Entry, Policy_EntryType, Role as RoleProto,
        RoleList,
    },
    FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

use super::ProtocolBuildError;

/// A named set of [`Permissions`](enum.Permission.html)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    name: String,
    permissions: Vec<Permission>,
}

impl Policy {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn permissions(&self) -> &[Permission] {
        &self.permissions
    }
}

impl FromBytes<Policy> for Policy {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Policy from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<Policy> for PolicyProto {
    fn from_native(policy: Policy) -> Result<Self, ProtoConversionError> {
        let permissions = policy
            .permissions
            .into_iter()
            .map(FromNative::from_native)
            .collect::<Result<_, _>>()?;

        let mut policy_proto = PolicyProto::new();
        policy_proto.set_name(policy.name);
        policy_proto.set_entries(permissions);

        Ok(policy_proto)
    }
}

impl FromProto<PolicyProto> for Policy {
    fn from_proto(policy: PolicyProto) -> Result<Self, ProtoConversionError> {
        let permissions = policy
            .entries
            .into_iter()
            .map(FromProto::from_proto)
            .collect::<Result<_, _>>()?;

        PolicyBuilder::new()
            .with_name(policy.name)
            .with_permissions(permissions)
            .build()
            .map_err(|err| {
                ProtoConversionError::DeserializationError(format!(
                    "Unable to get Policy from proto: {}",
                    err
                ))
            })
    }
}

impl IntoBytes for Policy {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError("Unable to get bytes from Policy".to_string())
        })
    }
}

impl IntoNative<Policy> for PolicyProto {}
impl IntoProto<PolicyProto> for Policy {}

impl FromBytes<Vec<Policy>> for Vec<Policy> {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Vec<Policy> from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<Vec<Policy>> for PolicyList {
    fn from_native(policies: Vec<Policy>) -> Result<Self, ProtoConversionError> {
        let policies = policies
            .into_iter()
            .map(FromNative::from_native)
            .collect::<Result<_, _>>()?;

        let mut policy_list_proto = PolicyList::new();
        policy_list_proto.set_policies(policies);

        Ok(policy_list_proto)
    }
}

impl FromProto<PolicyList> for Vec<Policy> {
    fn from_proto(policy_list: PolicyList) -> Result<Self, ProtoConversionError> {
        policy_list
            .policies
            .into_iter()
            .map(FromProto::from_proto)
            .collect()
    }
}

impl IntoBytes for Vec<Policy> {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from Vec<Policy>".to_string(),
            )
        })
    }
}

impl IntoNative<Vec<Policy>> for PolicyList {}
impl IntoProto<PolicyList> for Vec<Policy> {}

/// Represents a permitted or denied public key
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    PermitKey(String),
    DenyKey(String),
}

impl FromBytes<Permission> for Permission {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Permission from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<Permission> for Policy_Entry {
    fn from_native(entry: Permission) -> Result<Self, ProtoConversionError> {
        let (entry_type, key) = match entry {
            Permission::PermitKey(key) => (Policy_EntryType::PERMIT_KEY, key),
            Permission::DenyKey(key) => (Policy_EntryType::DENY_KEY, key),
        };

        let mut entry_proto = Policy_Entry::new();
        entry_proto.set_field_type(entry_type);
        entry_proto.set_key(key);

        Ok(entry_proto)
    }
}

impl FromProto<Policy_Entry> for Permission {
    fn from_proto(entry: Policy_Entry) -> Result<Self, ProtoConversionError> {
        match entry.field_type {
            Policy_EntryType::PERMIT_KEY => Ok(Self::PermitKey(entry.key)),
            Policy_EntryType::DENY_KEY => Ok(Self::DenyKey(entry.key)),
            _ => Err(ProtoConversionError::InvalidTypeError(
                "Cannot convert Policy_EntryType with type unset".into(),
            )),
        }
    }
}

impl IntoBytes for Permission {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from Permission".to_string(),
            )
        })
    }
}

impl IntoNative<Permission> for Policy_Entry {}
impl IntoProto<Policy_Entry> for Permission {}

/// Builder for [`Policy`](struct.Policy.html)
#[derive(Default, Clone)]
pub struct PolicyBuilder {
    name: Option<String>,
    permissions: Option<Vec<Permission>>,
}

impl PolicyBuilder {
    /// Creates a new `PolicyBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the name of the policy to be built
    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    /// Sets the permissions of the policy to be built
    pub fn with_permissions(mut self, permissions: Vec<Permission>) -> Self {
        self.permissions = Some(permissions);
        self
    }

    /// Builds the `Policy`
    ///
    /// # Errors
    ///
    /// Returns an error if the name or permissions are not set
    pub fn build(self) -> Result<Policy, ProtocolBuildError> {
        let name = self.name.ok_or_else(|| {
            ProtocolBuildError::MissingField("'name' field is required".to_string())
        })?;
        let permissions = self.permissions.ok_or_else(|| {
            ProtocolBuildError::MissingField("'permissions' field is required".to_string())
        })?;

        Ok(Policy { name, permissions })
    }
}

/// A link between a set of activities and a [`Policy`](struct.Policy.html)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    name: String,
    policy_name: String,
}

impl Role {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn policy_name(&self) -> &str {
        &self.policy_name
    }
}

impl FromBytes<Role> for Role {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Role from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<Role> for RoleProto {
    fn from_native(role: Role) -> Result<Self, ProtoConversionError> {
        let mut role_proto = RoleProto::new();

        role_proto.set_name(role.name);
        role_proto.set_policy_name(role.policy_name);

        Ok(role_proto)
    }
}

impl FromProto<RoleProto> for Role {
    fn from_proto(role: RoleProto) -> Result<Self, ProtoConversionError> {
        RoleBuilder::new()
            .with_name(role.name)
            .with_policy_name(role.policy_name)
            .build()
            .map_err(|err| {
                ProtoConversionError::DeserializationError(format!(
                    "Unable to get Role from proto: {}",
                    err
                ))
            })
    }
}

impl IntoBytes for Role {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError("Unable to get bytes from Role".to_string())
        })
    }
}

impl IntoNative<Role> for RoleProto {}
impl IntoProto<RoleProto> for Role {}

impl FromBytes<Vec<Role>> for Vec<Role> {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Vec<Role> from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<Vec<Role>> for RoleList {
    fn from_native(roles: Vec<Role>) -> Result<Self, ProtoConversionError> {
        let roles = roles
            .into_iter()
            .map(FromNative::from_native)
            .collect::<Result<_, _>>()?;

        let mut role_list_proto = RoleList::new();
        role_list_proto.set_roles(roles);

        Ok(role_list_proto)
    }
}

impl FromProto<RoleList> for Vec<Role> {
    fn from_proto(role_list: RoleList) -> Result<Self, ProtoConversionError> {
        role_list
            .roles
            .into_iter()
            .map(FromProto::from_proto)
            .collect()
    }
}

impl IntoBytes for Vec<Role> {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from Vec<Role>".to_string(),
            )
        })
    }
}

impl IntoNative<Vec<Role>> for RoleList {}
impl IntoProto<RoleList> for Vec<Role> {}

/// Builder for [`Role`](struct.Role.html)
#[derive(Default, Clone)]
pub struct RoleBuilder {
    name: Option<String>,
    policy_name: Option<String>,
}

impl RoleBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn with_policy_name(mut self, policy_name: String) -> Self {
        self.policy_name = Some(policy_name);
        self
    }

    pub fn build(self) -> Result<Role, ProtocolBuildError> {
        let name = self.name.ok_or_else(|| {
            ProtocolBuildError::MissingField("'name' field is required".to_string())
        })?;
        let policy_name = self.policy_name.ok_or_else(|| {
            ProtocolBuildError::MissingField("'policy_name' field is required".to_string())
        })?;

        Ok(Role { name, policy_name })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY_NAME: &str = "policy_name";
    const ROLE_NAME: &str = "role_name";
    const KEY1: &str = "012345";
    const KEY2: &str = "abcdef";

    /// Verify that the `PolicyBuilder` can be successfully used in a chain.
    ///
    /// 1. Construct the policy using a builder chain
    /// 2. Verify that the resulting policy is correct
    #[test]
    fn policy_builder_chain() {
        let permissions = vec![
            Permission::PermitKey(KEY1.into()),
            Permission::DenyKey(KEY2.into()),
        ];

        let policy = PolicyBuilder::new()
            .with_name(POLICY_NAME.into())
            .with_permissions(permissions.clone())
            .build()
            .expect("Failed to build policy");

        assert_eq!(policy.name(), POLICY_NAME);
        assert_eq!(policy.permissions(), permissions.as_slice());
    }

    /// Verify that the `PolicyBuilder` can be successfully used with separate calls to its methods.
    ///
    /// 1. Construct the policy using separate method calls and assignments of the builder
    /// 2. Verify that the resulting policy is correct
    #[test]
    fn policy_builder_separate() {
        let permissions = vec![
            Permission::PermitKey(KEY1.into()),
            Permission::DenyKey(KEY2.into()),
        ];

        let mut builder = PolicyBuilder::new();
        builder = builder.with_name(POLICY_NAME.into());
        builder = builder.with_permissions(permissions.clone());

        let policy = builder.build().expect("Failed to build policy");

        assert_eq!(policy.name(), POLICY_NAME);
        assert_eq!(policy.permissions(), permissions.as_slice());
    }

    /// Verify that the `PolicyBuilder` fails when any of the required fields are missing.
    ///
    /// 1. Attempt to build a policy without setting `name` and verify that it fails.
    /// 2. Attempt to build a policy without setting `permissions` and verify that it fails.
    #[test]
    fn policy_builder_missing_fields() {
        match PolicyBuilder::new()
            .with_permissions(vec![
                Permission::PermitKey(KEY1.into()),
                Permission::DenyKey(KEY2.into()),
            ])
            .build()
        {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }

        match PolicyBuilder::new().with_name(POLICY_NAME.into()).build() {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }
    }

    /// Verify that the `RoleBuilder` can be successfully used in a chain.
    ///
    /// 1. Construct the role using a builder chain
    /// 2. Verify that the resulting role is correct
    #[test]
    fn role_builder_chain() {
        let role = RoleBuilder::new()
            .with_name(ROLE_NAME.into())
            .with_policy_name(POLICY_NAME.into())
            .build()
            .expect("Failed to build role");

        assert_eq!(role.name(), ROLE_NAME);
        assert_eq!(role.policy_name(), POLICY_NAME);
    }

    /// Verify that the `RoleBuilder` can be successfully used with separate calls to its methods.
    ///
    /// 1. Construct the role using separate method calls and assignments of the builder
    /// 2. Verify that the resulting role is correct
    #[test]
    fn role_builder_separate() {
        let mut builder = RoleBuilder::new();
        builder = builder.with_name(ROLE_NAME.into());
        builder = builder.with_policy_name(POLICY_NAME.into());

        let role = builder.build().expect("Failed to build role");

        assert_eq!(role.name(), ROLE_NAME);
        assert_eq!(role.policy_name(), POLICY_NAME);
    }

    /// Verify that the `RoleBuilder` fails when any of the required fields are missing.
    ///
    /// 1. Attempt to build a role without setting `name` and verify that it fails.
    /// 2. Attempt to build a role without setting `permissions` and verify that it fails.
    #[test]
    fn role_builder_missing_fields() {
        match RoleBuilder::new()
            .with_policy_name(POLICY_NAME.into())
            .build()
        {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }

        match RoleBuilder::new().with_name(ROLE_NAME.into()).build() {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }
    }
}
