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

//! The permissions modules defines the representation of roles and policies for the Sawtooth
//! system.  Policies are a named set of permissions.  Roles are associations between a named group
//! of activities and a policy.

mod error;
pub mod verifier;

pub use error::IdentityError;

/// A permission either allows or denies a given public key.
#[derive(Clone)]
pub enum Permission {
    /// Allow the specified public key.
    PermitKey(String),
    /// Deny the specified public key.
    DenyKey(String),
}

/// A Policy is a named set of permissions.
#[derive(Clone)]
pub struct Policy {
    name: String,
    permissions: Vec<Permission>,
}

impl Policy {
    /// Construct a new Policy with the given name and set of permissions.
    pub fn new<S: Into<String>>(name: S, permissions: Vec<Permission>) -> Self {
        Policy {
            name: name.into(),
            permissions,
        }
    }

    pub fn permissions(&self) -> &[Permission] {
        &self.permissions
    }
}

/// A role is link between a set of activities and a Policy.
#[derive(Clone)]
pub struct Role {
    name: String,
    policy_name: String,
}

impl Role {
    /// Construct a new Role with a given name and associated policy.
    pub fn new<N: Into<String>, P: Into<String>>(name: N, policy_name: P) -> Self {
        Role {
            name: name.into(),
            policy_name: policy_name.into(),
        }
    }
}

/// A source of Roles and Policies.
pub trait IdentitySource: Sync + Send {
    /// Get a Role by its name, if available.
    ///
    /// # Errors
    ///
    /// Return an error if the underlying implementation is unable to complete the request.
    fn get_role(&self, name: &str) -> Result<Option<Role>, IdentityError>;

    /// Return a Policy by its name, if available.
    ///
    /// # Errors
    ///
    /// Return an error if the underlying implementation is unable to complete the request.
    fn get_policy_by_name(&self, name: &str) -> Result<Option<Policy>, IdentityError>;

    /// Return the Policy for a given Role, if available.
    ///
    /// # Errors
    ///
    /// Return an error if the underlying implementation is unable to complete the request.
    fn get_policy(&self, role: &Role) -> Result<Option<Policy>, IdentityError> {
        self.get_policy_by_name(&role.policy_name)
    }
}
