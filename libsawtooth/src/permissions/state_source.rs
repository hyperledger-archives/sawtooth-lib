/*
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

use crate::protocol::identity::{Policy, Role};
use crate::state::identity_view::IdentityView;

use super::{IdentityError, IdentitySource};

impl IdentitySource for IdentityView {
    fn get_role(&self, name: &str) -> Result<Option<Role>, IdentityError> {
        IdentityView::get_role(self, name).map_err(|err| {
            IdentityError::ReadError(format!("unable to read role from state: {:?}", err))
        })
    }

    fn get_policy_by_name(&self, name: &str) -> Result<Option<Policy>, IdentityError> {
        IdentityView::get_policy(self, name).map_err(|err| {
            IdentityError::ReadError(format!("unable to read policy from state: {:?}", err))
        })
    }
}
