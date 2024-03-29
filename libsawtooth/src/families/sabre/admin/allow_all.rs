// Copyright 2021 Cargill Incorporated
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

//! An implementations of `AdminPermission` that always returns true

use crate::families::sabre::state::SabreState;
use crate::transact::handler::ApplyError;

use super::AdminPermission;

#[derive(Default)]
pub struct AllowAllAdminPermission;

impl AdminPermission for AllowAllAdminPermission {
    fn is_admin(&self, _signer: &str, _state: &mut SabreState) -> Result<bool, ApplyError> {
        Ok(true)
    }
}
