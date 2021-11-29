// Copyright 2018-2020 Cargill Incorporated
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

//! Default `BatchInjectorFactory` for the block publisher

use cylinder::Signer;
use log::warn;

use crate::journal::publisher::BlockPublisherError;
use crate::protocol::block::BlockPair;
use crate::state::{settings_view::SettingsView, state_view_factory::StateViewFactory};

use super::{block_info::BlockInfoInjector, BatchInjector, BatchInjectorFactory};

/// The name of the setting where the batch injector configuration is stored
const BATCH_INJECTORS_SETTING: &str = "sawtooth.validator.batch_injectors";
/// The name of the `BlockInfoInjector` as it appears in the batch injectors setting
const BLOCK_INFO_INJECTOR_NAME: &str = "block_info";

/// The default `BatchInjectorFactory` used by the block publisher
pub struct DefaultBatchInjectorFactory {
    signer: Box<dyn Signer>,
    state_view_factory: StateViewFactory,
}

impl DefaultBatchInjectorFactory {
    /// Creates a new `DefaultBatchInjectorFactory`
    #[allow(dead_code)]
    pub fn new(signer: Box<dyn Signer>, state_view_factory: StateViewFactory) -> Self {
        Self {
            signer,
            state_view_factory,
        }
    }
}

impl BatchInjectorFactory for DefaultBatchInjectorFactory {
    fn create_injectors(
        &self,
        previous_block: &BlockPair,
    ) -> Result<Vec<Box<dyn BatchInjector>>, BlockPublisherError> {
        let settings_view = self
            .state_view_factory
            .create_view::<SettingsView>(previous_block.header().state_root_hash())?;
        Ok(
            match settings_view.get_setting_str(BATCH_INJECTORS_SETTING, None)? {
                Some(injector_names) => injector_names
                    .split(',')
                    .filter_map(|injector_name| {
                        if injector_name == BLOCK_INFO_INJECTOR_NAME {
                            Some(Box::new(BlockInfoInjector::new(self.signer.clone()))
                                as Box<dyn BatchInjector>)
                        } else {
                            warn!("Ignoring unknown batch injector: {}", injector_name);
                            None
                        }
                    })
                    .collect(),
                None => vec![],
            },
        )
    }
}
