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

//! Sawtooth settings protocol

use protobuf::Message;

use crate::protos::{
    setting::{Setting as SettingProto, Setting_Entry},
    FromBytes, FromNative, FromProto, IntoBytes, IntoNative, IntoProto, ProtoConversionError,
};

use super::ProtocolBuildError;

/// An on-chain configuration key/value pair
#[derive(Debug, Clone)]
pub struct Setting {
    key: String,
    value: String,
}

impl Setting {
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl FromBytes<Setting> for Setting {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Setting from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<Setting> for Setting_Entry {
    fn from_native(setting: Setting) -> Result<Self, ProtoConversionError> {
        let mut setting_entry_proto = Setting_Entry::new();
        setting_entry_proto.set_key(setting.key);
        setting_entry_proto.set_value(setting.value);

        Ok(setting_entry_proto)
    }
}

impl FromProto<Setting_Entry> for Setting {
    fn from_proto(setting: Setting_Entry) -> Result<Self, ProtoConversionError> {
        SettingBuilder::new()
            .with_key(setting.key)
            .with_value(setting.value)
            .build()
            .map_err(|err| {
                ProtoConversionError::DeserializationError(format!(
                    "Unable to get Setting from proto: {}",
                    err
                ))
            })
    }
}

impl IntoBytes for Setting {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError("Unable to get bytes from Setting".to_string())
        })
    }
}

impl IntoNative<Setting> for Setting_Entry {}
impl IntoProto<Setting_Entry> for Setting {}

impl FromBytes<Vec<Setting>> for Vec<Setting> {
    fn from_bytes(bytes: &[u8]) -> Result<Self, ProtoConversionError> {
        Message::parse_from_bytes(bytes)
            .map_err(|_| {
                ProtoConversionError::SerializationError(
                    "Unable to get Vec<Setting> from bytes".to_string(),
                )
            })
            .and_then(Self::from_proto)
    }
}

impl FromNative<Vec<Setting>> for SettingProto {
    fn from_native(settings: Vec<Setting>) -> Result<Self, ProtoConversionError> {
        let entries = settings
            .into_iter()
            .map(FromNative::from_native)
            .collect::<Result<_, _>>()?;

        let mut setting_proto = SettingProto::new();
        setting_proto.set_entries(entries);

        Ok(setting_proto)
    }
}

impl FromProto<SettingProto> for Vec<Setting> {
    fn from_proto(setting: SettingProto) -> Result<Self, ProtoConversionError> {
        setting
            .entries
            .into_iter()
            .map(FromProto::from_proto)
            .collect()
    }
}

impl IntoBytes for Vec<Setting> {
    fn into_bytes(self) -> Result<Vec<u8>, ProtoConversionError> {
        self.into_proto()?.write_to_bytes().map_err(|_| {
            ProtoConversionError::SerializationError(
                "Unable to get bytes from Vec<Setting>".to_string(),
            )
        })
    }
}

impl IntoNative<Vec<Setting>> for SettingProto {}
impl IntoProto<SettingProto> for Vec<Setting> {}

/// Builder for [`Setting`](struct.Setting.html)
#[derive(Default, Clone)]
pub struct SettingBuilder {
    key: Option<String>,
    value: Option<String>,
}

impl SettingBuilder {
    /// Creates a new `SettingBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the key for the setting to be built
    pub fn with_key(mut self, key: String) -> Self {
        self.key = Some(key);
        self
    }

    /// Sets the value for the setting to be built
    pub fn with_value(mut self, value: String) -> Self {
        self.value = Some(value);
        self
    }

    /// Builds the `Setting`
    ///
    /// # Errors
    ///
    /// Returns an error if the key or value are not set
    pub fn build(self) -> Result<Setting, ProtocolBuildError> {
        let key = self.key.ok_or_else(|| {
            ProtocolBuildError::MissingField("'key' field is required".to_string())
        })?;
        let value = self.value.ok_or_else(|| {
            ProtocolBuildError::MissingField("'value' field is required".to_string())
        })?;

        Ok(Setting { key, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "key";
    const VALUE: &str = "value";

    /// Verify that the `SettingBuilder` can be successfully used in a chain.
    ///
    /// 1. Construct the setting using a builder chain
    /// 2. Verify that the resulting setting is correct
    #[test]
    fn builder_chain() {
        let setting = SettingBuilder::new()
            .with_key(KEY.into())
            .with_value(VALUE.into())
            .build()
            .expect("Failed to build setting");

        assert_eq!(setting.key(), KEY);
        assert_eq!(setting.value(), VALUE);
    }

    /// Verify that the `SettingBuilder` can be successfully used with separate calls to its
    /// methods.
    ///
    /// 1. Construct the setting using separate method calls and assignments of the builder
    /// 2. Verify that the resulting setting is correct
    #[test]
    fn builder_separate() {
        let mut builder = SettingBuilder::new();
        builder = builder.with_key(KEY.into());
        builder = builder.with_value(VALUE.into());

        let setting = builder.build().expect("Failed to build setting");

        assert_eq!(setting.key(), KEY);
        assert_eq!(setting.value(), VALUE);
    }

    /// Verify that the `SettingBuilder` fails when any of the required fields are missing.
    ///
    /// 1. Attempt to build a setting without setting `key` and verify that it fails.
    /// 2. Attempt to build a setting without setting `value` and verify that it fails.
    #[test]
    fn builder_missing_fields() {
        match SettingBuilder::new().with_value(VALUE.into()).build() {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }

        match SettingBuilder::new().with_key(KEY.into()).build() {
            Err(ProtocolBuildError::MissingField(_)) => {}
            res => panic!(
                "Expected Err(ProtocolBuildError::MissingField), got {:?}",
                res
            ),
        }
    }
}
