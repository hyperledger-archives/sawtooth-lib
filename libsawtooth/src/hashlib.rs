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

use sha2::{Digest, Sha256, Sha512};

pub fn sha256_digest_str(item: &str) -> String {
    hex::encode(sha256_digest_strs(&[item.to_string()]))
}

pub fn sha256_digest_strs(strs: &[String]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    for item in strs {
        hasher.update(item.as_bytes());
    }
    let mut bytes = Vec::new();
    bytes.extend(hasher.finalize().into_iter());
    bytes
}

pub fn sha512_digest_bytes(item: &[u8]) -> Vec<u8> {
    let mut hasher = Sha512::new();
    hasher.update(item);
    hasher.finalize().to_vec()
}
