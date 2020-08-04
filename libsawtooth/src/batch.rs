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

use std::convert::TryFrom;

use protobuf::Message;

use transact::protocol::batch::Batch as TransactBatch;
use transact::protos::FromBytes;

use crate::protos;
use crate::transaction::Transaction;

#[derive(Clone, Debug, PartialEq)]
pub struct Batch {
    pub header_signature: String,
    pub transactions: Vec<Transaction>,
    pub signer_public_key: String,
    pub transaction_ids: Vec<String>,
    pub trace: bool,

    pub header_bytes: Vec<u8>,
}

impl From<Batch> for protos::batch::Batch {
    fn from(batch: Batch) -> Self {
        let mut proto_batch = protos::batch::Batch::new();
        proto_batch.set_transactions(protobuf::RepeatedField::from_vec(
            batch
                .transactions
                .into_iter()
                .map(protos::transaction::Transaction::from)
                .collect(),
        ));
        proto_batch.set_header_signature(batch.header_signature);
        proto_batch.set_header(batch.header_bytes);
        proto_batch.set_trace(batch.trace);
        proto_batch
    }
}

impl From<protos::batch::Batch> for Batch {
    fn from(mut proto_batch: protos::batch::Batch) -> Batch {
        let mut batch_header: protos::batch::BatchHeader =
            protobuf::parse_from_bytes(proto_batch.get_header())
                .expect("Unable to parse BatchHeader bytes");

        Batch {
            header_signature: proto_batch.take_header_signature(),
            header_bytes: proto_batch.take_header(),
            signer_public_key: batch_header.take_signer_public_key(),
            transaction_ids: batch_header.take_transaction_ids().into_vec(),
            trace: proto_batch.get_trace(),

            transactions: proto_batch
                .take_transactions()
                .into_iter()
                .map(Transaction::from)
                .collect(),
        }
    }
}

impl TryFrom<TransactBatch> for Batch {
    type Error = &'static str;

    fn try_from(batch: TransactBatch) -> Result<Self, Self::Error> {
        let mut batch_header: protos::batch::BatchHeader =
            protobuf::parse_from_bytes(batch.header()).expect("Unable to parse BatchHeader bytes");

        let mut transactions = vec![];
        for txn in batch.transactions().iter() {
            transactions.push(Transaction::try_from(txn.clone())?);
        }

        Ok(Batch {
            header_signature: batch.header_signature().to_string(),
            header_bytes: batch.header().to_vec(),
            signer_public_key: batch_header.take_signer_public_key(),
            transaction_ids: batch_header.take_transaction_ids().into_vec(),
            trace: batch.trace(),

            transactions,
        })
    }
}

impl TryFrom<Batch> for TransactBatch {
    type Error = &'static str;

    fn try_from(batch: Batch) -> Result<Self, Self::Error> {
        let batch_bytes = protos::batch::Batch::from(batch)
            .write_to_bytes()
            .map_err(|_| "Unable to convert Batch to bytes")?;

        Ok(TransactBatch::from_bytes(&batch_bytes)
            .map_err(|_| "Unable to get transact Batch from bytes")?)
    }
}
