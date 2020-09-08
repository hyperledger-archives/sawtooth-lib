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

use std::collections::{HashSet, VecDeque};
use std::slice::Iter;

use transact::protocol::batch::BatchPair;

/// The default number of most-recently-published blocks to use when computing the limit of the
/// pending batches pool
const DEFAULT_BATCH_POOL_SAMPLE_SIZE: usize = 5;
/// The default value used to seed the pending batch pool's limit
const DEFAULT_BATCH_POOL_INITIAL_SAMPLE_VALUE: usize = 30;
/// The multiplier of the rolling average for computing the pending batch pool's limit
const BATCH_POOL_MULTIPLIER: usize = 10;

/// Ordered batches waiting to be processed
pub struct PendingBatchesPool {
    batches: Vec<BatchPair>,
    ids: HashSet<String>,
    limit: QueueLimit,
}

impl Default for PendingBatchesPool {
    fn default() -> Self {
        Self::new(
            DEFAULT_BATCH_POOL_SAMPLE_SIZE,
            DEFAULT_BATCH_POOL_INITIAL_SAMPLE_VALUE,
        )
    }
}

impl PendingBatchesPool {
    pub fn new(sample_size: usize, initial_value: usize) -> PendingBatchesPool {
        PendingBatchesPool {
            batches: Vec::new(),
            ids: HashSet::new(),
            limit: QueueLimit::new(sample_size, initial_value),
        }
    }

    pub fn len(&self) -> usize {
        self.batches.len()
    }

    pub fn limit(&self) -> usize {
        self.limit.get()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.batches.len() >= self.limit.get()
    }

    pub fn iter(&self) -> Iter<BatchPair> {
        self.batches.iter()
    }

    pub fn contains(&self, id: &str) -> bool {
        self.ids.contains(id)
    }

    fn reset(&mut self) {
        self.batches = Vec::new();
        self.ids = HashSet::new();
    }

    pub fn append(&mut self, batch: BatchPair) -> bool {
        if self
            .ids
            .insert(batch.batch().header_signature().to_string())
        {
            self.batches.push(batch);
            true
        } else {
            false
        }
    }

    /// Recomputes the list of pending batches
    ///
    /// Args:
    ///   committed (List<Batches>): Batches committed in the current chain
    ///   since the root of the fork switching from.
    ///   uncommitted (List<Batches): Batches that were committed in the old
    ///   fork since the common root.
    pub fn rebuild(
        &mut self,
        committed: Option<Vec<BatchPair>>,
        uncommitted: Option<Vec<BatchPair>>,
    ) {
        let committed_set = committed
            .map(|committed| {
                committed
                    .iter()
                    .map(|i| i.batch().header_signature().to_string())
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        let previous_batches = self.batches.clone();

        self.reset();

        // Uncommitted and pending are disjoint sets since batches can only be
        // committed to a chain once.

        if let Some(batch_list) = uncommitted {
            for batch in batch_list {
                if !committed_set.contains(batch.batch().header_signature()) {
                    self.append(batch);
                }
            }
        }

        for batch in previous_batches {
            if !committed_set.contains(batch.batch().header_signature()) {
                self.append(batch);
            }
        }

        gauge!(
            "publisher.BlockPublisher.pending_batch_gauge",
            self.batches.len() as i64
        );
    }

    /// Removes the batches with the given IDs from the pool
    ///
    /// This method is called when a new block is published
    pub fn update(&mut self, published_batch_ids: HashSet<&str>) {
        self.batches
            .retain(|batch| !published_batch_ids.contains(batch.batch().header_signature()));
        for id in published_batch_ids {
            self.ids.remove(id);
        }
        gauge!(
            "publisher.BlockPublisher.pending_batch_gauge",
            self.batches.len() as i64
        );
    }

    pub fn update_limit(&mut self, consumed: usize) {
        self.limit.update(self.batches.len(), consumed);
    }
}

struct RollingAverage {
    samples: VecDeque<usize>,
    current_average: usize,
}

impl RollingAverage {
    pub fn new(sample_size: usize, initial_value: usize) -> RollingAverage {
        let mut samples = VecDeque::with_capacity(sample_size);
        samples.push_back(initial_value);

        RollingAverage {
            samples,
            current_average: initial_value,
        }
    }

    pub fn value(&self) -> usize {
        self.current_average
    }

    /// Add the sample and return the updated average.
    pub fn update(&mut self, sample: usize) -> usize {
        self.samples.push_back(sample);
        self.current_average = self.samples.iter().sum::<usize>() / self.samples.len();
        self.current_average
    }
}

struct QueueLimit {
    avg: RollingAverage,
}

impl QueueLimit {
    pub fn new(sample_size: usize, initial_value: usize) -> QueueLimit {
        QueueLimit {
            avg: RollingAverage::new(sample_size, initial_value),
        }
    }

    /// Use the current queue size and the number of items consumed to
    /// update the queue limit, if there was a significant enough change.
    /// Args:
    ///     queue_length (int): the current size of the queue
    ///     consumed (int): the number items consumed
    pub fn update(&mut self, queue_length: usize, consumed: usize) {
        if consumed > 0 {
            // Only update the average if either:
            // a. Not drained below the current average
            // b. Drained the queue, but the queue was not bigger than the
            //    current running average

            let remainder = queue_length.saturating_sub(consumed);

            if remainder > self.avg.value() || consumed > self.avg.value() {
                self.avg.update(consumed);
            }
        }
    }

    pub fn get(&self) -> usize {
        // Limit the number of items to BATCH_POOL_MULTIPLIER times the publishing
        // average.  This allows the queue to grow geometrically, if the queue
        // is drained.
        BATCH_POOL_MULTIPLIER * self.avg.value()
    }
}
