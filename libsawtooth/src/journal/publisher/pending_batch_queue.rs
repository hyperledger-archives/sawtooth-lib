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

//! A bounded queue of batches for populating new blocks

use std::collections::{HashSet, VecDeque};

use crate::transact::protocol::batch::BatchPair;

/// The default number of most-recently-published blocks to use when computing the upper bound of
/// the pending batch queue
const DEFAULT_BATCH_QUEUE_SAMPLE_SIZE: usize = 5;
/// The default value used to seed the pending batch queue's upper bound
const DEFAULT_BATCH_QUEUE_INITIAL_SAMPLE_VALUE: usize = 30;
/// The default multiplier of the rolling average for computing the pending batch queue's upper
/// bound
const DEFAULT_BATCH_QUEUE_MULTIPLIER: usize = 10;
/// The name of the batch queue's size gauge metric
const PENDING_BATCH_GAUGE: &str = "publisher.BlockPublisher.pending_batch_gauge";

/// A queue of batches to be scheduled
///
/// The queue has a variable upper bound that is determined by computing the rolling average of the
/// number of batches in the latest blocks, and combining the average with a multiplier (the
/// default multiplier is 10). The purpose of the upper bound is to provide backpressure when more
/// batches are being submitted than the node (or network) can handle.
pub struct PendingBatchQueue {
    batches: VecDeque<BatchPair>,
    ids: HashSet<String>,
    multiplier: usize,
    rolling_average: RollingAverage,
}

impl Default for PendingBatchQueue {
    fn default() -> Self {
        Self::new(None, None, None)
    }
}

impl PendingBatchQueue {
    /// Creates a new queue with the given sample size and initial value used for the rolling
    /// average
    pub fn new(
        sample_size: Option<usize>,
        initial_value: Option<usize>,
        multiplier: Option<usize>,
    ) -> Self {
        Self {
            batches: VecDeque::new(),
            ids: HashSet::new(),
            multiplier: multiplier.unwrap_or(DEFAULT_BATCH_QUEUE_MULTIPLIER),
            rolling_average: RollingAverage::new(
                sample_size.unwrap_or(DEFAULT_BATCH_QUEUE_SAMPLE_SIZE),
                initial_value.unwrap_or(DEFAULT_BATCH_QUEUE_INITIAL_SAMPLE_VALUE),
            ),
        }
    }

    /// Determines if the queue has the batch with the given ID
    pub fn contains(&self, id: &str) -> bool {
        self.ids.contains(id)
    }

    /// Returns the current upper bound on the queue's size
    pub fn bound(&self) -> usize {
        self.rolling_average.value() * self.multiplier
    }

    /// Determines if the queue is full
    pub fn is_full(&self) -> bool {
        self.batches.len() >= self.bound()
    }

    /// Takes a batch from the front of the queue
    pub fn pop(&mut self) -> Option<BatchPair> {
        self.batches.pop_front().map(|batch| {
            self.ids.remove(batch.batch().header_signature());
            gauge!(PENDING_BATCH_GAUGE, self.batches.len() as f64);
            batch
        })
    }

    /// Takes the given number of batches from the front of the queue
    ///
    /// If no number is specified, all batches in the queue will be returned.
    ///
    /// This method may return fewer than the requested number of batches if the queue is smaller
    /// than the requested number
    pub fn drain(&mut self, num: Option<usize>) -> Vec<BatchPair> {
        let batches = match num {
            Some(num) => {
                let batches = self.batches.drain(..num).collect::<Vec<_>>();
                for batch in &batches {
                    self.ids.remove(batch.batch().header_signature());
                }
                batches
            }
            None => {
                self.ids.clear();
                self.batches.drain(..).collect()
            }
        };
        gauge!(PENDING_BATCH_GAUGE, self.batches.len() as f64);
        batches
    }

    /// Adds a batch to the end of the queue
    ///
    /// If `force`, the batch will be added to the queue even if it's full.
    ///
    /// # Errors
    ///
    /// * Returns `PendingBatchQueueAppendError::DuplicateBatch` if the batch is already in the
    ///   queue; the batch is returned with the error.
    /// * Returns `PendingBatchQueueAppendError::QueueFull` if the queue is full; the batch is
    ///   returned with the error.
    pub fn push_back(
        &mut self,
        batch: BatchPair,
        force: bool,
    ) -> Result<(), Box<PendingBatchQueueAppendError>> {
        if !force && self.is_full() {
            Err(PendingBatchQueueAppendError::QueueFull(batch))
        } else if self.contains(batch.batch().header_signature()) {
            Err(PendingBatchQueueAppendError::DuplicateBatch(batch))
        } else {
            self.ids.insert(batch.batch().header_signature().into());
            self.batches.push_back(batch);
            gauge!(PENDING_BATCH_GAUGE, self.batches.len() as f64);
            Ok(())
        }
    }

    /// Adds batches to the front of the queue in the order they are passed in
    ///
    /// This is used to prioritize batches, usually when they are uncommitted (when switching away
    /// from a fork in the blockchain, for instance) or when they should skip the queue and be
    /// executed as soon as possible (settings transactions may be prioritized, for instance).
    ///
    /// Unlike `push_back`, this method will add the batches to the queue even if it's full. If any
    /// of the batches is already in the queue, the old one will be removed.
    pub fn append_front(&mut self, batches: Vec<BatchPair>) {
        // Remove any duplicates
        self.batches.retain(|batch| !batches.contains(batch));

        for batch in batches.into_iter().rev() {
            self.ids.insert(batch.batch().header_signature().into());
            self.batches.push_front(batch);
        }

        gauge!(PENDING_BATCH_GAUGE, self.batches.len() as f64);
    }

    /// Removes batches with the given IDs from the queue
    ///
    /// Any batches that don't exist in the queue will be ignored.
    pub fn remove_batches(&mut self, batch_ids: &[&str]) {
        self.batches
            .retain(|batch| !batch_ids.contains(&batch.batch().header_signature()));
        for id in batch_ids {
            self.ids.remove(*id);
        }
        gauge!(PENDING_BATCH_GAUGE, self.batches.len() as f64);
    }

    /// Updates the queue's upper bound by adding the number of consumed batches as a new sample
    ///
    /// The new sample is added if either of the following conditions are met:
    ///
    /// * The number of consumed batches is greater than the current rolling average
    /// * The number of batches remaining in the queue is greater than the current rolling average
    ///
    /// The first condition ensures that the average is adjusted upward when more batches than the
    /// current average are consumed. The second condition ensures that the average is adjusted
    /// downward when more batches have been added than have been consumed.
    ///
    /// If neither of the conditions is met, the sample is not added. This ensures that the average
    /// is not adjusted when fewer batches were added than the rolling average suggests can be
    /// consumed. The purpose of the queue's upper bound is to provide a best guess for how many
    /// batches can be consumed per block; it should not be reduced when the limiting factor is how
    /// many batches are being submitted instead of how many batches are being consumed. This
    /// sample would not be a good representation of the network's throughput capability.
    pub fn update_bound(&mut self, consumed: usize) {
        let current_average = self.rolling_average.value();
        if consumed > current_average || self.batches.len() > current_average {
            self.rolling_average.add_sample(consumed);
        }
    }
}

/// Errors that may occur when pushing a batch to the queue
#[derive(Debug)]
pub enum PendingBatchQueueAppendError {
    DuplicateBatch(BatchPair),
    QueueFull(BatchPair),
}

impl std::error::Error for PendingBatchQueueAppendError {}

impl std::fmt::Display for PendingBatchQueueAppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::DuplicateBatch(batch) => write!(
                f,
                "attempted to add duplicate batch: {}",
                batch.batch().header_signature()
            ),
            Self::QueueFull(_) => f.write_str("queue full"),
        }
    }
}

/// Used by the `PendingBatchQueue` to determine its upper bound
struct RollingAverage {
    samples: VecDeque<usize>,
    current_average: usize,
}

impl RollingAverage {
    /// Creates a new `RollingAverage`
    pub fn new(sample_size: usize, initial_value: usize) -> RollingAverage {
        let mut samples = VecDeque::with_capacity(sample_size);
        samples.push_back(initial_value);

        RollingAverage {
            samples,
            current_average: initial_value,
        }
    }

    /// Gets the current rolling average value
    pub fn value(&self) -> usize {
        self.current_average
    }

    /// Adds the sample and returns the updated average
    pub fn add_sample(&mut self, sample: usize) -> usize {
        self.samples.push_back(sample);
        self.current_average = self.samples.iter().sum::<usize>() / self.samples.len();
        self.current_average
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;
    use std::rc::Rc;

    use cylinder::{secp256k1::Secp256k1Context, Context, Signer};
    use metrics::{GaugeValue, Key, Recorder};

    use crate::transact::protocol::{
        batch::BatchBuilder,
        transaction::{HashMethod, TransactionBuilder},
    };

    /// Verifies that the `RollingAverage` properly computes its value based on the provided
    /// samples
    ///
    /// 1. Create a new `RollingAverage`
    /// 2. Check that the initial value is returned when checking the average
    /// 3. Add some new samples until the sample size is reached, verifying all of the averages are
    ///    correct
    /// 4. Add an additional sample that replaces the oldest one and check the average
    #[test]
    fn rolling_average() {
        let mut avg = RollingAverage::new(3, 5);

        assert_eq!(avg.value(), 5);

        avg.add_sample(3);
        assert_eq!(avg.value(), 4);
        avg.add_sample(1);
        assert_eq!(avg.value(), 3);

        avg.add_sample(8);
        assert_eq!(avg.value(), 4);
    }

    /// Verifies the functionality of the `PendingBatchQueue::contains` method
    ///
    /// 1. Create a new `PendingBatchQueue` with a large enough upper bound to avoid filling the
    ///    queue
    /// 2. Add some batches using the `push_back` method
    /// 3. Verify that the `contains` method properly determines if a batch exists in the queue
    #[test]
    fn queue_contains() {
        let mut q = PendingBatchQueue::new(None, Some(10), Some(10));

        let batches = batches(&*new_signer(), 3);
        q.push_back(batches[0].clone(), false)
            .expect("Failed to add batch1");
        q.push_back(batches[1].clone(), false)
            .expect("Failed to add batch2");
        q.push_back(batches[2].clone(), false)
            .expect("Failed to add batch3");

        assert!(q.contains(batches[0].batch().header_signature()));
        assert!(q.contains(batches[1].batch().header_signature()));
        assert!(q.contains(batches[2].batch().header_signature()));
        assert!(!q.contains("unkonwn_batch_id"));
    }

    /// Verifies the functionality of the `PendingBatchQueue::is_full` method
    ///
    /// 1. Create a new `PendingBatchQueue` with a small upper bound
    /// 2. Verify that the queue is not full initially
    /// 2. Fill the queue with new batches
    /// 3. Verify that the queue is full
    #[test]
    fn queue_is_full() {
        let mut q = PendingBatchQueue::new(None, Some(1), Some(2));

        assert!(!q.is_full());

        let signer = new_signer();
        q.push_back(batch(&*signer, 1), false)
            .expect("Failed to add batch1");
        q.push_back(batch(&*signer, 2), false)
            .expect("Failed to add batch2");

        assert!(q.is_full());
    }

    /// Verifies the functionality of the `PendingBatchQueue::pop` method
    ///
    /// 1. Create a new `PendingBatchQueue` with a large enough upper bound to avoid filling the
    ///    queue
    /// 2. Add some batches using the `push_back` method
    /// 3. Pop all of the batches from the queue, verifying that every batch is returned in the
    ///    order it was inserted until there are none left
    /// 4. Verify that none of the batches remain in the queue
    #[test]
    fn queue_pop() {
        let mut q = PendingBatchQueue::new(None, Some(10), Some(10));

        let batches = batches(&*new_signer(), 3);
        q.push_back(batches[0].clone(), false)
            .expect("Failed to add batch1");
        q.push_back(batches[1].clone(), false)
            .expect("Failed to add batch2");
        q.push_back(batches[2].clone(), false)
            .expect("Failed to add batch3");

        assert_eq!(q.pop().as_ref(), Some(&batches[0]));
        assert_eq!(q.pop().as_ref(), Some(&batches[1]));
        assert_eq!(q.pop().as_ref(), Some(&batches[2]));
        assert_eq!(q.pop(), None);

        assert!(q.batches.is_empty());
        assert!(q.ids.is_empty());
    }

    /// Verifies the functionality of the `PendingBatchQueue::drain` method
    ///
    /// 1. Create a new `PendingBatchQueue` with a large enough upper bound to avoid filling the
    ///    queue
    /// 2. Add some batches using the `push_back` method
    /// 3. Drain some of the batches from the queue, verifying that the requested number of batches
    ///    is returned in the order they were inserted
    /// 4. Drain the rest of the queue, verifying that the batches are returned in the order they
    ///    were inserted
    /// 5. Verify that the queue is empty
    #[test]
    fn queue_drain() {
        let mut q = PendingBatchQueue::new(None, Some(10), Some(10));

        let batches = batches(&*new_signer(), 4);
        q.push_back(batches[0].clone(), false)
            .expect("Failed to add batch1");
        q.push_back(batches[1].clone(), false)
            .expect("Failed to add batch2");
        q.push_back(batches[2].clone(), false)
            .expect("Failed to add batch3");
        q.push_back(batches[3].clone(), false)
            .expect("Failed to add batch3");

        assert_eq!(q.drain(Some(2)).as_slice(), &batches[0..2]);

        assert_eq!(q.drain(None).as_slice(), &batches[2..4]);

        assert!(q.batches.is_empty());
        assert!(q.ids.is_empty());
    }

    /// Verifies that the `PendingBatchQueue::push_back` method returns a
    /// `PendingBatchQueueAppendError::DuplicateBatch` error when attempting to add a batch that is
    /// already in the queue
    ///
    /// 1. Create a new `PendingBatchQueue` with a large enough upper bound to avoid filling the
    ///    queue
    /// 2. Add a batch
    /// 3. Attempt to add the batch again; verify that the expected error is returned and that it
    ///    contains the batch
    #[test]
    fn queue_push_back_duplicate() {
        let mut q = PendingBatchQueue::new(None, Some(10), Some(10));

        let batch = batch(&*new_signer(), 0);
        q.push_back(batch.clone(), false)
            .expect("Failed to add batch");

        match q.push_back(batch.clone(), false) {
            Err(PendingBatchQueueAppendError::DuplicateBatch(err_batch)) if err_batch == batch => {}
            res => panic!(
                "Expected PendingBatchQueueAppendError::DuplicateBatch({:?}), got: {:?}",
                batch, res
            ),
        }
    }

    /// Verifies that the `PendingBatchQueue::push_back` method returns a
    /// `PendingBatchQueueAppendError::QueueFull` error when attempting to add a batch when the
    /// queue is full and `force` is `false`
    ///
    /// 1. Create a new `PendingBatchQueue` with an upper bound of 0
    /// 2. Attempt to add a batch; verify that the expected error is returned and that it contains
    ///    the batch
    #[test]
    fn queue_push_back_full() {
        let mut q = PendingBatchQueue::new(None, Some(0), Some(0));

        let batch = batch(&*new_signer(), 0);
        match q.push_back(batch.clone(), false) {
            Err(PendingBatchQueueAppendError::QueueFull(err_batch)) if err_batch == batch => {}
            res => panic!(
                "Expected PendingBatchQueueAppendError::QueueFull({:?}), got: {:?}",
                batch, res
            ),
        }
    }

    /// Verifies that the `PendingBatchQueue::push_back` method adds a batch when `force` is `true`
    /// and the queue is full
    ///
    /// 1. Create a new `PendingBatchQueue` with an upper bound of 0
    /// 2. Force-add a batch
    /// 3. Verify that the batch is in the queue
    #[test]
    fn queue_push_back_force() {
        let mut q = PendingBatchQueue::new(None, Some(0), Some(0));

        let batch = batch(&*new_signer(), 0);
        q.push_back(batch.clone(), true)
            .expect("Failed to force-add batch");

        assert_eq!(q.pop(), Some(batch));
    }

    /// Verifies the functionality of the `PendingBatchQueue::append_front` method
    ///
    /// 1. Create a new `PendingBatchQueue` with an upper bound of 2
    /// 2. Add batches 0 and 1 in order using `push_back`
    /// 3. Add batches 1 and 2 using `append_front`
    /// 4. Verify that the batch queue is now [batch 1, batch 2, batch 0]
    #[test]
    fn queue_append_front() {
        let mut q = PendingBatchQueue::new(None, Some(2), Some(1));

        let batches = batches(&*new_signer(), 3);
        q.push_back(batches[0].clone(), false)
            .expect("Failed to add batch1");
        q.push_back(batches[1].clone(), false)
            .expect("Failed to add batch2");

        q.append_front(vec![batches[1].clone(), batches[2].clone()]);

        assert_eq!(q.pop().as_ref(), Some(&batches[1]));
        assert_eq!(q.pop().as_ref(), Some(&batches[2]));
        assert_eq!(q.pop().as_ref(), Some(&batches[0]));
        assert_eq!(q.pop(), None);
    }

    /// Verifies the functionality of the `PendingBatchQueue::remove_batches` method
    ///
    /// 1. Create a new `PendingBatchQueue` with a large enough upper bound to avoid filling the
    ///    queue
    /// 2. Add some batches
    /// 3. Removes some of the batches that were added
    /// 4. Verify that the removed batches are no longer in the queue, but the others are
    #[test]
    fn queue_remove_batches() {
        let mut q = PendingBatchQueue::new(None, Some(10), Some(10));

        let batches = batches(&*new_signer(), 4);
        q.push_back(batches[0].clone(), false)
            .expect("Failed to add batch1");
        q.push_back(batches[1].clone(), false)
            .expect("Failed to add batch2");
        q.push_back(batches[2].clone(), false)
            .expect("Failed to add batch3");
        q.push_back(batches[3].clone(), false)
            .expect("Failed to add batch4");

        q.remove_batches(
            &batches
                .iter()
                .take(2)
                .map(|batch| batch.batch().header_signature())
                .collect::<Vec<_>>(),
        );

        assert_eq!(q.pop().as_ref(), Some(&batches[2]));
        assert_eq!(q.pop().as_ref(), Some(&batches[3]));
        assert_eq!(q.pop(), None);
    }

    /// Verifies the functionality of the `PendingBatchQueue::bound` and
    /// `PendingBatchQueue::update_bound` methods
    ///
    /// 1. Create a new `PendingBatchQueue`
    /// 2. Verify that the initial bound is correct
    /// 3. Add a sample where the number of consumed batches is greater than the current average
    ///    and verify that the bound is updated accordingly
    /// 4. Add more batches to the queue than the current average
    /// 5. Add a sample and verify that the average is updated accordingly
    /// 6. Empty the queue and add a sample that is less than the current average; verify that the
    ///    bound is not updated.
    #[test]
    fn queue_bound() {
        let mut q = PendingBatchQueue::new(Some(3), Some(1), Some(2));

        assert_eq!(q.bound(), 2); // (1 / 1) * 2 = 2

        q.update_bound(5);
        assert_eq!(q.bound(), 6); // ((1 + 5) / 2) * 2 = 6

        q.append_front(batches(&*new_signer(), 4));

        q.update_bound(6);
        assert_eq!(q.bound(), 8); // ((1 + 5 + 6) / 3) * 2 = 8

        while let Some(_) = q.pop() {}
        q.update_bound(4);
        assert_eq!(q.bound(), 8);
    }

    /// Verifies that the `PendingBatchQueue` properly tracks its size for metrics
    ///
    /// 1. Initialize metrics
    /// 2. Create a new `PendingBatchQueue` with a large enough upper bound to avoid filling the
    ///    queue
    /// 3. Add a batch using `push_back` and verify that the metric is updated
    /// 4. Add some batches using `append_front` and verify that the metric is updated
    /// 5. Remove some batches using `remove_batches` and verify that the metric is updated
    /// 6. Remove a batch using `pop` and verify that the metric is updated
    /// 7. Drain the queue using `drain` and verify that the metric is updated
    #[test]
    fn queue_metrics() {
        let metrics = Box::new(MockMetricsRecorder::default());
        metrics::set_boxed_recorder(metrics.clone()).expect("Failed to set metrics recorder");

        let mut q = PendingBatchQueue::new(None, Some(10), Some(10));

        let signer = new_signer();
        q.push_back(batch(&*signer, 0), false)
            .expect("Failed to add batch");
        assert_eq!(metrics.batch_queue_size(), 1);

        q.append_front(batches(&*signer, 4));
        assert_eq!(metrics.batch_queue_size(), 4); // The first batch is replaced

        q.remove_batches(
            &batches(&*signer, 2)
                .iter()
                .map(|batch| batch.batch().header_signature())
                .collect::<Vec<_>>(),
        );
        assert_eq!(metrics.batch_queue_size(), 2);

        q.pop();
        assert_eq!(metrics.batch_queue_size(), 1);

        q.drain(None);
        assert_eq!(metrics.batch_queue_size(), 0);
    }

    fn new_signer() -> Box<dyn Signer> {
        let context = Secp256k1Context::new();
        context.new_signer(context.new_random_private_key())
    }

    fn batches(signer: &dyn Signer, num: u8) -> Vec<BatchPair> {
        (0..num).map(|i| batch(signer, i)).collect()
    }

    fn batch(signer: &dyn Signer, nonce: u8) -> BatchPair {
        let txn = TransactionBuilder::new()
            .with_family_name("test".into())
            .with_family_version("0.1".into())
            .with_inputs(vec![])
            .with_outputs(vec![])
            .with_payload_hash_method(HashMethod::Sha512)
            .with_payload(vec![])
            .with_nonce(vec![nonce])
            .build(signer)
            .expect("Failed to build txn");

        BatchBuilder::new()
            .with_transactions(vec![txn])
            .build_pair(signer)
            .expect("Failed to build batch")
    }

    #[derive(Clone)]
    struct MockMetricsRecorder {
        batch_queue_size: Rc<RefCell<GaugeValue>>,
    }

    impl Default for MockMetricsRecorder {
        fn default() -> Self {
            Self {
                batch_queue_size: Rc::new(RefCell::new(GaugeValue::Absolute(0.0))),
            }
        }
    }

    impl MockMetricsRecorder {
        pub fn batch_queue_size(&self) -> i64 {
            match &*self.batch_queue_size.borrow_mut() {
                GaugeValue::Absolute(f) => *f as i64,
                GaugeValue::Increment(f) => *f as i64,
                GaugeValue::Decrement(f) => *f as i64,
            }
        }
    }

    impl Recorder for MockMetricsRecorder {
        fn increment_counter(&self, _key: &Key, _value: u64) {}

        fn update_gauge(&self, key: &Key, value: GaugeValue) {
            if key.name() == PENDING_BATCH_GAUGE {
                *self.batch_queue_size.borrow_mut() = value
            }
        }

        fn record_histogram(&self, _key: &Key, _value: f64) {}

        fn register_histogram(
            &self,
            _key: &Key,
            _unit: Option<metrics::Unit>,
            _description: Option<&'static str>,
        ) {
        }

        fn register_gauge(
            &self,
            _key: &Key,
            _unit: Option<metrics::Unit>,
            _description: Option<&'static str>,
        ) {
        }
        fn register_counter(
            &self,
            _key: &Key,
            _unit: Option<metrics::Unit>,
            _description: Option<&'static str>,
        ) {
        }
    }
}
