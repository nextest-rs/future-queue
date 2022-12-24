// Copyright (c) The future-queue Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use future_queue::{
    traits::{GroupedWeightedFuture, WeightedFuture},
    FutureQueue, FutureQueueGrouped, StreamExt as _,
};
use futures::{future::BoxFuture, stream, Future, FutureExt, Stream, StreamExt as _};
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use std::{borrow::Borrow, collections::HashMap, pin::Pin, sync::Arc, time::Duration};
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Clone, Debug, Arbitrary)]
struct TestState<G: GroupSpec> {
    #[proptest(strategy = "1usize..64")]
    max_weight: usize,
    #[proptest(strategy = "prop::collection::vec(TestFutureDesc::arbitrary(), 0..512usize)")]
    future_descriptions: Vec<TestFutureDesc<G>>,
    group_desc: G::GroupDesc,
}

#[derive(Copy, Clone, Debug, Arbitrary)]
struct TestFutureDesc<G: GroupSpec> {
    #[proptest(strategy = "duration_strategy()")]
    start_delay: Duration,
    #[proptest(strategy = "duration_strategy()")]
    delay: Duration,
    #[proptest(strategy = "0usize..8")]
    weight: usize,
    #[allow(dead_code)]
    group: G,
}

fn duration_strategy() -> BoxedStrategy<Duration> {
    // Allow for a delay between 0ms and 1000ms uniformly at random.
    (0u64..1000).prop_map(Duration::from_millis).boxed()
}

trait GroupSpec: Arbitrary + Send + Sync + Copy + 'static {
    type Item: Send;
    type GroupDesc;
    type CheckState: Default;

    fn create_stream<'a, St>(stream: St, state: &TestState<Self>) -> BoxedWeightedStream<'a, ()>
    where
        St: Stream<Item = Self::Item> + Send + 'static;

    fn create_stream_item(
        desc: &TestFutureDesc<Self>,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Self::Item;

    fn check_started(
        check_state: &mut Self::CheckState,
        id: usize,
        desc: &TestFutureDesc<Self>,
        state: &TestState<Self>,
    );

    fn check_finished(
        check_state: &mut Self::CheckState,
        desc: &TestFutureDesc<Self>,
        state: &TestState<Self>,
    );
}

trait WeightedStream: Stream {
    fn current_weight(&self) -> usize;
}

impl<St, Fut> WeightedStream for FutureQueue<St>
where
    St: Stream<Item = Fut>,
    Fut: WeightedFuture,
{
    fn current_weight(&self) -> usize {
        self.current_weight()
    }
}

impl<St, K, Fut> WeightedStream for FutureQueueGrouped<St, K>
where
    St: Stream<Item = Fut>,
    Fut: GroupedWeightedFuture,
    K: Eq + std::hash::Hash + std::fmt::Debug + Borrow<Fut::Q>,
    Fut::Q: Eq + std::hash::Hash + std::fmt::Debug,
{
    fn current_weight(&self) -> usize {
        self.current_global_weight()
    }
}

type BoxedWeightedStream<'a, Item> = Pin<Box<dyn WeightedStream<Item = Item> + Send + 'a>>;

impl GroupSpec for () {
    type Item = (usize, BoxFuture<'static, ()>);
    type GroupDesc = ();
    type CheckState = NonGroupedCheckState;

    fn create_stream<'a, St>(stream: St, state: &TestState<Self>) -> BoxedWeightedStream<'a, ()>
    where
        St: Stream<Item = Self::Item> + Send + 'static,
    {
        Box::pin(stream.future_queue(state.max_weight))
    }

    fn create_stream_item(
        desc: &TestFutureDesc<Self>,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Self::Item {
        (desc.weight, future.boxed())
    }

    fn check_started(
        check_state: &mut Self::CheckState,
        id: usize,
        desc: &TestFutureDesc<Self>,
        state: &TestState<Self>,
    ) {
        // last_started_id must be 1 less than id.
        let expected_id = check_state.last_started_id.map_or(0, |id| id + 1);
        assert_eq!(
            expected_id, id,
            "expected future id to start != actual id that started"
        );
        check_state.last_started_id = Some(id);

        // Check that current_weight doesn't go over the limit.
        assert!(
            check_state.current_weight < state.max_weight,
            "current weight {} exceeds max weight {}",
            check_state.current_weight,
            state.max_weight,
        );
        check_state.current_weight += desc.weight;
    }

    fn check_finished(
        check_state: &mut Self::CheckState,
        desc: &TestFutureDesc<Self>,
        _state: &TestState<Self>,
    ) {
        check_state.current_weight -= desc.weight;
    }
}

#[derive(Debug, Default)]
struct NonGroupedCheckState {
    last_started_id: Option<usize>,
    current_weight: usize,
}

#[derive(Clone, Copy, Hash, Eq, PartialEq, Debug, Arbitrary)]
enum TestGroup {
    A,
    B,
    C,
    D,
}

#[derive(Debug, Default)]
struct TestGroupState {
    map: HashMap<TestGroup, usize>,
}

impl Arbitrary for TestGroupState {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        (1usize..64, 1usize..64, 1usize..64, 1usize..64)
            .prop_map(|(a, b, c, d)| {
                let mut map = HashMap::new();
                map.insert(TestGroup::A, a);
                map.insert(TestGroup::B, b);
                map.insert(TestGroup::C, c);
                map.insert(TestGroup::D, d);
                TestGroupState { map }
            })
            .boxed()
    }
}

impl GroupSpec for Option<TestGroup> {
    type Item = (usize, Option<TestGroup>, BoxFuture<'static, ()>);
    type GroupDesc = TestGroupState;
    type CheckState = GroupedCheckState;

    fn create_stream<'a, St>(stream: St, state: &TestState<Self>) -> BoxedWeightedStream<'a, ()>
    where
        St: Stream<Item = Self::Item> + Send + 'static,
    {
        // Use `Arc` here to ensure that we're using the Borrow functionality.
        let groups = state
            .group_desc
            .map
            .iter()
            .map(|(group, max_weight)| (Arc::new(*group), *max_weight));
        Box::pin(stream.future_queue_grouped(state.max_weight, groups))
    }

    fn create_stream_item(
        desc: &TestFutureDesc<Self>,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Self::Item {
        (desc.weight, desc.group, future.boxed())
    }

    fn check_started(
        check_state: &mut Self::CheckState,
        _id: usize,
        desc: &TestFutureDesc<Self>,
        state: &TestState<Self>,
    ) {
        // Check that current_weight doesn't go over the limit.
        assert!(
            check_state.current_weight < state.max_weight,
            "current weight {} exceeds max weight {}",
            check_state.current_weight,
            state.max_weight,
        );
        check_state.current_weight += desc.weight;
        if let Some(group) = desc.group {
            let max_group_weight = state.group_desc.map[&group];
            let current_group_weight = check_state.group_weights.get_mut(&group).unwrap();
            assert!(
                *current_group_weight < max_group_weight,
                "current weight {} exceeds max weight {} for group {:?}",
                *current_group_weight,
                max_group_weight,
                group,
            );
            *current_group_weight += desc.weight;
        }
    }

    fn check_finished(
        check_state: &mut Self::CheckState,
        desc: &TestFutureDesc<Self>,
        _state: &TestState<Self>,
    ) {
        check_state.current_weight -= desc.weight;
        if let Some(group) = desc.group {
            let current_group_weight = check_state.group_weights.get_mut(&group).unwrap();
            *current_group_weight -= desc.weight;
        }
        // Note that this code doesn't currently check that futures from this group are
        // preferentially queued up first. That is a surprisingly hard problem that is somewhat
        // low-impact to test (since it falls out of the basic correct implementation).
    }
}

#[derive(Debug)]
struct GroupedCheckState {
    current_weight: usize,
    group_weights: HashMap<TestGroup, usize>,
}

impl Default for GroupedCheckState {
    fn default() -> Self {
        let mut group_weights = HashMap::new();
        group_weights.insert(TestGroup::A, 0);
        group_weights.insert(TestGroup::B, 0);
        group_weights.insert(TestGroup::C, 0);
        group_weights.insert(TestGroup::D, 0);
        GroupedCheckState {
            current_weight: 0,
            group_weights,
        }
    }
}

// ---
// Tests
// ---

#[test]
fn test_examples() {
    let state = TestState {
        max_weight: 1,
        future_descriptions: vec![TestFutureDesc {
            start_delay: Duration::ZERO,
            delay: Duration::ZERO,
            weight: 0,
            group: (),
        }],
        group_desc: (),
    };
    test_future_queue_impl(state);

    let state = TestState {
        max_weight: 1,
        future_descriptions: vec![TestFutureDesc {
            start_delay: Duration::ZERO,
            delay: Duration::ZERO,
            weight: 0,
            group: None,
        }],
        group_desc: TestGroupState {
            map: maplit::hashmap! {TestGroup::A => 1, TestGroup::B => 1, TestGroup::C => 1, TestGroup::D => 1},
        },
    };
    test_future_queue_impl(state);
}

proptest! {
    #[test]
    fn proptest_future_queue(state: TestState<()>) {
        test_future_queue_impl(state)
    }

    #[test]
    fn proptest_future_queue_grouped(state: TestState<Option<TestGroup>>) {
        test_future_queue_impl(state)
    }
}

#[derive(Clone, Copy, Debug)]
enum FutureEvent<G: GroupSpec> {
    Started(usize, TestFutureDesc<G>),
    Finished(usize, TestFutureDesc<G>),
}

fn test_future_queue_impl<G: GroupSpec>(state: TestState<G>) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("tokio builder succeeded");
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let (future_sender, future_receiver) = tokio::sync::mpsc::unbounded_channel();

    let futures = state
        .future_descriptions
        .iter()
        .enumerate()
        .map(move |(id, desc)| {
            let desc = *desc;
            let sender = sender.clone();
            let future_sender = future_sender.clone();
            async move {
                // First, sleep for this long.
                tokio::time::sleep(desc.start_delay).await;
                // For each description, create a future.
                let delay_fut = async move {
                    // Send the fact that this future started to the mpsc queue.
                    sender
                        .send(FutureEvent::Started(id, desc))
                        .expect("receiver held open by loop");
                    tokio::time::sleep(desc.delay).await;
                    sender
                        .send(FutureEvent::Finished(id, desc))
                        .expect("receiver held open by loop");
                };
                // Errors should never occur here.
                if let Err(err) = future_sender.send(G::create_stream_item(&desc, delay_fut)) {
                    panic!("future_receiver held open by loop: {}", err);
                }
            }
        })
        .collect::<Vec<_>>();
    let combined_future = stream::iter(futures).buffer_unordered(1).collect::<()>();
    runtime.spawn(combined_future);

    // We're going to use future_receiver as a stream.
    let stream = UnboundedReceiverStream::new(future_receiver);

    let mut completed_map = vec![false; state.future_descriptions.len()];
    let mut check_state = G::CheckState::default();

    runtime.block_on(async move {
        // Record values that have been completed in this map.
        let mut stream = G::create_stream(stream, &state);
        let mut receiver_done = false;
        loop {
            tokio::select! {
                // biased ensures that the receiver is drained before the stream is polled. Without
                // it, it's possible that we fail to record the completion of some futures in status_map.
                biased;

                recv = receiver.recv(), if !receiver_done => {
                    match recv {
                        Some(FutureEvent::Started(id, desc)) => {
                            G::check_started(&mut check_state, id, &desc, &state);
                        }
                        Some(FutureEvent::Finished(id, desc)) => {
                            // Record that this value was completed.
                            completed_map[id] = true;
                            G::check_finished(&mut check_state, &desc, &state);
                        }
                        None => {
                            // All futures finished -- going to check for completion in stream.next() below.
                            receiver_done = true;
                        }
                    }
                }
                next = stream.next() => {
                    if next.is_none() {
                        assert_eq!(stream.current_weight(), 0, "all futures complete => current weight is 0");
                        break;
                    }
                }
                else => {
                    tokio::time::advance(Duration::from_millis(1)).await;
                }
            }
        }

        // Check that all futures completed.
        let not_completed: Vec<_> = completed_map
            .iter()
            .enumerate()
            .filter_map(|(n, &v)| (!v).then(|| n.to_string()))
            .collect();
        if !not_completed.is_empty() {
            let not_completed_ids = not_completed.join(", ");
            panic!("some futures did not complete: {}", not_completed_ids);
        }
    })
}
