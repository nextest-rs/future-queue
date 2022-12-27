// Copyright (c) The future-queue Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use future_queue::{traits::WeightedFuture, FutureQueue, StreamExt as _};
use futures::{future::BoxFuture, stream, Future, FutureExt, Stream, StreamExt as _};
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use std::{pin::Pin, time::Duration};
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Clone, Debug, Arbitrary)]
struct TestState<G: GroupSpec> {
    #[proptest(strategy = "1usize..64")]
    max_weight: usize,
    #[proptest(strategy = "prop::collection::vec(TestFutureDesc::arbitrary(), 0..512usize)")]
    future_descriptions: Vec<TestFutureDesc<G>>,
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

trait GroupSpec: Arbitrary + Send + Copy + 'static {
    type Item: Send;
    type CheckState: Default;

    fn create_stream<St>(stream: St, state: &TestState<Self>) -> BoxedWeightedStream<()>
    where
        St: Stream<Item = Self::Item> + Send + 'static;

    fn create_stream_item(
        desc: &TestFutureDesc<Self>,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Self::Item;

    fn check_started(
        check_state: &mut Self::CheckState,
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

type BoxedWeightedStream<Item> = Pin<Box<dyn WeightedStream<Item = Item> + Send>>;

impl GroupSpec for () {
    type Item = (usize, BoxFuture<'static, ()>);
    type CheckState = NonGroupedCheckState;

    fn create_stream<St>(stream: St, state: &TestState<Self>) -> BoxedWeightedStream<()>
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
    current_weight: usize,
}

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
    };
    test_future_queue_impl(state);
}

proptest! {
    #[test]
    fn proptest_future_queue(state: TestState<()>) {
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
    let mut last_started_id: Option<usize> = None;
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
                            // last_started_id must be 1 less than id.
                            let expected_id = last_started_id.map_or(0, |id| id + 1);
                            assert_eq!(expected_id, id, "expected future id to start != actual id that started");
                            last_started_id = Some(id);

                            G::check_started(&mut check_state, &desc, &state);
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
