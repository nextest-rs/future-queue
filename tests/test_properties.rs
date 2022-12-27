// Copyright (c) The future-queue Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use future_queue::StreamExt as _;
use futures::{stream, StreamExt as _};
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use std::time::Duration;
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Clone, Debug, Arbitrary)]
struct TestState {
    #[proptest(strategy = "1usize..64")]
    max_weight: usize,
    #[proptest(strategy = "prop::collection::vec(TestFutureDesc::arbitrary(), 0..512usize)")]
    future_descriptions: Vec<TestFutureDesc>,
}

#[derive(Copy, Clone, Debug, Arbitrary)]
struct TestFutureDesc {
    #[proptest(strategy = "duration_strategy()")]
    start_delay: Duration,
    #[proptest(strategy = "duration_strategy()")]
    delay: Duration,
    #[proptest(strategy = "0usize..8")]
    weight: usize,
}

fn duration_strategy() -> BoxedStrategy<Duration> {
    // Allow for a delay between 0ms and 1000ms uniformly at random.
    (0u64..1000).prop_map(Duration::from_millis).boxed()
}

#[test]
fn test_examples() {
    let state = TestState {
        max_weight: 1,
        future_descriptions: vec![TestFutureDesc {
            start_delay: Duration::ZERO,
            delay: Duration::ZERO,
            weight: 0,
        }],
    };
    test_future_queue_impl(state);
}

proptest! {
    #[test]
    fn proptest_future_queue(state: TestState) {
        test_future_queue_impl(state)
    }
}

#[derive(Clone, Copy, Debug)]
enum FutureEvent {
    Started(usize, TestFutureDesc),
    Finished(usize, TestFutureDesc),
}

fn test_future_queue_impl(state: TestState) {
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
                if let Err(err) = future_sender.send((desc.weight, delay_fut)) {
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
    let mut current_weight = 0;

    runtime.block_on(async move {
        // Record values that have been completed in this map.
        let mut stream = stream.future_queue(state.max_weight);
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

                            // Check that the current weight doesn't go over the limit.
                            assert!(
                                current_weight < state.max_weight,
                                "current weight {} exceeds max weight {}",
                                current_weight,
                                state.max_weight,
                            );
                            current_weight += desc.weight;
                        }
                        Some(FutureEvent::Finished(id, desc)) => {
                            // Record that this value was completed.
                            completed_map[id] = true;
                            current_weight -= desc.weight;
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
