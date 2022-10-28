use buffer_unordered_weighted::StreamExt as _;
use futures::{stream, StreamExt as _};
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use std::time::Duration;

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
    delay: Duration,
    #[proptest(strategy = "0usize..8")]
    weight: usize,
}

fn duration_strategy() -> BoxedStrategy<Duration> {
    // Allow for a delay between 0ms and 1000ms uniformly at random.
    (0u64..1000).prop_map(Duration::from_millis).boxed()
}

proptest! {
    #[test]
    fn proptest_buffer_unordered(state: TestState) {
        proptest_buffer_unordered_impl(state)
    }
}

#[derive(Clone, Copy, Debug)]
enum FutureEvent {
    Started(usize, TestFutureDesc),
    Finished(usize, TestFutureDesc),
}

fn proptest_buffer_unordered_impl(state: TestState) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("tokio builder succeeded");
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let futures = state
        .future_descriptions
        .iter()
        .enumerate()
        .map(|(id, desc)| {
            let sender = sender.clone();
            // For each description, create a future.
            let delay_fut = async move {
                // Send the fact that this future started to the mpsc queue.
                sender
                    .send(FutureEvent::Started(id, *desc))
                    .expect("receiver held open by loop");
                tokio::time::sleep(desc.delay).await;
                sender
                    .send(FutureEvent::Finished(id, *desc))
                    .expect("receiver held open by loop");
            };
            (desc.weight, delay_fut)
        });
    let stream = stream::iter(futures);

    let mut completed_map = vec![false; state.future_descriptions.len()];
    let mut last_started_id: Option<usize> = None;
    let mut current_weight = 0;

    runtime.block_on(async move {
        // Record values that have been completed in this map.
        let mut stream = stream.buffer_unordered_weighted(state.max_weight);
        loop {
            tokio::select! {
                // biased ensures that the receiver is drained before the stream is polled. Without
                // it, it's possible that we fail to record the completion of some futures in status_map.
                biased;

                recv = receiver.recv() => {
                    match recv {
                        Some(FutureEvent::Started(id, desc)) => {
                            // last_started_id must be 1 less than id.
                            let expected_id = last_started_id.map_or(0, |id| id + 1);
                            assert_eq!(expected_id, id, "expected future id to start != actual id that started");
                            last_started_id = Some(id);

                            // Check that the current weight doesn't go over the limit.
                            assert!(
                                current_weight < state.max_weight,
                                "current weight {current_weight} exceeds max weight {}",
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
            .filter_map(|(n, &v)| (!v).then(|| format!("{n}")))
            .collect();
        if !not_completed.is_empty() {
            let not_completed_ids = not_completed.join(", ");
            panic!("some futures did not complete: {not_completed_ids}");
        }
    })
}
