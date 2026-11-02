// Copyright (c) The future-queue Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use future_queue::{FutureQueueContext, StreamExt as _};
use futures::{future::BoxFuture, FutureExt, StreamExt as _};
use std::time::Duration;
use tokio::time::Instant;
use tokio_stream::wrappers::UnboundedReceiverStream;

fn item(
    weight: usize,
    group: Option<&'static str>,
    name: &'static str,
    delay: Duration,
) -> (
    usize,
    Option<&'static str>,
    impl FnOnce(FutureQueueContext) -> BoxFuture<'static, &'static str>,
) {
    (weight, group, move |_cx: FutureQueueContext| {
        async move {
            tokio::time::sleep(delay).await;
            name
        }
        .boxed()
    })
}

// An item arriving while futures run must start right away, not when one
// completes.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn grouped_picks_up_items_arriving_while_futures_run() {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut queue = UnboundedReceiverStream::new(receiver).future_queue_grouped(4, [("group1", 1)]);
    queue.set_extra_verify(true);

    let start = Instant::now();
    sender
        .send(item(1, None, "long", Duration::from_secs(100)))
        .expect("receiver is held by the queue");

    let late_sender = sender.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        late_sender
            .send(item(1, Some("group1"), "late", Duration::from_millis(10)))
            .expect("receiver is held by the queue");
    });
    drop(sender);

    assert_eq!(queue.next().await, Some("late"));
    assert_eq!(start.elapsed(), Duration::from_millis(1010));
    assert_eq!(queue.next().await, Some("long"));
    assert_eq!(start.elapsed(), Duration::from_secs(100));
    assert_eq!(queue.next().await, None);
}
