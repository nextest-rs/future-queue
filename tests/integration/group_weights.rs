// Copyright (c) The future-queue Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use future_queue::{FutureQueueContext, StreamExt as _};
use futures::{channel::oneshot, executor::block_on, stream, FutureExt, StreamExt as _};

#[test]
fn group_weights_track_running_futures() {
    let (send_small, recv_small) = oneshot::channel::<()>();
    let (send_large, recv_large) = oneshot::channel::<()>();
    let items = vec![
        (2, Some("group1"), recv_small),
        // A weight above the group max (for group2, 3, as configured below) is
        // clamped to the group max.
        (7, Some("group2"), recv_large),
    ];
    let mut queue = stream::iter(items)
        .map(|(weight, group, future)| (weight, group, move |_cx: FutureQueueContext| future))
        .future_queue_grouped(10, [("group1", 5), ("group2", 3)]);

    assert_eq!(queue.current_group_weight("group1"), Some(0));
    assert_eq!(queue.current_group_weight("group2"), Some(0));
    assert_eq!(queue.max_group_weight("group1"), Some(5));
    assert_eq!(queue.max_group_weight("group2"), Some(3));
    assert_eq!(queue.current_group_weight("unknown"), None);
    assert_eq!(queue.max_group_weight("unknown"), None);

    assert_eq!(queue.next().now_or_never(), None, "no future is ready yet");
    assert_eq!(queue.current_group_weight("group1"), Some(2));
    assert_eq!(queue.current_group_weight("group2"), Some(3));
    assert_eq!(queue.current_global_weight(), 9);

    send_small.send(()).expect("receiver is held by the queue");
    assert_eq!(block_on(queue.next()), Some(Ok(())));
    assert_eq!(queue.current_group_weight("group1"), Some(0));
    assert_eq!(queue.current_group_weight("group2"), Some(3));

    send_large.send(()).expect("receiver is held by the queue");
    assert_eq!(block_on(queue.next()), Some(Ok(())));
    assert_eq!(queue.current_group_weight("group2"), Some(0));
    assert_eq!(block_on(queue.next()), None);
}
