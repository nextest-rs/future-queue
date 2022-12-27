// Copyright (c) The future-queue Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `future_queue` is a variant of
//! [`buffer_unordered`](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html#method.buffer_unordered),
//! where each future can be assigned a different weight.
//!
//! This crate is part of the [nextest organization](https://github.com/nextest-rs) on GitHub, and is
//! designed to serve the needs of [cargo-nextest](https://nexte.st).
//!
//! # Motivation
//!
//! Async programming in Rust often uses an adaptor called
//! [`buffer_unordered`](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html#method.buffer_unordered):
//! this adaptor takes a stream of futures[^1], and executes all the futures limited to a maximum
//! amount of concurrency.
//!
//! * Futures are started in the order the stream returns them in.
//! * Once started, futures are polled simultaneously, and completed future outputs are returned
//!   in arbitrary order (hence the `unordered`).
//!
//! Common use cases for `buffer_unordered` include:
//!
//! * Sending network requests concurrently, but limiting the amount of concurrency to avoid
//!   overwhelming the remote server.
//! * Running tests with a tool like [cargo-nextest](https://nexte.st).
//!
//! `buffer_unordered` works well for many use cases. However, one issue with it is that it treats
//! all futures as equally taxing: there's no way to say that some futures consume more resources
//! than others. For nextest in particular, some tests can be much heavier than others, and fewer of
//! those tests should be run simultaneously.
//!
//! [^1]: This adaptor takes a stream of futures for maximum generality. In practice this is often
//!     an *iterator* of futures, converted over using
//!     [`stream::iter`](https://docs.rs/futures/latest/futures/stream/fn.iter.html).
//!
//! # About this crate
//!
//! This crate provides an adaptor on streams called `future_queue`, which can run
//! several futures simultaneously, limiting the concurrency to a maximum *weight*.
//!
//! Rather than taking a stream of futures, this adaptor takes a stream of `(usize, future)` pairs,
//! where the `usize` indicates the weight of each future. This adaptor will schedule and buffer
//! futures to be run until the maximum weight is exceeded. Once that happens, this adaptor will
//! wait until some of the currently executing futures complete, and the current weight of running
//! futures drops below the maximum weight, before scheduling new futures.
//!
//! Note that in some cases, the current weight may exceed the maximum weight. For example:
//!
//! * Let's say the maximum weight is **24**, and the current weight is **20**.
//! * If the next future has weight **6**, then it will be scheduled and the current weight will become **26**.
//! * No new futures will be scheduled until the current weight falls to **23** or below.
//!
//! It is possible to have a variant of this adaptor which always stays below the limit and holds
//! the next future in abeyance; however, the implementation for that variant is a bit more
//! complicated, and is also not the behavior desired by nextest. This variant may be provided in
//! the future.
//!
//! The weight of a future can be zero, in which case it doesn't count towards the maximum weight.
//!
//! If all weights are 1, then `future_queue` is exactly the same as `buffer_unordered`.
//!
//! # Examples
//!
//! ```
//! # futures::executor::block_on(async {
//! use futures::{channel::oneshot, stream, StreamExt as _};
//! use future_queue::{StreamExt as _};
//!
//! let (send_one, recv_one) = oneshot::channel();
//! let (send_two, recv_two) = oneshot::channel();
//!
//! let stream_of_futures = stream::iter(vec![(1, recv_one), (2, recv_two)]);
//! let mut queue = stream_of_futures.future_queue(10);
//!
//! send_two.send("hello")?;
//! assert_eq!(queue.next().await, Some(Ok("hello")));
//!
//! send_one.send("world")?;
//! assert_eq!(queue.next().await, Some(Ok("world")));
//!
//! assert_eq!(queue.next().await, None);
//! # Ok::<(), &'static str>(()) }).unwrap();
//! ```
//!
//! # Minimum supported Rust version (MSRV)
//!
//! The minimum supported Rust version is **Rust 1.56.**
//!
//! The MSRV will likely not change in the medium term, but while this crate is a pre-release
//! (0.x.x) it may have its MSRV bumped in a patch release. Once this crate has reached 1.x, any
//! MSRV bump will be accompanied with a new minor version.
//!
//! # Notes
//!
//! This crate used to be called `buffer-unordered-weighted`. It was renamed to `future-queue` to be
//! more descriptive about what the crate does rather than how it's implemented.

mod future_queue;

pub use crate::future_queue::FutureQueue;

/// Traits to aid in type definitions.
///
/// These traits are normally not required by end-user code, but may be necessary for some generic
/// code.
pub mod traits {
    pub use crate::future_queue::WeightedFuture;
}

use futures_util::{Future, Stream};

impl<T: ?Sized> StreamExt for T where T: Stream {}

/// An extension trait for `Stream`s that provides
/// [`future_queue`](StreamExt::future_queue).
pub trait StreamExt: Stream {
    /// An adaptor for creating a buffered list of pending futures (unordered), where
    /// each future has a different weight.
    ///
    /// This stream must return values of type `(usize, impl Future)`, where the `usize` indicates
    /// the weight of each future. This adaptor will buffer futures up to weight `max_weight`, and
    /// then return the outputs in the order in which they complete.
    ///
    /// The weight may be exceeded if the last future to be queued has a weight greater than
    /// `max_weight` minus the total weight of currently executing futures. However, no further
    /// futures will be queued until the total weights of running futures falls below `max_weight`.
    ///
    /// The adaptor will schedule futures in the order they're returned by the stream, without doing
    /// any reordering based on weight.
    ///
    /// The weight of a future can be zero, in which case it will not count towards the total weight.
    ///
    /// The returned stream will be a stream of each future's output.
    ///
    /// # Examples
    ///
    /// See [the crate documentation](crate#examples) for an example.
    fn future_queue<Fut>(self, max_weight: usize) -> FutureQueue<Self>
    where
        Self: Sized + Stream<Item = (usize, Fut)>,
        Fut: Future,
    {
        assert_stream::<Fut::Output, _>(FutureQueue::new(self, max_weight))
    }
}

pub(crate) fn assert_stream<T, S>(stream: S) -> S
where
    S: Stream<Item = T>,
{
    stream
}
