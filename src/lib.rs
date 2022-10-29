// Copyright (c) The buffer-unordered-weighted Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! `buffer_unordered_weighted` is a variant of
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
//! This crate provides an adapter on streams called `buffer_unordered_weighted`, which can run
//! several futures simultaneously, limiting the concurrency to a maximum *weight*.
//!
//! Rather than taking a stream of futures, this adaptor takes a stream of `(usize, future)` pairs,
//! where the `usize` indicates the weight of each future. This adapter will run and buffer up
//! futures until the maximum weight is exceeded. After that, futures will only be polled after the
//! current weight of running futures drops below the maximum weight.
//!
//! Note that in some cases, the current weight may exceed the maximum weight.
//!
//! * For example, let's say the maximum weight is 24, and the current weight is 20. If the next
//! future has weight 5, then it will be buffered and the current weight will become 25. No further
//! futures will be buffered until the current weight falls to 23 or below.
//!
//! It is possible to have a variant which always stays below the limit and hold the next future in
//! abeyance; however, the implementation for that variant is a bit more complicated, and is also
//! not the behavior desired by nextest. An adaptor to do so may be provided in the future.
//!
//! The weight of a future can even be zero, in which case it doesn't count towards the maximum
//! weight.
//!
//! If all weights are 1, then `buffer_unordered_weighted` is exactly the same as `buffer_unordered`.
//!
//! # Examples
//!
//! ```
//! # futures::executor::block_on(async {
//! use futures::{channel::oneshot, stream, StreamExt as _};
//! use buffer_unordered_weighted::{StreamExt as _};
//!
//! let (send_one, recv_one) = oneshot::channel();
//! let (send_two, recv_two) = oneshot::channel();
//!
//! let stream_of_futures = stream::iter(vec![(1, recv_one), (2, recv_two)]);
//! let mut buffered = stream_of_futures.buffer_unordered_weighted(10);
//!
//! send_two.send("hello")?;
//! assert_eq!(buffered.next().await, Some(Ok("hello")));
//!
//! send_one.send("world")?;
//! assert_eq!(buffered.next().await, Some(Ok("world")));
//!
//! assert_eq!(buffered.next().await, None);
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

use futures_util::{
    stream::{Fuse, FuturesUnordered},
    Future, Stream, StreamExt as _,
};
use pin_project_lite::pin_project;
use private::WeightedFuture;
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

impl<T: ?Sized> StreamExt for T where T: Stream {}

/// An extension trait for `Stream`s that provides
/// [`buffer_unordered_weighted`](StreamExt::buffer_unordered_weighted).
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
    /// The adaptor will buffer futures in the order they're returned by the stream, without doing
    /// any reordering based on weight.
    ///
    /// The weight of a future can be 0, in which case it will not count towards the total weight.
    ///
    /// The returned stream will be a stream of each future's output.
    ///
    /// # Examples
    ///
    /// See [the crate documentation](crate#examples) for an example.
    ///
    fn buffer_unordered_weighted<Fut>(self, max_weight: usize) -> BufferUnorderedWeighted<Self>
    where
        Self: Sized + Stream<Item = (usize, Fut)>,
        Fut: Future,
    {
        assert_stream::<Fut::Output, _>(BufferUnorderedWeighted::new(self, max_weight))
    }
}

pin_project! {
    /// Stream for the [`buffer_unordered_weighted`](StreamExt::buffer_unordered_weighted) method.
    #[must_use = "streams do nothing unless polled"]
    pub struct BufferUnorderedWeighted<St>
    where
        St: Stream,
        St::Item: WeightedFuture,
     {
        #[pin]
        stream: Fuse<St>,
        in_progress_queue: FuturesUnordered<FutureWithWeight<<St::Item as WeightedFuture>::Future>>,
        max_weight: usize,
        current_weight: usize,
    }
}

impl<St> fmt::Debug for BufferUnorderedWeighted<St>
where
    St: Stream + fmt::Debug,
    St::Item: WeightedFuture,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferUnorderedWeighted")
            .field("stream", &self.stream)
            .field("in_progress_queue", &self.in_progress_queue)
            .field("max_weight", &self.max_weight)
            .field("current_weight", &self.current_weight)
            .finish()
    }
}

impl<St> BufferUnorderedWeighted<St>
where
    St: Stream,
    St::Item: WeightedFuture,
{
    pub(crate) fn new(stream: St, max_weight: usize) -> Self {
        Self {
            stream: stream.fuse(),
            in_progress_queue: FuturesUnordered::new(),
            max_weight,
            current_weight: 0,
        }
    }

    /// Returns the maximum weight of futures allowed to be run by this adaptor.
    pub fn max_weight(&self) -> usize {
        self.max_weight
    }

    /// Returns the currently running weight of futures.
    pub fn current_weight(&self) -> usize {
        self.current_weight
    }

    /// Acquires a reference to the underlying sink or stream that this combinator is
    /// pulling from.
    pub fn get_ref(&self) -> &St {
        self.stream.get_ref()
    }

    /// Acquires a mutable reference to the underlying sink or stream that this
    /// combinator is pulling from.
    ///
    /// Note that care must be taken to avoid tampering with the state of the
    /// sink or stream which may otherwise confuse this combinator.
    pub fn get_mut(&mut self) -> &mut St {
        self.stream.get_mut()
    }

    /// Acquires a pinned mutable reference to the underlying sink or stream that this
    /// combinator is pulling from.
    ///
    /// Note that care must be taken to avoid tampering with the state of the
    /// sink or stream which may otherwise confuse this combinator.
    pub fn get_pin_mut(self: Pin<&mut Self>) -> core::pin::Pin<&mut St> {
        self.project().stream.get_pin_mut()
    }

    /// Consumes this combinator, returning the underlying sink or stream.
    ///
    /// Note that this may discard intermediate state of this combinator, so
    /// care should be taken to avoid losing resources when this is called.
    pub fn into_inner(self) -> St {
        self.stream.into_inner()
    }
}

impl<St> Stream for BufferUnorderedWeighted<St>
where
    St: Stream,
    St::Item: WeightedFuture,
{
    type Item = <<St::Item as WeightedFuture>::Future as Future>::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures.
        while *this.current_weight < *this.max_weight {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(weighted_future)) => {
                    let (weight, future) = weighted_future.into_components();
                    *this.current_weight =
                        this.current_weight.checked_add(weight).unwrap_or_else(|| {
                            panic!(
                                "buffer_unordered_weighted: added weight {} to current {}, overflowed",
                                weight,
                                this.current_weight,
                            )
                        });
                    this.in_progress_queue
                        .push(FutureWithWeight::new(weight, future));
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // Attempt to pull the next value from the in_progress_queue
        match this.in_progress_queue.poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some((weight, output))) => {
                *this.current_weight = this.current_weight.checked_sub(weight).unwrap_or_else(|| {
                    panic!(
                        "buffer_unordered_weighted: subtracted weight {} from current {}, overflowed",
                        weight,
                        this.current_weight,
                    )
                });
                return Poll::Ready(Some(output));
            }
            Poll::Ready(None) => {}
        }

        // If more values are still coming from the stream, we're not done yet
        if this.stream.is_done() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let queue_len = self.in_progress_queue.len();
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(queue_len);
        let upper = match upper {
            Some(x) => x.checked_add(queue_len),
            None => None,
        };
        (lower, upper)
    }
}

mod private {
    use futures_util::Future;

    pub trait WeightedFuture {
        type Future: Future;

        fn into_components(self) -> (usize, Self::Future);
    }

    impl<Fut> WeightedFuture for (usize, Fut)
    where
        Fut: Future,
    {
        type Future = Fut;

        #[inline]
        fn into_components(self) -> (usize, Self::Future) {
            self
        }
    }
}

pin_project! {
    #[must_use = "futures do nothing unless polled"]
    struct FutureWithWeight<Fut> {
        #[pin]
        future: Fut,
        weight: usize,
    }
}

impl<Fut> FutureWithWeight<Fut> {
    pub fn new(weight: usize, future: Fut) -> Self {
        Self { future, weight }
    }
}

impl<Fut> Future for FutureWithWeight<Fut>
where
    Fut: Future,
{
    type Output = (usize, Fut::Output);
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.future.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => Poll::Ready((*this.weight, output)),
        }
    }
}

pub(crate) fn assert_stream<T, S>(stream: S) -> S
where
    S: Stream<Item = T>,
{
    stream
}
