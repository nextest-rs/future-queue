// Copyright (c) The future-queue Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use futures_util::{
    stream::{Fuse, FuturesUnordered},
    Future, Stream, StreamExt as _,
};
use pin_project_lite::pin_project;
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

pin_project! {
    /// Stream for the [`future_queue`](crate::StreamExt::future_queue) method.
    #[must_use = "streams do nothing unless polled"]
    pub struct FutureQueue<St>
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

impl<St> fmt::Debug for FutureQueue<St>
where
    St: Stream + fmt::Debug,
    St::Item: WeightedFuture,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FutureQueue")
            .field("stream", &self.stream)
            .field("in_progress_queue", &self.in_progress_queue)
            .field("max_weight", &self.max_weight)
            .field("current_weight", &self.current_weight)
            .finish()
    }
}

impl<St> FutureQueue<St>
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

impl<St> Stream for FutureQueue<St>
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
                                "future_queue: added weight {} to current {}, overflowed",
                                weight, this.current_weight,
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
                *this.current_weight =
                    this.current_weight.checked_sub(weight).unwrap_or_else(|| {
                        panic!(
                            "future_queue: subtracted weight {} from current {}, overflowed",
                            weight, this.current_weight,
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

/// A trait for types which can be converted into a `Future` and a weight.
///
/// Provided in case it's necessary. This trait is only implemented for `(usize, impl Future)`.
pub trait WeightedFuture: private::Sealed {
    /// The associated `Future` type.
    type Future: Future;

    /// Turns self into its components.
    fn into_components(self) -> (usize, Self::Future);
}

mod private {
    pub trait Sealed {}
}

impl<Fut> private::Sealed for (usize, Fut) where Fut: Future {}

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
