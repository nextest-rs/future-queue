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

impl<T: ?Sized> StreamExt for T where T: Stream {}

/// An extension trait for `Stream`s that provides `buffer_unordered_weighted`.
pub trait StreamExt: Stream {
    /// An adaptor for creating a buffered list of pending futures (unordered), where
    /// each future has a different weight.
    ///
    /// If this stream's item can be converted into a [`WeightedFuture`], then this adaptor
    /// will buffer futures up to weight `n`, possibly (TODO)
    /// and then return the outputs in the order in which they complete. Futures with total weight
    /// less than `n` may also be buffered depending on the state of each future.
    ///
    /// The returned stream will be a stream of each future's output.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::{channel::oneshot, stream, FutureExt as _, StreamExt as _};
    /// use buffer_unordered_weighted::{StreamExt as _};
    ///
    /// let (send_one, recv_one) = oneshot::channel();
    /// let (send_two, recv_two) = oneshot::channel();
    ///
    /// let stream_of_futures = stream::iter(vec![(1, recv_one), (2, recv_two)]);
    /// let mut buffered = stream_of_futures.buffer_unordered_weighted(10);
    ///
    /// send_two.send("hello")?;
    /// assert_eq!(buffered.next().await, Some(Ok("hello")));
    ///
    /// send_one.send("world")?;
    /// assert_eq!(buffered.next().await, Some(Ok("world")));
    ///
    /// assert_eq!(buffered.next().await, None);
    /// # Ok::<(), &'static str>(()) }).unwrap();
    /// ```
    fn buffer_unordered_weighted(self, n: usize) -> BufferUnorderedWeighted<Self>
    where
        Self: Sized,
        Self::Item: WeightedFuture,
    {
        assert_stream::<<<Self::Item as WeightedFuture>::Future as Future>::Output, _>(
            BufferUnorderedWeighted::new(self, n),
        )
    }
}

pin_project! {
    /// Stream for the [`buffer_unordered_weighted`](super::StreamExt::buffer_unordered)
    /// method.
    #[must_use = "streams do nothing unless polled"]
    pub struct BufferUnorderedWeighted<St>
    where
        St: Stream,
        St::Item: WeightedFuture,
     {
        #[pin]
        stream: Fuse<St>,
        in_progress_queue: FuturesUnordered<FutureWithWeight<<St::Item as WeightedFuture>::Future>>,
        max: usize,
        permits_issued: usize,
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
            .field("max", &self.max)
            .field("permits_issued", &self.permits_issued)
            .finish()
    }
}

impl<St> BufferUnorderedWeighted<St>
where
    St: Stream,
    St::Item: WeightedFuture,
{
    pub(crate) fn new(stream: St, n: usize) -> Self {
        Self {
            stream: stream.fuse(),
            in_progress_queue: FuturesUnordered::new(),
            max: n,
            permits_issued: 0,
        }
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
        while *this.permits_issued < *this.max {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(weighted_future)) => {
                    let (weight, future) = weighted_future.into_components();
                    *this.permits_issued =
                        this.permits_issued.checked_add(weight).unwrap_or_else(|| {
                            panic!(
                                "buffer_unordered_weighted: added weight {weight} to issued permits {}, overflowed",
                                this.permits_issued
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
                *this.permits_issued = this.permits_issued.checked_sub(weight).unwrap_or_else(|| {
                    panic!(
                        "buffer_unordered_weighted: subtracted weight {weight} from issued permits {}, overflowed",
                        this.permits_issued
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
