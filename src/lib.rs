use fnv::FnvHashMap;
use futures_util::{
    stream::{Fuse, FuturesUnordered},
    Future, Stream, StreamExt as _,
};
use pin_project_lite::pin_project;
use std::{
    fmt,
    hash::Hash,
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
    /// use buffer_unordered_weighted::{FutureWithId, StreamExt as _};
    ///
    /// let (send_one, recv_one) = oneshot::channel();
    /// let (send_two, recv_two) = oneshot::channel();
    ///
    /// let recv_one = FutureWithId::new(0u32, recv_one);
    /// let recv_two = FutureWithId::new(1u32, recv_two);
    ///
    /// let stream_of_futures = stream::iter(vec![(1usize, 0u32, recv_one), (2, 1u32, recv_two)]);
    /// let mut buffered = stream_of_futures.buffer_unordered_weighted::<u32>(10);
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
    fn buffer_unordered_weighted<Id>(self, n: usize) -> BufferUnorderedWeighted<Self>
    where
        Self: Sized,
        Self::Item: WeightedFuture<Id = Id>,
        Id: Hash + Eq,
        <<Self::Item as WeightedFuture>::Fut as Future>::Output: WeightedOutput<Id = Id>,
    {
        assert_stream::<
            <<<Self::Item as WeightedFuture>::Fut as Future>::Output as WeightedOutput>::Output,
            _,
        >(BufferUnorderedWeighted::new(self, n))
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
        in_progress_queue: FuturesUnordered<<St::Item as WeightedFuture>::Fut>,
        max: usize,
        // Invariant: sum of all the weights in in_progress_ids is the same as in_progress_issued
        in_progress_ids: FnvHashMap<<St::Item as WeightedFuture>::Id, usize>,
        // isize because this can go into the negatives
        remaining_permits: isize,
    }
}

impl<St> fmt::Debug for BufferUnorderedWeighted<St>
where
    St: Stream + fmt::Debug,
    St::Item: WeightedFuture,
    <St::Item as WeightedFuture>::Id: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferUnorderedWeighted")
            .field("stream", &self.stream)
            .field("in_progress_queue", &self.in_progress_queue)
            .field("max", &self.max)
            .field("in_progress_ids", &self.in_progress_ids)
            .finish()
    }
}

impl<St> BufferUnorderedWeighted<St>
where
    St: Stream,
    St::Item: WeightedFuture,
{
    pub(crate) fn new(stream: St, n: usize) -> Self {
        assert!(
            n < usize::MAX << 1,
            "buffer_unordered parallelism is {n}, must be smaller than {}",
            usize::MAX << 1,
        );
        Self {
            stream: stream.fuse(),
            in_progress_queue: FuturesUnordered::new(),
            max: n,
            in_progress_ids: FnvHashMap::with_hasher(Default::default()),
            remaining_permits: n as isize,
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

impl<St, Id> Stream for BufferUnorderedWeighted<St>
where
    St: Stream,
    St::Item: WeightedFuture<Id = Id>,
    Id: Hash + Eq,
    <<St::Item as WeightedFuture>::Fut as Future>::Output: WeightedOutput<Id = Id>,
{
    type Item = <<<St::Item as WeightedFuture>::Fut as Future>::Output as WeightedOutput>::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures.
        while *this.remaining_permits > 0 {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(weighted_fut)) => {
                    let (weight, id, fut) = weighted_fut.into_components();
                    this.in_progress_ids.insert(id, weight);
                    *this.remaining_permits -= weight as isize;
                    this.in_progress_queue.push(fut);
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // Attempt to pull the next value from the in_progress_queue
        match this.in_progress_queue.poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(output)) => {
                let (id, output) = output.into_components();
                this.in_progress_ids
                    .remove(&id)
                    .expect("id not found in in_progress_ids map");
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
    type Fut: Future;
    type Id: Hash + Eq;

    fn into_components(self) -> (usize, Self::Id, Self::Fut);
}

impl<Fut, Id> WeightedFuture for (usize, Id, Fut)
where
    Fut: Future,
    Id: Hash + Eq,
{
    type Fut = Fut;
    type Id = Id;

    #[inline]
    fn into_components(self) -> (usize, Self::Id, Self::Fut) {
        self
    }
}

pub trait WeightedOutput {
    type Output;
    type Id: Hash + Eq;

    fn into_components(self) -> (Self::Id, Self::Output);
}

impl<Output, Id> WeightedOutput for (Id, Output)
where
    Id: Hash + Eq,
{
    type Output = Output;
    type Id = Id;

    #[inline]
    fn into_components(self) -> (Self::Id, Self::Output) {
        self
    }
}

pin_project! {
    #[must_use = "futures do nothing unless polled"]
    pub struct FutureWithId<Id, Fut> {
        id: Option<Id>,
        #[pin]
        future: Fut,
    }
}

impl<Id, Fut> FutureWithId<Id, Fut> {
    pub fn new(id: Id, future: Fut) -> Self {
        Self {
            id: Some(id),
            future,
        }
    }
}

impl<Id, Fut> Future for FutureWithId<Id, Fut>
where
    Fut: Future,
{
    type Output = (Id, Fut::Output);
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if this.id.is_none() {
            // This future has completed.
            return Poll::Pending;
        }

        match this.future.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => Poll::Ready((
                this.id
                    .take()
                    .expect("already checked that self.id is None above"),
                output,
            )),
        }
    }
}

pub(crate) fn assert_stream<T, S>(stream: S) -> S
where
    S: Stream<Item = T>,
{
    stream
}
