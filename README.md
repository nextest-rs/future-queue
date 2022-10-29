# buffer-unordered-weighted

[![buffer-unordered-weighted on crates.io](https://img.shields.io/crates/v/buffer-unordered-weighted)](https://crates.io/crates/buffer-unordered-weighted)
[![Documentation (latest release)](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://docs.rs/buffer-unordered-weighted/)
[![Documentation (main)](https://img.shields.io/badge/docs-main-purple)](https://nextest-rs.github.io/buffer-unordered-weighted/rustdoc/buffer_unordered_weighted)
[![Changelog](https://img.shields.io/badge/changelog-latest-blue)](CHANGELOG.md)
[![License](https://img.shields.io/badge/license-Apache-green.svg)](LICENSE-APACHE)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE-MIT)

`buffer_unordered_weighted` is a variant of
[`buffer_unordered`](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html#method.buffer_unordered),
where each future can be assigned a different weight.

This crate is part of the [nextest organization](https://github.com/nextest-rs) on GitHub, and is
designed to serve the needs of [cargo-nextest](https://nexte.st).

## Motivation

Async programming in Rust often uses an adaptor called
[`buffer_unordered`](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html#method.buffer_unordered):
this adaptor takes a stream of futures[^1], and executes all the futures limited to a maximum
amount of concurrency.

* Futures are started in the order the stream returns them in.
* Once started, futures are polled simultaneously, and completed future outputs are returned
  in arbitrary order (hence the `unordered`).

Common use cases for `buffer_unordered` include:

* Sending network requests concurrently, but limiting the amount of concurrency to avoid
  overwhelming the remote server.
* Running tests with a tool like [cargo-nextest](https://nexte.st).

`buffer_unordered` works well for many use cases. However, one issue with it is that it treats
all futures as equally taxing: there's no way to say that some futures consume more resources
than others. For nextest in particular, some tests can be much heavier than others, and fewer of
those tests should be run simultaneously.

[^1]: This adaptor takes a stream of futures for maximum generality. In practice this is often
    an *iterator* of futures, converted over using
    [`stream::iter`](https://docs.rs/futures/latest/futures/stream/fn.iter.html).

## About this crate

This crate provides an adaptor on streams called `buffer_unordered_weighted`, which can run
several futures simultaneously, limiting the concurrency to a maximum *weight*.

Rather than taking a stream of futures, this adaptor takes a stream of `(usize, future)` pairs,
where the `usize` indicates the weight of each future. This adaptor will schedule and buffer
futures to be run until the maximum weight is exceeded. Once that happens, this adaptor will
wait until some of the currently executing futures complete, and the current weight of running
futures drops below the maximum weight, before scheduling new futures.

Note that in some cases, the current weight may exceed the maximum weight. For example:

* Let's say the maximum weight is **24**, and the current weight is **20**.
* If the next future has weight **6**, then it will be scheduled and the current weight will become **26**.
* No new futures will be scheduled until the current weight falls to **23** or below.

It is possible to have a variant of this adaptor which always stays below the limit and holds
the next future in abeyance; however, the implementation for that variant is a bit more
complicated, and is also not the behavior desired by nextest. This variant may be provided in
the future.

The weight of a future can be zero, in which case it doesn't count towards the maximum weight.

If all weights are 1, then `buffer_unordered_weighted` is exactly the same as `buffer_unordered`.

## Examples

```rust
use futures::{channel::oneshot, stream, StreamExt as _};
use buffer_unordered_weighted::{StreamExt as _};

let (send_one, recv_one) = oneshot::channel();
let (send_two, recv_two) = oneshot::channel();

let stream_of_futures = stream::iter(vec![(1, recv_one), (2, recv_two)]);
let mut buffered = stream_of_futures.buffer_unordered_weighted(10);

send_two.send("hello")?;
assert_eq!(buffered.next().await, Some(Ok("hello")));

send_one.send("world")?;
assert_eq!(buffered.next().await, Some(Ok("world")));

assert_eq!(buffered.next().await, None);
```

## Minimum supported Rust version (MSRV)

The minimum supported Rust version is **Rust 1.56.**

The MSRV will likely not change in the medium term, but while this crate is a pre-release
(0.x.x) it may have its MSRV bumped in a patch release. Once this crate has reached 1.x, any
MSRV bump will be accompanied with a new minor version.


## Contributing

See the [CONTRIBUTING](CONTRIBUTING.md) file for how to help out.

## License

This project is available under the terms of either the [Apache 2.0 license](LICENSE-APACHE) or
the [MIT license](LICENSE-MIT).

<!--
README.md is generated from README.tpl by cargo readme. To regenerate, run from the repository root:

./scripts/regenerate-readmes.sh
-->
