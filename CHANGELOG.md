# Changelog

## [0.3.0] - 2023-03-18

### Changed

- Changed definitions of `future_queue` and `future_queue_grouped` such that weights can no longer be
  exceeded. This is easier to explain and enables certain additional use cases in nextest.

## [0.2.2] - 2022-12-27

### Added

- Added documentation link to Cargo.toml metadata.

## [0.2.1] - 2022-12-27

## Changed

- Internal change: switch to `FnvHashMap` for `future_queue_grouped`.

## [0.2.0] - 2022-12-27

## Added

- New adaptor `future_queue_grouped` is similar to `future_queue`, except it allows an additional *group* to be specified.

## Changed

- Crate renamed to `future_queue`.
- `buffer_unordered_weighted` renamed to `future_queue`.

## [0.1.2] - 2022-11-01

- Add repository link.

## [0.1.1] - 2022-10-29

- Documentation updates.

## [0.1.0] - 2022-10-28

- Initial release.

[0.3.0]: https://github.com/nextest-rs/future-queue/releases/tag/0.3.0
[0.2.2]: https://github.com/nextest-rs/future-queue/releases/tag/0.2.2
[0.2.1]: https://github.com/nextest-rs/future-queue/releases/tag/0.2.1
[0.2.0]: https://github.com/nextest-rs/future-queue/releases/tag/0.2.0
[0.1.2]: https://github.com/nextest-rs/future-queue/releases/tag/0.1.2
[0.1.1]: https://github.com/nextest-rs/future-queue/releases/tag/0.1.1
[0.1.0]: https://github.com/nextest-rs/future-queue/releases/tag/0.1.0
