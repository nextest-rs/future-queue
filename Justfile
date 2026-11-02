set positional-arguments

# Note: help messages should be 1 line long as required by just.

# Print a help message.
help:
    just --list

# Generate README.md using `cargo-sync-rdme`.
generate-readmes:
    cargo +stable sync-rdme --toolchain nightly-2026-09-30 --all-features
