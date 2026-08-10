# Contributing

Thank you for contributing to Apache OpenDAL™ File System.

## Set up the repository

Install Rust with [rustup]. Rustup reads the repository's
`rust-toolchain.toml`, selects the stable toolchain, and installs rustfmt and
Clippy automatically. The minimum supported Rust version is 1.91.0, as
configured in `Cargo.toml`.

The lint and compliance tasks also use Taplo, typos, Hawkeye, and cargo-deny.
Install them before running the complete check suite:

```shell
cargo install --locked taplo-cli
cargo install --locked typos-cli
cargo install --locked hawkeye
cargo install --locked cargo-deny --version 0.14.22
```

## Run repository tasks

The workspace provides `cargo x` as the shared entry point for local development
and CI. Run `cargo x --help` to list its commands.

- `cargo x check` checks all workspace targets.
- `cargo x test` runs all workspace tests.
- `cargo x lint` checks Rust formatting, Clippy, documentation, TOML formatting,
  and spelling.
- `cargo x licenses` checks source headers and dependency licenses.

Use `cargo x lint --fix` to apply the available formatting and Clippy fixes.

Before opening or updating a pull request, run:

```shell
cargo x lint
cargo x check
cargo x test
cargo x licenses
```

## Prepare a change

Keep runtime design, implementation, documentation, and repository tooling in
separate commits when practical. Use semantic commit and pull request titles,
for example `feat: add filesystem contract` or `ci: check dependency licenses`.

All contributors must follow the [Apache Software Foundation Code of Conduct].

[apache software foundation code of conduct]: https://www.apache.org/foundation/policies/conduct
[rustup]: https://rustup.rs/
