# Repository instructions for agents

Read [CONTRIBUTING.md](CONTRIBUTING.md) before planning or modifying this
repository.

## Repository workflow

Use `cargo x` as the source of truth for repository tasks. Run `cargo x --help`
before choosing a command and `cargo x <command> --help` for command-specific
usage.

Before handing work back, run:

```shell
cargo x lint
cargo x check
cargo x test
cargo x licenses
```

Keep commits focused and use semantic commit and pull request titles such as
`feat:`, `fix:`, `docs:`, `build:`, or `ci:`.

## Important notes

- Minimum Rust version is 1.91.0, configured in `Cargo.toml`. The development
  toolchain tracks stable through `rust-toolchain.toml`.
