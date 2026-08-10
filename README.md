# Apache OpenDAL™ File System

[![Build Status]][actions] [![Latest Version]][crates.io] [![Crate Downloads]][crates.io] [![MSRV 1.85]][msrv] [![Apache 2.0 licensed]][license] [![chat]][discord]

[build status]: https://img.shields.io/github/actions/workflow/status/apache/opendal-ofs/ci.yml?branch=main
[actions]: https://github.com/apache/opendal-ofs/actions?query=branch%3Amain
[latest version]: https://img.shields.io/crates/v/ofs.svg
[crates.io]: https://crates.io/crates/ofs
[crate downloads]: https://img.shields.io/crates/d/ofs.svg
[msrv 1.85]: https://img.shields.io/badge/MSRV-1.85-green?logo=rust
[msrv]: https://www.whatrustisit.com
[apache 2.0 licensed]: https://img.shields.io/crates/l/ofs
[license]: https://www.apache.org/licenses/LICENSE-2.0
[chat]: https://img.shields.io/discord/1081052318650339399
[discord]: https://opendal.apache.org/discord

`ofs` is the Apache OpenDAL filesystem project.

> [!IMPORTANT]
> **Status: active redesign**
>
> We are actively working on the design of the next `ofs` release under
> [RFC-0016]. The `main` branch is a buildable project scaffold and does not
> currently provide a mount command or runtime API.

## Previous releases

The implementation used by earlier published releases is preserved on the
[`backup`] branch. It predates RFC-0016 and remains available for reference
while the new runtime is designed.

[rfc-0016]: rfcs/0016_filesystem_architecture.md
[`backup`]: https://github.com/apache/opendal-ofs/tree/backup

## Development

Run the current project checks with:

```shell
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

## Branding

The first and most prominent mentions must use the full form: **Apache OpenDAL™** of the name for any individual usage (webpage, handout, slides, etc.) Depending on the context and writing style, you should use the full form of the name sufficiently often to ensure that readers clearly understand the association of both the OpenDAL project and the OpenDAL software product to the ASF as the parent organization.

For more details, see the [Apache Product Name Usage Guide](https://www.apache.org/foundation/marks/guide).

## License and Trademarks

Licensed under the Apache License, Version 2.0: <http://www.apache.org/licenses/LICENSE-2.0>

Apache OpenDAL, OpenDAL, and Apache are either registered trademarks or trademarks of the Apache Software Foundation.
