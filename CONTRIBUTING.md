# Contributing to RAIO

## Layout

```
core/src
  app/           what a node program is written with: App, ports, actions, reactions, connections
  node/          the scheduler that orders the events of one node by timestamp
  coordinator/   the graph of an app; wires its nodes at startup and shuts them down
  diagram/       draws an app in the style of Lingua Franca
  messages.rs    what crosses process boundaries
  serializer/    converting port values to and from bytes
  transport/     carrying bytes between processes
  runtime/       the single-threaded executor under each node
  driver/        its I/O and timers
core/macros     the `Ports` derive macro, a separate crate as Rust requires of procedural macros
python
  src/           the Rust half of the binding: Python coroutines run as raio tasks
  python/raio/   the Python half: Node, ports, connect, run
docs             the website, built with Docusaurus
```

## Setup

RAIO builds on Linux and macOS. You need a stable Rust toolchain, Python 3.9 or later, and
[maturin](https://www.maturin.rs), which builds the Python binding.

### Rust

```bash
cargo build
cargo test --all
```

### Python

Run these in a virtual environment or a conda environment; `maturin develop` installs the package
into it.

```bash
cd python
pip install maturin pytest
maturin develop
python -m pytest tests/
```

### Website

```bash
cd docs
yarn
yarn start
```

## Checks before a commit and a push

```bash
pip install pre-commit
pre-commit install
```

The hooks then run `cargo fmt` and check the commit message on every commit, and run the Rust and
Python tests on every push. `pre-commit run --all-files` runs the commit hooks by hand.

## Commits and pull requests

- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org), such as
  `feat: tcp transport` or `fix(py): shutdown order`. The type decides the next version: `fix` is
  a patch, `feat` a minor version, and `feat!` a breaking change.
- A pull request is squashed into one commit named by its title, so the title follows the same
  format. A check on the pull request enforces it.
- Start each pull request from a new branch off `main`. A branch whose pull request was
  squash-merged no longer shares history with `main`.
- CI runs `cargo fmt --check`, the Rust tests, and the Python tests on Linux and macOS.

## Release

`raio.__version__` (Python) and `raio::VERSION` (Rust) report the version. A build from a git
checkout appends its commit hash, such as `0.2.0+abc1234`; a published package reports `0.2.0`.

Releases are made on GitHub with [release-please](https://github.com/googleapis/release-please):

1. On every push to `main`, release-please opens or updates a release pull request. It bumps
   the version and writes `CHANGELOG.md` from the conventional commit messages: `fix` bumps the
   patch version, `feat` the minor version, and a breaking change (`feat!`) the minor version
   until 1.0.0 and the major version after. To choose the version yourself, push a commit whose
   message ends with the line `Release-As: 1.0.0`.
2. The pull request runs the tests and builds and tests every wheel. Merge it to release;
   release-please then tags the commit `v<version>` and creates the GitHub release.
3. The **Release** workflow runs the same checks on `main` in the `release` environment, which
   can require the approval of a member of the Admin team. It then publishes `raio-macros` and `raio-rs` to
   crates.io and the wheels to PyPI.
