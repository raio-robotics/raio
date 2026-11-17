# raio

## Developer Setup

### Setup Pre-Commit

```bash
pip install pre-commit
pre-commit install --hook-type commit-msg --hook-type pre-push
pre-commit run --all-files
```

### Build Project
#### Rust
```bash
cargo build
cargo test
```

#### Python
```bash
cd raio-py
maturin develop
python -m pytest tests/
```

### Publish packasdfage

#### Cargo
```bash
cargo publish -p raio-rs --dry-run
cargo publish -p raio-rs
```

#### PyPI
```bash
cd raio-py
maturin build --release
uv publish ../target/wheels/*
```
