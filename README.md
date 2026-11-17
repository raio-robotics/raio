# raio

## Developer Setup

### Setup Pre-Commit

```bash
pip install pre-commit
pre-commit install --hook-type commit-msg --hook-type pre-push
pre-commit run --all-files
```

### Publish package

```bash
cargo publish --dry-run
cargo publish
```