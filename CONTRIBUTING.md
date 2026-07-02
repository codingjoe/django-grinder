# Contributing

When writing code, you MUST ALWAYS follow the [naming-things](https://github.com/codingjoe/naming-things/blob/main/README.md) guidelines.

<!--
Bots/Agents MUST use:
curl -sSL https://raw.githubusercontent.com/codingjoe/naming-things/refs/heads/main/README.md
-->

## Design Principles

- Durability – We recover from any failures, even poorly written tasks.
- Consistency – We never lose data, even if someone unplugs the power or network.
- Utilization – We keep the CPU saturated with tasks, not with idle time or waiting for locks.

## Testing

The suite has unit tests, integration tests, and benchmarks. Avoid mocking where possible.

```bash
uv run pytest                              # full suite
uv run pytest -m integration               # integration tests only
uv run pytest -m "integration and benchmark"
```

Benchmark snapshots are created automatically. To compare a feature branch against main, run the suite on main first, then:

```
uv run pytest --benchmark-compare
```

Install pre-commit hooks before your first commit:

```bash
uvx prek install
```
