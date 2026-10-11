# VoidB PostgreSQL Plugin (`voidb-plugin-postgres`)

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Independent process plugin for [VoidB](https://github.com/limmytian/voidb) to connect, inspect, and manage PostgreSQL databases.

## Features

- **Autonomous Process Architecture**: Runs in an isolated OS process communicating with VoidB via `stdio-jsonrpc`.
- **Tri-Modal Operation**:
  - `test`: Automated connectivity verification.
  - `serve`: `stdio-jsonrpc` protocol server for AI Agents and automated orchestration.
  - Domain commands: SQL execution, database cataloging, table inspection, and schema description.

## Usage & Operational Modes

```bash
# Connectivity test
voidb-cli postgres test --profile my-postgres

# Run SQL queries and catalog exploration
voidb-cli postgres databases --profile my-postgres
voidb-cli postgres tables --database mydb --profile my-postgres
voidb-cli postgres describe users --database mydb --profile my-postgres
voidb-cli postgres query "SELECT * FROM users LIMIT 10;" --database mydb --profile my-postgres

# Machine / Agent stdio-jsonrpc server
voidb-cli postgres serve
```

## Optional local SDK development

Release and CI builds resolve SDK/core from the immutable Git revision in Cargo.toml. To test against a sibling `../voidb` checkout, explicitly opt in with `cargo --config .cargo/config.local.example.toml test --all-targets`. Do not commit an active `.cargo/config.toml` path override.
