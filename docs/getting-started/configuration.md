# Configuration

## computers.toml

The pipeline uses a `computers.toml` file to locate servers and external binaries. This file is searched in the following order:

1. Path in the `EVN_COMPUTERS_TOML` environment variable.
2. `./computers.toml` (current directory).
3. `$XDG_CONFIG_HOME/evn/computers.toml` (typically `~/.config/evn/computers.toml`).
4. `~jops/.config/evn/computers.toml` (shared account).

### Example `computers.toml`

```toml
[eee]
hostname = "eee.jive.nl"
user = "jops"
path = "/data0"

[ccs]
hostname = "ccs.jive.nl"
user = "jops"
path = "/ccs/expr"

[pipe]
hostname = "pipe.jive.nl"
user = "jops"
path = "/pipe_data"
```

Each server entry provides `hostname`, `user`, and `path` for SSH/SCP operations.

## Environment variables

See [Environment Variables reference](../reference/env-vars.md) for the full list of tunables (SSH timeouts, tool overrides, etc.).

## Communications (comms.toml)

To receive notifications at pipeline review points, create a `comms.toml` file. See [Communications guide](../guide/comms.md) for setup instructions.

## Policy file (policy.toml)

For batch/unattended mode, a `policy.toml` provides all decisions that would normally be asked interactively. See [Batch Mode](../guide/batch-mode.md) and [Policy File reference](../reference/policy.md).
