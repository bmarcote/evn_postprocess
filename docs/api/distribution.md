# distribution

Distribution backends: how the finished experiment is delivered (`jive`, `none`,
`sweeps`). See [Operating Modes](../guide/modes.md) for the user-facing overview.

## Interface & registry

::: evn_postprocess.distribution
    options:
      show_root_heading: true
      members_order: source
      show_source: false

## `jive` backend

::: evn_postprocess.distribution.jive
    options:
      show_root_heading: true
      members_order: source
      show_source: false

## PI letter (`jive`)

Generation of the PI letter from `templates/piletter.md.template` and its renderings
(plain text, HTML, `.eml` draft), plus the chat delivery. Used only by the `jive`
backend. See [The PI Letter](../guide/pi-letter.md).

::: evn_postprocess.distribution.piletter
    options:
      show_root_heading: true
      members_order: source
      show_source: false
