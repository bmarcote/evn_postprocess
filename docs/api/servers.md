# servers

Server configuration (`Server`/`Servers`, read from `computers.toml`). Isolated in its
own leaf module and imported only by the JIVE backends and `tools`; the server-agnostic
core never imports it.

::: evn_postprocess.servers
    options:
      show_root_heading: true
      members_order: source
      show_source: false
