# verification

The three checks the `verification` step runs on the final FITS-IDI files, between
`prearchive` and `distribute`: the ANTAB Tsys/gain-curve tables are attached, no data
was lost between the multi-part FITS-IDI files, and their content still matches the
MS they came from. See
[Workflow Steps & Local Tools](../reference/steps.md#16-verification-verify).

::: evn_postprocess.verification
    options:
      show_root_heading: true
      members_order: source
      show_source: false
