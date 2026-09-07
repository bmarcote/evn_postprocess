# The PI Letter

The letter that tells the PI their data are on the EVN archive is **generated** for every
experiment from one template, filled with the experiment metadata and with what the support
scientist wrote in the dashboard [Comments](dashboard.md#comments) tab. It is never patched
in place: it is written again from the template every time, so the template is the only
place where its wording lives.

It belongs to the **JIVE delivery** and to nothing else: the letter lives in
`distribution/piletter.py`, next to the `jive` backend that uses it. The `piletter` workflow
step and the delivery both go through the mode's distribution backend
(`Distributor.prepare_letter()` / `send_letter()`), so in `regular` and `sweeps` mode — which
archive nowhere and have no PI to write to — no letter is written at all and the review pause
does not mention one. See [Operating Modes](modes.md).

## What is produced

The `piletter` step (after the pipeline) and the `distribute` step (before the delivery)
both call `JiveDistributor.prepare_letter()`, which writes, in the experiment directory:

| File | What it is |
|------|------------|
| `{exp}.piletter` | The plain text letter, wrapped at 78 columns. This is the copy archived next to the data, so it carries **neither the credentials nor the `To:`/`Cc:` headers** — the PI's email address does not belong in a public archive. |
| `{exp}.piletter_auth` | The same in plain text, **with** the archive username/password. Written only once the credentials exist. |
| `{exp}.piletter.html` | The rich-text version: hyperlinks, the acknowledgment paragraph in grey italics. Open it in a browser and copy-paste it into the mail client, keeping the formatting. |
| `{exp}.piletter.eml` | A ready-to-send draft (recipients, subject, plain text + HTML). Open it locally and the mail client shows it as a draft. |

The previous version of any of them is kept as `{name}.bak`, so a letter edited by hand is
never lost silently — but **edit the dashboard, not the file**: the file is generated again
before the letter is sent.

## How it reaches you

`postprocess` deliberately holds no mail credentials and sends nothing itself. When the
`distribute` step finishes the letter, it posts it to the operator's chat (see
[Communications](comms.md)):

- the letter itself as Markdown, so the chat renders it and it can be copied into an email
  (**including the archive credentials**, which is the point — they are what the PI needs);
- `{exp}.piletter.eml` and `{exp}.piletter.html` attached — download the `.eml`, open it,
  and the draft is there with the recipients, the subject and the formatting in place.

The terminal panel at the end of the step names the same files, for a run without comms.

## What goes into it

| Section | Where it comes from |
|---------|--------------------|
| Recipients, subject, greeting | `[[pi]]` in the experiment toml (recovered from the `.jex` file, or asked for) |
| Data access | The archive credentials (`auth_pipe.py`), only in the `_auth`/HTML/`.eml` versions |
| Correlation parameters | The frequency setup of each correlator pass, and the weight-flagging threshold and percentage |
| General remarks | The dashboard general note, then the automatic remarks: PolConvert, per-antenna bandwidth limitations, opacity correction |
| Remarks on individual stations | The dashboard per-station notes and their status label; stations never reviewed fall back to the automatic findings (did not observe, missed time ranges) |
| Acknowledgment | Fixed text; the project code drops the epoch letter (`EB101B` → `EB101`) |

A section with nothing to say is left out of the letter entirely.

## The template

`src/evn_postprocess/templates/piletter.md.template` is Markdown, with an RFC-822 header
block (`To:`, `Cc:`, `Subject:`) on top; a header left empty is dropped, which is how the
archived copy ends up without recipients. Edit it to change any wording; the placeholders
(`{expname}`, `{obsdate}`, `{passes}`, `{remarks}`, ...) are filled by
`evn_postprocess.distribution.piletter.build()`.

The Markdown understood by the renderers is small and fixed:

- `## Heading`, paragraphs, `- bullets`, `> block quotes` (rendered grey and italic in HTML:
  this is what sets the acknowledgment apart);
- inline `[text](url)`, `<user@host>`, bare URLs, `**bold**` and `` `code` ``. Every link
  becomes a real hyperlink in HTML and is spelled out as `text (url)` in plain text;
- a line ending in `\` forces a line break (used between the signature lines); every other
  newline inside a paragraph is just template wrapping and is re-flowed.
