# EVN Postprocess Pipeline

**This is a program meant for internal use. It is expected to not run in other environments**

Pipeline to run all post-processing steps of EVN data in a semi-interactive and semi-smart way. This code will run all steps required from post-correlation at JIVE to user delivery of the data. That is, it runs the steps defined in the _SFXC Post-Correlation Checklist_.


## Usage

Being in `eee` at a `/data0/ {supsci} / {EXPNAME}` folder, you can call it as simple as:

```
postprocess
``` 
to run the full process semi-automatically.
Or use the `--expname` and `--supsci` options if you are in a different location.


The program has the following extra command options:

```
postprocess info
```
to show the info related to the experiment if it already run though the required steps that recover it (when copying expsum, lis, and after creating the MS file).

```
postprocess last
```
to show the last successfully-run step.

```
postprocess run STEP1  [STEP2]
```
to run the post-process from `STEP1`  until `STEP2` (or until the end if the later is not specified).

```
postprocess exec COMMAND
```
to run only a single command by using the existing metadata, so you don't care on parameters. do `exec -h` to see all of them.

```
postprocess edit PARAM VALUE
```
to edit something that was wrong from the metadata (e.g. which sources are targets, or change the reference antenna, etc).

As always, `postprocess -h` for the full help, or inside each command as `postprocess command -h`.



