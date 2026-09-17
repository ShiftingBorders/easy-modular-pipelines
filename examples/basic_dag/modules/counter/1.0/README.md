# Counter stage

This example accepts the runner's `--emp-context` JSON file. `settings.ticks`
and `settings.delay_seconds` control its duration. The previous stage's optional
`input_data` is copied into the artifact; it is not required.

The stage imports StageClient from the core, reports progress and intermediate
state, and checks cooperative cancellation between ticks. Stdout contains
only one final success/fail JSON; the executor records its validated outcome in
the shared journal after subprocess exit. The result points to `counter.json` relative
to the experiment directory. Code files are never modified.

The example uses the project's uv environment and library SDK path supplied
by the executor. Its only network connection is the local executor control channel; it has no
external services or external state. Repeating
it produces a new attempt artifact. Interrupting it may leave no final artifact;
it does not start child processes or require a custom cleanup command.
