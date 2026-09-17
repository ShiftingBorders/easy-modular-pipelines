"""Experiment execution and its supporting components.

experimentrunner: DAG lifecycle and coordination of the components below.
state: execution records and their persistence.
connection: participant TCP transport.
executor: independent stage subprocess and result publication.
runtimeio: atomic JSON publication and exact OS process identity.
launch: module inputs, settings, integrity, and launch context.
stages/services: runner-side participant lifecycles.
journal: experiment journal setup and template history.
snapshots: consistent snapshots and restoration.

participant_server: shared server and durable participant observations.
stage_client: synchronous module-side context, progress and cancellation client.
results: validated references to results in the shared journal.
StageRunner owns DAG calls; ServiceManager owns service process lifecycles.
core.experimentcontroller provides web-facing control and calls the runner.
"""
