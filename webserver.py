"""Web API entry point skeleton.

Exchange commands and responses with core.experimentcontroller.ExperimentController
through the two multiprocessing queues created from a spawn context.
The controller delegates DAG execution to ExperimentRunner inside its process.
Web routes and process startup are not implemented yet.
"""
