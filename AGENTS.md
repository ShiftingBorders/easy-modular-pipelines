# Repository Guidelines

## Project Structure & Module Organization

This is a Python 3.12 library-first project managed with `uv`. Core pipeline code lives in `core/`; when the maintainer defines or authorizes new structure, keep reusable classes and services there, with one focused module per concern (for example, `core/modulemanager.py` and `core/hashdb.py`). `experimentrunner.py` and `webserver.py` are top-level entry points for batch and FastAPI workflows. Default, version-controlled configuration belongs in `default_settings/`, while tests belong in `tests/` and should mirror the source layout. Do not commit generated databases, caches, virtual environments, or build output.

Library behavior must not depend on the caller's current working directory.
Resolve relative paths stored in configuration files against the directory of
the configuration file that contains them. Preserve absolute configured paths.

## Build, Test, and Development Commands

Use only the project environment managed by `uv` for project-related Python execution. Run scripts, approved tests, linters, formatters, and Python utilities supplied by skills or agents with `uv run <command>`. Do not invoke the system Python directly, activate or create a separate environment, or install project dependencies outside `uv`. Run `uv sync` first when the locked environment is unavailable or outdated.

- `uv sync`: create or update `.venv` from `pyproject.toml` and `uv.lock`.
- `uv run python -m compileall core experimentrunner.py webserver.py`: perform a quick syntax check.
- `uv run python -m unittest discover -s tests -p "test_*.py" -v`: run the
  approved standard-library test suite.
- `uv run --with ruff ruff check core tests`: run Ruff without adding it as a
  project dependency.
- `uv run python experimentrunner.py`: run the experiment entry point once implemented.
- `uv run fastapi dev webserver.py`: start the development API after `webserver.py` exposes a FastAPI application.

There is currently no packaging backend or external automated test dependency.
The approved test framework is Python's standard-library `unittest`. When the
maintainer authorizes a development dependency, add it through `uv` and commit
the resulting `pyproject.toml` and `uv.lock` changes together.

## Coding Style & Naming Conventions

Readability for contributors of every experience level is the project's primary rule. Code should read as a direct description of the work being done. Prefer ordinary functions, simple classes, explicit branches, and descriptive intermediate variables. A reader should be able to follow the main path without jumping through many wrappers or understanding a custom framework.

Keep each module focused on one responsibility, as suggested by `hashdb.py`, `modulemanager.py`, and the separate experiment modules. Methods should perform one clear operation. Split code when a unit handles multiple stages or becomes difficult to scan, but do not fragment a short linear operation into many one-line helpers. Keep call chains shallow and place related logic close together.

Choose the simplest design that satisfies the current requirement:

- Do not add factories, registries, plugin layers, base classes, dependency-injection containers, or generic adapters for hypothetical future needs.
- Do not introduce abstractions only for hypothetical needs. When a requirement already has multiple variants or a maintainer-defined extension point, recommend the smallest composition boundary that keeps new behavior additive; implement it only within existing structures or after explicit authorization.
- Prefer composition and plain data over inheritance and hidden behavior.
- Make state changes and I/O visible. Avoid decorators, metaprogramming, implicit global state, and surprising constructor side effects.
- Use early validation and clear returns to keep nesting shallow. Raise specific exceptions with actionable messages.
- Comments should explain constraints or reasons, not restate the code. Remove dead code and unused imports.

Follow PEP 8 with four-space indentation and consistent spacing. Use type hints for parameters, returns, attributes where useful, and concrete collection types. Prefer `pathlib.Path` for filesystem paths, context managers for resources, and explicit UTF-8 encodings for text files. Use one consistent import form within a module.

Use `snake_case` for modules, functions, variables, and JSON fields; `PascalCase` for classes and enums; `UPPER_SNAKE_CASE` for constants and enum members. Prefix implementation-only methods and attributes with one underscore. Keep public names descriptive; avoid abbreviations unless they are established domain terms such as `db` or `hash`.

## Extensibility & Open/Closed Design

Keep stable behavior open to extension and closed to unnecessary modification. Design variants around a small, explicit boundary—such as composition, a callable, or a narrow interface—instead of repeatedly editing central conditionals. This is a design criterion, not permission for an agent to add that boundary. Separate orchestration from replaceable policies and depend on stable contracts.

Apply this principle to demonstrated extension needs, not imagined ones. Prefer the smallest seam supporting current variants; do not build a general framework, deep inheritance tree, or plugin system without explicit approval. Readability and directness remain the deciding constraints.

Before implementing, assess whether a change makes future variants require edits to stable core logic, couples unrelated responsibilities, or exposes implementation details as a contract. If it does, warn the maintainer before proceeding. Describe the concrete risk, the extension scenario affected, and the simplest alternative, then wait for direction if the alternative requires new structure. Never silently accept an open/closed-principle tradeoff.

## Workspace Cleanliness

Keep the repository root limited to source entry points and essential project configuration. Put generated logs, scratch output, temporary Markdown plans, diagnostics, and other disposable agent artifacts under `.artifacts/`, using subfolders such as `.artifacts/logs/`, `.artifacts/test-plans/`, and `.artifacts/tmp/`. Never place these files directly in the root. Do not store source code, permanent documentation, fixtures, or required project inputs in `.artifacts/`.

## Agent Implementation Boundaries

Work only inside functions and classes already defined by the maintainer. Fill their existing bodies according to the request, nearby code, and supplied context. Do not add unrelated behavior or silently expand the feature scope.

If the requested behavior appears to require a new function, method, class, module, or other logic outside those predefined boundaries, stop and ask the maintainer which of these paths to take:

1. The maintainer defines the additional structure, after which the agent fills it in.
2. The maintainer explicitly authorizes the agent to design and implement the additional structure.

Explain what is missing and why it is needed before requesting permission. Do not treat architectural convenience, cleanup, or anticipated future use as authorization. Permission applies only to the additional structure discussed.

## Testing Guidelines and Approval Workflow

Tests are added only after a feature's implementation is finalized and the maintainer approves the test behavior. Do not infer test cases and immediately implement them.

When a feature is ready for testing, first create a Markdown test-plan template under `.artifacts/test-plans/`, named for the feature (for example, `.artifacts/test-plans/hash_db_validation.md`). The template must list:

- every source file affected by the feature;
- each observable behavior that requires testing;
- relevant inputs, outputs, errors, boundaries, or side effects;
- open questions for the maintainer, without prescribing test logic.

The maintainer then describes in natural language how each listed file or behavior should be tested. Review that description together and resolve ambiguities. Write or modify test code only after the maintainer explicitly confirms the plan. Apply this same plan-and-approval workflow to all later test changes; existing approval does not automatically authorize new cases or altered assertions.

After approval, place `unittest` tests under `tests/` using names such as
`test_hashdb.py` and methods such as `test_rejects_invalid_schema`. Keep each
test traceable by name and scope to the approved behavior; the temporary plan
is coordination material and is not a permanent project specification. No
coverage threshold is established yet; request approval before adding another
test framework, changing test dependencies, or establishing a coverage policy.

VS Code test discovery is configured in `.vscode/settings.json` for `unittest`,
the `tests/` start directory, and the `test_*.py` filename pattern. Keep these
settings aligned with the command-line discovery command above.

## Commit & Pull Request Guidelines

The repository has no commit history from which to infer a convention. Use short, imperative subjects such as `Add hash database schema validation`, and keep each commit focused. Pull requests should explain the purpose and behavior change, list verification commands, link relevant issues, and call out configuration or schema changes. Include sample API requests or screenshots when web behavior changes.

## Security & Configuration

Never commit credentials, local databases, or machine-specific paths. Validate JSON schemas before modifying persistent data.

