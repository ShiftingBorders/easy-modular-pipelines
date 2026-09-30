"""Serial module maintenance over the existing controller queues, without a runner."""

from core.primitives.json_values import JsonObject


def _validate_module_read_args(name: str, args: JsonObject) -> None:
    if name == "stats.modules" and args:
        raise ValueError("Module list does not accept arguments.")
    if name == "stats.module" and args.keys() != {"name", "version"}:
        raise ValueError("Module inspection requires name/version.")
    if name == "stats.template" and args.keys() != {"template_path"}:
        raise ValueError("Template validation requires template_path.")
