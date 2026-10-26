"""Persistent rules and incidents, evaluated independently of browser polling."""

import asyncio
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from core.models.dashboard_alerts import (
    AlertConfiguration,
    AlertIncident,
    AlertRule,
    NotificationChannels,
    SavedAlertState,
)
from core.models.dashboard_icmp import ICMPIncident
from core.models.updates import _update_model
from dashboard.icmp import ICMPMonitor
from dashboard.notifications import deliver
from dashboard.projections import instant
from dashboard.views import DashboardViews


class AlertMonitor:
    def __init__(
        self, directory: Path, views: DashboardViews, icmp: ICMPMonitor
    ) -> None:
        self.directory, self.views, self.icmp = directory, views, icmp
        self.rules: list[AlertRule] = []
        self.incidents: list[AlertIncident] = []
        self.channels = NotificationChannels.model_validate(
            {
                "desktop": False,
                "sound": False,
                "on_recovery": True,
                "repeat_seconds": 600,
            }
        )
        self.error = None
        self.last_evaluated = None
        self._streaks: dict[str, float] = {}
        self._pending: set[str] = set()
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=64)
        self._tasks: list[asyncio.Task] = []
        self._lock = asyncio.Lock()
        self._closed = False
        self._dirty = False

    async def open(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.directory / "alerts.json"
        if path.exists():
            if path.stat().st_size > 8388608:
                raise ValueError("Alert state is too large.")
            try:
                document = SavedAlertState.model_validate(
                    json.loads(path.read_text(encoding="utf-8"))
                )
            except TypeError as error:
                raise ValueError(str(error)) from error
            self._restore_state(document)
        self._tasks = [
            asyncio.create_task(self._run()),
            asyncio.create_task(self._deliver()),
        ]

    def _restore_state(self, document: SavedAlertState) -> None:
        self.rules = list(document.rules)
        self.channels = document.channels
        self.incidents = []
        for incident in document.incidents[-1000:]:
            values: dict[str, object] = dict(incident.model_extra or {})
            for name in AlertIncident.model_fields:
                if name in incident.model_fields_set:
                    values[name] = getattr(incident, name)
            self.incidents.append(AlertIncident.model_validate(values))

    def validate_rule(self, rule: dict) -> dict:
        try:
            return AlertRule.model_validate(rule).document()
        except TypeError as error:
            raise ValueError(str(error)) from error

    def validate_channels(self, document: dict) -> dict:
        try:
            return NotificationChannels.model_validate(document).model_dump()
        except TypeError as error:
            raise ValueError(str(error)) from error

    def _write(self) -> None:
        temporary = self.directory / f".alerts-{uuid4()}.json"
        try:
            with temporary.open("w", encoding="utf-8") as stream:
                json.dump(
                    {
                        "rules": [rule.document() for rule in self.rules],
                        "channels": self.channels.model_dump(),
                        "incidents": [
                            incident.model_dump(exclude_unset=True)
                            for incident in self.incidents
                        ],
                    },
                    stream,
                    ensure_ascii=False,
                    allow_nan=False,
                )
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(self.directory / "alerts.json")
        finally:
            temporary.unlink(missing_ok=True)

    async def _save(self) -> None:
        writing = asyncio.create_task(asyncio.to_thread(self._write))
        try:
            await asyncio.shield(writing)
        except asyncio.CancelledError:
            await writing
            raise

    async def configure(
        self,
        rule: dict | None = None,
        *,
        delete: str | None = None,
        channels: dict | None = None,
    ) -> dict:
        try:
            request = AlertConfiguration.model_validate(
                {"rule": rule, "channels": channels, "delete": delete}
            )
        except TypeError as error:
            raise ValueError(str(error)) from error
        return await self._configure(request)

    async def _configure(self, request: AlertConfiguration) -> dict:
        value = request.rule
        channels = request.channels
        delete = request.delete
        async with self._lock:
            previous_rules, previous_channels = list(self.rules), self.channels
            previous_incidents = list(self.incidents)
            previous_streaks = dict(self._streaks)
            if (
                value is not None
                and len(self.rules) >= 100
                and not any(item.id == value.id for item in self.rules)
            ):
                raise ValueError("At most 100 Alert rules are supported.")
            if value is not None:
                self.rules = [item for item in self.rules if item.id != value.id] + [
                    value
                ]
                self._streaks.pop(value.id, None)
            if delete:
                self.rules = [item for item in self.rules if item.id != delete]
                self._streaks.pop(delete, None)
            if channels is not None:
                self.channels = channels
            changed_id = value.id if value is not None else delete
            for index, incident in enumerate(self.incidents):
                if (
                    changed_id
                    and incident.rule_id == changed_id
                    and incident.status == "active"
                ):
                    self.incidents[index] = _update_model(
                        incident,
                        status="closed",
                        ended_at=datetime.now(UTC).isoformat(),
                        resolution="configuration_changed",
                    )
            try:
                await self._save()
            except OSError:
                self.rules, self.channels = previous_rules, previous_channels
                self.incidents, self._streaks = previous_incidents, previous_streaks
                raise
        return self.status()

    def status(self, *, system_only: bool = False) -> dict:
        items = [
            item
            for item in self.incidents
            if not system_only or item.source != "dashboard_icmp"
        ]
        return {
            "rules": [rule.document() for rule in self.rules],
            "notifications": self.channels.model_dump(),
            "items": [item.model_dump(exclude_unset=True) for item in reversed(items)],
            "active_count": sum(item.status == "active" for item in items),
            "error": self.error,
            "evaluated_at": self.last_evaluated,
            "notification_host": self.icmp.host_name,
        }

    def _notify(self, incident: AlertIncident, recovery: bool = False) -> None:
        if not self.channels.desktop and not self.channels.sound:
            return
        if recovery and not self.channels.on_recovery:
            return
        key = incident.id + (":recovery" if recovery else ":active")
        last = instant(incident.last_notification)
        if (
            key in self._pending
            or not recovery
            and last is not None
            and time.time() - last < self.channels.repeat_seconds
        ):
            return
        try:
            self._queue.put_nowait((key, incident, recovery))
            self._pending.add(key)
        except asyncio.QueueFull:
            self.error = "Notification queue is full."

    async def _run(self) -> None:
        while not self._closed:
            try:
                compute = (
                    await self.views.compute({})
                    if any(
                        rule.enabled and rule.kind == "resource" for rule in self.rules
                    )
                    else None
                )
                try:
                    models = (
                        await self.views.models()
                        if any(
                            rule.enabled and rule.kind == "errors"
                            for rule in self.rules
                        )
                        else []
                    )
                except Exception as error:  # noqa: BLE001 - Sources fail independently.
                    models, model_error = [], str(error)
                else:
                    model_error = None
                async with self._lock:
                    now = datetime.now(UTC).isoformat()
                    changed = self._close_removed_rules(now)
                    for rule in self.rules:
                        if (
                            not rule.enabled
                            or rule.kind == "resource"
                            and compute is None
                        ):
                            continue
                        value, known, breach = await self._rule_condition(
                            rule, compute, models, model_error
                        )
                        changed = (
                            self._apply_rule_observation(
                                rule, value, known, breach, now
                            )
                            or changed
                        )
                    changed = self._merge_icmp_incidents() or changed
                    active = [
                        item for item in self.incidents if item.status == "active"
                    ]
                    closed = [
                        item for item in self.incidents if item.status != "active"
                    ]
                    self.incidents = sorted(
                        closed[-(1000 - len(active)) :] + active,
                        key=lambda item: item.started_at,
                    )
                    self.last_evaluated = now
                    self._dirty = self._dirty or changed
                    if self._dirty:
                        await self._save()
                        self._dirty = False
                        self.error = None
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - Monitoring failures never affect DAG execution.
                self.error = str(error)
            await asyncio.sleep(1)

    def _close_removed_rules(self, now: str) -> bool:
        known_rules = {rule.id for rule in self.rules if rule.enabled}
        changed = False
        for index, incident in enumerate(self.incidents):
            if (
                incident.source == "system"
                and incident.status == "active"
                and incident.rule_id not in known_rules
            ):
                self.incidents[index] = _update_model(
                    incident,
                    status="closed",
                    ended_at=now,
                    resolution="configuration_changed",
                )
                changed = True
        return changed

    async def _rule_condition(
        self,
        rule: AlertRule,
        compute: dict | None,
        models: list,
        model_error: str | None,
    ) -> tuple:
        if rule.kind == "resource":
            key = rule.metric
            if key == "disk_free_gib":
                metric = compute["metrics"]["disk"]
                value = metric.get("free_bytes")
                value = value / 1073741824 if value is not None else None
                known = metric["fresh"]
            elif key.startswith("internet_"):
                metric = compute["metrics"]["internet"]
                value = metric.get(key.removeprefix("internet_") + "_mbps")
                known = metric.get("fresh", False)
            else:
                metric = compute["metrics"][key]
                value, known = metric["value"], metric["fresh"]
            breach = value is not None and (
                value > rule.threshold
                if rule.operator == "above"
                else value < rule.threshold
            )
            return value, known, breach
        selected = [
            (dataset, model)
            for dataset, model in models
            if not rule.experiment_id or dataset["experiment_id"] == rule.experiment_id
        ]
        value = 0
        for dataset, model in selected:
            value += (
                await asyncio.to_thread(
                    self.views.error_count, dataset, rule.window_seconds, time.time()
                )
                if dataset.get("cache")
                else sum(
                    0
                    <= time.time() - (instant(error["occurred_at"]) or 0)
                    <= rule.window_seconds
                    for error in model["errors"]
                )
            )
        known = (
            model_error is None
            and bool(selected)
            and all(dataset["complete"] for dataset, _ in selected)
        )
        return value, known, value >= rule.threshold

    def _apply_rule_observation(
        self, rule: AlertRule, value: object, known: bool, breach: bool, now: str
    ) -> bool:
        index = next(
            (
                index
                for index, item in enumerate(self.incidents)
                if item.status == "active" and item.rule_id == rule.id
            ),
            None,
        )
        incident = None if index is None else self.incidents[index]
        if value is None or not known:
            self._streaks.pop(rule.id, None)
            if incident is not None:
                self.incidents[index] = _update_model(incident, fresh=False)
            return False
        if breach:
            started = self._streaks.setdefault(rule.id, time.monotonic())
            if time.monotonic() - started < rule.duration_seconds:
                return False
            created = incident is None
            if created:
                incident = AlertIncident.model_validate(
                    {
                        "id": str(uuid4()),
                        "rule_id": rule.id,
                        "name": rule.name,
                        "type": rule.kind,
                        "source": "system",
                        "status": "active",
                        "started_at": now,
                        "ended_at": None,
                    }
                )
                index = len(self.incidents)
                self.incidents.append(incident)
            incident = _update_model(incident, value=value, fresh=True)
            self.incidents[index] = incident
            self._notify(incident)
            return created
        self._streaks.pop(rule.id, None)
        if incident is None:
            return False
        incident = _update_model(
            incident,
            status="resolved",
            ended_at=now,
            resolution="condition_cleared",
            fresh=True,
        )
        self.incidents[index] = incident
        self._notify(incident, recovery=True)
        return True

    def _merge_icmp_incidents(self) -> bool:
        changed = False
        for observed in self.icmp.incidents:
            if isinstance(observed, ICMPIncident):
                values: dict[str, object] = dict(observed.model_extra or {})
                for name in ICMPIncident.model_fields:
                    if name in observed.model_fields_set:
                        values[name] = getattr(observed, name)
            else:
                values = dict(observed)
            identity = "icmp:" + values["id"]
            index = next(
                (
                    index
                    for index, item in enumerate(self.incidents)
                    if item.id == identity
                ),
                None,
            )
            if index is None:
                incident = AlertIncident.model_validate(
                    {
                        **values,
                        "id": identity,
                        "name": "ICMP · " + values["host"],
                        "source": "dashboard_icmp",
                    }
                )
                index = len(self.incidents)
                self.incidents.append(incident)
                changed = True
            incident = self.incidents[index]
            previous_status = incident.status
            changes = {
                key: value
                for key, value in values.items()
                if key not in {"id", "source"}
            }
            incident = _update_model(incident, **changes)
            self.incidents[index] = incident
            if incident.status == "active" and self.icmp.snapshot()["fresh"]:
                self._notify(incident)
            elif previous_status == "active" and incident.status == "resolved":
                self._notify(incident, recovery=True)
                changed = True
        return changed

    async def _deliver(self) -> None:
        while not self._closed:
            key, incident, recovery = await self._queue.get()
            incident = next(
                (item for item in self.incidents if item.id == incident.id), incident
            )
            if not recovery and incident.status != "active":
                self._pending.discard(key)
                continue
            result = await deliver(
                "EMP · " + ("Recovered" if recovery else "Alert"),
                incident.name,
                self.channels.model_dump(),
            )
            async with self._lock:
                index = next(
                    (
                        index
                        for index, item in enumerate(self.incidents)
                        if item.id == incident.id
                    ),
                    None,
                )
                if index is not None:
                    self.incidents[index] = _update_model(
                        self.incidents[index],
                        last_notification=datetime.now(UTC).isoformat(),
                        delivery=result,
                    )
                self._pending.discard(key)
                try:
                    await self._save()
                except OSError as error:
                    self._dirty = True
                    self.error = str(error)

    async def close(self) -> None:
        self._closed = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
