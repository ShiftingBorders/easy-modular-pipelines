"use strict";

import {escape as e, numeric, duration, dateTime, address, badge, warning, panel, empty, stat, inspectButton, table, gauge, lineChart} from "./ui.js";

export function icmpPanel(monitor, compact = true) {
    if (!monitor) return panel("ICMP Echo Reply", empty("Loading monitor…"));
    const settings = monitor.settings;
    const latest = monitor.latest;
    const reply = monitor.fresh && monitor.status === "reply";
    const value = reply ? (latest?.rtt_upper_bound ? "< " : "") + numeric(latest?.rtt_ms, 1) + " ms" : "—";
    return panel("ICMP Echo Reply", `<div class="panel-body"><div class="ping-layout"><div class="ping-current">${badge(monitor.status)}<div class="ping-value ${reply ? "good" : monitor.status === "no_reply" ? "bad" : ""}">${e(value)}</div><small>Probe source: ${e(monitor.probe_host)}</small><small>${e(dateTime(latest?.observed_at))}</small></div><form id="icmp-form" class="ping-form"><label class="field">Host or IPv4 address<input name="host" value="${e(settings.host)}" placeholder="example.org" maxlength="253"></label><label class="field">Reply timeout · s<input name="timeout_seconds" type="number" min="0.1" max="60" step="0.1" value="${settings.timeout_seconds}"></label><label class="field">Probe interval · s<input name="interval_seconds" type="number" min="1" max="600" value="${settings.interval_seconds}"></label><label class="checkbox"><input type="checkbox" name="enabled" ${settings.enabled ? "checked" : ""}>Enabled</label><button class="button primary" type="submit">Save</button><button class="button" type="button" data-action="probe" ${!settings.enabled || monitor.probing ? "disabled" : ""}>${monitor.probing ? "Probing…" : "Check now"}</button></form></div>${latest?.reason && monitor.fresh ? `<p class="source-note">${e(latest.reason)}</p>` : ""}${monitor.storage_error ? `<p class="source-note">${warning(monitor.storage_error)} ${e(monitor.storage_error)}</p>` : ""}${compact ? "" : `<div class="section-title">Round-trip time · ms</div>${lineChart(monitor.history.filter(row => row.host === settings.host), "rtt_ms", "ms")}`}</div>`, '<span class="muted">From the dashboard host</span>');
}

export function overview(data, monitor) {
    const metrics = data?.metrics || {};
    const cards = `<div class="stats">${stat("Active experiments", numeric(metrics.active_experiments))}${stat("Completed experiments", numeric(metrics.completed_experiments))}${stat("Failed experiments", numeric(metrics.failed_experiments))}${stat("Execution errors", numeric(metrics.error_events))}</div>`;
    const attention = panel("Needs attention", table(data?.attention || [], [
        ["Experiment", row => `<a href="${address("timeline", row.experiment_id, row.run_id)}">${e(row.name || row.experiment_id)}</a>`],
        ["Status", row => badge(row.status)], ["Errors", row => numeric(row.error_count)],
        ["Observed", row => e(dateTime(row.observed_at))]], [["name", "Experiment"], ["status", "Status"]]));
    return `<h2 class="section-title">Key metrics</h2>${cards}${data ? attention : ""}<div id="icmp-panel">${icmpPanel(monitor)}</div><h2 class="section-title">Compute resources</h2><div class="gauge-grid overview-gauges">${gauge("CPU", data?.compute?.cpu)}${gauge("RAM", data?.compute?.ram)}${gauge("Disk", data?.compute?.disk)}</div>`;
}

export function experiments(rows, selected) {
    return panel("Experiments", table(rows, [
        ["", row => `<input type="radio" name="experiment" value="${e(row.experiment_id)}" aria-label="Select ${e(row.name || row.experiment_id)}" ${row.experiment_id === selected ? "checked" : ""}>`],
        ["Experiment", row => `<label><strong>${e(row.name || row.experiment_id)}</strong><small class="mono">${e(row.experiment_id)}</small></label>`],
        ["Status", row => badge(row.status)],
        ["Latest logical run", row => row.run_id ? `<a href="${address("timeline", row.experiment_id, row.run_id)}" class="mono">${e(row.run_id)}</a>` : "—"],
        ["Template", row => e(row.template_revision_id || "—")],
        ["Cycles", row => `${numeric(row.completed_cycles)} / ${numeric(row.total_cycles)}`],
        ["Started", row => e(dateTime(row.started_at))]], [["name", "Experiment"], ["status", "Status"]], "experiments")) + '<div id="run-history"></div>';
}

export function runHistory(rows, experiment) {
    return panel("Run history", rows.length ? `<div class="history-card">${rows.map(row => `<a class="history-run" href="${address("timeline", experiment, row.run_id)}"><strong class="mono">${e(row.run_id)}</strong><small>Revision ${e(row.template_revision_id || "—")}</small>${badge(row.status)}</a>`).join('<span aria-hidden="true">→</span>')}</div>` : empty("No recorded runs"), `<span class="muted mono">${e(experiment)}</span>`);
}

export function events(rows, mode) {
    return panel("Events", table(rows, [
        ["Time", row => e(dateTime(row.occurred_at))], ["Type", row => inspectButton(row.event_type, row)],
        ["Module", row => e(row.context?.module_name || row.context?.source || "—")],
        ["Run", row => e(row.context?.run_id || "—")], ["Observation", row => row.ignored ? `${badge("ignored")} ${warning(row.ignored)}` : badge(row.confirmation || "recorded")]],
    [["event_type", "Type"]], "events"), `<div class="segmented"><button data-mode="effective" class="${mode === "effective" ? "active" : ""}">Effective</button><button data-mode="raw" class="${mode === "raw" ? "active" : ""}">Full journal</button></div>`);
}

export function errors(rows) {
    const groups = new Map();
    for (const row of rows) {
        const key = JSON.stringify([row.type || row.error_type, row.module_name, row.stage_id, row.phase]);
        const group = groups.get(key) || {...row, count: 0, first_seen: row.occurred_at, last_seen: row.occurred_at};
        group.count++;
        if (row.occurred_at < group.first_seen) group.first_seen = row.occurred_at;
        if (row.occurred_at > group.last_seen) group.last_seen = row.occurred_at;
        groups.set(key, group);
    }
    const statistics = panel("Error statistics", table([...groups.values()].sort((a,b) => b.count - a.count), [
        ["Type", row => e(row.type || row.error_type)], ["Module", row => e(row.module_name || "—")],
        ["Stage", row => e(row.stage_id || "—")], ["Phase", row => e(row.phase)], ["Occurrences", row => numeric(row.count)],
        ["First seen", row => e(dateTime(row.first_seen))], ["Last seen", row => e(dateTime(row.last_seen))]], [], "error groups"));
    return statistics + panel("Error events", table(rows, [
        ["Type", row => inspectButton(row.type || row.error_type || "Error", row)],
        ["Module", row => e(row.module_name || "—")], ["Stage", row => e(row.stage_id || "—")],
        ["Phase", row => e(row.phase || "—")], ["Time", row => e(dateTime(row.occurred_at))],
        ["Message", row => e(row.message || "—")]], [["type", "Type"], ["phase", "Phase"], ["module_name", "Module"]], "errors"));
}

export function timeline(rows, observedAt) {
    const recorded = rows.filter(row => Number.isFinite(new Date(row.started_at).valueOf()));
    if (!recorded.length) return panel("Timeline", empty("No operations recorded"));
    const observed = new Date(observedAt).valueOf();
    const starts = recorded.map(row => new Date(row.started_at).valueOf());
    const finish = row => row.finished_at ? new Date(row.finished_at).valueOf() : Number.isFinite(observed) ? Math.max(observed, new Date(row.started_at).valueOf()) : new Date(row.started_at).valueOf();
    const start = Math.min(...starts), end = Math.max(start, ...recorded.map(finish).filter(Number.isFinite)), span = Math.max(1, end - start);
    const byId = new Map(recorded.map(row => [row.operation_id, row]));
    const children = new Map();
    for (const row of recorded) {
        const parent = byId.has(row.parent_operation_id) ? row.parent_operation_id : null;
        if (!children.has(parent)) children.set(parent, []);
        children.get(parent).push(row);
    }
    for (const group of children.values()) group.sort((left, right) => new Date(left.started_at) - new Date(right.started_at));
    const ordered = [], visited = new Set();
    function visit(row, depth) {
        if (visited.has(row.operation_id)) return;
        visited.add(row.operation_id); ordered.push([row, depth]);
        for (const child of children.get(row.operation_id) || []) visit(child, Math.min(depth + 1, 12));
    }
    for (const row of children.get(null) || []) visit(row, 0);
    for (const row of recorded) if (!visited.has(row.operation_id)) visit(row, 0);
    return panel("Timeline", `<div class="table-scroll"><div class="trace"><div class="trace-row trace-ruler"><div class="trace-name">Operation</div><div class="trace-track">${[0, .25, .5, .75, 1].map(fraction => `<span>${duration(span * fraction / 1000)}</span>`).join("")}</div></div>${ordered.map(([row, depth]) => {
        const left = (new Date(row.started_at).valueOf() - start) / span * 100;
        const width = Math.max(0, (finish(row) - new Date(row.started_at)) / span * 100);
        const status = ["failed", "error"].includes(row.status) ? "failed" : row.finished_at ? "" : "running";
        return `<div class="trace-row"><button class="trace-name" style="padding-left:${12 + depth * 16}px" data-inspect="${e(JSON.stringify(row))}">${e(row.name || row.operation_type || row.operation_id)}</button><div class="trace-track"><span class="trace-bar ${status}" style="left:${left}%;width:${width}%" title="${e(row.name)} · ${duration((finish(row) - new Date(row.started_at)) / 1000)}"></span></div></div>`;
    }).join("")}</div></div>`, `<span class="muted">${e(dateTime(new Date(start).toISOString()))} · ${duration(span / 1000)}</span>`);
}

export function dag(document) {
    const nodes = document.nodes || document.template?.stages || [];
    if (!nodes.length) return panel("DAG", empty("No DAG definition recorded"));
    const positions = new Map(nodes.map((node, index) => [node.stage_id, 30 + index * 255]));
    const edges = (document.edges || []).filter(edge => positions.has(edge.from) && positions.has(edge.to));
    const paths = edges.map(edge => {
        const from = positions.get(edge.from), to = positions.get(edge.to);
        const forward = to > from;
        const path = forward ? `M${from + 210},100 C${from + 240},100 ${to - 25},100 ${to},100`
            : `M${from + 105},152 C${from + 105},260 ${to + 105},260 ${to + 105},152`;
        return `<path d="${path}" fill="none" stroke="${forward ? "#719bcf" : "#c0a0dc"}" stroke-width="2" marker-end="url(#dag-arrow)"><title>${e(edge.condition || `${edge.from} → ${edge.to}`)}</title></path>`;
    }).join("");
    const width = Math.max(760, nodes.length * 255 + 30);
    const graph = `<div class="table-scroll"><svg class="dag-canvas" width="${width}" height="285" viewBox="0 0 ${width} 285" role="group" aria-label="DAG stages and explicit dependencies"><defs><marker id="dag-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#8dafdb"/></marker></defs>${paths}${nodes.map(node => `<foreignObject x="${positions.get(node.stage_id)}" y="48" width="210" height="110"><button xmlns="http://www.w3.org/1999/xhtml" class="dag-node" data-inspect="${e(JSON.stringify(node))}"><strong>${e(node.name || node.stage_id)}</strong><small>${e(node.module?.name || node.module_name || "")} ${e(node.module?.version || "")}</small>${node.status ? badge(node.status) : ""}</button></foreignObject>`).join("")}</svg></div>`;
    return panel("DAG", graph) + panel("Dependencies", table(document.edges || [], [["From", row => e(row.from)], ["To", row => e(row.to)], ["Condition", row => e(row.condition || "—")]], [], "edges"));
}

export function metricCards(rows, selections, forecast = null) {
    const usable = rows.filter(row => row.module_name && row.metric && row.cycle_number != null);
    const modules = [...new Set(usable.map(row => `${row.module_name} / ${row.module_version || "—"}`))];
    if (!modules.length) return panel("Module statistics", empty("No reported cycle statistics", "Statistics require a module, metric, unit and completed DAG cycle."));
    return `<div class="context"><span>Statistics unit: <b>1 DAG cycle</b></span><span>Sample: <b>${new Set(usable.map(row => row.cycle_number)).size} completed cycles</b></span></div><div class="metric-grid">${[0, 1, 2].map(index => {
        const selection = selections[index] || {};
        const module = modules.includes(selection.module) ? selection.module : modules[Math.min(index, modules.length - 1)];
        const moduleRows = usable.filter(row => `${row.module_name} / ${row.module_version || "—"}` === module);
        const metrics = [...new Set(moduleRows.map(row => row.metric))];
        const metric = metrics.includes(selection.metric) ? selection.metric : metrics.find(name => !name.includes("token")) || metrics[0];
        selections[index] = {module, metric};
        const selected = moduleRows.filter(row => row.metric === metric).sort((a, b) => a.cycle_number - b.cycle_number);
        const units = new Set(selected.map(row => row.unit));
        const revisions = new Set(selected.map(row => row.template_revision_id).filter(Boolean));
        const compatible = units.size === 1 && revisions.size <= 1 && new Set(selected.map(row => row.cycle_number)).size === selected.length;
        const known = selected.filter(row => typeof row.value === "number" && Number.isFinite(row.value));
        const mean = compatible && known.length === selected.length ? known.reduce((sum, row) => sum + row.value, 0) / known.length : null;
        const maximum = Math.max(1e-10, ...known.map(row => Math.abs(row.value)));
        const unit = units.size === 1 ? [...units][0] : "Mixed units";
        const incomplete = !compatible || known.length !== selected.length || selected.some(row => row.complete === false || row.estimated);
        const additive = selected.every(row => row.aggregation === "sum");
        const remaining = forecast && Number.isFinite(forecast.total_cycles) && Number.isFinite(forecast.completed_cycles) ? Math.max(0, forecast.total_cycles - forecast.completed_cycles) : null;
        const projected = additive && !incomplete && mean != null && remaining != null ? mean * remaining : null;
        const projection = forecast && additive ? `<dl class="kv metric-projection"><dt>Remaining cycles</dt><dd>${numeric(projected, 4)} ${e(unit)}</dd><dt>Total at completion</dt><dd>${numeric(projected == null ? null : projected + known.reduce((sum, row) => sum + row.value, 0), 4)} ${e(unit)}</dd></dl>` : "";
        return `<section class="metric-card"><h3>Statistic ${index + 1}</h3><div class="metric-fields"><label class="field">Module<select data-metric-module="${index}">${modules.map(value => `<option ${module === value ? "selected" : ""}>${e(value)}</option>`).join("")}</select></label><label class="field">Metric<select data-metric-name="${index}">${metrics.map(value => `<option ${metric === value ? "selected" : ""}>${e(value)}</option>`).join("")}</select></label></div><div class="metric-value">${numeric(mean, 4)}<small>${e(unit)}</small> ${incomplete ? warning("Incomplete, estimated or incompatible cycle measurements. Inspect individual values.") : ""}</div><small>Mean per DAG cycle</small>${projection}<div class="metric-samples">${selected.slice(-20).map(row => `<div class="metric-sample"><span>Cycle ${e(row.cycle_number)}</span><div class="bar-track"><i style="width:${Number.isFinite(row.value) ? Math.min(100, Math.abs(row.value) / maximum * 100) : 0}%"></i></div><strong>${numeric(row.value, 4)}</strong></div>`).join("")}</div></section>`;
    }).join("")}</div>`;
}

export function modules(rows, detail) {
    if (detail) {
        const module = rows.find(row => row.module_id === detail || `${row.name}/${row.version}` === detail);
        if (!module) return empty("Module unavailable");
        return `<div class="stats">${stat("Runs", numeric(module.runs))}${stat("Errors", numeric(module.error_count))}${stat("Duration · p95", duration(module.p95_seconds))}${stat("Restarts", numeric(module.restarts))}</div>` + panel("Recent attempts", table(module.recent_attempts || [], [
            ["Run", row => `<a href="${address("timeline", row.experiment_id, row.run_id)}">${e(row.run_id)}</a>`], ["Stage / attempt", row => `${e(row.stage_id)} / ${e(row.attempt_number)}`], ["Outcome", row => badge(row.status)], ["Duration", row => duration(row.duration_seconds)]], [["status", "Outcome"]]));
    }
    return panel("Error statistics", table([...rows].sort((a, b) => (b.error_count || 0) - (a.error_count || 0)), [["Module", row => `<a href="${address("modules", null, null, {detail: row.module_id || `${row.name}/${row.version}`})}">${e(row.name)}</a>`], ["Version", row => e(row.version)], ["Errors", row => numeric(row.error_count)]], [], "modules")) + panel("Modules", table(rows, [["Module", row => e(row.name)], ["Version", row => e(row.version)], ["Runs", row => numeric(row.runs)], ["Errors", row => numeric(row.error_count)], ["p50", row => duration(row.p50_seconds)], ["p95", row => duration(row.p95_seconds)], ["Restarts", row => numeric(row.restarts)]], [["version", "Version"], ["experiment_name", "Experiment"]]));
}

export function services(rows, detail) {
    if (!detail) return panel("Services", table(rows, [["Service", row => `<a href="${address("services", null, null, {detail: row.instance_id})}">${e(row.name)}</a>`], ["Version", row => e(row.version)], ["Instance", row => e(row.instance_id)], ["State", row => badge(row.state)], ["Observed", row => e(dateTime(row.observed_at))]], [["state", "State"]]));
    const service = rows.find(row => row.instance_id === detail);
    if (!service) return empty("Service unavailable");
    const process = service.process_metrics;
    return `<div class="context"><span>Instance <b>${e(service.instance_id)}</b></span><span>State <b>${badge(service.state)}</b></span><span>Observed <b>${e(dateTime(service.observed_at))}</b></span></div>` +
        `<div class="stats">${stat("Uptime", duration(service.uptime_seconds))}${stat("Process RAM", process?.rss_bytes == null ? "—" : `${numeric(process.rss_bytes / 1048576)} MiB`, "RSS / working set")}${stat("Process CPU", process?.cpu_percent == null ? "—" : `${numeric(process.cpu_percent)}%`, "100% = 1 logical CPU")}${stat("Queued requests", numeric(service.queue_length))}</div>` +
        `<div class="grid2">${panel("Process RAM · bytes", `<div class="panel-body">${lineChart(process?.history || [], "rss_bytes", "bytes")}</div>`)}${panel("Process CPU · %", `<div class="panel-body">${lineChart(process?.history || [], "cpu_percent", "%")}</div>`)}</div>` + panel("Service details", `<div class="panel-body"><pre>${e(JSON.stringify(service, null, 2))}</pre></div>`);
}

export function compute(document) {
    const metrics = document.metrics || {};
    const problems = [document.error, document.journal_error, document.history_error, document.gap ? "Some resource history is unavailable." : null].filter(Boolean);
    return (problems.length ? `<div class="source-note">${warning(problems.join(" "))}</div>` : "") + `<div class="gauge-grid">${gauge("CPU", metrics.cpu)}${gauge("RAM", metrics.ram)}${gauge("Disk", metrics.disk)}<div class="gauge-card"><div class="gauge-title" title="${e(metrics.internet?.interface || "Interface unavailable")}">Internet I/O ${!metrics.internet?.fresh || metrics.internet?.exceeded ? warning(metrics.internet?.exceeded ? "Threshold exceeded" : "Measurement unavailable or stale") : ""}</div><div class="ping-value">↓ ${numeric(metrics.internet?.fresh ? metrics.internet?.receive_mbps : null, 1)}</div><div>↑ ${numeric(metrics.internet?.fresh ? metrics.internet?.transmit_mbps : null, 1)} Mbps</div></div></div><div class="section-title">Resource history</div><div class="filters"><label class="field">Time range<select id="resource-window">${[5,10,15,20,30,45,60,120].map(n => `<option value="${n}" ${n === 15 ? "selected" : ""}>${n < 60 ? n + " min" : n / 60 + " h"}</option>`).join("")}<option value="custom">Custom range</option></select></label><label class="field" data-custom-range hidden>From<input type="datetime-local" id="range-from"></label><label class="field" data-custom-range hidden>To<input type="datetime-local" id="range-to"></label><button class="button" data-action="resource-range">Apply</button></div><div class="grid2">${["cpu", "ram", "disk"].map(key => panel(key.toUpperCase() + " · %", `<div class="panel-body">${lineChart(document.history?.[key] || [], "value", "%")}</div>`)).join("")}</div>`;
}

export function forecast(document, selections) {
    const components = document.component_durations || [];
    const maximum = Math.max(1, ...components.map(row => row.mean_seconds || 0));
    const componentChart = components.length ? `<div class="panel-body">${components.map(row => `<div class="component-duration"><span>${e(row.module_name)}</span><div class="bar-track"><i style="width:${Math.max(0, (row.mean_seconds || 0) / maximum * 100)}%"></i></div><strong>${duration(row.mean_seconds)}</strong></div>`).join("")}</div>` : empty("No completed cycles");
    const progress = Number.isFinite(document.total_cycles) && document.total_cycles > 0 && Number.isFinite(document.completed_cycles) ? Math.min(100, Math.max(0, document.completed_cycles / document.total_cycles * 100)) : null;
    return `<div class="forecast-summary">${stat("ETA", duration(document.eta_seconds), document.paused ? "After resume" : "Remaining")}${panel("Execution progress", `<svg class="progress-ring" viewBox="0 0 150 150"><circle cx="75" cy="75" r="55" fill="none" stroke="#324862" stroke-width="10"/><circle cx="75" cy="75" r="55" fill="none" stroke="#83b4fc" stroke-width="10" pathLength="100" stroke-dasharray="${progress || 0} 100" transform="rotate(-90 75 75)"/><text x="75" y="84" text-anchor="middle">${numeric(progress, 0)}%</text></svg>`)}${panel("Forecast basis", `<div class="panel-body"><dl class="kv"><dt>Sample mean</dt><dd>${duration(document.sample_mean_seconds)}</dd><dt>Completed in sample</dt><dd>${numeric(document.sample_cycles)}</dd><dt>ETA range</dt><dd>${duration(document.eta_low_seconds)} – ${duration(document.eta_high_seconds)}</dd></dl></div>`)}</div>${metricCards(document.measurements || [], selections, document)}${panel("Mean duration of DAG components", componentChart)}`;
}

export function alertControls(document, experiments) {
    const channels=document.notifications;
    const rules=panel("Alert rules",table(document.rules,[["Rule",row=>e(row.name)],["Type",row=>e(row.kind)],
        ["Condition",row=>row.kind === "errors" ? `${e(row.threshold)} errors / ${e(row.window_seconds)} s` : `${e(row.metric)} ${e(row.operator)} ${e(row.threshold)}`],
        ["State",row=>badge(row.enabled ? "enabled" : "disabled")],
        ["",row=>`<button class="button quiet" data-edit-rule="${e(JSON.stringify(row))}">Edit</button> <button class="button quiet" data-delete-rule="${e(row.id)}">Delete</button>`]],[],"rules"),'<button class="button" data-action="new-rule">Add rule</button>');
    return rules+panel("Notifications",`<div class="panel-body"><p class="chart-caption">Delivery on ${e(document.notification_host)}</p><form id="notification-form" class="notification-form"><label class="checkbox"><input type="checkbox" name="desktop" ${channels.desktop?"checked":""}>Desktop notification</label><label class="checkbox"><input type="checkbox" name="sound" ${channels.sound?"checked":""}>Sound</label><label class="checkbox"><input type="checkbox" name="on_recovery" ${channels.on_recovery?"checked":""}>Notify on recovery</label><label class="field">Repeat reminder · s<input type="number" name="repeat_seconds" value="${channels.repeat_seconds}" min="10" max="86400"></label><button class="button primary" type="submit">Save</button><button class="button" type="button" data-action="test-notification">Test delivery</button></form>${document.error?`<p class="source-note">${warning(document.error)} ${e(document.error)}</p>`:""}</div>`);
}

export function alertRuleForm(rule, experiments) {
    const metricNames={cpu:"CPU · %",ram:"RAM · %",disk:"Disk usage · %",disk_free_gib:"Disk free space · GiB",internet_receive:"Internet receive · Mbps",internet_transmit:"Internet transmit · Mbps"};
    const kind=rule.kind || "resource";
    return `<form id="alert-rule-form" class="form-stack"><input type="hidden" name="id" value="${e(rule.id || "")}"><label class="field">Name<input name="name" value="${e(rule.name || "")}" required maxlength="100"></label><label class="field">Type<select name="kind" id="rule-kind"><option value="resource" ${kind==="resource"?"selected":""}>Resource threshold</option><option value="errors" ${kind==="errors"?"selected":""}>DAG errors</option><option value="icmp">ICMP Echo Reply</option></select></label><div class="form-grid" data-rule-resource ${kind==="errors"?"hidden":""}><label class="field">Metric<select name="metric">${Object.entries(metricNames).map(([name,label])=>`<option value="${name}" ${name===rule.metric?"selected":""}>${label}</option>`).join("")}</select></label><label class="field">Condition<select name="operator"><option value="above" ${rule.operator!=="below"?"selected":""}>Above</option><option value="below" ${rule.operator==="below"?"selected":""}>Below</option></select></label></div><label class="field">Threshold / error count<input type="number" name="threshold" min="0" step="any" value="${e(rule.threshold ?? 90)}" required></label><label class="field" data-rule-resource ${kind==="errors"?"hidden":""}>Sustained duration · s<input name="duration_seconds" type="number" min="0" max="86400" value="${e(rule.duration_seconds ?? 60)}"></label><div class="form-grid" data-rule-errors ${kind!=="errors"?"hidden":""}><label class="field">Experiment<select name="experiment_id"><option value="">All experiments</option>${experiments.map(row=>`<option value="${e(row.experiment_id)}" ${row.experiment_id===rule.experiment_id?"selected":""}>${e(row.name || row.experiment_id)}</option>`).join("")}</select></label><label class="field">Error window · s<input name="window_seconds" type="number" min="1" max="86400" value="${e(rule.window_seconds ?? 60)}"></label></div><label class="checkbox"><input type="checkbox" name="enabled" ${rule.enabled!==false?"checked":""}>Enabled</label><button class="button primary" type="submit">Save rule</button></form>`;
}
