"use strict";

import {recordKey, updateContent, escape as e, numeric, dateTime, address, badge, warning, panel, empty, unavailable, stat, inspectButton, table, lineChart} from "./ui.js";
import * as views from "./views.js";

const navigation = [
    ["overview", "Overview", "M3 3h7v7H3zM14 3h7v7h-7zM3 14h7v7H3zM14 14h7v7h-7z"],
    ["experiments", "Experiments", "M8 4l12 8-12 8zM3 4v16"],
    ["forecast", "Iterations and forecast", "M20 7v5h-5M4 17v-5h5M6 7a7 7 0 0112-1l2 3M4 15l2 3a7 7 0 0012-1"],
    ["modules", "Modules", "M12 3l9 5-9 5-9-5zM3 12l9 5 9-5M3 16l9 5 9-5"],
    ["services", "Services", "M3 3h18v7H3zM3 14h18v7H3zM6 7h1M6 18h1"],
    ["compute", "Compute resources", "M2 3h20v14H2zM8 21h8M12 17v4M6 12l3-4 4 5 4-7"],
    ["alerts", "Alerts", "M6 8a6 6 0 0112 0v7l2 3H4l2-3zM10 21h4"],
];
const runViews = [
    ["timeline", "Execution", "operations"], ["dag", "DAG", "template"], ["errors", "Errors", "errors"],
    ["events", "Events", "events"], ["resources", "Resources", "measurements"],
    ["artifacts", "Artifacts", "artifacts"], ["settings", "Run settings", "template"],
    ["commands", "Commands", "commands"], ["snapshots", "Snapshots", "snapshots"],
];
const main = document.getElementById("main");
const state = {page: "overview", experiment: null, run: null, detail: null, info: null, icmp: null,
    experiments: [], modules: [], data: null, rows: [], cursor: null, nextCursor: null, journal: null,
    mode: "effective", metrics: [{}, {}, {}], filters: [], loading: false, request: 0,
    abort: null, timer: null, systemAlerts: null, refresh: 5, bookmarks: [], window: "15", settingsMode: "yaml"};

function readLocation() {
    const params = new URLSearchParams(location.search);
    state.page = params.get("page") || "overview";
    state.experiment = params.get("experiment"); state.run = params.get("run");
    state.detail = params.get("detail"); state.revision = params.get("revision");
    if (![...navigation, ...runViews, ["icmp"]].some(([page]) => page === state.page)) state.page = "overview";
}

function toast(message) {
    const target = document.getElementById("toast"); target.textContent = message; target.hidden = false;
    clearTimeout(target.dismissTimer); target.dismissTimer = setTimeout(() => { target.hidden = true; }, 6000);
}

async function request(path, options = {}) {
    const response = await fetch(path, {cache: "no-store", ...options});
    let document;
    try { document = await response.json(); } catch { throw new Error("The server returned an invalid response."); }
    if (!response.ok) {
        const error = new Error(document.error?.message || (typeof document.detail === "string" ? document.detail : "Request failed."));
        error.code = document.error?.code;
        throw error;
    }
    return document;
}

function rows(document) {
    if (!Array.isArray(document?.items) || document.items.some(item => !item || typeof item !== "object" || Array.isArray(item)))
        throw new Error("The system API response is missing its records list.");
    return document.items;
}

async function systemRead(resource, signal, params = {}) {
    const query = new URLSearchParams(params);
    const result = await request(`/api/system/${resource}${query.size ? "?" + query : ""}`, {signal});
    return result;
}

function experimentPath(view, experiment = state.experiment) {
    return `experiments/${encodeURIComponent(experiment)}/${view}`;
}

function checkContext(document, experiment) {
    if (document.experiment_id !== experiment) throw new Error("The system returned data for a different or unspecified experiment.");
}

function navigationChrome() {
    const parent = runViews.some(([page]) => page === state.page) ? "experiments" : state.page === "icmp" ? "alerts" : state.page;
    document.getElementById("navigation").innerHTML = navigation.map(([page, label, icon]) => `<a href="${address(page)}" class="${parent === page ? "active" : ""}" ${parent === page ? 'aria-current="page"' : ""}><svg class="nav-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="${icon}"/></svg>${label}</a>`).join("");
    document.getElementById("bookmarks").innerHTML = state.bookmarks.length ? state.bookmarks.map(item => `<a href="${e(item.href)}">♧ ${e(item.label)}</a>`).join("") : '<p class="muted">Pin a screen for quick access.</p>';
}

function header(title, subtitle = "", extra = "") {
    const pinned = state.bookmarks.some(item => item.href === location.pathname + location.search);
    return `<div class="breadcrumb"><a href="${address("overview")}">EMP</a> / ${e(title)}</div><div class="heading"><div><h1>${e(title)}</h1><p>${e(subtitle)}</p></div><div class="actions">${extra}<button class="button quiet" data-action="reload">Refresh</button><button class="button" data-action="pin">${pinned ? "Remove from" : "Add to"} quick access</button></div></div>`;
}

function tabs() {
    return `<nav class="tabs" aria-label="Experiment views">${runViews.filter(([page]) => page !== "dag").map(([page, label]) => `<a href="${address(page, state.experiment, state.run)}" class="${page === state.page || page === "timeline" && state.page === "dag" ? "active" : ""}">${label}</a>`).join("")}</nav>`;
}

function runHeading(summary) {
    const title = summary?.name || state.experiment;
    const terminal = ["completed", "stopped", "failed"].includes(summary?.status);
    const paused = summary?.status === "paused";
    const controls = summary?.fresh ? [
        ["pause", "Pause", !terminal && !paused], ["resume", "Resume", paused], ["step", "Step", paused],
        ["stop", "Stop", !terminal], ["snapshot", "Snapshot", paused]
    ].map(([command, label, enabled]) => `<button class="button" data-command="${command}" ${enabled ? "" : "disabled"}>${label}</button>`).join("") + '<button class="button" data-action="control-command">More commands</button>' : '<button class="button" data-action="recover-experiment">Recover</button>';
    return header(title, summary?.status ? `${summary.status} · ${dateTime(summary.observed_at)}` : "", `<a class="button quiet" href="${address("experiments")}">← Experiments</a>${controls}`) +
        `<div class="context"><span>Experiment <b class="mono">${e(state.experiment)}</b></span><span>Logical run <b class="mono">${e(state.run || summary?.run_id || "All recorded runs")}</b></span>${summary?.template_revision_id ? `<span>Template <b>${e(summary.template_revision_id)}</b></span>` : ""}</div>${tabs()}`;
}

function rememberFilters() {
    state.filters = [...main.querySelectorAll("[data-table]")].map(scope => ({query: scope.querySelector("[data-filter-query]").value,
        facets: Object.fromEntries([...scope.querySelectorAll("[data-facet]")].map(select => [select.dataset.facet, select.value]))}));
}

function applyFilters(scope) {
    const words = scope.querySelector("[data-filter-query]").value.toLowerCase().trim().split(/\s+/).filter(Boolean);
    const facets = [...scope.querySelectorAll("[data-facet]")];
    let count = 0;
    for (const row of scope.querySelectorAll("[data-row]")) {
        const values = JSON.parse(row.dataset.fields), text = row.textContent.toLowerCase();
        row.hidden = !words.every(word => text.includes(word)) || !facets.every(select => !select.value || values[select.dataset.facet] === select.value);
        if (!row.hidden) count++;
    }
    scope.querySelector("[data-count]").textContent = count;
    scope.querySelector("[data-empty-row]").hidden = count > 0;
    const radios = [...scope.querySelectorAll('input[name="experiment"]')];
    if (radios.length) {
        const visible = radios.filter(radio => !radio.closest("tr").hidden);
        if (state.experiment && !visible.some(radio => radio.checked)) {
            radios.forEach(radio => { radio.checked = radio === visible[0]; });
            void selectExperiment(visible[0]?.value || null);
        }
    }
}

function restoreFilters() {
    main.querySelectorAll("[data-table]").forEach((scope, index) => {
        const saved = state.filters[index];
        if (saved) {
            scope.querySelector("[data-filter-query]").value = saved.query;
            scope.querySelectorAll("[data-facet]").forEach(select => {
                if ([...select.options].some(option => option.value === saved.facets[select.dataset.facet])) select.value = saved.facets[select.dataset.facet];
            });
        }
        applyFilters(scope);
    });
}

function updateAlertCount() {
    const local = state.icmp?.incidents.filter(item => item.status === "active").length;
    const knownLocal = Number.isInteger(local);
    const count = document.getElementById("alert-count");
    count.textContent = Number.isInteger(state.systemAlerts) && knownLocal ? state.systemAlerts + local : knownLocal && local ? `${local}+` : "—";
    count.parentElement.setAttribute("aria-label", Number.isInteger(state.systemAlerts) && knownLocal ? `${local + state.systemAlerts} active Alerts` : `${knownLocal ? local : "Unknown"} local Alerts; system Alert count unavailable`);
    count.parentElement.title = count.parentElement.getAttribute("aria-label");
}

async function refreshConnection() {
    if (state.connectionRequest) return state.connectionRequest;
    state.connectionRequest = request("/api/application").then(info => {
        state.info = info;
        const initial = info.cache_activity?.building || [];
        const cacheStatus = document.getElementById("cache-activity");
        cacheStatus.hidden = initial.length === 0;
        cacheStatus.textContent = initial.length ? `Building history cache for ${initial.join(", ")}. Pages may respond more slowly until caching finishes.` : "";
        const connection = info.system_connection;
        const status = document.getElementById("connection-status");
        const connected = connection?.connected === true;
        status.textContent = connected ? "System connected" : "System unavailable";
        status.className = connected ? "connection online" : "connection offline";
        status.title = connected ? `Runtime observed: ${dateTime(connection.observed_at)}` : connection?.error || (info.system_api_configured ? "Waiting for a current runtime observation." : "System API is not configured.");
    }).catch(markOffline).finally(() => { state.connectionRequest = null; });
    return state.connectionRequest;
}

async function refreshICMP() {
    void refreshConnection();
    // Alert availability is independent of the selected screen or a failed resource source.
    if (!state.alertRequest) {
        state.alertRequest = request("/api/system/alerts").then(result => {
            state.systemAlerts = Number.isInteger(result.active_count) && result.active_count >= 0 ? result.active_count : null;
        }, () => { state.systemAlerts = null; }).finally(() => {
            state.alertRequest = null;
            updateAlertCount();
        });
    }
    try {
        state.icmp = await request("/api/icmp"); updateAlertCount();
        const container = document.getElementById("icmp-panel");
        if (container && !container.contains(document.activeElement)) updateContent(container, views.icmpPanel(state.icmp, state.page !== "icmp"));
    } catch (error) {
        const container = document.getElementById("icmp-panel");
        if (container && !container.contains(document.activeElement)) container.innerHTML = panel("ICMP Echo Reply", empty("Monitor unavailable", error.message));
    }
}

async function selectExperiment(identity) {
    state.experiment = identity;
    const params = new URLSearchParams(location.search);
    if (identity) params.set("experiment", identity); else params.delete("experiment");
    history.replaceState(null, "", "/?" + params);
    const target = document.getElementById(state.page === "forecast" ? "forecast-body" : "run-history");
    if (!target) return;
    if (!identity) { target.innerHTML = empty("No experiment selected"); return; }
    if (target.experiment !== identity) {
        target.innerHTML = '<div class="loading">Loading execution history…</div>';
        target.rendered = null;
    }
    target.experiment = identity;
    const serial = target.request = (target.request || 0) + 1;
    try {
        const response = await systemRead(experimentPath(state.page === "forecast" ? "forecast" : "runs", identity));
        checkContext(response, identity);
        if (state.experiment !== identity || !target.isConnected || serial !== target.request) return;
        const content = state.page === "forecast" ? views.forecast(response, state.metrics) : views.runHistory(rows(response), identity);
        if (state.page === "forecast") state.data = response;
        if (target.rendered !== content) { updateContent(target, content); target.rendered = content; }
    } catch (error) {
        if (target.isConnected && state.experiment === identity && serial === target.request) {
            target.innerHTML = unavailable(error); target.rendered = null;
        }
    }
}

function renderAlerts(document) {
    const incidents = state.icmp?.incidents || [];
    const system = document?.items || [];
    if (document && Number.isInteger(document.active_count)) state.systemAlerts = document.active_count;
    const allIncidents = [...incidents, ...system];
    const active = allIncidents.filter(row => row.status === "active").length;
    const buckets = new Map();
    for (const incident of allIncidents) {
        const minute = new Date(incident.started_at); minute.setSeconds(0, 0);
        if (!Number.isFinite(minute.valueOf())) continue;
        const key = minute.toISOString(); buckets.set(key, (buckets.get(key) || 0) + 1);
    }
    const samples = [...buckets].sort(([a], [b]) => a.localeCompare(b)).map(([observed_at, value]) => ({observed_at, value}));
    return panel("Alert activity", `<div class="panel-body grid2"><div><p class="chart-caption">ICMP incidents · dashboard host</p>${lineChart(samples, "value", "incidents")}</div><div><span class="muted">Active Alerts</span><div class="error-count">${active}</div><a class="button" href="${address("icmp")}">ICMP rule</a></div></div>`) +
        panel("ICMP incidents", table(incidents.slice().reverse(), [["Target", row => e(row.host)], ["Status", row => badge(row.status)], ["Started", row => e(dateTime(row.started_at))], ["Ended", row => e(dateTime(row.ended_at))], ["Probe source", row => e(row.probe_host)], ["Details", row => inspectButton("Inspect", row)]], [["status", "Status"]])) +
        panel("System Alerts", document ? table(system, [["Rule", row => inspectButton(row.name || row.type, row)], ["Status", row => badge(row.status)], ["Started", row => e(dateTime(row.started_at))]], [["status", "Status"]]) : empty("System Alerts unavailable"));
}

function renderRun(response) {
    const items = ["dag", "settings"].includes(state.page) ? [] : rows(response);
    const key = response.journal ? `${response.journal.journal_id}/${response.journal.generation}` : null;
    if (state.journal && key && key !== state.journal) { state.rows = []; state.cursor = null; toast("Execution history changed. The previous selection was cleared."); document.getElementById("details-dialog").close(); }
    state.journal = key;
    if (state.cursor) {
        const byId = new Map(state.rows.map((row, index) => [row.event_id || row.operation_id || `previous-${index}`, row]));
        items.forEach((row, index) => byId.set(row.event_id || row.operation_id || `page-${state.rows.length + index}`, row));
        state.rows = [...byId.values()];
    } else state.rows = items;
    state.nextCursor = response.next_cursor || null;
    let content = "";
    switch (state.page) {
        case "timeline": content = `<div class="actions" style="margin-bottom:16px"><a class="button" href="${address("timeline", state.experiment, state.run)}">Timeline</a><a class="button quiet" href="${address("dag", state.experiment, state.run)}">DAG</a></div>${views.timeline(state.rows, response.observed_at)}`; break;
        case "dag": content = `<div class="actions" style="margin-bottom:16px"><a class="button quiet" href="${address("timeline", state.experiment, state.run)}">Timeline</a><a class="button" href="${address("dag", state.experiment, state.run)}">DAG</a></div>${views.dag(response)}`; break;
        case "events": content = views.events(state.rows, state.mode); break;
        case "errors": content = views.errors(state.rows); break;
        case "resources": content = `<div id="metric-cards">${views.metricCards(state.data?.measurements || state.rows, state.metrics, null, state.data?.metric_summaries, state.data?.measurement_cycles)}</div>`; break;
        case "commands": content = panel("Commands", table(state.rows, [["Command", row => inspectButton(row.command || row.name, row)], ["Target", row => e(row.target || "—")], ["Status", row => badge(row.status || row.outcome)], ["Run", row => e(row.run_id)], ["Sent", row => e(dateTime(row.sent_at))]], [["kind", "Kind"], ["status", "Status"]])); break;
        case "snapshots": content = panel("Snapshots & recovery", table(state.rows, [["Snapshot", row => inspectButton(row.snapshot_id, row)], ["State", row => badge(row.status)], ["Template", row => e(row.template_revision_id)], ["Cycle", row => numeric(row.cycle_number)], ["Created", row => e(dateTime(row.created_at))], ["", row => `<button class="button quiet" data-restore="${e(row.snapshot_id)}" ${row.available === false || !state.controlAvailable ? "disabled" : ""}>Restore</button>`]], [["status", "State"]])); break;
        case "artifacts": content = panel("Artifacts", table(state.rows, [["Artifact", row => inspectButton(row.path || row.name, row)], ["Purpose", row => e(row.purpose || "—")], ["Module", row => e(row.module_name || "—")], ["Attempt", row => e(row.attempt_id || "—")], ["Size", row => row.size_bytes == null ? "—" : numeric(row.size_bytes) + " B"], ["", row => row.artifact_id ? `<a class="button quiet" href="/api/experiments/${encodeURIComponent(state.experiment)}/artifacts/${encodeURIComponent(row.artifact_id)}/download" download>Download</a>` : "—"]], [["purpose", "Purpose"], ["module_name", "Module"]])); break;
        case "settings": content = panel("Recorded settings", `<div class="panel-body"><label class="field" style="margin-bottom:16px">Template revision<select id="template-revision"><option value="">Latest in this selection</option>${(response.revisions || []).map(row=>`<option value="${e(row.template_revision_id)}" ${state.revision === row.template_revision_id ? "selected" : ""}>${e(dateTime(row.occurred_at))} · ${e(row.template_revision_id)}</option>`).join("")}</select></label><div class="segmented" style="margin-bottom:16px"><button data-settings="yaml" class="${state.settingsMode === "yaml" ? "active" : ""}">YAML</button><button data-settings="json" class="${state.settingsMode === "json" ? "active" : ""}">JSON</button><button data-settings="parameters" class="${state.settingsMode === "parameters" ? "active" : ""}">Attempt parameters</button></div><div id="settings-content">${state.settingsMode === "parameters" ? views.parameters(state.parameterRows || []) : `<pre>${e(state.settingsMode === "yaml" ? response.template_yaml || "Original YAML is unavailable." : JSON.stringify(response.template || {}, null, 2))}</pre>`}</div></div>`); break;
    }
    return (response.complete === false ? `<div class="error-banner">${warning(response.error || "History is still loading. Aggregate statistics may be incomplete.")} ${e(response.error || "History is still loading.")}</div>` : "") + content + (state.nextCursor ? '<button class="button" data-action="load-more">Load more records</button>' : "");
}

async function loadPage(automatic = false, retryHistory = true) {
    clearTimeout(state.historyTimer);
    // Paged history is a browsing session. Refresh explicitly to return to its first page.
    if (automatic && state.cursor) return;
    if (automatic && (state.loading || main.contains(document.activeElement) && /INPUT|SELECT|TEXTAREA/.test(document.activeElement.tagName))) return;
    state.abort?.abort(); state.abort = new AbortController(); const signal = state.abort.signal;
    const serial = ++state.request; state.loading = true; if (automatic) rememberFilters();
    const context = `${state.experiment || ""}:${state.run || ""}`;
    const sameExperiment = runViews.some(([page]) => page === state.page) && main.dataset.context === context && main.querySelector(".tabs");
    main.setAttribute("aria-busy", "true");
    if (!automatic && !sameExperiment) {
        main.innerHTML = header("Loading…") + '<div class="loading" role="status">Loading observations…</div>';
    } else if (!automatic) {
        main.querySelectorAll(".tabs a").forEach(link => link.classList.toggle("active", new URL(link.href).searchParams.get("page") === state.page));
        const loading = document.createElement("span"); loading.id = "page-loading"; loading.className = "loading"; loading.setAttribute("role", "status"); loading.textContent = "Loading…";
        document.getElementById("page-loading")?.remove(); main.querySelector(".heading .actions")?.prepend(loading);
    }
    if (!automatic) navigationChrome();
    void refreshConnection();
    let title = navigation.find(([page]) => page === state.page)?.[1] || runViews.find(([page]) => page === state.page)?.[1] || "ICMP ping";
    let pendingHistory = false;
    try {
        let content = "", heading = header(title);
        if (state.page === "icmp") { heading = header(title, "Checks run on the dashboard host", `<a class="button quiet" href="${address("alerts")}">← Alerts</a>`); content = `<div id="icmp-panel">${views.icmpPanel(state.icmp, false)}</div>`; }
        else if (state.page === "overview") {
            let data = null, error = null;
            try { data = await systemRead("overview", signal); if (Number.isInteger(data.active_alerts)) state.systemAlerts = data.active_alerts; } catch (failure) { error = failure; }
            content = (error ? `<div class="error-banner">${e(error.message)}</div>` : "") + views.overview(data, state.icmp);
        } else if (state.page === "alerts") {
            let data = null; try { data = await systemRead("alerts", signal); rows(data); } catch { data = null; }
            state.experiments = rows(await systemRead("experiments", signal).catch(() => ({items: []})));
            const settings = await request("/api/alerts");
            state.alertSettings = settings;
            content = renderAlerts(data) + views.alertControls(settings, state.experiments);
        } else if (["experiments", "forecast"].includes(state.page)) {
            if (state.page === "experiments") heading = header(title, "", '<button class="button primary" data-action="run-experiment">Run experiment</button>');
            const document = await systemRead("experiments", signal); state.experiments = rows(document);
            const candidates = state.page === "forecast" ? state.experiments.filter(row => ["running", "paused", "waiting"].includes(row.status)) : state.experiments;
            if (!candidates.some(row => row.experiment_id === state.experiment)) state.experiment = state.page === "forecast" ? candidates[0]?.experiment_id || null : null;
            content = views.experiments(candidates, state.experiment);
            if (state.page === "forecast") content = content.replace('id="run-history"', 'id="forecast-body"');
        } else if (state.page === "modules") { const document = await systemRead("modules", signal); state.modules = rows(document); content = (document.complete === false ? `<div class="error-banner">${e(document.error || "Some experiment histories have not been cached. Open an experiment to include its statistics.")}</div>` : "") + views.modules(state.modules, state.detail); }
        else if (state.page === "services") content = views.services(rows(await systemRead("services", signal)), state.detail);
        else if (state.page === "compute") {
            const params = {};
            if (state.range) Object.assign(params, state.range);
            else params.since = new Date(Date.now() - Number(state.window) * 60000).toISOString();
            const compute = await systemRead("compute", signal, params);
            content = views.compute(compute);
            const settings = await request("/api/alerts");
            content += panel("Alerts", table(settings.rules.filter(rule => rule.kind === "resource"), [["Rule", row => e(row.name)], ["Metric", row => e(row.metric)], ["Condition", row => `${e(row.operator)} ${e(row.threshold)}`], ["Duration", row => `${numeric(row.duration_seconds)} s`], ["State", row => badge(row.enabled ? "enabled" : "disabled")]], [], "rules"), `<a href="${address("alerts")}">Configure</a>`);
            heading = header(title, "Runtime host", `<label class="refresh-control">Refresh<select id="refresh-rate">${[1,2,5,10,15,30,60,120,300,600].map(value => `<option value="${value}" ${value === state.refresh ? "selected" : ""}>${value < 60 ? value + " s" : value / 60 + " min"}</option>`).join("")}</select></label>`);
        } else {
            if (!state.experiment) {
                state.experiments = rows(await systemRead("experiments", signal));
                state.experiment = state.experiments[0]?.experiment_id || null;
            }
            if (!state.experiment) { content = empty("No experiment selected", "Open Experiments and select an execution history."); }
            else {
                const selected = state.experiment;
                const params = {limit: "200", compact: "1"}; if (state.run) params.run_id = state.run;
                if (state.page === "settings" && state.revision) params.revision = state.revision;
                if (state.page === "events") params.view = state.mode;
                if (state.cursor) params.cursor = typeof state.cursor === "string" ? state.cursor : JSON.stringify(state.cursor);
                const endpoint = runViews.find(([page]) => page === state.page)[2];
                const document = await systemRead(experimentPath(endpoint), signal, params);
                if (serial !== state.request) return;
                const summary = document.summary || null;
                checkContext(document, selected); if (summary) checkContext(summary, selected);
                if (state.page === "settings" && state.settingsMode === "parameters") {
                    const attempts = await systemRead(experimentPath("parameters"), signal, {compact: "1", ...(state.run ? {run_id: state.run} : {})});
                    checkContext(attempts, selected); state.parameterRows = rows(attempts);
                }
                state.controlAvailable = Boolean(summary?.fresh); state.data = document; heading = runHeading(summary); content = renderRun(document);
                pendingHistory = document.complete === false && document.cache_pending;
            }
        }
        if (serial !== state.request) return;
        updateContent(main, heading + content);
        main.dataset.context = `${state.experiment || ""}:${state.run || ""}`;
        const dialog = document.getElementById("details-dialog");
        if (dialog.open && dialog.dataset.recordKey && dialog.dataset.selection === location.search) {
            const present = [...main.querySelectorAll("[data-inspect]")].some(button => recordKey(JSON.parse(button.dataset.inspect)) === dialog.dataset.recordKey);
            if (!present) { dialog.close(); toast("The inspected record is no longer in the current view."); }
        }
        document.title = `${title} · EMP`;
        if (["experiments", "forecast"].includes(state.page)) void selectExperiment(state.experiment);
        if (state.page === "compute") {
            const field = document.getElementById("resource-window");
            if (field) field.value = state.window;
            main.querySelectorAll("[data-custom-range]").forEach(label => { label.hidden = state.window !== "custom"; });
            if (state.range) {
                for (const [id, key] of [["range-from", "since"], ["range-to", "until"]]) {
                    const local = new Date(state.range[key]);
                    local.setMinutes(local.getMinutes() - local.getTimezoneOffset());
                    document.getElementById(id).value = local.toISOString().slice(0, 16);
                }
            }
        }
        restoreFilters(); updateAlertCount();
    } catch (error) {
        if (error.name === "AbortError" || serial !== state.request) return;
        if (error.code === "history_changed" && retryHistory) {
            if (state.cursor) {
                state.cursor = null; state.rows = []; document.getElementById("details-dialog").close();
                toast("History changed. Loading its first page.");
            }
            state.loading = false;
            return await loadPage(automatic, false);
        }
        if (automatic) {
            let banner = document.getElementById("refresh-error");
            if (!banner) { banner = document.createElement("div"); banner.id = "refresh-error"; banner.className = "error-banner"; main.prepend(banner); }
            banner.textContent = "Refresh failed; showing previous observations. " + error.message;
        } else { main.innerHTML = header(title) + unavailable(error); }
    } finally {
        if (serial === state.request) {
            state.loading = false; main.removeAttribute("aria-busy"); document.getElementById("page-loading")?.remove();
            if (pendingHistory) {
                state.historyTimer = setTimeout(() => { if (serial === state.request) void loadPage(true); }, 50);
            }
        }
    }
}

function markOffline(error) {
    const status = document.getElementById("connection-status"); status.textContent = "System unavailable"; status.className = "connection offline"; status.title = error.message;
}

function navigate(url) {
    history.pushState(null, "", url); readLocation(); state.filters = []; state.cursor = null; state.rows = []; state.journal = null; state.metrics = [{}, {}, {}];
    void loadPage(); window.scrollTo(0, 0);
}

document.addEventListener("click", async event => {
    const link = event.target.closest("a[href]");
    if (link && !event.ctrlKey && !event.metaKey && !event.shiftKey && event.button === 0) {
        const url = new URL(link.href, location.href);
        if (url.origin === location.origin && url.pathname === "/") { event.preventDefault(); closeSearch(); navigate(url.pathname + url.search); return; }
    }
    const button = event.target.closest("button"); if (!button) return;
    if (button.dataset.action === "recover-experiment") { await sendCommand("recover", {experiment_id: state.experiment}); return; }
    if (button.dataset.action === "control-command") {
        document.getElementById("detail-title").textContent = "Control current experiment";
        document.getElementById("detail-content").innerHTML = `<form id="control-form" class="command-form"><label class="field">Command<select name="command">${["rerun","retry","move","reset_retries","reload_template"].map(name=>`<option>${name}</option>`).join("")}</select></label><label class="field">Arguments ? JSON<textarea name="args" rows="6" spellcheck="false">{}</textarea></label><button class="button primary">Send command</button></form>`;
        document.getElementById("details-dialog").showModal(); return;
    }
    if (button.dataset.command) { await sendCommand(button.dataset.command, {}); return; }
    if (button.dataset.restore) {
        if (confirm("Restore this snapshot? Later experiment history will be replaced and execution will be paused.")) await sendCommand("rollback", {snapshot_id: button.dataset.restore});
        return;
    }
    if (button.dataset.editRule) { document.getElementById("detail-title").textContent = "Alert rule"; document.getElementById("detail-content").innerHTML = views.alertRuleForm(JSON.parse(button.dataset.editRule), state.experiments); document.getElementById("details-dialog").showModal(); return; }
    if (button.dataset.deleteRule) { try { await request("/api/alerts/rules/" + encodeURIComponent(button.dataset.deleteRule), {method: "DELETE", headers: {"X-Dashboard-Request": "1"}}); await loadPage(); } catch (error) { toast(error.message); } return; }
    if (button.dataset.inspect) {
        let record = JSON.parse(button.dataset.inspect); const pre = document.createElement("pre"); pre.textContent = JSON.stringify(record, null, 2);
        document.getElementById("detail-title").textContent = record.name || record.event_type || record.operation_type || "Recorded details";
        document.getElementById("detail-content").replaceChildren(pre);
        const dialog = document.getElementById("details-dialog"); dialog.showModal();
        dialog.dataset.recordKey = recordKey(record); dialog.dataset.selection = location.search;
        const selected = state.experiment, selection = location.search;
        if (record.detail_ref) {
            try {
                const response = await systemRead(experimentPath("detail", selected), undefined, {ref: JSON.stringify(record.detail_ref)});
                if (!dialog.open || !pre.isConnected || selection !== location.search || state.experiment !== selected) return;
                checkContext(response, selected);
                pre.textContent = JSON.stringify({...record, ...response.record}, null, 2);
            } catch (error) { if (pre.isConnected && dialog.open) pre.textContent = "Details unavailable: " + error.message; }
        }
        return;
    }
    if (button.dataset.mode) { state.mode = button.dataset.mode; state.cursor = null; state.rows = []; state.filters = []; await loadPage(); return; }
    if (button.dataset.settings) {
        state.settingsMode = button.dataset.settings;
        main.querySelectorAll("[data-settings]").forEach(item => item.classList.toggle("active", item === button));
        const container = document.getElementById("settings-content");
        if (state.settingsMode === "parameters") {
            container.innerHTML = '<div class="loading">Loading attempt parameters…</div>';
            const selected = state.experiment, selection = location.search;
            try {
                const result = await systemRead(experimentPath("parameters"), undefined, {compact: "1", ...(state.run ? {run_id: state.run} : {})});
                checkContext(result, selected);
                if (!container.isConnected || selection !== location.search || state.settingsMode !== "parameters") return;
                state.parameterRows = rows(result); updateContent(container, views.parameters(state.parameterRows));
            } catch (error) { if (container.isConnected && state.settingsMode === "parameters") updateContent(container, unavailable(error)); }
        } else container.innerHTML = `<pre>${e(state.settingsMode === "yaml" ? state.data.template_yaml || "Original YAML unavailable." : JSON.stringify(state.data.template || {}, null, 2))}</pre>`;
        return;
    }
    switch (button.dataset.action) {
        case "reload": state.cursor = null; await loadPage(); void refreshICMP(); break;
        case "close-dialog": document.getElementById("details-dialog").close(); break;
        case "run-experiment":
            document.getElementById("detail-title").textContent = "Run experiment";
            document.getElementById("detail-content").innerHTML = '<form id="run-form" class="form-stack"><label class="field">Experiment ID<input name="experiment_id" required></label><label class="field">Template path on the runtime host<input name="template_path" required></label><button class="button primary" type="submit">Run</button></form>';
            document.getElementById("details-dialog").showModal(); break;
        case "new-rule":
            document.getElementById("detail-title").textContent = "New Alert rule";
            document.getElementById("detail-content").innerHTML = views.alertRuleForm({}, state.experiments);
            document.getElementById("details-dialog").showModal(); break;
        case "test-notification":
            try { const result = await request('/api/alerts/notifications/test', {method:'POST',headers:{'X-Dashboard-Request':'1'}}); toast(result.status === 'submitted' ? 'Notification submitted on the dashboard host.' : result.message || 'Enable a notification channel first.'); } catch(error) { toast(error.message); } break;
        case "reset-filters": { const scope = button.closest("[data-table]"); scope.querySelectorAll("input[type=search],select").forEach(field => { field.value = ""; }); applyFilters(scope); break; }
        case "pin": {
            const href = location.pathname + location.search;
            const index = state.bookmarks.findIndex(item => item.href === href);
            if (index >= 0) state.bookmarks.splice(index, 1); else state.bookmarks.push({href, label: main.querySelector("h1").textContent + (state.run ? " · " + state.run : "")});
            try { localStorage.setItem("emp-dashboard-bookmarks", JSON.stringify(state.bookmarks)); } catch { toast("Browser storage is unavailable; bookmarks will last for this session."); }
            button.textContent = (index >= 0 ? "Add to" : "Remove from") + " quick access"; navigationChrome(); break;
        }
        case "probe":
            button.disabled = true; button.textContent = "Probing…";
            try { state.icmp = await request("/api/icmp/probe", {method: "POST", headers: {"X-Dashboard-Request": "1"}}); toast(state.icmp.status === "reply" ? `Echo Reply received by ${state.icmp.probe_host}.` : "Probe finished: " + state.icmp.status.replaceAll("_", " ")); }
            catch (error) { toast(error.message); }
            finally { button.disabled = false; button.textContent = "Check now"; await refreshICMP(); }
            break;
        case "load-more": state.cursor = state.nextCursor; rememberFilters(); await loadPage(); break;
        case "resource-range": {
            state.window = document.getElementById("resource-window").value;
            if (state.window === "custom") {
                const since = new Date(document.getElementById("range-from").value), until = new Date(document.getElementById("range-to").value);
                if (!Number.isFinite(since.valueOf()) || !Number.isFinite(until.valueOf()) || since >= until) { toast("Choose a valid start and end time."); break; }
                state.range = {since: since.toISOString(), until: until.toISOString()};
            } else state.range = null;
            await loadPage(); break;
        }
    }
});

async function sendCommand(command, args) {
    if (command === "stop" && !confirm("Stop the current execution? Active work may be interrupted.")) return;
    try {
        const receipt = await request("/api/commands", {method:"POST",headers:{"Content-Type":"application/json","X-Dashboard-Request":"1"},body:JSON.stringify({command,args,...(!["run","recover"].includes(command) && state.experiment ? {expected_experiment_id:state.experiment} : {})})});
        document.getElementById("details-dialog").close();
        toast("Command submitted. Waiting for its outcome.");
        const deadline = Date.now() + 30000;
        while (Date.now() < deadline) {
            const outcome = await request("/api/commands/" + encodeURIComponent(receipt.command_id));
            if (outcome.state !== "pending") { toast(outcome.result === "success" ? "Command completed." : outcome.error?.message || `Command ${outcome.state}.`); await loadPage(); return; }
            await new Promise(resolve=>setTimeout(resolve,500));
        }
        toast("The command is still pending. Its outcome remains available in Commands.");
    } catch (error) { toast(error.message); }
}

document.addEventListener("submit", async event => {
    if (event.target.id === "control-form") {
        event.preventDefault();
        try {
            const fields = new FormData(event.target);
            const args = JSON.parse(fields.get("args"));
            if (!args || typeof args !== "object" || Array.isArray(args)) throw new Error("Arguments must be a JSON object.");
            await sendCommand(fields.get("command"), args);
        } catch (error) { toast(error.message); }
        return;
    }
    const form=event.target;
    const formId=form.getAttribute("id");
    if (!["run-form","alert-rule-form","notification-form"].includes(formId)) return;
    event.preventDefault();
    const fields=new FormData(form);
    try {
        if (formId === "run-form") { await sendCommand("run", {experiment_id:fields.get("experiment_id"),template_path:fields.get("template_path")}); return; }
        if (formId === "notification-form") {
            await request("/api/alerts/notifications",{method:"PUT",headers:{"Content-Type":"application/json","X-Dashboard-Request":"1"},body:JSON.stringify({desktop:fields.has("desktop"),sound:fields.has("sound"),on_recovery:fields.has("on_recovery"),repeat_seconds:Number(fields.get("repeat_seconds"))})});
        } else {
            const rule={name:fields.get("name"),enabled:fields.has("enabled"),kind:fields.get("kind"),metric:fields.get("metric"),operator:fields.get("operator"),threshold:Number(fields.get("threshold")),duration_seconds:Number(fields.get("duration_seconds")),window_seconds:Number(fields.get("window_seconds")),experiment_id:fields.get("experiment_id")||null};
            if(fields.get("id"))rule.id=fields.get("id");
            await request("/api/alerts/rules",{method:"POST",headers:{"Content-Type":"application/json","X-Dashboard-Request":"1"},body:JSON.stringify(rule)});
        }
        document.getElementById("details-dialog").close(); toast("Settings saved."); await loadPage();
    } catch(error) {toast(error.message);}
});

document.addEventListener("change", event => {
    if (event.target.id !== "rule-kind") return;
    const kind=event.target.value, form=event.target.form;
    if(kind === "icmp") {document.getElementById("details-dialog").close(); navigate(address("icmp"));return;}
    form.querySelectorAll('[data-rule-resource]').forEach(element=>{element.hidden=kind!=="resource";});
    form.querySelectorAll('[data-rule-errors]').forEach(element=>{element.hidden=kind!=="errors";});
    if(kind === "errors") form.elements.threshold.value=10;
});

main.addEventListener("input", event => { const scope = event.target.closest("[data-table]"); if (scope && event.target.matches("[data-filter-query]")) applyFilters(scope); });
main.addEventListener("change", event => {
    const field = event.target;
    if (field.dataset.facet) applyFilters(field.closest("[data-table]"));
    if (field.name === "experiment") { state.metrics = [{}, {}, {}]; void selectExperiment(field.value); }
    if (field.dataset.metricModule !== undefined || field.dataset.metricName !== undefined) {
        const index = Number(field.dataset.metricModule ?? field.dataset.metricName);
        if (field.dataset.metricModule !== undefined) state.metrics[index] = {module: field.value}; else state.metrics[index].metric = field.value;
        const target = document.getElementById("metric-cards");
        if (target) target.innerHTML = views.metricCards(state.data?.measurements || state.rows, state.metrics, null, state.data?.metric_summaries, state.data?.measurement_cycles);
        else document.getElementById("forecast-body").innerHTML = views.forecast(state.data, state.metrics);
    }
    if (field.id === "template-revision") {
        const query = new URLSearchParams(location.search);
        if (field.value) query.set("revision", field.value); else query.delete("revision");
        navigate("/?" + query); return;
    }
    if (field.id === "resource-window") main.querySelectorAll("[data-custom-range]").forEach(label => { label.hidden = field.value !== "custom"; });
    if (field.id === "refresh-rate") { state.refresh = Number(field.value); schedule(); }
});

main.addEventListener("submit", async event => {
    if (event.target.id !== "icmp-form") return; event.preventDefault();
    const form = event.target, data = new FormData(form), button = form.querySelector('[type="submit"]');
    const settings = {enabled: data.has("enabled"), host: data.get("host"), timeout_seconds: Number(data.get("timeout_seconds")), interval_seconds: Number(data.get("interval_seconds"))};
    button.disabled = true;
    try { state.icmp = await request("/api/icmp/settings", {method: "PUT", headers: {"Content-Type": "application/json", "X-Dashboard-Request": "1"}, body: JSON.stringify(settings)}); toast("ICMP settings saved on " + state.icmp.probe_host + "."); button.blur(); document.getElementById("icmp-panel").innerHTML = views.icmpPanel(state.icmp, state.page !== "icmp"); updateAlertCount(); }
    catch (error) { toast(error.message); button.disabled = false; }
});

const search = document.getElementById("global-search"), results = document.getElementById("search-results");
let searchSelection = -1;
function closeSearch() { results.hidden = true; search.setAttribute("aria-expanded", "false"); search.removeAttribute("aria-activedescendant"); searchSelection = -1; }
function searchNavigation() {
    const candidates = [...navigation.map(([page, name]) => ({name, description: "Screen", href: address(page)})),
        ...runViews.map(([page, name]) => ({name, description: "Experiment view", href: address(page, state.experiment, state.run)})),
        {name: "ICMP ping", description: "Dashboard host monitoring", href: address("icmp")},
        ...state.experiments.map(row => ({name: row.name || row.experiment_id, description: row.experiment_id, href: address("timeline", row.experiment_id, row.run_id)})),
        ...state.modules.map(row => ({name: row.name, description: row.version, href: address("modules", null, null, {detail: row.module_id || `${row.name}/${row.version}`})}))];
    const words = search.value.toLowerCase().trim().split(/\s+/);
    const matches = candidates.filter(item => words.every(word => `${item.name} ${item.description}`.toLowerCase().includes(word))).slice(0, 10);
    results.innerHTML = matches.length ? matches.map((item, index) => `<a id="search-${index}" role="option" aria-selected="false" href="${e(item.href)}">${e(item.name)}<small>${e(item.description)}</small></a>`).join("") : '<div class="empty">No matches</div>';
    results.hidden = false; searchSelection = -1; search.setAttribute("aria-expanded", "true");
}
search.addEventListener("input", searchNavigation); search.addEventListener("focus", searchNavigation);
search.addEventListener("keydown", event => {
    if (event.key === "Escape") { closeSearch(); return; }
    if (!["ArrowDown", "ArrowUp", "Enter"].includes(event.key)) return;
    event.preventDefault(); if (results.hidden) searchNavigation();
    const links = [...results.querySelectorAll("a")]; if (!links.length) return;
    if (event.key === "Enter") { links[Math.max(0, searchSelection)].click(); return; }
    searchSelection = (searchSelection + (event.key === "ArrowDown" ? 1 : -1) + links.length) % links.length;
    links.forEach((link, index) => link.setAttribute("aria-selected", String(index === searchSelection)));
    search.setAttribute("aria-activedescendant", links[searchSelection].id); links[searchSelection].scrollIntoView({block: "nearest"});
});
document.addEventListener("keydown", event => {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") { event.preventDefault(); search.focus(); search.select(); }
    if (event.key === "Escape") closeSearch();
});
document.addEventListener("click", event => { if (!event.target.closest(".global-search")) closeSearch(); });
window.addEventListener("popstate", () => { readLocation(); state.cursor = null; state.rows = []; state.filters = []; state.journal = null; state.metrics = [{}, {}, {}]; void loadPage(); });

function schedule() {
    clearInterval(state.timer);
    state.timer = setInterval(() => { if (!document.hidden) { void refreshICMP(); void loadPage(true); } }, state.refresh * 1000);
}

async function start() {
    readLocation();
    try { const saved = JSON.parse(localStorage.getItem("emp-dashboard-bookmarks") || "[]"); state.bookmarks = Array.isArray(saved) ? saved.filter(item => typeof item.label === "string" && typeof item.href === "string" && item.href.startsWith("/?page=")) : []; } catch { state.bookmarks = []; }
    navigationChrome();
    await refreshConnection();
    if (state.info) state.refresh = state.info.refresh_seconds;
    await refreshICMP(); await loadPage(); schedule();
}
void start();
