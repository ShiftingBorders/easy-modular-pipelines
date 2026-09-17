"use strict";

export function escape(value) {
    return String(value ?? "").replace(/[&<>"']/g, character => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[character]);
}

export function numeric(value, digits = 2) {
    return typeof value === "number" && Number.isFinite(value)
        ? value.toLocaleString("en", {maximumFractionDigits: digits}) : "—";
}

export function duration(seconds) {
    if (typeof seconds !== "number" || !Number.isFinite(seconds) || seconds < 0) return "—";
    if (seconds < 1) return `${numeric(seconds * 1000, 1)} ms`;
    if (seconds < 60) return `${numeric(seconds, 1)} s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)} min ${Math.floor(seconds % 60)} s`;
    return `${Math.floor(seconds / 3600)} h ${Math.floor(seconds % 3600 / 60)} min`;
}

export function dateTime(value) {
    if (!value) return "—";
    const date = new Date(value);
    return Number.isFinite(date.valueOf()) ? date.toLocaleString("en-GB", {hour12: false}) : "—";
}

export function address(page, experiment = null, run = null, extra = {}) {
    const query = new URLSearchParams({page, ...extra});
    if (experiment) query.set("experiment", experiment);
    if (run) query.set("run", run);
    return "/?" + query;
}

export function badge(value) {
    const status = String(value || "Unknown");
    const colors = {succeeded:"good", completed:"good", ready:"good", reply:"good", resolved:"good",
        failed:"bad", error:"bad", no_reply:"bad", timed_out:"bad", active:"bad",
        running:"active", starting:"active", paused:"warn", waiting:"warn", stale:"warn"};
    const label = status.replaceAll("_", " ");
    return `<span class="badge ${colors[status.toLowerCase()] || ""}">${escape(label.charAt(0).toUpperCase() + label.slice(1))}</span>`;
}

export function warning(message) {
    return `<span class="warning" tabindex="0" role="img" aria-label="${escape(message)}" title="${escape(message)}">⚠</span>`;
}

export function panel(title, content, extra = "") {
    return `<section class="panel"><div class="panel-head"><h2>${escape(title)}</h2>${extra}</div>${content}</section>`;
}

export function empty(title, message = "", retry = false) {
    return `<div class="empty"><strong>${escape(title)}</strong><p>${escape(message)}</p>${retry ? '<button class="button" data-action="reload">Retry</button>' : ""}</div>`;
}

export function unavailable(error) {
    return panel("Data unavailable", empty(error?.message || "The system API is unavailable.", "The last observation does not establish the current execution state.", true));
}

export function stat(label, value, foot = "") {
    return `<div class="stat"><div class="label">${escape(label)}</div><div class="value">${escape(value)}</div><div class="foot">${escape(foot)}</div></div>`;
}

export function inspectButton(label, record) {
    return `<button class="record-link" data-inspect="${escape(JSON.stringify(record))}">${escape(label || "Details")}</button>`;
}

export function table(rows, columns, facets = [], label = "records") {
    if (!rows.length) return empty("No records", "No matching observations were returned for this selection.");
    return `<div data-table><div class="filters"><label class="field search-field">Search<input type="search" data-filter-query placeholder="Search ${escape(label)}"></label>${facets.map(([key, title]) => `<label class="field">${escape(title)}<select data-facet="${escape(key)}"><option value="">All</option>${[...new Set(rows.map(row => String(row[key] ?? "")))].filter(Boolean).sort().map(value => `<option>${escape(value)}</option>`).join("")}</select></label>`).join("")}<button class="button quiet" data-action="reset-filters">Reset</button></div><div class="table-scroll"><table><thead><tr>${columns.map(column => `<th scope="col">${escape(column[0])}</th>`).join("")}</tr></thead><tbody>${rows.map(row => `<tr data-row data-fields="${escape(JSON.stringify(Object.fromEntries(facets.map(([key]) => [key, String(row[key] ?? "")]))))}">${columns.map(([, render]) => `<td>${render(row)}</td>`).join("")}</tr>`).join("")}<tr data-empty-row hidden><td colspan="${columns.length}">${empty("No matches", "Change the search or reset the filters.")}</td></tr></tbody></table></div><div class="record-count" aria-live="polite"><span data-count>${rows.length}</span> of ${rows.length} loaded ${escape(label)}</div></div>`;
}

export function gauge(name, sample, unit = "%") {
    const known = sample && Number.isFinite(sample.value);
    const value = known ? sample.value : null;
    const fraction = known && unit === "%" ? Math.min(100, Math.max(0, value)) / 100 : 0;
    const color = sample?.exceeded ? "#eac172" : "#83b4fc";
    return `<div class="gauge-card"><div class="gauge-title">${escape(name)}${!known || sample?.fresh === false || sample?.exceeded ? warning(!known ? "Measurement unavailable" : sample?.fresh === false ? "Last known measurement" : "Threshold exceeded") : ""}</div><svg viewBox="0 0 210 140" role="img" aria-label="${escape(name)}: ${numeric(value)} ${escape(unit)}"><path d="M28 108 A77 77 0 1 1 182 108" fill="none" stroke="#334862" stroke-width="13" stroke-linecap="round" pathLength="100"/><path d="M28 108 A77 77 0 1 1 182 108" fill="none" stroke="${color}" stroke-opacity="${known ? 1 : 0}" stroke-width="13" stroke-linecap="round" pathLength="100" stroke-dasharray="${fraction * 100} 100"/><text x="105" y="88" text-anchor="middle">${numeric(value, 1)}</text><text class="gauge-unit" x="105" y="112" text-anchor="middle">${escape(unit)}</text></svg></div>`;
}

export function lineChart(samples, valueKey = "value", unit = "", timeKey = "observed_at") {
    const rows = samples.filter(row => Number.isFinite(new Date(row[timeKey]).valueOf()));
    if (!rows.length) return empty("No measurements");
    const times = rows.map(row => new Date(row[timeKey]).valueOf());
    const low = Math.min(...times), high = Math.max(...times);
    const valid = rows.filter(row => typeof row[valueKey] === "number" && Number.isFinite(row[valueKey]));
    const maximum = Math.max(1, ...valid.map(row => row[valueKey]));
    let path = "", penDown = false;
    const points = [];
    for (const row of rows) {
        if (typeof row[valueKey] !== "number" || !Number.isFinite(row[valueKey])) { penDown = false; continue; }
        const x = 55 + (new Date(row[timeKey]).valueOf() - low) / Math.max(1, high - low) * 615;
        const y = 166 - Math.max(0, row[valueKey]) / maximum * 138;
        path += `${penDown ? "L" : "M"}${x},${y} `;
        penDown = true;
        points.push(`<circle class="point" cx="${x}" cy="${y}" r="3"><title>${escape(dateTime(row[timeKey]))}: ${numeric(row[valueKey])} ${escape(unit)}</title></circle>`);
    }
    return `<svg class="chart" viewBox="0 0 720 205" role="img" aria-label="${escape(unit)} over time"><path class="grid" d="M55 28H670 M55 97H670 M55 166H670"/><text x="4" y="32">${numeric(maximum, 1)}</text><text x="4" y="101">${numeric(maximum / 2, 1)}</text><text x="4" y="170">0</text><path class="series" d="${path}"/>${points.join("")}<text x="55" y="195">${escape(new Date(low).toLocaleTimeString("en-GB"))}</text><text x="670" y="195" text-anchor="end">${escape(new Date(high).toLocaleTimeString("en-GB"))}</text></svg>`;
}
