// Focused D08 checks of production DAG rendering and keyed DOM refresh in Chromium.
import {writeFile} from "node:fs/promises";

const [port, url, screenshot] = process.argv.slice(2);
const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
const socket = new WebSocket(targets.find(target => target.type === "page").webSocketDebuggerUrl);
await new Promise(resolve => socket.addEventListener("open", resolve, {once: true}));
let sequence = 0;
const pending = new Map();
socket.addEventListener("message", event => {
    const message = JSON.parse(event.data);
    if (!message.id) return;
    const job = pending.get(message.id);
    pending.delete(message.id);
    if (message.error) job.reject(new Error(JSON.stringify(message.error)));
    else job.resolve(message.result);
});
function send(method, params = {}) {
    return new Promise((resolve, reject) => {
        const id = ++sequence;
        pending.set(id, {resolve, reject});
        socket.send(JSON.stringify({id, method, params}));
    });
}
async function evaluate(expression) {
    const result = await send("Runtime.evaluate", {expression, awaitPromise: true, returnByValue: true});
    if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
    return result.result.value;
}
try {
    await send("Page.enable");
    await send("Runtime.enable");
    await send("Emulation.setDeviceMetricsOverride", {width: 1400, height: 900, deviceScaleFactor: 1, mobile: false});
    const loaded = new Promise(resolve => socket.addEventListener("message", event => {
        if (JSON.parse(event.data).method === "Page.loadEventFired") resolve();
    }));
    await send("Page.navigate", {url});
    await loaded;
    const moved = await evaluate(`(async () => {
        const {dag} = await import('/views.js');
        const {updateContent} = await import('/ui.js');
        const frames = await (await fetch('/frames.json')).json();
        const root = document.getElementById('dag-root');
        const node = id => root.querySelector('[data-key="stage:' + id + '"]');
        const badge = id => node(id).querySelector('.badge');
        const render = index => updateContent(root, dag(frames[index]));
        frames[0].nodes.find(item => item.stage_id === 'B').status = 'completed';
        frames[0].nodes.find(item => item.stage_id === 'C').status = 'success';
        frames[0].nodes.find(item => item.stage_id === 'C').name = '<b>unsafe</b>';
        render(0);
        const oldNode = node('A');
        const oldBadge = badge('A');
        const escaped = node('C').querySelector('strong').textContent === '<b>unsafe</b>' && !node('C').querySelector('strong b');
        const before = ['A', 'B', 'C'].map(id => badge(id).textContent);
        render(1);
        await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
        window.dagCheck = {frames, render, node, badge};
        return {
            before, escaped,
            sameNode: node('A') === oldNode,
            sameBadge: badge('A') === oldBadge,
            pending: ['A', 'B', 'C'].every(id => badge(id).textContent === 'Pending' && !badge(id).classList.contains('good')),
            prefix: badge('P').textContent,
            cursor: node('A').querySelector('.dag-node').classList.contains('current'),
            conditional: node('C').querySelector('.dag-node').innerText.includes('Conditional') && node('C').querySelector('.dag-node').innerText.includes('Passes input'),
            activeMoves: root.querySelectorAll('[data-key^="edge:conditional_move:"]').length,
        };
    })()`);
    const shot = await send("Page.captureScreenshot", {format: "png", captureBeyondViewport: true});
    await writeFile(screenshot, Buffer.from(shot.data, "base64"));
    const continued = await evaluate(`(() => {
        const {render, node, badge} = window.dagCheck;
        render(2);
        const running = badge('A').textContent === 'Running';
        render(3);
        const accepted = badge('A').textContent === 'Succeeded' && badge('B').textContent === 'Pending' && badge('C').textContent === 'Pending';
        render(4);
        const loop = document.querySelector('[data-key="edge:conditional_move:C:C"] path');
        const bounds = loop.getBBox();
        return {running, accepted, loopWidth: bounds.width, loopHeight: bounds.height, conditional: node('C').querySelector('.dag-node').classList.contains('conditional')};
    })()`);
    console.log(JSON.stringify({...moved, ...continued}));
} finally {
    socket.close();
}
