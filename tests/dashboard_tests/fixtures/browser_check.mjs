// Real browser interactions driven through Chromium's built-in CDP; no test framework.
const [port, url] = process.argv.slice(2);
const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
const socket = new WebSocket(targets.find(item => item.type === 'page').webSocketDebuggerUrl);
await new Promise(resolve => socket.addEventListener('open', resolve, {once:true}));
let sequence = 0;
const pending = new Map(), errors = [];
socket.addEventListener('message', event => {
    const message = JSON.parse(event.data);
    if (message.method === 'Runtime.exceptionThrown') errors.push(message.params.exceptionDetails.text);
    if (!message.id) return;
    const job = pending.get(message.id); pending.delete(message.id);
    if (message.error) job.reject(new Error(JSON.stringify(message.error))); else job.resolve(message.result);
});
function send(method, params={}) {
    return new Promise((resolve,reject) => {const id=++sequence; pending.set(id,{resolve,reject});socket.send(JSON.stringify({id,method,params}));});
}
try {
    await send('Runtime.enable');
    await send('Page.enable');
    await send('Emulation.setDeviceMetricsOverride',{width:1600,height:1100,deviceScaleFactor:1,mobile:false});
    const loaded = new Promise(resolve => socket.addEventListener('message', event => {
        if (JSON.parse(event.data).method === 'Page.loadEventFired') resolve();
    }));
    await send('Page.navigate',{url});
    await loaded;
    const deadline=Date.now()+15000;
    while(Date.now()<deadline) {
        const result=await send('Runtime.evaluate',{expression:'Boolean(document.querySelector("#main h1") && !document.querySelector("#main .loading"))',returnByValue:true});
        if(result.result?.value)break;
        await new Promise(resolve=>setTimeout(resolve,50));
    }
    const result=await send('Runtime.evaluate',{awaitPromise:true,returnByValue:true,expression:`(async()=>{
        const wait=async(predicate)=>{const deadline=Date.now()+10000;while(!predicate()){if(Date.now()>deadline)throw new Error('UI deadline exceeded');await new Promise(r=>setTimeout(r,20));}};
        const go=async(page)=>{const a=document.createElement('a');a.href='/?page='+page+'&experiment=exp-test';document.body.append(a);a.click();a.remove();await wait(()=>document.querySelector('#main h1')?.textContent!=='Loading\u2026' && !document.querySelector('#main .loading'));};
        const output={screens:{}};
        for(const page of ['overview','experiments','timeline','dag','errors','events','resources','artifacts','settings','commands','snapshots','forecast','modules','services','compute','alerts','icmp']){
            await go(page);
            output.screens[page]={heading:document.querySelector('#main h1')?.textContent,unavailable:document.getElementById('main').innerText.includes('Data unavailable')};
        }
        await go('timeline');output.traceRows=document.querySelectorAll('.trace-row').length;
        document.querySelector('[data-action="pin"]').click();output.bookmarked=document.getElementById('bookmarks').innerText.includes('timing');
        await go('events');const filter=document.querySelector('[data-filter-query]');filter.value='impossible-no-match';filter.dispatchEvent(new Event('input',{bubbles:true}));
        output.filteredCount=document.querySelector('[data-count]').textContent;
        document.querySelector('[data-action="reset-filters"]').click();output.resetCount=Number(document.querySelector('[data-count]').textContent);
        document.querySelector('[data-inspect]').click();output.detailsOpen=document.getElementById('details-dialog').open;document.getElementById('details-dialog').close();
        await go('forecast');output.forecastText=document.getElementById('forecast-body').innerText;
        await go('compute');output.refreshChoices=[...document.querySelector('#refresh-rate').options].map(o=>o.value);
        output.rangeChoices=[...document.querySelector('#resource-window').options].map(o=>o.value);
        output.gpuVisible=/GPU|VRAM/.test(document.getElementById('main').innerText);
        await go('alerts');document.querySelector('[data-action="new-rule"]').click();
        const form=document.getElementById('alert-rule-form');form.elements.name.value='Browser CPU rule';form.elements.threshold.value='0';form.elements.duration_seconds.value='0';form.requestSubmit();
        await wait(()=>!document.getElementById('details-dialog').open && document.getElementById('main').innerText.includes('Browser CPU rule'));
        await wait(()=>document.getElementById('alert-count').textContent==='1');output.bell=document.getElementById('alert-count').textContent;
        document.querySelector('[data-delete-rule]').click();await wait(()=>!document.getElementById('main').innerText.includes('Browser CPU rule'));
        output.ruleDeleted=true;
        const search=document.getElementById('global-search');search.value='Compute';search.dispatchEvent(new Event('input',{bubbles:true}));
        output.searchMatches=document.getElementById('search-results').innerText;
        search.dispatchEvent(new KeyboardEvent('keydown',{key:'Escape',bubbles:true}));output.searchClosed=document.getElementById('search-results').hidden;
        output.connection=document.getElementById('connection-status').textContent;
        output.fontsLoaded=await document.fonts.ready.then(()=>document.fonts.check('15px Manrope'));
        return output;
    })()`});
    if(result.exceptionDetails)throw new Error(JSON.stringify(result.exceptionDetails));
    await send('Emulation.setDeviceMetricsOverride',{width:600,height:900,deviceScaleFactor:1,mobile:false});
    const narrow=await send('Runtime.evaluate',{expression:'document.body.scrollWidth <= innerWidth',returnByValue:true});
    console.log(JSON.stringify({...result.result.value,narrowLayout:narrow.result.value,runtimeErrors:errors}));
} finally {
    await send('Browser.close').catch(()=>{});
    socket.close();
}
