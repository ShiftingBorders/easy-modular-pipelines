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
        const go=async(page,experiment='exp-test')=>{const a=document.createElement('a');a.href='/?page='+page+'&experiment='+encodeURIComponent(experiment);document.body.append(a);a.click();a.remove();await wait(()=>document.querySelector('#main h1')?.textContent!=='Loading\u2026' && !document.querySelector('#main .loading') && !document.querySelector('#main[aria-busy]'));};
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
        const ui=await import('/static/ui.js');
        const sandbox=document.createElement('div');document.body.append(sandbox);
        ui.updateContent(sandbox,'<div data-key="a"><input value="original"><details><summary>Details</summary>Text</details></div><div data-key="b">Old</div>');
        const first=sandbox.firstChild, field=first.querySelector('input'), details=first.querySelector('details');
        field.value='edited';field.focus();field.setSelectionRange(1,3);details.open=true;
        ui.updateContent(sandbox,'<div data-key="a"><input value="server"><details><summary>Details</summary>New text</details></div><div data-key="b">Updated</div>');
        output.domState={identity:sandbox.firstChild===first,focus:document.activeElement===field,value:field.value==='edited',selection:field.selectionStart===1&&field.selectionEnd===3,details:details.open};
        ui.updateContent(sandbox,'<div data-key="b">Updated</div><div data-key="a"><input><details><summary>Details</summary>New text</details></div><div data-key="c">Inserted</div>');
        output.domState.reorder=sandbox.children[1]===first&&sandbox.children.length===3;
        sandbox.remove();
        await go('events');
        const nativeFetch=window.fetch.bind(window);let release;
        const held=new Promise(resolve=>release=resolve);
        window.fetch=async(...args)=>{if(String(args[0]).includes('/exp-test/errors'))await held;return nativeFetch(...args);};
        const heading=document.querySelector('#main .heading'), tabs=document.querySelector('#main .tabs');
        document.querySelector('.tabs a[href*="page=errors"]').click();
        output.tabsPreserved=document.querySelector('#main .heading')===heading&&document.querySelector('#main .tabs')===tabs&&Boolean(document.querySelector('#page-loading'));
        await go('artifacts');release();await new Promise(resolve=>setTimeout(resolve,50));
        output.lateResponseIgnored=new URL(location.href).searchParams.get('page')==='artifacts'&&document.getElementById('main').innerText.includes('Artifacts');
        window.fetch=nativeFetch;
        let retries=0;
        window.fetch=(...args)=>String(args[0]).includes('/exp-test/errors')?(retries++,Promise.resolve(new Response(JSON.stringify({error:{code:'history_changed',message:'Changed'}}),{status:409,headers:{'content-type':'application/json'}}))):nativeFetch(...args);
        await go('errors');output.historyRetryCount=retries;window.fetch=nativeFetch;
        await go('events');
        const oldRows=[...document.querySelectorAll('[data-row]')];
        window.fetch=(...args)=>String(args[0]).includes('/exp-test/events')?Promise.resolve(new Response(JSON.stringify({error:{code:'unavailable',message:'Temporary read failure'}}),{status:503,headers:{'content-type':'application/json'}})):nativeFetch(...args);
        await wait(()=>Boolean(document.getElementById('refresh-error')));
        output.previousDataRetained=oldRows.every(row=>row.isConnected);window.fetch=nativeFetch;
        await go('events');
        let releaseDetail;const detailHeld=new Promise(resolve=>releaseDetail=resolve);let detailRequests=0;
        window.fetch=async(...args)=>{if(String(args[0]).includes('/exp-test/detail')&&++detailRequests===1)await detailHeld;return nativeFetch(...args);};
        const inspect=[...document.querySelectorAll('[data-inspect]')];
        inspect[0].click();document.getElementById('details-dialog').close();inspect[1].click();
        const secondId=JSON.parse(inspect[1].dataset.inspect).event_id;
        await wait(()=>document.getElementById('detail-content').textContent.includes(secondId));
        releaseDetail();await new Promise(resolve=>setTimeout(resolve,80));
        output.detailRaceSafe=document.getElementById('detail-content').textContent.includes(secondId);
        document.getElementById('details-dialog').close();window.fetch=nativeFetch;
        await go('events');document.querySelector('[data-action="load-more"]').click();
        await wait(()=>Number(document.querySelector('[data-count]')?.textContent)===400);
        await new Promise(resolve=>setTimeout(resolve,1100));
        output.paginationRetained=Number(document.querySelector('[data-count]')?.textContent)===400;
        await go('settings');document.querySelector('[data-settings="json"]').click();
        await new Promise(resolve=>setTimeout(resolve,1100));
        output.settingsModeRetained=document.querySelector('[data-settings="json"]').classList.contains('active')&&document.querySelector('#settings-content pre').textContent.trim().startsWith('{');
        window.fetch=async(...args)=>{
            const response=await nativeFetch(...args);
            if(!String(args[0]).includes('/api/application'))return response;
            const info=await response.json();info.cache_activity={active:[{experiment_id:'large-cold',initial:true,event_count:10000}],building:['large-cold'],queued:[]};
            return new Response(JSON.stringify(info),{headers:{'content-type':'application/json'}});
        };
        await go('errors');await wait(()=>!document.getElementById('cache-activity').hidden);
        output.initialCacheNotice=document.getElementById('cache-activity').textContent.includes('large-cold')&&document.getElementById('cache-activity').textContent.includes('more slowly');
        window.fetch=nativeFetch;
        const history=await (await fetch('/api/system/experiments/exp-test/events?compact=1')).json();
        output.historyEvents=history.total;
        if(!history.complete)throw new Error('Performance history is incomplete');
        output.pageTimings={};output.summaryRequests=0;
        window.fetch=(...args)=>{if(String(args[0]).includes('/exp-test/summary'))output.summaryRequests++;return nativeFetch(...args);};
        for(const page of ['overview','experiments','modules','errors','artifacts','events','resources','settings']){
            await go(page);output.pageTimings[page]=[];
            for(let sample=0;sample<5;sample++){
                const start=performance.now();await go(page);
                await new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)));
                output.pageTimings[page].push(performance.now()-start);
            }
        }
        window.fetch=nativeFetch;
        await fetch('/test/register-cold',{method:'POST'});
        const coldStart=performance.now();await go('events','cold-test');
        await wait(()=>!document.querySelector('#main .error-banner')&&!document.querySelector('#main[aria-busy]')&&Number(document.querySelector('#main [data-count]')?.textContent)===200);
        await new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)));
        output.coldOpenMs=performance.now()-coldStart;
        const cold=await (await fetch('/api/system/experiments/cold-test/events?compact=1')).json();
        if(!cold.complete)throw new Error('Cold history has not completed');
        output.coldHistoryEvents=cold.total;
        output.httpTimings={};
        for(const entry of performance.getEntriesByType('resource')){
            const path=new URL(entry.name).pathname;
            if(entry.initiatorType==='fetch'&&path.startsWith('/api/')&&Number.isFinite(entry.duration))
                (output.httpTimings[path]??=[]).push(entry.duration);
        }
        return output;
    })()`});
    if(result.exceptionDetails)throw new Error(JSON.stringify(result.exceptionDetails));
    await send('Emulation.setDeviceMetricsOverride',{width:600,height:900,deviceScaleFactor:1,mobile:false});
    const narrow=await send('Runtime.evaluate',{expression:'document.body.scrollWidth <= innerWidth',returnByValue:true});
    console.log(JSON.stringify({...result.result.value,narrowLayout:narrow.result.value,runtimeErrors:errors}));
} finally {
    // Drain requests before the owner closes the server; navigating/closing the
    // browser here can reset active Windows Proactor sockets during teardown.
    await send('Runtime.evaluate',{expression:'(()=>{const last=setTimeout(()=>{},2147483647);for(let id=1;id<=last;id++){clearTimeout(id);clearInterval(id);}})()'}).catch(()=>{});
    await new Promise(resolve=>setTimeout(resolve,150));
    socket.close();
}
