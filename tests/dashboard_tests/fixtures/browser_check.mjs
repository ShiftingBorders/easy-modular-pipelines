// Real browser interactions driven through Chromium's built-in CDP; no test framework.
const [port, url, browserPath] = process.argv.slice(2);
let websocket;
if (url === '--close') {
    if (!/^\/devtools\/browser\/[a-zA-Z0-9-]+$/.test(browserPath)) throw new Error('Invalid browser identity');
    websocket = `ws://127.0.0.1:${port}${browserPath}`;
} else {
    const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
    websocket = targets.find(item => item.type === 'page').webSocketDebuggerUrl;
}
const socket = new WebSocket(websocket);
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
if (url === '--close') {
    await send('Browser.close');
    socket.close();
    process.exit(0);
}
try {
    await send('Runtime.enable');
    await send('Page.enable');
    await send('Emulation.setDeviceMetricsOverride',{width:1600,height:1100,deviceScaleFactor:1,mobile:false});
    await send('Page.addScriptToEvaluateOnNewDocument',{source:`
        performance.setResourceTimingBufferSize(8192);
        window.testResourceTiming={entries:[],bufferFullCount:0};
        performance.addEventListener('resourcetimingbufferfull',()=>window.testResourceTiming.bufferFullCount++);
        window.testResourceTiming.observer=new PerformanceObserver(list=>window.testResourceTiming.entries.push(...list.getEntries()));
        window.testResourceTiming.observer.observe({type:'resource',buffered:true});
    `});
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
        // A posted task runs after the response consumer's promise continuations.
        // This acknowledges handling without guessing a network/rendering delay.
        const nextTurn=()=>new Promise(resolve=>{
            const channel=new MessageChannel();
            channel.port1.onmessage=()=>{channel.port1.close();channel.port2.close();resolve();};
            channel.port2.postMessage(null);
        });
        const responseWithinDeadline=async(promise,label)=>{
            let deadline;
            try {
                return await Promise.race([promise,new Promise((_,reject)=>{
                    deadline=setTimeout(()=>reject(new Error(label+' deadline exceeded')),10000);
                })]);
            } finally {clearTimeout(deadline);}
        };
        const acknowledgedResponse=(document,handled)=>{
            const response=new Response(JSON.stringify(document),{headers:{'content-type':'application/json'}});
            const read=response.json.bind(response);
            response.json=async()=>{const result=await read();void nextTurn().then(handled);return result;};
            return response;
        };
        const refreshConnectionHandled=trigger=>new Promise((resolve,reject)=>{
            const observer=new MutationObserver(()=>{
                observer.disconnect();
                // The update belongs to the response consumer; its finally
                // clears the shared request before the posted task runs.
                void nextTurn().then(()=>{clearTimeout(deadline);resolve();});
            });
            const deadline=setTimeout(()=>{
                observer.disconnect();reject(new Error('Application refresh deadline exceeded'));
            },10000);
            // This element is written only by a successful application response.
            observer.observe(document.getElementById('cache-activity'),{
                subtree:true,childList:true,attributes:true,characterData:true,
            });
            trigger();
        });
        const go=(page,experiment='exp-test',allowError=false)=>{
            const startedAt=performance.now(),main=document.getElementById('main');
            return new Promise((resolve,reject)=>{
                let navigationObserved=false;
                const finish=(error)=>{
                    observer.disconnect();clearTimeout(deadline);
                    if(error)reject(error);else resolve({startedAt,readyAt:performance.now()});
                };
                const observer=new MutationObserver(records=>{
                    navigationObserved ||= records.some(record=>record.target===main&&record.attributeName==='aria-busy');
                    const route=new URL(location.href).searchParams;
                    if(!navigationObserved||route.get('page')!==page||route.get('experiment')!==experiment)return;
                    if(main.hasAttribute('aria-busy')||main.querySelector('.loading')||!main.querySelector('h1'))return;
                    const error=main.querySelector('.error-banner,[data-key="panel:Data unavailable"]');
                    if(error&&!allowError){finish(new Error('Navigation failed for '+page+': '+error.textContent));return;}
                    finish();
                });
                const deadline=setTimeout(()=>finish(new Error('Navigation deadline exceeded for '+page)),10000);
                observer.observe(main,{subtree:true,childList:true,attributes:true,characterData:true});
                const link=document.createElement('a');
                link.href='/?page='+page+'&experiment='+encodeURIComponent(experiment);
                document.body.append(link);link.click();link.remove();
            });
        };
        const output={screens:{},navigationTimings:[],refreshMarkers:[]};
        const measurePage=async(page,experiment,temperature,sample)=>{
            const timing={page,experiment,temperature,sample,...await go(page,experiment)};
            await new Promise(resolve=>requestAnimationFrame(frame=>{
                timing.firstFrameAt=performance.now();timing.firstFrameTimestamp=frame;
                requestAnimationFrame(nextFrame=>{
                    timing.secondFrameAt=performance.now();timing.secondFrameTimestamp=nextFrame;resolve();
                });
            }));
            timing.endedAt=performance.now();timing.durationMs=timing.endedAt-timing.startedAt;
            output.navigationTimings.push(timing);
            return timing.durationMs;
        };
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
        // Capture the actual refresh scheduled by the public refresh control.
        let refreshCallback,refreshInterval,refreshTimer,scheduledRefreshes=0;
        const invokeRefresh=source=>{
            const route=new URL(location.href).searchParams;
            output.refreshMarkers.push({source,startedAt:performance.now(),page:route.get('page'),experiment:route.get('experiment')});
            refreshCallback();
        };
        const nativeInterval=window.setInterval;
        window.setInterval=(callback,interval,...args)=>{
            refreshCallback=()=>callback(...args);refreshInterval=interval;
            refreshTimer=nativeInterval(()=>{scheduledRefreshes++;invokeRefresh('periodic');},interval);
            return refreshTimer;
        };
        try {document.querySelector('#refresh-rate').dispatchEvent(new Event('change',{bubbles:true}));}
        finally {window.setInterval=nativeInterval;}
        await wait(()=>scheduledRefreshes>0&&!document.querySelector('#main[aria-busy]'));
        output.periodicRefreshObserved=scheduledRefreshes>0;
        output.rangeChoices=[...document.querySelector('#resource-window').options].map(o=>o.value);
        output.gpuVisible=/GPU|VRAM/.test(document.getElementById('main').innerText);
        await go('alerts');document.querySelector('[data-action="new-rule"]').click();
        const form=document.getElementById('alert-rule-form');form.elements.name.value='Browser CPU rule';form.elements.threshold.value='0';form.elements.duration_seconds.value='0';form.requestSubmit();
        await wait(()=>!document.getElementById('details-dialog').open && document.getElementById('main').innerText.includes('Browser CPU rule'));
        await wait(()=>document.getElementById('alert-count').textContent==='1');output.bell=document.getElementById('alert-count').textContent;
        const deleteRule=document.querySelector('[data-delete-rule]'), deletedRuleId=deleteRule.dataset.deleteRule;
        deleteRule.click();
        // Closed incidents retain the name; only the loaded rules table proves deletion.
        await wait(()=>{
            const rules=document.querySelector('[data-key="panel:Alert rules"]');
            return rules && !document.querySelector('#main .loading') &&
                ![...rules.querySelectorAll('[data-delete-rule]')].some(button=>button.dataset.deleteRule===deletedRuleId);
        });
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
        // Functional refresh assertions control the callback after the real timer
        // has been observed. Restore the same interval for performance samples.
        clearInterval(refreshTimer);
        await go('events');
        const nativeFetch=window.fetch.bind(window);let release,oldResponseReady=false,oldResponseHandled=false;
        const held=new Promise(resolve=>release=resolve);
        window.fetch=async(...args)=>{
            if(!String(args[0]).includes('/exp-test/errors'))return nativeFetch(...args);
            const document=await (await nativeFetch(...args)).json();oldResponseReady=true;
            await held;return acknowledgedResponse(document,()=>oldResponseHandled=true);
        };
        const heading=document.querySelector('#main .heading'), tabs=document.querySelector('#main .tabs');
        document.querySelector('.tabs a[href*="page=errors"]').click();
        output.tabsPreserved=document.querySelector('#main .heading')===heading&&document.querySelector('#main .tabs')===tabs&&Boolean(document.querySelector('#page-loading'));
        try {
            await wait(()=>oldResponseReady);
            await go('artifacts');release();await wait(()=>oldResponseHandled);
            output.lateResponseIgnored=new URL(location.href).searchParams.get('page')==='artifacts'&&document.getElementById('main').innerText.includes('Artifacts');
        } finally {release();window.fetch=nativeFetch;}
        let retries=0;
        window.fetch=(...args)=>String(args[0]).includes('/exp-test/errors')?(retries++,Promise.resolve(new Response(JSON.stringify({error:{code:'history_changed',message:'Changed'}}),{status:409,headers:{'content-type':'application/json'}}))):nativeFetch(...args);
        await go('errors','exp-test',true);output.historyRetryCount=retries;window.fetch=nativeFetch;
        await go('events');
        const oldRows=[...document.querySelectorAll('[data-row]')];
        window.fetch=(...args)=>String(args[0]).includes('/exp-test/events')?Promise.resolve(new Response(JSON.stringify({error:{code:'unavailable',message:'Temporary read failure'}}),{status:503,headers:{'content-type':'application/json'}})):nativeFetch(...args);
        refreshCallback();
        await wait(()=>Boolean(document.getElementById('refresh-error')));
        output.previousDataRetained=oldRows.every(row=>row.isConnected);window.fetch=nativeFetch;
        await go('events');
        let releaseDetail,firstDetailReady=false,firstDetailHandled=false;
        const detailHeld=new Promise(resolve=>releaseDetail=resolve);let detailRequests=0;
        window.fetch=async(...args)=>{
            if(!String(args[0]).includes('/exp-test/detail'))return nativeFetch(...args);
            const request=++detailRequests;
            const document=await (await nativeFetch(...args)).json();
            document.record.__test_response='detail-'+request;
            if(request===1){firstDetailReady=true;await detailHeld;}
            return acknowledgedResponse(document,()=>{if(request===1)firstDetailHandled=true;});
        };
        const inspect=[...document.querySelectorAll('[data-inspect]')];
        try {
            inspect[0].click();await wait(()=>firstDetailReady);
            document.getElementById('details-dialog').close();inspect[1].click();
            const secondId=JSON.parse(inspect[1].dataset.inspect).event_id;
            await wait(()=>document.getElementById('detail-content').textContent.includes('detail-2'));
            releaseDetail();await wait(()=>firstDetailHandled);
            const detail=document.getElementById('detail-content').textContent;
            output.detailRaceSafe=detail.includes(secondId)&&detail.includes('detail-2')&&!detail.includes('detail-1');
        } finally {releaseDetail();document.getElementById('details-dialog').close();window.fetch=nativeFetch;}
        await go('events');document.querySelector('[data-action="load-more"]').click();
        await wait(()=>Number(document.querySelector('[data-count]')?.textContent)===400);
        let paginationRequests=0;
        window.fetch=(...args)=>{if(String(args[0]).includes('/exp-test/events'))paginationRequests++;return nativeFetch(...args);};
        refreshCallback();await nextTurn();
        output.paginationRequestsOnRefresh=paginationRequests;window.fetch=nativeFetch;
        output.paginationRetained=Number(document.querySelector('[data-count]')?.textContent)===400;
        await go('settings');document.querySelector('[data-settings="json"]').click();
        let settingsRefreshed=false;
        window.fetch=async(...args)=>{
            const response=await nativeFetch(...args);
            if(!String(args[0]).includes('/exp-test/template'))return response;
            return acknowledgedResponse(await response.json(),()=>settingsRefreshed=true);
        };
        refreshCallback();await wait(()=>settingsRefreshed&&!document.querySelector('#main[aria-busy]'));
        window.fetch=nativeFetch;
        output.settingsModeRetained=document.querySelector('[data-settings="json"]').classList.contains('active')&&document.querySelector('#settings-content pre').textContent.trim().startsWith('{');
        // Navigation may reuse a previous connection request, even after page
        // rendering finishes. Drain it before controlling the notice response.
        // A response that already updated the DOM may still have a queued finally.
        await nextTurn();
        await refreshConnectionHandled(refreshCallback);
        let releaseApplication,applicationReady,applicationHandled,noticeHandled;
        const applicationHeld=new Promise(resolve=>releaseApplication=resolve);
        const applicationResponseReady=new Promise(resolve=>applicationReady=resolve);
        const applicationResponseHandled=new Promise(resolve=>applicationHandled=resolve);
        const noticeResponseHandled=new Promise(resolve=>noticeHandled=resolve);
        window.fetch=async(...args)=>{
            const response=await nativeFetch(...args);
            if(!String(args[0]).includes('/api/application'))return response;
            const info=await response.json();applicationReady();await applicationHeld;
            return acknowledgedResponse(info,applicationHandled);
        };
        try {
            refreshCallback();
            await responseWithinDeadline(applicationResponseReady,'Held application response');
            window.fetch=async(...args)=>{
                const response=await nativeFetch(...args);
                if(!String(args[0]).includes('/api/application'))return response;
                const info=await response.json();info.cache_activity={active:[{experiment_id:'large-cold',initial:true,event_count:10000}],building:['large-cold'],queued:[]};
                return acknowledgedResponse(info,noticeHandled);
            };
            releaseApplication();
            await responseWithinDeadline(applicationResponseHandled,'Previous application handling');
            await go('errors');
            await responseWithinDeadline(noticeResponseHandled,'Cache notice handling');
            output.initialCacheNotice=!document.getElementById('cache-activity').hidden&&document.getElementById('cache-activity').textContent.includes('large-cold')&&document.getElementById('cache-activity').textContent.includes('more slowly');
        } finally {releaseApplication();window.fetch=nativeFetch;}
        const history=await (await fetch('/api/system/experiments/exp-test/events?compact=1')).json();
        output.historyEvents=history.total;
        if(!history.complete)throw new Error('Performance history is incomplete');
        refreshTimer=nativeInterval(()=>invokeRefresh('periodic'),refreshInterval);
        output.pageTimings={};output.summaryRequests=0;
        window.fetch=(...args)=>{if(String(args[0]).includes('/exp-test/summary'))output.summaryRequests++;return nativeFetch(...args);};
        for(const page of ['overview','experiments','modules','errors','artifacts','events','resources','settings']){
            await go(page);output.pageTimings[page]=[];
            for(let sample=0;sample<5;sample++){
                output.pageTimings[page].push(await measurePage(page,'exp-test','warm',sample));
            }
        }
        window.fetch=nativeFetch;
        await fetch('/test/register-cold',{method:'POST'});
        let coldRequests=0,holdNextColdRequest=false,refreshPending=false,releaseRefresh;
        const coldRefresh={};output.coldRefresh=coldRefresh;
        const refreshHeld=new Promise(resolve=>releaseRefresh=resolve);
        window.releaseTestRefresh=releaseRefresh;
        window.fetch=async(...args)=>{
            if(String(args[0]).includes('/cold-test/')){
                coldRequests++;
                if(holdNextColdRequest){
                    holdNextColdRequest=false;refreshPending=true;
                    coldRefresh.requestNumber=coldRequests;coldRefresh.heldAt=performance.now();
                    await refreshHeld;refreshPending=false;
                }
            }
            return nativeFetch(...args);
        };
        output.coldOpenMs=await measurePage('events','cold-test','cold',0);
        if(Number(document.querySelector('#main [data-count]')?.textContent)!==200)throw new Error('Cold opening did not render 200 rows');
        // Keep the real periodic timer active during opening. Only arm the gate
        // after the measured frames, even when a slow opening crosses a tick.
        await wait(()=>!document.querySelector('#main[aria-busy]'));
        holdNextColdRequest=true;coldRefresh.triggeredAt=performance.now();
        invokeRefresh('forced-cold');
        await wait(()=>refreshPending);
        output.coldRefreshPending=refreshPending&&Boolean(document.querySelector('#main[aria-busy]'));
        output.coldRowsWhileRefreshing=Number(document.querySelector('#main [data-count]')?.textContent);
        coldRefresh.releasedAt=performance.now();
        releaseRefresh();await wait(()=>!document.querySelector('#main[aria-busy]'));
        coldRefresh.completedAt=performance.now();
        window.fetch=nativeFetch;delete window.releaseTestRefresh;
        const cold=await (await fetch('/api/system/experiments/cold-test/events?compact=1')).json();
        if(!cold.complete)throw new Error('Cold history has not completed');
        output.coldHistoryEvents=cold.total;
        const resourceTiming=window.testResourceTiming;
        resourceTiming.entries.push(...resourceTiming.observer.takeRecords());resourceTiming.observer.disconnect();
        output.httpTimings={};output.httpRequests=[];
        for(const entry of resourceTiming.entries){
            const path=new URL(entry.name).pathname;
            if(entry.initiatorType==='fetch'&&path.startsWith('/api/')&&Number.isFinite(entry.duration)){
                (output.httpTimings[path]??=[]).push(entry.duration);
                output.httpRequests.push({...entry.toJSON(),path});
            }
        }
        output.measurementEnvironment={
            timeOrigin:performance.timeOrigin,userAgent:navigator.userAgent,
            hardwareConcurrency:navigator.hardwareConcurrency,deviceMemory:navigator.deviceMemory,
            viewport:{width:innerWidth,height:innerHeight,devicePixelRatio},visibilityState:document.visibilityState,
            refreshIntervalMs:refreshInterval,resourceBufferSize:8192,
            resourceBufferFullCount:resourceTiming.bufferFullCount,resourceEntryCount:resourceTiming.entries.length,
        };
        return output;
    })()`});
    if(result.exceptionDetails)throw new Error(JSON.stringify(result.exceptionDetails));
    await send('Emulation.setDeviceMetricsOverride',{width:600,height:900,deviceScaleFactor:1,mobile:false});
    const narrow=await send('Runtime.evaluate',{expression:'document.body.scrollWidth <= innerWidth',returnByValue:true});
    console.log(JSON.stringify({...result.result.value,narrowLayout:narrow.result.value,runtimeErrors:errors}));
} finally {
    // Drain requests before the owner closes the server; navigating/closing the
    // browser here can reset active Windows Proactor sockets during teardown.
    await send('Runtime.evaluate',{expression:'(()=>{window.releaseTestRefresh?.();const last=setTimeout(()=>{},2147483647);for(let id=1;id<=last;id++){clearTimeout(id);clearInterval(id);}})()'}).catch(()=>{});
    await new Promise(resolve=>setTimeout(resolve,150));
    socket.close();
}
