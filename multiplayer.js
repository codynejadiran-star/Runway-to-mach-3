/* Runway to Mach 3 — no-setup multiplayer.
   Players connect straight to each other (WebRTC peer-to-peer, matched through public Nostr relays via Trystero).
   Gives the game the same small user/db/room API it used inside Claude. Shared lists (leaderboard, airlines)
   are passed peer to peer and remembered on each device. */
(function(){
  const APP='runway-to-mach3-public-sky-v1',ROOM='main-sky';
  const LS=k=>{try{return localStorage.getItem(k);}catch(e){return null;}},LSS=(k,v)=>{try{localStorage.setItem(k,v);}catch(e){}};
  let myId=LS('rtm3-pid');if(!myId){myId='p_'+Math.random().toString(36).slice(2,12)+Date.now().toString(36).slice(-4);LSS('rtm3-pid',myId);}
  const clean=v=>String(v||'').replace(/[\u0000-\u001f<>]/g,'').trim().slice(0,24);
  function avatar(name,id){try{const c=document.createElement('canvas');c.width=c.height=64;const x=c.getContext('2d');let h=0;for(const ch of String(id||'x'))h=(h*31+ch.charCodeAt(0))|0;
    x.fillStyle='hsl('+(Math.abs(h)%360)+',55%,45%)';x.fillRect(0,0,64,64);x.fillStyle='#fff';x.font='bold 30px sans-serif';x.textAlign='center';x.textBaseline='middle';x.fillText((name||'?').slice(0,1).toUpperCase(),32,34);return c.toDataURL();}catch(e){return '';}}
  /* ---------- shared documents: own docs + docs gossiped by peers, kept per device ---------- */
  let DOCS={};try{DOCS=JSON.parse(LS('rtm3-p2pdb')||'{}')||{};}catch(e){DOCS={};}
  let saveT=null;const persist=()=>{if(saveT)return;saveT=setTimeout(()=>{saveT=null;LSS('rtm3-p2pdb',JSON.stringify(DOCS));},800);};
  const watchers=new Set();const NAMES=new Map();
  const isPrivate=p=>p.startsWith('data/');const owner=p=>{const s=p.split('/');return s[0]==='live'?'*':s[1];};
  function putDoc(path,data,ts,from){const cur=DOCS[path];if(cur&&cur.ts>=ts)return false;if(data===null)DOCS[path]={data:null,ts};else DOCS[path]={data,ts};persist();
    if(data&&data.nm&&owner(path)!=='*')NAMES.set(owner(path),clean(data.nm));watchers.forEach(w=>{try{w(path);}catch(e){console.error(e);}});return true;}
  let sendDoc=null;
  function localWrite(path,data){const ts=Date.now();putDoc(path,data===null?null:JSON.parse(JSON.stringify(data)),ts);if(!isPrivate(path)&&sendDoc)sendDoc({path,data:DOCS[path].data,ts});}
  const snapDoc=path=>{const d=DOCS[path];return{id:path.split('/').pop(),exists:!!(d&&d.data!==null&&d.data!==undefined),data:()=>d&&d.data?JSON.parse(JSON.stringify(d.data)):null};};
  function docRef(path){return{get:async()=>snapDoc(path),set:async d=>localWrite(path,d),update:async d=>localWrite(path,Object.assign({},(DOCS[path]||{}).data||{},d)),delete:async()=>localWrite(path,null),
    onSnapshot:(cb)=>{const w=p=>{if(p===path)cb(snapDoc(path));};watchers.add(w);setTimeout(()=>cb(snapDoc(path)),0);return()=>watchers.delete(w);}};}
  function query(col,ord,dir,lim){const list=()=>{let docs=Object.keys(DOCS).filter(p=>p.startsWith(col+'/')&&p.split('/').length===col.split('/').length+1&&DOCS[p].data).map(p=>({id:p.split('/').pop(),exists:true,data:()=>JSON.parse(JSON.stringify(DOCS[p].data))}));
      if(ord)docs.sort((a,b)=>((+a.data()[ord]||0)-(+b.data()[ord]||0))*(dir==='desc'?-1:1));if(lim)docs=docs.slice(0,lim);return{docs,size:docs.length,empty:!docs.length};};
    return{orderBy:(f,d)=>query(col,f,d||'asc',lim),limit:n=>query(col,ord,dir,n),where:()=>query(col,ord,dir,lim),doc:id=>docRef(col+'/'+id),
      onSnapshot:(cb)=>{let t=null;const w=p=>{if(p.startsWith(col+'/')&&!t)t=setTimeout(()=>{t=null;cb(list());},150);};watchers.add(w);setTimeout(()=>cb(list()),0);return()=>watchers.delete(w);}};}
  /* ---------- peer-to-peer room: three matchmaking routes at once (Nostr relays, BitTorrent trackers, MQTT brokers) ---------- */
  const peers=new Map(),peerCbs=[],conCbs=[],topicCbs={};let connected=false,sendPres=null,sendEv=null,state={},presT=null,lastPres=0,seq=0;
  const links=new Map();// "route:pid" -> player id
  const seenEv=new Set(),seenQ=[];const seen=id=>{if(!id)return false;if(seenEv.has(id))return true;seenEv.add(id);seenQ.push(id);if(seenQ.length>600)seenEv.delete(seenQ.shift());return false;};
  const notify=(j,l,u)=>{const ch={peers:[...peers.values()],joined:j,left:l,updated:u};peerCbs.forEach(f=>{try{f(ch);}catch(e){console.error(e);}});};
  const setCon=c=>{if(c===connected)return;connected=c;conCbs.forEach(f=>{try{f(c);}catch(e){}});};
  function action(r,name){const a=r.makeAction(name);if(Array.isArray(a))return{send:(d,t)=>a[0](d,t),on:cb=>a[1](cb)};// old API: [send, receive]; new API (0.26+): {send(data,{target}), onMessage=(data,{peerId})}
    return{send:(d,t)=>a.send(d,t?{target:t}:undefined),on:cb=>{const f=(data,ctx)=>cb(data,ctx&&typeof ctx==='object'?ctx.peerId:ctx);if(typeof a.onMessage==='function'&&a.onMessage.length===1&&!a.__set)try{a.onMessage(f);return;}catch(e){}a.onMessage=f;a.__set=1;}};}
  const V='0.26.0',ROUTES=[['nostr',['https://esm.run/@trystero-p2p/nostr@'+V,'https://esm.run/trystero@'+V]],['torrent',['https://esm.run/@trystero-p2p/torrent@'+V]],['mqtt',['https://esm.run/@trystero-p2p/mqtt@'+V]]];
  const RTC={iceServers:[{urls:['stun:stun.l.google.com:19302','stun:stun1.l.google.com:19302']},{urls:'stun:stun.cloudflare.com:3478'}]};
  const routes=[];const status={};window.__mp={status,peers:()=>peers.size,routes};
  const pres=[],evs=[],docs=[];
  sendPres=d=>{for(const a of pres)try{a.send(d);}catch(e){}};sendEv=d=>{for(const a of evs)try{a.send(d);}catch(e){}};
  sendDoc=(d,to)=>{if(to){try{to.a.send(d,to.pid);}catch(e){}return;}for(const a of docs)try{a.send(d);}catch(e){}};
  function dropLink(key){const by=links.get(key);if(!by)return;links.delete(key);const p=peers.get(by);if(!p)return;p.links.delete(key);if(!p.links.size){peers.delete(by);notify([],[p],[]);}}
  async function joinRoute(name,urls){let mod=(window.__trysteroRoutes&&window.__trysteroRoutes[name])||(window.__trystero&&name==='nostr'?window.__trystero:null);
    for(const u of urls){if(mod)break;try{const m=await import(u);if(m&&m.joinRoom){mod=m;break;}}catch(e){console.warn('Multiplayer route '+name+' failed to load from',u,e);}}
    if(!mod){status[name]='failed to load';return false;}
    let r;try{r=mod.joinRoom({appId:APP,rtcConfig:RTC,relayConfig:name==='nostr'?{redundancy:8}:undefined},ROOM);}catch(e){status[name]='failed to join: '+e.message;console.warn(e);return false;}
    status[name]='searching';const pr=action(r,'pres'),ev=action(r,'ev'),dc=action(r,'doc');pres.push(pr);evs.push(ev);docs.push(dc);routes.push(name);
    pr.on((d,pid)=>{if(!d||typeof d!=='object'||typeof d.by!=='string')return;const by=d.by.slice(0,40);if(by===myId)return;const key=name+':'+pid;links.set(key,by);status[name]='connected';
      const ps=d.p&&typeof d.p==='object'?d.p:{};if(ps.nm)NAMES.set(by,clean(ps.nm));let p=peers.get(by),isNew=!p;
      if(!p){p={peer:'p:'+by,by,kind:'viewer',guest:false,isMe:false,sameTab:false,presence:ps,updatedAt:Date.now(),links:new Map(),seq:-1};peers.set(by,p);}
      p.links.set(key,{a:dc,pid});const sq=typeof d.s==='number'?d.s:0;if(sq<p.seq&&p.seq-sq<1000)return;p.seq=sq;p.presence=ps;p.updatedAt=Date.now();isNew?notify([p],[],[]):notify([],[],[p]);});
    ev.on((d,pid)=>{if(!d||typeof d.topic!=='string'||seen(d.id))return;const by=links.get(name+':'+pid)||(typeof d.by==='string'?d.by:null);const p=by&&peers.get(by);
      const m={topic:d.topic,data:d.data,by:p?p.by:by,peer:p?p.peer:'p:'+by,isMe:false,sameTab:false,guest:false};(topicCbs[d.topic]||[]).forEach(f=>{try{f(m);}catch(e){console.error(e);}});});
    dc.on((d,pid)=>{if(!d||typeof d.path!=='string'||d.path.length>120||isPrivate(d.path)||typeof d.ts!=='number')return;const by=links.get(name+':'+pid);const o=owner(d.path);
      // a peer may only publish its own records (the shared world settings are open to admins with the code)
      if(o!=='*'&&by!==o)return;if(JSON.stringify(d.data||{}).length>8000)return;putDoc(d.path,d.data===undefined?null:d.data,Math.min(d.ts,Date.now()+60000),pid);});
    const onJoin=pid=>{try{pr.send({by:myId,p:state,s:seq},pid);}catch(e){}
      for(const path of Object.keys(DOCS)){const o=owner(path);if(!isPrivate(path)&&(o===myId||o==='*'))sendDoc({path,data:DOCS[path].data,ts:DOCS[path].ts},{a:dc,pid});}};
    const onLeave=pid=>dropLink(name+':'+pid);
    const setH=(k,f)=>{try{if(typeof r[k]==='function')r[k](f);else r[k]=f;}catch(e){try{r[k]=f;}catch(_){}}};setH('onPeerJoin',pid=>{status[name]='connected';onJoin(pid);});setH('onPeerLeave',onLeave);
    setCon(true);console.info('Multiplayer route '+name+' ready');return true;}
  // relay (TURN) servers from the site's own server, for players behind strict networks (phone hotspots, some schools); skipped if not set up
  const turnReady=(async()=>{if(location.protocol==='file:'||window.__trysteroRoutes)return;try{const c=new AbortController();const t=setTimeout(()=>c.abort(),3000);const r=await fetch('/api/turn',{signal:c.signal});clearTimeout(t);
      const j=await r.json();if(j&&Array.isArray(j.iceServers)&&j.iceServers.length){RTC.iceServers=RTC.iceServers.concat(j.iceServers);status.relay='on';}else status.relay='not set up';}catch(e){status.relay='not set up';}})();
  const ready=(async()=>{await turnReady;const res=await Promise.all(ROUTES.map(([n,u])=>joinRoute(n,u).catch(e=>{console.warn(e);return false;})));
    setInterval(()=>{for(const [k,p] of peers)if(Date.now()-p.updatedAt>30000){peers.delete(k);for(const lk of p.links.keys())links.delete(lk);notify([],[p],[]);}},10000);
    if(!res.some(Boolean)){console.error('Multiplayer unavailable: no route could load');return false;}return true;})();
  function flushPres(force){if(!sendPres)return;presT=null;lastPres=Date.now();seq++;try{sendPres({by:myId,p:state,s:seq});}catch(e){}if(state.nm)NAMES.set(myId,clean(state.nm));}
  const me={peer:'me',by:myId,kind:'viewer',isMe:true,sameTab:true,presence:{}};
  const roomApi={presence:async patch=>{state=Object.assign(state,patch);for(const k in state)if(state[k]===null)delete state[k];me.presence=state;const wait=Math.max(0,220-(Date.now()-lastPres));if(!wait)flushPres();else if(!presT)presT=setTimeout(flushPres,wait);},
    onPeers:cb=>{peerCbs.push(cb);setTimeout(()=>{try{cb({peers:[me,...peers.values()],joined:[me,...peers.values()],left:[],updated:[]});}catch(e){}},0);return()=>{};},
    peers:()=>[me,...peers.values()],
    emit:async(topic,data)=>{if(!sendEv)throw Object.assign(new Error('offline'),{code:'upstream_error'});sendEv({topic,data:data===undefined?null:JSON.parse(JSON.stringify(data)),by:myId,id:myId+':'+Date.now().toString(36)+Math.random().toString(36).slice(2,7)});const m={topic,data,by:myId,peer:'me',isMe:true,sameTab:true};(topicCbs[topic]||[]).forEach(f=>{try{f(m);}catch(e){}});},
    on:(topic,cb)=>{(topicCbs[topic]=topicCbs[topic]||[]).push(cb);return()=>{};},
    connected:()=>connected,onConnection:cb=>{conCbs.push(cb);setTimeout(()=>cb(connected),0);return()=>{};}};
  const userApi={id:async()=>myId,isOwner:()=>false,canEdit:()=>false,can:()=>null,
    me:async()=>({id:myId,name:'',email:'',isOwner:false,canEdit:false,guest:false,avatarUrl:avatar(NAMES.get(myId)||'P',myId)}),
    profiles:async ids=>{const out={};for(const id of (ids||[]).slice(0,64)){const n=NAMES.get(id)||(DOCS['crew/'+id]&&DOCS['crew/'+id].data&&clean(DOCS['crew/'+id].data.nm))||(DOCS['board/'+id]&&DOCS['board/'+id].data&&clean(DOCS['board/'+id].data.nm))||'';out[id]={id,name:n,avatarUrl:avatar(n,id)};}return out;},
    search:async q=>{q=clean(q).toLowerCase();const seen=new Map();for(const p of peers.values())if(p.by&&p.presence&&p.presence.nm)seen.set(p.by,clean(p.presence.nm));for(const [id,n] of NAMES)if(!seen.has(id))seen.set(id,n);
      return[...seen.entries()].filter(([id,n])=>id!==myId&&n&&n.toLowerCase().includes(q)).slice(0,12).map(([id,n])=>({id,name:n,avatarUrl:avatar(n,id)}));}};

  /* ---------- pilot accounts: email + password, saves kept by the site's own server (_worker.js) ---------- */
  (function(){if(location.protocol==='file:')return;let pend=null,timer=null,busy=false;
    const post=async(path,body,keep)=>{const r=await fetch(path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body),keepalive:!!keep});let j={};try{j=await r.json();}catch(e){j={error:r.ok?'bad':'offline'};}
      if(!r.ok&&!j.error)j.error='server';if(r.status===404||r.status===405&&!j.error)j.error='nostore';return j;};
    const tok=()=>LS('rtm3-tok')||'';
    async function flush(keep){if(!pend||!tok()||busy)return;const s=pend;pend=null;busy=true;try{const j=await post('/api/save',{token:tok(),save:s},keep);if(j.error==='auth'){LSS('rtm3-tok','');}}catch(e){pend=pend||s;}busy=false;}
    window.__accounts={has:()=>!!tok(),email:()=>LS('rtm3-acct')||'',
      async login(email,pass){let j;try{j=await post('/api/login',{email,pass});}catch(e){return{error:'offline'};}if(j.ok){LSS('rtm3-tok',j.token);LSS('rtm3-acct',email);}return j;},
      async resume(){if(!tok())return{error:'auth'};let j;try{j=await post('/api/load',{token:tok()});}catch(e){return{error:'offline'};}if(j.error==='auth')LSS('rtm3-tok','');return j;},
      saveSoon(s){if(!tok())return;pend=s;if(!timer)timer=setTimeout(()=>{timer=null;flush();},120000);},// every 2 minutes (and when you leave the page) to stay inside the free plan
      saveNow(s){if(!tok())return;pend=s;flush();},
      forget(){LSS('rtm3-tok','');},
      async logout(){const t=tok();LSS('rtm3-tok','');if(t)try{await post('/api/logout',{token:t});}catch(e){}}};
    document.addEventListener('visibilitychange',()=>{if(document.hidden)flush(true);});addEventListener('pagehide',()=>flush(true));
    // leaderboard of everyone with an account, from the site's server
    let boardIds=[];async function pullBoard(){if(document.hidden)return;let j;try{j=await post('/api/board',{token:tok()});}catch(e){return;}if(!j||!Array.isArray(j.board))return;
      const now=Date.now(),ids=[];for(const e of j.board){if(e.you)continue;const path='board/a_'+e.id;ids.push(path);putDoc(path,{money:e.money,tier:e.tier,plane:e.plane,nm:e.nm,acct:1},now);}
      for(const old of boardIds)if(!ids.includes(old))putDoc(old,null,now+1);boardIds=ids;}
    setTimeout(pullBoard,4000);setInterval(pullBoard,90000);})();
  const dbApi={doc:p=>docRef(p),collection:p=>query(p)};
  window.claude={use:async name=>name==='user'?userApi:name==='db'?dbApi:name==='room'?((await ready)?roomApi:null):null};
})();
