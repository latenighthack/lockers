#!/usr/bin/env python3
"""Generate portable dashboards and their catalog. --check rejects stale shipped artifacts."""
import argparse, json, pathlib, re
ROOT=pathlib.Path(__file__).resolve().parents[1]
S='{service_name=~"${service_name:regex}",environment=~"${environment:regex}",job=~"${job:regex}",instance=~"${instance:regex}"}'
R='{job=~"${job:regex}",instance=~"${instance:regex}"}'
CAT={}
def metric(name,kind='counter',labels=(),scope='process',description=''):
    suffix='_total' if kind=='counter' else '_seconds' if kind=='timer' else ''
    exported=name.replace('.','_')+suffix
    CAT[name]={'name':name,'prometheus':exported,'type':kind,'unit':'unix_seconds' if name.endswith('timestamp') else 'seconds' if kind=='timer' or name.endswith('.seconds') else 'count','labels':list(labels),'scope':scope,'description':description or name}
    return exported
rpc=metric('lockers.rpc.requests',labels=('service','operation','outcome'))
rpcdur=metric('lockers.rpc.duration','timer',('service','operation','outcome'))
backlog=metric('lockers.backlog.pending','gauge',('queue','storage'),'shared_storage','Cached durable count; -1 means not sampled. Aggregate replicas with max by storage.')
fresh=metric('lockers.backlog.sample.timestamp','gauge',('storage',),'shared_storage','Last successful sample in Unix seconds.')
healthy=metric('lockers.backlog.sample.healthy','gauge',('storage',),'process')
metric('lockers.backlog.refresh.failures',labels=('storage',))
metric('lockers.monitoring.info','gauge',('contract_version','library_version','client_service_name'))
metric('lockers.monitoring.component.enabled','gauge',('component',))
for n in ['lockers.delivery.accepted','lockers.delivery.attempts','lockers.delivery.failures','lockers.delivery.retry.rounds']:metric(n,labels=('queue',))
for n in ['lockers.storage.operations']:metric(n,labels=('store','operation','outcome'))
metric('lockers.storage.duration','timer',('store','operation','outcome'))
metric('lockers.agent.invocations',labels=('outcome',));metric('lockers.agent.duration','timer',('outcome',));metric('lockers.agent.derived.writes')
metric('lockers.claim.renew.last.success.timestamp','gauge');metric('lockers.claim.renew.interval.seconds','gauge');metric('lockers.claim.renew.failures')
for n in ['lockers.connector.operations','lockers.connector.duration']:
    metric(n,'timer' if n.endswith('duration') else 'counter',('operation','outcome','platform'))
for n,labels in [('lockers.connector.telemetry.dropped',('platform','reason')),('lockers.connector.telemetry.batches',('platform',)),('lockers.connector.diagnostics.dropped',('platform',))]:metric(n,labels=labels)
legacy={
'lockers.room.locker.rejected.oversize':('counter',()),'lockers.room.locker.rejected.ratelimited':('counter',()),
'lockers.room.cache.hits':('counter',()),'lockers.room.cache.misses':('counter',()),'lockers.room.cache.size':('gauge',()),
'lockers.room.forward.writes':('counter',()),'lockers.room.forward.failures':('counter',()),'lockers.room.gateway.lookup.failures':('counter',()),
'lockers.room.events.post.success':('counter',()),'lockers.room.events.post.failure':('counter',()),'lockers.room.agent.failures':('counter',()),
'lockers.room.subscriptions':('counter',('operation','result')),
'lockers.session.opens':('counter',('result',)),'lockers.session.streams.active':('gauge',()),'lockers.session.streams.cancelled':('counter',()),
'lockers.session.events.delivered':('counter',('delivery_type',)),'lockers.session.events.posted':('counter',()),'lockers.session.events.queued':('counter',()),
'lockers.session.inbox.saves':('counter',()),'lockers.session.inbox.deletes':('counter',()),'lockers.session.events.preopen':('counter',()),
'lockers.session.events.delivery.time':('timer',('delivery_type',)),
'lockers.push.provider.configured':('gauge',('backend',)),'lockers.push.send.duration':('timer',('backend','result')),
'lockers.push.sent':('counter',('backend',)),'lockers.push.failed':('counter',('backend',)),'lockers.push.retried':('counter',('backend',)),
'lockers.push.rejected':('counter',('backend',)),'lockers.push.deadlettered':('counter',('backend',)),
'lockers.push.queue.depth':('gauge',('backend',)),'lockers.push.deadletter.depth':('gauge',('backend',)),
'lockers.claim.rooms.owned':('gauge',()),'lockers.claim.renew.duration':('timer',()),
'lockers.claim.acquires':('counter',()),'lockers.claim.steals':('counter',()),'lockers.claim.lost':('counter',()),
'lockers.claim.redirects':('counter',()),'lockers.gateway.registry.misses':('counter',()),
'lockers.shard.ownership.moves':('counter',()),'lockers.shard.inhandoff':('gauge',()),'lockers.shard.lease.expiries':('counter',()),
'lockers.reshard.duration':('timer',()),'lockers.reshard.rooms.rebuilt':('counter',()),'lockers.reshard.cas.conflicts':('counter',()),
'lockers.write.phase':('timer',('phase',)),
'lockers.room.dispatcher.time':('timer',()),'lockers.session.dispatcher.time':('timer',()),
'lockers.room.locker.get.time':('timer',()),'lockers.room.locker.getall.time':('timer',()),
'lockers.room.locker.count':('summary',()),'lockers.push.queue.size':('gauge',()),
'lockers.push.enqueued':('counter',('backend',)),'lockers.push.registrations':('counter',('backend',)),
'lockers.session.creates':('counter',('result',))}
for n in ['lockers.room.locker.subscribe','lockers.room.locker.get','lockers.room.locker.getall','lockers.room.locker.postlockerchange','lockers.room.locker.deletelocker','lockers.room.locker.lock','lockers.room.locker.unlock','lockers.session.post','lockers.session.broadcast','lockers.push.register','lockers.push.unregister','lockers.push.config','lockers.push.sendpush']:legacy[n]=('counter',('result',))
metric('lockers.delivery.worker.active','gauge',('queue',));metric('lockers.delivery.duration','timer',('queue',))
for n,(k,l) in legacy.items():metric(n,k,l)
def m(name):return CAT[name]['prometheus']
def rate(name,extra=''):return 'sum by ('+(extra or 'service_name')+') (rate('+m(name)+S+'[$__rate_interval]))'
def pct(name,extra=''):
    return 'histogram_quantile(0.95, sum by (le'+(','+extra if extra else '')+') (rate('+m(name)+'_bucket'+S+'[$__rate_interval])))'
def scoped(selector,**labels):return selector[:-1]+''.join(','+k+'="'+v+'"' for k,v in labels.items())+'}'
def common(service):return [('RPC throughput', 'sum by (operation,outcome) (rate('+rpc+scoped(S,service=service)+'[$__rate_interval]))','ops'),('RPC p95 latency','histogram_quantile(0.95,sum by(le,operation)(rate('+rpcdur+'_bucket'+scoped(S,service=service)+'[$__rate_interval])))','s')]
packs={
'overview':[('Monitoring contract','lockers_monitoring_info'+S,'none'),('Enabled components','lockers_monitoring_component_enabled'+S,'none'),('RPC throughput',rate('lockers.rpc.requests','service,outcome'),'ops'),('RPC p95',pct('lockers.rpc.duration','service'),'s'),('Live streams','sum(lockers_session_streams_active'+S+')','short'),('Durable backlog','max by(storage,queue)('+backlog+S+')','short'),('Snapshot age','time()-max by(storage)('+fresh+S+')','s'),('Connector uploads',rate('lockers.connector.telemetry.batches','platform'),'ops')],
'rooms':common('room')+[('Write phases p95',pct('lockers.write.phase','phase'),'s'),('Cache hits / misses',rate('lockers.room.cache.hits')+' / ( '+rate('lockers.room.cache.hits')+' + '+rate('lockers.room.cache.misses')+' )','percentunit'),('Oversize writes',rate('lockers.room.locker.rejected.oversize'),'ops'),('Rate-limited writes',rate('lockers.room.locker.rejected.ratelimited'),'ops'),('Forward failures',rate('lockers.room.forward.failures'),'ops')],
'sessions':common('session')+[('Live streams','sum(lockers_session_streams_active'+S+')','short'),('Session opens',rate('lockers.session.opens','result'),'ops'),('Live and queued events',rate('lockers.session.events.delivered','delivery_type'),'ops'),('Stream cancellations',rate('lockers.session.streams.cancelled'),'ops'),('Inbox saves',rate('lockers.session.inbox.saves'),'ops'),('Inbox deletes',rate('lockers.session.inbox.deletes'),'ops')],
'delivery':[('Durable pending intents','max by(storage,queue)('+backlog+scoped(S,queue='room')+')','short'),('Pending push delivery','max by(storage)('+backlog+scoped(S,queue='push_delivery')+')','short'),('Attempts',rate('lockers.delivery.attempts','queue'),'ops'),('Accepted gateway groups',rate('lockers.delivery.accepted','queue'),'ops'),('Gateway failures',rate('lockers.delivery.failures','queue'),'ops'),('Retry rounds',rate('lockers.delivery.retry.rounds','queue'),'ops'),('Active workers','sum by(queue)(lockers_delivery_worker_active'+S+')','short'),('Delivery round p95',pct('lockers.delivery.duration','queue'),'s'),('Snapshot age','time()-max by(storage)('+fresh+S+')','s'),('Snapshot health','min by(storage)('+healthy+S+')','none')],
'storage':[('Operations',rate('lockers.storage.operations','store,operation,outcome'),'ops'),('Storage p95',pct('lockers.storage.duration','store,operation'),'s'),('Snapshot age','time()-max by(storage)('+fresh+S+')','s'),('Refresh failures',rate('lockers.backlog.refresh.failures','storage'),'ops')],
'push':common('push')+[('Configured providers','max by(backend)(lockers_push_provider_configured'+S+')','none'),('Send p95',pct('lockers.push.send.duration','backend'),'s'),('Sent pushes',rate('lockers.push.sent','backend'),'ops'),('Retryable failures',rate('lockers.push.failed','backend'),'ops'),('Retries',rate('lockers.push.retried','backend'),'ops'),('Rejected pushes',rate('lockers.push.rejected','backend'),'ops'),('New dead letters',rate('lockers.push.deadlettered','backend'),'ops'),('Shared pending pushes','max by(storage)('+backlog+scoped(S,queue='push')+')','short'),('Shared dead letters','max by(storage)('+backlog+scoped(S,queue='deadletter')+')','short'),('Local backend depth estimate','lockers_push_queue_depth'+S,'short')],
'ownership':[('Ownership modes','lockers_monitoring_component_enabled'+scoped(S,component='claim')+' or lockers_monitoring_component_enabled'+scoped(S,component='ring'),'none'),('Owned rooms','sum(lockers_claim_rooms_owned'+S+')','short'),('Renewal p95',pct('lockers.claim.renew.duration'),'s'),('Renewal failures',rate('lockers.claim.renew.failures'),'ops'),('Last renewal age','time()-lockers_claim_renew_last_success_timestamp'+S,'s'),('Lost claims',rate('lockers.claim.lost'),'ops'),('Redirects',rate('lockers.claim.redirects'),'ops'),('Registry misses',rate('lockers.gateway.registry.misses'),'ops'),('Handoffs','sum(lockers_shard_inhandoff'+S+')','short'),('Ownership moves',rate('lockers.shard.ownership.moves'),'ops'),('Reshard p95',pct('lockers.reshard.duration'),'s')],
'agents':[('Invocations',rate('lockers.agent.invocations','outcome'),'ops'),('Agent p95',pct('lockers.agent.duration','outcome'),'s'),('Agent failures',rate('lockers.room.agent.failures'),'ops'),('Derived writes',rate('lockers.agent.derived.writes'),'ops')],
'connector':[('Fleet operation samples',rate('lockers.connector.operations','platform,operation,outcome'),'ops'),('Fleet p95',pct('lockers.connector.duration','platform,operation'),'s'),('Accepted telemetry batches',rate('lockers.connector.telemetry.batches','platform'),'ops'),('Reported telemetry loss',rate('lockers.connector.telemetry.dropped','platform,reason'),'ops'),('Spans without exporter',rate('lockers.connector.diagnostics.dropped','platform'),'ops')],
'runtime':[('Heap usage','sum(jvm_memory_used_bytes'+scoped(R,area='heap')+')','bytes'),('GC time','sum(rate(jvm_gc_pause_seconds_sum'+R+'[$__rate_interval]))','s'),('CPU','process_cpu_usage'+R,'percentunit'),('Live threads','jvm_threads_live_threads'+R,'short'),('Uptime','process_uptime_seconds'+R,'s')]
}
def dashboard(pack,panels):
    variables=[{'name':n,'label':label,'type':'datasource','query':typ,'current':{},'options':[],'refresh':1} for n,label,typ in [('prometheus','Prometheus','prometheus'),('loki','Loki','loki'),('tempo','Tempo','tempo')]]
    for n,q in [('service_name','label_values(lockers_monitoring_info, service_name)'),('client_service_name','label_values(lockers_monitoring_info, client_service_name)'),('environment','label_values(lockers_monitoring_info{service_name=~"${service_name:regex}"},environment)'),('job','label_values(lockers_monitoring_info{service_name=~"${service_name:regex}",environment=~"${environment:regex}"},job)'),('instance','label_values(lockers_monitoring_info{job=~"${job:regex}",service_name=~"${service_name:regex}"},instance)')]:
        variables.append({'name':n,'type':'query','datasource':{'type':'prometheus','uid':'${prometheus}'},'query':q,'definition':q,'refresh':2,'includeAll':True,'allValue':'.*','multi':True,'current':{'text':'All','value':'$__all'}})
    out=[{'id':1,'title':'Monitoring status','type':'text','gridPos':{'x':0,'y':0,'w':24,'h':3},'options':{'mode':'markdown','content':'Contract v1. Select your existing data sources and deployment. Component enabled=0 means disabled; missing contract or stale snapshots mean monitoring is unavailable. Idle counters remain zero. Connector data is best effort and represents reporting devices. Shared backlog counts use max across replicas; local estimates describe each replica.'}}]
    for i,(title,expr,unit) in enumerate(panels):
        out.append({'id':i+2,'title':title,'type':'timeseries','datasource':{'type':'prometheus','uid':'${prometheus}'},'gridPos':{'x':(i%2)*12,'y':3+(i//2)*8,'w':12,'h':8},'targets':[{'refId':'A','expr':expr,'legendFormat':'{{operation}} {{outcome}} {{backend}} {{queue}} {{platform}} {{store}} {{instance}}'}],'fieldConfig':{'defaults':{'unit':unit,'noValue':'No telemetry'},'overrides':[]},'options':{'legend':{'displayMode':'table','placement':'bottom'},'tooltip':{'mode':'multi'}}})
    y=3+((len(panels)+1)//2)*8
    component={'rooms':'room','sessions':'session','agents':'agent'}.get(pack,pack)
    if pack not in ('overview','runtime'):
        selector=S[:-1]+',component=~"local|claim|ring"}' if pack=='ownership' else scoped(S,component=component)
        out.append({'id':len(out)+1,'title':'Component configured (0 = disabled)','type':'stat','datasource':{'type':'prometheus','uid':'${prometheus}'},'gridPos':{'x':0,'y':y,'w':24,'h':3},'targets':[{'refId':'A','expr':'max by(component)(lockers_monitoring_component_enabled'+selector+')','legendFormat':'{{component}}'}],'fieldConfig':{'defaults':{'noValue':'Monitoring unavailable'},'overrides':[]}})
        y+=3
    component={'rooms':'room','sessions':'session','agents':'agent','ownership':'ownership'}.get(pack,pack)
    logs='{service_name=~"${service_name:regex}",deployment_environment=~"${environment:regex}"} | json'+(' | lockers_component="'+component+'"' if pack not in ('overview','runtime') else '')
    out.append({'id':len(out)+1,'title':'Operational events — trace IDs link to Tempo via Loki data-source settings','type':'logs','datasource':{'type':'loki','uid':'${loki}'},'gridPos':{'x':0,'y':y,'w':24,'h':9},'targets':[{'refId':'A','expr':logs}],'options':{'showTime':True,'wrapLogMessage':True,'sortOrder':'Descending'}})
    service='client_service_name' if pack=='connector' else 'service_name'
    trace='resource.service.name =~ "${'+service+':regex}" && resource.deployment.environment =~ "${environment:regex}"'+(' && span.lockers.component = "'+component+'"' if pack not in ('overview','runtime') else '')
    out.append({'id':len(out)+1,'title':'Recent operation traces','type':'table','datasource':{'type':'tempo','uid':'${tempo}'},'gridPos':{'x':0,'y':y+9,'w':24,'h':8},'targets':[{'refId':'A','queryType':'traceql','query':'{ '+trace+' }','limit':20}],'fieldConfig':{'defaults':{},'overrides':[]}})
    links=[{'title':'Lockers '+p.title(),'type':'link','url':'/d/lockers-'+p,'includeVars':True,'keepTime':True} for p in packs if p!=pack]
    return {'uid':'lockers-'+pack,'title':'Lockers / '+pack.title(),'id':None,'version':1,'schemaVersion':39,'tags':['lockers','monitoring-v1',pack],'editable':True,'timezone':'browser','refresh':'30s','time':{'from':'now-1h','to':'now'},'templating':{'list':variables},'panels':out,'links':links}
def generated():
    files={ROOT/'dashboards'/f'lockers-{p}.json':json.dumps(dashboard(p,v),indent=2)+'\n' for p,v in packs.items()}
    catalog={'contract_version':1,'resource_labels':['service_name','environment'],'latency_buckets_seconds':[.001,.005,.01,.025,.05,.1,.25,.5,1,2.5,5,10,30,60],'metrics':list(CAT.values())}
    files[ROOT/'catalog.json']=json.dumps(catalog,indent=2)+'\n'
    return files
if __name__=='__main__':
    args=argparse.ArgumentParser();args.add_argument('--check',action='store_true');a=args.parse_args()
    for path,content in generated().items():
        if a.check:
            assert path.read_text()==content,str(path)+' is stale'
        else:path.parent.mkdir(parents=True,exist_ok=True);path.write_text(content)
    print(f'{len(packs)} independent dashboards; {len(CAT)} catalog entries verified' if a.check else f'Generated {len(packs)} dashboards and catalog')
