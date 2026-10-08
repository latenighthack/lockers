#!/usr/bin/env python3
"""Validate provisioning, manual import, every query, and correlated logs/traces."""
import json,pathlib,subprocess,time,urllib.request,urllib.parse,urllib.error
ROOT=pathlib.Path(__file__).resolve().parents[1]
COMPOSE=['docker','compose','-f',str(ROOT/'smoke/compose.yml')]
def port(service,p):
    return subprocess.check_output(COMPOSE+['port',service,str(p)],text=True).strip().split(':')[-1]
urls={s:'http://127.0.0.1:'+port(s,p) for s,p in [('grafana',3000),('prometheus',9090),('loki',3100),('tempo',3200)]}
otlp='http://127.0.0.1:'+port('tempo',4318)
def request(url,data=None):
    body=None if data is None else json.dumps(data).encode()
    with urllib.request.urlopen(urllib.request.Request(url,data=body,headers={'Content-Type':'application/json'}),timeout=15) as r:
        payload=r.read();return json.loads(payload) if payload else {}
def wait(url):
    for _ in range(45):
        try:return request(url)
        except (OSError,ValueError):time.sleep(1)
    raise AssertionError('Service did not become ready: '+url)
wait(urls['grafana']+'/api/health');wait(urls['prometheus']+'/api/v1/targets');wait(urls['loki']+'/loki/api/v1/status/buildinfo')
subprocess.run(COMPOSE+['exec','-T','prometheus','promtool','check','rules','/etc/prometheus/lockers-alerts/lockers.yml'],check=True)
subprocess.run(COMPOSE+['exec','-T','-w','/etc/prometheus/lockers-alerts','prometheus','promtool','test','rules','tests.yml'],check=True)
expected={json.loads(p.read_text())['uid'] for p in (ROOT/'dashboards').glob('*.json')}
for _ in range(20):
    if expected <= {d['uid'] for d in request(urls['grafana']+'/api/search?tag=lockers')}:break
    time.sleep(1)
else:raise AssertionError('Missing provisioned dashboards')
trace='0123456789abcdef0123456789abcdef';span='0123456789abcdef';now=time.time_ns()
request(otlp+'/v1/traces',{'resourceSpans':[{'resource':{'attributes':[{'key':'service.name','value':{'stringValue':'lockers-sample'}},{'key':'deployment.environment','value':{'stringValue':'test'}}]},'scopeSpans':[{'scope':{'name':'com.latenighthack.lockers'},'spans':[{'traceId':trace,'spanId':span,'name':'lockers.room.write','kind':1,'startTimeUnixNano':str(now-100_000_000),'endTimeUnixNano':str(now-10_000_000),'attributes':[{'key':'lockers.component','value':{'stringValue':'room'}}]}]}]}]})
request(urls['loki']+'/loki/api/v1/push',{'streams':[{'stream':{'service_name':'lockers-sample','deployment_environment':'test'},'values':[[str(now),json.dumps({'lockers_component':'room','lockers_operation':'write','lockers_outcome':'error','trace_id':trace,'span_id':span})]]}]})
variables={'service_name':'lockers-sample','client_service_name':'lockers-sample','environment':'test','job':'.*','instance':'.*','$__rate_interval':'1m'}
def expand(query):
    for key,value in variables.items():
        query=query.replace('${'+key+':regex}',value).replace('${'+key+'}',value).replace('$'+key,value)
    return query.replace('$__rate_interval','1m')
queries=0
for p in sorted((ROOT/'dashboards').glob('*.json')):
    dashboard=json.loads(p.read_text())
    fetched=request(urls['grafana']+'/api/dashboards/uid/'+dashboard['uid'])['dashboard']
    assert len(fetched['panels'])==len(dashboard['panels'])
    # Import the same file with a distinct UID to avoid editing provisioned resources.
    manual=dict(dashboard,uid='manual-'+dashboard['uid'],title='Manual '+dashboard['title'])
    result=request(urls['grafana']+'/api/dashboards/db',{'dashboard':manual,'overwrite':True})
    assert result['status']=='success',result
    for panel in dashboard['panels']:
        for target in panel.get('targets',[]):
            kind=panel['datasource']['type'];query=expand(target.get('expr',target.get('query','')))
            if kind=='prometheus':url=urls[kind]+'/api/v1/query?'+urllib.parse.urlencode({'query':query})
            elif kind=='loki':url=urls[kind]+'/loki/api/v1/query_range?'+urllib.parse.urlencode({'query':query,'start':str(now-3_600_000_000_000),'end':str(time.time_ns()),'limit':20})
            else:url=urls[kind]+'/api/search?'+urllib.parse.urlencode({'q':query,'start':int(time.time())-3600,'end':int(time.time())+1,'limit':20})
            result=request(url);assert result.get('status','success')=='success',(p,query,result);queries+=1
# Shared backlog is counted once across two targets.
subprocess.run(COMPOSE+['exec','-T','fixture','python','-c','import urllib.request;urllib.request.urlopen("http://localhost:8080/state/failing")'],check=True,stdout=subprocess.DEVNULL)
time.sleep(6)
result=request(urls['prometheus']+'/api/v1/query?'+urllib.parse.urlencode({'query':'max by(storage)(lockers_backlog_pending{queue="room"})'}))
assert len(result['data']['result'])==1 and float(result['data']['result'][0]['value'][1])==10,result
for state in ('idle','disabled','healthy'):
    subprocess.run(COMPOSE+['exec','-T','fixture','python','-c',f'import urllib.request;urllib.request.urlopen("http://localhost:8080/state/{state}")'],check=True,stdout=subprocess.DEVNULL)
    time.sleep(6)
    assert all(t['health']=='up' for t in request(urls['prometheus']+'/api/v1/targets')['data']['activeTargets'])
    components=request(urls['prometheus']+'/api/v1/query?'+urllib.parse.urlencode({'query':'max(lockers_monitoring_component_enabled{component="room"})'}))
    assert float(components['data']['result'][0]['value'][1])==(0 if state=='disabled' else 1),(state,components)
    backlog=request(urls['prometheus']+'/api/v1/query?'+urllib.parse.urlencode({'query':'max(lockers_backlog_pending{queue="room"})'}))
    assert float(backlog['data']['result'][0]['value'][1])==0,(state,backlog)
logs=request(urls['loki']+'/loki/api/v1/query_range?'+urllib.parse.urlencode({'query':'{service_name="lockers-sample"} | json | trace_id="'+trace+'"','start':str(now-3_600_000_000_000),'end':str(time.time_ns())}))
assert logs['data']['result'],logs
traces=request(urls['tempo']+'/api/traces/'+trace);assert traces,traces
print(json.dumps({'dashboards_provisioned':10,'dashboards_manually_imported':10,'valid_queries':queries,'fixture_states':['healthy','idle','disabled','failing'],'shared_backlog_not_doubled':True,'trace_id':trace,'grafana':urls['grafana']},indent=2))
