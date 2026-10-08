"""Synthetic catalog-shaped fixtures; library tests separately verify actual exposition."""
import http.server,json,pathlib,time
catalog=json.loads((pathlib.Path(__file__).parents[1]/'catalog.json').read_text())
started=time.time();state='healthy'
def labels(extra):return '{'+','.join(f'{k}="{v}"' for k,v in dict(service_name='lockers-sample',environment='test',**extra).items())+'}'
def metrics(replica):
    age=max(1,time.time()-started);out=[]
    for metric in catalog['metrics']:
        name=metric['prometheus'];typ=metric['type']
        values={k:{'service':'room','operation':'write','outcome':'ok','platform':'ios','backend':'apns','result':'OK','delivery_type':'live','store':'locker','storage':'shared','queue':'room','reason':'upload','phase':'agent','contract_version':'1','library_version':'test','client_service_name':'lockers-sample'}.get(k,'test') for k in metric['labels']}
        combinations=[values]
        if name=='lockers_monitoring_component_enabled':combinations=[dict(values,component=c) for c in ['room','session','storage','push','agent','delivery','delivery_worker','push_worker','claim','ring','local','connector']]
        if name=='lockers_backlog_pending':combinations=[dict(values,queue=c) for c in ['room','push_delivery','push','deadletter']]
        for v in combinations:
            value=1
            if typ=='counter':
                value=age*100 if state!='idle' else 100
                if any(x in name for x in ('fail','rejected','deadlettered','dropped','ratelimited','oversize','misses')):value=age*2 if state=='failing' else 0
                if name in ('lockers_delivery_accepted_total','lockers_push_sent_total') and state=='failing':value=0
            if name=='lockers_rpc_requests_total' and state=='failing':v=dict(v,outcome='error')
            if name=='lockers_backlog_pending':value=10 if state=='failing' else 0
            if name=='lockers_monitoring_component_enabled':value=0 if state=='disabled' or v.get('component') in ['claim','ring'] else 1
            if name.endswith('_timestamp'):value=int(time.time())-120 if state=='failing' else int(time.time())
            if name=='lockers_claim_renew_interval_seconds':value=5
            if name=='lockers_claim_rooms_owned':value=0
            if typ=='timer':
                for b in catalog['latency_buckets_seconds']+[float('inf')]:out.append(name+'_bucket'+labels(dict(v,le='+Inf' if b==float('inf') else str(b)))+' '+str(age*100 if b>=.01 else 0))
                out += [name+'_sum'+labels(v)+' '+str(age),name+'_count'+labels(v)+' '+str(age*100)]
            else:out.append(name+labels(v)+' '+str(value))
    out+=['jvm_memory_used_bytes{area="heap"} 10485760','jvm_gc_pause_seconds_sum '+str(age/100),'process_cpu_usage 0.1','jvm_threads_live_threads 10','process_uptime_seconds '+str(age)]
    return '\n'.join(out)+'\n'
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        global state
        if self.path.startswith('/state/'):
            state=self.path.rsplit('/',1)[-1];body=state
        else:body=metrics(self.path=='/replica')
        self.send_response(200);self.send_header('Content-Type','text/plain; version=0.0.4');self.end_headers();self.wfile.write(body.encode())
    def log_message(self,*args):pass
http.server.HTTPServer(('0.0.0.0',8080),Handler).serve_forever()
