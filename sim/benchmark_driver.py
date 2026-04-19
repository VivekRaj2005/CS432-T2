import time, json, requests
BASE="http://127.0.0.1:8000"

def tget(path, params=None):
    t0=time.perf_counter()
    r=requests.get(BASE+path, params=params, timeout=20); r.raise_for_status()
    return (time.perf_counter()-t0)*1000

def tpost(path, body):
    t0=time.perf_counter()
    r=requests.post(BASE+path, json=body, timeout=20); r.raise_for_status()
    return (time.perf_counter()-t0)*1000

requests.post(BASE+"/benchmark/reset", timeout=20)

read_lat=[]
for _ in range(1000):
    read_lat.append(tget("/fetch", {"source":"merged","limit":20}))

write_lat=[]
for i in range(1000):
    write_lat.append(tpost("/create", {"user_id":1000+i,"name":"u"+str(i),"age":22,"dept":"eng","active":True}))
    time.sleep(0.05)

time.sleep(5)
metrics=requests.get(BASE+"/benchmark/metrics", timeout=20).json()

out={
  "reads_1000_avg_ms": sum(read_lat)/len(read_lat),
  "writes_1000_avg_ms": sum(write_lat)/len(write_lat),
  "server_metrics": metrics
}
print(json.dumps(out, indent=2))