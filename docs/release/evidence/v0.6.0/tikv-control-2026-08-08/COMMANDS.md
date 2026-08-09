# Reproduction commands

The commands below were executed on 2026-08-08 against Docker Engine 29.4.1.
The cluster was created on the user-defined bridge network
`tikv-control-net` (`192.168.128.0/20`) with one PD and three TiKV stores.

```text
docker network create tikv-control-net
docker volume create tikv-control-pd
docker volume create tikv-control-1
docker volume create tikv-control-2
docker volume create tikv-control-3

docker run -d --name tikv-control-pd --network tikv-control-net \
  -v tikv-control-pd:/data pingcap/pd:v8.5.0 \
  --name=pd --data-dir=/data \
  --client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://tikv-control-pd:2379 \
  --peer-urls=http://0.0.0.0:2380 \
  --advertise-peer-urls=http://tikv-control-pd:2380 \
  --initial-cluster=pd=http://tikv-control-pd:2380

Repeat the following command with `N=1`, `N=2`, and `N=3` (shell variable
substitution is shown only for readability):

```text
docker run -d --name tikv-control-N --network tikv-control-net \
  -v tikv-control-N:/data pingcap/tikv:v8.5.0 \
  --addr=0.0.0.0:20160 --advertise-addr=tikv-control-N:20160 \
  --status-addr=0.0.0.0:20180 \
  --advertise-status-addr=tikv-control-N:20180 \
  --pd-endpoints=http://tikv-control-pd:2379 --data-dir=/data \
  --log-level=warn
```

docker run --rm --network tikv-control-net pingcap/go-ycsb:latest \
  load tikv --threads 8 -p tikv.pd=tikv-control-pd:2379 \
  -p recordcount=10000 -p fieldcount=10 -p fieldlength=100 \
  -p workload=core -p insertproportion=1 -p updateproportion=0 \
  -p readproportion=0 -p requestdistribution=uniform

docker run --rm --network tikv-control-net pingcap/go-ycsb:latest \
  run tikv --threads 16 -p tikv.pd=tikv-control-pd:2379 \
  -p recordcount=10000 -p operationcount=50000 \
  -p fieldcount=10 -p fieldlength=100 -p workload=core \
  -p readallfields=true -p readproportion=0.5 -p updateproportion=0.5 \
  -p insertproportion=0 -p scanproportion=0 \
  -p requestdistribution=uniform
```

For the shaped run, a `nicolaka/netshoot:latest` client namespace was created
and the YCSB container shared that namespace:

```text
docker run -d --name tikv-control-client-net --network tikv-control-net \
  --cap-add NET_ADMIN nicolaka/netshoot:latest sleep 600
docker exec tikv-control-client-net \
  tc qdisc replace dev eth0 root netem delay 500us
docker exec tikv-control-client-net ping -c 20 -i 0.05 tikv-control-1
docker run --rm --network container:tikv-control-client-net \
  pingcap/go-ycsb:latest run tikv --threads 16 ...
```

The final command uses the same properties as the unshaped Workload A command.
