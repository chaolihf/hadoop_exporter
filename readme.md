1. hadoop exporter
2. example
http://localhost:8828/metrics?target=192.168.100.6-namenode&module=NM&showall=1

if showall=1 , will show all metrics ,otherwise will show only some metrics defined in config.json
3.config.json
codeMaps: define map for string to int ,such as namenode FSState have value "Operational",it will be mapped to 1.otherwise string value will be mapped to 0

4.build
go build -ldflags='-w -s' -o hadoop_exporter