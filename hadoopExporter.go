package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strings"

	jjson "github.com/chaolihf/udpgo/json"
	lang "github.com/chaolihf/udpgo/lang"
	"github.com/go-resty/resty/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type ExporterConfig struct {
	ListenAddress string         `json:"listen"`
	Metrics       []MetricType   `json:"metrics"`
	TargetServers []TargetServer `json:"servers"`
}

type MetricType struct {
	Name string `json:"name"`
}

type TargetServer struct {
	Name   string `json:"name"`
	Url    string `json:"url"`
	Module string `json:"module"`
}

var logger *zap.Logger
var client *resty.Client

type beanHandler func(beanInfo *jjson.JsonObject, keySet map[string][]string, modulePrefix string) []prometheus.Metric

var handlerMap map[string]beanHandler
var exporterInfo ExporterConfig

type hadoopCollector struct {
	remoteUrl    string
	modulePrefix string
}

func (collector *hadoopCollector) Describe(ch chan<- *prometheus.Desc) {

}

func (collector *hadoopCollector) Collect(ch chan<- prometheus.Metric) {
	metrics := getJmxInfo(collector.remoteUrl, collector.modulePrefix)
	for _, metric := range metrics {
		ch <- metric
	}
}

func getJmxInfo(url string, modulePrefix string) []prometheus.Metric {
	var metrics []prometheus.Metric
	logger.Info(fmt.Sprintf("get url %s", url))
	resp, err := client.R().EnableTrace().Get(url)
	if err != nil {
		logger.Error(err.Error())
	}
	jsonInfo, _ := jjson.FromBytes(resp.Body())
	beanInfos := jsonInfo.GetJsonArray("beans")
	var keySet = make(map[string][]string)
	for _, beanInfo := range beanInfos {
		beanName := beanInfo.GetString("name")
		if strings.HasPrefix(beanName, "Hadoop:") {
			handler := handlerMap[beanName]
			if handler == nil {
				handler = getBeanMetrics
			}
			metrics = append(metrics, handler(beanInfo, keySet, modulePrefix)...)
		}
	}
	return metrics
}

// @title
func getAttributeValue(attrValue *jjson.JsonObject) float64 {
	var value float64
	switch attrValue.VType {
	case reflect.Float64:
		{
			value = attrValue.Value.(float64)
		}
	case reflect.Int32:
		{
			value = float64(attrValue.Value.(int64))
		}
	}
	return value
}

func init() {
	logger = lang.InitLogger()
	client = resty.New()
	handlerMap = make(map[string]beanHandler)
	filePath := "config.json"
	content, err := os.ReadFile(filePath)
	if err != nil {
		logger.Error("读取文件出错:" + filePath + err.Error())
	} else {
		err := json.Unmarshal(content, &exporterInfo)
		if err != nil {
			logger.Error("解析文件出错:" + filePath + err.Error())
		}
	}
}

func main() {
	flag.Parse()
	registerNameHandler("Hadoop:service=HBase,name=RegionServer,sub=Regions", handlerRegionServerRegions)
	registerNameHandler("Hadoop:service=HBase,name=RegionServer,sub=Tables", handlerRegionServerRegions)
	registerNameHandler("Hadoop:service=HBase,name=RegionServer,sub=TableLatencies", handlerRegionServerRegions)

	//http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		registry := prometheus.NewRegistry()
		params := r.URL.Query()
		targetName := params.Get("target")
		module := params.Get("module")
		if targetName == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing target parameter!"))
			return
		}
		var targetUrl string = ""
		var defaultModule string = ""
		for _, targetServer := range exporterInfo.TargetServers {
			if targetServer.Name == targetName {
				targetUrl = targetServer.Url
				defaultModule = targetServer.Module
				break
			}
		}
		if targetUrl == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("can't find target name on server " + targetName))
			return
		}
		if module == "" {
			if defaultModule == "" {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("missing module parameter!"))
				return
			}
			module = defaultModule
		}
		registry.MustRegister(&hadoopCollector{remoteUrl: targetUrl, modulePrefix: module})

		// probeSuccessGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		// 	Name: "probe_success",
		// 	Help: "Displays whether or not the probe was a success",
		// })
		// registry.MustRegister(probeSuccessGauge)
		// probeSuccessGauge.Set(1010)
		h := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
		h.ServeHTTP(w, r)
	})
	// prometheus.MustRegister(&hadoopCollector{})
	logger.Info("start to listen on port " + exporterInfo.ListenAddress)
	http.ListenAndServe(exporterInfo.ListenAddress, nil)

}

func registerNameHandler(name string, handler beanHandler) {
	handlerMap[name] = handler
}

func getBeanMetrics(beanInfo *jjson.JsonObject, keySet map[string][]string, modulePrefix string) []prometheus.Metric {
	var metrics []prometheus.Metric
	var tags = make(map[string]string)
	metrixPerfix := getNameLabelInfo(beanInfo, tags, modulePrefix)
	tagString := getTagNames(tags)
	for _, key := range beanInfo.GetKeys() {
		if key != "name" && key != "modelerType" && !strings.HasPrefix(key, "tag.") {
			metricName := renameMetricName(keySet, metrixPerfix+"_"+key, tagString)
			if isInExportList(metricName) {
				value := getAttributeValue(beanInfo.Attributes[key])
				hadoopMetric := prometheus.NewDesc(metricName, metricName, nil, tags)
				metric := prometheus.MustNewConstMetric(hadoopMetric, prometheus.CounterValue, value)
				metrics = append(metrics, metric)
			}
		}
	}
	return metrics
}

// @title handler for hbase region server
//因为存在指标名称小写一样的问题，对于小写一样但指标名称不一样的进行编号；key为指标名称的小写，

func handlerRegionServerRegions(beanInfo *jjson.JsonObject, keySet map[string][]string, modulePrefix string) []prometheus.Metric {
	var metrics []prometheus.Metric
	var tags = make(map[string]string)
	metrixPerfix := getNameLabelInfo(beanInfo, tags, modulePrefix)
	tagString := getTagNames(tags)
	for _, key := range beanInfo.GetKeys() {
		if key != "name" && key != "modelerType" && !strings.HasPrefix(key, "tag.") {
			metricIndex := strings.Index(key, "_metric_")
			if metricIndex != -1 {
				parts := strings.Split(key, "_")
				var regionIndex int
				metriTag := tagString
				for index, part := range parts {
					if part == "region" {
						tagName := strings.Join(parts[3:index], "_")
						metriTag = metriTag + "_tableName"
						tags["tableName"] = tagName
						regionIndex = index
					} else if part == "metric" {
						tagName := strings.Join(parts[regionIndex+1:index], "_")
						metriTag = metriTag + "_tableId"
						tags["tableId"] = tagName
					}
				}
				metricName := renameMetricName(keySet, metrixPerfix+"_"+key[metricIndex+8:], metriTag)
				if isInExportList(metricName) {
					value := getAttributeValue(beanInfo.Attributes[key])
					hadoopMetric := prometheus.NewDesc(metricName, metricName, nil, tags)
					metric := prometheus.MustNewConstMetric(hadoopMetric, prometheus.CounterValue, value)
					metrics = append(metrics, metric)
				}
				delete(tags, "tableName")
				delete(tags, "tableId")
			}
		}
	}
	return metrics
}

func isInExportList(metricName string) bool {
	if exporterInfo.Metrics != nil {
		for _, metricType := range exporterInfo.Metrics {
			if metricName == metricType.Name {
				return true
			}
		}
		return false
	}
	return true
}

func getTagNames(tags map[string]string) string {
	result := []string{}
	for key, _ := range tags {
		result = append(result, key)
	}
	sort.Strings(result)
	return strings.Join(result, "_")
}

// @Title 替换特殊字符，对重复的指标名称进行替换
func renameMetricName(keySet map[string][]string, metricName string, tagString string) string {
	metricName = strings.ReplaceAll(metricName, "(", "")
	metricName = strings.ReplaceAll(metricName, ")", "")
	metricName = strings.ReplaceAll(metricName, ".", "_")
	metricName = strings.ReplaceAll(metricName, "-", "_")
	metricName = strings.ReplaceAll(metricName, ":", "_")
	shortMetricName := strings.ToLower(metricName)
	dupMetrics := keySet[shortMetricName]
	if dupMetrics == nil {
		keySet[shortMetricName] = []string{metricName + "_" + tagString}
	} else {
		var index int
		var oriName string
		isFind := false
		for index, oriName = range dupMetrics {
			if oriName == metricName+"_"+tagString {
				isFind = true
				break
			}
		}
		if isFind {
			if index != 0 {
				metricName = fmt.Sprintf("%s%d", metricName, index-1)
			}
		} else {
			dupMetrics = append(dupMetrics, metricName+"_"+tagString)
			keySet[shortMetricName] = dupMetrics
			metricName = fmt.Sprintf("%s%d", metricName, index)
		}
	}
	return metricName
}

// @title get metric name and label information
func getNameLabelInfo(beanInfo *jjson.JsonObject, tags map[string]string, modulePrefix string) string {
	var metrixPerfix string
	for _, key := range beanInfo.GetKeys() {
		switch key {
		case "name":
			{
				name := beanInfo.GetString("name")
				prefix := []string{"Hadoop"}
				for _, label := range strings.Split(name[7:], ",") {
					items := strings.Split(label, "=")
					if items[0] == "service" {
						prefix = append(prefix, items[1])
					} else if items[0] != "name" {
						tags[items[0]] = items[1]
					}
				}

				metrixPerfix = strings.Join(prefix, "_")
				if modulePrefix != "" {
					metrixPerfix = metrixPerfix + "_" + modulePrefix
				}
			}
		case "modelerType":
			{
				//tags["modelerTypee"] = beanInfo.GetString("modelerType")
			}
		default:
			{
				if strings.HasPrefix(key, "tag.") {
					tags[key[4:]] = beanInfo.GetString(key)
				}
			}
		}
	}
	return metrixPerfix
}
