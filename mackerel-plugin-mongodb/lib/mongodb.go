package mpmongodb

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	mp "github.com/mackerelio/go-mackerel-plugin-helper"
	"github.com/mackerelio/golib/logging"
)

type KeyNotFound string

func (k KeyNotFound) Error() string {
	return fmt.Sprintf("%v not found", k)
}

var logger = logging.GetLogger("metrics.plugin.mongodb")

var graphdef = map[string]mp.Graphs{
	"mongodb.background_flushing": {
		Label: "MongoDB Command",
		Unit:  "float",
		Metrics: []mp.Metrics{
			{Name: "duration_ms", Label: "Duration in ms", Diff: true, Type: "uint64"},
		},
	},
	"mongodb.connections": {
		Label: "MongoDB Connections",
		Unit:  "integer",
		Metrics: []mp.Metrics{
			{Name: "connections_current", Label: "current"},
			{Name: "connections_available", Label: "available"},
		},
	},
	"mongodb.index_counters.btree": {
		Label: "MongoDB Index Counters Btree",
		Unit:  "integer",
		Metrics: []mp.Metrics{
			{Name: "index_hits", Label: "hits", Diff: true, Type: "uint64"},
			{Name: "index_btree_hits", Label: "hits", Diff: true, Type: "uint64"},
		},
	},
	"mongodb.opcounters": {
		Label: "MongoDB opcounters",
		Unit:  "integer",
		Metrics: []mp.Metrics{
			{Name: "opcounters_insert", Label: "Insert", Diff: true, Type: "uint64"},
			{Name: "opcounters_query", Label: "Query", Diff: true, Type: "uint64"},
			{Name: "opcounters_update", Label: "Update", Diff: true, Type: "uint64"},
			{Name: "opcounters_delete", Label: "Delete", Diff: true, Type: "uint64"},
			{Name: "opcounters_getmore", Label: "Getmore", Diff: true, Type: "uint64"},
			{Name: "opcounters_command", Label: "Command", Diff: true, Type: "uint64"},
		},
	},
	"mongodb.globallock": {
		Label: "MongoDB GlobalLock operations",
		Unit:  "integer",
		Metrics: []mp.Metrics{
			{Name: "globallock_client_readers", Label: "ClientReaders", Diff: false, Type: "uint64"},
			{Name: "globallock_client_writers", Label: "ClientWriters", Diff: false, Type: "uint64"},
			{Name: "globallock_queue_readers", Label: "QueueReaders", Diff: false, Type: "uint64"},
			{Name: "globallock_queue_writers", Label: "QueueWriters", Diff: false, Type: "uint64"},
		},
	},
	"mongodb.dur_commits": {
		Label: "MongoDB MMAPv1 journal transactions",
		Unit:  "integer",
		Metrics: []mp.Metrics{
			{Name: "dur_commits", Label: "Commits", Diff: false, Type: "uint64"},
		},
	},
	"mongodb.dur_journaled": {
		Label: "MongoDB MMAPv1 journal size",
		Unit:  "bytes",
		Metrics: []mp.Metrics{
			{Name: "dur_journaled", Label: "JournalSize", Diff: false, Type: "uint64", Scale: 1024 * 1024},
		},
	},
	"mongodb.memory": {
		Label: "MongoDB Memory",
		Unit:  "bytes",
		Metrics: []mp.Metrics{
			{Name: "mem_virtual", Label: "Virtual", Diff: false, Type: "uint64", Scale: 1024 * 1024},
			{Name: "mem_resident", Label: "Resident", Diff: false, Type: "uint64", Scale: 1024 * 1024},
			{Name: "mem_mapped", Label: "Mapped", Diff: false, Type: "uint64", Scale: 1024 * 1024},
		},
	},
	"mongodb.page_faults": {
		Label: "MongoDB Page Faults",
		Unit:  "integer",
		Metrics: []mp.Metrics{
			{Name: "page_faults", Label: "Count", Diff: false, Type: "uint64"},
		},
	},
	"mongodb.asserts": {
		Label: "MongoDB Asserts",
		Unit:  "integer",
		Metrics: []mp.Metrics{
			{Name: "asserts_msg", Label: "Msg", Diff: false, Type: "uint64"},
			{Name: "asserts_warning", Label: "Warning", Diff: false, Type: "uint64"},
			{Name: "asserts_regular", Label: "Regular", Diff: false, Type: "uint64"},
			{Name: "asserts_user", Label: "User", Diff: false, Type: "uint64"},
		},
	},
	"mongodb.wiredtiger_transactions": {
		Label: "MongoDB WiredTiger Transaction Tickets",
		Unit:  "integer",
		Metrics: []mp.Metrics{
			{Name: "wiredtiger_read_out", Label: "ReadOut", Diff: false, Type: "uint64"},
			{Name: "wiredtiger_write_out", Label: "WriteOut", Diff: false, Type: "uint64"},
			{Name: "wiredtiger_read_avl", Label: "ReadAvailable", Diff: false, Type: "uint64"},
			{Name: "wiredtiger_write_avl", Label: "WriteAvailable", Diff: false, Type: "uint64"},
		},
	},
	"mongodb.wiredtiger_cache": {
		Label: "MongoDB WiredTiger Caches",
		Unit:  "bytes",
		Metrics: []mp.Metrics{
			{Name: "wiredtiger_cache_bytes", Label: "Current", Diff: false, Type: "uint64"},
			{Name: "wiredtiger_cache_maximum", Label: "Max", Diff: false, Type: "uint64"},
			{Name: "wiredtiger_cache_tracked", Label: "Tracked", Diff: false, Type: "uint64"},
			{Name: "wiredtiger_cache_unmodified", Label: "Unmodified", Diff: false, Type: "uint64"},
			{Name: "wiredtiger_cache_modified", Label: "Modified", Diff: false, Type: "uint64"},
		},
	},
}

var metricPlace = map[string][]string{
	"duration_ms":                 {"backgroundFlushing", "total_ms"},
	"dur_commits":                 {"dur", "commits"},
	"dur_journaled":               {"dur", "journaledMB"},
	"connections_current":         {"connections", "current"},
	"connections_available":       {"connections", "available"},
	"index_btree_hits":            {"indexCounters", "btree", "hits"},
	"index_hits":                  {"indexCounters", "hits"},
	"opcounters_insert":           {"opcounters", "insert"},
	"opcounters_query":            {"opcounters", "query"},
	"opcounters_update":           {"opcounters", "update"},
	"opcounters_delete":           {"opcounters", "delete"},
	"opcounters_getmore":          {"opcounters", "getmore"},
	"opcounters_command":          {"opcounters", "command"},
	"globallock_client_readers":   {"globalLock", "activeClients", "readers"},
	"globallock_client_writers":   {"globalLock", "activeClients", "writers"},
	"globallock_queue_readers":    {"globalLock", "currentQueue", "readers"},
	"globallock_queue_writers":    {"globalLock", "currentQueue", "writers"},
	"mem_virtual":                 {"mem", "virtual"},
	"mem_resident":                {"mem", "resident"},
	"mem_mapped":                  {"mem", "mapped"},
	"page_faults":                 {"extra_info", "page_faults"},
	"asserts_msg":                 {"asserts", "msg"},
	"asserts_warning":             {"asserts", "warning"},
	"asserts_regular":             {"asserts", "regular"},
	"asserts_user":                {"asserts", "user"},
	"wiredtiger_read_out":         {"wiredTiger", "concurrentTransactions", "read", "out"},
	"wiredtiger_read_avl":         {"wiredTiger", "concurrentTransactions", "read", "available"},
	"wiredtiger_write_out":        {"wiredTiger", "concurrentTransactions", "write", "out"},
	"wiredtiger_write_avl":        {"wiredTiger", "concurrentTransactions", "write", "available"},
	"wiredtiger_cache_bytes":      {"wiredTiger", "cache", "bytes currently in the cache"},
	"wiredtiger_cache_maximum":    {"wiredTiger", "cache", "maximum bytes configured"},
	"wiredtiger_cache_tracked":    {"wiredTiger", "cache", "tracked dirty bytes in the cache"},
	"wiredtiger_cache_unmodified": {"wiredTiger", "cache", "unmodified pages evicted"},
	"wiredtiger_cache_modified":   {"wiredTiger", "cache", "modified pages evicted"},
}

func getFloatValue(s map[string]interface{}, keys []string) (float64, error) {
	var val float64
	sm := s
	var err error
	for i, k := range keys {
		v, ok := sm[k]
		if !ok {
			return 0, KeyNotFound(k)
		}
		if i+1 < len(keys) {
			switch v.(type) {
			case bson.M:
				sm = sm[k].(bson.M)
			default:
				return 0, fmt.Errorf("Cannot handle as a hash for %s", k)
			}
		} else {
			val, err = strconv.ParseFloat(fmt.Sprint(v), 64)
			if err != nil {
				return 0, err
			}
		}
	}

	return val, nil
}

// MongoDBPlugin mackerel plugin for mongo
type MongoDBPlugin struct {
	URL      string
	Username string
	Password string
	Verbose  bool
}

func (m MongoDBPlugin) fetchStatus() (bson.M, error) {
	mongoDBDialInfo := &mgo.DialInfo{
		Addrs:    []string{m.URL},
		Username: m.Username,
		Password: m.Password,
		Direct:   true,
		Timeout:  10 * time.Second,
	}
	session, err := mgo.DialWithInfo(mongoDBDialInfo)
	if err != nil {
		return nil, err
	}

	defer session.Close()
	session.SetMode(mgo.Eventual, true)
	serverStatus := bson.M{}
	if err := session.Run("serverStatus", &serverStatus); err != nil {
		return nil, err
	}
	if m.Verbose {
		str, err := json.Marshal(serverStatus)
		if err != nil {
			fmt.Println(fmt.Errorf("Marshaling error: %s", err.Error()))
		}
		fmt.Println(string(str))
	}
	return serverStatus, nil
}

// FetchMetrics interface for mackerelplugin
func (m MongoDBPlugin) FetchMetrics() (map[string]interface{}, error) {
	serverStatus, err := m.fetchStatus()
	if err != nil {
		return nil, err
	}
	return m.parseStatus(serverStatus)
}

func (m MongoDBPlugin) parseStatus(serverStatus bson.M) (map[string]interface{}, error) {
	stat := make(map[string]interface{})

	for k, v := range metricPlace {
		val, err := getFloatValue(serverStatus, v)
		if err != nil {
			if _, ok := err.(KeyNotFound); ok {
				logger.Debugf("Cannot fetch metric %s: %s", v, err)
				continue
			} else {
				logger.Warningf("Cannot fetch metric %s: %s", v, err)
			}
		} else {
			stat[k] = val
		}
	}

	return stat, nil
}

// GraphDefinition interface for mackerelplugin
func (m MongoDBPlugin) GraphDefinition() map[string]mp.Graphs {
	serverStatus, err := m.fetchStatus()

	g := make(map[string]mp.Graphs)
	if err != nil {
		return graphdef
	}

	for k, v := range graphdef {
		var metrics []mp.Metrics
		for _, m := range v.Metrics {
			name := m.Name
			_, err := getFloatValue(serverStatus, metricPlace[name])
			if err == nil {
				metrics = append(metrics, m)
			}
		}
		if len(metrics) > 0 {
			v.Metrics = metrics
			g[k] = v
		}
	}

	return g
}

// Do the plugin
func Do() {
	optHost := flag.String("host", "localhost", "Hostname")
	optPort := flag.String("port", "27017", "Port")
	optUser := flag.String("username", "", "Username")
	optPass := flag.String("password", os.Getenv("MONGODB_PASSWORD"), "Password")
	optVerbose := flag.Bool("v", false, "Verbose mode")
	optTempfile := flag.String("tempfile", "", "Temp file name")
	flag.Parse()

	var mongodb MongoDBPlugin
	mongodb.Verbose = *optVerbose
	mongodb.URL = fmt.Sprintf("%s:%s", *optHost, *optPort)
	mongodb.Username = fmt.Sprintf("%s", *optUser)
	mongodb.Password = fmt.Sprintf("%s", *optPass)

	helper := mp.NewMackerelPlugin(mongodb)
	if *optTempfile != "" {
		helper.Tempfile = *optTempfile
	} else {
		helper.SetTempfileByBasename(fmt.Sprintf("mackerel-plugin-mongodb-%s-%s", *optHost, *optPort))
	}

	helper.Run()
}
