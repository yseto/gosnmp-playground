package main

import (
	"container/list"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gosnmp/gosnmp"
	"github.com/sirupsen/logrus"
	mackerel "github.com/mackerelio/mackerel-client-go"
)

const (
	MIBifNumber     = "1.3.6.1.2.1.2.1.0"
	MIBifIndex      = "1.3.6.1.2.1.2.2.1.1"
	MIBifDescr      = "1.3.6.1.2.1.2.2.1.2"
	MIBifOperStatus = "1.3.6.1.2.1.2.2.1.8"
)

var mibOidmapping = map[string]string{
	"ifInOctets":    "1.3.6.1.2.1.2.2.1.10",
	"ifOutOctets":   "1.3.6.1.2.1.2.2.1.16",
	"ifHCInOctets":  "1.3.6.1.2.1.31.1.1.1.6",
	"ifHCOutOctets": "1.3.6.1.2.1.31.1.1.1.10",
	"ifInDiscards":  "1.3.6.1.2.1.2.2.1.13",
	"ifOutDiscards": "1.3.6.1.2.1.2.2.1.19",
	"ifInErrors":    "1.3.6.1.2.1.2.2.1.14",
	"ifOutErrors":   "1.3.6.1.2.1.2.2.1.20",
}

var overflowValue = map[string]uint64{
	"ifInOctets":    math.MaxUint32,
	"ifOutOctets":   math.MaxUint32,
	"ifHCInOctets":  math.MaxUint64,
	"ifHCOutOctets": math.MaxUint64,
	"ifInDiscards":  math.MaxUint64,
	"ifOutDiscards": math.MaxUint64,
	"ifInErrors":    math.MaxUint64,
	"ifOutErrors":   math.MaxUint64,
}

var deltaValues = map[string]bool{
	"ifInOctets":    true,
	"ifOutOctets":   true,
	"ifHCInOctets":  true,
	"ifHCOutOctets": true,
}

var receiveDirection = map[string]bool{
	"ifInOctets":   true,
	"ifHCInOctets": true,
	"ifInDiscards": true,
	"ifInErrors":   true,
}

type ResultWithName struct {
	name  string
	value map[uint64]uint64
}

type SnapshotDutum struct {
	ifIndex uint64
	key     string
	value   uint64
}

type MetricsDutum struct {
	name   string
	ifName string
	value  uint64
	delta  bool
}

var graphDefs = []*mackerel.GraphDefsParam{
	&mackerel.GraphDefsParam{
		Name:        "custom.interface.ifInDiscards",
		Unit:        "integer",
		DisplayName: "In Discards",
		Metrics: []*mackerel.GraphDefsMetric{
			&mackerel.GraphDefsMetric{
				Name:        "custom.interface.ifInDiscards.*",
				DisplayName: "%1",
			},
		},
	},
	&mackerel.GraphDefsParam{
		Name:        "custom.interface.ifOutDiscards",
		Unit:        "integer",
		DisplayName: "Out Discards",
		Metrics: []*mackerel.GraphDefsMetric{
			&mackerel.GraphDefsMetric{
				Name:        "custom.interface.ifOutDiscards.*",
				DisplayName: "%1",
			},
		},
	},
	&mackerel.GraphDefsParam{
		Name:        "custom.interface.ifInErrors",
		Unit:        "integer",
		DisplayName: "In Errors",
		Metrics: []*mackerel.GraphDefsMetric{
			&mackerel.GraphDefsMetric{
				Name:        "custom.interface.ifInErrors.*",
				DisplayName: "%1",
			},
		},
	},
	&mackerel.GraphDefsParam{
		Name:        "custom.interface.ifOutErrors",
		Unit:        "integer",
		DisplayName: "Out Errors",
		Metrics: []*mackerel.GraphDefsMetric{
			&mackerel.GraphDefsMetric{
				Name:        "custom.interface.ifOutErrors.*",
				DisplayName: "%1",
			},
		},
	},
}

type CollectParams struct {
	community, target, snapshotPath    string
	mibs                               []string
	includeRegexp, excludeRegexp       *regexp.Regexp
	includeInterface, excludeInterface *string
	skipDownLinkState                  *bool
}

var buffers = list.New()
var mutex = &sync.Mutex{}
var apikey = os.Getenv("MACKEREL_API_KEY")
var log = logrus.New()

func parseFlags() (*CollectParams, error) {
	var community, target string
	flag.StringVar(&community, "community", "public", "the community string for device")
	flag.StringVar(&target, "target", "127.0.0.1", "ip address")
	includeInterface := flag.String("include-interface", "", "include interface name")
	excludeInterface := flag.String("exclude-interface", "", "exclude interface name")
	rawMibs := flag.String("mibs", "all", "mib name joind with ',' or 'all'")
	level := flag.Bool("verbose", false, "verbose")
	skipDownLinkState := flag.Bool("skip-down-link-state", false, "skip down link state")
	flag.Parse()

	logLevel := logrus.WarnLevel
	if *level {
		logLevel = logrus.DebugLevel
	}
	log.SetLevel(logLevel)

	if *includeInterface != "" && *excludeInterface != "" {
		return nil, errors.New("excludeInterface, includeInterface is exclusive control.")
	}
	includeReg, err := regexp.Compile(*includeInterface)
	if err != nil {
		return nil, err
	}
	excludeReg, err := regexp.Compile(*excludeInterface)
	if err != nil {
		return nil, err
	}

	mibs, err := mibsValidate(rawMibs)
	if err != nil {
		return nil, err
	}

	ssPath, err := snapshotPath(target)
	if err != nil {
		return nil, err
	}

	return &CollectParams{
		target:            target,
		community:         community,
		mibs:              mibs,
		snapshotPath:      ssPath,
		includeRegexp:     includeReg,
		excludeRegexp:     excludeReg,
		includeInterface:  includeInterface,
		excludeInterface:  excludeInterface,
		skipDownLinkState: skipDownLinkState,
	}, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.SetFormatter(&logrus.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})

	collectParams, err := parseFlags()
	if err != nil {
		log.Fatal(err)
	}

	log.Info("start")

	wg := sync.WaitGroup{}
	wg.Add(1)
	go ticker(ctx, &wg, collectParams)

	if apikey == "" {
		log.Info("skip mackerel")
	} else {
		wg.Add(1)
		go sendTicker(ctx, &wg)
	}
	wg.Wait()
}

func ticker(ctx context.Context, wg *sync.WaitGroup, c *CollectParams) {
	t := time.NewTicker(1 * time.Minute)
	defer func() {
		log.Info("stopping...")
		t.Stop()
		wg.Done()
	}()

	var hostId *string
	if apikey != "" {
		var err error
		hostId, err = initialForMackerel(c.target)
		if err != nil {
			log.Fatal(err)
		}
	}

	for {
		select {
		case <-t.C:
			metrics, err := collect(ctx, c)
			if err != nil {
				log.Warn(err)
			}

			if apikey != "" {
				mkMetrics := transform(hostId, metrics)
				mutex.Lock()
				buffers.PushBack(mkMetrics)
				mutex.Unlock()
			}

		case <-ctx.Done():
			log.Warn("cancellation from context:", ctx.Err())
			return
		}
	}
}

func sendTicker(ctx context.Context, wg *sync.WaitGroup) {
	t := time.NewTicker(500 * time.Millisecond)
	client := mackerel.NewClient(apikey)

	defer func() {
		log.Info("stopping...")
		t.Stop()
		wg.Done()
	}()

	for {
		select {
		case <-t.C:
			send(ctx, client)

		case <-ctx.Done():
			log.Warn("cancellation from context:", ctx.Err())
			return
		}
	}
}

func send(ctx context.Context, client *mackerel.Client) {
	if buffers.Len() == 0 {
		return
	}

	e := buffers.Front()
	// log.Info("send current value: %#v\n", e.Value)
	// log.Info("buffers len: %d\n", buffers.Len())

	err := client.PostHostMetricValues(e.Value.([]*mackerel.HostMetricValue))
	if err != nil {
		log.Warn(err)
		return
	} else {
		log.Info("send mackerel")
	}
	mutex.Lock()
	buffers.Remove(e)
	mutex.Unlock()
}

func collect(ctx context.Context, c *CollectParams) ([]MetricsDutum, error) {
	gosnmp.Default.Target = c.target
	gosnmp.Default.Community = c.community
	gosnmp.Default.Timeout = time.Duration(10 * time.Second)
	gosnmp.Default.Context = ctx
	err := gosnmp.Default.Connect()
	if err != nil {
		return nil, err
	}
	defer gosnmp.Default.Conn.Close()

	ifNumber, err := getInterfaceNumber()
	if err != nil {
		return nil, err
	}
	ifDescr, err := bulkWalkGetInterfaceName(ifNumber)
	if err != nil {
		return nil, err
	}

	var ifOperStatus map[uint64]bool
	if *c.skipDownLinkState {
		ifOperStatus, err = bulkWalkGetInterfaceState(ifNumber)
		if err != nil {
			return nil, err
		}
	}

	ifIndex, err := bulkWalkGetInterfaceNumber(ifNumber)
	if err != nil {
		return nil, err
	}

	results := make([]ResultWithName, 0, len(c.mibs))
	for _, mib := range c.mibs {
		result, err := bulkWalk(mibOidmapping[mib], ifNumber)
		if err != nil {
			return nil, err
		}
		results = append(results, ResultWithName{name: mib, value: result})
	}

	var snapshotData []SnapshotDutum
	if _, err := os.Stat(c.snapshotPath); err == nil {
		snapshotData, err = loadSnapshot(c.snapshotPath)
		if err != nil {
			return nil, err
		}
	}

	savedSnapshot := make([]SnapshotDutum, 0)

	metrics := make([]MetricsDutum, 0, len(results))
	for _, ifNum := range ifIndex {
		ifName := ifDescr[ifNum]
		if *c.includeInterface != "" && !c.includeRegexp.MatchString(ifName) {
			continue
		}

		if *c.excludeInterface != "" && c.excludeRegexp.MatchString(ifName) {
			continue
		}

		// skip when down(2)
		if *c.skipDownLinkState && ifOperStatus[ifNum] == false {
			continue
		}

		for _, rwn := range results {
			key := rwn.name
			value := rwn.value[ifNum]

			savedSnapshot = append(savedSnapshot, SnapshotDutum{ifIndex: ifNum, key: key, value: value})

			prevValue := value
			for _, k := range snapshotData {
				if k.ifIndex == ifNum && k.key == key {
					prevValue = k.value
					break
				}
			}
			value = calcurateDiff(prevValue, value, overflowValue[key])
			metrics = append(metrics, MetricsDutum{name: key, ifName: ifName, value: value, delta: deltaValues[key]})

			log.Debugf("%s\t%s\t%d\n", escapeInterfaceName(ifName), key, value)
		}
	}
	err = saveSnapshot(c.snapshotPath, savedSnapshot)
	if err != nil {
		return nil, err
	}
	return metrics, nil
}

func escapeInterfaceName(ifName string) string {
	return strings.Replace(strings.Replace(strings.Replace(ifName, "/", "-", -1), ".", "_", -1), " ", "", -1)
}

func initialForMackerel(target string) (*string, error) {
	client := mackerel.NewClient(apikey)

	log.Info("init for mackerel")

	idPath, err := hostIdPath(target)
	if err != nil {
		return nil, err
	}
	mkInterfaces := []mackerel.Interface{
		mackerel.Interface{
			Name:          "main",
			IPv4Addresses: []string{target},
		},
	}
	var hostId string
	if _, err := os.Stat(idPath); err == nil {
		bytes, err := os.ReadFile(idPath)
		if err != nil {
			return nil, err
		}
		hostId = string(bytes)
		_, err = client.UpdateHost(hostId, &mackerel.UpdateHostParam{
			Name:       target,
			Interfaces: mkInterfaces,
		})
		if err != nil {
			return nil, err
		}
	} else {
		hostId, err = client.CreateHost(&mackerel.CreateHostParam{
			Name:       target,
			Interfaces: mkInterfaces,
		})
		if err != nil {
			return nil, err
		}
		err = os.WriteFile(idPath, []byte(hostId), 0666)
		if err != nil {
			return nil, err
		}
	}
	err = client.CreateGraphDefs(graphDefs)
	if err != nil {
		return nil, err
	}
	return &hostId, nil
}

func transform(hostId *string, metrics []MetricsDutum) []*mackerel.HostMetricValue {
	now := time.Now().Unix()

	mkMetrics := make([]*mackerel.HostMetricValue, 0, len(metrics))
	for _, metric := range metrics {
		if metric.delta {
			direction := "txBytes"
			if receiveDirection[metric.name] {
				direction = "rxBytes"
			}
			mkName := fmt.Sprintf("interface.%s.%s.delta", escapeInterfaceName(metric.ifName), direction)
			mkMetrics = append(mkMetrics, &mackerel.HostMetricValue{
				HostID: *hostId,
				MetricValue: &mackerel.MetricValue{
					Name:  mkName,
					Time:  now,
					Value: (metric.value / 60),
				},
			})
		} else {
			mkName := fmt.Sprintf("custom.interface.%s.%s", metric.name, escapeInterfaceName(metric.ifName))
			mkMetrics = append(mkMetrics, &mackerel.HostMetricValue{
				HostID: *hostId,
				MetricValue: &mackerel.MetricValue{
					Name:  mkName,
					Time:  now,
					Value: metric.value,
				},
			})
		}
	}
	return mkMetrics
}

func hostIdPath(target string) (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(wd, fmt.Sprintf("%s.id.txt", target)), nil
}

func snapshotPath(target string) (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(wd, fmt.Sprintf("%s.txt", target)), nil
}

func saveSnapshot(ssPath string, snapshot []SnapshotDutum) error {
	file, err := os.Create(ssPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	records := make([][]string, 0)

	for _, v := range snapshot {
		records = append(records, []string{strconv.FormatUint(v.ifIndex, 10), v.key, strconv.FormatUint(v.value, 10)})
	}

	err = writer.WriteAll(records)
	if err != nil {
		return err
	}

	if err := writer.Error(); err != nil {
		return err
	}
	return nil
}

func loadSnapshot(ssPath string) ([]SnapshotDutum, error) {
	file, err := os.Open(ssPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data := make([]SnapshotDutum, 0)
	reader := csv.NewReader(file)
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ifIndex, err := strconv.ParseUint(line[0], 10, 64)
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseUint(line[2], 10, 64)
		if err != nil {
			return nil, err
		}
		data = append(data, SnapshotDutum{ifIndex: ifIndex, key: line[1], value: value})
	}
	return data, nil
}

func mibsValidate(rawMibs *string) ([]string, error) {
	var parseMibs []string
	switch *rawMibs {
	case "all":
		for key := range mibOidmapping {
			// skipped 32 bit octets.
			if key == "ifInOctets" || key == "ifOutOctets" {
				continue
			}
			parseMibs = append(parseMibs, key)
		}
	case "":
	default:
		for _, name := range strings.Split(*rawMibs, ",") {
			if _, exists := mibOidmapping[name]; !exists {
				return nil, fmt.Errorf("mib %s is not supported.", name)
			}
			parseMibs = append(parseMibs, name)
		}
	}
	return parseMibs, nil
}

func calcurateDiff(a, b, overflow uint64) uint64 {
	if b < a {
		return overflow - a + b
	} else {
		return b - a
	}
}

func captureIfIndex(oid, name string) (uint64, error) {
	indexStr := strings.Replace(name, "."+oid+".", "", 1)
	return strconv.ParseUint(indexStr, 10, 64)
}

func getInterfaceNumber() (uint64, error) {
	result, err := gosnmp.Default.Get([]string{MIBifNumber})
	if err != nil {
		return 0, err
	}
	variable := result.Variables[0]
	switch variable.Type {
	case gosnmp.OctetString:
		return 0, errors.New("cant get interface number")
	default:
		return gosnmp.ToBigInt(variable.Value).Uint64(), nil
	}
}

func bulkWalkGetInterfaceNumber(length uint64) ([]uint64, error) {
	kv := make([]uint64, 0, length)
	err := gosnmp.Default.BulkWalk(MIBifIndex, func(pdu gosnmp.SnmpPDU) error {
		switch pdu.Type {
		case gosnmp.OctetString:
			return errors.New("cant parse interface number.")
		default:
			kv = append(kv, gosnmp.ToBigInt(pdu.Value).Uint64())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(kv, func(i, j int) bool {
		return kv[i] < kv[j]
	})
	return kv, nil
}

func bulkWalkGetInterfaceName(length uint64) (map[uint64]string, error) {
	kv := make(map[uint64]string, length)
	err := gosnmp.Default.BulkWalk(MIBifDescr, func(pdu gosnmp.SnmpPDU) error {
		index, err := captureIfIndex(MIBifDescr, pdu.Name)
		if err != nil {
			return err
		}
		switch pdu.Type {
		case gosnmp.OctetString:
			kv[index] = string(pdu.Value.([]byte))
		default:
			return errors.New("cant parse interface name.")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return kv, nil
}

func bulkWalkGetInterfaceState(length uint64) (map[uint64]bool, error) {
	kv := make(map[uint64]bool, length)
	err := gosnmp.Default.BulkWalk(MIBifOperStatus, func(pdu gosnmp.SnmpPDU) error {
		index, err := captureIfIndex(MIBifOperStatus, pdu.Name)
		if err != nil {
			return err
		}
		switch pdu.Type {
		case gosnmp.OctetString:
			return errors.New("cant parse value.")
		default:
			tmp := gosnmp.ToBigInt(pdu.Value).Uint64()
			if tmp != 2 {
				kv[index] = true
			} else {
				kv[index] = false
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return kv, nil
}

func bulkWalk(oid string, length uint64) (map[uint64]uint64, error) {
	kv := make(map[uint64]uint64, length)
	err := gosnmp.Default.BulkWalk(oid, func(pdu gosnmp.SnmpPDU) error {
		index, err := captureIfIndex(oid, pdu.Name)
		if err != nil {
			return err
		}
		switch pdu.Type {
		case gosnmp.OctetString:
			return errors.New("cant parse value.")
		default:
			kv[index] = gosnmp.ToBigInt(pdu.Value).Uint64()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return kv, nil
}
