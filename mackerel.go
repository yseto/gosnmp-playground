package main

import (
	"container/list"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	mackerel "github.com/mackerelio/mackerel-client-go"
	"github.com/sirupsen/logrus"
)

var buffers = list.New()
var mutex = &sync.Mutex{}

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

var receiveDirection = map[string]bool{
	"ifInOctets":   true,
	"ifHCInOctets": true,
	"ifInDiscards": true,
	"ifInErrors":   true,
}

func runMackerel(ctx context.Context, collectParams *CollectParams) {
	client := mackerel.NewClient(apikey)
	hostId, err := initialForMackerel(collectParams.target, client)
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go ticker(ctx, &wg, hostId, collectParams)

	wg.Add(1)
	go sendTicker(ctx, &wg)
	wg.Wait()
}

func initialForMackerel(target string, client *mackerel.Client) (*string, error) {

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
	// delta
	for _, metric := range metrics {
		if deltaValues[metric.mib] {
			direction := "txBytes"
			if receiveDirection[metric.mib] {
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
			mkName := fmt.Sprintf("custom.interface.%s.%s", metric.mib, escapeInterfaceName(metric.ifName))
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
		records = append(records, []string{strconv.FormatUint(v.ifIndex, 10), v.mib, strconv.FormatUint(v.value, 10)})
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
		col, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ifIndex, err := strconv.ParseUint(col[0], 10, 64)
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseUint(col[2], 10, 64)
		if err != nil {
			return nil, err
		}
		data = append(data, SnapshotDutum{ifIndex: ifIndex, mib: col[1], value: value})
	}
	return data, nil
}

func ticker(ctx context.Context, wg *sync.WaitGroup, hostId *string, collectParams *CollectParams) {
	t := time.NewTicker(1 * time.Minute)
	defer func() {
		log.Info("stopping...")
		t.Stop()
		wg.Done()
	}()

	for {
		select {
		case <-t.C:
			err := innerTicker(ctx, hostId, collectParams)
			if err != nil {
				log.Warn(err)
			}
		case <-ctx.Done():
			log.Warn("cancellation from context:", ctx.Err())
			return
		}
	}
}

func innerTicker(ctx context.Context, hostId *string, collectParams *CollectParams) error {
	var snapshotData []SnapshotDutum
	if _, err := os.Stat(collectParams.snapshotPath); err == nil {
		snapshotData, err = loadSnapshot(collectParams.snapshotPath)
		if err != nil {
			return err
		}
	}
	savedSnapshot := make([]SnapshotDutum, 0)

	metrics := make([]MetricsDutum, 0)

	rawMetrics, err := collect(ctx, collectParams)
	if err != nil {
		return err
	}
	for _, metric := range rawMetrics {
		savedSnapshot = append(savedSnapshot,
			SnapshotDutum{
				ifIndex: metric.ifIndex,
				mib:     metric.mib,
				value:   metric.value,
			},
		)

		prevValue := metric.value
		for _, k := range snapshotData {
			if k.ifIndex == metric.ifIndex && k.mib == metric.mib {
				prevValue = k.value
				break
			}
		}

		value := calcurateDiff(prevValue, metric.value, overflowValue[metric.mib])
		metrics = append(metrics, MetricsDutum{
			ifIndex: metric.ifIndex,
			mib:     metric.mib,
			value:   value,
			ifName:  metric.ifName,
		})

		log.WithFields(logrus.Fields{
			"rawIfName": metric.ifName,
			"ifName":    escapeInterfaceName(metric.ifName),
			"mib":       metric.mib,
			"value":     value,
		}).Debug()
	}

	err = saveSnapshot(collectParams.snapshotPath, savedSnapshot)
	if err != nil {
		return err
	}

	mkMetrics := transform(hostId, metrics)
	mutex.Lock()
	buffers.PushBack(mkMetrics)
	mutex.Unlock()

	return nil
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
	log.Infof("send current value: %#v\n", e.Value)
	log.Infof("buffers len: %d\n", buffers.Len())

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
